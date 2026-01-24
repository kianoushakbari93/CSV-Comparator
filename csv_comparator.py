#!/usr/bin/env python3
"""
CSV Comparator - High-Performance Migration Validation
Hive to Snowflake Data Comparison Tool

Features:
- Multiprocessing for parallel comparison
- Intelligent composite key detection
- Deep row-by-row comparison with normalisation
- Optimised for tables with millions of rows
"""

import pandas as pd
import sys
import re
import os
import csv
import hashlib
import tempfile
import argparse
import multiprocessing as mp
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager


def normalise_value(value, skip_normalisation=False):
    """
    Normalise values for consistent comparison.
    Preserves whitespace exactly as-is for strict validation.
    Handles nulls, numeric formatting, timestamps, and booleans.
    Does NOT convert ambiguous date formats (DD/MM/YYYY vs MM/DD/YYYY).
    
    If skip_normalisation=True, returns value as-is (string conversion only).
    """
    if pd.isna(value):
        return None
    
    str_value = str(value)
    
    # If skipping normalisation, just return string value (but still handle pandas NA)
    if skip_normalisation:
        if str_value.lower().strip() in ['', 'nan', 'nat']:
            return None
        return str_value
    
    # Check for null values (but preserve whitespace in actual data)
    if str_value.lower().strip() in ['', 'null', 'none', 'nan', 'nat', 'n/a', 'na', '#n/a']:
        return None
    
    stripped = str_value.strip()
    
    # Normalise boolean values to lowercase true/false
    # Note: '0' and '1' are intentionally excluded to avoid false positives with numeric data
    if stripped.lower() in ['true', 'yes', 'y']:
        return 'true'
    if stripped.lower() in ['false', 'no', 'n']:
        return 'false'
    
    # Normalise timestamp formats - strip ALL decimal places
    # Format: YYYY-MM-DD HH:MM:SS.fffffffff -> YYYY-MM-DD HH:MM:SS
    timestamp_match = re.match(r'^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})\.(\d+)$', stripped)
    if timestamp_match:
        datetime_part = timestamp_match.group(1).replace('T', ' ')
        return datetime_part
    
    # Timestamp without fractional seconds - return as-is
    if re.match(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}$', stripped):
        return stripped.replace('T', ' ')
    
    # DO NOT convert DD/MM/YYYY or MM/DD/YYYY formats - they are ambiguous
    # Leave them exactly as-is in the source data
    
    # ISO format YYYY-MM-DD: validate but don't convert (already in correct format)
    iso_date_match = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', stripped)
    if iso_date_match:
        year, month, day = int(iso_date_match.group(1)), int(iso_date_match.group(2)), int(iso_date_match.group(3))
        # Only accept as valid ISO date if values are reasonable
        if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
            return stripped
    
    # Normalise numeric formatting (only for pure numeric values)
    try:
        clean_num = stripped.replace(',', '')
        float_val = float(clean_num)
        if str_value.strip() == str_value:
            if float_val.is_integer():
                return str(int(float_val))
            # Round to 6 decimal places for consistent comparison
            return str(round(float_val, 6))
    except (ValueError, TypeError):
        pass
    
    # Return value exactly as-is, preserving all whitespace
    return str_value


def detect_delimiter_for_line(line):
    """Detect the most likely delimiter for a single line."""
    delimiters = {'|': 0, ',': 0, '\t': 0, '~': 0, ';': 0}
    for delim in delimiters:
        delimiters[delim] = line.count(delim)
    
    best_delim = max(delimiters, key=delimiters.get)
    if delimiters[best_delim] == 0:
        return ','
    return best_delim


def detect_delimiters(filepath, sample_lines=10):
    """
    Detect CSV delimiters separately for header and data rows.
    
    Returns tuple: (header_delimiter, row_delimiter)
    """
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = []
            for _ in range(sample_lines + 1):
                line = f.readline()
                if line:
                    lines.append(line.strip())
        
        if not lines:
            return ',', ','
        
        header_line = lines[0]
        header_delim = detect_delimiter_for_line(header_line)
        
        data_lines = lines[1:] if len(lines) > 1 else []
        
        if not data_lines:
            return header_delim, header_delim
        
        delimiters = {'|': 0, ',': 0, '\t': 0, '~': 0, ';': 0}
        for line in data_lines:
            for delim in delimiters:
                delimiters[delim] += line.count(delim)
        
        row_delim = max(delimiters, key=delimiters.get)
        if delimiters[row_delim] == 0:
            row_delim = header_delim
        
        return header_delim, row_delim
        
    except Exception:
        return ',', ','


def preprocess_quoted_rows(filepath):
    """
    Fix rows that are entirely quoted (start with " and end with ").
    Also normalises Windows CRLF line endings to Unix LF.
    
    Some exports wrap entire rows in quotes, e.g.:
        "value1|value2|value3"
    This causes delimiters inside to be treated as data, not separators.
    
    Returns: (cleaned_filepath, rows_fixed_count)
    """
    # First pass: check if any rows need fixing
    rows_needing_quote_fix = 0
    has_crlf = False
    
    with open(filepath, 'rb') as f:
        sample = f.read(65536)
        if b'\r\n' in sample:
            has_crlf = True
    
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        f.readline()  # Skip header
        for line in f:
            stripped = line.rstrip('\r\n')
            if stripped.startswith('"') and stripped.endswith('"') and len(stripped) > 1:
                rows_needing_quote_fix += 1
    
    if rows_needing_quote_fix == 0 and not has_crlf:
        return filepath, 0
    
    # Second pass: create cleaned file
    temp_fd, temp_path = tempfile.mkstemp(suffix='.csv', prefix='cleaned_')
    
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as infile, \
         os.fdopen(temp_fd, 'w', encoding='utf-8', newline='') as outfile:
        
        for line in infile:
            stripped = line.rstrip('\r\n')
            if stripped.startswith('"') and stripped.endswith('"') and len(stripped) > 1:
                stripped = stripped[1:-1]
            outfile.write(stripped + '\n')
    
    return temp_path, rows_needing_quote_fix


@contextmanager
def cleaned_csv(filepath):
    """
    Context manager for temporary cleaned CSV files.
    Ensures temp files are always cleaned up, even if exceptions occur.
    """
    cleaned_path, rows_fixed = preprocess_quoted_rows(filepath)
    try:
        yield cleaned_path, rows_fixed
    finally:
        if cleaned_path != filepath:
            try:
                os.remove(cleaned_path)
            except OSError:
                pass  # Temp file cleanup is best-effort


def load_csv_file(filepath, source_name):
    """Load CSV with automatic delimiter detection and row_text handling."""
    with cleaned_csv(filepath) as (cleaned_filepath, rows_fixed):
        try:
            if rows_fixed > 0:
                print(f"  [!] Fixed {rows_fixed:,} rows with errant surrounding quotes")
            
            header_delim, row_delim = detect_delimiters(cleaned_filepath)
            header_delim_name = {',': 'comma', '|': 'pipe', '\t': 'tab', '~': 'tilde', ';': 'semicolon'}.get(header_delim, repr(header_delim))
            row_delim_name = {',': 'comma', '|': 'pipe', '\t': 'tab', '~': 'tilde', ';': 'semicolon'}.get(row_delim, repr(row_delim))
            
            if header_delim == row_delim:
                print(f"  Detected delimiter: {header_delim_name}")
            else:
                print(f"  Detected header delimiter: {header_delim_name}")
                print(f"  Detected row delimiter: {row_delim_name}")
            
            if header_delim != row_delim:
                df = load_mixed_delimiter_csv(cleaned_filepath, header_delim, row_delim)
            else:
                quoting_mode = csv.QUOTE_NONE if rows_fixed > 0 else csv.QUOTE_MINIMAL
                
                df = pd.read_csv(
                    cleaned_filepath,
                    sep=header_delim,
                    dtype=str,
                    keep_default_na=False,
                    quoting=quoting_mode,
                    skipinitialspace=True,
                    on_bad_lines='warn',
                    engine='c',
                    low_memory=False
                )
            
            df.columns = df.columns.str.strip().str.upper()
            
            if len(df.columns) == 1 and df.columns[0] in ['ROW_TEXT', 'ROW', 'TEXT', 'DATA', 'LINE']:
                print(f"  Detected single-column format: {df.columns[0]}")
                df = expand_row_text_column(df)
                print(f"  Expanded to {len(df.columns)} columns")
            
            print(f"  [OK] Loaded {source_name}: {len(df):,} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            print(f"  [X] Error loading {source_name}: {e}")
            sys.exit(1)


def load_mixed_delimiter_csv(filepath, header_delim, row_delim):
    """
    Load CSV where header uses one delimiter but data rows use another.
    """
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        header_line = f.readline().strip()
        column_names = [col.strip().upper() for col in header_line.split(header_delim)]
        
        data_rows = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            row_values = line.split(row_delim)
            data_rows.append(row_values)
    
    df = pd.DataFrame(data_rows)
    
    if len(df.columns) == len(column_names):
        df.columns = column_names
    elif len(df.columns) > 0:
        print(f"  Warning: Column count mismatch (header: {len(column_names)}, data: {len(df.columns)})")
        if len(df.columns) < len(column_names):
            df.columns = column_names[:len(df.columns)]
        else:
            extra_cols = [f'COL_{i+1}' for i in range(len(column_names), len(df.columns))]
            df.columns = column_names + extra_cols
    
    return df


def expand_row_text_column(df):
    """
    Expand a single row_text column into multiple columns.
    Detects the delimiter used within the data and creates COL_1, COL_2, etc.
    """
    if len(df) == 0:
        return df
    
    col_name = df.columns[0]
    sample_values = df[col_name].head(10).tolist()
    
    delimiters = {'|': 0, ',': 0, '\t': 0, ';': 0, '~': 0}
    for val in sample_values:
        if pd.notna(val) and val:
            for delim in delimiters:
                delimiters[delim] += str(val).count(delim)
    
    internal_delim = max(delimiters, key=delimiters.get)
    
    if delimiters[internal_delim] == 0:
        return df
    
    delim_name = {'|': 'pipe', ',': 'comma', '\t': 'tab', ';': 'semicolon', '~': 'tilde'}.get(internal_delim, internal_delim)
    print(f"  Detected internal delimiter: {delim_name}")
    
    first_valid = None
    for val in sample_values:
        if pd.notna(val) and val and internal_delim in str(val):
            first_valid = str(val)
            break
    
    if not first_valid:
        return df
    
    num_cols = len(first_valid.split(internal_delim))
    col_names = [f'COL_{i+1}' for i in range(num_cols)]
    
    expanded_data = []
    for idx, row in df.iterrows():
        val = row[col_name]
        if pd.notna(val) and val:
            parts = str(val).split(internal_delim)
            if len(parts) < num_cols:
                parts.extend([''] * (num_cols - len(parts)))
            elif len(parts) > num_cols:
                parts = parts[:num_cols]
            expanded_data.append(parts)
        else:
            expanded_data.append([''] * num_cols)
    
    return pd.DataFrame(expanded_data, columns=col_names)


def detect_composite_key(df, max_columns=10):
    """
    Intelligently detect the MINIMUM set of columns needed to uniquely identify rows.
    
    Prioritises columns that are good key candidates:
    - Integer columns (numbers without decimals) - likely IDs, codes
    - Alphanumeric columns (mix of letters and numbers) - like IDs, codes
    - High uniqueness, non-null columns
    
    Excludes columns that are NOT good keys:
    - Decimal numbers (amounts, percentages, prices)
    - Timestamps, filenames, audit columns
    - Pure text (names, descriptions)
    - Boolean/flag columns (low cardinality)
    """
    if len(df) == 0:
        return list(df.columns)[:3]
    
    total_columns = len(df.columns)
    print(f"  Total columns: {total_columns}")
    
    # Column name patterns to ALWAYS avoid as keys
    avoid_name_patterns = [
        r'.*_?DESC.*', r'.*_?DESCRIPTION.*', r'.*_?COMMENT.*', r'.*_?NOTE.*', r'.*_?TEXT.*',
        r'.*_?AMOUNT.*', r'.*_?VALUE.*', r'.*_?PRICE.*', r'.*_?QUANTITY.*', r'.*_?QTY.*',
        r'.*_?BALANCE.*', r'.*_?TOTAL.*', r'.*_?PERCENT.*', r'.*_?PCT.*', r'.*_?RATIO.*', r'.*_?RATE.*',
        r'CREATED.*', r'UPDATED.*', r'MODIFIED.*',
        r'.*_?TIMESTAMP.*', r'.*_?DATETIME.*', r'.*_?TIME$', r'.*_?DT$', r'.*_?TS$',
        r'.*_?FILE.*', r'.*_?PATH.*', r'.*_?FILENAME.*',
        r'.*_?LOAD.*', r'.*_?ETL.*', r'.*_?BATCH.*', r'.*_?RUN.*', r'.*_?PROCESS.*',
        r'.*_?AUDIT.*', r'.*_?INSERT.*', r'.*_?UPDATE.*',
        r'^ROW_.*', r'.*_?SOURCE$', r'.*_?SRC$', r'.*_?NAME$', r'^NAME_?.*',
        r'.*_?STATUS.*', r'.*_?FLAG.*', r'.*_?IND$', r'.*_?INDICATOR.*',
    ]
    
    def analyse_column_values(values):
        """Analyse column values to determine if it's a good key candidate."""
        sample = values.dropna().head(50).astype(str)
        if len(sample) == 0:
            return 'empty', -100
        
        integers = 0
        decimals = 0
        alphanumeric = 0
        alphabetic_codes = 0
        timestamps = 0
        filenames = 0
        booleans = 0
        text = 0
        
        timestamp_patterns = [
            r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}',
            r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}',
            r'^\d{4}-\d{2}-\d{2}$',
        ]
        
        filename_patterns = [
            r'.*\.(csv|txt|dat|xml|json|xlsx?|parquet)$',
            r'^[A-Za-z0-9_-]+_\d{8}.*\.',
        ]
        
        boolean_values = {'true', 'false', 'yes', 'no', 'y', 'n', '0', '1', 't', 'f'}
        
        for v in sample:
            v_str = str(v).strip()
            v_lower = v_str.lower()
            
            if v_lower in boolean_values:
                booleans += 1
                continue
            
            if any(re.match(p, v_str) for p in timestamp_patterns):
                timestamps += 1
                continue
            
            if any(re.search(p, v_str, re.IGNORECASE) for p in filename_patterns):
                filenames += 1
                continue
            
            if re.match(r'^-?\d+$', v_str):
                integers += 1
                continue
            
            if re.match(r'^-?\d+\.\d+$', v_str):
                decimals += 1
                continue
            
            if re.match(r'^[A-Za-z0-9_-]+$', v_str) and re.search(r'[A-Za-z]', v_str) and re.search(r'\d', v_str):
                alphanumeric += 1
                continue
            
            if re.match(r'^[A-Za-z_-]+$', v_str) and len(v_str) <= 20:
                alphabetic_codes += 1
                continue
            
            text += 1
        
        total = len(sample)
        
        if alphanumeric / total > 0.5:
            return 'alphanumeric', 150
        if integers / total > 0.5:
            return 'integer', 140
        if alphabetic_codes / total > 0.5:
            return 'alphabetic_code', 80
        if timestamps / total > 0.3:
            return 'timestamp', -200
        if filenames / total > 0.3:
            return 'filename', -200
        if booleans / total > 0.3:
            return 'boolean', -100
        if decimals / total > 0.5:
            return 'decimal', -50
        if text / total > 0.5:
            return 'text', -30
        
        return 'mixed', 0
    
    col_stats = {}
    excluded_cols = []
    
    for col in df.columns:
        if col.startswith('_'):
            continue
        
        should_avoid = any(re.match(p, col, re.IGNORECASE) for p in avoid_name_patterns)
        if should_avoid:
            excluded_cols.append((col, 'name pattern'))
            continue
        
        col_type, type_boost = analyse_column_values(df[col])
        
        if col_type in ('timestamp', 'filename'):
            excluded_cols.append((col, f'{col_type} values'))
            continue
        
        non_null_count = df[col].notna().sum()
        unique_count = df[col].nunique()
        non_null_ratio = non_null_count / len(df)
        unique_ratio = unique_count / len(df) if len(df) > 0 else 0
        
        priority = type_boost
        priority += unique_ratio * 50
        priority += non_null_ratio * 30
        
        if unique_count < 10:
            priority -= 80
        if non_null_ratio < 0.5:
            priority -= 50
        
        col_stats[col] = {
            'priority': priority,
            'unique_ratio': unique_ratio,
            'non_null_ratio': non_null_ratio,
            'unique_count': unique_count,
            'col_type': col_type
        }
    
    if excluded_cols:
        excluded_display = [f"{c}({r})" for c, r in excluded_cols[:5]]
        print(f"  Excluded {len(excluded_cols)} column(s) from key detection: {', '.join(excluded_display)}" + 
              (f" and {len(excluded_cols) - 5} more" if len(excluded_cols) > 5 else ""))
    
    sorted_cols = sorted(col_stats.items(), key=lambda x: -x[1]['priority'])
    
    if sorted_cols:
        top_candidates = [(c, s['col_type'], f"{s['priority']:.0f}") for c, s in sorted_cols[:5]]
        print(f"  Top key candidates: {', '.join(f'{c}({t})' for c, t, p in top_candidates)}")
    
    candidate_cols = [col for col, _ in sorted_cols[:max_columns]]
    unique_cols = []
    
    for col, stats in sorted_cols:
        if stats['unique_ratio'] == 1.0 and stats['non_null_ratio'] == 1.0:
            unique_cols = [col]
            break
    
    if not unique_cols:
        for i in range(1, min(len(candidate_cols) + 1, 6)):
            combo = candidate_cols[:i]
            combined = df[combo].apply(lambda row: '||'.join(str(v) for v in row), axis=1)
            unique_ratio = combined.nunique() / len(df)
            
            if unique_ratio >= 0.999:
                unique_cols = combo
                break
    
    if not unique_cols:
        unique_cols = candidate_cols[:3] if candidate_cols else list(df.columns)[:3]
    
    result = list(unique_cols)
    
    combined = df[result].apply(lambda row: '||'.join(str(v) for v in row), axis=1)
    unique_ratio = combined.nunique() / len(df)
    
    print(f"  Selected {len(result)} key column(s): {', '.join(result)}")
    print(f"  Composite key uniqueness: {unique_ratio:.4%}")
    
    return result


def normalise_chunk_parallel(args):
    """
    Normalise a chunk of the dataframe.
    Used for parallel processing of large tables.
    """
    chunk_df, skip_norm = args
    for col in chunk_df.columns:
        chunk_df[col] = chunk_df[col].apply(lambda v: normalise_value(v, skip_norm))
    return chunk_df


def compute_hashes_chunk_parallel(args):
    """
    Compute row hashes for a chunk of the dataframe.
    Used for parallel processing of large tables.
    """
    chunk_df, columns, skip_norm = args
    
    def hash_row(row):
        normalised = []
        for v in row:
            norm_v = normalise_value(v, skip_norm)
            normalised.append(str(norm_v) if norm_v is not None else 'NULL')
        return hashlib.md5('|'.join(normalised).encode()).hexdigest()
    
    chunk_df['_ROW_HASH_'] = chunk_df[columns].apply(hash_row, axis=1)
    return chunk_df


def detect_duplicates_hash_chunk(args):
    """
    Compute row hashes for duplicate detection in a chunk.
    Returns list of (index, hash) tuples.
    Used for parallel processing.
    """
    chunk_df, compare_cols, skip_norm = args
    
    def row_to_hash(row):
        normalised = []
        for v in row:
            norm_v = normalise_value(v, skip_norm)
            normalised.append(str(norm_v) if norm_v is not None else 'NULL')
        return hashlib.md5('|'.join(normalised).encode()).hexdigest()
    
    results = []
    for idx, row in chunk_df.iterrows():
        row_hash = row_to_hash(row[compare_cols])
        results.append((idx, row_hash))
    
    return results


def compare_rows_parallel(args):
    """
    Compare a chunk of source rows against target using deep comparison.
    Used for parallel processing.
    """
    source_chunk, target_key_to_indices, target_df, compare_cols, key_cols, skip_normalisation = args
    
    discrepancies = []
    matched_target_indices = set()
    
    for src_idx, src_row in source_chunk.iterrows():
        # Cache normalised values for all columns in source row
        src_normalised = {col: normalise_value(src_row[col], skip_normalisation) for col in compare_cols}
        
        # Build composite key using cached values
        src_key = '||'.join([
            f"{c}={str(src_normalised[c]) if src_normalised[c] is not None else 'NULL'}"
            for c in key_cols
        ])
        
        if src_key in target_key_to_indices:
            target_indices = target_key_to_indices[src_key]
            
            matched_tgt_idx = None
            for tgt_idx in target_indices:
                if tgt_idx not in matched_target_indices:
                    matched_tgt_idx = tgt_idx
                    break
            
            if matched_tgt_idx is not None:
                matched_target_indices.add(matched_tgt_idx)
                tgt_row = target_df.loc[matched_tgt_idx]
                
                # Cache normalised values for target row
                tgt_normalised = {col: normalise_value(tgt_row[col], skip_normalisation) for col in compare_cols}
                
                differences = []
                for col in compare_cols:
                    norm_src = src_normalised[col]
                    norm_tgt = tgt_normalised[col]
                    
                    # Compare using cached normalised values
                    values_equal = (norm_src == norm_tgt) or (
                        not skip_normalisation and 
                        norm_src is not None and 
                        norm_tgt is not None and 
                        norm_src.lower() == norm_tgt.lower()
                    )
                    
                    if not values_equal:
                        differences.append({
                            'column': col,
                            'source': str(norm_src) if norm_src is not None else 'NULL',
                            'target': str(norm_tgt) if norm_tgt is not None else 'NULL'
                        })
                
                if differences:
                    full_source_row = '|'.join([
                        str(src_row[c]) if src_row[c] is not None else 'NULL' 
                        for c in compare_cols
                    ])
                    full_target_row = '|'.join([
                        str(tgt_row[c]) if tgt_row[c] is not None else 'NULL' 
                        for c in compare_cols
                    ])
                    
                    for diff in differences:
                        discrepancies.append({
                            'discrepancy_type': 'VALUE_MISMATCH',
                            'composite_key': src_key,
                            'column_name': diff['column'],
                            'source_value': diff['source'],
                            'target_value': diff['target'],
                            'full_source_row': full_source_row,
                            'full_target_row': full_target_row
                        })
                continue
        
        full_row_data = '|'.join([
            str(src_row[c]) if src_row[c] is not None else 'NULL' 
            for c in compare_cols
        ])
        discrepancies.append({
            'discrepancy_type': 'MISSING_IN_TARGET',
            'composite_key': src_key,
            'column_name': 'ALL',
            'source_value': full_row_data,
            'target_value': 'ROW_NOT_FOUND',
            'full_source_row': full_row_data,
            'full_target_row': 'ROW_NOT_FOUND'
        })
    
    return discrepancies, matched_target_indices


class HighPerformanceComparator:
    """High-performance CSV comparison engine with parallel processing."""
    
    def __init__(self, source_df, target_df, key_columns=None, skip_normalisation=False, num_workers=None):
        self.source_df = source_df.copy()
        self.target_df = target_df.copy()
        self.key_columns = key_columns
        self.skip_normalisation = skip_normalisation
        self.num_workers = num_workers or max(1, mp.cpu_count() - 1)
        self.discrepancies = []
        self.common_columns = []
        
        print(f"\nUsing {self.num_workers} CPU cores for parallel processing")
    
    def _normalise_dataframe(self, df, label):
        """Apply normalisation to all values in dataframe using parallel processing for large tables."""
        action = "Skipping normalisation" if self.skip_normalisation else "Normalising"
        print(f"  {action} for {label} data...")
        
        total_cells = len(df) * len(df.columns)
        
        # For small dataframes, single-threaded is faster (avoids multiprocessing overhead)
        if total_cells < 100_000:
            for col in df.columns:
                df[col] = df[col].apply(lambda v: normalise_value(v, self.skip_normalisation))
            return df
        
        # Split into chunks for parallel processing
        num_chunks = self.num_workers * 4  # More chunks than workers for better load balancing
        chunk_size = max(500, len(df) // num_chunks)
        chunks = [df.iloc[i:i+chunk_size].copy() for i in range(0, len(df), chunk_size)]
        
        print(f"    Processing {len(chunks)} chunks across {self.num_workers} workers...")
        
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            args = [(chunk, self.skip_normalisation) for chunk in chunks]
            results = list(executor.map(normalise_chunk_parallel, args))
        
        # Concatenate results, preserving original indices
        return pd.concat(results)
    
    def _align_columns(self):
        """Align columns between dataframes, preserving original order from source."""
        source_cols = set(self.source_df.columns)
        target_cols = set(self.target_df.columns)
        
        common_set = source_cols & target_cols
        self.common_columns = [c for c in self.source_df.columns if c in common_set]
        
        source_only = source_cols - target_cols
        target_only = target_cols - source_cols
        
        if source_only:
            print(f"  [!] Columns only in source: {', '.join(sorted(source_only))}")
        if target_only:
            print(f"  [!] Columns only in target: {', '.join(sorted(target_only))}")
        
        self.source_df = self.source_df[self.common_columns].copy()
        self.target_df = self.target_df[self.common_columns].copy()
        
        print(f"  Common columns for comparison: {len(self.common_columns)}")
    
    def _compute_row_hashes(self, df, label):
        """Compute hash for each row using parallel processing for large tables."""
        print(f"  Computing hashes for {label}...")
        
        total_cells = len(df) * len(self.common_columns)
        
        # For small dataframes, single-threaded is faster (avoids multiprocessing overhead)
        if total_cells < 100_000:
            skip_norm = self.skip_normalisation
            
            def hash_row(row):
                normalised = []
                for v in row:
                    norm_v = normalise_value(v, skip_norm)
                    normalised.append(str(norm_v) if norm_v is not None else 'NULL')
                return hashlib.md5('|'.join(normalised).encode()).hexdigest()
            
            df = df.copy()
            df['_ROW_HASH_'] = df[self.common_columns].apply(hash_row, axis=1)
            return df
        
        # Split into chunks for parallel processing
        num_chunks = self.num_workers * 4  # More chunks than workers for better load balancing
        chunk_size = max(500, len(df) // num_chunks)
        chunks = [df.iloc[i:i+chunk_size].copy() for i in range(0, len(df), chunk_size)]
        
        print(f"    Processing {len(chunks)} chunks across {self.num_workers} workers...")
        
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            args = [(chunk, list(self.common_columns), self.skip_normalisation) for chunk in chunks]
            results = list(executor.map(compute_hashes_chunk_parallel, args))
        
        # Concatenate results, preserving original indices
        return pd.concat(results)
    
    def compare(self):
        """Execute comprehensive comparison."""
        print("\n" + "=" * 60)
        print("COMPREHENSIVE COMPARISON")
        print("=" * 60)
        
        print("\nStep 1: Aligning columns...")
        self._align_columns()
        
        print("\nStep 2: Normalising data...")
        self.source_df = self._normalise_dataframe(self.source_df, "source")
        self.target_df = self._normalise_dataframe(self.target_df, "target")
        
        print("\nStep 3: Computing row hashes...")
        self.source_df = self._compute_row_hashes(self.source_df, "source")
        self.target_df = self._compute_row_hashes(self.target_df, "target")
        
        print("\nStep 4: Hash-based exact matching...")
        source_hash_map = defaultdict(list)
        target_hash_map = defaultdict(list)
        
        for idx, h in self.source_df['_ROW_HASH_'].items():
            source_hash_map[h].append(idx)
        for idx, h in self.target_df['_ROW_HASH_'].items():
            target_hash_map[h].append(idx)
        
        exact_match_hashes = set(source_hash_map.keys()) & set(target_hash_map.keys())
        
        source_matched = set()
        target_matched = set()
        
        for h in exact_match_hashes:
            src_indices = source_hash_map[h]
            tgt_indices = target_hash_map[h]
            
            match_count = min(len(src_indices), len(tgt_indices))
            source_matched.update(src_indices[:match_count])
            target_matched.update(tgt_indices[:match_count])
            
            if len(src_indices) != len(tgt_indices):
                self.discrepancies.append({
                    'discrepancy_type': 'DUPLICATE_COUNT_MISMATCH',
                    'composite_key': f'HASH:{h[:16]}',
                    'column_name': 'ALL',
                    'source_value': f'{len(src_indices)} occurrences',
                    'target_value': f'{len(tgt_indices)} occurrences'
                })
        
        print(f"  [OK] Exact matches: {len(source_matched):,} rows")
        
        source_unmatched_idx = list(set(self.source_df.index) - source_matched)
        target_unmatched_idx = list(set(self.target_df.index) - target_matched)
        
        print(f"  Rows to compare further: {len(source_unmatched_idx):,} source, {len(target_unmatched_idx):,} target")
        
        if not source_unmatched_idx and not target_unmatched_idx:
            print("\n[OK] All rows matched exactly!")
            return self.discrepancies
        
        print("\nStep 5: Deep key-based comparison...")
        
        source_unmatched = self.source_df.loc[source_unmatched_idx].drop(columns=['_ROW_HASH_']).copy()
        target_unmatched = self.target_df.loc[target_unmatched_idx].drop(columns=['_ROW_HASH_']).copy()
        
        if self.key_columns:
            key_cols = [c.upper() for c in self.key_columns if c.upper() in self.common_columns]
            print(f"  Using specified key columns: {', '.join(key_cols)}")
        else:
            print("  Auto-detecting composite key columns...")
            key_cols = detect_composite_key(source_unmatched)
        
        compare_cols = list(self.common_columns)
        
        print("\n  Detecting duplicate rows...")
        self._detect_duplicates(self.source_df.drop(columns=['_ROW_HASH_']), key_cols, compare_cols, 'SOURCE')
        self._detect_duplicates(self.target_df.drop(columns=['_ROW_HASH_']), key_cols, compare_cols, 'TARGET')
        
        print("\n  Building target index...")
        target_key_to_indices = defaultdict(list)
        
        for idx, row in target_unmatched.iterrows():
            key = '||'.join([
                f"{c}={str(row[c]) if row[c] is not None else 'NULL'}"
                for c in key_cols
            ])
            target_key_to_indices[key].append(idx)
        
        duplicate_keys = {k: v for k, v in target_key_to_indices.items() if len(v) > 1}
        if duplicate_keys:
            print(f"  [!] Warning: {len(duplicate_keys)} duplicate keys found in unmatched target rows")
        
        print("  Running parallel deep comparison...")
        self._parallel_deep_comparison(source_unmatched, target_unmatched, target_key_to_indices, compare_cols, key_cols)
        
        return self.discrepancies
    
    def _detect_duplicates(self, df, key_cols, compare_cols, label):
        """Detect and report true duplicate rows using full row hash with normalised values."""
        skip_norm = self.skip_normalisation
        total_cells = len(df) * len(compare_cols)
        
        hash_to_indices = defaultdict(list)
        
        # For small dataframes, single-threaded is faster (avoids multiprocessing overhead)
        if total_cells < 100_000:
            def row_to_hash(row):
                normalised = []
                for v in row:
                    norm_v = normalise_value(v, skip_norm)
                    normalised.append(str(norm_v) if norm_v is not None else 'NULL')
                return hashlib.md5('|'.join(normalised).encode()).hexdigest()
            
            for idx, row in df.iterrows():
                row_hash = row_to_hash(row[compare_cols])
                hash_to_indices[row_hash].append(idx)
        else:
            # Split into chunks for parallel processing
            num_chunks = self.num_workers * 4
            chunk_size = max(500, len(df) // num_chunks)
            chunks = [df.iloc[i:i+chunk_size].copy() for i in range(0, len(df), chunk_size)]
            
            with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
                args = [(chunk, list(compare_cols), skip_norm) for chunk in chunks]
                results = list(executor.map(detect_duplicates_hash_chunk, args))
            
            # Merge results from all chunks
            for chunk_results in results:
                for idx, row_hash in chunk_results:
                    hash_to_indices[row_hash].append(idx)
        
        duplicate_hashes = {h: v for h, v in hash_to_indices.items() if len(v) > 1}
        
        if duplicate_hashes:
            total_duplicate_rows = sum(len(v) for v in duplicate_hashes.values())
            unique_duplicates = len(duplicate_hashes)
            print(f"    {label}: Found {unique_duplicates} unique rows duplicated ({total_duplicate_rows} total duplicate rows)")
            
            discrepancy_type = f'DUPLICATE_IN_{label}'
            
            for row_hash, indices in duplicate_hashes.items():
                first_row = df.loc[indices[0]]
                
                # Cache normalised values to avoid repeated normalisation calls
                normalised_key_vals = {c: normalise_value(first_row[c], skip_norm) for c in key_cols}
                normalised_compare_vals = {c: normalise_value(first_row[c], skip_norm) for c in compare_cols}
                
                composite_key = '||'.join([
                    f"{c}={str(normalised_key_vals[c]) if normalised_key_vals[c] is not None else 'NULL'}"
                    for c in key_cols
                ])
                
                full_row_data = '|'.join([
                    str(normalised_compare_vals[c]) if normalised_compare_vals[c] is not None else 'NULL'
                    for c in compare_cols
                ])
                
                self.discrepancies.append({
                    'discrepancy_type': discrepancy_type,
                    'composite_key': composite_key,
                    'column_name': f'IDENTICAL_ROWS_COUNT={len(indices)}',
                    'source_value': full_row_data if label == 'SOURCE' else 'N/A',
                    'target_value': full_row_data if label == 'TARGET' else 'N/A',
                    'full_source_row': full_row_data if label == 'SOURCE' else 'N/A',
                    'full_target_row': full_row_data if label == 'TARGET' else 'N/A'
                })
        else:
            print(f"    {label}: No duplicate rows found")
    
    def _parallel_deep_comparison(self, source_df, target_df, target_key_to_indices, compare_cols, key_cols):
        """Perform deep comparison using parallel processing."""
        all_target_indices = set()
        for indices in target_key_to_indices.values():
            all_target_indices.update(indices)
        
        chunk_size = max(500, len(source_df) // (self.num_workers * 8))
        chunks = [source_df.iloc[i:i + chunk_size] for i in range(0, len(source_df), chunk_size)]
        
        print(f"  Processing {len(chunks)} chunks with {self.num_workers} workers...")
        
        all_matched_target_indices = set()
        processed = 0
        total_chunks = len(chunks)
        
        target_key_dict = dict(target_key_to_indices)
        
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            futures = []
            for chunk in chunks:
                args = (chunk, target_key_dict, target_df, compare_cols, key_cols, self.skip_normalisation)
                futures.append(executor.submit(compare_rows_parallel, args))
            
            for future in as_completed(futures):
                chunk_discrepancies, matched_indices = future.result()
                self.discrepancies.extend(chunk_discrepancies)
                all_matched_target_indices.update(matched_indices)
                processed += 1
                
                if processed % max(1, total_chunks // 10) == 0 or processed == total_chunks:
                    pct = (processed / total_chunks) * 100
                    print(f"    Progress: {processed}/{total_chunks} chunks ({pct:.0f}%)")
        
        print("  Identifying rows only in target...")
        unmatched_target_indices = all_target_indices - all_matched_target_indices
        
        for tgt_idx in unmatched_target_indices:
            tgt_row = target_df.loc[tgt_idx]
            key = '||'.join([
                f"{c}={str(tgt_row[c]) if tgt_row[c] is not None else 'NULL'}"
                for c in key_cols
            ])
            full_row_data = '|'.join([
                str(tgt_row[c]) if tgt_row[c] is not None else 'NULL' 
                for c in compare_cols
            ])
            self.discrepancies.append({
                'discrepancy_type': 'MISSING_IN_SOURCE',
                'composite_key': key,
                'column_name': 'ALL',
                'source_value': 'ROW_NOT_FOUND',
                'target_value': full_row_data,
                'full_source_row': 'ROW_NOT_FOUND',
                'full_target_row': full_row_data
            })
        
        matched_count = len(all_matched_target_indices)
        missing_in_target = len(source_df) - matched_count
        missing_in_source = len(unmatched_target_indices)
        
        print(f"\n  Comparison Summary:")
        print(f"    Rows matched by key: {matched_count:,}")
        print(f"    Rows only in source: {missing_in_target:,}")
        print(f"    Rows only in target: {missing_in_source:,}")


def generate_report(discrepancies, output_file, column_headers=None):
    """Generate detailed CSV report with column reference."""
    print("\n" + "=" * 60)
    print("REPORT GENERATION")
    print("=" * 60)
    
    if not discrepancies:
        print("\n[OK] SUCCESS: No discrepancies found!")
        df = pd.DataFrame(columns=[
            'discrepancy_type', 'composite_key', 'column_name', 
            'source_value', 'target_value', 'full_source_row', 'full_target_row'
        ])
        df.to_csv(output_file, index=False)
        print(f"\n[OK] Empty report saved to: {output_file}")
        return
    
    df = pd.DataFrame(discrepancies)
    
    col_order = [
        'discrepancy_type', 'composite_key', 'column_name', 
        'source_value', 'target_value', 'full_source_row', 'full_target_row'
    ]
    df = df[[c for c in col_order if c in df.columns]]
    df = df.sort_values(['discrepancy_type', 'composite_key', 'column_name'])
    
    df.to_csv(output_file, index=False)
    
    print(f"\n[X] DISCREPANCIES FOUND: {len(discrepancies):,}")
    print("\nBreakdown by type:")
    for dtype, count in df['discrepancy_type'].value_counts().items():
        print(f"  {dtype}: {count:,}")
    
    if 'VALUE_MISMATCH' in df['discrepancy_type'].values:
        mismatch_df = df[df['discrepancy_type'] == 'VALUE_MISMATCH']
        print(f"\nValue mismatches by column (top 10):")
        col_counts = mismatch_df['column_name'].value_counts().head(10)
        for col, count in col_counts.items():
            print(f"  {col}: {count:,}")
    
    print(f"\n[OK] Report saved to: {output_file}")
    
    if column_headers:
        ref_file = output_file.replace('.csv', '_column_reference.txt')
        with open(ref_file, 'w') as f:
            f.write("COLUMN REFERENCE FOR ROW DATA\n")
            f.write("=" * 50 + "\n\n")
            f.write("The full_source_row and full_target_row columns contain\n")
            f.write("pipe-delimited (|) data for the complete row.\n\n")
            f.write("Use this reference to identify which column each value belongs to:\n\n")
            f.write("Column Order:\n")
            for i, col in enumerate(column_headers):
                f.write(f"  {i+1}. {col}\n")
            f.write(f"\nTotal columns: {len(column_headers)}\n")
        print(f"[OK] Column reference saved to: {ref_file}")
    
    print("\nSample discrepancies (first 5):")
    sample_df = df.head(5).copy()
    for col in ['source_value', 'target_value']:
        sample_df[col] = sample_df[col].apply(lambda x: x[:50] + '...' if len(str(x)) > 50 else x)
    print(sample_df.to_string(index=False))


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="CSV Comparator - High-Performance Migration Validation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Key columns are optional - auto-detected if not provided."
    )
    
    parser.add_argument("source_csv", nargs="?", help="Path to the source (Hive) CSV file")
    parser.add_argument("target_csv", nargs="?", help="Path to the target (Snowflake) CSV file")
    parser.add_argument("key_columns", nargs="*", help="Optional key columns for comparison")
    parser.add_argument("--output-dir", dest="output_dir", help="Directory to save the discrepancy report")
    parser.add_argument("--no-normalisation", dest="no_normalisation", action="store_true", default=False,
                        help="Disable value normalisation (nulls, booleans, timestamps, numbers)")
    
    return parser.parse_args()


def main():
    print("=" * 70)
    print("CSV Comparator - High-Performance Migration Validation")
    print("Hive to Snowflake Data Comparison")
    print("=" * 70)
    
    args = parse_arguments()
    
    if args.source_csv and args.target_csv:
        source_file = args.source_csv
        target_file = args.target_csv
        key_columns = args.key_columns if args.key_columns else None
        output_dir = args.output_dir
        skip_normalisation = args.no_normalisation
    else:
        print("\nUsage: python csv_comparator.py <source_csv> <target_csv> [key_col1 key_col2 ...] [--output-dir DIR] [--no-normalisation]")
        print("\nKey columns are optional - auto-detected if not provided.")
        source_file = input("Source (Hive) CSV path: ").strip()
        target_file = input("Target (Snowflake) CSV path: ").strip()
        key_input = input("Key columns (comma-separated, or Enter for auto): ").strip()
        key_columns = [c.strip() for c in key_input.split(',')] if key_input else None
        output_dir = None
        skip_normalisation = False
    
    for path, name in [(source_file, "Source"), (target_file, "Target")]:
        if not os.path.exists(path):
            print(f"\n[X] Error: {name} file not found: {path}")
            sys.exit(1)
    
    print(f"\nSource: {source_file}")
    print(f"Target: {target_file}")
    print(f"Keys: {', '.join(key_columns) if key_columns else 'Auto-detect'}")
    if output_dir:
        print(f"Output directory: {output_dir}")
    print(f"Normalisation: {'Disabled' if skip_normalisation else 'Enabled'}")
    
    print("\n" + "-" * 40)
    print("Loading files...")
    source_df = load_csv_file(source_file, "Source (Hive)")
    target_df = load_csv_file(target_file, "Target (Snowflake)")
    
    comparator = HighPerformanceComparator(source_df, target_df, key_columns, skip_normalisation)
    discrepancies = comparator.compare()
    
    source_basename = os.path.basename(source_file)
    name_without_ext = os.path.splitext(source_basename)[0]
    parts = name_without_ext.rsplit('_', 2)
    if len(parts) >= 3 and parts[-2].isdigit() and parts[-1].isdigit():
        table_name = '_'.join(parts[:-2])
    else:
        table_name = name_without_ext
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{table_name}_discrepancy_report_{timestamp}.csv"
    
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, output_filename)
    else:
        output_file = output_filename
    
    generate_report(discrepancies, output_file, comparator.common_columns)
    
    print("\n" + "=" * 70)
    print("Comparison Complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
