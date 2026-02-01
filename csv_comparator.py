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

from __future__ import annotations

import pandas as pd
import numpy as np
import sys
import re
import os
import csv
import hashlib
import tempfile
import argparse
import logging
import multiprocessing as mp
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Any, Optional, Dict, List, Tuple, Set, DefaultDict


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS - Column exclusion patterns for key detection
# =============================================================================

# Column name patterns to ALWAYS avoid as keys
COLUMN_EXCLUSION_PATTERNS: List[str] = [
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

# Default timestamp precision (number of decimal places to preserve, None = strip all)
DEFAULT_TIMESTAMP_PRECISION: Optional[int] = None

# Default numeric precision for rounding
DEFAULT_NUMERIC_PRECISION: int = 6


def normalise_value(
    value: Any,
    skip_normalisation: bool = False,
    timestamp_precision: Optional[int] = DEFAULT_TIMESTAMP_PRECISION,
    numeric_precision: int = DEFAULT_NUMERIC_PRECISION
) -> Optional[str]:
    """
    Normalise values for consistent comparison.
    Preserves whitespace exactly as-is for strict validation.
    Handles nulls, numeric formatting, timestamps, and booleans.
    Does NOT convert ambiguous date formats (DD/MM/YYYY vs MM/DD/YYYY).

    Args:
        value: The value to normalise
        skip_normalisation: If True, returns value as-is (string conversion only)
        timestamp_precision: Number of decimal places to preserve in timestamps.
                           None = strip all decimals (default for backwards compatibility)
        numeric_precision: Number of decimal places for numeric rounding (default: 6)

    Returns:
        Normalised string value or None for null values
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

    # Normalise timestamp formats
    # Format: YYYY-MM-DD HH:MM:SS.fffffffff -> YYYY-MM-DD HH:MM:SS[.fff]
    timestamp_match = re.match(r'^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})\.(\d+)$', stripped)
    if timestamp_match:
        datetime_part = timestamp_match.group(1).replace('T', ' ')
        fractional_part = timestamp_match.group(2)

        if timestamp_precision is None or timestamp_precision == 0:
            return datetime_part
        else:
            # Truncate or pad fractional seconds to specified precision
            truncated = fractional_part[:timestamp_precision].ljust(timestamp_precision, '0')
            return f"{datetime_part}.{truncated}"

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
            # Round to specified decimal places for consistent comparison
            return str(round(float_val, numeric_precision))
    except (ValueError, TypeError):
        pass

    # Return value exactly as-is, preserving all whitespace
    return str_value


def normalise_key_for_fuzzy(value: Any) -> str:
    """
    Normalise a key value for fuzzy matching.
    Removes/normalises slashes, pipes, extra whitespace, and special characters.

    Args:
        value: The value to normalise

    Returns:
        Normalised string for fuzzy comparison
    """
    if pd.isna(value) or value is None:
        return 'NULL'

    str_value = str(value).strip()
    if str_value.lower() in ['', 'null', 'none', 'nan', 'nat', 'n/a', 'na', '<null>']:
        return 'NULL'

    # Replace slashes, pipes, backslashes with space
    normalised = re.sub(r'[/|\\]', ' ', str_value)
    # Collapse multiple whitespace to single space
    normalised = re.sub(r'\s+', ' ', normalised)
    # Remove common special characters that might differ
    normalised = re.sub(r'[_\-\.\(\)\[\]\{\}]', '', normalised)
    # Uppercase for case-insensitive comparison
    normalised = normalised.strip().upper()

    return normalised


def key_values_fuzzy_equal(
    src_val: Any,
    tgt_val: Any,
    skip_normalisation: bool = False,
    numeric_precision: int = DEFAULT_NUMERIC_PRECISION,
    numeric_tolerance: float = 100.0,
    numeric_tolerance_pct: float = 0.01
) -> Tuple[bool, str]:
    """
    Check if two key values are similar enough for fuzzy matching.

    For strings: normalise (slashes, pipes, whitespace, special chars) and compare
    For numbers: check if within tolerance (absolute or percentage)

    Args:
        src_val: Source value
        tgt_val: Target value
        skip_normalisation: Whether to skip value normalisation
        numeric_precision: Decimal precision for numeric comparison
        numeric_tolerance: Absolute tolerance for numeric differences (default: 100)
        numeric_tolerance_pct: Percentage tolerance for numeric differences (default: 1%)

    Returns:
        Tuple of (is_similar: bool, reason: str)
        reason is 'EXACT', 'FUZZY_STRING', 'FUZZY_NUMERIC', or 'DIFFERENT'
    """
    # First normalise both values
    src_norm = normalise_value(src_val, skip_normalisation, numeric_precision=numeric_precision)
    tgt_norm = normalise_value(tgt_val, skip_normalisation, numeric_precision=numeric_precision)

    # Exact match after normalisation
    if src_norm == tgt_norm:
        return True, 'EXACT'

    # Handle NULL cases
    if src_norm is None and tgt_norm is None:
        return True, 'EXACT'
    if src_norm is None or tgt_norm is None:
        return False, 'DIFFERENT'

    # Try numeric comparison with tolerance
    try:
        src_num = float(str(src_norm).replace(',', ''))
        tgt_num = float(str(tgt_norm).replace(',', ''))

        diff = abs(src_num - tgt_num)
        max_val = max(abs(src_num), abs(tgt_num), 1.0)

        # Similar if within absolute tolerance OR percentage tolerance
        if diff <= numeric_tolerance or (diff / max_val) <= numeric_tolerance_pct:
            return True, 'FUZZY_NUMERIC'
        return False, 'DIFFERENT'
    except (ValueError, TypeError):
        pass

    # Fuzzy string comparison
    src_fuzzy = normalise_key_for_fuzzy(src_norm)
    tgt_fuzzy = normalise_key_for_fuzzy(tgt_norm)

    if src_fuzzy == tgt_fuzzy:
        return True, 'FUZZY_STRING'

    # Check if one string contains the other (for extra character cases)
    if len(src_fuzzy) > 0 and len(tgt_fuzzy) > 0:
        # Allow up to 2 character difference for small strings, more for longer ones
        max_diff = max(2, min(len(src_fuzzy), len(tgt_fuzzy)) // 10)
        if abs(len(src_fuzzy) - len(tgt_fuzzy)) <= max_diff:
            # Simple character-level similarity check
            longer = src_fuzzy if len(src_fuzzy) >= len(tgt_fuzzy) else tgt_fuzzy
            shorter = tgt_fuzzy if len(src_fuzzy) >= len(tgt_fuzzy) else src_fuzzy
            if shorter in longer:
                return True, 'FUZZY_STRING'

    return False, 'DIFFERENT'


def build_fuzzy_key(row: pd.Series, key_cols: List[str]) -> str:
    """
    Build a normalised fuzzy key for indexing.
    Uses first 3 key columns for the index lookup.

    Args:
        row: DataFrame row
        key_cols: List of key column names

    Returns:
        Fuzzy key string for index lookup
    """
    # Use first 3 key columns for fuzzy index (balance between specificity and flexibility)
    index_cols = key_cols[:3]
    parts = []
    for c in index_cols:
        val = row[c] if c in row.index else None
        fuzzy_val = normalise_key_for_fuzzy(val)
        parts.append(f"{c}={fuzzy_val}")
    return '||'.join(parts)


def detect_delimiter_for_line(line: str) -> str:
    """Detect the most likely delimiter for a single line."""
    delimiters: Dict[str, int] = {'|': 0, ',': 0, '\t': 0, '~': 0, ';': 0}
    for delim in delimiters:
        delimiters[delim] = line.count(delim)

    best_delim = max(delimiters, key=delimiters.get)
    if delimiters[best_delim] == 0:
        return ','
    return best_delim


def detect_delimiters(filepath: str, sample_lines: int = 10) -> Tuple[str, str]:
    """
    Detect CSV delimiters separately for header and data rows.

    Args:
        filepath: Path to the CSV file
        sample_lines: Number of data lines to sample for detection

    Returns:
        Tuple of (header_delimiter, row_delimiter)
    """
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines: List[str] = []
            for _ in range(sample_lines + 1):
                line = f.readline()
                if line:
                    lines.append(line.strip())

        if not lines:
            logger.warning(f"Empty file detected: {filepath}, defaulting to comma delimiter")
            return ',', ','

        header_line = lines[0]
        header_delim = detect_delimiter_for_line(header_line)

        data_lines = lines[1:] if len(lines) > 1 else []

        if not data_lines:
            return header_delim, header_delim

        delimiters: Dict[str, int] = {'|': 0, ',': 0, '\t': 0, '~': 0, ';': 0}
        for line in data_lines:
            for delim in delimiters:
                delimiters[delim] += line.count(delim)

        row_delim = max(delimiters, key=delimiters.get)
        if delimiters[row_delim] == 0:
            row_delim = header_delim

        return header_delim, row_delim

    except IOError as e:
        logger.warning(f"Error reading file for delimiter detection: {e}, defaulting to comma")
        return ',', ','
    except Exception as e:
        logger.warning(f"Unexpected error in delimiter detection: {e}, defaulting to comma")
        return ',', ','


def preprocess_quoted_rows(filepath: str) -> Tuple[str, int]:
    """
    Fix rows that are entirely quoted (start with " and end with ").
    Also normalises Windows CRLF line endings to Unix LF.

    Some exports wrap entire rows in quotes, e.g.:
        "value1|value2|value3"
    This causes delimiters inside to be treated as data, not separators.

    Args:
        filepath: Path to the CSV file to process

    Returns:
        Tuple of (cleaned_filepath, rows_fixed_count)
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
def cleaned_csv(filepath: str):
    """
    Context manager for temporary cleaned CSV files.
    Ensures temp files are always cleaned up, even if exceptions occur.

    Args:
        filepath: Path to the CSV file to process

    Yields:
        Tuple of (cleaned_filepath, rows_fixed_count)
    """
    cleaned_path, rows_fixed = preprocess_quoted_rows(filepath)
    try:
        yield cleaned_path, rows_fixed
    finally:
        if cleaned_path != filepath:
            try:
                os.remove(cleaned_path)
            except OSError as e:
                logger.debug(f"Failed to clean up temp file {cleaned_path}: {e}")


def load_csv_file(filepath: str, source_name: str, escape_char: Optional[str] = None) -> pd.DataFrame:
    """
    Load CSV with automatic delimiter detection and row_text handling.

    Args:
        filepath: Path to the CSV file
        source_name: Display name for logging
        escape_char: Optional escape character for CSV parsing (default: None)

    Returns:
        Loaded DataFrame with normalised column names
    """
    with cleaned_csv(filepath) as (cleaned_filepath, rows_fixed):
        try:
            if rows_fixed > 0:
                logger.info(f"  [!] Fixed {rows_fixed:,} rows with errant surrounding quotes")

            header_delim, row_delim = detect_delimiters(cleaned_filepath)
            delim_names: Dict[str, str] = {
                ',': 'comma', '|': 'pipe', '\t': 'tab', '~': 'tilde', ';': 'semicolon'
            }
            header_delim_name = delim_names.get(header_delim, repr(header_delim))
            row_delim_name = delim_names.get(row_delim, repr(row_delim))

            if header_delim == row_delim:
                logger.info(f"  Detected delimiter: {header_delim_name}")
            else:
                logger.info(f"  Detected header delimiter: {header_delim_name}")
                logger.info(f"  Detected row delimiter: {row_delim_name}")

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
                    low_memory=False,
                    escapechar=escape_char
                )

            df.columns = df.columns.str.strip().str.upper()

            if len(df.columns) == 1 and df.columns[0] in ['ROW_TEXT', 'ROW', 'TEXT', 'DATA', 'LINE']:
                logger.info(f"  Detected single-column format: {df.columns[0]}")
                df = expand_row_text_column(df)
                logger.info(f"  Expanded to {len(df.columns)} columns")

            logger.info(f"  [OK] Loaded {source_name}: {len(df):,} rows, {len(df.columns)} columns")
            return df

        except Exception as e:
            logger.error(f"  [X] Error loading {source_name}: {e}")
            sys.exit(1)


def load_mixed_delimiter_csv(filepath: str, header_delim: str, row_delim: str) -> pd.DataFrame:
    """
    Load CSV where header uses one delimiter but data rows use another.

    Args:
        filepath: Path to the CSV file
        header_delim: Delimiter used in header row
        row_delim: Delimiter used in data rows

    Returns:
        Loaded DataFrame
    """
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        header_line = f.readline().strip()
        column_names = [col.strip().upper() for col in header_line.split(header_delim)]

        data_rows: List[List[str]] = []
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
        logger.warning(f"  Column count mismatch (header: {len(column_names)}, data: {len(df.columns)})")
        if len(df.columns) < len(column_names):
            df.columns = column_names[:len(df.columns)]
        else:
            extra_cols = [f'COL_{i+1}' for i in range(len(column_names), len(df.columns))]
            df.columns = column_names + extra_cols

    return df


def expand_row_text_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expand a single row_text column into multiple columns.
    Detects the delimiter used within the data and creates COL_1, COL_2, etc.

    Args:
        df: DataFrame with a single row_text column

    Returns:
        DataFrame with expanded columns
    """
    if len(df) == 0:
        return df

    col_name = df.columns[0]
    sample_values = df[col_name].head(10).tolist()

    delimiters: Dict[str, int] = {'|': 0, ',': 0, '\t': 0, ';': 0, '~': 0}
    for val in sample_values:
        if pd.notna(val) and val:
            for delim in delimiters:
                delimiters[delim] += str(val).count(delim)

    internal_delim = max(delimiters, key=delimiters.get)

    if delimiters[internal_delim] == 0:
        return df

    delim_names: Dict[str, str] = {
        '|': 'pipe', ',': 'comma', '\t': 'tab', ';': 'semicolon', '~': 'tilde'
    }
    delim_name = delim_names.get(internal_delim, internal_delim)
    logger.info(f"  Detected internal delimiter: {delim_name}")

    first_valid: Optional[str] = None
    for val in sample_values:
        if pd.notna(val) and val and internal_delim in str(val):
            first_valid = str(val)
            break

    if not first_valid:
        return df

    num_cols = len(first_valid.split(internal_delim))
    col_names = [f'COL_{i+1}' for i in range(num_cols)]

    # Use vectorized string split instead of iterrows
    def split_and_pad(val: Any) -> List[str]:
        if pd.notna(val) and val:
            parts = str(val).split(internal_delim)
            if len(parts) < num_cols:
                parts.extend([''] * (num_cols - len(parts)))
            elif len(parts) > num_cols:
                parts = parts[:num_cols]
            return parts
        return [''] * num_cols

    expanded_data = df[col_name].apply(split_and_pad).tolist()

    return pd.DataFrame(expanded_data, columns=col_names)


def detect_composite_key(
    df: pd.DataFrame,
    max_columns: int = 30,
    exclusion_patterns: Optional[List[str]] = None
) -> List[str]:
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

    Args:
        df: DataFrame to analyse
        max_columns: Maximum number of columns to consider as key candidates
        exclusion_patterns: Optional list of regex patterns for columns to exclude.
                          Uses COLUMN_EXCLUSION_PATTERNS if not provided.

    Returns:
        List of column names forming the composite key
    """
    if len(df) == 0:
        return list(df.columns)[:20]

    total_columns = len(df.columns)
    logger.info(f"  Total columns: {total_columns}")

    # Use provided patterns or default to global constant
    avoid_name_patterns = exclusion_patterns or COLUMN_EXCLUSION_PATTERNS

    def analyse_column_values(values: pd.Series) -> Tuple[str, int]:
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
        logger.info(f"  Excluded {len(excluded_cols)} column(s) from key detection: {', '.join(excluded_display)}" +
              (f" and {len(excluded_cols) - 5} more" if len(excluded_cols) > 5 else ""))

    sorted_cols = sorted(col_stats.items(), key=lambda x: -x[1]['priority'])

    if sorted_cols:
        top_candidates = [(c, s['col_type'], f"{s['priority']:.0f}") for c, s in sorted_cols[:5]]
        logger.info(f"  Top key candidates: {', '.join(f'{c}({t})' for c, t, p in top_candidates)}")
    
    candidate_cols = [col for col, _ in sorted_cols[:max_columns]]
    unique_cols = []
    
    for col, stats in sorted_cols:
        if stats['unique_ratio'] == 1.0 and stats['non_null_ratio'] == 1.0:
            unique_cols = [col]
            break
    
    if not unique_cols:
        for i in range(1, min(len(candidate_cols) + 1, 25)):
            combo = candidate_cols[:i]
            combined = df[combo].apply(lambda row: '||'.join(str(v) for v in row), axis=1)
            unique_ratio = combined.nunique() / len(df)
            
            if unique_ratio >= 0.999:
                unique_cols = combo
                break
    
    if not unique_cols:
        unique_cols = candidate_cols[:20] if candidate_cols else list(df.columns)[:20]
    
    result = list(unique_cols)
    
    combined = df[result].apply(lambda row: '||'.join(str(v) for v in row), axis=1)
    unique_ratio = combined.nunique() / len(df)
    
    logger.info(f"  Selected {len(result)} key column(s): {', '.join(result)}")
    logger.info(f"  Composite key uniqueness: {unique_ratio:.4%}")

    return result


def _compute_row_hash(
    row_values: List[Any],
    skip_norm: bool = False,
    decimal_prec: int = DEFAULT_NUMERIC_PRECISION
) -> str:
    """
    Compute a SHA-256 hash for a row of values.

    Args:
        row_values: List of values to hash
        skip_norm: Whether to skip normalisation
        decimal_prec: Number of decimal places for numeric precision

    Returns:
        Hex digest of the hash (first 32 chars for reasonable length)
    """
    normalised = []
    for v in row_values:
        norm_v = normalise_value(v, skip_norm, numeric_precision=decimal_prec)
        normalised.append(str(norm_v) if norm_v is not None else 'NULL')
    return hashlib.sha256('|'.join(normalised).encode()).hexdigest()[:32]


def normalise_chunk_parallel(
    args: Tuple[pd.DataFrame, bool, int]
) -> pd.DataFrame:
    """
    Normalise a chunk of the dataframe.
    Used for parallel processing of large tables.

    Args:
        args: Tuple of (chunk_df, skip_normalisation, decimal_precision)

    Returns:
        Normalised DataFrame chunk
    """
    chunk_df, skip_norm, decimal_prec = args
    for col in chunk_df.columns:
        chunk_df[col] = chunk_df[col].apply(
            lambda v: normalise_value(v, skip_norm, numeric_precision=decimal_prec)
        )
    return chunk_df


def compute_hashes_chunk_parallel(
    args: Tuple[pd.DataFrame, List[str], bool, int]
) -> pd.DataFrame:
    """
    Compute row hashes for a chunk of the dataframe.
    Used for parallel processing of large tables.

    Args:
        args: Tuple of (chunk_df, columns, skip_normalisation, decimal_precision)

    Returns:
        DataFrame chunk with _ROW_HASH_ column added
    """
    chunk_df, columns, skip_norm, decimal_prec = args

    def hash_row(row: pd.Series) -> str:
        return _compute_row_hash(row.tolist(), skip_norm, decimal_prec)

    chunk_df['_ROW_HASH_'] = chunk_df[columns].apply(hash_row, axis=1)
    return chunk_df


def detect_duplicates_hash_chunk(
    args: Tuple[pd.DataFrame, List[str], bool, int]
) -> List[Tuple[Any, str]]:
    """
    Compute row hashes for duplicate detection in a chunk.
    Returns list of (index, hash) tuples.
    Used for parallel processing.

    Args:
        args: Tuple of (chunk_df, compare_cols, skip_normalisation, decimal_precision)

    Returns:
        List of (index, hash) tuples
    """
    chunk_df, compare_cols, skip_norm, decimal_prec = args

    # Use vectorized apply instead of iterrows for better performance
    def hash_row(row: pd.Series) -> str:
        return _compute_row_hash(row.tolist(), skip_norm, decimal_prec)

    hashes = chunk_df[compare_cols].apply(hash_row, axis=1)
    return list(zip(chunk_df.index.tolist(), hashes.tolist()))


def compare_rows_parallel(
    args: Tuple[pd.DataFrame, Dict[str, List[Any]], Dict[str, List[Any]], pd.DataFrame, List[str], List[str], bool, int]
) -> Tuple[List[Dict[str, Any]], Set[Any]]:
    """
    Compare a chunk of source rows against target using deep comparison.
    Supports both exact and fuzzy key matching.
    Used for parallel processing.

    Args:
        args: Tuple of (source_chunk, target_key_to_indices, target_fuzzy_key_to_indices,
              target_df, compare_cols, key_cols, skip_normalisation, decimal_precision)

    Returns:
        Tuple of (discrepancies list, matched_target_indices set)
    """
    (source_chunk, target_key_to_indices, target_fuzzy_key_to_indices,
     target_df, compare_cols, key_cols, skip_normalisation, decimal_prec) = args

    discrepancies: List[Dict[str, Any]] = []
    matched_target_indices: Set[Any] = set()

    for src_idx, src_row in source_chunk.iterrows():
        # Cache normalised values for all columns in source row
        src_normalised = {col: normalise_value(src_row[col], skip_normalisation, numeric_precision=decimal_prec) for col in compare_cols}
        
        # Build composite key using cached values
        src_key = '||'.join([
            f"{c}={str(src_normalised[c]) if src_normalised[c] is not None else 'NULL'}"
            for c in key_cols
        ])
        
        matched_tgt_idx = None
        is_fuzzy_match = False
        fuzzy_key_differences = []
        
        # Step 1: Try exact key match first
        if src_key in target_key_to_indices:
            target_indices = target_key_to_indices[src_key]
            for tgt_idx in target_indices:
                if tgt_idx not in matched_target_indices:
                    matched_tgt_idx = tgt_idx
                    break
        
        # Step 2: If no exact match, try fuzzy key match
        if matched_tgt_idx is None and target_fuzzy_key_to_indices:
            src_fuzzy_key = build_fuzzy_key(src_row, key_cols)
            
            if src_fuzzy_key in target_fuzzy_key_to_indices:
                fuzzy_candidates = target_fuzzy_key_to_indices[src_fuzzy_key]
                
                for tgt_idx in fuzzy_candidates:
                    if tgt_idx in matched_target_indices:
                        continue
                    
                    tgt_row = target_df.loc[tgt_idx]
                    
                    # Check if ALL key columns are fuzzy-similar
                    all_keys_similar = True
                    temp_key_diffs = []
                    
                    for col in key_cols:
                        src_val = src_row[col]
                        tgt_val = tgt_row[col]
                        is_similar, match_type = key_values_fuzzy_equal(
                            src_val, tgt_val, skip_normalisation, decimal_prec
                        )
                        
                        if not is_similar:
                            all_keys_similar = False
                            break
                        
                        # Track key differences for reporting (even fuzzy matches)
                        if match_type != 'EXACT':
                            src_norm = normalise_value(src_val, skip_normalisation, numeric_precision=decimal_prec)
                            tgt_norm = normalise_value(tgt_val, skip_normalisation, numeric_precision=decimal_prec)
                            temp_key_diffs.append({
                                'column': col,
                                'source': str(src_norm) if src_norm is not None else 'NULL',
                                'target': str(tgt_norm) if tgt_norm is not None else 'NULL',
                                'match_type': match_type
                            })
                    
                    if all_keys_similar:
                        matched_tgt_idx = tgt_idx
                        is_fuzzy_match = True
                        fuzzy_key_differences = temp_key_diffs
                        break
        
        # Step 3: If match found (exact or fuzzy), compare all values
        if matched_tgt_idx is not None:
            matched_target_indices.add(matched_tgt_idx)
            tgt_row = target_df.loc[matched_tgt_idx]

            # Cache normalised values for target row
            tgt_normalised = {col: normalise_value(tgt_row[col], skip_normalisation, numeric_precision=decimal_prec) for col in compare_cols}
            
            differences = []
            
            # First add key column differences from fuzzy matching
            if is_fuzzy_match and fuzzy_key_differences:
                for key_diff in fuzzy_key_differences:
                    differences.append({
                        'column': key_diff['column'],
                        'source': key_diff['source'],
                        'target': key_diff['target'],
                        'is_key_column': True
                    })
            
            # Then compare all columns (skip key columns already reported)
            reported_key_cols = {d['column'] for d in fuzzy_key_differences} if fuzzy_key_differences else set()
            
            for col in compare_cols:
                if col in reported_key_cols:
                    continue
                    
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
                        'target': str(norm_tgt) if norm_tgt is not None else 'NULL',
                        'is_key_column': col in key_cols
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
                
                # Build the target key for reporting
                tgt_key = '||'.join([
                    f"{c}={str(tgt_normalised[c]) if tgt_normalised[c] is not None else 'NULL'}"
                    for c in key_cols
                ])
                
                for diff in differences:
                    # Use KEY_VALUE_MISMATCH for key column differences, VALUE_MISMATCH for others
                    disc_type = 'KEY_VALUE_MISMATCH' if diff.get('is_key_column') else 'VALUE_MISMATCH'
                    
                    discrepancies.append({
                        'discrepancy_type': disc_type,
                        'composite_key': f"{src_key} ~> {tgt_key}" if is_fuzzy_match else src_key,
                        'column_name': diff['column'],
                        'source_value': diff['source'],
                        'target_value': diff['target'],
                        'full_source_row': full_source_row,
                        'full_target_row': full_target_row
                    })
            continue
        
        # Step 4: No match found - report as missing
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

    def __init__(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        key_columns: Optional[List[str]] = None,
        skip_normalisation: bool = False,
        num_workers: Optional[int] = None,
        decimal_precision: int = DEFAULT_NUMERIC_PRECISION
    ) -> None:
        """
        Initialise the comparator.

        Args:
            source_df: Source DataFrame (will not be modified)
            target_df: Target DataFrame (will not be modified)
            key_columns: Optional list of key columns for matching
            skip_normalisation: Whether to skip value normalisation
            num_workers: Number of parallel workers (defaults to CPU count - 1)
            decimal_precision: Number of decimal places for numeric comparison (default: 6)
        """
        # Use views where possible, only copy when necessary for modifications
        self.source_df = source_df
        self.target_df = target_df
        self.key_columns = key_columns
        self.skip_normalisation = skip_normalisation
        self.decimal_precision = decimal_precision
        self.num_workers = num_workers or max(1, mp.cpu_count() - 1)
        self.discrepancies: List[Dict[str, Any]] = []
        self.common_columns: List[str] = []

        logger.info(f"\nUsing {self.num_workers} CPU cores for parallel processing")

    def _normalise_dataframe(self, df: pd.DataFrame, label: str) -> pd.DataFrame:
        """
        Apply normalisation to all values in dataframe using parallel processing for large tables.

        Args:
            df: DataFrame to normalise
            label: Display label for logging

        Returns:
            Normalised DataFrame
        """
        action = "Skipping normalisation" if self.skip_normalisation else "Normalising"
        logger.info(f"  {action} for {label} data...")

        total_cells = len(df) * len(df.columns)
        decimal_prec = self.decimal_precision

        # For small dataframes, single-threaded is faster (avoids multiprocessing overhead)
        if total_cells < 100_000:
            df = df.copy()  # Only copy when we need to modify
            for col in df.columns:
                df[col] = df[col].apply(
                    lambda v: normalise_value(v, self.skip_normalisation, numeric_precision=decimal_prec)
                )
            return df

        # Split into chunks for parallel processing
        num_chunks = self.num_workers * 4  # More chunks than workers for better load balancing
        chunk_size = max(500, len(df) // num_chunks)
        chunks = [df.iloc[i:i+chunk_size].copy() for i in range(0, len(df), chunk_size)]

        logger.info(f"    Processing {len(chunks)} chunks across {self.num_workers} workers...")

        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            args = [(chunk, self.skip_normalisation, decimal_prec) for chunk in chunks]
            results = list(executor.map(normalise_chunk_parallel, args))

        # Concatenate results, preserving original indices
        return pd.concat(results)

    def _align_columns(self) -> None:
        """Align columns between dataframes, preserving original order from source."""
        source_cols = set(self.source_df.columns)
        target_cols = set(self.target_df.columns)

        common_set = source_cols & target_cols
        self.common_columns = [c for c in self.source_df.columns if c in common_set]

        source_only = source_cols - target_cols
        target_only = target_cols - source_cols

        if source_only:
            logger.info(f"  [!] Columns only in source: {', '.join(sorted(source_only))}")
        if target_only:
            logger.info(f"  [!] Columns only in target: {', '.join(sorted(target_only))}")

        self.source_df = self.source_df[self.common_columns].copy()
        self.target_df = self.target_df[self.common_columns].copy()

        logger.info(f"  Common columns for comparison: {len(self.common_columns)}")

    def _compute_row_hashes(self, df: pd.DataFrame, label: str) -> pd.DataFrame:
        """
        Compute hash for each row using parallel processing for large tables.

        Args:
            df: DataFrame to hash
            label: Display label for logging

        Returns:
            DataFrame with _ROW_HASH_ column added
        """
        logger.info(f"  Computing hashes for {label}...")

        total_cells = len(df) * len(self.common_columns)
        skip_norm = self.skip_normalisation
        decimal_prec = self.decimal_precision

        # For small dataframes, single-threaded is faster (avoids multiprocessing overhead)
        if total_cells < 100_000:
            def hash_row(row: pd.Series) -> str:
                return _compute_row_hash(row.tolist(), skip_norm, decimal_prec)

            df = df.copy()
            df['_ROW_HASH_'] = df[self.common_columns].apply(hash_row, axis=1)
            return df

        # Split into chunks for parallel processing
        num_chunks = self.num_workers * 4  # More chunks than workers for better load balancing
        chunk_size = max(500, len(df) // num_chunks)
        chunks = [df.iloc[i:i+chunk_size].copy() for i in range(0, len(df), chunk_size)]

        logger.info(f"    Processing {len(chunks)} chunks across {self.num_workers} workers...")

        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            args = [(chunk, list(self.common_columns), skip_norm, decimal_prec) for chunk in chunks]
            results = list(executor.map(compute_hashes_chunk_parallel, args))

        # Concatenate results, preserving original indices
        return pd.concat(results)
    
    def compare(self) -> List[Dict[str, Any]]:
        """
        Execute comprehensive comparison.

        Returns:
            List of discrepancy dictionaries
        """
        logger.info("\n" + "=" * 60)
        logger.info("COMPREHENSIVE COMPARISON")
        logger.info("=" * 60)

        logger.info("\nStep 1: Aligning columns...")
        self._align_columns()

        logger.info("\nStep 2: Normalising data...")
        self.source_df = self._normalise_dataframe(self.source_df, "source")
        self.target_df = self._normalise_dataframe(self.target_df, "target")

        logger.info("\nStep 3: Computing row hashes...")
        self.source_df = self._compute_row_hashes(self.source_df, "source")
        self.target_df = self._compute_row_hashes(self.target_df, "target")

        logger.info("\nStep 4: Hash-based exact matching...")
        source_hash_map: DefaultDict[str, List[Any]] = defaultdict(list)
        target_hash_map: DefaultDict[str, List[Any]] = defaultdict(list)

        for idx, h in self.source_df['_ROW_HASH_'].items():
            source_hash_map[h].append(idx)
        for idx, h in self.target_df['_ROW_HASH_'].items():
            target_hash_map[h].append(idx)

        exact_match_hashes = set(source_hash_map.keys()) & set(target_hash_map.keys())

        source_matched: Set[Any] = set()
        target_matched: Set[Any] = set()

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

        logger.info(f"  [OK] Exact matches: {len(source_matched):,} rows")

        source_unmatched_idx = list(set(self.source_df.index) - source_matched)
        target_unmatched_idx = list(set(self.target_df.index) - target_matched)

        logger.info(f"  Rows to compare further: {len(source_unmatched_idx):,} source, {len(target_unmatched_idx):,} target")

        if not source_unmatched_idx and not target_unmatched_idx:
            logger.info("\n[OK] All rows matched exactly!")
            return self.discrepancies

        logger.info("\nStep 5: Deep key-based comparison...")

        source_unmatched = self.source_df.loc[source_unmatched_idx].drop(columns=['_ROW_HASH_']).copy()
        target_unmatched = self.target_df.loc[target_unmatched_idx].drop(columns=['_ROW_HASH_']).copy()

        if self.key_columns:
            key_cols = [c.upper() for c in self.key_columns if c.upper() in self.common_columns]
            logger.info(f"  Using specified key columns: {', '.join(key_cols)}")
        else:
            logger.info("  Auto-detecting composite key columns...")
            key_cols = detect_composite_key(source_unmatched)

        compare_cols = list(self.common_columns)

        logger.info("\n  Detecting duplicate rows...")
        self._detect_duplicates(self.source_df.drop(columns=['_ROW_HASH_']), key_cols, compare_cols, 'SOURCE')
        self._detect_duplicates(self.target_df.drop(columns=['_ROW_HASH_']), key_cols, compare_cols, 'TARGET')

        logger.info("\n  Building target index...")
        target_key_to_indices: DefaultDict[str, List[Any]] = defaultdict(list)
        target_fuzzy_key_to_indices: DefaultDict[str, List[Any]] = defaultdict(list)

        for idx, row in target_unmatched.iterrows():
            # Build exact key
            key = '||'.join([
                f"{c}={str(row[c]) if row[c] is not None else 'NULL'}"
                for c in key_cols
            ])
            target_key_to_indices[key].append(idx)
            
            # Build fuzzy key for fallback matching
            fuzzy_key = build_fuzzy_key(row, key_cols)
            target_fuzzy_key_to_indices[fuzzy_key].append(idx)

        duplicate_keys = {k: v for k, v in target_key_to_indices.items() if len(v) > 1}
        if duplicate_keys:
            logger.warning(f"  [!] Warning: {len(duplicate_keys)} duplicate keys found in unmatched target rows")
        
        logger.info(f"  Built {len(target_key_to_indices):,} exact keys, {len(target_fuzzy_key_to_indices):,} fuzzy keys")

        logger.info("  Running parallel deep comparison (with fuzzy key matching)...")
        self._parallel_deep_comparison(source_unmatched, target_unmatched, target_key_to_indices, target_fuzzy_key_to_indices, compare_cols, key_cols)

        return self.discrepancies

    def _detect_duplicates(
        self,
        df: pd.DataFrame,
        key_cols: List[str],
        compare_cols: List[str],
        label: str
    ) -> None:
        """
        Detect and report true duplicate rows using full row hash with normalised values.

        Args:
            df: DataFrame to check for duplicates
            key_cols: Key columns for composite key display
            compare_cols: Columns to include in hash
            label: 'SOURCE' or 'TARGET' for reporting
        """
        skip_norm = self.skip_normalisation
        decimal_prec = self.decimal_precision
        total_cells = len(df) * len(compare_cols)

        hash_to_indices: DefaultDict[str, List[Any]] = defaultdict(list)

        # For small dataframes, single-threaded is faster (avoids multiprocessing overhead)
        if total_cells < 100_000:
            def row_to_hash(row: pd.Series) -> str:
                return _compute_row_hash(row.tolist(), skip_norm, decimal_prec)

            # Use vectorized apply instead of iterrows
            hashes = df[compare_cols].apply(row_to_hash, axis=1)
            for idx, row_hash in zip(df.index, hashes):
                hash_to_indices[row_hash].append(idx)
        else:
            # Split into chunks for parallel processing
            num_chunks = self.num_workers * 4
            chunk_size = max(500, len(df) // num_chunks)
            chunks = [df.iloc[i:i+chunk_size].copy() for i in range(0, len(df), chunk_size)]

            with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
                args = [(chunk, list(compare_cols), skip_norm, decimal_prec) for chunk in chunks]
                results = list(executor.map(detect_duplicates_hash_chunk, args))

            # Merge results from all chunks
            for chunk_results in results:
                for idx, row_hash in chunk_results:
                    hash_to_indices[row_hash].append(idx)

        duplicate_hashes = {h: v for h, v in hash_to_indices.items() if len(v) > 1}

        if duplicate_hashes:
            total_duplicate_rows = sum(len(v) for v in duplicate_hashes.values())
            unique_duplicates = len(duplicate_hashes)
            logger.info(f"    {label}: Found {unique_duplicates} unique rows duplicated ({total_duplicate_rows} total duplicate rows)")

            discrepancy_type = f'DUPLICATE_IN_{label}'

            for row_hash, indices in duplicate_hashes.items():
                first_row = df.loc[indices[0]]

                # Cache normalised values to avoid repeated normalisation calls
                normalised_key_vals = {c: normalise_value(first_row[c], skip_norm, numeric_precision=decimal_prec) for c in key_cols}
                normalised_compare_vals = {c: normalise_value(first_row[c], skip_norm, numeric_precision=decimal_prec) for c in compare_cols}

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
            logger.info(f"    {label}: No duplicate rows found")

    def _parallel_deep_comparison(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        target_key_to_indices: Dict[str, List[Any]],
        target_fuzzy_key_to_indices: Dict[str, List[Any]],
        compare_cols: List[str],
        key_cols: List[str]
    ) -> None:
        """
        Perform deep comparison using parallel processing.
        Supports both exact and fuzzy key matching.

        Args:
            source_df: Source DataFrame (unmatched rows only)
            target_df: Target DataFrame (unmatched rows only)
            target_key_to_indices: Mapping of exact composite keys to target row indices
            target_fuzzy_key_to_indices: Mapping of fuzzy keys to target row indices
            compare_cols: Columns to compare
            key_cols: Key columns for composite key
        """
        all_target_indices: Set[Any] = set()
        for indices in target_key_to_indices.values():
            all_target_indices.update(indices)

        chunk_size = max(500, len(source_df) // (self.num_workers * 8))
        chunks = [source_df.iloc[i:i + chunk_size] for i in range(0, len(source_df), chunk_size)]

        logger.info(f"  Processing {len(chunks)} chunks with {self.num_workers} workers...")

        all_matched_target_indices: Set[Any] = set()
        processed = 0
        total_chunks = len(chunks)

        target_key_dict = dict(target_key_to_indices)
        target_fuzzy_key_dict = dict(target_fuzzy_key_to_indices)

        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            futures = []
            for chunk in chunks:
                args = (chunk, target_key_dict, target_fuzzy_key_dict, target_df, compare_cols, key_cols, self.skip_normalisation, self.decimal_precision)
                futures.append(executor.submit(compare_rows_parallel, args))

            for future in as_completed(futures):
                chunk_discrepancies, matched_indices = future.result()
                self.discrepancies.extend(chunk_discrepancies)
                all_matched_target_indices.update(matched_indices)
                processed += 1

                if processed % max(1, total_chunks // 10) == 0 or processed == total_chunks:
                    pct = (processed / total_chunks) * 100
                    logger.info(f"    Progress: {processed}/{total_chunks} chunks ({pct:.0f}%)")

        logger.info("  Identifying rows only in target...")
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

        logger.info(f"\n  Comparison Summary:")
        logger.info(f"    Rows matched by key: {matched_count:,}")
        logger.info(f"    Rows only in source: {missing_in_target:,}")
        logger.info(f"    Rows only in target: {missing_in_source:,}")


def _write_chunk_to_csv(args: Tuple[pd.DataFrame, str, bool]) -> str:
    """
    Write a DataFrame chunk to a temporary CSV file.
    
    Args:
        args: Tuple of (chunk_df, temp_dir, include_header)
    
    Returns:
        Path to the temporary file
    """
    chunk_df, temp_dir, include_header = args
    temp_fd, temp_path = tempfile.mkstemp(suffix='.csv', dir=temp_dir)
    with os.fdopen(temp_fd, 'w', encoding='utf-8', newline='') as f:
        chunk_df.to_csv(f, index=False, header=include_header)
    return temp_path


def _compute_stats_chunk(args: Tuple[pd.DataFrame, str]) -> Dict[str, Any]:
    """
    Compute statistics for a chunk of the report DataFrame.
    
    Args:
        args: Tuple of (chunk_df, stat_type)
    
    Returns:
        Dictionary with statistics
    """
    chunk_df, stat_type = args
    
    if stat_type == 'type_counts':
        return {'type_counts': chunk_df['discrepancy_type'].value_counts().to_dict()}
    elif stat_type == 'column_counts':
        mismatch_df = chunk_df[chunk_df['discrepancy_type'] == 'VALUE_MISMATCH']
        if len(mismatch_df) > 0:
            return {'column_counts': mismatch_df['column_name'].value_counts().to_dict()}
        return {'column_counts': {}}
    return {}


def generate_report(
    discrepancies: List[Dict[str, Any]],
    output_file: str,
    column_headers: Optional[List[str]] = None,
    num_workers: Optional[int] = None
) -> None:
    """
    Generate detailed CSV report with column reference using parallel processing.

    Args:
        discrepancies: List of discrepancy dictionaries
        output_file: Path to output CSV file
        column_headers: Optional list of column names for reference file
        num_workers: Number of parallel workers (default: CPU count - 1)
    """
    logger.info("\n" + "=" * 60)
    logger.info("REPORT GENERATION")
    logger.info("=" * 60)

    if num_workers is None:
        num_workers = max(1, mp.cpu_count() - 1)

    if not discrepancies:
        logger.info("\n[OK] SUCCESS: No discrepancies found!")
        df = pd.DataFrame(columns=[
            'discrepancy_type', 'composite_key', 'column_name',
            'source_value', 'target_value', 'full_source_row', 'full_target_row'
        ])
        df.to_csv(output_file, index=False)
        logger.info(f"\n[OK] Empty report saved to: {output_file}")
        return

    total_discrepancies = len(discrepancies)
    logger.info(f"\n  Processing {total_discrepancies:,} discrepancies with {num_workers} workers...")

    # Convert to DataFrame
    df = pd.DataFrame(discrepancies)

    col_order = [
        'discrepancy_type', 'composite_key', 'column_name',
        'source_value', 'target_value', 'full_source_row', 'full_target_row'
    ]
    df = df[[c for c in col_order if c in df.columns]]

    # For small datasets, use single-threaded approach
    if total_discrepancies < 10_000:
        df = df.sort_values(['discrepancy_type', 'composite_key', 'column_name'])
        df.to_csv(output_file, index=False)
    else:
        # Parallel sorting and writing for large datasets
        logger.info("  Sorting discrepancies...")
        df = df.sort_values(['discrepancy_type', 'composite_key', 'column_name'])

        logger.info("  Writing report in parallel chunks...")
        chunk_size = max(5000, total_discrepancies // (num_workers * 2))
        chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        temp_dir = tempfile.mkdtemp(prefix='csv_report_')
        try:
            # Write chunks in parallel
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                args_list = [(chunk, temp_dir, i == 0) for i, chunk in enumerate(chunks)]
                temp_files = list(executor.map(_write_chunk_to_csv, args_list))

            # Concatenate temp files into final output
            with open(output_file, 'wb') as outfile:
                for temp_file in temp_files:
                    with open(temp_file, 'rb') as infile:
                        outfile.write(infile.read())

            # Cleanup temp files
            for temp_file in temp_files:
                try:
                    os.remove(temp_file)
                except OSError:
                    pass
        finally:
            try:
                os.rmdir(temp_dir)
            except OSError:
                pass

    # Compute and display statistics
    logger.info(f"\n[X] DISCREPANCIES FOUND: {total_discrepancies:,}")
    
    logger.info("\nBreakdown by type:")
    for dtype, count in df['discrepancy_type'].value_counts().items():
        logger.info(f"  {dtype}: {count:,}")

    if 'VALUE_MISMATCH' in df['discrepancy_type'].values:
        mismatch_df = df[df['discrepancy_type'] == 'VALUE_MISMATCH']
        logger.info(f"\nValue mismatches by column (top 10):")
        col_counts = mismatch_df['column_name'].value_counts().head(10)
        for col, count in col_counts.items():
            logger.info(f"  {col}: {count:,}")

    logger.info(f"\n[OK] Report saved to: {output_file}")

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
        logger.info(f"[OK] Column reference saved to: {ref_file}")

    logger.info("\nSample discrepancies (first 5):")
    sample_df = df.head(5).copy()
    for col in ['source_value', 'target_value']:
        sample_df[col] = sample_df[col].apply(lambda x: x[:50] + '...' if len(str(x)) > 50 else x)
    logger.info(sample_df.to_string(index=False))


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        Parsed argument namespace
    """
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
    parser.add_argument("--decimal-precision", dest="decimal_precision", type=int, default=6,
                        help="Number of decimal places for numeric comparison (default: 6)")
    parser.add_argument("--esc-char", dest="escape_char", type=str, default=None,
                        help="Escape character for CSV parsing (e.g., --esc-char \"\\\\\" for backslash). Default: None")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable verbose/debug logging")

    return parser.parse_args()


def main() -> None:
    """Main entry point for the CSV comparator."""
    args = parse_arguments()

    # Configure logging level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("=" * 70)
    logger.info("CSV Comparator - High-Performance Migration Validation")
    logger.info("Hive to Snowflake Data Comparison")
    logger.info("=" * 70)

    if args.source_csv and args.target_csv:
        source_file = args.source_csv
        target_file = args.target_csv
        key_columns = args.key_columns if args.key_columns else None
        output_dir = args.output_dir
        skip_normalisation = args.no_normalisation
        decimal_precision = args.decimal_precision
        escape_char = args.escape_char
    else:
        logger.info("\nUsage: python csv_comparator.py <source_csv> <target_csv> [key_col1 key_col2 ...] [--output-dir DIR] [--no-normalisation] [--decimal-precision N] [--esc-char CHAR]")
        logger.info("\nKey columns are optional - auto-detected if not provided.")
        source_file = input("Source (Hive) CSV path: ").strip()
        target_file = input("Target (Snowflake) CSV path: ").strip()
        key_input = input("Key columns (comma-separated, or Enter for auto): ").strip()
        key_columns = [c.strip() for c in key_input.split(',')] if key_input else None
        output_dir = None
        skip_normalisation = False
        decimal_precision = DEFAULT_NUMERIC_PRECISION
        escape_char = None

    for path, name in [(source_file, "Source"), (target_file, "Target")]:
        if not os.path.exists(path):
            logger.error(f"\n[X] Error: {name} file not found: {path}")
            sys.exit(1)

    logger.info(f"\nSource: {source_file}")
    logger.info(f"Target: {target_file}")
    logger.info(f"Keys: {', '.join(key_columns) if key_columns else 'Auto-detect'}")
    if output_dir:
        logger.info(f"Output directory: {output_dir}")
    logger.info(f"Normalisation: {'Disabled' if skip_normalisation else 'Enabled'}")
    logger.info(f"Decimal precision: {decimal_precision}")
    logger.info(f"Escape character: {repr(escape_char) if escape_char else 'None'}")

    logger.info("\n" + "-" * 40)
    logger.info("Loading files...")
    source_df = load_csv_file(source_file, "Source (Hive)", escape_char)
    target_df = load_csv_file(target_file, "Target (Snowflake)", escape_char)

    comparator = HighPerformanceComparator(
        source_df, target_df, key_columns, skip_normalisation, decimal_precision=decimal_precision
    )
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

    logger.info("\n" + "=" * 70)
    logger.info("Comparison Complete!")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
