# CSV Comparator

High-performance CSV comparison tool for Hive to Snowflake data migration validation.

There are two editions of the tool in this repository:

| Edition | Entry point | Engine | Use when |
|---------|-------------|--------|----------|
| **DuckDB edition (current)** | `csv_comparator_duckdb.py` | DuckDB + pandas | **Recommended.** Large files (multi-GB), streams from disk, low memory. |
| Legacy edition | `csv_comparator.py` | pandas | Older pandas-only implementation, kept for reference. |

The DuckDB edition is the newer version of the same tooling. It preserves every
feature of the legacy edition but replaces the pandas core with DuckDB so that
two multi-GB files can be compared without loading both fully into RAM. Unless
you have a specific reason to use the legacy edition, prefer the DuckDB edition.

The rest of this README documents the **DuckDB edition**. See
[Legacy Edition](#legacy-edition) at the end for the pandas version.

---

## Why DuckDB?

The DuckDB edition replaces pandas as the core data engine:

- **Streams CSVs from disk** - no 12GB+ RAM needed for two 6GB files
- **SQL-based normalisation, hashing, and matching** - runs inside DuckDB
- **Only pulls small unmatched sets into pandas** for fuzzy comparison
- **Report generation unchanged** - pandas + openpyxl for XLSX

---

## Features

- **Intelligent delimiter detection** - Automatically detects pipe, comma, tab, semicolon, and tilde delimiters (including mixed header/row delimiters)
- **Value normalisation** - Handles differences in timestamps, booleans, nulls, and numeric formats
- **Timezone-aware timestamps** - Strips Snowflake zero-offset suffixes (`Z`, ` UTC`, ` GMT`, `+00:00`, `+0000`)
- **Scientific notation expansion** - Expands Hive doubles (`1.5E7`) to positional form to match Snowflake output
- **Hive `\N` null marker** - Recognised and normalised to null
- **BOM and CRLF handling** - Strips UTF-8 BOM from the first column and tolerates Windows line endings
- **Composite key detection** - Automatically identifies optimal key columns for matching, with name-based boosting for ID/code/date columns
- **Fuzzy key matching** - Matches rows with similar keys (slashes, pipes, whitespace, numeric tolerance)
- **Schema diffing** - Reports columns present in only one file (`MISSING_COLUMN`) and excludes them from value comparison
- **Duplicate detection** - Opt-in via `--detect-duplicates`; reports duplicate rows in source and target
- **Batch folder mode** - Compare whole directories of `Source_*` / `Target_*` files in one run
- **Parallel report generation** - Multiprocessing for large XLSX reports
- **Detailed reporting** - Generates XLSX reports by default (CSV optional) with full row context

---

## Requirements

- Python 3.8 or higher
- duckdb >= 0.9.0
- pandas >= 1.5.0
- openpyxl (for XLSX output)

---

## Installation

```bash
pip install -r requirements.txt
```

Or install the packages directly:

```bash
pip install duckdb pandas openpyxl
```

---

## Usage

### Basic Usage

```bash
python csv_comparator_duckdb.py <source_csv> <target_csv>
```

### With Key Columns

```bash
python csv_comparator_duckdb.py source.csv target.csv SEC_ID AS_OF_DATE ACCOUNT_NUM
```

### Batch Folder Mode

Pass two **directories** instead of two files to compare every matching pair in one run:

```bash
python csv_comparator_duckdb.py ./source_csv ./target_csv --output-dir ./reports
```

Files are paired by name: `Source_<TABLE>_<...>.csv` is matched against
`Target_<TABLE>_<...>.csv` (the `Source_`/`Target_` prefix and any trailing
`_<digits>` segments such as date/time stamps are stripped to derive the table
key). A per-table log and discrepancy report are written to the output
directory, plus a `batch_summary_<timestamp>.txt` roll-up.

### Command Line Options

| Option | Description |
|--------|-------------|
| `source_csv` | Path to the source (Hive) CSV file, or a directory in batch mode |
| `target_csv` | Path to the target (Snowflake) CSV file, or a directory in batch mode |
| `key_columns` | Optional: Space-separated list of key columns |
| `--output-dir DIR` | Directory to save the discrepancy report |
| `--no-normalisation` | Disable value normalisation (strict, case-sensitive comparison) |
| `--decimal-precision N` | Number of decimal places for numeric comparison (default: 6) |
| `--esc-char CHAR` | Escape character for CSV parsing (default: None) |
| `--output-format FORMAT` | Output format: `xlsx` or `csv` (default: xlsx) |
| `--detect-duplicates` | Enable duplicate-row detection (off by default) |
| `-v, --verbose` | Enable verbose/debug logging |

### Examples

```bash
# Basic comparison with auto-detected keys
python csv_comparator_duckdb.py hive_export.csv snowflake_export.csv

# Specify key columns manually
python csv_comparator_duckdb.py hive_export.csv snowflake_export.csv TRADE_ID TRADE_DATE

# Save report to specific directory
python csv_comparator_duckdb.py hive_export.csv snowflake_export.csv --output-dir ./reports

# Enable duplicate detection
python csv_comparator_duckdb.py hive_export.csv snowflake_export.csv --detect-duplicates

# Disable normalisation for strict comparison
python csv_comparator_duckdb.py hive_export.csv snowflake_export.csv --no-normalisation

# Use 2 decimal places for currency values
python csv_comparator_duckdb.py hive_export.csv snowflake_export.csv --decimal-precision 2

# Use backslash as escape character (for Hive exports with backslash escaping)
# Windows (CMD/PowerShell):
python csv_comparator_duckdb.py hive_export.csv snowflake_export.csv --esc-char "\"
# Unix/Linux/macOS (Bash):
python csv_comparator_duckdb.py hive_export.csv snowflake_export.csv --esc-char "\\"

# Generate report in CSV format (instead of default XLSX)
python csv_comparator_duckdb.py hive_export.csv snowflake_export.csv --output-format csv
```

### Interactive Mode

Run without arguments to enter interactive mode:

```bash
python csv_comparator_duckdb.py
```

You will be prompted for:

- Source CSV path
- Target CSV path
- Key columns (optional, comma-separated)

---

## How It Works

The DuckDB edition compares files in five stages, doing as much work as possible
in SQL and only falling back to Python/pandas for the small set of rows that
cannot be matched in bulk:

1. **Align columns** - Determine common columns; report source-only and target-only columns as `MISSING_COLUMN`.
2. **Raw hash match** - MD5-hash every row's raw column values in SQL and pair byte-identical rows instantly.
3. **Normalised hash match** - Apply SQL normalisation (timestamps, booleans, nulls, numerics) to the remaining unmatched rows and re-hash, catching format-only differences (`100.50` vs `100.5`, `NULL` vs `None`, `2025-01-01T00:00:00` vs `2025-01-01 00:00:00`).
4. **Composite key detection** - If keys were not supplied, auto-detect them on the still-unmatched rows.
5. **Deep key-based comparison** - Pull only the unmatched rows into pandas for column-by-column comparison with fuzzy key matching.

Because steps 2-3 eliminate the bulk of matching rows in SQL, only genuinely
differing rows reach the Python deep-comparison stage.

---

## Value Normalisation

The comparator normalises values to handle common differences between Hive and Snowflake:

**Timestamps:**
- `2025-01-15 10:30:00.123456789` -> `2025-01-15 10:30:00`
- `2025-01-15T10:30:00` -> `2025-01-15 10:30:00`
- `2025-01-15T10:30:00Z` / `... UTC` / `...+00:00` -> `2025-01-15 10:30:00` (zero-offset timezone suffixes stripped)

**Booleans:**
- `true`, `TRUE`, `Yes`, `Y`, `1` -> `true`
- `false`, `FALSE`, `No`, `N`, `0` -> `false`

**Nulls:**
- `NULL`, `None`, `NaN`, `NaT`, `N/A`, `NA`, `#N/A`, `<null>`, `\N`, `""` -> `None`

**Numbers:**
- `123.000`, `123.0` -> `123`
- `1,234,567` -> `1234567`
- `1.5E7` -> `15000000` (scientific notation expanded to positional form)
- Decimal values are truncated to 6 decimal places by default

Use `--decimal-precision N` to change the number of decimal places used for
numeric comparison. Use `--no-normalisation` to disable normalisation for a
strict, case-sensitive, byte-for-byte comparison.

> **Note:** because `0` and `1` normalise to booleans, a numeric `0` and a
> numeric `0.0` are treated as different (`false` vs `0`). This matches how Hive
> and Snowflake export boolean columns and is intentional.

---

## Fuzzy Key Matching

When exact key matching fails, the comparator attempts fuzzy key matching to find similar rows. This helps identify rows that should match but have minor formatting differences in key columns.

### Fuzzy Matching Rules

**For String Values:**
| Source | Target | Match? | Reason |
|--------|--------|--------|--------|
| `UPAM/9741/2265` | `UPAM 9741 2265` | ✓ | Slashes replaced with spaces |
| `ABC\|DEF` | `ABC DEF` | ✓ | Pipes replaced with spaces |
| `ABC\\DEF` | `ABC DEF` | ✓ | Backslashes replaced with spaces |
| `ABC_DEF_123` | `ABCDEF123` | ✓ | Underscores removed |
| `ABC-DEF-123` | `ABCDEF123` | ✓ | Dashes removed |
| `ABC.DEF.123` | `ABCDEF123` | ✓ | Dots removed |
| `ABC  DEF` | `ABC DEF` | ✓ | Multiple spaces collapsed |
| `abc/def` | `ABC DEF` | ✓ | Case-insensitive |

**For Numeric Values:**
| Source | Target | Match? | Reason |
|--------|--------|--------|--------|
| `156999` | `157050` | ✓ | Within 100 absolute tolerance |
| `1000000` | `1005000` | ✓ | Within 1% relative tolerance |
| `100` | `300` | ✗ | Outside tolerance |

### Fuzzy Match Reporting

When rows are matched via fuzzy matching:
- Key column differences are reported as `KEY_VALUE_MISMATCH`
- The composite key shows both keys: `source_key ~> target_key`
- Non-key column differences are still reported as `VALUE_MISMATCH`

---

## Escape Character Option

The `--esc-char` option allows you to specify an escape character for CSV parsing. This is useful when:

- Hive exports use backslash escaping (`\|`, `\n`, `\"`)
- CSV files contain special characters that need escaping
- Different systems use different escape conventions

**Default behaviour:** No escape character (None)

**Note:** Only single-character escape values are supported. The backslash
character requires different escaping depending on your shell - Windows shells
pass `"\"` as a single backslash, whilst Unix shells require `"\\"` to produce a
single backslash.

---

## Composite Key Detection

When key columns are not supplied, the comparator auto-detects them on the
unmatched rows. Key column candidates are ranked by:

1. Name patterns - columns matching `*_ID`, `*_CD`, `*_CODE`, `*_KEY`, `*_NUM`, `*_REF`, common business-date names (`AS_OF_DATE`, `TRADE_DATE`, `SETTLE_DATE`, ...) and identifiers (`ISIN`, `CUSIP`, `SEDOL`, ...) get a priority boost
2. Integer columns (likely IDs)
3. Alphanumeric / alphabetic code columns
4. Columns with high cardinality and few nulls

Columns excluded from key detection (unless they match a key-candidate name pattern):

- Timestamp/date columns
- Amount/value/price/quantity/balance/ratio/rate columns
- Description/comment/note/text columns
- ETL/audit metadata columns (`EDD_*`, `ETL_*`, `DW_*`, `EXTRACT_*`, `UUID`, `GUID`, `HASH`, ...)

The detector targets 99.9% composite-key uniqueness, trying combinations of the
top candidates and falling back to the top columns if no unique combination is
found.

---

## Output

### Console Output

The tool displays file loading status, delimiter detection, DuckDB memory/thread
settings, schema differences, matching progress at each stage, key column
selection with composite-key uniqueness, and a summary of matches and
discrepancies.

### Output Formats

**XLSX (default)**
- Formatted with headers, filters, and frozen panes
- Excel row limit: 1,048,576 rows (including header)
- **Automatic splitting**: reports exceeding the limit are split into `report_part1.xlsx`, `report_part2.xlsx`, ... written in parallel
- Requires `openpyxl`: `pip install openpyxl`

**CSV**
- No row limit
- Best for very large reports (> 1 million rows)

### Discrepancy Report

A report is generated with the following columns:

| Column | Description |
|--------|-------------|
| `discrepancy_type` | Type of discrepancy (see below) |
| `composite_key` | The key values identifying the row |
| `column_name` | Column where mismatch occurred |
| `source_value` | Value in source file |
| `target_value` | Value in target file |
| `full_source_row` | Complete source row (pipe-delimited) |
| `full_target_row` | Complete target row (pipe-delimited) |

### Discrepancy Types

| Type | Description |
|------|-------------|
| `VALUE_MISMATCH` | Values differ between source and target (non-key columns) |
| `KEY_VALUE_MISMATCH` | Key column values differ (fuzzy matched rows) |
| `MISSING_IN_SOURCE` | Row exists in target but not in source |
| `MISSING_IN_TARGET` | Row exists in source but not in target |
| `MISSING_COLUMN` | Column exists in only one of the two files |
| `DUPLICATE_IN_SOURCE` | Duplicate row found in source (requires `--detect-duplicates`) |
| `DUPLICATE_IN_TARGET` | Duplicate row found in target (requires `--detect-duplicates`) |
| `DUPLICATE_COUNT_MISMATCH` | Different number of identical rows in source vs target |

### Column Reference File

A `_column_reference.txt` file is also generated listing column positions for interpreting the `full_source_row` and `full_target_row` fields, including any source-only / target-only columns.

---

## Running Tests

The DuckDB edition ships with two test files:

| File | What it covers |
|------|----------------|
| `test_comparator.py` | 42 functional tests across matching, normalisation, duplicates, fuzzy keys, wide tables, large row counts, SQL-vs-Python normalisation parity, and report generation |
| `test_migration_e2e.py` | A realistic 5,000-row Hive-vs-Snowflake migration simulation with shuffled row order, per-engine formatting, a BOM+CRLF target file, and exactly-known injected discrepancies |

Both files import `csv_comparator_duckdb` and run as plain scripts (no test
framework required). They `exit(0)` on success and `exit(1)` on failure, so they
slot straight into CI.

### Prerequisites

Ensure the test files and `csv_comparator_duckdb.py` are in the same directory,
and that `duckdb`, `pandas`, and `openpyxl` are installed.

### Run the Tests

```bash
# Functional test suite (prints "RESULTS: N passed, M failed")
python test_comparator.py

# End-to-end migration simulation (prints "ALL CHECKS PASSED" on success)
python test_migration_e2e.py
```

The end-to-end test is the strongest correctness signal: it passes only if the
comparator reports **exactly** the injected discrepancies - zero false positives
from formatting differences and zero missed real differences.

---

## Troubleshooting

### Windows Encoding Errors

If you see `UnicodeEncodeError`, ensure you're using the latest version of the scripts, which use ASCII-safe console output. UTF-8 BOMs in the source files are stripped automatically.

### Memory Issues with Large Files

The DuckDB edition streams from disk and spills intermediate results to a temp
directory (under the system temp folder) when memory is tight, so it should
handle files far larger than RAM. If you still hit memory pressure:

- Ensure the temp directory's filesystem has free space
- Use `--output-format csv` to avoid building large XLSX workbooks in memory

### Escape Character Errors

If you see `Only length-1 escapes supported`, you're passing more than one
character to `--esc-char`. This is usually a shell-escaping difference: Windows
shells pass `"\"` as a single backslash, whilst Unix shells need `"\\"`.

### High Missing Row Counts

If you see many `MISSING_IN_SOURCE` and `MISSING_IN_TARGET` discrepancies:

1. **Check if source and target overlap** - the files may cover different date ranges or subsets
2. **Review composite key selection** - the auto-detected keys may not be optimal; try specifying keys manually
3. **Enable verbose mode** - use `-v` to see key column selection details
4. **Check fuzzy matching** - rows with similar but not identical keys are fuzzy-matched and reported as `KEY_VALUE_MISMATCH`

---

## Changelog

### DuckDB Edition (current)

- Replaced the pandas core with a DuckDB engine that streams CSVs from disk and runs normalisation, hashing, and matching in SQL - two multi-GB files can now be compared without loading both into RAM
- Added **batch folder mode** - compare directories of `Source_*` / `Target_*` files in one run with a roll-up summary
- Added `--detect-duplicates` - duplicate detection is now opt-in (it was always-on in the legacy edition)
- Added `MISSING_COLUMN` discrepancy type for schema differences; missing columns are excluded from value comparison
- Added timezone-suffix stripping for Snowflake `TIMESTAMP_TZ` exports (`Z`, ` UTC`, ` GMT`, `+00:00`, `+0000`)
- Added scientific-notation expansion (`1.5E7` -> positional) and Hive `\N` null-marker handling
- Added UTF-8 BOM and CRLF tolerance
- Added name-based composite-key boosting for ID/code/date candidate columns
- Replaced the test suite with `test_comparator.py` (42 functional tests, including SQL-vs-Python normalisation parity) and `test_migration_e2e.py` (5,000-row migration simulation)
- Fixed Python/SQL normalisation parity for large whole-number decimals (e.g. `12345678901234567890.0`), which previously lost precision in Python via `int(float(...))`
- Fixed disk-spill temp directory to use the platform temp folder and create it up front (was hardcoded to `/tmp/duckdb_temp`, which fails on Windows)

### Legacy Edition

- Added `--output-format` option for XLSX output with automatic splitting for large reports
- Added `--esc-char` option for configurable escape character (default: None)
- Added fuzzy key matching and the `KEY_VALUE_MISMATCH` discrepancy type
- Added ETL metadata column exclusions (EDD_*, ETL_*, UUID, GUID, EXTRACT_*)
- Added parallel processing for identifying rows only in target

---

## Legacy Edition

The original pandas-based implementation lives in `csv_comparator.py`, with its
test suite in `csv_comparator_tests.py`. It exposes the same CLI (minus
`--detect-duplicates` and batch folder mode) and is functionally equivalent for
files that fit comfortably in memory. It is retained for reference; new work
should target the DuckDB edition.

```bash
# Legacy edition
python csv_comparator.py source.csv target.csv

# Legacy test suite
python csv_comparator_tests.py
```

---

## License

Internal use only.
