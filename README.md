# CSV Comparator

High-performance CSV comparison tool for Hive to Snowflake data migration validation.

---

## Features

- **Intelligent delimiter detection** - Automatically detects pipe, comma, tab, and other delimiters
- **Value normalisation** - Handles differences in timestamps, booleans, nulls, and numeric formats
- **Composite key detection** - Automatically identifies optimal key columns for matching (up to 30 columns)
- **Fuzzy key matching** - Matches rows with similar keys (slashes, pipes, whitespace, numeric tolerance)
- **Duplicate detection** - Reports duplicate rows in source and target files
- **Parallel processing** - Uses multiprocessing for large datasets (100k+ cells)
- **Configurable escape character** - Optional escape character for CSV parsing
- **Detailed reporting** - Generates CSV reports with full row context

---

## Requirements

- Python 3.8 or higher
- pandas >= 1.5.0

---

## Installation

```bash
pip install -r requirements.txt
```

Or simply:

```bash
pip install pandas
```

---

## Usage

### Basic Usage

```bash
python csv_comparator.py <source_csv> <target_csv>
```

### With Key Columns

```bash
python csv_comparator.py source.csv target.csv ID TRADE_DATE ACCOUNT_ID
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `source_csv` | Path to the source (Hive) CSV file |
| `target_csv` | Path to the target (Snowflake) CSV file |
| `key_columns` | Optional: Space-separated list of key columns |
| `--output-dir DIR` | Directory to save the discrepancy report |
| `--no-normalisation` | Disable value normalisation |
| `--decimal-precision N` | Number of decimal places for numeric comparison (default: 6) |
| `--esc-char CHAR` | Escape character for CSV parsing (default: None) |
| `-v, --verbose` | Enable verbose/debug logging |

### Examples

```bash
# Basic comparison with auto-detected keys
python csv_comparator.py hive_export.csv snowflake_export.csv

# Specify key columns manually
python csv_comparator.py hive_export.csv snowflake_export.csv TRADE_ID TRADE_DATE

# Save report to specific directory
python csv_comparator.py hive_export.csv snowflake_export.csv --output-dir ./reports

# Disable normalisation for strict comparison
python csv_comparator.py hive_export.csv snowflake_export.csv --no-normalisation

# Use 10 decimal places for high-precision financial data
python csv_comparator.py hive_export.csv snowflake_export.csv --decimal-precision 10

# Use 2 decimal places for currency values
python csv_comparator.py hive_export.csv snowflake_export.csv --decimal-precision 2

# Use backslash as escape character (for Hive exports with backslash escaping)
python csv_comparator.py hive_export.csv snowflake_export.csv --esc-char "\\"

# Use tilde as escape character
python csv_comparator.py hive_export.csv snowflake_export.csv --esc-char "~"
```

### Interactive Mode

Run without arguments to enter interactive mode:

```bash
python csv_comparator.py
```

You will be prompted for:

- Source CSV path
- Target CSV path
- Key columns (optional)

---

## Value Normalisation

The comparator normalises values to handle common differences between Hive and Snowflake:

**Timestamps:**
- `2025-01-15 10:30:00.123456789` -> `2025-01-15 10:30:00`
- `2025-01-15T10:30:00` -> `2025-01-15 10:30:00`

**Booleans:**
- `true`, `TRUE`, `Yes`, `Y` -> `true`
- `false`, `FALSE`, `No`, `N` -> `false`

**Nulls:**
- `NULL`, `None`, `NaN`, `N/A`, `""` -> `None`

**Numbers:**
- `123.000`, `123.0` -> `123`
- `1,234,567` -> `1234567`
- Decimal values are rounded to 6 decimal places by default

Use `--decimal-precision N` to change the number of decimal places used for numeric comparison. For example, `--decimal-precision 2` for currency values or `--decimal-precision 10` for high-precision scientific data.

Use `--no-normalisation` to disable this behaviour for strict byte-for-byte comparison.

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
| `ABC(DEF)123` | `ABCDEF123` | ✓ | Parentheses removed |
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

```bash
# For Hive exports with backslash escaping
python csv_comparator.py hive_export.csv snowflake_export.csv --esc-char "\\"

# For files using tilde as escape character
python csv_comparator.py source.csv target.csv --esc-char "~"
```

**Note:** Only single-character escape values are supported.

---

## Composite Key Detection

The comparator automatically detects optimal composite keys for large tables:

- **Maximum columns evaluated:** 30
- **Search range:** Up to 25 columns in combinations
- **Fallback:** Uses top 20 candidate columns if no unique combination found
- **Target uniqueness:** 99.9% (falls back if not achievable)

Key column candidates are ranked by:
1. Integer columns (likely IDs)
2. Alphabetic code columns
3. Alphanumeric columns
4. Columns with high cardinality

Columns excluded from key detection:
- Timestamp/date columns
- Columns with patterns like `_RATIO`, `_AMOUNT`, `_VALUE`, etc.
- Columns named `DESCRIPTION`, `COMMENT`, `NOTE`, etc.

---

## Output

### Console Output

The tool displays:

- File loading status
- Delimiter detection results
- Escape character setting
- Key column selection
- Composite key uniqueness percentage
- Comparison progress
- Summary of matches and discrepancies

### Discrepancy Report

A CSV report is generated with the following columns:

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
| `DUPLICATE_IN_SOURCE` | Duplicate row found in source |
| `DUPLICATE_IN_TARGET` | Duplicate row found in target |
| `DUPLICATE_COUNT_MISMATCH` | Different number of identical rows in source vs target |

### Column Reference File

A `_column_reference.txt` file is also generated listing column positions for interpreting the `full_source_row` and `full_target_row` fields.

---

## Running Tests

### Prerequisites

Ensure both files are in the same directory:

- `csv_comparator.py`
- `csv_comparator_tests.py`

### Run All Tests

```bash
python csv_comparator_tests.py
```

### Run Specific Test Categories

```bash
# Unit tests only (fastest, ~5 seconds)
python csv_comparator_tests.py --skip-integration --skip-performance --skip-error

# Integration tests only
python csv_comparator_tests.py --skip-unit --skip-performance --skip-error

# Skip slow performance tests
python csv_comparator_tests.py --skip-performance

# Error handling tests only
python csv_comparator_tests.py --skip-unit --skip-integration --skip-performance
```

### Test Options

| Option | Description |
|--------|-------------|
| `--skip-unit` | Skip unit tests |
| `--skip-integration` | Skip integration tests |
| `--skip-performance` | Skip performance tests |
| `--skip-error` | Skip error handling tests |
| `--performance-rows N` | Number of rows for performance test (default: 10000) |
| `-c PATH` | Path to csv_comparator.py if not in same directory |

### Examples

```bash
# Run with custom comparator path
python csv_comparator_tests.py -c /path/to/csv_comparator.py

# Run performance test with 50,000 rows
python csv_comparator_tests.py --skip-unit --skip-integration --performance-rows 50000
```

### Using pytest (Optional)

If you have pytest installed:

```bash
# Run all tests with verbose output
pytest csv_comparator_tests.py -v

# Run only unit tests
pytest csv_comparator_tests.py -v -k "unit"
```

---

## Test Coverage

The test suite includes:

- **Unit Tests (130+ tests)** - Tests individual functions
- **Integration Tests (38 tests)** - End-to-end CSV comparison tests
- **Performance Tests (3 tests)** - Large dataset benchmarks
- **Error Handling (4 tests)** - Edge cases and error conditions

### Unit Tests Cover

| Function | Tests |
|----------|-------|
| `normalise_value()` | 58 tests - Value normalisation |
| `detect_delimiter_for_line()` | 9 tests - Single line delimiter detection |
| `detect_delimiters()` | 5 tests - File delimiter detection |
| `preprocess_quoted_rows()` | 4 tests - Quoted row handling |
| `expand_row_text_column()` | 4 tests - Row text expansion |
| `detect_composite_key()` | 3 tests - Key column detection |
| `generate_report()` | 3 tests - Report generation |
| `HighPerformanceComparator` | 3 tests - Main comparison class |
| `cleaned_csv()` | 2 tests - Context manager |
| `load_mixed_delimiter_csv()` | 2 tests - Mixed delimiter handling |
| `normalise_key_for_fuzzy()` | 16 tests - Fuzzy key normalisation |
| `key_values_fuzzy_equal()` | 16 tests - Fuzzy value comparison |
| `build_fuzzy_key()` | 2 tests - Fuzzy key building |
| Parallel processing functions | 3 tests |

### Integration Tests Cover

| Category | Tests |
|----------|-------|
| Timestamp normalisation | 3 tests |
| Boolean normalisation | 2 tests |
| Null value handling | 1 test |
| Numeric formatting | 4 tests |
| Column name case sensitivity | 1 test |
| Duplicate row handling | 2 tests |
| Missing row detection | 1 test |
| Value mismatch detection | 1 test |
| Multiple delimiter types | 2 tests |
| Special characters | 1 test |
| Unicode characters | 1 test |
| Edge cases | 4 tests |
| Escape character handling | 2 tests |
| Fuzzy key matching | 6 tests |

---

## Troubleshooting

### Windows Encoding Errors

If you see `UnicodeEncodeError`, ensure you're using the latest version of the scripts which use ASCII-safe characters.

### Multiprocessing Errors in Tests

Some unit tests may show `skipped (multiprocessing not available in test env)` - this is expected when running tests with dynamically imported modules and does not indicate a problem.

### Memory Issues with Large Files

For very large files (millions of rows), consider:

- Increasing available RAM
- Processing in batches
- Using the `--no-normalisation` flag to reduce memory usage

### Escape Character Errors

If you see `Only length-1 escapes supported`, ensure you're passing a single character to `--esc-char`:

```bash
# Correct
python csv_comparator.py source.csv target.csv --esc-char "\\"

# Incorrect (double escaped)
python csv_comparator.py source.csv target.csv --esc-char "\\\\"
```

### High Missing Row Counts

If you see many `MISSING_IN_SOURCE` and `MISSING_IN_TARGET` discrepancies:

1. **Check if source and target have overlapping data** - The files may contain different date ranges or subsets
2. **Review composite key selection** - The auto-detected keys may not be optimal; try specifying keys manually
3. **Enable verbose mode** - Use `-v` to see key column selection details
4. **Check fuzzy matching** - Rows with similar but not identical keys will be fuzzy-matched and reported as `KEY_VALUE_MISMATCH`

---

## Changelog

### Latest Version

- Added `--esc-char` option for configurable escape character (default: None)
- Added fuzzy key matching for rows with similar keys
- Added `KEY_VALUE_MISMATCH` discrepancy type for fuzzy-matched rows
- Increased composite key limits: max 30 columns, search up to 25 combinations, fallback to 20
- Improved test coverage with 38 integration tests and 130+ unit tests

---

## License

Internal use only.
