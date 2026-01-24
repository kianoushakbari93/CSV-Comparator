# CSV Comparator

High-performance CSV comparison tool for Hive to Snowflake data migration validation.

---

## Features

- **Intelligent delimiter detection** - Automatically detects pipe, comma, tab, and other delimiters
- **Value normalisation** - Handles differences in timestamps, booleans, nulls, and numeric formats
- **Composite key detection** - Automatically identifies optimal key columns for matching
- **Duplicate detection** - Reports duplicate rows in source and target files
- **Parallel processing** - Uses multiprocessing for large datasets (100k+ cells)
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

- `source_csv` - Path to the source (Hive) CSV file
- `target_csv` - Path to the target (Snowflake) CSV file
- `key_columns` - Optional: Space-separated list of key columns
- `--output-dir DIR` - Directory to save the discrepancy report
- `--no-normalisation` - Disable value normalisation

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

Use `--no-normalisation` to disable this behaviour for strict byte-for-byte comparison.

---

## Output

### Console Output

The tool displays:

- File loading status
- Delimiter detection results
- Key column selection
- Comparison progress
- Summary of matches and discrepancies

### Discrepancy Report

A CSV report is generated with the following columns:

- `discrepancy_type` - Type of discrepancy:
  - `VALUE_MISMATCH` - Values differ between source and target
  - `MISSING_IN_SOURCE` - Row exists in target but not in source
  - `MISSING_IN_TARGET` - Row exists in source but not in target
  - `DUPLICATE_IN_SOURCE` - Duplicate row found in source
  - `DUPLICATE_IN_TARGET` - Duplicate row found in target
- `composite_key` - The key values identifying the row
- `column_name` - Column where mismatch occurred
- `source_value` - Value in source file
- `target_value` - Value in target file
- `full_source_row` - Complete source row (pipe-delimited)
- `full_target_row` - Complete target row (pipe-delimited)

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

- `--skip-unit` - Skip unit tests
- `--skip-integration` - Skip integration tests
- `--skip-performance` - Skip performance tests
- `--skip-error` - Skip error handling tests
- `--performance-rows N` - Number of rows for performance test (default: 10000)
- `-c PATH` - Path to csv_comparator.py if not in same directory

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

- **Unit Tests (96 tests)** - Tests individual functions
- **Integration Tests (30 tests)** - End-to-end CSV comparison tests
- **Performance Tests (3 tests)** - Large dataset benchmarks
- **Error Handling (4 tests)** - Edge cases and error conditions

### Unit Tests Cover

- `normalise_value()` - Value normalisation
- `detect_delimiter_for_line()` - Single line delimiter detection
- `detect_delimiters()` - File delimiter detection
- `preprocess_quoted_rows()` - Quoted row handling
- `expand_row_text_column()` - Row text expansion
- `detect_composite_key()` - Key column detection
- `generate_report()` - Report generation
- `HighPerformanceComparator` - Main comparison class
- `cleaned_csv()` - Context manager
- `load_mixed_delimiter_csv()` - Mixed delimiter handling
- Parallel processing functions

### Integration Tests Cover

- Timestamp normalisation
- Boolean normalisation
- Null value handling
- Numeric formatting
- Column name case sensitivity
- Duplicate row handling
- Missing row detection
- Value mismatch detection
- Multiple delimiter types
- Unicode characters
- Edge cases (empty files, single row, single column)

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

---

## License

Internal use only.
