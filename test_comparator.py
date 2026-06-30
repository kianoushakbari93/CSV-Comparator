#!/usr/bin/env python3
"""Comprehensive test suite for csv_comparator_duckdb.py"""

import csv_comparator_duckdb as cmp
import tempfile, os, sys, duckdb, traceback

PASS = 0
FAIL = 0

def write_csv(rows, delim='|'):
    f = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, dir='/tmp')
    for row in rows:
        f.write(delim.join(str(c) for c in row) + '\n')
    f.close()
    return f.name

def run_test(name, src_rows, tgt_rows, expected_types, key_columns=None,
             delim='|', detect_duplicates=False, skip_normalisation=False,
             check_fn=None):
    """Run one test. expected_types is a dict of {type: expected_count}."""
    global PASS, FAIL
    src_file = write_csv(src_rows, delim)
    tgt_file = write_csv(tgt_rows, delim)
    try:
        con = duckdb.connect()
        sc, _ = cmp.load_csv_to_duckdb(con, src_file, 'source_raw', 'Src')
        tc, _ = cmp.load_csv_to_duckdb(con, tgt_file, 'target_raw', 'Tgt')
        comp = cmp.DuckDBComparator(con, 'source_raw', 'target_raw', sc, tc,
            key_columns=key_columns, detect_duplicates=detect_duplicates,
            skip_normalisation=skip_normalisation)
        discs = comp.compare()
        con.close()

        # Check counts by type
        actual = {}
        for d in discs:
            t = d['discrepancy_type']
            actual[t] = actual.get(t, 0) + 1

        ok = True
        msgs = []
        for etype, ecount in expected_types.items():
            acount = actual.get(etype, 0)
            if acount != ecount:
                ok = False
                msgs.append(f"  {etype}: expected {ecount}, got {acount}")

        # Check for unexpected types
        for atype, acount in actual.items():
            if atype not in expected_types:
                ok = False
                msgs.append(f"  UNEXPECTED {atype}: {acount}")

        # Custom check function
        if check_fn and ok:
            check_result = check_fn(discs)
            if check_result:
                ok = False
                msgs.append(f"  {check_result}")

        if ok:
            PASS += 1
            print(f"  PASS  {name}")
        else:
            FAIL += 1
            print(f"  FAIL  {name}")
            for m in msgs:
                print(m)
            if len(discs) <= 10:
                for d in discs:
                    print(f"    {d['discrepancy_type']:25s} | col={d['column_name']:15s} | s={str(d['source_value'])[:30]:30s} | t={str(d['target_value'])[:30]}")
    except Exception as e:
        FAIL += 1
        print(f"  FAIL  {name} — EXCEPTION: {e}")
        traceback.print_exc()
    finally:
        os.unlink(src_file)
        os.unlink(tgt_file)


# ============================================================
print("\n" + "=" * 70)
print("TEST SUITE: CSV Comparator DuckDB Edition")
print("=" * 70)

# ============================================================
print("\n--- 1. BASIC MATCHING ---")

run_test("Identical files (all match)",
    [['ID','NAME','VAL'], ['1','Alice','100'], ['2','Bob','200']],
    [['ID','NAME','VAL'], ['1','Alice','100'], ['2','Bob','200']],
    {},  # no discrepancies
    key_columns=['ID'])

run_test("Simple value mismatch",
    [['ID','VAL'], ['1','AAA'], ['2','BBB']],
    [['ID','VAL'], ['1','AAA'], ['2','CCC']],
    {'VALUE_MISMATCH': 1},
    key_columns=['ID'])

run_test("Missing in target",
    [['ID','VAL'], ['1','A'], ['2','B'], ['3','C']],
    [['ID','VAL'], ['1','A'], ['2','B']],
    {'MISSING_IN_TARGET': 1},
    key_columns=['ID'])

run_test("Missing in source",
    [['ID','VAL'], ['1','A']],
    [['ID','VAL'], ['1','A'], ['2','B']],
    {'MISSING_IN_SOURCE': 1},
    key_columns=['ID'])

run_test("Missing column (source-only and target-only)",
    [['ID','SRC_COL','COMMON'], ['1','x','y']],
    [['ID','TGT_COL','COMMON'], ['1','x','y']],
    {'MISSING_COLUMN': 2},
    key_columns=['ID'])


# ============================================================
print("\n--- 2. NULL/EMPTY NORMALISATION ---")

run_test("NULL vs None vs empty vs N/A (all should match)",
    [['ID','A','B','C','D'],
     ['1','NULL','None','','N/A']],
    [['ID','A','B','C','D'],
     ['1','null','none','nan','na']],
    {},
    key_columns=['ID'])

run_test("NULL vs actual value (should mismatch)",
    [['ID','VAL'], ['1','NULL']],
    [['ID','VAL'], ['1','hello']],
    {'VALUE_MISMATCH': 1},
    key_columns=['ID'])

run_test("<null> and #N/A normalisation",
    [['ID','A','B'],
     ['1','<null>','#N/A']],
    [['ID','A','B'],
     ['1','null','none']],
    {},
    key_columns=['ID'])


# ============================================================
print("\n--- 3. BOOLEAN NORMALISATION ---")

run_test("Boolean: TRUE/true/True/yes/y/1 all match",
    [['ID','A','B','C','D','E'],
     ['K1','TRUE','True','yes','y','1']],
    [['ID','A','B','C','D','E'],
     ['K1','true','true','true','true','true']],
    {},
    key_columns=['ID'])

run_test("Boolean: FALSE/false/no/n/0 all match",
    [['ID','A','B','C','D'],
     ['K1','FALSE','no','n','0']],
    [['ID','A','B','C','D'],
     ['K1','false','false','false','false']],
    {},
    key_columns=['ID'])

run_test("Boolean true vs false (should mismatch)",
    [['ID','VAL'], ['1','true']],
    [['ID','VAL'], ['1','false']],
    {'VALUE_MISMATCH': 1},
    key_columns=['ID'])


# ============================================================
print("\n--- 4. NUMERIC NORMALISATION ---")

run_test("Integer with trailing zeros: 200.00 vs 200",
    [['ID','VAL'], ['K1','200.00']],
    [['ID','VAL'], ['K1','200']],
    {},
    key_columns=['ID'])

run_test("Decimal trailing zeros: 100.50 vs 100.5",
    [['ID','VAL'], ['K1','100.50']],
    [['ID','VAL'], ['K1','100.5']],
    {},
    key_columns=['ID'])

run_test("Decimal truncation: 400.123456789 vs 400.123456 (6dp)",
    [['ID','VAL'], ['K1','400.123456789']],
    [['ID','VAL'], ['K1','400.123456']],
    {},
    key_columns=['ID'])

run_test("Commas in numbers: 1,234,567 vs 1234567",
    [['ID','VAL'], ['K1','1,234,567']],
    [['ID','VAL'], ['K1','1234567']],
    {},
    key_columns=['ID'])

run_test("Negative integer: -42 matches",
    [['ID','VAL'], ['K1','-42']],
    [['ID','VAL'], ['K1','-42']],
    {},
    key_columns=['ID'])

run_test("Negative decimal: -3.14 matches",
    [['ID','VAL'], ['K1','-3.14']],
    [['ID','VAL'], ['K1','-3.14']],
    {},
    key_columns=['ID'])

run_test("Zero forms: 0→false (boolean), 0.0→0 (numeric), so A mismatches",
    [['ID','A','B'],
     ['K1','0','0.0']],
    [['ID','A','B'],
     ['K1','0','0']],
    {'VALUE_MISMATCH': 1},  # A: false vs false=OK, B: 0 vs false=MISMATCH
    key_columns=['ID'])

run_test("Leading plus sign: +42 vs 42",
    [['ID','VAL'], ['K1','+42']],
    [['ID','VAL'], ['K1','42']],
    {},
    key_columns=['ID'])

run_test("Actual numeric difference detected",
    [['ID','VAL'], ['K1','100']],
    [['ID','VAL'], ['K1','200']],
    {'VALUE_MISMATCH': 1},
    key_columns=['ID'])


# ============================================================
print("\n--- 5. TIMESTAMP NORMALISATION ---")

run_test("T-separator vs space: 2025-01-15T10:30:00 vs 2025-01-15 10:30:00",
    [['ID','TS'], ['K1','2025-01-15T10:30:00']],
    [['ID','TS'], ['K1','2025-01-15 10:30:00']],
    {},
    key_columns=['ID'])

run_test("Fractional seconds stripped: .123456 vs no fraction",
    [['ID','TS'], ['K1','2025-01-15 10:30:00.123456']],
    [['ID','TS'], ['K1','2025-01-15 10:30:00']],
    {},
    key_columns=['ID'])

run_test("Different fractional seconds both stripped",
    [['ID','TS'], ['K1','2025-01-15T10:30:00.111']],
    [['ID','TS'], ['K1','2025-01-15 10:30:00.999']],
    {},
    key_columns=['ID'])

run_test("ISO date preserved: 2025-01-15 matches",
    [['ID','DT'], ['K1','2025-01-15']],
    [['ID','DT'], ['K1','2025-01-15']],
    {},
    key_columns=['ID'])


# ============================================================
print("\n--- 6. MIXED NORMALISATION (multiple types in same row) ---")

run_test("Complex row: booleans + nulls + decimals + timestamps",
    [['ID','BOOL_COL','NULL_COL','DEC_COL','TS_COL'],
     ['K1','TRUE','NULL','99.990','2025-06-01T12:00:00.500']],
    [['ID','BOOL_COL','NULL_COL','DEC_COL','TS_COL'],
     ['K1','true','none','99.99','2025-06-01 12:00:00']],
    {},
    key_columns=['ID'])

run_test("All columns different formats, all should normalise-match",
    [['ID','A','B','C','D','E','F','G'],
     ['K1','Yes','N/A','1,000','42.0','2025-01-01T00:00:00','FALSE','200.50']],
    [['ID','A','B','C','D','E','F','G'],
     ['K1','true','null','1000','42','2025-01-01 00:00:00','false','200.5']],
    {},
    key_columns=['ID'])


# ============================================================
print("\n--- 7. DUPLICATE DETECTION ---")

run_test("Exact duplicate rows detected (--detect-duplicates)",
    [['ID','VAL'], ['1','A'], ['1','A'], ['2','B']],
    [['ID','VAL'], ['1','A'], ['2','B'], ['2','B']],
    # Hash match pairs 1 of each, extra copy stays unmatched.
    # DUPLICATE_COUNT_MISMATCH x2 (from hash step: 2vs1 for each hash)
    # DUPLICATE_IN_SOURCE: [1,A] x2, DUPLICATE_IN_TARGET: [2,B] x2
    # MISSING_IN_TARGET: extra [1,A], MISSING_IN_SOURCE: extra [2,B]
    {'DUPLICATE_COUNT_MISMATCH': 2, 'DUPLICATE_IN_SOURCE': 1,
     'DUPLICATE_IN_TARGET': 1, 'MISSING_IN_TARGET': 1, 'MISSING_IN_SOURCE': 1},
    key_columns=['ID'], detect_duplicates=True)

run_test("Duplicate count mismatch (3 in source, 2 in target)",
    [['ID','VAL'], ['1','X'], ['1','X'], ['1','X']],
    [['ID','VAL'], ['1','X'], ['1','X']],
    # Hash match pairs 2, leaves 1 source unmatched. 
    # DUPLICATE_COUNT_MISMATCH from hash step (3 vs 2)
    # MISSING_IN_TARGET for the unpaired extra row
    {'DUPLICATE_COUNT_MISMATCH': 1, 'MISSING_IN_TARGET': 1},
    key_columns=['ID'])


# ============================================================
print("\n--- 8. COMMA-DELIMITED CSV ---")

run_test("Comma-delimited basic comparison",
    [['ID','NAME','CITY'],
     ['1','Alice','London'],
     ['2','Bob','Paris']],
    [['ID','NAME','CITY'],
     ['1','Alice','London'],
     ['2','Bob','Berlin']],
    {'VALUE_MISMATCH': 1},
    key_columns=['ID'], delim=',')


# ============================================================
print("\n--- 9. COLUMN NAME EDGE CASES ---")

run_test("Lowercase column names (should be uppercased)",
    [['id','name','val'], ['1','Alice','100']],
    [['ID','NAME','VAL'], ['1','Alice','100']],
    {},
    key_columns=['ID'])


# ============================================================
print("\n--- 10. EMPTY/EDGE DATA ---")

run_test("Single row files",
    [['ID','VAL'], ['1','A']],
    [['ID','VAL'], ['1','A']],
    {},
    key_columns=['ID'])

run_test("Row with all NULL-like values except key",
    [['ID','A','B','C'],
     ['1','NULL','','N/A']],
    [['ID','A','B','C'],
     ['1','none','nan','na']],
    {},
    key_columns=['ID'])


# ============================================================
print("\n--- 11. SKIP NORMALISATION MODE ---")

run_test("--no-normalisation: TRUE vs true should mismatch",
    [['ID','VAL'], ['1','TRUE']],
    [['ID','VAL'], ['1','true']],
    {'VALUE_MISMATCH': 1},
    key_columns=['ID'], skip_normalisation=True)

run_test("--no-normalisation: 100.50 vs 100.5 should mismatch",
    [['ID','VAL'], ['1','100.50']],
    [['ID','VAL'], ['1','100.5']],
    {'VALUE_MISMATCH': 1},
    key_columns=['ID'], skip_normalisation=True)


# ============================================================
print("\n--- 12. MULTI-COLUMN COMPOSITE KEYS ---")

run_test("Two-column composite key",
    [['SEC_ID','AS_OF_DATE','VAL'],
     ['S001','2025-01-01','100'],
     ['S001','2025-01-02','200'],
     ['S002','2025-01-01','300']],
    [['SEC_ID','AS_OF_DATE','VAL'],
     ['S001','2025-01-01','100'],
     ['S001','2025-01-02','999'],
     ['S002','2025-01-01','300']],
    {'VALUE_MISMATCH': 1},
    key_columns=['SEC_ID','AS_OF_DATE'])


# ============================================================
print("\n--- 13. FUZZY KEY MATCHING ---")

run_test("Fuzzy key: trailing space in key value",
    [['ID','VAL'], ['ABC ','100']],
    [['ID','VAL'], ['ABC','100']],
    {},
    key_columns=['ID'])


# ============================================================
print("\n--- 14. LARGE ROW COUNTS ---")

# Generate 500 rows with 20 columns
def gen_large_data(n, offset=0):
    header = ['ID'] + [f'COL_{i:02d}' for i in range(1, 20)]
    rows = [header]
    for r in range(n):
        rows.append([str(r + offset)] + [f'val_{r}_{i}' for i in range(1, 20)])
    return rows

run_test("500 rows, 20 columns, all identical",
    gen_large_data(500),
    gen_large_data(500),
    {},
    key_columns=['ID'])

# 500 rows, last 5 rows only in source, first 5 of target different
def gen_src_500():
    header = ['ID'] + [f'COL_{i:02d}' for i in range(1, 6)]
    rows = [header]
    for r in range(500):
        rows.append([str(r)] + [f'v{r}_{i}' for i in range(1, 6)])
    return rows

def gen_tgt_500():
    header = ['ID'] + [f'COL_{i:02d}' for i in range(1, 6)]
    rows = [header]
    for r in range(495):
        if r < 3:
            rows.append([str(r)] + [f'CHANGED_{r}_{i}' for i in range(1, 6)])
        else:
            rows.append([str(r)] + [f'v{r}_{i}' for i in range(1, 6)])
    # 5 new target-only rows
    for r in range(500, 505):
        rows.append([str(r)] + [f'new_{r}_{i}' for i in range(1, 6)])
    return rows

run_test("500 rows: 3 mismatched, 5 missing-in-target, 5 missing-in-source",
    gen_src_500(), gen_tgt_500(),
    {'VALUE_MISMATCH': 15, 'MISSING_IN_TARGET': 5, 'MISSING_IN_SOURCE': 5},  # 3 rows × 5 cols = 15 value mismatches
    key_columns=['ID'])


# ============================================================
print("\n--- 15. WIDE TABLE (100+ columns) ---")

def gen_wide(n_cols, val_suffix=''):
    header = ['ID'] + [f'C{i:03d}' for i in range(1, n_cols)]
    row = ['WK1'] + [f'{i}{val_suffix}' for i in range(1, n_cols)]
    return [header, row]

run_test("150-column table, identical",
    gen_wide(150), gen_wide(150),
    {},
    key_columns=['ID'])

def gen_wide_diff(n_cols, change_col=75):
    header = ['ID'] + [f'C{i:03d}' for i in range(1, n_cols)]
    row = ['WK1'] + [f'{i}' if i != change_col else 'CHANGED' for i in range(1, n_cols)]
    return [header, row]

run_test("150-column table, 1 column different at col 75",
    gen_wide(150), gen_wide_diff(150, 75),
    {'VALUE_MISMATCH': 1},
    key_columns=['ID'])


# ============================================================
print("\n--- 16. NORMALISATION PARITY (SQL vs Python) ---")

# Verify SQL normalisation matches Python normalise_value() for tricky cases
print("  Checking SQL vs Python normalisation parity...")
parity_errors = 0

test_values = [
    ('NULL', None), ('null', None), ('None', None), ('none', None),
    ('', None), ('nan', None), ('NaN', None), ('N/A', None), ('na', None),
    ('NAT', None), ('#N/A', None),
    ('TRUE', 'true'), ('true', 'true'), ('Yes', 'true'), ('y', 'true'), ('1', 'true'),
    ('FALSE', 'false'), ('false', 'false'), ('No', 'false'), ('n', 'false'), ('0', 'false'),
    ('100.50', '100.5'), ('200.00', '200'), ('42', '42'), ('-7', '-7'),
    ('1,234', '1234'), ('1,234.56', '1234.56'),
    ('400.123456789', '400.123456'),
    ('2025-01-15T10:30:00', '2025-01-15 10:30:00'),
    ('2025-01-15 10:30:00.123456', '2025-01-15 10:30:00'),
    ('hello world', 'hello world'),
    ('+42', '42'),
    ('0.0', '0'), ('0.00', '0'),
    ('-0', '0'),
]

con = duckdb.connect()
for raw, expected_py in test_values:
    # Python result
    py_result = cmp.normalise_value(raw)

    # SQL result
    col_expr = "'val'"
    norm_sql = cmp._build_normalise_sql(col_expr)
    try:
        sql_result = con.execute(f"SELECT {norm_sql.replace(col_expr, repr(raw))}").fetchone()[0]
    except Exception as e:
        sql_result = f"SQL_ERROR: {e}"

    # Compare
    py_str = str(py_result).lower() if py_result is not None else None
    sql_str = str(sql_result).lower() if sql_result is not None else None

    if py_str != sql_str:
        parity_errors += 1
        if parity_errors <= 10:
            print(f"    PARITY MISMATCH: {raw!r:25s} → Python={py_str!r:15s}  SQL={sql_str!r}")

con.close()
if parity_errors == 0:
    PASS += 1
    print(f"  PASS  SQL vs Python parity ({len(test_values)} values)")
else:
    FAIL += 1
    print(f"  FAIL  SQL vs Python parity: {parity_errors}/{len(test_values)} mismatches")


# ============================================================
print("\n--- 17. REPORT GENERATION ---")

# Test CSV report output
src_rows = [['ID','VAL'], ['1','A'], ['2','B']]
tgt_rows = [['ID','VAL'], ['1','A'], ['2','C']]
src_file = write_csv(src_rows)
tgt_file = write_csv(tgt_rows)
try:
    con = duckdb.connect()
    sc, _ = cmp.load_csv_to_duckdb(con, src_file, 'source_raw', 'Src')
    tc, _ = cmp.load_csv_to_duckdb(con, tgt_file, 'target_raw', 'Tgt')
    comp = cmp.DuckDBComparator(con, 'source_raw', 'target_raw', sc, tc, key_columns=['ID'])
    discs = comp.compare()
    report_path = '/tmp/test_report'
    cmp.generate_report(discs, report_path, output_format='csv')
    csv_path = report_path + '.csv'
    if os.path.exists(csv_path):
        import pandas as pd
        df = pd.read_csv(csv_path)
        if len(df) == 1 and 'VALUE_MISMATCH' in df['discrepancy_type'].values:
            PASS += 1
            print("  PASS  CSV report generation")
        else:
            FAIL += 1
            print(f"  FAIL  CSV report: unexpected content, {len(df)} rows")
        os.unlink(csv_path)
        # Clean up column reference too
        ref = report_path + '_column_reference.txt'
        if os.path.exists(ref):
            os.unlink(ref)
    else:
        FAIL += 1
        print("  FAIL  CSV report: file not created")
    con.close()
except Exception as e:
    FAIL += 1
    print(f"  FAIL  CSV report: {e}")
finally:
    os.unlink(src_file)
    os.unlink(tgt_file)


# ============================================================
print("\n" + "=" * 70)
print(f"RESULTS: {PASS} passed, {FAIL} failed, {PASS+FAIL} total")
print("=" * 70)
sys.exit(1 if FAIL > 0 else 0)
