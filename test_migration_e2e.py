#!/usr/bin/env python3
"""
END-TO-END MIGRATION SIMULATION
Simulates a realistic Hive export vs Snowflake export of the same table:
  - 5,000 rows, 18 columns (IDs, codes, dates, timestamps, decimals, booleans, text)
  - Row order COMPLETELY shuffled between the two files
  - Every value formatted differently per engine (Hive vs Snowflake conventions)
  - Exactly-known injected discrepancies
PASS = the comparator reports EXACTLY the injected discrepancies: zero false
positives from formatting, zero missed real differences.
"""
import csv_comparator_duckdb as cmp
import duckdb, random, os, tempfile

random.seed(7)
N = 5000

# ---------------------------------------------------------------- build truth
rows = []
for i in range(N):
    rows.append({
        'SEC_ID': f'SEC{i:06d}',
        'AS_OF_DATE': f'2025-0{random.randint(1,9)}-{random.randint(1,28):02d}',
        'SERVICE_CD': random.choice(['SVC_A','SVC_B','SVC_C','SVC_D']),
        'ACCOUNT_NUM': str(1000000 + i),
        'PRICE': round(random.uniform(0.0001, 99999), random.randint(0, 8)),
        'QUANTITY': random.randint(-50000, 50000),
        'RATIO': random.uniform(1e-7, 1e-2),
        'BIG_AMT': random.randint(10**9, 10**12),
        'IS_ACTIVE': random.choice([True, False]),
        'TRADE_TS': f'2025-06-{random.randint(1,28):02d} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}',
        'TS_FRAC': random.randint(0, 999999),
        'MAYBE_NULL': random.random() < 0.3,
        'NOTE': random.choice(['ok', 'pending review', 'priority', 'standard', 'escalated']),
        'CCY': random.choice(['USD','EUR','GBP','JPY']),
    })

HDR = ['SEC_ID','AS_OF_DATE','SERVICE_CD','ACCOUNT_NUM','PRICE','QUANTITY',
       'RATIO','BIG_AMT','IS_ACTIVE','TRADE_TS','MAYBE_NULL_COL','NOTE','CCY']

def hive_row(r):
    """Render with Hive export conventions."""
    return [
        r['SEC_ID'],
        r['AS_OF_DATE'],
        r['SERVICE_CD'],
        r['ACCOUNT_NUM'],
        f"{r['PRICE']:.10f}".rstrip('0').rstrip('.') if r['PRICE'] != int(r['PRICE']) else f"{r['PRICE']:.1f}",
        str(r['QUANTITY']),
        f"{r['RATIO']:.6E}",                                     # Hive: scientific notation
        str(r['BIG_AMT']),
        'true' if r['IS_ACTIVE'] else 'false',                   # Hive: lowercase bool
        f"{r['TRADE_TS']}.{r['TS_FRAC']:06d}",                   # Hive: fractional seconds
        r'\N' if r['MAYBE_NULL'] else 'present',                 # Hive: \N null marker
        r['NOTE'],
        r['CCY'],
    ]

def snow_row(r):
    """Render with Snowflake export conventions."""
    return [
        r['SEC_ID'],
        r['AS_OF_DATE'],
        r['SERVICE_CD'],
        r['ACCOUNT_NUM'],
        f"{r['PRICE']:.10f}",                                    # Snowflake: padded decimals
        f"{r['QUANTITY']}.00" if random.random() < .5 else str(r['QUANTITY']),  # sometimes x.00
        f"{r['RATIO']:.10f}",                                    # Snowflake: expanded positional
        f"{r['BIG_AMT']}",
        'TRUE' if r['IS_ACTIVE'] else 'FALSE',                   # Snowflake: uppercase bool
        f"{r['TRADE_TS'].replace(' ', 'T')}Z",                   # Snowflake: T-sep + Zulu, no fraction
        'NULL' if r['MAYBE_NULL'] else 'present',                # Snowflake: NULL literal
        r['NOTE'],
        r['CCY'],
    ]

# Precision note: Hive RATIO sci has 7 sig figs (x.xxxxxxE±n), Snowflake .10f
# expands the same double — both truncate identically at 6dp normalisation.

# -------------------------------------------------------- inject discrepancies
# 1) 7 rows with a changed PRICE (VALUE_MISMATCH x7)
changed_price_ids = set(f'SEC{i:06d}' for i in random.sample(range(N), 7))
# 2) 5 rows only in Hive (MISSING_IN_TARGET x5)
hive_only_ids = set(f'SEC{i:06d}' for i in random.sample(range(N), 5) ) - changed_price_ids
while len(hive_only_ids) < 5:
    hive_only_ids.add(f'SEC{random.randint(0,N-1):06d}')
    hive_only_ids -= changed_price_ids
# 3) 4 brand-new rows only in Snowflake (MISSING_IN_SOURCE x4)
snow_only = []
for j in range(4):
    snow_only.append({
        'SEC_ID': f'SECNEW{j:04d}', 'AS_OF_DATE': '2025-07-01', 'SERVICE_CD': 'SVC_X',
        'ACCOUNT_NUM': str(9000000+j), 'PRICE': 1.5, 'QUANTITY': 10, 'RATIO': 0.001,
        'BIG_AMT': 12345, 'IS_ACTIVE': True, 'TRADE_TS': '2025-07-01 09:00:00',
        'TS_FRAC': 0, 'MAYBE_NULL': False, 'NOTE': 'new in snowflake', 'CCY': 'USD',
    })
# 4) 3 rows with NOTE changed (VALUE_MISMATCH x3)
changed_note_ids = set()
pool = [f'SEC{i:06d}' for i in range(N)]
while len(changed_note_ids) < 3:
    cand = random.choice(pool)
    if cand not in changed_price_ids and cand not in hive_only_ids:
        changed_note_ids.add(cand)

hive_rows, snow_rows = [], []
for r in rows:
    sid = r['SEC_ID']
    hive_rows.append(hive_row(r))
    if sid in hive_only_ids:
        continue  # absent from Snowflake
    r2 = dict(r)
    if sid in changed_price_ids:
        r2['PRICE'] = r['PRICE'] + 1.11
    if sid in changed_note_ids:
        r2['NOTE'] = 'CHANGED_' + r['NOTE']
    snow_rows.append(snow_row(r2))
for r in snow_only:
    snow_rows.append(snow_row(r))

random.shuffle(hive_rows)   # completely different row order
random.shuffle(snow_rows)

hf = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, dir='/tmp')
hf.write('|'.join(HDR) + '\n')
for r in hive_rows: hf.write('|'.join(r) + '\n')
hf.close()
sf = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, dir='/tmp')
sf.write('\ufeff' + '|'.join(HDR) + '\r\n')                      # BOM + CRLF (Windows export tool)
for r in snow_rows: sf.write('|'.join(r) + '\r\n')
sf.close()

print(f"Hive file:      {len(hive_rows):,} rows ({os.path.getsize(hf.name):,} bytes)")
print(f"Snowflake file: {len(snow_rows):,} rows ({os.path.getsize(sf.name):,} bytes, BOM+CRLF)")
print(f"Injected: 7 PRICE mismatches, 3 NOTE mismatches, 5 Hive-only, 4 Snowflake-only\n")

con = duckdb.connect()
sc, _ = cmp.load_csv_to_duckdb(con, hf.name, 'source_raw', 'Hive')
tc, _ = cmp.load_csv_to_duckdb(con, sf.name, 'target_raw', 'Snowflake')
comp = cmp.DuckDBComparator(con, 'source_raw', 'target_raw', sc, tc,
                            key_columns=['SEC_ID','AS_OF_DATE'])
discs = comp.compare()
con.close()
os.unlink(hf.name); os.unlink(sf.name)

by_type = {}
for d in discs:
    by_type.setdefault(d['discrepancy_type'], []).append(d)

print("\n" + "=" * 70)
print("VERIFICATION")
print("=" * 70)
expected = {'VALUE_MISMATCH': 10, 'MISSING_IN_TARGET': 5, 'MISSING_IN_SOURCE': 4}
all_ok = True
for t, exp in expected.items():
    got = len(by_type.get(t, []))
    ok = got == exp
    all_ok &= ok
    print(f"  {'OK  ' if ok else 'FAIL'} {t:25s} expected {exp}, got {got}")
unexpected = {t: len(v) for t, v in by_type.items() if t not in expected}
if unexpected:
    all_ok = False
    print(f"  FAIL unexpected types (false positives from formatting!): {unexpected}")
    for t, ds in by_type.items():
        if t not in expected:
            for d in ds[:5]:
                print(f"       {t} | {d['column_name']} | src={str(d['source_value'])[:40]!r} tgt={str(d['target_value'])[:40]!r}")
else:
    print("  OK   zero false positives from format differences")

# verify mismatch columns are exactly PRICE x7 and NOTE x3
if 'VALUE_MISMATCH' in by_type:
    cols = sorted(d['column_name'] for d in by_type['VALUE_MISMATCH'])
    pc, nc = cols.count('PRICE'), cols.count('NOTE')
    ok = pc == 7 and nc == 3 and len(cols) == 10
    all_ok &= ok
    print(f"  {'OK  ' if ok else 'FAIL'} mismatch columns: PRICE x{pc}, NOTE x{nc}")

print("\n" + ("ALL CHECKS PASSED — migration comparison is exact" if all_ok else "CHECKS FAILED"))
import sys; sys.exit(0 if all_ok else 1)
