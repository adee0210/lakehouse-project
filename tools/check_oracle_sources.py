"""
Đối chiếu 28 bảng nguồn / 392 cột trong sheet Bronze_Mapping_1_1 với Oracle thật.
Quy ước: schema Oracle = 'PG_' + Source Schema trong sheet (vd T24CORE -> PG_T24CORE).
Usage: python tools/check_oracle_sources.py   (đọc ORACLE_* từ .env)
Ghi kết quả ra tools/oracle_source_check.json
"""

import json
import os
import sys

import oracledb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")
env = dict(
    l.strip().split("=", 1)
    for l in open(os.path.join(ROOT, ".env"), encoding="utf-8")
    if "=" in l and not l.strip().startswith("#")
)
conn = oracledb.connect(
    user=env["ORACLE_USER"],
    password=env["ORACLE_PASSWORD"],
    dsn=f"{env['ORACLE_HOST']}:{env.get('ORACLE_PORT', '1521')}/{env['ORACLE_SERVICE']}",
)
cur = conn.cursor()
schema = json.load(
    open(os.path.join(ROOT, "scripts", "source_schema.json"), encoding="utf-8")
)
OVERRIDE = {
    k: v
    for k, v in json.load(
        open(os.path.join(ROOT, "scripts", "oracle_source_map.json"), encoding="utf-8")
    ).items()
    if not k.startswith("_")
}

mode = sys.argv[1] if len(sys.argv) > 1 else "check"
if mode == "owners":
    cur.execute(
        "select owner, count(*) from all_tables where owner like 'PG%' group by owner order by 1"
    )
    for r in cur:
        print(r)
    cur.execute(
        "select owner, object_name, object_type from all_objects where owner like 'PG%' "
        "and object_type in ('TABLE','VIEW','SYNONYM') order by 1,2"
    )
    for r in cur:
        print(r)
    sys.exit()

result = {}
for bronze, meta in schema.items():
    owner, tbl = OVERRIDE.get(
        bronze,
        "PG_"
        + meta["src_schema"].upper()
        + "."
        + meta["src_table"].split("@")[0].upper(),
    ).split(".")
    cur.execute(
        "select column_name, data_type, data_length, data_precision, data_scale "
        "from all_tab_columns where owner = :o and table_name = :t order by column_id",
        o=owner,
        t=tbl,
    )
    cols = {r[0]: r[1:] for r in cur}
    expected = [c["name"].upper() for c in meta["columns"]]
    rows = None
    if cols:
        cur.execute(f'select count(*) from "{owner}"."{tbl}"')
        rows = cur.fetchone()[0]
    missing = [c for c in expected if c not in cols]
    result[bronze] = {
        "owner": owner,
        "table": tbl,
        "exists": bool(cols),
        "rows": rows,
        "missing_cols": missing,
        "extra_cols": [c for c in cols if c not in expected],
        "types": {c: cols[c] for c in expected if c in cols},
    }
    flag = (
        "OK "
        if cols and not missing
        else ("MISSING TABLE" if not cols else "COL MISMATCH")
    )
    print(f"{flag:14s} {owner}.{tbl:35s} rows={rows} missing={missing}")
json.dump(
    result,
    open(
        os.path.join(ROOT, "tools", "oracle_source_check.json"), "w", encoding="utf-8"
    ),
    ensure_ascii=False,
    indent=1,
    default=str,
)
