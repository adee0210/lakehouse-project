"""
Chuẩn bị nguồn để dbt (tầng Bronze) đọc qua source():

  --mode oracle : đăng ký 28 bảng Oracle dạng JDBC table trong spark_catalog.ora_<schema>.<table>.
                  Bronze đọc TRỰC TIẾP Oracle (Source -> PG_BRONZE 1:1), không qua staging trung gian.
                  - Bảng nguồn = PG_<SOURCE_SCHEMA>.<SOURCE_TABLE> theo sheet Bronze_Mapping_1_1 (bỏ DB link @PGDB/@TO_LOS),
                    override bảng sai tên bằng scripts/oracle_source_map.json (vd SMY.STMT_DAILY -> PG_SMY.STMT_ENTRY_LOAN_GRA).
                  - Lấy đúng danh sách cột DDL (392 cột). Cột DDL không tồn tại ở nguồn -> CAST(NULL AS <kiểu DDL>) + cảnh báo.
                  - NUMBER/FLOAT không khai báo scale -> đọc dạng text nguyên bản (tránh '1000385248.0000000000'
                    làm lệch khoá join); NUMBER(p,s) -> DECIMAL(p,s); DATE -> TIMESTAMP (Spark JDBC).
  --mode mock   : CSV /data/mock_source/<etl_date>/*.csv -> Iceberg lakehouse.src_<schema>.<table> (dev/test offline)

Usage: python load_source.py --etl-date 2025-10-07 [--mode oracle|mock] [--host spark-thrift]
"""

import argparse
import json
import os

from pyhive import hive

HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA = json.load(open(os.path.join(HERE, "source_schema.json"), encoding="utf-8"))
OVERRIDE = {
    k: v
    for k, v in json.load(
        open(os.path.join(HERE, "oracle_source_map.json"), encoding="utf-8")
    ).items()
    if not k.startswith("_")
}

ap = argparse.ArgumentParser()
ap.add_argument("--etl-date", required=True)
ap.add_argument(
    "--mode",
    default=os.environ.get("SOURCE_MODE", "oracle"),
    choices=["mock", "oracle"],
)
ap.add_argument("--host", default=os.environ.get("DBT_SPARK_HOST", "spark-thrift"))
ap.add_argument("--path", default="/data/mock_source")
args = ap.parse_args()

conn = hive.connect(host=args.host, port=10000, username="loader", auth="NOSASL")
cur = conn.cursor()


def run(sql):
    cur.execute(sql)


def q(c):
    return f"`{c}`"


def ora_null_type(c):
    t, ln = c["ora_type"].upper(), (c["length"] or "").strip()
    if t == "DATE":
        return "DATE"
    if t == "TIMESTAMP":
        return "TIMESTAMP"
    if t == "NUMBER":
        if "." in ln:
            p, s = ln.split(".")
            return f"NUMBER({int(p)},{int(s)})"
        return f"NUMBER({int(float(ln))})" if ln else "NUMBER"
    return "VARCHAR2(4000)"


if args.mode == "mock":
    for tbl, meta in SCHEMA.items():
        schema = meta["src_schema"].lower()
        name = meta["src_table"].split("@")[0].lower()
        run(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.src_{schema}")
        csv_path = f"{args.path}/{args.etl_date}/{tbl}.csv"
        run(
            f"CREATE OR REPLACE TEMPORARY VIEW v_src USING csv OPTIONS "
            f"(path '{csv_path}', header 'true', escape '\"', inferSchema 'false', encoding 'UTF-8')"
        )
        cols = ",\n  ".join(
            f"CAST(NULLIF({q(c['name'])}, '') AS {c['type']}) AS {q(c['name'])}"
            for c in meta["columns"]
        )
        run(
            f"CREATE OR REPLACE TABLE lakehouse.src_{schema}.{name} USING iceberg "
            f"LOCATION 's3://source/{schema}/{name}' AS SELECT\n  {cols}\nFROM v_src"
        )
        cur.execute(f"SELECT count(*) FROM lakehouse.src_{schema}.{name}")
        print(
            f"[mock] src_{schema}.{name:40s} {cur.fetchone()[0]:6d} rows ({args.etl_date})"
        )
    conn.close()
    raise SystemExit(0)

# ------------------------------------------------------------------------------------------ oracle
host, port, svc = (
    os.environ["ORACLE_HOST"],
    os.environ.get("ORACLE_PORT", "1521"),
    os.environ["ORACLE_SERVICE"],
)
user, pwd = os.environ["ORACLE_USER"], os.environ["ORACLE_PASSWORD"]
url = f"jdbc:oracle:thin:@//{host}:{port}/{svc}"
jdbc_opts = f"url '{url}', driver 'oracle.jdbc.OracleDriver', user '{user}', password '{pwd}', fetchsize '10000'"

# Metadata cột thực tế trên Oracle (qua chính Spark JDBC – không cần client Oracle trong container dbt)
run(
    f"CREATE OR REPLACE TEMPORARY VIEW ora_cols USING org.apache.spark.sql.jdbc OPTIONS ({jdbc_opts}, "
    f"dbtable '(SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE "
    f"FROM ALL_TAB_COLUMNS WHERE OWNER LIKE \\'PG%\\')')"
)
cur.execute(
    "SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE FROM ora_cols"
)
ora = {}
for o, t, c, dt, p, s in cur.fetchall():
    ora.setdefault(f"{o}.{t}", {})[c] = (dt, p, s)

report = []
for tbl, meta in SCHEMA.items():
    schema = meta["src_schema"].lower()
    name = meta["src_table"].split("@")[0].lower()
    ora_obj = OVERRIDE.get(
        tbl,
        f"PG_{meta['src_schema'].upper()}.{meta['src_table'].split('@')[0].upper()}",
    )
    actual = ora.get(ora_obj)
    if not actual:
        raise SystemExit(
            f"[oracle] KHÔNG tìm thấy bảng nguồn {ora_obj} cho {tbl} – kiểm tra scripts/oracle_source_map.json"
        )
    sel, custom, missing = [], [], []
    for c in meta["columns"]:
        cn = c["name"].upper()
        if cn not in actual:
            missing.append(cn)
            sel.append(f'CAST(NULL AS {ora_null_type(c)}) AS "{cn}"')
            continue
        sel.append(f'"{cn}"')
        dt, p, s = actual[cn]
        if dt in ("NUMBER", "FLOAT") and s is None:
            custom.append(f"{cn} STRING")  # giữ nguyên giá trị dạng text
        elif dt == "NUMBER" and p is not None:
            custom.append(f"{cn} DECIMAL({min(int(p), 38)},{int(s or 0)})")
    sub = f"(SELECT {', '.join(sel)} FROM {ora_obj})".replace(
        "'", "\\'"
    )  # escape cho literal Spark SQL
    opts = f"{jdbc_opts}, dbtable '{sub}'" + (
        f", customSchema '{', '.join(custom)}'" if custom else ""
    )
    run(f"CREATE DATABASE IF NOT EXISTS spark_catalog.ora_{schema}")
    run(f"DROP TABLE IF EXISTS spark_catalog.ora_{schema}.{name}")
    run(
        f"CREATE TABLE spark_catalog.ora_{schema}.{name} USING org.apache.spark.sql.jdbc OPTIONS ({opts})"
    )
    report.append({"bronze": tbl, "oracle": ora_obj, "missing_cols_as_null": missing})
    print(
        f"[oracle] spark_catalog.ora_{schema}.{name:34s} <- {ora_obj:40s}"
        + (
            f" | thiếu {len(missing)} cột -> NULL: {','.join(missing[:6])}{'...' if len(missing) > 6 else ''}"
            if missing
            else ""
        )
    )

with open("/tmp/oracle_source_report.json", "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=1)
conn.close()
