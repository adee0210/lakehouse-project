"""Thống kê số dòng các bảng 3 tầng tại ${ETL_DATE}: python layer_stats.py 2025-10-07"""
import os, sys
from pyhive import hive
d = sys.argv[1]
cur = hive.connect(host=os.environ.get("DBT_SPARK_HOST", "spark-thrift"), port=10000, username="stats", auth="NOSASL").cursor()
PART = {"ETL_DATE", "CDR_DT", "SNPST_DT", "PST_DT", "CDR_DT_DIM_KEY"}
print("LAYER|TABLE|COLS|ROWS|CURRENT(SCD2)")
for ns in ["pg_bronze", "pg_silver", "pg_gold"]:
    cur.execute(f"show tables in lakehouse.{ns}")
    for _, t, *_ in sorted(cur.fetchall(), key=lambda r: r[1]):
        cur.execute(f"describe lakehouse.{ns}.{t}")
        cols = [r[0] for r in cur.fetchall() if r[0] and not r[0].startswith("#")]
        p = next((c for c in cols if c.upper() in PART), None)
        scd = "SYS_EXP_DT" in [c.upper() for c in cols]
        q = f"select count(*) {', sum(case when SYS_EXP_DT is null then 1 else 0 end)' if scd else ''} from lakehouse.{ns}.{t}"
        if p and not scd:
            q += f" where {p} = DATE'{d}'"
        cur.execute(q); r = cur.fetchone()
        print(f"{ns}|{t}|{len(cols)}|{r[0]}|{r[1] if scd else ''}")
