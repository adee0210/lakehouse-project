"""Xoá (PURGE) toàn bộ bảng các tầng để chạy lại từ đầu: python reset_lakehouse.py [pg_bronze pg_silver pg_gold ...]"""
import os, sys
from pyhive import hive
cur = hive.connect(host=os.environ.get("DBT_SPARK_HOST", "spark-thrift"), port=10000, username="reset", auth="NOSASL").cursor()
for ns in (sys.argv[1:] or ["pg_bronze", "pg_silver", "pg_silver_stg", "pg_gold"]):
    cur.execute(f"SHOW NAMESPACES IN lakehouse")
    if ns not in [r[0] for r in cur.fetchall()]:
        continue
    cur.execute(f"SHOW TABLES IN lakehouse.{ns}")
    for _, t, *_ in cur.fetchall():
        cur.execute(f"DROP TABLE IF EXISTS lakehouse.{ns}.{t} PURGE")
        print("dropped", ns, t)
