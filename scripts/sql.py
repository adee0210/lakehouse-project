"""Chạy nhanh câu SQL qua Spark Thrift: python sql.py "select ..." """
import os, sys
from pyhive import hive
cur = hive.connect(host=os.environ.get("DBT_SPARK_HOST", "spark-thrift"), port=10000, username="cli", auth="NOSASL").cursor()
for stmt in [s for s in sys.argv[1].split(";") if s.strip()]:
    cur.execute(stmt)
    try:
        rows = cur.fetchall()
    except Exception:
        rows = []
    cols = [d[0] for d in cur.description] if cur.description else []
    if cols:
        print(" | ".join(cols))
    for r in rows:
        print(" | ".join("" if v is None else str(v) for v in r))
