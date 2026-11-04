#!/bin/sh
# Chạy toàn bộ pipeline 1 ngày theo đúng thứ tự Wave của sheet Job_dependence.
# Chạy trong container dbt:  docker compose exec dbt sh /usr/app/scripts/run_pipeline.sh 2025-11-26 [mock|oracle]
set -e
ETL_DATE=${1:?Usage: run_pipeline.sh <YYYY-MM-DD> [mock|oracle]}
MODE=${2:-${SOURCE_MODE:-oracle}}
VARS="{etl_date: ${ETL_DATE}, source_mode: ${MODE}}"
cd /usr/app/dbt

echo "=== [Source] chuẩn bị nguồn (${MODE}) cho ${ETL_DATE}"
if [ "$MODE" = "mock" ]; then
  [ -d /data/mock_source/${ETL_DATE} ] || python /usr/app/scripts/generate_mock_source.py --etl-date ${ETL_DATE}
fi
python /usr/app/scripts/load_source.py --etl-date ${ETL_DATE} --mode ${MODE}

for WAVE in wave0 wave1 wave2 wave3 wave4; do
  echo "=== [${WAVE}] dbt run"
  dbt run -s tag:${WAVE} --vars "${VARS}"
done

echo "=== [DQ] dbt test (PK, SCD2, fn_hash match, column contract, đối soát)"
dbt test --vars "${VARS}"
