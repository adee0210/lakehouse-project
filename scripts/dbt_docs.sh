#!/usr/bin/env bash
# Sinh + phục vụ dbt docs (lineage graph 74 model, mô tả cột lấy từ sheet BA) tại http://localhost:8082
# Chạy: docker compose exec dbt bash /usr/app/scripts/dbt_docs.sh [ETL_DATE]
set -euo pipefail
ETL_DATE=${1:-2025-10-07}
cd /usr/app/dbt
dbt --no-use-colors docs generate --vars "{etl_date: '${ETL_DATE}', source_mode: ${SOURCE_MODE:-oracle}}"
pkill -f "dbt docs serve" 2>/dev/null || true
nohup dbt docs serve --port 8082 --host 0.0.0.0 --no-browser > /tmp/dbt_docs.log 2>&1 &
sleep 3
echo "dbt docs: http://localhost:8082"
