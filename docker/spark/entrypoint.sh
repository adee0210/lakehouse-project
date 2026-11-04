#!/bin/bash
# Khởi động Spark Thrift Server (local mode) làm SQL endpoint cho dbt-spark.
set -e
CONF=/opt/spark/conf/spark-defaults.conf
TMP=$(mktemp)
sed -e "s|__PG_USER__|${POSTGRES_USER}|" \
    -e "s|__PG_PASSWORD__|${POSTGRES_PASSWORD}|" \
    -e "s|__S3_KEY__|${MINIO_ROOT_USER}|" \
    -e "s|__S3_SECRET__|${MINIO_ROOT_PASSWORD}|" "$CONF" > "$TMP"
cp "$TMP" /tmp/spark-defaults.conf
export SPARK_CONF_DIR=/tmp/spark-conf
mkdir -p $SPARK_CONF_DIR && cp /tmp/spark-defaults.conf $SPARK_CONF_DIR/

# Bootstrap: Thrift session luôn "USE default" -> namespace lakehouse.default phải tồn tại.
# Catalog lưu trên Postgres nên chỉ cần tạo 1 lần (lệnh idempotent).
/opt/spark/bin/spark-sql --master local[1] --driver-memory 512m \
  -e "CREATE NAMESPACE IF NOT EXISTS lakehouse.default; CREATE NAMESPACE IF NOT EXISTS lakehouse.pg_bronze; CREATE NAMESPACE IF NOT EXISTS lakehouse.pg_silver; CREATE NAMESPACE IF NOT EXISTS lakehouse.pg_silver_stg; CREATE NAMESPACE IF NOT EXISTS lakehouse.pg_gold" 2>&1 | grep -v -E "WARN|INFO" || true

export SPARK_NO_DAEMONIZE=true
exec /opt/spark/sbin/start-thriftserver.sh \
  --master "local[${SPARK_CORES:-4}]" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1600m}" \
  --hiveconf hive.server2.thrift.port=10000 \
  --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
  --hiveconf hive.server2.authentication=NOSASL
