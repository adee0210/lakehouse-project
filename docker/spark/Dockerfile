# Spark 3.5.3 + Apache Iceberg 1.6.1 (S3FileIO -> MinIO, JDBC catalog -> Postgres)
# + Oracle JDBC driver (đọc nguồn Oracle cho tầng Bronze)
FROM apache/spark:3.5.3

USER root
ARG ICEBERG_VERSION=1.6.1
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; cd /opt/spark/jars; \
    curl -fsSLO ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar; \
    curl -fsSLO ${MAVEN}/com/oracle/database/jdbc/ojdbc8/23.5.0.24.07/ojdbc8-23.5.0.24.07.jar; \
    mkdir -p /tmp/spark-events /opt/spark/work-dir && chown -R spark:spark /tmp/spark-events /opt/spark/work-dir

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY entrypoint.sh /opt/entrypoint-thrift.sh
RUN chmod +x /opt/entrypoint-thrift.sh

USER spark
ENTRYPOINT ["/opt/entrypoint-thrift.sh"]
