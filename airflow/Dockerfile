
FROM apache/airflow:2.10.5-python3.11

USER airflow

RUN pip install --no-cache-dir \
    "dbt-core==1.9.2" \
    "dbt-spark[PyHive]==1.9.0"
