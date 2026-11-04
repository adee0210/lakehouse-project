FROM apache/airflow:2.10.4-python3.12
USER airflow
RUN pip install --no-cache-dir "dbt-core==1.9.*" "dbt-spark[PyHive]==1.9.0" "openpyxl==3.1.5" "oracledb==2.4.1"
