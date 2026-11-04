"""
DAG điều phối 69 job ETL theo sheet `Job_dependence` (docs/Training_Fresher_DeepChecked.xlsx).

- jobs.json được sinh tự động bởi tools/gen_from_excel.py (OBJECT_NAME, GRP_OBJ, WAITING, SCHEDULE, SQL_EXC, PIC).
- Mỗi job = 1 task `dbt run -s <model>`; task group theo GRP_OBJ (WAVE_0 .. WAVE_4).
- Phụ thuộc task-level đúng cột WAITING: bảng PG_* trong WAITING -> upstream task;
  bảng nguồn (T24CORE.*, FLEXBO.*, ...) -> task `prepare_source`.
- Các bảng cùng Wave chạy song song (giới hạn bởi pool `spark_thrift`), khác Wave chạy tuần tự theo dependency.
- Lịch: 00:30 hằng ngày (= lịch Wave 0). Mốc giờ của các Wave sau (01:00, 01:30, 01:45, 02:00, 02:15-02:45)
  trong sheet được thay bằng dependency thực tế -> job chạy ngay khi upstream xong, không phải chờ cron.
- ${ETL_DATE} = data_interval_start (ngày nghiệp vụ T-1 so với thời điểm chạy), override bằng conf {"etl_date": "YYYY-MM-DD"}.
- Kết thúc bằng `dbt test` (PK, SCD2 single-current, fn_hash match rate, column contract, đối soát dư nợ).
"""
import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS = json.load(open(os.path.join(DAG_DIR, "jobs.json"), encoding="utf-8"))

DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
SCRIPTS = "/opt/airflow/scripts"
ETL_DATE = "{{ dag_run.conf.get('etl_date') or ds }}"
SOURCE_MODE = os.environ.get("SOURCE_MODE", "mock")
DBT_VARS = "{etl_date: '" + ETL_DATE + "', source_mode: " + SOURCE_MODE + "}"
DBT = ("cd " + DBT_DIR + " && dbt --no-use-colors __CMD__ --vars \"" + DBT_VARS + "\""
       " --target-path /tmp/dbt_target_{{ ti.task_id }} --log-path /tmp/dbt_logs_{{ ti.task_id }}")

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "pool": "default_pool",
}

# Model có bảng staging (pg_silver_stg.stg_<model>) – chạy kèm trong cùng task (staging trước, MERGE SCD2 sau)
STG_MODELS = {os.path.basename(f)[4:-4] for root, _, fs in os.walk(os.path.join(DBT_DIR, "models"))
              for f in fs if f.startswith("stg_") and f.endswith(".sql")}


def dbt_select(model):
    return f"stg_{model} {model}" if model in STG_MODELS else model


WAVE_ORDER = ["WAVE_0_BRONZE_1_1", "WAVE_1_SILVER_TEMP1", "WAVE_2_SILVER_TEMP2", "WAVE_3_GOLD_DIM",
              "WAVE_4_GOLD_INTERFACE", "WAVE_4_GOLD_FACT"]

with DAG(
    dag_id="dag_lakehouse_daily",
    description="Medallion Bronze -> Silver (Temp1/Temp2) -> Gold (DIM/INTF/FCT) – 69 job theo Job_dependence",
    schedule="30 0 * * *",
    start_date=datetime(2025, 11, 26),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args=default_args,
    tags=["lakehouse", "dbt-spark", "medallion"],
    doc_md=__doc__,
) as dag:

    prepare_source = BashOperator(
        task_id="prepare_source",
        bash_command=(
            f"if [ '{SOURCE_MODE}' = 'mock' ]; then "
            f"python {SCRIPTS}/generate_mock_source.py --etl-date {ETL_DATE} --out /data/mock_source; fi && "
            f"python {SCRIPTS}/load_source.py --etl-date {ETL_DATE} --mode {SOURCE_MODE}"
        ),
        doc_md="Mock: sinh + nạp CSV vào Iceberg src_*. Oracle: đăng ký 28 bảng JDBC (spark_catalog.ora_*).",
    )

    groups, tasks = {}, {}
    for wave in WAVE_ORDER:
        groups[wave] = TaskGroup(group_id=wave, dag=dag)

    for job in JOBS:
        with groups[job["group"]]:
            tasks[job["object"].upper()] = BashOperator(
                task_id=job["model"],
                bash_command=DBT.replace("__CMD__", f"run -s {dbt_select(job['model'])}"),
                doc_md=(f"**{job['object']}** | {job['group']} | PIC: {job['pic']}\n\n"
                        f"Schedule (sheet): `{job['schedule']}` | SQL: `{job['sql']}`\n\n"
                        f"WAITING: {', '.join(job['waiting'])}"),
            )

    for job in JOBS:
        t = tasks[job["object"].upper()]
        upstream = [w.upper() for w in job["waiting"]]
        internal = [tasks[u] for u in upstream if u in tasks]
        if internal:
            internal >> t
        if any(u not in tasks for u in upstream):  # bảng nguồn Oracle/mock
            prepare_source >> t

    dq = BashOperator(task_id="dbt_test_data_quality", bash_command=DBT.replace("__CMD__", "test"), trigger_rule="all_done")
    end = EmptyOperator(task_id="end")
    [tasks[j["object"].upper()] for j in JOBS] >> dq >> end
