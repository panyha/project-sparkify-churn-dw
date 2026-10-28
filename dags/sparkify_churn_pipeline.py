"""
Sparkify Streaming Behaviour & Churn — end-to-end pipeline

    check_raw_files ──► load_raw_events ──► dbt_seed ──► dbt_build_staging ──► dbt_build_marts ──► dbt_build_kpi ──► dbt_docs
         (E)                (E+L)             (lookup)         (T: clean)           (T: star schema)     (T: KPI layer)

  E/L : Python + pandas reads the JSON event logs from raw_data/ into raw.event_logs
  T   : dbt turns raw rows into staging views → star schema (dims + fct) → KPI tables
  `dbt build` = run + test, so a failed data test stops the downstream layer.
"""
import sys

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow/scripts")  # scripts/ is mounted here (see docker-compose)

DBT_DIR = "/opt/airflow/dbt_projects/sparkify_dw"
DBT = f"cd {DBT_DIR} && dbt {{cmd}} --profiles-dir /opt/airflow/dbt_root --target dev"


@dag(
    dag_id="sparkify_churn_pipeline",
    description="Load Sparkify event logs and build the streaming/churn star schema with dbt",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["project", "sparkify", "dbt", "churn"],
)
def sparkify_churn_pipeline():

    @task
    def check_raw_files() -> list[str]:
        from load_raw_events import find_source_files

        files = find_source_files()
        print("Found:", [f.name for f in files])
        return [str(f) for f in files]

    @task
    def load_raw_events(_files: list[str]) -> int:
        from load_raw_events import load

        rows = load(host="postgres", port=5432, dbname="sparkify_dw")
        if rows == 0:
            raise ValueError("Nothing was loaded into raw.event_logs")
        return rows

    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=DBT.format(cmd="seed"))
    dbt_staging = BashOperator(task_id="dbt_build_staging", bash_command=DBT.format(cmd="build --select staging"))
    dbt_marts = BashOperator(task_id="dbt_build_marts", bash_command=DBT.format(cmd="build --select marts"))
    dbt_kpi = BashOperator(task_id="dbt_build_kpi", bash_command=DBT.format(cmd="build --select kpi"))
    dbt_docs = BashOperator(task_id="dbt_docs_generate", bash_command=DBT.format(cmd="docs generate"))

    load_raw_events(check_raw_files()) >> dbt_seed >> dbt_staging >> dbt_marts >> dbt_kpi >> dbt_docs


sparkify_churn_pipeline()
