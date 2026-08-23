from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator      # Airflow 3 import

DBT = "/opt/airflow/dbt_venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/zomato"

with DAG(
    dag_id="zomato_batch",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["zomato", "dbt", "databricks"],
    doc_md=__doc__,
) as dag:


    dbt_build_core = BashOperator(
        task_id="dbt_build_core",
        bash_command=f"{DBT} build --exclude tag:ai --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT}",
    )

    enrich_reviews = BashOperator(
        task_id="enrich_reviews",
        bash_command="python /opt/airflow/project/zomato/ai/enrich_reviews.py",
        append_env=True,
        env={
            "OPENAI_API_KEY": "{{ var.value.get('OPENAI_API_KEY', '') }}",
            "DATABRICKS_SERVER_HOSTNAME": "{{ var.value.get('DATABRICKS_SERVER_HOSTNAME', '') }}",
            "DATABRICKS_HTTP_PATH": "{{ var.value.get('DATABRICKS_HTTP_PATH', '') }}",
            "DATABRICKS_TOKEN": "{{ var.value.get('DATABRICKS_TOKEN', '') }}",
        },
    )

    dbt_build_ai = BashOperator(
        task_id = "dbt_build_ai",
        bash_command=f"{DBT} build --select tag:ai --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT}"
    )

    dbt_build_core >> enrich_reviews >> dbt_build_ai