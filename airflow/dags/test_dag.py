from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["test"],
):

    task_1 = BashOperator(
        task_id="hello",
        bash_command="echo 'Hello Airflow'",
    )

    task_2 = BashOperator(
        task_id="show_date",
        bash_command="date",
    )

    task_1 >> task_2