from datetime import datetime

from airflow.sdk import DAG
from airflow.operators.python import PythonOperator


def check():
    import cosmos
    print(cosmos.__version__)


with DAG(
    dag_id="cosmos_simple_check",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
):
    PythonOperator(
        task_id="check_cosmos",
        python_callable=check,
    )