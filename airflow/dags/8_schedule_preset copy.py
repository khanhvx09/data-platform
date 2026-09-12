from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta


@dag(
    dag_id = "8_schedule_preset_dag",
    start_date = datetime(2023, 1, 1),
    schedule = "@daily",
    catchup = False,
    is_paused_upon_creation = False,
)
def first_schedule_preset_dag():

    @task.python
    def extract_task(**kwargs):
        print("Extracting data...")
        ti = kwargs['ti']
        extracted_data_dict = {
            "api_extracted_data": [1, 2, 3, 4, 5],
            "db_extracted_data": [6, 7, 8, 9, 10],
            "s3_extracted_data": [11, 12, 13, 14, 15],
            "is_weekend": False
        }

        ti.xcom_push(key='return_value', value=extracted_data_dict)


    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(task_ids = 'extract_task',key='return_value')['api_extracted_data']
        print(f"Transforming API data: {fetched_data}")
        transformed_data = [x * 10 for x in fetched_data]
        ti.xcom_push(key='return_value', value=transformed_data)

    @task.python
    def transform_task_db(**kwargs):
        print("This is the third task")
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids = 'extract_task',key='return_value')['db_extracted_data']
        print(f"Transforming DB data: {data}")
        transformed_data = [x * 100 for x in data]
        ti.xcom_push(key='return_value', value=transformed_data)

    @task.python
    def transform_task_s3(**kwargs):
        print("This is the third task")
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids = 'extract_task',key='return_value')['s3_extracted_data']
        print(f"Transforming S3 data: {data}")
        transformed_data = [x * 1000 for x in data]
        ti.xcom_push(key='return_value', value=transformed_data)


    @task.python
    def no_load_task(**kwargs):
        print("It's a weekday, no load task.")


    @task.python
    def load_task(**kwargs):
        print("This is the fourth task")
        ti = kwargs['ti']
        api_data = ti.xcom_pull(task_ids = 'transform_task_api',key='return_value')
        db_data = ti.xcom_pull(task_ids = 'transform_task_db',key='return_value')
        s3_data = ti.xcom_pull(task_ids = 'transform_task_s3',key='return_value')

        print(f"Loading transformed data: API: {api_data}, DB: {db_data}, S3: {s3_data}")

    @task.branch
    def decide_load_or_no_load(**kwargs):
        ti = kwargs['ti']
        is_weekend = ti.xcom_pull(task_ids = 'extract_task',key='return_value')['is_weekend']

        if is_weekend:
            return 'no_load_task'
        else:
            return 'load_task'

    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    no_load = no_load_task()
    load = load_task()

    decide = decide_load_or_no_load()

    
    extract >> [transform_api, transform_db, transform_s3] >> decide >> [no_load, load]

first_schedule_preset_dag()
    

    