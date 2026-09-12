from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator



@dag(
    dag_id = "4_xcom_auto_dag",    
)
def xcom_auto_dag():

    @task.python
    def first_task():
        print("This is the first task")
        fetched_data = {"data":[1,2,3,4,5]}

        return fetched_data


    @task.python
    def second_task(data: dict):
        print("This is the second task")
        fetched_data = data['data']

        transformed_data = [x * 2 for x in fetched_data]
        return {"transformed_data": transformed_data}

    @task.python
    def third_task(data: dict):
        print("This is the third task")
        transformed_data = data['transformed_data']

        print(f"Transformed data: {transformed_data}")

    first = first_task()
    second = second_task(first)
    third = third_task(second)

    
    first >> second >> third

xcom_auto_dag()
    

    