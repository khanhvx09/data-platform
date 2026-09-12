from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator



@dag(
    dag_id = "5_xcom_kwargs_dag",    
)
def xcom_kwargs_dag():

    @task.python
    def first_task(**kwargs):
        print("This is the first task")
        fetched_data = {"data":[1,2,3,4,5]}
        ti = kwargs['ti']
        ti.xcom_push(key='fetched_data', value=fetched_data)


    @task.python
    def second_task(**kwargs):
        print("This is the second task")
        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(task_ids = 'first_task',key='fetched_data')

        transformed_data = [x * 2 for x in fetched_data['data']]
        ti.xcom_push(key='transformed_data', value={"transformed_data": transformed_data})

    @task.python
    def third_task(**kwargs):
        print("This is the third task")
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids = 'second_task',key='transformed_data')

        print(f"Transformed data: {data}")


    first = first_task()
    second = second_task()
    third = third_task()

    
    first >> second >> third

xcom_kwargs_dag()
    

    