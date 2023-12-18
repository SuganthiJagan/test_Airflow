import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from task_a import main as task1_main
from task_b import main as task2_main

default_args = {
    "depends_on_past": False,
    "start_date": datetime.datetime(2023, 12, 19),
    "description": "<description here>",
    "catchup": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=15),
    "provide_context": True,
    "concurrency": 2,
    "schedule_interval": "@daily", # eg "0 8 * * *"
    "tags": ["interview"],
}

with DAG(
    dag_id='<name of the pipeline>',
    default_args=default_args,
) as dag:
    
    task_1 = PythonOperator(
        task_id='execute_task1',
        python_callable=task1_main,
        # op_kwargs={'url': 'https://jsonplaceholder.typicode.com/users'}
    )

    task_2 = PythonOperator(
        task_id='execute_task2',
        python_callable=task2_main
    )

    task_1 >> task_2