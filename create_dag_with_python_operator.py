from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'ssinga',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def greet(some_dict, ti):
    print("some dict: ", some_dict)
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    occupation = ti.xcom_pull(task_ids='get_occupation', key='occupation')
    print(f"Hello World! My name is {first_name} {last_name} and I am a {occupation}!")


def get_name(ti):
    ti.xcom_push(key='first_name', value='Sushma')
    ti.xcom_push(key='last_name', value='Singa')


def get_occupation(ti):
    ti.xcom_push(key='occupation', value='Data Engineer')


with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator',
    description='Our first dag using python operator',
    start_date=datetime(2022, 12, 28),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'some_dict': {'a': 1, 'b': 2}}
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id='get_occupation',
        python_callable=get_occupation
    )

    [task2, task3] >> task1