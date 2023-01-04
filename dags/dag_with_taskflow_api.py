from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'ssinga',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='',
     default_args=default_args,
     start_date=datetime(2022, 12, 29),
     schedule_interval='@daily')

def hello_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Sushma',
            'last_name': 'Singa'
        }

    @task()
    def get_occupation():
         return 'Data Engineer'

    @task()
    def greet(first_name, last_name, occupation):
        print(f"Hello! My name is {first_name} {last_name} and I am a {occupation}!")

    name_dict = get_name()
    occupation = get_occupation()
    greet(first_name=name_dict['first_name'],
          last_name=name_dict['last_name'],
          occupation=occupation)

greet_dag = hello_etl()