from airflow import DAG
from airflow.decorators import task
from datetime import datetime

@task
def print_hello():
    print("hello!")
    return "hello!"

@task
def print_goodbye():
    print("goodbye!")
    return "goodbye!"

with DAG(
    dag_id="hello_world",
    start_date=datetime(2024, 9, 25),
    catchup=False,
    tags=['examples'],
    schedule='0 2 * * *'
) as dag:
    print_hello() >> print_goodbye()
