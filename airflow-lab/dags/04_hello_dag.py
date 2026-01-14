from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="hello_world_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "tutorial"],
)
def hello_world():

    @task
    def say_hello():
        print("Hello, Airflow!")

    say_hello()

hello_world()