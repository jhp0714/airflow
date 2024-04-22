from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task(task_id="python_task1")
    def print_context(input):
        print(input)

    python_task1 = print_context('task_decorator 실행')