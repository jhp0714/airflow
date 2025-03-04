from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
import pendulum

with DAG(
        dag_id='dags_python_with_task_group',
        schedule=None,
        start_date=pendulum.datetime(2025, 2, 1, tz='Asia/Seoul'),
        catchup=False
) as dag:

    def inner_func(**kwargs):
        msg = kwargs.get('get') or ''
        print(msg)

    @task_group(group_id='first_group')
    def group_1():
        """ task_group 데커레이터를 이용한 첫 번째 그룹 """

        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('첫 번째 taskgroup 내 첫 번째 task')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'첫 번째 taskgroup 내 두 번째 task'}
        )

        inner_func1() >> inner_function2

    with TaskGroup(group_id='second_group', tooltip='두 번째 그룹') as group_2:
        """여기에 적은 docstring은 표시되지 않는다."""

        @task(task_id='inner_function1')
        def inner_funct1(**kwargs):
            print('두 번째 taskgroup 내 첫 번째 task')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg' : '두 번째 taskgroup 내 두 번째 task'}
        )

        inner_funct1() >> inner_function2

    group_1() >> group_2