from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
import pendulum

with DAG(
        dag_id='dags_branch_python_operator',
        schedule=None,
        start_date=pendulum.datetime(2025, 2, 1, tz='Asia/Seoul'),
        catchup=False
) as dag:

    def select_random():
        import random

        item_lst = ['a', 'b', 'c']
        selected_item = random.choice(item_lst)

        if selected_item == 'a':
            return 'task_a'
        else:
            return ['task_b', 'task_c']

    python_branch_task = BranchPythonOperator(
        task_id='python_branch_task',
        python_callable=select_random
    )

    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'a'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected' : 'b'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected' : 'c'}
    )

    python_branch_task >> [task_a, task_b, task_c]