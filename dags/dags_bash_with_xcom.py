from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
        dag_id='dags_bash_with_xcom',
        schedule='10 0 * * *',
        start_date=pendulum.datetime(2025, 2, 1, tz='Asia/Seoul'),
        catchup=False
) as dag :
    bash_push = BashOperator(
        task_id='bash_push',
        bash_command="echo START && "
                     "echo xcom_pushed "
                     "{{ ti.xcom_push(key='bash_pushed', value='first_bash_message') }} && "
                     "echo complete"

    )

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={'pushed_value':"{{ ti.xcom_pull(key='bash_pushed') }}",
             'return_value':"{{ ti.xcom_pull(task_ids='bash_push') }}"},
        bash_command="echo $pushed_value && echo $return_value",
        do_xcom_push=False
    )

    bash_push >> bash_pull