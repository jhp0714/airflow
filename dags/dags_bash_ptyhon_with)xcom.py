from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
import pendulum

with DAG(
        dag_id='dags_bash_python_with_xcom',
        schedule='10 0 * * *',
        start_date=pendulum.datetime(2025, 2, 1, tz='Asia/Seoul'),
        catchup=False
) as dag :
    @task(task_id='python_push')
    def python_push_xcom():
        result_dict = {'status':'good', 'data':[1,2,3], 'options_cnt':100}
        return result_dict

    bash_pull = BashOperator(
        task_id='bash_pull',
        env = {
            'status':'{{ti.xcom_pull(task_ids="python_push")["status"]}}',
            'data':'{{ti.xcom_pull(task_ids="python_push")["data"]}}',
            'options_cnt':'{{ti.xcom_pull(task_ids="python_push")["options_cnt"]}}'
        },
        bash_command='echo $status && echo $data && echo $options_cnt'
    )

    python_push_xcom() >> bash_pull

    bash_push = BashOperator(
        task_id='bash_push',
        bash_command='echo push_start '
                     '{{ti.xcom_push(key="bash_pushed", value=200)}} && '
                     'echo push_complete'
    )

    @task(task_id='python_pull')
    def python_pull_xcom(**kwargs):
        ti = kwargs['ti']
        status_value = ti.xcom_pull(key='bash_pushed')
        return_value = ti.xcom_pull(task_ids='bash_push')
        print('status_value:' + str(status_value))
        print('return_value:' + return_value)

    bash_push >> python_pull_xcom()