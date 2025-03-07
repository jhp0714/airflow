from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

with DAG(
        dag_id='dags_trigger_dag_run_operator',
        schedule='30 9 * * *',
        start_date=pendulum.datetime(2025, 2, 1, tz='Asia/Seoul'),
        catchup=False
) as dag:

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start!"',
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='dags_python_operator',  # trigger 될 덱(실행시킬 덱)
        trigger_run_id=None,                    # Run_id 값 직접 지정
        execution_date='{{ data_interval_start }}',# run_id를 manual__{{execution_date}}로 수행
        reset_dag_run=True,                     # 이미 run_id 값이 있는 경우에도 재수행 여부 결정
        wait_for_completion=False,              # 트리거된 dag이 완료가 되어야 현재 task도 완료 상태가 되는지 여부
        poke_interval=60,                       # 트리거된 dag이 완료됬는지 확인하는 주기
        allowed_states=['success'],             # 트리거된 dag이 어떤 상태로 끝나야하는지를 명시
        failed_states=None                      # 현재 트리거가 fail로 마킹되기 위한 trigger 된 dga의 상태를 명시
    )

    start_task >> trigger_dag_task