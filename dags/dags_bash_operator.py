from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator",# airflow 화면에서 보이는 이름(주로 파일명과 일치 시키는게 좋음)
    schedule="0 0 * * *",       # 크론 스케줄 (분 시 일 월 요일)
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"), # 덱이 언제부터 돌지
    catchup=False,
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami"
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME"
    )

    bash_t1 >> bash_t2