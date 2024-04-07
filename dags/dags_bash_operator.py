from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_bash_operator",                                    # dag이름
    schedule="0 0 * * *",                                           # cron schedule로 dag이 언제 주기로 도는지 설정
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),      # dag이 언제부터 돌지 결정
    catchup=False,                                                  # 기간 상 누락된 부분도 돌릴꺼냐
    # dagrun_timeout=datetime.timedelta(minutes=60),                  # 타이아웃 설정
    # tags=["example", "example2"],                                   # 태그 설정
    # params={"example_key": "example_value"},                      # 파라미터
) as dag:
    # [START howto_operator_bash]
    bash_t1 = BashOperator(                    # task 객체명
        task_id="bash_t1",               # 그래프에 나오는 객체명
        bash_command="echo whomi",              # 수행할 쉘스크립트
    )
    # [END howto_operator_bash]
    bash_t2 = BashOperator(                    
        task_id="bash_t2",               
        bash_command="echo $HOSTNAME",              
    )

    bash_t1 >> bash_t2                  # task 수행 순서