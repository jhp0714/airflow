from airflow import DAG
from airflow.providers.http.operators.simple_http import SimpleHttpOperator
from airflow.decorators import task
import pendulum

with DAG(
        dag_id='dags_simple_http_operator',
        schedule=None,
        start_date=pendulum.datetime(2025, 2, 1, tz='Asia/Seoul'),
        catchup=False
) as dag:

    '''서울시 공공자전거 대여소 정보'''
    tb_cycle_station_info = SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr',
        # endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/5/',
        endpoint=f'5a5156674f676f7737384c61764f55/json/tbCycleStationInfo/1/5/',
        method='GET',
        headers={'Content-Type':'application/json',
                 'charset':'utf-8',
                 'Accept':'*/*'
                 }
    )

    @task(task_id='python_2')
    def python_2(**kwargs) :
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info')

        if not rslt :
            raise ValueError("No data pulled from XCom! API 응답을 확인하세요.")

        import json
        from pprint import pprint

        try :
            parsed_json = json.loads(rslt)
            pprint(parsed_json)
        except json.JSONDecodeError :
            print("❌ JSON 디코딩 실패. 응답이 JSON 형식인지 확인하세요.")
            print(rslt)  # 원본 응답 출력

    tb_cycle_station_info >> python_2()