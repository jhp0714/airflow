from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_seoul_api_corona',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2025,4,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''서울시 코로나 19 확진자 발생 동향'''
    tb_corona19_count_status = SeoulApiToCsvOperator(
        task_id='tb_corona19_count_status',
        dataset_nm='tbCycleStationInfo',
        path='opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TbCorona19CountStatus.csv'
    )

    '''서울시 코로나 19 백신 예방접종 현황'''
    tv_corona19_vaccine_stat_new = SeoulApiToCsvOperator(
        task_id='tv_corona19_vaccine_stat_new',
        dataset_nm='TvCorona19VaccinestatNew',
        path='opt/airflow/files/TvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TvCorona19VaccinestatNew.csv'
    )
    tb_corona19_count_status
