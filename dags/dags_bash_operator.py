from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime
import pendulum

with DAG(
    dag_id="dags_bash_operator", #dag 이름 파일명과 똑같이 하는게 관리하기 쉽다
    schedule="0 0 * * *", #매일마다 0시 0분에 돌고 있는거다
    start_date=pendulum.datetime(2024, 3, 28, tz="Asia/Seoul"), #dag이 언제부터 돌건지
    catchup=False, #datetime 누락된 구간 할꺼냐 안할꺼냐
    #dagrun_timeout=datetime.timedelta(minutes=60), #60분 이상돌면 취소
    #tags=["example", "example2"], #이름 밑에 태크들
    #params={"example_key": "example_value"},
) as dag:
     bash_t1 = BashOperator(
        task_id="bash_t1", #데스크명과 task명과 동일
        bash_command="echo whoami",
    )
     
     bash_t2 = BashOperator(
        task_id="bash_t2", #데스크명과 task명과 동일
        bash_command="echo $HOSTNAME",
    )
     
    bash_t1 >> bash_t2 #dag 실행 순서
