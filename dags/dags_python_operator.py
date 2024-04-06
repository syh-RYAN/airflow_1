from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import pendulum
import random

with DAG(
    dag_id="dags_python_operator", #dag 이름 파일명과 똑같이 하는게 관리하기 쉽다
    schedule="30 6 * * *", #매일마다 0시 0분에 돌고 있는거다
    start_date=pendulum.datetime(2024, 3, 28, tz="Asia/Seoul"), #dag이 언제부터 돌건지
    catchup=False
    
) as dag:
    def select_fruit():
        fruit = ['APPLE','BANANA','ORANGE','AVOCADO']
        rand_int = random.randint(0,3) # 0~3부터 임의로 랜덤 값을 선택하겠다.
        print(fruit[rand_int])
        
    py_t1 = PythonOperator(
        task_id = 'py_t1',
        python_callable=select_fruit #어떤함수를 실행시킬것인지
    )
    
    py_t1
        