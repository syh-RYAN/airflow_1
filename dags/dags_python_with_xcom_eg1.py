from airflow import DAG
import pendulum
import datetime 
from airflow.decorators import task 

with DAG(
    dag_id='dags_python_with_xcome_eg1',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2023,3,1,tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    @task(task_id = 'python_xcome_push_task1')
    def xcome_push1(**kwargs):
        ti = kwargs['ti']
        ti.xcome_push(key='result1',value='value_1')
        ti.xcome_push(key='result2',value=[1,2,3])
        
    @task(task_id='python_xcome_push_task2')
    def xcome_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcome_push(key='result1',value='value_2')
        ti.xcome_push(key='result2',value=[1,2,3,4])
        
    @task(task_id = 'python_xcome_pull_task')
    def xcome_pull(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcome_pull(key='result1')
        value2 = ti.xcome_pull(key='result2',task_ids='python_xcome_push_task1')
        print(value1)
        print(value2)
        
    xcome_push1() >> xcome_push2() >> xcome_pull()