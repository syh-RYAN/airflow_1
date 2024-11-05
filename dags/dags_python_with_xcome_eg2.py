from airflow import DAG
import pendulum
import datetime 
from airflow.decorators import task 

with DAG(
    dag_id='dags_python_with_xcome_eg2',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2023,3,1,tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    @task(task_id='python_xcome_push_by_return')
    def xcome_push_result(**kwargs):
        return 'Success'
    
    @task(task_id='python_xcome_pull_1')
    def xcome_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcome_pull(task_ids = 'python_xcome_push_by_return')
        print('xcome_pull 메서드로 직접 찾은 리턴 값:' +value1)
        
    @task(task_id = 'python_xcome_pull_2')
    def xcome_pull_2(status,**kwargs):
        print('함수 입력으로 받은 값:' +status)
        
    python_xcome_push_by_return = xcome_push_result()
    xcome_pull_2(python_xcome_push_by_return)
    python_xcome_push_by_return >> xcome_pull_1()