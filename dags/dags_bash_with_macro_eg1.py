from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule= "10 0 L * *",
    start_date=pendulum.datetime(2023,3,1,tz="Asia/Seoul"),
    catchup=False
) as dag:
    # START_DATE : 전월 말일 , END_DATE : 1일전
    bash_task_1 = BashOperator(
        task_id = "bash_tastk_1",
        env={'START_DATE':'{{data_interval_start.in_timezone("Asia/Seoul") |ds }}', #우리나라 시간에 맞출려면 timezone Asia/Seoul작성
             'END_DATE':'{{(data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}' #yyyy-mm-dd |ds
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE : $END_DATE"'
    )