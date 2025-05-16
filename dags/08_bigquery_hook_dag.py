import pendulum
from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
import pandas as pd

"""
DAG에 어떤 파일의 경로를 명시해야 하는 경우
해당 DAG는 docker 시스템 내에서 작동하기 때문에
docker container 내부의 경로를 명시해줘야 함
어떤 파일 경로를 docker container 내부 경로로 명시해야 하지만 그 파일이 docker container 내부에 없으면
-dags/ ---> dag 관련 개발 파일만 추가
-logs/ --> dag가 작동한 log가 저장되는 폴더
-plugins/ 
-config/
"""

default_args = dict(
    owner='haein',
    email=['haein@airflow.com'],
    email_on_failure=False,
    retries=3
)

# Airflow UI의 Connections 탭에서 Google Cloud 연결 정보 생성이 선행!
bigquery_hook = BigQueryHook(
    gcp_conn_id='google_cloud_conn',
    location='asia-northeast3'
).get_sqlalchemy_engine()

# Bigquery Hook을 활용해 Pandas로 테이블을 dataframe 형태로 읽어오는 함수!
def fetch_bq_data():
    df = pd.read_sql(
        sql="SELECT * FROM sprint_pokemon.pokemon",
        con=bigquery_hook # sqlalchemy conn
    )
    # 10행까지만 로그에 프린트!
    print(df.head(10))
    
    return df # return_value

with DAG(
    dag_id = '08_bigquery_hook_dag',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20250516'],
    default_args = default_args,
    catchup=False
):
    py_task1 = PythonOperator(
        task_id = 'py_task1',
        python_callable=fetch_bq_data
    )
    
py_task1
    
