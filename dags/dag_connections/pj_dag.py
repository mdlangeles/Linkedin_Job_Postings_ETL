from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys 
import os

sys.path.append(os.path.abspath("/home/emmanuel/Escritorio/linkedin_job_postings_etl/dags"))

from dag_connections.etl import read_linkedin



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'project_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
) as dag:
    
    read_linkedin_task = PythonOperator(
        task_id='read_db_linkedin',
        python_callable=read_linkedin,
    )

#     read_api_task = PythonOperator(
#         task_id='read_api_task',
#         python_callable=extract_sql,
#         provide_context = True,
#         )

#     transform_linkedin_task = PythonOperator(
#         task_id='transform_linkedin_task',
#         python_callable=transform,
#         provide_context = True,
# )

#     transform_api_task = PythonOperator(
#         task_id='transform_db_task',
#         python_callable=transform_sql,
#         provide_context = True,
#         )
    
    # merge_task = PythonOperator(
    #     task_id='merge_task',
    #     python_callable=merge,
    #     provide_context = True,
    #     )
    
    # load_task = PythonOperator(
    #     task_id='load_task',
    #     python_callable=load,
    #     provide_context = True,
    #     )
    
    # store_task = PythonOperator(
    #     task_id='store_task',
    #     python_callable=store,
    #     provide_context = True,
    #     )

    read_linkedin_task