from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys 
import os

sys.path.append(os.path.abspath("/home/emmanuel/Escritorio/linkedin_job_postings_etl/dags"))

from dag_connections.etl import *



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
    
    read_db_linkedin = PythonOperator(
        task_id='read_db_linkedin',
        python_callable=read_linkedin,

    )

    read_db_jobs = PythonOperator(
        task_id='read_db_jobs',
        python_callable=read_linkedin_jobs,
    )

    read_db_industries = PythonOperator(
        task_id='read_db_industries',
        python_callable=read_linkedin_industries,
    )

    jobs_merge = PythonOperator(
        task_id='jobs_merge',
        python_callable= merge_jobs,
    )

    read_db_api = PythonOperator(
        task_id='read_db_api',
        python_callable=read_api,
    )

    transform_db_linkedin = PythonOperator(
        task_id='transform_db_linkedin',
        python_callable=transform_linkedin,
    )

    

    transform_api_task = PythonOperator(
        task_id='transform_api_task',
        python_callable=transform_api,
        provide_context = True,
    )
    
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

    
    read_db_linkedin >> jobs_merge #>> transform_db_linkedin
    read_db_jobs >> jobs_merge #>> transform_db_linkedin
    read_db_industries >> jobs_merge #>> transform_db_linkedin

    jobs_merge >> transform_db_linkedin
    
    read_db_api >> transform_api_task