import pandas as pd
import json
import logging
import psycopg2
import os
import sys
from kafka import KafkaProducer
import datetime as dt
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Boolean, Date, CHAR
sys.path.append(os.path.abspath("/opt/airflow/dags/dag_connections/"))
from dag_connections.db import *
from transformations.transformations import *

logging.basicConfig(level=logging.INFO)

def read_linkedin():
    query = "SELECT * FROM jobslinkedin"
    
    engine = engine_creation()
    
    df_linkedin = pd.read_sql(query, engine)

    #Cerramos la conexion a la db
    finish_engine(engine)

    logging.info("database read succesfully")
    logging.info('data extracted is %s', df_linkedin.head(5))
    return df_linkedin.to_json(orient='records')

def read_linkedin_jobs():
    query = "SELECT * FROM jobsindustries"
    
    engine = engine_creation()
    
    df_industries = pd.read_sql(query, engine)

    #Cerramos la conexion a la db
    finish_engine(engine)

    logging.info("database read succesfully")
    logging.info('data extracted is %s', df_industries.head(5))
    return df_industries.to_json(orient='records')

def read_linkedin_industries():
    query = "SELECT * FROM industries"
    
    engine = engine_creation()
    
    df_indus = pd.read_sql(query, engine)

    #Cerramos la conexion a la db
    finish_engine(engine)

    logging.info("database read succesfully")
    logging.info('data extracted is %s', df_indus.head(5))
    return df_indus.to_json(orient='records')

def merge_jobs(**kwargs):
    ti = kwargs['ti']

    logging.info( f"Linkedin has started the merge proccess")
    data_strg = ti.xcom_pull(task_ids='read_db_linkedin')
    json_data = json.loads(data_strg)
    df_linkedin = pd.json_normalize(data=json_data)

    logging.info( f"JobsLink has started the merge proccess")
    data_strg = ti.xcom_pull(task_ids="read_db_jobs")
    json_data = json.loads(data_strg)
    df_industries = pd.json_normalize(data=json_data)

    logging.info( f"Industries has started the merge proccess")
    data_strg = ti.xcom_pull(task_ids="read_db_industries")
    json_data = json.loads(data_strg)
    df_indus = pd.json_normalize(data=json_data)

    df_merge= df_linkedin.merge(df_industries, on='job_id')\
    .merge(df_indus, on='industry_id')

    logging.info( f"THe merge is Done %s", df_merge.head(5))

    return df_merge.to_json(orient='records')

def transform_linkedin(**kwargs):
    logging.info("The linkedin data has started transformation process")

    ti = kwargs['ti']
    data_strg = ti.xcom_pull(task_ids='jobs_merge')
    json_data = json.loads(data_strg)
    df_linkedin = pd.json_normalize(data=json_data)

    logging.info("df is type: %s", type(df_linkedin))
    
    df_linkedin = select_columns(df_linkedin)
    logging.info("Columns selected %s", df_linkedin.head(5)) 

    df_linkedin=  salary_standardization(df_linkedin)
    logging.info("salary standardization done %s", df_linkedin.head(5)) 

    df_linkedin= average_salary(df_linkedin)
    logging.info("average salary done %s", df_linkedin.head(5))

    df_linkedin= delete_columns1(df_linkedin)
    logging.info("COlumns deleted1 %s", df_linkedin.head(5))

    df_linkedin=annual(df_linkedin)
    logging.info("annual configuration done %s", df_linkedin.head(5))

    df_linkedin= delete_columns2(df_linkedin)
    logging.info("columns deleted2 %s", df_linkedin.head(5))

    df_linkedin= last_changes(df_linkedin)
    logging.info("imputation done %s", df_linkedin.head(5))

    logging.info("The data has ended transformation process %s", df_linkedin.isnull().sum())

    logging.info("The data has ended transformation process")

    return df_linkedin.to_json(orient='records')


def load_linkedin(**kwargs):
    logging.info("Starting data loading process...")

    ti = kwargs["ti"]
    data_strg = ti.xcom_pull(task_ids='transform_db_linkedin')
    json_data = json.loads(data_strg)
    df_linkedin = pd.json_normalize(data=json_data)

    engine = engine_creation()
    #insert_transform_db(df_linkedin)
    finish_engine(engine)

    logging.info("df_linkedin loaded into database")

    create_data_warehouse()
    
    fact_salary = create_salary_facts(df_linkedin)
    logging.info('Number of rows loaded into fact_salary: %s', len(fact_salary))
    insert_fact_data_warehouse(fact_salary,'fact_salary')

    company_dimension = create_company_dimension(df_linkedin)
    logging.info('Number of rows loaded into dim_company: %s', len(company_dimension))
    insert_company_data_warehouse(company_dimension,'dim_company')

    industry_dimension = create_industry_dimension(df_linkedin)
    logging.info('Number of rows loaded into dim_industry: %s', len(industry_dimension))
    insert_industry_data_warehouse(industry_dimension,'dim_industry')

    jobs_dimension = create_jobs_dimension(df_linkedin)
    logging.info('Number of rows loaded into jobs_dimension: %s', len(jobs_dimension))
    insert_jobs_data_warehouse(jobs_dimension,'dim_jobs')


    logging.info("Data loaded into data warehouse")


    return df_linkedin.to_json(orient='records')




def read_api():
    query = "SELECT * FROM jobs_api"
    
    engine = engine_creation()
    
    df_api = pd.read_sql(query, engine)

    #Cerramos la conexion a la db
    finish_engine(engine)

    logging.info("database read succesfully")
    logging.info('data extracted is %s', df_api.head(5))
    return df_api.to_json(orient='records')

def transform_api(**kwargs):
    logging.info("The API data has started transformation process")

    ti = kwargs['ti']
    data_strg = ti.xcom_pull(task_ids='read_db_api')
    json_data = json.loads(data_strg)
    df_api = pd.json_normalize(data=json_data)

    logging.info("df is type: %s", type(df_api))

    df_api= drop_duplicates(df_api)

    df_api= replacing_values(df_api)

    df_api= mapping_company_location(df_api)

    df_api= mapping_employee_residence(df_api)

    df_api= outliers(df_api)

    df_api= remove_columns(df_api)

    logging.info("The data has ended transformation process %s", df_api.head(5))

    logging.info("The data has ended transformation process")

    return df_api.to_json(orient='records')



def load_api(**kwargs):
    logging.info("Load proccess is started")
    ti = kwargs["ti"]
    data_strg = ti.xcom_pull(task_ids="transform_api_task")
    json_data = json.loads(data_strg)
    df_load_api = pd.json_normalize(data=json_data)
    engine = engine_creation()

    df_load_api.to_sql('API_transform', engine, if_exists='replace', index=False)

    #Close the connection to the DB
    finish_engine(engine)
    df_load_api.to_csv("API_transform.csv", index=False)
    logging.info( f"API_transform is ready")

    return df_load_api.to_json(orient='records')


def kafka_producer(**kwargs):
    ti = kwargs['ti']
    data_strg = ti.xcom_pull(task_ids='transform_db_linkedin')
    json_data = json.loads(data_strg)

    df= pd.json_normalize(data=json_data)
    df_linkedin = df[['title', 'formatted_work_type', 'location', 'views', 'application_type', 
                               'formatted_experience_level', 'industry_name', 'annual_salary']]
    logging.info(f"data is: {df_linkedin.head(5)}")

    producer = KafkaProducer(
        value_serializer = lambda m: json.dumps(m).encode('utf-8'),
        bootstrap_servers = ['localhost:9092']
    )
 
    batch = []
    
    batch_size = 500  

    for _, row in df_linkedin.iterrows():
        
        row_json = row.to_json()
        batch.append(row_json)
                
        if len(batch) == batch_size:
            
            message = '\n'.join(batch) 
            producer.send("linkedin.streaming", value=message)
            
            print(f"new batch sent at {dt.datetime.utcnow()}")
            
            batch = []

    if batch:
        message = '\n'.join(batch)
        producer.send("linkedin.streaming", value=message)
        print(f"last batch sent at {dt.datetime.utcnow()}")

    print("All rows sent")



    
    
