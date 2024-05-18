import pandas as pd
import json
import logging
import psycopg2
import os
import sys
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Boolean, Date, CHAR
sys.path.append(os.path.abspath("/opt/airflow/dags/dag_connections/"))
# sys.path.append(os.path.abspath("/home/emmanuel/Escritorio/linkedin_job_postings_etl/"))
# sys.path.append(os.path.abspath("/home/emmanuel/Escritorio/linkedin_job_postings_etl/dags/dag_connections"))
# sys.path.append(os.path.abspath("/opt/airflow/dags/dag_connections/"))
# from transformations.transformations import delete_column, delete_duplicated_id, duration_transformation, cat_genre, drop_transformation, fill_na_merge, fill_na_merge1, category_na, nominee, delete_artist, title
# from transformations.transformations import drop_columns, parenthesis_transformation, fill_nulls_first, fill_nulls_arts, fill_nulls_worker, drop_nulls, lower_case, rename_column
from dag_connections.db import engine_creation, finish_engine
from transformations.transformations import *
# from driveconf import upload_file



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








    
    
    # df_merge = df_spotify.merge(grammys_df, how='left', left_on='track_name', right_on='nominee')
    # df_merge = fill_na_merge(df_merge)
    # df_merge= fill_na_merge1(df_merge)
    # df_merge=delete_artist(df_merge)
    # df_merge=category_na(df_merge)
    # df_merge=nominee(df_merge)
    # df_merge=title(df_merge)
    # logging.info( f"THe merge is Done")
    # logging.info(f"The dimension is: {df_merge.shape}")
    # logging.info(f"the columns are: {df_merge.columns}")



    # return df_merge.to_json(orient='records')


# def load(**kwargs):
#     logging.info("Load proccess is started")
#     ti = kwargs["ti"]
#     data_strg = ti.xcom_pull(task_ids="merge_task")
#     json_data = json.loads(data_strg)
#     df_load = pd.json_normalize(data=json_data)
#     engine = engine_creation()

#     df_load.to_sql('merge', engine, if_exists='replace', index=False)

#     #Close the connection to the DB
#     finish_engine(engine)
#     df_load.to_csv("merge.csv", index=False)
#     logging.info( f"Merge is ready")

#     return df_load.to_json(orient='records')



# def store(**kwargs):
#     logging.info("The Store Process has Started")
#     ti = kwargs["ti"]
#     data_strg = ti.xcom_pull(task_ids="load_task")
#     json_data = json.loads(data_strg)
#     df_store = pd.json_normalize(data=json_data)

#     upload_file("merge.csv","11xQ7d8wvT5wcHQToTfNAmsGUvceG_6cX")    
#     logging.info( f"THe Data is Uploaded In GoogleDrive")