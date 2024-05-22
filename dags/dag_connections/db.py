import json
import pandas as pd
import sys
import os
import psycopg2
import logging 
sys.path.append(os.path.abspath("/opt/airflow/"))

from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Float, CHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


api_csv = './Data/jobs1.csv'


with open('Credentials/keys_e.json', 'r') as json_file:
    data = json.load(json_file)
    user = data["user"]
    password = data["password"]
    port= data["port"]
    server = data["server"]
    db = data["db"]

db_connection = f"postgresql://{user}:{password}@{server}:{port}/{db}"
engine=create_engine(db_connection)
Base = declarative_base()


def engine_creation():
    engine = create_engine(db_connection)
    return engine

def create_session(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Dimensions and facts creation
def create_company_dimension(df_linkedin):
    company_dimension = df_linkedin.drop(columns=[
        'title',
        'description',
        'formatted_work_type',
        'location',
        'views',
        'job_posting_url',
        'application_type',
        'formatted_experience_level',
        'posting_domain',
        'sponsored',
        'currency',
        'compensation_type',
        'scraped',
        'annual_salary'   
    ])

    return company_dimension

def create_industry_dimension(df_linkedin):
    industry_dimension = df_linkedin.drop(columns=[
        'title',
        'description',
        'formatted_work_type',
        'location',
        'views',
        'job_posting_url',
        'application_type',
        'formatted_experience_level',
        'posting_domain',
        'sponsored',
        'currency',
        'compensation_type',
        'scraped',
        'annual_salary',
        'company_id'  
    ])

    return industry_dimension

def create_jobs_dimension(df_linkedin):
    jobs_dimension = df_linkedin.drop(columns=[
        'posting_domain',
        'currency',
        'compensation_type',
        'industry_id',
        'industry_name',
        'scraped',
        'annual_salary',
        'company_id'  
    ])

    return jobs_dimension


def create_salary_facts(df_linkedin):
    fact_salary = df_linkedin.drop(columns=[
        'company_id',
        'title',
        'description',
        'formatted_work_type',
        'location',
        'views',
        'job_posting_url',
        'application_type',
        'formatted_experience_level',
        'posting_domain',
        'sponsored',
        'scraped',
        'industry_id',
        'industry_name',

    ])

    return fact_salary


# Datawarehouse creation 
def create_data_warehouse():
    create_company_dimension = '''
    CREATE TABLE IF NOT EXISTS dim_company(
        company_id FLOAT PRIMARY KEY,
        industry_id BIGINT,
        industry_name VARCHAR(255),
        job_id BIGINT,
        FOREIGN KEY (job_id) REFERENCES dim_jobs(job_id)
    );
    '''

    create_industry_dimension = '''
    CREATE TABLE IF NOT EXISTS dim_industry(
        industry_id BIGINT PRIMARY KEY,
        industry_name VARCHAR(255),
        job_id BIGINT,
        FOREIGN KEY (job_id) REFERENCES dim_jobs(job_id)
    );
    ''' 

    create_salary_facts = '''   
    CREATE TABLE IF NOT EXISTS fact_salary(
        job_id BIGINT PRIMARY KEY,
        annual_salary FLOAT,
        compensation_type VARCHAR(255),
        currency VARCHAR(255),
        FOREIGN KEY (job_id) REFERENCES dim_jobs(job_id)
    );
    ''' 

    create_jobs_dimension = '''
    CREATE TABLE IF NOT EXISTS dim_jobs(
        job_id BIGINT PRIMARY KEY,
        title VARCHAR(255),
        description TEXT,
        formatted_work_type VARCHAR(255),
        location VARCHAR(255),
        views FLOAT,
        job_posting_url VARCHAR(255),
        application_type VARCHAR(255),
        formatted_experience_level VARCHAR(255),
        sponsored BOOLEAN
    );
    '''

    engine = create_engine(db_connection)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        session.execute(create_jobs_dimension)
        session.execute(create_salary_facts)
        session.execute(create_company_dimension)
        session.execute(create_industry_dimension)
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()
    finally:
        session.close()


# Datawarehouse insertion
def insert_fact_data_warehouse(df, table):
    df = df.astype(str)
    column_names = df.columns.tolist()
    insert_salary_query = f"""
        INSERT INTO {table}({", ".join(column_names)})
        VALUES ({", ".join([f":{col}" for col in column_names])})
        ON CONFLICT (job_id) DO UPDATE SET
        currency = EXCLUDED.currency,
        compensation_type = EXCLUDED.compensation_type,
        annual_salary = EXCLUDED.annual_salary;
    """

    engine = create_engine(db_connection)  # Asegúrate de que db_connection es la cadena de conexión a tu base de datos
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        for index, row in df.iterrows():
            values = {col: val for col, val in zip(column_names, row)}
            session.execute(insert_salary_query, values)
        session.commit()
        print(f"Data has been loaded into: {table}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        session.close()

def insert_company_data_warehouse(df, table):
    df = df.astype(str)
    column_names = df.columns.tolist()
    insert_company_query = f"""
        INSERT INTO {table}({", ".join(column_names)})
        VALUES ({", ".join([f":{col}" for col in column_names])})
        ON CONFLICT (company_id) DO UPDATE SET
        industry_id = EXCLUDED.industry_id,
        industry_name = EXCLUDED.industry_name,
        job_id = EXCLUDED.job_id;
    """

    engine = create_engine(db_connection)  
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        for index, row in df.iterrows():
            values = {col: val for col, val in zip(column_names, row)}
            session.execute(insert_company_query, values)
        session.commit()
        print(f"Data has been loaded into: {table}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        session.close()



def insert_industry_data_warehouse(df, table):
    df = df.astype(str)
    column_names = df.columns.tolist()
    insert_industry_query = f"""
        INSERT INTO {table}({", ".join(column_names)})
        VALUES ({", ".join([f":{col}" for col in column_names])})
        ON CONFLICT (industry_id) DO UPDATE SET
        industry_name = EXCLUDED.industry_name,
        job_id = EXCLUDED.job_id;
    """

    engine = create_engine(db_connection)  # Asegúrate de que db_connection es la cadena de conexión a tu base de datos
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        for index, row in df.iterrows():
            values = {col: val for col, val in zip(column_names, row)}
            session.execute(insert_industry_query, values)
        session.commit()
        print(f"Data has been loaded into: {table}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        session.close()

def insert_jobs_data_warehouse(df, table):
    df = df.astype(str)
    column_names = df.columns.tolist()
    insert_jobs_query = f"""
        INSERT INTO {table}({", ".join(column_names)})
        VALUES ({", ".join([f":{col}" for col in column_names])})
        ON CONFLICT (job_id) DO UPDATE SET
        application_type = EXCLUDED.application_type,
        description = EXCLUDED.description,
        formatted_experience_level = EXCLUDED.formatted_experience_level,
        formatted_work_type = EXCLUDED.formatted_work_type,
        job_posting_url = EXCLUDED.job_posting_url,
        location = EXCLUDED.location,
        sponsored = EXCLUDED.sponsored,
        title = EXCLUDED.title;
    """

    engine = create_engine(db_connection)  
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        for index, row in df.iterrows():
            values = {col: val for col, val in zip(column_names, row)}
            session.execute(insert_jobs_query, values)
        session.commit()
        print(f"Data has been loaded into: {table}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        session.close()



def create_api_table(engine):

    class api(Base):
        _tablename_ = 'API_transform'
        id = Column(Integer, primary_key=True, autoincrement=True)
        work_year = Column(Integer, nullable=False)
        experience_level = Column(String(100), nullable=False)
        employment_type = Column(String(100), nullable=False)
        job_title = Column(String(100), nullable=False)
        salary = Column(Integer, nullable=False)
        salary_currency = Column(String(100), nullable=False)
        salary_in_usd = Column(Integer, nullable=False)
        employee_residence = Column(String(100), nullable=False)
        company_location = Column(String(100), nullable=False)
        company_size = Column(String(100), nullable=False)

    Base.metadata.create_all(engine)
    api._table_

def insert_merge():
    df_api = pd.read_csv(api_csv)
    df_api.to_sql('API_transform', engine, if_exists='replace', index=False)

def finish_engine(engine):
    engine.dispose()

def get_jobs_data():
        
    try: 
        conx = create_engine()
        cursor = conx.cursor()

        get_data = "SELECT * FROM linkedinjobsalary"
        
        cursor.execute(get_data)

        data = cursor.fetchall()
        columns = ['title','formatted_work_type','location','views','application_type','formatted_experience_level', 
                   'industry_name','annual_salary']
        
        df = pd.DataFrame(data, columns=columns)

        conx.commit()
        cursor.close()
        conx.close()

        logging.info("Data fetched successfully")
        return df

    except Exception as err:
        logging.error(f"Error while getting data: {err}")