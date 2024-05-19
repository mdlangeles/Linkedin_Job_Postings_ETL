import json
import pandas as pd
import sys
import os
import psycopg2
sys.path.append(os.path.abspath("/opt/airflow/"))

from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Float, CHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


api_csv = './Data/jobs1.csv'


with open('Credentials/keys.json', 'r') as json_file:
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

def create_data_warehouse():
    create_company_dimension = '''
    CREATE TABLE IF NOT EXISTS dim_company(
        company_id INT PRIMARY KEY,
        industry_id INT,
        industry_name VARCHAR(255),
        job_id INT
    );
    '''

    create_industry_dimension = '''
    CREATE TABLE IF NOT EXISTS dim_industry(
        industry_id INT PRIMARY KEY,
        industry_name VARCHAR(255),
        job_id INT
    );
    '''

    create_jobs_dimension = '''
    CREATE TABLE IF NOT EXISTS dim_jobs(
        job_id INT PRIMARY KEY,
        application_type VARCHAR(255),
        description VARCHAR(255),
        formatted_experience_level VARCHAR(255),
        formatted_work_type VARCHAR(255),
        job_posting_url VARCHAR(255),
        location VARCHAR(255),
        sponsored BOOLEAN,
        title VARCHAR(255)
    );
    '''

    create_salary_facts = '''
    CREATE TABLE IF NOT EXISTS fact_salary(
        job_id INT PRIMARY KEY,
        annual_salary INT,
        compensation_type VARCHAR(255),
        currency VARCHAR(255)
    );
    '''

    cnx = None
    try:
        cnx = create_engine()
        cur = cnx.cursor()
        cur.execute(create_salary_facts)
        cur.execute(create_jobs_dimension)
        cur.execute(create_industry_dimension)
        cur.execute(create_company_dimension)
        cur.close()
        cnx.commit()
        print('Data Warehouse created successfully')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cnx is not None:
            cnx.close()

def insert_data_warehouse(df,table):
    column_names = df.columns.tolist()
    insert_query = f"""
        INSERT INTO {table}({", ".join(column_names)})
        VALUES ({", ".join(["%s"] * len(column_names))})
    """
    cnx = None
    try:
        cnx = create_engine()
        cur = cnx.cursor()
        for index, row in df.iterrows():
            values = tuple(row)
            cur.execute(insert_query, values)
        cur.close()
        cnx.commit()
        print(f"Data has been loaded into: {table}")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cnx is not None:
            cnx.close()
    



def create_api_table(engine):

    class api(Base):
        __tablename__ = 'API_transform'
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
    api.__table__

def insert_merge():
    df_api = pd.read_csv(api_csv)
    df_api.to_sql('API_transform', engine, if_exists='replace', index=False)

def finish_engine(engine):
    engine.dispose()