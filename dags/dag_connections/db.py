import json
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath("/opt/airflow/"))

from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


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

# def create_table(engine):

#     class Grammys(Base):
#         __tablename__ = 'grammys'
#         id = Column(Integer, primary_key=True, autoincrement=True)
#         year = Column(Integer, nullable=False)
#         title = Column(String(100), nullable=False)
#         published_at = Column(DateTime, nullable=False)
#         updated_at = Column(DateTime, nullable=False)
#         category = Column(String(100), nullable=False)
#         nominee = Column(String(100), nullable=False)
#         artist = Column(String(100), nullable=False)
#         workers = Column(String(100), nullable=False)
#         img = Column(String(100), nullable=False)
#         winner = Column(Boolean, nullable=False)

#     Base.metadata.create_all(engine)
#     Grammys.__table__

# def insert_data():
#     df = pd.read_csv(csv_grammys)
#     df.to_sql('grammys', engine, if_exists='replace', index=False)
    
    

# def create_merge_table(engine):

#     class Merged(Base):
#         __tablename__ = 'MusicAwards'
#         id = Column(Integer, primary_key=True, autoincrement=True)
#         track_id = Column(String(250), nullable=False)
#         artist = Column(String(250), nullable=False)
#         album_name = Column(String(250), nullable=False)
#         track_name = Column(String(250), nullable=False)
#         energy = Column(Float, nullable=False)
#         loudness = Column(Float, nullable=False)
#         popularity_level = Column(String(100), nullable=False)
#         duration_mins_secs = Column(DateTime, nullable=False)
#         is_explicit = Column(Boolean, nullable=False)
#         is_danceable = Column(Boolean, nullable=False)
#         track_feeling = Column(String(100), nullable=False)
#         general_category = Column(String(100), nullable=False)
#         year = Column(Integer, nullable=False)
#         title = Column(String(250), nullable=False)
#         award = Column(String(250), nullable=False)
#         nominee = Column(String(250), nullable=False)
#         was_nominated = Column(Boolean, nullable=False)

#     Base.metadata.create_all(engine)
#     Merged.__table__

# def insert_merge(df_merged):
#     df_merged.to_sql('MusicAwards', engine, if_exists='replace', index=False)

def finish_engine(engine):
    engine.dispose()