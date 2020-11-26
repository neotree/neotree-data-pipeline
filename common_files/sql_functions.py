# ref: https://docs.sqlalchemy.org/en/13/core/connections.html
# to speed up mcl execution ref: https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#psycopg2-fast-execution-helpers

from common_files.config import config
import pandas as pd
from sqlalchemy import event, create_engine
import logging

params = config()
connectionstring = 'postgresql+psycopg2://' + params["user"] + ':' + params["password"] +  '@' + params["host"] +  ':' + '5432' + '/' + params["database"] 
engine = create_engine(connectionstring, executemany_mode='batch')

def inject_sql(sql_script,file_name):
    # ref: https://stackoverflow.com/questions/19472922/reading-external-sql-script-in-python/19473206
    sql_commands = sql_script.split(';')
    for command in sql_commands:
        try:
            engine.connect().execute(command)
        # last element in list is empty and causes error without this except clause
        except:
            pass
    logging.info('... {0} has successfully run'.format(file_name))

def read_table(query):
    # Read the deduplicated admissions/discharges tables
    data_raw = pd.read_sql_query(query, con=engine, index_col="uid")
    return data_raw

def create_table(df,table_name):
    # create tables in derived schema
    df.to_sql(table_name, con=engine, schema='derived', if_exists='replace')
