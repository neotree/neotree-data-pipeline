# From https://www.postgresqltutorial.com/postgresql-python/connect/

#!/usr/bin/python
import psycopg2
from config import config

def database_connection():
    # Database connection
    conn = None 
    try: 
        params = config()
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    return conn