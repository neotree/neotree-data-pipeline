from config import config

def create_table(df,table_name):
    from sqlalchemy import create_engine
    params = config()
    connectionstring = 'postgresql+psycopg2://' + params["user"] + ':' + params["password"] +  '@' + params["host"] +  ':' + '5432' + '/' + params["database"] 
    engine = create_engine(connectionstring)
    df.to_sql(table_name, con=engine, schema='derived', if_exists='replace')