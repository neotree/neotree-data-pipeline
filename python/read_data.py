import pandas as pd

def read_table(query, conn):
    # Read the deduplicated admissions or discharge tables
    data_raw = pd.read_sql_query(query, conn, index_col="uid")
    return data_raw