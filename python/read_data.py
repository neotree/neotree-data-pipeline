import pandas as pd

def read_table(query, conn):
    # Read the deduplicated admissions
    admission_entries_raw = pd.read_sql_query(query, conn, index_col="uid")
    return admission_entries_raw