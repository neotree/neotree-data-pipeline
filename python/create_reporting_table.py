# Import created modules (need to be stored in the same directory as notebook)
import connect as cn
import read_data as rd
import extract_key_values as ekv
import merge_df as mdf
import create_postgres_table as cpt

# Import libraries
import pandas as pd
import sqlalchemy
import numpy as np
import json
import pytest

def main():
    print("Starting process to create admissions table")

    # Connect to database
    print("1. Connecting to database")
    try:
        conn = cn.database_connection()
    except:
        print("A connection error occured")

    # Read the raw admissions and discharge data into dataframes
    print("2. Fetching raw data")
    try:
        admin_query = '''
                select 
                    uid,
                    ingested_at,
                    "data"->'entries' as "entries"
                from scratch.deduplicated_admissions;
            '''

        dis_query = '''
            select 
                uid,
                ingested_at,
                "data"->'entries' as "entries"
            from scratch.deduplicated_discharges;
        '''

        admin_raw = rd.read_table(admin_query, conn)
        dis_raw = rd.read_table(dis_query, conn)
    except:
        print("An error occured fetching the data")

    # Now let's fetch the list of properties recorded in that table
    print("3. Extracting keys")
    try:
        admin_new_entries = ekv.get_key_values(admin_raw)
        dis_new_entries = ekv.get_key_values(dis_raw)
    except:
        print("An error occured extracting keys")

    # Create the dataframe (df) where each property is pulled out into its own colum
    print("4. Creating normalized dataframes")
    try:
        admin_df = pd.json_normalize(admin_new_entries, max_level=2)
        dis_df = pd.json_normalize(dis_new_entries, max_level=2)
    except:
        print("An error occured normalized dataframes)
    
    # Add back the  ingested_at and session
    print("4. Merging records")
    try:
        admissions = mdf.merge_df(admin_raw,admin_df)
        discharges = mdf.merge_df(dis_raw,dis_df)
    except:
        print("An error occured merging records")

    # Now write the table back to the database
    print("5. Writing the output back to the database")
    try:
        admin_table_name = "new_admissions"
        dis_table_name ='new_discharges'
        cpt.create_table(admissions,admin_table_name)
        cpt.create_table(discharges, dis_table_name)
    except:
        print("An error occured writing output back to the database")
    
    print("6. Script completed!")

if __name__ == "__main__":
    main()