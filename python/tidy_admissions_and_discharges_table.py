# Import created modules (need to be stored in the same directory as notebook)
import connect as cn
import read_data as rd
import extract_key_values as ekv
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
    print("2. Fetching raw admission and discharge data")
    try:
        adm_query = '''
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

        adm_raw = rd.read_table(adm_query, conn)
        dis_raw = rd.read_table(dis_query, conn)
    except:
        print("An error occured fetching the data")

    # Now let's fetch the list of properties recorded in that table
    print("3. Extracting keys")
    try:
        adm_new_entries = ekv.get_key_values(adm_raw)
        dis_new_entries = ekv.get_key_values(dis_raw)
    except:
        print("An error occured extracting keys")

    # Create the dataframe (df) where each property is pulled out into its own colum
    print("4. Creating normalized dataframes - one for admissions and one for discharges")
    try:
        adm_df = pd.json_normalize(adm_new_entries)
        adm_df.set_index('uid',inplace=True)
        dis_df = pd.json_normalize(dis_new_entries)
        dis_df.set_index('uid',inplace=True)
    except:
        print("An error occured normalized dataframes")

    # Now write the cleaned up admission and discharge tables back to the database
    print("5. Writing the tidied admission and discharge data back to the database")
    try:
        adm_tbl_n = 'admissions'
        dis_tbl_n ='discharges'
        
        cpt.create_table(adm_df,adm_tbl_n)
        cpt.create_table(dis_df, dis_tbl_n)
    except:
        print("An error occured writing output back to the database")    
    
    print("7. Script completed!")

if __name__ == "__main__":
    main()