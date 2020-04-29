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
    print("2. Fetching raw data")
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
    print("4. Creating normalized dataframes")
    try:
        adm_df = pd.json_normalize(adm_new_entries)
        adm_df.set_index('uid',inplace=True)
        dis_df = pd.json_normalize(dis_new_entries)
        dis_df.set_index('uid',inplace=True)
    except:
        print("An error occured normalized dataframes")

    # Create count dataframes
    print("5. Creating count tables")
    try:
        # explode the AdmReason.label column (not setting uid as index)
        cnt_admreason_label = adm_df[['AdmReason.label']]
        cnt_admreason_label = cnt_admreason_label.explode('AdmReason.label')
        
        # explode the contCauseDeath.label column (not setting uid as index)
        cnt_contcausedeath_label = dis_df[['ContCauseDeath.label']]
        cnt_contcausedeath_label = cnt_contcausedeath_label.explode('ContCauseDeath.label')
    except:
        print("An error occured creating count dataframes")

    # Create join of admissions & discharges (left outter join)
    print("6. Creating joined admissions and discharge table")
    try:
        # join admissions and discharges
        jn_adm_dis = adm_df.merge(dis_df, how='left',left_index=True, right_index=True,suffixes=('_admission','_discharge'))
    except:
        print("An error occured joining dataframes")
    
    # Now write the table back to the database
    print("7. Writing the output back to the database")
    try:
        adm_tbl_n = 'admissions'
        dis_tbl_n ='discharges'
        cnt_admreason_label_tbl_n = 'count_admission_reason'
        cnt_contcausedeath_label_tbl_n = 'count_cont_death_causes'
        jn_adm_dis_tbl_n = 'joined_admissions_discharges'
        
        cpt.create_table(adm_df,adm_tbl_n)
        cpt.create_table(dis_df, dis_tbl_n)
        cpt.create_table(cnt_admreason_label, cnt_admreason_label_tbl_n)
        cpt.create_table(cnt_contcausedeath_label, cnt_contcausedeath_label_tbl_n)
        cpt.create_table(jn_adm_dis, jn_adm_dis_tbl_n)
    except:
        print("An error occured writing output back to the database")
    
    print("7. Script completed!")

if __name__ == "__main__":
    main()