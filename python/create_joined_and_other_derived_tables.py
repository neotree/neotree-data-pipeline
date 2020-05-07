# Import created modules (need to be stored in the same directory as notebook)
import connect as cn
import read_data as rd
import extract_key_values as ekv
import create_postgres_table as cpt
import create_derived_columns as cdc

# Import libraries
import pandas as pd
import sqlalchemy
import numpy as np
import json
import pytest

def main():
    print("Starting script to create joined and other derived tables")

    # Connect to database
    print("1. Connecting to database")
    try:
        conn = cn.database_connection()
    except:
        print("A connection error occured")

    # Read the raw admissions and discharge data into dataframes
    print("2. Fetching admissions and discharges data")
    try:
        adm_query = '''
                select 
                    *
                from derived.admissions;
            '''

        dis_query = '''
            select 
                *
            from derived.discharges;
        '''

        adm_df = rd.read_table(adm_query, conn)
        dis_df = rd.read_table(dis_query, conn)
    except:
        print("An error occured fetching the data")

 
    # Create count dataframes
    print("3. Creating count tables")
    try:
        # explode the AdmReason.label column (not setting uid as index)
        cnt_admreason_label = adm_df[['AdmReason.label']]
        cnt_admreason_label_tbl = cnt_admreason_label.explode('AdmReason.label')
        
        # explode the contCauseDeath.label column (not setting uid as index)
        cnt_contcausedeath_label = dis_df[['ContCauseDeath.label']]
        cnt_contcausedeath_label_tbl = cnt_contcausedeath_label.explode('ContCauseDeath.label')
    except:
        print("An error occured creating count dataframes")

    # Create join of admissions & discharges (left outter join)
    print("4. Creating joined admissions and discharge table and derived columns")
    try:
        # join admissions and discharges
        jn_adm_dis = adm_df.merge(dis_df, how='left',left_index=True, right_index=True,suffixes=('_admission','_discharge'))
        # Extend join table with derived columns based on power bi logic
        jn_adm_dis_ext = cdc.create_columns(jn_adm_dis)    
    except:
        print("An error occured creating joined dataframe")
        
  
    # Now write the table back to the database
    print("5. Writing the output back to the database")
    try:
        cnt_admreason_label_tbl_n = 'count_admission_reason'
        cnt_contcausedeath_label_tbl_n = 'count_cont_death_causes'
        jn_adm_dis_tbl_n = 'joined_admissions_discharges'
        
        cpt.create_table(cnt_admreason_label_tbl, cnt_admreason_label_tbl_n)
        cpt.create_table(cnt_contcausedeath_label_tbl, cnt_contcausedeath_label_tbl_n)
        cpt.create_table(jn_adm_dis_ext, jn_adm_dis_tbl_n)
    except:
        print("An error occured writing output back to the database")
    
    print("6. Script completed!")

if __name__ == "__main__":
    main()