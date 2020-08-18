# Import created modules (need to be stored in the same directory as notebook)
from common_files.sql_functions import read_table
from common_files.sql_functions import create_table

# Import libraries
import pandas as pd

def join_table():
    
    print("... Starting script to create joined table")

    # Read the raw admissions and discharge data into dataframes
    print("... Fetching admissions and discharges data")
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

        adm_df = read_table(adm_query)
        dis_df = read_table(dis_query)
    except Exception as e:
        print("!!! An error occured fetching the data: ")
        raise e
        

    # Create join of admissions & discharges (left outter join)
    print("... Creating joined admissions and discharge table")
    try:
        # join admissions and discharges
        jn_adm_dis = adm_df.merge(dis_df, how='left',left_index=True, right_index=True,suffixes=('_admission','_discharge'))
        # Extend join table with derived columns based on power bi logic - DEL
        #jn_adm_dis_ext = create_columns(jn_adm_dis) - DEL   
    except Exception as e:
        print("!!! An error occured creating joined dataframe: ")
        raise e
        
        
    # Now write the table back to the database
    print("... Writing the output back to the database")
    try:
        jn_adm_dis_tbl_n = 'joined_admissions_discharges'
        create_table(jn_adm_dis, jn_adm_dis_tbl_n)
    except Exception as e:
        print("!!! An error occured writing join output back to the database: ")
        raise e

    print("... Join script completed!")
