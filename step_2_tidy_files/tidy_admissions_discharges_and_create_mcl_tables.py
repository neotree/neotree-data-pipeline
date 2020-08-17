# Import created modules (need to be stored in the same directory as notebook)
from step_2_tidy_files.extract_key_values import get_key_values
from step_2_tidy_files.explode_mcl_columns import explode_column
from common_files.sql_functions import read_table
from common_files.sql_functions import create_table

# Import libraries
import pandas as pd
from datetime import datetime as dt

def tidy_tables():
    print("... Starting process to create tidied admissions, discharges and MCL tables (derived.admissions and derived.discharges)")

    # Read the raw admissions and discharge data into dataframes
    print("... Fetching raw admission and discharge data")
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

        adm_raw = read_table(adm_query)
        dis_raw = read_table(dis_query)
    except Exception as e:
        print("!!! An error occured fetching the data: ")
        raise e

    # Now let's fetch the list of properties recorded in that table
    print("... Extracting keys")
    try:
        adm_new_entries,adm_mcl = get_key_values(adm_raw)
        dis_new_entries,dis_mcl = get_key_values(dis_raw)
    except Exception as e:
        print("!!! An error occured extracting keys: ")
        raise e

    # Create the dataframe (df) where each property is pulled out into its own colum
    print("... Creating normalized dataframes - one for admissions and one for discharges")
    try:
        adm_df = pd.json_normalize(adm_new_entries)
        adm_df.set_index('uid',inplace=True)
        dis_df = pd.json_normalize(dis_new_entries)
        dis_df.set_index('uid',inplace=True)
        # set data type for DateTimeAdmission.value and DateAdmission.value
        # watch out for time zone (tz) issues if you change code (ref: https://github.com/pandas-dev/pandas/issues/25571)
        adm_df['DateTimeAdmission.value'] =  pd.to_datetime(adm_df['DateTimeAdmission.value'], format ='%Y-%m-%dT%H:%M:%S' , utc=True)
        adm_df['DateAdmission.value'] = adm_df['DateTimeAdmission.value'].dt.date     
    except Exception as e:
        print("!!! An error occured normalized dataframes: ")
        raise e

    # Now write the cleaned up admission and discharge tables back to the database
    print("... Writing the tidied admission and discharge back to the database")
    try:
        adm_tbl_n = 'admissions'
        dis_tbl_n ='discharges'
        
        create_table(adm_df,adm_tbl_n)
        create_table(dis_df,dis_tbl_n)
    except Exception as e:
        print("!!! An error occured writing admissions and discharge output back to the database: ")
        raise e

    print("... Creating MCL count tables")
    try:
        explode_column(adm_df,adm_mcl)
        explode_column(dis_df,dis_mcl)
    except Exception as e:
        print("!!! An error occured creating MCL count tables: ")
        raise e
    
    print("... Tidy script completed!")