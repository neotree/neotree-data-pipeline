# Import created modules (need to be stored in the same directory as notebook)
from step_2_tidy_files.extract_key_values import get_key_values
from common_files.sql_functions import read_table
from common_files.sql_functions import create_table

# Import libraries
import pandas as pd

def tidy_tables():
    print("... Starting process to create tidied admissions and discharges table (derived.admissions and derived.discharges)")

    # Read the raw admissions and discharge data into dataframes
    print("... Fetching raw admission and discharge data")
    try:
        adm_query = '''
                select 
                    uid,
                    ingested_at,
                    "data"->'entries' as "entries"
                from scratch.test_deduplicated_admissions;
            '''

        dis_query = '''
            select 
                uid,
                ingested_at,
                "data"->'entries' as "entries"
            from scratch.test_deduplicated_discharges;
        '''

        adm_raw = read_table(adm_query)
        dis_raw = read_table(dis_query)
    except:
        print("An error occured fetching the data")

    # Now let's fetch the list of properties recorded in that table
    print("... Extracting keys")
    try:
        adm_new_entries = get_key_values(adm_raw)
        dis_new_entries = get_key_values(dis_raw)
    except:
        print("An error occured extracting keys")

    # Create the dataframe (df) where each property is pulled out into its own colum
    print("... Creating normalized dataframes - one for admissions and one for discharges")
    try:
        adm_df = pd.json_normalize(adm_new_entries)
        adm_df.set_index('uid',inplace=True)
        dis_df = pd.json_normalize(dis_new_entries)
        dis_df.set_index('uid',inplace=True)
    except:
        print("An error occured normalized dataframes")

    # Now write the cleaned up admission and discharge tables back to the database
    print("... Writing the tidied admission and discharge back to the database")
    try:
        adm_tbl_n = 'test_admissions'
        dis_tbl_n ='test_discharges'
        
        create_table(adm_df,adm_tbl_n)
        create_table(dis_df,dis_tbl_n)
    except:
        print("An error occured writing admissions and discharge output back to the database")  

    print("... Creating MCL count tables")
    try:
        # explode the AdmReason.label column (not setting uid as index)
        cnt_admreason_label = adm_df[['AdmReason.label']]
        cnt_admreason_label_exp = cnt_admreason_label.explode('AdmReason.label')

        # explode the contCauseDeath.label column (not setting uid as index)
        cnt_contcausedeath_label = dis_df[['ContCauseDeath.label']]
        cnt_contcausedeath_label_exp = cnt_contcausedeath_label.explode('ContCauseDeath.label')
    except:
        print("An error occured creating MCL count tables")   

    print("... Writing MCL count output back to the database")
    try:
        # create count tables
        cnt_admreason_label_tbl_n = 'test_count_admission_reason'
        cnt_contcausedeath_label_tbl_n = 'test_count_cont_death_causes'
        create_table(cnt_admreason_label_exp, cnt_admreason_label_tbl_n)
        create_table(cnt_contcausedeath_label_exp, cnt_contcausedeath_label_tbl_n)
    except:
        print("An error occured writing MCL count output back to the database")  
    
    print("... Tidy script completed!")