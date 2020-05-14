from common_files.sql_functions import inject_sql 
from step_2_tidy_files.tidy_admissions_discharges_and_create_mcl_tables import tidy_tables
from step_4_join_and_derived_files.create_joined_table_and_derived_columns import join_table
import os

def main():
    cwd = os.getcwd()
    print("Step 1: deduplicate admissions and discharges ")
    try:
        file_name = (cwd + "/step_1_deduplicate_files/1-deduplicate-admissions.sql")
        sql_file = open(file_name,"r")
        sql_script = sql_file.read()
        sql_file.close()
        inject_sql(sql_script,"deduplicate-admissions")

        file_name = (cwd + "/step_1_deduplicate_files/1-deduplicate-discharges.sql")
        sql_file = open(file_name,"r")
        sql_script = sql_file.read()
        sql_file.close()
        inject_sql(sql_script,"deduplicate-discharges")
    except Exception as e:
        print("!!! An error occured deduplicating admissions and discharges: " + e)


    print("Step 2: tidy admissions and discharges and create MCL tables")
    try:
        tidy_tables()
    except Exception as e:
        print("!!! An error occured tidying or creating MCL tables: " + e)

    print("Step 3: fix admissions and discharges issues") 
    try:
        file_name = (cwd + "/step_3_fix_record_files/2a-admissions-manually-fix-records.sql")
        sql_file = open(file_name,"r")
        sql_script = sql_file.read()
        sql_file.close()
        inject_sql(sql_script,"admissions-manually-fix-records")

        file_name = (cwd + "/step_3_fix_record_files/2b-discharges-manually-fix-records.sql")
        sql_file = open(file_name,"r")
        sql_script = sql_file.read()
        sql_file.close()
        inject_sql(sql_script,"discharges-manually-fix-records")
    except Exception as e:
        print("!!! An error occured fixing admissions and discharge tables: " + e)

    print("Step 4: create join and derived tables")
    try:
        join_table()
    except Exception as e:
        print("!!! An error occured joining tables: " + e)

    print("Step 5: grant access")
    try:
        file_name = (cwd + "/step_5_access_files/3-grant-usage-on-tables.sql")
        sql_file = open(file_name,"r")
        sql_script = sql_file.read()
        sql_file.close()
        inject_sql(sql_script,"grant-usage-on-tables")
    except Exception as e:
        print("!!! An error occured granting access to new tables: " + e)

    print("Data pipeline complete!")

if __name__ == "__main__":
    main()