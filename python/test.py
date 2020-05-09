from common_files.sql_functions import inject_sql 
import tidy_admissions_discharges_and_create_derived_tables as tt
import create_joined_table as cj
import os

def main():
    cwd = os.getcwd()
    file_name = (cwd + "/step_1_deduplicate_files/1-deduplicate-admissions.sql")
    sql_file = open(file_name,"r")
    sql_script = sql_file.read()
    inject_sql(sql_script,"1-deduplicate-admissions.sql")

if __name__ == "__main__":
    main()