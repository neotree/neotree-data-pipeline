from common_files.sql_functions import inject_sql 
import tidy_admissions_discharges_and_create_derived_tables as tt
import create_joined_table as cj

def main():
    file_name = ("1-deduplicate-admissions.sql")
    sql_file = open(file_name,"r")
    sql_script = sql_file.read()
    sql_file.close()
    inject_sql(sql_script,"deduplicate-admissions")
    
    file_name = ("1-deduplicate-discharges.sql")
    sql_file = open(file_name,"r")
    sql_script = sql_file.read()
    sql_file.close()
    inject_sql(sql_script,"deduplicate-discharges")

if __name__ == "__main__":
    main()