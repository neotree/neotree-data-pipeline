from common_files.test_sql_functions import inject_sql 
import tidy_admissions_and_discharges_table as tt
import create_joined_and_other_derived_tables as cj

def main():

    print("Step 1: deduplicate admissions and discharges ")
    try:
        file_name = "step_1_duduplicate_files/1-deduplicate-admissions.sql"
        sql_file = open(file_name,"r")
        sql_script = sql_file.read()
        inject_sql(sql_script,file_name)

        file_name = "step_1_duduplicate_files/1-deduplicate-discharges.sql"
        sql_file = open(file_name,"r")
        sql_script = sql_file.read()
        inject_sql(sql_script,file_name)
    except:
        print("An error occured deduplicating admissions and discharges")


    print("Step 2: tidy admissions, discharges and count tables")
    try:
        tt.tidy_tables()
    except:
        print("An error occured tidying admissions and discharges tables ")

    print("Step 3: fix admissions and discharges issues") 
    try:
        file_name = "step_3_fix_record_files/2a-admissions-manually-fix-records.sql"
        sql_file = open(file_name,"r")
        sql_script = sql_file.read()
        inject_sql(sql_script,file_name)

        file_name = "step_3_fix_record_files/2b-discharges-manually-fix-records.sql"
        sql_file = open(file_name,"r")
        sql_script = sql_file.read()
        inject_sql(sql_script,file_name)
    except:
        print("An error occured fixing admissions and discharge tables")

    print("Step 4: create join and derived tables")
    try:
        cj.join_table()
    except:
        print("An error occured joining tables")

    print("Step 5: grant access")
    try:
        file_name = "step_5_access_files/3-grant-usage-on-tables.sql"
        sql_file = open(file_name,"r")
        sql_script = sql_file.read()
        inject_sql(sql_script,file_name)
    except:
        print("An error occured granting access to new tables")

    print("Data pipeline complete!")

if __name__ == "__main__":
    main()