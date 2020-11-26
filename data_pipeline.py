from common_files.sql_functions import inject_sql 
from common_files.format_error import formatError;
from step_2_tidy_files.tidy_admissions_discharges_and_create_mcl_tables import tidy_tables
from step_4_join_and_derived_files.create_joined_table_and_derived_columns import join_table
import os
import logging
import time
import sys

def main():
    start = time.time();
    cwd = os.getcwd()
    logging.info("Step 1: deduplicate admissions and discharges ")
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
        logging.error("!!! An error occured deduplicating admissions and discharges: ")
        logging.error(formatError(e))
        sys.exit(1)


    logging.info("Step 2: tidy admissions and discharges and create MCL tables")
    try:
        tidy_tables()
    except Exception as e:
        logging.error("!!! An error occured tidying or creating MCL tables: ")
        logging.error(formatError(e))
        sys.exit(1)

    logging.info("Step 3: fix admissions and discharges issues") 
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
        logging.error("!!! An error occured fixing admissions and discharge tables: ")
        logging.error(formatError(e))
        sys.exit(1)

    logging.info("Step 4: create join and derived tables")
    try:
        join_table()
    except Exception as e:
        logging.error("!!! An error occured joining tables: ")

        logging.error(formatError(e))
        sys.exit(1)

    logging.info("Step 4b: create convenience views")
    try:
        file_name = (cwd + "/step_4b_create_convenience_views/create-convenience-views.sql")
        sql_file = open(file_name,"r")
        sql_script = sql_file.read()
        sql_file.close()
        inject_sql(sql_script,"create-convenience-views")
    except Exception as e:
        logging.error("!!! An error occured creating convenience views: ")
        logging.error(formatError(e))
        sys.exit(1)

    logging.info("Step 5: grant access")
    try:
        file_name = (cwd + "/step_5_access_files/3-grant-usage-on-tables.sql")
        sql_file = open(file_name,"r")
        sql_script = sql_file.read()
        sql_file.close()
        inject_sql(sql_script,"grant-usage-on-tables")
    except Exception as e:
        logging.error("!!! An error occured granting access to new tables: ")
        logging.error(formatError(e))
        sys.exit(1)

    end= time.time()
    execution_time = end-start
    execution_time_seconds=0
    execution_time_minutes = 0
    if execution_time>0:
        execution_time_minutes = round(execution_time//60);
        execution_time_seconds = round(execution_time % 60)

    logging.info("Data pipeline complete in {0} minutes {1} seconds!".format(execution_time_minutes,execution_time_seconds))

if __name__ == "__main__":
    main()