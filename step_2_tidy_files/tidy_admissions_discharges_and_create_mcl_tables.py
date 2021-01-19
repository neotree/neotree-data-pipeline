# Import created modules (need to be stored in the same directory as notebook)
from step_2_tidy_files.extract_key_values import get_key_values
from step_2_tidy_files.explode_mcl_columns import explode_column
from step_2_tidy_files.create_derived_columns import create_columns
from common_files.sql_functions import read_table
from common_files.sql_functions import create_table

# Import libraries
import pandas as pd
from datetime import datetime as dt
import numpy as np
import logging


def tidy_tables():
    logging.info(
        "... Starting process to create tidied admissions, discharges and MCL tables (derived.admissions and derived.discharges)")

    # Read the raw admissions and discharge data into dataframes
    logging.info("... Fetching raw admission and discharge data")
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
        logging.error("!!! An error occured fetching the data: ")
        raise e

    # Now let's fetch the list of properties recorded in that table
    logging.info("... Extracting keys")
    try:
        adm_new_entries, adm_mcl = get_key_values(adm_raw)
        dis_new_entries, dis_mcl = get_key_values(dis_raw)
    except Exception as e:
        logging.error("!!! An error occured extracting keys: ")
        raise e

    # Create the dataframe (df) where each property is pulled out into its own colum
    logging.info(
        "... Creating normalized dataframes - one for admissions and one for discharges")
    try:
        adm_df = pd.json_normalize(adm_new_entries)
        adm_df.set_index('uid', inplace=True)
        dis_df = pd.json_normalize(dis_new_entries)
        dis_df.set_index('uid', inplace=True)
        # change data types for date time columns
        # watch out for time zone (tz) issues if you change code (ref: https://github.com/pandas-dev/pandas/issues/25571)

        # admissions tables
        # remove timezone in string to fix issues caused by converting to UTC
        adm_df['DateTimeAdmission.value'] = adm_df['DateTimeAdmission.value'].map(
            lambda x: str(x)[:-4])
        adm_df['DateTimeAdmission.value'] = pd.to_datetime(
            adm_df['DateTimeAdmission.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        adm_df['EndScriptDatetime.value'] = adm_df['EndScriptDatetime.value'].map(
            lambda x: str(x)[:-4])
        adm_df['EndScriptDatetime.value'] = pd.to_datetime(
            adm_df['EndScriptDatetime.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        adm_df['DateHIVtest.value'] = adm_df['DateHIVtest.value'].map(lambda x: str(x)[
                                                                      :-4])
        adm_df['DateHIVtest.value'] = pd.to_datetime(
            adm_df['DateHIVtest.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        adm_df['ANVDRLDate.value'] = adm_df['ANVDRLDate.value'].map(lambda x: str(x)[
                                                                    :-4])
        adm_df['ANVDRLDate.value'] = pd.to_datetime(
            adm_df['ANVDRLDate.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)

        # Remove Space From BW.Value :: Issue Was Affecting Dev Database
        if 'BW .label' in adm_df.columns: 
            adm_df['BW.label'] = adm_df['BW .label']
            adm_df.drop('BW .label',axis='columns',inplace=True)

        if 'BW .value' in adm_df.columns:
            adm_df['BW.value'] = adm_df['BW .value']
            adm_df.drop('BW .value', axis='columns', inplace=True)

        # discharges tables
        dis_df['DateAdmissionDC.value'] = dis_df['DateAdmissionDC.value'].map(
            lambda x: str(x)[:-4])
        dis_df['DateAdmissionDC.value'] = pd.to_datetime(
            dis_df['DateAdmissionDC.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        dis_df['DateDischVitals.value'] = dis_df['DateDischVitals.value'].map(
            lambda x: str(x)[:-4])
        dis_df['DateDischVitals.value'] = pd.to_datetime(
            dis_df['DateDischVitals.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        dis_df['DateDischWeight.value'] = dis_df['DateDischWeight.value'].map(
            lambda x: str(x)[:-4])
        dis_df['DateDischWeight.value'] = pd.to_datetime(
            dis_df['DateDischWeight.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        dis_df['DateTimeDischarge.value'] = dis_df['DateTimeDischarge.value'].map(
            lambda x: str(x)[:-4])
        dis_df['DateTimeDischarge.value'] = pd.to_datetime(
            dis_df['DateTimeDischarge.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        dis_df['EndScriptDatetime.value'] = dis_df['EndScriptDatetime.value'].map(
            lambda x: str(x)[:-4])
        dis_df['EndScriptDatetime.value'] = pd.to_datetime(
            dis_df['EndScriptDatetime.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        dis_df['DateWeaned.value'] = dis_df['DateWeaned.value'].map(lambda x: str(x)[
                                                                    :-4])
        dis_df['DateWeaned.value'] = pd.to_datetime(
            dis_df['DateWeaned.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        dis_df['DateTimeDeath.value'] = dis_df['DateTimeDeath.value'].map(
            lambda x: str(x)[:-4])
        dis_df['DateTimeDeath.value'] = pd.to_datetime(
            dis_df['DateTimeDeath.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        # Make changes to admissions to match fields in power bi

        adm_df = create_columns(adm_df)

    except Exception as e:
        logging.error(
            "!!! An error occured normalized dataframes/changing data types: ")
        raise e

    # Now write the cleaned up admission and discharge tables back to the database
    logging.info(
        "... Writing the tidied admission and discharge back to the database")
    try:
        adm_tbl_n = 'admissions'
        dis_tbl_n = 'discharges'

    
        logging.info("STEP-2 DISC:",dis_df)

        create_table(adm_df, adm_tbl_n)
        create_table(dis_df, dis_tbl_n)
    except Exception as e:
        logging.error(
            "!!! An error occured writing admissions and discharge output back to the database: ")
        raise e

    logging.info("... Creating MCL count tables")
    try:
        explode_column(adm_df, adm_mcl)
        explode_column(dis_df, dis_mcl)
    except Exception as e:
        logging.error("!!! An error occured creating MCL count tables: ")
        raise e

    logging.info("... Tidy script completed!")
