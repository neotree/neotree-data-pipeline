# Import created modules (need to be stored in the same directory as notebook)
import connect as cn
import read_data as rd
import extract_key_values as ekv
import merge_df as mdf
import create_postgres_table as cpt

# Import libraries
import pandas as pd
import sqlalchemy
import numpy as np
import json
import pytest

def main():
    print("Starting process to create admissions table")

    # Connect to database
    print("1. Connecting to database")
    conn = cn.database_connection()

    # Read the raw admissions data into the admin_raw dataframe
    admin_query = '''
            select 
                uid,
                ingested_at,
                "data"->'entries' as "entries"
            from scratch.deduplicated_admissions;
        '''
    admin_raw = rd.read_table(admin_query, conn)

    # Now let's fetch the list of properties recorded in that table
    print("2. Extracting keys")
    newentrieslist = ekv.get_key_values(admin_raw)

    # Create the dataframe (df) where each property is pulled out into its own colum
    print("3. Creating the normalized dataframe")
    df = pd.json_normalize(newentrieslist, max_level=2)

    # Add back the  ingested_at and session
    print("4. Merging records")
    admissions = mdf.merge_df(admin_raw,df)

    # Now write the table back to the database
    print("5. Writing the output back to the database")
    cpt.create_table(admissions)

    print("6. Script completed!")



if __name__ == "__main__":
    main()