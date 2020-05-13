# End to End Data Pipeline

## Overview

The data pipeline process ingests raw admission and discharge data in the `public.sessions` table and runs a series of python and sql tranformations. These transformation generate a series of tables that be easily consumed by reporting applications.

Implementing the data pipeline process reduces the amount of complex and fragile logic required in a reporting application. The data pipeline should be owned by the IT development and support team, freeing up time from report builders and improving reliability.

Building the data pipeline in python and sql removes any dependencies on proprietory software removing barriers for future adoption.

Input:
- `public.sessions`

outputs:
- `derived.admissions`
- `derived.discharges`
- `derived.joined_admissions_and_discharges`
- a serices of MCL `explode_` tables (the default is to explode all mcl columns in the admissions and discharages tables)

## Environment Setup
Run the following to install the required python dependencies on any virtual environment:

```
$ pip install -r requirements.txt
```


## Folder structure (TBC)
The following is a decription of the directory structures and functions:

```bash
├── README.md
├── common_files                               <- files that are used across different functions
│   ├── config.py                              <- used to connect to postgres
│   ├── database.ini                           <- used to store credentials
│   └── sql_functions.py                       <- includes common read, write and inject sql functions
├── create_joined_table_and_derived_columns.py <- creates the joined admissions and discharge table and derived columns
├── data_pipeline.py                           <- runs the end to end data pipeline process
├── requirements.txt                           <- used to set up python environment
├── step_1_deduplicate_files                   <- files that deduplicate admissions and discharges
│   ├── 1-deduplicate-admissions.sql           <- sql script to deduplicate admissions
│   └── 1-deduplicate-discharges.sql           <- sql script to deduplicate discharges
├── step_2_tidy_files                          <- files to tidy admissions and discharges
│   ├── explode_mcl_columns.py                 <- used to explode mcl columns and save tables in postgres
│   ├── extract_key_values.py                  <- used to extract json key-value pairs
│   └── json_restructure.py                    <- used to manage MCL logic
├── step_3_fix_record_files                    <- files to fix admissions and discharge records manually e.g. malformed uid's
│   ├── 2a-admissions-manually-fix-records.sql <- sql script to fix admissions
│   └── 2b-discharges-manually-fix-records.sql <- sql script to fix discharges
├── step_4_join_and_derived_files              <- files used by create_joined_table_and_derived_columns.py
│   └── create_derived_columns.py              <- create new derived columns for the join table
├── step_5_access_files                        <- files to grant access
│   └── 3-grant-usage-on-tables.sql            <- sql script to grant access
└── tidy_admissions_discharges_and_create_mcl_tables.py <- creates tidy admissions, discharge and derived columns
```


## Pre-requisites

Before the pipeline can be run you need to create a `database.ini` file in the `python -> common_files` directory with the following template:

```
[postgresql]
host=
database=
user=
password=
```

This is then used to connect to the correct database using the appropriate credentials. Remember to add `database.ini` to the `.gitignore` file.

## Running this step in the data pipeline

```
$ python data_pipeline.py
```

The output should be as follows:

```
$ python data_pipeline.py
Step 1: deduplicate admissions and discharges 
... deduplicate-admissions has successfully run
... deduplicate-discharges has successfully run
Step 2: tidy admissions and discharges and create MCL tables
... Starting process to create tidied admissions, discharges and MCL tables (derived.admissions and derived.discharges)
... Fetching raw admission and discharge data
... Extracting keys
... Creating normalized dataframes - one for admissions and one for discharges
... Writing the tidied admission and discharge back to the database
... Creating MCL count tables
... Tidy script completed!
Step 3: fix admissions and discharges issues
... admissions-manually-fix-records has successfully run
... discharges-manually-fix-records has successfully run
Step 4: create join and derived tables
... Starting script to create joined table
... Fetching admissions and discharges data
... Creating joined admissions and discharge table
... Writing the output back to the database
... Join script completed!
Step 5: grant access
... grant-usage-on-tables has successfully run
Data pipeline complete!
```