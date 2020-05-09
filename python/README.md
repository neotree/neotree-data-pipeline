# End to End Data Pipeline

## Overview

The data pipeline process ingests raw admission and discharge data in the `public.sessions` table and runs a series of python and sql tranformations. These transformation generate a series of tables that be easily consumed by reporting applications.

Implementing the data pipeline process reduces the amount of complex and fragile logic required in a reporting application. The data pipeline should be owned by the IT development and support team, freeing up time from report builders and improving reliability.

Building the data pipeline in python and sql removes any dependencies on proprietory software removing barriers for adoption.

- 

## Environment Setup
Run the following to install python dependencies on virtual environment:

```
$ pip install -r requirements.txt
```


## Folder structure (TBC)
Insert description of folder structure and what is in the folders.

```bash
├── README.md
├── common_files
│   ├── __pycache__
│   │   ├── config.cpython-37.pyc
│   │   └── sql_functions.cpython-37.pyc
│   ├── config.py
│   ├── database.ini
│   └── sql_functions.py
├── create_joined_table.py
├── data_pipeline.py
├── requirements.txt
├── step_1_deduplicate_files
│   ├── 1-deduplicate-admissions.sql
│   └── 1-deduplicate-discharges.sql
├── step_2_tidy_files
│   ├── __pycache__
│   │   ├── extract_key_values.cpython-37.pyc
│   │   └── json_restructure.cpython-37.pyc
│   ├── extract_key_values.py
│   └── json_restructure.py
├── step_3_fix_record_files
│   ├── 2a-admissions-manually-fix-records.sql
│   └── 2b-discharges-manually-fix-records.sql
├── step_4_join_files
│   ├── __pycache__
│   │   └── create_derived_columns.cpython-37.pyc
│   └── create_derived_columns.py
├── step_5_access_files
│   └── 3-grant-usage-on-tables.sql
└── tidy_admissions_discharges_and_create_derived_tables.py
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
... Writing MCL count output back to the database
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