# Creating the derived admissions and discharges tables

## Overview

The most complicated step in the data pipeline takes the unstructured JSON output by the mobile app and converts it into a tidy admissions and discharges table, with values for different fields stored in easy-to-query columns.

This step in the data pipeline is written in Python.

## Environment Setup
Run the following to install python dependencies on virtual environment:

```
$ pip install -r requirements.txt
```


## Folder structure (TBC)
Insert description of folder structure and what is in the folders.


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