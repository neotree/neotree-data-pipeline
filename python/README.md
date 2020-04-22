# Creating the derived admissions and discharges tables

## Overview

The most complicated step in the data pipeline takes the unstructured JSON output by the mobile app and converts it into a tidy admissions and discharges table, with values for different fields stored in easy-to-query columns.

This step in the data pipeline is written in Python.

## Pre-requisites

Before the pipeline can be run you need to create a `database.ini` file in the `python` directory with the following template:

```
[postgresql]
host=
database=
user=
password=
```

This is then used to connect to the correct database using the appropriate credentials.

## Running this step in the data pipeline

```
$ python create_reporting_table.py
```

The output should be as follows:

```
$ python create_reporting_table.py
Starting process to create admissions table
1. Connecting to database
Connecting to the PostgreSQL database...
2. Extracting keys
3. Creating the normalized dataframe
4. Merging records
5. Writing the output back to the database
6. Script completed!
```