# neotree-data-pipeline

This repo includes playbooks for the data pipeline that takes the raw data POSTed by the NeoTree app and turns it into a tidy table suitable for visualising in a Business Intelligence tool.

## Overview

The data pipeline is divided into three stages:

1. Steps performed directly on the data in the database in SQL
  - break out the admission and discharge data from the raw `sessions` table
  - deduplciate the admission and discharge data
2. A set of steps performed in Python that transform the entries in the admission and discharge data (in an unstructured JSON format) into a clean tabular format suitable for easy visualization
3. A set of steps performed in SQL to manually apply any fixes to individual entries and join the admissions and discharges data

Steps (1) and (3) are orchestrated using SQL-Runner. Step (2) is a Python script.

## Pre-requisites

To run the data pipeline you need:

* Credentials to access a database with NeoTree data
* A local copy of [sql-runner](https://github.com/snowplow/sql-runner) to run steps (1) and (3) 
* To create a `database.ini` file in the `python` directory, so that step (2) can run successfully

### Setting up SQL-Runner

On a Mac:

```
$ wget http://dl.bintray.com/snowplow/snowplow-generic/sql_runner_0.8.0_darwin_amd64.zip
$ unzip sql_runner_0.8.0_darwin_amd64.zip
$ ./sql-runner -usage
```

On Linux:

```
$ wget http://dl.bintray.com/snowplow/snowplow-generic/sql_runner_0.8.0_linux_amd64.zip
$ unzip sql_runner_0.8.0_linux_amd64.zip
$ ./sql-runner -usage
```

### Creating the database.ini file

```
touch python/database.ini
```

Then in your favorite text editor insert the following contents into the `database.ini` file with your database credentials:

```
[postgresql]
host=ENTER_DATABASE_HOST_HERE
database=ENTER_DATABASE_NAME_HERE
user=ENTER_USERNAME_HERE
password=ENTER_PASSWORD_HERE
```

## Running the data pipeline

### Step 1: separating out the admissions and discharges records, and deduplicating them

The first step in the data pipeline does the following:

* Fetches the admissions and discharges records from the raw jsonsessions table
* Deduplicates them
* Write them out to two separate tables (`scratch.deduplicated_admissions` and `scratch.deduplicated_discharges`).

This process is written in SQL. It is run using SQL-Runner as follows:

```
$ sql-runner -playbook playbooks/deduplicate_admissions_and_discharges.yml.tmpl -var host=ENTER_DATABASE_HOST_HERE,username=ENTER_USERNAME_HERE,port=5432,database=ENTER_DATABASE_NAME_HERE,password=ENTER_PASSWORD_HERE
```

It could alternatively be run directly using `psql`.

### Step 2: transforming the JSON for admissions and discharges so that we have two tables that are easy to query

This step takes the admission and discharge tables that have been deduplicated, but contain all the interesting data in a hard-to-work-with JSON, and transform the JSON into a tidy set of columns.

This part of the data pipeline is written in Python and run as follows:

```
$ python python/tidy_admissions_and_discharges_table.py
```

The resulting tables created in Postgres are `derived.admissions` and `derived.discharges`.

### Step 3: fix up the admissions and discharges data

Sometimes it is necessary to manually edit the data in the database because of errors made when the data was entered into the tablet. For example, the UID for a discharge record might have an innaccurate character. Because of this character it is not successfully joined onto the corresponding admission record. (The join uses the UID.)

Any updates to the data are made to the `derived.admissions` or `derived.discharges` tables. Updates should be written as `UPDATE` statements that are added to the [sql/common/5-admissions-manually-fix-records.sql](/neotree/neotree-data-pipeline/blob/master/sql/common/5-admissions-manually-fix-records.sql) or [sql/common/5-discharges-manually-fix-records.sqlcommon/5-ad](/neotree/neotree-data-pipeline/blob/master/sql/common/5-discharges-manually-fix-records.sql) files. These are therefore run each time the data pipeline is run on the derived tables: **we do not ever modify the raw data output from the app**.

This step can be run with SQL-Runner as follows:

```
$ sql-runner -playbook playbooks/fix_data.yml.tmpl -var host=ENTER_DATABASE_HOST_HERE,username=ENTER_USERNAME_HERE,port=5432,database=ENTER_DATABASE_NAME_HERE,password=ENTER_PASSWORD_HERE
```

It can alternatively be run using `psql`.

### Step 4: joining the admissions and discharges table and creating other derived views

This step in the pipeline joins the admissions and discharges table. It also creates other tables that are required for reporting.

This step is written in Python and run as follows:

```
$ python python/create_joined_and_other_derived_tables.py
```

It creates the following tables:

* `derived.joined_admissions_discharges`
* `derived.count_admission_reason`
* `derived.count_cont_death_causes`

### Step 5: granting permissions on the newly created tables

This step is run as follows:

```
$ sql-runner -playbook playbooks/deduplicate_admissions_and_discharges.yml.tmpl -var host=ENTER_DATABASE_HOST_HERE,username=ENTER_USERNAME_HERE,port=5432,database=ENTER_DATABASE_NAME_HERE,password=ENTER_PASSWORD_HERE
```
