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

To run the first step we use SQL-Runner to run the `dedupliate_admissions_and_discharges` playbook:

```
$ sql-runner -playbook playbooks/deduplicate_admissions_and_discharges.yml.tmpl -var host=ENTER_DATABASE_HOST_HERE,username=ENTER_USERNAME_HERE,port=5432,database=ENTER_DATABASE_NAME_HERE,password=ENTER_PASSWORD_HERE
```

Then we run the second step in Python

```
$ python python/create_reporting_table.py
```

Lastly we run the `fix_data_and_join_admissions_and_discharges` playbook:

```
$ sql-runner -playbook playbooks/deduplicate_admissions_and_discharges.yml.tmpl -var host=ENTER_DATABASE_HOST_HERE,username=ENTER_USERNAME_HERE,port=5432,database=ENTER_DATABASE_NAME_HERE,password=ENTER_PASSWORD_HERE
```

## Running the update playbook

The update playbook updates the tables and views in-place. This is marginally faster than running the setup playbook

```
$ sql-runner -playbook playbooks/update.yml.tmpl -var host=ENTER_DATABASE_HOST_HERE,username=ENTER_USERNAME_HERE,port=ENTER_PORT_HERE,database=ENTER_DATABASE_NAME_HERE,password=ENTER_PASSWORD_HERE
```

## Modifying the data as part of data cleaning

Sometimes it is necessary to update lines of data in the database if this has been found to have been inacurately entered in the NeoTree app.

Please NEVER update any lines of the raw data in Postgres. Instead, add UPDATE statements to the `/sql/common/5-admissions-manually-fix-records.sql` file (to fix admissions records), or the `[sql/common/5-discharges-manually-fix-records.sql` (to fix the discharge records).

Once the files have been updated please rerun the `update` playbook and commit the updated file to Github.