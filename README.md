# neotree-data-pipeline

This repo includes playbooks for the data pipeline that takes the raw data POSTed by the NeoTree app and turns it into a tidy table suitable for visualising in a Business Intelligence tool.

## Pre-requisites

To run the data pipeline you need:

* Credentials to access a database with NeoTree data
* A local copy of [sql-runner](https://github.com/snowplow/sql-runner)

## Setting up SQL-Runner

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

## Running the setup playbook

The setup playbook sets up and runs the data pipeline, including creating all the required derived tables and views. This is necessary if e.g. you are setting up the data pipeline for the first time.

```
$ sql-runner -playbook playbooks/setup.yml.tmpl -var host=ENTER_DATABASE_HOST_HERE,username=ENTER_USERNAME_HERE,port=ENTER_PORT_HERE,database=ENTER_DATABASE_NAME_HERE,password=ENTER_PASSWORD_HERE
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