# redshift_show_create_table
python script, 'show create table' equivalent for aws redshift. Command-line syntax is similar to pg_dump.

# Command-line parameters

Basic usage: 
```
show_create_table.py -h HOST -U USER -d DBNAME [-W PASSWORD] [-p PORT]
[-f FILE] [-F {directory}] [-n SCHEMANAME]
[-t TABLENAME] [-c PGPASS_FILE]
```

## Required parameters
* [-h/--host=] HOSTNAME: hostname for Redshift database 
* [-U/--user=] USERNAME: username to connect to Redshift database with
* [-d/--dbname=] DBNAME: name of database to connect to on host

## Optional parameters
* [-p/--port=] PORT: port to connect to, defaults to 5432
* [-f/--file=] FILE: file/directory to write output to, defaults to standard output
* [-F/--format=] FORMAT: requires --file, currently only valid option (and default) is 'directory',
which creates directories for each non-system schema and creates a separate SQL file for each table/view
* [-n/--schema=] SCHEMANAME: name of schema to show tables from, if none provided it will iterate over all 
non-system schemas
* [-t/--table=] TABLENAME: name of a single table to dump, if none provided it will iterate over all in schema
* [-W/--password=] PASSWORD: Redshift password for username. If not provided, it will look for .pgpass credential file under user home directory, or file specified in '-c/--credential' argument.
* [-c/--credential=] PGPASS_FILE: the path to the file containing Redshift connection credential, default path is <user_home_directory>/.pgpass. See https://www.postgresql.org/docs/9.1/static/libpq-pgpass.html
