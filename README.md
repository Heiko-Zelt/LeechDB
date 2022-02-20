# LeechDB
Simple data migration from Oracle to MySQL database

This little program helps to migrate data from an Oracle to a MySQL/Maria database.
It reads a schema from an Oracle database via JDBC and writes the contents of the tables into files.
The files contain MySQL-INSERT-Statements to be imported via mysql command line client.
If columns contains large objects (CLOBs or BLOBs) they are written to separate files and referenced.
Reads configuration file "export.properties" from current working directory.
It is assumed that the schema in the target database already exists e.g. created using "Liquibase".
Before importing the new data, foreign key constraints must be switched off temporarily.
