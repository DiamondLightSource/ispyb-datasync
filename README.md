# datasync

This package is used for synching certain kinds of metadata from other
data sources into an ISPyB database. Currently, the synched metadata is:
* proposals
* sessions
* persons
* session - person associations
* staff user permissions based on LDAP groups  

### Requirements
* Python 2.7
* The mysql.connector Python package
* The cx_Oracle Python package and an Oracle client (for reading the user database)  
* An ISPyB database on either MariaDB 10.0+ or MySQL 5.6+
* A Diamond user database
