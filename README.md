# ispyb-propagation

These scripts are used for propagating certain kinds of metadata 
from our user database and LDAP into the ISPyB database. Currently,
the propagated metadata is:
* proposals
* sessions
* persons
* session - person associations
* staff user permissions based on LDAP groups  

### Requirements
* Python 2.7
* The MySQLdb Python package
* The cx_Oracle Python package (for reading the user database)  
* An ISPyB database on either MariaDB 10.0+ or MySQL 5.6+
* A Diamond user database
