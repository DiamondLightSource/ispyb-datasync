[![Build Status](https://travis-ci.org/DiamondLightSource/ispyb-propagation.svg?branch=master)](https://travis-ci.org/DiamondLightSource/ispyb-propagation)
[![Coverage Status](https://coveralls.io/repos/github/DiamondLightSource/ispyb-propagation/badge.svg?branch=master)](https://coveralls.io/github/DiamondLightSource/ispyb-propagation?branch=master)

# datasync

This package is used for synching certain kinds of metadata from other
data sources into an ISPyB database. Currently, the synched metadata is:
* proposals
* sessions
* session types
* persons
* session - person associations
* proposal - person associations
* staff user permissions based on LDAP groups  

See the [```Wiki```](https://github.com/DiamondLightSource/ispyb-propagation/wiki) for details.

### Requirements
* Python 2.7
* The mysql.connector Python package
* The cx_Oracle Python package and an Oracle client (for reading the user database)  
* An ISPyB database on either MariaDB 10.0+ or MySQL 5.6+
* A Diamond user database
