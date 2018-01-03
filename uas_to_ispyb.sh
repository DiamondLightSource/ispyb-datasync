#!/bin/sh

ROOT_FOLDER=/usr/local/dbscripts/propagation
LOG_FOLDER=/exports/propagation-logs

. /etc/profile.d/modules.sh

module --silent load oracle
module --silent load python/ana

# Permissions for log files 
umask u=rw,g=rw,o=

python $ROOT_FOLDER/uas_to_ispyb.py -c $ROOT_FOLDER/../config/credentials.cfg -l $LOG_FOLDER/uas_to_ispyb.log --gt=100 2> $LOG_FOLDER/uas_to_ispyb.err

