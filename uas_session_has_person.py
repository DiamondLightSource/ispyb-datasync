#!/usr/bin/env python

import uaspropagate
import sys
import logging

# Speed-up options
# Whenever a new Person is found, add its Session associations as well
# Whenever a new Session is found, add its Person associations as well

# Keep a copy (in memory?) of the UAS result-set from the previous run, compare the new result-set with that, add any new associations  
# Run a process for each beamline only for that beamline's sessions ...

usage = """Script for replicating master data in UAS into ISPyB tables
Specifically, this propagates data into the following tables:
Session_has_Person, 
Syntax: %s -c <configuration file> [-rp]
Arguments: 
     -h|--help : display this help
     -c|--conf <conf file> : use the given configuration file
     -l|--log <log file>: use the given log file""" % sys.argv[0]


uaspropagate.init(usage)
uaspropagate.propagate_session_has_persons()
uaspropagate.tidy()

