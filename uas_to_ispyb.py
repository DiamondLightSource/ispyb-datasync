#!/usr/bin/env python

import uaspropagate
import sys
import logging

usage = """Script for propagating master data in UAS into ISPyB tables:
New or updated rows in are propagated to the Proposal, BLSession and Person tables.
Additionally, for new BLSession and new Person rows, the associated relationships 
are also propagated (into Session_has_Person). 
Syntax: %s -c <configuration file> [-rp]
Arguments: 
     -h|--help : display this help
     -c|--conf <conf file> : use the given configuration file
     -l|--log <log file>: use the given log file""" % sys.argv[0]

uaspropagate.init(usage)
uaspropagate.propagate_proposals()
uaspropagate.propagate_sessions()
uaspropagate.propagate_session_types()
uaspropagate.propagate_persons()
uaspropagate.propagate_proposal_has_persons()
uaspropagate.propagate_session_has_persons()
uaspropagate.tidy()

