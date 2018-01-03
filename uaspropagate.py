#!/usr/bin/env python

import MySQLdb
import cx_Oracle
import getopt
import logging
from logging.handlers import RotatingFileHandler
import sys
import os
import atexit
import signal
import time
import ConfigParser

def connect_to_uas(uas_user, uas_pw, uas_TNS):
    global uas_conn
    global uas_cursor
    try:
        uas_conn=cx_Oracle.connect(user=uas_user, password=uas_pw, dsn=uas_TNS)
    except:
        logging.getLogger().exception("%s: error while connecting to UAS DB :-(" % sys.argv[0])
    else:
        try:
            #uas_conn.autocommit(True)
            uas_conn.autocommit=True
        except AttributeError:
            pass
        logging.getLogger().info("%s: Connected to database (Oracle v. %s)" % (sys.argv[0], uas_conn.version))
        logging.getLogger().info("%s:    Database user: %s" % (sys.argv[0], uas_user))
        logging.getLogger().info("%s:    TNS name: %s" % (sys.argv[0], uas_TNS))
        last_UAS_cursor_time=time.time()
        try:
            uas_cursor = uas_conn.cursor()
        except:
            logging.getLogger().exception("%s: unable to create cursor :-(" % sys.argv[0])
        else:
            logging.getLogger().debug("%s: default cursor ok :-)" % sys.argv[0])

    return (uas_conn, uas_cursor)


def connect_to_ispyb(ispyb_host, ispyb_user, ispyb_pw, ispyb_db, ispyb_port=3306):
    global ispyb_conn
    global ispyb_cursor
    ispyb_conn = None
    ispyb_cursor = None
    try:
        ispyb_conn = MySQLdb.connect(host=ispyb_host, user=ispyb_user, passwd=ispyb_pw, db=ispyb_db, port=ispyb_port)
    except Exception as e:
        logging.getLogger().exception("%s: error while connecting to ISPyB DB :-(" % sys.argv[0])
        raise
    else:
        try:
            ispyb_conn.autocommit(True)
        except AttributeError:
            sys.exit("Failed to set autocommit.")

        logging.getLogger().info("%s: Connected to database %s on %s" % (sys.argv[0], ispyb_conn.get_server_info(), ispyb_conn.get_host_info()))
        logging.getLogger().info("%s:    Database user: %s" % (sys.argv[0], ispyb_user))
        logging.getLogger().info("%s:    DB name: %s" % (sys.argv[0], ispyb_db))
        logging.getLogger().info("%s:    DB port: %s" % (sys.argv[0], ispyb_port))
            
        try:
            ispyb_cursor = ispyb_conn.cursor()
        except Exception as e:
            logging.getLogger().exception("%s: unable to create cursor :-(" % sys.argv[0])
            raise
        else:
            logging.getLogger().debug("%s: default cursor ok :-)" % sys.argv[0])

    return (ispyb_conn, ispyb_cursor)

def disconnect_from_db(conn):
    '''Release the connection previously created.'''
    if conn is not None:
        conn.close()
        conn = None
    return None


def do_ispyb_query(querystr, params, return_fetch=True, return_id=False, log_query=True):
    global ispyb_cursor
    if ispyb_cursor is None:
        sys.exit("Cursor not given.")

    if log_query:    
        logging.getLogger().debug(querystr + " " + str(params))  
    start_time=time.time()
    try:
        ret=ispyb_cursor.execute(querystr, params)
    except:
        logging.getLogger().exception("%s: exception running sql statement :-(" % sys.argv[0])
        logging.getLogger().exception(querystr + " " + str(params))
        raise
    else:
        if log_query:        
            logging.getLogger().debug("%s: query took %f seconds" %  (sys.argv[0], (time.time()-start_time)))

    if return_fetch:
        start_time=time.time()
        try:
            ret=ispyb_cursor.fetchall()
        except:
            logging.getLogger().exception("%s: exception fetching ispyb_cursor :-(" % sys.argv[0])
            raise
        if log_query:
            logging.getLogger().debug("%s: fetch took %f seconds" %  (sys.argv[0], (time.time()-start_time)))
    elif return_id:
        start_time=time.time()
            
        #try:
        #    ret=int(self.icat_conn.insert_id())
        #except:
        #    logging.getLogger().exception("%s: exception getting inserted id :-(" % sys.argv[0])
        #    raise

        ret = ispyb_cursor.lastrowid
        if log_query:
            logging.getLogger().debug("%s: id took %f seconds" % (sys.argv[0], (time.time()-start_time)))
    return ret

def do_uas_query(querystr, params, return_fetch=True, return_id=False, log_query=True):
    global uas_cursor
    if uas_cursor is None:
        sys.exit("Cursor not given.")

    if log_query:    
        logging.getLogger().debug(querystr + " " + str(params))  
    start_time=time.time()
    try:
        ret=uas_cursor.execute(querystr, params)
    except:
        logging.getLogger().exception("%s: exception running sql statement :-(" % sys.argv[0])
        logging.getLogger().exception(querystr + " " + str(params))
        raise
    else:
        if log_query:        
            logging.getLogger().debug("%s: query took %f seconds" %  (sys.argv[0], (time.time()-start_time)))

    if return_fetch:
        start_time=time.time()
        try:
            ret=uas_cursor.fetchall()
        except:
            logging.getLogger().exception("%s: exception fetching uas_cursor :-(" % sys.argv[0])
            raise
        if log_query:
            logging.getLogger().debug("%s: fetch took %f seconds" %  (sys.argv[0], (time.time()-start_time)))
    elif return_id:
        start_time=time.time()
            
        #try:
        #    ret=int(self.icat_conn.insert_id())
        #except:
        #    logging.getLogger().exception("%s: exception getting inserted id :-(" % sys.argv[0])
        #    raise

        ret = uas_cursor.lastrowid
        if log_query:
            logging.getLogger().debug("%s: id took %f seconds" % (sys.argv[0], (time.time()-start_time)))
    return ret

def insert_proposal(proposal, title, uas_id):
    code = str(proposal[0]) + str(proposal[1])
    num = int(proposal[2:])
    query = 'INSERT IGNORE INTO Proposal (proposalCode, proposalNumber, title, externalId, personId, blTimeStamp) VALUES (%s, %s, %s, unhex(%s), 1, NOW())'
    params = [code, str(num), title, uas_id] 
    return do_ispyb_query(query, params, False, True)

def update_proposal(title, uas_id, id):
    query = 'UPDATE Proposal SET title=%s, blTimeStamp = NOW(), externalId=unhex(%s) WHERE proposalId=%s'
    params = [title, uas_id, str(id)]
    do_ispyb_query(query, params, False, False)

def update_proposal_code(proposal_code, proposal_id):
    query = 'UPDATE Proposal SET proposalCode=%s WHERE proposalId=%s'
    params = [proposal_code, proposal_id]
    do_ispyb_query(query, params, False, False)

def delete_proposal(id):
    query = 'DELETE FROM ProposalHasPerson WHERE proposalId=%s'
    params = [str(id)]
    do_ispyb_query(query, params, False, False)

    query = 'DELETE FROM Proposal WHERE proposalId=%s'
    params = [str(id)]
    do_ispyb_query(query, params, False, False)

def retrieve_proposal_id(proposal_code, proposal_number):
    query = 'SELECT max(proposalId) FROM Proposal WHERE proposalCode=%s AND proposalNumber=%s'
    params = [proposal_code, proposal_number]
    rs = do_ispyb_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None and rs[0][0] != None:
        return int(rs[0][0])
    return None

def retrieve_proposal_id_for_uas_id(uas_id):
    query = 'SELECT max(proposalId) FROM Proposal WHERE externalId=unhex(%s)'
    params = [uas_id]
    rs = do_ispyb_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None and rs[0][0] != None:
        return int(rs[0][0])
    return None

def insert_session(uas_id, beamline, comments, start_date, end_date, session_name, beamline_operators, scheduled):
    code = str(session_name[0]) + str(session_name[1])
    i = session_name.find('-', 2)
    if i == -1:
        logging.getLogger().warning("Problem session_name: %s" % session_name)
        return
    try:
        num = int(session_name[2:i])
    except ValueError:
        logging.getLogger().warning("Problem session_name: %s" % session_name)
        return
    try:
        visit_number = int(session_name[i+1:])
    except ValueError:
        logging.getLogger().warning("Problem session_name: %s" % session_name)
        return
        
    proposal_id = retrieve_proposal_id(code, num)
    if proposal_id is None:
        return
    
    query = '''INSERT IGNORE INTO BLSession (proposalId, externalId, beamlineName, comments, startDate, endDate, visit_number, beamLineOperator, scheduled) 
VALUES (%s, unhex(%s), %s, %s, %s, %s, %s, %s, %s)'''
    params = [proposal_id, uas_id, beamline, comments, start_date, end_date, visit_number, beamline_operators, scheduled] 
    ispyb_session_id = do_ispyb_query(query, params, False, True)

    if ispyb_session_id is None:
        logging.getLogger().debug("ispyb_session_id is None!")

    uas_persons_rs = retrieve_uas_persons_for_session(uas_id)
    if uas_persons_rs != None:
        insert_persons_for_session(ispyb_session_id, uas_persons_rs)
    else:
        logging.getLogger().debug("uas_persons_rs is None!")
        
def update_session(uas_id, beamline, start_date, end_date, local_contacts, scheduled, id):
    query = '''UPDATE BLSession SET externalId=unhex(%s), beamlinename=%s, startDate=%s, endDate=%s, beamLineOperator=%s, scheduled=%s 
    WHERE sessionId=%s'''
    params = [uas_id, beamline, start_date, end_date, local_contacts, scheduled, str(id)]
    do_ispyb_query(query, params, False, False)

def session_has_data(id):
    query = """SELECT SUM(num) from ( 
SELECT COUNT(*) as num FROM DataCollectionGroup WHERE sessionid = %s 
UNION ALL 
SELECT COUNT(*) as num FROM DataCollection WHERE sessionid = %s 
UNION ALL 
SELECT COUNT(*) as num FROM EnergyScan WHERE sessionid = %s 
UNION ALL 
SELECT COUNT(*) as num FROM XFEFluorescenceSpectrum WHERE sessionid = %s 
UNION ALL 
SELECT COUNT(*) as num FROM ShippingHasSession WHERE sessionid = %s
UNION ALL
SELECT COUNT(*) as num FROM SaxsDataCollection WHERE blsessionid = %s
UNION ALL
SELECT COUNT(*) as num FROM SamplePlate WHERE blsessionid = %s
UNION ALL
SELECT COUNT(*) as num FROM Specimen WHERE blsessionid = %s
UNION ALL
SELECT COUNT(*) as num FROM BF_fault WHERE sessionid = %s
UNION ALL
SELECT COUNT(*) as num FROM RobotAction WHERE blsessionId = %s
UNION ALL
SELECT COUNT(*) as num FROM BeamlineAction WHERE sessionId = %s
UNION ALL
SELECT COUNT(*) as num FROM Dewar WHERE firstExperimentId = %s
) a"""
    params = [id] * 12

    rs = do_ispyb_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None:
        rows = int(rs[0][0])
        logging.getLogger().debug("session_has_data rows: %d" % rows)
        if rows > 0:
            return True 
    return False

def delete_session(id):
    if not session_has_data(id):
        query = 'DELETE FROM BLSession WHERE sessionId=%s'
        params = [str(id)]
        do_ispyb_query(query, params, False, False)  

def retrieve_session_id(uas_session_id):
    query = 'SELECT max(sessionId) FROM BLSession WHERE externalId=unhex(%s)'
    params = [uas_session_id]
    rs = do_ispyb_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None and rs[0][0] != None:
        return int(rs[0][0])
    return None

def insert_persons_for_session(ispyb_session_id, uas_persons_rs):
    prev_id = None
    for row in uas_persons_rs:
        id = row[0]
        uas_role = row[1]
        is_remote = 1 if row[2] == 0 else 0 if row[2] == 1 else None
        if id != prev_id: 
            role = uas_role_2_ispyb_role(uas_role)
            ispyb_person_id = retrieve_person_id(id)
            if ispyb_person_id is None:
                p_row = retrieve_uas_person_row(id) # lower(federal_id), title, given_name, family_name
                login = p_row[0] 
                if login is None:
                    continue
                title = p_row[1]
                given_name = p_row[2]
                family_name = p_row[3]
                
                ispyb_person_id = insert_person(id, login, title, given_name, family_name, insert_shp=False)

            insert_session_has_person(role, ispyb_session_id, ispyb_person_id, is_remote)
        prev_id = id

def insert_sessions_for_person(ispyb_person_id, uas_sessions_rs):
    prev_id = None 
    for row in uas_sessions_rs:
        if row[0] != prev_id: 
            is_remote = 1 if row[2] == 0 else 0 if row[2] == 1 else None
            insert_session_has_person(uas_role_2_ispyb_role(row[1]),
                                  retrieve_session_id(row[0]),
                                  ispyb_person_id,
                                  is_remote)
        prev_id = row[0]

def insert_session_has_person(role, session_id, person_id, is_remote):
    if session_id != None and person_id != None:
        query = '''INSERT IGNORE INTO Session_has_Person (sessionId, personId, role, remote) 
VALUES (%s, %s, %s, %s)'''
        params = [session_id, person_id, role, is_remote] 
        do_ispyb_query(query, params, False, False)
    else:
        if session_id is None:
            logging.getLogger().debug("session_id is None!")
        if person_id is None:
            logging.getLogger().debug("person_id is None!")

def update_session_has_person(uas_role, is_remote, ispyb_session_id, ispyb_person_id):
    query = 'UPDATE Session_has_Person SET role=%s, remote=%s WHERE sessionId=%s AND personId=%s'
    params = [uas_role_2_ispyb_role(uas_role), is_remote, ispyb_session_id, ispyb_person_id]
    do_ispyb_query(query, params, False, False)

def uas_role_2_ispyb_role(uas_role):
    ispyb_role = None
    if uas_role == 'PRINCIPAL_INVESTIGATOR':
        ispyb_role = 'Principal Investigator'
    elif uas_role == 'ALTERNATE_CONTACT':
        ispyb_role = 'Alternate Contact'
    elif uas_role == 'TEAM_LEADER':
        ispyb_role = 'Team Leader'
    elif uas_role == 'TEAM_MEMBER':
        ispyb_role = 'Team Member'
    elif uas_role == 'LOCAL_CONTACT':
        ispyb_role = 'Local Contact'
    elif uas_role == 'LOCAL_CONTACT_1ST':
        ispyb_role = 'Local Contact'
    elif uas_role == 'LOCAL_CONTACT_2ND':
        ispyb_role = 'Local Contact 2'
    elif uas_role == 'CO_INVESTIGATOR':
        ispyb_role = 'Co-Investigator'
    elif uas_role == 'DATA_ACCESS':
        ispyb_role = 'Data Access'
    return ispyb_role

def insert_session_type(uas_id, tag, session_name):
    session_id = retrieve_session_id(uas_id)
    if session_id != None:
        query = '''INSERT IGNORE INTO SessionType (sessionId, typeName) VALUES (%s, %s)'''
        params = [session_id, tag] 
        ispyb_session_id = do_ispyb_query(query, params, False, False)

def insert_person(uas_id, login, title, given_name, family_name, insert_shp=True):
    query = '''INSERT IGNORE INTO Person (externalId, login, title, givenName, familyName) 
VALUES (unhex(%s), %s, %s, %s, %s)'''
    params = [uas_id, login, title, given_name, family_name] 
    ispyb_person_id = do_ispyb_query(query, params, False, True)

    if ispyb_person_id is None:
        logging.getLogger().debug("ispyb_person_id is None!")
    
    if insert_shp:
        uas_sessions_rs = retrieve_uas_sessions_for_person(uas_id)
        if uas_sessions_rs != None:
            insert_sessions_for_person(ispyb_person_id, uas_sessions_rs)
        else:
            logging.getLogger().debug("uas_sessions_rs is None!")

def update_person(uas_id, title, given_name, family_name, id):
    query = 'UPDATE Person SET externalId=unhex(%s), title=%s, givenName=%s, familyName=%s WHERE personId=%s'
    params = [uas_id, title, given_name, family_name, str(id)]
    do_ispyb_query(query, params, False, False)

def retrieve_person_id(uas_person_id):
    query = 'SELECT max(personId) FROM Person WHERE externalId=unhex(%s)'
    params = [uas_person_id]
    rs = do_ispyb_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None and rs[0][0] != None:
        return int(rs[0][0])
    return None

def retrieve_uas_person_row(uas_person_id):
    query = """SELECT lower(federal_id), title, given_name, family_name 
FROM facility_user
WHERE person_id=hextoraw('%s')""" % uas_person_id
    rs = do_uas_query(query, [], return_fetch=True, return_id=False, log_query=True)
    if rs != None and len(rs) > 0 and rs[0] != None:
        return rs[0]
    return None

def retrieve_uas_persons_for_session(uas_id):
    query = """SELECT person_id, "role", on_site
FROM (
SELECT rawtohex(lc.person_id) person_id, decode(lc.local_contact_level,0,'LOCAL_CONTACT_1ST',1,'LOCAL_CONTACT_2ND','LOCAL_CONTACT') "role", 1 on_site, 1 rank  
FROM local_contact lc
  INNER JOIN facility_user fu on lc.person_id = fu.person_id
WHERE fu.federal_id is not NULL AND lc.session_id=hextoraw('%s') 
UNION ALL
SELECT rawtohex(iu.person_id) person_id, iu.role, iu.on_site, 2 rank  
FROM investigation_user iu 
  INNER JOIN facility_user fu on iu.person_id = fu.person_id
WHERE fu.federal_id is not NULL AND iu.session_id=hextoraw('%s') 
)
ORDER BY person_id""" % (uas_id, uas_id)
    params = []
    rs = do_uas_query(query, params, return_fetch=True, return_id=False, log_query=True)
    if rs != None and len(rs) > 0 and rs[0] != None and rs[0][0] != None:
        return rs
    return None

def retrieve_uas_sessions_for_person(uas_id):
    query = """SELECT rawtohex(iu.session_id), iu.role, iu.on_site 
FROM investigation_user iu
  INNER JOIN shift s ON s.session_id = iu.session_id
WHERE iu.person_id=hextoraw('%s') AND s.state <> 'Cancelled'
ORDER BY iu.session_id""" % uas_id   
    params = []
    rs = do_uas_query(query, params, return_fetch=True, return_id=False, log_query=True)
    if rs != None and len(rs) > 0 and rs[0] != None and rs[0][0] != None:
        return rs
    return None


def insert_protein(uas_id, proposal_id, name, acronym, origin_txt):
    query = """INSERT IGNORE INTO Protein (externalId, proposalId, name, acronym, proteinType) 
VALUES (
  unhex(%s), 
  %s,
  %s, 
  %s, 
  %s
)"""
    params = [uas_id, proposal_id, name, acronym, origin_txt] 
    return do_ispyb_query(query, params, False, True)

def update_protein(name, acronym, id):
    query = 'UPDATE Protein SET name=%s, acronym=%s WHERE proteinId=%s'
    params = [name, acronym, str(id)]
    do_ispyb_query(query, params, False, False)

def update_protein_uas_id(uas_id, id):
    query = 'UPDATE Protein SET externalId=unhex(%s) WHERE proteinId=%s'
    params = [uas_id, str(id)]
    do_ispyb_query(query, params, False, False)
    
def update_protein_name(name, id):
    query = 'UPDATE Protein SET name=%s WHERE proteinId=%s'
    params = [name, str(id)]
    do_ispyb_query(query, params, False, False)

def retrieve_number_of_proteins_for_uas_id(uas_id):
    query = 'SELECT count(*) FROM Protein WHERE externalId=unhex(%s)'
    params = [uas_id]
    rs = do_ispyb_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None and rs[0][0] != None:
        return int(rs[0][0])
    return None
   
def retrieve_number_of_proteins_for_proposal_and_acronym(uas_id, acronym):
    query = 'SELECT count(*) FROM Protein WHERE proposalId in (SELECT proposalId FROM Proposal WHERE externalId=unhex(%s)) AND acronym=%s'
    params = [uas_id, acronym]
    rs = do_ispyb_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None and rs[0][0] != None:
        return int(rs[0][0])
    return None

def propagate_proposal_has_persons():
    uas_proposal_has_persons_select = """SELECT rawtohex(pu.proposal_id) proposal_id, rawtohex(pu.person_id) person_id, pu.role
FROM proposal_user pu
  INNER JOIN facility_user fu on fu.person_id = pu.person_id
  INNER JOIN proposal p on p.id = pu.proposal_id
WHERE fu.federal_id is not NULL and p.state in ('Open', 'Closed')
"""
    ispyb_proposal_has_persons_select = """SELECT hex(pr.externalId) uas_proposal_id, hex(pe.externalId) uas_person_id, php.role, php.proposalId, php.personId
FROM ProposalHasPerson php 
  INNER JOIN Proposal pr on pr.proposalId = php.proposalId
  INNER JOIN Person pe on pe.personId = php.personId
WHERE pe.login is not NULL"""

    ispyb_rs = list(do_ispyb_query(ispyb_proposal_has_persons_select, []))
    logging.getLogger().debug("Proposal - Persons: ISPyB database returns " + str(len(ispyb_rs)) + " rows.")
    uas_rs = do_uas_query(uas_proposal_has_persons_select, [])
    logging.getLogger().debug("Proposal - Persons: UAS database returns " + str(len(uas_rs)) + " rows.")

    prev_proposal_id = None
    prev_person_id = None
    for uas_row in uas_rs:
        if uas_row[0] == prev_proposal_id and uas_row[1] == prev_person_id: # UAS allows multiple roles per person per session. ISPyB doesn't, so ...
            #logging.getLogger().debug("Skipping")
            continue
        prev_proposal_id = uas_row[0] 
        prev_person_id = uas_row[1]
        
        for ispyb_row in ispyb_rs:
            if (uas_row[0] == ispyb_row[0] and uas_row[1] == ispyb_row[1]): # Compare GUIDs of both session and person 
                
                if uas_role_2_ispyb_role(uas_row[2]) != ispyb_row[2]:       # Compare roles  
                    update_proposal_has_person(uas_row[2], ispyb_row[3], ispyb_row[4])
                break
        else:
            pr_id = retrieve_proposal_id_for_uas_id(uas_row[0])
            pe_id = retrieve_person_id(uas_row[1])

            if pr_id != None and pe_id != None:
                insert_proposal_has_person(uas_role_2_ispyb_role(uas_row[2]), pr_id, pe_id)
            elif pr_id is None:
                logging.getLogger().debug("Not found: Proposal.externalId %s for personId %d" % (uas_row[0], pe_id if pe_id is not None else -1))
            elif pe_id is None:
                logging.getLogger().debug("Not found: Person.externalId %s for proposalId %d" % (uas_row[1], pr_id if pr_id is not None else -1))


def insert_proposal_has_person(role, proposal_id, person_id):
    if proposal_id != None and person_id != None:
        query = '''INSERT IGNORE INTO ProposalHasPerson (proposalId, personId, role) 
VALUES (%s, %s, %s)'''
        params = [proposal_id, person_id, role] 
        do_ispyb_query(query, params, False, False)
    else:
        if proposal_id is None:
            logging.getLogger().debug("proposal_id is None!")
        if person_id is None:
            logging.getLogger().debug("person_id is None!")

def update_proposal_has_person(uas_role, ispyb_proposal_id, ispyb_person_id):
    query = 'UPDATE ProposalHasPerson SET role=%s WHERE proposalId=%s AND personId=%s'
    params = [uas_role_2_ispyb_role(uas_role), ispyb_proposal_id, ispyb_person_id]
    do_ispyb_query(query, params, False, False)

    
def propagate_session_has_persons():
    uas_session_has_persons_select = None
    ispyb_session_has_persons_select = None
    if greater_than is not None:
        
        uas_session_has_persons_select = """SELECT session_id, person_id, "role", on_site 
FROM (
SELECT rawtohex(lc.session_id) session_id, rawtohex(lc.person_id) person_id, decode(lc.local_contact_level,0,'LOCAL_CONTACT_1ST',1,'LOCAL_CONTACT_2ND','LOCAL_CONTACT') "role", 1 on_site, 1 rank  
FROM local_contact lc
  INNER JOIN shift s on lc.session_id = s.session_id
  INNER JOIN facility_user fu on lc.person_id = fu.person_id
WHERE s.enddate > sysdate - %d AND fu.federal_id is not NULL AND s.state <> 'Cancelled'
UNION ALL
SELECT rawtohex(iu.session_id) session_id, rawtohex(iu.person_id) person_id, iu.role, iu.on_site, 2 rank  
FROM investigation_user iu
  INNER JOIN shift s on iu.session_id = s.session_id
  INNER JOIN facility_user fu on iu.person_id = fu.person_id
WHERE s.enddate > sysdate - %d AND fu.federal_id is not NULL AND s.state <> 'Cancelled'
)
ORDER BY session_id, person_id, rank, "role" """ % (int(greater_than), int(greater_than))

        ispyb_session_has_persons_select = """SELECT hex(bs.externalId) uas_session_id, hex(p.externalId) uas_person_id, shs.role, shs.sessionId, shs.personId, shs.remote
FROM Session_has_Person shs 
  INNER JOIN BLSession bs on bs.sessionId = shs.sessionId
  INNER JOIN Person p on p.personId = shs.personId
WHERE bs.endDate > subdate(now(), INTERVAL %d DAY) AND p.login is not NULL""" % (int(greater_than)+1)

    else: 
        uas_session_has_persons_select = """SELECT session_id, person_id, "role", on_site 
FROM (
SELECT rawtohex(lc.session_id) session_id, rawtohex(lc.person_id) person_id, decode(lc.local_contact_level,0,'LOCAL_CONTACT_1ST',1,'LOCAL_CONTACT_2ND','LOCAL_CONTACT') "role", 1 on_site, 1 rank  
FROM local_contact lc
  INNER JOIN shift s on lc.session_id = s.session_id
  INNER JOIN facility_user fu on lc.person_id = fu.person_id
WHERE fu.federal_id is not NULL AND s.state <> 'Cancelled'
UNION ALL
SELECT rawtohex(iu.session_id) session_id, rawtohex(iu.person_id) person_id, iu.role, iu.on_site, 2 rank  
FROM investigation_user iu
  INNER JOIN shift s on iu.session_id = s.session_id
  INNER JOIN facility_user fu on iu.person_id = fu.person_id
WHERE fu.federal_id is not NULL AND s.state <> 'Cancelled'
)
ORDER BY session_id, person_id, rank, "role" """ 

        ispyb_session_has_persons_select = """SELECT hex(bs.externalId) uas_session_id, hex(p.externalId) uas_person_id, shs.role, shs.sessionId, shs.personId, shs.remote
FROM Session_has_Person shs 
  INNER JOIN BLSession bs on bs.sessionId = shs.sessionId
  INNER JOIN Person p on p.personId = shs.personId
WHERE p.login is not NULL"""

    ispyb_rs = list(do_ispyb_query(ispyb_session_has_persons_select, []))
    logging.getLogger().debug("Session - Persons: ISPyB database returns " + str(len(ispyb_rs)) + " rows.")
    uas_rs = do_uas_query(uas_session_has_persons_select, [])
    logging.getLogger().debug("Session - Persons: UAS database returns " + str(len(uas_rs)) + " rows.")

    prev_session_id = None
    prev_person_id = None
    for uas_row in uas_rs:
        if uas_row[0] == prev_session_id and uas_row[1] == prev_person_id: # UAS allows multiple roles per person per session. ISPyB doesn't, so ...
            #logging.getLogger().debug("Skipping")
            continue
        prev_session_id = uas_row[0] 
        prev_person_id = uas_row[1]
        
        for ispyb_row in ispyb_rs:
            if (uas_row[0] == ispyb_row[0] and uas_row[1] == ispyb_row[1]): # Compare GUIDs of both session and person 
                uas_is_remote = 1 if uas_row[3] == 0 else 0 if uas_row[3] == 1 else None   
                ispyb_is_remote = 1 if ispyb_row[5] == 1 else 0 if ispyb_row[5] == 0 else None  
                
                if uas_role_2_ispyb_role(uas_row[2]) != ispyb_row[2] or uas_is_remote != ispyb_is_remote:  # Compare roles and remote / on-site status 
                    update_session_has_person(uas_row[2], uas_is_remote, ispyb_row[3], ispyb_row[4])
                break
        else:
            s_id = retrieve_session_id(uas_row[0])
            p_id = retrieve_person_id(uas_row[1])
            is_remote = 1 if uas_row[3] == 0 else 0 if uas_row[3] == 1 else None

            if s_id != None and p_id != None:
                insert_session_has_person(uas_role_2_ispyb_role(uas_row[2]), s_id, p_id, is_remote)
            elif s_id is None:
                logging.getLogger().debug("Not found: BLSession.externalId %s for personId %d" % (uas_row[0], p_id if p_id is not None else -1))
            elif p_id is None:
                logging.getLogger().debug("Not found: Person.externalId %s for sessionId %d" % (uas_row[1], s_id if s_id is not None else -1))


def propagate_proposals():
    ''' Proposal state - from Sam Hough:
    /** Proposal state: initial submission being drafted. */
    public static final String STATE_DRAFT = "Draft";
    /** Proposal state: draft was deleted and never submitted. */
    public static final String STATE_DELETED = "Deleted";
    /** Proposal state: submitted and awaiting decision. */
    public static final String STATE_DECISION_PENDING = "Decision Pending";
    /** Proposal state: reserved - should later be accepted or rejected. */
    public static final String STATE_RESERVED = "Reserved";
    /** Proposal state: rejected. */
    public static final String STATE_REJECTED = "Rejected";
    /** Proposal state: open and in progress. */
    public static final String STATE_OPEN = "Open";
    /** Proposal state: closed - final read only state. */
    public static final String STATE_CLOSED = "Closed";
    /** Proposal state: cancelled - final read only state. */
    public static final String STATE_CANCELLED = "Cancelled";
'''
    
    uas_proposals_select = """SELECT lower(p.name), rawtohex(p.id), p.title, p.state 
FROM proposal p 
WHERE p.state in ('Open', 'Closed', 'Cancelled') 
ORDER BY p.name""" # p.summary

    ispyb_proposals_select = """SELECT concat(proposalcode, proposalnumber), hex(externalId), title, proposalId 
FROM Proposal 
ORDER BY concat(proposalcode, proposalnumber)"""

    ispyb_rs = list(do_ispyb_query(ispyb_proposals_select, []))
    logging.getLogger().debug("Proposals: ISPyB database returns " + str(len(ispyb_rs)) + " rows.")
    uas_rs = do_uas_query(uas_proposals_select, [])
    logging.getLogger().debug("Proposals: UAS database returns " + str(len(uas_rs)) + " rows.")

    for uas_row in uas_rs:
        for ispyb_row in ispyb_rs:

            if (uas_row[1] == ispyb_row[1]) or (uas_row[0] == ispyb_row[0]): # UAS GUID vs ISPyB externalId OR UAS proposal name vs ISPyB proposal   
                
                if uas_row[3] == 'Cancelled':
                    delete_proposal(ispyb_row[3]) # ispyb_row[3] is the ISPyB proposalId
                    break
                if uas_row[0][0:2] != ispyb_row[0][0:2]: # UAS proposal code vs ISPyB proposal code
                    update_proposal_code(uas_row[0][0:2], ispyb_row[3])
                if uas_row[2] != ispyb_row[2] or uas_row[1] != ispyb_row[1]: # UAS title vs ISPyB title OR UAS GUID vs ISPyB externalId   
                    update_proposal(uas_row[2], uas_row[1], ispyb_row[3])
                break
        else:
            if uas_row[3] != 'Cancelled':
                insert_proposal(uas_row[0], uas_row[1], uas_row[2])    
    

def propagate_sessions():
    uas_sessions_select = """SELECT rawtohex(s.session_id), 
    lower(s.visit_id), 
    lower(s.instrument), 
    s."COMMENT", 
    s.startdate, 
    s.enddate, 
    s.state, 
    substr(rtrim(xmlagg (xmlelement (e, fu.title || ' ' || fu.given_name || ' ' || fu.family_name || ', ')).extract ('//text()'), ', '), 1, 255) beamlineOperator
FROM shift s
  LEFT OUTER JOIN local_contact lc on lc.visit_id = s.visit_id 
  LEFT OUTER JOIN facility_user fu on fu.person_id = lc.person_id
WHERE substr(s.visit_id, 3,1) <> '-' 
GROUP BY rawtohex(s.session_id), lower(s.visit_id), lower(s.instrument), s."COMMENT", s.startdate, s.enddate, s.state"""

    ispyb_sessions_select = """SELECT 
hex(s.externalId),
CONCAT(p.proposalcode, p.proposalnumber, '-', s.visit_number) as visit_id,
s.beamlinename, 
s.comments,
s.startdate, 
s.enddate, 
s.sessionid,
s.beamLineOperator,
s.scheduled 
FROM Proposal p INNER JOIN BLSession s ON p.proposalid = s.proposalid 
ORDER BY p.proposalnumber, p.proposalcode, s.visit_number""" 

    ispyb_rs = list(do_ispyb_query(ispyb_sessions_select, []))
    logging.getLogger().debug("Sessions: ISPyB database returns " + str(len(ispyb_rs)) + " rows.")
    uas_rs = do_uas_query(uas_sessions_select, [])
    logging.getLogger().debug("Sessions: UAS database returns " + str(len(uas_rs)) + " rows.")

    for uas_row in uas_rs:
        scheduled = 1
        if uas_row[6] == 'Queued':
            scheduled = 0
        for ispyb_row in ispyb_rs:
            if (uas_row[0] == ispyb_row[0]) or (uas_row[1] == ispyb_row[1]): # UAS GUID, ISPyB externalId 
                if uas_row[6] == 'Cancelled':
                    delete_session(ispyb_row[6]) # TO BE IMPLEMENTED
                # NOTE: deliberately not comparing comments, as they may have changed in ISPyB and we don't want to overwrite
                elif uas_row[0] != ispyb_row[0] or uas_row[1] != ispyb_row[1] or uas_row[2] != ispyb_row[2] or uas_row[4] != ispyb_row[4] or uas_row[5] != ispyb_row[5] or uas_row[7] != ispyb_row[7] or ispyb_row[8] != scheduled:
                    update_session(uas_row[0], uas_row[2], uas_row[4], uas_row[5], uas_row[7], scheduled, ispyb_row[6])
                break
        else:
            if uas_row[6] != 'Cancelled':
                insert_session(uas_row[0], uas_row[2], uas_row[3], uas_row[4], uas_row[5], uas_row[1], uas_row[7], scheduled)    


def propagate_session_types():
    ispyb_session_types_select = """SELECT hex(bs.externalId), st.typeName
FROM SessionType st 
  INNER JOIN BLSession bs on st.sessionId = bs.sessionId"""

    uas_session_tags_select = """SELECT rawtohex(session_id), tag, visit_id
FROM investigation_tag it
ORDER BY session_id"""

    ispyb_rs = list(do_ispyb_query(ispyb_session_types_select, []))
    logging.getLogger().debug("Session types: ISPyB database returns " + str(len(ispyb_rs)) + " rows.")
    uas_rs = do_uas_query(uas_session_tags_select, [])
    logging.getLogger().debug("Session types: UAS database returns " + str(len(uas_rs)) + " rows.")

    for uas_row in uas_rs:
        for ispyb_row in ispyb_rs:
            if uas_row[0] == ispyb_row[0] and uas_row[1] == ispyb_row[1]:
                break
        else:
            insert_session_type(uas_row[0], uas_row[1], uas_row[2])    

def propagate_persons():
    uas_persons_select = """SELECT rawtohex(person_id), lower(federal_id), title, given_name, family_name 
FROM facility_user
WHERE federal_id is not NULL"""
    ispyb_persons_select = """SELECT hex(externalId), lower(login), title, givenName, familyName, personId
FROM Person
WHERE login is not NULL"""

    ispyb_rs = list(do_ispyb_query(ispyb_persons_select, []))
    logging.getLogger().debug("Persons: ISPyB database returns " + str(len(ispyb_rs)) + " rows.")
    uas_rs = do_uas_query(uas_persons_select, [])
    logging.getLogger().debug("Persons: UAS database returns " + str(len(uas_rs)) + " rows.")

    for uas_row in uas_rs:
        for ispyb_row in ispyb_rs:
            if (uas_row[0] == ispyb_row[0]) or (uas_row[1] == ispyb_row[1]): # UAS GUID, ISPyB externalId 

                if uas_row[0] != ispyb_row[0] or uas_row[2] != ispyb_row[2] or uas_row[3] != ispyb_row[3] or uas_row[4] != ispyb_row[4]: 
                    update_person(uas_row[0], uas_row[2], uas_row[3], uas_row[4], ispyb_row[5])
                break
        else:
            insert_person(uas_row[0], uas_row[1], uas_row[2], uas_row[3], uas_row[4])    


def propagate_samples():
    # Pre-registered "samples" in ICAT, which should hopefully be proteins for these particular proposal types ...
    # "WHERE substr(visit_id, 1, 2) in ('MX', 'IN', 'SW', 'CM', 'NT', 'NR') "\
    #    "  and inv.STARTDATE > systimestamp "\
    icat_sample_select = """SELECT DISTINCT lower(inv.name) proposal, trim(s.NAME) sample_name, substr('UAS-' || LTRIM(REGEXP_REPLACE(sp.string_value, '[^[_a-zA-Z0-9-]]*', ''), '-_'), 1, 25) acronym 
FROM icatdls42.investigation@dicat_ro inv 
inner join icatdls42.SAMPLE@dicat_ro s on s.investigation_id=inv.id 
inner join icatdls42.sampleparameter@dicat_ro sp on sp.sample_Id=s.id and sp.parameter_type_id=1 
WHERE visit_id = 'NR4987-5' 
  and not substr('UAS-' || LTRIM(REGEXP_REPLACE(sp.string_value, '[^[_a-zA-Z0-9-]]*', ''), '-_'), 1, 25) is NULL 
ORDER BY 
  lower(inv.name), trim(s.NAME), substr('UAS-' || LTRIM(REGEXP_REPLACE(sp.string_value, '[^[_a-zA-Z0-9-]]*', ''), '-_'), 1, 25)"""

    # This needs to truncate material to 255 chars
    # and make sure only one instance or proposal_id + sample acronym exists
    # Pick the first one if more than one 
    uas_sample_select = """SELECT rawtohex(s.id), 
rawtohex(p.id),
substr(trim(s.material), 1, 255) "name", 
substr(LTRIM(REGEXP_REPLACE(s.acronym, '[^[_a-zA-Z0-9-]]*', ''), '-_'), 1, 25),
s.state
FROM sample s
  INNER JOIN sample_hazard sh on sh.sample_id = s.id
  INNER JOIN proposal_sample ps ON s.id = ps.sample_id
  INNER JOIN proposal p ON p.id = ps.proposal_id
WHERE 
  p.state in ('Open', 'Closed') AND 
  sh.risk_rating_name = 'Low' AND 
  substr(trim(s.material), 1, 255) is not NULL AND
  substr(LTRIM(REGEXP_REPLACE(s.acronym, '[^[_a-zA-Z0-9-]]*', ''), '-_'), 1, 25) is not NULL"""
# s.state = 'Accepted' AND

    # Proteins in ISPyB
    ispyb_sample_select = """SELECT hex(prot.externalId), hex(p.externalId), prot.name, prot.acronym, prot.proteinId 
FROM Proposal p 
INNER JOIN Protein prot on p.proposalId = prot.proposalId 
ORDER BY concat(p.proposalcode, p.proposalnumber), prot.name, prot.acronym"""

    
    uas_rs = do_uas_query(uas_sample_select, [])
    logging.getLogger().debug("UAS Samples: UAS database returns " + str(len(uas_rs)) + " rows.")
    ispyb_rs = do_ispyb_query(ispyb_sample_select, [])
    logging.getLogger().debug("Proteins: ISPyB database returns " + str(len(ispyb_rs)) + " rows.")

    # 0 - UAS protein ID
    # 1 - UAS proposal ID
    # 2 - protein name
    # 3 - protein acronym
    # 4 - UAS sample state, ISPyB protein ID

    for uas_row in uas_rs:
        for ispyb_row in ispyb_rs:
            # IF same UAS sample ID: 
            if uas_row[0] == ispyb_row[0]:
                # IF UAS state no longer valid:
                if uas_row[4] != 'Accepted':
                    update_protein_uas_id(None, ispyb_row[4])
                if uas_row[2] != "" and uas_row[2] is not None and \
                    (ispyb_row[2] is None or ispyb_row[2] == ''):
                    update_protein_name(uas_row[2], ispyb_row[4])
                break
            # IF no UAS sample ID in ispyb AND same UAS proposal ID AND same acronym:
            elif ispyb_row[0] is None and uas_row[1] == ispyb_row[1] and uas_row[3] == ispyb_row[3]:
                # IF state is 'Accepted'
                if uas_row[4] == 'Accepted':
                    # IF the protein's UAS ID doesn't already exist in ISPyB:
                    if 0 == retrieve_number_of_proteins_for_uas_id(uas_row[0]):
                        update_protein_uas_id(uas_row[0], ispyb_row[4])
                        # IF ISPyB name is empty
                        if ispyb_row[2] is None or ispyb_row[2] == '':
                            update_protein_name(uas_row[2], ispyb_row[4])
                break
        else:
            if uas_row[4] == 'Accepted':
                # At this point we know the protein's UAS ID doesn't exist in ISPyB, 
                # but we still need to make sure the acronym doesn't already exist in the proposal
                if 0 == retrieve_number_of_proteins_for_proposal_and_acronym(uas_row[1], uas_row[3]):
                    ispyb_proposal_id = retrieve_proposal_id_for_uas_id(uas_row[1])
                    if ispyb_proposal_id != None:
                        insert_protein(uas_row[0], ispyb_proposal_id, uas_row[2], uas_row[3], 'ORIGIN:UAS') 


def clean_up():
    global pidfile
    os.unlink(pidfile)
    logging.getLogger().info("%s: exiting python interpreter :-(" % sys.argv[0])
    logging.shutdown()

def kill_handler(sig,frame):
    logging.getLogger().warning("%s: got SIGTERM on %s :-O" % (sys.argv[0], os.uname()[1]))
    logging.shutdown()
    os._exit(-1)

def print_usage():
    global usage_string
    print usage_string

def init(usage):
    global pidfile
    global usage_string
    global ispyb_conn
    global uas_conn
    global greater_than
    global less_than 
    
    usage_string = usage
    
    conf_file = None
    log_file = None
    greater_than = None
    less_than = None

    # Get command-line arguments
    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:], "hc:l:", ["help", "conf", "log", "lt=", "gt="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)

    for o,a in opts:
        if o in ("-h", "--help"):
            print_usage()
            sys.exit()
        elif o in ("-c", "--conf"):
            conf_file = a
        elif o in ("-l", "--log"):
            log_file = a
        elif o == "--gt":
            greater_than = a
        elif o == "--lt":
            less_than = a


    # Read the config file
    if conf_file == None:
        print_usage()
        sys.exit()
    
    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read(conf_file)

    uas_user = config.get('UAS', 'user')
    uas_pw = config.get('UAS', 'pw')
    uas_TNS = config.get('UAS', 'TNS')

    ispyb_user = config.get('ISPyB', 'user')
    ispyb_pw = config.get('ISPyB', 'pw')
    ispyb_db = config.get('ISPyB', 'db')
    ispyb_host = config.get('ISPyB', 'host')
    ispyb_port = config.getint('ISPyB', 'port')

    # Configure logging
    if log_file is None:
        log_file="uas_propagation_%s.log" % os.uname()[1]  

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('* %(asctime)s [id=%(thread)d] <%(levelname)s> %(message)s')
    hdlr = RotatingFileHandler(filename=log_file, maxBytes=1000000, backupCount=30)
    hdlr.setFormatter(formatter)
    logging.getLogger().addHandler(hdlr)

    signal.signal(signal.SIGTERM,kill_handler) # Log SIGTERM

    # Create a pid file
    pid = str(os.getpid())
    pidfile = "/tmp/uas_propagation.pid"  
    if os.path.isfile(pidfile):        
        logging.getLogger().error("%s already exists, exiting" % pidfile)
        sys.exit()
    else:
        file(pidfile, 'w').write(pid)

    atexit.register(clean_up) # Remove pid file when exiting

    # Set up DB connections     
    connect_to_uas(uas_user, uas_pw, uas_TNS)
    connect_to_ispyb(ispyb_host, ispyb_user, ispyb_pw, ispyb_db, int(ispyb_port))

def tidy():
    global ispyb_conn
    global uas_conn
    # Release DB connections
    uas_conn = disconnect_from_db(uas_conn)
    ispyb_conn = disconnect_from_db(ispyb_conn)

