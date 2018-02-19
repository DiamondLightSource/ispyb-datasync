import mysql.connector
try:
  import configparser
except ImportError:
  import ConfigParser as configparser
import logging
import threading
import time
import sys
from dbsource import DBSource
from dbtarget import DBTarget

def open(configuration_file):
  '''Create an ISPyB connection using settings from a configuration file.'''
  config = configparser.RawConfigParser(allow_no_value=True)
  if not config.read(configuration_file):
    raise AttributeError('No configuration found at %s' % configuration_file)

  conn = None
  if config.has_section('ispyb'):
    credentials = dict(config.items('ispyb'))
    logging.getLogger().debug('Creating MySQL connection from %s', configuration_file)
    print(credentials)
    conn = ISPyBConnector(**credentials)
  else:
    raise AttributeError('No supported connection type found in %s' % configuration_file)

  return conn

class ISPyBConnector(DBSource, DBTarget):
  def __init__(self, user=None, pw=None, host='localhost', db=None, port=3306, unix_socket = None, conn_inactivity=360):
    self.lock = threading.Lock()
    self.connect(user=user, pw=pw, host=host, db=db, port=port, unix_socket = unix_socket, conn_inactivity=conn_inactivity)

  def __enter__(self):
    if hasattr(self, 'conn') and self.conn is not None:
        return self
    else:
        raise Exception

  def __exit__(self, type, value, traceback):
    self.disconnect()

  def connect(self, user=None, pw=None, host='localhost', db=None, port=3306, unix_socket = None, conn_inactivity=360):
    self.disconnect()
    self.user = user
    self.pw = pw
    self.host = host
    self.db = db
    self.port = port
    self.conn_inactivity = int(conn_inactivity)

    self.conn = None
    if unix_socket is not None and unix_socket != '':
        self.conn = mysql.connector.connect(user=user, unix_socket=unix_socket, database=db)
    else:
        self.conn = mysql.connector.connect(user=user, password=pw, host=host, database=db, port=int(port))

    if self.conn is not None:
        self.conn.autocommit=True
    else:
        raise ISPyBConnectionException
    self.last_activity_ts = time.time()

  def __del__(self):
    self.disconnect()

  def disconnect(self):
    '''Release the connection previously created.'''
    if hasattr(self, 'conn') and self.conn is not None:
    	self.conn.close()
    self.conn = None

  def create_cursor(self, dictionary=False):
      if time.time() - self.last_activity_ts > self.conn_inactivity:
          # re-connect:
          self.connect(self.user, self.pw, self.host, self.db, self.port)
      self.last_activity_ts = time.time()
      if self.conn is None:
          raise Exception

      cursor = self.conn.cursor(dictionary=dictionary)
      if cursor is None:
          raise Exception
      return cursor

  def do_query(self, querystr, params, return_fetch=True, return_id=False, log_query=True):
        cursor = self.create_cursor(dictionary=False)

        if log_query:
            logging.getLogger().debug(querystr + " " + str(params))
        start_time=time.time()
        try:
            ret=cursor.execute(querystr, params)
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
                ret=cursor.fetchall()
            except:
                logging.getLogger().exception("%s: exception fetching cursor :-(" % sys.argv[0])
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

            ret = cursor.lastrowid
            if log_query:
                logging.getLogger().debug("%s: id took %f seconds" % (sys.argv[0], (time.time()-start_time)))
        return ret

  def insert_proposal(self, proposal, title, src_id):
    code = str(proposal[0]) + str(proposal[1])
    num = int(proposal[2:])
    query = 'INSERT IGNORE INTO Proposal (proposalCode, proposalNumber, title, externalId, personId, blTimeStamp) VALUES (%s, %s, %s, unhex(%s), 1, NOW())'
    params = [code, str(num), title, src_id]
    return self.do_query(query, params, False, True)

  def update_proposal(self, title, src_id, id):
    query = 'UPDATE Proposal SET title=%s, blTimeStamp = NOW(), externalId=unhex(%s) WHERE proposalId=%s'
    params = [title, src_id, str(id)]
    self.do_query(query, params, False, False)

  def update_proposal_code(self, proposal_code, proposal_id):
    query = 'UPDATE Proposal SET proposalCode=%s WHERE proposalId=%s'
    params = [proposal_code, proposal_id]
    self.do_query(query, params, False, False)

  def delete_proposal(self, id):
    query = 'DELETE FROM ProposalHasPerson WHERE proposalId=%s'
    params = [str(id)]
    self.do_query(query, params, False, False)

    query = 'DELETE FROM Proposal WHERE proposalId=%s'
    params = [str(id)]
    self.do_query(query, params, False, False)

  def retrieve_proposal_id(self, proposal_code, proposal_number):
    query = 'SELECT max(proposalId) FROM Proposal WHERE proposalCode=%s AND proposalNumber=%s'
    params = [proposal_code, proposal_number]
    rs = self.do_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None and rs[0][0] != None:
        return int(rs[0][0])
    return None

  def retrieve_proposal_id_for_src_id(self, src_id):
    query = 'SELECT max(proposalId) FROM Proposal WHERE externalId=unhex(%s)'
    params = [src_id]
    rs = self.do_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None and rs[0][0] != None:
        return int(rs[0][0])
    return None

  def insert_session(self, src_id, beamline, comments, start_date, end_date, session_name, beamline_operators, scheduled, persons_rs=None):
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

    proposal_id = self.retrieve_proposal_id(code, num)
    if proposal_id is None:
        return

    query = '''INSERT IGNORE INTO BLSession (proposalId, externalId, beamlineName, comments, startDate, endDate, visit_number, beamLineOperator, scheduled)
VALUES (%s, unhex(%s), %s, %s, %s, %s, %s, %s, %s)'''
    params = [proposal_id, src_id, beamline, comments, start_date, end_date, visit_number, beamline_operators, scheduled]
    ispyb_session_id = self.do_query(query, params, False, True)

    if ispyb_session_id is None:
        logging.getLogger().debug("ispyb_session_id is None!")

    if persons_rs != None:
        self.insert_persons_for_session(ispyb_session_id, persons_rs)
    else:
        logging.getLogger().debug("persons_rs is None!")

  def update_session(self, src_id, beamline, start_date, end_date, local_contacts, scheduled, id):
    query = '''UPDATE BLSession SET externalId=unhex(%s), beamlinename=%s, startDate=%s, endDate=%s, beamLineOperator=%s, scheduled=%s
    WHERE sessionId=%s'''
    params = [src_id, beamline, start_date, end_date, local_contacts, scheduled, str(id)]
    self.do_query(query, params, False, False)

  def session_has_data(self, id):
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

    rs = self.do_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None:
        rows = int(rs[0][0])
        logging.getLogger().debug("session_has_data rows: %d" % rows)
        if rows > 0:
            return True
    return False

  def delete_session(self, id):
    if not session_has_data(id):
        query = 'DELETE FROM BLSession WHERE sessionId=%s'
        params = [str(id)]
        self.do_query(query, params, False, False)

  def retrieve_session_id(self, uas_session_id):
    query = 'SELECT max(sessionId) FROM BLSession WHERE externalId=unhex(%s)'
    params = [uas_session_id]
    rs = self.do_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None and rs[0][0] != None:
        return int(rs[0][0])
    return None

  def insert_session_has_person(self, role, session_id, person_id, is_remote):
    if session_id != None and person_id != None:
        query = '''INSERT IGNORE INTO Session_has_Person (sessionId, personId, role, remote)
VALUES (%s, %s, %s, %s)'''
        params = [session_id, person_id, role, is_remote]
        self.do_query(query, params, False, False)
    else:
        if session_id is None:
            logging.getLogger().debug("session_id is None!")
        if person_id is None:
            logging.getLogger().debug("person_id is None!")

  def update_session_has_person(self, uas_role, is_remote, ispyb_session_id, ispyb_person_id):
    query = 'UPDATE Session_has_Person SET role=%s, remote=%s WHERE sessionId=%s AND personId=%s'
    params = [self.uas_role_2_ispyb_role(uas_role), is_remote, ispyb_session_id, ispyb_person_id]
    self.do_query(query, params, False, False)

  def insert_session_type(self, src_id, tag, session_name):
    session_id = self.retrieve_session_id(src_id)
    if session_id != None:
        query = '''INSERT IGNORE INTO SessionType (sessionId, typeName) VALUES (%s, %s)'''
        params = [session_id, tag]
        ispyb_session_id = self.do_query(query, params, False, False)

  def insert_person(self, src_id, login, title, given_name, family_name, sessions_rs=None):
    query = '''INSERT IGNORE INTO Person (externalId, login, title, givenName, familyName)
VALUES (unhex(%s), %s, %s, %s, %s)'''
    params = [src_id, login, title, given_name, family_name]
    ispyb_person_id = self.do_query(query, params, False, True)

    if ispyb_person_id is None:
        logging.getLogger().debug("ispyb_person_id is None!")

    if sessions_rs is not None:
        if len(sessions_rs) > 0:
            self.insert_sessions_for_person(ispyb_person_id, sessions_rs)
        else:
            logging.getLogger().debug("uas_sessions_rs is None!")

  def update_person(self, src_id, login, title, given_name, family_name, id):
    query = 'UPDATE Person SET externalId=unhex(%s), login=%s, title=%s, givenName=%s, familyName=%s WHERE personId=%s'
    params = [src_id, login, title, given_name, family_name, str(id)]
    self.do_query(query, params, False, False)

  def retrieve_person_id(self, uas_person_id):
    query = 'SELECT max(personId) FROM Person WHERE externalId=unhex(%s)'
    params = [uas_person_id]
    rs = self.do_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None and rs[0][0] != None:
        return int(rs[0][0])
    return None

  def insert_protein(self, src_id, proposal_id, name, acronym, origin_txt):
    query = """INSERT IGNORE INTO Protein (externalId, proposalId, name, acronym, proteinType)
VALUES (
  unhex(%s),
  %s,
  %s,
  %s,
  %s
)"""
    params = [src_id, proposal_id, name, acronym, origin_txt]
    return self.do_query(query, params, False, True)

  def update_protein(self, name, acronym, id):
    query = 'UPDATE Protein SET name=%s, acronym=%s WHERE proteinId=%s'
    params = [name, acronym, str(id)]
    self.do_query(query, params, False, False)

  def update_protein_src_id(self, src_id, id):
    query = 'UPDATE Protein SET externalId=unhex(%s) WHERE proteinId=%s'
    params = [src_id, str(id)]
    self.do_query(query, params, False, False)

  def update_protein_name(self, name, id):
    query = 'UPDATE Protein SET name=%s WHERE proteinId=%s'
    params = [name, str(id)]
    self.do_query(query, params, False, False)

  def retrieve_number_of_proteins_for_src_id(self, src_id):
    query = 'SELECT count(*) FROM Protein WHERE externalId=unhex(%s)'
    params = [src_id]
    rs = self.do_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None and rs[0][0] != None:
        return int(rs[0][0])
    return None

  def retrieve_number_of_proteins_for_proposal_and_acronym(self, src_id, acronym):
    query = 'SELECT count(*) FROM Protein WHERE proposalId in (SELECT proposalId FROM Proposal WHERE externalId=unhex(%s)) AND acronym=%s'
    params = [src_id, acronym]
    rs = self.do_query(query, params, return_fetch=True, return_id=False, log_query=False)
    if rs != None and rs[0] != None and rs[0][0] != None:
        return int(rs[0][0])
    return None

  def insert_persons_for_session(self, ispyb_session_id, uas_persons_rs):
    prev_id = None
    for row in uas_persons_rs:
        id = row[0]
        uas_role = row[1]
        is_remote = 1 if row[2] == 0 else 0 if row[2] == 1 else None
        if id != prev_id:
            role = self.uas_role_2_ispyb_role(uas_role)
            ispyb_person_id = self.retrieve_person_id(id)
            if ispyb_person_id is None:
                login = row[3]
                if login is None:
                    continue
                title = row[4]
                given_name = row[5]
                family_name = row[6]

                ispyb_person_id = self.insert_person(id, login, title, given_name, family_name)

            self.insert_session_has_person(role, ispyb_session_id, ispyb_person_id, is_remote)
        prev_id = id

  def insert_sessions_for_person(self, ispyb_person_id, uas_sessions_rs):
    prev_id = None
    for row in uas_sessions_rs:
        if row[0] != prev_id:
            is_remote = 1 if row[2] == 0 else 0 if row[2] == 1 else None
            self.insert_session_has_person(self.uas_role_2_ispyb_role(row[1]),
                                  self.retrieve_session_id(row[0]),
                                  ispyb_person_id,
                                  is_remote)
        prev_id = row[0]


  def uas_role_2_ispyb_role(self, uas_role):
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

  def update_proposal_has_person(self, uas_role, ispyb_proposal_id, ispyb_person_id):
    query = 'UPDATE ProposalHasPerson SET role=%s WHERE proposalId=%s AND personId=%s'
    params = [self.uas_role_2_ispyb_role(uas_role), ispyb_proposal_id, ispyb_person_id]
    self.do_query(query, params, False, False)

  def insert_proposal_has_person(self, role, proposal_id, person_id):
    if proposal_id != None and person_id != None:
        query = '''INSERT IGNORE INTO ProposalHasPerson (proposalId, personId, role)
VALUES (%s, %s, %s)'''
        params = [proposal_id, person_id, role]
        self.do_query(query, params, False, False)
    else:
        if proposal_id is None:
            logging.getLogger().debug("proposal_id is None!")
        if person_id is None:
            logging.getLogger().debug("person_id is None!")

  def extract_proposals_have_persons(self):
    select = """SELECT hex(pr.externalId) uas_proposal_id, hex(pe.externalId) uas_person_id, php.role, php.proposalId, php.personId
FROM ProposalHasPerson php
  INNER JOIN Proposal pr on pr.proposalId = php.proposalId
  INNER JOIN Person pe on pe.personId = php.personId
WHERE pe.login is not NULL"""
    rs = list(self.do_query(select, []))
    logging.getLogger().debug("Proposal - Persons: ISPyB database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_sessions_have_persons(self, greater_than = 100):
    select = """SELECT hex(bs.externalId) uas_session_id, hex(p.externalId) uas_person_id, shs.role, shs.sessionId, shs.personId, shs.remote
FROM Session_has_Person shs
  INNER JOIN BLSession bs on bs.sessionId = shs.sessionId
  INNER JOIN Person p on p.personId = shs.personId
WHERE bs.endDate > subdate(now(), INTERVAL %d DAY) AND p.login is not NULL""" % (int(greater_than)+1)
    rs = list(self.do_query(select, []))
    logging.getLogger().debug("Session - Persons: ISPyB database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_proposals(self):
    select = """SELECT concat(proposalcode, proposalnumber), hex(externalId), title, proposalId
FROM Proposal
ORDER BY concat(proposalcode, proposalnumber)"""
    rs = list(self.do_query(select, []))
    logging.getLogger().debug("Proposals: ISPyB database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_sessions(self):
    select = """SELECT
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
    rs = list(self.do_query(select, []))
    logging.getLogger().debug("Sessions: ISPyB database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_session_types(self):
    select = """SELECT hex(bs.externalId), st.typeName
FROM SessionType st
  INNER JOIN BLSession bs on st.sessionId = bs.sessionId"""
    rs = list(self.do_query(select, []))
    logging.getLogger().debug("Session types: ISPyB database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_persons(self):
    select = """SELECT hex(externalId), lower(login), title, givenName, familyName, personId
FROM Person
WHERE login is not NULL"""
    rs = list(self.do_query(select, []))
    logging.getLogger().debug("Persons: ISPyB database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_components(self):
    select = """SELECT hex(prot.externalId), hex(p.externalId), prot.name, prot.acronym, prot.proteinId
FROM Proposal p
INNER JOIN Protein prot on p.proposalId = prot.proposalId
ORDER BY concat(p.proposalcode, p.proposalnumber), prot.name, prot.acronym"""
    rs = list(self.do_query(select, []))
    logging.getLogger().debug("Components: ISPyB database returns " + str(len(rs)) + " rows.")
    return rs
