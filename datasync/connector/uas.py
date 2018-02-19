import cx_Oracle
try:
  import configparser
except ImportError:
  import ConfigParser as configparser
import logging
import threading
import time
import sys
from dbsource import DBSource

def open(configuration_file):
  '''Create an user admin DB connection using settings from a configuration file.'''
  config = configparser.RawConfigParser(allow_no_value=True)
  if not config.read(configuration_file):
    raise AttributeError('No configuration found at %s' % configuration_file)

  conn = None
  if config.has_section('uas'):
    credentials = dict(config.items('uas'))
    logging.getLogger().debug('Creating Oracle connection from %s', configuration_file)
    conn = UASConnector(**credentials)
  else:
    raise AttributeError('No supported connection type found in %s' % configuration_file)

  return conn

class UASConnector(DBSource):
  def __init__(self, user=None, pw=None, schema=None, tns=None, conn_inactivity=360):
    self.lock = threading.Lock()
    self.connect(user=user, pw=pw, schema=schema, tns=tns, conn_inactivity=conn_inactivity)

  def __enter__(self):
    if hasattr(self, 'conn') and self.conn is not None:
        return self
    else:
        raise Exception

  def __exit__(self, type, value, traceback):
    self.disconnect()


  def connect(self, user, pw, schema, tns, conn_inactivity):
    self.conn = None
    self.disconnect()
    self.user = user
    self.pw = pw
    self.schema = schema
    self.tns = tns
    self.conn_inactivity = int(conn_inactivity)

    try:
        self.conn=cx_Oracle.connect(user=user, password=pw, dsn=tns)
    except:
        logging.getLogger().exception("%s: error while connecting to UAS DB :-(" % sys.argv[0])
    else:
        try:
            #conn.autocommit(True)
            conn.autocommit=True
        except AttributeError:
            pass
        logging.getLogger().info("%s: Connected to database (Oracle v. %s)" % (sys.argv[0], conn.version))
        logging.getLogger().info("%s:    Database user: %s" % (sys.argv[0], user))
        logging.getLogger().info("%s:    TNS name: %s" % (sys.argv[0], tns))

    return self.conn #, uas_cursor)

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
          self.connect(self.user, self.pw, self.schema, self.tns, self.conn_inactivity)

      self.last_activity_ts = time.time()
      if self.conn is None:
          raise ISPyBConnectionException

      try:
          cursor = self.conn.cursor()
      except:
          logging.getLogger().exception("%s: unable to create cursor :-(" % sys.argv[0])
      else:
          logging.getLogger().debug("%s: default cursor ok :-)" % sys.argv[0])

      if cursor is None:
          raise Exception
      return cursor

  def do_query(self, querystr, params, return_fetch=True, return_id=False, log_query=True):
    cursor = self.create_cursor(dictionary=True)

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

  def retrieve_persons_for_session(self, id):
    query = """SELECT person_id, role, on_site, federal_id, title, given_name, family_name
FROM (
SELECT rawtohex(lc.person_id) person_id, decode(lc.local_contact_level,0,'LOCAL_CONTACT_1ST',1,'LOCAL_CONTACT_2ND','LOCAL_CONTACT') "role", 1 on_site, 1 rank, lower(fu.federal_id) federal_id, fu.title, fu.given_name, fu.family_name
FROM local_contact lc
  INNER JOIN facility_user fu on lc.person_id = fu.person_id
WHERE fu.federal_id is not NULL AND lc.session_id=hextoraw('%s')
UNION ALL
SELECT rawtohex(iu.person_id) person_id, iu.role, iu.on_site, 2 rank, lower(fu.federal_id) federal_id, fu.title, fu.given_name, fu.family_name
FROM investigation_user iu
  INNER JOIN facility_user fu on iu.person_id = fu.person_id
WHERE fu.federal_id is not NULL AND iu.session_id=hextoraw('%s')
)
ORDER BY person_id""" % (id, id)
    params = []
    rs = do_uas_query(query, params, return_fetch=True, return_id=False, log_query=True)
    if rs != None and len(rs) > 0 and rs[0] != None and rs[0][0] != None:
        return rs
    return None

  def retrieve_sessions_for_person(self, id):
    query = """SELECT rawtohex(iu.session_id), iu.role, iu.on_site
FROM investigation_user iu
  INNER JOIN shift s ON s.session_id = iu.session_id
WHERE iu.person_id=hextoraw('%s') AND s.state <> 'Cancelled'
ORDER BY iu.session_id""" % id
    params = []
    rs = do_uas_query(query, params, return_fetch=True, return_id=False, log_query=True)
    if rs != None and len(rs) > 0 and rs[0] != None and rs[0][0] != None:
        return rs
    return None



  def extract_proposals_have_persons(self):
    select = """SELECT rawtohex(pu.proposal_id) proposal_id, rawtohex(pu.person_id) person_id, pu.role
FROM proposal_user pu
  INNER JOIN facility_user fu on fu.person_id = pu.person_id
  INNER JOIN proposal p on p.id = pu.proposal_id
WHERE fu.federal_id is not NULL and p.state in ('Open', 'Closed')
"""
    rs = self.do_query(select, [])
    logging.getLogger().debug("Proposal - Persons: UAS database returns " + str(len(rs)) + " rows.")
    return rs


  def extract_sessions_have_persons(self, greater_than = 100):
    select = """SELECT session_id, person_id, "role", on_site
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
    rs = self.do_query(select, [])
    logging.getLogger().debug("Session - Persons: UAS database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_proposals(self):
    select = """SELECT lower(p.name), rawtohex(p.id), p.title, p.state
FROM proposal p
WHERE p.state in ('Open', 'Closed', 'Cancelled')
ORDER BY p.name""" # p.summary
    rs = self.do_query(select, [])
    logging.getLogger().debug("Proposals: UAS database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_sessions(self):
    select = """SELECT rawtohex(s.session_id),
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
    rs = self.do_query(select, [])
    logging.getLogger().debug("Sessions: UAS database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_session_types(self):
    select = """SELECT rawtohex(session_id), tag, visit_id
FROM investigation_tag it
ORDER BY session_id"""
    rs = list(self.do_query(select, []))
    logging.getLogger().debug("Session types: UAS database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_persons(self):
    select = """SELECT rawtohex(person_id), lower(federal_id), title, given_name, family_name
FROM facility_user
WHERE federal_id is not NULL"""
    rs = list(self.do_query(select, []))
    logging.getLogger().debug("Persons: UAS database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_components(self):
    # This needs to truncate material to 255 chars
    # and make sure only one instance or proposal_id + sample acronym exists
    # Pick the first one if more than one
    select = """SELECT rawtohex(s.id),
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
    rs = list(self.do_query(select, []))
    logging.getLogger().debug("UAS Components: UAS database returns " + str(len(rs)) + " rows.")
    return rs
