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

def open(configuration_file=None):
  '''Create a dummy user admin DB connection, ignoring settings from the configuration file.'''
  config = configparser.RawConfigParser(allow_no_value=True)
  if not config.read(configuration_file):
    raise AttributeError('No configuration found at %s' % configuration_file)

  conn = None
  logging.getLogger().debug('Creating dummy Oracle connection')
  conn = DummyUASConnector()

  return conn

class DummyUASConnector(DBSource):
  def __init__(self):
    pass

  def __enter__(self):
    pass

  def __exit__(self, type, value, traceback):
    pass

  def __del__(self):
    pass

  def disconnect(self):
    pass

  def extract_proposals_have_persons(self):
    rs = [('99017EB35BD34E55E04017AC41627AFF', 'E70E7EB35BD34E55E04017AC41627FFB', 'PRINCIPAL_INVESTIGATOR'),
        ('99017EB35BD34E55E04017AC41627AFF', 'E70E7EB35BD34E55E04017AC41627FFC', 'CO_INVESTIGATOR')]
    logging.getLogger().debug("Proposal - Persons: Dummy UAS database returns " + str(len(rs)) + " rows.")
    return rs


  def extract_sessions_have_persons(self, greater_than = 100):
    rs = [('99017EB35BD34E55E04017AC41627AFF', 'E70E7EB35BD34E55E04017AC41627FFB', 'TEAM_LEADER', 1),
        ('99017EB35BD34E55E04017AC41627AFF', 'E70E7EB35BD34E55E04017AC41627FFC', 'TEAM_MEMBER', 1),
        ('99017EB35BD34E55E04017AC41627AFF', 'E70E7EB35BD34E55E04017AC41627FFD', 'TEAM_MEMBER', 1)]
    logging.getLogger().debug("Session - Persons: Dummy UAS database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_proposals(self):
    rs = [('nt20', '99017EB35BD34E55E04017AC41627AFF', 'Software testing', 'Open'),
        ('cm12345', '99017EB35BD34E55E04017AC41627BFF', 'Commissioning i03', 'Open'),
        ('cm12346', '99017EB35BD34E55E04017AC41627CFF', 'Commissioning i04', 'Open')]
    logging.getLogger().debug("Proposals: Dummy UAS database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_sessions(self):
    rs = [('99017EB35BD34E55E04017AC41627AFE', 'cm12345-6', 'i03', 'Funny comment here ...', '2018-01-15 09:00:00', '2018-01-16 08:59:59', '', 'Dr Carlos Garcia'),
        ('99017EB35BD34E55E04017AC41627AFF', 'cm12346-7', 'i04', 'Even funnier comment here ...', '2018-01-15 09:00:00', '2018-01-16 08:59:59', '', 'Dr Maria de Santos')]
    logging.getLogger().debug("Sessions: Dummy UAS database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_session_types(self):
    rs = [('99017EB35BD34E55E04017AC41627AFE', 'Compulsarily remote', 'cm12345-6'),
        ('99017EB35BD34E55E04017AC41627AFF', 'In situ', 'cm12345-7'),
        ('99017EB35BD34E55E04017AC41627AFF', 'Humidity Control (HC1b)', 'cm12345-7')]
    logging.getLogger().debug("Session types: Dummy UAS database returns " + str(len(rs)) + " rows.")
    return rs

  def extract_persons(self):
    rs = [('E70E7EB35BD34E55E04017AC41627FFB', 'gok13476', 'Mr', 'Grok', 'Trok'),
        ('E70E7EB35BD34E55E04017AC41627FFC', 'fra47613', 'Dr', 'Spok', 'Drok'),
        ('E70E7EB35BD34E55E04017AC41627FFD', 'pro46731', 'Dr', 'Mok', 'Krok')]
    logging.getLogger().debug("Persons: UAS database returns " + str(len(rs)) + " rows.")
    return rs

  def retrieve_sessions_for_person(self, uas_id):
    rs = []
    if uas_id == 'E70E7EB35BD34E55E04017AC41627FFB':
        rs.append(('99017EB35BD34E55E04017AC41627AFF', 'TEAM_LEADER', 1))
    elif uas_id == 'E70E7EB35BD34E55E04017AC41627FFC':
        rs.append(('99017EB35BD34E55E04017AC41627AFF', 'TEAM_MEMBER', 1))
    elif uas_id == 'E70E7EB35BD34E55E04017AC41627FFD':
        rs.append(('99017EB35BD34E55E04017AC41627AFF', 'TEAM_MEMBER', 1))
    return rs

  def retrieve_persons_for_session(self, uas_id):
    rs = []
    if uas_id == '99017EB35BD34E55E04017AC41627AFF':
        rs.append(('E70E7EB35BD34E55E04017AC41627FFB', 'TEAM_LEADER', 1))
        rs.append(('E70E7EB35BD34E55E04017AC41627FFC', 'TEAM_MEMBER', 1))
        rs.append(('E70E7EB35BD34E55E04017AC41627FFD', 'TEAM_MEMBER', 1))
    return rs

  def extract_components(self):
    rs = [('D70E7EB35BD34E55E04017AC41627FFB', '99017EB35BD34E55E04017AC41627BFF', 'Deoxyribonucleic acid', 'DNA-x', 'Accepted'),
        ('D70E7EB35BD34E55E04017AC41627FFC', '99017EB35BD34E55E04017AC41627BFF', 'Deoxyribonucleic acid', 'DNA-y', 'Accepted'),
        ('D70E7EB35BD34E55E04017AC41627FFD', '99017EB35BD34E55E04017AC41627BFF', 'Deoxyribonucleic acid', 'DNA-z', 'Accepted')]
    return rs
