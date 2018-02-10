import logging
try:
  import configparser
except ImportError:
  import ConfigParser as configparser
import importlib
import datasync.main

__version__ = '0.1.0'

_log = logging.getLogger('datasync')

def open(conf_file = None, source = None, target = None):
  ds = datasync.main.DataSync(conf_file)
  conf_file = ds.get_conf_file()

  '''Create source and target connections using settings from a configuration file.'''
  config = configparser.RawConfigParser(allow_no_value=True)
  if not config.read(conf_file):
    raise AttributeError('No configuration found at %s' % conf_file)

  source_conn = None
  if config.has_section(source):
    conn_mod = importlib.import_module('%s.%s' % ('datasync.connector', source))
    _log.debug('Creating database connection from %s', conf_file)
    source_conn = conn_mod.open(conf_file)
  else:
    raise AttributeError('No supported connection type found in %s for %s' % (conf_file, source))

  target_conn = None
  if config.has_section(target):
    conn_mod = importlib.import_module('%s.%s' % ('datasync.connector', target))
    _log.debug('Creating database connection from %s', conf_file)
    target_conn = conn_mod.open(conf_file)
  else:
    raise AttributeError('No supported connection type found in %s for %s' % (conf_file, target))

  ds.set_source(source_conn)
  ds.set_target(target_conn)
  return ds
