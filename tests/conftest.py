# pytest configuration file

import os

import pytest

@pytest.fixture
def testconfig():
  '''Return the path to a configuration file pointing to a test database.'''
  config_file = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             '..', 'conf', 'config.cfg'))
  if not os.path.exists(config_file):
    pytest.skip("No configuration file for test database found. Skipping database tests")
  return config_file
