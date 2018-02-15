import context
import datasync

def test_sync_proposals(testconfig):
    with datasync.open(conf_file = testconfig, source='dummyuas', target='ispyb') as ds:
        ds.sync_proposals()

    with datasync.open(conf_file = testconfig, source='dummyuas', target='ispyb') as ds:
        ds.sync_proposals()

def test_sync_proposals_have_persons(testconfig):
    with datasync.open(conf_file = testconfig, source='dummyuas', target='ispyb') as ds:
        ds.sync_proposals_have_persons()

def test_sync_sessions(testconfig):
    with datasync.open(conf_file = testconfig, source='dummyuas', target='ispyb') as ds:
        ds.sync_sessions()

def test_sync_persons(testconfig):
    with datasync.open(conf_file = testconfig, source='dummyuas', target='ispyb') as ds:
        ds.sync_persons()

def test_sync_session_types(testconfig):
    with datasync.open(conf_file = testconfig, source='dummyuas', target='ispyb') as ds:
        ds.sync_session_types()

def test_sync_sessions_have_persons(testconfig):
    with datasync.open(conf_file = testconfig, source='dummyuas', target='ispyb') as ds:
        ds.sync_sessions_have_persons()

def test_sync_components(testconfig):
    with datasync.open(conf_file = testconfig, source='dummyuas', target='ispyb') as ds:
        ds.sync_components()
