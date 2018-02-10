import getopt
import logging
from logging.handlers import RotatingFileHandler
import sys
import os
import time

class DataSync:
  def __init__(self, conf_file = None):
    def print_usage():
        print("""Syntax: %s -c <configuration file> [-rp]
        Arguments:
             -h|--help : display this help
             -c|--conf <conf file> : use the given configuration file
             -l|--log <log file>: use the given log file""" % sys.argv[0])

    self.conf_file = conf_file
    self.log_file = None

    # Get command-line arguments
    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:], "hc:l:", ["help", "conf", "log"])
    except getopt.GetoptError:
        print_usage(usage)
        sys.exit(2)

    for o,a in opts:
        if o in ("-h", "--help"):
            print_usage(usage)
            sys.exit()
        elif o in ("-c", "--conf"):
            self.conf_file = a
        elif o in ("-l", "--log"):
            self.log_file = a

    # Read the config file
    if self.conf_file is None:
        print_usage()
        sys.exit()

    if self.log_file is None:
        stem = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        self.log_file = "/tmp/%s.log" % stem

    # Create a pid file
    pid = str(os.getpid())
    stem = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    self.pidfile = "/tmp/%s.pid" % stem
    if os.path.isfile(self.pidfile):
        logging.getLogger().error("%s already exists, exiting" % self.pidfile)
        sys.exit()
    else:
        file(self.pidfile, 'w').write(pid)

    if self.conf_file is not None:
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('* %(asctime)s [id=%(thread)d] <%(levelname)s> %(message)s')
        hdlr = RotatingFileHandler(filename=self.log_file, maxBytes=1000000, backupCount=30)
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr)

  def __enter__(self):
    if hasattr(self, 'pidfile') and self.pidfile is not None and hasattr(self, 'logger') and self.logger is not None:
        return self
    else:
        raise Exception

  def __exit__(self, type, value, traceback):
    os.unlink(self.pidfile)
    self.pidfile = None
    logging.getLogger().info("%s: exiting class  :-(" % sys.argv[0])
    logging.shutdown()
    self.logger = None

  def get_conf_file(self):
      return self.conf_file

  def set_source(self, source_conn):
      self.source_conn = source_conn

  def set_target(self, target_conn):
      self.target_conn = target_conn

  def sync_proposals(self):
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
    uas_rs = self.source_conn.extract_proposals()
    ispyb_rs = self.target_conn.extract_proposals()

    for uas_row in uas_rs:
        print(uas_row)
        for ispyb_row in ispyb_rs:
            print(ispyb_row)

            if (uas_row[1] == ispyb_row[1]) or (uas_row[0] == ispyb_row[0]): # UAS GUID vs ISPyB externalId OR UAS proposal name vs ISPyB proposal

                if uas_row[3] == 'Cancelled':
                    self.target_conn.delete_proposal(ispyb_row[3]) # ispyb_row[3] is the ISPyB proposalId
                    break
                if uas_row[0][0:2] != ispyb_row[0][0:2]: # UAS proposal code vs ISPyB proposal code
                    self.target_conn.update_proposal_code(uas_row[0][0:2], ispyb_row[3])
                if uas_row[2] != ispyb_row[2] or uas_row[1] != ispyb_row[1]: # UAS title vs ISPyB title OR UAS GUID vs ISPyB externalId
                    self.target_conn.update_proposal(uas_row[2], uas_row[1], ispyb_row[3])
                break
        else:
            if uas_row[3] != 'Cancelled':
                self.target_conn.insert_proposal(uas_row[0], uas_row[2], uas_row[1])


  def sync_sessions(self):
    uas_rs = self.source_conn.extract_sessions()
    ispyb_rs = self.target_conn.extract_sessions()

    for uas_row in uas_rs:
        scheduled = 1
        if uas_row[6] == 'Queued':
            scheduled = 0
        for ispyb_row in ispyb_rs:
            if (uas_row[0] == ispyb_row[0]) or (uas_row[1] == ispyb_row[1]): # UAS GUID, ISPyB externalId
                if uas_row[6] == 'Cancelled':
                    self.target_conn.delete_session(ispyb_row[6]) # TO BE IMPLEMENTED
                # NOTE: deliberately not comparing comments, as they may have changed in ISPyB and we don't want to overwrite
                elif uas_row[0] != ispyb_row[0] or uas_row[1] != ispyb_row[1] or uas_row[2] != ispyb_row[2] or uas_row[4] != ispyb_row[4] or uas_row[5] != ispyb_row[5] or uas_row[7] != ispyb_row[7] or ispyb_row[8] != scheduled:
                    self.target_conn.update_session(uas_row[0], uas_row[2], uas_row[4], uas_row[5], uas_row[7], scheduled, ispyb_row[6])
                break
        else:
            if uas_row[6] != 'Cancelled':
                person_rs = self.source_conn.retrieve_persons_for_session(uas_row[0])
                self.target_conn.insert_session(uas_row[0], uas_row[2], uas_row[3], uas_row[4], uas_row[5], uas_row[1], uas_row[7], scheduled, person_rs)

  def sync_session_types(self):
    uas_rs = self.source_conn.extract_session_types()
    ispyb_rs = self.target_conn.extract_session_types()

    for uas_row in uas_rs:
        for ispyb_row in ispyb_rs:
            if uas_row[0] == ispyb_row[0] and uas_row[1] == ispyb_row[1]:
                break
        else:
            self.target_conn.insert_session_type(uas_row[0], uas_row[1], uas_row[2])

  def sync_persons(self):
    uas_rs = self.source_conn.extract_persons()
    ispyb_rs = self.target_conn.extract_persons()

    for uas_row in uas_rs:
        for ispyb_row in ispyb_rs:
            if (uas_row[0] == ispyb_row[0]) or (uas_row[1] == ispyb_row[1]): # UAS GUID, ISPyB externalId

                if uas_row[0] != ispyb_row[0] or uas_row[1] != ispyb_row[1] or uas_row[2] != ispyb_row[2] or uas_row[3] != ispyb_row[3] or uas_row[4] != ispyb_row[4]:
                    self.target_conn.update_person(uas_row[0], uas_row[1], uas_row[2], uas_row[3], uas_row[4], ispyb_row[5])
                break
        else:
            uas_sessions_rs = self.source_conn.retrieve_sessions_for_person(uas_row[0])
            self.target_conn.insert_person(uas_row[0], uas_row[1], uas_row[2], uas_row[3], uas_row[4], uas_sessions_rs)


  def sync_components(self):
    uas_rs = self.source_conn.extract_components()
    ispyb_rs = self.target_conn.extract_components()

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
                    self.target_conn.update_protein_uas_id(None, ispyb_row[4])
                if uas_row[2] != "" and uas_row[2] is not None and \
                    (ispyb_row[2] is None or ispyb_row[2] == ''):
                    self.target_conn.update_protein_name(uas_row[2], ispyb_row[4])
                break
            # IF no UAS sample ID in ispyb AND same UAS proposal ID AND same acronym:
            elif ispyb_row[0] is None and uas_row[1] == ispyb_row[1] and uas_row[3] == ispyb_row[3]:
                # IF state is 'Accepted'
                if uas_row[4] == 'Accepted':
                    # IF the protein's UAS ID doesn't already exist in ISPyB:
                    if 0 == target_conn.retrieve_number_of_proteins_for_uas_id(uas_row[0]):
                        self.target_conn.update_protein_uas_id(uas_row[0], ispyb_row[4])
                        # IF ISPyB name is empty
                        if ispyb_row[2] is None or ispyb_row[2] == '':
                            self.target_conn.update_protein_name(uas_row[2], ispyb_row[4])
                break
        else:
            if uas_row[4] == 'Accepted':
                # At this point we know the protein's UAS ID doesn't exist in ISPyB,
                # but we still need to make sure the acronym doesn't already exist in the proposal
                if 0 == self.target_conn.retrieve_number_of_proteins_for_proposal_and_acronym(uas_row[1], uas_row[3]):
                    ispyb_proposal_id = self.target_conn.retrieve_proposal_id_for_uas_id(uas_row[1])
                    if ispyb_proposal_id != None:
                        self.target_conn.insert_protein(uas_row[0], ispyb_proposal_id, uas_row[2], uas_row[3], 'ORIGIN:UAS')


  def sync_proposals_have_persons(self):
    uas_rs = self.source_conn.extract_proposals_have_persons()
    ispyb_rs = self.target_conn.extract_proposals_have_persons()

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

                if self.target_conn.uas_role_2_ispyb_role(uas_row[2]) != ispyb_row[2]:       # Compare roles
                    self.target_conn.update_proposal_has_person(uas_row[2], ispyb_row[3], ispyb_row[4])
                break
        else:
            pr_id = self.target_conn.retrieve_proposal_id_for_uas_id(uas_row[0])
            pe_id = self.target_conn.retrieve_person_id(uas_row[1])

            if pr_id != None and pe_id != None:
                self.target_conn.insert_proposal_has_person(self.target_conn.uas_role_2_ispyb_role(uas_row[2]), pr_id, pe_id)
            elif pr_id is None:
                logging.getLogger().debug("Not found: Proposal.externalId %s for personId %d" % (uas_row[0], pe_id if pe_id is not None else -1))
            elif pe_id is None:
                logging.getLogger().debug("Not found: Person.externalId %s for proposalId %d" % (uas_row[1], pr_id if pr_id is not None else -1))

  def sync_sessions_have_persons(self):
    uas_rs = self.source_conn.extract_sessions_have_persons()
    ispyb_rs = self.target_conn.extract_sessions_have_persons()

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

                if self.target_conn.uas_role_2_ispyb_role(uas_row[2]) != ispyb_row[2] or uas_is_remote != ispyb_is_remote:  # Compare roles and remote / on-site status
                    self.target_conn.update_session_has_person(uas_row[2], uas_is_remote, ispyb_row[3], ispyb_row[4])
                break
        else:
            s_id = self.target_conn.retrieve_session_id(uas_row[0])
            p_id = self.target_conn.retrieve_person_id(uas_row[1])
            is_remote = 1 if uas_row[3] == 0 else 0 if uas_row[3] == 1 else None

            if s_id != None and p_id != None:
                self.target_conn.insert_session_has_person(ispyb_conn.uas_role_2_ispyb_role(uas_row[2]), s_id, p_id, is_remote)
            elif s_id is None:
                logging.getLogger().debug("Not found: BLSession.externalId %s for personId %d" % (uas_row[0], p_id if p_id is not None else -1))
            elif p_id is None:
                logging.getLogger().debug("Not found: Person.externalId %s for sessionId %d" % (uas_row[1], s_id if s_id is not None else -1))
