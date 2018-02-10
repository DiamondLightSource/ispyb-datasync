import MySQLdb
import getopt
import logging
from logging.handlers import RotatingFileHandler
import sys
import os
import atexit
import signal
import time
import sched
import subprocess
import ConfigParser
        
        
            
class Replicator:
    def __init__(self, conf_file, log_file = None):

        self.connInactivity = None
        self.conn = None

        # Get configuration
        config = ConfigParser.RawConfigParser(allow_no_value=True)
        config.read(conf_file)

        self.ispyb_user = config.get('ISPyB', 'user')
        self.ispyb_pw = config.get('ISPyB', 'pw')
        self.ispyb_db = config.get('ISPyB', 'db')
        self.ispyb_host = config.get('ISPyB', 'host')
        self.ispyb_port = config.getint('ISPyB', 'port')

    def connect(self):
        self.conn = None
        self.ispyb_cursor = None
        try:
            self.conn = MySQLdb.connect(host=self.ispyb_host, \
                                        user=self.ispyb_user, \
                                        passwd=self.ispyb_pw, \
                                        db=self.ispyb_db, \
                                        port=int(self.ispyb_port))
        except Exception as e:
            logging.getLogger().exception("%s: error while connecting to ISPyB DB :-(" % sys.argv[0])
            raise
        else:
            try:
                self.conn.autocommit(True)
            except AttributeError:
                sys.exit("Failed to set autocommit.")

            logging.getLogger().info("%s: Connected to database %s on %s" % (sys.argv[0], self.conn.get_server_info(), self.conn.get_host_info()))
            logging.getLogger().info("%s:    Database user: %s" % (sys.argv[0], self.ispyb_user))
            logging.getLogger().info("%s:    DB name: %s" % (sys.argv[0], self.ispyb_db))
            
            try:
                self.ispyb_cursor = self.conn.cursor()
            except Exception as e:
                logging.getLogger().exception("%s: unable to create cursor :-(" % sys.argv[0])
                raise
            else:
                logging.getLogger().debug("%s: default cursor ok :-)" % sys.argv[0])

        return (self.conn, self.ispyb_cursor)

    def disconnect(self):
        '''Release the connection previously created.'''
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        return

    def dispose_cursor(self,cursor):
        if cursor is not None:
            cursor.close()
        else:
            logging.getLogger().warning("%s: trying to dispose of an unknown cursor :-P" % sys.argv[0])

    def cleanup(self, cursor=None):
        if cursor is not None:
            self.dispose_cursor(cursor)

    def do_query(self,querystr,cursor=None,return_fetch=True,return_id=False):
        if cursor is None:
            cursor=self.ispyb_cursor
            logging.getLogger().warning("%s: using default cursor :-P" % sys.argv[0])
            
        start_time=time.time()
        try:
            ret=cursor.execute(querystr)
        except:
            logging.getLogger().exception("%s: exception running sql statement :-(" % sys.argv[0])
            logging.getLogger().exception(querystr)
            raise
        else:
            logging.getLogger().debug("%s: query took %f seconds" %  (sys.argv[0], (time.time()-start_time)))

        if return_fetch:
            start_time=time.time()
            try:
                ret=cursor.fetchall()
            except:
                logging.getLogger().exception("%s: exception fetching cursor :-(" % sys.argv[0])
                raise
            logging.getLogger().debug("%s: fetch took %f seconds" %  (sys.argv[0], (time.time()-start_time)))
        elif return_id:
            start_time=time.time()
            
            #try:
            #    ret=int(self.icat_conn.insert_id())
            #except:
            #    logging.getLogger().exception("%s: exception getting inserted id :-(" % sys.argv[0])
            #    raise

            ret = cursor.lastrowid
            logging.getLogger().debug("%s: id took %f seconds" % (sys.argv[0], (time.time()-start_time)))

        return ret

    def update_person(self, login, family_name, given_name, cursor):
        if login is None:
            return None
        q_login = "'%s'" % login.replace("'", "''")
        if family_name is None:
            q_family_name = "NULL"
        else:
            q_family_name = "'%s'" % family_name.replace("'", "''")
        if given_name is None:
            q_given_name = "NULL"
        else:
            q_given_name = "'%s'" % given_name.replace("'", "''")
        
        sql = """UPDATE %s.Person SET familyName=%s, givenName=%s WHERE login=%s"""\
         % (self.ispyb_db, q_family_name, q_given_name, q_login)  
        logging.getLogger().debug(sql)
        return self.do_query(sql, cursor, return_fetch=False, return_id=False)

    def insert_person(self, login, cursor):
        if login is None:
            return None
        (sn, given_name) = self.ldapsearch_person(login)
        q_login = "'%s'" % login.replace("'", "''")
        q_sn = "'%s'" % sn.replace("'", "''")
        q_given_name = "'%s'" % given_name.replace("'", "''")
        
        sql = """insert into %s.Person (login, familyName, givenName) values (
                %s, %s, %s
            )""" % (self.ispyb_db, q_login, q_sn, q_given_name)  
        logging.getLogger().debug(sql)
        return self.do_query(sql, cursor, return_fetch=False, return_id=True)

    def ldapsearch_person(self, uid):
        output = subprocess.check_output("""ldapsearch -x -LLL -s sub -b uid=%s,ou=People,dc=diamond,dc=ac,dc=uk "(&(objectClass=person))" sn givenName""" % uid, stderr=subprocess.STDOUT, shell=True)
        if output is not None and output != '':
            lines = output.split('\n')
            sn = None
            given_name = None
            sn_l = [i for i in lines if i.startswith('sn:')]
            if len(sn_l) == 1:
                sn = sn_l[0][4:]
            given_name_l = [i for i in lines if i.startswith('givenName:')]
            if len(given_name_l) == 1:
                given_name = given_name_l[0][11:]
            return (sn, given_name)

    def ldapsearch_group(self, group_names):
        people_set = set()
        for group_name in group_names:
            output = subprocess.check_output("""ldapsearch -x -LLL -s sub "(&(objectClass=posixGroup)(cn=%s))" memberUid | grep 'memberUid:' | cut -c 12-""" % group_name, stderr=subprocess.STDOUT, shell=True)
            if output is not None and output != '':
                people = output.split('\n')
                people.pop() # remove last item which is empty
                people_set = people_set | set(people)
        return people_set

    def select_usergroup(self, usergroup, cursor):
        ugid = None
        sql = \
        """SELECT ug.userGroupId
        FROM %s.UserGroup ug
        WHERE ug.name = '%s'""" % (self.ispyb_db, usergroup)
        rs = self.do_query(sql, cursor, return_fetch=True)
        
        if rs is not None and len(rs) > 0 and rs[0] is not None:
            ugid = rs[0][0]
        else:
            return (None, None)

        sql = \
        """SELECT p.login, p.familyName, p.givenName 
        FROM %s.Person p
            INNER JOIN %s.UserGroup_has_Person ughp ON ughp.personId = p.personId 
        WHERE ughp.userGroupId = %s""" % (self.ispyb_db, self.ispyb_db, ugid)

        rs = self.do_query(sql, cursor, return_fetch=True)
        return (ugid, rs)
    
    
    def run_group_propagation(self, cursor):
        # sets of members of LDAP groups
        ldap_super_admin = self.ldapsearch_group(["dls_dasc"])
        ldap_mx_admin = self.ldapsearch_group(["mx_staff"])
        ldap_saxs_admin = self.ldapsearch_group(["b21_staff"])
        ldap_powder_admin = self.ldapsearch_group(["i11_staff"])
        ldap_tomo_admin = self.ldapsearch_group(["i12_staff", "b24_staff"])
        ldap_em_admin = self.ldapsearch_group(["m01_staff", "m02_staff", "m03_staff", "m04_staff"])
        

        # get sets (fed_id, DB personId) for members of DB usergroups
        (db_super_admin_ugid, db_super_admin) = self.select_usergroup("super_admin", cursor)
        (db_mx_admin_ugid, db_mx_admin) = self.select_usergroup("mx_admin", cursor)
        (db_saxs_admin_ugid, db_saxs_admin) = self.select_usergroup("saxs_admin", cursor)
        (db_powder_admin_ugid, db_powder_admin) = self.select_usergroup("powder_admin", cursor)
        (db_tomo_admin_ugid, db_tomo_admin) = self.select_usergroup("tomo_admin", cursor)
        (db_em_admin_ugid, db_em_admin) = self.select_usergroup("em_admin", cursor)

        groups = [  [db_super_admin_ugid, db_super_admin, ldap_super_admin], 
                    [db_mx_admin_ugid, db_mx_admin, ldap_mx_admin],
                    [db_saxs_admin_ugid, db_saxs_admin, ldap_saxs_admin], 
                    [db_powder_admin_ugid, db_powder_admin, ldap_powder_admin], 
                    [db_tomo_admin_ugid, db_tomo_admin, ldap_tomo_admin], 
                    [db_em_admin_ugid, db_em_admin, ldap_em_admin]
                ]
        for group in groups:
            ugid = group[0]
            db_group_members = group[1]
            ldap_group_members = group[2]

            if ugid is None or db_group_members is None:
                continue

            # Create DB logins set + update entry in Person table if needed 
            db_logins_set = set()
            for row in db_group_members:
                db_logins_set.add(row[0])
                (ldap_family_name, ldap_given_name) = self.ldapsearch_person(row[0])
                if ldap_family_name != row[1] or ldap_given_name != row[2]:
                    self.update_person(row[0], ldap_family_name, ldap_given_name, cursor)
            
            # The set of ldap_group_members after removing elements found in db_group_members:
            members_2_insert = ldap_group_members.difference(db_logins_set) 
            logging.getLogger().debug("members_2_insert")
            logging.getLogger().debug(members_2_insert)
            # The set of db_group_members after removing elements found in ldap_group_members
            members_2_delete = db_logins_set.difference(ldap_group_members) 
            logging.getLogger().debug("members_2_delete")
            logging.getLogger().debug(members_2_delete)
            
            if members_2_insert is not None:
                for member in members_2_insert:
                    self.insert_usergroup_has_person(ugid, member, cursor)
            if members_2_delete is not None:
                for member in members_2_delete:
                    self.delete_usergroup_has_person(ugid, member, cursor)


    def select_person(self, login, cursor):
        pid = None
        select  = "SELECT personId FROM %s.Person WHERE login = '%s'" % (self.ispyb_db, login)
        logging.getLogger().debug(select)
        rs = self.do_query(select, cursor, return_fetch=True)
        if rs is not None and len(rs) > 0 and rs[0] is not None:
            pid = rs[0][0]
        return pid

    def insert_usergroup_has_person(self, ugid, login, cursor):
        pid = self.select_person(login, cursor)
        if pid is None:
            pid = self.insert_person(login, cursor)
            
        insert = "INSERT INTO %s.UserGroup_has_Person (userGroupId, personId) values (%s, %s)" % (self.ispyb_db, ugid, pid)
        logging.getLogger().debug(insert)
        self.do_query(insert, cursor, return_fetch=False)
            
    def delete_usergroup_has_person(self, ugid, login, cursor):
        pid = self.select_person(login, cursor)
        if pid is not None:
            delete = "DELETE FROM %s.UserGroup_has_Person WHERE userGroupId = %s AND personId = %s" % (self.ispyb_db, ugid, pid)
            self.do_query(delete, cursor, return_fetch=False)
            
def printQuitMessage():
    logging.getLogger().info("%s: exiting python interpreter :-(" % sys.argv[0])
    logging.shutdown()

def printUsage():
    print "Script for replicating master data in LDAP about staff users into ISPyB"
    print "Syntax: " +sys.argv[0]+ " -c <configuration file> [-r]"
    print "Arguments: "
    print "     -h|--help : display this help"
    print "     -c|--conf <conf file> : use the given configuration file"
    print "     -l|--log <log file>: use the given log file"
    print "The default configuration file is config/credentials.cfg"

def killHandler(sig,frame):
    hostname = os.uname()[1]
    logging.getLogger().warning("%s: got SIGTERM on %s :-O" % (sys.argv[0], hostname))
    logging.shutdown()
    os._exit(-1)

    
if __name__ == '__main__':
    conf_file=None
    log_file=None

    # Get command-line arguments
    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:], "hc:l:", ["help", "conf", "log"])
    except getopt.GetoptError:
        printUsage()
        sys.exit(2)

    for o,a in opts:
        if o in ("-h", "--help"):
            printUsage()
            sys.exit()
        elif o in ("-c", "--conf"):
            conf_file=a
        elif o in ("-l", "--log"):
            log_file=a

    if conf_file is None:
        conf_file="config/credentials.cfg" 

    if log_file is None:
        log_file="replicator.log" 

    # Configure logging
    _logger = logging.getLogger()
    _logger.setLevel(logging.DEBUG)
    _formatter = logging.Formatter('* %(asctime)s [id=%(thread)d] <%(levelname)s> %(message)s')
    _hdlr = RotatingFileHandler(filename=log_file, maxBytes=1000000, backupCount=10)
    _hdlr.setFormatter(_formatter)
    logging.getLogger().addHandler(_hdlr)

    atexit.register(printQuitMessage)
    signal.signal(signal.SIGTERM,killHandler)
    r = Replicator(conf_file, log_file)
    (conn, c) = r.connect()

    r.run_group_propagation(c)
        
    r.dispose_cursor(c)
    r.disconnect()
    

