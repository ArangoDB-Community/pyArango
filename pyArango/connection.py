import uuid
import json as json_mod
from datetime import datetime

import requests
import base64
import tempfile
import shutil

from .action import ConnectionAction
from .database import Database, DBHandle
from .theExceptions import CreationError, ConnectionError
from .users import Users

from .ca_certificate import CA_Certificate

from json.decoder import JSONDecodeError

class JsonHook(object):
    """This one replaces requests' original json() function. If a call to json() fails, it will print a message with the request content"""
    def __init__(self, ret):
        self.ret = ret
        self.ret.json_originalFct = self.ret.json

    def __call__(self, *args, **kwargs):
        try:
            return self.ret.json_originalFct(*args, **kwargs)
        except Exception as e:
            print( "Unable to get json for request: %s. Content: %s" % (self.ret.url, self.ret.content) )
            raise e

class AikidoSession(object):
    """Magical Aikido being that you probably do not need to access directly that deflects every http request to requests in the most graceful way.
    It will also save basic stats on requests in it's attribute '.log'.
    """

    class Holder(object):
        def __init__(self, fct, auth, max_conflict_retries=5, verify=True):
            self.fct = fct
            self.auth = auth
            self.max_conflict_retries = max_conflict_retries
            if not isinstance(verify, bool) and not isinstance(verify, CA_Certificate) and not not isinstance(verify, str) :
                raise ValueError("'verify' argument can only be of type: bool, CA_Certificate or str ")
            self.verify = verify

        def __call__(self, *args, **kwargs):
            if self.auth:
                kwargs["auth"] = self.auth
            if isinstance(self.verify, CA_Certificate):
                kwargs["verify"] = self.verify.get_file_path()
            else :
                kwargs["verify"] = self.verify

            try:
                do_retry = True
                retry = 0
                while do_retry and retry < self.max_conflict_retries :
                    ret = self.fct(*args, **kwargs)
                    do_retry = ret.status_code == 1200
                    try :
                        data = ret.json()
                        do_retry = do_retry or ("errorNum" in data and data["errorNum"] == 1200) 
                    except JSONDecodeError:
                        pass
                    
                    retry += 1
            except:
                print ("===\nUnable to establish connection, perhaps arango is not running.\n===")
                raise

            if len(ret.content) < 1:
                raise ConnectionError("Empty server response", ret.url, ret.status_code, ret.content)
            elif ret.status_code == 401:
                raise ConnectionError("Unauthorized access, you must supply a (username, password) with the correct credentials", ret.url, ret.status_code, ret.content)

            ret.json = JsonHook(ret)
            return ret

    def __init__(self, username, password, verify=True, max_conflict_retries=5, max_retries=5, single_session=True, log_requests=False):
        if username:
            self.auth = (username, password)
        else:
            self.auth = None

        self.verify = verify
        self.max_retries = max_retries
        self.log_requests = log_requests
        self.max_conflict_retries = max_conflict_retries

        self.session = None
        if single_session:
            self.session = self._make_session()

        if log_requests:
            self.log = {}
            self.log["nb_request"] = 0
            self.log["requests"] = {}

    def _make_session(self):
        session = requests.Session()
        http = requests.adapters.HTTPAdapter(max_retries=self.max_retries)
        https = requests.adapters.HTTPAdapter(max_retries=self.max_retries)
        session.mount('http://', http)
        session.mount('https://', https)

        return session

    def __getattr__(self, request_function_name):
        if self.session is not None:
            session = self.session
        else :
            session = self._make_session()

        try:
            request_function = getattr(session, request_function_name)
        except AttributeError:
            raise AttributeError("Attribute '%s' not found (no Aikido move available)" % request_function_name)

        auth = object.__getattribute__(self, "auth")
        verify = object.__getattribute__(self, "verify")
        if self.log_requests:
            log = object.__getattribute__(self, "log")
            log["nb_request"] += 1
            log["requests"][request_function.__name__] += 1

        return AikidoSession.Holder(request_function, auth, max_conflict_retries=self.max_conflict_retries, verify=verify)

    def disconnect(self):
        pass

class Connection(object):
    """This is the entry point in pyArango and directly handles databases.
    @param arangoURL: can be either a string url or a list of string urls to different coordinators
    @param use_grequests: allows for running concurent requets.

    Parameters
    ----------
    arangoURL: list or str
        list of urls or url for connecting to the db

    username: str
        for credentials
    password: str
        for credentials
    verify: bool
        check the validity of the CA certificate
    verbose: bool
        flag for addictional prints during run
    statsdClient: instance
        statsd instance    
    reportFileName: str
        where to save statsd report
    loadBalancing: str
        type of load balancing between collections
    use_grequests: bool
        parallelise requests using gevents. Use with care as gevents monkey patches python, this could have unintended concequences on other packages
    use_jwt_authentication: bool
        use JWT authentication
    use_lock_for_reseting_jwt: bool
        use lock for reseting gevents authentication
    max_retries: int
        max number of retries for a request
    max_conflict_retries: int
        max number of requests for a conflict error (1200 arangodb error). Does not work with gevents (grequests),
    """

    LOAD_BLANCING_METHODS = {'round-robin', 'random'}

    def __init__(self,
        arangoURL = 'http://127.0.0.1:8529',
        username = None,
        password = None,
        verify = True,
        verbose = False,
        statsdClient = None,
        reportFileName = None,
        loadBalancing = "round-robin",
        use_grequests = False,
        use_jwt_authentication=False,
        use_lock_for_reseting_jwt=True,
        max_retries=5,
        max_conflict_retries=5
    ):

        if loadBalancing not in Connection.LOAD_BLANCING_METHODS:
            raise ValueError("loadBalancing should be one of : %s, got %s" % (Connection.LOAD_BLANCING_METHODS, loadBalancing) )

        self.loadBalancing = loadBalancing
        self.currentURLId = 0
        self.username = username
        self.use_grequests = use_grequests
        self.use_jwt_authentication = use_jwt_authentication
        self.use_lock_for_reseting_jwt = use_lock_for_reseting_jwt
        self.max_retries = max_retries
        self.max_conflict_retries = max_conflict_retries
        self.action = ConnectionAction(self)

        self.databases = {}
        self.verbose = verbose

        if isinstance(arangoURL, str):
            self.arangoURL = [arangoURL]
        else:
            self.arangoURL = arangoURL

        for i, url in enumerate(self.arangoURL):
            if url[-1] == "/":
                self.arangoURL[i] = url[:-1]

        self.identifier = None
        self.startTime = None
        self.session = None
        self.resetSession(username, password, verify)

        self.users = Users(self)

        if reportFileName != None:
            self.reportFile = open(reportFileName, 'a')
        else:
            self.reportFile = None

        self.statsdc = statsdClient
        self.reload()

    def getEndpointURL(self):
        """return an endpoint url applying load balacing strategy"""
        if self.loadBalancing == "round-robin":
            url = self.arangoURL[self.currentURLId]
            self.currentURLId = (self.currentURLId + 1) % len(self.arangoURL)
            return url
        elif self.loadBalancing == "random":
            import random
            return random.choice(self.arangoURL)

    def getURL(self):
        """return an URL for the connection"""
        return '%s/_api' % self.getEndpointURL()

    def getDatabasesURL(self):
        """return an URL to the databases"""
        if not self.session.auth:
            return '%s/database/user' % self.getURL()
        else:
            return '%s/user/%s/database' % (self.getURL(), self.username)

    def updateEndpoints(self, coordinatorURL = None):
        """udpdates the list of available endpoints from the server"""
        raise NotImplementedError("Not done yet.")

    def disconnectSession(self):
        if self.session:
            self.session.disconnect()

    def getVersion(self):
        """fetches the arangodb server version"""
        r = self.session.get(self.getURL() + "/version")
        data = r.json()
        if r.status_code == 200 and not "error" in data:
            return data
        else:
            raise CreationError(data["errorMessage"], data)

    def resetSession(self, username=None, password=None, verify=True):
        """resets the session"""
        self.disconnectSession()
        if self.use_grequests:
            from .gevent_session import AikidoSession_GRequests
            self.session = AikidoSession_GRequests(
                username, password, self.arangoURL,
                self.use_jwt_authentication,
                self.use_lock_for_reseting_jwt, self.max_retries,
                verify
            )
        else:
            # self.session = AikidoSession(username, password, verify, self.max_retries)
            self.session = AikidoSession(
                username=username,
                password=password,
                verify=verify,
                single_session=True,
                max_conflict_retries=self.max_conflict_retries,
                max_retries=self.max_retries,
                log_requests=False
            )

    def reload(self):
        """Reloads the database list.
        Because loading a database triggers the loading of all collections and graphs within,
        only handles are loaded when this function is called. The full databases are loaded on demand when accessed
        """

        r = self.session.get(self.getDatabasesURL())

        data = r.json()
        if r.status_code == 200 and not data["error"]:
            self.databases = {}
            for dbName in data["result"]:
                if dbName not in self.databases:
                    self.databases[dbName] = DBHandle(self, dbName)
        else:
            raise ConnectionError(data["errorMessage"], self.getDatabasesURL(), r.status_code, r.content)

    def createDatabase(self, name, **dbArgs):
        "use dbArgs for arguments other than name. for a full list of arguments please have a look at arangoDB's doc"
        dbArgs['name'] = name
        payload = json_mod.dumps(dbArgs, default=str)
        url = self.getURL() + "/database"
        r = self.session.post(url, data = payload)
        data = r.json()
        if r.status_code == 201 and not data["error"]:
            db = Database(self, name)
            self.databases[name] = db
            return self.databases[name]
        else:
            raise CreationError(data["errorMessage"], r.content)

    def hasDatabase(self, name):
        """returns true/false wether the connection has a database by the name of 'name'"""
        return name in self.databases

    def __getitem__(self, dbName):
        """Collection[dbName] returns a database by the name of 'dbName', raises a KeyError if not found"""
        try:
            return self.databases[dbName]
        except KeyError:
            self.reload()
            try:
                return self.databases[dbName]
            except KeyError:
                raise KeyError("Can't find any database named : %s" % dbName)

    def reportStart(self, name):
        if self.statsdc != None:
            self.identifier = str(uuid.uuid5(uuid.NAMESPACE_DNS, name))[-6:]
            if self.reportFile != None:
                self.reportFile.write("[%s]: %s\n" % (self.identifier, name))
                self.reportFile.flush()
            self.startTime = datetime.now()

    def reportItem(self):
        if self.statsdc != None:
            diff = datetime.now() - self.startTime
            microsecs = (diff.total_seconds() * (1000 ** 2) ) + diff.microseconds
            self.statsdc.timing("pyArango_" + self.identifier, int(microsecs))
