from base64 import b64decode
import requests
from requests import exceptions as requests_exceptions
import time
import uuid
import json as json_mod

from datetime import datetime

from .database import Database, DBHandle
from .theExceptions import CreationError, ConnectionError
from .users import Users

import grequests
import gevent
from gevent.threading import Lock
import logging


class JWTAuth(requests.auth.AuthBase):

    # Half a day before the actual expiration.
    REAUTH_TIME_INTERVEL = 43200

    def __init__(
            self, username, password, urls, use_lock_for_reseting_jwt=True,
            max_retries=5
    ):
        self.username = username
        self.password = password
        self.urls = urls
        self.lock_for_reseting_jwt = Lock() if use_lock_for_reseting_jwt else None
        self.__init_request_session(max_retries)
        self.__set_token()

    def __init_request_session(self, max_retries):
        self.max_retries = max_retries
        self.session = requests.Session()
        http = requests.adapters.HTTPAdapter(max_retries=max_retries)
        https = requests.adapters.HTTPAdapter(max_retries=max_retries)
        self.session.mount('http://', http)
        self.session.mount('https://', https)

    def __parse_token(self):
        decoded_token = b64decode(self.token.split('.')[1].encode())
        return json_mod.loads(decoded_token.decode())

    def __get_auth_token(self):
        request_data = '{"username":"%s","password":"%s"}' % (self.username, self.password)
        for connection_url in self.urls:
            try:
                response = self.session.post('%s/_open/auth' % connection_url, data=request_data)
                if response.ok:
                    json_data = response.content
                    if json_data:
                        data_dict = json_mod.loads(json_data.decode("utf-8"))
                        return data_dict.get('jwt')
            except requests_exceptions.ConnectionError:
                if connection_url is not self.urls[-1]:
                    logging.critical("Unable to connect to %s trying another", connection_url)
                else:
                    logging.critical("Unable to connect to any of the urls: %s", self.urls)
                    raise

    def __set_token(self):
        self.token = self.__get_auth_token()
        self.parsed_token = \
            self.__parse_token() if self.token is not None else {}
        self.token_last_updated = time.time()

    def reset_token(self):
        logging.warning("Reseting the token.")
        self.__set_token()

    def is_token_expired(self):
        return (
            self.parsed_token.get("exp", 0) - time.time() <
            JWTAuth.REAUTH_TIME_INTERVEL
        )

    def __call__(self, req):
        # Implement JWT authentication
        if self.is_token_expired():
            if self.lock_for_reseting_jwt is not None:
                self.lock_for_reseting_jwt.acquire()
            if self.is_token_expired():
                self.reset_token()
            if self.lock_for_reseting_jwt is not None:
                self.lock_for_reseting_jwt.release()
        req.headers['Authorization'] = 'Bearer %s' % self.token
        return req


class AikidoSession_GRequests(object):
    """A version of Aikido that uses grequests and can bacth several requests together"""

    def __init__(self, username, password, urls, use_lock_for_reseting_jwt=True, max_retries=5):
        self.max_retries = max_retries
        if username:
            self.auth = JWTAuth(
                username, password, urls, use_lock_for_reseting_jwt, max_retries
            )
        else:
            self.auth = None

    def __reset_auth(self):
        if self.auth.lock_for_reseting_jwt is not None:
            self.auth.lock_for_reseting_jwt.acquire()
        self.auth.reset_token()
        if self.auth.lock_for_reseting_jwt is not None:
            self.auth.lock_for_reseting_jwt.release()

    def _run(self, req):
        """Run request or append it to the the current batch"""
        for _ in range(self.max_retries):
            gevent.joinall([gevent.spawn(req.send)])
            if hasattr(req, 'exception'):
                logging.critical("%s is raised, will try to reset the auth and request again.", req.exception)
                self.__reset_auth()
            elif req.response.status_code == 401:
                logging.critical("Invalid authentication token provided, will try to reset the auth and request again.")
                self.__reset_auth()
            else:
                return req.response
        logging.critical("Tried to send the request max number of times.")
        return req.response

    def post(self, url, data=None, json=None, **kwargs):
        """HTTP Method"""
        if data is not None:
            kwargs['data'] = data
        if json is not None:
            kwargs['json'] = json

        kwargs['auth'] = self.auth

        req = grequests.post(url, **kwargs)
        return self._run(req)

    def get(self, url, **kwargs):
        """HTTP Method"""
        kwargs['auth'] = self.auth
        req = grequests.get(url, **kwargs)
        return self._run(req)

    def put(self, url, data=None, **kwargs):
        """HTTP Method"""
        if data is not None:
            kwargs['data'] = data
        kwargs['auth'] = self.auth
        req = grequests.put(url, **kwargs)
        return self._run(req)

    def head(self, url, **kwargs):
        """HTTP Method"""
        kwargs['auth'] = self.auth
        req = grequests.put(url, **kwargs)
        return self._run(req)

    def options(self, url, **kwargs):
        """HTTP Method"""
        kwargs['auth'] = self.auth
        req = grequests.options(url, **kwargs)
        return self._run(req)

    def patch(self, url, data=None, **kwargs):
        """HTTP Method"""
        if data is not None:
            kwargs['data'] = data
        kwargs['auth'] = self.auth
        req = grequests.patch(url, **kwargs)
        return self._run(req)

    def delete(self, url, **kwargs):
        """HTTP Method"""
        kwargs['auth'] = self.auth
        req = grequests.delete(url, **kwargs)
        return self._run(req)

    def disconnect(self):
        pass

class JsonHook(object):
    """This one replaces requests' original json() function. If a call to json() fails, it will print a message with the request content"""
    def __init__(self, ret):
        self.ret = ret
        self.ret.json_originalFct = self.ret.json

    def __call__(self, *args, **kwargs):
        try :
            return self.ret.json_originalFct(*args, **kwargs)
        except Exception as e :
            print( "Unable to get json for request: %s. Content: %s" % (self.ret.url, self.ret.content) )
            raise e

class AikidoSession(object):
    """Magical Aikido being that you probably do not need to access directly that deflects every http request to requests in the most graceful way.
    It will also save basic stats on requests in it's attribute '.log'.
    """

    class Holder(object):
        def __init__(self, fct, auth, verify=True):
            self.fct = fct
            self.auth = auth
            if verify != None:
              self.verify = verify

        def __call__(self, *args, **kwargs):
            if self.auth :
                kwargs["auth"] = self.auth
            if self.verify != True:
                kwargs["verify"] = self.verify

            try :
                ret = self.fct(*args, **kwargs)
            except :
                print ("===\nUnable to establish connection, perhaps arango is not running.\n===")
                raise

            if len(ret.content) < 1:
                raise ConnectionError("Empty server response", ret.url, ret.status_code, ret.content)
            elif ret.status_code == 401 :
                raise ConnectionError("Unauthorized access, you must supply a (username, password) with the correct credentials", ret.url, ret.status_code, ret.content)

            ret.json = JsonHook(ret)
            return ret

    def __init__(self, username, password, verify=True, max_retries=5):
        if username :
            self.auth = (username, password)
        else :
            self.auth = None

        self.verify = verify
        self.session = requests.Session()
        http = requests.adapters.HTTPAdapter(max_retries=max_retries)
        https = requests.adapters.HTTPAdapter(max_retries=max_retries)
        self.session.mount('http://', http)
        self.session.mount('https://', https)

        self.log = {}
        self.log["nb_request"] = 0
        self.log["requests"] = {}

    def __getattr__(self, k):
        try :
            reqFct = getattr(object.__getattribute__(self, "session"), k)
        except :
            raise AttributeError("Attribute '%s' not found (no Aikido move available)" % k)

        holdClass = object.__getattribute__(self, "Holder")
        auth = object.__getattribute__(self, "auth")
        verify = object.__getattribute__(self, "verify")
        log = object.__getattribute__(self, "log")
        log["nb_request"] += 1
        try :
            log["requests"][reqFct.__name__] += 1
        except :
            log["requests"][reqFct.__name__] = 1

        return holdClass(reqFct, auth, verify)

    def disconnect(self):
        try:
            self.session.close()
        except Exception :
            pass

class Connection(object):
    """This is the entry point in pyArango and directly handles databases.
    @param arangoURL: can be either a string url or a list of string urls to different coordinators
    @param use_grequests: allows for running concurent requets."""

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
        use_grequests = True,
        use_lock_for_reseting_jwt=True,
        max_retries=5
    ):

        if loadBalancing not in Connection.LOAD_BLANCING_METHODS:
            raise ValueError("loadBalancing should be one of : %s, got %s" % (Connection.LOAD_BLANCING_METHODS, loadBalancing) )

        self.loadBalancing = loadBalancing
        self.currentURLId = 0
        self.username = username
        self.use_grequests = use_grequests
        self.use_lock_for_reseting_jwt = use_lock_for_reseting_jwt
        self.max_retries = max_retries

        self.databases = {}
        self.verbose = verbose

        if type(arangoURL) is str :
            self.arangoURL = [arangoURL]
        else :
            self.arangoURL = arangoURL

        for i, url in enumerate(self.arangoURL):
            if url[-1] == "/" :
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
        if self.loadBalancing == "round-robin" :
            url = self.arangoURL[self.currentURLId]
            self.currentURLId = (self.currentURLId + 1) % len(self.arangoURL)
            return url
        elif self.loadBalancing == "random" :
            import random
            return random.choice(self.arangoURL)

    def getURL(self):
        """return an URL for the connection"""
        return '%s/_api' % self.getEndpointURL()

    def getDatabasesURL(self):
        """return an URL to the databases"""
        if not self.session.auth :
            return '%s/database/user' % self.getURL()
        else :
            return '%s/user/%s/database' % (self.getURL(), self.username)

    def updateEndpoints(self, coordinatorURL = None):
        """udpdates the list of available endpoints from the server"""
        raise NotImplemented("Not done yet.")

    def disconnectSession(self):
        if self.session:
            self.session.disconnect()

    def resetSession(self, username=None, password=None, verify=True):
        """resets the session"""
        self.disconnectSession()
        if self.use_grequests :
            self.session = AikidoSession_GRequests(username, password, self.arangoURL, self.use_lock_for_reseting_jwt, self.max_retries)
        else :
            self.session = AikidoSession(username, password, verify, self.max_retries)

    def reload(self):
        """Reloads the database list.
        Because loading a database triggers the loading of all collections and graphs within,
        only handles are loaded when this function is called. The full databases are loaded on demand when accessed
        """

        r = self.session.get(self.getDatabasesURL())

        data = r.json()
        if r.status_code == 200 and not data["error"] :
            self.databases = {}
            for dbName in data["result"] :
                if dbName not in self.databases :
                    self.databases[dbName] = DBHandle(self, dbName)
        else :
            raise ConnectionError(data["errorMessage"], self.getDatabasesURL(), r.status_code, r.content)

    def createDatabase(self, name, **dbArgs):
        "use dbArgs for arguments other than name. for a full list of arguments please have a look at arangoDB's doc"
        dbArgs['name'] = name
        payload = json_mod.dumps(dbArgs, default=str)
        url = self.getURL() + "/database"
        r = self.session.post(url, data = payload)
        data = r.json()
        if r.status_code == 201 and not data["error"] :
            db = Database(self, name)
            self.databases[name] = db
            return self.databases[name]
        else :
            raise CreationError(data["errorMessage"], r.content)

    def hasDatabase(self, name):
        """returns true/false wether the connection has a database by the name of 'name'"""
        return name in self.databases

    def __getitem__(self, dbName):
        """Collection[dbName] returns a database by the name of 'dbName', raises a KeyError if not found"""
        try :
            return self.databases[dbName]
        except KeyError :
            self.reload()
            try :
                return self.databases[dbName]
            except KeyError :
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
