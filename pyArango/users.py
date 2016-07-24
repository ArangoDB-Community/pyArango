from .theExceptions import ConnectionError, CreationError, DeletionError, UpdateError

class User(object) :
    """This class represents a user"""
    def __init__(self, users, jsonData = {}) :
        self._store = {}
        self.users = users
        self.connection = self.users.connection

        self._store = {
            "username": None,
            "active": True,
            "extra": None,
            "changePassword": None,
            "password": None,
        }

        self.URL = None
        if len(jsonData) > 0 :
            self._set(jsonData)

    def _set(self, jsonData) :
        """Initialize all fields at once. If no password is specified, it will be set as an empty string"""
        self["username"] = jsonData["user"]
        self["active"] = jsonData["active"]
        self["extra"] = jsonData["extra"]
        self["changePassword"] = jsonData["changePassword"]
        try :
            self["password"] = jsonData["passwd"]
        except KeyError :
            self["password"] = ""

        self.URL = "%s/user/%s" % (self.connection.URL, self["username"])

    def save(self):
        """Save/updates the user"""

        import json

        payload = {}
        payload.update(self._store)
        payload["user"] = payload["username"]
        payload["passwd"] = payload["password"]
        del(payload["username"])
        del(payload["password"])

        payload = json.dumps(payload)
        if not self.URL :
            if "username" not in self._store or "password" not in self._store :
                raise KeyError("You must define self['name'] and self['password'] to be able to create a new user")    

            r = self.connection.session.post(self.users.URL, data = payload)
            data = r.json()
            if r.status_code == 201 :
                self._set(data)
            else :
                raise CreationError("Unable to create new user", data)
        else :
            r = self.connection.session.put(self.URL, data = payload)
            data = r.json()
            if r.status_code == 200 :
                self._set(data)
            else :
                raise UpdateError("Unable to update user, status: %s" %r.status_code, data)

    def setPermissions(self, dbName, access) :
        """Grant revoke rights on a database, 'access' is supposed to be boolean. ArangoDB grants/revokes both read and write rights at the same time"""
        import json

        if not self.URL :
            raise CreationError("Please save user first", None, None)

        rights = []
        if access :
            rights.append("rw")

        rights = ''.join(rights)

        if not self.connection.hasDatabase(dbName) :
            raise KeyError("Unknown database: %s" % dbName)

        url = "%s/database/%s" % (self.URL, dbName)
        r = self.connection.session.put(url, data = json.dumps({"grant": rights}))
        if r.status_code < 200 or r.status_code > 202 :
            raise CreationError("Unable to grant rights", r.content)

    def delete(self) :
        """Permanently remove the user"""
        if not self.URL :
            raise CreationError("Please save user first", None, None)

        r = self.connection.session.delete(self.URL)
        if r.status_code < 200 or r.status_code > 202 :
            raise DeletionError("Unable to delete user, url: %s, status: %s" %(r.url, r.status_code), r.content )

        self.URL = None

    def __repr__(self) :
        return "ArangoUser: %s" % (self._store)

    def __setitem__(self, k, v) :
        if k not in list(self._store.keys()) :
            raise KeyError("The only keys available for user are: %s" % (list(self._store.keys())))
        self._store[k] = v

    def __getitem__(self, k) :
        return self._store[k]

class Users(object) :
    """This one manages users."""
    def __init__(self, connection) :
        self.connection = connection
        self.URL = "%s/user" % (self.connection.URL)

    def createUser(self, username, password) :
        u = User(self)
        u["username"] = username
        u["password"] = password
        return u

    def fetchAllUsers(self, rawResults = False) :
        """Returns all available users. if rawResults, the result will be a list of python dicts instead of User objects"""
        r = self.connection.session.get(self.URL)
        if r.status_code == 200 :
            data = r.json()
            if rawResults :
                return data["result"]
            else :
                res = []
                for resu in data["result"] :
                    u = User(self, resu)
                    res.append(u)
                return res
        else :
            raise ConnectionError("Unable to get user list", r.url, r.status_code)

    def fetchUser(self, username, rawResults = False) :
        """Returns a single user. if rawResults, the result will be a list of python dicts instead of User objects"""
        url = "%s/%s" % (self.URL, username)

        r = self.connection.session.get(url)
        if r.status_code == 200 :
            data = r.json()
            if rawResults :
                return data["result"]
            else :
                u = User(self, data)
                return u
        else :
            raise KeyError("Unable to get user: %s" % username)

    def __getitem__(self, k) :
        return self.fetchUser(k)
