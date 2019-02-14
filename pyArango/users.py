from .theExceptions import ConnectionError, CreationError, DeletionError, UpdateError
import json


class User(object):
    """
    This class represents a user
    """
    def __init__(self, users, json_data=None):
        if json_data is None:
            json_data = {}
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
        if len(json_data) > 0:
            self._set(json_data)

    def _set(self, json_data):
        """
        Initialize all fields at once. If no password is specified, it will be set as an empty string
        """
        
        self["username"] = json_data["user"]
        self["active"] = json_data["active"]
        self["extra"] = json_data["extra"]
        try:
            self["changePassword"] = json_data["changePassword"]
        except Exception as e:
            pass
            # self["changePassword"] = ""
        
        try:
            self["password"] = json_data["passwd"]
        except KeyError:
            self["password"] = ""

        self.URL = "%s/user/%s" % (self.connection.URL, self["username"])

    def save(self):
        """
        Save/updates the user
        """
        payload = {}
        payload.update(self._store)
        payload["user"] = payload["username"]
        payload["passwd"] = payload["password"]
        del(payload["username"])
        del(payload["password"])

        payload = json.dumps(payload, default=str)
        if not self.URL:
            if "username" not in self._store or "password" not in self._store:
                raise KeyError("You must define self['name'] and self['password'] to be able to create a new user")

            response = self.connection.session.post(self.users.URL, data=payload)
            data = response.json()
            if response.status_code == 201:
                self._set(data)
            else:
                raise CreationError("Unable to create new user", data)
        else:
            response= self.connection.session.put(self.URL, data=payload)
            data = response.json()
            if response.status_code == 200:
                self._set(data)
            else:
                raise UpdateError("Unable to update user, status: %s" %response.status_code, data)

    def setPermissions(self, db_name, access):
        """
        Grant revoke rights on a database, 'access' is supposed to be boolean. ArangoDB grants/revokes both read and write rights at the same time
        """

        if not self.URL:
            raise CreationError("Please save user first", None, None)

        rights = []
        if access:
            rights.append("rw")

        rights = ''.join(rights)

        if not self.connection.has_database(db_name):
            raise KeyError("Unknown database: %s" % db_name)

        url = "%s/database/%s" % (self.URL, db_name)
        response = self.connection.session.put(url, data = json.dumps({"grant": rights}, default=str))
        if response.status_code < 200 or r.status_code > 202:
            raise CreationError("Unable to grant rights", response.content)

    def delete(self):
        """
        Permanently remove the user
        """
        if not self.URL:
            raise CreationError("Please save user first", None, None)

        response = self.connection.session.delete(self.URL)
        if response.status_code < 200 or response.status_code > 202:
            raise DeletionError("Unable to delete user, url: %s, status: %s" %(response.url, response.status_code), response.content )

        self.URL = None

    def __repr__(self):
        return "ArangoUser: %s" % (self._store)

    def __setitem__(self, k, v):
        if k not in list(self._store.keys()):
            raise KeyError("The only keys available for user are: %s" % (list(self._store.keys())))
        self._store[k] = v

    def __getitem__(self, k):
        return self._store[k]

class Users(object):
    """
    This one manages users.
    """
    def __init__(self, connection):
        self.connection = connection
        self.URL = "%s/user" % (self.connection.URL)

    def create_user(self, username, password):
        u = User(self)
        u["username"] = username
        u["password"] = password
        return u

    def fetch_all_users(self, raw_results = False):
        """Returns all available users. if raw_results, the result will be a list of python dicts instead of User objects"""
        response = self.connection.session.get(self.URL)
        if response.status_code == 200:
            data = response.json()
            if raw_results:
                return data["result"]
            else:
                results = []
                for result in data["result"]:
                    user = User(self, result)
                    results.append(user)
                return results
        else:
            raise ConnectionError("Unable to get user list", response.url, response.status_code)

    def fetch_user(self, username, raw_results = False):
        """
        Returns a single user. if raw_results, the result will be a list of python dicts instead of User objects
        """
        url = "%s/%s" % (self.URL, username)

        response = self.connection.session.get(url)
        if response.status_code == 200:
            data = response.json()
            if raw_results:
                return data["result"]
            else:
                user = User(self, data)
                return user
        else:
            raise KeyError("Unable to get user: %s" % username)

    def __getitem__(self, k):
        return self.fetch_user(k)
