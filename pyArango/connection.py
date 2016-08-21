#!/usr/bin/env python
"""main pyArongo connection class"""

import json
import requests

from .database import Database, DBHandle
from .theExceptions import CreationError, ConnectionError
from .users import Users


class Connection(object):
    """This is the entry point in pyArango and directly handles databases."""
    def __init__(self, arango_url='http://127.0.0.1:8529', username="", password="",
                 arangoURL=""):
        self.arango_url = arango_url
        if arangoURL:
            # TODO deprecation warning
            self.arango_url = arangoURL
            self.arangoURL = arangoURL
        self.base_url = arango_url.rstrip("/")  # shouldn't have trailing slashes
        self.api_url = self.base_url + "/_api"
        self.db_url = self.api_url + "/database/user"
        self.username = username
        self.password = password
        self.databases = {}
        self.session = requests.Session()

        if self.username:
            self.session.auth = (self.username, self.password)
            self.db_url = self.api_url + "/user/" + self.username + "/database"

        self.users = Users(self)
        self.reload()

    def disconnectSession(self):
        if self.session:
            self.session.close()

    def resetSession(self, username="", password=""):
        """resets the session"""
        self.disconnectSession()
        self.username = username or self.username
        self.password = password or self.password
        self.session = requests.Session()

        if self.username:
            self.session.auth = (self.username, self.password)
            self.db_url = self.api_url + "/user/" + self.username + "/database"

    def reload(self):
        """
        Reloads the database list.  Because loading a database triggers the
        loading of all collections and graphs within, only handles are loaded
        when this function is called. The full databases are loaded on demand
        when accessed """

        result = self.session.get(self.db_url)

        data = result.json()
        if result.status_code == 200 and not data["error"]:
            self.databases = {}
            for db_name in data["result"]:
                if db_name not in self.databases:
                    self.databases[db_name] = DBHandle(self, db_name)
        else:
            raise ConnectionError(data["errorMessage"], self.db_url,
                                  result.status_code, result.content)

    def createDatabase(self, name, **dbArgs):
        """
        use dbArgs for arguments other than name. for a full list of arguments
        please have a look at arangoDB's doc
        """
        dbArgs['name'] = name
        payload = json.dumps(dbArgs)
        url = self.api_url + "/database"
        result = self.session.post(url, data=payload)
        data = result.json()
        if result.status_code == 201 and not data["error"]:
            self.databases[name] = Database(self, name)
            return self.databases[name]
        else:
            raise CreationError(data["errorMessage"], result.content)

    def hasDatabase(self, name):
        """
        returns true/false wether the connection has a database by the name of 'name'
        """
        return name in self.databases

    def __getitem__(self, db_name):
        """
        Collection[db_name] returns a database by the name of 'dbName',
        raises a KeyError if not found
        """
        try:
            return self.databases[db_name]
        except KeyError:
            self.reload()
            try:
                return self.databases[db_name]
            except KeyError:
                raise KeyError("Can't find any database named: %s" % db_name)
