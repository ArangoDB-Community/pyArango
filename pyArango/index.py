import json
from .theExceptions import (CreationError, DeletionError, UpdateError)

class Index(object):
    """An index on a collection's fields. Indexes are meant to de created by ensureXXX functions of Collections. 
Indexes have a .infos dictionary that stores all the infos about the index"""

    def __init__(self, collection, infos = None, creationData = None):

        self.collection = collection
        self.connection = self.collection.database.connection
        self.infos = None
        
        if infos:
            self.infos = infos
        elif creationData:
            self._create(creationData)

    def getURL(self):
        if self.infos:
            return "%s/%s" % (self.getIndexesURL(), self.infos["id"])
        return None

    def getIndexesURL(self):
        return "%s/index" % self.collection.database.getURL()

    def _create(self, postData):
        """Creates an index of any type according to postData"""
        if self.infos is None:
            r = self.connection.session.post(self.getIndexesURL(), params = {"collection" : self.collection.name}, data = json.dumps(postData, default=str))
            data = r.json()
            if (r.status_code >= 400) or data['error']:
                raise CreationError(data['errorMessage'], data)
            self.infos = data

    def delete(self):
        """Delete the index"""
        r = self.connection.session.delete(self.getURL())
        data = r.json()
        if (r.status_code != 200 and r.status_code != 202) or data['error']:
            raise DeletionError(data['errorMessage'], data)
