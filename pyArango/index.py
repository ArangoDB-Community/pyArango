import json
from .theExceptions import (CreationError, DeletionError, UpdateError)

class Index(object) :
    """An index on a collection's fields. Indexes are meant to de created by ensureXXX functions of Collections. 
Indexes have a .infos dictionary that stores all the infos about the index"""

    def __init__(self, collection, infos = None, creationData = None) :

        self.collection = collection
        self.connection = self.collection.database.connection
        self.indexesURL = "%s/index" % self.collection.database.URL
        self.infos = None
        if infos  :
            self.infos = infos
        elif creationData :
            self._create(creationData)

        if self.infos :
            self.URL = "%s/%s" % (self.indexesURL, self.infos["id"])

    def _create(self, postData) :
        """Creates an index of any type according to postData"""
        if self.infos is None :
            r = self.connection.session.post(self.indexesURL, params = {"collection" : self.collection.name}, data = json.dumps(postData))
            data = r.json()
            if (r.status_code >= 400) or data['error'] :
                raise CreationError(data['errorMessage'], data)
            self.infos = data

    def delete(self) :
        """Delete the index"""
        r = self.connection.session.delete(self.URL)
        data = r.json()
        if (r.status_code != 200 and r.status_code != 202) or data['error'] :
            raise DeletionError(data['errorMessage'], data)
