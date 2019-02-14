import json
from .theExceptions import (CreationError, DeletionError, UpdateError)

class Index(object):
    """
    An index on a collection's fields.
    Indexes are meant to de created by ensure functions of Collections. 
    Indexes have a .infos dictionary that stores all the infos about the index
    """

    def __init__(self, collection, infos = None, creation_data = None):

        self.collection = collection
        self.connection = self.collection.database.connection
        self.indexes_URL = "%s/index" % self.collection.database.URL
        self.infos = None
        if infos:
            self.infos = infos
        elif creation_data:
            self._create(creation_data)

        if self.infos:
            self.URL = "%s/%s" % (self.indexes_URL, self.infos["id"])

    def _create(self, post_data):
        """
        Creates an index of any type according to post_data
        """
        if self.infos is None:
            response = self.connection.session.post(
                    self.indexes_URL,
                    params = {
                        "collection": self.collection.name
                        },
                    data = json.dumps(post_data, default=str))
            data = response.json()
            if (response.status_code >= 400) or data['error']:
                raise CreationError(data['errorMessage'], data)
            self.infos = data

    def delete(self):
        """
        Delete the index
        """
        response = self.connection.session.delete(self.URL)
        data = response.json()
        if (response.status_code != 200 and response.status_code != 202) \
                or data['error']:
            raise DeletionError(data['errorMessage'], data)
