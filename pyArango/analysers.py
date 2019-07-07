from .theExceptions import CreationError, ArangoError

class Analysers(object):
    """Manages analyserss"""
    def __init__(self, database):
        super(Analysers, self).__init__()
        self.database = database
        self.URL = self.database.getAnalysersURL()
        self.analysers = {}
    
    def reload(self):
        response = self.database.connection.session.get(self.URL)
        data = response.json()
        if data["error"]:
            raise ArangoError(data)

        for ana in response["result"]:
            self.analysers[ana["name"]] = ana

    def create(self, name, ana_type, properties=None, features=None):
        """
        Begin a transaction on the server, return value contains the created transaction Id.

        Parameters
        ----------
        name: The analyzer name.

        type: The analyzer type.

        properties: The properties used to configure the specified type. Value may be a string, an object or null.
        The default value is null.

        features: The set of features to set on the analyzer generated fields. The default value is an empty array.

        """
        if features is None:
            features = []

        payload = {
            "name": name,
            "allowImplicit": allowImplicit,
            "type": ana_type,
            "properties": properties,
            "features": features,
        }

        response = self.database.connection.session.post(self.URL, data=payload) 
        
        data = response.json()
        if data["error"]:
            raise CreationError(data["errorMessage"], data)
        self.analysers[data["name"]] = data["result"]
        return data["result"]

    def get(self, name, force_db=False):
        """
        Return an anylser. If force_db, will bypass the cache and query the db 
        """
        
        if not force_db :
            try :
                return self.analysers[name]
            except :
                pass

        response = self.database.connection.session.get(self.URL + "/%s" % name)
        data = response.json()
        if data["error"]:
            raise ArangoError(data["errorMessage"], data)
        self.analysers[data["name"]] = data["result"]
        return data["result"]

    def delete(self, name):
        """
        Delete an existing analyser
        """

        response = self.database.connection.session.delete(self.URL + "/%s" % name)
        data = response.json()
        if data["error"]:
            raise ArangoError(data["errorMessage"], data)
        del self.analysers[data["name"]]
        return data["result"]
