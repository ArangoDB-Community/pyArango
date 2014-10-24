import requests
import json

from theExceptions import (CreationError, DeletionError, UpdateError)

class Graph(object) :

	def __init__(self, database, jsonInit) :
		self.database = database

		self._key = jsonInit["_key"]
		self._rev = jsonInit["_rev"]
		self._id = jsonInit["_id"]
		self.edgeDefinitions = jsonInit["edgeDefinitions"]
		try :
			self.orphanCollections = jsonInit["orphanCollections"]
		except KeyError :
			self.orphanCollections = None

		self.URL = "%s/%s" % (self.database.graphsURL, self._key)

	def createVertex(self, docAttributes, docName = None) :
		url = "%s/vertex" % (self.URL)
		
		if docName is not None :
			payload = {"_key" : docName}
			payload.update
		r = requests.post()

	def delete(self) :
		r = requests.delete(self.URL)
		data = r.json()
		if not r.status_code == 200 or data["error"] :
			raise DeletionError(data["errorMessage"], data)

	def __str__(self) :
		return "ArangoGraph; %s" % self._key
