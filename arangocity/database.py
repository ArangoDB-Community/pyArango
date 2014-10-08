import requests
import json
import types

from collection import Collection, SystemCollection, GenericCollection, Collection_metaclass
from document import Document
from theExceptions import CreationError, UpdateError, AQLQueryError

class AQLQueryResult(object) :
	
	def __init__(self, database, queryPost, jsonData) :
		"queryPost contains a dictionnary representation of the initial POST payload sent to the database"

		self.database = database
		self.queryPost = queryPost

		if jsonData["hasMore"] :
			self.id = jsonData["id"]
			self.URL = "%s/%s" % (self.database.cursorURL, self.id)
		else :
			self.id = None
			self.URL = None
		
		self._resetBatch(jsonData)

	def _resetBatch(self, jsonData) :
		self.hasMore = jsonData["hasMore"]
		self.error = jsonData["error"]
		self.code = jsonData["code"]

		try :
			self.count = jsonData["count"]
		except KeyError :
			self.count = None
		
		try :
			self.extra = jsonData["extra"]
		except KeyError :
			self.extra = None

		self.result = jsonData["result"]

	def getDocument(self, i, raw = False) :
		"returns a document from the list. if raw = True, will return the json representation of the doc as a dict"
		if raw : return self.result[i]
		return self._developDoc(i)

	def _developDoc(self, i) :
		docJson = self.result[i]
		collection = self.database[docJson["_id"].split("/")[0]]
		return Document(collection, docJson)

	def nextBatch(self) :
		"become the next batch. raises a StopIteration if there is None"
		if not self.hasMore :
			raise StopIteration("That was the last batch")

		r = requests.put(self.URL)
		data = r.json()
		if r.status_code == 200 and not data['error'] :
			self._resetBatch(data)
		else :
			raise AQLQueryError(data["errorMessage"], self.queryPost["query"], data)

	def delete(self) :
		"kills the cursor"
		requests.delete(self.URL)

class Database(object) :
	
	def __init__(self, connection, name) :
		"meant to be called by the connection only"

		self.name = name
		self.connection = connection
		self.collections = {}
		
		self.URL = '%s/_db/%s/_api' % (self.connection.arangoURL, self.name)
		self.collectionsURL = '%s/collection' % (self.URL)
		self.cursorURL = '%s/cursor' % (self.URL)
		self.queryURL = '%s/query' % (self.URL)

		self.update()
	
	def update(self) :
		"updates the collection list"
		r = requests.get(self.collectionsURL)
		data = r.json()
		if r.status_code == 200 and not data["error"] :
			self.collections = {}
			for colData in data["collections"] :
				colName = colData['name']
				if colData['isSystem'] :
					colObj = SystemCollection(self, colData)
				else :
					try :
						colClass = Collection_metaclass.getCollectionClass(colName)
						colObj = colClass(self, colData)
					except KeyError :
						colObj = GenericCollection(self, colData)

				if colName not in self.collections :
					self.collections[colName] = colObj
		else :
			raise UpdateError(data["errorMessage"], data)

	def createCollection(self, className = 'GenericCollection', **colArgs) :
		""" Must be the name of a class inheriting from Collection. Use colArgs to put things such as 'isVolatile = True'.
		The 'name' parameter will be ignored if className != 'GenericCollection' since it is already specified by className"""
		
		if className != 'GenericCollection' :
			colArgs['name'] = className
		else :
			if 'name' not in colArgs :
				raise ValueError("a 'name' argument mush be supplied if you want to create a generic collection")
					
		colClass = Collection_metaclass.getCollectionClass(className)

		if colArgs['name'] in self.collections :
			raise CreationError("Database %s already has a collection named %s" % (self.name, colArgs['name']) )

		payload = json.dumps(colArgs)
		r = requests.post(self.collectionsURL, data = payload)
		data = r.json()
		if r.status_code == 200 and not data["error"] :
			col = colClass(self, data)
			self.collections[col.name] = col
			return self.collections[col.name]
		else :
			raise CreationError(data["errorMessage"], data)

	def hasCollection(self, name) :
		return name in self.collections

	def AQLQuery(self, query, batchSize, count = False) :
		payload = {'query' : query, 'count' : count, 'batchSize' : batchSize}
		r = requests.post(self.cursorURL, data = payload)
		data = r.json()
		if r.status_code == 201 and not data["error"] :
			return AQLQueryResult(self, payload, data)
		else :
			raise AQLQueryError(data["errorMessage"], query, data)

	def ValidateAQLQuery(self, query) :
		"returns the server answer is the query is valid. Raises an AQLQueryError if not"
		payload = {'query' : query}
		r = requests.post(self.cursorURL, data = payload)
		data = r.json()
		if r.status_code == 200 and not data["error"] :
			return data
		else :
			raise AQLQueryError(data["errorMessage"], query, data)

	def __repr__(self) :
		return "ArangoDB database: %s" % self.name

	def __getitem__(self, collectionName) :
		try :
			return self.collections[collectionName]
		except KeyError :
			self.update()
			try :
				return self.collections[collectionName]
			except KeyError :
				raise KeyError("Can't find any collection named : %s" % collectionName)