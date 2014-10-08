import requests
import json
import types

from collection import Collection, SystemCollection, GenericCollection, Collection_metaclass
from document import Document
from theExceptions import CreationError, UpdateError, AQLQueryError

class AQLQueryResult(object) :
	
	def __init__(self, database, rawResults, queryPost, jsonData) :
		"if rawResults = True, will always return the json representation of the results and not a Document object. queryPost contains a dictionnary representation of the initial POST payload sent to the database"

		self.database = database
		self.rawResults = rawResults
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

		if not self.rawResults :
			self._developed = range(len(self.result))
		else :
			self._developed = None

	def __getitem__(self, i) :
		"returns a ith result of the query."
		if not self.rawResults and self._developed[i] is not True : self._developDoc(i)
		return self.result[i]
		
	def _developDoc(self, i) :
		docJson = self.result[i]
		try :
			collection = self.database[docJson["_id"].split("/")[0]]
		except KeyError :
			raise CreationError("result %d is not a valid Document. Try setting rawResults to True" % i)

		self.result[i] = Document(collection, docJson)
		self._developed[i] = True

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

	def AQLQuery(self, query, rawResults, batchSize, bindVars = {}, options = {}, count = False, fullCount = False) :
		"Set rawResults = True if you want the query to returns dictionnaries instead of Document objects"
		
		payload = {'query' : query, 'batchSize' : batchSize, 'bindVars' : bindVars, 'options' : options, 'count' : count, 'fullCount' : fullCount}
		r = requests.post(self.cursorURL, data = json.dumps(payload))
		data = r.json()
		if r.status_code == 201 and not data["error"] :
			return AQLQueryResult(self, rawResults, payload, data)
		else :
			raise AQLQueryError(data["errorMessage"], query, data)

	def validateAQLQuery(self, query, bindVars = {}, options = {}) :
		"returns the server answer is the query is valid. Raises an AQLQueryError if not"
		payload = {'query' : query, 'bindVars' : bindVars, 'options' : options}
		r = requests.post(self.cursorURL, data = json.dumps(payload))
		data = r.json()
		if r.status_code == 201 and not data["error"] :
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