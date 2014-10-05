import requests
import json

#schemaless
_COLLECTION_CRITICAL_LEVEL_LOW = 0
#enforces schema
_COLLECTION_CRITICAL_LEVEL_NORNAL = 1

_COLLECTION_DOCUMENT_TYPE = 2
_COLLECTION_EDGE_TYPE = 3

_COLLECTION_NEWBORN_STATUS = 1
_COLLECTION_UNLOADED_STATUS = 2
_COLLECTION_LOADED_STATUS = 3
_COLLECTION_LOADING_STATUS = 4
_COLLECTION_DELETED_STATUS = 5


class ArrangoException(Exception) :
	def __init__(self, message, errors = {}) :
		Exception.__init__(self, message)
		self.errors = errors

	def __str__(self) :
		return self.message + ". Errors: " + str(self.errors)

class ConnectonError(ArrangoException) :
	def __init__(self, message, errors = {}) :
		ArrangoException.__init__(self, message, errors)

class CreationError(ArrangoException) :
	def __init__(self, message, errors = {}) :
		ArrangoException.__init__(self, message, errors)

class DeletionError(ArrangoException) :
	def __init__(self, message, errors = {}) :
		ArrangoException.__init__(self, message, errors)

class ConstraintViolation(ArrangoException) :
	def __init__(self, message, errors = {}) :
		ArrangoException.__init__(self, message, errors)

class Field(object) :

	def __init__(self, notNull = False, constrainFct = None) :
		self.notNull = notNull
		self.constrainFct = constrainFct

	def test(self, v) :
		if v != None  and v != "" :
			if not self.constraintFct(v) :
				raise ConstraintViolation("Violation of constraint fct: %s" %(self.constraintFct.func_name))
		
		if self.notNull :
			raise ConstraintViolation("This fields can't have a NULL value (\"None\" or \"\")")
		
		return True

	def __repr__(self) :
		return "<Field, not null: %s, constraint fct: %s>" %(self.notNull, self.constraintFct.func_name)

class Connection(object) :
	"""Handles databases. Can't create db's and has no conception of users for now"""
	def __init__(self, arangoURL = 'http://localhost:8529') :
		self.databases = {}
		if arangoURL[-1] == "/" :
			self.arangoURL = url[:-1]
		else :
			self.arangoURL = arangoURL
		
		self.URL = '%s/_api' % self.arangoURL
			
		self.update()
	
	def update(self) :
		dbURL = '%s/database' % self.URL
		r = requests.get(dbURL)
		data = r.json()
		if r.status_code == 200  and not data["error"] :
			for dbName in data["result"] :
				db = Database(self, dbName)
				self.databases[dbName] = db
		else :
			raise ConnectonError(data["errorMessage"], data)

	def __getitem__(self, dbName) :
		try :
			return self.databases[dbName]
		except KeyError :
			self.update()
			try :
				return self.databases[dbName]
			except KeyError :
				raise KeyError("Can't find any database named : %s" % dbName)

class Database(object) :
	
	def __init__(self, connection, name) :
		self.name = name
		self.connection = connection
		self.collections = {}
		
		self.URL = '%s/_db/%s/_api' % (self.connection.arangoURL, self.name)

		self.update()
	
	def update(self) :
		colURL = '%s/collection' % (self.URL)
		r = requests.get(colURL)
		data = r.json()
		if r.status_code == 200 and not data["error"] :
			for colData in data["collections"] :
				self.collections[colData['name']] = Collection(self, colData)
		else :
			raise ConnectonError(data["errorMessage"], data)

	def _createCollection(self, **colArgs) :
		r = postJson(self.httpPool, self.URL, json.dumps(colArgs))
		data = json.loads(r.data)
		if r.status_code == 200 and not data["error"] :
			col = Collection(self, data)
			self.collections[col.name] = col
		else :
			raise CreationError(data["errorMessage"], data)

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

class Collection(object) :

	_fields = { 
		"_id" : Field(),
		"_key" : Field(),
		"_rev" : Field()
	}

	_criticalLevel = _COLLECTION_CRITICAL_LEVEL_LOW

	def __init__(self, database, jsonData) :
		self.database = database
		
		for k in jsonData :
			setattr(self, k, jsonData[k])
		
		self.URL = "%s/collection/%s" % (self.database.URL, self.id)

	def delete(self) :
		r = self.httpPool.request('DELETE', self.URL)
		if not r.status_code == 200 or data["error"] :
			raise DeletionError(data["errorMessage"], data)

	def createDocument(self) :
		return Document(self)

	def findId(self) :
		pass

	def findExample(self) :
		pass

	def findAQL(self) :
		pass

	def action(self, method, action, **params) :
		"a generic fct for interacting everything that doesn't have an assigned fct"
		return json.loads(self.httpPool.request(method, self.URL + "/" + action, fields = params).data)

	def load(self) :
		"loads collection in memory"
		return self.action('PUT', 'load')
	
	def unload(self) :
		"unloads collection from memory"
		return self.action('PUT', 'unload')

	def revision(self) :
		return self.action('GET', 'revision')["revision"]

	def properties(self) :
		return self.action('GET', 'properties')

	def checksum(self) :
		return self.action('GET', 'checksum')["checksum"]

	def count(self) :
		return self.action('GET', 'count')["count"]

	def figures(self) :
		"a more elaborate version of count, see arangodb docs for more infos"
		return self.action('GET', 'figures')

	def __repr__(self) :
		return "ArangoDB collection name: %s, id: %s, type: %s, status: %s" % (self.name, self.id, self.type, self.status)

class Document(object) :

	def __init__(self, collection) :
		self.collection = collection

		self.URL = "%s/document" % (self.collection.database.URL)

		self._store = {}
		for k in collection.__class__._fields.keys() :
			self._store[k] = ""

	def save(self, waitForSync = True) :
		if self.collection._criticalLevel > _COLLECTION_CRITICAL_LEVEL_LOW :
			for k, v in self._store.iteritems() :
				self.collection._fields[k].test(v)
		
		if not self["_id"] :
			headers = {'content-type': 'application/json'}
			params = {'collection': self.collection.name, "waitForSync" : waitForSync}
			payload = json.dumps(self._store)
			r = requests.post(self.URL, headers = headers, params = params, data = payload)
			data = r.json()
			print data
			for k, v in data.iteritems() :
				self[k] = v
	
	def __getattribute__(self, k) :
		if k == "store" :
			raise AttributeError("_store can be accessed directly, use self[key] instead")
		
		return object.__getattribute__(self, k)

	def __getitem__(self, k) :
		return self._store[k]

	def __setitem__(self, k, v) :
		self._store[k] = v

if __name__ == "__main__" :
	conn = Connection()
	db = conn["bluwr_test"]
	col = db["lala"]
	doc = col.createDocument()
	doc["name"] = 1
	doc.save()
	