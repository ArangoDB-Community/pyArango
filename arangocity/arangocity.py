import urllib3
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

def urlRemoveLastSlash(url) :
	if url[-1] == "/" :
		return url[:-1]
	else :
		return url

def postJson(httpPool, URL, payload) :
	return httpPool.urlopen('POST', URL, headers={'Content-Type':'application/json'}, body=payload)

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
		self.arangoURL = urlRemoveLastSlash(arangoURL)
		self.URL = '%s/_api/database' % self.arangoURL
		
		self.httpPool = urllib3.PoolManager()
		r = self.httpPool.request('GET', self.URL)
		data = json.loads(r.data)
		if r.status == 200  and not data["error"] :
			for dbName in data["result"] :
				db = Database(self, dbName)
				self.databases[dbName] = db
		else :
			raise ConnectonError(data["errorMessage"], data)

	def __getitem__(self, dbName) :
		return self.databases[dbName]

class Database(object) :
	
	def __init__(self, connection, name) :
		self.name = name
		self.connection = connection
		self.collections = {}
		
		self.URL = '%s/_db/%s/_api/collection' % (self.connection.arangoURL, self.name)
		self.httpPool = self.connection.httpPool
		r = self.httpPool.request('GET', self.URL)
		data = json.loads(r.data)
		if r.status == 200 and not data["error"] :
			for colData in data["collections"] :
				self.collections[colData['name']] = Collection(self, colData)
		else :
			raise ConnectonError(data["errorMessage"], data)

	def _createCollection(self, **colArgs) :
		r = postJson(self.httpPool, self.URL, json.dumps(colArgs))
		data = json.loads(r.data)
		if r.status == 200 and not data["error"] :
			col = Collection(self, data)
			self.collections[col.name] = col
		else :
			raise CreationError(data["errorMessage"], data)

	def __repr__(self) :
		return "ArangoDB database: %s" % self.name

	def __getitem__(self, k) :
		return self.collections[k]

class Collection(object) :

	_fields = { 
		"_id" : Field(),
		"_key" : Field(),
		"_rev" : Field()
	}

	_criticalLevel = _COLLECTION_CRITICAL_LEVEL_LOW

	def __init__(self, database, jsonData) :
		self.database = database
		self.httpPool = self.database.httpPool

		for k in jsonData :
			setattr(self, k, jsonData[k])
		
		self.URL = "%s/%s" % (self.database.URL, self.id)

	def delete(self) :
		r = self.httpPool.request('DELETE', self.URL)
		if not r.status == 200 or data["error"] :
			raise DeletionError(data["errorMessage"], data)

	def createDocument(self) :
		return Document(self, self._fields)

	def findId(self) :
		pass

	def findExample(self) :
		pass

	def findAQL(self) :
		pass

	def action(self, method, action, **args) :
		"a generic fct for interacting everything that doesn't have an assigned fct"
		return json.loads(self.httpPool.request(method, self.URL + "/" + action, fields = args))

	def load(self) :
		"loads collection in memory"
		return json.loads(self.httpPool.request('PUT', self.URL + "/load").data)
	
	def unload(self) :
		"unloads collection from memory"
		return json.loads(self.httpPool.request('PUT', self.URL + "/unload").data)

	def revision(self) :
		return json.loads(self.httpPool.request('GET', self.URL + "/revision").data)["revision"]

	def properties(self) :
		return json.loads(self.httpPool.request('GET', self.URL + "/properties").data)

	def checksum(self) :
		return json.loads(self.httpPool.request('GET', self.URL + "/checksum").data)["checksum"]

	def count(self) :
		return json.loads(self.httpPool.request('GET', self.URL + "/count").data)["count"]

	def figures(self) :
		"a more elaborate version of count, see arangodb docs for more infos"
		return json.loads(self.httpPool.request('GET', self.URL + "/figures").data)

	def __repr__(self) :
		return "ArangoDB collection name: %s, id: %s, type: %s, status: %s" % (self.name, self.id, self.type, self.status)

class Document(object) :

	def __init__(self, collection, collectionFields) :
		self.collection = collection
		self.httpPool = self.collection.httpPool

		self.URL = "%s/document" % (self.collection.database.URL)
		
		self.store = {}
		for k in collectionFields.keys() :
			self.store[k] = ""

	def save(self) :
		if self.collection._criticalLevel > _COLLECTION_CRITICAL_LEVEL_LOW :
			for k, v in self.store.iteritems() :
				self.collection._fields[k].test(v)
		
		if not self["_id"] :
			print self.URL
			url = "http://localhost:8529/_api/document?collection=test_col"
			r = postJson(self.httpPool, url, json.dumps({3:2}))
			print r.data
			# print self.httpPool.request('POST', url, fields = {"a" : 1}).data
			
	def __getitem__(self, k) :
		return self.store[k]

	def __setitem__(self, k, v) :
		self.store[k] = v

if __name__ == "__main__" :
	conn = Connection()
	db = conn["bluwr_test"]
	col = db["test_col"]
	doc = col.createDocument()
	doc["name"] = 1
	doc.save()
	print col.count()