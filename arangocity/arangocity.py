import requests
import json

#schemaless, no tests performed
_COLLECTION_CRITICAL_LEVEL_BLIND = 0
#tests performed on canonical fields before saving, non canonical fields will be saved without testing
_COLLECTION_CRITICAL_LEVEL_TEST = 1
#test and enforces schemas
_COLLECTION_CRITICAL_LEVEL_SCHEMA_ENFORCE = 2

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

class ConnectionError(ArrangoException) :
	def __init__(self, message, errors = {}) :
		ArrangoException.__init__(self, message, errors)

class CreationError(ArrangoException) :
	def __init__(self, message, errors = {}) :
		ArrangoException.__init__(self, message, errors)

class UpdateError(ArrangoException) :
	def __init__(self, message, errors = {}) :
		ArrangoException.__init__(self, message, errors)

class DeletionError(ArrangoException) :
	def __init__(self, message, errors = {}) :
		ArrangoException.__init__(self, message, errors)

class ConstraintViolation(ArrangoException) :
	def __init__(self, message, errors = {}) :
		ArrangoException.__init__(self, message, errors)

class SchemaViolation(ArrangoException) :
	def __init__(self, collection, field, errors = {}) :
		message = "Collection %s does not a field %s in it's schema" % (collection.__class__.__name__, field)
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
		self.databasesURL = '%s/database' % self.URL

		self.update()
	
	def update(self) :
		r = requests.get(self.databasesURL)
		data = r.json()
		if r.status_code == 200  and not data["error"] :
			for dbName in data["result"] :
				if dbName not in self.databases :
					db = Database(self, dbName)
					self.databases[dbName] = db
		else :
			raise ConnectionError(data["errorMessage"], data)

	def createDatabase(self, **dbArgs) :
		"use dbArgs to put things such as 'name = products'"

		payload = json.dumps(dbArgs)
		r = requests.post(self.databasesURL, data = payload)
		data = r.json()
		if r.status_code == 201 and not data["error"] :
			db = Database(self, name)
			self.databases[name] = db
			return self.databases[name]
		else :
			raise CreationError(data["errorMessage"], data)

	def hasDatabase(self, name) :
		return name in self.databases

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
		self.collectionsURL = '%s/collection' % (self.URL)

		self.update()
	
	def update(self) :
		r = requests.get(self.collectionsURL)
		data = r.json()
		if r.status_code == 200 and not data["error"] :
			for colData in data["collections"] :
				colName = colData['name']
				colClass = Mongocity_metaclass.getCollectionClass(colName)
				if colName not in self.collections :
					self.collections[colName] = colClass(self, colData)
		else :
			raise ConnectionError(data["errorMessage"], data)

	def createCollection(self, classOrClassName, **colArgs) :
		"""classOrClassName must be either a class inheriting from Collection or the name 
		of a class inheriting from Collection. Use colArgs to put things such as 'isVolatile = True'.
		The 'name' parameter will be ignored since it is already specified by classOrClassName"""
		
		if type(classOrClassName) is types.StringType
			colClass = Mongocity_metaclass.getCollectionClass(classOrClassName)
			colArgs['name'] = classOrClassName
		elif isinstance(classOrClassName, Collection) :
			colClass = classOrClassName
			colArgs['name'] = classOrClassName.__name__ 
		else :
			raise ValueError("classOrClassName must be either a class inheriting from Collection or the name of a class inheriting from Collection")

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

class Mongocity_metaclass(type) :
	
	collectionClasses = {}
	
	def __new__(cls, name, bases, attrs) :
		
		privateFields = { 
			"_id" : Field(),
			"_key" : Field(),
			"_rev" : Field()
		}
		
		if '_fields' not in attrs :
			attrs['_fields'] = privateFields
		else :
			attrs['_fields'].update(privateFields)

		attrs['_privates'] = ["_id", "_rev", "_key"]
		
		clsObj = type.__new__(cls, name, bases, attrs)
		Mongocity_metaclass.collectionClasses['name'] = clsObj
		return clsObj

	@classmethod
	def getCollectionClass(cls, name) :
		try :
			return cls.collectionClasses[name]
		except KeyError :
			raise KeyError("There's no child of Collection by the name of: %s" % name)

class Collection(object) :

	#here you specify the fields that you want for the documents in your collection
	_fields = {}
	
	__metaclass__ = Collection_metaclass

	#defines the level of schema enforcement
	_criticalLevel = _COLLECTION_CRITICAL_LEVEL_BLIND

	def __init__(self, database, jsonData) :
		self.database = database
		self.name = self.__class__.__name__
		for k in jsonData :
			setattr(self, k, jsonData[k])
		
		self.URL = "%s/collection/%s" % (self.database.URL, self.name)

	def delete(self) :
		r = requests.delete(self.URL)
		data = r.json()
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
		fct = getattr(requests, method.lower())
		r = fct(self.URL + "/" + action, params = params)
		return r.json()

	def truncate(self) :
		return self.action('PUT', 'truncate')

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
		self.privates = self.collection.__class__._privates
		self.documentsURL = "%s/document" % (self.collection.database.URL)
		self._reset()

	def _reset(self) :
		self.URL = None
		self._store = {}
		for k in self.collection.__class__._fields.keys() :
			self._store[k] = None

	def save(self, **docArgs) :
		"use docArgs to put things such as 'waitForSync = True'"

		if self.collection._criticalLevel > _COLLECTION_CRITICAL_LEVEL_BLIND :
			for k, v in self._store.iteritems() :
				if (k not in self.collection.__class__._fields) and self.collection._criticalLevel == _COLLECTION_CRITICAL_LEVEL_SCHEMA_ENFORCE :
					raise SchemaViolation(self.collection, k)
				self.collection.__class__.fields[k].test(v)
		
		params = dict(docArgs)
		params.update({'collection': self.collection.name })
		payload = {}
		for k in self._store.iterkeys() :
			if k not in self.privates :
				payload[k] = self._store[k]
		payload = json.dumps(payload)

		if self.URL is None :
			r = requests.post(self.documentsURL, params = params, data = payload)
			update = False
		else :
			r = requests.put(self.URL, params = params, data = payload)
			update = True

		data = r.json()
		if (r.status_code == 201 or r.status_code == 202) and not data['error'] :
			if update :
				self['_rev'] = data['_rev']
			else :
				for k in self.privates :
					self[k] = data[k]
				self.URL = "%s/%s" % (self.documentsURL, self["_id"])
		else :
			if update :
				raise UpdateError(data['errorMessage'], data)		
			else :
				raise CreationError(data['errorMessage'], data)

	def saveCopy(self) :
		"saves a copy of the object and become that copy"
		self._reset()
		self.save()

	def delete(self) :
		if self.URL is None :
			raise DeletionError("Can't delete a document that was not saved") 
		r = requests.delete(self.URL)
		data = r.json()
		if (r.status_code != 200 and r.status_code != 202) or data['error'] :
			raise DeletionError(data['errorMessage'], data)
		self._reset()

	def __getattribute__(self, k) :
		if k == "store" :
			raise AttributeError("_store can be accessed directly, use self[key] instead")
		
		return object.__getattribute__(self, k)

	def __getitem__(self, k) :
		return self._store[k]

	def __setitem__(self, k, v) :
		self._store[k] = v
	