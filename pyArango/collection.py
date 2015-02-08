import requests
import json
import types

from document import Document, Edge
from theExceptions import ValidationError, SchemaViolation, CreationError, UpdateError, DeletionError, InvalidDocument, AbstractInstanciationError
from query import SimpleQuery

COLLECTION_DOCUMENT_TYPE = 2
COLLECTION_EDGE_TYPE = 3

COLLECTION_NEWBORN_STATUS = 1
COLLECTION_UNLOADED_STATUS = 2
COLLECTION_LOADED_STATUS = 3
COLLECTION_LOADING_STATUS = 4
COLLECTION_DELETED_STATUS = 5

class CachedDoc(object) :
	def __init__(self, document, prev, next) :
		self.prev = prev
		self.document = document
		self.next = next
		self.key = document.key

class DocumentCache(object) :
	"Document cache for collection, with insert, deletes and updates in O(1)"

	def __init__(self, cacheSize) :
		self.cacheSize = cacheSize
		self.cacheStore = {}
		self.head = None
		self.tail = None

	def cache(self, doc) :
		if doc.key in self.cacheStore :
			ret = self.cacheStore[doc.key]
			if ret.prev is not None :
				ret.prev.next = ret.next
				self.head.prev = ret
				ret.next = self.head
				self.head = ret
			return self.head
		else :
			if len(self.cacheStore) == 0 :
				ret = CachedDoc(doc, prev = None, next = None)
				self.head = ret
				self.tail = self.head
				self.cacheStore[doc.key] = ret
			else :
				if len(self.cacheStore) >= self.cacheSize :
					del(self.cacheStore[self.tail.key])
					self.tail = self.tail.prev
					self.tail.next = None

				ret = CachedDoc(doc, prev = None, next = self.head)
				self.head.prev = ret
				self.head = self.head.prev
				self.cacheStore[doc.key] = ret

	def delete(self, key) :
		"removes a document from the cache"
		try :
			doc = self.cacheStore[key]
			doc.prev.next = doc.next
			doc.next.prev = doc.prev
			del(self.cacheStore[key])
		except KeyError :
			raise KeyError("Document with key %s is not available in cache" % key)
	
	def getChain(self) :
		"returns a list of keys representing the chain of documents"
		l = []
		h = self.head
		while h :
			l.append(h.key)
			h = h.next
		return l

	def stringify(self) :
		"a pretty str version of getChain()"
		l = []
		h = self.head
		while h :
			l.append(str(h.key))
			h = h.next
		return "<->".join(l)

	def __getitem__(self, key) :
		if key in self.cacheStore :
			try :
				ret = self.cacheStore[key]
				self.cache(ret)
				return ret
			except KeyError :
				raise KeyError("Document with key %s is not available in cache" % key)

	def __repr__(self) :
		return "[DocumentCache, size: %d, full: %d]" %(self.cacheSize, len(self.cacheStore))

class Field(object) :
	"""The class for defining pyArango fields."""
	def __init__(self, validators = []) :
		"validators must be a list of validators"
		self.validators = validators

	def validate(self, value) :
		"checks the validity of 'value' given the lits of validators"
		for v in self.validators :
			v.validate(value)
		return True

	def __str__(self) :
		strv = []
		for v in self.validators :
			strv.append(str(v))
		return "<Field, validators: '%s'>" % ', '.join(strv) 

class Collection_metaclass(type) :
	"""The metaclass that takes care of keeping a register of all collection types"""
	collectionClasses = {}
	
	_validationDefault = {
			'on_save' : False,
			'on_set' : False,
			'allow_foreign_fields' : True
		}

	def __new__(cls, name, bases, attrs) :
		def check_set_ConfigDict(dictName) :
			defaultDict = getattr(cls, "%sDefault" % dictName)

			if dictName not in attrs :
				attrs[dictName] = defaultDict
			else :
				for k, v in attrs[dictName].iteritems() :
					if k not in defaultDict :
						raise KeyError("Unknown validation parameter '%s' for class '%s'"  %(k, name))
					if type(v) is not type(defaultDict[k]) :
						raise ValueError("'%s' parameter '%s' for class '%s' is of type '%s' instead of '%s'"  %(dictName, k, name, type(v), type(defaultDict[k])))

				for k, v in defaultDict.iteritems() :
					if k not in attrs[dictName] :
						attrs[dictName][k] = v

		check_set_ConfigDict('_validation')
		clsObj = type.__new__(cls, name, bases, attrs)
		if name != "Collection" and name != "Edges" :
			Collection_metaclass.collectionClasses[name] = clsObj
	
		return clsObj

	@classmethod
	def getCollectionClass(cls, name) :
		"""Return the class object of a collection given its 'name'"""
		try :
			return cls.collectionClasses[name]
		except KeyError :
			raise KeyError("There's no child of Collection by the name of: %s" % name)

	@classmethod
	def isCollection(cls, name) :
		"""return true or false wether 'name' is the name of collection."""
		return name in cls.collectionClasses

	@classmethod
	def isDocumentCollection(cls, name) :
		"""return true or false wether 'name' is the name of a document collection."""
		try :
			col = cls.getCollectionClass(name)
			return issubclass(col, Collection)
		except KeyError :
			return False

	@classmethod
	def isEdgeCollection(cls, name) :
		"""return true or false wether 'name' is the name of an edge collection."""
		try :
			col = cls.getCollectionClass(name)
			return issubclass(col, Edges)
		except KeyError :
			return False

def getCollectionClass(name) :
	"""return true or false wether 'name' is the name of collection."""
	return Collection_metaclass.getCollectionClass(name)

def isCollection(name) :
	"""return true or false wether 'name' is the name of a document collection."""
	return Collection_metaclass.isCollection(name)

def isDocumentCollection(name) :
	"""return true or false wether 'name' is the name of a document collection."""
	return Collection_metaclass.isDocumentCollection(name)

def isEdgeCollection(name) :
	"""return true or false wether 'name' is the name of an edge collection."""
	return Collection_metaclass.isEdgeCollection(name)

def getCollectionClasses() :
	"returns a dictionary of all defined collection classes"
	return Collection_metaclass.collectionClasses

class Collection(object) :

	#here you specify the fields that you want for the documents in your collection
	_fields = {}
	
	_validation = {
		'on_save' : False,
		'on_set' : False,
		'allow_foreign_fields' : True
	}

	__metaclass__ = Collection_metaclass

	def __init__(self, database, jsonData) :
		"meant to be called by the database only"
		
		if self.__class__ is Collection :
			raise AbstractInstanciationError(self.__class__)

		self.database = database
		self.name = self.__class__.__name__
		for k in jsonData :
			setattr(self, k, jsonData[k])
		
		self.URL = "%s/collection/%s" % (self.database.URL, self.name)
		self.documentsURL = "%s/document" % (self.database.URL)
		self.documentCache = None
		
		self.documentClass = Document

	def activateCache(self, cacheSize) :
		"Activate the caching system. Cached documents are only available through the __getitem__ interface"
		self.documentCache = DocumentCache(cacheSize)
	
	def deactivateCache(self) :
		"deactivate the caching system"
		self.documentCache = None

	def delete(self) :
		"deletes the collection from the database"
		r = requests.delete(self.URL)
		data = r.json()
		if not r.status_code == 200 or data["error"] :
			raise DeletionError(data["errorMessage"], data)

	def createDocument(self, initValues = {}) :
		"create and returns a document"
		return self.documentClass(self, initValues)

	@classmethod
	def validateField(cls, fieldName, value) :
		"checks if 'value' is a valid for field 'fieldName'. If the validation is unsuccefull, raises a SchemaViolation or a ValidationError"
		if not cls._validation["allow_foreign_fields"] and (fieldName not in cls._fields) :
			raise SchemaViolation(cls, fieldName)
		
		try : #if foreign field
			return cls._fields[fieldName].validate(value)
		except KeyError :
			pass

	@classmethod
	def validateDct(cls, dct) :
		"validates a dictionary. The dictionary must be defined such as {field: value}. If the validation is unsuccefull, raises an InvalidDocument"
		res = {}
		for k, v in dct.iteritems() :
			try :
				cls.validateField(k, v)
			except (ValidationError, SchemaViolation) as e:
				res[k] = str(e)

		if len(res) > 0 :
			raise InvalidDocument(res)

		return True

	@classmethod
	def hasField(cls, k) :
		"returns True/False wether the collection has field K in it's schema"
		return k in cls._fields

	def fetchDocument(self, key, rawResults = False, rev = None) :
		"""Fetches a document from the collection given it's key. This function always goes straight to the db and bypasses the cache. If you
		want to take advantage of the cache use the __getitem__ interface: collection[key]"""
		url = "%s/%s/%s" % (self.documentsURL, self.name, key)
		if rev is not None :
			r = requests.get(url, params = {'rev' : rev})
		else :
			r = requests.get(url)
		if (r.status_code - 400) < 0 :
			if rawResults :
				return r.json()
			return self.documentClass(self, r.json())
		else :
			raise KeyError("Unable to find document with _key: %s" % key, r.json())

	def fetchByExample(self, exampleDict, batchSize, rawResults = False, **queryArgs) :
		"exampleDict should be something like {'age' : 28}"
		return self.simpleQuery('by-example', rawResults, example = exampleDict, batchSize = batchSize, **queryArgs)

	def fetchFirst(self, count, rawResults = False) :
		"""exampleDict should be something like {'age' : 28}. returns only a single element but still in a SimpleQuery object.
		returns count examples ordered by insert/update date, the last one being the first on the list"""
		return self.simpleQuery('first', rawResults = rawResults, count = count)

	def fetchLast(self, count, rawResults = False) :
		"""exampleDict should be something like {'age' : 28}. returns only a single element but still in a SimpleQuery object.
		returns count examples ordered by insert/update date, the last one being the first on the list"""
		return self.simpleQuery('last', rawResults = rawResults, count = count)

	def fetchFirstExample(self, exampleDict, rawResults = False) :
		"""exampleDict should be something like {'age' : 28}. returns only a single element but still in a SimpleQuery object.
		returns the first example found that matches the example"""
		return self.simpleQuery('first-example', rawResults = rawResults, example = exampleDict)

	def fetchAll(self, rawResults = False, **queryArgs) :
		"""Returns all the documents in the collection. You can use the optinal arguments 'skip' and 'limit'::
	
			fetchAlll(limit = 3, shik = 10)"""
		return self.simpleQuery('all', rawResults = rawResults, **queryArgs)

	def simpleQuery(self, queryType, rawResults = False, **queryArgs) :
		"""General interface for simple queries. queryType can be something like 'all', 'by-example' etc... everything is in the arango doc.
		If rawResults, the query will return dictionaries instead of Document objetcs.
		"""
		return SimpleQuery(self, queryType, rawResults, **queryArgs)

	def action(self, method, action, **params) :
		"a generic fct for interacting everything that doesn't have an assigned fct"
		fct = getattr(requests, method.lower())
		r = fct(self.URL + "/" + action, params = params)
		return r.json()

	def truncate(self) :
		"deletes every document in the collection"
		return self.action('PUT', 'truncate')

	def empty(self) :
		"alias for truncate"
		return self.truncate()

	def load(self) :
		"loads collection in memory"
		return self.action('PUT', 'load')
	
	def unload(self) :
		"unloads collection from memory"
		return self.action('PUT', 'unload')

	def revision(self) :
		"""returns the current revision"""
		return self.action('GET', 'revision')["revision"]

	def properties(self) :
		"""returns the current properties"""
		return self.action('GET', 'properties')

	def checksum(self) :
		"""returns the current checksum"""
		return self.action('GET', 'checksum')["checksum"]

	def count(self) :
		"""returns the number of documents in the collection"""
		return self.action('GET', 'count')["count"]

	def figures(self) :
		"a more elaborate version of count, see arangodb docs for more infos"
		return self.action('GET', 'figures')
	
	# def createEdges(self, className, **colArgs) :
	# 	"an alias of createCollection"
	# 	self.createCollection(className, **colArgs)

	def getType(self) :
		"returns a word describing the type of the collection (edges or ducments) instead of a number, if you prefer the number it's in self.type"
		if self.type == COLLECTION_DOCUMENT_TYPE :
			return "document"
		elif self.type == COLLECTION_EDGE_TYPE :
			return "egde"
		else :
			raise ValueError("The collection is of Unknown type %s" % self.type)

	def getStatus(self) :
		"returns a word describing the status of the collection (loaded, loading, deleted, unloaded, newborn) instead of a number, if you prefer the number it's in self.status"
		if self.status == COLLECTION_LOADING_STATUS :
			return "loading"
		elif self.status == COLLECTION_LOADED_STATUS :
			return "loaded"
		elif self.status == COLLECTION_DELETED_STATUS :
			return "deleted"
		elif self.status == COLLECTION_UNLOADED_STATUS :
			return "unloaded"
		elif self.status == COLLECTION_NEWBORN_STATUS :
			return "newborn"
		else :
			raise ValueError("The collection has an Unknown status %s" % self.status)

	def __repr__(self) :
		return "ArangoDB collection name: %s, id: %s, type: %s, status: %s" % (self.name, self.id, self.getType(), self.getStatus())

	def __getitem__(self, key) :
		"returns a document from the cache. If it's not there, fetches it from the db and caches it first. If the cache is not activated this is equivalent to fetchDocument( rawResults = False)"
		if self.documentCache is None :
			return self.fetchDocument(key, rawResults = False) 
		try :
			return self.documentCache[key] 
		except KeyError :
			doc = self.fetchDocument(key, rawResults = False)
			self.documentCache.cache(doc)
		return doc

class SystemCollection(Collection) :
	"for all collections with isSystem = True"
	def __init__(self, database, jsonData) :
		Collection.__init__(self, database, jsonData)

class GenericCollection(Collection) :
	"The default collection. It does not do any validation and can store anything"
	def __init__(self, database, jsonData) :
		Collection.__init__(self, database, jsonData)

class Edges(Collection) :
	"The default edge collection. All edge Collections must inherit from it"

	def __init__(self, database, jsonData) :
		"This one is meant to be called by the database"
		Collection.__init__(self, database, jsonData)
		self.documentClass = Edge
		self.documentsURL = "%s/edge" % (self.database.URL)
		self.edgesURL = "%ss/%s" % (self.documentsURL, self.name)

	def createEdge(self, initValues = {}) :
		"alias for createDocument, both functions create an edge"
		return self.createDocument(initValues)

	def getInEdges(self, vertex, rawResults = False) :
		"An alias for getEdges() that returns only the in Edges"
		return self.getEdges(vertex, inEdges = True, outEdges = False, rawResults = rawResults)
		
	def getOutEdges(self, vertex, rawResults = False) :
		"An alias for getEdges() that returns only the out Edges"
		return self.getEdges(vertex, inEdges = False, outEdges = True, rawResults = rawResults)

	def getEdges(self, vertex, inEdges = True, outEdges = True, rawResults = False) :
		"""returns in, out, or both edges liked to a given document. vertex can be either a Document object or a string for an _id.
		If rawResults a arango results will be return as fetched, if false, will return a liste of Edge objects"""
		if vertex.__class__ is Document :
			vId = vertex._id
		elif (type(vertex) is types.UnicodeType) or (type(vertex) is types.StringType) :
			vId = vertex
		else :
			raise ValueError("Vertex is neither a Document nor a String")

		params = {"vertex" : vId}
		if inEdges and outEdges :
			pass
		elif inEdges :
			params["direction"] = "in"
		elif outEdges :
			params["direction"] = "out"
		else :
			raise ValueError("inEdges, outEdges or both must have a boolean value")
		
		r = requests.get(self.edgesURL, params = params)
		data = r.json()
		if r.status_code == 200 :
			if not rawResults :
				ret = []
				for e in data["edges"] :
					ret.append(Edge(self, e))
				return ret
			else :
				return data["edges"]
		else :
			raise CreationError("Unable to return edges for vertex: %s" % vId, data)
