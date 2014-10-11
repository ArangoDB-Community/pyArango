import requests
import json
import types

from document import Document
from theExceptions import ConstraintViolation, SchemaViolation, CreationError, UpdateError, DeletionError, SimpleQueryError
from query import SimpleQueryResult

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
	"Doument cache for collection with insert, deletes and updates in O(1)"

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
		try :
			doc = self.cacheStore[key]
			doc.prev.next = doc.next
			doc.next.prev = doc.prev
			del(self.cacheStore[key])
		except KeyError :
			raise KeyError("Document with key %s is not available in cache" % key)
	
	def getChain(self) :
		"returns a list ok keys representing the chain of documents"		
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

	def __init__(self, notNull = False, constraintFct = None) :
		self.notNull = notNull
		self.constraintFct = constraintFct

	def validate(self, v) :
		if v != None  and v != "" :
			if self.constraintFct and not self.constraintFct(v) :
				raise ConstraintViolation("Violation of constraint fct: %s" %(self.constraintFct.func_name))
		
		if self.notNull :
			raise ConstraintViolation("This fields can't have a NULL value (\"None\" or \"\")")
		
		return True

	def __repr__(self) :
		return "<Field, not null: %s, constraint fct: %s>" %(self.notNull, self.constraintFct.func_name)

class Collection_metaclass(type) :
	
	collectionClasses = {}
	
	validationDefault = {
			'on_save' : False,
			'on_set' : False,
			'allow_foreign_fields' : True
		}

	def __new__(cls, name, bases, attrs) :

		if '_validation' not in attrs :
			attrs['validation'] = cls.validationDefault
		else :
			for k, v in attrs['_validation'].iteritems() :
				if k not in cls.validationDefault :
					raise KeyError("Unknown validation parameter %s for class %s"  %(k, name))
				if type(v) is not types.BooleanType :
					raise ValueError("validation parameter %s for class %s has a non boolean value"  %(k, name))

			for k, v in cls.validationDefault.iteritems() :
				if k not in attrs['_validation']	:
					attrs['_validation'][k] = v
			
		clsObj = type.__new__(cls, name, bases, attrs)
		Collection_metaclass.collectionClasses[name] = clsObj
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
	
	_validation = {
		'on_save' : False,
		'on_set' : False,
		'allow_foreign_fields' : True
	}
	
	__metaclass__ = Collection_metaclass

	def __init__(self, database, jsonData) :
		"meant to be called by the database only"
		
		if self.__class__ is Collection :
			raise ValueError("Collection is abstract and is not supposed to be instanciated. Collections my inherit from it")

		self.database = database
		self.name = self.__class__.__name__
		for k in jsonData :
			setattr(self, k, jsonData[k])
		
		self.URL = "%s/collection/%s" % (self.database.URL, self.name)
		self.documentsURL = "%s/document" % (self.database.URL)
		self.documentCache = None
	
	def activateCache(self, cacheSize) :
		"activate the caching system. Cached documents are only available through the __getitem__ interface"
		self.documentCache = DocumentCache(cacheSize)
	
	def deactivateCache(self) :
		"deactivate the caching system"
		self.documentCache = None

	def delete(self) :
		r = requests.delete(self.URL)
		data = r.json()
		if not r.status_code == 200 or data["error"] :
			raise DeletionError(data["errorMessage"], data)

	def createDocument(self) :
		"returns an empty document"
		return Document(self)

	def validateField(self, fieldName, value) :
		if not self._validation["allow_foreign_fields"] and (fieldName not in self._fields) :
			raise SchemaViolation(self, fieldName)
		self.__class__._fields[fieldName].validate(value)

	def fetchDocument(self, key, rev = None) :
		"Fetches a document from the collection given it's key. This function always goes straight to the db and bypasses the cache"
		url = "%s/%s/%s" % (self.documentsURL, self.name, key)
		if rev is not None :
			r = requests.get(url, params = {'rev' : rev})
		else :
			r = requests.get(url)
		if r.status_code != 404 :
			return Document(self, r.json())
	
	def fetchByExample(self, exampleDict, batchSize, rawResults = False, **queryArgs) :
		"exampleDict should be something like {'age' : 28}"
		return self.simpleQuery('by-example', batchSize, rawResults, example = exampleDict, **queryArgs)

	def fetchFirstExample(self, exampleDict, batchSize, rawResults = False, **queryArgs) :
		"exampleDict should be something like {'age' : 28}. returns only a single element but still in a SimpleQueryResult object"
		return self.simpleQuery('first-example', batchSize, rawResults, example = exampleDict, **queryArgs)

	def fetchAll(self, batchSize, rawResults = False, **queryArgs) :
		return self.simpleQuery('all', batchSize, rawResults, **queryArgs)

	def simpleQuery(self, queryType, batchSize, rawResults = False, **queryArgs) :
		"""General interface for simple queries. queryType can be something like 'all', 'by-example' etc... everything is in the arango doc.
		If rawResults, the query will return dictionaries instead of Document objetcs.
		"""
		payload = {'collection' : self.name, 'batchSize' : batchSize}
		payload.update(queryArgs)
		payload = json.dumps(payload)
		url = "%s/simple/%s" % (self.database.URL, queryType)

		r = requests.put(url, data = payload)
		data = r.json()
		if r.status_code == 201 and not data['error'] :
			return SimpleQueryResult(url, self, rawResults, payload, data)
		else :
			raise SimpleQueryError(data["errorMessage"], data)

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

	def __getitem__(self, key) :
		"returns a document from the cache. If it's not there, fetches it from the db and caches it first. If the cache is not activated this is equivalent to fetchDocument()"
		if self.documentCache is None :
			return self.fetchDocument(key) 
		try :
			return self.documentCache[key] 
		except KeyError :
			doc = self.fetchDocument(key)
			self.documentCache.cache(doc)
		return doc

class SystemCollection(Collection) :
	"for all collections with isSystem = True"
	
	def __init__(self, database, jsonData) :
		Collection.__init__(self, database, jsonData)

class GenericCollection(Collection) :
	"The default collection. Can store anything"
	
	def __init__(self, database, jsonData) :
		Collection.__init__(self, database, jsonData)
		