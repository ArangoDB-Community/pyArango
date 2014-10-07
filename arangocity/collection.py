import requests
import json

from document import Document
from theExceptions import ConstraintViolation, CreationError, UpdateError, DeletionError, SchemaViolation

COLLECTION_DOCUMENT_TYPE = 2
COLLECTION_EDGE_TYPE = 3

COLLECTION_NEWBORN_STATUS = 1
COLLECTION_UNLOADED_STATUS = 2
COLLECTION_LOADED_STATUS = 3
COLLECTION_LOADING_STATUS = 4
COLLECTION_DELETED_STATUS = 5

class Field(object) :

	def __init__(self, notNull = False, constraintFct = None) :
		self.notNull = notNull
		self.constraintFct = constraintFct

	def test(self, v) :
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

	def __new__(cls, name, bases, attrs) :
		
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
	
	_test_fields_on_save = False
	_test_fields_on_set = False
	_allow_foreign_fields = True

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

	def delete(self) :
		r = requests.delete(self.URL)
		data = r.json()
		if not r.status_code == 200 or data["error"] :
			raise DeletionError(data["errorMessage"], data)

	def createDocument(self) :
		"returns an empty document"
		return Document(self)

	def testFieldValue(self, fieldName, value) :
		if not self._allow_foreign_fields and (fieldName not in self._fields) :
			raise SchemaViolation(self, fieldName)
		self.__class__._fields[fieldName].test(value)

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

class SystemCollection(Collection) :
	"for all collections with isSystem = True"
	
	def __init__(self, database, jsonData) :
		Collection.__init__(self, database, jsonData)

class GenericCollection(Collection) :
	"The default collection. Can store anything"
	
	_test_fields_on_save = False
	_test_fields_on_set = False
	_allow_foreign_fields = True

	def __init__(self, database, jsonData) :
		Collection.__init__(self, database, jsonData)
		