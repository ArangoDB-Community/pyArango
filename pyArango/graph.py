import requests
import json

from theExceptions import (CreationError, DeletionError, UpdateError)
import collection as COL

class Graph_metaclass(type) :
	
	graphClasses = {}
	
	def __new__(cls, name, bases, attrs) :
		clsObj = type.__new__(cls, name, bases, attrs)
		Graph_metaclass.graphClasses[name] = clsObj
		return clsObj

	@classmethod
	def getGraphClass(cls, name) :
		try :
			return cls.graphClasses[name]
		except KeyError :
			raise KeyError("There's no child of Graph by the name of: %s" % name)

	@classmethod
	def isGraph(cls, name) :
		return name in cls.graphClasses

	@classmethod
	def isDocumentGraph(cls, name) :
		try :
			col = cls.getGraphClass(name)
			return issubclass(col, Graph)
		except KeyError :
			return False

	@classmethod
	def isEdgeGraph(cls, name) :
		try :
			col = cls.getGraphClass(name)
			return issubclass(col, Edges)
		except KeyError :
			return False

def getGraphClass(name) :
	return Graph_metaclass.getGraphClass(name)

def isGraph(name) :
	return Graph_metaclass.isGraph(name)

class Graph(object) :

	_definitions = {}
	_orphanedCollections = []

	def __init__(self, database, jsonInit) :
		self.database = database
		try :
			self._key = jsonInit["_key"]
		except KeyError :
			self._key = jsonInit["name"]
		except KeyError :
			raise KeyError("'jsonInit' must have a field '_key' or a field 'name'")

		self._rev = jsonInit["_rev"]
		self._id = jsonInit["_id"]
	
		defs = []
		for e in jsonInit["edgeDefinitions"] :
			if e["collection"] not in self._definitions :
				raise CreationError("Collection '%s' is not mentioned in the definition of graph '%s'" (e["collection"], self.__class__,__name__))
			if e["from"] != self._definitions[e["collection"]]["from"] :
				raise CreationError("Edge definition '%s' of graph '%s' mismatch for 'from':\npython:%s\narangoDB:%s" (e["collection"], self.__class__,__name__, self._definitions[e["collection"]]["from"], e["from"]))
			if e["to"] != self._definitions[e["collection"]]["to"] :
				raise CreationError("Edge definition '%s' of graph '%s' mismatch for 'to':\npython:%s\narangoDB:%s" (e["collection"], self.__class__,__name__, self._definitions[e["collection"]]["to"], e["to"]))if e["to"] != self._definitions[e["collection"]]["to"] :
			defs.append(e["collection"])

		# if len(defs) < len(self._definitions) :
		# 	for e in self._definitions :
		# 		self.addDefinition(**e)
			
		if jsonInit["orphanCollections"] != self._orphanCollections
			raise CreationError("Orphan collection '%s' of graph '%s' mismatch:\npython:%s\narangoDB:%s" (e["collection"], self.__class__,__name__, self._orphanCollections, jsonInit["orphanCollections"]))
			
		self.URL = "%s/%s" % (self.database.graphsURL, self._key)

	def createVertex(self, collectionName, docAttributes, waitForSync = False) :
		url = "%s/vertex/%s" % (self.URL, collectionName)
		col = COL.getCollection(collectionName)
		col._validateDct(docAttributes)

		r = requests.post(url, data = docAttributes, params = {'waitForSync' : waitForSync})
		if r.status_code == 201 or r.status_code == 202 :
			return col[r.json()["_key"]]
		raise CreationError("Unable to create vertice, %s" % r.json()["errorMessage"], data)

	def deleteVertex(self, _key, waitForSync = False) :
		url = "%s/vertex/%s" % (self.URL, key)
		
		r = requests.delete(url, params = {'waitForSync' : waitForSync})
		if r.status_code == 200 or r.status_code == 202 :
			return True
		raise DeletionError("Unable to delete vertice, %s" % _key, data)

	def createEdge(self, collectionName, _fromId, _toId, docAttributes, waitForSync = False) :
		url = "%s/edge/%s" % (self.URL, collectionName)
		col = COL.getCollection(collectionName)
		col._validateDct(docAttributes)
		payload = docAttributes
		payload.update({'_from' : _fromId, '_to' : _toId})

		r = requests.post(url, data = payload, params = {'waitForSync' : waitForSync})
		if r.status_code == 201 or r.status_code == 202 :
			return col[r.json()["_key"]]
		raise CreationError("Unable to create vertice, %s" % r.json()["errorMessage"], data)

	def link(self, definition, doc1, doc2, waitForSync = False) :
		"A shorthand for createEdge that takes two documents as inputs"
		self.createEdge(definition, doc1._id, doc2._id, waitForSync)

	def deleteEdge(self, _key, waitForSync = False) :
		url = "%s/edge/%s" % (self.URL, key)
		
		r = requests.delete(url, params = {'waitForSync' : waitForSync})
		if r.status_code == 200 or r.status_code == 202 :
			return True
		raise DeletionError("Unable to delete edge, %s" % _key, data)

	def delete(self) :
		r = requests.delete(self.URL)
		data = r.json()
		if not r.status_code == 200 or data["error"] :
			raise DeletionError(data["errorMessage"], data)

	
	def __str__(self) :
		return "ArangoGraph; %s" % self._key

if False :
	
	class Social(Graph) :

		_definitions = {
			"friend" : EdgeDefinition(_from = [""], _to = []),
			"livesIn" : {"from" : [], "to" : []}
		}

		_orphanedCollections = [""]


	s = Social()
	s.link("friend", a, b)
	s["friend"](a, b)
	s.newDocument()