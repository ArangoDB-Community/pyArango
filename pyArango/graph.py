import json

from theExceptions import (CreationError, DeletionError, UpdateError, TraversalError)
import collection as COL

__all__ = ["Graph", "getGraphClass", "isGraph", "getGraphClasses", "Graph_metaclass", "EdgeDefinition"]

class Graph_metaclass(type) :
	"""Keeps track of all graph classes and does basic validations on fields"""
	graphClasses = {}
	
	def __new__(cls, name, bases, attrs) :
		clsObj = type.__new__(cls, name, bases, attrs)
		if name != 'Graph' :
			try :
				if len(attrs['_edgeDefinitions']) < 1 :
					raise CreationError("Graph class '%s' has no edge definition" % name)
			except KeyError :
				raise CreationError("Graph class '%s' has no field _edgeDefinition" % name)

		if name != "Graph" :
			Graph_metaclass.graphClasses[name] = clsObj
		return clsObj

	@classmethod
	def getGraphClass(cls, name) :
		"""return a graph class by its name"""
		try :
			return cls.graphClasses[name]
		except KeyError :
			raise KeyError("There's no child of Graph by the name of: %s" % name)

	@classmethod
	def isGraph(cls, name) :
		"""returns true/false depending if there is a graph called name"""
		return name in cls.graphClasses

def getGraphClass(name) :
	"""alias for Graph_metaclass.getGraphClass()"""
	return Graph_metaclass.getGraphClass(name)

def isGraph(name) :
	"""alias for Graph_metaclass.isGraph()"""
	return Graph_metaclass.isGraph(name)

def getGraphClasses() :
	"returns a dictionary of all defined graph classes"
	return Graph_metaclass.graphClasses

class EdgeDefinition(object) :
	"""An edge definition for a graph"""

	def __init__(self, edgesCollection, fromCollections, toCollections) :
		self.edgesCollection = self.name = edgesCollection
		self.fromCollections = fromCollections
		self.toCollections = toCollections

	def toJson(self) :
		return { 'collection' : self.edgesCollection, 'from' : self.fromCollections, 'to' : self.toCollections }

	def __str__(self) :
		return '<ArangoED>'+ str(self.toJson())

	def __repr__(self) :
		return str(self)
	
class Graph(object) :
	"""The class from witch all your graph types must derive"""

	__metaclass__ = Graph_metaclass

	_edgeDefinitions = []
	_orphanedCollections = []

	def __init__(self, database, jsonInit) :
		self.database = database
		self.connection = self.database.connection
		try :
			self._key = jsonInit["_key"]
		except KeyError :
			self._key = jsonInit["name"]
		except KeyError :
			raise KeyError("'jsonInit' must have a field '_key' or a field 'name'")

		self.name = self._key
		self._rev = jsonInit["_rev"]
		self._id = jsonInit["_id"]
	
		self.definitions = {}
		for de in self._edgeDefinitions :
			if de.name not in self.database.collections and not COL.isEdgeCollection(de.name) :
				raise KeyError("'%s' is not a valid edge collection" % de.name)
			self.definitions[de.name] = de
		
		# for e in jsonInit["edgeDefinitions"] :
		# 	if e["collection"] not in self._edgeDefinitions :
		# 		raise CreationError("Collection '%s' is not mentioned in the definition of graph '%s'" % (e["collection"], self.__class__,__name__))
		# 	if e["from"] != self._edgeDefinitions[e["collection"]]["from"] :
		# 		vals = (e["collection"], self.__class__,__name__, self._edgeDefinitions[e["collection"]]["from"], e["from"])
		# 		raise CreationError("Edge definition '%s' of graph '%s' mismatch for 'from':\npython:%s\narangoDB:%s" % vals)
		# 	if e["to"] != self._edgeDefinitions[e["collection"]]["to"] :
		# 		vals = (e["collection"], self.__class__,__name__, self._edgeDefinitions[e["collection"]]["to"], e["to"])
		# 		raise CreationError("Edge definition '%s' of graph '%s' mismatch for 'to':\npython:%s\narangoDB:%s" % vals )
		# 	defs.append(e["collection"])

		# if jsonInit["orphanCollections"] != self._orphanCollections :
		# 	raise CreationError("Orphan collection '%s' of graph '%s' mismatch:\npython:%s\narangoDB:%s" (e["collection"], self.__class__,__name__, self._orphanCollections, jsonInit["orphanCollections"]))
			
		self.URL = "%s/%s" % (self.database.graphsURL, self._key)

	def createVertex(self, collectionName, docAttributes, waitForSync = False) :
		"""adds a vertex to the graph and returns it"""
		url = "%s/vertex/%s" % (self.URL, collectionName)
		self.database[collectionName].validateDct(docAttributes)

		r = self.connection.session.post(url, data = json.dumps(docAttributes), params = {'waitForSync' : waitForSync})
		
		data = r.json()
		if r.status_code == 201 or r.status_code == 202 :
			return self.database[collectionName][data["vertex"]["_key"]]
		
		raise CreationError("Unable to create vertice, %s" % data["errorMessage"], data)

	def deleteVertex(self, document, waitForSync = False) :
		"""deletes a vertex from the graph as well as al linked edges"""
		url = "%s/vertex/%s" % (self.URL, document._id)
		
		r = self.connection.session.delete(url, params = {'waitForSync' : waitForSync})
		data = r.json()
		if r.status_code == 200 or r.status_code == 202 :
			return True

		raise DeletionError("Unable to delete vertice, %s" % document._id, data)

	def createEdge(self, collectionName, _fromId, _toId, edgeAttributes, waitForSync = False) :
		"""creates an edge between two documents"""
		
		if collectionName not in self.definitions :
			raise KeyError("'%s' is not among the edge definitions" % collectionName)
		
		url = "%s/edge/%s" % (self.URL, collectionName)
		self.database[collectionName].validateDct(edgeAttributes)
		payload = edgeAttributes
		payload.update({'_from' : _fromId, '_to' : _toId})

		r = self.connection.session.post(url, data = json.dumps(payload), params = {'waitForSync' : waitForSync})
		data = r.json()
		if r.status_code == 201 or r.status_code == 202 :
			return self.database[collectionName][data["edge"]["_key"]]
		raise CreationError("Unable to create edge, %s" % r.json()["errorMessage"], data)

	def link(self, definition, doc1, doc2, edgeAttributes, waitForSync = False) :
		"A shorthand for createEdge that takes two documents as input"
		return self.createEdge(definition, doc1._id, doc2._id, edgeAttributes, waitForSync)

	def unlink(self, definition, doc1, doc2) :
		"deletes all links between doc1 and doc2"
		links = self.database[definition].fetchByExample( {"_from": doc1._id,"_to" : doc2._id}, batchSize = 100)
		for l in links :
			self.deleteEdge(l)

	def deleteEdge(self, edge, waitForSync = False) :
		"""removes an edge from the graph"""
		url = "%s/edge/%s" % (self.URL, edge._id)
		r = self.connection.session.delete(url, params = {'waitForSync' : waitForSync})
		if r.status_code == 200 or r.status_code == 202 :
			return True
		raise DeletionError("Unable to delete edge, %s" % edge._id, r.json())

	def delete(self) :
		"""deletes the graph"""
		r = self.connection.session.delete(self.URL)
		data = r.json()
		if not r.status_code == 200 or data["error"] :
			raise DeletionError(data["errorMessage"], data)

	def traverse(self, startVertex, **kwargs) :
		"""Traversal! see: https://docs.arangodb.com/HttpTraversal/README.html for a full list of the possible kwargs.
		The function must have as argument either: direction = "outbout"/"any"/"inbound" or expander = "custom JS (see arangodb's doc)".
		The function can't have both 'direction' and 'expander' as arguments.
		"""

		url = "%s/traversal" % self.database.URL
		payload = {	"startVertex": startVertex._id, "graphName" : self.name}
		if "expander" in kwargs :
			if "direction" in kwargs :
					raise ValueError("""The function can't have both 'direction' and 'expander' as arguments""") 
		elif "direction" not in kwargs :
			raise ValueError("""The function must have as argument either: direction = "outbout"/"any"/"inbound" or expander = "custom JS (see arangodb's doc)" """) 

		payload.update(kwargs)

		r = self.connection.session.post(url, data = json.dumps(payload))
		data = r.json()
		if not r.status_code == 200 or data["error"] :
			raise TraversalError(data["errorMessage"], data)

		return data["result"]

	def __str__(self) :
		return "ArangoGraph: %s" % self.name
