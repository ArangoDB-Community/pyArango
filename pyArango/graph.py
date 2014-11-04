import requests
import json

from theExceptions import (CreationError, DeletionError, UpdateError)
import collection as COL

class EdgeDefinition(object) :
	def __init__(self, graph, jsonInit) :
		self.graph = graph

		self.name = jsonInit["collection"]
		
		self.fromCollections = set(jsonInit["from"])
		self.toCollections = set(jsonInit["to"])
		
	def link(self, doc1, doc2, edgeValues) :
		if doc1.collection.__class__.__name__ not in self.fromCollections :
			raise ValueError("'doc1' collection must one of the following '%s', got %s" % (self.fromCollections, doc1.collection.__class__.__name__))

		if doc2.collection.__class__.__name__ not in self.toCollections :
			raise ValueError("'doc2' collection must one of the following '%s', got %s" % (self.toCollections, doc2.collection.__class__.__name__))

		edge = self.graph.database[self.name].createEdge(edgeValues)
		edge.links(doc1, doc2)

class Graph(object) :

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
	
		self.edgeDefinitions = {}
		for e in jsonInit["edgeDefinitions"] :
			ed = EdgeDefinition(self, e)
			self.edgeDefinitions[ed.name] = ed
		
		self.orphanCollections = jsonInit["orphanCollections"]
		
		self.URL = "%s/%s" % (self.database.graphsURL, self._key)

	# def createVertex(self, docAttributes, docName = None) :
	# 	url = "%s/vertex" % (self.URL)
		
	# 	if docName is not None :
	# 		payload = {"_key" : docName}
	# 		payload.update
	# 	r = requests.post()

	def delete(self) :
		r = requests.delete(self.URL)
		data = r.json()
		if not r.status_code == 200 or data["error"] :
			raise DeletionError(data["errorMessage"], data)

	def _link(self, definitionName, edgeValues, document1, document2) :
		edge = self.database[definitionName].createEdge()
		edge.set(edgeValues)
		edge.links(document1, document2)

	def getRelations(self) :
		return self.edgeDefinitions.keys()

	def addVertexCollection(self, collection) :
		pass

	def removeVertexCollection(self) :
		pass

	def addEdgeDefinition(self):
		pass	
	
	def replaceEdgeDefinition(self):	
		pass

	def removeEdgeDefinition(self):	
		pass

	def __getitem__(self, k) :
		return self.edgeDefinitions[k]

	def __str__(self) :
		return "ArangoGraph; %s" % self._key

if False :
	
	class Friendship(Graph) :

		_definitions = {
			"friend" : EdgeDefinition(edges = "e", _from = [""], _to = []),
			"livesIn" : EdgeDefinition(edges = "e", _from = [""], _to = [])
		}

		_orphanedCollections = [""]
