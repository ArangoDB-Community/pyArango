import requests
import json
import types

import collection as COL
import graph as GR

from document import Document
from graph import Graph
from query import AQLQuery
from theExceptions import CreationError, UpdateError

class Database(object) :
	
	def __init__(self, connection, name) :
		"meant to be called by the connection only"

		self.name = name
		self.connection = connection
		self.collections = {}
		
		self.URL = '%s/_db/%s/_api' % (self.connection.arangoURL, self.name)
		self.collectionsURL = '%s/collection' % (self.URL)
		self.cursorsURL = '%s/cursor' % (self.URL)
		self.graphsURL = "%s/gharial" % self.URL

		self.collections = {}
		self.graphs = {}

		self.reload()
	
	def reloadCollections(self) :
		"reloads the collection list."
		r = requests.get(self.collectionsURL)
		data = r.json()
		if r.status_code == 200 :
			self.collections = {}
			
			for colData in data["collections"] :
				colName = colData['name']
				if colData['isSystem'] :
					colObj = COL.SystemCollection(self, colData)
				else :
					try :
						colClass = COL.getCollectionClass(colName)
						colObj = colClass(self, colData)
					except KeyError :
						colObj = COL.GenericCollection(self, colData)
				self.collections[colName] = colObj
		else :
			raise updateError(data["errorMessage"], data)

	def reloadGraphs(self) :
		"reloads the graph list"
		r = requests.get(self.graphsURL)
		data = r.json()
		if r.status_code == 200 :
			self.graphs = {}
			for graphData in data["graphs"] :
				try :
					self.graphs[graphData["_key"]] = GR.getGraphClass(graphData["_key"])(self, graphData)
				except KeyError :
					self.graphs[graphData["_key"]] = Graph(self, graphData)
		else :
			raise UpdateError(data["errorMessage"], data)
	
	def reload(self) :
		"reloads collections and graphs"
		self.reloadCollections()
		self.reloadGraphs()
	
	def createCollection(self, className = 'GenericCollection', **colArgs) :
		"""Creeats a collection and returns it.
		ClassName the name of a class inheriting from Collection or Egdes. Use colArgs to put things such as 'isVolatile = True' (see ArangoDB's doc
		for a full list of possible arugments)."""
		
		if className != 'GenericCollection' :
			colArgs['name'] = className
		else :
			if 'name' not in colArgs :
				raise ValueError("a 'name' argument mush be supplied if you want to create a generic collection")
					
		colClass = COL.getCollectionClass(className)

		if colArgs['name'] in self.collections :
			raise CreationError("Database %s already has a collection named %s" % (self.name, colArgs['name']) )

		if issubclass(colClass, COL.Edges) :
			colArgs["type"] = COL.COLLECTION_EDGE_TYPE
		else :
			colArgs["type"] = COL.COLLECTION_DOCUMENT_TYPE
				
		payload = json.dumps(colArgs)
		r = requests.post(self.collectionsURL, data = payload)
		data = r.json()
		if r.status_code == 200 and not data["error"] :
			col = colClass(self, data)
			self.collections[col.name] = col
			return self.collections[col.name]
		else :
			raise CreationError(data["errorMessage"], data)

	def fetchDocument(self, _id) :
		"fetchs a document using it's _id"
		sid = _id.split("/")
		return self[sid[0]][sid[1]]

	def createGraph(self, name, createCollections = True) :
		"""Creates a graph and returns it. 'name' must be the name of a class inheriting from Graph.
		You can decide weither or not you want non existing collections to be created by setting the value of 'createCollections'.
		If the value if 'false' checks will be performed to make sure that every collection mentionned in the edges definition exist. Raises a ValueError in case of
		a non-existing collection."""

		def _checkCollectionList(lst) :
			for colName in lst :
				if not COL.isCollection(colName) :
					raise ValueError("'%s' is not a defined Collection" % colName)

		graphClass = GR.getGraphClass(name)

		ed = []
		for e in graphClass._edgeDefinitions :
			if not createCollections :
				if not COL.isEdgeCollection(e.edgesCollection) :
					raise ValueError("'%s' is not a defined Edge Collection" % e.edgesCollection)
				_checkCollectionList(e.fromCollections)
				_checkCollectionList(e.toCollections)

			ed.append(e.toJson())
		
		if not createCollections :
			_checkCollectionList(graphClass._orphanedCollections)

		payload = {
				"name": name,
				"edgeDefinitions": ed,
				"orphanCollections": graphClass._orphanedCollections
			}
		

		payload = json.dumps(payload)
		r = requests.post(self.graphsURL, data = payload)
		data = r.json()
		if r.status_code == 201 :
			self.graphs[name] = graphClass(self, data["graph"])
		else :
			raise CreationError(data["errorMessage"], data)		
		return self.graphs[name]

	# def _checkGraphCollections(self, edgeDefinitions, orphanCollections) :
	# 	for ed in edgeDefinitions :	
	# 		checkList(ed["from"])
	# 		checkList(ed["to"])
		
	# 	checkList(orphanCollections)

	def hasCollection(self, name) :
		"""returns true if the databse has a collection by the name of 'name'"""
		return name in self.collections

	def hasGraph(name):
		"""returns true if the databse has a graph by the name of 'name'"""
		return name in self.graphs
	
	def AQLQuery(self, query, rawResults = False, batchSize = 0, bindVars = {}, options = {}, count = False, fullCount = False) :
		"Set rawResults = True if you want the query to return dictionnaries instead of Document objects"
		return AQLQuery(self, query, rawResults, batchSize, bindVars, options, count, fullCount)

	def validateAQLQuery(self, query, bindVars = {}, options = {}) :
		"returns the server answer is the query is valid. Raises an AQLQueryError if not"
		payload = {'query' : query, 'bindVars' : bindVars, 'options' : options}
		r = requests.post(self.cursorsURL, data = json.dumps(payload))
		data = r.json()
		if r.status_code == 201 and not data["error"] :
			return data
		else :
			raise AQLQueryError(data["errorMessage"], query, data)

	def __repr__(self) :
		return "ArangoDB database: %s" % self.name

	def __getitem__(self, collectionName) :
		"""use database[collectionName] to get a collection from the database"""
		try :
			return self.collections[collectionName]
		except KeyError :
			self.reload()
			try :
				return self.collections[collectionName]
			except KeyError :
				raise KeyError("Can't find any collection named : %s" % collectionName)


class DBHandle(Database) :
	"As the loading of a DB triggers the loading of collections and graphs within. Only handles are loaded first. The full database are loaded on demand."
	def __init__(self, connection, name) :
		self.connection = connection
		self.name = name

	def __getattr__(self, k) :
		name = Database.__getattribute__(self, 'name')
		connection = Database.__getattribute__(self, 'connection')
		Database.__init__(self, connection, name)
		return Database.__getattribute__(self, k)
