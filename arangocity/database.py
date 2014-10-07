import requests
import json
import types

from collection import Collection, SystemCollection, GenericCollection, Collection_metaclass
from theExceptions import CreationError, UpdateError

class Database(object) :
	
	def __init__(self, connection, name) :
		"meant to be called by the connection only"

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
			self.collections = {}
			for colData in data["collections"] :
				colName = colData['name']
				if colData['isSystem'] :
					colObj = SystemCollection(self, colData)
				else :
					try :
						colClass = Collection_metaclass.getCollectionClass(colName)
						colObj = colClass(self, colData)
					except KeyError :
						colObj = GenericCollection(self, colData)

				if colName not in self.collections :
					self.collections[colName] = colObj
		else :
			raise UpdateError(data["errorMessage"], data)

	def createCollection(self, className = 'GenericCollection', **colArgs) :
		""" Must be the name of a class inheriting from Collection. Use colArgs to put things such as 'isVolatile = True'.
		The 'name' parameter will be ignored if className != 'GenericCollection' since it is already specified by className"""
		
		if className != 'GenericCollection' :
			colArgs['name'] = className
		else :
			if 'name' not in colArgs :
				raise ValueError("a 'name' argument mush be supplied if you want to create a generic collection")
					
		colClass = Collection_metaclass.getCollectionClass(className)

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