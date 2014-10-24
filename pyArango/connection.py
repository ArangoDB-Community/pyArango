import requests
import json

from database import Database
from theExceptions import SchemaViolation, CreationError, ConnectionError

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

		self.reload()
	
	def reload(self) :
		"reloads the database list"
		r = requests.get(self.databasesURL)
		data = r.json()
		if r.status_code == 200  and not data["error"] :
			self.databases = {}
			for dbName in data["result"] :
				if dbName not in self.databases :
					db = Database(self, dbName)
					self.databases[dbName] = db
		else :
			raise ConnectionError(data["errorMessage"], self.databasesURL, data)

	def createDatabase(self, name, **dbArgs) :
		"use dbArgs for arguments other than name"

		dbArgs['name'] = name
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
			self.reload()
			try :
				return self.databases[dbName]
			except KeyError :
				raise KeyError("Can't find any database named : %s" % dbName)