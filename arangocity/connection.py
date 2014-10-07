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

		self.update()
	
	def update(self) :
		r = requests.get(self.databasesURL)
		data = r.json()
		if r.status_code == 200  and not data["error"] :
			self.databases = {}
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