import requests
import json

from database import Database, DBHandle
from theExceptions import SchemaViolation, CreationError, ConnectionError

class Connection(object) :
	"""This is the entry point in pyArango and direcltt handles databases."""
	def __init__(self, arangoURL = 'http://localhost:8529') :
		self.databases = {}
		if arangoURL[-1] == "/" :
			self.arangoURL = url[:-1]
		else :
			self.arangoURL = arangoURL
		
		self.URL = '%s/_api' % self.arangoURL
		self.databasesURL = '%s/database' % self.URL

		self.session = None
		self.resetSession()
		self.reload()

	def resetSession(self) :
		"""resets the session"""
		self.session = requests.Session()

	def reload(self) :
		"""Reloads the database list.
		Because loading a database triggers the loading of all collections and graphs within,
		only handles are loaded when this function is called. The full databases are loaded on demand when accessed
		"""
		r = self.session.get(self.databasesURL)
		data = r.json()
		if r.status_code == 200  and not data["error"] :
			self.databases = {}
			for dbName in data["result"] :
				if dbName not in self.databases :
					self.databases[dbName] = DBHandle(self, dbName)
		else :
			raise ConnectionError(data["errorMessage"], self.databasesURL, data)

	def createDatabase(self, name, **dbArgs) :
		"use dbArgs for arguments other than name. for a full list of arguments please have a look at arangoDB's doc"
		dbArgs['name'] = name
		payload = json.dumps(dbArgs)
		r = self.session.post(self.databasesURL, data = payload)
		data = r.json()
		if r.status_code == 201 and not data["error"] :
			db = Database(self, name)
			self.databases[name] = db
			return self.databases[name]
		else :
			raise CreationError(data["errorMessage"], data)

	def hasDatabase(self, name) :
		"""returns true/false wether the connection has a database by the name of 'name'"""
		return name in self.databases

	def __getitem__(self, dbName) :
		"""Collection[dbName] returns a database by the name of 'dbName', raises a KeyError if not found"""
		try :
			return self.databases[dbName]
		except KeyError :
			self.reload()
			try :
				return self.databases[dbName]
			except KeyError :
				raise KeyError("Can't find any database named : %s" % dbName)
