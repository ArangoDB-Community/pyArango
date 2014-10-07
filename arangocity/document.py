import requests
import json

from theExceptions import (CreationError, UpdateError)

class Document(object) :

	def __init__(self, collection, _id = None, _rev = None, _key = None, fieldsToSet = {}) :
		"meant to be called by the collection only"
		self.reset(collection)

	def reset(self, collection, _id = None, _rev = None, _key = None, fieldsToSet = {}) :
		self.collection = collection
		self.documentsURL = "%s/document" % (self.collection.database.URL)
		
		self._id = _id
		self._key = _key 
		self._rev = _rev
		self._store = {}

		if _id is not None :
			self.URL = "%s/%s" % (self.documentsURL, self._id)
		else :
			self.URL = None
		
		if len(fieldsToSet) > 0 :
			self.set(fieldsToSet)
		else :
			for k in self.collection.__class__._fields.keys() :
				self._store[k] = None
			
		self._patchStore = {}

	def set(self, fieldsToSet) :
		"""Sets the document according to values contained in the dictinnary fieldsToSet"""
		for k in fieldsToSet.keys() :
			self[k] = fieldsToSet[k]
	
	def save(self, **docArgs) :
		"""This fct either performs a POST (for a new document) or a PUT (complete document overwrite).
		If you want to only update the modified fields use the .path() function.
		Use docArgs to put things such as 'waitForSync = True'"""

		for k, v in self._store.iteritems() :
			if self.collection._test_fields_on_save :
				self.collection.testFieldValue(k, v)
	
		params = dict(docArgs)
		params.update({'collection': self.collection.name })
		payload = json.dumps(self._store)

		if self.URL is None :
			r = requests.post(self.documentsURL, params = params, data = payload)
			update = False
		else :
			r = requests.put(self.URL, params = params, data = payload)
			update = True

		data = r.json()
		if (r.status_code == 201 or r.status_code == 202) and not data['error'] :
			if update :
				self._rev = data['_rev']
			else :
				self._id = data["_id"]
				self._key = data["_key"]
				self._rev = data["_rev"]
				self.URL = "%s/%s" % (self.documentsURL, self._id)
		else :
			if update :
				raise UpdateError(data['errorMessage'], data)
			else :
				raise CreationError(data['errorMessage'], data)

	def saveCopy(self) :
		"saves a copy of the object and become that copy. returns a tuple (old _id, new _id)"
		old_id = self._id
		self.reset(self.collection)
		self.save()
		return (old_id, self._id)

	def patch(self, keepNull = True, **docArgs) :
		"""Updates only the modified fields.
		The default behaviour concening the keepNull parameter is the opposite of ArangoDB's default, Null values won't be ignored
		Use docArgs for things such as waitForSync = True"""
		
		if self.URL is None :
			raise ValueError("Cannot patch a document that was not previously saved")
		
		params = dict(docArgs)
		params.update({'collection': self.collection.name, 'keepNull' : keepNull})
		payload = json.dumps(self._patchStore)
		
		r = requests.patch(self.URL, params = params, data = payload)
		data = r.json()
		if (r.status_code == 201 or r.status_code == 202) and not data['error'] :
			self._rev = data['_rev']
		else :
			raise UpdateError(data['errorMessage'], data)

	def delete(self) :
		if self.URL is None :
			raise DeletionError("Can't delete a document that was not saved") 
		r = requests.delete(self.URL)
		data = r.json()
		if (r.status_code != 200 and r.status_code != 202) or data['error'] :
			raise DeletionError(data['errorMessage'], data)
		self.reset(self.collection)

	def __getattribute__(self, k) :
		return object.__getattribute__(self, k)

	def __getitem__(self, k) :
		return self._store[k]

	def __setitem__(self, k, v) :
		if self.collection._test_fields_on_set :
			self.collection.testFieldValue(k, v)

		self._store[k] = v
		if self.URL is not None :
			self._patchStore[k] = self._store[k]
		