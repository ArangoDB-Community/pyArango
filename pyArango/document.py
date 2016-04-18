import json, types

from theExceptions import (CreationError, DeletionError, UpdateError)

__all__ = ["Document", "Edge"]

class Document(object) :
	"""The class that represents a document. Documents are meant to be instanciated by collections"""

	def __init__(self, collection, jsonFieldInit = {}) :
		self.reset(collection, jsonFieldInit)
		self.typeName = "ArangoDoc"

	def reset(self, collection, jsonFieldInit = {}) :
		"""replaces the current values in the document by those in jsonFieldInit"""
		self.collection = collection
		self.connection = self.collection.connection
		self.documentsURL = self.collection.documentsURL
		
		self._store = {}
		self._patchStore = {}

		self._id, self._rev, self._key = None, None, None
		self.URL = None

		self.set(jsonFieldInit)
		self.modified = True

	def setPrivates(self, fieldDict) :
		"""will set self._id, self._rev and self._key field. Private fields (starting by '_') are all accessed using the self. interface,
		other fields are accessed through self[fielName], the same as regular dictionnary in python"""
		try :
			self._id = fieldDict["_id"]
			self.URL = "%s/%s" % (self.documentsURL, self._id)
			del(fieldDict["_id"])
		
			self._rev = fieldDict["_rev"]
			del(fieldDict["_rev"])
		
			self._key = fieldDict["_key"]
			del(fieldDict["_key"])
		except KeyError :
			self._id, self._rev, self._key = None, None, None
			self.URL = None

	def set(self, fieldDict = None) :
		"""Sets the document according to values contained in the dictinnary fieldDict. This will also set self._id/_rev/_key"""
		
		if fieldDict and self._id is None :
			self.setPrivates(fieldDict)

		if self.collection._validation['on_set']:
			for k in fieldDict.keys() :
				self[k] = fieldDict[k]
		else :
			self._store.update(fieldDict)

	def save(self, waitForSync = False, **docArgs) :
		"""Saves the document to the database by either performing a POST (for a new document) or a PUT (complete document overwrite).
		If you want to only update the modified fields use the .path() function.
		Use docArgs to put things such as 'waitForSync = True' (for a full list cf ArangoDB's doc).
		It will only trigger a saving of the document if it has been modified since the last save. If you want to force the saving you can use forceSave()"""

		if self.modified :
			if self.collection._validation['on_save'] :
				self.validate(patch = False)

			params = dict(docArgs)
			params.update({'collection': self.collection.name, "waitForSync" : waitForSync })
			payload = {} 
			payload.update(self._store)
			
			if self.URL is None :
				if self._key is not None :
					payload["_key"] = self._key
				payload = json.dumps(payload)
				r = self.connection.session.post(self.documentsURL, params = params, data = payload)
				update = False
			else :
				payload = json.dumps(payload)
				r = self.connection.session.put(self.URL, params = params, data = payload)
				update = True

			data = r.json()
			if (r.status_code == 201 or r.status_code == 202) and not data['error'] :
				if update :
					self._rev = data['_rev']
				else :
					self.setPrivates(data)
			else :
				if update :
					raise UpdateError(data['errorMessage'], data)
				else :
					raise CreationError(data['errorMessage'], data)

			self.modified = False

	def forceSave(self, **docArgs) :
		"saves even if the document has not been modified since the last save"
		self.modified = True
		self.save(**docArgs)

	def saveCopy(self) :
		"saves a copy of the object and become that copy. returns a tuple (old _key, new _key)"
		old_key = self._key
		self.reset(self.collection)
		self.save()
		return (old_key, self._key)

	def patch(self, keepNull = True, **docArgs) :
		"""Saves the document by only updating the modified fields.
		The default behaviour concening the keepNull parameter is the opposite of ArangoDB's default, Null values won't be ignored
		Use docArgs for things such as waitForSync = True"""

		if self.collection._validation['on_save'] :
			self.validate(patch = True)

		if self.URL is None :
			raise ValueError("Cannot patch a document that was not previously saved")
		
		if len(self._patchStore) > 0 :
			params = dict(docArgs)
			params.update({'collection': self.collection.name, 'keepNull' : keepNull})
			payload = json.dumps(self._patchStore)
			
			r = self.connection.session.patch(self.URL, params = params, data = payload)
			data = r.json()
			if (r.status_code == 201 or r.status_code == 202) and not data['error'] :
				self._rev = data['_rev']
			else :
				raise UpdateError(data['errorMessage'], data)

			self.modified = False

	def delete(self) :
		"deletes the document from the database"
		if self.URL is None :
			raise DeletionError("Can't delete a document that was not saved") 
		r = self.connection.session.delete(self.URL)
		data = r.json()
		if (r.status_code != 200 and r.status_code != 202) or data['error'] :
			raise DeletionError(data['errorMessage'], data)
		self.reset(self.collection)

		self.modified = True

	def validate(self, patch = False) :
		"validates either the whole store, or only the patch store( patch = True) of the document according to the collection's settings.If logErrors returns a dictionary of errros per field, else raises exceptions"
		if patch :
			return self.collection.validateDct(self._patchStore)
		else :
			return self.collection.validateDct(self._store)

	def getInEdges(self, edges, rawResults = False) :
		"An alias for getEdges() that returns only the in Edges"
		return self.getEdges(edges, inEdges = True, outEdges = False, rawResults = rawResults)
		
	def getOutEdges(self, edges, rawResults = False) :
		"An alias for getEdges() that returns only the out Edges"
		return self.getEdges(edges, inEdges = False, outEdges = True, rawResults = rawResults)

	def getEdges(self, edges, inEdges = True, outEdges = True, rawResults = False) :
		"""returns in, out, or both edges linked to self belonging the collection 'edges'.
		If rawResults a arango results will be return as fetched, if false, will return a liste of Edge objects"""
		try :
			return edges.getEdges(self, inEdges, outEdges, rawResults)
		except AttributeError :
			raise AttributeError("%s does not seem to be a valid Edges object" % edges)

	def __getitem__(self, k) :
		"""Document fields are accessed in a dictionary like fashion: doc[fieldName]. With the exceptions of private fiels (starting with '_')
		that are accessed as object fields: doc._key"""
		if self.collection._validation['allow_foreign_fields'] or self.collection.hasField(k) :
			return self._store.get(k)

		try :
			return self._store[k]
		except KeyError :
			raise KeyError("Document of collection '%s' has no field '%s', for a permissive behaviour set 'allow_foreign_fields' to True" % (self.collection.name, k))

	def __setitem__(self, k, v) :
		"""Documents work just like dictionaries doc[fieldName] = value. With the exceptions of private fiels (starting with '_')
		that are accessed as object fields: doc._key"""	
		
		def _recValidate(k, v) :
			if type(v) is types.DictType :
				for kk, vv in v.iteritems() :
					newk = "%s.%s" % (k, kk) 
					_recValidate(newk, vv)	
			else :
				self.collection.validateField(k, v)

		
		if self.collection._validation['on_set'] :
			_recValidate(k, v)

		self._store[k] = v
		if self.URL is not None :
			self._patchStore[k] = self._store[k]
		
		self.modified = True

	def __delitem__(self, k) :
		del(self._store[k])

	def __str__(self) :
		return "%s '%s': %s" % (self.typeName, self._id, repr(self._store))

	def __repr__(self) :
		return "%s '%s': %s" % (self.typeName, self._id, repr(self._store))

class Edge(Document) :
	"""An Edge document"""
	def __init__(self, edgeCollection, jsonFieldInit = {}) :
		self.reset(edgeCollection, jsonFieldInit)

	def reset(self, edgeCollection, jsonFieldInit = {}) :
		Document.reset(self, edgeCollection, jsonFieldInit)
		self.typeName = "ArangoEdge"

	def setPrivates(self, jsonFieldInit) :
		try :
			self._from = jsonFieldInit["_from"]
			del(jsonFieldInit["_from"])
			self._to = jsonFieldInit["_to"]
			del(jsonFieldInit["_to"])
		except KeyError :
			self._from, self._to = None, None
		Document.setPrivates(self, jsonFieldInit)

	def links(self, fromVertice, toVertice, **edgeArgs) :
		"An alias of save that works only for first saves. It will also trigger the saving of fromVertice and toVertice"
		if self.URL is not None :
			raise AttributeError("It appears that the edge has already been saved. You can now use save() and patch()")
		
		fromVertice.save()
		toVertice.save()

		self.save(fromVertice, toVertice, **edgeArgs)

	def save(self, fromVertice = None, toVertice = None, **edgeArgs) :
		"""Works like Document's except that the first time you save an Edge you must specify the 'from' and 'to' vertices.
		There's also a links() function especially for first saves"""
		import types

		if self.URL is None and (fromVertice is None or toVertice is None) :
			raise ValueError("The first time you save an Edge you must specify the 'from' and 'to' vertices")

		if fromVertice.__class__ is Document :
			edgeArgs["from"] = fromVertice._id
			self._from = edgeArgs["from"]
		elif (type(fromVertice) is types.StringType) or (type(fromVertice) is types.UnicodeType) :
			edgeArgs["from"] = fromVertice
			self._from = edgeArgs["from"]
		else :
			if not self._from :
				raise ValueError("fromVertice must be either a Document or a String, got: %s" % fromVertice)
		
		if toVertice.__class__ is Document :
			edgeArgs["to"] = toVertice._id
			self._to = edgeArgs["to"]
		elif (type(toVertice) is types.StringType) or (type(toVertice) is types.UnicodeType) :
			edgeArgs["to"] = toVertice
			self._to = edgeArgs["to"]
		else :
			if not self._to :
				raise ValueError("toVertice must be either a Document or a String, got: %s" % fromVertice)

		Document.save(self, **edgeArgs)
