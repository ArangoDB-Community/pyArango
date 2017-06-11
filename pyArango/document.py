import json, types

from .theExceptions import (CreationError, DeletionError, UpdateError, ValidationError, SchemaViolation, InvalidDocument)

__all__ = ["DocumentStore", "Document", "Edge"]

class DocumentStore(object) :
    """Store all the data of a document in hierarchy of stores and handles validation.
    Does not store private information, these are in the document."""

    def __init__(self, collection, validators={}, initDct={}, patch=False, subStore=False) :
        self.store = {}
        self.patchStore = {}
        self.collection = collection
        self.validators = validators
        
        self.mustValidate = False
        for v in self.collection._validation.values() :
            if v :
                self.mustValidate = True
                break
        
        self.isSubStore = subStore
        self.subStores = {}
        self.patching = patch
        self.set(initDct)
        self.patching = True

    def resetPatch(self) :
        """reset patches"""
        self.patchStore = {}

    def getPatches(self) :
        """get patches as a dictionary"""
        if not self.mustValidate :
            return self.getStore()

        res = {}
        res.update(self.patchStore)
        for k, v in self.subStores.items() :
            res[k] = v.getPatches()
        
        return res
        
    def getStore(self) :
        """get the inner store as dictionary"""
        res = {}
        res.update(self.store)
        for k, v in self.subStores.items() :
            res[k] = v.getStore()
        
        return res

    def validateField(self, field) :
        """Validatie a field"""
        if field not in self.validators and not self.collection._validation['allow_foreign_fields'] :
            raise SchemaViolation(self.collection.__class__, field)

        if field in self.store:
            if isinstance(self.store[field], DocumentStore) :
                return self[field].validate()
            
            if field in self.patchStore :
                return self.validators[field].validate(self.patchStore[field])
            else :
                return self.validators[field].validate(self.store[field])

        return True

    def validate(self) :
        """Validate the hole document"""
        if not self.mustValidate :
            return True

        res = {}
        for field in self.validators.keys() :
            try :
                if isinstance(self.validators[field], dict) and field not in self.store :
                    self.store[field] = DocumentStore(self.collection, validators = self.validators[field], initDct = {}, subStore=True)
                self.validateField(field)
            except InvalidDocument as e :
                res.update(e.errors)
            except (ValidationError, SchemaViolation) as e:
                res[field] = str(e)

        if len(res) > 0 :
            raise InvalidDocument(res)
        
        return True

    def set(self, dct) :
        """Set the store using a dictionary"""
        if not self.mustValidate :
            self.store = dct
            self.patchStore = dct
            return

        for field, value in dct.items() :
            if field not in self.collection.arangoPrivates :
                if isinstance(value, dict) :
                    if field in self.validators :
                        vals = self.validators[field]
                    else :
                        vals = {}
                    self[field] = DocumentStore(self.collection, validators = vals, initDct = value, patch = self.patching, subStore=True)
                    self.subStores[field] = self.store[field]
                else :
                    self[field] = value

    def __getitem__(self, field) :
        """Get an element from the store"""
        if (field in self.validators) and isinstance(self.validators[field], dict) and (field not in self.store) :
            self.store[field] = DocumentStore(self.collection, validators = self.validators[field], initDct = {}, patch = self.patching, subStore=True)
            self.subStores[field] = self.store[field]
            self.patchStore[field] = self.store[field]

        if self.collection._validation['allow_foreign_fields'] or self.collection.hasField(field) :
            return self.store.get(field)

        try :
            return self.store[field]
        except KeyError :
            raise SchemaViolation(self.collection.__class__, field)

    def __setitem__(self, field, value) :
        """Set an element in the store"""
        if (not self.collection._validation['allow_foreign_fields']) and (field not in self.validators) and (field not in self.collection.arangoPrivates):
            raise SchemaViolation(self.collection.__class__, field)
        
        if field in self.collection.arangoPrivates :
            raise ValueError("DocumentStore cannot contain private field (got %s)" % field)

        if isinstance(value, dict) :
            if field in self.validators :
                vals = self.validators[field]
            else :
                vals = {}
            self.store[field] = DocumentStore(self.collection, validators = vals, initDct = value, patch = self.patching, subStore=True)
            
            self.subStores[field] = self.store[field]
        else :
            self.store[field] = value

        if self.patching :
            self.patchStore[field] = self.store[field]

        if self.collection._validation['on_set'] :
            self.validateField(field)

    def __delitem__(self, k) :
        """removes an element from the store"""
        try :
            del(self.store[k])
        except :
            pass
    
        try :
            del(self.patchStore[k])
        except :
            pass

    def __contains__(self, k) :
        """returns true or false weither the store has a key k"""
        return (k in self.store) or (k in self.validators)

    def __repr__(self) :
        return "<store: %s>" % repr(self.store)

class Document(object) :
    """The class that represents a document. Documents are meant to be instanciated by collections"""

    def __init__(self, collection, jsonFieldInit = {}) :
        self.typeName = "ArangoDoc"
        self.reset(collection, jsonFieldInit)

    def reset(self, collection, jsonFieldInit = {}) :
        """replaces the current values in the document by those in jsonFieldInit"""
        self.collection = collection
        self.connection = self.collection.connection
        self.documentsURL = self.collection.documentsURL

        self.setPrivates(jsonFieldInit)
        self._store = DocumentStore(self.collection, validators=self.collection._fields, initDct=jsonFieldInit)
        if self.collection._validation['on_load']:
            self.validate()

        self.modified = True

    def validate(self) :
        """validate the document"""
        # print self._store
        self._store.validate()
        for pField in self.collection.arangoPrivates :
            self.collection.validatePrivate(pField, getattr(self, pField))

    def setPrivates(self, fieldDict) :
        """will set self._id, self._rev and self._key field."""
        
        try :
            for priv in ["_id", "_key", "_rev"] :
                setattr(self, priv, fieldDict[priv])
            self.URL = "%s/%s" % (self.documentsURL, self._id)
        except KeyError :
            for priv in ["_id", "_key", "_rev"] :
                setattr(self, priv, None)
            self.URL = None

    def set(self, fieldDict) :
        """set the document with a dictionary"""
        self._store.set(fieldDict)

    def save(self, waitForSync = False, **docArgs) :
        """Saves the document to the database by either performing a POST (for a new document) or a PUT (complete document overwrite).
        If you want to only update the modified fields use the .path() function.
        Use docArgs to put things such as 'waitForSync = True' (for a full list cf ArangoDB's doc).
        It will only trigger a saving of the document if it has been modified since the last save. If you want to force the saving you can use forceSave()"""
        payload = self._store.getStore()
        self._save(payload, waitForSync = False, **docArgs)

    def _save(self, payload, waitForSync = False, **docArgs) :

        if self.modified :

            params = dict(docArgs)
            params.update({'collection': self.collection.name, "waitForSync" : waitForSync })

            if self.collection._validation['on_save'] :
                self.validate()
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

            if (r.status_code == 201 or r.status_code == 202) and "error" not in data :
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

        self._store.resetPatch()

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

        if self.URL is None :
            raise ValueError("Cannot patch a document that was not previously saved")

        payload = self._store.getPatches()
        
        if self.collection._validation['on_save'] :
            self.validate()

        if len(payload) > 0 :
            params = dict(docArgs)
            params.update({'collection': self.collection.name, 'keepNull' : keepNull})
            payload = json.dumps(payload)

            r = self.connection.session.patch(self.URL, params = params, data = payload)
            data = r.json()
            if (r.status_code == 201 or r.status_code == 202) and "error" not in data :
                self._rev = data['_rev']
            else :
                raise UpdateError(data['errorMessage'], data)

            self.modified = False

        self._store.resetPatch()

    def delete(self) :
        "deletes the document from the database"
        if self.URL is None :
            raise DeletionError("Can't delete a document that was not saved")
        r = self.connection.session.delete(self.URL)
        data = r.json()

        if (r.status_code != 200 and r.status_code != 202) or 'error' in data :
            raise DeletionError(data['errorMessage'], data)
        self.reset(self.collection)

        self.modified = True

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
        """get an element from the document"""
        if k in self.collection.arangoPrivates :
            return getattr(self, k)
        return self._store[k]

    def __setitem__(self, k, v) :
        """set an element in the document"""
        if k in self.collection.arangoPrivates :
            setattr(self, k, v)
        else :
            self._store[k] = v

    def __delitem__(self, k) :
        """removes an element from the document"""
        del(self._store[k])
    
    def __str__(self) :
        return repr(self)

    def __repr__(self) :
        privStr = []
        for p in self.collection.arangoPrivates :
            privStr.append("%s: %s" % (p, getattr(self, p)) )

        privStr = ', '.join(privStr)
        return "%s '%s': %s" % (self.typeName, privStr, repr(self._store))

class Edge(Document) :
    """An Edge document"""
    def __init__(self, edgeCollection, jsonFieldInit = {}) :
        self.typeName = "ArangoEdge"
        self.reset(edgeCollection, jsonFieldInit)

    def reset(self, edgeCollection, jsonFieldInit = {}) :
        Document.reset(self, edgeCollection, jsonFieldInit)

    def setPrivates(self, fieldDict) :
        """set _id, _key, _rev, _from, _to"""
        super(Edge, self).setPrivates(fieldDict)
        if "_from" in fieldDict :
            self._from = fieldDict["_from"]
        
        if "_to" in fieldDict :
            self._to = fieldDict["_to"]

    def links(self, fromVertice, toVertice, **edgeArgs) :
        """
        An alias to save that updates the _from and _to attributes.
        fromVertice and toVertice, can be either strings or documents. It they are unsaved documents, they will be automatically saved.
        """

        if fromVertice.__class__ is Document :
            if not fromVertice._id :
                fromVertice._id.save()

            self._from = fromVertice._id
        elif (type(fromVertice) is bytes) or (type(fromVertice) is str) :
            self._from  = fromVertice

        if toVertice.__class__ is Document :
            if not toVertice._id :
                toVertice._id.save()

            self._to = toVertice._id
        elif (type(toVertice) is bytes) or (type(toVertice) is str) :
            self._to = toVertice

        self.save(**edgeArgs)

    def save(self, **edgeArgs) :
        """Works like Document's except that you must specify '_from' and '_to' vertices before.
        There's also a links() function especially for first saves."""

        if not getattr(self, "_from") or not getattr(self, "_to") :
            raise AttributeError("You must specify '_from' and '_to' attributes before saving. You can also use the function 'links()'")

        payload = self._store.getStore()
        payload["_from"] = self._from
        payload["_to"] = self._to
        Document._save(self, payload, **edgeArgs)

    def __getattr__(self, k) :
        if k == "_from" or k == "_to" :
            return self._store[k]
        else :
            return Document.__getattr__(self, k)