import json
import types
from future.utils import with_metaclass
from enum import Enum
from . import consts as CONST

from .document import Document, Edge

from .theExceptions import ValidationError, SchemaViolation, CreationError, UpdateError, DeletionError, InvalidDocument, ExportError, DocumentNotFoundError, ArangoError, BulkOperationError, IndexError

from .query import SimpleQuery
from .index import Index

__all__ = ["Collection", "Edges", "Field", "DocumentCache", "CachedDoc", "Collection_metaclass", "getCollectionClass", "isCollection", "isDocumentCollection", "isEdgeCollection", "getCollectionClasses"]

class BulkMode(Enum):
    NONE = 0
    INSERT = 1
    UPDATE = 2
    DELETE = 3

class CachedDoc(object):
    """A cached document."""
    def __init__(self, document, prev, nextDoc):
        self.prev = prev
        self.document = document
        self.nextDoc = nextDoc
        self._key = document._key

    def __getitem__(self, k):
        return self.document[k]

    def __setitem__(self, k, v):
        self.document[k] = v

    def __getattribute__(self, k):
        try:
            return object.__getattribute__(self, k)
        except Exception as e1:
            try:
                return getattr(self.document, k)
            except Exception as e2:
                raise e2

class DocumentCache(object):
    """Document cache for collection, with insert, deletes and updates in O(1)."""

    def __init__(self, cacheSize):
        self.cacheSize = cacheSize
        self.cacheStore = {}
        self.head = None
        self.tail = None

    def cache(self, doc):
        if doc._key in self.cacheStore:
            ret = self.cacheStore[doc._key]
            if ret.prev is not None:
                ret.prev.nextDoc = ret.nextDoc
                self.head.prev = ret
                ret.nextDoc = self.head
                self.head = ret
            return self.head
        else:
            if len(self.cacheStore) == 0:
                ret = CachedDoc(doc, prev = None, nextDoc = None)
                self.head = ret
                self.tail = self.head
                self.cacheStore[doc._key] = ret
            else:
                if len(self.cacheStore) >= self.cacheSize:
                    del(self.cacheStore[self.tail._key])
                    self.tail = self.tail.prev
                    self.tail.nextDoc = None

                ret = CachedDoc(doc, prev = None, nextDoc = self.head)
                self.head.prev = ret
                self.head = self.head.prev
                self.cacheStore[doc._key] = ret

    def delete(self, _key):
        """Remove a document from the cache."""
        try:
            doc = self.cacheStore[_key]
            doc.prev.nextDoc = doc.nextDoc
            doc.nextDoc.prev = doc.prev
            del(self.cacheStore[_key])
        except KeyError:
            raise KeyError("Document with _key %s is not available in cache" % _key)

    def getChain(self):
        """Return a list of keys representing the chain of documents."""
        l = []
        h = self.head
        while h:
            l.append(h._key)
            h = h.nextDoc
        return l

    def stringify(self) -> str:
        """Return a pretty string of 'getChain()'."""
        l = []
        h = self.head
        while h:
            l.append(str(h._key))
            h = h.nextDoc
        return "<->".join(l)

    def __getitem__(self, _key):
        try:
            ret = self.cacheStore[_key]
            self.cache(ret)
            return ret
        except KeyError:
            raise KeyError("Document with _key %s is not available in cache" % _key)

    def __repr__(self):
        return "[DocumentCache, size: %d, full: %d]" %(self.cacheSize, len(self.cacheStore))

class Field(object):
    """The class for defining pyArango fields."""
    def __init__(self, validators = None, default = None):
        """Validators must be a list of validators.

        'default' can also be a callable."""
        if not validators:
            validators = []
        self.validators = validators
        self.default = default

    def validate(self, value):
        """Check the validity of 'value' given the list of validators."""
        for v in self.validators:
            v.validate(value)
        return True

    def __str__(self):
        strv = []
        for v in self.validators:
            strv.append(str(v))
        return "<Field, validators: '%s'>" % ', '.join(strv)

class Collection_metaclass(type):
    """The metaclass that takes care of keeping a register of all collection types."""
    collectionClasses = {}

    _validationDefault = {
            'on_save' : False,
            'on_set' : False,
            'on_load' : False,
            'allow_foreign_fields' : True
        }

    def __new__(cls, name, bases, attrs):
        def check_set_ConfigDict(dictName):
            defaultDict = getattr(cls, "%sDefault" % dictName)

            if dictName not in attrs:
                attrs[dictName] = defaultDict
            else:
                for k, v in attrs[dictName].items():
                    if k not in defaultDict:
                        raise KeyError("Unknown validation parameter '%s' for class '%s'"  %(k, name))
                    if type(v) is not type(defaultDict[k]):
                        raise ValueError("'%s' parameter '%s' for class '%s' is of type '%s' instead of '%s'"  %(dictName, k, name, type(v), type(defaultDict[k])))

                for k, v in defaultDict.items():
                    if k not in attrs[dictName]:
                        attrs[dictName][k] = v

        check_set_ConfigDict('_validation')
        clsObj = type.__new__(cls, name, bases, attrs)
        Collection_metaclass.collectionClasses[name] = clsObj

        return clsObj

    @classmethod
    def getCollectionClass(cls, name):
        """Return the class object of a collection given its 'name'."""
        try:
            return cls.collectionClasses[name]
        except KeyError:
            raise KeyError( "There is no Collection Class of type: '%s'; currently supported values: [%s]" % (name, ', '.join(getCollectionClasses().keys())) )

    @classmethod
    def isCollection(cls, name) -> bool:
        """return 'True' or 'False' whether 'name' is the name of collection."""
        return name in cls.collectionClasses

    @classmethod
    def isDocumentCollection(cls, name) -> bool:
        """Return 'True' or 'False' whether 'name' is the name of a document collection."""
        try:
            col = cls.getCollectionClass(name)
            return issubclass(col, Collection)
        except KeyError:
            return False

    @classmethod
    def isEdgeCollection(cls, name) -> bool:
        """Return 'True' or 'False' whether 'name' is the name of an edge collection."""
        try:
            col = cls.getCollectionClass(name)
            return issubclass(col, Edges)
        except KeyError:
            return False

def getCollectionClass(name) -> bool:
    """Return 'True' or 'False' whether 'name' is the name of collection."""
    return Collection_metaclass.getCollectionClass(name)

def isCollection(name) -> bool:
    """Return 'True' or 'False' whether 'name' is the name of a document collection."""
    return Collection_metaclass.isCollection(name)

def isDocumentCollection(name) -> bool:
    """Return 'True' or 'False' whether 'name' is the name of a document collection."""
    return Collection_metaclass.isDocumentCollection(name)

def isEdgeCollection(name) -> bool:
    """Return 'True' or 'False' whether 'name' is the name of an edge collection."""
    return Collection_metaclass.isEdgeCollection(name)

def getCollectionClasses() -> bool:
    """Return a dictionary of all defined collection classes."""
    return Collection_metaclass.collectionClasses

class Collection(with_metaclass(Collection_metaclass, object)):
    """A document collection. Collections are meant to be instantiated by databases."""
    # here you specify the fields that you want for the documents in your collection
    _fields = {}

    _validation = {
        'on_save' : False,
        'on_set' : False,
        'on_load' : False,
        'allow_foreign_fields' : True
    }

    arangoPrivates = ["_id", "_key", "_rev"]

    def __init__(self, database, jsonData):

        self.database = database
        self.connection = self.database.connection
        self.name = self.__class__.__name__
        for k in jsonData:
            setattr(self, k, jsonData[k])

        self.documentCache = None

        self.documentClass = Document
        self.indexes = {
            "primary" : {},
            "hash" : {},
            "skiplist" : {},
            "persistent": {},
            "ttl": {},
            "geo" : {},
            "fulltext" : {},
        }
        self.indexes_by_name = {}
        # self.defaultDocument = None #getDefaultDoc(self._fields, {})
        self._isBulkInProgress = False
        self._bulkSize = 0
        self._bulkCache = []
        self._bulkMode = BulkMode.NONE

    def getDefaultDocument(self, fields=None, dct=None):
        if dct is None:
            dct = {}
        if fields is None:
            fields = self._fields

        for k, v in fields.items():
            if isinstance(v, dict):
                dct[k] = self.getDefaultDocument(fields[k], None)
            elif isinstance(v, list) or isinstance(v, tuple):
                dct[k] = []

            elif isinstance(v, Field):
                if callable(v.default):
                    dct[k] = v.default()
                else :
                    dct[k] = v.default
            else:
                raise ValueError("Field '%s' is of invalid type '%s'" % (k, type(v)) )
        return dct

    def getURL(self):
        return "%s/collection/%s" % (self.database.getURL(), self.name)

    def getDocumentsURL(self):
        return "%s/document" % (self.database.getURL())

    def getIndexes(self):
        """Fill 'self.indexes' with all the indexes associated with the collection and return it."""
        self.indexes_by_name = {}
        url = "%s/index" % self.database.getURL()
        r = self.connection.session.get(url, params = {"collection": self.name})
        data = r.json()
        for ind in data["indexes"]:
            index = Index(collection = self, infos = ind)
            self.indexes[ind["type"]][ind["id"]] = index
            if "name" in ind:
                self.indexes_by_name[ind["name"]] = index

        return self.indexes

    def getIndex(self, name):
        if len(self.indexes_by_name) == 0:
            raise IndexError("named indices unsupported")
        return self.indexes_by_name[name]

    def activateCache(self, cacheSize):
        """Activate the caching system.

        Cached documents are only available through the __getitem__ interface."""
        self.documentCache = DocumentCache(cacheSize)

    def deactivateCache(self):
        """Deactivate the caching system."""
        self.documentCache = None

    def delete(self):
        """Delete the collection from the database."""
        r = self.connection.session.delete(self.getURL())
        data = r.json()
        if not r.status_code == 200 or data["error"]:
            raise DeletionError(data["errorMessage"], data)

    def createDocument(self, initDict = None):
        """Create and return a completely empty document unless the initial document is set via 'initDict'."""
        # res = dict(self.defaultDocument)
        res = self.getDefaultDocument()

        if initDict is not None:
            res.update(initDict)

        return self.documentClass(self, res)

    def _writeBatch(self):
        if not self._bulkCache:
            return
        if self._bulkMode != BulkMode.INSERT:
            raise UpdateError("Mixed bulk operations not supported - have " + str(self._bulkMode))
        payload = []
        for d in self._bulkCache:
            if isinstance(d,dict):
                payload.append(json.dumps(d, default=str))
            else:
                try:
                    payload.append(d.toJson())
                except Exception as e:
                    payload.append(json.dumps(d.getStore(), default=str))

        payload = '[' + ','.join(payload) + ']'
        r = self.connection.session.post(self.getDocumentsURL(), params = self._batchParams, data = payload)
        data = r.json()
        if (not isinstance(data, list)):
            raise UpdateError("expected reply to be a json array" + r)
        i = 0
        bulkError = None
        for xd in data:
            if not '_key' in xd and 'error' in xd and 'errorNum' in xd:
                if bulkError is None:
                    bulkError = BulkOperationError("saving failed")
                bulkError.addBulkError(ArangoError(xd), self._bulkCache[i])
            else:
                self._bulkCache[i].setPrivates(xd)
                self._bulkCache[i]._key = \
                    xd['_key']
            i += 1
        if bulkError is not None:
            self._bulkCache = []
            raise bulkError

        self._bulkCache = []

    def _saveBatch(self, document, params):
        if self._bulkMode != BulkMode.NONE and self._bulkMode != BulkMode.INSERT:
            raise UpdateError("Mixed bulk operations not supported - have " + str(self._bulkMode))
        self._bulkMode = BulkMode.INSERT
        self._bulkCache.append(document)
        self._batchParams = params
        if len(self._bulkCache) == self._bulkSize:
            self._writeBatch()
            self._bulkMode = BulkMode.NONE

    def _updateBatch(self):
        if not self._bulkCache:
            return
        if self._bulkMode != BulkMode.UPDATE:
            raise UpdateError("Mixed bulk operations not supported - have " + str(self._bulkMode))
        payload = []
        for d in self._bulkCache:
            dPayload = d._store.getPatches()

            if d.collection._validation['on_save']:
                d.validate()

            if isinstance(d,dict):
                payload.append(json.dumps(d, default=str))
            else:
                try:
                    payload.append(d.toJson())
                except Exception as e:
                    payload.append(json.dumps(d.getStore(), default=str))
        payload = '[' + ','.join(payload) + ']'
        r = self.connection.session.patch(self.getDocumentsURL(), params = self._batchParams, data = payload)
        data = r.json()
        if (not isinstance(data, list)):
            raise UpdateError("expected reply to be a json array" + dir(r))
        i = 0
        bulkError = None
        for xd in data:
            if not '_key' in xd and 'error' in xd and 'errorNum' in xd:
                if bulkError is None:
                    bulkError = BulkOperationError("patching failed")
                bulkError.addBulkError(ArangoError(xd), str(self._bulkCache[i]))
            else:
                self._bulkCache[i].setPrivates(xd)
                self._bulkCache[i]._key = \
                    xd['_key']
            i += 1
        self._bulkCache = []
        if bulkError is not None:
            raise bulkError


    def _patchBatch(self, document, params):
        if self._bulkMode != BulkMode.NONE and self._bulkMode != BulkMode.UPDATE:
            raise UpdateError("Mixed bulk operations not supported - have " + str(self._bulkMode))
        self._bulkMode = BulkMode.UPDATE
        self._bulkCache.append(document)
        self._batchParams = params
        if len(self._bulkCache) == self._bulkSize:
            self._updateBatch()
            self._bulkMode = BulkMode.NONE

    def _removeBatch(self):
        if not self._bulkCache:
            return
        if self._bulkMode != BulkMode.DELETE:
            raise UpdateError("Mixed bulk operations not supported - have " + self._bulkMode)
        payload = []
        for d in self._bulkCache:
            if isinstance(d,dict):
                payload.append('"%s"' % d['_key'])
            else:
                try:
                    payload.append('"%s"' % d['_key'])
                except Exception as e:
                    payload.append('"%s"' % d['_key'])

        payload = '[' + ','.join(payload) + ']'
        r = self.connection.session.delete(self.getDocumentsURL() + "/" + self.name, params = self._batchParams, data = payload)
        data = r.json()
        if (not isinstance(data, list)):
            raise UpdateError("expected reply to be a json array" + r)
        i = 0
        bulkError = None
        for xd in data:
            if not '_key' in xd and 'error' in xd and 'errorNum' in xd:
                if bulkError is None:
                    bulkError = BulkOperationError("deleting failed")
                bulkError.addBulkError(ArangoError(xd), self._bulkCache[i])
            else:
                self._bulkCache[i].reset(self)
            i += 1
        self._bulkCache = []
        if bulkError is not None:
            raise bulkError

    def _deleteBatch(self, document, params):
        if self._bulkMode != BulkMode.NONE and self._bulkMode != BulkMode.DELETE:
            raise UpdateError("Mixed bulk operations not supported - have " + str(self._bulkMode))
        self._bulkMode = BulkMode.DELETE
        self._bulkCache.append(document)
        self._batchParams = params
        if len(self._bulkCache) == self._bulkSize:
            self._removeBatch()
            self._bulkMode = BulkMode.NONE


    def _finalizeBatch(self):
        if self._bulkMode == BulkMode.INSERT:
            self._writeBatch()
        elif self._bulkMode == BulkMode.UPDATE:
            self._updateBatch()
        elif self._bulkMode == BulkMode.DELETE:
            self._removeBatch()
        # elif self._bulkMode == BulkMode.NONE:
        self._bulkSize = 0
        self._isBulkInProgress = False
        self._batchParams = None
        self._bulkMode = BulkMode.NONE

    def importBulk(self, data, **addParams):
        url = "%s/import" % (self.database.getURL())
        payload = json.dumps(data, default=str)
        params = {"collection": self.name, "type": "auto"}
        params.update(addParams)
        r = self.connection.session.post(url , params = params, data = payload)
        data = r.json()
        if not r.status_code == 201 or data["error"]:
            raise CreationError(data["errorMessage"], data)
        return data

    def exportDocs( self, **data):
        url = "%s/export" % (self.database.getURL())
        params = {"collection": self.name}
        payload = json.dumps(data)
        r = self.connection.session.post(url, params = params, data = payload)
        data = r.json()
        if not r.status_code == 201 or data["error"]:
          raise ExportError( data["errorMessage"], data )
        docs = data['result']
        return docs

    def ensureHashIndex(self, fields, unique = False, sparse = True, deduplicate = False, name = None):
        """Create a hash index if it does not already exist, then return it."""
        data = {
            "type" : "hash",
            "fields" : fields,
            "unique" : unique,
            "sparse" : sparse,
            "deduplicate": deduplicate
        }
        if name:
            data["name"] = name
        ind = Index(self, creationData = data)
        self.indexes["hash"][ind.infos["id"]] = ind
        if name:
            self.indexes_by_name[name] = ind
        return ind

    def ensureSkiplistIndex(self, fields, unique = False, sparse = True, deduplicate = False, name = None):
        """Create a skiplist index if it does not already exist, then return it."""
        data = {
            "type" : "skiplist",
            "fields" : fields,
            "unique" : unique,
            "sparse" : sparse,
            "deduplicate": deduplicate
        }
        if name:
            data["name"] = name
        ind = Index(self, creationData = data)
        self.indexes["skiplist"][ind.infos["id"]] = ind
        if name:
            self.indexes_by_name[name] = ind
        return ind

    def ensurePersistentIndex(self, fields, unique = False, sparse = True, deduplicate = False, name = None):
        """Create a persistent index if it does not already exist, then return it."""
        data = {
            "type" : "persistent",
            "fields" : fields,
            "unique" : unique,
            "sparse" : sparse,
            "deduplicate": deduplicate
        }
        if name:
            data["name"] = name
        ind = Index(self, creationData = data)
        self.indexes["skiplist"][ind.infos["id"]] = ind
        if name:
            self.indexes_by_name[name] = ind
        return ind

    def ensureTTLIndex(self, fields, expireAfter, unique = False, sparse = True, name = None):
        """Create a TTL index if it does not already exist, then return it."""
        data = {
            "type" : "ttl",
            "fields" : fields,
            "unique" : unique,
            "sparse" : sparse,
            "expireAfter" : expireAfter
        }
        if name:
            data["name"] = name
        ind = Index(self, creationData = data)
        self.indexes["skiplist"][ind.infos["id"]] = ind
        if name:
            self.indexes_by_name[name] = ind
        return ind

    def ensureGeoIndex(self, fields, name = None):
        """Create a geo index if it does not already exist, then return it."""
        data = {
            "type" : "geo",
            "fields" : fields,
        }
        if name:
            data["name"] = name
        ind = Index(self, creationData = data)
        self.indexes["geo"][ind.infos["id"]] = ind
        if name:
            self.indexes_by_name[name] = ind
        return ind

    def ensureFulltextIndex(self, fields, minLength = None, name = None):
        """Create a fulltext index if it does not already exist, then return it."""
        data = {
            "type" : "fulltext",
            "fields" : fields,
        }
        if name:
            data["name"] = name
        if minLength is not None:
            data["minLength"] = minLength

        ind = Index(self, creationData = data)
        self.indexes["fulltext"][ind.infos["id"]] = ind
        if name:
            self.indexes_by_name[name] = ind
        return ind

    def ensureIndex(self, index_type, fields, name=None, **index_args):
        """Create an index of any type."""
        data = {
            "type" : index_type,
            "fields" : fields,
        }
        data.update(index_args)

        if name:
            data["name"] = name

        ind = Index(self, creationData = data)
        self.indexes[index_type][ind.infos["id"]] = ind
        if name:
            self.indexes_by_name[name] = ind
        return ind

    def restoreIndexes(self, indexes_dct=None):
        """Restore all previously removed indexes."""
        if indexes_dct is None:
            indexes_dct = self.indexes

        for typ in indexes_dct.keys():
            if typ != "primary":
                for name, idx in indexes_dct[typ].items():
                    infos = dict(idx.infos)
                    del infos["fields"]
                    self.ensureIndex(typ, idx.infos["fields"], **infos)

    def validatePrivate(self, field, value):
        """Validate a private field value."""
        if field not in self.arangoPrivates:
            raise ValueError("%s is not a private field of collection %s" % (field, self))

        if field in self._fields:
            self._fields[field].validate(value)
        return True

    @classmethod
    def hasField(cls, fieldName):
        """Return 'True' or 'False' whether the collection has field 'K' in its schema.

        Use the dot notation for the nested fields: address.street"""
        path = fieldName.split(".")
        v = cls._fields
        for k in path:
            try:
                v = v[k]
            except KeyError:
                return False
        return True

    def fetchDocument(self, key, rawResults = False, rev = None):
        """Fetche a document from the collection given its key.

        This function always goes straight to the db and bypasses the cache.
        If you want to take advantage of the cache use the '__getitem__' interface: collection[key]"""
        url = "%s/%s/%s" % (self.getDocumentsURL(), self.name, key)
        if rev is not None:
            r = self.connection.session.get(url, params = {'rev' : rev})
        else:
            r = self.connection.session.get(url)

        if r.status_code < 400:
            if rawResults:
                return r.json()
            return self.documentClass(self, r.json(), on_load_validation=self._validation["on_load"])
        elif r.status_code == 404 :
            raise DocumentNotFoundError("Unable to find document with _key: %s" % key, r.json())
        raise DocumentNotFoundError("Unable to find document with _key: %s, response: %s" % (key, r.json()), r.json())

    def fetchByExample(self, exampleDict, batchSize, rawResults = False, **queryArgs):
        """'exampleDict' should be something like {'age' : 28}."""
        return self.simpleQuery('by-example', rawResults, example = exampleDict, batchSize = batchSize, **queryArgs)

    def fetchFirstExample(self, exampleDict, rawResults = False):
        """'exampleDict' should be something like {'age' : 28}.

        Return the first example found that matches the example, still in a 'SimpleQuery' object."""
        return self.simpleQuery('first-example', rawResults = rawResults, example = exampleDict)

    def fetchAll(self, rawResults = False, **queryArgs):
        """Returns all the documents in the collection.
        You can use the optinal arguments 'skip' and 'limit'::
            fetchAlll(limit = 3, shik = 10)"""

        return self.simpleQuery('all', rawResults = rawResults, **queryArgs)

    def simpleQuery(self, queryType, rawResults = False, **queryArgs):
        """General interface for simple queries.

        'queryType' takes the arguments known to the ArangoDB, for instance: 'all' or 'by-example'.
        See the ArangoDB documentation for a list of valid 'queryType's.
        If 'rawResults' is set to 'True', the query will return dictionaries instead of 'Document' objetcs."""
        return SimpleQuery(self, queryType, rawResults, **queryArgs)

    def action(self, method, action, **params):
        """A generic 'fct' for interacting everything that does not have an assigned 'fct'."""
        fct = getattr(self.connection.session, method.lower())
        r = fct(self.getURL() + "/" + action, params = params)
        return r.json()

    def bulkSave(self, docs, onDuplicate="error", **params):
        """Parameter 'docs' must be either an iterable of documents or dictionaries.

        This function will return the number of documents, created and updated, and will raise an UpdateError exception if there is at least one error.
        'params' are any parameters from the ArangoDB documentation."""

        payload = []
        for d in docs:
            if isinstance(d,dict):
                payload.append(json.dumps(d, default=str))
            else:
                try:
                    payload.append(d.toJson())
                except Exception as e:
                    payload.append(json.dumps(d.getStore(), default=str))

        payload = '\n'.join(payload)

        params["type"] = "documents"
        params["onDuplicate"] = onDuplicate
        params["collection"] = self.name
        url = "%s/import" % self.database.getURL()

        r = self.connection.session.post(url, params = params, data = payload)
        data = r.json()
        if (r.status_code == 201) and "error" not in data:
            return True
        if "errors" in data and data["errors"] > 0:
            raise UpdateError("%d documents could not be created" % data["errors"], data)
        elif data["error"]:
            raise UpdateError("Documents could not be created", data)

        return data["updated"] + data["created"]

    def bulkImport_json(self, filename, onDuplicate="error", formatType="auto", **params):
        """Bulk import from a file following the ArangoDB key-value format."""

        url = "%s/import" % self.database.getURL()
        params["onDuplicate"] = onDuplicate
        params["collection"] = self.name
        params["type"] = formatType
        with open(filename) as f:
            data = f.read()
            r = self.connection.session.post(url, params = params, data = data)

            if r.status_code != 201:
                raise UpdateError('Unable to bulk import JSON', r)

    def bulkImport_values(self, filename, onDuplicate="error", **params):
        """Bulk import from a file following the ArangoDB json format."""

        url = "%s/import" % self.database.getURL()
        params["onDuplicate"] = onDuplicate
        params["collection"] = self.name
        with open(filename) as f:
            data = f.read()
            r = self.connection.session.post(url, params = params, data = data)

            if r.status_code != 201:
                raise UpdateError('Unable to bulk import values', r)

    def truncate(self):
        """Delete every document in the collection."""
        return self.action('PUT', 'truncate')

    def empty(self):
        """Alias for truncate."""
        return self.truncate()

    def load(self):
        """Load collection in memory."""
        return self.action('PUT', 'load')

    def unload(self):
        """Unload collection from memory."""
        return self.action('PUT', 'unload')

    def revision(self):
        """Return the current revision."""
        return self.action('GET', 'revision')["revision"]

    def properties(self):
        """Return the current properties."""
        return self.action('GET', 'properties')

    def checksum(self):
        """Return the current checksum."""
        return self.action('GET', 'checksum')["checksum"]

    def count(self):
        """Return the number of documents in the collection."""
        return self.action('GET', 'count')["count"]

    def figures(self):
        """A more elaborate version of 'count', see the ArangoDB documentation for more."""
        return self.action('GET', 'figures')

    def getType(self):
        """Return a word describing the type of the collection (edges or ducments) instead of a number.

        If you prefer the number it is in 'self.type'."""
        if self.type == CONST.COLLECTION_DOCUMENT_TYPE:
            return "document"
        elif self.type == CONST.COLLECTION_EDGE_TYPE:
            return "edge"
        raise ValueError("The collection is of Unknown type %s" % self.type)

    def getStatus(self):
        """Return a word describing the status of the collection (loaded, loading, deleted, unloaded, newborn) instead of a number, if you prefer the number it is in 'self.status'."""
        if self.status == CONST.COLLECTION_LOADING_STATUS:
            return "loading"
        elif self.status == CONST.COLLECTION_LOADED_STATUS:
            return "loaded"
        elif self.status == CONST.COLLECTION_DELETED_STATUS:
            return "deleted"
        elif self.status == CONST.COLLECTION_UNLOADED_STATUS:
            return "unloaded"
        elif self.status == CONST.COLLECTION_NEWBORN_STATUS:
            return "newborn"
        raise ValueError("The collection has an Unknown status %s" % self.status)

    def __len__(self):
        """Return the number of documents in the collection."""
        return self.count()

    def __repr__(self):
        return "ArangoDB collection name: %s, id: %s, type: %s, status: %s" % (self.name, self.id, self.getType(), self.getStatus())

    def __getitem__(self, key):
        """Return a document from the cache.

        If it is not there, fetch from the db and cache it first.
        If the cache is not activated, this is equivalent to 'fetchDocument(rawResults=False)'."""
        if self.documentCache is None:
            return self.fetchDocument(key, rawResults = False)
        try:
            return self.documentCache[key]
        except KeyError:
            doc = self.fetchDocument(key, rawResults = False)
            self.documentCache.cache(doc)
        return doc

    def __contains__(self, key):
        """Return 'True' or 'False' whether the doc is in the collection."""
        try:
            self.fetchDocument(key, rawResults = False)
            return True
        except DocumentNotFoundError as e:
            return False

class SystemCollection(Collection):
    """For all collections with 'isSystem=True'."""
    def __init__(self, database, jsonData):
        Collection.__init__(self, database, jsonData)

class Edges(Collection):
    """The default edge collection. All edge Collections must inherit from it."""

    arangoPrivates = ["_id", "_key", "_rev", "_to", "_from"]

    def __init__(self, database, jsonData):
        """This one is meant to be called by the database."""
        Collection.__init__(self, database, jsonData)
        self.documentClass = Edge
        self.edgesURL = "%s/edges/%s" % (self.database.getURL(), self.name)

    @classmethod
    def validateField(cls, fieldName, value):
        """Check if 'value' is valid for field 'fieldName'.

        If the validation fails, raise a 'SchemaViolation' or a 'ValidationError'.
        For nested dicts ex: {address : { street: xxx} }, 'fieldName' can take the form 'address.street'."""
        try:
            valValue = Collection.validateField(fieldName, value)
        except SchemaViolation as e:
            if fieldName == "_from" or fieldName == "_to":
                return True
            raise e
        return valValue

    def createEdge(self, initValues = None):
        """Create an edge populated with defaults."""
        return self.createDocument(initValues)

    def getInEdges(self, vertex, rawResults = False):
        """An alias for 'getEdges()' that returns only the in 'Edges'."""
        return self.getEdges(vertex, inEdges = True, outEdges = False, rawResults = rawResults)

    def getOutEdges(self, vertex, rawResults = False):
        """An alias for 'getEdges()' that returns only the out 'Edges'."""
        return self.getEdges(vertex, inEdges = False, outEdges = True, rawResults = rawResults)

    def getEdges(self, vertex, inEdges = True, outEdges = True, rawResults = False):
        """Return in, out, or both edges linked to a given document.

        Vertex can be either a 'Document' object or a string for an '_id'.
        If 'rawResults' is set to 'True', return the results just as fetched without any processing.
        Otherwise, return a list of Edge objects."""
        if isinstance(vertex, Document):
            vId = vertex._id
        elif isinstance(vertex,str) or isinstance(vertex,bytes):
            vId = vertex
        else:
            raise ValueError("Vertex is neither a Document nor a String")

        params = {"vertex" : vId}
        if inEdges and outEdges:
            pass
        elif inEdges:
            params["direction"] = "in"
        elif outEdges:
            params["direction"] = "out"
        else:
            raise ValueError("inEdges, outEdges or both must have a boolean value")

        r = self.connection.session.get(self.edgesURL, params = params)
        data = r.json()
        if r.status_code == 200:
            if not rawResults:
                ret = []
                for e in data["edges"]:
                    ret.append(Edge(self, e))
                return ret
            return data["edges"]
        else:
            raise CreationError("Unable to return edges for vertex: %s" % vId, data)


class BulkOperation(object):
    def __init__(self, collection, batchSize=100):
        self.coll = collection
        self.batchSize = batchSize

    def __enter__(self):
        self.coll._isBulkInProgress = True
        self.coll._bulkSize = self.batchSize
        return self.coll
    def __exit__(self, type, value, traceback):
        self.coll._finalizeBatch();
