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
    """A cached document"""
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
    "Document cache for collection, with insert, deletes and updates in O(1)"

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
        "removes a document from the cache"
        try:
            doc = self.cacheStore[_key]
            doc.prev.nextDoc = doc.nextDoc
            doc.nextDoc.prev = doc.prev
            del(self.cacheStore[_key])
        except KeyError:
            raise KeyError("Document with _key %s is not available in cache" % _key)

    def getChain(self):
        "returns a list of keys representing the chain of documents"
        l = []
        h = self.head
        while h:
            l.append(h._key)
            h = h.nextDoc
        return l

    def stringify(self):
        "a pretty str version of getChain()"
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
        """validators must be a list of validators. default can also be a callable"""
        if not validators:
            validators = []
        self.validators = validators
        self.default = default

    def validate(self, value):
        """checks the validity of 'value' given the lits of validators"""
        for v in self.validators:
            v.validate(value)
        return True

    def __str__(self):
        strv = []
        for v in self.validators:
            strv.append(str(v))
        return "<Field, validators: '%s'>" % ', '.join(strv)

class Collection_metaclass(type):
    """The metaclass that takes care of keeping a register of all collection types"""
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
        """Return the class object of a collection given its 'name'"""
        try:
            return cls.collectionClasses[name]
        except KeyError:
            raise KeyError( "There is no Collection Class of type: '%s'; currently supported values: [%s]" % (name, ', '.join(getCollectionClasses().keys())) )

    @classmethod
    def isCollection(cls, name):
        """return true or false wether 'name' is the name of collection."""
        return name in cls.collectionClasses

    @classmethod
    def isDocumentCollection(cls, name):
        """return true or false wether 'name' is the name of a document collection."""
        try:
            col = cls.getCollectionClass(name)
            return issubclass(col, Collection)
        except KeyError:
            return False

    @classmethod
    def isEdgeCollection(cls, name):
        """return true or false wether 'name' is the name of an edge collection."""
        try:
            col = cls.getCollectionClass(name)
            return issubclass(col, Edges)
        except KeyError:
            return False

def getCollectionClass(name):
    """return true or false wether 'name' is the name of collection."""
    return Collection_metaclass.getCollectionClass(name)

def isCollection(name):
    """return true or false wether 'name' is the name of a document collection."""
    return Collection_metaclass.isCollection(name)

def isDocumentCollection(name):
    """return true or false wether 'name' is the name of a document collection."""
    return Collection_metaclass.isDocumentCollection(name)

def isEdgeCollection(name):
    """return true or false wether 'name' is the name of an edge collection."""
    return Collection_metaclass.isEdgeCollection(name)

def getCollectionClasses():
    """returns a dictionary of all defined collection classes"""
    return Collection_metaclass.collectionClasses

class Collection(with_metaclass(Collection_metaclass, object)):
    """A document collection. Collections are meant to be instanciated by databases"""
    #here you specify the fields that you want for the documents in your collection
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
        """Fills self.indexes with all the indexes associates with the collection and returns it"""
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
        """Activate the caching system. Cached documents are only available through the __getitem__ interface"""
        self.documentCache = DocumentCache(cacheSize)

    def deactivateCache(self):
        "deactivate the caching system"
        self.documentCache = None

    def delete(self):
        """deletes the collection from the database"""
        r = self.connection.session.delete(self.getURL())
        data = r.json()
        if not r.status_code == 200 or data["error"]:
            raise DeletionError(data["errorMessage"], data)

    def createDocument(self, initDict = None):
        """create and returns a completely empty document or one populated with initDict"""
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
            if type(d) is dict:
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
                if bulkError == None:
                    bulkError = BulkOperationError("saving failed")
                bulkError.addBulkError(ArangoError(xd), self._bulkCache[i])
            else:
                self._bulkCache[i].setPrivates(xd)
                self._bulkCache[i]._key = \
                    xd['_key']
            i += 1
        if bulkError != None:
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

            if type(d) is dict:
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
                if bulkError == None:
                    bulkError = BulkOperationError("patching failed")
                bulkError.addBulkError(ArangoError(xd), str(self._bulkCache[i]))
            else:
                self._bulkCache[i].setPrivates(xd)
                self._bulkCache[i]._key = \
                    xd['_key']
            i += 1
        self._bulkCache = []
        if bulkError != None:
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
            if type(d) is dict:
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
                if bulkError == None:
                    bulkError = BulkOperationError("deleting failed")
                bulkError.addBulkError(ArangoError(xd), self._bulkCache[i])
            else:
                self._bulkCache[i].reset(self)
            i += 1
        self._bulkCache = []
        if bulkError != None:
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
        """Creates a hash index if it does not already exist, and returns it"""
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
        """Creates a skiplist index if it does not already exist, and returns it"""
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
        """Creates a persistent index if it does not already exist, and returns it"""
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
        """Creates a TTL index if it does not already exist, and returns it"""
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
        """Creates a geo index if it does not already exist, and returns it"""
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
        """Creates a fulltext index if it does not already exist, and returns it"""
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
        """Creates an index of any type."""
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
        """restores all previously removed indexes"""
        if indexes_dct is None:
            indexes_dct = self.indexes

        for typ in indexes_dct.keys():
            if typ != "primary":
                for name, idx in indexes_dct[typ].items():
                    infos = dict(idx.infos)
                    del infos["fields"]
                    self.ensureIndex(typ, idx.infos["fields"], **infos)

    def validatePrivate(self, field, value):
        """validate a private field value"""
        if field not in self.arangoPrivates:
            raise ValueError("%s is not a private field of collection %s" % (field, self))

        if field in self._fields:
            self._fields[field].validate(value)
        return True

    @classmethod
    def hasField(cls, fieldName):
        """returns True/False wether the collection has field K in it's schema. Use the dot notation for the nested fields: address.street"""
        path = fieldName.split(".")
        v = cls._fields
        for k in path:
            try:
                v = v[k]
            except KeyError:
                return False
        return True

    def fetchDocument(self, key, rawResults = False, rev = None):
        """Fetches a document from the collection given it's key. This function always goes straight to the db and bypasses the cache. If you
        want to take advantage of the cache use the __getitem__ interface: collection[key]"""
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
        else:
            raise DocumentNotFoundError("Unable to find document with _key: %s, response: %s" % (key, r.json()), r.json())

    def fetchByExample(self, exampleDict, batchSize, rawResults = False, **queryArgs):
        """exampleDict should be something like {'age' : 28}"""
        return self.simpleQuery('by-example', rawResults, example = exampleDict, batchSize = batchSize, **queryArgs)

    def fetchFirstExample(self, exampleDict, rawResults = False):
        """exampleDict should be something like {'age' : 28}. returns only a single element but still in a SimpleQuery object.
        returns the first example found that matches the example"""
        return self.simpleQuery('first-example', rawResults = rawResults, example = exampleDict)

    def fetchAll(self, rawResults = False, **queryArgs):
        """Returns all the documents in the collection. You can use the optinal arguments 'skip' and 'limit'::

            fetchAlll(limit = 3, shik = 10)"""
        return self.simpleQuery('all', rawResults = rawResults, **queryArgs)

    def simpleQuery(self, queryType, rawResults = False, **queryArgs):
        """General interface for simple queries. queryType can be something like 'all', 'by-example' etc... everything is in the arango doc.
        If rawResults, the query will return dictionaries instead of Document objetcs.
        """
        return SimpleQuery(self, queryType, rawResults, **queryArgs)

    def action(self, method, action, **params):
        """a generic fct for interacting everything that doesn't have an assigned fct"""
        fct = getattr(self.connection.session, method.lower())
        r = fct(self.getURL() + "/" + action, params = params)
        return r.json()

    def bulkSave(self, docs, onDuplicate="error", **params):
        """Parameter docs must be either an iterrable of documents or dictionnaries.
        This function will return the number of documents, created and updated, and will raise an UpdateError exception if there's at least one error.
        params are any parameters from arango's documentation"""

        payload = []
        for d in docs:
            if type(d) is dict:
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
        else:
            if "errors" in data and data["errors"] > 0:
                raise UpdateError("%d documents could not be created" % data["errors"], data)
            elif data["error"]:
                raise UpdateError("Documents could not be created", data)

        return data["updated"] + data["created"]

    def bulkImport_json(self, filename, onDuplicate="error", formatType="auto", **params):
        """bulk import from a file repecting arango's key/value format"""

        url = "%s/import" % self.database.getURL()
        params["onDuplicate"] = onDuplicate
        params["collection"] = self.name
        params["type"] = formatType
        with open(filename) as f:
            data = f.read()
            r = self.connection.session.post(url, params = params, data = data)

            try:
                errorMessage = "At least: %d errors. The first one is: '%s'\n\n more in <this_exception>.data" % (len(data), data[0]["errorMessage"])
            except KeyError:
                raise UpdateError(data['errorMessage'], data)

    def bulkImport_values(self, filename, onDuplicate="error", **params):
        """bulk import from a file repecting arango's json format"""
        
        url = "%s/import" % self.database.getURL()
        params["onDuplicate"] = onDuplicate
        params["collection"] = self.name
        with open(filename) as f:
            data = f.read()
            r = self.connection.session.post(url, params = params, data = data)

            try:
                errorMessage = "At least: %d errors. The first one is: '%s'\n\n more in <this_exception>.data" % (len(data), data[0]["errorMessage"])
            except KeyError:
                raise UpdateError(data['errorMessage'], data)

    def truncate(self):
        """deletes every document in the collection"""
        return self.action('PUT', 'truncate')

    def empty(self):
        """alias for truncate"""
        return self.truncate()

    def load(self):
        """loads collection in memory"""
        return self.action('PUT', 'load')

    def unload(self):
        """unloads collection from memory"""
        return self.action('PUT', 'unload')

    def revision(self):
        """returns the current revision"""
        return self.action('GET', 'revision')["revision"]

    def properties(self):
        """returns the current properties"""
        return self.action('GET', 'properties')

    def checksum(self):
        """returns the current checksum"""
        return self.action('GET', 'checksum')["checksum"]

    def count(self):
        """returns the number of documents in the collection"""
        return self.action('GET', 'count')["count"]

    def figures(self):
        "a more elaborate version of count, see arangodb docs for more infos"
        return self.action('GET', 'figures')

    def getType(self):
        """returns a word describing the type of the collection (edges or ducments) instead of a number, if you prefer the number it's in self.type"""
        if self.type == CONST.COLLECTION_DOCUMENT_TYPE:
            return "document"
        elif self.type == CONST.COLLECTION_EDGE_TYPE:
            return "edge"
        else:
            raise ValueError("The collection is of Unknown type %s" % self.type)

    def getStatus(self):
        """returns a word describing the status of the collection (loaded, loading, deleted, unloaded, newborn) instead of a number, if you prefer the number it's in self.status"""
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
        else:
            raise ValueError("The collection has an Unknown status %s" % self.status)

    def __len__(self):
        """returns the number of documents in the collection"""
        return self.count()

    def __repr__(self):
        return "ArangoDB collection name: %s, id: %s, type: %s, status: %s" % (self.name, self.id, self.getType(), self.getStatus())

    def __getitem__(self, key):
        """returns a document from the cache. If it's not there, fetches it from the db and caches it first. If the cache is not activated this is equivalent to fetchDocument( rawResults=False)"""
        if self.documentCache is None:
            return self.fetchDocument(key, rawResults = False)
        try:
            return self.documentCache[key]
        except KeyError:
            doc = self.fetchDocument(key, rawResults = False)
            self.documentCache.cache(doc)
        return doc

    def __contains__(self, key):
        """if doc in collection"""
        try:
            self.fetchDocument(key, rawResults = False)
            return True
        except DocumentNotFoundError as e:
            return False

class SystemCollection(Collection):
    """for all collections with isSystem = True"""
    def __init__(self, database, jsonData):
        Collection.__init__(self, database, jsonData)

class Edges(Collection):
    """The default edge collection. All edge Collections must inherit from it"""

    arangoPrivates = ["_id", "_key", "_rev", "_to", "_from"]

    def __init__(self, database, jsonData):
        """This one is meant to be called by the database"""
        Collection.__init__(self, database, jsonData)
        self.documentClass = Edge
        self.edgesURL = "%s/edges/%s" % (self.database.getURL(), self.name)

    @classmethod
    def validateField(cls, fieldName, value):
        """checks if 'value' is valid for field 'fieldName'. If the validation is unsuccessful, raises a SchemaViolation or a ValidationError.
        for nested dicts ex: {address : { street: xxx} }, fieldName can take the form address.street
        """
        try:
            valValue = Collection.validateField(fieldName, value)
        except SchemaViolation as e:
            if fieldName == "_from" or fieldName == "_to":
                return True
            else:
                raise e
        return valValue

    def createEdge(self, initValues = None):
        """Create an edge populated with defaults"""
        return self.createDocument(initValues)

    def getInEdges(self, vertex, rawResults = False):
        """An alias for getEdges() that returns only the in Edges"""
        return self.getEdges(vertex, inEdges = True, outEdges = False, rawResults = rawResults)

    def getOutEdges(self, vertex, rawResults = False):
        """An alias for getEdges() that returns only the out Edges"""
        return self.getEdges(vertex, inEdges = False, outEdges = True, rawResults = rawResults)

    def getEdges(self, vertex, inEdges = True, outEdges = True, rawResults = False):
        """returns in, out, or both edges liked to a given document. vertex can be either a Document object or a string for an _id.
        If rawResults a arango results will be return as fetched, if false, will return a liste of Edge objects"""
        if isinstance(vertex, Document):
            vId = vertex._id
        elif (type(vertex) is str) or (type(vertex) is bytes):
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
            else:
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

