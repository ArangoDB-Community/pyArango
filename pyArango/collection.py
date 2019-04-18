import json
import types
from future.utils import with_metaclass
from . import consts as CONST
from .document import Document, Edge
from .theExceptions import ValidationError, SchemaViolation, CreationError, UpdateError, DeletionError, InvalidDocument, ExportError, DocumentNotFoundError
from .query import SimpleQuery
from .index import Index


__all__ = [
        "Collection",
        "Edges",
        "Field",
        "DocumentCache",
        "CachedDoc",
        "CollectionMetaclass",
        "get_collection_class",
        "is_collection",
        "is_document_collection",
        "is_edge_collection",
        "get_collection_classes"
        ]


class CachedDoc:
    """
    A cached document
    """
    def __init__(self, document, previous, next_document):
        self.previous = previous
        self.document = document
        self.next_document = next_document
        self._key = document._key

    def __getitem__(self, key):
        return self.document[key]

    def __setitem__(self, key, value):
        self.document[key] = value

    def __getattribute__(self, key):
        try:
            return object.__getattribute__(self, key)
        except Exception as e1:
            try:
                return getattr(self.document, k)
            except Exception as e2:
                raise e2


class DocumentCache:
    """
    Document cache for collection, with insert, deletes and updates in O(1)
    """

    def __init__(self, cache_size):
        self.cache_size = cache_size
        self.cache_store = {}
        self.head = None
        self.tail = None

    def cache(self, document):
        if document._key in self.cache_store:
            cached_document = self.cache_store[document._key]
            if cached_document.previous is not None:
                cached_document.previous.next_document = cached_document.next_document
                self.head.previous = cached_document
                cached_document.next_document = self.head
                self.head = cached_document
            return self.head
        else:
            if len(self.cache_store) == 0:
                cached_document = CachedDoc(document, previous=None, next_document=None)
                self.head = cached_document
                self.tail = self.head
                self.cache_store[document._key] = cached_document
            else:
                if len(self.cache_store) >= self.cache_size:
                    del(self.cache_store[self.tail._key])
                    self.tail = self.tail.previous
                    self.tail.next_document = None

                cached_document = CachedDoc(document, previous=None, next_document=self.head)
                self.head.previous = cached_document
                self.head = self.head.previous
                self.cache_store[document._key] = cached_document

    def delete(self, _key):
        """
        removes a document from the cache
        """

        try:
            document = self.cache_store[_key]
            document.previous.next_document = doc.next_document
            document.next_document.previous = document.previous
            del(self.cache_store[_key])

        except KeyError:
            raise KeyError("Document with _key %s is not available in cache" % _key)

    def get_chain(self):
        """
        returns a list of keys representing the chain of documents
        """
        chain = []
        head = self.head
        while head:
            chain.append(head._key)
            head = head.next_document
        return chain

    def __str__(self):
        """
        A pretty str version of getChain()
        """
        chain = []
        head = self.head
        while head:
            chain.append(str(head._key))
            head = head.next_document
        return "<->".join(chain)

    def __getitem__(self, _key):
        try:
            cached_document = self.cache_store[_key]
            self.cache(cached_document)
            return cached_document
        except KeyError:
            raise KeyError("Document with _key %s is not available in cache" % _key)

    def __repr__(self):
        return "[DocumentCache, size: %d, full: %d]" %(self.cache_size, len(self.cache_store))


class Field:
    """
    The class for defining pyArango fields.
    """
    def __init__(self, validators = None, default = ""):
        """
        validators must be a list of validators
        """
        if validators is None:
            validators = []
        self.validators = validators
        self.default = default

    def validate(self, value):
        """
        checks the validity of 'value' given the lists of validators
        """
        for validator in self.validators:
            validator.validate(value)
        return True

    def __str__(self):
        string_validators = []
        for validator in self.validators:
            string_validators.append(str(validator))
        return "<Field, validators: '%s'>" % ', '.join(string_validators)


class CollectionMetaclass(type):
    """
    The metaclass that takes care of keeping a register of all collection types
    """
    collection_classes = {}

    _validation_default = {
            'on_save': False,
            'on_set': False,
            'on_load': False,
            'allow_foreign_fields': True
        }

    def __new__(cls, name, bases, attrs):
        def check_set_config_dict(dict_name):
            default_dict = getattr(cls, f"{dict_name}_default")

            if dict_name not in attrs:
                attrs[dict_name] = default_dict
            else:
                for key, value in attrs[dict_name].items():
                    if key not in default_dict:
                        raise KeyError(f"Unknown validation parameter '{key}' for class '{name}'")
                    if type(value) is not type(default_dict[key]):
                        raise ValueError(f"'{dict_name}' parameter '{key}' for class '{name}' is of type '{type(v)}' instead of '{type(default_dict[k])}'")

                for key, value in default_dict.items():
                    if key not in attrs[dict_name]:
                        attrs[dict_name][key] = value

        check_set_config_dict('_validation')
        class_object = type.__new__(cls, name, bases, attrs)
        CollectionMetaclass.collection_classes[name] = class_object

        return class_object

    @classmethod
    def get_collection_class(cls, name):
        """
        Return the class object of a collection given its 'name'
        """
        try:
            return cls.collection_classes[name]
        except KeyError:
            raise KeyError(f"There is no Collection Class of type: '{name}'; currently supported values: [{', '.join(get_collection_classes().keys())}]") 

    @classmethod
    def is_collection(cls, name):
        """
        Return true or false wether 'name' is the name of collection.
        """
        return name in cls.collection_classes

    @classmethod
    def is_document_collection(cls, name):
        """
        Return true or false whether 'name' is the name of a document collection.
        """
        try:
            Collection = cls.get_collection_class(name)
            return issubclass(collection, Collection)
        except KeyError:
            return False

    @classmethod
    def is_edge_collection(cls, name):
        """
        Return true or false whether 'name' is the name of an edge collection.
        """
        try:
            collection = cls.get_collection_class(name)
            return issubclass(collection, Edges)
        except KeyError:
            return False

def get_collection_class(name):
    """
    Return true or false whether 'name' is the name of collection.
    """
    return CollectionMetaclass.get_collection_class(name)


def is_collection(name):
    """
    Return true or false whether 'name' is the name of a document collection.
    """
    return CollectionMetaclass.is_collection(name)


def is_document_collection(name):
    """
    Return true or false whether 'name' is the name of a document collection.
    """
    return CollectionMetaclass.is_document_collection(name)


def is_edge_collection(name):
    """
    Return true or false whether 'name' is the name of an edge collection.
    """
    return CollectionMetaclass.is_edge_collection(name)


def get_collection_classes():
    """
    Returns a dictionary of all defined collection classes
    """
    return CollectionMetaclass.collection_classes


class Collection(with_metaclass(CollectionMetaclass, object)):
    """
    A document collection. Collections are meant to be instanciated by databases
    """
    #here you specify the fields that you want for the documents in your collection
    _fields = {}

    _validation = {
        'on_save': False,
        'on_set': False,
        'on_load': False,
        'allow_foreign_fields': True
    }

    arango_privates = ["_id", "_key", "_rev"]

    def __init__(self, database, json_data):

        def get_default_document(fields, dct):
            for k, v in fields.items():
                if isinstance(v, dict):
                    dct[k] = get_default_document(fields[k], {})
                elif isinstance(v, Field):
                    dct[k] = v.default
                else:
                    raise ValueError("Field '%s' is of invalid type '%s'" % (k, type(v)) )
            return dct

        self.database = database
        self.connection = self.database.connection
        self.name = self.__class__.__name__
        for key in json_data:
            setattr(self, key, json_data[key])

        self.URL = "%s/collection/%s" % (self.database.URL, self.name)
        self.documentsURL = "%s/document" % (self.database.URL)
        self.document_cache = None

        self.document_class = Document
        self.indexes = {
            "primary": {},
            "hash": {},
            "skiplist": {},
            "geo": {},
            "fulltext": {},
        }

        self.default_document = get_default_document(self._fields, {})

    def get_indexes(self):
        """
        Fills self.indexes with all the indexes associates with the collection and returns it
        """
        url = "%s/index" % self.database.URL
        response = self.connection.session.get(url, params = {"collection": self.name})
        data = response.json()
        for indx in data["indexes"]:
            self.indexes[indx["type"]][indx["id"]] = Index(collection = self, infos = indx)

        return self.indexes

    def activate_cache(self, cache_size):
        """
        Activate the caching system. Cached documents are only available through the __getitem__ interface
        """
        self.document_cache = DocumentCache(cache_size)

    def deactivate_cache(self):
        """
        deactivate the caching system
        """
        self.document_cache = None

    def delete(self):
        """
        deletes the collection from the database
        """
        response = self.connection.session.delete(self.URL)
        data = response.json()
        if not response.status_code == 200 or data["error"]:
            raise DeletionError(data["error_message"], data)

    def create_document(self, init_dict = None):
        """
        create and returns a document populated with the 
        defaults or with the values in init_dict
        """
        if init_dict is not None:
            return self.createDocument_(init_dict)
        else:
            if self._validation["on_load"]:
                self._validation["on_load"] = False
                return self.create_document_(self.default_document)
                self._validation["on_load"] = True
            else:
                return self.create_document_(self.default_document)

    def create_document_(self, init_dict = None):
        """
        Create and returns a completely empty document or one populated with init_dict
        """
        if init_dict is None:
            initV = {}
        else:
            initV = init_dict

        return self.document_class(self, initV)

    def import_bulk(self, data, **kwargs):
        url = "%s/import" % (self.database.URL)
        payload = json.dumps(data, default=str)
        params = {
            "collection": self.name,
            "type": "auto"
            }
        params.update(kwargs)
        response = self.connection.session.post(url , params = params, data = payload)
        data = response.json()
        if not response.status_code == 201 or data["error"]:
            raise CreationError(data["error_message"], data)
        return data

    def export_docs( self, **data):
        url = "%s/export" % (self.database.URL)
        params = {"collection": self.name}
        payload = json.dumps(data)
        response = self.connection.session.post(url, params = params, data = payload)
        data = response.json()
        if not response.status_code == 201 or data["error"]:
          raise ExportError(data["error_message"], data)
        documents = data['result']
        return documents

    def ensure_hash_index(self, fields, unique = False, sparse = True, deduplicate = False):
        """
        Creates a hash index if it does not already exist, and returns it
        """
        data = {
            "type": "hash",
            "fields": fields,
            "unique": unique,
            "sparse": sparse,
            "deduplicate": deduplicate
        }
        indx = Index(self, creation_data = data)
        self.indexes["hash"][indx.infos["id"]] = indx
        return indx

    def ensure_skiplist_index(self, fields, unique = False, sparse = True, deduplicate = False):
        """
        Creates a skiplist index if it does not already exist, and returns it
        """
        data = {
            "type": "skiplist",
            "fields": fields,
            "unique": unique,
            "sparse": sparse,
            "deduplicate": deduplicate
        }
        indx = Index(self, creation_data = data)
        self.indexes["skiplist"][indx.infos["id"]] = indx
        return indx

    def ensure_geo_index(self, fields):
        """
        Creates a geo index if it does not already exist, and returns it
        """
        data = {
            "type": "geo",
            "fields": fields,
        }
        indx = Index(self, creation_data = data)
        self.indexes["geo"][indx.infos["id"]] = indx
        return indx

    def ensure_fulltext_index(self, fields, min_length = None):
        """
        Creates a fulltext index if it does not already exist, and returns it
        """
        data = {
            "type": "fulltext",
            "fields": fields,
        }
        if min_length is not None:
            data["minLength"] = min_length

        indx = Index(self, creation_data = data)
        self.indexes["fulltext"][indx.infos["id"]] = indx
        return indx

    def validate_private(self, field, value):
        """
        validate a private field value
        """
        if field not in self.arango_privates:
            raise ValueError("%s is not a private field of collection %s" % (field, self))

        if field in self._fields:
            self._fields[field].validate(value)
        return True

    @classmethod
    def has_field(cls, field_name):
        """
        returns True/False wether the collection has field K in it's schema.
        Use the dot notation for the nested fields: address.street
        """
        path = field_name.split(".")
        value = cls._fields
        for key in path:
            try:
                value = v[key]
            except KeyError:
                return False
        return True

    def fetch_document(self, key, raw_results = False, rev = None):
        """
        Fetches a document from the collection given it's key. 
        This function always goes straight to the db and bypasses the cache. If you
        want to take advantage of the cache use the __getitem__ interface: collection[key]
        """
        url = "%s/%s/%s" % (self.documentsURL, self.name, key)
        if rev is not None:
            response = self.connection.session.get(url, params = {'rev': rev})
        else:
            response = self.connection.session.get(url)

        if response.status_code < 400:
            if raw_results:
                return response.json()
            return self.document_class(self, response.json())
        elif response.status_code == 404:
            raise DocumentNotFoundError("Unable to find document with _key: %s" % key, response.json())
        else:
            raise DocumentNotFoundError("Unable to find document with _key: %s, response: %s" % (key, response.json()), response.json())

    def fetch_by_example(self, example_dict, batch_size, raw_results = False, **query_args):
        """
        example_dict should be something like {'age': 28}
        """
        print("\n\nI am fetching by example!!\n\n\n\n\n")
        return self.simple_query('by-example', raw_results, example = example_dict, batch_size = batch_size, **query_args)

    def fetch_first_example(self, example_dict, raw_results = False):
        """
        example_dict should be something like {'age': 28}. 
        Returns only a single element but still in a SimpleQuery object.
        Returns the first example found that matches the example
        """
        return self.simple_query('first-example', raw_results = raw_results, example = example_dict)

    def fetch_all(self, raw_results = False, **query_args):
        """
        Returns all the documents in the collection. 
        You can use the optinal arguments 'skip' and 'limit'::

            fetchAlll(limit = 3, shik = 10)
        """
        return self.simple_query('all', raw_results = raw_results, **query_args)

    def simple_query(self, query_type, raw_results = False, **query_args):
        """
        General interface for simple queries. 
        Query_type can be something like 'all', 'by-example' etc... everything is in the arango doc.
        If raw_results, the query will return dictionaries instead of Document objetcs.
        """
        return SimpleQuery(self, query_type, raw_results, **query_args)

    def action(self, method, action, **params):
        """
        A generic fct for interacting everything that doesn't have an assigned fct
        """
        fct = getattr(self.connection.session, method.lower())
        response = fct(self.URL + "/" + action, params = params)
        return response.json()

    def bulk_save(self, documents, on_duplicate="error", **params):
        """
        Parameter documents must be either an iterrable of documents or dictionnaries.
        This function will return the number of documents, created and updated, and will raise an UpdateError exception if there's at least one error.
        params are any parameters from arango's documentation
        """

        payload = []
        for document in documents:
            if type(document) is dict:
                payload.append(json.dumps(document, default=str))
            else:
                try:
                    payload.append(document.to_json())
                except Exception as e:
                    payload.append(json.dumps(document.getStore(), default=str))

        payload = '\n'.join(payload)

        params["type"] = "documents"
        params["on_duplicate"] = onDuplicate
        params["collection"] = self.name
        URL = "%s/import" % self.database.URL

        response = self.connection.session.post(URL, params = params, data = payload)
        data = response.json()
        if (response.status_code == 201) and "error" not in data:
            return True
        else:
            if data["errors"] > 0:
                raise UpdateError("%d documents could not be created" % data["errors"], data)

        return data["updated"] + data["created"]

    def bulk_import_json(self, filename, on_duplicate="error", formatType="auto", **params):
        """
        bulk import from a file repecting arango's key/value format
        """

        url = "%s/import" % self.database.URL
        params["on_duplicate"] = onDuplicate
        params["collection"] = self.name
        params["type"] = formatType
        with open(filename) as f:
            data = f.read()
            response = self.connection.session.post(URL, params = params, data = data)

            try:
                error_message = "At least: %d errors. The first one is: '%s'\n\n more in <this_exception>.data" % (len(data), data[0]["error_message"])
            except KeyError:
                raise UpdateError(data['error_message'], data)

    def bulk_import_values(self, filename, on_duplicate="error", **params):
        """
        bulk import from a file repecting arango's json format
        """

        url = "%s/import" % self.database.URL
        params["on_duplicate"] = onDuplicate
        params["collection"] = self.name
        with open(filename) as f:
            data = f.read()
            response = self.connection.session.post(URL, params = params, data = data)

            try:
                error_message = "At least: %d errors. The first one is: '%s'\n\n more in <this_exception>.data" % (len(data), data[0]["error_message"])
            except KeyError:
                raise UpdateError(data['error_message'], data)

    def truncate(self):
        """
        deletes every document in the collection
        """
        return self.action('PUT', 'truncate')

    def empty(self):
        """
        alias for truncate
        """
        return self.truncate()

    def load(self):
        """
        loads collection in memory
        """
        return self.action('PUT', 'load')

    def unload(self):
        """
        unloads collection from memory
        """
        return self.action('PUT', 'unload')

    def revision(self):
        """
        returns the current revision
        """
        return self.action('GET', 'revision')["revision"]

    def properties(self):
        """
        returns the current properties
        """
        return self.action('GET', 'properties')

    def checksum(self):
        """
        returns the current checksum
        """
        return self.action('GET', 'checksum')["checksum"]

    def count(self):
        """
        returns the number of documents in the collection
        """
        return self.action('GET', 'count')["count"]

    def figures(self):
        """
        a more elaborate version of count, see arangodb docs for more infos
        """
        return self.action('GET', 'figures')

    def getType(self):
        """
        returns a word describing the type of the collection (edges or ducments) instead of a number, 
        if you prefer the number it's in self.type
        """
        if self.type == CONST.COLLECTION_DOCUMENT_TYPE:
            return "document"
        elif self.type == CONST.COLLECTION_EDGE_TYPE:
            return "edge"
        else:
            raise ValueError("The collection is of Unknown type %s" % self.type)

    def getStatus(self):
        """
        returns a word describing the status of the collection
        (loaded, loading, deleted, unloaded, newborn) instead of a number, 
        if you prefer the number it's in self.status
        """
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
        """
        returns the number of documents in the collection
        """
        return self.count()

    def __repr__(self):
        return "ArangoDB collection name: %s, id: %s, type: %s, status: %s" % (self.name, self.id, self.getType(), self.getStatus())

    def __getitem__(self, key):
        """
        Returns a document from the cache. 
        If it's not there, fetches it from the db and caches it first. 
        If the cache is not activated this is equivalent to fetch_document( raw_results=False)
        """
        if self.document_cache is None:
            return self.fetch_document(key, raw_results = False)
        try:
            return self.document_cache[key]
        except KeyError:
            document = self.fetch_document(key, raw_results = False)
            self.document_cache.cache(document)
        return document

    def __contains__(self, key):
        """if document in collection"""
        try:
            self.fetch_document(key, raw_results = False)
            return True
        except DocumentNotFoundError as e:
            return False


class SystemCollection(Collection):
    """
    for all collections with isSystem = True
    """
    def __init__(self, database, jsonData):
        Collection.__init__(self, database, jsonData)


class Edges(Collection):
    """
    The default edge collection. All edge Collections must inherit from it
    """

    arango_privates = ["_id", "_key", "_rev", "_to", "_from"]

    def __init__(self, database, jsonData):
        """
        This one is meant to be called by the database
        """
        Collection.__init__(self, database, jsonData)
        self.document_class = Edge
        self.edgesURL = "%s/edges/%s" % (self.database.URL, self.name)

    @classmethod
    def validate_field(cls, field_name, value):
        """checks if 'value' is valid for field 'fieldName'. 
        If the validation is unsuccessful, raises a SchemaViolation or a ValidationError.
        for nested dicts ex: {address: { street: xxx} }, fieldName can take the form address.street
        """
        try:
            validated_value = Collection.validate_field(field_name, value)
        except SchemaViolation as e:
            if field_name == "_from" or field_name == "_to":
                return True
            else:
                raise e
        return validated_value

    def create_edge(self):
        """
        Create an edge populated with defaults
        """
        return self.create_document()

    def create_edge_(self, init_values = None):
        """
        Create an edge populated with initValues
        """
        if not init_values:
            init_values = {}
        return self.create_document_(init_values)

    def get_in_edges(self, vertex, raw_results = False):
        """
        An alias for get_edges() that returns only the in Edges
        """
        return self.getEdges(vertex, in_edges = True, out_edges = False, raw_results = raw_results)

    def get_out_edges(self, vertex, raw_results = False):
        """
        An alias for get_edges() that returns only the out Edges
        """
        return self.get_edges(vertex, in_edges = False, out_edges = True, raw_results = raw_results)

    def get_edges(self, vertex, in_edges = True, out_edges = True, raw_results = False):
        """
        returns in, out, or both edges liked to a given document. 
        Vertex can be either a Document object or a string for an _id.
        If raw_results a arango results will be return as fetched, 
        if false, will return a liste of Edge objects
        """
        if isinstance(vertex, Document):
            vertex_id = vertex._id
        elif (type(vertex) is str) or (type(vertex) is bytes):
            vertex_id = vertex
        else:
            raise ValueError("Vertex is neither a Document nor a String")

        params = {"vertex": vertex_id}
        if in_edges and out_edges:
            pass
        elif in_edges:
            params["direction"] = "in"
        elif out_edges:
            params["direction"] = "out"
        else:
            raise ValueError("in_edges, out_edges or both must have a boolean value")

        response = self.connection.session.get(self.edgesURL, params = params)
        data = response.json()
        if response.status_code == 200:
            if not raw_results:
                cached_document = []
                for edge in data["edges"]:
                    cached_document.append(Edge(self, edge))
                return cached_document
            else:
                return data["edges"]
        else:
            raise CreationError("Unable to return edges for vertex: %s" % vertex_id, data)
