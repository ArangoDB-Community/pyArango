import json
import types

from . import collection as COL
from . import consts as CONST
from . import graph as GR

from .document import Document
from .graph import Graph
from .query import AQLQuery
from .theExceptions import CreationError, UpdateError, AQLQueryError, TransactionError


__all__ = ["Database", "DBHandle"]


class Database(object):
    """
    Databases are meant to be instantiated by connections
    """

    def __init__(self, connection, name):

        self.name = name
        self.connection = connection
        self.collections = {}

        self.URL = '%s/_db/%s/_api' % (self.connection.arangoURL, self.name)
        self.collectionsURL = '%s/collection' % (self.URL)
        self.cursors_URL = '%s/cursor' % (self.URL)
        self.explainURL = '%s/explain' % (self.URL)
        self.graphsURL = "%s/gharial" % (self.URL)
        self.transaction_URL = "%s/transaction" % (self.URL)

        self.collections = {}
        self.graphs = {}

        self.reload()

    def reload_collections(self) :
        """
        Reloads the collection list.
        """
        response = self.connection.session.get(self.collectionsURL)
        data = response.json()
        if response.status_code == 200:
            self.collections = {}

            for collection_data in data["result"]:
                collection_name = collection_data['name']
                if collection_data['isSystem']:
                    collection_object = COL.SystemCollection(self, collection_data)
                else:
                    try:
                        collection_class = COL.get_collection_class(collection_name)
                        collection_object = collection_class(self, collection_data)
                    except KeyError:
                        if collection_data["type"] == CONST.COLLECTION_EDGE_TYPE:
                            collection_object = COL.Edges(self, collection_data)
                        elif collection_data["type"] == CONST.COLLECTION_DOCUMENT_TYPE:
                            collection_object = COL.Collection(self, collection_data)
                        else:
                            print("Warning!! Collection of unknown type: %d, trying to load it as Collection nonetheless." % collection_data["type"])
                            collection_object = COL.Collection(self, collection_data)

                self.collections[collection_name] = collection_object
        else:
            raise UpdateError(data["errorMessage"], data)

    def reload_graphs(self) :
        """
        Reloads the graph list
        """
        response = self.connection.session.get(self.graphsURL)
        data = response.json()
        if response.status_code == 200:
            self.graphs = {}
            for graph_data in data["graphs"]:
                deserialised_graph_data = graph_data
                try:
                    self.graphs[graph_data["_key"]] = GR.get_graph_class(
                        deserialised_graph_data["_key"]
                        )(self, deserialised_graph_data)
                except KeyError:
                    self.graphs[deserialised_graph_data["_key"]] = Graph(
                            self, deserialised_graph_data
                            )
        else:
            raise UpdateError(data["errorMessage"], data)

    def reload(self) :
        """
        Reloads collections and graphs
        """
        self.reload_collections()
        self.reload_graphs()

    def create_collection(self, class_name='Collection', **collection_properties):
        """Creates a collection and returns it.
        class_name the name of a class inheriting from Collection or Egdes, 
        it can also be set to 'Collection' or 'Edges' in order to create 
        untyped collections of documents or edges.
        Use collection_properties to put things such as 'wait_for_sync = True' 
        (see ArangoDB's doc for a full list of possible arugments). 
        If a '_properties' dictionary is defined in the collection schema, 
        arguments to this function overide it.
        """

        collection_class = COL.get_collection_class(class_name)

        if len(collection_properties) > 0:
            collection_properties = dict(collection_properties)
        else:
            try:
                collection_properties = dict(collection_class._properties)
            except AttributeError:
                collection_properties = {}

        if class_name != 'Collection' and class_name != 'Edges':
            collection_properties['name'] = class_name
        else:
            if 'name' not in collection_properties:
                raise ValueError("a 'name' argument mush be supplied if you want to create a generic collection")

        if collection_properties['name'] in self.collections :
            raise CreationError("Database %s already has a collection named %s" % (self.name, collection_properties['name']) )

        if issubclass(collection_class, COL.Edges) or collection_class.__class__ is COL.Edges:
            collection_properties["type"] = CONST.COLLECTION_EDGE_TYPE
        else:
            collection_properties["type"] = CONST.COLLECTION_DOCUMENT_TYPE

        payload = json.dumps(collection_properties, default=str)
        response = self.connection.session.post(self.collectionsURL, data = payload)
        data = response.json()

        if response.status_code == 200 and not data["error"]:
            collection = collection_class(self, data)
            self.collections[collection.name] = collection
            return self.collections[collection.name]
        else:
            raise CreationError(data["errorMessage"], data)

    def fetch_document(self, _id) :
        """
        fetchs a document using it's _id
        """
        split_id = _id.split("/")
        return self[split_id[0]][split_id[1]]

    def create_graph(self, name, create_collections=True, is_smart=False, number_of_shards=None, smart_graph_attribute=None) :
        """
        Creates a graph and returns it. 'name' must be the name 
        of a class inheriting from Graph.
        Checks will be performed to make sure that every 
        collection mentionned in the edges definition exist. 
        Raises a ValueError in case of
        a non-existing collection.
        """

        def _check_collection_list(collection_list) :
            for collection_name in collection_list:
                if not COL.is_collection(collection_name):
                    raise ValueError("'%s' is not a defined Collection" % collection_name)

        graph_class = GR.get_graph_class(name)

        edge_definitions = []
        for edge_definition in graph_class._edge_definitions:
            if not COL.is_edge_collection(edge_definition.edges_collection):
                raise ValueError("'%s' is not a defined Edge Collection" % edge_definition.edges_collection)
            _check_collection_list(edge_definition.from_collections)
            _check_collection_list(edge_definition.to_collections)

            edge_definitions.append(edge_definition.to_json())

        _check_collection_list(graph_class._orphaned_collections)

        options = {}
        if number_of_shards:
            options['number_of_shards'] = number_of_shards
        if smart_graph_attribute:
            options['smart_graph_attribute'] = smart_graph_attribute

        payload = {
                "name": name,
                "edgeDefinitions": edge_definitions,
                "orphanCollections": graph_class._orphaned_collections
            }

        if is_smart:
                payload['is_smart'] = is_smart

        if options:
            payload['options'] = options

        payload = json.dumps(payload)

        response = self.connection.session.post(self.graphsURL, data = payload)

        data = response.json()

        graph_data = data["graph"]

        if response.status_code == 201 or response.status_code == 202:
            self.graphs[name] = graph_class(self, graph_data)
        else:
            raise CreationError(data["errorMessage"], data)
        return self.graphs[name]

    def has_collection(self, name) :
        """
        Returns true if the databse has a collection by the name of 'name'
        """
        return name in self.collections

    def has_graph(self, name):
        """
        Returns true if the databse has a graph by the name of 'name'
        """
        return name in self.graphs

    def drop_all_collections(self):
        """
        drops all public collections (graphs included) from the database
        """
        for graph_name in self.graphs:
            self.graphs[graph_name].delete()
        for collection_name in self.collections:
            # Collections whose name starts with '_' are system collections
            if not collection_name.startswith('_'):
                self[collection_name].delete()
        return

    def AQLQuery(self, query, batch_size = 100, raw_results = False, bind_variables = {}, options = {}, count = False, full_count = False,
                 json_encoder = None, **moreArgs) :
        """
        Set raw_results = True if you want the query to 
        return dictionnaries instead of Document objects.
        You can use **moreArgs to pass more arguments supported 
        by the api, such as ttl=60 (time to live)
        """
        return AQLQuery(
                self,
                query,
                raw_results=raw_results,
                batch_size=batch_size,
                bind_variables=bind_variables,
                options=options, count=count,
                full_count=full_count,
                json_encoder=json_encoder,
                **moreArgs
                )

    def explain_AQL_query(self, query, bind_variables={}, all_plans = False) :
        """
        Returns an explanation of the query. 
        Setting all_plans to True will result in ArangoDB returning all 
        possible plans. False returns only the optimal plan
        """
        payload = {
                'query': query,
                'bind_variables': bind_variables,
                'all_plans': all_plans
                }
        request = self.connection.session.post(self.explainURL, data = json.dumps(payload, default=str))
        return request.json()

    def validate_AQL_query(self, query, bind_variables = None, options = None) :
        """
        Returns the server answer is the query is valid. 
        Raises an AQLQueryError if not
        """
        if bind_variables is None :
            bind_variables = {}
        if options is None :
            options = {}
        payload = {
                'query': query,
                'bind_variables': bind_variables,
                'options': options
                }
        response = self.connection.session.post(self.cursors_URL, data = json.dumps(payload, default=str))
        data = response.json()
        if response.status_code == 201 and not data["error"]:
            return data
        else:
            raise AQLQueryError(data["errorMessage"], query, data)

    def transaction(self, collections, action, wait_for_sync = False, lock_timeout = None, params = None):
        """
        Execute a server-side transaction
        """
        payload = {
                "collections": collections,
                "action": action,
                "wait_for_sync": wait_for_sync
                }
        if lock_timeout is not None:
                payload["lock_timeout"] = lock_timeout
        if params is not None:
            payload["params"] = params

        self.connection.report_start(action)

        response = self.connection.session.post(self.transaction_URL, data = json.dumps(payload, default=str))

        self.connection.report_item()

        data = response.json()

        if (response.status_code == 200 or response.status_code == 201 or response.status_code == 202) and not data.get("error"):
            return data
        else:
            raise TransactionError(data["errorMessage"], action, data)

    def __repr__(self):
        return "ArangoDB database: %s" % self.name

    def __getitem__(self, collection_name):
        """
        use database[collectionName] to get a collection from the database
        """
        try:
            return self.collections[collection_name]
        except KeyError:
            self.reload()
            try:
                return self.collections[collection_name]
            except KeyError:
                raise KeyError("Can't find any collection named : %s" % collection_name)

class DBHandle(Database):
    """
    As the loading of a Database also triggers the loading of 
    collections and graphs within. Only handles are loaded first. 
    The full database are loaded on demand in a fully transparent manner.
    """
    def __init__(self, connection, name) :
        self.connection = connection
        self.name = name

    def __getattr__(self, k):
        name = Database.__getattribute__(self, 'name')
        connection = Database.__getattribute__(self, 'connection')
        Database.__init__(self, connection, name)
        return Database.__getattribute__(self, k)
