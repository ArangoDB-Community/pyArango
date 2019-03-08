import json
from future.utils import with_metaclass

from .theExceptions import (CreationError, DeletionError, UpdateError, TraversalError)
from . import collection as COL
from . import document as DOC

__all__ = [
        "Graph",
        "get_graph_class",
        "is_graph",
        "get_graph_classes",
        "GraphMetaclass",
        "EdgeDefinition"
        ]


class GraphMetaclass(type):
    """
    Keeps track of all graph classes and does basic validations on fields
    """
    graph_classes = {}

    def __new__(cls, name, bases, attrs):
        obj = type.__new__(cls, name, bases, attrs)
        if name != 'Graph':
            try:
                if len(attrs['_edge_definitions']) < 1:
                    raise CreationError("Graph class '%s' has no edge definition" % name)
            except KeyError:
                raise CreationError("Graph class '%s' has no field _edge_definitions" % name)

        if name != "Graph":
            GraphMetaclass.graph_classes[name] = obj
        return obj

    @classmethod
    def get_graph_class(cls, name):
        """
        return a graph class by its name
        """
        try:
            return cls.graph_classes[name]
        except KeyError:
            raise KeyError("There's no child of Graph by the name of: %s" % name)

    @classmethod
    def is_graph(cls, name):
        """
        returns True/False depending if there is a graph called name
        """
        return name in cls.graph_classes

def get_graph_class(name):
    """
    alias for GraphMetaclass.getGraphClass()
    """
    return GraphMetaclass.get_graph_class(name)

def is_graph(name):
    """
    alias for GraphMetaclass.is_graph()
    """
    return GraphMetaclass.is_graph(name)

def get_graph_classes():
    """
    returns a dictionary of all defined graph classes
    """
    return GraphMetaclass.graph_classes


class EdgeDefinition(object):
    """
    An edge definition for a graph
    """

    def __init__(self, edges_collection, from_collections, to_collections):
        self.name = edges_collection
        self.edges_collection = edges_collection
        self.from_collections = from_collections
        self.to_collections = to_collections

    def to_json(self):
        return {
                'collection': self.edges_collection,
                'from': self.from_collections,
                'to': self.to_collections
                }

    def __str__(self):
        return '<ArangoED>'+ str(self.to_json())

    def __repr__(self):
        return str(self)

class Graph(with_metaclass(GraphMetaclass, object)):
    """
    The class from which all your graph types must derive
    """

    _edge_definitions = []
    _orphaned_collections = []

    def __init__(self, database, json_init):
        self.database = database
        self.connection = self.database.connection
        try:
            self._key = json_init["_key"]
        except KeyError:
            self._key = json_init["name"]
        except KeyError:
            raise KeyError("'json_init' must have a field '_key' or a field 'name'")

        self.name = self._key
        self._rev = json_init["_rev"]
        self._id = json_init["_id"]

        orphans = set(self._orphaned_collections)
        for orphan in json_init["orphanCollections"]:
            if orphan not in orphans:
                self._orphaned_collections.append(orphan)
                if self.connection.verbose:
                    print("Orphan collection %s is not in graph definition. Added it" % orphan)

        self.definitions = {}
        edge_definition_names = set()
        for edge_definition in self._edge_definitions:
            self.definitions[edge_definition.edges_collection] = edge_definition.edges_collection


        for edge_definition in json_init["edgeDefinitions"]:
            if edge_definition["collection"] not in self.definitions:
                self.definitions[edge_definition["collection"]] = EdgeDefinition(edge_definition["collection"], from_collections = edge_definition["from"], to_collections = edge_definition["to"])
                if self.connection.verbose:
                    print("Edge definition %s is not in graph definition. Added it" % ed)

        for edge_definition in self._edge_definitions:
            if edge_definition.edges_collection not in self.database.collections and not COL.is_edge_collection(edge_definition.edges_collection):
                raise KeyError("'%s' is not a valid edge collection" % edge_definition.edges_collection)
            self.definitions[edge_definition.edges_collection] = edge_definition

        self.URL = "%s/%s" % (self.database.graphsURL, self._key)

    def create_vertex(self, collection_name, document_attributes, wait_for_sync = False):
        """
        Adds a vertex to the graph and returns it
        """
        url = "%s/vertex/%s" % (self.URL, collection_name)

        store = DOC.DocumentStore(
                self.database[collection_name],
                validators=self.database[collection_name]._fields,
                initialisation_dictionary=document_attributes
                )

        # self.database[collection_name].validateDct(docAttributes)
        store.validate()

        response = self.connection.session.post(url, data=json.dumps(document_attributes, default=str), params={'wait_for_sync': wait_for_sync})

        data = response.json()
        if response.status_code == 201 or response.status_code == 202:
            return self.database[collection_name][data["vertex"]["_key"]]

        raise CreationError("Unable to create vertice, %s" % data["errorMessage"], data)

    def delete_vertex(self, document, wait_for_sync = False):
        """
        deletes a vertex from the graph as well as al linked edges
        """
        url = "%s/vertex/%s" % (self.URL, document._id)

        response = self.connection.session.delete(url, params={'wait_for_sync': wait_for_sync})
        data = response.json()
        if response.status_code == 200 or response.status_code == 202:
            return True

        raise DeletionError("Unable to delete vertice, %s" % document._id, data)

    def create_edge(self, collection_name, _from_id, _to_id, edge_attributes, wait_for_sync = False):
        """
        creates an edge between two documents
        """
        if not _from_id:
            raise ValueError("Invalid _from_id: %s" % _from_id)

        if not _to_id:
            raise ValueError("Invalid _to_id: %s" % _to_id)

        if collection_name not in self.definitions:
            raise KeyError("'%s' is not among the edge definitions" % collection_name)

        url = "%s/edge/%s" % (self.URL, collection_name)
        self.database[collection_name].validate_private("_from", _from_id)
        self.database[collection_name].validate_private("_to", _to_id)
        
        edge_definition = self.database[collection_name].create_edge()
        edge_definition.set(edge_attributes)
        edge_definition.validate()

        payload = edge_definition.get_store()
        payload.update({'_from': _from_id, '_to': _to_id})

        response = self.connection.session.post(url, data = json.dumps(payload, default=str), params = {'wait_for_sync': wait_for_sync})
        data = response.json()
        if response.status_code == 201 or response.status_code == 202:
            return self.database[collection_name][data["edge"]["_key"]]
        # print "\ngraph 160, ", data, payload, _fromId
        else:
            raise CreationError("Unable to create edge, %s" % response.json()["errorMessage"], data)

    def link(self, definition, doc1, doc2, edge_attributes, wait_for_sync=False):
        """
        A shorthand for createEdge that takes two documents as input
        """
        if type(doc1) is DOC.Document:
            if not doc1._id:
                doc1.save()
            doc1_id = doc1._id
        else:
            doc1_id = doc1

        if type(doc2) is DOC.Document:
            if not doc2._id:
                doc2.save()
            doc2_id = doc2._id
        else:
            doc2_id = doc2

        return self.create_edge(definition, doc1_id, doc2_id, edge_attributes, wait_for_sync)

    def unlink(self, definition, doc1, doc2):
        """
        deletes all links between doc1 and doc2
        """
        links = self.database[definition].fetch_by_example({"_from": doc1._id,"_to": doc2._id}, batchSize = 100)
        for link in links:
            self.deleteEdge(link)

    def delete_edge(self, edge, wait_for_sync = False):
        """
        removes an edge from the graph
        """
        url = "%s/edge/%s" % (self.URL, edge._id)
        response = self.connection.session.delete(url, params={'wait_for_sync': wait_for_sync})
        if response.status_code == 200 or response.status_code == 202:
            return True
        raise DeletionError("Unable to delete edge, %s" % edge._id, response.json())

    def delete(self):
        """
        Deletes the graph
        """
        response = self.connection.session.delete(self.URL)
        data = response.json()
        if response.status_code < 200 or response.status_code > 202 or data["error"]:
            raise DeletionError(data["errorMessage"], data)

    def traverse(self, start_vertex, **kwargs):
        """
        Traversal! see: https://docs.arangodb.com/HttpTraversal/README.html for 
        a full list of the possible kwargs.
        The function must have as argument either: direction = 
        "outbout"/"any"/"inbound" or expander = "custom JS (see arangodb's doc)".
        The function can't have both 'direction' and 'expander' as arguments.
        """

        url = "%s/traversal" % self.database.URL
        if type(start_vertex) is DOC.Document:
            start_vertex_id = start_vertex._id
        else:
            start_vertex_id = start_vertex

        payload = {
                "startVertex": start_vertex_id, 
                "graphName": self.name
                }

        if "expander" in kwargs:
            if "direction" in kwargs:
                    raise ValueError(
                            """The function can't have both 'direction' and 'expander' as arguments""") 
        elif "direction" not in kwargs:
            raise ValueError(
                    """The function must have as argument either: direction = "outbout"/"any"/"inbound" or expander = "custom JS (see arangodb's doc)" """) 

        payload.update(kwargs)

        response = self.connection.session.post(url, data = json.dumps(payload, default=str))
        data = response.json()
        if response.status_code < 200 or r.status_code > 202 or data["error"]:
            raise TraversalError(data["errorMessage"], data)

        return data["result"]

    def __str__(self):
        return "ArangoGraph: %s" % self.name
