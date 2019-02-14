import json

from future.utils import implements_iterator

from .document import Document, Edge
from .theExceptions import QueryError, AQLQueryError, SimpleQueryError, CreationError
from . import consts as CONST

__all__ = ["Query", "AQLQuery", "SimpleQuery", "Cursor", "RawCursor"]

@implements_iterator
class RawCursor(object):
    """
    a raw interface to cursors that returns json
    """

    def __init__(self, database, cursor_id):
        self.database = database
        self.connection = self.database.connection
        self.id = cursor_id
        self.URL = "%s/cursor/%s" % (self.database.URL, self.id)

    def __next__(self):
        """
        returns the next batch
        """
        response = self.connection.session.put(self.URL)
        data = response.json()
        if response.status_code in [400, 404]:
            raise CursorError(data["errorMessage"], self.id, data)
        return response.json()

@implements_iterator
class Query(object):
    """
    This class is abstract and should not be instanciated. All query classes derive from it
    """

    def __init__(self, request, database, raw_results):
        """
        If raw_results = True, the results will be returned as dictionaries instead of Document objects.
        """

        self.raw_results = raw_results
        self.response = request.json()
        if self.response.get("error") and self.response["errorMessage"] != "no match":
            raise QueryError(self.response["errorMessage"], self.response)

        self.request = request
        self.database = database
        self.connection = self.database.connection
        self.current_i = 0
        if request.status_code == 201 or request.status_code == 200 or request.status_code == 202:
            self.batchNumber = 1
            try: #if there's only one element
                self.response = {"result": [self.response["document"]], 'hasMore' : False}
                del(self.response["document"])
            except KeyError:
                pass

            if "hasMore" in self.response and self.response["hasMore"]:
                cursor_id = self.response.get("id","")
                self.cursor = RawCursor(self.database, cursor_id)
            else:
                self.cursor = None
        elif request.status_code == 404:
            self.batch_number = 0
            self.result = []
        else:
            self._raise_init_failed(request)

    def _raise_init_failed(self, request):
        """
        must be implemented in child, this called if the __init__ fails
        """
        raise NotImplemented("Must be implemented in child")

    def _develop_doc(self, i):
        """
        private function that transforms a json returned by ArangoDB into a pyArango Document or Edge
        """
        doc_json = self.result[i]
        try:
            collection = self.database[doc_json["_id"].split("/")[0]]
        except KeyError:
            raise CreationError("result %d is not a valid Document. Try setting raw_results to True" % i)

        if collection.type == CONST.COLLECTION_EDGE_TYPE:
            self.result[i] = Edge(collection, doc_json)
        else:
            self.result[i] = Document(collection, doc_json)

    def next_batch(self):
        """
        become the next batch. raises a StopIteration if there is None
        """
        self.batch_number += 1
        self.current_i = 0
        try:
            if not self.response["hasMore"] or self.cursor is None:
                raise StopIteration("That was the last batch")
        except KeyError:
            raise AQLQueryError(self.response["errorMessage"], self.query, self.response)

        self.response = next(self.cursor)

    def delete(self):
        """
        kills the cursor
        """
        self.connection.session.delete(self.cursor)

    def __next__(self):
        """
        returns the next element of the query result. Automatomatically calls for new batches if needed
        """
        try:
            v = self[self.current_i]
        except IndexError:
            self.nextBatch()
        v = self[self.current_i]
        self.current_i += 1
        return v

    def __iter__(self):
        """
        Returns an itererator so you can do:
            for doc in query: print doc
        """
        return self

    def __getitem__(self, i):
        """
        returns a ith result of the query.
        """
        if not self.raw_results and (not isinstance(self.result[i], (Edge, Document))):
            self._develop_doc(i)
        return self.result[i]

    def __len__(self):
        """
        Returns the number of elements in the query results
        """
        return len(self.result)

    def __getattr__(self, k):
        try:
            resp = object.__getattribute__(self, "response")
            return resp[k]
        except (KeyError, AttributeError):
            raise  AttributeError("There's no attribute %s" %(k))

    def __str__(self):
        return str(self.result)

class AQLQuery(Query):
    """
    AQL queries are attached to and instanciated by a database
    """
    def __init__(self, database, query, batch_size, bind_vars, options, count, full_count, raw_results=True,
                 json_encoder=None, **more_args):
        # fullCount is passed in the options dict per https://docs.arangodb.com/3.1/HTTP/AqlQueryCursor/AccessingCursors.html
        options["fullCount"] = full_count
        payload = {'query': query, 'batchSize': batch_size, 'bindVars': bindVars, 'options': options, 'count': count}
        payload.update(more_args)

        self.query = query
        self.database = database
        self.connection = self.database.connection
        self.connection.report_start(query)
        request = self.connection.session.post(database.cursors_URL, data = json.dumps(payload, cls=json_encoder, default=str))
        self.connection.reportItem()

        try:
            Query.__init__(self, request, database, raw_results)
        except QueryError as e:
            raise AQLQueryError( message = e.message, query = self.query, errors = e.errors)

    def explain(self, bind_vars={}, all_plans=False):
        """
        Returns an explanation of the query. Setting allPlans to True will result in ArangoDB returning all possible plans. False returns only the optimal plan
        """
        return self.database.explainAQLQuery(self.query, bind_vars, all_plans)

    def _raiseInitFailed(self, request):
        data = request.json()
        raise AQLQueryError(data["errorMessage"], self.query, data)

class Cursor(Query):
    """
    Cursor queries are attached to and instanciated by a database, use them to continue from where you left
    """
    def __init__(self, database, cursor_id, raw_results):
        self.raw_results = raw_results
        self._developed = set()
        self.batch_number = 1
        self.cursor = RawCursor(database, cursor_id)
        self.response = next(self.cursor)

    def _raise_init_failed(self, request):
        data = request.json()
        raise CursorError(data["errorMessage"], self.id, data)


class SimpleQuery(Query):
    """
    Simple queries are attached to and instanciated by a collection
    """
    def __init__(self, collection, query_type, raw_results, json_encoder = None,
                 **query_args):

        self.collection = collection
        self.connection = self.collection.database.connection

        payload = {'collection': collection.name}
        payload.update(query_args)
        payload = json.dumps(payload, cls=json_encoder, default=str)
        URL = "%s/simple/%s" % (collection.database.URL, query_type)
        request = self.connection.session.put(URL, data=payload)

        Query.__init__(self, request, collection.database, raw_results)

    def _raise_init_failed(self, request):
        data = request.json()
        raise SimpleQueryError(data["errorMessage"], data)

    def _develop_doc(self, i):
        doc_json = self.result[i]
        if self.collection.type == CONST.COLLECTION_EDGE_TYPE:
            self.result[i] = Edge(self.collection, doc_json)
        else:
            self.result[i] = Document(self.collection, doc_json)
