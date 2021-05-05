import json

from future.utils import implements_iterator

from .document import Document, Edge
from .theExceptions import QueryError, AQLQueryError, SimpleQueryError, CreationError, CursorError
from . import consts as CONST

__all__ = ["Query", "AQLQuery", "SimpleQuery", "Cursor", "RawCursor"]

@implements_iterator
class RawCursor(object):
    "a raw interface to cursors that returns json"
    def __init__(self, database, cursorId):
        self.database = database
        self.connection = self.database.connection
        self.id = cursorId

    def getURL(self):
        return "%s/%s" % (self.database.getCursorsURL(), self.id)

    def __next__(self):
        "returns the next batch"
        r = self.connection.session.put(self.getURL())
        data = r.json()
        if r.status_code in [400, 404]:
            raise CursorError(data["errorMessage"], self.id, data)
        return r.json()

@implements_iterator
class Query(object):
    "This class is abstract and should not be instanciated. All query classes derive from it"

    def __init__(self, request, database, rawResults):
        "If rawResults = True, the results will be returned as dictionaries instead of Document objects."

        self.rawResults = rawResults
        self.response = request.json()
        if self.response.get("error") and self.response["errorMessage"] != "no match":
            raise QueryError(self.response["errorMessage"], self.response)

        self.request = request
        self.database = database
        self.connection = self.database.connection
        self.currI = 0
        if request.status_code == 201 or request.status_code == 200 or request.status_code == 202:
            self.batchNumber = 1
            try : #if there's only one element
                self.response = {"result" : [self.response["document"]], 'hasMore' : False}
                del(self.response["document"])
            except KeyError:
                pass

            if "hasMore" in self.response and self.response["hasMore"]:
                cursor_id = self.response.get("id","")
                self.cursor = RawCursor(self.database, cursor_id)
            else:
                self.cursor = None
        elif request.status_code == 404:
            self.batchNumber = 0
            self.result = []
        else:
            self._raiseInitFailed(request)

    def _raiseInitFailed(self, request):
        "must be implemented in child, this called if the __init__ fails"
        raise NotImplementedError("Must be implemented in child")

    def _developDoc(self, i):
        """private function that transforms a json returned by ArangoDB into a pyArango Document or Edge"""
        docJson = self.result[i]
        try:
            collection = self.database[docJson["_id"].split("/")[0]]
        except KeyError:
            raise CreationError("result %d is not a valid Document. Try setting rawResults to True" % i)

        if collection.type == CONST.COLLECTION_EDGE_TYPE:
            self.result[i] = Edge(collection, docJson)
        else:
            self.result[i] = Document(collection, docJson)

    def nextBatch(self):
        "become the next batch. raises a StopIteration if there is None"
        self.batchNumber += 1
        self.currI = 0
        try:
            if not self.response["hasMore"] or self.cursor is None:
                raise StopIteration("That was the last batch")
        except KeyError:
            raise AQLQueryError(self.response["errorMessage"], self.query, self.response)

        self.response = next(self.cursor)

    def delete(self):
        "kills the cursor"
        self.connection.session.delete(self.cursor)

    def __next__(self):
        """returns the next element of the query result. Automatomatically calls for new batches if needed"""
        try:
            v = self[self.currI]
        except IndexError:
            self.nextBatch()
        v = self[self.currI]
        self.currI += 1
        return v

    def __iter__(self):
        """Returns an itererator so you can do::

            for doc in query : print doc
        """
        return self

    def __getitem__(self, i):
        "returns a ith result of the query. Raises IndexError if we reached the end of the current batch."
        if not self.rawResults and (not isinstance(self.result[i], (Edge, Document))):
            self._developDoc(i)
        return self.result[i]

    def __len__(self):
        """Returns the number of elements in the query results"""
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
    "AQL queries are attached to and instanciated by a database"
    def __init__(self, database, query, batchSize, bindVars, options, count, fullCount, rawResults = True,
                 json_encoder = None, **moreArgs):
        # fullCount is passed in the options dict per https://docs.arangodb.com/3.1/HTTP/AqlQueryCursor/AccessingCursors.html
        options["fullCount"] = fullCount
        payload = {'query' : query, 'batchSize' : batchSize, 'bindVars' : bindVars, 'options' : options, 'count' : count}
        payload.update(moreArgs)

        self.query = query
        self.database = database
        self.connection = self.database.connection
        self.connection.reportStart(query)
        request = self.connection.session.post(database.getCursorsURL(), data = json.dumps(payload, cls=json_encoder, default=str))
        self.connection.reportItem()

        try:
            Query.__init__(self, request, database, rawResults)
        except QueryError as e:
            raise AQLQueryError( message = e.message, query = self.query, errors = e.errors)

    def explain(self, bindVars = None, allPlans = False):
        """Returns an explanation of the query. Setting allPlans to True will result in ArangoDB returning all possible plans. False returns only the optimal plan"""
        if bindVars is None:
            bindVars = {}
        return self.database.explainAQLQuery(self.query, bindVars, allPlans)

    def _raiseInitFailed(self, request):
        data = request.json()
        raise AQLQueryError(data["errorMessage"], self.query, data)

class Cursor(Query):
    "Cursor queries are attached to and instanciated by a database, use them to continue from where you left"
    def __init__(self, database, cursorId, rawResults):
        self.rawResults = rawResults
        self._developed = set()
        self.batchNumber = 1
        self.cursor = RawCursor(database, cursorId)
        self.response = next(self.cursor)

    def _raiseInitFailed(self, request):
        data = request.json()
        raise CursorError(data["errorMessage"], self.id, data)


class SimpleQuery(Query):
    "Simple queries are attached to and instanciated by a collection"
    def __init__(self, collection, queryType, rawResults, json_encoder = None,
                 **queryArgs):

        self.collection = collection
        self.connection = self.collection.database.connection

        payload = {'collection' : collection.name}
        payload.update(queryArgs)
        payload = json.dumps(payload, cls=json_encoder, default=str)
        URL = "%s/simple/%s" % (collection.database.getURL(), queryType)
        request = self.connection.session.put(URL, data = payload)

        Query.__init__(self, request, collection.database, rawResults)

    def _raiseInitFailed(self, request):
        data = request.json()
        raise SimpleQueryError(data["errorMessage"], data)

    def _developDoc(self, i):
        docJson = self.result[i]
        if self.collection.type == CONST.COLLECTION_EDGE_TYPE:
            self.result[i] = Edge(self.collection, docJson)
        else:
            self.result[i] = Document(self.collection, docJson)
