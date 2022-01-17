import json
import logging
import types

from . import collection as COL
from . import consts as CONST
from . import graph as GR

from .action import DatabaseAction
from .document import Document
from .foxx import Foxx
from .tasks import Tasks
from .graph import Graph
from .query import AQLQuery
from .theExceptions import CreationError, UpdateError, AQLQueryError, TransactionError, AQLFetchError

__all__ = ["Database", "DBHandle"]

class Database(object):
    """Databases are meant to be instanciated by connections"""

    def __init__(self, connection, name):

        self.name = name
        self.connection = connection
        self.action = DatabaseAction(self)
        self.collections = {}
        self.graphs = {}
        self.foxx = Foxx(self)
        self.tasks = Tasks(self)

        self.reload()

    def getURL(self):
        return '%s/_db/%s/_api' % (self.connection.getEndpointURL(), self.name)

    def getCollectionsURL(self):
        return '%s/collection' % (self.getURL())
    
    def getCursorsURL(self):
        return '%s/cursor' % (self.getURL())
        
    def getExplainURL(self):
        return '%s/explain' % (self.getURL())
        
    def getGraphsURL(self):
        return "%s/gharial" % self.getURL()
    
    def getTransactionURL(self):
        return "%s/transaction" % self.getURL()
    
    def reloadCollections(self):
        "reloads the collection list."
        r = self.connection.session.get(self.getCollectionsURL())
        data = r.json()
        if r.status_code == 200:
            self.collections = {}

            for colData in data["result"]:
                colName = colData['name']
                if colData['isSystem']:
                    colObj = COL.SystemCollection(self, colData)
                else:
                    try:
                        colClass = COL.getCollectionClass(colName)
                        colObj = colClass(self, colData)
                    except KeyError:
                        if colData["type"] == CONST.COLLECTION_EDGE_TYPE:
                            colObj = COL.Edges(self, colData)
                        elif colData["type"] == CONST.COLLECTION_DOCUMENT_TYPE:
                            colObj = COL.Collection(self, colData)
                        else:
                            print(("Warning!! Collection of unknown type: %d, trying to load it as Collection nonetheless." % colData["type"]))
                            colObj = COL.Collection(self, colData)

                self.collections[colName] = colObj
        else:
            raise UpdateError(data["errorMessage"], data)

    def reloadGraphs(self):
        "reloads the graph list"
        r = self.connection.session.get(self.getGraphsURL())
        data = r.json()
        if r.status_code == 200:
            self.graphs = {}
            for graphData in data["graphs"]:
                try:
                    self.graphs[graphData["_key"]] = GR.getGraphClass(graphData["_key"])(self, graphData)
                except KeyError:
                    self.graphs[graphData["_key"]] = Graph(self, graphData)
        else:
            raise UpdateError(data["errorMessage"], data)

    def reload(self):
        "reloads collections and graphs"
        self.reloadCollections()
        self.reloadGraphs()
        self.foxx.reload()

    def createCollection(self, className = 'Collection', **colProperties):
        """Creates a collection and returns it.
        ClassName the name of a class inheriting from Collection or Egdes, it can also be set to 'Collection' or 'Edges' in order to create untyped collections of documents or edges.
        Use colProperties to put things such as 'waitForSync = True' (see ArangoDB's doc
        for a full list of possible arugments). If a '_properties' dictionary is defined in the collection schema, arguments to this function overide it"""

        colClass = COL.getCollectionClass(className)

        if len(colProperties) > 0:
            colProperties = dict(colProperties)
        else:
            try:
                colProperties = dict(colClass._properties)
            except AttributeError:
                colProperties = {}

        if className != 'Collection' and className != 'Edges' and 'name' not in colProperties:
            colProperties['name'] = className
        else:
            if 'name' not in colProperties:
                raise ValueError("a 'name' argument mush be supplied if you want to create a generic collection")

        if colProperties['name'] in self.collections:
            raise CreationError("Database %s already has a collection named %s" % (self.name, colProperties['name']) )

        if issubclass(colClass, COL.Edges) or colClass.__class__ is COL.Edges:
            colProperties["type"] = CONST.COLLECTION_EDGE_TYPE
        else:
            colProperties["type"] = CONST.COLLECTION_DOCUMENT_TYPE

        payload = json.dumps(colProperties, default=str)
        req = self.connection.session.post(self.getCollectionsURL(), data = payload)
        data = req.json()

        if req.status_code == 200 and not data["error"]:
            col = colClass(self, data)
            self.collections[col.name] = col
            return self.collections[col.name]
        else:
            raise CreationError(data["errorMessage"], data)

    def fetchDocument(self, _id):
        "fetchs a document using it's _id"
        sid = _id.split("/")
        return self[sid[0]][sid[1]]

    def createGraph(self, name, createCollections = True, isSmart = False, numberOfShards = None, smartGraphAttribute = None, replicationFactor = None, writeConcern = None):
        """Creates a graph and returns it. 'name' must be the name of a class inheriting from Graph.
        Checks will be performed to make sure that every collection mentionned in the edges definition exist. Raises a ValueError in case of
        a non-existing collection."""

        def _checkCollectionList(lst):
            for colName in lst:
                if not COL.isCollection(colName):
                    raise ValueError("'%s' is not a defined Collection" % colName)

        graphClass = GR.getGraphClass(name)

        ed = []
        for e in graphClass._edgeDefinitions:
            if not COL.isEdgeCollection(e.edgesCollection):
                raise ValueError("'%s' is not a defined Edge Collection" % e.edgesCollection)
            _checkCollectionList(e.fromCollections)
            _checkCollectionList(e.toCollections)

            ed.append(e.toJson())

        _checkCollectionList(graphClass._orphanedCollections)

        options = {}
        if numberOfShards:
            options['numberOfShards'] = numberOfShards
        if smartGraphAttribute:
            options['smartGraphAttribute'] = smartGraphAttribute
        if replicationFactor:
            options['replicationFactor'] = replicationFactor
        if writeConcern:
            options['writeConcern'] = writeConcern

        payload = {
                "name": name,
                "edgeDefinitions": ed,
                "orphanCollections": graphClass._orphanedCollections
            }

        if isSmart:
                payload['isSmart'] = isSmart

        if options:
            payload['options'] = options

        payload = json.dumps(payload)

        r = self.connection.session.post(self.getGraphsURL(), data = payload)
        data = r.json()

        if r.status_code == 201 or r.status_code == 202:
            self.graphs[name] = graphClass(self, data["graph"])
        else:
            raise CreationError(data["errorMessage"], data)
        return self.graphs[name]

    def createSatelliteGraph(self, name, createCollections = True):
        return self.createGraph(name, createCollections, False, None, None, "satellite", None);

    def hasCollection(self, name):
        """returns true if the databse has a collection by the name of 'name'"""
        return name in self.collections

    def hasGraph(self, name):
        """returns true if the databse has a graph by the name of 'name'"""
        return name in self.graphs

    def __contains__(self, name):
        """if name in database"""
        return self.hasCollection(name) or self.hasGraph(name)

    def dropAllCollections(self):
        """drops all public collections (graphs included) from the database"""
        for graph_name in self.graphs:
            self.graphs[graph_name].delete()
        for collection_name in self.collections:
            # Collections whose name starts with '_' are system collections
            if not collection_name.startswith('_'):
                self[collection_name].delete()
        return

    def AQLQuery(self, query, batchSize = 100, rawResults = False, bindVars = None, options = None, count = False, fullCount = False,
                 json_encoder = None, **moreArgs):
        """Set rawResults = True if you want the query to return dictionnaries instead of Document objects.
        You can use **moreArgs to pass more arguments supported by the api, such as ttl=60 (time to live)"""
        if bindVars is None:
            bindVars = {}
        if options is None:
            options = {}

        return AQLQuery(self, query, rawResults = rawResults, batchSize = batchSize, bindVars  = bindVars, options = options, count = count, fullCount = fullCount,
                        json_encoder = json_encoder, **moreArgs)

    def __get_logger(self, logger, log_level):
        if logger is None:
            return None
        return getattr(logger, logging.getLevelName(log_level).lower())

    def fetch_element(
            self, aql_query, bind_vars=None, dont_raise_error_if_empty=False,
            default_output=None, logger=None, log_level=logging.DEBUG
    ):
        """Fetch element by running a query.

        Parameters
        ----------
        aql_query : str
            aql query string.
        bind_vars : dict, optional
            dictonary of bind variables (the default is None)
        dont_raise_error_if_empty: bool, optional
            do not raise error if the returned is empty. (the default is False)
        default_output: dict, optional
            default output if no value is returned. (the default is None)
        logger : Logger, optional
            logger to log the query and result.
            (the default is None means don't log)
        log_level: Logger.loglevel, optional
            level of the log. (the default is logging.DEBUG)

        Raises
        ------
        AQLFetchError
            When unable to fetch results or more than one 1 results returned.

        Returns
        -------
        any
            an element returned by query.

        """
        log = self.__get_logger(logger, log_level)
        if log is not None:
            log(aql_query)
        if bind_vars is None:
            bind_vars = {}
        response = self.AQLQuery(
            aql_query, bindVars=bind_vars, rawResults=True
        ).response
        if log is not None:
            log(response["result"])
        num_results = len(response["result"])
        if num_results == 1:
            return response["result"][0]
        if dont_raise_error_if_empty and num_results == 0:
            return default_output
        raise AQLFetchError(
            "No results matched for query." if num_results == 0
            else "More than one results received"
        )

    def fetch_list(
            self, aql_query, bind_vars=None, batch_size=200,
            dont_raise_error_if_empty=False, logger=None,
            log_level=logging.DEBUG
    ):
        """Fetch list of elements by running a query and merging all the batches.

        Parameters
        ----------
        aql_query : str
            aql query string.
        bind_vars : dict, optional
            dictonary of bind variables (the default is None)
        batch_size : int, optional
            fetching batch size (the default is 200)
        dont_raise_error_if_empty: bool, optional
            do not raise error if the returned is empty. (the default is False)
        logger : Logger, optional
            logger to log the query and result.
            (the default is None means don't log)
        log_level: Logger.loglevel, optional
            level of the log. (the default is logging.DEBUG)

        Raises
        ------
        AQLFetchError
            When unable to fetch results

        Returns
        -------
        list(any)
            a list returned by query.

        """
        try:
            log = self.__get_logger(logger, log_level)
            if log is not None:
                log(aql_query)
            query = self.AQLQuery(
                aql_query, batchSize=batch_size, rawResults=True,
                bindVars=(bind_vars if bind_vars is not None else {})
            )
            batch_index = 0
            result = []
            while True:
                if len(query.response['result']) == 0:
                    break
                result.extend(query.response['result'])
                batch_index += 1
                query.nextBatch()
        except StopIteration:
            if log is not None:
                log(result)
            if len(result) != 0:
                return result
        except:
            raise
        if batch_index == 0 and dont_raise_error_if_empty:
            return []
        raise AQLFetchError(
            "No results matched for query in fetching the batch index: %s." % (
                batch_index
            )
        )

    def fetch_list_as_batches(
            self, aql_query, bind_vars=None, batch_size=200,
            dont_raise_error_if_empty=False, logger=None,
            log_level=logging.DEBUG
    ):
        """Fetch list of elements as batches by running the query.

        Generator which yeilds each batch as result.

        Parameters
        ----------
        aql_query : str
            aql query string.
        bind_vars : dict, optional
            dictonary of bind variables (the default is None)
        batch_size : int, optional
            fetching batch size (the default is 200)
        dont_raise_error_if_empty: bool, optional
            do not raise error if the returned is empty. (the default is False)
        logger : Logger, optional
            logger to log the query and result.
            (the default is None means don't log)
        log_level: Logger.loglevel, optional
            level of the log. (the default is logging.DEBUG)

        Raises
        ------
        AQLFetchError
            When unable to fetch results

        Returns
        -------
        list(any)
            a list returned by query.

        """
        try:
            log = self.__get_logger(logger, log_level)
            if log is not None:
                log(aql_query)
            query = self.AQLQuery(
                aql_query, batchSize=batch_size, rawResults=True,
                bindVars=(bind_vars if bind_vars is not None else {})
            )
            batch_index = 0
            while True:
                if len(query.response['result']) == 0:
                    break
                if log is not None:
                    log(
                        "batch_result for index '%s': %s",
                        batch_index, query.response['result']
                    )
                yield query.response['result']
                batch_index += 1
                query.nextBatch()
        except StopIteration:
            return
        except:
            raise
        if batch_index == 0 and dont_raise_error_if_empty:
            return
        raise AQLFetchError(
            "No results matched for query in fetching the batch index: %s." % (
                batch_index
            )
        )

    def no_fetch_run(
            self, aql_query, bind_vars=None, logger=None,
            log_level=logging.DEBUG
    ):
        """Run query which doesn't have a return.

        Parameters
        ----------
        aql_query : str
            aql query string.
        bind_vars : dict, optional
            dictonary of bind variables (the default is None)
        logger : Logger, optional
            logger to log the query and result.
            (the default is None means don't log)
        log_level: Logger.loglevel, optional
            level of the log. (the default is logging.DEBUG)

        Raises
        ------
        AQLFetchError
            When able to fetch results.

        """
        log = self.__get_logger(logger, log_level)
        if log is not None:
            log(aql_query)
        response = self.AQLQuery(
            aql_query, rawResults=True,
            bindVars=(bind_vars if bind_vars is not None else {})
        ).response
        if log is not None:
            log(response["result"])
        if len(response["result"]) == 0:
            return
        raise AQLFetchError("No results should be returned for the query.")

    def explainAQLQuery(self, query, bindVars = None, allPlans = False):
        """Returns an explanation of the query. Setting allPlans to True will result in ArangoDB returning all possible plans. False returns only the optimal plan"""
        if bindVars is None:
            bindVars = {}

        payload = {'query' : query, 'bindVars' : bindVars, 'allPlans' : allPlans}
        request = self.connection.session.post(self.getExplainURL(), data = json.dumps(payload, default=str))
        return request.json()

    def validateAQLQuery(self, query, bindVars = None, options = None):
        "returns the server answer is the query is valid. Raises an AQLQueryError if not"
        if bindVars is None:
            bindVars = {}
        if options is None:
            options = {}
        payload = {'query' : query, 'bindVars' : bindVars, 'options' : options}
        r = self.connection.session.post(self.getCursorsURL(), data = json.dumps(payload, default=str))
        data = r.json()
        if r.status_code == 201 and not data["error"]:
            return data
        else:
            raise AQLQueryError(data["errorMessage"], query, data)

    def transaction(self, collections, action, waitForSync = False, lockTimeout = None, params = None):
        """Execute a server-side transaction"""
        payload = {
                "collections": collections,
                "action": action,
                "waitForSync": waitForSync}
        if lockTimeout is not None:
                payload["lockTimeout"] = lockTimeout
        if params is not None:
            payload["params"] = params

        self.connection.reportStart(action)

        r = self.connection.session.post(self.getTransactionURL(), data = json.dumps(payload, default=str))

        self.connection.reportItem()

        data = r.json()

        if (r.status_code == 200 or r.status_code == 201 or r.status_code == 202) and not data.get("error"):
            return data
        else:
            raise TransactionError(data["errorMessage"], action, data)

    def __repr__(self):
        return "ArangoDB database: %s" % self.name

    def __contains__(self, _id):
        """allows to check if _id:str is the id of an existing document"""
        col, key = _id.split('/')
        try:
            return key in self[col]
        except KeyError:
            return False

    def __getitem__(self, collectionName):
        """use database[collectionName] to get a collection from the database"""
        try:
            return self.collections[collectionName]
        except KeyError:
            self.reload()
            try:
                return self.collections[collectionName]
            except KeyError:
                raise KeyError("Can't find any collection named : %s" % collectionName)

class DBHandle(Database):
    "As the loading of a Database also triggers the loading of collections and graphs within. Only handles are loaded first. The full database are loaded on demand in a fully transparent manner."
    def __init__(self, connection, name):
        self.connection = connection
        self.name = name

    def __getattr__(self, k):
        name = Database.__getattribute__(self, 'name')
        connection = Database.__getattribute__(self, 'connection')
        Database.__init__(self, connection, name)
        return Database.__getattribute__(self, k)
