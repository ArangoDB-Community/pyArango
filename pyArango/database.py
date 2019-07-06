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

        self.transactions = set()
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
        ClassName the name of a class inheriting from Collection or Egdes, it can also be set to 'Collection' or 'Edges'
        in order to create untyped collections of documents or edges.
        Use colProperties to put things such as 'waitForSync = True'.
        If a '_properties' dictionary is defined in the collection schema, arguments to this function overide it
        
        Potential parameters (as of 3.5)
        ---------------------------------

        name: The name of the collection.

        waitForSync: If true then the data is synchronized to disk before returning from
        a document create, update, replace or removal operation. (default: false)

        doCompact: whether or not the collection will be compacted (default is true)
        This option is meaningful for the MMFiles storage engine only.

        journalSize: The maximal size of a journal or datafile in bytes. The value must
        be at least 1048576 (1 MiB). (The default is a configuration parameter) This
        option is meaningful for the MMFiles storage engine only.

        isSystem: If true, create a system collection. In this case collection-name
        should start with an underscore. End users should normally create non-system
        collections only. API implementors may be required to create system collections
        in very special occasions, but normally a regular collection will do. (The
        default is false)

        isVolatile: If true then the collection data is kept in-memory only and not made
        persistent. Unloading the collection will cause the collection data to be
        discarded. Stopping or re-starting the server will also cause full loss of data
        in the collection. Setting this option will make the resulting collection be
        slightly faster than regular collections because ArangoDB does not enforce any
        synchronization to disk and does not calculate any CRC checksums for datafiles
        (as there are no datafiles). This option should therefore be used for cache-type
        collections only, and not for data that cannot be re-created otherwise. (The
        default is false) This option is meaningful for the MMFiles storage engine only.

        keyOptions: additional options for key generation. If specified, then keyOptions
        should be a JSON array containing the following attributes:

        type: specifies the type of the key generator. The currently available
        generators are traditional, autoincrement, uuid and padded. The traditional key
        generator generates numerical keys in ascending order. The autoincrement key
        generator generates numerical keys in ascending order, the inital offset and the
        spacing can be configured The padded key generator generates keys of a fixed
        length (16 bytes) in ascending lexicographical sort order. This is ideal for
        usage with the RocksDB engine, which will slightly benefit keys that are
        inserted in lexicographically ascending order. The key generator can be used in
        a single-server or cluster. The uuid key generator generates universally unique
        128 bit keys, which are stored in hexadecimal human-readable format. This key
        generator can be used in a single-server or cluster to generate “seemingly
        random” keys. The keys produced by this key generator are not lexicographically
        sorted.

        allowUserKeys: if set to true, then it is allowed to supply own key values in
        the _key attribute of a document. If set to false, then the key generator will
        solely be responsible for generating keys and supplying own key values in the
        _key attribute of documents is considered an error.

        increment: increment value for autoincrement key generator. Not used for other
        key generator types.

        offset: Initial offset value for autoincrement key generator. Not used for other
        key generator types.

        type: (The default is 2): the type of the collection to create. The following
        values for type are valid:

        2: document collection 3: edge collection

        indexBuckets: The number of buckets into which indexes using a hash table are
        split. The default is 16 and this number has to be a power of 2 and less than or
        equal to 1024. For very large collections one should increase this to avoid long
        pauses when the hash table has to be initially built or resized, since buckets
        are resized individually and can be initially built in parallel. For example, 64
        might be a sensible value for a collection with 100 000 000 documents.
        Currently, only the edge index respects this value, but other index types might
        follow in future ArangoDB versions. Changes (see below) are applied when the
        collection is loaded the next time. This option is meaningful for the MMFiles
        storage engine only.

        numberOfShards: (The default is 1): in a cluster, this value determines the
        number of shards to create for the collection. In a single server setup, this
        option is meaningless.

        shardKeys: (The default is [ “_key” ]): in a cluster, this attribute determines
        which document attributes are used to determine the target shard for documents.
        Documents are sent to shards based on the values of their shard key attributes.
        The values of all shard key attributes in a document are hashed, and the hash
        value is used to determine the target shard. Note: Values of shard key
        attributes cannot be changed once set. This option is meaningless in a single
        server setup.

        replicationFactor: (The default is 1): in a cluster, this attribute determines
        how many copies of each shard are kept on different DBServers. The value 1 means
        that only one copy (no synchronous replication) is kept. A value of k means that
        k-1 replicas are kept. Any two copies reside on different DBServers. Replication
        between them is synchronous, that is, every write operation to the “leader” copy
        will be replicated to all “follower” replicas, before the write operation is
        reported successful.

        If a server fails, this is detected automatically and one of the servers holding
        copies take over, usually without an error being reported.

        distributeShardsLike: (The default is ”“): in an Enterprise Edition cluster,
        this attribute binds the specifics of sharding for the newly created collection
        to follow that of a specified existing collection. Note: Using this parameter
        has consequences for the prototype collection. It can no longer be dropped,
        before the sharding-imitating collections are dropped. Equally, backups and
        restores of imitating collections alone will generate warnings (which can be
        overridden) about missing sharding prototype.

        shardingStrategy: This attribute specifies the name of the sharding strategy to
        use for the collection. Since ArangoDB 3.4 there are different sharding
        strategies to select from when creating a new collection. The selected
        shardingStrategy value will remain fixed for the collection and cannot be
        changed afterwards. This is important to make the collection keep its sharding
        settings and always find documents already distributed to shards using the same
        initial sharding algorithm.

        The available sharding strategies are:

        community-compat: default sharding used by ArangoDB Community Edition before
        version 3.4 enterprise-compat: default sharding used by ArangoDB Enterprise
        Edition before version 3.4 enterprise-smart-edge-compat: default sharding used
        by smart edge collections in ArangoDB Enterprise Edition before version 3.4
        hash: default sharding used for new collections starting from version 3.4
        (excluding smart edge collections) enterprise-hash-smart-edge: default sharding
        used for new smart edge collections starting from version 3.4 If no sharding
        strategy is specified, the default will be hash for all collections, and
        enterprise-hash-smart-edge for all smart edge collections (requires the
        Enterprise Edition of ArangoDB). Manually overriding the sharding strategy does
        not yet provide a benefit, but it may later in case other sharding strategies
        are added.

        smartJoinAttribute: In an Enterprise Edition cluster, this attribute determines
        an attribute of the collection that must contain the shard key value of the
        referred-to smart join collection. Additionally, the shard key for a document in
        this collection must contain the value of this attribute, followed by a colon,
        followed by the actual primary key of the document. This feature can only be
        used in the Enterprise Edition and requires the distributeShardsLike attribute
        of the collection to be set to the name of another collection. It also requires
        the shardKeys attribute of the collection to be set to a single shard key
        attribute, with an additional ‘:’ at the end. A further restriction is that
        whenever documents are stored or updated in the collection, the value stored in
        the smartJoinAttribute must be a string.
        """

        colClass = COL.getCollectionClass(className)

        if len(colProperties) > 0:
            colProperties = dict(colProperties)
        else:
            try:
                colProperties = dict(colClass._properties)
            except AttributeError:
                colProperties = {}

        if className != 'Collection' and className != 'Edges':
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
        r = self.connection.session.post(self.getCollectionsURL(), data = payload)
        data = r.json()

        if r.status_code == 200 and not data["error"]:
            col = colClass(self, data)
            self.collections[col.name] = col
            return self.collections[col.name]
        else:
            raise CreationError(data["errorMessage"], data)

    def fetchDocument(self, _id):
        "fetchs a document using it's _id"
        sid = _id.split("/")
        return self[sid[0]][sid[1]]

    def createGraph(self, name, createCollections = True, isSmart = False, numberOfShards = None, smartGraphAttribute = None):
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

    def hasCollection(self, name):
        """returns true if the databse has a collection by the name of 'name'"""
        return name in self.collections

    def hasGraph(self, name):
        """returns true if the databse has a graph by the name of 'name'"""
        return name in self.graphs

    def dropAllCollections(self):
        """drops all public collections (graphs included) from the database"""
        for graph_name in self.graphs:
            self.graphs[graph_name].delete()
        for collection_name in self.collections:
            # Collections whose name starts with '_' are system collections
            if not collection_name.startswith('_'):
                self[collection_name].delete()
        return

    def AQLQuery(self, query, batchSize = 100, rawResults = False, bindVars = {}, options = {}, count = False, fullCount = False,
                 json_encoder = None, **moreArgs):
        """Set rawResults = True if you want the query to return dictionnaries instead of Document objects.
        You can use **moreArgs to pass more arguments supported by the api, such as ttl=60 (time to live)"""
        return AQLQuery(self, query, rawResults = rawResults, batchSize = batchSize, bindVars  = bindVars, options = options, count = count, fullCount = fullCount,
                        json_encoder = json_encoder, **moreArgs)

    def transactionBegin(self, collections, allowImplicit, lockTimeout, maxTransactionSize, waitForSync=False):
        """
        Begin a transaction on the server, return value contains the created transaction Id.

        Parameters
        ----------
        collections: collections must be a Dict object that can have one or all sub-attributes read,
        write or exclusive, each being an array of collection names or a single collection name as
        string. Collections that will be written to in the transaction must be declared with the write
        or exclusive attribute or it will fail, whereas non-declared collections from which is solely
        read will be added lazily. The optional sub-attribute allowImplicit can be set to false to let
        transactions fail in case of undeclared collections for reading. Collections for reading should
        be fully declared if possible, to avoid deadlocks. See locking and isolation for more information.

        waitForSync: an optional boolean flag that, if set, will force the transaction to write all data
        to disk before returning.

        allowImplicit: Allow reading from undeclared collections.

        lockTimeout: an optional numeric value that can be used to set a timeout for waiting on collection
        locks. If not specified, a default value will be used. Setting lockTimeout to 0 will make ArangoDB
        not time out waiting for a lock.

        maxTransactionSize: Transaction size limit in bytes. Honored by the RocksDB storage engine only.

        """
        response = self.connection.session.post(self.getTansactionUrl())
        data = response.json()
        if data["error"]:
            raise CreationError(data["errorMessage"], data)
        self.self.transactions.add(response["id"])
        return response["result"]

    def getTransaction(self, transaction_id):
        """
        Return the status of a runnning transation
        """
        response = self.connection.session.get(self.getTansactionUrl() + "/%s" % transaction_id)
        data = response.json()
        if data["error"]:
            raise ArangoError(data["errorMessage"], data)
        return response["result"]

    def commitTransaction(self, transaction_id):
        """
        Commit an existing transaction
        """

        response = self.connection.session.put(self.getTansactionUrl() + "/%s" % transaction_id)
        data = response.json()
        if data["error"]:
            raise ArangoError(data["errorMessage"], data)
        self.self.transactions.remove(response["id"])
        return response["result"]

    def deleteTransaction(self, transaction_id):
        """
        Delete an existing transaction
        """

        response = self.connection.session.delete(self.getTansactionUrl() + "/%s" % transaction_id)
        data = response.json()
        if data["error"]:
            raise ArangoError(data["errorMessage"], data)
        self.self.transactions.remove(response["id"])
        return response["result"]

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
                if len(query.response['result']) is 0:
                    break
                result.extend(query.response['result'])
                batch_index += 1
                query.nextBatch()
        except StopIteration:
            if log is not None:
                log(result)
            if len(result) is not 0:
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
                if len(query.response['result']) is 0:
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
        if len(response["result"]) is 0:
            return
        raise AQLFetchError("No results should be returned for the query.")

    def explainAQLQuery(self, query, bindVars={}, allPlans = False):
        """Returns an explanation of the query. Setting allPlans to True will result in ArangoDB returning all possible plans. False returns only the optimal plan"""
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
