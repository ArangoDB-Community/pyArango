from .theExceptions import CreationError, ArangoError

class Transactions(object):
    """Manages transactions"""
    def __init__(self, database):
        super(Transactions, self).__init__()
        self.database = database
        self.URL = self.database.getTransactionURL()
        self.transactions = {}
        
    def begin(self, collections, allowImplicit, lockTimeout, maxTransactionSize, waitForSync=False):
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
        payload = {
            "collections": collections,
            "allowImplicit": allowImplicit,
            "lockTimeout": lockTimeout,
            "maxTransactionSize": maxTransactionSize,
            "waitForSync": waitForSync,
        }
        response = self.database.connection.session.post(self.URL, data=payload)

        data = response.json()
        if data["error"]:
            raise CreationError(data["errorMessage"], data)
        self.transactions[data["id"]] = data["result"]
        return data["result"]

    def get(self, transaction_id):
        """
        Return the status of a runnning transation
        """
        response = self.database.connection.session.get(self.URL + "/%s" % transaction_id)
        data = response.json()
        if data["error"]:
            raise ArangoError(data)
        return data["result"]

    def commit(self, transaction_id):
        """
        Commit an existing transaction
        """

        response = self.database.connection.session.put(self.URL + "/%s" % transaction_id)
        data = response.json()
        if data["error"]:
            raise ArangoError(data["errorMessage"], data)
        del self.transactions[data["id"]]
        return data["result"]

    def delete(self, transaction_id):
        """
        Delete an existing transaction
        """

        response = self.database.connection.session.delete(self.URL + "/%s" % transaction_id)
        data = response.json()
        if data["error"]:
            raise ArangoError(data["errorMessage"], data)
        del self.transactions[data["id"]]
        return data["result"]
