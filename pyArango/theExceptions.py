class pyArangoException(Exception):
    """The calss from witch all Exceptions inherit"""
    def __init__(self, message, errors = None):
        Exception.__init__(self, message)
        if errors is None:
            errors = {}
        self.message = message
        self.errors = errors

    def __str__(self):
        return self.message + ". Errors: " + str(self.errors)

class ConnectionError(pyArangoException):
    """Something went wrong with the connection"""
    def __init__(self, message, URL, statusCode = "", errors = None):
        if errors is None:
            errors = {}
        mes = "%s. URL: %s, status: %s" % (message, URL, statusCode)
        pyArangoException.__init__(self, mes, errors)

class ArangoError(pyArangoException):
    """a generic arangodb error object"""
    def __init__(self, errorObject):
        self.errorNum = errorObject['errorNum']
        pyArangoException.__init__(self, errorObject['errorMessage'], errorObject)

class CreationError(pyArangoException):
    """Something went wrong when creating something"""
    def __init__(self, message, errors = None):
        if errors is None:
            errors = {}
        pyArangoException.__init__(self, message, errors)

class IndexError(pyArangoException):
    """wasn't able to get the index"""
    def __init__(self, message, errors = None):
        if errors is None:
            errors = {}
        pyArangoException.__init__(self, message, errors)

class UpdateError(pyArangoException):
    """Something went wrong when updating something"""
    def __init__(self, message, errors = None):
        if errors is None:
            errors = {}
        pyArangoException.__init__(self, message, errors)

class DeletionError(pyArangoException):
    """Something went wrong when deleting something"""
    def __init__(self, message, errors = None):
        if errors is None:
            errors = {}
        pyArangoException.__init__(self, message, errors)

class TraversalError(pyArangoException):
    """Something went wrong when doing a graph traversal"""
    def __init__(self, message, errors = None):
        if errors is None:
            errors = {}
        pyArangoException.__init__(self, message, errors)

class ValidationError(pyArangoException):
    """Something went wrong when validating something"""
    def __init__(self, message, errors = None):
        if errors is None:
            errors = {}
        pyArangoException.__init__(self, message, errors)

class SchemaViolation(pyArangoException):
    """Raised when someone tries to add a new field to an object belonging a to a Collection with enforced schema"""
    def __init__(self, collection, field, errors = None):
        if errors is None:
            errors = {}
        message = "Collection '%s' does not have a field '%s' in it's schema" % (collection.__name__, field)
        pyArangoException.__init__(self, message, errors)

class InvalidDocument(pyArangoException):
    """Raised when a Document does not respect schema/validation defined in its collection"""
    def __init__(self, errors):
        message = "Unsuccesful validation"
        self.strErrors = []
        for k, v in errors.items():
            self.strErrors.append("%s -> %s" % (k, v))
        self.strErrors = '\n\t'.join(self.strErrors)

        pyArangoException.__init__(self, message, errors)

    def __str__(self):
        strErrors = []
        for k, v in self.errors.items():
            strErrors.append("%s -> %s" % (k, v))
        strErrors = '\n\t'.join(strErrors)
        return self.message + ":\n\t" + strErrors

class SimpleQueryError(pyArangoException):
    """Something went wrong with a simple query"""
    def __init__(self, message, errors = None):
        if errors is None:
            errors = {}
        pyArangoException.__init__(self, message, errors)

class BulkOperationError(pyArangoException):
    """Something went wrong in one of the bulk operations. This error contains more errors"""
    def __init__(self, message):
        self._errors = []
        self._errmsgs = []
        self._documents = []
        pyArangoException.__init__(self, "Batch error - + " + message)

    def addBulkError(self, error, document):
        self._errors.append(error)
        self._errmsgs.append(str(error))
        self._documents.append(document)
    def __str__(self):
        strErrors = []
        i = 0
        for errMsg in self._errmsgs:
            err = "<unknown>"
            docstr = "<unknown>"
            try:
                err = errMsg
            except:
                pass
            try:
                docstr = self._documents[i]
            except:
                pass
            strErrors.append("\t<%s> -> %s" % (err, docstr))
            i+=1
        strErrors = '\n\t'.join(strErrors)
        return self.message + ":\n\t" + strErrors
        
class QueryError(pyArangoException):
    """Something went wrong with an aql query"""
    def __init__(self, message, errors = None):
        if errors is None:
            errors = {}
        pyArangoException.__init__(self, message, errors)

class AQLQueryError(pyArangoException):
    """Something went wrong with an aql query"""
    def __init__(self, message, query, errors = None):
        if errors is None:
            errors = {}
        lq = []
        for i, ll in enumerate(query.split("\n")):
            lq.append("%s: %s" % (i+1, ll))
        lq = '\n'.join(lq)

        message = "Error in:\n%s.\n->%s" % (lq, message)
        pyArangoException.__init__(self, message, errors)

class CursorError(pyArangoException):
    """Something went wrong when trying to fetch data with a cursor"""
    def __init__(self, message, cursorId, errors = None):
        if errors is None:
            errors = {}
        message = "Unable to retreive data for cursor %s: %s" % (cursorId, message)
        pyArangoException.__init__(self, message, errors)

class TransactionError(pyArangoException):
    """Something went wrong with a transaction"""
    def __init__(self, message, action, errors = None):
        if errors is None:
            errors = {}
        message = "Error in: %s.\n->%s" % (action, message)
        pyArangoException.__init__(self, message, errors)

class AbstractInstanciationError(Exception):
    """Raised when someone tries to instanciate an abstract class"""
    def __init__(self, cls):
        self.cls = cls
        self.message = "%s is abstract and is not supposed to be instanciated. Collections my inherit from it" % self.cls.__name__
        Exception.__init__(self, self.message)

    def __str__(self):
        return self.message

class ExportError(pyArangoException):
    """ Something went wrong using the export cursor """
    def __init__(self, message, errors = None ):
        if errors is None:
            errors = {}
        pyArangoException.__init__(self, message, errors)

class DocumentNotFoundError(pyArangoException):
    def __init__(self, message, errors = None):
        if errors is None:
            errors = {}
        pyArangoException.__init__(self, message, errors)


class AQLFetchError(Exception):
    """Raised error when fetching the data."""

    def __init__(self, err_message):
        """Error when unable to fetch.

        Parameters
        ----------
        err_message : str
            error message.

        """
        Exception.__init__(self, err_message)

