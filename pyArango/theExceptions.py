class pyArangoException(Exception) :
    """The calss from witch all Exceptions inherit"""
    def __init__(self, message, errors = {}) :
        Exception.__init__(self, message)
        self.message = message
        self.errors = errors

    def __str__(self) :
        return self.message + ". Errors: " + str(self.errors)

class ConnectionError(pyArangoException) :
    """Something went wrong with the connection"""
    def __init__(self, message, URL, statusCode="", errors = {}) :
        mes = "%s. URL: %s, status: %s" % (message, URL, statusCode)
        pyArangoException.__init__(self, mes, errors)

class CreationError(pyArangoException) :
    """Something went wrong when creating something"""
    def __init__(self, message, errors = {}) :
        pyArangoException.__init__(self, message, errors)

class UpdateError(pyArangoException) :
    """Something went wrong when updating something"""
    def __init__(self, message, errors = {}) :
        pyArangoException.__init__(self, message, errors)

class DeletionError(pyArangoException) :
    """Something went wrong when deleting something"""
    def __init__(self, message, errors = {}) :
        pyArangoException.__init__(self, message, errors)

class TraversalError(pyArangoException) :
    """Something went wrong when doing a graph traversal"""
    def __init__(self, message, errors = {}) :
        pyArangoException.__init__(self, message, errors)

class ValidationError(pyArangoException) :
    """Something went wrong when validating something"""
    def __init__(self, message, errors = {}) :
        pyArangoException.__init__(self, message, errors)

class SchemaViolation(pyArangoException) :
    """Raised when someone tries to add a new field to an object belonging a to a Collection with enforced schema"""
    def __init__(self, collection, field, errors = {}) :
        message = "Collection '%s' does not have a field '%s' in it's schema" % (collection.__name__, field)
        pyArangoException.__init__(self, message, errors)

class InvalidDocument(pyArangoException) :
    """Raised when a Document does not respect schema/validation defined in its collection"""
    def __init__(self, errors) :
        message = "Unsuccesful validation"
        self.errors = errors
        pyArangoException.__init__(self, message, errors)

    def __str__(self) :
        self.strErrors = []
        for k, v in self.errors.items() :
            strErrors.append("%s -> %s" % (k, v))
        strErrors = '\n\t'.join(strErrors)
        return self.message + ":\n\t" + strErrors

    def add(self, errors) :
        """add more errors"""
        self.errors.update(errors)

class SimpleQueryError(pyArangoException) :
    """Something went wrong with a simple query"""
    def __init__(self, message, errors = {}) :
        pyArangoException.__init__(self, message, errors)

class QueryError(pyArangoException) :
    """Something went wrong with an aql query"""
    def __init__(self, message, errors = {}) :
        pyArangoException.__init__(self, message, errors)

class AQLQueryError(pyArangoException) :
    """Something went wrong with an aql query"""
    def __init__(self, message, query, errors = {}) :
        message = "Error in: %s.\n->%s" % (query, message)
        pyArangoException.__init__(self, message, errors)

class CursorError(pyArangoException) :
    """Something went wrong when trying to fetch data with a cursor"""
    def __init__(self, message, cursorId, errors = {}) :
        message = "Unable to retreive data for cursor %s: %s" % (cursorId, message)
        pyArangoException.__init__(self, message, errors)

class TransactionError(pyArangoException) :
    """Something went wrong with a transaction"""
    def __init__(self, message, action, errors = {}) :
        message = "Error in: %s.\n->%s" % (action, message)
        pyArangoException.__init__(self, message, errors)

class AbstractInstanciationError(Exception) :
    """Raised when someone tries to instanciate an abstract class"""
    def __init__(self, cls) :
        self.cls = cls
        self.message = "%s is abstract and is not supposed to be instanciated. Collections my inherit from it" % self.cls.__name__
        Exception.__init__(self, self.message)

    def __str__(self) :
        return self.message
