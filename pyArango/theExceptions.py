class ArrangocityException(Exception) :
	"""The calss from witch all Exceptions inherit"""
	def __init__(self, message, errors = {}) :
		Exception.__init__(self, message)
		self.errors = errors

	def __str__(self) :
		return self.message + ". Errors: " + str(self.errors)

class ConnectionError(ArrangocityException) :
	"""Something went wrong with the connection"""
	def __init__(self, message, URL, errors = {}) :
		mes = "%s. URL: %s" % (message, URL)
		ArrangocityException.__init__(self, mes, errors)

class CreationError(ArrangocityException) :
	"""Something went wrong when creating something"""
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class UpdateError(ArrangocityException) :
	"""Something went wrong when updating something"""
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class DeletionError(ArrangocityException) :
	"""Something went wrong when deleting something"""
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class TraversalError(ArrangocityException) :
	"""Something went wrong when doing a graph traversal"""
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class ValidationError(ArrangocityException) :
	"""Something went wrong when validating something"""
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class SchemaViolation(ArrangocityException) :
	"""Raised when someone tries to add a new field to an object belonging a to a Collection with enforced schema"""
	def __init__(self, collection, field, errors = {}) :
		message = "Collection '%s' does not have a field '%s' in it's schema" % (collection.__name__, field)
		ArrangocityException.__init__(self, message, errors)

class InvalidDocument(ArrangocityException) :
	"""Raised when a Document does not respect schema/validation defined in its collection"""
	def __init__(self, errors) :
		message = "Unsuccesful validation" 
		self.strErrors = []
		for k, v in errors.iteritems() :
			self.strErrors.append("%s -> %s" % (k, v))
		self.strErrors = '\n\t'.join(self.strErrors)

		ArrangocityException.__init__(self, message, errors)
	
	def __str__(self) :
		return self.message + ":\n\t" + self.strErrors

class SimpleQueryError(ArrangocityException) :
	"""Something went wrong with a simple query"""
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class QueryError(ArrangocityException) :
	"""Something went wrong with an aql query"""
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class AQLQueryError(ArrangocityException) :
	"""Something went wrong with an aql query"""
	def __init__(self, message, query, errors = {}) :
		message = "Error in: %s.\n->%s" % (query, message)
		ArrangocityException.__init__(self, message, errors)

class CursorError(ArrangocityException) :
	"""Something went wrong when trying to fetch data with a cursor"""
	def __init__(self, message, cursorId, errors = {}) :
		message = "Unable to retreive data for cursor %s: %s" % (cursorId, message)
		ArrangocityException.__init__(self, message, errors)

class TransactionError(ArrangocityException) :
	"""Something went wrong with a transaction"""
	def __init__(self, message, action, errors = {}) :
                message = "Error in: %s.\n->%s" % (action, message)
		ArrangocityException.__init__(self, message, errors)

class AbstractInstanciationError(Exception) :
	"""Raised when someone tries to instanciate an abstract class"""
	def __init__(self, cls) :
		self.cls = cls
		self.message = "%s is abstract and is not supposed to be instanciated. Collections my inherit from it" % self.cls.__name__
		Exception.__init__(self, self.message)

	def __str__(self) :
		return self.message
