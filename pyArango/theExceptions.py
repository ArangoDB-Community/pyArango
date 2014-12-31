class ArrangocityException(Exception) :
	def __init__(self, message, errors = {}) :
		Exception.__init__(self, message)
		self.errors = errors

	def __str__(self) :
		return self.message + ". Errors: " + str(self.errors)

class ConnectionError(ArrangocityException) :
	def __init__(self, message, URL, errors = {}) :
		mes = "%s. URL: %s" % (message, URL)
		ArrangocityException.__init__(self, mes, errors)

class CreationError(ArrangocityException) :
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class UpdateError(ArrangocityException) :
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class DeletionError(ArrangocityException) :
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class TraversalError(ArrangocityException) :
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class ValidationError(ArrangocityException) :
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class SchemaViolation(ArrangocityException) :
	def __init__(self, collection, field, errors = {}) :
		message = "Collection '%s' does not have a field '%s' in it's schema" % (collection.__name__, field)
		ArrangocityException.__init__(self, message, errors)

class InvalidDocument(ArrangocityException) :
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
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class AQLQueryError(ArrangocityException) :
	def __init__(self, message, query, errors = {}) :
		message = "Error in: %s.\n->%s" % (query, message)
		ArrangocityException.__init__(self, message, errors)

class CursorError(ArrangocityException) :
	def __init__(self, message, cursorId, errors = {}) :
		message = "Unable to retreive data for cursor %s: %s" % (cursorId, message)
		ArrangocityException.__init__(self, message, errors)

class TraversalError(ArrangocityException) :
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class AbstractInstanciationError(Exception) :
	def __init__(self, cls) :
		self.cls = cls
		self.message = "%s is abstract and is not supposed to be instanciated. Collections my inherit from it" % self.cls.__name__
		Exception.__init__(self, self.message)

	def __str__(self) :
		return self.message