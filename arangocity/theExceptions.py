class ArrangocityException(Exception) :
	def __init__(self, message, errors = {}) :
		Exception.__init__(self, message)
		self.errors = errors

	def __str__(self) :
		return self.message + ". Errors: " + str(self.errors)

class ConnectionError(ArrangocityException) :
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class CreationError(ArrangocityException) :
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class UpdateError(ArrangocityException) :
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class DeletionError(ArrangocityException) :
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class ConstraintViolation(ArrangocityException) :
	def __init__(self, message, errors = {}) :
		ArrangocityException.__init__(self, message, errors)

class SchemaViolation(ArrangocityException) :
	def __init__(self, collection, field, errors = {}) :
		message = "Collection %s does not a field '%s' in it's schema" % (collection.__class__.__name__, field)
		ArrangocityException.__init__(self, message, errors)

class AQLQueryError(ArrangocityException) :
	def __init__(self, message, query, errors = {}) :
		message = "Error in: %s.\n->%s" % (query, message)
		ArrangocityException.__init__(self, message, errors)
