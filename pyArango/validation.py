from theExceptions import ValidationError

class Validator(object) :
	"""All validators must inherit from this class"""
	def __init__(self, *args, **kwrags) :
		pass

	def validate(self, value) :
		"""The only function that a validator must implement. Must return True if erevything went well or a ValidationError otherwise"""
		raise NotImplemented("Should be implemented in child")

	def __str__(self) :
		"""This function should be redifined in child to give a quick overview of the validator"""
		return self.__class__.__name__

class NotNull(Validator) :
	"""Checks that the Field has a non null value"""
	def validate(self, value) :
		if value is None or value == "" :
			raise ValidationError("Field can't have a null value: '%s'" % value)
		return True

class Length(Validator) :
	"""validates that the value length is between given bounds"""
	def __init__(self, minLen, maxLen) :
		self.minLen = minLen
		self.maxLen = maxLen

	def validate(self, value) :
		if len(value) <= self.minLen or len(value) >= self.maxLen :
			raise ValidationError("Field must have a len between '%s' and '%s' got: '%s'" % (self.minLen, self.maxLen, value))
		return True

	def __str__(self) :
		return "%s[%s, %s]" % (self.__class__.__name__, self.minLen, self.maxLen)
