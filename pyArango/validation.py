from .theExceptions import ValidationError

class Validator(object):
    """All validators must inherit from this class"""
    def __init__(self, *args, **kwrags):
        pass

    def validate(self, value):
        """The only function that a validator must implement. Must return True if erevything went well or a ValidationError otherwise"""
        raise NotImplemented("Should be implemented in child")

    def __str__(self):
        """This function should be redifined in child to give a quick overview of the validator"""
        return self.__class__.__name__

class NotNull(Validator):
    """Checks that the Field has a non null value. False is not considered a Null Value"""
    def __init__(self, reject_zero=False, reject_empty_string=True):
        self.reject_zero = reject_zero
        self.reject_empty_string = reject_empty_string

    def validate(self, value):
        if value is None or ( (value == 0 and type(value) != bool ) and self.reject_zero) or (value == "" and self.reject_empty_string):
            raise ValidationError("Field can't have a null value, got: '%s'" % value)
        return True

class Email(Validator):
    """Checks if the field contains an emailaddress"""
    def validate(self, value):
        import re
        pat = '^[A-z0-9._-]+@[A-z0-9.-]+\.[A-z]{2,4}$'
        if re.match(pat, value) is None:
            raise ValidationError("The email address: %s is invalid" % value)
        return True

class Numeric(Validator):
    """checks if the value is numerical"""
    def validate(self, value):
        try:
            float(value)
        except:
            raise ValidationError("%s is not valid numerical value" % value)
        return True

class Int(Validator):
    """The value must be an integer"""
    def validate(self, value):
        if not isinstance(value, int):
            raise ValidationError("%s is not a valid integer" % value)
        return True

class Bool(Validator):
    """The value must be a boolean"""
    def validate(self, value):
        if not isinstance(value, bool):
            raise ValidationError("%s is not a valid boolean" % value)
        return True

class String(Validator):
    """The value must be a string or unicode"""
    def validate(self, value):
        if not isinstance(value, str) and not isinstance(value, unicode):
            raise ValidationError("%s is not a valid string" % value)
        return True

class Enumeration(Validator):
    """The value must be in the allowed ones"""
    def __init__(self, allowed):
        self.allowed = set(allowed)
  
    def validate(self, value):
        if value not in self.allowed:
            raise ValidationError("%s is not among the allowed values %s" % (value, self.allowed))
        return True

class Range(Validator):
    """The value must une [lower, upper] range"""
    def __init__(self, lower, upper):
        self.lower = lower
        self.upper = upper

    def validate(self, value):
        if value < self.lower or value > self.upper:
            raise ValidationError("%s is not in [%s, %s]" % (value, self.lower, self.upper))
    
    def __str__(self):
        return "%s[%s, %s]" % (self.__class__.__name__, self.minLen, self.maxLen)


class Length(Validator):
    """validates that the value length is between given bounds"""
    def __init__(self, minLen, maxLen):
        self.minLen = minLen
        self.maxLen = maxLen

    def validate(self, value):
        try:
            length = len(value)
        except:
            raise ValidationError("Field '%s' of type '%s' has no length" % (value, type(value)))
            
        if self.minLen <= len(value) and len(value) <= self.maxLen:
            return True
        raise ValidationError("Field must have a length in ['%s';'%s'] got: '%s'" % (self.minLen, self.maxLen, len(value)))
    
    def __str__(self):
        return "%s[%s, %s]" % (self.__class__.__name__, self.minLen, self.maxLen)
