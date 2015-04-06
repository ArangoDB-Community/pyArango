import unittest, copy
from pyArango.validation import *
from pyArango.theExceptions import ValidationError

class ValidatorTests(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		pass

	def test_notNull(self) :
		v = NotNull()
		self.assertTrue(v.validate(33))
		self.assertRaises(ValidationError, v.validate, None)

	def test_email(self) :
		v = Email()
		self.assertTrue(v.validate('nicholas.tesla@simba.com'))
		self.assertRaises(ValidationError, v.validate, 'nicholas.tesla @simba.com')
		self.assertRaises(ValidationError, v.validate, 'nicholas.tesla&@simba.com')
		self.assertRaises(ValidationError, v.validate, 'nicholas.tesla @simba.com')
		self.assertRaises(ValidationError, v.validate, 'nicholas.tesla')
		self.assertRaises(ValidationError, v.validate, 'nicholas.tesla@.com')
		self.assertRaises(ValidationError, v.validate, 'nicholas.tesla@com')

	def test_length(self) :
		v = Length(2, 5)
		self.assertTrue(v.validate("12"))
		self.assertRaises(ValidationError, v.validate, '1')
		self.assertRaises(ValidationError, v.validate, '123456')
		
if __name__ == "__main__" :
	unittest.main()