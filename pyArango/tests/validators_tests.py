import unittest, copy
from pyArango.validation import *
from pyArango.theExceptions import ValidationError

class ValidatorTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_notNull(self):
        v = NotNull()
        self.assertTrue(v.validate(33))
        self.assertRaises(ValidationError, v.validate, None)

    def test_email(self):
        v = Email()
        self.assertTrue(v.validate('nicholas.tesla@simba.com'))
        self.assertRaises(ValidationError, v.validate, 'nicholas.tesla @simba.com')
        self.assertRaises(ValidationError, v.validate, 'nicholas.tesla&@simba.com')
        self.assertRaises(ValidationError, v.validate, 'nicholas.tesla @simba.com')
        self.assertRaises(ValidationError, v.validate, 'nicholas.tesla')
        self.assertRaises(ValidationError, v.validate, 'nicholas.tesla@.com')
        self.assertRaises(ValidationError, v.validate, 'nicholas.tesla@com')

    def test_length(self):
        v = Length(2, 5)
        self.assertTrue(v.validate("12"))
        self.assertRaises(ValidationError, v.validate, '1')
        self.assertRaises(ValidationError, v.validate, '123456')

    def test_string(self):
        v = String()
        good = [
            "Hello World",
            u"sadasda",   # unicode literal under Py2, str under Py3
            ""
        ]
        for val in good:
            self.assertTrue(v.validate(val), msg="Expected to accept {!r}".format(val))
        bad = [
            123,
            None,
            True,
            False,
            12.34,
            [1, 2, 3],
            {1: 'a', 2: 'b'},
            b"Hello World",
            bytearray(b"Hello World")
        ]
        for val in bad:
            self.assertRaises(ValidationError, v.validate, val)
        for val in (123, b"foo\nbar"):
            with self.assertRaises(ValidationError) as cm:
                v.validate(val)
            err = str(cm.exception)
            self.assertIn(repr(val), err)
            self.assertIn("is not a valid string", err)

if __name__ == "__main__":
    unittest.main()
