import unittest, copy

from connection import *
from database import *
from collection import *
from document import *
from theExceptions import *

class ArangocityTests(unittest.TestCase):

	def setUp(self):
		self.conn = Connection()

		try :
			self.conn.createDatabase(name = "test_db")
		except CreationError :
			pass

		self.db = self.conn["test_db"]
		self._resetUp()

	def _resetUp(self) :
		self.db.update()
		for colName in self.db.collections :
			if not self.db[colName].isSystem :
				self.db[colName].delete()

	def tearDown(self):
		self._resetUp()

	def test_collection_create_delete(self) :
		col = self.db.createCollection(name = "to_be_erased")
		self.db["to_be_erased"].delete()

		self.assertRaises(DeletionError, self.db["to_be_erased"].delete)
	
	def test_collection_count_truncate(self) :
		collection = self.db.createCollection(name = "lala")	
		collection.truncate()
		doc = collection.createDocument()
		doc.save()
		doc2 = collection.createDocument()
		doc2.save()
		self.assertEqual(2, collection.count())
		collection.truncate()
		self.assertEqual(0, collection.count())

	def test_document_create_update_delete(self) :
		collection = self.db.createCollection(name = "lala")
		doc = collection.createDocument()
		doc["name"] = "l-3ewd"
		self.assertTrue(doc.URL is None)
		doc.save()
		self.assertTrue(doc.URL is not None)
		url = copy.copy(doc.URL)
		doc["name"] = "l-3ewd2"
		doc.save()
		self.assertEqual(doc.URL, url)
		doc.delete()
		self.assertTrue(doc.URL is None)

	def test_fields_on_set(self) :
		def strFct(v) :
			import types
			return type(v) is types.StringType	

		class Col_on_set(Collection) :
			_test_fields_on_save = False
			_test_fields_on_set = True
			_allow_foreign_fields = False
			_fields = {
				"str" : Field(constraintFct = strFct),
				"notNull" : Field(notNull = True)
			}
			
		myCol = self.db.createCollection('Col_on_set')
		doc = myCol.createDocument()
		self.assertRaises(ConstraintViolation, doc.__setitem__, 'str', 3)
		self.assertRaises(ConstraintViolation, doc.__setitem__, 'notNull', None)
		self.assertRaises(SchemaViolation, doc.__setitem__, 'foreigner', None)


	def test_fields_on_save(self) :
		def strFct(v) :
			import types
			return type(v) is types.StringType	

		class Col_on_set(Collection) :
			_test_fields_on_save = True
			_test_fields_on_set = False
			_allow_foreign_fields = False
			_fields = {
				"str" : Field(constraintFct = strFct),
				"notNull" : Field(notNull = True)
			}
			
		myCol = self.db.createCollection('Col_on_set')
		doc = myCol.createDocument()
		doc["str"] = 3
		self.assertRaises(ConstraintViolation, doc.save)
		doc["str"] = "string"
		self.assertRaises(ConstraintViolation, doc.save)
		doc["foreigner"] = "string"
		self.assertRaises(SchemaViolation,  doc.save)	


if __name__ == "__main__" :
	unittest.main()
