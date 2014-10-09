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
	
	def test_document_fetch_by_key(self) :
		collection = self.db.createCollection(name = "lala")
		doc = collection.createDocument()
		doc["name"] = 'iop'
		doc.save()
		doc2 = collection.fetchDocument(doc._key)
		self.assertEqual(doc._id, doc2._id)

	def test_document_create_patch(self) :
		collection = self.db.createCollection(name = "lala")
		doc = collection.createDocument()
		doc["name"] = "l-3ewd3"
		self.assertRaises(ValueError, doc.patch)
		doc.save()
		doc.patch()
	
	def test_aql_validation(self) :
	 	collection = self.db.createCollection(name = "users")
		doc = collection.createDocument()
		doc["name"] = "l-3ewd"
		doc.save()

		aql = "FOR c IN users FILTER c.name == @name LIMIT 2 RETURN c.name"
		bindVars = {'name' : 'l-3ewd-3'}
		self.db.validateAQLQuery(aql, bindVars)
		
	def createManyUsers(self, nbUsers) :
	 	collection = self.db.createCollection(name = "users")
		for i in xrange(nbUsers) :
			doc = collection.createDocument()
			doc["name"] = "l-3ewd-%d" % i
			doc["number"] = i
			doc.save()

	def test_aql_query_rawResults_true(self) :
		self.createManyUsers(100)
		
		aql = "FOR c IN users FILTER c.name == @name LIMIT 10 RETURN c.name"
		bindVars = {'name' : 'l-3ewd-3'}
		q = self.db.AQLQuery(aql, rawResults = True, batchSize = 10, bindVars = bindVars)
		self.assertEqual(len(q.result), 1)
		self.assertEqual(q[0], 'l-3ewd-3')

	def test_aql_query_rawResults_false(self) :
		self.createManyUsers(100)

		aql = "FOR c IN users FILTER c.name == @name LIMIT 10 RETURN c"
		bindVars = {'name' : 'l-3ewd-3'}
		q = self.db.AQLQuery(aql, rawResults = False, batchSize = 10, bindVars = bindVars)
		self.assertEqual(len(q.result), 1)
		self.assertEqual(q[0]['name'], 'l-3ewd-3')		
		self.assertTrue(isinstance(q[0], Document))		
	
	def test_aql_query_batch(self) :
		nbUsers = 100
		self.createManyUsers(nbUsers)
		
		aql = "FOR c IN users LIMIT %s RETURN c" % nbUsers
		q = self.db.AQLQuery(aql, rawResults = False, batchSize = 1)
		lstRes = []
		for i in xrange(nbUsers) :
			lstRes.append(q[0]["number"])
			try :
				q.nextBatch()
			except StopIteration :
				self.assertEqual(i, nbUsers-1)
		
		lstRes.sort()
		self.assertEqual(lstRes, range(nbUsers))

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
