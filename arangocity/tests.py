from arangocity import *
import unittest, copy

class ArangocityTests(unittest.TestCase):

	def setUp(self):
		self.conn = Connection()

		try :
			self.conn.createDatabase(name = "test_db")
			self.db.createCollection("lala")
		except CreationError :
			pass

		self.db = self.conn["test_db"]
		self.collection = self.db["lala"]
		self._resetUp()

	def _resetUp(self) :
		try :
			self.db["to_be_erased"].delete()
		except :
			pass

	def tearDown(self):
		self._resetUp()

	def test_collection_create_delete(self) :
		col = self.db.createCollection(name = "to_be_erased")
		self.db["to_be_erased"].delete()

		self.assertRaises(DeletionError, self.db["to_be_erased"].delete)
	
	def test_collection_count_truncate(self) :
		self.collection.truncate()
		doc = self.collection.createDocument()
		doc.save()
		doc2 = self.collection.createDocument()
		doc2.save()
		self.assertEqual(2, self.collection.count())
		self.collection.truncate()
		self.assertEqual(0, self.collection.count())

	def test_document_create_update_delete(self) :
		doc = self.collection.createDocument()
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
	
if __name__ == "__main__" :
	unittest.main()
