import unittest, copy

from pyArango.connection import *
from pyArango.database import *
from pyArango.collection import *
from pyArango.document import *
from pyArango.query import *
from pyArango.graph import *
from pyArango.theExceptions import *

class pyArangoTests(unittest.TestCase):

	def setUp(self):
		self.conn = Connection()

		try :
			self.conn.createDatabase(name = "test_db_2")
		except CreationError :
			pass

		self.db = self.conn["test_db_2"]
		self._reset()

	def _reset(self) :
		self.db.reload()
		for colName in self.db.collections :
			if not self.db[colName].isSystem :
				self.db[colName].delete()

		for graph in self.db.graphs.itervalues() :
			graph.delete()

	def tearDown(self):
		self._reset()

	def createManyUsers(self, nbUsers) :
	 	collection = self.db.createCollection(name = "users")
		for i in xrange(nbUsers) :
			doc = collection.createDocument()
			doc["name"] = "Tesla-%d" % i
			doc["number"] = i
			doc["species"] = "human"
			doc.save()
		return collection
	
	#@unittest.skip("stand by")
	def test_collection_create_delete(self) :
		col = self.db.createCollection(name = "to_be_erased")
		d1 = col.createDocument()
		d1["name"] = "tesla"
		d1.save()
		self.assertEqual(1, col.count())
		
		self.db["to_be_erased"].delete()
		self.assertRaises(DeletionError, self.db["to_be_erased"].delete)
	
	#@unittest.skip("stand by")
	def test_edges_create_delete(self) :
		ed = self.db.createCollection(className = "Edges", name = "to_be_erased")
		col = self.db.createCollection(name = "to_be_erased_to")

		d1 = col.createDocument()
		d1["name"] = "tesla"
		d1.save()

		self.db.reloadCollections()
		ed = self.db.collections["to_be_erased"]
		e1 = ed.createEdge({"name": 'tesla'})
		e1.links(d1, d1)
		e2 = ed.createEdge()
		e2.links(d1, d1)
		self.assertEqual(2, ed.count())
		self.db["to_be_erased"].delete()
		self.assertRaises(DeletionError, self.db["to_be_erased"].delete)

	#@unittest.skip("stand by")
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

	#@unittest.skip("stand by")
	def test_document_create_update_delete(self) :
		collection = self.db.createCollection(name = "lala")
		doc = collection.createDocument()
		doc["name"] = "Tesla"
		self.assertTrue(doc.URL is None)
		doc.save()
		self.assertTrue(doc.URL is not None)
		url = copy.copy(doc.URL)
		doc["name"] = "Tesla2"
		doc.save()
		self.assertEqual(doc.URL, url)
		doc.delete()
		self.assertTrue(doc.URL is None)

	#@unittest.skip("stand by")
	def test_document_fetch_by_key(self) :
		collection = self.db.createCollection(name = "lala")
		doc = collection.createDocument()
		doc["name"] = 'iop'
		doc.save()
		doc2 = collection.fetchDocument(doc._key)
		self.assertEqual(doc._id, doc2._id)

	# @unittest.skip("stand by")
	def test_document_has_field(self) :
		class theCol(Collection) :
			_fields = {
				'address' : {
					'street' : Field(),
					}
			}

		col = self.db.createCollection("theCol")
		self.assertTrue(self.db['theCol'].hasField('address'))
		self.assertTrue(self.db['theCol'].hasField('address.street'))
		self.assertFalse(self.db['theCol'].hasField('street'))
		self.assertFalse(self.db['theCol'].hasField('banana'))
		self.assertFalse(self.db['theCol'].hasField('address.banana'))
		
	#@unittest.skip("stand by")
	def test_document_fetch_first_last_examples(self) :
		import time
		collection = self.db.createCollection(name = "lala")
		for i in xrange(10) :
			doc = collection.createDocument()
			doc["name"] = "hop"
			doc['i'] = i
			doc.save()
		res = collection.fetchFirst( count = 100, rawResults = True )
		self.assertEqual(len(res), 10)
		for i in xrange(10) :
			self.assertEqual(res[i]['i'], i)
		
		res = collection.fetchLast( count = 100, rawResults = True )
		self.assertEqual(len(res), 10)
		for i in xrange(10) :
			self.assertEqual(res[i]['i'], 10 - i - 1)

	#@unittest.skip("stand by")
	def test_document_create_patch(self) :
		collection = self.db.createCollection(name = "lala")
		doc = collection.createDocument()
		doc["name"] = "Tesla3"
		self.assertRaises(ValueError, doc.patch)
		doc.save()
		doc.patch()
	
	#@unittest.skip("stand by")
	def test_aql_validation(self) :
	 	collection = self.db.createCollection(name = "users")
		doc = collection.createDocument()
		doc["name"] = "Tesla"
		doc.save()

		aql = "FOR c IN users FILTER c.name == @name LIMIT 2 RETURN c.name"
		bindVars = {'name' : 'Tesla-3'}

	#@unittest.skip("stand by")
	def test_aql_query_rawResults_true(self) :
		self.createManyUsers(100)
		
		aql = "FOR c IN users FILTER c.name == @name LIMIT 10 RETURN c.name"
		bindVars = {'name' : 'Tesla-3'}
		q = self.db.AQLQuery(aql, rawResults = True, batchSize = 10, bindVars = bindVars)
		self.assertEqual(len(q.result), 1)
		self.assertEqual(q[0], 'Tesla-3')

	#@unittest.skip("stand by")
	def test_aql_query_rawResults_false(self) :
		self.createManyUsers(100)

		aql = "FOR c IN users FILTER c.name == @name LIMIT 10 RETURN c"
		bindVars = {'name' : 'Tesla-3'}
		q = self.db.AQLQuery(aql, rawResults = False, batchSize = 10, bindVars = bindVars)
		self.assertEqual(len(q.result), 1)
		self.assertEqual(q[0]['name'], 'Tesla-3')		
		self.assertTrue(isinstance(q[0], Document))		
	
	#@unittest.skip("stand by")
	def test_aql_query_batch(self) :
		nbUsers = 100
		self.createManyUsers(nbUsers)
		
		aql = "FOR c IN users LIMIT %s RETURN c" % nbUsers
		q = self.db.AQLQuery(aql, rawResults = False, batchSize = 1, count = True)
		lstRes = []
		for i in xrange(nbUsers) :
			lstRes.append(q[0]["number"])
			try :
				q.nextBatch()
			except StopIteration :
				self.assertEqual(i, nbUsers-1)
		
		lstRes.sort()
		self.assertEqual(lstRes, range(nbUsers))
		self.assertEqual(q.count, nbUsers)

	#@unittest.skip("stand by")
	def test_simple_query_example_batch(self) :
		nbUsers = 100
		col = self.createManyUsers(nbUsers)
		
		example = {'species' : "human"}

		q = col.fetchByExample(example, batchSize = 1, count = True)
		lstRes = []
		for i in xrange(nbUsers) :	
			lstRes.append(q[0]["number"])
			try :
				q.nextBatch()
			except StopIteration :
				self.assertEqual(i, nbUsers-1)
		
		lstRes.sort()
		self.assertEqual(lstRes, range(nbUsers))
		self.assertEqual(q.count, nbUsers)

	#@unittest.skip("stand by")
	def test_simple_query_all_batch(self) :
		nbUsers = 100
		col = self.createManyUsers(nbUsers)
		
		q = col.fetchAll(batchSize = 1, count = True)
		lstRes = []
		for i in xrange(nbUsers) :	
			lstRes.append(q[0]["number"])
			try :
				q.nextBatch()
			except StopIteration :
				self.assertEqual(i, nbUsers-1)
		
		lstRes.sort()
		self.assertEqual(lstRes, range(nbUsers))
		self.assertEqual(q.count, nbUsers)

	def test_nonRaw_creation_error(self):
		col = self.createManyUsers(1)
		docs = self.db.AQLQuery("for x in users return { name : x.name }", batchSize = 1);

		with self.assertRaises(CreationError):
			doc0 = docs[0]

	#@unittest.skip("stand by")
	def test_empty_query(self) :
		col = self.createManyUsers(1)
		example = {'species' : "rat"}
		q = col.fetchByExample(example, batchSize = 1, count = True)
		self.assertEqual(q.result, [])

	#@unittest.skip("stand by")
	def test_cursor(self) :
		nbUsers = 2
		col = self.createManyUsers(nbUsers)
		
		q = col.fetchAll(batchSize = 1, count = True)
		q2 = Cursor(q.database, q.cursor.id, rawResults = True)

		lstRes = [q.result[0]["number"], q2.result[0]["number"]]
		lstRes.sort()
		self.assertEqual(lstRes, range(nbUsers))
		self.assertEqual(q.count, nbUsers)

	#@unittest.skip("stand by")
	def test_fields_on_set(self) :
		import pyArango.validation as VAL

		class Col_on_set(Collection) :
			_validation = {
				"on_save" : False,
				"on_set" : True,
				"allow_foreign_fields" : False
			}
			
			_fields = {
				"str" : Field(validators = [VAL.Length(50, 51)]),
				"notNull" : Field(validators = [VAL.NotNull()]),
				"nestedStr": {
					"str": Field(validators = [VAL.Length(50, 51)])
				}
			}
			
		myCol = self.db.createCollection('Col_on_set')
		doc = myCol.createDocument()
		self.assertRaises(ValidationError, doc.__setitem__, 'str', "qwer")
		self.assertRaises(ValidationError, doc.__setitem__, 'nestedStr.str', "qwer")
		self.assertRaises(ValidationError, doc.__setitem__, 'notNull', None)
		self.assertRaises(SchemaViolation, doc.__setitem__, 'foreigner', None)

	#@unittest.skip("stand by")
	def test_fields_on_save(self) :
		import pyArango.validation as VAL
		import types
		class String_val(VAL.Validator) :

			def validate(self, value) :
				if type(value) is not types.StringType :
					raise ValidationError("Field value must be a string")
				return True

		class Col_on_set(Collection) :

			_validation = {
				"on_save" : True,
				"on_set" : False,
				"allow_foreign_fields" : False
			}

			_fields = {
				"str" : Field(validators = [String_val()]),
				"nestedStr": {
					"str": Field(validators = [VAL.Length(50, 51)])
				}
			}
			
		myCol = self.db.createCollection('Col_on_set')
		doc = myCol.createDocument()
		doc["str"] = 3
		self.assertRaises(InvalidDocument, doc.save)
		
		doc = myCol.createDocument()
		doc["str"] = "string"
		doc["foreigner"] = "string"
		self.assertRaises(InvalidDocument,  doc.save)

		doc = myCol.createDocument()
		doc["nestedStr"] = {}
		doc["nestedStr"]["str"] = 3
		doc["str"] = "string"
		self.assertRaises(InvalidDocument,  doc.save)

		doc = myCol.createDocument()
		doc["nestedStr"] = {}
		doc["nestedStr"]["str"] = "string"
		doc["str"] = "string"

	#@unittest.skip("stand by")
	def test_document_cache(self) :
		class DummyDoc(object) :
			def __init__(self, key) :
				self.key = key
			def __repr__(self) :
				return repr(self.key)

		docs = []
		for i in xrange(10) :
			docs.append(DummyDoc(i))

		cache = DocumentCache(5)
		for doc in docs :
			cache.cache(doc)
			self.assertEqual(cache.head.key, doc.key)
		
		self.assertEqual(cache.cacheStore.keys(), [5, 6, 7, 8, 9])	
		self.assertEqual(cache.getChain(), [9, 8, 7, 6, 5])
		doc = cache[5]
		self.assertEqual(cache.head.key, doc.key)
		self.assertEqual(cache.getChain(), [5, 9, 8, 7, 6])

	#@unittest.skip("stand by")
	def test_validation_default_settings(self) :

		class Col_empty(Collection) :
			pass

		class Col_empty2(Collection) :
			_validation = {
				"on_save" : False,
			}

		c = Col_empty
		self.assertEqual(c._validation, Collection_metaclass._validationDefault)

		c = Col_empty2
		self.assertEqual(c._validation, Collection_metaclass._validationDefault)

	#@unittest.skip("stand by")
	def test_validation_default_inlavid_key(self) :

		def keyTest() :
			class Col(Collection) :
				_validation = {
					"on_sav" : True,
				}

		self.assertRaises(KeyError, keyTest)
		
	#@unittest.skip("stand by")
	def test_validation_default_inlavid_value(self) :

		def keyTest() :
			class Col(Collection) :
				_validation = {
					"on_save" : "wrong",
				}

		self.assertRaises(ValueError, keyTest)
	
	#@unittest.skip("stand by")
	def test_collection_type_creation(self) :
		class Edgy(Edges) :
			pass
		
		class Coly(Collection) :
			pass

		edgy = self.db.createCollection("Edgy")
		self.assertEqual(edgy.type, COLLECTION_EDGE_TYPE)
		coly = self.db.createCollection("Coly")
		self.assertEqual(coly.type, COLLECTION_DOCUMENT_TYPE)

	#@unittest.skip("stand by")
	def test_save_edge(self) :
		class Human(Collection) :
			_fields = {
				"name" : Field()
			}

		class Relation(Edges) :
			_fields = {
				"ctype" : Field()
			}

		humans = self.db.createCollection("Human")
		rels = self.db.createCollection("Relation")

		tete = humans.createDocument()
		tete["name"] = "tete"
		tete.save()
		toto = humans.createDocument()
		toto["name"] = "toto"
		toto.save()

		link = rels.createEdge()
		link["ctype"] = "brother"
		link.links(tete, toto)

		sameLink = rels[link._key]
		self.assertEqual(sameLink["ctype"], link["ctype"])
		self.assertEqual(sameLink._from, tete._id)
		self.assertEqual(sameLink._to, toto._id)

	#@unittest.skip("stand by")
	def test_get_edges(self) :
		class Human(Collection) :
			_fields = {
				"number" : Field()
			}

		class Relation(Edges) :
			_fields = {
				"number" : Field()
			}

		humans = self.db.createCollection("Human")
		rels = self.db.createCollection("Relation")
		humansList = []
		
		for i in range(10) :
			h = humans.createDocument()
			h["number"] = i
			humansList.append(h)
			h.save()

		for i in range(10) :
			e = rels.createEdge()
			e["number"] = i
			if i % 2 == 1 :
				e.links(humansList[0], humansList[i])
			else :
				e.links(humansList[-1], humansList[i])

		outs = humansList[0].getOutEdges(rels)
		self.assertEqual(len(outs), 5)
		for o in outs :
			self.assertEqual(o["number"] % 2, 1)

		ins = humansList[-1].getOutEdges(rels)
		self.assertEqual(len(ins), 5)
		for i in ins :
			self.assertEqual(i["number"] % 2, 0)

	#@unittest.skip("stand by")
	def test_graph(self) :
		class Humans(Collection) :
			_fields = {
				"name" : Field()
			}

		class Friend(Edges) :
			_fields = {
				"number" : Field()
			}

		class MyGraph(Graph) :

			_edgeDefinitions = (EdgeDefinition("Friend", fromCollections = ["Humans"], toCollections = ["Humans"]), )
			_orphanedCollections = []
		
		humans = self.db.createCollection("Humans")
		rels = self.db.createCollection("Friend")
		g = self.db.createGraph("MyGraph")
		h1 = g.createVertex('Humans', {"name" : "simba"})
		h2 = g.createVertex('Humans', {"name" : "simba2"})
		h3 = g.createVertex('Humans', {"name" : "simba3"})
		h4 = g.createVertex('Humans', {"name" : "simba4"})
		
		g.link('Friend', h1, h3, {})
		g.link('Friend', h2, h3, {})
		self.assertEqual(len(h3.getEdges(rels)), 2)
		self.assertEqual(len(h2.getEdges(rels)), 1)
		g.deleteVertex(h3)
		self.assertEqual(len(h2.getEdges(rels)), 0)
		g.link('Friend', h1, h2, {})
		self.assertEqual(len(h2.getEdges(rels)), 1)

		g.link('Friend', h4, h1, {})
		g.link('Friend', h4, h2, {})
		g.link('Friend', h4, h3, {})
		g.unlink('Friend', h4, h3)
		self.assertEqual(len(h4.getEdges(rels)), 2)

		h5 = g.createVertex('Humans', {"name" : "simba5"})
		h6 = g.createVertex('Humans', {"name" : "simba6"})
		for i in xrange(200) :
			g.link('Friend', h5, h6, {})

		self.assertEqual(len(h5.getEdges(rels)), 200)
		g.unlink('Friend', h5, h6)
		self.assertEqual(len(h5.getEdges(rels)), 0)

		# g.deleteEdge()

	#@unittest.skip("stand by")
	def test_traversal(self) :

		class persons(Collection) :
			_fields = {
				"name" : Field()
			}

		class knows(Edges) :
			_fields = {
				"number" : Field()
			}

		class knows_graph(Graph) :

			_edgeDefinitions = (EdgeDefinition("knows", fromCollections = ["persons"], toCollections = ["persons"]), )
			_orphanedCollections = []
		
		conn = Connection(arangoURL = "http://localhost:8529")
		db = conn["test_db_2"]

		pers = db.createCollection("persons")
		rels = db.createCollection("knows")
		g = db.createGraph("knows_graph")

		alice = g.createVertex("persons", {"_key" : "alice"})
		bob = g.createVertex("persons", {"_key" : "bob"})
		charlie = g.createVertex("persons", {"_key" : "charlie"})
		dave = g.createVertex("persons", {"_key" : "dave"})
		eve = g.createVertex("persons", {"_key" : "eve"})

		e = g.link("knows", alice, alice, {'me' : "aa"})
		
		g.link("knows", alice, bob, {})
		g.link("knows", bob, charlie, {})
		g.link("knows", bob, dave, {})
		g.link("knows", eve, alice, {})
		g.link("knows", eve, bob, {})

		travVerts = g.traverse(alice, direction = "outbound")["visited"]["vertices"]
		_keys = set()
		for v in travVerts :
			_keys.add(v["_key"])

		pers = [alice, bob, charlie, dave]
		for p in pers :
			self.assertTrue(p._key in _keys)

		travVerts = g.traverse(alice, direction = "inbound")["visited"]["vertices"]
		_keys = set()
		for v in travVerts :
			_keys.add(v["_key"])

		pers = [alice, eve]
		for p in pers :
			self.assertTrue(p._key in _keys)

		travVerts = g.traverse(alice, direction = "any")["visited"]["vertices"]
		_keys = set()
		for v in travVerts :
			_keys.add(v["_key"])

		pers = [alice, bob, charlie, dave, eve]
		for p in pers :
			self.assertTrue(p._key in _keys)

	#@unittest.skip("stand by")
	def testIndexes(self) :
		class persons(Collection) :
			_fields = {
				"name" : Field(),
				"Description": Field(),
				"geo": Field(),
				"skip": Field()
			}

		conn = Connection(arangoURL = "http://localhost:8529")
		db = conn["test_db_2"]
		pers = db.createCollection("persons")

		capInd = pers.ensureCapConstraint(2)
		capInd.delete()
		capInd2 = pers.ensureCapConstraint(2)
		self.assertTrue(capInd.infos["id"] != capInd2.infos["id"])
		
		hashInd = pers.ensureHashIndex(["name"])
		hashInd.delete()
		hashInd2 = pers.ensureHashIndex(["name"])
		self.assertTrue(hashInd.infos["id"] != hashInd2.infos["id"])

		ftInd = pers.ensureFulltextIndex(["Description"])
		ftInd.delete()
		ftInd2 = pers.ensureFulltextIndex(["Description"])
		self.assertTrue(ftInd.infos["id"] != ftInd2.infos["id"])

		skipInd = pers.ensureFulltextIndex(["skip"])
		skipInd.delete()
		skipInd2 = pers.ensureFulltextIndex(["skip"])
		self.assertTrue(skipInd.infos["id"] != skipInd2.infos["id"])

		geoInd = pers.ensureFulltextIndex(["geo"])
		geoInd.delete()
		geoInd2 = pers.ensureFulltextIndex(["geo"])
		self.assertTrue(geoInd.infos["id"] != geoInd2.infos["id"])

        def test_transaction(self) :
                transaction = self.db.transaction(
                        collections = {},
                        action = "function (params) {return params['some_param'];}",
                        params = {"some_param": "lala param"})
                self.assertEqual(transaction, {"code": 200, "result": "lala param", "error": False})

        def test_transaction_exception(self) :
                self.assertRaises(TransactionError, self.db.transaction, collections = {}, action = "function () { return value; }")

if __name__ == "__main__" :
	unittest.main()
