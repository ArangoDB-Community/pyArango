Arangocity (alpha)
==========

Arangocity is still in active developpement, but it's tested and perfectly usable.
Arangocity aims to be an easy to use driver for arangoDB with built in validation. Collections are treated as types that apply to the documents within. You can be 100% permissive or enforce schemas and validate fields on set, on save or on both.
I am developping Arangocity for the purpose of an other project and adding features as they are needed.


Initiatilisation and document saving
---------

.. code:: python
  
  from arangocity.Connection import *
  
  conn = Connection()
  conn.createDatabase(name = "test_db")
  db = self.conn["test_db"]
  collection = db.createCollection(name = "users")
  # collection.delete() # self explanatory
  
  for i in xrange(100) :
  	doc = collection.createDocument()
  	doc["name"] = "Tesla-%d" % i
  	doc["number"] = i
  	doc["species"] = "human"
  	doc.save()

  doc = collection.createDocument()
  doc["name"] = "Tesla-101"
  doc["number"] = 101
  doc["species"] = "human"
  
  doc["name"] = "Simba"
  # doc.save() # overwrites the document
  doc.patch() # updates the modified field
  doc.delete()

Queries : AQL
-------
  
.. code:: python
  
  aql = "FOR c IN users FILTER c.name == @name LIMIT 10 RETURN c"
  bindVars = {'name' : 'Tesla-3'}
  # by setting rawResults to True you'll get dictionaries instead of Document objects, useful if you want to result to set of fields for example 
  queryResult = db.AQLQuery(aql, rawResults = False, batchSize = 1, bindVars = bindVars)
  document = queryResult[0]

Queries : Simple queries by example
-------
.. code:: python

  example = {'species' : "human"}
  query = collection.fetchByExample(example, batchSize = 20, count = True)
  print query.count # print the total number or documents

Queries : Batches
-------

.. code:: python

  while query.hasMore :
    query.nextBatch()
    print query[0]['name']

Validation
-------
.. code:: python

  from arangocity.Collection import *
  
  def cstFct(value) :
    return value != "human"
    
  class Humans(Collection) :
  
    _validate_fields_on_save = True
  	_validate_fields_on_set = True
  	_allow_foreign_fields = True # allow fields that are not part of the schema
  	
  	_fields = {
  	  'name' : Field(NotNull = True),
  	  'anything' : Field(),
  	  'species' : Field(NotNull = True, constraintFct = cstFct)
  	}
