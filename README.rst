pyArango (alpha)
==========

Key Features :
--------------
 - Light and Simple interface
 - Built-in Validation of fields
 - Caching of documents with Insertions and Lookups in O(1)

pyArango is still in active developpement, but it's tested and perfectly usable.
pyArango aims to be an easy to use driver for arangoDB with built in validation. Collections are treated as types that apply to the documents within. You can be 100% permissive or enforce schemas and validate fields on set, on save or on both.
I am developping pyArango for the purpose of an other project and adding features as they are needed.


Initiatilisation and document saving
---------

.. code:: python
  
  from pyArango.Connection import *
  
  conn = Connection()
  conn.createDatabase(name = "test_db")
  db = self.conn["test_db"] #all databases are loaded automatically into the connection and accessible in this fashion
  collection = db.createCollection(name = "users") #all collection are loaded automatically into the database and accessible in this fashion
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
    print query[0]['name']
    query.nextBatch()

Validation
-------
.. code:: python

  from pyArango.Collection import *
  
  def cstFct(value) :
    return value == "human"
    
  class Humans(Collection) :
    
    _validation = {
      'on_save' : False,
      'on_set' : False,
      'allow_foreign_fields' : True # allow fields that are not part of the schema
    }
  	
  	_fields = {
  	  'name' : Field(NotNull = True),
  	  'anything' : Field(),
  	  'species' : Field(NotNull = True, constraintFct = cstFct)
  	}
  	
  collection = db.createCollection('Humans')

A note on inheritence:
----------------------

pyArango does not support the inheritence of the "_validation" and "_fields" dictionaries.
If a class does not fully define it's own, the defaults will be automatically assigned to any missing value.

Creating Edges:
---------

.. code:: python

  from pyArango.Collection import Edges
  
  class Connections(Edges) :
    
    _validation = {
      'on_save' : False,
      'on_set' : False,
      'allow_foreign_fields' : True # allow fields that are not part of the schema
    }
  	
  	_fields = {
  	  'length' : Field(NotNull = True),
  	}
  	
Linking Documents with Edges:
--------------

.. code:: python

 from pyArango.Collection import *
 
 class Things(Collection) :
   ....

 class Connections(Edges) :
   ....

 ....
 a = myThings.createDocument()
 b = myThings.createDocument()
 
 conn = myConnections.createEdge()
 
 conn.links(a, b)
 conn["someField"] = 35
 conn.save() #once an edge links documents, save() and patch() can be used as with any other Document object


Geting Edges linked to a vertex:
--------------

You can do it either from a Document or an Edges collection:

.. code:: python
 # in edges
 myDocument.getInEdges(myConnections)
 myConnection.getInEdges(myDocument)
 
 # out edges
 myDocument.getOutEdges(myConnections)
 myConnection.getOutEdges(myDocument)
 
 # both
 myDocument.getEdges(myConnections)
 myConnection.getEdges(myDocument)
 
 #you can also of ask for the raw json with
 myDocument.getInEdges(myConnections, rawResults = True)
 #otherwise Document objects are retuned in a list
