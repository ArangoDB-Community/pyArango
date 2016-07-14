pyArango
=========

NoSQL is really cool, but in this harsh world it is impossible to live without field validation.

**WARNING**: The last versions of pyArango are only compatible with ArangoDB 3.X. For the old version checkout the branch ArangoDBV2_ 

.. _ArangoDBV2: https://github.com/tariqdaouda/pyArango/tree/ArangoDBV2

Key Features
------------
pyArango, is geared toward the developer. It's here to help to you develop really cool apps using ArangoDB, really fast.

 - Light and Simple interface
 - Built-in Validation of fields on seting or on saving
 - Support for all index types
 - Supports graphs, traversals and all types of queries
 - Caching of documents with Insertions and Lookups in O(1)

Collections are treated as types that apply to the documents within. That means that you can define
a Collection and then create instances of this Collection in several databases. The same goes for graphs

In other words, you can have two databases **cache_db** and **real_db** each of them with an instance of a 
**Users** Collection. You can then be assured that documents of both collections will be subjected to the same 
validation rules. Ain't that cool?

You can be 100% permissive or enforce schemas and validate fields, on set, on save or both.

Installation
------------

Only python 2 is supported.

From PyPi:

.. code:: shell

 pip install pyArango

For the latest version:

.. code:: shell

 git clone https://github.com/tariqdaouda/pyArango.git
 cd pyArango
 python setup.py develop

Full documentation
-------------------

This is the quickstart guide, you can find the full documentation here_.

.. _here: http://pyArango.tariqdaouda.com

Initiatilisation and document saving
-------------------------------------

.. code:: python
  
  from pyArango.connection import *
  
  conn = Connection()
  conn.createDatabase(name = "test_db")
  db = self.conn["test_db"] #all databases are loaded automatically into the connection and are accessible in this fashion
  collection = db.createCollection(name = "users") #all collections are also loaded automatically
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
-------------
  
.. code:: python
  
  aql = "FOR c IN users FILTER c.name == @name LIMIT 10 RETURN c"
  bindVars = {'name' : 'Tesla-3'}
  # by setting rawResults to True you'll get dictionaries instead of Document objects, useful if you want to result to set of fields for example 
  queryResult = db.AQLQuery(aql, rawResults = False, batchSize = 1, bindVars = bindVars)
  document = queryResult[0]

Queries : Simple queries by example
-------------------------------------
PyArango supports all types of simple queries (see collection.py for the full list). Here's how you do a query by example:

.. code:: python

  example = {'species' : "human"}
  query = collection.fetchByExample(example, batchSize = 20, count = True)
  print query.count # print the total number or documents

Queries : Batches
------------------

.. code:: python

  for e in query :
    print e['name']

Defining a Collection and field/schema Validation
-------------------------------------------------

PyArango allows you to implement your own field validation.
Validators are simple objects deriving from classes that inherit
from **Validator** and implement a **validate()** method.

.. code:: python
  
  import pyArango.Collection as COL
  import pyArango.Validator as VAL
  from pyArango.theExceptions import ValidationError
  import types
  
  class String_val(VAL.Validator) :
   def validate(self, value) :
  		if type(value) is not types.StringType :
  			raise ValidationError("Field value must be a string")
  		return True
  
  class Humans(COL.Collection) :
    
    _validation = {
      'on_save' : False,
      'on_set' : False,
      'allow_foreign_fields' : True # allow fields that are not part of the schema
    }
  	
  	_fields = {
  	  'name' : Field(validators = [VAL.NotNull(), String_val()]),
  	  'anything' : Field(),
  	  'species' : Field(validators = [VAL.NotNull(), VAL.Length(5, 15), String_val()])
  	}
  	
  collection = db.createCollection('Humans')

A note on inheritence
----------------------

There is no inheritence of the "_validation" and "_fields" dictionaries.
If a class does not fully define it's own, the defaults will be automatically assigned to any missing value.

Creating Edges
----------------

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
  	
Linking Documents with Edges
-----------------------------

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


Geting Edges linked to a vertex
--------------------------------

You can do it either from a Document or an Edges collection:

.. code:: python
  
  # in edges
  myDocument.getInEdges(myConnections)
  myConnections.getInEdges(myDocument)
  
  # out edges
  myDocument.getOutEdges(myConnections)
  myConnections.getOutEdges(myDocument)
  
  # both
  myDocument.getEdges(myConnections)
  myConnections.getEdges(myDocument)
  
  #you can also of ask for the raw json with
  myDocument.getInEdges(myConnections, rawResults = True)
  #otherwise Document objects are retuned in a list

Creating a Graph
-----------------

By using the graph interface you ensure for example that, whenever you delete a document, all the edges linking
to that document are also deleted.

.. code:: python

 from pyArango.Collection import Collection, Field
 from pyArango.Graph import Graph, EdgeDefinition
 
 class Humans(Collection) :
  _fields = {
  "name" : Field()
  }
 
 class Friend(Edges) :theGraphtheGraph
  _fields = {
  "lifetime" : Field()
  }
 
 #Here's how you define a graph
 class MyGraph(Graph) :
  _edgeDefinitions = (EdgeDefinition("Friend", fromCollections = ["Humans"], toCollections = ["Humans"]), )
  _orphanedCollections = []
 
 #create the collections (do this only if they don't already exist in the database)
 self.db.createCollection("Humans")
 self.db.createCollection("Friend")
 #same for the graph
 theGraph = self.db.createGraph("MyGraph")
 
 #creating some documents
 h1 = theGraph.createVertex('Humans', {"name" : "simba"})
 h2 = theGraph.createVertex('Humans', {"name" : "simba2"})
 
 #linking them
 theGraph.link('Friend', h1, h2, {"lifetime" : "eternal"})
 
 #deleting one of them along with the edge
 theGraph.deleteVertex(h2)

Document Cache
--------------

pyArango collections have a caching system for documents that performs insertions and retrievals in O(1)

.. code:: python

 #create a cache a of 1500 documents for collection humans
 humans.activateCache(1500)
 
 #disable the cache
 humans.deactivateCache()
