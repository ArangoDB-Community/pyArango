pyArango
========

.. image:: https://pepy.tech/badge/pyarango
   :alt: downloads
   :target: https://pepy.tech/project/pyarango

.. image:: https://pepy.tech/badge/pyarango/month
   :alt: downloads_month
   :target: https://pepy.tech/project/pyarango/month

.. image:: https://pepy.tech/badge/pyarango/week
   :alt: downloads_week
   :target: https://pepy.tech/project/pyarango/week
   
.. image:: https://travis-ci.com/Alexsaphir/pyArango.svg?branch=master
    :target: https://travis-ci.com/github/Alexsaphir/pyArango
.. image:: https://img.shields.io/badge/python-2.7%2C%203.5-blue.svg
.. image:: https://img.shields.io/badge/arangodb-3.0-blue.svg

NoSQL is really cool, but in this harsh world it is impossible to live without field validation.

**WARNING**: The last versions of pyArango are only compatible with ArangoDB 3.X. For the old version checkout the branch ArangoDBV2_

.. _ArangoDBV2: https://github.com/tariqdaouda/pyArango/tree/ArangoDBV2

Key Features
------------
pyArango is geared toward the developer. It's here to help to you develop really cool apps using ArangoDB, really fast.

 - Light and simple interface
 - Built-in validation of fields on setting or on saving
 - Support for all index types
 - Supports graphs, traversals and all types of queries
 - Caching of documents with Insertions and Lookups in O(1)

Collections are treated as types that apply to the documents within. That means you can define
a Collection and then create instances of this Collection in several databases. The same goes for graphs.

In other words, you can have two databases, **cache_db** and **real_db**, each of them with an instance of a
**Users** Collection. You can then be assured that documents of both collections will be subjected to the same
validation rules. Ain't that cool?

You can be 100% permissive or enforce schemas and validate fields on set, on save or both.

Installation
------------

Supports python 2.7 and 3.5.

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

This is the quickstart guide; you can find the full documentation here_.

.. _here: https://pyarango.readthedocs.io/en/stable/

Initialization and document saving
-------------------------------------

.. code:: python

  from pyArango.connection import *

  conn = Connection()

  conn.createDatabase(name="test_db")
  db = conn["test_db"] # all databases are loaded automatically into the connection and are accessible in this fashion
  collection = db.createCollection(name="users") # all collections are also loaded automatically

  # collection.delete() # self explanatory

  for i in xrange(100):
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
  bindVars = {'name': 'Tesla-3'}
  # by setting rawResults to True you'll get dictionaries instead of Document objects, useful if you want to result to set of fields for example
  queryResult = db.AQLQuery(aql, rawResults=False, batchSize=1, bindVars=bindVars)
  document = queryResult[0]

Queries : Simple queries by example
-------------------------------------
PyArango supports all types of simple queries (see collection.py for the full list). Here's an example query:

.. code:: python

  example = {'species': "human"}
  query = collection.fetchByExample(example, batchSize=20, count=True)
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
from **Validator** and implement a **validate()** method:

.. code:: python

  import pyArango.collection as COL
  import pyArango.validation as VAL
  from pyArango.theExceptions import ValidationError
  import types

  class String_val(VAL.Validator):
   def validate(self, value):
       if type(value) is not types.StringType :
           raise ValidationError("Field value must be a string")
       return True

  class Humans(COL.Collection):

      _validation = {
          'on_save': False,
          'on_set': False,
          'allow_foreign_fields': True  # allow fields that are not part of the schema
      }

      _fields = {
          'name': COL.Field(validators=[VAL.NotNull(), String_val()]),
          'anything': COL.Field(),
          'species': COL.Field(validators=[VAL.NotNull(), VAL.Length(5, 15), String_val()])
      }

  collection = db.createCollection('Humans')


In addition, you can also define collection properties_ (creation arguments for ArangoDB) right inside the definition:

.. code:: python

  class Humans(COL.Collection):

    _properties = {
        "keyOptions" : {
            "allowUserKeys": False,
            "type": "autoincrement",
            "increment": 1,
            "offset": 0,
        }
    }

      _validation = {
          'on_save': False,
          'on_set': False,
          'allow_foreign_fields': True  # allow fields that are not part of the schema
      }

      _fields = {
          'name': COL.Field(validators=[VAL.NotNull(), String_val()]),
          'anything': COL.Field(),
          'species': COL.Field(validators=[VAL.NotNull(), VAL.Length(5, 15), String_val()])
      }

.. _properties: https://docs.arangodb.com/3.1/HTTP/Collection/Creating.html

A note on inheritence
----------------------

There is no inheritance of the "_validation" and "_fields" dictionaries.
If a class does not fully define its own, the defaults will be automatically assigned to any missing value.

Creating Edges
----------------

.. code:: python

  from pyArango.collection import Edges

  class Connections(Edges):

      _validation = {
          'on_save': False,
          'on_set': False,
          'allow_foreign_fields': True # allow fields that are not part of the schema
      }

      _fields = {
          'length': Field(NotNull=True),
      }

Linking Documents with Edges
-----------------------------

.. code:: python

 from pyArango.collection import *

 class Things(Collection):
   ....

 class Connections(Edges):
   ....

 ....
 a = myThings.createDocument()
 b = myThings.createDocument()

 conn = myConnections.createEdge()

 conn.links(a, b)
 conn["someField"] = 35
 conn.save() # once an edge links documents, save() and patch() can be used as with any other Document object


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

  # you can also of ask for the raw json with
  myDocument.getInEdges(myConnections, rawResults=True)
  # otherwise Document objects are retuned in a list

Creating a Graph
-----------------

By using the graph interface you ensure for example that, whenever you delete a document, all the edges linking
to that document are also deleted:

.. code:: python

 from pyArango.collection import Collection, Field
 from pyArango.graph import Graph, EdgeDefinition

 class Humans(Collection):
     _fields = {
         "name": Field()
     }

 class Friend(Edges): # theGraphtheGraph
     _fields = {
         "lifetime": Field()
     }

 # Here's how you define a graph
 class MyGraph(Graph) :
     _edgeDefinitions = [EdgeDefinition("Friend", fromCollections=["Humans"], toCollections=["Humans"])]
     _orphanedCollections = []

 # create the collections (do this only if they don't already exist in the database)
 self.db.createCollection("Humans")
 self.db.createCollection("Friend")
 # same for the graph
 theGraph = self.db.createGraph("MyGraph")

 # creating some documents
 h1 = theGraph.createVertex('Humans', {"name": "simba"})
 h2 = theGraph.createVertex('Humans', {"name": "simba2"})

 # linking them
 theGraph.link('Friend', h1, h2, {"lifetime": "eternal"})

 # deleting one of them along with the edge
 theGraph.deleteVertex(h2)

Creating a Satellite Graph
-----------------

If you want to benefit from the advantages of satellite graphs, you can also create them of course.
Please read the official ArangoDB Documentation for further technical information.

.. code:: python

  from pyArango.connection import *
  from pyArango.collection import Collection, Edges, Field
  from pyArango.graph import Graph, EdgeDefinition

  databaseName = "satellite_graph_db"

  conn = Connection()

  # Cleanup (if needed)
  try:
      conn.createDatabase(name=databaseName)
  except Exception:
      pass

  # Select our "satellite_graph_db" database
  db = conn[databaseName] # all databases are loaded automatically into the connection and are accessible in this fashion

  # Define our vertex to use
  class Humans(Collection):
      _fields = {
          "name": Field()
      }

  # Define our edge to use
  class Friend(Edges):
      _fields = {
          "lifetime": Field()
      }

  # Here's how you define a Satellite Graph
  class MySatelliteGraph(Graph) :
      _edgeDefinitions = [EdgeDefinition("Friend", fromCollections=["Humans"], toCollections=["Humans"])]
      _orphanedCollections = []

  theSatelliteGraph = db.createSatelliteGraph("MySatelliteGraph")

Document Cache
--------------

pyArango collections have a caching system for documents that performs insertions and retrievals in O(1):

.. code:: python

 # create a cache a of 1500 documents for collection humans
 humans.activateCache(1500)

 # disable the cache
 humans.deactivateCache()

Statsd Reporting
----------------

pyArango can optionally report query times to a statsd server for statistical evaluation:

  import statsd
  from pyArango.connection import Connection
  statsdclient = statsd.StatsClient(os.environ.get('STATSD_HOST'), int(os.environ.get('STATSD_PORT')))
  conn = Connection('http://127.0.0.1:8529', 'root', 'opensesame', statsdClient = statsdclient, reportFileName = '/tmp/queries.log')

It's intended to be used in a two phase way: (we assume you're using bind values - right?)
 - First run, which will trigger all usecases. You create the connection by specifying statsdHost, statsdPort and reportFileName.
   reportFilename will be filled with your queries paired with your hash identifiers. It's reported to statsd as 'pyArango_<hash>'.
   Later on you can use this digest to identify your queries to the gauges.
 - On subsequent runs you only specify statsdHost and statsdPort; only the request times are reported to statsd.
 
Examples
========
More examples can be found in the examples directory.
To try them out change the connection strings according to your local setup.

Debian Dependency Graph
-----------------------
If you are on a Debian / Ubuntu you can install packages with automatic dependency resolution.
In the end this is a graph. This example parses Debian package files using the `deb_pkg_tools`,
and will then create vertices and edges from packages and their relations.

Use `examples/debiangraph.py` to install it, or `examples/fetchDebianDependencyGraph.py` to browse
it as an ascii tree.

ArangoDB Social Graph
---------------------
You can create the `ArangoDB SocialGraph <https://docs.arangodb.com/latest/Manual/Graphs/#the-social-graph>`_ using `examples/createSocialGraph.py`.
It resemples `The original ArangoDB Javascript implementation: <https://github.com/arangodb/arangodb/blob/devel/js/common/modules/%40arangodb/graph-examples/example-graph.js#L56>`_ in python.
