Arangocity (alpha)
==========

Arangocity is still in active developpement, but it's tested and perfectly usable.
Arangocity aims to be an easy to use driver for arangoDB with built in validation. Collections are treated as types that apply to the documents within. You can be 100% permissive or enforce schemas and validate fields on set, on save or on both.
I am developping Arangocity for the purpose of an other project and adding features as they are needed.

Quick Doc
=========

Basic use
---------

.. code:: python

  conn = Connection()
  conn.createDatabase(name = "test_db")
  db = self.conn["test_db"]
  collection = db.createCollection(name = "users")
  
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
doc.savePatch() # updates the modified field

