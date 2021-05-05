.. pyArango documentation master file, created by
   sphinx-quickstart on Sat Feb  7 19:33:06 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pyArango's documentation!
====================================

.. image:: https://travis-ci.org/tariqdaouda/pyArango.svg?branch=1.2.2
    :target: https://travis-ci.org/tariqdaouda/pyArango
.. image:: https://img.shields.io/badge/python-2.7%2C%203.5-blue.svg
.. image:: https://img.shields.io/badge/arangodb-3.6-blue.svg

pyArango is a python driver for the NoSQL amazing database ArangoDB_ first written by `Tariq Daouda`_. As of January 2019 pyArango was handed over to the ArangoDB-Community that now ensures the developement and maintenance. It has a very light interface and built in validation. pyArango is distributed under the ApacheV2 Licence and the full source code can be found on github_.

Key Features
------------
pyArango is geared toward the developer. It's here to help to you develop really cool apps using ArangoDB, really fast.

 - Light and Simple interface
 - Built-in Validation of fields on setting or on saving
 - Support for all index types
 - Supports graphs, traversals and all types of queries
 - Caching of documents with Insertions and Lookups in O(1)

Collections are treated as types that apply to the documents within. That means that you can define
a Collection and then create instances of this Collection in several databases. The same goes for graphs

In other words, you can have two databases **cache_db** and **real_db** each of them with an instance of a
**Users** Collection. You can then be assured that documents of both collections will be subjected to the same
validation rules. Ain't that cool?

You can be 100% permissive or enforce schemas and validate fields, on set, on save or both.

.. _ArangoDB: http://www.arangodb.com
.. _Tariq Daouda: http://www.tariqdaouda.com
.. _github: https://github.com/tariqdaouda/pyArango

Installation:
-------------

From PyPi:

.. code::

	pip install pyArango

For the latest version:

.. code::

	git clone https://github.com/tariqdaouda/pyArango.git
	cd pyArango
	python setup.py develop

Quickstart:
-----------

pyArango's github has list of examples to get you started here_.

.. _here: https://github.com/tariqdaouda/pyArango

Contents:
---------

.. toctree::
   :maxdepth: 3

   connection
   database
   collection
   indexes
   document
   users
   query
   graph
   users
   exceptions
   validation
   admin
   ca_certificate
   foxx
   jwauth
   tasks
   gevent_session

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

