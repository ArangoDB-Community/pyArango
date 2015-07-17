Python Object Wrapper for ArangoDB_ with built-in validation
===========================================================

pyArango aims to be an easy to use driver for ArangoDB with built in validation. Collections are treated as types that apply to the documents within. You can be 100% permissive or enforce schemas and validate fields on set, on save or on both.

pyArango supports graphs, indexes and probably everything that arangodb_ can do.

pyArango is developed by `Tariq Daouda`_, the full source code is available from github_.

.. _Tariq Daouda: http://bioinfo.iric.ca/~daoudat/
.. _github: https://github.com/tariqdaouda/pyArango
.. _arangodb: http://www.arangodb.com
.. _ArangoDB: http://www.arangodb.com

For the latest news about pyArango, you can follow me on twitter `@tariqdaouda`_.
If you have any issues with it, please file a github issue.

.. _@tariqdaouda: https://www.twitter.com/tariqdaouda

Changelog
===========

1.0.3
-------
* Added support for all types of indexes
* Connections are now managed into sessions that the user can restart if necessary. This allows for a much more stable interaction with ArangoDB when a lot of queries are involved