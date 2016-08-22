1.2.7
=====

* Fixed connection reuse

1.2.6
=====

* Fixed Cache

* Cache now exposes document store and attributes transparently

1.2.5
=====

* Added getter for users

* Edges back compatibility with 2.8 solved "_from" "_to" are no longer foreign fields, ._from ._to work again

* Calls to json() now print the request's content upon failure.


1.2.4
=====

* missing import in collections.py added

1.2.3
=====

* Some more meaningful error messages

1.2.2
======

* Cross python support for iterators

1.2.1
======

* Cross python support for metclasses

1.2.0
======

* Support for python 3, does not support python 2.7 yet.
* Test root password and username can be defined in environement variables.

1.1.0
======

* Support for ArangoDB 3.X, pyArango no longer supports 2.X versions
* Support for authentication
* User support added
* Adedd AikidoSession to seemlessly manage request sessions
* AikidoSession stores basic stats about the requests
* AikidoSession detects 401 errors and notifies the user that authentication is required
* AikidoSession detects connection errors and notifies the user that arango is probably not running
* save() and patch() functions now empty _patchStore is succesfull
* Added free key word arguments for the creation of AQL Queries
