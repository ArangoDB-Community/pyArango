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
