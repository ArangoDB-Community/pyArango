1.1.0
======

* Support for ArangoDB 3.X, pyArango no longer supports 2.X versions
* Support for authentication
* Adedd AikidoSession to seemlessly manage request sessions
* AikidoSession stores basic stats about the requests
* AikidoSession detects 401 errors and notifies the user that authentication is required
* AikidoSession detects connection errors and notifies the user that arango is probably not running
* save() and patch() functions now empty _patchStore is succesfull
* _from and _to are are no longer private object attribute, they are now accessed using ["_from"] and ["_to"].