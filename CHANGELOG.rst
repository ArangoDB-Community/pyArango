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