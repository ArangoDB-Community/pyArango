1.1.0
======

* Support for ArangoDB 3.X, pyArango no longer supports 2.X versions
* Support for anthentification
* Adedd AikidoSession to seemlessly manage request sessions
* AikidoSession stores basic stats about the requests
* save() and patch() functions now empty _patchStore is succesfull
* _from and _to are are no longer private object attribute, they are now accessed using ["_from"] and ["_to"].