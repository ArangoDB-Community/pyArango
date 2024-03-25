2.1.1
=====
* Added missing fields value settings on getitem

=====

2.1.0
=====
* Added getitem for documents at the database level
* Added fill_default() on documents to replace None values by schema defaults
* fill_default() is automatically called on save
=====

2.0.3
=====
* Added support for authentication via client-side certificates

2.0.2
=====
* Fixed contains functions
* Added UniqueConstrainViolation exception, inherits from CreationError

2.0.1
=====

* Fixed max retries for write conflicts
* Added parameter ``pool_maxsize`` on class ``Connection`` to allow user configure the http pool size.
=======

2.0
=====

* changed the default value of reject_zero in NotNull from True to False
* added to_default function to reset a document to its default values
* fixed bug in default documents where default values could be overwritten
* default value for fields is now None

1.3.5
=====

* restoreIndex and restoreIndexes in collection will restore previously deleted indexes
* added max_conflict_retries to handle arango's 1200
* added single session so AikidoSessio.Holders can share a single request session
* added task deletion to tests reset
* added drop() to tasks to remove all tasks in one command 
* better documentation of connection class
* False is not considered a Null value while validating
* Removed redundant document creation functions
* More explicit validation error with field name

1.3.4
=====
* Bugfix: Query iterator now returns all elements instead of a premature empty list
* Bugfix: Collection naming when using the arango's name argument
* New: Schema validation example
* New: Satellite graphs

1.3.3
=====

* SSL certificate support
* More doc
* Fixed on_load schema validation
* Gevent, monkey patching breaks python's multi=processing. Removed grequests as the default, back to requests.
* Removed grequests and gevent as hard dependencies. Added explicit error messages, to prompt users can install them if needed.
* Jwauth is not in its own file
* Generic rest call to database support (action) for connection, database.
* Foxx support
* Tasks create, delete, fetch support

1.3.2
=====

* Validation bug fixes
* New Numeric, Int, Bool, String, Enumeration, Range validators
* Fields can have default values
* When creating a new document, Collection will serve one populated with defaults
* stastd support thx to: @dothebart
* properties definition in schema
* AQL errors now come with prints and line numbers for everyone's convenience
* Bulk save for Document objects and dicts

1.3.1
=====

* Will die gracefully if server response is empty
* getStore and getPatches shorthands added to Document

1.3.0
=====

* Fixed nested store patch update
* REFACT: New DocumentStore class for taking care of storing document in a hierarchy of stores (nested objects) and validate them
* Minor bug fixes

1.2.9
=====

* Added bulk import to connection
* Added bindvars to explain

1.2.8
=====

* BugFix: recursive field validation
* BugFix: fullCount option now works
* Length validator will raise a ValidationError if value has no length
* users can now specify custom json encoders

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
* Added AikidoSession to seemlessly manage request sessions
* AikidoSession stores basic stats about the requests
* AikidoSession detects 401 errors and notifies the user that authentication is required
* AikidoSession detects connection errors and notifies the user that arango is probably not running
* save() and patch() functions now empty _patchStore is succesfull
* Added free key word arguments for the creation of AQL Queries
