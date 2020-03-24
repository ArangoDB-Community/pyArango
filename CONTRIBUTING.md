pyArango contributor guidelines
===============================

Thank you for your interest in pyArango, is now a community project and everybody is welcome to join in and contribute.
We do not a have specific schedule for releases and pyArango's releases tend be in synch with ArangoDB releases.

Our guidelines are simple:

* Write beautiful code.
* The master branch is the stable branch. Only critical pull requests are directly merged into this branch.
* Send all non-critical pull requests to the dev branch.
* If you add a new feature please provide a test for it. Otherwise your pull request might be rejected.
* Any pull request that improves code coverage is highly appreciated.
* Function names are arguments follow the naming used in ArangoDB's API documentation (hence the camel case).
* Update the CHANGELOG.rst. We use a very simple nomenclature. Bug fixes descriptions are prefixed with *bugfix:*, new features with *new:*, removed features with *removed:*.

