import json
import logging
import types

import requests

from .connection import Connection
from .theExceptions import (ArangoError)


class Admin(object):
    """administrative tasks with arangodb"""
    def __init__(self, connection):
        self.connection = connection

    def status(self):
        """ fetches the server status."""
        url = "%s/_admin/status" % self.connection.getEndpointURL()
        result = self.connection.session.get(url)
        if result.status_code < 400:
            return result.json()

        raise ArangoError(result.json()['errorMessage'], result.json())

    def is_cluster(self):
        status = self.status()
        return status['serverInfo']['role'] == 'COORDINATOR'
