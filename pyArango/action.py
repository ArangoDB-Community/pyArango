"""Action Base Classes to do actions on to db."""

class ConnectionAction:
    """Base class for using the session to execute action."""

    def __init__(self, connection):
        """Initialise connection."""
        self.connection = connection

    @property
    def session(self):
        """Session of the connection."""
        return self.connection.session

    @property
    def end_point_url(self):
        """End point url for connection."""
        return self.connection.getEndpointURL()

    def get(self, url, **kwargs):
        """HTTP GET Method."""
        action_url = '%s%s' % (self.end_point_url, url)
        return self.session.get(action_url, **kwargs)

    def post(self, url, data=None, json=None, **kwargs):
        """HTTP POST Method."""
        action_url = '%s%s' % (self.end_point_url, url)
        return self.session.post(
            action_url, data, json, **kwargs
        )

    def put(self, url, data=None, **kwargs):
        """HTTP PUT Method."""
        action_url = '%s%s' % (self.end_point_url, url)
        return self.session.put(action_url, data, **kwargs)

    def head(self, url, **kwargs):
        """HTTP HEAD Method."""
        action_url = '%s%s' % (self.end_point_url, url)
        return self.session.head(action_url, **kwargs)

    def options(self, url, **kwargs):
        """HTTP OPTIONS Method."""
        action_url = '%s%s' % (self.end_point_url, url)
        return self.session.options(action_url, **kwargs)

    def patch(self, url, data=None, **kwargs):
        """HTTP PATCH Method."""
        action_url = '%s%s' % (self.end_point_url, url)
        return self.session.patch(action_url, data, **kwargs)

    def delete(self, url, **kwargs):
        """HTTP DELETE Method."""
        action_url = '%s%s' % (self.end_point_url, url)
        return self.session.delete(action_url, **kwargs)


class DatabaseAction(ConnectionAction):
    """Base class for using the session to execute action."""

    def __init__(self, database):
        """Initialise database."""
        self.database = database

    @property
    def session(self):
        """Session of the connection."""
        return self.database.connection.session

    @property
    def end_point_url(self):
        """End point url for database."""
        return '%s/_db/%s' % (
            self.database.connection.getEndpointURL(), self.database.name
        )
