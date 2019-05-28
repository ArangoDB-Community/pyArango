"""All foxx related methods."""


class Foxx(object):
    """A foxx function executor."""

    def __init__(self, database):
        """Initialise the database."""
        self.database = database

    def getBaseUrl(self):
        """Database base url for calling foxx."""
        return '%s/_db/%s' % (
            self.database.connection.getEndpointURL(), self.database.name
        )

    def get(self, url, **kwargs):
        """HTTP get Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.database.connection.session.get(foxx_url, **kwargs)

    def post(self, url, data=None, json=None, **kwargs):
        """HTTP post Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.database.connection.session.post(
            foxx_url, data, json, **kwargs
        )

    def put(self, url, data=None, **kwargs):
        """HTTP put Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.database.connection.session.put(foxx_url, data, **kwargs)

    def head(self, url, **kwargs):
        """HTTP head Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.database.connection.session.head(foxx_url, **kwargs)

    def options(self, url, **kwargs):
        """HTTP options Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.database.connection.session.options(foxx_url, **kwargs)

    def patch(self, url, data=None, **kwargs):
        """HTTP patch Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.database.connection.session.patch(foxx_url, data, **kwargs)

    def delete(self, url, **kwargs):
        """HTTP delete Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.database.connection.session.delete(foxx_url, **kwargs)
