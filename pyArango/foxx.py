"""All foxx related methods."""


class _BaseFoxx:
    """Base class for using the session to execute foxx."""

    @property
    def session(self):
        """Session of the connection."""
        raise NotImplementedError

    def getBaseUrl(self):
        """Database base url for calling foxx."""
        raise NotImplementedError

    def get(self, url, **kwargs):
        """HTTP GET Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.session.get(foxx_url, **kwargs)

    def post(self, url, data=None, json=None, **kwargs):
        """HTTP POST Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.session.post(
            foxx_url, data, json, **kwargs
        )

    def put(self, url, data=None, **kwargs):
        """HTTP PUT Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.session.put(foxx_url, data, **kwargs)

    def head(self, url, **kwargs):
        """HTTP HEAD Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.session.head(foxx_url, **kwargs)

    def options(self, url, **kwargs):
        """HTTP OPTIONS Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.session.options(foxx_url, **kwargs)

    def patch(self, url, data=None, **kwargs):
        """HTTP PATCH Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.session.patch(foxx_url, data, **kwargs)

    def delete(self, url, **kwargs):
        """HTTP DELETE Method."""
        foxx_url = '%s%s' % (self.getBaseUrl(), url)
        return self.session.delete(foxx_url, **kwargs)


class Foxx(_BaseFoxx):
    """A generic foxx function executor."""

    def __init__(self, database):
        """Initialise the database."""
        self.database = database

    @property
    def session(self):
        """Session of the connection."""
        return self.database.connection.session

    def getBaseUrl(self):
        """Database base url for calling foxx."""
        return '%s/_db/%s' % (
            self.database.connection.getEndpointURL(), self.database.name
        )

    def service(self, mount):
        """Return a service so that only route after the mount.

        Parameters
        ----------
        mount : str
            mount point.

        Returns
        -------
        FoxxService
            A mounted service

        """
        return FoxxService(self.database, mount)


class FoxxService(_BaseFoxx):
    """A foxx mount function executor."""

    def __init__(self, database, mount):
        """Initialise mount and database."""
        self.database = database
        self.mount = mount

    @property
    def session(self):
        """Session of the connection."""
        return self.database.connection.session

    def getBaseUrl(self):
        """Database base url for calling mounted foxx function."""
        return '%s/_db/%s%s' % (
            self.database.connection.getEndpointURL(), self.database.name,
            self.mount
        )
