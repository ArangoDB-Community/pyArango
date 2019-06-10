"""All foxx related methods."""
from .action import DatabaseAction


class Foxx:
    """A generic foxx function executor."""

    def __init__(self, database):
        """Initialise database and its services."""
        self.database = database
        self.services = []
        self.mounts = {}

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
        if mount not in self.mounts:
            self.reload()
        if mount not in self.mounts:
            raise ValueError("Unable to find the mount: '%s'", mount)
        return FoxxService(self.database, mount)

    def get_available_services(self):
        response = self.database.action.get('/_api/foxx', params={'excludeSystem': False})
        response.raise_for_status()
        return response.json()

    def reload(self):
        self.services = self.get_available_services()
        self.mounts = {service['mount'] for service in self.services}



class FoxxService(DatabaseAction):
    """A foxx mount function executor."""

    def __init__(self, database, mount):
        """Initialise mount and database."""
        self.database = database
        self.mount = mount

    @property
    def end_point_url(self):
        """End point url for foxx service."""
        return '%s/_db/%s%s' % (
            self.database.connection.getEndpointURL(), self.database.name,
            self.mount
        )
