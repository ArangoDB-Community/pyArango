"""Gevent Session."""

try:
    import grequests
    import gevent
    from gevent.threading import Lock
except ModuleNotFoundError as e:
    print("grequests is not installed, try pip install grequests")
    raise e

try:
    import gevent
    from gevent.threading import Lock
except ModuleNotFoundError as e:
    print("gevent is not installed, try pip install gevent")
    raise e

import logging
import requests
from requests import exceptions as requests_exceptions

from .jwauth import JWTAuth
from .ca_certificate import CA_Certificate

class AikidoSession_GRequests(object):
    """A version of Aikido that uses grequests."""

    def __init__(
            self, username, password, urls, use_jwt_authentication=False,
            use_lock_for_reseting_jwt=True, max_retries=5, verify=None
    ):
        self.max_retries = max_retries
        self.use_jwt_authentication = use_jwt_authentication
        if username:
            if self.use_jwt_authentication:
                self.auth = JWTAuth(
                    username, password, urls,
                    use_lock_for_reseting_jwt, max_retries
                )
            else:
                self.auth = (username, password)

                if (verify is not None) and not isinstance(verify, bool) and not isinstance(verify, CA_Certificate) and not isinstance(verify, str) :
                    raise ValueError("'verify' argument can only be of type: bool, CA_Certificate or str or None")
                self.verify = verify
        else:
            self.auth = None

    def __reset_auth(self):
        if not self.use_jwt_authentication:
            return
        if self.auth.lock_for_reseting_jwt is not None:
            self.auth.lock_for_reseting_jwt.acquire()
        self.auth.reset_token()
        if self.auth.lock_for_reseting_jwt is not None:
            self.auth.lock_for_reseting_jwt.release()

    def _run(self, req):
        """Run the request."""
        if not self.use_jwt_authentication and self.verify is not None:
            if isinstance(self.verify, CA_Certificate):
                req.kwargs['verify'] = self.verify.get_file_path()
            else :
                req.kwargs['verify'] = self.verify
        for _ in range(self.max_retries):
            gevent.joinall([gevent.spawn(req.send)])
            if self.use_jwt_authentication:
                if hasattr(req, 'exception'):
                    logging.critical("%s is raised, will try to reset the auth and request again.", req.exception)
                    self.__reset_auth()
                elif req.response.status_code == 401:
                    logging.critical("Invalid authentication token provided, will try to reset the auth and request again.")
                    self.__reset_auth()
                else:
                    return req.response
            else:
                if hasattr(req, 'exception'):
                    logging.critical("%s is raised, will try to request again", req.exception)
                elif req.response.status_code == 401:
                    logging.critical("Unauthorized access, you must supply a (username, password) with the correct credentials")
                else:
                    return req.response
        logging.critical("Tried to send the request max number of times.")
        return req.response

    def post(self, url, data=None, json=None, **kwargs):
        """HTTP POST Method."""
        if data is not None:
            kwargs['data'] = data
        if json is not None:
            kwargs['json'] = json

        kwargs['auth'] = self.auth

        req = grequests.post(url, **kwargs)
        return self._run(req)

    def get(self, url, **kwargs):
        """HTTP GET Method."""
        kwargs['auth'] = self.auth
        req = grequests.get(url, **kwargs)
        return self._run(req)

    def put(self, url, data=None, **kwargs):
        """HTTP PUT Method."""
        if data is not None:
            kwargs['data'] = data
        kwargs['auth'] = self.auth
        req = grequests.put(url, **kwargs)
        return self._run(req)

    def head(self, url, **kwargs):
        """HTTP HEAD Method."""
        kwargs['auth'] = self.auth
        req = grequests.head(url, **kwargs)
        return self._run(req)

    def options(self, url, **kwargs):
        """HTTP OPTIONS Method."""
        kwargs['auth'] = self.auth
        req = grequests.options(url, **kwargs)
        return self._run(req)

    def patch(self, url, data=None, **kwargs):
        """HTTP PATCH Method."""
        if data is not None:
            kwargs['data'] = data
        kwargs['auth'] = self.auth
        req = grequests.patch(url, **kwargs)
        return self._run(req)

    def delete(self, url, **kwargs):
        """HTTP DELETE Method."""
        kwargs['auth'] = self.auth
        req = grequests.delete(url, **kwargs)
        return self._run(req)

    def disconnect(self):
        pass
