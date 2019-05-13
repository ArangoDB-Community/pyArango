"""Gevent Session."""

from base64 import b64decode
import time
import json as json_mod
import logging


import requests
from requests import exceptions as requests_exceptions

import grequests
import gevent
from gevent.threading import Lock


class JWTAuth(requests.auth.AuthBase):

    # Half a day before the actual expiration.
    REAUTH_TIME_INTERVEL = 43200

    def __init__(
            self, username, password, urls, use_lock_for_reseting_jwt=False,
            max_retries=5
    ):
        self.username = username
        self.password = password
        self.urls = urls
        self.lock_for_reseting_jwt = Lock() if use_lock_for_reseting_jwt else None
        self.__init_request_session(max_retries)
        self.__set_token()

    def __init_request_session(self, max_retries):
        self.max_retries = max_retries
        self.session = requests.Session()
        http = requests.adapters.HTTPAdapter(max_retries=max_retries)
        https = requests.adapters.HTTPAdapter(max_retries=max_retries)
        self.session.mount('http://', http)
        self.session.mount('https://', https)

    def __parse_token(self):
        decoded_token = b64decode(self.token.split('.')[1].encode())
        return json_mod.loads(decoded_token.decode())

    def __get_auth_token(self):
        request_data = '{"username":"%s","password":"%s"}' % (self.username, self.password)
        for connection_url in self.urls:
            try:
                response = self.session.post('%s/_open/auth' % connection_url, data=request_data)
                if response.ok:
                    json_data = response.content
                    if json_data:
                        data_dict = json_mod.loads(json_data.decode("utf-8"))
                        return data_dict.get('jwt')
            except requests_exceptions.ConnectionError:
                if connection_url is not self.urls[-1]:
                    logging.critical("Unable to connect to %s trying another", connection_url)
                else:
                    logging.critical("Unable to connect to any of the urls: %s", self.urls)
                    raise

    def __set_token(self):
        self.token = self.__get_auth_token()
        self.parsed_token = \
            self.__parse_token() if self.token is not None else {}
        self.token_last_updated = time.time()

    def reset_token(self):
        logging.warning("Reseting the token.")
        self.__set_token()

    def is_token_expired(self):
        return (
            self.parsed_token.get("exp", 0) - time.time() <
            JWTAuth.REAUTH_TIME_INTERVEL
        )

    def __call__(self, req):
        # Implement JWT authentication
        if self.is_token_expired():
            if self.lock_for_reseting_jwt is not None:
                self.lock_for_reseting_jwt.acquire()
            if self.is_token_expired():
                self.reset_token()
            if self.lock_for_reseting_jwt is not None:
                self.lock_for_reseting_jwt.release()
        req.headers['Authorization'] = 'Bearer %s' % self.token
        return req


class AikidoSession_GRequests(object):
    """A version of Aikido that uses grequests and can bacth several requests together"""

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
                if verify is not None:
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
        """Run request or append it to the the current batch"""
        if not self.use_jwt_authentication and self.verify is not None:
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
        """HTTP Method"""
        if data is not None:
            kwargs['data'] = data
        if json is not None:
            kwargs['json'] = json

        kwargs['auth'] = self.auth

        req = grequests.post(url, **kwargs)
        return self._run(req)

    def get(self, url, **kwargs):
        """HTTP Method"""
        kwargs['auth'] = self.auth
        req = grequests.get(url, **kwargs)
        return self._run(req)

    def put(self, url, data=None, **kwargs):
        """HTTP Method"""
        if data is not None:
            kwargs['data'] = data
        kwargs['auth'] = self.auth
        req = grequests.put(url, **kwargs)
        return self._run(req)

    def head(self, url, **kwargs):
        """HTTP Method"""
        kwargs['auth'] = self.auth
        req = grequests.put(url, **kwargs)
        return self._run(req)

    def options(self, url, **kwargs):
        """HTTP Method"""
        kwargs['auth'] = self.auth
        req = grequests.options(url, **kwargs)
        return self._run(req)

    def patch(self, url, data=None, **kwargs):
        """HTTP Method"""
        if data is not None:
            kwargs['data'] = data
        kwargs['auth'] = self.auth
        req = grequests.patch(url, **kwargs)
        return self._run(req)

    def delete(self, url, **kwargs):
        """HTTP Method"""
        kwargs['auth'] = self.auth
        req = grequests.delete(url, **kwargs)
        return self._run(req)

    def disconnect(self):
        pass
