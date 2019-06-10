from base64 import b64decode
import time
import json as json_mod
import logging


import requests
from requests import exceptions as requests_exceptions


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
