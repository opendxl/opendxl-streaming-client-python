""" Auth APIs """

from __future__ import absolute_import
from furl import furl
import requests
from requests import RequestException
from .error import TemporaryError, PermanentError


class TemporaryAuthenticationError(TemporaryError):
    """
    Exception raised when an unexpected/unknown (but possibly recoverable)
    error occurs during an authentication attempt.
    """
    pass


class PermanentAuthenticationError(PermanentError):
    """
    Exception raised when an error occurs during an authentication attempt due to
    the user being unauthorized.
    """
    pass


def login(url, username, password, path_fragment="/identity/v1/login"):
    """
    Make a login request to the supplied login url.

    :param str url: Base URL at which to make the request.
    :param str username: User name to supply for request auth.
    :param str password: Password to supply for request auth.
    :param str path_fragment: Path to append to the base URL for the request.
    :raise TemporaryAuthenticationError: if an unexpected (but possibly
        recoverable) auth error occurs for the request.
    :raise PermanentAuthenticationError: if the request fails due to the
        user not being authenticated successfully or if the user is
        unauthorized to make the request.
    """
    auth = (username, password)
    try:
        res = requests.get(furl(url).add(path=path_fragment).url, auth=auth)
        if res.status_code == 200:
            try:
                token = res.json()['AuthorizationToken']
                return token
            except Exception as exp:
                raise PermanentAuthenticationError(str(exp))
        elif res.status_code == 401 or res.status_code == 403:
            raise PermanentAuthenticationError(
                "Unauthorized {}: {}".format(res.status_code, res.text))
        else:
            raise TemporaryAuthenticationError(
                "Unexpected status code {}: {}".format(
                    res.status_code,
                    res.text
                )
            )
    except RequestException as exp:
        raise TemporaryAuthenticationError(
            "Unexpected error: {}".format(str(exp))
        )
