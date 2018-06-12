""" Authentication APIs """

from __future__ import absolute_import
import warnings
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


def login(url, username, password, path_fragment="/identity/v1/login",
          verify_cert_bundle=""):
    """
    Make a login request to the supplied login url.

    :param str url: Base URL at which to make the request.
    :param str username: User name to supply for request authentication.
    :param str password: Password to supply for request authentication.
    :param str path_fragment: Path to append to the base URL for the request.
    :param str verify_cert_bundle: Path to a CA bundle file containing
        certificates of trusted CAs. The CA bundle is used to validate that the
        certificate of the authentication server being connected to was signed
        by a valid authority. If set to an empty string, the server certificate
        is not validated.
    :raise TemporaryAuthenticationError: if an unexpected (but possibly
        recoverable) authentication error occurs for the request.
    :raise PermanentAuthenticationError: if the request fails due to the
        user not being authenticated successfully or if the user is
        unauthorized to make the request.
    """
    auth = (username, password)
    try:
        with warnings.catch_warnings():
            if not verify_cert_bundle:
                warnings.filterwarnings("ignore", "Unverified HTTPS request")
            res = requests.get(furl(url).add(path=path_fragment).url,
                               auth=auth,
                               verify=verify_cert_bundle)
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
