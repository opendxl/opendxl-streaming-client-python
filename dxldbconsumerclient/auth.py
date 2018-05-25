import requests
from requests import RequestException
from .error import TemporaryError, PermanentError
from furl import furl


class TemporaryAuthenticationError(TemporaryError):
    pass


class PermanentAuthenticationError(PermanentError):
    pass


def login(url, username, password, path_fragment="/identity/v1/login"):
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
