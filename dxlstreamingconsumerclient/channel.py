"""
Contains the :class:`Channel` class, which is used to connect to the consumer
service.
"""

from __future__ import absolute_import
from functools import wraps
import base64
import json
import logging
import threading
import time
import warnings
import requests
from retrying import Retrying
from furl import furl
from .auth import login
from .error import TemporaryError, PermanentError
from ._compat import is_string


_RETRY_WAIT_EXPONENTIAL_MULTIPLIER = 1000
_RETRY_WAIT_EXPONENTIAL_MAX = 10000


def _retry(f):
    @wraps(f)
    def retry_wrapper(*args, **kwargs):
        channel = args[0]
        if channel._destroyed:
            raise PermanentError("Channel has been destroyed")
        return Retrying(
            wait_exponential_multiplier=_RETRY_WAIT_EXPONENTIAL_MULTIPLIER,
            wait_exponential_max=_RETRY_WAIT_EXPONENTIAL_MAX,
            retry_on_exception=channel._retry_if_not_consumer_error). \
            call(f, *args, **kwargs)
    return retry_wrapper


class ConsumerError(TemporaryError):
    """
    Error raised when a channel operation fails due to the associated consumer
    not being recognized by the consumer service.
    """
    pass


class ChannelAuth(requests.auth.AuthBase):
    """
    Authentication class for use with channel requests.
    """
    def __init__(self, base, username, password, verify=""):
        """
        Constructor parameters:

        :param str base: Base URL to forward authentication requests to.
        :param str username: User name to supply for request authentication.
        :param str password: Password to supply for request authentication.
        :param str verify: Path to a CA bundle file containing certificates of
            trusted CAs. The CA bundle is used to validate that the certificate
            of the authentication server being connected to was signed by a
            valid authority. If set to an empty string, the server certificate
            is not validated.
        """
        self._username = username
        self._password = password
        self._base = base
        self._token = None
        self._verify = verify
        super(ChannelAuth, self).__init__()

    def reset(self):
        """
        Purge any credentials cached from a previous authentication.
        """
        self._token = None

    def __call__(self, r):
        # Implement my authentication
        if not self._token:
            self._token = login(self._base, self._username,
                                self._password, verify=self._verify)
        r.headers['Authorization'] = "Bearer {}".format(self._token)
        return r


class Channel(object):
    """
    The :class:`Channel` class is responsible for all communication with the
    consumer service.

    The following example demonstrates the creation of a :class:`Channel`
    instance and creating a consumer for the consumer group:

    .. code-block:: python

        # Create the channel
        with Channel("http://channel-server",
                     auth=ChannelAuth("http://channel-server,
                        "user", "password"),
                     consumer_group="thegroup") as channel:
            # Create a new consumer on the consumer group
            channel.create()

    **NOTE:** The preferred way to construct the channel is via the Python
    "with" statement as shown above. The "with" statement ensures that
    resources associated with the channel are properly cleaned up when the block
    is exited.
    """

    # Default number of seconds to wait between consume queries made to the
    # consumer service
    _DEFAULT_WAIT_BETWEEN_QUERIES = 30

    def __init__(self, base, auth,
                 consumer_group,
                 path_prefix='/databus/consumer-service/v1',
                 offset='latest',  # earliest
                 timeout=300,
                 retry_on_fail=True,
                 verify=""):
        """
        Constructor parameters:

        :param str base: Base URL at which the consumer service resides.
        :param requests.auth.AuthBase auth: Authentication object to use
            for channel requests.
        :param str consumer_group: Consumer group to subscribe the channel
            consumer to.
        :param str path_prefix: Path to append to consumer service requests.
        :param str offset: Offset for the next record to retrieve from the
            consumer service for the new :meth:`consume` call. Must be one
            of 'latest', 'earliest', or 'none'.
        :param int timeout: Channel session timeout (in seconds).
        :param bool retry_on_fail: Whether or not the channel will
            automatically retry a call which failed due to a temporary error.
        :param str verify: Path to a CA bundle file containing certificates of
            trusted CAs. The CA bundle is used to validate that the certificate
            of the authentication server being connected to was signed by a
            valid authority. If set to an empty string, the server certificate
            is not validated.
        """
        self._base = base
        self._path_prefix = path_prefix

        if not consumer_group:
            raise PermanentError("Value must be specified for 'consumer_group'")
        self._consumer_group = consumer_group

        offset_values = ['latest', 'earliest', 'none']
        if offset not in offset_values:
            raise PermanentError(
                "Value for 'offset' must be one of: {}".format(
                    ', '.join(offset_values)))
        self._offset = offset
        self._timeout = timeout

        # state variables
        self._consumer_id = None
        self._subscribed = False
        self._records_commit_log = []

        # Create a session object so that we can store cookies across requests
        self._session = requests.Session()
        self._session.auth = auth
        self._session.verify = verify

        self._retry_on_fail = retry_on_fail
        self._retry_if_not_consumer_error = \
            self._retry_if_not_consumer_error_fn()

        self._destroy_lock = threading.RLock()
        self._destroyed = False

    def __enter__(self):
        """Enter with"""
        return self

    def __exit__(self, *_):
        """Exit with"""
        self.destroy()

    def _retry_if_not_consumer_error_fn(self):
        def _retry_if_not_consumer_error(exception):
            should_retry = (not isinstance(exception, ConsumerError) and
                            self.retry_on_fail)
            if should_retry:
                logging.info("Retrying due to: %s", exception)
            else:
                logging.warning(
                    "Will not retry due to: %s %s", exception,
                    "" if self.retry_on_fail else "(retries disabled)")
            return should_retry
        return _retry_if_not_consumer_error

    def _request(self, method, url, **kwargs):
        with warnings.catch_warnings():
            if not self._session.verify:
                warnings.filterwarnings("ignore", "Unverified HTTPS request")
            response = self._session.request(method, url, **kwargs)
            if response.status_code in [401, 403]:
                if hasattr(self._session.auth, "reset"):
                    self._session.auth.reset()
                raise TemporaryError(
                    "Token potentially expired ({}): {}".format(
                        response.status_code, response.text))
        return response

    def _delete_request(self, url, **kwargs):
        return self._request("delete", url, **kwargs)

    def _get_request(self, url, **kwargs):
        return self._request("get", url, **kwargs)

    def _post_request(self, url, json=None, **kwargs): # pylint: disable=redefined-outer-name
        return self._request("post", url, json=json, **kwargs)

    # low-level methods
    def reset(self):
        """
        Resets local consumer data stored for the channel.
        """
        self._consumer_id = None
        self._subscribed = False
        self._records_commit_log = []

    @_retry
    def create(self):
        """
        Creates a new consumer on the consumer group

        :raise TemporaryError: if the creation attempt fails and
            :attr:`retry_on_fail` is set to False.
        :raise PermanentError: if the channel has been destroyed.
        """
        self.reset()

        url = furl(self._base).add(path=self._path_prefix).add(
            path="consumers").url
        payload = {
            'consumerGroup': self._consumer_group,
            'configs': {
                'session.timeout.ms': str(self._timeout * 1000),
                'enable.auto.commit': 'false',  # this has to be false for now
                'auto.offset.reset': self._offset
            }
        }
        res = self._post_request(url, json=payload)

        if res.status_code in [200, 201, 202, 204]:
            self._consumer_id = res.json()['consumerInstanceId']
        else:
            raise TemporaryError(
                "Unexpected temporary error {}: {}".format(
                    res.status_code, res.text))

    @_retry
    def subscribe(self, topics):
        """
        Subscribes the consumer to a list of topics

        :param topics: Topic list.
        :type topics: str or list(str)
        :raise ConsumerError: if the consumer associated with the channel
            does not exist on the server and :attr:`retry_on_fail` is set
            to False.
        :raise TemporaryError: if the subscription attempt fails and
            :attr:`retry_on_fail` is set to False.
        :raise PermanentError: if the channel has been destroyed.
        """
        if not topics:
            raise PermanentError("Non-empty value must be specified for topics")
        topics = [topics] if is_string(topics) else topics

        if not self._consumer_id:
            # Auto-create consumer group if none present
            self.create()

        url = furl(self._base).add(path=self._path_prefix).add(
            path="consumers/{}/subscription".format(
                self._consumer_id)).url
        res = self._post_request(url, json={'topics': topics})

        if res.status_code in [200, 201, 202, 204]:
            self._subscribed = True
        elif res.status_code in [404]:
            raise ConsumerError("Consumer '{}' does not exist".format(
                self._consumer_id
            ))
        else:
            raise TemporaryError(
                "Unexpected temporary error {}: {}".format(
                    res.status_code, res.text))

    @_retry
    def consume(self):
        """
        Consumes records from all the subscribed topics

        :raise ConsumerError: if the consumer associated with the channel
            does not exist on the server and :attr:`retry_on_fail` is set
            to False.
        :raise TemporaryError: if the consume attempt fails and
            :attr:`retry_on_fail` is set to False.
        :raise PermanentError: if the channel has been destroyed or the
            channel has not been subscribed to any topics.
        :return: A list of the payloads (decoded as dictionaries) from the
            records returned from the server.
        :rtype: list(dict)
        """
        if not self._subscribed:
            raise PermanentError("Channel is not subscribed to any topic")

        url = furl(self._base).add(path=self._path_prefix).add(
            path="consumers/{}/records".format(self._consumer_id)).url

        res = self._get_request(url)

        if res.status_code in [200, 201, 202, 204]:
            try:
                commit_log = []
                payloads = []
                for record in res.json()['records']:
                    commit_log.append({
                        'topic': record['routingData']['topic'],
                        'partition': record['partition'],
                        'offset': record['offset']
                    })
                    payloads.append(json.loads(base64.b64decode(
                        record['message']['payload']).decode()))
                self._records_commit_log.extend(commit_log)
                return payloads
            except Exception as exp:
                raise TemporaryError(
                    "Error while parsing response: {}".format(
                        str(exp)))
        elif res.status_code in [404]:
            raise ConsumerError("Consumer '{}' does not exist".format(
                self._consumer_id
            ))
        else:
            raise TemporaryError(
                "Unexpected temporary error {}: {}".format(
                    res.status_code, res.text))

    @_retry
    def commit(self):
        """
        Commits the record offsets to the channel

        :raise ConsumerError: if the consumer associated with the channel
            does not exist on the server and :attr:`retry_on_fail` is set
            to False.
        :raise TemporaryError: if the commit attempt fails and
            :attr:`retry_on_fail` is set to False.
        :raise PermanentError: if the channel has been destroyed.
        """
        if not self._records_commit_log:
            return
        url = furl(self._base).add(path=self._path_prefix).add(
            path="consumers/{}/offsets".format(
                self._consumer_id)).url

        payload = {
            'offsets': self._records_commit_log
        }
        res = self._post_request(url, json=payload)

        if res.status_code in [200, 201, 202, 204]:
            self._records_commit_log = []
        elif res.status_code in [404]:
            raise ConsumerError("Consumer '{}' does not exist".format(
                self._consumer_id
            ))
        else:
            raise TemporaryError(
                "Unexpected temporary error {}: {}".format(
                    res.status_code, res.text))

    def run(self, consume_callback,
            wait_between_queries=_DEFAULT_WAIT_BETWEEN_QUERIES):
        """
        Repeatedly consume records from the subscribed topics. The supplied
        ``consume_callback`` is invoked with a ``list`` containing each payload
        (as a dictionary) extracted from its corresponding record.

        The ``consume_callback`` should return a value of ``True`` in order for
        this function to continue consuming additional records. For a return
        value of ``False`` or no return value, no additional records will be
        consumed and this function will return.

        This function will stop consuming records and will return if a
        :class:`ConsumerError` is raised during a consume attempt - for example,
        if a token created for the consumer has been revoked by the server.

        :param consume_callback: Callable which is invoked with a list of
            payloads from records which have been consumed.
        :param int wait_between_queries: Number of seconds to wait between
            calls to consume records.
        :raise PermanentError: if the channel has been destroyed.
        """
        if not consume_callback:
            raise PermanentError("consume_callback not provided")

        continue_loop = True
        while continue_loop:
            try:
                payloads = self.consume()
                continue_loop = consume_callback(payloads)
                # Commit the offsets for the records which were just consumed.
                self.commit()
                time.sleep(wait_between_queries)
            except ConsumerError as exp:
                # This exception could be raised if the consumer has been
                # removed.
                logging.error("Resetting consumer loop: %s", exp)
                continue_loop = False

    def delete(self):
        """
        Deletes the consumer from the consumer group

        :raise TemporaryError: if the delete attempt fails.
        """
        if not self._consumer_id:
            return
        url = furl(self._base).add(path=self._path_prefix).add(
            path="consumers/{}".format(
                self._consumer_id)).url

        res = self._delete_request(url)

        if res.status_code in [200, 201, 202, 204]:
            self._consumer_id = None
        elif res.status_code in [404]:
            logging.warning(
                "Consumer with ID %s not found. "
                "Resetting consumer anyways.", self._consumer_id)
            self._consumer_id = None
        else:
            raise TemporaryError(
                "Unexpected temporary error {}: {}".format(
                    res.status_code, res.text))

    @property
    def retry_on_fail(self):
        """
        Whether or not the channel will automatically retry a call which
        failed due to a temporary error.
        """
        return self._retry_on_fail

    @retry_on_fail.setter
    def retry_on_fail(self, val):
        self._retry_on_fail = val

    def destroy(self):
        """
        Destroys the channel (releases all associated resources).

        **NOTE:** Once the method has been invoked, no other calls should be
        made to the channel.

        Also note that this method should rarely be called directly. Instead,
        the preferred usage of the channel is via a Python "with" statement as
        shown below:

        .. code-block:: python

            # Create the channel
            with Channel("http://channel-server",
                         auth=ChannelAuth("http://channel-server,
                             "user", "password"),
                         consumer_group="thegroup") as channel:
                # Create a new consumer on the consumer group
                channel.create()

        The "with" statement ensures that resources associated with the channel
        are properly cleaned up when the block is exited (the :func:`destroy`
        method is invoked).

        :raise TemporaryError: if a consumer has previously been created for
            the channel but an attempt to delete the consumer from the
            channel fails.
        """
        with self._destroy_lock:
            if not self._destroyed:
                self.delete()
                self._session.close()
                self._destroyed = True
