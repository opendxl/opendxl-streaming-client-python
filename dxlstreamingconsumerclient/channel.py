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
import requests
from retrying import Retrying
from furl import furl
from .auth import login
from .error import TemporaryError, PermanentError


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
    def __init__(self, base, username, password):
        """
        Constructor parameters:

        :param str base: Base URL to forward authentication requests to.
        :param str username: User name to supply for request auth.
        :param str password: Password to supply for request auth.
        """
        self.username = username
        self.password = password
        self.base = base
        self.token = None
        super(ChannelAuth, self).__init__()

    def reset(self):
        """
        Perform a new login attempt in order to establish a new server token.
        """
        self.token = login(self.base, self.username, self.password)

    def __call__(self, r):
        # Implement my authentication
        if not self.token:
            self.token = login(self.base, self.username, self.password)
        r.headers['Authorization'] = "Bearer {}".format(self.token)
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

    def __init__(self, base, auth,
                 path_prefix='/databus/consumer-service/v1',
                 consumer_group='mcafee_investigator_events',
                 offset='latest',  # earliest
                 timeout=300000,
                 retry_on_fail=True):
        """
        Constructor parameters:

        :param str base: Base URL at which the consumer service resides.
        :param requests.auth.AuthBase auth: Authentication object to use
            for channel requests.
        :param str path_prefix: Path to append to consumer service requests.
        :param str consumer_group: Consumer group to subscribe the channel
            consumer to.
        :param str offset: Offset for the next record to retrieve from the
            consumer service for the new :meth:`consume` call. Must be one
            of 'latest', 'earliest', or 'none'.
        :param int timeout: Channel session timeout (in milliseconds).
        :param bool retry_on_fail: Whether or not the channel will
            automatically retry a call which failed due to a temporary error.
        """
        self._base = base
        self._path_prefix = path_prefix

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
        self._request = requests.Session()
        self._request.auth = auth
        self._request.hooks['response'].append(self.__hook)

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

    def __hook(self, res, *args, **kwargs): # pylint: disable=inconsistent-return-statements, unused-argument
        if res.status_code in [401, 403]:
            logging.warning("Token potentially expired (%s): %s",
                            res.status_code, res.text)
            if not self.retry_on_fail:
                logging.warning("Not retrying failures, will not attempt to "
                                "refresh token")
                return res  # returning original result
            self._request.auth.reset()

            req = res.request
            logging.warning("Resending request: %s, %s, %s",
                            req.method, req.url, req.headers)
            req.headers["Authorization"] = self._request.auth.token
            return self._request.send(res.request)

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
        """
        self.reset()

        url = furl(self._base).add(path=self._path_prefix).add(
            path="consumers").url
        payload = {
            'consumerGroup': self._consumer_group,
            'configs': {
                'session.timeout.ms': str(self._timeout),
                'enable.auto.commit': 'false',  # this has to be false for now
                'auto.offset.reset': self._offset
            }
        }
        res = self._request.post(url, json=payload)

        if res.status_code in [200, 201, 202, 204]:
            self._consumer_id = res.json()['consumerInstanceId']
        else:
            raise TemporaryError(
                "Unexpected temporary error {}: {}".format(
                    res.status_code, res.text))

    @_retry
    def subscribe(self, topics=None):
        """
        Subscribes the consumer to a list of topics

        :param topics: Topic list. Defaults to "case-mgmt-events" and
            "BusinessEvents" if not specified.
        :type topics: list(str)
        """
        topics = topics or ["case-mgmt-events", "BusinessEvents"]

        if not self._consumer_id:
            # Auto-create consumer group if none present
            self.create()

        url = furl(self._base).add(path=self._path_prefix).add(
            path="consumers/{}/subscription".format(
                self._consumer_id)).url
        res = self._request.post(url, json={'topics': topics})

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
        """
        if not self._subscribed:
            raise PermanentError("Channel is not subscribed to any topic")

        url = furl(self._base).add(path=self._path_prefix).add(
            path="consumers/{}/records".format(self._consumer_id)).url

        res = self._request.get(url)

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
        """
        if not self._records_commit_log:
            return
        url = furl(self._base).add(path=self._path_prefix).add(
            path="consumers/{}/offsets".format(
                self._consumer_id)).url

        payload = {
            'offsets': self._records_commit_log
        }
        res = self._request.post(url, json=payload)

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

    def delete(self):
        """
        Deletes the consumer from the consumer group
        """
        if not self._consumer_id:
            return
        url = furl(self._base).add(path=self._path_prefix).add(
            path="consumers/{}".format(
                self._consumer_id)).url

        res = self._request.delete(url)

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
        """
        with self._destroy_lock:
            if not self._destroyed:
                self.delete()
                self._request.close()
                self._destroyed = True
