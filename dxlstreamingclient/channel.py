"""
Contains the :class:`Channel` class, which is used to connect to the consumer
service.
"""

from __future__ import absolute_import
from functools import wraps
import base64
import copy
import json
import logging
import threading
import warnings
import requests
from retrying import Retrying
from furl import furl
from .auth import login
from .error import TemporaryError, PermanentError, StopError
from ._compat import is_string

_DEFAULT_CONSUMER_PATH_PREFIX = "/databus/consumer-service/v1"
_DEFAULT_PRODUCER_PATH_PREFIX = "/databus/cloudproxy/v1"
_PRODUCE_CONTENT_TYPE = "application/vnd.dxl.intel.records.v1+json"

_RETRY_WAIT_EXPONENTIAL_MULTIPLIER = 1000
_RETRY_WAIT_EXPONENTIAL_MAX = 10000


def _retry(f):
    @wraps(f)
    def retry_wrapper(*args, **kwargs):
        channel = args[0]
        if not channel._active:
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
    not being recognized by the streaming service.
    """
    pass


class ChannelAuth(requests.auth.AuthBase):
    """
    Authentication class for use with channel requests.
    """
    def __init__(self, base, username, password, verify_cert_bundle=""):
        """
        Constructor parameters:

        :param str base: Base URL to forward authentication requests to.
        :param str username: User name to supply for request authentication.
        :param str password: Password to supply for request authentication.
        :param str verify_cert_bundle: Path to a CA bundle file containing
            certificates of trusted CAs. The CA bundle is used to validate that
            the certificate of the authentication server being connected to was
            signed by a valid authority. If set to an empty string, the server
            certificate is not validated.
        """
        self._username = username
        self._password = password
        self._base = base
        self._token = None
        self._verify_cert_bundle = verify_cert_bundle
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
                                self._password,
                                verify_cert_bundle=self._verify_cert_bundle)
        r.headers['Authorization'] = "Bearer {}".format(self._token)
        return r


class Channel(object):
    """
    The :class:`Channel` class is responsible for all communication with the
    streaming service.

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
    # streaming service
    _DEFAULT_WAIT_BETWEEN_QUERIES = 30

    # Constants for consumer config settings
    _AUTO_OFFSET_RESET_CONFIG_SETTING = "auto.offset.reset"
    _ENABLE_AUTO_COMMIT_CONFIG_SETTING = "enable.auto.commit"
    _REQUEST_TIMEOUT_CONFIG_SETTING = "request.timeout.ms"
    _SESSION_TIMEOUT_CONFIG_SETTING = "session.timeout.ms"

    def __init__(self, base, auth,
                 consumer_group=None,
                 path_prefix=None,
                 consumer_path_prefix=_DEFAULT_CONSUMER_PATH_PREFIX,
                 producer_path_prefix=_DEFAULT_PRODUCER_PATH_PREFIX,
                 offset="latest",  # earliest
                 request_timeout=None,
                 session_timeout=None,
                 retry_on_fail=True,
                 verify_cert_bundle="",
                 extra_configs=None):
        """
        Constructor parameters:

        :param str base: Base URL at which the streaming service resides.
        :param requests.auth.AuthBase auth: Authentication object to use
            for channel requests.
        :param str consumer_group: Consumer group to subscribe the channel
            consumer to.
        :param str path_prefix: Path to append to streaming service requests.
        :param str consumer_path_prefix: Path to append to consumer-related
            requests made to the streaming service. Note that if the
            `path_prefix` parameter is set to a non-empty value, the
            `path_prefix` value will be appended to consumer-related requests
            instead of the `consumer_path_prefix` value.
        :param str producer_path_prefix: Path to append to producer-related
            requests made to the streaming service. Note that if the
            `path_prefix` parameter is set to a non-empty value, the
            `path_prefix` value will be appended to producer-related requests
            instead of the `producer_path_prefix` value.
        :param str offset: Offset for the next record to retrieve from the
            streaming service for the new :meth:`consume` call. Must be one
            of 'latest', 'earliest', or 'none'.
        :param int request_timeout: The configuration controls the maximum
            amount of time the client (consumer) will wait for the broker
            response of a request. If the response is not received before the
            request timeout elapses the client may resend the request or fail
            the request if retries are exhausted. If set to `None` (the
            default), the request timeout is determined automatically by
            the streaming service. Note that if a value is set for the request
            timeout, the value should exceed the `session_timeout`. Otherwise,
            the streaming service may fail to create new consumers properly. To
            ensure that the request timeout is greater than the
            `session_timeout`, values for either both (or neither) of the
            `request_timeout` and `session_timeout` parameters should be
            specified.
        :param int session_timeout: The timeout (in seconds) used to detect
            channel consumer failures. The consumer sends periodic heartbeats
            to indicate its liveness to the broker. If no heartbeats are
            received by the broker before the expiration of this session
            timeout, then the broker may remove this consumer from the group.
            If set to `None` (the default), the session timeout is determined
            automatically by the streaming service. Note that if a value is set
            for the session timeout, the value should be less than the
            `request_timeout`. Otherwise, the streaming service may fail to
            create new consumers properly. To ensure that the session timeout is
            less than the `request_timeout`, values for either both (or neither)
            of the `request_timeout` and `session_timeout` parameters should be
            specified.
        :param bool retry_on_fail: Whether or not the channel will
            automatically retry a call which failed due to a temporary error.
        :param str verify_cert_bundle: Path to a CA bundle file containing
            certificates of trusted CAs. The CA bundle is used to validate that
            the certificate of the authentication server being connected to was
            signed by a valid authority. If set to an empty string, the server
            certificate is not validated.
        :param dict extra_configs: Dictionary of key/value pairs containing
            any custom configuration settings which should be sent to the
            streaming service when a consumer is created. Note that any
            values specified for the `offset`, `request_timeout`, and/or
            `session_timeout` parameters will override the corresponding
            values, if specified, in the `extra_configs` parameter.
        """
        self._base = base

        if path_prefix:
            self._consumer_path_prefix = path_prefix
            self._producer_path_prefix = path_prefix
        else:
            self._consumer_path_prefix = consumer_path_prefix
            self._producer_path_prefix = producer_path_prefix

        self._consumer_group = consumer_group

        offset_values = ['latest', 'earliest', 'none']
        if offset not in offset_values:
            raise PermanentError(
                "Value for 'offset' must be one of: {}".format(
                    ', '.join(offset_values)))

        # Setup customer configs from supplied parameters
        self._configs = copy.deepcopy(extra_configs) if extra_configs else {}

        if self._ENABLE_AUTO_COMMIT_CONFIG_SETTING not in self._configs:
            # this has to be false for now
            self._configs[self._ENABLE_AUTO_COMMIT_CONFIG_SETTING] = "false"
        self._configs[self._AUTO_OFFSET_RESET_CONFIG_SETTING] = offset

        if session_timeout is not None:
            # Convert from seconds to milliseconds
            self._configs[self._SESSION_TIMEOUT_CONFIG_SETTING] = str(
                session_timeout * 1000)

        if request_timeout is not None:
            # Convert from seconds to milliseconds
            self._configs[self._REQUEST_TIMEOUT_CONFIG_SETTING] = str(
                request_timeout * 1000)

        # state variables
        self._consumer_id = None
        self._subscriptions = []
        self._records_commit_log = []

        # Create a session object so that we can store cookies across requests
        self._session = requests.Session()
        self._session.auth = auth
        self._session.verify = verify_cert_bundle

        self._retry_on_fail = retry_on_fail
        self._retry_if_not_consumer_error = \
            self._retry_if_not_consumer_error_fn()

        self._destroy_lock = threading.RLock()
        self._active = True

        self._run_lock = threading.RLock()
        self._running = False
        self._stop_requested = False
        self._stop_requested_condition = threading.Condition(self._run_lock)
        self._stopped_condition = threading.Condition(self._run_lock)

    def __enter__(self):
        """Enter with"""
        return self

    def __exit__(self, *_):
        """Exit with"""
        self.destroy()

    def _retry_if_not_consumer_error_fn(self):
        def _retry_if_not_consumer_error(exception):
            should_retry = (not isinstance(exception, ConsumerError)) and \
                            self.retry_on_fail and not self._stop_requested
            if should_retry:
                logging.info("Retrying due to: %s", exception)
            else:
                logging.warning(
                    "Will not retry due to: %s%s%s", exception,
                    "" if self.retry_on_fail else " (retries disabled)",
                    " (stop requested)" if self._stop_requested else "")
            return should_retry
        return _retry_if_not_consumer_error

    def _request(self, method, url, **kwargs):
        with warnings.catch_warnings():
            if not self._session.verify:
                warnings.filterwarnings("ignore", "Unverified HTTPS request")
            if self._running and self._stop_requested:
                raise StopError("Channel was stopped")
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
        self._subscriptions = []
        self._records_commit_log = []

    @_retry
    def create(self):
        """
        Creates a new consumer on the consumer group

        :raise TemporaryError: if the creation attempt fails and
            :attr:`retry_on_fail` is set to False.
        :raise PermanentError: if the channel has been destroyed.
        """
        if not self._consumer_group:
            raise PermanentError(
                "No value specified for 'consumer_group' during channel init")

        self.reset()

        url = furl(self._base).add(path=self._consumer_path_prefix).add(
            path="consumers").url
        payload = {
            "consumerGroup": self._consumer_group,
            "configs": self._configs
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

        url = furl(self._base).add(path=self._consumer_path_prefix).add(
            path="consumers/{}/subscription".format(
                self._consumer_id)).url
        res = self._post_request(url, json={'topics': topics})

        if res.status_code in [200, 201, 202, 204]:
            self._subscriptions = topics
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
        if not self._subscriptions:
            raise PermanentError("Channel is not subscribed to any topic")

        url = furl(self._base).add(path=self._consumer_path_prefix).add(
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
        url = furl(self._base).add(path=self._consumer_path_prefix).add(
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

    def _consume_loop(self, process_callback, wait_between_queries, topics):
        continue_running = True
        while continue_running:
            try:
                payloads = self.consume()
                continue_running = process_callback(payloads)
                # Commit the offsets for the records which were just consumed.
                self.commit()
                with self._run_lock:
                    if self._stop_requested:
                        continue_running = False
                    elif continue_running:
                        self._stop_requested_condition.wait(
                            wait_between_queries)
                        continue_running = not self._stop_requested
            except ConsumerError as exp:
                # This exception could be raised if the consumer has been
                # removed.
                logging.error("Resetting consumer loop: %s", exp)
                topics = self._subscriptions
                self.reset()
                if not self._retry_on_fail:
                    continue_running = False
                break
        return continue_running, topics

    def run(self, process_callback,
            wait_between_queries=_DEFAULT_WAIT_BETWEEN_QUERIES,
            topics=None):
        """
        Repeatedly consume records from the subscribed topics. The supplied
        ``process_callback`` is invoked with a ``list`` containing each payload
        (as a dictionary) extracted from its corresponding record.

        The ``process_callback`` should return a value of ``True`` in order for
        this function to continue consuming additional records. For a return
        value of ``False`` or no return value, no additional records will be
        consumed and this function will return.

        The :meth:`stop` method can also be called to halt an execution of
        this method.

        :param process_callback: Callable which is invoked with a list of
            payloads from records which have been consumed.
        :param int wait_between_queries: Number of seconds to wait between
            calls to consume records.
        :param topics: Topic list. If set to a non-empty value, the channel
            will be subscribed to the specified topics. If set to an empty
            value, the channel will use topics previously subscribed via a
            call to the :meth:`subscribe` method.
        :type topics: str or list(str)
        :raise PermanentError: if the channel has been destroyed or a prior
            run is already in progress.
        """
        if not self._consumer_group:
            raise PermanentError(
                "No value specified for 'consumer_group' during channel init")

        if not process_callback:
            raise PermanentError("process_callback not provided")

        if topics:
            topics = [topics] if is_string(topics) else topics
        else:
            topics = self._subscriptions

        continue_running = True
        with self._run_lock:
            if self._running:
                raise PermanentError("Previous run already in progress")
            self._running = True
            continue_running = not self._stop_requested
        try:
            while continue_running:
                self.subscribe(topics)
                continue_running, topics = self._consume_loop(
                    process_callback, wait_between_queries, topics)
        except StopError:
            pass
        finally:
            with self._run_lock:
                self._running = False
                self._stop_requested = False
                self._stopped_condition.notify_all()

    def stop(self):
        """
        Stop an active execution of the :meth:`run` call. If no :meth:`run`
        call is active, this function returns immediately. If a :meth:`run`
        call is active, this function blocks until the run has been completed.
        """
        with self._run_lock:
            if self._running:
                self._stop_requested = True
                self._stop_requested_condition.notify_all()
                while self._running:
                    self._stopped_condition.wait()

    def produce(self, payload):
        """
        Produces records to the channel.

        :param payload: Payload containing the records to be posted to the
            channel.
        :raise PermanentError: if an unsuccessful response is received from
            the streaming service.
        """
        url = furl(self._base).add(path=self._producer_path_prefix).add(
            path="produce").url

        headers = {"Content-Type": _PRODUCE_CONTENT_TYPE}

        res = self._post_request(url, json=payload, headers=headers)

        if res.status_code not in [200, 201, 202, 204]:
            raise PermanentError(
                "Unexpected permanent error {}: {}".format(
                    res.status_code, res.text))

    def delete(self):
        """
        Deletes the consumer from the consumer group

        :raise TemporaryError: if the delete attempt fails.
        """
        if not self._consumer_id:
            return
        url = furl(self._base).add(path=self._consumer_path_prefix).add(
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
            if self._active:
                self.stop()
                self.delete()
                self._session.close()
                self._active = False
