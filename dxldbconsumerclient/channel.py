import requests
import base64
import json
import logging
from . import globals
from retrying import retry
from furl import furl
from .auth import login
from .error import TemporaryError, PermanentError


def retry_if_not_consumer_error(exception):
    should_retry = (not isinstance(exception, ConsumerError) and
                    not globals.interrupted)
    if should_retry:
        logging.info("Retrying due to: %s", exception)
    else:
        logging.warn("Will not retry due to: %s %s", exception,
                     "(interrupted)" if globals.interrupted else "")
    return should_retry


class ConsumerError(TemporaryError):
    pass


class ChannelAuth(requests.auth.AuthBase):
    def __init__(self, base, username, password):
        self.username = username
        self.password = password
        self.base = base
        self.token = None
        super(ChannelAuth, self).__init__()

    def reset(self):
        self.token = login(self.base, self.username, self.password)

    def __call__(self, r):
        # Implement my authentication
        if not self.token:
            self.token = login(self.base, self.username, self.password)
        r.headers['Authorization'] = "Bearer {}".format(self.token)
        return r


class Channel(object):

    def __init__(self, base, auth,
                 path_prefix='/databus/consumer-service/v1',
                 consumer_group='mcafee_investigator_events',
                 offset='latest',  # earliest
                 timeout=30000):
        self.base = base
        self.path_prefix = path_prefix
        # self.auth = auth

        self.consumer_group = consumer_group
        offset_values = ['latest', 'earliest', 'none']
        if offset not in offset_values:
            raise PermanentError(
                "Value for 'offset' must be one of: {}".format(
                    ', '.join(offset_values)))
        self.offset = offset
        self.timeout = timeout

        # state variables
        self.consumer_id = None
        self.subscribed = False
        self.records_commit_log = []

        # Create a session object so that we can store cookies across requests
        self.request = requests.Session()
        self.request.auth = auth
        self.request.hooks['response'].append(self.__hook)

    def __hook(self, res, *args, **kwargs):
        if res.status_code in [401, 403]:
            logging.warning('Token potentially expired (%s): %s',
                            res.status_code, res.text)
            if globals.interrupted:
                logging.warning("Application interrupted, will not attempt to "
                                "refresh token")
                return res  # returning original result
            self.request.auth.reset()

            req = res.request
            logging.warning('Resending request: %s, %s, %s',
                            req.method, req.url, req.headers)
            req.headers['Authorization'] = self.request.auth.token
            return self.request.send(res.request)

    # low-level methods
    def reset(self):
        self.consumer_id = None
        self.subscribed = False
        self.records_commit_log = []

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000,
           retry_on_exception=retry_if_not_consumer_error)
    def create(self):
        """Creates a new consumer on the consumer group
        """

        self.reset()

        url = furl(self.base).add(path=self.path_prefix).add(
            path="consumers").url
        payload = {
            'consumerGroup': self.consumer_group,
            'configs': {
                'session.timeout.ms': str(self.timeout),
                'enable.auto.commit': 'false',  # this has to be false for now
                'auto.offset.reset': self.offset
            }
        }
        res = self.request.post(url, json=payload)

        if res.status_code in [200, 201, 202, 204]:
            self.consumer_id = res.json()['consumerInstanceId']
        else:
            raise TemporaryError(
                "Unexpected temporary error {}: {}".format(
                    res.status_code, res.text))

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000,
           retry_on_exception=retry_if_not_consumer_error)
    def subscribe(self, topics=['case-mgmt-events', 'BusinessEvents']):
        """Subscribes the consumer to a list of topics
        """
        if not self.consumer_id:
            # Auto-create consumer group if none present
            self.create()

        url = furl(self.base).add(path=self.path_prefix).add(
            path="consumers/{}/subscription".format(
                self.consumer_id)).url
        res = self.request.post(url, json={'topics': topics})

        if res.status_code in [200, 201, 202, 204]:
            self.subscribed = True
        elif res.status_code in [404]:
            raise ConsumerError("Consumer '{}' does not exist".format(
                self.consumer_id
            ))
        else:
            raise TemporaryError(
                "Unexpected temporary error {}: {}".format(
                    res.status_code, res.text))

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000,
           retry_on_exception=retry_if_not_consumer_error)
    def consume(self):
        """Consumes records from all the subscribed topics
        """
        if not self.subscribed:
            raise PermanentError("Channel is not subscribed to any topic")

        url = furl(self.base).add(
            path="/databus/consumer-service/v1/consumers/{}/records".format(
                self.consumer_id)).url

        res = self.request.get(url)

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
                        record['message']['payload'])))
                self.records_commit_log.extend(commit_log)
                return payloads
            except Exception as exp:
                raise TemporaryError(
                    "Error while parsing response: {}".format(
                        str(exp)))
        elif res.status_code in [404]:
            raise ConsumerError("Consumer '{}' does not exist".format(
                self.consumer_id
            ))
        else:
            raise TemporaryError(
                "Unexpected temporary error {}: {}".format(
                    res.status_code, res.text))

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000,
           retry_on_exception=retry_if_not_consumer_error)
    def commit(self):
        """Commits the record offsets to the channel
        """
        if not self.records_commit_log:
            return
        url = furl(self.base).add(path=self.path_prefix).add(
            path="consumers/{}/offsets".format(
                self.consumer_id)).url

        payload = {
            'offsets': self.records_commit_log
        }
        res = self.request.post(url, json=payload)

        if res.status_code in [200, 201, 202, 204]:
            self.records_commit_log = []
        elif res.status_code in [404]:
            raise ConsumerError("Consumer '{}' does not exist".format(
                self.consumer_id
            ))
        else:
            raise TemporaryError(
                "Unexpected temporary error {}: {}".format(
                    res.status_code, res.text))

    def delete(self):
        """Deletes the consumer from the consumer group
        """
        if not self.consumer_id:
            return
        url = furl(self.base).add(path=self.path_prefix).add(
            path="consumers/{}".format(
                self.consumer_id)).url

        res = self.request.delete(url)

        if res.status_code in [200, 201, 202, 204]:
            self.consumer_id = None
        elif res.status_code in [404]:
            logging.warning(
                "Consumer with ID %s not found. "
                "Resetting consumer anyways.", self.consumer_id)
            self.consumer_id = None
        else:
            raise TemporaryError(
                "Unexpected temporary error {}: {}".format(
                    res.status_code, res.text))
