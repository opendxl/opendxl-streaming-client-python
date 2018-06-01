#!/usr/bin/env python

from __future__ import absolute_import
import base64
from functools import wraps
import json
import re
import random
import signal
import string
import sys
import threading
import logging

try:
    from http.server import HTTPServer, SimpleHTTPRequestHandler
except ImportError:
    from BaseHTTPServer import HTTPServer
    from SimpleHTTPServer import SimpleHTTPRequestHandler

DEFAULT_PORT = 50000
DEFAULT_LOG_LEVEL = logging.INFO
RUN_CHECK_WAIT = 5
MAX_SHUTDOWN_WAIT = 10
PATH_PREFIX = "/databus/consumer-service/v1"

AUTH_USER = "me"
AUTH_PASSWORD = "secret"
AUTH_USER_HEADER = "Basic {}".format(base64.b64encode(
    "{}:{}".format(AUTH_USER, AUTH_PASSWORD).encode()).decode())

AUTH_TOKEN = "AnAuthorizationToken"
AUTH_TOKEN_HEADER = "Bearer {}".format(AUTH_TOKEN)

COOKIE_NAME = "AWSALB"
CONSUMER_GROUP = "mcafee_investigator_events"


def encode_payload(obj):
    return base64.b64encode(json.dumps(obj).encode()).decode()


DEFAULT_RECORDS = [
    {
        "routingData": {
            "topic": "case-mgmt-events",
            "shardingKey": "123"
        },
        "message": {
            "headers": {
                "sourceId": "00359D70-A5CC-44A0-AE12-6B8D1EB31759"
            },
            "payload": encode_payload({
                "id": "a45a03de-5c3d-452a-8a37-f68be954e784",
                "entity": "case",
                "type": "creation",
                "tenant-id": "7af4746a-63be-45d8-9fb5-5f58bf909c25",
                "user": "jmdacruz",
                "origin": "",
                "nature": "",
                "timestamp": "",
                "transaction-id": "",
                "case":
                    {
                        "id": "9ab2cebb-6b5f-418b-a15f-df1a9ee213f2",
                        "name": "A great case full of malware",
                        "url": "https://mycaseserver.com/#/cases/"
                               "9ab2cebb-6b5f-418b-a15f-df1a9ee213f2",
                        "priority": "Low"
                    }
            })
        },
        "partition": 1,
        "offset": 100
    },
    {
        "routingData": {
            "topic": "case-mgmt-events",
            "shardingKey": "456"
        },
        "message": {
            "headers": {
                "tenantId": "16D8086D-BCC2-41E5-9B05-2624BDA2624B",
                "sourceId": "7526C9DB-F692-40AC-BF0B-652E71DBD58C"
            },
            "payload": encode_payload({
                "id": "a45a03de-5c3d-452a-8a37-f68be954e784",
                "entity": "case",
                "type": "priority-update",
                "tenant-id": "7af4746a-63be-45d8-9fb5-5f58bf909c25",
                "user": "other",
                "origin": "",
                "nature": "",
                "timestamp": "",
                "transaction-id": "",
                "case":
                    {
                        "id": "9ab2cebb-6b5f-418b-a15f-df1a9ee213f2",
                        "name": "A great case full of malware",
                        "url": "https://mycaseserver.com/#/cases"
                               "/9ab2cebb-6b5f-418b-a15f-df1a9ee213f2",
                        "priority": "Low"
                    }
            })
        },
        "partition": 1,
        "offset": 101
    }
]

LOG = logging.getLogger(__name__)


def consumer_service_handler(consumer_service):
    class ConsumerServiceHandler(SimpleHTTPRequestHandler):
        def __init__(self, request, client_address, server):
            self._consumer_service = consumer_service
            self._get_routes = {
                "^/identity/v1/login$": _login,
                "^{}/consumers/[^/]+/records$".format(PATH_PREFIX):
                    _get_records
            }
            self._post_routes = {
                "^{}/consumers$".format(PATH_PREFIX): _create_consumer,
                "^{}/consumers/[^/]+/subscription$".format(PATH_PREFIX):
                    _create_subscription,
                "^{}/consumers/[^/]+/offsets$".format(PATH_PREFIX):
                    _commit_offsets,
                "^/reset-records$": _reset_records
            }
            self._delete_routes = {
                "^{}/consumers/[^/]+$".format(PATH_PREFIX):
                    _delete_consumer
            }
            SimpleHTTPRequestHandler.__init__(self, request, client_address,
                                              server)

        def _send_response(self, status_code, body=None, headers=None):
            self.send_response(status_code)
            headers = headers or {}
            if isinstance(body, dict):
                headers["Content-Type"] = "application/json"
                body = json.dumps(body)
            elif body:
                headers["Content-Type"] = "text/plain; charset=utf-8"
            for header_name, header_value in headers.items():
                self.send_header(header_name, header_value)
            self.end_headers()
            if body:
                self.wfile.write(body.encode())

        def _handle_request(self, routes):
            matched = False
            for route_path, route_func in routes.items():
                if re.match(route_path, self.path):
                    matched = True
                    response = route_func(
                        handler=self,
                        consumer_service=self._consumer_service)
                    self._send_response(*response)
                    break
            if not matched:
                self._send_response(404, 'Route not found: ' + self.path)

        def do_GET(self):
            self._handle_request(self._get_routes)

        def do_POST(self): # pylint: disable=invalid-name
            self._handle_request(self._post_routes)

        def do_DELETE(self): # pylint: disable=invalid-name
            self._handle_request(self._delete_routes)

    return ConsumerServiceHandler


class ConsumerService(object):
    def __init__(self, port=DEFAULT_PORT):
        self.port = port
        self._active_consumers = {}
        self._active_records = list(DEFAULT_RECORDS)
        self._lock = threading.Lock()
        self._server = None
        self._server_thread = None
        self._started = False
        self._subscribed_topics = set()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def start(self):
        with self._lock:
            if not self._started:
                self._started = True
                LOG.info("Starting service")
                self._server = HTTPServer(
                    ('', self.port), consumer_service_handler(self))
                self.port = self._server.server_port
                LOG.info("Started service on %s:%s",
                         self._server.server_name, self.port)
                self._server_thread = threading.Thread(
                    target=self._server.serve_forever)
                self._server_thread.start()

    def stop(self):
        with self._lock:
            LOG.info("Stopping service...")
            if self._started:
                if self._server:
                    self._server.shutdown()
                    if self._server_thread:
                        self._server_thread.join(MAX_SHUTDOWN_WAIT)
                self._started = False
            LOG.info("Service stopped")


def _user_auth(f):
    @wraps(f)
    def decorated(handler, *args, **kwargs):
        if handler.headers.get("Authorization") == AUTH_USER_HEADER:
            kwargs['handler'] = handler
            response = f(*args, **kwargs)
        else:
            response = 403, "Invalid user", {"WWW-Authenticate": "Basic"}
        return response
    return decorated


def _token_auth(f):
    @wraps(f)
    def decorated(handler, *args, **kwargs):
        if handler.headers.get("Authorization") == AUTH_TOKEN_HEADER:
            kwargs['handler'] = handler
            response = f(*args, **kwargs)
        else:
            response = 403, "Invalid user", {"WWW-Authenticate": "Bearer"}
        return response
    return decorated


def _json_body(f):
    @wraps(f)
    def decorated(handler, *args, **kwargs):
        kwargs['body'] = json.loads(
            handler.rfile.read(int(handler.headers['Content-Length'])).decode())
        kwargs['handler'] = handler
        return f(*args, **kwargs)
    return decorated


def _consumer_auth(f):
    @wraps(f)
    def decorated(handler, consumer_service, *args, **kwargs):
        consumer_instance_id_match = re.match(".*/consumers/([^/]+)",
                                              handler.path)
        if not consumer_instance_id_match:
            response = 400, "Consumer not specified"
        else:
            consumer_instance_id = consumer_instance_id_match.group(1)
            with consumer_service._lock:
                consumer_cookie = consumer_service._active_consumers.get(
                    consumer_instance_id)
            if not consumer_cookie:
                response = 404, "Unknown consumer"
            elif handler.headers.get(
                    "Cookie") != "{}={}".format(COOKIE_NAME, consumer_cookie):
                response = 403, "Invalid cookie"
            else:
                kwargs["consumer_instance_id"] = consumer_instance_id
                kwargs["handler"] = handler
                kwargs["consumer_service"] = consumer_service
                response = f(*args, **kwargs)
        return response
    return decorated


@_user_auth
def _login(*args, **kwargs): # pylint: disable=unused-argument
    return 200, {"AuthorizationToken": AUTH_TOKEN}


def random_val():
    return "".join(random.choice(string.ascii_uppercase) for _ in range(5))


@_consumer_auth
@_token_auth
def _delete_consumer(consumer_instance_id, consumer_service, **kwargs): # pylint: disable=unused-argument
    status_code = 204 \
        if consumer_service._active_consumers.pop(consumer_instance_id, None) \
        else 404
    return status_code, ""


@_token_auth
@_json_body
def _create_consumer(body, consumer_service, **kwargs): # pylint: disable=unused-argument
    if body.get("consumerGroup") == CONSUMER_GROUP:
        consumer_id = random_val()
        cookie_value = random_val()
        with consumer_service._lock:
            consumer_service._active_consumers[consumer_id] = cookie_value
        response = 200, {"consumerInstanceId": consumer_id}, \
                   {"Set-Cookie": "{}={}".format(COOKIE_NAME, cookie_value)}
    else:
        response = 400, "Unknown consumer group"
    return response


@_consumer_auth
@_token_auth
@_json_body
def _create_subscription(body, consumer_service, **kwargs): # pylint: disable=unused-argument
    topics = body.get("topics")
    if topics:
        with consumer_service._lock:
            for topic in topics:
                consumer_service._subscribed_topics.add(topic)
    return 204, ""

@_consumer_auth
@_token_auth
def _get_records(consumer_service, **kwargs): # pylint: disable=unused-argument
    with consumer_service._lock:
        subscribed_records = \
            [record for record in consumer_service._active_records \
             if record["routingData"]["topic"] in consumer_service._subscribed_topics]
    return 200, {"records": subscribed_records}


def record_matches_offset(record, offset):
    return record["routingData"]["topic"] == offset["topic"] and \
        record["partition"] == offset["partition"] and \
        record["offset"] == offset["offset"]


def record_in_offsets(record, offsets):
    return any(record_matches_offset(record, offset) for offset in offsets)


@_consumer_auth
@_token_auth
@_json_body
def _commit_offsets(body, consumer_service, **kwargs): # pylint: disable=unused-argument
    committed_offsets = body.get("offsets")
    with consumer_service._lock:
        consumer_service._active_records[:] = \
            [record for record in consumer_service._active_records
             if not record_in_offsets(record, committed_offsets)]
    return 204, ""


def _reset_records(consumer_service, **kwargs): # pylint: disable=unused-argument
    with consumer_service._lock:
        consumer_service._active_records = list(DEFAULT_RECORDS)
    return 200, ""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    PORT = DEFAULT_PORT
    if len(sys.argv) > 1:
        try:
            PORT = int(sys.argv[1])
        except ValueError:
            sys.exit("Numeric value not specified for port")

    RUNNING = [True]
    RUN_CONDITION = threading.Condition()

    def signal_handler(*_):
        with RUN_CONDITION:
            RUNNING[0] = False
            RUN_CONDITION.notify_all()

    with ConsumerService(PORT):
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        with RUN_CONDITION:
            while RUNNING[0]:
                RUN_CONDITION.wait(RUN_CHECK_WAIT)
