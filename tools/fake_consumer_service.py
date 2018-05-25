import base64
from functools import wraps
import json
import random
import string
from flask import Flask, jsonify, make_response, request

def encode_payload(obj):
    return base64.b64encode(json.dumps(obj).encode()).decode()

AUTH_USER = "me"
AUTH_PASSWORD = "secret"
AUTH_TOKEN = "AnAuthorizationToken"
COOKIE_NAME = "AWSALB"
CONSUMER_GROUP = "mcafee_investigator_events"

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
                        "url": "https://ui-int-cop.soc.mcafee.com/#/cases/9ab2cebb-6b5f-418b-a15f-df1a9ee213f2",

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
                        "url": "https://ui-int-cop.soc.mcafee.com/#/cases/9ab2cebb-6b5f-418b-a15f-df1a9ee213f2",

                        "priority": "Low"
                    }
            })
        },
        "partition": 1,
        "offset": 101
    }
]

active_consumers = {}
active_records = list(DEFAULT_RECORDS)
subscribed_topics = set()

app = Flask(__name__)


def user_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.authorization and \
                request.authorization.get('username') == AUTH_USER and \
                request.authorization.get('password') == AUTH_PASSWORD:
            response = f(*args, **kwargs)
        else:
            response = make_response('Invalid user', 403)
            response.headers['WWW-Authenticate'] = 'Basic'
        return response
    return decorated


def token_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get('authorization')
        if auth == 'Bearer {}'.format(AUTH_TOKEN):
            response = f(*args, **kwargs)
        else:
            response = make_response('Invalid token', 403)
            response.headers['WWW-Authenticate'] = 'Basic'
        return response
    return decorated


def consumer_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        consumer_instance_id = kwargs.get('consumer_instance_id')
        if not consumer_instance_id:
            response = make_response('Consumer not specified', 400)
        else:
            consumer_cookie = active_consumers.get(consumer_instance_id)
            if not consumer_cookie:
                response = make_response('Unknown consumer', 404)
            elif request.cookies.get(COOKIE_NAME) != consumer_cookie:
                response = make_response('Invalid cookie', 403)
            else:
                response = f(*args, **kwargs)
        return response
    return decorated


@app.route('/identity/v1/login', methods=['GET'])
@user_auth
def login():
    return jsonify({"AuthorizationToken": AUTH_TOKEN})


def random_val():
    return "".join(random.choice(string.ascii_uppercase) for _ in range(5))


@app.route('/databus/consumer-service/v1/consumers/<consumer_instance_id>',
           methods=['DELETE'])
@token_auth
def delete_consumer(consumer_instance_id):
    status_code = 204 \
        if active_consumers.pop(consumer_instance_id, None) else 404
    return "", status_code


@app.route('/databus/consumer-service/v1/consumers', methods=['POST'])
@token_auth
def create_consumer():
    consumer_id = random_val()
    cookie_value = random_val()
    active_consumers[consumer_id] = cookie_value
    response = jsonify({"consumerInstanceId": consumer_id})
    response.set_cookie(COOKIE_NAME, cookie_value)
    return response


@app.route(
    '/databus/consumer-service/v1/consumers/<consumer_instance_id>/subscription',
    methods=['POST'])
@consumer_auth
@token_auth
def subscription(consumer_instance_id):
    topics = request.json.get("topics")
    if topics:
        [subscribed_topics.add(topic) for topic in topics]
    return "", 204


@app.route(
    '/databus/consumer-service/v1/consumers/<consumer_instance_id>/records',
    methods=['GET'])
@consumer_auth
@token_auth
def records(consumer_instance_id):
    return jsonify(
        {"records": [record for record in active_records \
                     if record["routingData"]["topic"] in subscribed_topics]}
    )


def record_matches_offset(record, offset):
    return record["routingData"]["topic"] == offset["topic"] and \
        record["partition"] == offset["partition"] and \
        record["offset"] == offset["offset"]


def record_in_offsets(record, offsets):
    return any(record_matches_offset(record, offset) for offset in offsets)


@app.route(
    '/databus/consumer-service/v1/consumers/<consumer_instance_id>/offsets',
    methods=['POST'])
@consumer_auth
@token_auth
def offsets(consumer_instance_id):
    committed_offsets = request.json["offsets"]
    active_records[:] = [record for record in active_records \
                           if not record_in_offsets(record, committed_offsets)]
    return "", 204


@app.route('/reset-records')
def reset_records():
    global active_records
    active_records = list(DEFAULT_RECORDS)
    return "", 200
