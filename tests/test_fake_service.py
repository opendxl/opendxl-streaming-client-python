import base64
import requests
import socket
import threading
import unittest
from furl import furl
from dxldbconsumerclient.channel import (Channel, ChannelAuth)
from tools import fake_consumer_service

import json

BASE_CHANNEL_URL = "http://localhost"

def run_app():
    fake_consumer_service.app.run(port=Test.app_port)

class Test(unittest.TestCase):
    app_thread = None
    app_port = 0
    channel_url = furl(BASE_CHANNEL_URL)

    @classmethod
    def url(cls, path):
        return furl(BASE_CHANNEL_URL).set(port=Test.app_port).add(path=path).url

    @classmethod
    def get_active_consumers(cls):
        response = requests.get(Test.url("/active-consumers"))
        return response.json() if response.status_code == 200 else []

    @classmethod
    def setUpClass(cls):
        Test.app_thread = threading.Thread(target=run_app)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 0))
        Test.app_port = sock.getsockname()[1]
        sock.close()
        Test.channel_url.set(port=Test.app_port)
        Test.app_thread.start()

    @classmethod
    def tearDownClass(cls):
        if Test.app_thread:
            requests.post(Test.url("/shutdown"))
            Test.app_thread.join()

    def test_service(self):
        channel = Channel(
            Test.channel_url.url,
            auth=ChannelAuth(Test.channel_url.url,
                             fake_consumer_service.AUTH_USER,
                             fake_consumer_service.AUTH_PASSWORD),
            consumer_group=fake_consumer_service.CONSUMER_GROUP)
        channel.create()
        channel.subscribe()

        expected_records = \
            [json.loads(base64.b64decode(record['message']['payload']))
             for record in fake_consumer_service.DEFAULT_RECORDS]
        records_consumed = channel.consume()
        self.assertEqual(expected_records, records_consumed)

        channel.commit()
        self.assertEqual([], channel.consume())

        self.assertEqual(len(Test.get_active_consumers()), 1)
        channel.delete()
        self.assertEqual(len(Test.get_active_consumers()), 0)
