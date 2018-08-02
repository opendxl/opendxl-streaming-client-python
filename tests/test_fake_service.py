from __future__ import absolute_import
import base64
import json
import os
import sys
import unittest
from furl import furl
from dxlstreamingclient.channel import (Channel, ChannelAuth)

# Add the local sample directory to the path just during the
# fake_streaming_service import to avoid inadvertently resolving "sample" to an
# unintended location in the Python module path.
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT_DIR + "/..")
from sample import fake_streaming_service # pylint: disable=wrong-import-position
del sys.path[0]

BASE_CHANNEL_URL = "http://localhost"


class Test(unittest.TestCase):
    def test_service(self):
        with fake_streaming_service.ConsumerService(0) as service:
            channel_url = furl(BASE_CHANNEL_URL).set(port=service.port)
            with Channel(channel_url,
                         auth=ChannelAuth(
                             channel_url,
                             fake_streaming_service.AUTH_USER,
                             fake_streaming_service.AUTH_PASSWORD),
                         consumer_group=fake_streaming_service.CONSUMER_GROUP) \
                    as channel:
                channel.create()
                topic = "case-mgmt-events"
                channel.subscribe(topic)

                expected_records = []
                for record in fake_streaming_service.DEFAULT_RECORDS:
                    if record['routingData']['topic'] == topic:
                        expected_records.append(
                            json.loads(base64.b64decode(
                                record['message']['payload']).decode())
                        )

                records_consumed = channel.consume()
                self.assertEqual(expected_records, records_consumed)

                channel.commit()
                self.assertEqual([], channel.consume())

                self.assertEqual(len(service._active_consumers), 1)
            self.assertEqual(len(service._active_consumers), 0)
