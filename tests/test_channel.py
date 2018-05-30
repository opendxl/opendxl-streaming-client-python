from __future__ import absolute_import
import unittest
import base64
import json
from mock import patch, MagicMock
from dxldbconsumerclient.channel import (retry_if_not_consumer_error,
                                         ConsumerError, Channel, ChannelAuth)
from dxldbconsumerclient.error import TemporaryError
from dxldbconsumerclient import globals


class Test(unittest.TestCase):
    def setUp(self):
        self.url = "http://localhost"
        self.username = "someone"
        self.password = "password"
        self.consumer_group = "a_consumer_group"

    def tearDown(self):
        pass

    def test_retry_condition(self):
        self.assertFalse(retry_if_not_consumer_error(ConsumerError()))
        self.assertTrue(retry_if_not_consumer_error(Exception()))

    def test_channel_auth(self):
        auth = ChannelAuth(self.url, self.username, self.password)
        req = MagicMock()
        req.headers = {}
        with patch('requests.get') as r:
            r.return_value = MagicMock()
            r.return_value.status_code = 200
            token = "1234567890"
            r.return_value.json = MagicMock(
                return_value={'AuthorizationToken': token})

            req = auth(req)
            self.assertIsNotNone(req)
            self.assertEqual(req.headers['Authorization'],
                             'Bearer {}'.format(token))

            res = MagicMock()
            res.status_code = 403
            res.request.headers = {}

            with patch('requests.Session'):
                channel = Channel(self.url,
                                  auth=auth,
                                  consumer_group=self.consumer_group)
                channel._Channel__hook(res)
                channel.request.send.assert_called_with(res.request)

    def test_main(self):
        globals.interrupted = True  # force disabling of retry mechanism
        auth = ChannelAuth(self.url, self.username, self.password)

        case_event = {
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
                "id": "c00547df-6d74-4833-95ad-3a377c7274a6",
                "name": "A great case full of malware",
                "url": "https://ui-int-cop.soc.mcafee.com/#/cases"
                      "/4e8e23f4-9fe9-4215-92c9-12c9672be9f1",
                "priority": "Low"
            }
        }

        encoded_event = base64.b64encode(json.dumps(case_event).encode())

        with patch('requests.Session') as session:
            session.return_value = MagicMock()  # self.request
            session.return_value.post = MagicMock()
            session.return_value.get = MagicMock()
            session.return_value.delete = MagicMock()

            create_mock = MagicMock()
            create_mock.status_code = 200
            create_mock.json = MagicMock(
                return_value={'consumerInstanceId': 1234})

            subscr_mock = MagicMock()
            subscr_mock.status_code = 204

            consum_mock = MagicMock()
            consum_mock.status_code = 200
            consum_mock.json = MagicMock(
                return_value={'records': [
                    {
                        'routingData': {
                            'topic': 'foo-topic'
                        },
                        'message': {
                            'payload': encoded_event
                        },
                        'partition': 1,
                        'offset': 1
                    }
                ]})

            commit_consumer_error_mock = MagicMock()
            commit_consumer_error_mock.status_code = 404
            commit_error_mock = MagicMock()
            commit_error_mock.status_code = 500
            commit_mock = MagicMock()
            commit_mock.status_code = 204
            delete_mock = MagicMock()
            delete_mock.status_code = 204
            delete_404_mock = MagicMock()
            delete_404_mock.status_code = 404
            delete_500_mock = MagicMock()
            delete_500_mock.status_code = 500

            session.return_value.post.side_effect = [
                create_mock, subscr_mock, commit_consumer_error_mock,
                commit_error_mock, commit_mock]
            session.return_value.get.side_effect = [consum_mock]
            session.return_value.delete.side_effect = [
                delete_500_mock, delete_404_mock, delete_mock]

            channel = Channel(self.url,
                              auth=auth,
                              consumer_group=self.consumer_group)

            channel.commit()  # forcing early exit due to no records to commit

            channel.subscribe()
            records = channel.consume()
            self.assertEqual(records[0]['id'],
                             'a45a03de-5c3d-452a-8a37-f68be954e784')

            with self.assertRaises(ConsumerError):
                channel.commit()
            with self.assertRaises(TemporaryError):
                channel.commit()

            channel.commit()

        with self.assertRaises(TemporaryError):
            channel.delete()  # trigger 500
            session.return_value.delete.assert_called_with(
                "http://localhost/databus/consumer-service/v1/consumers/1234")
            session.return_value.delete.reset_mock()

        channel.delete()  # trigger silent 404
        session.return_value.delete.assert_called_with(
            "http://localhost/databus/consumer-service/v1/consumers/1234")
        session.return_value.delete.reset_mock()

        channel.consumer_id = "1234"  # resetting consumer
        channel.delete()  # Proper deletion
        session.return_value.delete.assert_called_with(
            "http://localhost/databus/consumer-service/v1/consumers/1234")
        session.return_value.delete.reset_mock()

        channel.delete()  # trigger early exit

        globals.interrupted = False  # re-enabling retry mechanism
