from __future__ import absolute_import
import unittest
import base64
import json
from mock import patch, MagicMock
from dxlstreamingconsumerclient.channel import \
    (ConsumerError, Channel, ChannelAuth)
from dxlstreamingconsumerclient.error import TemporaryError

class Test(unittest.TestCase):
    def setUp(self):
        self.url = "http://localhost"
        self.username = "someone"
        self.password = "password"
        self.consumer_group = "a_consumer_group"

    def tearDown(self):
        pass

    def test_retry_condition(self):
        auth = ChannelAuth(self.url, self.username, self.password)
        with patch('requests.Session'):
            channel = Channel(self.url,
                              auth=auth,
                              consumer_group=self.consumer_group)
            self.assertFalse(channel._retry_if_not_consumer_error(
                ConsumerError()))
            self.assertTrue(channel._retry_if_not_consumer_error(Exception()))

    def test_channel_auth(self):
        auth = ChannelAuth(self.url, self.username, self.password)
        req = MagicMock()
        req.headers = {}
        with patch('requests.get') as req_get:
            req_get.return_value = MagicMock()
            req_get.return_value.status_code = 200

            original_token = "1234567890"
            req_get.return_value.json = MagicMock(
                return_value={'AuthorizationToken': original_token})

            req = auth(req)
            self.assertIsNotNone(req)
            self.assertEqual(req.headers['Authorization'],
                             'Bearer {}'.format(original_token))

            new_token = "ABCDEFGHIJ"
            req_get.return_value.json = MagicMock(
                return_value={'AuthorizationToken': new_token})

            # Even though the token that would be returned for a login attempt
            # has changed, the original token should be returned because it
            # was cached on the auth object.
            req = auth(req)
            self.assertIsNotNone(req)
            self.assertEqual(req.headers['Authorization'],
                             'Bearer {}'.format(original_token))

            res = MagicMock()
            res.status_code = 403
            res.request.headers = {}

            with patch('requests.Session') as session:
                channel = Channel(self.url,
                                  auth=auth,
                                  consumer_group=self.consumer_group)

                create_403_mock = MagicMock()
                create_403_mock.status_code = 403

                create_200_mock = MagicMock()
                create_200_mock.status_code = 200
                create_200_mock.json = MagicMock(
                    return_value={'consumerInstanceId': 1234},
                )

                self.assertIsNone(channel._consumer_id)
                self.assertEqual(auth._token, original_token)
                session.return_value.request.side_effect = [
                    create_403_mock, create_200_mock
                ]

                channel.create()

                self.assertEqual(channel._consumer_id, 1234)

                # The 403 returned from the channel create call above should
                # lead to a new token being issued for the next authentication
                # call.
                req = auth(req)
                self.assertIsNotNone(req)
                self.assertEqual(req.headers['Authorization'],
                                 'Bearer {}'.format(new_token))
                self.assertEqual(auth._token, new_token)

    def test_main(self):
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
                "url": "https://mycaseserver.com/#/cases"
                       "/4e8e23f4-9fe9-4215-92c9-12c9672be9f1",
                "priority": "Low"
            }
        }

        encoded_event = base64.b64encode(json.dumps(case_event).encode())

        with patch('requests.Session') as session:
            session.return_value = MagicMock()  # self._session
            session.return_value.request = MagicMock()

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

            session.return_value.request.side_effect = [
                create_mock, subscr_mock,
                consum_mock, commit_consumer_error_mock,
                commit_error_mock, commit_mock,
                delete_500_mock, delete_404_mock, delete_mock]

            channel = Channel(self.url,
                              auth=auth,
                              consumer_group=self.consumer_group,
                              retry_on_fail=False,
                              verify="cabundle.crt")

            self.assertEqual(channel._session.verify, "cabundle.crt")

            channel.commit()  # forcing early exit due to no records to commit

            channel.subscribe(["topic1", "topic2"])
            session.return_value.request.assert_called_with(
                "post",
                "http://localhost/databus/consumer-service/v1/consumers/1234/subscription",
                json={"topics": ["topic1", "topic2"]}
            )

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
            session.return_value.request.assert_called_with(
                "delete",
                "http://localhost/databus/consumer-service/v1/consumers/1234")
            session.return_value.request.reset_mock()

        channel.delete()  # trigger silent 404
        session.return_value.request.assert_called_with(
            "delete",
            "http://localhost/databus/consumer-service/v1/consumers/1234")
        session.return_value.request.reset_mock()

        channel._consumer_id = "1234"  # resetting consumer
        channel.delete()  # Proper deletion
        session.return_value.request.assert_called_with(
            "delete",
            "http://localhost/databus/consumer-service/v1/consumers/1234")
        session.return_value.request.reset_mock()

        channel.delete()  # trigger early exit
