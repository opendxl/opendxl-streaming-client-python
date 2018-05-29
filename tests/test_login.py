import unittest
from mock import patch, MagicMock
from dxldbconsumerclient.auth import (login, PermanentAuthenticationError,
                                      TemporaryAuthenticationError)
from requests.exceptions import RequestException


class Test(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_login_success(self):
        with patch('requests.get') as r:
            r.return_value = MagicMock()
            r.return_value.status_code = 200
            token = "1234567890"
            r.return_value.json = MagicMock(
                return_value={'AuthorizationToken': token})
            res = login("/", "username", "password")
            self.assertIsNotNone(res)
            self.assertEqual(res, token)

    def test_login_not_200(self):
        with patch('requests.get') as r:
            r.return_value = MagicMock()
            r.return_value.status_code = 403
            r.return_value.json = MagicMock(
                return_value={
                    "message": "Invalid authentication credentials"})
            with self.assertRaises(PermanentAuthenticationError):
                login("/", "username", "password")

    def test_unexpected(self):
        with patch('requests.get') as r:
            r.return_value = MagicMock()
            r.return_value.status_code = 500

            with self.assertRaises(TemporaryAuthenticationError):
                login("/", "username", "password")

    def test_login_success_but_no_json_response(self):
        with patch('requests.get') as r:
            r.return_value = MagicMock()
            r.return_value.status_code = 200
            r.return_value.json = MagicMock(
                return_value="invalid")
            with self.assertRaises(PermanentAuthenticationError):
                login("/", "username", "password")

    def test_login_request_exception(self):
        with patch('requests.get') as r:
            r.side_effect = RequestException("Error")
            with self.assertRaises(TemporaryAuthenticationError):
                login("/", "username", "password")
