from __future__ import absolute_import
import unittest
from mock import patch, MagicMock
from requests.exceptions import RequestException
from dxlstreamingclient.auth import \
    (login, PermanentAuthenticationError, TemporaryAuthenticationError)


class Test(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_login_success(self):
        with patch('requests.get') as req_get:
            req_get.return_value = MagicMock()
            req_get.return_value.status_code = 200
            token = "1234567890"
            req_get.return_value.json = MagicMock(
                return_value={'AuthorizationToken': token})
            res = login("/", "username", "password",
                        verify_cert_bundle="cabundle.crt")
            req_get.assert_called_with(
                "/identity/v1/login", auth=("username", "password"),
                verify="cabundle.crt")
            self.assertIsNotNone(res)
            self.assertEqual(res, token)

    def test_login_not_200(self):
        with patch('requests.get') as req_get:
            req_get.return_value = MagicMock()
            req_get.return_value.status_code = 403
            req_get.return_value.json = MagicMock(
                return_value={
                    "message": "Invalid authentication credentials"})
            with self.assertRaises(PermanentAuthenticationError):
                login("/", "username", "password")

    def test_unexpected(self):
        with patch('requests.get') as req_get:
            req_get.return_value = MagicMock()
            req_get.return_value.status_code = 500

            with self.assertRaises(TemporaryAuthenticationError):
                login("/", "username", "password")

    def test_login_success_but_no_json_response(self):
        with patch('requests.get') as req_get:
            req_get.return_value = MagicMock()
            req_get.return_value.status_code = 200
            req_get.return_value.json = MagicMock(
                return_value="invalid")
            with self.assertRaises(PermanentAuthenticationError):
                login("/", "username", "password")

    def test_login_request_exception(self):
        with patch('requests.get') as req_get:
            req_get.side_effect = RequestException("Error")
            with self.assertRaises(TemporaryAuthenticationError):
                login("/", "username", "password")
