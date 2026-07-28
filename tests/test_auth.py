import asyncio
import json
import os
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

from fastapi import HTTPException

from pinchana_server.main import (
    TURNSTILE_SITEVERIFY_URL,
    WebVerifyRequest,
    _configured_api_keys,
    _issue_mobile_access_session,
    _issue_web_session,
    _rewrite_media_urls,
    _turnstile_rejection_reason,
    _valid_turnstile_result,
    _validate_mobile_session,
    _validate_web_session,
    mobile_verify,
    web_identity,
    web_verify,
)
from pinchana_server.mobile_auth import MobileInstallation


class GatewayAuthTests(unittest.TestCase):
    def test_instance_identity_serves_configured_certificate_with_cors(self):
        certificate = {"payload": "signed-payload", "signature": "signed-value"}
        with patch.dict(os.environ, {"PINCHANA_INSTANCE_CERTIFICATE": json.dumps(certificate)}):
            response = asyncio.run(web_identity())
        self.assertEqual(response.headers["access-control-allow-origin"], "*")
        self.assertEqual(json.loads(response.body), certificate)

    def test_instance_identity_requires_configuration(self):
        with patch.dict(os.environ, {"PINCHANA_INSTANCE_CERTIFICATE": "", "PINCHANA_INSTANCE_CERTIFICATE_FILE": ""}):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(web_identity())
        self.assertEqual(raised.exception.status_code, 503)

    def test_named_api_keys_are_loaded(self):
        with patch.dict(os.environ, {"PINCHANA_API_KEYS": '{"bot":"one","ci":"two"}'}):
            self.assertEqual(_configured_api_keys(), {"bot": "one", "ci": "two"})

    def test_web_session_round_trip_and_tamper_rejection(self):
        environment = {
            "TURNSTILE_SESSION_SECRET": "0123456789abcdef0123456789abcdef",
            "TURNSTILE_SESSION_MAX_AGE": "600",
        }
        with patch.dict(os.environ, environment):
            token, expires_at = _issue_web_session()
            claims = _validate_web_session(token)
            self.assertEqual(claims["exp"], expires_at)
            self.assertGreater(expires_at, int(time.time()))
            with self.assertRaises(HTTPException):
                _validate_web_session(f"{token}tampered")

    def test_web_response_uses_web_media_namespace(self):
        payload = {
            "video_url": "/media/tiktok/id/video.mp4",
            "carousel": [{"thumbnail_url": "/media/tiktok/id/one.jpg"}],
        }
        self.assertEqual(
            _rewrite_media_urls(payload, "/web/media"),
            {
                "video_url": "/web/media/tiktok/id/video.mp4",
                "carousel": [{"thumbnail_url": "/web/media/tiktok/id/one.jpg"}],
            },
        )

    def test_turnstile_hostname_and_action_are_enforced(self):
        environment = {
            "TURNSTILE_EXPECTED_HOSTNAME": "pinchana.example.com",
            "TURNSTILE_EXPECTED_ACTION": "turnstile-spin-v1",
        }
        with patch.dict(os.environ, environment):
            valid = {
                "success": True,
                "hostname": "pinchana.example.com",
                "action": "turnstile-spin-v1",
            }
            self.assertTrue(_valid_turnstile_result(valid))
            self.assertFalse(_valid_turnstile_result({**valid, "hostname": "attacker.example"}))
            self.assertFalse(_valid_turnstile_result({**valid, "action": "different"}))
            self.assertTrue(
                _valid_turnstile_result(
                    {"success": True, "hostname": "example.com", "action": None},
                    enforce_metadata=False,
                )
            )

    def test_turnstile_rejections_are_classified(self):
        environment = {
            "TURNSTILE_EXPECTED_HOSTNAME": "pinchana.example.com",
            "TURNSTILE_EXPECTED_ACTION": "turnstile-spin-v1",
        }
        valid = {
            "success": True,
            "hostname": "pinchana.example.com",
            "action": "turnstile-spin-v1",
        }
        with patch.dict(os.environ, environment):
            self.assertIsNone(_turnstile_rejection_reason(valid))
            self.assertEqual(
                _turnstile_rejection_reason({**valid, "hostname": "attacker.example"}),
                "hostname-mismatch",
            )
            self.assertEqual(
                _turnstile_rejection_reason({**valid, "action": "different"}),
                "action-mismatch",
            )
            self.assertEqual(
                _turnstile_rejection_reason({"success": False, "error-codes": ["timeout-or-duplicate"]}),
                "cloudflare-rejected",
            )

    def test_web_verify_logs_siteverify_rejection_reason(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "success": True,
            "hostname": "wrong.example.com",
            "action": "turnstile-spin-v1",
        }
        client = Mock()
        client.post = AsyncMock(return_value=response)
        environment = {
            "TURNSTILE_SECRET_KEY": "private-secret",
            "TURNSTILE_EXPECTED_HOSTNAME": "pinchana.example.com",
            "TURNSTILE_EXPECTED_ACTION": "turnstile-spin-v1",
            "TURNSTILE_SESSION_SECRET": "0123456789abcdef0123456789abcdef",
        }
        with (
            patch.dict(os.environ, environment),
            patch("pinchana_server.main.forward_client", client),
            self.assertLogs("pinchana_server.main", level="INFO") as captured,
        ):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(web_verify(WebVerifyRequest(token="browser-token")))

        self.assertEqual(raised.exception.status_code, 403)
        self.assertTrue(any("reason=hostname-mismatch" in message for message in captured.output))

    def test_web_verify_calls_cloudflare_siteverify_directly(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "success": True,
            "hostname": "pinchana.example.com",
            "action": "turnstile-spin-v1",
        }
        client = Mock()
        client.post = AsyncMock(return_value=response)
        environment = {
            "TURNSTILE_SECRET_KEY": "private-secret",
            "TURNSTILE_EXPECTED_HOSTNAME": "pinchana.example.com",
            "TURNSTILE_EXPECTED_ACTION": "turnstile-spin-v1",
            "TURNSTILE_SESSION_SECRET": "0123456789abcdef0123456789abcdef",
        }
        with (
            patch.dict(os.environ, environment),
            patch("pinchana_server.main.forward_client", client),
        ):
            session = asyncio.run(web_verify(WebVerifyRequest(token="browser-token")))

        self.assertTrue(session.access_token)
        call = client.post.await_args
        self.assertEqual(call.args[0], TURNSTILE_SITEVERIFY_URL)
        self.assertEqual(call.kwargs["data"]["secret"], "private-secret")
        self.assertEqual(call.kwargs["data"]["response"], "browser-token")

    def test_mobile_access_session_is_typed_scoped_and_tamper_resistant(self):
        environment = {
            "MOBILE_SESSION_SECRET": "mobile-secret-0123456789abcdef0123456789",
            "MOBILE_ACCESS_TOKEN_MAX_AGE": "600",
        }
        with patch.dict(os.environ, environment):
            token, expires_at = _issue_mobile_access_session(
                MobileInstallation(
                    installation_id="install_0123456789",
                    platform="ios",
                    app_id="cc.pinchana.mobile",
                    trust="attested",
                )
            )
            claims = _validate_mobile_session(token)
            self.assertEqual(claims["aud"], "pinchana-mobile")
            self.assertEqual(claims["typ"], "mobile_access")
            self.assertEqual(claims["sub"], "install_0123456789")
            self.assertIn("mobile:scrape", claims["scope"])
            self.assertEqual(claims["exp"], expires_at)
            with self.assertRaises(HTTPException) as raised:
                _validate_mobile_session(f"{token}tampered")
        self.assertEqual(raised.exception.status_code, 401)

    def test_web_session_cannot_be_used_as_mobile_session(self):
        environment = {
            "TURNSTILE_SESSION_SECRET": "0123456789abcdef0123456789abcdef",
            "MOBILE_SESSION_SECRET": "mobile-secret-0123456789abcdef0123456789",
        }
        with patch.dict(os.environ, environment):
            token, _expires_at = _issue_web_session()
            with self.assertRaises(HTTPException) as raised:
                _validate_mobile_session(token)
        self.assertEqual(raised.exception.status_code, 401)

    def test_static_mobile_key_exchange_is_retired(self):
        with self.assertRaises(HTTPException) as raised:
            asyncio.run(mobile_verify())
        self.assertEqual(raised.exception.status_code, 410)


if __name__ == "__main__":
    unittest.main()
