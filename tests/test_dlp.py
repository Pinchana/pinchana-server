import asyncio
import os
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from pinchana_server.main import (
    DlpSubmitRequest,
    _dlp_headers,
    _dlp_job_owner,
    _require_api_key,
    _require_web_session,
    app,
    web_capabilities,
)


ENVIRONMENT = {
    "TURNSTILE_SESSION_SECRET": "s" * 32,
    "DLP_ENABLED": "true",
    "DLP_URL": "http://dlp-api:8080",
    "DLP_GATEWAY_TOKEN": "g" * 32,
    "DLP_OWNER_SECRET": "o" * 32,
}
CAPABILITIES = {
    "services": ["youtube"],
    "qualities": ["best", "4k", "audio"],
    "codecs": ["auto", "h264", "av1", "vp9"],
    "containers": ["auto", "mp4", "webm", "mkv"],
    "audioFormats": ["best", "mp3", "ogg", "wav", "opus"],
    "audioBitrates": ["320", "256", "128", "96", "64", "8"],
    "dubLanguages": ["en", "de", "fr"],
    "betterAudio": True,
}


def test_job_owner_is_stable_and_nonce_bound():
    with patch.dict(os.environ, ENVIRONMENT, clear=False):
        first = _dlp_job_owner({"nonce": "session-one"})
        assert first == _dlp_job_owner({"nonce": "session-one"})
        assert first != _dlp_job_owner({"nonce": "session-two"})
        assert _dlp_headers({"nonce": "session-one"}) == {
            "x-dlp-service-token": "g" * 32,
            "x-job-owner": first,
        }


def test_job_owner_requires_signed_session_nonce():
    with patch.dict(os.environ, ENVIRONMENT, clear=False), pytest.raises(HTTPException) as failure:
        _dlp_job_owner({})
    assert failure.value.status_code == 401


def test_capability_is_feature_gated():
    with patch.dict(os.environ, ENVIRONMENT, clear=False), patch(
        "pinchana_server.main._dlp_capabilities", AsyncMock(return_value=CAPABILITIES)
    ):
        enabled = asyncio.run(web_capabilities({"nonce": "n"}))
    assert enabled["dlp"]["available"] is True
    assert "4k" in enabled["dlp"]["qualities"]
    assert enabled["dlp"]["codecs"] == ["auto", "h264", "av1", "vp9"]
    assert enabled["dlp"]["containers"] == ["auto", "mp4", "webm", "mkv"]
    assert enabled["dlp"]["services"] == ["youtube"]
    assert enabled["dlp"]["audioFormats"] == ["best", "mp3", "ogg", "wav", "opus"]
    assert enabled["dlp"]["dubLanguages"] == ["en", "de", "fr"]
    assert enabled["dlp"]["betterAudio"] is True
    with patch.dict(os.environ, {**ENVIRONMENT, "DLP_ENABLED": "false"}, clear=False):
        disabled = asyncio.run(web_capabilities({"nonce": "n"}))
    assert disabled["dlp"] == {
        "available": False,
        "protocol": None,
        "services": [],
        "qualities": [],
        "codecs": [],
        "containers": [],
        "audioFormats": [],
        "audioBitrates": [],
        "dubLanguages": [],
        "betterAudio": False,
    }


def test_capability_fails_closed_when_dlp_is_unhealthy():
    with patch.dict(os.environ, ENVIRONMENT, clear=False), patch(
        "pinchana_server.main._dlp_capabilities", AsyncMock(return_value=None)
    ):
        result = asyncio.run(web_capabilities({"nonce": "n"}))
    assert result["dlp"]["available"] is False


def test_gateway_rejects_raw_format_and_unknown_quality():
    with pytest.raises(ValidationError):
        DlpSubmitRequest(url="https://youtube.com/watch?v=abcdefghijk", quality="best", format="raw")
    with pytest.raises(ValidationError):
        DlpSubmitRequest(url="https://youtube.com/watch?v=abcdefghijk", quality="unbounded")
    with pytest.raises(ValidationError):
        DlpSubmitRequest(url="https://youtube.com/watch?v=abcdefghijk", codec="custom")
    with pytest.raises(ValidationError):
        DlpSubmitRequest(url="https://youtube.com/watch?v=abcdefghijk", container="avi")
    with pytest.raises(ValidationError):
        DlpSubmitRequest(url="https://youtube.com/watch?v=abcdefghijk", audioFormat="flac")
    with pytest.raises(ValidationError):
        DlpSubmitRequest(url="https://youtube.com/watch?v=abcdefghijk", audioBitrate="192")
    with pytest.raises(ValidationError):
        DlpSubmitRequest(url="https://youtube.com/watch?v=abcdefghijk", dubLanguage="xx-invalid")


def test_dlp_routes_accept_only_signed_web_sessions_not_machine_keys():
    dlp_routes = [route for route in app.routes if getattr(route, "path", "").startswith("/web/dlp")]
    assert dlp_routes
    for route in dlp_routes:
        dependency_calls = {dependency.call for dependency in route.dependant.dependencies}
        assert _require_web_session in dependency_calls
        assert _require_api_key not in dependency_calls
