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
    web_capabilities,
)


ENVIRONMENT = {
    "TURNSTILE_SESSION_SECRET": "s" * 32,
    "DLP_ENABLED": "true",
    "DLP_URL": "http://dlp-api:8080",
    "DLP_GATEWAY_TOKEN": "g" * 32,
    "DLP_OWNER_SECRET": "o" * 32,
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
        "pinchana_server.main._dlp_healthy", AsyncMock(return_value=True)
    ):
        enabled = asyncio.run(web_capabilities({"nonce": "n"}))
    assert enabled["dlp"]["available"] is True
    with patch.dict(os.environ, {**ENVIRONMENT, "DLP_ENABLED": "false"}, clear=False):
        disabled = asyncio.run(web_capabilities({"nonce": "n"}))
    assert disabled["dlp"] == {"available": False, "protocol": None, "qualities": []}


def test_capability_fails_closed_when_dlp_is_unhealthy():
    with patch.dict(os.environ, ENVIRONMENT, clear=False), patch(
        "pinchana_server.main._dlp_healthy", AsyncMock(return_value=False)
    ):
        result = asyncio.run(web_capabilities({"nonce": "n"}))
    assert result["dlp"]["available"] is False


def test_gateway_rejects_raw_format_and_unknown_quality():
    with pytest.raises(ValidationError):
        DlpSubmitRequest(url="https://youtube.com/watch?v=abcdefghijk", quality="best", format="raw")
    with pytest.raises(ValidationError):
        DlpSubmitRequest(url="https://youtube.com/watch?v=abcdefghijk", quality="unbounded")
