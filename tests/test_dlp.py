import asyncio
import json
import os
import uuid
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from starlette.requests import Request

from pinchana_server.main import (
    DlpSubmitRequest,
    _dlp_headers,
    _dlp_job_owner,
    _public_build_manifest,
    _require_api_key,
    _require_web_session,
    app,
    web_build,
    web_capabilities,
    web_dlp_file,
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
    "filenameStyles": ["classic", "basic", "pretty", "nerdy"],
    "subtitleLanguages": ["en", "de", "fr"],
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
    assert enabled["dlp"]["filenameStyles"] == ["classic", "basic", "pretty", "nerdy"]
    assert enabled["dlp"]["subtitleLanguages"] == ["en", "de", "fr"]
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
        "filenameStyles": [],
        "subtitleLanguages": [],
        "betterAudio": False,
    }


def test_capability_fails_closed_when_dlp_is_unhealthy():
    with patch.dict(os.environ, ENVIRONMENT, clear=False), patch(
        "pinchana_server.main._dlp_capabilities", AsyncMock(return_value=None)
    ):
        result = asyncio.run(web_capabilities({"nonce": "n"}))
    assert result["dlp"]["available"] is False


def test_public_build_manifest_filters_untrusted_values():
    manifest = json.dumps({
        "api": {"commit": "A" * 40, "repository": "https://github.com/Pinchana/pinchana-api"},
        "threads": {"commit": "b" * 40, "repository": "https://github.com/Pinchana/pinchana-threads"},
        "bad name": {"commit": "c" * 40},
        "secret": {"commit": "not-a-commit", "repository": "https://internal.example/repo"},
    })
    with patch.dict(os.environ, {"PINCHANA_BUILD_COMMITS": manifest}, clear=False):
        result = _public_build_manifest()
    assert result == {
        "version": "preview",
        "commits": {
            "api": {"commit": "a" * 40, "repository": "https://github.com/Pinchana/pinchana-api"},
            "threads": {"commit": "b" * 40, "repository": "https://github.com/Pinchana/pinchana-threads"},
        },
    }


def test_public_build_endpoint_needs_no_session():
    response = asyncio.run(web_build())
    assert response.status_code == 200
    assert response.headers["cache-control"] == "public, max-age=300"


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
    with pytest.raises(ValidationError):
        DlpSubmitRequest(url="https://youtube.com/watch?v=abcdefghijk", filenameStyle="random")
    with pytest.raises(ValidationError):
        DlpSubmitRequest(url="https://youtube.com/watch?v=abcdefghijk", subtitleLanguage="xx-invalid")


def test_new_dlp_fields_remain_optional_for_staggered_rollout():
    request = DlpSubmitRequest(url="https://youtube.com/watch?v=abcdefghijk")
    payload = request.model_dump(mode="json", exclude_none=True)
    assert "filenameStyle" not in payload
    assert "subtitleLanguage" not in payload


def test_dlp_routes_accept_only_signed_web_sessions_not_machine_keys():
    dlp_routes = [route for route in app.routes if getattr(route, "path", "").startswith("/web/dlp")]
    assert dlp_routes
    for route in dlp_routes:
        dependency_calls = {dependency.call for dependency in route.dependant.dependencies}
        assert _require_web_session in dependency_calls
        assert _require_api_key not in dependency_calls


def test_private_file_forwards_range_and_partial_response_headers():
    seen_headers = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal seen_headers
        seen_headers = request.headers
        return httpx.Response(
            206,
            content=b"edi",
            headers={
                "Accept-Ranges": "bytes",
                "Content-Range": "bytes 1-3/5",
                "Content-Length": "3",
                "Content-Type": "video/mp4",
                "Content-Disposition": 'attachment; filename="video [pinchana.cc].mp4"',
                "ETag": '"private-video"',
                "Last-Modified": "Wed, 15 Jul 2026 12:00:00 GMT",
            },
        )

    async def exercise():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        request = Request({
            "type": "http",
            "method": "GET",
            "path": "/web/dlp/jobs/12345678-1234-4234-9234-123456789abc/file",
            "headers": [(b"range", b"bytes=1-3"), (b"if-range", b'"private-video"')],
        })
        with patch.dict(os.environ, ENVIRONMENT, clear=False), patch("pinchana_server.main.forward_client", client):
            response = await web_dlp_file(
                uuid.UUID("12345678-1234-4234-9234-123456789abc"),
                request,
                {"nonce": "session-one"},
            )
            await response.background()
        await client.aclose()
        return response

    response = asyncio.run(exercise())
    assert seen_headers is not None
    assert seen_headers["range"] == "bytes=1-3"
    assert seen_headers["if-range"] == '"private-video"'
    assert response.status_code == 206
    assert response.headers["content-range"] == "bytes 1-3/5"
    assert response.headers["accept-ranges"] == "bytes"
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"


def test_private_file_preserves_unsatisfied_range_response():
    async def exercise():
        client = httpx.AsyncClient(transport=httpx.MockTransport(
            lambda _request: httpx.Response(416, headers={"Content-Range": "*/5", "Accept-Ranges": "bytes"})
        ))
        request = Request({
            "type": "http",
            "method": "GET",
            "path": "/web/dlp/jobs/12345678-1234-4234-9234-123456789abc/file",
            "headers": [(b"range", b"bytes=10-20")],
        })
        with patch.dict(os.environ, ENVIRONMENT, clear=False), patch("pinchana_server.main.forward_client", client):
            response = await web_dlp_file(
                uuid.UUID("12345678-1234-4234-9234-123456789abc"),
                request,
                {"nonce": "session-one"},
            )
            await response.background()
        await client.aclose()
        return response

    response = asyncio.run(exercise())
    assert response.status_code == 416
    assert response.headers["content-range"] == "*/5"
