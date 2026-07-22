"""Integration and unit tests for the v2 backend slice (contracts, ticket store, SSRF, zero-cache, streaming, and error handling)."""

import os
os.environ["TURNSTILE_SESSION_SECRET"] = "secret_session_key_32_bytes_long!!"

import asyncio
import ipaddress
import json
import pathlib
import secrets
import tempfile
import time
import unittest.mock
from types import SimpleNamespace
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pinchana_core.models import RemoteAssetDescriptor, ScrapeV2Context, ScrapeV2ExtractedData, ScraperCapabilitiesV2
from pinchana_core.plugins import registry
import pinchana_inst.main  # Ensures instagram router is mounted in registry
import pinchana_server.main as server_main

from pinchana_server.main import app, _issue_web_session, ticket_store
from pinchana_server.ssrf import PinnedAsyncNetworkBackend, validate_upstream_url, is_ip_forbidden
from pinchana_server.tickets import InMemoryTicketStore, RedisTicketStore, TicketData


@pytest.fixture
def client():
    if not hasattr(app, "_instagram_mounted"):
        app.include_router(pinchana_inst.main.router, prefix="/instagram")
        setattr(app, "_instagram_mounted", True)
    with TestClient(app) as c:
        yield c


@pytest.fixture
def web_session_header():
    token, _exp = _issue_web_session()
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# 1. Zero Persistent-Cache Writes Test
# ---------------------------------------------------------------------------
def test_zero_persistent_cache_writes_on_unmigrated_fallback(client, web_session_header):
    cache_dir = pathlib.Path("./cache")
    cache_dir.mkdir(exist_ok=True)
    initial_files = set(cache_dir.glob("**/*"))

    # Execute v2 scrape request
    mock_raw_data = {
        "__typename": "GraphImage",
        "shortcode": "ZERO123",
        "owner": {"username": "testuser"},
        "display_url": "https://1.1.1.1/image.jpg",
        "edge_media_to_caption": {"edges": [{"node": {"text": "Zero Cache"}}]},
    }
    with unittest.mock.patch("pinchana_inst.main.scraper.extract_media", new_callable=unittest.mock.AsyncMock) as mock_extract:
        mock_extract.return_value = mock_raw_data
        res = client.post(
            "/v2/scrape",
            json={"url": "https://www.instagram.com/p/ZERO123/"},
            headers=web_session_header,
        )
        assert res.status_code == 200

    after_files = set(cache_dir.glob("**/*"))
    assert after_files == initial_files, "Persistent cache directory must remain completely empty/unchanged!"


# ---------------------------------------------------------------------------
# 2. SSRF Tests: IP Ranges, Mapped IPv6, Credentials, Hostname Resolution
# ---------------------------------------------------------------------------
def test_ssrf_forbidden_ips():
    forbidden_ips = [
        "0.0.0.0",
        "127.0.0.1",
        "10.0.0.1",
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",
        "224.0.0.1",
        "::1",
        "::ffff:127.0.0.1",
        "::ffff:10.0.0.1",
        "fe80::1",
    ]
    for ip_str in forbidden_ips:
        ip_obj = ipaddress.ip_address(ip_str)
        assert is_ip_forbidden(ip_obj) is True, f"IP {ip_str} should be forbidden"


def test_ssrf_url_validation():
    with pytest.raises(HTTPException) as exc_info:
        validate_upstream_url("http://127.0.0.1/admin")
    assert exc_info.value.status_code == 403

    with pytest.raises(HTTPException) as exc_info:
        validate_upstream_url("http://user:pass@example.com/file.mp4")
    assert exc_info.value.status_code == 400

    with pytest.raises(HTTPException) as exc_info:
        validate_upstream_url("ftp://example.com/file.mp4")
    assert exc_info.value.status_code == 400

    with pytest.raises(HTTPException) as exc_info:
        validate_upstream_url("http://[::ffff:127.0.0.1]/secret")
    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_pinned_network_backend_dials_validated_address():
    backend = PinnedAsyncNetworkBackend("cdn.example.com", "203.0.113.10")
    delegate = unittest.mock.AsyncMock()
    delegate.connect_tcp.return_value = unittest.mock.MagicMock()
    backend.backend = delegate

    await backend.connect_tcp("cdn.example.com", 443, timeout=3)

    delegate.connect_tcp.assert_awaited_once_with(
        "203.0.113.10",
        443,
        timeout=3,
        local_address=None,
        socket_options=None,
    )
    with pytest.raises(OSError, match="validated hostname"):
        await backend.connect_tcp("rebound.example.com", 443)


# ---------------------------------------------------------------------------
# 3. Ticket Store Lifecycle, Expiry, Lease Leases, and Multi-Worker Guard
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_ticket_store_lifecycle_and_expiry():
    store = InMemoryTicketStore(check_workers=False)
    desc = RemoteAssetDescriptor(
        index=0,
        media_type="video",
        role="content",
        filename="test.mp4",
        mime_type="video/mp4",
        upstream_url="https://1.1.1.1/video.mp4",
        asset_id="test:123:0:content:abc123",
    )
    ticket = await store.create_ticket(
        session_nonce="nonce123",
        instance_id="pinchana-project",
        descriptor=desc,
        ttl_seconds=1,
    )

    assert ticket.ticket_id is not None
    assert ticket.active_leases == 0

    # Acquire lease while valid
    acquired = await store.acquire_lease(ticket.ticket_id)
    assert acquired is not None
    assert acquired.active_leases == 1

    # Simulate ticket expiry while stream is active
    ticket.expires_at = int(time.time()) - 10
    assert ticket.is_expired() is True

    # Active stream survives ticket expiry because active_leases > 0
    fetched = await store.get_ticket(ticket.ticket_id)
    assert fetched is not None

    # Releasing lease cleans up expired ticket
    await store.release_lease(ticket.ticket_id)
    assert await store.get_ticket(ticket.ticket_id) is None


def test_multi_worker_in_memory_store_guard():
    os.environ["WORKERS"] = "4"
    try:
        with pytest.raises(RuntimeError) as exc_info:
            InMemoryTicketStore(check_workers=True)
        assert "cannot be used when multiple server workers are configured" in str(exc_info.value)
    finally:
        os.environ.pop("WORKERS", None)


@pytest.mark.asyncio
async def test_redis_ticket_leases_are_atomic_for_concurrent_streams():
    import fakeredis

    store = RedisTicketStore.__new__(RedisTicketStore)
    store.redis = fakeredis.FakeAsyncRedis(decode_responses=True)
    eval_lock = asyncio.Lock()

    async def atomic_eval(script, _key_count, key, *args):
        async with eval_lock:
            raw = await store.redis.get(key)
            if not raw:
                return None if "return nil" in script else 0
            payload = json.loads(raw)
            if "+ 1" in script:
                payload["active_leases"] = payload.get("active_leases", 0) + 1
                await store.redis.set(key, json.dumps(payload), ex=300)
                return json.dumps(payload)
            payload["active_leases"] = max(0, payload.get("active_leases", 0) - 1)
            await store.redis.set(key, json.dumps(payload), ex=300)
            return 1

    store.redis.eval = atomic_eval
    descriptor = RemoteAssetDescriptor(
        index=0,
        media_type="video",
        role="content",
        filename="test.mp4",
        mime_type="video/mp4",
        upstream_url="https://1.1.1.1/video.mp4",
    )
    ticket = await store.create_ticket("nonce", "instance", descriptor)

    first, second = await asyncio.gather(
        store.acquire_lease(ticket.ticket_id),
        store.acquire_lease(ticket.ticket_id),
    )

    assert first is not None and second is not None
    current = await store.get_ticket(ticket.ticket_id)
    assert current is not None and current.active_leases == 2
    await asyncio.gather(
        store.release_lease(ticket.ticket_id),
        store.release_lease(ticket.ticket_id),
    )
    current = await store.get_ticket(ticket.ticket_id)
    assert current is not None and current.active_leases == 0


# ---------------------------------------------------------------------------
# 4. Capabilities Negotiation & Serialization
# ---------------------------------------------------------------------------
def test_v2_capabilities_endpoint(client):
    response = client.get("/instagram/v2/capabilities")
    assert response.status_code == 200
    payload = response.json()
    cap = ScraperCapabilitiesV2(**payload)
    assert cap.supports_v2_remote is True
    assert "tunnel" in cap.supported_delivery_policies


# ---------------------------------------------------------------------------
# 5. Asset Streaming, HEAD, 206, 416, Unknown Content-Length, and Cross-Origin Stripping
# ---------------------------------------------------------------------------
def test_v2_scrape_and_asset_streaming_full(client, web_session_header):
    mock_raw_data = {
        "__typename": "GraphImage",
        "shortcode": "STREAM1",
        "owner": {"username": "testuser"},
        "display_url": "https://1.1.1.1/image.jpg",
        "edge_media_to_caption": {"edges": [{"node": {"text": "Stream Test"}}]},
    }
    with unittest.mock.patch("pinchana_inst.main.scraper.extract_media", new_callable=unittest.mock.AsyncMock) as mock_extract:
        mock_extract.return_value = mock_raw_data
        response = client.post(
            "/v2/scrape",
            json={"url": "https://www.instagram.com/p/STREAM1/"},
            headers=web_session_header,
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "ready"

        asset = payload["assets"][0]
        ticket_id = asset["delivery"]["url"].removeprefix("/v2/assets/")

        # Test HEAD request
        with unittest.mock.patch("httpx.AsyncClient.send", new_callable=unittest.mock.AsyncMock) as mock_send:
            mock_resp = unittest.mock.MagicMock()
            mock_resp.status_code = 200
            mock_resp.headers = {"content-type": "image/jpeg", "content-length": "1234"}
            mock_resp.aclose = unittest.mock.AsyncMock()
            mock_send.return_value = mock_resp

            head_resp = client.head(f"/v2/assets/{ticket_id}", headers=web_session_header)
            assert head_resp.status_code == 200
            assert head_resp.headers.get("content-type") == "image/jpeg"
            assert head_resp.content == b""  # HEAD returns empty body

        # Test 206 Partial Content Range request
        with unittest.mock.patch("httpx.AsyncClient.send", new_callable=unittest.mock.AsyncMock) as mock_send:
            mock_resp = unittest.mock.MagicMock()
            mock_resp.status_code = 206
            mock_resp.headers = {
                "content-type": "image/jpeg",
                "content-range": "bytes 0-10/100",
                "content-length": "11",
            }
            async def mock_aiter():
                yield b"0123456789X"
            mock_resp.aiter_raw = mock_aiter
            mock_resp.aclose = unittest.mock.AsyncMock()
            mock_send.return_value = mock_resp

            range_resp = client.get(
                f"/v2/assets/{ticket_id}",
                headers={**web_session_header, "Range": "bytes=0-10"},
            )
            assert range_resp.status_code == 206
            assert range_resp.headers.get("content-range") == "bytes 0-10/100"
            assert range_resp.content == b"0123456789X"


def test_v2_asset_status_416_and_unknown_length(client, web_session_header):
    mock_raw_data = {
        "__typename": "GraphImage",
        "shortcode": "STATUS416",
        "owner": {"username": "testuser"},
        "display_url": "https://1.1.1.1/image.jpg",
    }
    with unittest.mock.patch("pinchana_inst.main.scraper.extract_media", new_callable=unittest.mock.AsyncMock) as mock_extract:
        mock_extract.return_value = mock_raw_data
        res = client.post("/v2/scrape", json={"url": "https://www.instagram.com/p/STATUS416/"}, headers=web_session_header)
        ticket_id = res.json()["assets"][0]["delivery"]["url"].removeprefix("/v2/assets/")

        # Test 416 Range Not Satisfiable
        with unittest.mock.patch("httpx.AsyncClient.send", new_callable=unittest.mock.AsyncMock) as mock_send:
            mock_resp = unittest.mock.MagicMock()
            mock_resp.status_code = 416
            mock_resp.headers = {"content-range": "bytes */100"}
            async def mock_aiter():
                yield b""
            mock_resp.aiter_raw = mock_aiter
            mock_resp.aclose = unittest.mock.AsyncMock()
            mock_send.return_value = mock_resp

            resp_416 = client.get(f"/v2/assets/{ticket_id}", headers={**web_session_header, "Range": "bytes=9999-10000"})
            assert resp_416.status_code == 416

        # Test Unknown Content-Length (chunked encoding without Content-Length)
        with unittest.mock.patch("httpx.AsyncClient.send", new_callable=unittest.mock.AsyncMock) as mock_send:
            mock_resp = unittest.mock.MagicMock()
            mock_resp.status_code = 200
            mock_resp.headers = {"content-type": "video/mp4"}
            async def mock_aiter_chunked():
                yield b"chunk1"
                yield b"chunk2"
            mock_resp.aiter_raw = mock_aiter_chunked
            mock_resp.aclose = unittest.mock.AsyncMock()
            mock_send.return_value = mock_resp

            resp_chunked = client.get(f"/v2/assets/{ticket_id}", headers=web_session_header)
            assert resp_chunked.status_code == 200
            assert resp_chunked.content == b"chunk1chunk2"


def test_tunnel_uses_negotiated_webp_mime_and_filename(client, web_session_header):
    mock_raw_data = {
        "__typename": "GraphImage",
        "shortcode": "WEBP1",
        "owner": {"username": "testuser"},
        "display_url": "https://1.1.1.1/image.jpg",
    }
    with unittest.mock.patch(
        "pinchana_inst.main.scraper.extract_media",
        new_callable=unittest.mock.AsyncMock,
        return_value=mock_raw_data,
    ):
        resolved = client.post(
            "/v2/scrape",
            json={"url": "https://www.instagram.com/p/WEBP1/"},
            headers=web_session_header,
        )
    ticket_url = resolved.json()["assets"][0]["delivery"]["url"]

    with unittest.mock.patch(
        "httpx.AsyncClient.send", new_callable=unittest.mock.AsyncMock
    ) as mock_send:
        upstream = unittest.mock.MagicMock()
        upstream.status_code = 200
        upstream.headers = {"content-type": "image/webp", "content-length": "4"}

        async def body():
            yield b"RIFF"

        upstream.aiter_raw = body
        upstream.aclose = unittest.mock.AsyncMock()
        mock_send.return_value = upstream
        response = client.get(ticket_url, headers=web_session_header)

    assert response.status_code == 200
    assert response.headers["content-type"] == "image/webp"
    assert 'filename="instagram_WEBP1_1.webp"' in response.headers["content-disposition"]
    assert response.content == b"RIFF"


def test_expired_ticket_returns_410_before_upstream_access(client, web_session_header):
    mock_raw_data = {
        "__typename": "GraphImage",
        "shortcode": "EXPIRED1",
        "owner": {"username": "testuser"},
        "display_url": "https://1.1.1.1/image.jpg",
    }
    with unittest.mock.patch(
        "pinchana_inst.main.scraper.extract_media",
        new_callable=unittest.mock.AsyncMock,
        return_value=mock_raw_data,
    ):
        resolved = client.post(
            "/v2/scrape",
            json={"url": "https://www.instagram.com/p/EXPIRED1/"},
            headers=web_session_header,
        )
    ticket_id = resolved.json()["assets"][0]["delivery"]["url"].rsplit("/", 1)[-1]
    assert isinstance(server_main.ticket_store, InMemoryTicketStore)
    server_main.ticket_store._tickets[ticket_id].expires_at = int(time.time()) - 1

    with unittest.mock.patch("httpx.AsyncClient.send") as upstream:
        response = client.get(f"/v2/assets/{ticket_id}", headers=web_session_header)

    assert response.status_code == 410
    upstream.assert_not_called()


def test_v2_unmigrated_scraper_is_not_routed_through_false_zero_cache_fallback(client, web_session_header):
    # Staged rollout keeps unmigrated modules on the public v1 web route.
    with unittest.mock.patch("pinchana_server.main._resolve_module") as mock_resolve:
        mock_resolve.return_value = ("in_process", "unmigrated_module", None)
        with unittest.mock.patch("httpx.AsyncClient.get", new_callable=unittest.mock.AsyncMock) as mock_cap_get:
            mock_cap_resp = unittest.mock.MagicMock()
            mock_cap_resp.status_code = 404
            mock_cap_get.return_value = mock_cap_resp

            res = client.post(
                "/v2/scrape",
                json={"url": "https://unmigrated.example.com/post/1"},
                headers=web_session_header,
            )
            assert res.status_code == 409
            assert res.json()["detail"]["code"] == "v2_disabled"
            mock_cap_get.assert_not_called()


@pytest.mark.parametrize(
    ("platform", "url", "flag"),
    [
        ("tiktok", "https://www.tiktok.com/@u/video/1", "PINCHANA_V2_TIKTOK"),
        ("threads", "https://www.threads.com/@u/post/abc", "PINCHANA_V2_THREADS"),
        ("twitter", "https://x.com/u/status/1", "PINCHANA_V2_TWITTER"),
    ],
)
def test_native_v2_dispatch_for_phase4_platforms(client, web_session_header, monkeypatch, platform, url, flag):
    monkeypatch.setenv(flag, "true")
    target = SimpleNamespace(endpoint=f"http://{platform}.internal")
    extracted = {
        "shortcode": "native-1",
        "caption": "caption",
        "author": "creator",
        "media_type": "video",
        "platform_id": f"{platform}:native-1",
        "assets": [{
            "index": 0,
            "media_type": "video",
            "role": "content",
            "filename": f"{platform}.mp4",
            "mime_type": "video/mp4",
            "upstream_url": "https://1.1.1.1/media.mp4?signature=hidden",
            "safe_headers": {"Referer": f"https://{platform}.example/"},
            "size": 123,
            "duration_seconds": 4.5,
            "dimensions": {"width": 720, "height": 1280},
            "bitrate": 1000000,
            "looping": platform == "twitter",
            "supports_range": True,
            "asset_id": f"{platform}:native-1:media-1:content",
            "source_revision": "rev-1",
            "source_fingerprint": "fingerprint-1",
        }],
    }

    def fake_response(status, payload):
        response = unittest.mock.MagicMock()
        response.status_code = status
        response.json.return_value = payload
        return response

    with unittest.mock.patch("pinchana_server.main._resolve_module", return_value=("container", platform, target)), \
         unittest.mock.patch("httpx.AsyncClient.get", new_callable=unittest.mock.AsyncMock, return_value=fake_response(200, {"supports_v2_remote": True})), \
         unittest.mock.patch("httpx.AsyncClient.post", new_callable=unittest.mock.AsyncMock, return_value=fake_response(200, extracted)):
        response = client.post("/v2/scrape", json={"url": url}, headers=web_session_header)

    assert response.status_code == 200
    body = response.json()
    assert body["source"]["platform"] == platform
    assert body["assets"][0]["asset_key"] == f"{platform}:native-1:media-1:content"
    assert body["assets"][0]["dimensions"] == {"width": 720, "height": 1280}
    assert body["assets"][0]["looping"] is (platform == "twitter")
    serialized = json.dumps(body)
    assert "signature=hidden" not in serialized
    assert "safe_headers" not in serialized


def test_capability_failure_uses_explicit_rollback_code(client, web_session_header, monkeypatch):
    monkeypatch.setenv("PINCHANA_V2_TWITTER", "true")
    target = SimpleNamespace(endpoint="http://twitter.internal")
    response = unittest.mock.MagicMock()
    response.status_code = 503
    response.json.return_value = {}
    with unittest.mock.patch("pinchana_server.main._resolve_module", return_value=("container", "twitter", target)), \
         unittest.mock.patch("httpx.AsyncClient.get", new_callable=unittest.mock.AsyncMock, return_value=response):
        result = client.post("/v2/scrape", json={"url": "https://x.com/u/status/1"}, headers=web_session_header)
    assert result.status_code == 502
    assert result.json()["detail"]["code"] == "v2_capability_unavailable"


@pytest.mark.asyncio
async def test_ticket_ttl_never_exceeds_upstream_expiry_margin():
    from pinchana_server.main import _descriptor_ticket_ttl, _descriptor_can_tunnel

    descriptor = RemoteAssetDescriptor(
        index=0,
        media_type="video",
        role="content",
        filename="short.mp4",
        mime_type="video/mp4",
        upstream_url="https://1.1.1.1/short.mp4",
        expires_at=int(time.time()) + 80,
        supports_range=True,
    )
    assert _descriptor_ticket_ttl(descriptor) <= 20
    assert _descriptor_can_tunnel(descriptor) is False


def test_non_range_asset_uses_session_bound_ephemeral_spool_job(
    client, web_session_header, monkeypatch
):
    monkeypatch.setenv("PINCHANA_V2_THREADS", "true")
    target = SimpleNamespace(endpoint="http://threads.internal")

    def fake_response(status, payload):
        response = unittest.mock.MagicMock()
        response.status_code = status
        response.json.return_value = payload
        return response

    extracted = {
        "shortcode": "spool-1",
        "caption": "caption",
        "author": "creator",
        "media_type": "image",
        "platform_id": "threads:spool-1",
        "assets": [{
            "index": 0,
            "media_type": "image",
            "role": "content",
            "filename": "threads.jpg",
            "upstream_url": "https://1.1.1.1/media.jpg?oh=hidden",
            "supports_range": False,
            "asset_id": "threads:spool-1:media-1:content",
            "source_fingerprint": "stable-fingerprint",
        }],
    }

    def discard_background(coroutine):
        coroutine.close()
        future = asyncio.get_running_loop().create_future()
        future.set_result(None)
        return future

    with unittest.mock.patch(
        "pinchana_server.main._resolve_module",
        return_value=("container", "threads", target),
    ), unittest.mock.patch(
        "httpx.AsyncClient.get",
        new_callable=unittest.mock.AsyncMock,
        return_value=fake_response(200, {"supports_v2_remote": True}),
    ), unittest.mock.patch(
        "httpx.AsyncClient.post",
        new_callable=unittest.mock.AsyncMock,
        return_value=fake_response(200, extracted),
    ), unittest.mock.patch(
        "pinchana_server.main.asyncio.create_task", side_effect=discard_background
    ):
        result = client.post(
            "/v2/scrape",
            json={"url": "https://www.threads.com/@u/post/spool-1"},
            headers=web_session_header,
        )

    assert result.status_code == 200
    body = result.json()
    assert body["status"] == "processing"
    assert "assets" not in body
    assert "hidden" not in json.dumps(body)

    other_token, _ = _issue_web_session()
    forbidden = client.get(
        body["status_url"], headers={"Authorization": f"Bearer {other_token}"}
    )
    assert forbidden.status_code == 403


@pytest.mark.asyncio
async def test_ephemeral_job_state_uses_shared_redis_when_configured(monkeypatch):
    import fakeredis
    import pinchana_server.main as server_main

    redis = fakeredis.FakeAsyncRedis(decode_responses=True)
    monkeypatch.setattr(server_main, "normalization_redis", redis)
    job = {
        "status": "processing",
        "expires_at": int(time.time()) + 300,
        "session_nonce": "nonce",
        "instance_id": "instance",
        "spool_dir": None,
    }

    await server_main._set_ephemeral_job("shared-job", job)
    assert await server_main._get_ephemeral_job("shared-job") == job
    await server_main._delete_ephemeral_job("shared-job")
    assert await server_main._get_ephemeral_job("shared-job") is None
    await redis.aclose()
