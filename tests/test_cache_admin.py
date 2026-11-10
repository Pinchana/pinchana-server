import asyncio
import json
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
from fastapi import HTTPException

from pinchana_server import main
from pinchana_server.cache_admin import clear_media_cache, resolve_cache_target


def post(root, shortcode):
    folder = root / shortcode
    folder.mkdir()
    (folder / "metadata.json").write_text('{}')
    (folder / "video.mp4").write_bytes(b"cached video")
    return folder


def test_clear_one_and_missing_are_idempotent(tmp_path):
    post(tmp_path, "ABC")
    other = post(tmp_path, "ABCD")
    result = clear_media_cache(tmp_path, "ABC")
    assert result.shortcode == "ABC"
    assert result.deleted_posts == 1
    assert result.freed_bytes == 14
    assert other.is_dir()
    assert clear_media_cache(tmp_path, "ABC").deleted_posts == 0


def test_clear_all_preserves_databases_service_state_and_root(tmp_path):
    post(tmp_path, "ABC")
    post(tmp_path, "12345")
    database = tmp_path / "mobile-sessions.sqlite3"
    database.write_bytes(b"database")
    state = tmp_path / "sessions"
    state.mkdir()
    (state / "state.db").write_bytes(b"state")
    result = clear_media_cache(tmp_path, None)
    assert result.shortcode is None
    assert result.deleted_posts == 2
    assert database.read_bytes() == b"database"
    assert (state / "state.db").read_bytes() == b"state"
    assert tmp_path.is_dir()


def test_clear_never_follows_symlinks(tmp_path):
    root = tmp_path / "cache"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "metadata.json").write_text('{}')
    secret = outside / "secret.mp4"
    secret.write_bytes(b"secret")
    (root / "ALIAS").symlink_to(outside, target_is_directory=True)
    folder = post(root, "ABC")
    (folder / "external").symlink_to(outside, target_is_directory=True)
    (folder / "linked.mp4").symlink_to(secret)
    assert clear_media_cache(root, "ALIAS").deleted_posts == 0
    assert clear_media_cache(root, None).deleted_posts == 1
    assert secret.read_bytes() == b"secret"
    assert (root / "ALIAS").is_symlink()


@pytest.mark.parametrize("target", ["..", ".", "../ABC", "/tmp", "ABC/def", "a\\b", "", "a" * 129])
def test_clear_rejects_unsafe_shortcodes(tmp_path, target):
    with pytest.raises(ValueError):
        clear_media_cache(tmp_path, target)


@pytest.mark.parametrize(("target", "expected"), [
    ("all", None), (" ALL ", "ALL"), ("ABC_-123", "ABC_-123"),
    ("https://www.tiktok.com/@user/video/7695429756879195425?foo=1", "7695429756879195425"),
    ("https://m.tiktok.com/@user/photo/12345", "12345"),
    ("https://www.instagram.com/p/ABC_123/", "ABC_123"),
    ("https://instagram.com/reel/ABC-123/?igsh=1", "ABC-123"),
    ("https://instagram.com/creator/reel/ABC/", "ABC"),
    ("https://www.threads.com/@user/post/ABC123", "ABC123"),
    ("https://threads.net/t/ABC123/", "ABC123"),
    ("https://x.com/user/status/123456/photo/1", "123456"),
    ("https://twitter.com/i/web/status/123456", "123456"),
    ("https://youtu.be/abcdefghijk?t=3", "abcdefghijk"),
    ("https://www.youtube.com/shorts/abcdefghijk", "abcdefghijk"),
    ("https://music.youtube.com/watch?v=abcdefghijk&list=xyz", "abcdefghijk"),
    ("https://coub.com/view/abc123", "abc123"),
    ("https://coub.com/embed/abc123", "abc123"),
    ("https://music.apple.com/us/song/example/123", "am-123"),
    ("https://music.apple.com/us/album/example/456?i=123", "am-123"),
    ("https://open.spotify.com/intl-uk/track/ABC123", "sp-ABC123"),
    ("https://open.spotify.com/album/ABC123", "sp-album-ABC123"),
    ("https://deezer.com/uk/track/12345", "dz-12345"),
    ("https://www.deezer.com/album/12345", "dz-album-12345"),
])
def test_target_resolution_without_network(target, expected):
    assert asyncio.run(resolve_cache_target(target, None)) == expected


@pytest.mark.parametrize("target", [
    "../ABC", "https://evil.example/p/ABC", "http://www.tiktok.com/@user/video/123",
    "https://tiktok.com.evil.example/t/ABC", "https://user:pass@www.tiktok.com/t/ABC",
    "https://www.tiktok.com:8443/t/ABC", "https://www.tiktok.com:bad/t/ABC",
    "https://www.instagram.com/p/%2e%2e/", "https://www.instagram.com/p/..%2fABC/",
    "https://www.instagram.com/p/" + "A" * 129, "https://www.tiktok.com/@user",
    "https://www.instagram.com/p/ABC/ extra",
])
def test_unsafe_or_unresolvable_target_rejected(target):
    with pytest.raises(HTTPException) as error:
        asyncio.run(resolve_cache_target(target, None))
    assert error.value.status_code == 400


def resolve_with_transport(target, handler):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await resolve_cache_target(target, client)
    return asyncio.run(run())


@pytest.mark.parametrize("target", ["https://tt.site/t/ABC", "https://www.tt.site/t/ABC/"])
def test_tt_site_fallback_stops_before_final_post(target):
    requests = []
    def handler(request):
        requests.append(str(request.url))
        if request.url.host == "www.tt.site":
            return httpx.Response(502)
        return httpx.Response(301, headers={"location": "https://www.tiktok.com/@user/video/12345"})
    assert resolve_with_transport(target, handler) == "12345"
    assert requests[0].startswith("https://www.tt.site/t/ABC")
    assert requests[1] == "https://vm.tiktok.com/ABC/"
    assert len(requests) == 2


def test_tt_site_transport_fallback():
    def handler(request):
        if request.url.host == "www.tt.site":
            raise httpx.ConnectError("temporary", request=request)
        return httpx.Response(302, headers={"location": "https://www.tiktok.com/@user/photo/123"})
    assert resolve_with_transport("https://tt.site/t/ABC/", handler) == "123"


@pytest.mark.parametrize("status", [403, 404, 429])
def test_tt_site_does_not_fallback_for_permanent_or_throttled_errors(status):
    requests = []
    def handler(request):
        requests.append(request)
        return httpx.Response(status)
    with pytest.raises(HTTPException):
        resolve_with_transport("https://tt.site/t/ABC/", handler)
    assert len(requests) == 1


@pytest.mark.parametrize("location", [
    "https://evil.example/video/123", "http://www.tiktok.com/@user/video/123",
    "https://www.tiktok.com:8443/@user/video/123", "https://www.instagram.com/p/ABC/",
    "https://user:pass@www.tiktok.com/@user/video/123", "http://127.0.0.1/admin",
])
def test_redirect_destination_is_validated_before_request(location):
    requests = []
    def handler(request):
        requests.append(request)
        return httpx.Response(302, headers={"location": location})
    with pytest.raises(HTTPException) as error:
        resolve_with_transport("https://vm.tiktok.com/ABC/", handler)
    assert error.value.status_code == 400
    assert len(requests) == 1


def test_redirect_budget_and_relative_redirects():
    requests = []
    def handler(request):
        requests.append(request)
        return httpx.Response(302, headers={"location": "/t/ABC/"})
    with pytest.raises(HTTPException):
        resolve_with_transport("https://www.tiktok.com/t/ABC/", handler)
    assert len(requests) == 3


def test_third_redirect_can_resolve_without_fourth_get():
    requests = []
    def handler(request):
        requests.append(request)
        location = "https://www.tiktok.com/@user/video/123" if len(requests) == 3 else "/t/ABC/"
        return httpx.Response(302, headers={"location": location})
    assert resolve_with_transport("https://www.tiktok.com/t/ABC/", handler) == "123"
    assert len(requests) == 3


def test_instagram_share_redirect():
    assert resolve_with_transport(
        "https://www.instagram.com/share/reel/ABC/",
        lambda request: httpx.Response(302, headers={"location": "/reel/REAL123/?foo=1"}),
    ) == "REAL123"


@pytest.mark.parametrize(("key", "allowed", "expected"), [
    (None, "bot", 401), ("wrong", "bot", 401), ("public-key", "bot", 403),
    ("bot-key", "", 403), ("bot-key", "bot", 200),
])
def test_endpoint_authentication_and_client_allowlist(tmp_path, monkeypatch, key, allowed, expected):
    folder = post(tmp_path, "ABC")
    monkeypatch.setenv("PINCHANA_API_KEYS", json.dumps({"bot": "bot-key", "public": "public-key"}))
    monkeypatch.setenv("PINCHANA_CACHE_ADMIN_CLIENTS", allowed)
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as client:
            return await client.post(
                "/v1/admin/cache/clear", json={"target": "ABC"},
                headers={"X-API-Key": key} if key else {},
            )
    with patch.object(main.storage, "base_path", tmp_path):
        response = asyncio.run(run())
    assert response.status_code == expected
    assert folder.exists() == (expected != 200)
    if expected == 200:
        assert response.json() == {"shortcode": "ABC", "deleted_posts": 1, "freed_bytes": 14}
    else:
        assert "error" in response.json()
