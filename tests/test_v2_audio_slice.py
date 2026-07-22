import json
import time
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from fastapi.testclient import TestClient

import pinchana_server.main as server


@pytest.fixture
def client():
    with TestClient(server.app) as test_client:
        yield test_client


@pytest.fixture
def web_headers(monkeypatch):
    monkeypatch.setenv(
        "TURNSTILE_SESSION_SECRET", "phase-4b-test-session-secret-32-bytes"
    )
    token, _expiry = server._issue_web_session()
    return {"Authorization": f"Bearer {token}"}


def response(status, payload):
    result = mock.MagicMock()
    result.status_code = status
    result.json.return_value = payload
    return result


def module_result(client, headers, monkeypatch, *, platform, url, payload):
    monkeypatch.setenv(f"PINCHANA_V2_{platform.upper()}", "true")
    target = SimpleNamespace(endpoint=f"http://{platform}.internal")
    with mock.patch.object(server, "_resolve_module", return_value=("container", platform, target)), \
         mock.patch("httpx.AsyncClient.get", new_callable=mock.AsyncMock, return_value=response(200, {"supports_v2_remote": True})), \
         mock.patch("httpx.AsyncClient.post", new_callable=mock.AsyncMock, return_value=response(200, payload)):
        return client.post("/v2/scrape", json={"url": url}, headers=headers)


@pytest.mark.parametrize(
    ("platform", "url", "flag"),
    [
        ("soundcloud", "https://soundcloud.com/a/b", "PINCHANA_V2_SOUNDCLOUD"),
        ("spotify", "https://open.spotify.com/track/abc", "PINCHANA_V2_SPOTIFY"),
        ("deezer", "https://www.deezer.com/track/123", "PINCHANA_V2_DEEZER"),
        ("ytmusic", "https://music.youtube.com/watch?v=abcdefghijk", "PINCHANA_V2_YTMUSIC"),
    ],
)
def test_audio_flags_are_disabled_by_default(client, web_headers, monkeypatch, platform, url, flag):
    monkeypatch.delenv(flag, raising=False)
    target = SimpleNamespace(endpoint=f"http://{platform}.internal")
    with mock.patch.object(server, "_resolve_module", return_value=("container", platform, target)), \
         mock.patch("httpx.AsyncClient.get") as capability:
        result = client.post("/v2/scrape", json={"url": url}, headers=web_headers)
    assert result.status_code == 409
    assert result.json()["detail"]["code"] == "v2_disabled"
    capability.assert_not_called()


def test_preview_and_artwork_have_truthful_public_semantics(client, web_headers, monkeypatch):
    payload = {
        "shortcode": "sp-track-abc",
        "caption": "Track",
        "author": "Artist",
        "media_type": "audio",
        "platform_id": "abc",
        "availability": "preview",
        "classifications": ["preview_audio"],
        "assets": [
            {
                "index": 0,
                "media_type": "audio",
                "role": "preview",
                "availability": "preview",
                "filename": "Track.mp3",
                "mime_type": "audio/mpeg",
                "upstream_url": "https://1.1.1.1/preview.mp3?token=hidden",
                "supports_range": True,
                "asset_id": "spotify:abc:preview",
                "source_fingerprint": "preview-fingerprint",
            },
            {
                "index": 1,
                "media_type": "image",
                "role": "artwork",
                "filename": "Track.jpg",
                "upstream_url": "https://1.1.1.1/art.jpg?signature=hidden",
                "asset_id": "spotify:abc:artwork",
                "source_fingerprint": "art-fingerprint",
            },
        ],
    }
    result = module_result(
        client, web_headers, monkeypatch,
        platform="spotify", url="https://open.spotify.com/track/abc", payload=payload,
    )
    assert result.status_code == 200
    body = result.json()
    assert body["content"]["availability"] == "preview"
    assert body["assets"][0]["availability"] == "preview"
    assert body["assets"][0]["role"] == "preview"
    assert body["assets"][1]["role"] == "artwork"
    serialized = json.dumps(body)
    assert "token=hidden" not in serialized
    assert "signature=hidden" not in serialized
    assert "upstream_url" not in serialized


def test_metadata_only_collection_is_ready_without_eager_tickets(client, web_headers, monkeypatch):
    before = len(server.ticket_store._tickets)
    payload = {
        "shortcode": "dz-album-42",
        "caption": "Album",
        "media_type": "collection",
        "availability": "metadata-only",
        "classifications": ["collection"],
        "assets": [],
        "collection": [
            {
                "index": 0,
                "item_id": "1",
                "title": "One",
                "availability": "preview",
                "assets": [{
                    "index": 0,
                    "media_type": "audio",
                    "role": "preview",
                    "availability": "preview",
                    "filename": "one.mp3",
                    "upstream_url": "https://1.1.1.1/one.mp3?token=hidden",
                }],
            },
            {
                "index": 1,
                "item_id": "2",
                "title": "Two",
                "availability": "metadata-only",
                "classifications": ["metadata_only"],
            },
        ],
    }
    result = module_result(
        client, web_headers, monkeypatch,
        platform="deezer", url="https://www.deezer.com/album/42", payload=payload,
    )
    body = result.json()
    assert result.status_code == 200
    assert body["assets"] == []
    assert [item["item_id"] for item in body["collection"]] == ["1", "2"]
    assert len(server.ticket_store._tickets) == before
    assert "token=hidden" not in json.dumps(body)


def test_youtube_music_delegates_to_existing_dlp_job(client, web_headers, monkeypatch):
    monkeypatch.setenv("PINCHANA_V2_YTMUSIC", "true")
    monkeypatch.setenv("DLP_ENABLED", "true")
    target = SimpleNamespace(endpoint="http://ytmusic.internal")
    extracted = {
        "shortcode": "ytm-abcdefghijk",
        "caption": "Track",
        "author": "Artist",
        "media_type": "audio",
        "availability": "full",
        "classifications": ["full_audio", "processing_job"],
        "processing": {
            "kind": "dlp",
            "source_url": "https://www.youtube.com/watch?v=abcdefghijk",
            "options": {
                "quality": "audio",
                "audioFormat": "mp3",
                "asset_id": "ytmusic:abcdefghijk:full",
                "source_fingerprint": "ytm-fingerprint",
                "filename": "Track.mp3",
            },
        },
    }

    async def dlp_json(method, path, _claims, body=None):
        if method == "POST" and path == "/v2/jobs":
            return {"jobId": "12345678-1234-4234-9234-123456789abc"}
        if method == "POST" and path.endswith("/submit"):
            assert body["url"] == "https://www.youtube.com/watch?v=abcdefghijk"
            assert body["quality"] == "audio"
            assert "asset_id" not in body
            return {"status": "QUEUED"}
        return {
            "status": "READY",
            "expiresAt": int(time.time()) + 600,
            "size": 1234,
            "mime": "audio/mpeg",
        }

    with mock.patch.object(server, "_resolve_module", return_value=("container", "ytmusic", target)), \
         mock.patch("httpx.AsyncClient.get", new_callable=mock.AsyncMock, return_value=response(200, {"supports_v2_remote": True})), \
         mock.patch("httpx.AsyncClient.post", new_callable=mock.AsyncMock, return_value=response(200, extracted)), \
         mock.patch.object(server, "_dlp_capabilities", new_callable=mock.AsyncMock, return_value={"services": ["youtube"]}), \
         mock.patch.object(server, "_dlp_json", side_effect=dlp_json):
        started = client.post(
            "/v2/scrape",
            json={"url": "https://music.youtube.com/watch?v=abcdefghijk"},
            headers=web_headers,
        )
        assert started.status_code == 200
        assert started.json()["status"] == "processing"
        assert "youtube.com" not in json.dumps(started.json())
        ready = client.get(started.json()["status_url"], headers=web_headers)

    assert ready.status_code == 200
    body = ready.json()
    assert body["status"] == "ready"
    assert body["assets"][0]["availability"] == "full"
    assert body["assets"][0]["delivery"]["url"].startswith("/v2/assets/")
    assert "credential_ref" not in json.dumps(body)


def test_large_known_audio_uses_spool_policy(monkeypatch):
    monkeypatch.setenv("PINCHANA_V2_MAX_DIRECT_AUDIO_BYTES", "1048576")
    descriptor = server.RemoteAssetDescriptor(
        index=0,
        media_type="audio",
        role="content",
        availability="full",
        filename="large.mp3",
        upstream_url="https://1.1.1.1/large.mp3",
        size=1048577,
        supports_range=True,
    )
    assert server._descriptor_can_tunnel(descriptor) is False


def test_audio_operational_limits_are_bounded(monkeypatch):
    monkeypatch.setenv("PINCHANA_V2_MAX_COLLECTION_ITEMS", "99999")
    monkeypatch.setenv("PINCHANA_V2_MAX_INITIAL_TICKETS", "0")
    monkeypatch.setenv("PINCHANA_V2_MAX_ARCHIVE_ITEMS", "not-a-number")
    assert server._max_collection_items() == 500
    assert server._max_initial_tickets() == 1
    assert server._max_archive_items() == 32


@pytest.mark.asyncio
async def test_telegram_metadata_collection_has_no_shared_cache_asset(monkeypatch, tmp_path):
    extracted = server.ScrapeV2ExtractedData.model_validate({
        "shortcode": "spotify-playlist-abc",
        "caption": "Playlist",
        "media_type": "collection",
        "availability": "metadata-only",
        "classifications": ["collection"],
        "assets": [],
        "collection": [{
            "index": 0,
            "item_id": "track-1",
            "title": "Track one",
            "availability": "preview",
            "assets": [],
        }],
    })
    monkeypatch.setattr(server, "storage", SimpleNamespace(base_path=tmp_path))
    monkeypatch.setattr(
        server,
        "_extract_v2_for_telegram",
        mock.AsyncMock(return_value=extracted),
    )

    result = await server._process_v2_telegram_audio(
        server.ScrapeRequest(url="https://open.spotify.com/playlist/abc"),
        platform="spotify",
        mode="container",
        target=SimpleNamespace(endpoint="http://spotify.internal"),
        client_name="bot",
    )

    assert result.assets == []
    assert result.collection[0]["item_id"] == "track-1"
    assert not any(tmp_path.iterdir())


class AudioResponse:
    status_code = 200
    headers = {"content-type": "audio/mpeg"}

    def __init__(self, body: bytes = b"audio"):
        self.body = body
        self.closed = False

    async def aiter_raw(self):
        yield self.body

    async def aclose(self):
        self.closed = True


@pytest.mark.asyncio
async def test_telegram_preview_cache_identity_includes_availability(monkeypatch, tmp_path):
    extracted = server.ScrapeV2ExtractedData.model_validate({
        "shortcode": "spotify-track-abc",
        "caption": "Track",
        "author": "Artist",
        "media_type": "audio",
        "availability": "preview",
        "classifications": ["preview_audio"],
        "assets": [{
            "index": 0,
            "media_type": "audio",
            "role": "preview",
            "availability": "preview",
            "filename": "Track.mp3",
            "upstream_url": "https://cdn.example.test/preview.mp3",
            "asset_id": "spotify:abc:preview",
            "source_fingerprint": "preview-fingerprint",
        }],
    })
    response = AudioResponse()
    monkeypatch.setattr(server, "storage", SimpleNamespace(base_path=tmp_path))
    monkeypatch.setattr(
        server,
        "_extract_v2_for_telegram",
        mock.AsyncMock(return_value=extracted),
    )
    monkeypatch.setattr(
        server,
        "_open_telegram_descriptor",
        mock.AsyncMock(return_value=(response, None)),
    )

    result = await server._process_v2_telegram_audio(
        server.ScrapeRequest(url="https://open.spotify.com/track/abc"),
        platform="spotify",
        mode="container",
        target=SimpleNamespace(endpoint="http://spotify.internal"),
        client_name="bot",
    )

    asset = result.assets[0]
    assert asset.availability == "preview"
    assert ":preview:" in asset.delivery.cache_key
    assert asset.delivery.source_fingerprint == "preview-fingerprint"
    assert (tmp_path / Path(asset.delivery.relative_variant_path)).read_bytes() == b"audio"
    assert response.closed is True


@pytest.mark.asyncio
async def test_telegram_youtube_music_uses_dlp_output(monkeypatch, tmp_path):
    extracted = server.ScrapeV2ExtractedData.model_validate({
        "shortcode": "ytm-abcdefghijk",
        "caption": "Track",
        "media_type": "audio",
        "availability": "full",
        "classifications": ["full_audio", "processing_job"],
        "processing": {
            "kind": "dlp",
            "source_url": "https://www.youtube.com/watch?v=abcdefghijk",
            "options": {"audioFormat": "mp3"},
        },
    })
    descriptor = server.RemoteAssetDescriptor(
        index=0,
        media_type="audio",
        role="content",
        availability="full",
        filename="Track.mp3",
        credential_ref="dlp.12345678-1234-4234-9234-123456789abc",
        asset_id="ytmusic:abcdefghijk:full",
        source_fingerprint="ytm-fingerprint",
    )
    monkeypatch.setattr(server, "storage", SimpleNamespace(base_path=tmp_path))
    monkeypatch.setattr(
        server,
        "_extract_v2_for_telegram",
        mock.AsyncMock(return_value=extracted),
    )
    dlp = mock.AsyncMock(return_value=(descriptor, AudioResponse(b"processed"), None))
    monkeypatch.setattr(server, "_telegram_dlp_descriptor", dlp)

    result = await server._process_v2_telegram_audio(
        server.ScrapeRequest(url="https://music.youtube.com/watch?v=abcdefghijk"),
        platform="ytmusic",
        mode="container",
        target=SimpleNamespace(endpoint="http://ytmusic.internal"),
        client_name="bot",
    )

    dlp.assert_awaited_once()
    assert result.assets[0].availability == "full"
    assert result.assets[0].delivery.source_fingerprint == "ytm-fingerprint"
