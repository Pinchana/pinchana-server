import asyncio
from unittest.mock import AsyncMock

import httpx
import pytest
from PIL import Image
from pinchana_core.storage import MediaStorage

from pinchana_bluesky import main as bluesky
from pinchana_server import main
from pinchana_server.media_probe import MediaDimensionProbe


URL = "https://bsky.app/profile/example.com/post/example"
URI = "at://did:plc:example/app.bsky.feed.post/example"
QUOTE_URI = "at://did:plc:quoted/app.bsky.feed.post/example"
PLAYLIST = "https://video.bsky.app/watch/example/playlist.m3u8"


def _thread(*, embed=None, author_labels=()):
    return {"thread": {"post": {
        "uri": URI,
        "author": {"handle": "example.com", "displayName": "Example", "labels": list(author_labels)},
        "record": {"$type": bluesky.COLLECTION, "text": "First line\n\nSecond paragraph"},
        "embed": embed,
    }}}


@pytest.fixture
def request_api(monkeypatch, tmp_path):
    monkeypatch.setenv("PINCHANA_API_KEYS", '{"bot":"fixture-key"}')
    monkeypatch.setenv("PINCHANA_TELEGRAM_CLIENTS", "bot")
    storage = MediaStorage(base_path=str(tmp_path))
    monkeypatch.setattr(main, "storage", storage)
    monkeypatch.setattr(bluesky, "storage", storage)
    monkeypatch.setattr(main, "dimension_probe", MediaDimensionProbe(tmp_path))

    async def request(method, path, *, upstream, **kwargs):
        async def appview_handler(req):
            assert str(req.url.copy_with(query=None)) == bluesky.APPVIEW
            assert req.url.params["depth"] == req.url.params["parentHeight"] == "0"
            return upstream

        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(appview_handler)) as appview,
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app), base_url="http://testserver",
                headers={"X-API-Key": "fixture-key"},
            ) as client,
        ):
            monkeypatch.setattr(bluesky, "client", appview)
            return await client.request(method, path, **kwargs)

    return storage, request


def test_scrape_stores_photos_and_quoted_video_in_telegram_scope(request_api, monkeypatch, tmp_path):
    storage, request = request_api
    images = {"$type": "app.bsky.embed.images#view", "images": [
        {"fullsize": "https://cdn.bsky.app/img/first"},
        {"fullsize": "https://cdn.bsky.app/img/second"},
    ]}
    quote = {"$type": "app.bsky.embed.record#view", "record": {
        "$type": "app.bsky.embed.record#viewRecord",
        "uri": QUOTE_URI,
        "author": {"handle": "quoted.example"},
        "value": {"$type": bluesky.COLLECTION, "text": "Quoted text"},
        "embeds": [{
            "$type": "app.bsky.embed.recordWithMedia#view",
            "media": {"$type": "app.bsky.embed.video#view", "playlist": PLAYLIST},
            "record": {"$type": "app.bsky.embed.record#view", "record": {"uri": URI}},
        }],
    }}
    upstream = httpx.Response(200, json=_thread(embed={
        "$type": "app.bsky.embed.recordWithMedia#view", "record": quote, "media": images,
    }))

    async def save_photo(source, path):
        assert source.endswith("@jpeg")
        Image.new("RGB", (40, 60)).save(path)
        return True

    async def save_video(source, path):
        assert source == PLAYLIST
        path.write_bytes(b"video fixture")

    monkeypatch.setattr(storage, "download", AsyncMock(side_effect=save_photo))
    monkeypatch.setattr(bluesky, "_remux_video", AsyncMock(side_effect=save_video))
    monkeypatch.setattr(main.dimension_probe, "_probe_video", lambda _: None)

    async def run():
        response = await request(
            "POST", "/v1/scrape", upstream=upstream, json={"url": URL},
            headers={"X-Pinchana-Delivery": "telegram"},
        )
        assert response.status_code == 200
        scope = response.headers["X-Pinchana-Cache-Scope"]
        data = response.json()["data"]
        assert data["source"]["platform"] == "bluesky"
        assert data["content"]["text"] == "First line\n\nSecond paragraph"
        assert [image["type"] for image in data["media"]] == ["image", "image"]
        assert data["media"][0]["dimensions"] == {"width": 40, "height": 60}
        assert data["quote"]["content"]["text"] == "Quoted text"
        assert data["quote"]["source"]["url"] == "https://bsky.app/profile/did:plc:quoted/post/example"
        assert "quote" not in data["quote"]
        assert data["id"] != data["quote"]["id"]

        video = data["quote"]["media"][0]
        assert video["type"] == "video"
        assert video["url"].startswith(f"/media/bluesky/.transient/{scope}/{data['id']}/")
        media_response = await request("GET", video["url"], upstream=upstream)
        assert media_response.content == b"video fixture"
        assert media_response.headers["content-type"] == "video/mp4"
        deletion = await request("DELETE", f"/v1/bot/cache/{scope}", upstream=upstream)
        assert deletion.status_code == 204
        assert not (tmp_path / ".transient" / scope).exists()

    asyncio.run(run())


@pytest.mark.parametrize(("upstream", "status", "code"), [
    (httpx.Response(400, json={"error": "NotFound"}), 404, "not_found"),
    (httpx.Response(429), 429, "rate_limited"),
    (
        httpx.Response(200, json=_thread(author_labels=[{"val": "!no-unauthenticated"}])),
        403, "restricted_media",
    ),
])
def test_bluesky_gateway_maps_upstream_errors(request_api, upstream, status, code):
    _, request = request_api
    response = asyncio.run(request("POST", "/v1/scrape", upstream=upstream, json={"url": URL}))
    assert response.status_code == status
    assert response.json()["error"]["code"] == code


def test_bluesky_inspect_accepts_did_without_downloading(request_api, monkeypatch):
    storage, request = request_api
    monkeypatch.setattr(storage, "download", AsyncMock())
    upstream = httpx.Response(200, json=_thread(embed={
        "$type": "app.bsky.embed.video#view", "playlist": PLAYLIST,
    }))
    response = asyncio.run(request("POST", "/v1/inspect", upstream=upstream, json={
        "url": "https://bsky.app/profile/did:plc:example/post/example?ref=share",
    }))
    assert response.status_code == 200
    assert response.json()["data"]["content"]["text"] == "First line\n\nSecond paragraph"
    storage.download.assert_not_awaited()


def test_bluesky_route_matches_the_host_and_needs_no_container():
    module = main._resolve_module(URL)
    assert module.name == "bluesky"
    assert module.routes["/scrape"].endpoint is bluesky.process_scrape_request
    assert main._resolve_module(f"https://example.org/?url={URL}") is None
