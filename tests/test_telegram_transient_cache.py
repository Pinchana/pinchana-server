import asyncio
import re
from unittest.mock import AsyncMock, patch

from fastapi import Response
from pinchana_core.models import ScrapeRequest
from pinchana_server.main import (
    _transient_media_urls,
    _remove_transient_scope,
    process_v1_scrape_request,
)


def test_transient_urls_rewrite_nested_media_but_not_text():
    payload = {
        "caption": "/media/twitter/123/should remain text only if part of a string",
        "video_url": "/media/twitter/123/video.mp4",
        "quote": {"carousel": [{"thumbnail_url": "/media/twitter/123/quote_0.jpg"}]},
    }
    rewritten = _transient_media_urls(payload, "a" * 32)
    assert rewritten["video_url"] == "/media/twitter/.transient/" + "a" * 32 + "/123/video.mp4"
    assert rewritten["quote"]["carousel"][0]["thumbnail_url"].endswith("/123/quote_0.jpg")
    assert rewritten["caption"] == payload["caption"]


def test_machine_bot_request_returns_scope_without_affecting_normal_clients():
    async def run(client, delivery):
        response = Response()
        with patch("pinchana_server.main._normalized_scrape_response", new_callable=AsyncMock) as scraper:
            scraper.return_value = {"data": {"id": "123"}}
            result = await process_v1_scrape_request(
                request=ScrapeRequest(url="https://x.com/person/status/123"),
                http_request=None,
                response=response,
                client_name=client,
                x_pinchana_delivery=delivery,
            )
            assert result == scraper.return_value
            return response.headers.get("X-Pinchana-Cache-Scope"), scraper.call_args.kwargs.get("cache_scope")

    bot_header, bot_scope = asyncio.run(run("bot", "telegram"))
    assert bot_header == bot_scope
    assert re.fullmatch(r"[a-f0-9]{32}", bot_scope)
    web_header, web_scope = asyncio.run(run("automation", "telegram"))
    assert web_header is None and web_scope is None


def test_only_exact_transient_scope_is_deleted(tmp_path):
    from pinchana_server import main as server

    target = tmp_path / ".transient" / ("c" * 32)
    target.mkdir(parents=True)
    (target / "media.mp4").write_bytes(b"temporary")
    persistent = tmp_path / "public-post"
    persistent.mkdir()
    (persistent / "video.mp4").write_bytes(b"persistent")

    with patch.object(server.storage, "_base_path", tmp_path):
        _remove_transient_scope("c" * 32)
    assert not target.exists()
    assert (persistent / "video.mp4").read_bytes() == b"persistent"
