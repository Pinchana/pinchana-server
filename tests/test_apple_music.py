import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from starlette.requests import Request

from pinchana_core.models import ScrapeRequest, ScrapeResponse
from pinchana_server import main


class AppleMusicGatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_calls_bundled_handler_and_normalizes_audio(self):
        url = "https://music.apple.com/us/album/example/456?i=123"
        scrape = AsyncMock(return_value=ScrapeResponse(
            shortcode="am-123", caption="Example", title="Example",
            author="Artist", media_type="audio", thumbnail_url="/media/apple_music/am-123/cover.jpg",
            cover_url="/media/apple_music/am-123/cover.jpg",
            audio_url="/media/apple_music/am-123/audio.mp3", duration=180,
        ))
        module = main.modules["apple_music"]
        route = SimpleNamespace(methods={"POST"}, endpoint=scrape)
        with patch.dict(main.modules, {"apple_music": replace(module, routes={"/scrape": route})}):
            result = await main._normalized_scrape_response(
                ScrapeRequest(url=url),
                Request({"type": "http", "method": "POST", "path": "/v1/scrape", "headers": []}),
            )
        self.assertEqual(str(scrape.await_args.args[0].url), url)
        self.assertEqual(result.data.source.platform, "apple_music")
        self.assertEqual(len(result.data.media), 2)
        self.assertEqual(result.data.media[0].type, "audio")
        self.assertEqual(result.data.media[0].artist, "Artist")
        cover = next(item for item in result.data.media if item.role == "cover")
        self.assertEqual(cover.type, "image")
