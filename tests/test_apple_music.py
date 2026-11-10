import unittest
from unittest.mock import patch

import httpx
from starlette.requests import Request

from pinchana_core.docker_manager import ContainerModule, ContainerRegistry
from pinchana_core.models import ScrapeRequest
from pinchana_server import main


class AppleMusicGatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_routes_to_apple_service_and_normalizes_audio(self):
        url = "https://music.apple.com/us/album/example/456?i=123"
        module = ContainerModule(name="apple_music", enabled=True, source_type="local",
                                 source_url="", route_patterns=["music.apple.com"],
                                 endpoint="http://apple_music:8091")
        def forward(request):
            self.assertEqual(str(request.url), "http://apple_music:8091/scrape")
            self.assertEqual(request.extensions["timeout"]["read"], 600)
            return httpx.Response(200, json={
                "shortcode": "am-123", "caption": "Example", "title": "Example",
                "author": "Artist", "media_type": "audio", "thumbnail_url": "",
                "audio_url": "/media/apple_music/am-123/audio.mp3", "duration": 180,
            })
        async with httpx.AsyncClient(transport=httpx.MockTransport(forward)) as client:
            with patch.object(main.container_registry, "modules", {"apple_music": module}), \
                    patch.object(main, "forward_client", client):
                result = await main._normalized_scrape_response(
                    ScrapeRequest(url=url),
                    Request({"type": "http", "method": "POST", "path": "/v1/scrape", "headers": []}),
                )
        self.assertEqual(result.data.source.platform, "apple_music")
        self.assertEqual(len(result.data.media), 1)
        self.assertEqual(result.data.media[0].type, "audio")
        self.assertEqual(result.data.media[0].artist, "Artist")

    def test_shipped_registry_includes_apple_music(self):
        from pathlib import Path
        config = Path(__file__).resolve().parents[2] / "config" / "modules.yaml"
        registry = ContainerRegistry(str(config))
        self.assertEqual(registry.modules["apple_music"].route_patterns, ["music.apple.com"])
