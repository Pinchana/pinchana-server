import asyncio
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException
from PIL import Image
from starlette.requests import Request

from pinchana_server.main import (
    _http_error_code,
    _require_api_key,
    _require_web_session,
    api_http_exception_handler,
    app,
)
from pinchana_server.media_probe import MediaDimensionProbe
from pinchana_server.response_adapter import normalize_scrape_response


def _legacy_payload(**overrides):
    payload = {
        "shortcode": "POST123",
        "caption": "A caption",
        "author": "creator",
        "media_type": "image",
        "thumbnail_url": "/media/instagram/POST123/image.jpg",
        "video_url": None,
        "audio_url": None,
        "cover_url": None,
        "duration": None,
        "title": None,
        "album": None,
        "carousel": None,
        "tracklist": None,
    }
    payload.update(overrides)
    return payload


class V1ResponseAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_mixed_carousel_has_dimensions_order_and_named_metadata(self):
        from pinchana_server.schemas import MediaDimensions

        probe = AsyncMock(spec=MediaDimensionProbe)
        probe.dimensions_for.side_effect = [
            MediaDimensions(width=1200, height=800),
            MediaDimensions(width=1080, height=1920),
        ]
        response = await normalize_scrape_response(
            _legacy_payload(
                shortcode="tweet-1",
                author="creator",
                author_name="Creator Name",
                username="creator",
                thumbnail_url="/media/twitter/tweet-1/one.jpg",
                carousel=[
                    {
                        "index": 0,
                        "media_type": "image",
                        "thumbnail_url": "/media/twitter/tweet-1/one.jpg",
                        "video_url": None,
                    },
                    {
                        "index": 1,
                        "media_type": "video",
                        "thumbnail_url": "",
                        "video_url": "/media/twitter/tweet-1/two.mp4",
                        "looping": True,
                    },
                ],
                like_count=12,
                reply_count=3,
                repost_count=2,
                quote_count=1,
                view_count=99,
                nsfw=False,
                source="Twitter for iPhone",
                link="https://example.com/article",
            ),
            platform="twitter",
            source_url="https://x.com/creator/status/tweet-1",
            probe=probe,
        )

        data = response.data
        self.assertEqual(data.author.model_dump(), {"name": "Creator Name", "username": "creator"})
        self.assertEqual([item.type for item in data.media], ["image", "video"])
        self.assertEqual([item.index for item in data.media], [0, 1])
        self.assertFalse(data.media[0].looping)
        self.assertTrue(data.media[1].looping)
        self.assertEqual(data.media[0].dimensions.model_dump(), {"width": 1200, "height": 800})
        self.assertEqual(data.media[1].dimensions.model_dump(), {"width": 1080, "height": 1920})
        self.assertEqual(data.engagement.views, 99)
        self.assertFalse(data.safety.nsfw)
        self.assertEqual(data.source.application, "Twitter for iPhone")
        self.assertEqual(data.link.url, "https://example.com/article")

    async def test_slideshow_audio_and_music_cover_are_typed_without_duplicates(self):
        probe = AsyncMock(spec=MediaDimensionProbe)
        probe.dimensions_for.return_value = None
        slideshow = await normalize_scrape_response(
            _legacy_payload(
                shortcode="slides",
                thumbnail_url="/media/tiktok/slides/0.jpg",
                carousel=[
                    {"index": 0, "media_type": "image", "thumbnail_url": "/media/tiktok/slides/0.jpg"},
                    {"index": 1, "media_type": "image", "thumbnail_url": "/media/tiktok/slides/1.jpg"},
                ],
                audio_url="/media/tiktok/slides/audio.mp3",
            ),
            platform="tiktok",
            source_url="https://tiktok.com/photo/slides",
            probe=probe,
        )
        self.assertEqual(
            [(item.type, item.role) for item in slideshow.data.media],
            [("image", "content"), ("image", "content"), ("audio", "soundtrack")],
        )

        music = await normalize_scrape_response(
            _legacy_payload(
                shortcode="track",
                thumbnail_url="/media/spotify/track/cover.jpg",
                audio_url="/media/spotify/track/audio.mp3",
                cover_url="/media/spotify/track/cover.jpg",
                title="Track title",
                album="Album title",
                duration=180,
            ),
            platform="spotify",
            source_url="https://open.spotify.com/track/track",
            probe=probe,
        )
        self.assertEqual(
            [(item.type, item.role) for item in music.data.media],
            [("audio", "content"), ("image", "cover")],
        )
        self.assertEqual(music.data.media[0].duration_seconds, 180)
        self.assertEqual(music.data.music.album, "Album title")

    async def test_text_only_response_has_empty_media(self):
        probe = AsyncMock(spec=MediaDimensionProbe)
        response = await normalize_scrape_response(
            _legacy_payload(
                shortcode="thread",
                media_type="text",
                thumbnail_url="",
                text_html="<p>A caption</p>",
                spoiler=False,
                text_spoiler=True,
            ),
            platform="threads",
            source_url="https://threads.com/@creator/post/thread",
            probe=probe,
        )
        self.assertEqual(response.data.media, [])
        self.assertEqual(response.data.content.html, "<p>A caption</p>")
        self.assertTrue(response.data.safety.text_spoiler)
        probe.dimensions_for.assert_not_awaited()

    async def test_threads_music_and_timestamp_are_normalized(self):
        probe = AsyncMock(spec=MediaDimensionProbe)
        probe.dimensions_for.return_value = None
        response = await normalize_scrape_response(
            _legacy_payload(
                shortcode="thread-music",
                thumbnail_url="/media/threads/thread-music/image.jpg",
                taken_at=1784074282,
                music={
                    "audio_url": "/media/threads/thread-music/music_preview.m4a",
                    "cover_url": "/media/threads/thread-music/music_cover.jpg",
                    "title": "Kalinka",
                    "artist": "Russian Balalaika Orchestra",
                    "duration_seconds": 30,
                },
            ),
            platform="threads",
            source_url="https://threads.com/@creator/post/thread-music",
            probe=probe,
        )

        assert [(item.type, item.role) for item in response.data.media] == [
            ("image", "content"),
            ("audio", "soundtrack"),
            ("image", "cover"),
        ]
        assert response.data.media[1].title == "Kalinka"
        assert response.data.media[1].artist == "Russian Balalaika Orchestra"
        assert response.data.media[1].duration_seconds == 30
        assert response.data.content.published_at.isoformat() == "2026-07-15T00:11:22+00:00"

    async def test_tracklist_becomes_ordered_audio_items(self):
        probe = AsyncMock(spec=MediaDimensionProbe)
        probe.dimensions_for.return_value = None
        response = await normalize_scrape_response(
            _legacy_payload(
                shortcode="album",
                thumbnail_url="/media/deezer/album/cover.jpg",
                cover_url="/media/deezer/album/cover.jpg",
                album="Album",
                tracklist=[
                    {"index": 0, "title": "One", "artist": "Artist A", "audio_url": "/media/deezer/album/one.mp3"},
                    {"index": 1, "title": "Two", "artist": "Artist B", "audio_url": "/media/deezer/album/two.mp3"},
                ],
            ),
            platform="deezer",
            source_url="https://deezer.com/album/album",
            probe=probe,
        )
        self.assertEqual([item.index for item in response.data.media], [0, 1, 2])
        self.assertEqual([item.role for item in response.data.media], ["content", "content", "cover"])
        self.assertEqual(response.data.media[0].title, "One")
        self.assertEqual(response.data.media[1].artist, "Artist B")


class MediaDimensionProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_image_probe_uses_actual_file_and_caches_result(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            post = base / "post"
            post.mkdir()
            Image.new("RGB", (640, 360)).save(post / "image.png")
            probe = MediaDimensionProbe(base)
            first = await probe.dimensions_for("/media/test/post/image.png", "image")
            with patch.object(probe, "_probe_image", wraps=probe._probe_image) as image_probe:
                second = await probe.dimensions_for("/media/test/post/image.png", "image")
            self.assertEqual(first.model_dump(), {"width": 640, "height": 360})
            self.assertEqual(second, first)
            image_probe.assert_not_called()

    async def test_video_probe_applies_rotation(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            post = base / "post"
            post.mkdir()
            video = post / "video.mp4"
            video.write_bytes(b"placeholder")
            process = subprocess.CompletedProcess(
                args=["ffprobe"],
                returncode=0,
                stdout=json.dumps({
                    "streams": [{"width": 1920, "height": 1080, "side_data_list": [{"rotation": -90}]}]
                }).encode(),
                stderr=b"",
            )
            with patch("subprocess.run", return_value=process):
                dimensions = await MediaDimensionProbe(base).dimensions_for(
                    "/media/test/post/video.mp4", "video"
                )
            self.assertEqual(dimensions.model_dump(), {"width": 1080, "height": 1920})

    async def test_external_and_missing_media_return_null(self):
        probe = MediaDimensionProbe("/tmp/does-not-exist")
        self.assertIsNone(await probe.dimensions_for("https://example.com/image.jpg", "image"))
        self.assertIsNone(await probe.dimensions_for("/media/test/missing/image.jpg", "image"))
        self.assertIsNone(await probe.dimensions_for("/media/test/missing/audio.mp3", "audio"))


class V1EndpointTests(unittest.IsolatedAsyncioTestCase):
    async def _request(self, path, *, payload, headers=None):
        async def authenticated_client():
            return "test"

        app.dependency_overrides[_require_api_key] = authenticated_client
        try:
            transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                return await client.post(path, json=payload, headers=headers)
        finally:
            app.dependency_overrides.pop(_require_api_key, None)

    async def _web_request(self, *, payload, authenticated=True, headers=None):
        async def authenticated_session():
            return {"nonce": "test-browser-session"}

        if authenticated:
            app.dependency_overrides[_require_web_session] = authenticated_session
        try:
            transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                return await client.post("/v1/web/scrape", json=payload, headers=headers)
        finally:
            app.dependency_overrides.pop(_require_web_session, None)

    async def _mobile_request(self, *, payload, headers=None):
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.post("/v1/mobile/scrape", json=payload, headers=headers)

    async def test_v1_success_and_legacy_contract_coexist(self):
        raw = _legacy_payload()
        environment = {"PINCHANA_API_KEYS": '{"test":"secret"}'}
        with patch.dict(os.environ, environment, clear=False), patch(
            "pinchana_server.main._process_scrape_payload",
            AsyncMock(return_value=("instagram", raw)),
        ):
            v1 = await self._request(
                "/v1/scrape",
                payload={"url": "https://www.instagram.com/p/POST123/"},
                headers={"X-API-Key": "secret"},
            )
            legacy = await self._request(
                "/scrape",
                payload={"url": "https://www.instagram.com/p/POST123/"},
                headers={"X-API-Key": "secret"},
            )
        self.assertEqual(v1.status_code, 200)
        self.assertEqual(v1.json()["meta"], {"api_version": "1"})
        self.assertEqual(v1.json()["data"]["source"]["platform"], "instagram")
        self.assertEqual(legacy.status_code, 200)
        self.assertEqual(legacy.json(), raw)

    async def test_v1_web_uses_session_auth_and_web_media_namespace(self):
        raw = _legacy_payload()
        with patch(
            "pinchana_server.main._process_scrape_payload",
            AsyncMock(return_value=("instagram", raw)),
        ):
            response = await self._web_request(
                payload={"url": "https://www.instagram.com/p/POST123/"},
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["meta"], {"api_version": "1"})
        self.assertEqual(
            body["data"]["media"][0]["url"],
            "/web/media/instagram/POST123/image.jpg",
        )

    async def test_v1_web_rejects_machine_key_without_web_session(self):
        response = await self._web_request(
            authenticated=False,
            payload={"url": "https://www.instagram.com/p/POST123/"},
            headers={"X-API-Key": "machine-key"},
        )
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error"]["code"], "unauthorized")
        self.assertEqual(response.json()["error"]["message"], "Invalid or missing web session")

    async def test_v1_mobile_authentication_is_optional_but_invalid_tokens_are_rejected(self):
        raw = _legacy_payload()
        with patch.dict(os.environ, {"MOBILE_AUTH_REQUIRED": "false"}, clear=False), patch(
            "pinchana_server.main._process_scrape_payload",
            AsyncMock(return_value=("instagram", raw)),
        ):
            anonymous = await self._mobile_request(
                payload={"url": "https://www.instagram.com/p/POST123/"},
            )
            invalid = await self._mobile_request(
                payload={"url": "https://www.instagram.com/p/POST123/"},
                headers={"Authorization": "Bearer invalid"},
            )

        self.assertEqual(anonymous.status_code, 200)
        self.assertEqual(
            anonymous.json()["data"]["media"][0]["url"],
            "/mobile/media/instagram/POST123/image.jpg",
        )
        self.assertEqual(invalid.status_code, 401)
        self.assertEqual(invalid.json()["error"]["code"], "unauthorized")

    async def test_v1_mobile_authentication_can_be_required_by_configuration(self):
        with patch.dict(os.environ, {"MOBILE_AUTH_REQUIRED": "true"}, clear=False):
            response = await self._mobile_request(
                payload={"url": "https://www.instagram.com/p/POST123/"},
            )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error"]["message"], "Invalid or missing mobile session")

    async def test_v1_auth_and_validation_use_error_envelopes(self):
        request = Request({"type": "http", "method": "POST", "path": "/v1/scrape", "headers": []})
        unauthorized = await api_http_exception_handler(
            request,
            HTTPException(status_code=401, detail="Invalid or missing API key"),
        )
        invalid = await self._request(
            "/v1/scrape",
            payload={"url": "not-a-url"},
            headers={"X-API-Key": "secret"},
        )
        self.assertEqual(unauthorized.status_code, 401)
        self.assertEqual(json.loads(unauthorized.body)["error"]["code"], "unauthorized")
        self.assertEqual(invalid.status_code, 422)
        self.assertEqual(invalid.json()["error"]["code"], "validation_error")
        self.assertIsInstance(invalid.json()["error"]["details"], list)

    def test_structured_upstream_errors_keep_stable_codes_and_safe_messages(self):
        tiktok = _http_error_code(
            403,
            '{"detail":{"code":"authentication_required","message":"This TikTok post requires login or audience confirmation"}}',
        )
        instagram = _http_error_code(
            403,
            {"code": "restricted_media", "message": "This Instagram post is not accessible anonymously"},
        )
        extraction = _http_error_code(
            502,
            {"detail": {"code": "extraction_failed", "message": "TikTok extraction failed"}},
        )

        self.assertEqual(
            tiktok,
            (
                "authentication_required",
                "This TikTok post requires login or audience confirmation",
            ),
        )
        self.assertEqual(
            instagram,
            ("restricted_media", "This Instagram post is not accessible anonymously"),
        )
        self.assertEqual(extraction, ("extraction_failed", "TikTok extraction failed"))

    async def test_unsupported_url_error_does_not_expose_route_configuration(self):
        request = Request({"type": "http", "method": "POST", "path": "/v1/scrape", "headers": []})
        response = await api_http_exception_handler(
            request,
            HTTPException(
                status_code=400,
                detail="No module handles this URL. Plugins: {'secret': ['internal']} Containers: {}",
            ),
        )
        body = json.loads(response.body)
        self.assertEqual(body["error"]["code"], "unsupported_url")
        self.assertEqual(body["error"]["message"], "No scraper supports this URL")
        self.assertNotIn("internal", response.body.decode())

    async def test_invalid_upstream_payload_returns_stable_502(self):
        with patch(
            "pinchana_server.main._process_scrape_payload",
            AsyncMock(return_value=("instagram", {"caption": "missing id"})),
        ):
            response = await self._request(
                "/v1/scrape",
                payload={"url": "https://www.instagram.com/p/POST123/"},
                headers={"X-API-Key": "secret"},
            )
        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["error"]["code"], "invalid_upstream_response")

    async def test_legacy_http_errors_keep_fastapi_detail_shape(self):
        request = Request({"type": "http", "method": "POST", "path": "/scrape", "headers": []})
        response = await api_http_exception_handler(
            request,
            HTTPException(status_code=400, detail="Legacy detail"),
        )
        self.assertEqual(json.loads(response.body), {"detail": "Legacy detail"})

    def test_openapi_publishes_v1_success_and_error_schemas(self):
        for path in ("/v1/scrape", "/v1/web/scrape"):
            operation = app.openapi()["paths"][path]["post"]
            self.assertEqual(
                operation["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/ScrapeV1Response",
            )
            self.assertEqual(
                operation["responses"]["422"]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/ApiErrorResponse",
            )


if __name__ == "__main__":
    unittest.main()
