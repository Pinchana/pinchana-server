import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from pydantic import ValidationError

from pinchana_server.main import (
    GIF_MAX_INPUT_BYTES,
    GifConversionRequest,
    _acquire_gif_conversion,
    _release_gif_conversion,
    storage,
    web_convert_gif,
)


class GifConversionTests(unittest.TestCase):
    def test_request_rejects_traversal_and_non_cache_coordinates(self):
        for values in (
            {"platform": "twitter", "postId": "../secret", "filename": "video.mp4"},
            {"platform": "twitter", "postId": "post", "filename": "../video.mp4"},
            {"platform": "https://example.com", "postId": "post", "filename": "video.mp4"},
        ):
            with self.assertRaises(ValidationError):
                GifConversionRequest(**values)

    def test_conversion_uses_cached_file_and_cleans_temporary_output(self):
        async def run_test():
            with tempfile.TemporaryDirectory() as cache_directory:
                cache = Path(cache_directory)
                source_directory = cache / "post-1"
                source_directory.mkdir()
                (source_directory / "video.mp4").write_bytes(b"cached-video")

                async def write_gif(_source: Path, output: Path) -> None:
                    output.write_bytes(b"GIF89a")

                with (
                    patch.object(storage, "base_path", cache),
                    patch("pinchana_server.main._probe_media_duration", AsyncMock(return_value=3.5)),
                    patch("pinchana_server.main._convert_media_to_gif", side_effect=write_gif),
                ):
                    response = await web_convert_gif(
                        GifConversionRequest(platform="twitter", postId="post-1", filename="video.mp4"),
                        {"nonce": "test-session"},
                    )
                    output = Path(response.path)
                    self.assertEqual(response.media_type, "image/gif")
                    self.assertEqual(output.read_bytes(), b"GIF89a")
                    self.assertEqual(response.headers["cache-control"], "no-store")
                    self.assertIsNotNone(response.background)
                    response.background.func(
                        *response.background.args,
                        **response.background.kwargs,
                    )
                    self.assertFalse(output.exists())

        asyncio.run(run_test())

    def test_conversion_rejects_oversized_cached_input_before_ffmpeg(self):
        async def run_test():
            with tempfile.TemporaryDirectory() as cache_directory:
                cache = Path(cache_directory)
                source_directory = cache / "post-1"
                source_directory.mkdir()
                source = source_directory / "video.mp4"
                with source.open("wb") as oversized:
                    oversized.truncate(GIF_MAX_INPUT_BYTES + 1)
                with patch.object(storage, "base_path", cache):
                    with self.assertRaises(HTTPException) as raised:
                        await web_convert_gif(
                            GifConversionRequest(platform="twitter", postId="post-1", filename="video.mp4"),
                            {"nonce": "large-session"},
                        )
                self.assertEqual(raised.exception.status_code, 413)

        asyncio.run(run_test())

    def test_only_one_conversion_runs_per_session(self):
        async def run_test():
            owner = await _acquire_gif_conversion({"nonce": "same-session"})
            try:
                with self.assertRaises(HTTPException) as raised:
                    await _acquire_gif_conversion({"nonce": "same-session"})
                self.assertEqual(raised.exception.status_code, 429)
            finally:
                await _release_gif_conversion(owner)

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
