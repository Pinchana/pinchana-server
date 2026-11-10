import asyncio
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from pinchana_server.gif_conversion import (
    GIF_MAX_OUTPUT_BYTES,
    GIF_RAW_OUTPUT_BYTES,
    GIF_TARGET_OUTPUT_BYTES,
    convert_media_to_gif,
    profiles_for_duration,
    video_filter,
)
from pinchana_server.main import _probe_media_duration, _run_media_process


class GifEncoderTests(unittest.IsolatedAsyncioTestCase):
    async def encode(self, sizes, *, optimized_sizes=None, actual_duration=3.5, fail_stage=None):
        self.calls = []
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        source = root / "source.mp4"
        source.write_bytes(b"cached-video")
        output = root / "output.gif"
        profile = -1

        async def process(*args, timeout):
            nonlocal profile
            self.calls.append((args, timeout))
            if args[0] == "ffprobe":
                return 0, str(actual_duration).encode(), b""
            if "-vf" in args:
                profile += 1
                if fail_stage == "retry_timeout" and profile == 1:
                    raise HTTPException(504, "timed out")
                if fail_stage == "palette":
                    return 1, b"", b"decode failed"
                Path(args[-1]).write_bytes(b"palette")
            else:
                size = sizes[min(profile, len(sizes) - 1)]
                if args[0].endswith("gifsicle"):
                    if fail_stage == "optimizer":
                        raise HTTPException(504, "optimizer timed out")
                    size = optimized_sizes[min(profile, len(optimized_sizes) - 1)]
                with Path(args[-1]).open("wb") as file:
                    file.write(b"GIF89a")
                    file.truncate(size)
            return 0, b"", b""

        with patch("pinchana_server.gif_conversion.shutil.which", return_value="/usr/bin/gifsicle" if optimized_sizes else None):
            await convert_media_to_gif(source, output, duration=3.5, run_process=process)
        self.assertEqual(sorted(p.name for p in root.iterdir()), ["output.gif", "source.mp4"])
        return output

    async def test_uses_separate_full_palette_and_explicit_delta_encoding(self):
        output = await self.encode([100_000])
        self.assertEqual(output.stat().st_size, 100_000)
        palette, encode, probe = [call[0] for call in self.calls]
        self.assertIn("palettegen=max_colors=256:stats_mode=full:reserve_transparent=1", palette[palette.index("-vf") + 1])
        self.assertNotIn("split", palette[palette.index("-vf") + 1])
        self.assertIn("palette-0.png", " ".join(encode))
        self.assertIn("+offsetting+transdiff", encode)
        self.assertIn("diff_mode=rectangle", encode[encode.index("-filter_complex") + 1])
        self.assertEqual(probe[0], "ffprobe")
        self.assertTrue(all(0 < timeout <= 45 for _, timeout in self.calls))

    async def test_retries_smaller_profile_until_target_fits(self):
        await self.encode([GIF_TARGET_OUTPUT_BYTES + 100, 100_000])
        encodes = [args for args, _ in self.calls if "-filter_complex" in args]
        self.assertEqual(len(encodes), 2)
        self.assertIn("fps=12", encodes[1][encodes[1].index("-filter_complex") + 1])
        self.assertIn("512", encodes[1][encodes[1].index("-filter_complex") + 1])

    async def test_lossless_optimization_is_used_only_when_smaller(self):
        output = await self.encode([200_000], optimized_sizes=[100_000])
        self.assertEqual(output.stat().st_size, 100_000)
        optimizer = next(args for args, _ in self.calls if args[0].endswith("gifsicle"))
        self.assertIn("--optimize=3", optimizer)
        self.assertIn("--conserve-memory", optimizer)
        self.assertNotIn("--lossy", " ".join(optimizer))
        output = await self.encode([100_000], optimized_sizes=[200_000])
        self.assertEqual(output.stat().st_size, 100_000)

    async def test_optimizer_timeout_preserves_complete_raw_gif(self):
        output = await self.encode([100_000], optimized_sizes=[10], fail_stage="optimizer")
        self.assertEqual(output.stat().st_size, 100_000)

    async def test_raw_size_guard_never_delivers_or_optimizes_a_truncated_file(self):
        await self.encode([GIF_RAW_OUTPUT_BYTES + 10, 100_000], optimized_sizes=[10, 90_000])
        optimizer_inputs = [" ".join(args) for args, _ in self.calls if args[0].endswith("gifsicle")]
        self.assertEqual(len(optimizer_inputs), 1)
        self.assertNotIn("raw-0.gif", optimizer_inputs[0])

    async def test_all_oversized_profiles_are_rejected_and_cleaned(self):
        with self.assertRaises(HTTPException) as raised:
            await self.encode([GIF_MAX_OUTPUT_BYTES + 1])
        self.assertEqual(raised.exception.status_code, 413)
        self.assertEqual([p.name for p in Path(self.directory.name).iterdir()], ["source.mp4"])

    async def test_shortened_timeline_is_never_delivered(self):
        with self.assertRaises(HTTPException) as raised:
            await self.encode([100_000], actual_duration=1.0)
        self.assertEqual(raised.exception.status_code, 422)
        self.assertEqual([p.name for p in Path(self.directory.name).iterdir()], ["source.mp4"])

    async def test_timeout_can_return_a_previously_validated_budget_fitting_candidate(self):
        size = GIF_TARGET_OUTPUT_BYTES + 100
        output = await self.encode([size], fail_stage="retry_timeout")
        self.assertEqual(output.stat().st_size, size)

    async def test_decode_failure_cleans_palette_and_candidates(self):
        with self.assertRaises(HTTPException) as raised:
            await self.encode([100_000], fail_stage="palette")
        self.assertEqual(raised.exception.status_code, 422)
        self.assertEqual([p.name for p in Path(self.directory.name).iterdir()], ["source.mp4"])

    async def test_probe_uses_video_duration_instead_of_a_longer_audio_track(self):
        for stream, container, expected in [(3.5, 30, 3.5), ("N/A", 4, 4)]:
            payload = json.dumps({"streams": [{"duration": stream}], "format": {"duration": container}}).encode()
            with patch("pinchana_server.main._run_media_process", AsyncMock(return_value=(0, payload, b""))):
                self.assertEqual(await _probe_media_duration(Path("video.mp4")), expected)

    async def test_probe_rejects_audio_only_non_finite_and_overlong_sources(self):
        for payload in [
            {"streams": [], "format": {"duration": 3}},
            {"streams": [{"duration": "nan"}], "format": {"duration": "inf"}},
            {"streams": [{"duration": 61}], "format": {"duration": 61}},
        ]:
            with patch("pinchana_server.main._run_media_process", AsyncMock(return_value=(0, json.dumps(payload).encode(), b""))):
                with self.assertRaises(HTTPException) as raised:
                    await _probe_media_duration(Path("video.mp4"))
                self.assertEqual(raised.exception.status_code, 422)

    def test_long_sources_start_with_a_smaller_profile(self):
        self.assertEqual(profiles_for_duration(60)[0].edge, 480)
        self.assertEqual(profiles_for_duration(3)[0].colors, 256)
        self.assertIn("setsar=1", video_filter(profiles_for_duration(3)[0]))


@unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "native FFmpeg tools unavailable")
class NativeGifEncoderTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_landscape_portrait_small_and_non_square_pixel_sources(self):
        cases = [(640, 360, 1, 640, 360), (720, 1280, 1, 360, 640), (80, 64, 1, 80, 64), (80, 64, 2, 160, 64)]
        for width, height, sar, expected_width, expected_height in cases:
            with self.subTest(size=(width, height), sar=sar), tempfile.TemporaryDirectory() as directory:
                source, output = Path(directory) / "source.mp4", Path(directory) / "output.gif"
                subprocess.run([
                    "ffmpeg", "-nostdin", "-y", "-v", "error", "-f", "lavfi",
                    "-i", f"testsrc2=size={width}x{height}:rate=20:duration=0.6",
                    "-vf", f"setsar={sar}", "-c:v", "mpeg4", "-q:v", "2", str(source),
                ], check=True, capture_output=True, timeout=15)
                duration = await _probe_media_duration(source)
                await convert_media_to_gif(source, output, duration=duration, run_process=_run_media_process)
                metadata = json.loads(subprocess.check_output([
                    "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                    "stream=width,height:format=duration", "-of", "json", str(output),
                ], timeout=5))
                self.assertEqual(metadata["streams"][0]["width"], expected_width)
                self.assertEqual(metadata["streams"][0]["height"], expected_height)
                self.assertAlmostEqual(float(metadata["format"]["duration"]), duration, delta=0.12)
                self.assertLess(output.stat().st_size, GIF_TARGET_OUTPUT_BYTES)
                self.assertIn(b"NETSCAPE2.0", output.read_bytes())
                self.assertEqual(sorted(p.name for p in Path(directory).iterdir()), ["output.gif", "source.mp4"])

    async def test_static_animation_stays_small_without_losing_its_hold_duration(self):
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory) / "source.mp4", Path(directory) / "output.gif"
            subprocess.run([
                "ffmpeg", "-nostdin", "-y", "-v", "error", "-f", "lavfi", "-i",
                "color=c=blue:size=320x180:rate=20:duration=2", "-c:v", "mpeg4", str(source),
            ], check=True, capture_output=True, timeout=15)
            await convert_media_to_gif(source, output, duration=2, run_process=_run_media_process)
            self.assertLess(output.stat().st_size, 50_000)
            duration = float(subprocess.check_output([
                "ffprobe", "-v", "error", "-show_entries", "format=duration", "-of",
                "default=noprint_wrappers=1:nokey=1", str(output),
            ], timeout=5))
            self.assertAlmostEqual(duration, 2, delta=0.12)
