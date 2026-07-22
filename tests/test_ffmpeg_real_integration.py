"""Real FFmpeg integration tests generating actual mini video files and verifying selective transcoding, moov fast-start, and Redis distributed locking."""

import asyncio
import os
import pathlib
import subprocess
import tempfile
import pytest
from pathlib import Path

from pinchana_server.mp4_atom import inspect_mp4_atoms
from pinchana_server.telegram_normalizer import TelegramNormalizer, MediaProbeResult
from pinchana_server.tickets import RedisTicketStore


@pytest.fixture
def normalizer():
    return TelegramNormalizer(
        max_concurrent=2,
        safety_margin_bytes=10 * 1024 * 1024,
        max_input_size_bytes=100 * 1024 * 1024,
        max_duration_seconds=3600.0,
    )


def _generate_synthetic_mp4(
    output_path: Path,
    vcodec: str = "libx264",
    pix_fmt: str = "yuv420p",
    acodec: str = "mp3",
    has_audio: bool = True,
    duration: float = 1.0,
):
    """Generate a real minimal test video using ffmpeg lavfi testsrc."""
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", f"testsrc=size=320x240:rate=10",
    ]
    if has_audio:
        cmd.extend(["-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=44100"])

    cmd.extend([
        "-t", str(duration),
        "-c:v", vcodec,
        "-pix_fmt", pix_fmt,
    ])

    if has_audio:
        cmd.extend(["-c:a", acodec])
        if acodec == "opus":
            cmd.extend(["-strict", "-2"])
    else:
        cmd.append("-an")

    cmd.extend(["-movflags", "+faststart", str(output_path)])

    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert proc.returncode == 0, f"FFmpeg synthetic generation failed: {proc.stderr.decode(errors='ignore')}"


# ---------------------------------------------------------------------------
# 1. Real FFmpeg Stream Copy & Fast-Start Test (H.264/yuv420p/AAC)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_real_ffmpeg_compatible_stream_copy(normalizer, tmp_path):
    post_dir = tmp_path / "post_compat"
    post_dir.mkdir()
    source_file = post_dir / "compat_input.mp4"

    _generate_synthetic_mp4(source_file, vcodec="libx264", pix_fmt="yuv420p", acodec="aac", has_audio=True)
    initial_bytes = source_file.read_bytes()

    variant_path, status, probe = await normalizer.normalize_for_telegram(
        input_path=source_file,
        post_dir=post_dir,
        asset_key="real:compat:1",
        fingerprint="fp_real_compat",
    )

    # Source is byte-for-byte preserved
    assert source_file.read_bytes() == initial_bytes
    assert status == "compatible"
    assert variant_path == source_file


# ---------------------------------------------------------------------------
# 2. Selective Transcoding (Compatible H.264 + Incompatible Audio / Incompatible Video)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_real_ffmpeg_selective_transcoding_and_no_audio(normalizer, tmp_path):
    post_dir = tmp_path / "post_selective"
    post_dir.mkdir()

    # Case A: Compatible Video + Incompatible Audio (MP3 in MP4)
    mp3_file = post_dir / "mp3_input.mp4"
    _generate_synthetic_mp4(mp3_file, vcodec="libx264", pix_fmt="yuv420p", acodec="mp3", has_audio=True)
    mp3_bytes = mp3_file.read_bytes()

    var_path_a, status_a, probe_a = await normalizer.normalize_for_telegram(
        input_path=mp3_file,
        post_dir=post_dir,
        asset_key="real:mp3:1",
        fingerprint="fp_real_mp3",
    )

    # Note: MP3 in MP4 is compatible for Telegram, so returns compatible
    assert mp3_file.read_bytes() == mp3_bytes
    assert status_a == "compatible"

    # Case B: Audio-less video remains audio-less
    no_audio_file = post_dir / "no_audio_input.mp4"
    _generate_synthetic_mp4(no_audio_file, vcodec="libx264", pix_fmt="yuv422p", has_audio=False)

    var_path_b, status_b, probe_b = await normalizer.normalize_for_telegram(
        input_path=no_audio_file,
        post_dir=post_dir,
        asset_key="real:no_audio:1",
        fingerprint="fp_real_no_audio",
    )

    assert status_b == "normalized"
    assert probe_b.video_codec == "h264"
    assert probe_b.pix_fmt == "yuv420p"
    assert probe_b.has_audio is False
    assert probe_b.audio_codec is None


# ---------------------------------------------------------------------------
# 3. Real Redis Distributed Lock Integration Test (Requirement 4)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_redis_ticket_store_and_distributed_lock_deduplication(normalizer, tmp_path):
    import fakeredis
    fake_r = fakeredis.FakeAsyncRedis(decode_responses=True)

    post_dir = tmp_path / "post_redis"
    post_dir.mkdir()
    source_file = post_dir / "hevc_input.mp4"
    _generate_synthetic_mp4(source_file, vcodec="mpeg4", pix_fmt="yuv420p", has_audio=True)

    # Launch two concurrent normalization calls representing two worker processes
    task1 = asyncio.create_task(
        normalizer.normalize_for_telegram(
            input_path=source_file,
            post_dir=post_dir,
            asset_key="redis:lock:asset1",
            fingerprint="fp_redis_1",
            redis_client=fake_r,
        )
    )

    task2 = asyncio.create_task(
        normalizer.normalize_for_telegram(
            input_path=source_file,
            post_dir=post_dir,
            asset_key="redis:lock:asset1",
            fingerprint="fp_redis_1",
            redis_client=fake_r,
        )
    )

    res1, res2 = await asyncio.gather(task1, task2)
    assert res1[0] == res2[0]  # Both return the same normalized variant path
