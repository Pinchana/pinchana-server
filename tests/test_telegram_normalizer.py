"""Comprehensive unit and integration tests for Telegram Delivery Normalization, MP4 atom parsing, and selective transcoding."""

import asyncio
import os
import pathlib
import tempfile
import time
import unittest.mock
import pytest
from pathlib import Path

from fastapi.testclient import TestClient
from pinchana_server.main import app, dimension_probe, storage
from pinchana_server.mp4_atom import inspect_mp4_atoms
from pinchana_server.telegram_normalizer import TelegramNormalizer, MediaProbeResult


@pytest.fixture
def normalizer():
    return TelegramNormalizer(
        max_concurrent=2,
        safety_margin_bytes=10 * 1024 * 1024,
        max_input_size_bytes=100 * 1024 * 1024,
        max_duration_seconds=3600.0,
    )


# ---------------------------------------------------------------------------
# 1. MP4 Atom Parser Tests
# ---------------------------------------------------------------------------
def test_mp4_atom_parser_truncated_and_corrupt(tmp_path):
    # Test non-existent file
    res = inspect_mp4_atoms(tmp_path / "nonexistent.mp4")
    assert res["is_faststart"] is False

    # Test corrupt binary file
    corrupt_file = tmp_path / "corrupt.mp4"
    corrupt_file.write_bytes(b"INVALID_HEADER_DATA_1234567890")
    res_corrupt = inspect_mp4_atoms(corrupt_file)
    assert res_corrupt["is_faststart"] is False

    # Mock synthetic MP4 atom header (ftyp + moov + mdat)
    synthetic_mp4 = tmp_path / "synthetic.mp4"
    # ftyp box (16 bytes)
    ftyp_box = b"\x00\x00\x00\x10ftypisom\x00\x00\x02\x00"
    # moov box (12 bytes)
    moov_box = b"\x00\x00\x00\x0cmoovtest"
    # mdat box (12 bytes)
    mdat_box = b"\x00\x00\x00\x0cmdattest"
    synthetic_mp4.write_bytes(ftyp_box + moov_box + mdat_box)

    res_synth = inspect_mp4_atoms(synthetic_mp4)
    assert res_synth["is_faststart"] is True
    assert res_synth["moov_offset"] == 16
    assert res_synth["mdat_offset"] == 28


# ---------------------------------------------------------------------------
# 2. Compatibility Logic Tests
# ---------------------------------------------------------------------------
def test_selective_compatibility_checks(normalizer):
    # Compatible video probe
    comp_probe = MediaProbeResult(
        duration=10.0,
        width=1920,
        height=1080,
        video_codec="h264",
        audio_codec="aac",
        pix_fmt="yuv420p",
        profile="main",
        level=40,
        bit_depth=8,
        container="mov,mp4,m4a,3gp,3g2,mj2",
        is_faststart=True,
        has_video=True,
        has_audio=True,
    )
    assert normalizer.is_video_compatible(comp_probe) is True
    assert normalizer.is_audio_compatible(comp_probe) is True

    # Incompatible video probe (HEVC codec, 10-bit)
    hevc_probe = MediaProbeResult(
        duration=10.0,
        width=1920,
        height=1080,
        video_codec="hevc",
        audio_codec="aac",
        pix_fmt="yuv420p10le",
        profile="main10",
        bit_depth=10,
        container="mp4",
        is_faststart=True,
        has_video=True,
        has_audio=True,
    )
    assert normalizer.is_video_compatible(hevc_probe) is False
    assert normalizer.is_audio_compatible(hevc_probe) is True

    # Incompatible audio probe (Opus codec)
    opus_probe = MediaProbeResult(
        duration=10.0,
        width=1920,
        height=1080,
        video_codec="h264",
        audio_codec="opus",
        pix_fmt="yuv420p",
        profile="main",
        bit_depth=8,
        container="mp4",
        is_faststart=True,
        has_video=True,
        has_audio=True,
    )
    assert normalizer.is_video_compatible(opus_probe) is True
    assert normalizer.is_audio_compatible(opus_probe) is False

    # No-audio probe
    no_audio_probe = MediaProbeResult(
        duration=5.0,
        width=1280,
        height=720,
        video_codec="h264",
        audio_codec=None,
        pix_fmt="yuv420p",
        profile="high",
        bit_depth=8,
        container="mp4",
        is_faststart=True,
        has_video=True,
        has_audio=False,
    )
    assert normalizer.is_video_compatible(no_audio_probe) is True
    assert normalizer.is_audio_compatible(no_audio_probe) is True


# ---------------------------------------------------------------------------
# 3. Source Preservation & Unique Variant Paths
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_source_preservation_and_variant_path(normalizer, tmp_path):
    post_dir = tmp_path / "post_123"
    post_dir.mkdir()
    source_file = post_dir / "original.mp4"
    source_content = b"ORIGINAL_UNTOUCHED_SOURCE_MEDIA_BYTES"
    source_file.write_bytes(source_content)

    mock_probe = MediaProbeResult(
        duration=10.0,
        width=1920,
        height=1080,
        video_codec="hevc",
        audio_codec="opus",
        pix_fmt="yuv420p10le",
        profile="main10",
        bit_depth=10,
        container="mp4",
        is_faststart=False,
        has_video=True,
        has_audio=True,
        file_size=len(source_content),
    )

    mock_norm_probe = MediaProbeResult(
        duration=10.0,
        width=1920,
        height=1080,
        video_codec="h264",
        audio_codec="aac",
        pix_fmt="yuv420p",
        profile="main",
        bit_depth=8,
        container="mp4",
        is_faststart=True,
        has_video=True,
        has_audio=True,
        file_size=100,
    )

    with unittest.mock.patch.object(normalizer, "probe_media", new_callable=unittest.mock.AsyncMock) as mock_p:
        mock_p.side_effect = [mock_probe, mock_norm_probe]

        with unittest.mock.patch("asyncio.create_subprocess_exec", new_callable=unittest.mock.AsyncMock) as mock_exec:
            mock_proc = unittest.mock.MagicMock()
            mock_proc.returncode = 0
            mock_proc.communicate = unittest.mock.AsyncMock(return_value=(b"", b""))
            mock_exec.return_value = mock_proc

            # Helper to simulate ffmpeg writing output file
            async def fake_ffmpeg(*args, **kwargs):
                out_path = Path(args[-1])
                out_path.write_bytes(b"NORMALIZED_VARIANT_CONTENT")
                return mock_proc

            mock_exec.side_effect = fake_ffmpeg

            variant_path, status, norm_probe = await normalizer.normalize_for_telegram(
                input_path=source_file,
                post_dir=post_dir,
                asset_key="instagram:123:0:content",
                fingerprint="fp12345",
            )

            # Assert original source file was preserved completely untouched
            assert source_file.read_bytes() == source_content
            assert status == "normalized"

            # Assert variant is stored under variants/telegram-v1/{asset_key_hash}/{fingerprint}/
            import hashlib
            asset_key_hash = hashlib.sha256(b"instagram:123:0:content").hexdigest()[:16]
            expected_variant_dir = post_dir / "variants" / "telegram-v1" / asset_key_hash / "fp12345"
            assert variant_path.parent == expected_variant_dir
            assert variant_path.name == "telegram_original.mp4"
            assert variant_path.is_file()


# ---------------------------------------------------------------------------
# 4. Failure Backoff & Invalidation
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_failure_backoff_and_invalidation(normalizer, tmp_path):
    post_dir = tmp_path / "post_backoff"
    post_dir.mkdir()
    source_file = post_dir / "bad_video.mp4"
    source_file.write_bytes(b"BAD_VIDEO_CONTENT")

    mock_probe = MediaProbeResult(
        duration=10.0,
        video_codec="hevc",
        container="mp4",
        has_video=True,
        file_size=10,
    )

    with unittest.mock.patch.object(normalizer, "probe_media", new_callable=unittest.mock.AsyncMock) as mock_p:
        mock_p.return_value = mock_probe

        with unittest.mock.patch("asyncio.create_subprocess_exec", new_callable=unittest.mock.AsyncMock) as mock_exec:
            mock_proc = unittest.mock.MagicMock()
            mock_proc.returncode = 1
            mock_proc.communicate = unittest.mock.AsyncMock(return_value=(b"", b"FFmpeg Error"))
            mock_exec.return_value = mock_proc

            # First attempt fails and sets backoff
            with pytest.raises(RuntimeError):
                await normalizer.normalize_for_telegram(
                    input_path=source_file,
                    post_dir=post_dir,
                    asset_key="test:bad:0",
                    fingerprint="fp_fail_1",
                )

            # Immediate second attempt with SAME fingerprint raises backoff error
            with pytest.raises(RuntimeError) as exc_info:
                await normalizer.normalize_for_telegram(
                    input_path=source_file,
                    post_dir=post_dir,
                    asset_key="test:bad:0",
                    fingerprint="fp_fail_1",
                )
            assert "Backoff active" in str(exc_info.value)

            # Attempt with CHANGED fingerprint bypasses backoff
            with pytest.raises(RuntimeError) as exc_info_new_fp:
                await normalizer.normalize_for_telegram(
                    input_path=source_file,
                    post_dir=post_dir,
                    asset_key="test:bad:0",
                    fingerprint="fp_fail_NEW_FINGERPRINT",
                )
            # Fails due to ffmpeg error, not backoff
            assert "ffmpeg normalization failed" in str(exc_info_new_fp.value)


# ---------------------------------------------------------------------------
# 5. Endpoint Authorization Scope Tests
# ---------------------------------------------------------------------------
def test_telegram_scrape_endpoint_authorization_scopes():
    os.environ["PINCHANA_API_KEYS"] = '{"bot_client": "secret_bot_key", "machine_client": "secret_machine_key"}'
    os.environ["PINCHANA_API_KEY_SCOPES"] = '{"bot_client": ["delivery:telegram"], "machine_client": ["scrape:v1"]}'
    try:
        with TestClient(app) as client:
            # 1. Credential with server-configured delivery:telegram scope succeeds (passes auth)
            res_bot = client.post(
                "/v2/telegram/scrape",
                json={"url": "https://www.instagram.com/p/TEST_TG_SCOPE/"},
                headers={"x-api-key": "secret_bot_key"},
            )
            assert res_bot.status_code in {200, 400, 403, 404, 502}
            if res_bot.status_code == 403:
                assert "delivery:telegram scope" not in res_bot.text

            # 2. Machine API key EVEN IF it sends x-delivery-scope: delivery:telegram header is REJECTED with 403
            res_machine_spoofed = client.post(
                "/v2/telegram/scrape",
                json={"url": "https://www.instagram.com/p/TEST_TG_SCOPE/"},
                headers={"x-api-key": "secret_machine_key", "x-delivery-scope": "delivery:telegram"},
            )
            assert res_machine_spoofed.status_code == 403

            # 3. Missing API key rejected with 401
            res_no_key = client.post(
                "/v2/telegram/scrape",
                json={"url": "https://www.instagram.com/p/TEST_TG_SCOPE/"},
            )
            assert res_no_key.status_code == 401
    finally:
        os.environ.pop("PINCHANA_API_KEYS", None)
        os.environ.pop("PINCHANA_API_KEY_SCOPES", None)


def test_telegram_scrape_serializes_ordered_fingerprinted_shared_cache_asset(tmp_path):
    from PIL import Image

    post_dir = tmp_path / "ORDER123"
    post_dir.mkdir()
    image_path = post_dir / "thumbnail.jpg"
    Image.new("RGB", (640, 480), color="blue").save(image_path)
    payload = {
        "shortcode": "ORDER123",
        "caption": "Ordered asset",
        "author": "creator",
        "thumbnail_url": "/media/instagram/ORDER123/thumbnail.jpg",
    }
    old_storage_base = storage.base_path
    old_probe_base = dimension_probe.base_path
    storage.base_path = tmp_path
    dimension_probe.base_path = tmp_path
    os.environ["PINCHANA_API_KEYS"] = '{"bot_client": "secret_bot_key"}'
    os.environ["PINCHANA_API_KEY_SCOPES"] = '{"bot_client": ["delivery:telegram"]}'
    try:
        with unittest.mock.patch(
            "pinchana_server.main._process_scrape_payload",
            new=unittest.mock.AsyncMock(return_value=("instagram", payload)),
        ):
            with TestClient(app) as client:
                response = client.post(
                    "/v2/telegram/scrape",
                    json={"url": "https://www.instagram.com/p/ORDER123/"},
                    headers={"x-api-key": "secret_bot_key"},
                )
        assert response.status_code == 200, response.text
        asset = response.json()["assets"][0]
        assert asset["index"] == 0
        assert asset["dimensions"] == {"width": 640, "height": 480}
        delivery = asset["delivery"]
        assert delivery["relative_variant_path"] == "ORDER123/thumbnail.jpg"
        assert delivery["source_fingerprint"] in delivery["cache_key"]
    finally:
        storage.base_path = old_storage_base
        dimension_probe.base_path = old_probe_base
        os.environ.pop("PINCHANA_API_KEYS", None)
        os.environ.pop("PINCHANA_API_KEY_SCOPES", None)


def test_telegram_scrape_rejects_final_symlink_before_probing(tmp_path):
    from PIL import Image

    post_dir = tmp_path / "LINK123"
    post_dir.mkdir()
    target = tmp_path / "target.jpg"
    Image.new("RGB", (320, 240), color="red").save(target)
    (post_dir / "thumbnail.jpg").symlink_to(target)
    payload = {
        "shortcode": "LINK123",
        "caption": "Symlink asset",
        "author": "creator",
        "thumbnail_url": "/media/instagram/LINK123/thumbnail.jpg",
    }
    old_storage_base = storage.base_path
    old_probe_base = dimension_probe.base_path
    storage.base_path = tmp_path
    dimension_probe.base_path = tmp_path
    os.environ["PINCHANA_API_KEYS"] = '{"bot_client": "secret_bot_key"}'
    os.environ["PINCHANA_API_KEY_SCOPES"] = '{"bot_client": ["delivery:telegram"]}'
    try:
        with unittest.mock.patch(
            "pinchana_server.main._process_scrape_payload",
            new=unittest.mock.AsyncMock(return_value=("instagram", payload)),
        ):
            with TestClient(app) as client:
                response = client.post(
                    "/v2/telegram/scrape",
                    json={"url": "https://www.instagram.com/p/LINK123/"},
                    headers={"x-api-key": "secret_bot_key"},
                )
        assert response.status_code == 400, response.text
        assert "Symlink media is not allowed" in response.json()["detail"]
    finally:
        storage.base_path = old_storage_base
        dimension_probe.base_path = old_probe_base
        os.environ.pop("PINCHANA_API_KEYS", None)
        os.environ.pop("PINCHANA_API_KEY_SCOPES", None)
