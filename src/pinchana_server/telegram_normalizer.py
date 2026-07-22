"""Telegram Media Prober, Fast-Start Inspector, and Selective Normalizer."""

import asyncio
import json
import logging
import os
import shutil
import subprocess
import time
import uuid
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, Set

from .distributed_lock import RedisOwnedLock
from .mp4_atom import inspect_mp4_atoms
from .v2_observability import v2_observability

logger = logging.getLogger(__name__)


@dataclass
class MediaProbeResult:
    duration: float = 0.0
    width: int = 0
    height: int = 0
    video_codec: Optional[str] = None
    audio_codec: Optional[str] = None
    pix_fmt: Optional[str] = None
    profile: Optional[str] = None
    level: Optional[int] = None
    bit_depth: int = 8
    container: str = ""
    is_faststart: bool = False
    has_video: bool = False
    has_audio: bool = False
    file_size: int = 0


class TelegramNormalizer:
    def __init__(
        self,
        max_concurrent: int = 4,
        safety_margin_bytes: int = 100 * 1024 * 1024,
        max_input_size_bytes: int = 2 * 1024 * 1024 * 1024,
        max_duration_seconds: float = 7200.0,
        probe_timeout: float = 30.0,
        remux_timeout: float = 60.0,
        transcode_timeout: float = 300.0,
    ):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.safety_margin_bytes = safety_margin_bytes
        self.max_input_size_bytes = max_input_size_bytes
        self.max_duration_seconds = max_duration_seconds
        self.probe_timeout = probe_timeout
        self.remux_timeout = remux_timeout
        self.transcode_timeout = transcode_timeout

        self._in_process_locks: Dict[str, asyncio.Lock] = {}
        self._in_process_backoff: Dict[str, float] = {}

    async def probe_media(self, file_path: Path) -> MediaProbeResult:
        """Run ffprobe asynchronously and inspect MP4 atoms."""
        if not file_path.is_file():
            raise FileNotFoundError(f"Media file not found: {file_path}")

        file_size = file_path.stat().st_size
        if file_size > self.max_input_size_bytes:
            raise ValueError(f"Input file size ({file_size} bytes) exceeds maximum limit ({self.max_input_size_bytes} bytes)")

        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            str(file_path),
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=self.probe_timeout)
        except asyncio.TimeoutError as exc:
            proc.kill()
            raise TimeoutError("ffprobe timed out") from exc

        if proc.returncode != 0:
            raise RuntimeError(f"ffprobe failed: {stderr.decode(errors='ignore')}")

        data = json.loads(stdout.decode("utf-8"))
        fmt = data.get("format", {})
        streams = data.get("streams", [])

        container = fmt.get("format_name", "")
        fmt_duration = float(fmt.get("duration", 0.0) or 0.0)

        video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
        audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)

        has_video = video_stream is not None
        has_audio = audio_stream is not None

        video_codec = video_stream.get("codec_name") if video_stream else None
        audio_codec = audio_stream.get("codec_name") if audio_stream else None
        pix_fmt = video_stream.get("pix_fmt") if video_stream else None
        profile = (video_stream.get("profile") or "").lower() if video_stream else None

        level = video_stream.get("level") if video_stream else None
        if isinstance(level, str) and level.isdigit():
            level = int(level)

        bit_depth = 8
        if video_stream:
            raw_sample = video_stream.get("bits_per_raw_sample")
            if raw_sample and str(raw_sample).isdigit() and int(raw_sample) > 8:
                bit_depth = int(raw_sample)
            elif pix_fmt and ("10" in pix_fmt or "12" in pix_fmt or "p10" in pix_fmt):
                bit_depth = 10

        width = int(video_stream.get("width", 0)) if video_stream else 0
        height = int(video_stream.get("height", 0)) if video_stream else 0

        # Duration logic with format.duration fallback
        duration = 0.0
        if video_stream and video_stream.get("duration"):
            try:
                duration = float(video_stream["duration"])
            except ValueError:
                duration = fmt_duration
        else:
            duration = fmt_duration

        if duration > self.max_duration_seconds:
            raise ValueError(f"Media duration ({duration}s) exceeds maximum limit ({self.max_duration_seconds}s)")

        atom_info = inspect_mp4_atoms(file_path)
        is_faststart = atom_info.get("is_faststart", False)

        return MediaProbeResult(
            duration=duration,
            width=width,
            height=height,
            video_codec=video_codec,
            audio_codec=audio_codec,
            pix_fmt=pix_fmt,
            profile=profile,
            level=level,
            bit_depth=bit_depth,
            container=container,
            is_faststart=is_faststart,
            has_video=has_video,
            has_audio=has_audio,
            file_size=file_size,
        )

    def is_video_codec_compatible(self, probe: MediaProbeResult) -> bool:
        """Check whether the video stream can be copied into a Telegram MP4."""
        if not probe.has_video:
            return True
        if probe.video_codec != "h264":
            return False
        if probe.pix_fmt != "yuv420p":
            return False
        if probe.bit_depth > 8:
            return False
        if probe.profile and probe.profile not in {"main", "high", "baseline", "constrained baseline"}:
            return False
        if probe.level and probe.level > 51:
            return False
        return True

    def is_video_compatible(self, probe: MediaProbeResult) -> bool:
        """Check if the complete file is streamable without normalization."""
        if not self.is_video_codec_compatible(probe):
            return False
        if not any(c in probe.container for c in ("mp4", "mov", "m4a", "3gp")):
            return False
        if not probe.is_faststart:
            return False
        return True

    def is_audio_compatible(self, probe: MediaProbeResult) -> bool:
        """Check if audio stream is Telegram compatible."""
        if not probe.has_audio:
            return True
        return probe.audio_codec in {"aac", "mp3"}

    def _check_disk_space(self, input_size: int, target_dir: Path):
        required_free = int(input_size * 2.5) + self.safety_margin_bytes
        total, used, free = shutil.disk_usage(target_dir if target_dir.exists() else target_dir.parent)
        if free < required_free:
            raise RuntimeError(
                f"Insufficient disk space for normalization. Required: {required_free / (1024*1024):.1f}MB, Available: {free / (1024*1024):.1f}MB"
            )

    async def normalize_for_telegram(
        self,
        input_path: Path,
        post_dir: Path,
        asset_key: str,
        fingerprint: Optional[str] = None,
        redis_client=None,
    ) -> Tuple[Path, str, MediaProbeResult]:
        """
        Normalize media for Telegram delivery.

        Returns:
            (variant_path, streamability_status, probe_result)
        """
        probe = await self.probe_media(input_path)

        v_codec_compat = self.is_video_codec_compatible(probe)
        v_compat = self.is_video_compatible(probe)
        a_compat = self.is_audio_compatible(probe)

        # Multi-factor source fingerprint if not explicitly supplied
        if not fingerprint:
            fingerprint = self.source_fingerprint(input_path, asset_key)

        asset_key_hash = hashlib.sha256(asset_key.encode("utf-8")).hexdigest()[:16]
        variant_dir = post_dir / "variants" / "telegram-v1" / asset_key_hash / fingerprint
        variant_filename = f"telegram_{input_path.name}"
        final_variant_path = variant_dir / variant_filename

        # If completely compatible, return original input path
        if v_compat and a_compat:
            return input_path, "compatible", probe

        # Check backoff
        backoff_key = f"pinchana:norm_fail:{asset_key}:{fingerprint}:telegram-v1:transcode"
        if redis_client:
            if await redis_client.get(backoff_key):
                raise RuntimeError(f"Normalization failed recently for asset {asset_key}. Backoff active.")
        else:
            if backoff_time := self._in_process_backoff.get(backoff_key):
                if time.time() < backoff_time:
                    raise RuntimeError(f"Normalization failed recently for asset {asset_key}. Backoff active.")

        # Check existing variant
        if final_variant_path.is_file():
            variant_probe = await self.probe_media(final_variant_path)
            if self.is_video_compatible(variant_probe) and self.is_audio_compatible(variant_probe):
                return final_variant_path, "normalized", variant_probe

        # Distributed lock with ownership token
        lock_key = f"pinchana:norm_lock:{asset_key}:telegram-v1"
        redis_lock = RedisOwnedLock(redis_client, lock_key) if redis_client else None
        got_lock = False
        renew_task = None

        if redis_client:
            # Token-based SETNX lock with 60s TTL
            got_lock = await redis_lock.acquire()
            if not got_lock:
                for _ in range(30):
                    await asyncio.sleep(1.0)
                    if final_variant_path.is_file():
                        variant_probe = await self.probe_media(final_variant_path)
                        if self.is_video_compatible(variant_probe) and self.is_audio_compatible(variant_probe):
                            return final_variant_path, "normalized", variant_probe
                raise RuntimeError(f"Failed to acquire normalization lock for {asset_key}")

            # Start background lock renewal loop
            async def _renew_loop():
                try:
                    while True:
                        await asyncio.sleep(10)
                        try:
                            await redis_lock.renew()
                        except Exception:
                            pass
                except asyncio.CancelledError:
                    pass

            renew_task = asyncio.create_task(_renew_loop())

        lock = None
        if not redis_client:
            if lock_key not in self._in_process_locks:
                self._in_process_locks[lock_key] = asyncio.Lock()
            lock = self._in_process_locks[lock_key]

        async with self.semaphore:
            try:
                if lock:
                    async with lock:
                        return await self._execute_ffmpeg_normalization(
                            input_path, final_variant_path, v_codec_compat, a_compat, probe, backoff_key, redis_client
                        )
                else:
                    return await self._execute_ffmpeg_normalization(
                        input_path, final_variant_path, v_codec_compat, a_compat, probe, backoff_key, redis_client
                    )
            finally:
                if renew_task:
                    renew_task.cancel()
                if redis_lock and got_lock:
                    try:
                        await redis_lock.release()
                    except Exception:
                        pass

    async def _execute_ffmpeg_normalization(
        self,
        input_path: Path,
        final_variant_path: Path,
        copy_video: bool,
        a_compat: bool,
        probe: MediaProbeResult,
        backoff_key: str,
        redis_client=None,
    ) -> Tuple[Path, str, MediaProbeResult]:

        variant_dir = final_variant_path.parent
        variant_dir.mkdir(parents=True, exist_ok=True)

        self._check_disk_space(probe.file_size, variant_dir)

        tmp_path = variant_dir / f".tmp.{os.getpid()}.{uuid.uuid4().hex}.mp4"

        cmd = ["ffmpeg", "-y", "-i", str(input_path)]

        # Video selective transcoding
        if copy_video:
            cmd.extend(["-c:v", "copy"])
        else:
            cmd.extend(["-c:v", "libx264", "-pix_fmt", "yuv420p", "-profile:v", "main", "-level", "4.0"])

        # Audio selective transcoding
        if not probe.has_audio:
            cmd.append("-an")
        elif a_compat:
            cmd.extend(["-c:a", "copy"])
        else:
            cmd.extend(["-c:a", "aac", "-b:a", "192k"])

        cmd.extend(["-movflags", "+faststart", str(tmp_path)])

        is_pure_remux = copy_video and a_compat
        timeout = self.remux_timeout if is_pure_remux else self.transcode_timeout

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)

            if proc.returncode != 0:
                raise RuntimeError(f"ffmpeg normalization failed: {stderr.decode(errors='ignore')}")

            # Atomic publication
            os.replace(tmp_path, final_variant_path)

            variant_probe = await self.probe_media(final_variant_path)
            return final_variant_path, "normalized", variant_probe

        except Exception as exc:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass

            # Record failure backoff for 300s
            if redis_client:
                try:
                    await redis_client.set(backoff_key, "failed", ex=300)
                except Exception:
                    pass
            else:
                self._in_process_backoff[backoff_key] = time.time() + 300.0

            logger.error("telegram_normalization_failed error=%s", type(exc).__name__)
            v2_observability.increment("normalization_failure")
            raise

    @staticmethod
    def source_fingerprint(input_path: Path, asset_key: str) -> str:
        stat = input_path.stat()
        signature = f"{asset_key}:{stat.st_size}:{stat.st_mtime_ns}:telegram-v1"
        return hashlib.sha256(signature.encode("utf-8")).hexdigest()[:16]
