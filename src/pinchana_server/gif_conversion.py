"""Palette-based, bounded GIF encoding for already-cached media only."""

import logging
import math
import shutil
from time import monotonic
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from fastapi import HTTPException

logger = logging.getLogger(__name__)
GIF_MAX_INPUT_BYTES = 50 * 1024 * 1024
GIF_TARGET_OUTPUT_BYTES = 4 * 1024 * 1024
GIF_MAX_OUTPUT_BYTES = 8 * 1024 * 1024
GIF_MAX_DURATION_SECONDS = 60.0
GIF_PROCESS_TIMEOUT_SECONDS = 45.0
# A disk guard, not a deliverable size: any output stopped by -fs is discarded.
GIF_RAW_OUTPUT_BYTES = 32 * 1024 * 1024
ProcessRunner = Callable[..., Awaitable[tuple[int, bytes, bytes]]]


@dataclass(frozen=True)
class GifProfile:
    edge: int
    fps: int
    colors: int = 256
    bayer_scale: int = 3


def profiles_for_duration(duration: float) -> tuple[GifProfile, ...]:
    if duration > 30:
        return (GifProfile(480, 12), GifProfile(384, 10), GifProfile(320, 8, 192, 4))
    if duration > 15:
        return (GifProfile(640, 12), GifProfile(480, 10), GifProfile(360, 8, 192, 4))
    return (GifProfile(640, 15), GifProfile(512, 12), GifProfile(384, 10))


def video_filter(profile: GifProfile) -> str:
    # Work in display dimensions, including non-square pixels. No intermediate
    # full-size RGB buffer, upscaling of display dimensions, or unbounded portrait height.
    sar = "if(gt(sar,0),sar,1)"
    ratio = f"min(1,min({profile.edge}/(iw*{sar}),{profile.edge}/ih))"
    return (
        f"fps={profile.fps},"
        f"scale=w='max(1,trunc(iw*{sar}*{ratio}))':"
        f"h='max(1,trunc(ih*{ratio}))':flags=lanczos,setsar=1"
    )


async def convert_media_to_gif(
    source: Path,
    output: Path,
    *,
    duration: float,
    run_process: ProcessRunner,
) -> None:
    if not math.isfinite(duration) or not 0 < duration <= GIF_MAX_DURATION_SECONDS:
        raise HTTPException(422, "GIF conversion supports media up to 60 seconds")
    deadline = monotonic() + GIF_PROCESS_TIMEOUT_SECONDS
    optimizer = shutil.which("gifsicle")
    best = output.with_name("best.gif")
    best_size: int | None = None
    incomplete = False

    def remaining() -> float:
        seconds = deadline - monotonic()
        if seconds <= 0:
            raise HTTPException(504, "Media conversion timed out")
        return seconds

    async def run(*args: str, timeout: float | None = None) -> tuple[int, bytes, bytes]:
        budget = remaining()
        return await run_process(*args, timeout=min(budget, timeout) if timeout else budget)

    try:
        for index, profile in enumerate(profiles_for_duration(duration)):
            palette = output.with_name(f"palette-{index}.png")
            raw = output.with_name(f"raw-{index}.gif")
            optimized = output.with_name(f"optimized-{index}.gif")
            try:
                filters = video_filter(profile)
                common = (
                    "ffmpeg", "-nostdin", "-y", "-hide_banner", "-loglevel", "error",
                    "-max_alloc", str(64 * 1024 * 1024),
                    "-threads", "2", "-filter_threads", "1", "-filter_complex_threads", "1",
                )
                # A separate palette pass avoids retaining an entire animation in
                # a split filter while palettegen waits for the last frame.
                code, _, stderr = await run(
                    *common, "-i", str(source), "-map", "0:v:0", "-an", "-sn", "-dn",
                    "-vf", filters + f",palettegen=max_colors={profile.colors}:stats_mode=full:reserve_transparent=1",
                    "-frames:v", "1", "-update", "1", str(palette),
                )
                if code != 0 or not palette.is_file():
                    logger.info("gif_conversion_failed stage=palette stderr_bytes=%d", len(stderr))
                    raise HTTPException(422, "The media file cannot be converted to GIF")
                code, _, stderr = await run(
                    *common, "-i", str(source), "-i", str(palette),
                    "-filter_complex",
                    f"[0:v:0]{filters}[frames];[frames][1:v:0]"
                    f"paletteuse=dither=bayer:bayer_scale={profile.bayer_scale}:diff_mode=rectangle[gif]",
                    "-map", "[gif]", "-an", "-sn", "-dn", "-map_metadata", "-1",
                    "-gifflags", "+offsetting+transdiff", "-global_palette", "1", "-loop", "0",
                    "-fs", str(GIF_RAW_OUTPUT_BYTES + 1), str(raw),
                )
                if code != 0 or not raw.is_file() or raw.stat().st_size <= 0:
                    logger.info("gif_conversion_failed stage=encode stderr_bytes=%d", len(stderr))
                    raise HTTPException(422, "The media file cannot be converted to GIF")
                # Never optimize a size-truncated GIF into an apparently valid,
                # smaller deliverable. Retry the complete source at a lower profile.
                if raw.stat().st_size > GIF_RAW_OUTPUT_BYTES:
                    logger.info("gif_conversion_retry reason=raw_limit profile=%d", index)
                    continue
                candidate = raw
                if optimizer:
                    try:
                        code, _, _ = await run(
                            optimizer, "--optimize=3", "--conserve-memory", "--no-comments",
                            "--no-names", "--loopcount=0", str(raw), "--output", str(optimized),
                            timeout=8.0,
                        )
                        if code == 0 and optimized.is_file() and 0 < optimized.stat().st_size < raw.stat().st_size:
                            candidate = optimized
                    except HTTPException as exc:
                        if exc.status_code not in (503, 504):
                            raise
                        logger.info("gif_optimizer_unavailable status=%d", exc.status_code)
                size = candidate.stat().st_size
                if size > GIF_MAX_OUTPUT_BYTES:
                    logger.info("gif_conversion_retry reason=output_limit profile=%d bytes=%d", index, size)
                    continue
                # Both FFmpeg's file-size guard and external optimizers can finish
                # successfully with a shortened timeline. Validate before delivery.
                code, stdout, _ = await run(
                    "ffprobe", "-v", "error", "-show_entries", "format=duration",
                    "-of", "default=noprint_wrappers=1:nokey=1", str(candidate), timeout=5.0,
                )
                try:
                    actual_duration = float(stdout.strip())
                except (ValueError, OverflowError):
                    actual_duration = math.nan
                tolerance = max(0.12, 1 / profile.fps + 0.03)
                if code != 0 or not math.isfinite(actual_duration) or abs(actual_duration - duration) > tolerance:
                    incomplete = True
                    logger.info("gif_conversion_retry reason=incomplete_timeline profile=%d", index)
                    continue
                logger.info(
                    "gif_conversion_candidate profile=%d edge=%d fps=%d colors=%d bytes=%d optimized=%s",
                    index, profile.edge, profile.fps, profile.colors, size, candidate == optimized,
                )
                if size <= GIF_TARGET_OUTPUT_BYTES:
                    candidate.replace(output)
                    return
                if best_size is None or size < best_size:
                    candidate.replace(best)
                    best_size = size
            except HTTPException as exc:
                if exc.status_code == 504 and best_size is not None:
                    break
                raise
            finally:
                for path in (palette, raw, optimized):
                    path.unlink(missing_ok=True)
        if best_size is not None:
            best.replace(output)
            return
        if incomplete:
            raise HTTPException(422, "GIF conversion could not preserve the complete animation")
        raise HTTPException(413, "The GIF cannot fit within 8 MiB; use a shorter clip or keep the video")
    finally:
        best.unlink(missing_ok=True)
