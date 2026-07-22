"""Read visual dimensions from media files in the shared scraper cache."""

from __future__ import annotations

import asyncio
import json
import logging
import subprocess
from pathlib import Path
from typing import Any

from PIL import Image

from .schemas import MediaDimensions

logger = logging.getLogger(__name__)


class MediaDimensionProbe:
    """Probe cached images and videos without fetching arbitrary URLs."""

    def __init__(self, base_path: str | Path, concurrency: int = 4):
        self.base_path = Path(base_path)
        self._limit = asyncio.Semaphore(max(1, concurrency))
        self._cache: dict[Path, tuple[tuple[int, int], MediaDimensions | None]] = {}
        self._cache_lock = asyncio.Lock()

    def _resolve_media_path(self, url: str) -> Path | None:
        if not isinstance(url, str) or not url.startswith("/media/"):
            return None
        relative = url.split("?", 1)[0].removeprefix("/media/")
        parts = relative.split("/", 2)
        if len(parts) != 3 or not all(parts):
            return None
        _platform, post_id, filename = parts
        if ".." in filename or filename.startswith("/"):
            return None
        candidate = self.base_path / post_id / filename
        resolved = candidate.resolve()
        try:
            resolved.relative_to(self.base_path.resolve())
        except ValueError:
            return None
        return candidate

    async def dimensions_for(self, url: str, media_type: str) -> MediaDimensions | None:
        if media_type not in {"image", "video"}:
            return None
        path = self._resolve_media_path(url)
        if path is None or not path.is_file():
            return None
        stat = path.stat()
        signature = (stat.st_mtime_ns, stat.st_size)
        async with self._cache_lock:
            cached = self._cache.get(path)
            if cached and cached[0] == signature:
                return cached[1]

        async with self._limit:
            if media_type == "image":
                # Pillow reads only the image header for ``size``; keeping this
                # inline avoids creating a worker thread for a tiny cache read.
                dimensions = self._probe_image(path)
            else:
                dimensions = self._probe_video(path)

        async with self._cache_lock:
            self._cache[path] = (signature, dimensions)
        return dimensions

    @staticmethod
    def _probe_image(path: Path) -> MediaDimensions | None:
        try:
            with Image.open(path) as image:
                width, height = image.size
            return MediaDimensions(width=width, height=height)
        except (OSError, ValueError):
            logger.warning("media_dimension_probe_failed type=image")
            return None

    @staticmethod
    def _rotation(stream: dict[str, Any]) -> int:
        tags = stream.get("tags")
        if isinstance(tags, dict):
            try:
                return int(tags.get("rotate", 0))
            except (TypeError, ValueError):
                pass
        side_data = stream.get("side_data_list")
        if isinstance(side_data, list):
            for item in side_data:
                if not isinstance(item, dict) or "rotation" not in item:
                    continue
                try:
                    return int(item["rotation"])
                except (TypeError, ValueError):
                    continue
        return 0

    def _probe_video(self, path: Path) -> MediaDimensions | None:
        try:
            process = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "v:0",
                    "-show_entries",
                    "stream=width,height:stream_tags=rotate:stream_side_data=rotation",
                    "-of",
                    "json",
                    str(path),
                ],
                capture_output=True,
                check=False,
                timeout=5,
            )
        except FileNotFoundError:
            logger.warning("media_dimension_probe_unavailable executable=ffprobe")
            return None
        except (OSError, subprocess.TimeoutExpired):
            logger.warning("media_dimension_probe_failed type=video")
            return None
        if process.returncode != 0:
            logger.warning(
                "media_dimension_probe_failed type=video path=%s error=%s",
                path,
                process.stderr.decode("utf-8", errors="replace")[:200],
            )
            return None
        try:
            payload = json.loads(process.stdout)
            streams = payload.get("streams") or []
            stream = streams[0]
            width = int(stream["width"])
            height = int(stream["height"])
            if abs(self._rotation(stream)) % 180 == 90:
                width, height = height, width
            return MediaDimensions(width=width, height=height)
        except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            logger.warning("media_dimension_probe_invalid type=video")
            return None
