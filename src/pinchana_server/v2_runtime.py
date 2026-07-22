"""Deployment and response helpers for native v2 delivery."""

from __future__ import annotations

import mimetypes
import os
import re
from pathlib import Path


MIME_EXTENSIONS = {
    "image/avif": ".avif",
    "image/gif": ".gif",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "video/mp4": ".mp4",
    "video/webm": ".webm",
    "audio/mpeg": ".mp3",
    "audio/mp4": ".m4a",
    "audio/ogg": ".ogg",
}


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def validate_spool_topology() -> None:
    """Fail closed when replicas could receive tickets for inaccessible files."""
    try:
        replicas = int(os.getenv("PINCHANA_API_REPLICAS", "1"))
    except ValueError as exc:
        raise RuntimeError("PINCHANA_API_REPLICAS must be a positive integer") from exc
    if replicas < 1:
        raise RuntimeError("PINCHANA_API_REPLICAS must be a positive integer")
    if replicas <= 1:
        return
    if not os.getenv("REDIS_URL", "").strip():
        raise RuntimeError("Multiple API replicas require REDIS_URL for shared v2 state")
    if not env_flag("V2_SPOOL_SHARED"):
        raise RuntimeError(
            "Multiple API replicas require V2_SPOOL_SHARED=true and the same shared "
            "V2_SPOOL_PATH mounted in every replica"
        )
    spool_path = Path(os.getenv("V2_SPOOL_PATH", "./spool"))
    if not spool_path.is_absolute():
        raise RuntimeError("Shared V2_SPOOL_PATH must be absolute")


def validate_internal_token() -> None:
    environment = os.getenv("PINCHANA_ENV", os.getenv("ENVIRONMENT", "development"))
    production = environment.strip().lower() in {"prod", "production"}
    if not production or not env_flag("PINCHANA_V2_TIKTOK"):
        return
    if len(os.getenv("PINCHANA_INTERNAL_TOKEN", "")) < 32:
        raise RuntimeError(
            "PINCHANA_V2_TIKTOK requires a PINCHANA_INTERNAL_TOKEN of at least "
            "32 characters in production"
        )


def normalized_filename(filename: str, content_type: str | None) -> str:
    """Align a safe download suffix with authoritative delivered media bytes."""
    safe_name = re.sub(r'[\r\n"\\]', "_", Path(filename).name) or "download"
    mime = (content_type or "").split(";", 1)[0].strip().lower()
    extension = MIME_EXTENSIONS.get(mime)
    if not extension:
        return safe_name
    current_mime = mimetypes.guess_type(safe_name)[0]
    if current_mime == mime:
        return safe_name
    stem = Path(safe_name).stem or "download"
    return f"{stem}{extension}"
