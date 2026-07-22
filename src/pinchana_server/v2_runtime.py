"""Deployment and response helpers for native v2 delivery."""

from __future__ import annotations

import mimetypes
import json
import os
import re
import secrets
import shutil
from pathlib import Path
from typing import Any


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


SPOOL_MARKER_NAME = ".pinchana-v2-spool.json"
SPOOL_REGISTRY_PREFIX = "pinchana:v2:spool-topology:"


def _positive_env_int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive integer") from exc
    if value < 1:
        raise RuntimeError(f"{name} must be a positive integer")
    return value


def _read_or_create_spool_marker(spool_path: Path, deployment_id: str) -> str:
    marker = spool_path / SPOOL_MARKER_NAME
    if marker.exists() and not marker.is_file():
        raise RuntimeError("Shared spool deployment marker is invalid")
    if not marker.exists():
        payload = {
            "version": 1,
            "deployment_id": deployment_id,
            "storage_id": secrets.token_urlsafe(24),
        }
        try:
            with marker.open("x", encoding="utf-8") as handle:
                json.dump(payload, handle, separators=(",", ":"))
        except FileExistsError:
            pass
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("Shared spool deployment marker is unreadable") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("version") != 1
        or payload.get("deployment_id") != deployment_id
        or not isinstance(payload.get("storage_id"), str)
        or len(payload["storage_id"]) < 24
    ):
        raise RuntimeError("Shared spool deployment marker does not match this deployment")
    return payload["storage_id"]


def validate_spool_topology() -> dict[str, Any]:
    """Validate local storage and fail closed for inaccessible replica spools."""
    try:
        replicas = int(os.getenv("PINCHANA_API_REPLICAS", "1"))
    except ValueError as exc:
        raise RuntimeError("PINCHANA_API_REPLICAS must be a positive integer") from exc
    if replicas < 1:
        raise RuntimeError("PINCHANA_API_REPLICAS must be a positive integer")
    configured_path = os.getenv("V2_SPOOL_PATH", "").strip()
    shared = env_flag("V2_SPOOL_SHARED")
    if replicas > 1:
        if not os.getenv("REDIS_URL", "").strip():
            raise RuntimeError("Multiple API replicas require REDIS_URL for shared v2 state")
        if not shared:
            raise RuntimeError(
                "Multiple API replicas require V2_SPOOL_SHARED=true and the same shared "
                "V2_SPOOL_PATH mounted in every replica"
            )
        if not configured_path:
            raise RuntimeError("Multiple API replicas require V2_SPOOL_PATH")
    if not configured_path:
        return {"configured": False, "replicas": replicas, "shared": False}
    spool_path = Path(configured_path)
    if replicas > 1:
        if not spool_path.is_absolute():
            raise RuntimeError("Shared V2_SPOOL_PATH must be absolute")
    require_existing = env_flag("V2_SPOOL_REQUIRE_EXISTING")
    if require_existing and not spool_path.exists():
        raise RuntimeError("V2 spool directory does not exist")
    try:
        spool_path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RuntimeError("V2 spool directory cannot be initialized") from exc
    if not spool_path.is_dir():
        raise RuntimeError("V2 spool storage is not a directory")
    try:
        probe = spool_path / f".pinchana-write-probe-{os.getpid()}-{secrets.token_hex(4)}"
        probe.write_bytes(b"ok")
        probe.unlink()
    except OSError as exc:
        raise RuntimeError("V2 spool storage is not writable and cleanable") from exc
    minimum_free = _positive_env_int("V2_SPOOL_MIN_FREE_BYTES", 64 * 1024 * 1024)
    try:
        free_bytes = shutil.disk_usage(spool_path).free
    except OSError as exc:
        raise RuntimeError("V2 spool capacity cannot be inspected") from exc
    if free_bytes < minimum_free:
        raise RuntimeError("V2 spool storage has insufficient free space")
    storage_id = None
    deployment_id = os.getenv("PINCHANA_DEPLOYMENT_ID", "").strip()
    if shared:
        if len(deployment_id) < 8:
            raise RuntimeError("Shared spool requires PINCHANA_DEPLOYMENT_ID")
        storage_id = _read_or_create_spool_marker(spool_path.resolve(), deployment_id)
    return {
        "configured": True,
        "replicas": replicas,
        "shared": shared,
        "storage_id": storage_id,
        "free_bytes": free_bytes,
    }


async def validate_shared_spool_registry(redis_client: Any, status: dict[str, Any]) -> None:
    """Use Redis to prove every replica sees the same initialized spool volume."""
    if not status.get("shared"):
        return
    deployment_id = os.getenv("PINCHANA_DEPLOYMENT_ID", "").strip()
    storage_id = status.get("storage_id")
    if not deployment_id or not storage_id:
        raise RuntimeError("Shared spool identity is unavailable")
    key = f"{SPOOL_REGISTRY_PREFIX}{deployment_id}"
    created = await redis_client.set(key, storage_id, nx=True)
    registered = storage_id if created else await redis_client.get(key)
    if not isinstance(registered, str):
        registered = registered.decode("utf-8") if registered else ""
    if not secrets.compare_digest(registered, storage_id):
        raise RuntimeError("API replicas do not share the same V2 spool storage")


def validate_internal_token() -> None:
    environment = os.getenv("PINCHANA_ENV", os.getenv("ENVIRONMENT", "development"))
    production = environment.strip().lower() in {"prod", "production", "staging"}
    if not production or not (
        env_flag("PINCHANA_V2_TIKTOK") or env_flag("PINCHANA_V2_SOUNDCLOUD")
    ):
        return
    if len(os.getenv("PINCHANA_INTERNAL_TOKEN", "")) < 32:
        raise RuntimeError(
            "Native credential resolvers require a PINCHANA_INTERNAL_TOKEN of at least "
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
