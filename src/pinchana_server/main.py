"""Pinchana Server — dynamically loads plugins or manages containers."""

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import shutil
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.exception_handlers import http_exception_handler, request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, Response, StreamingResponse
from starlette.background import BackgroundTask
from starlette.exceptions import HTTPException as StarletteHTTPException
import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from pinchana_core.models import ScrapeRequest, ScrapeResponse
from pinchana_core.plugins import registry
from pinchana_core.storage import MediaStorage
from pinchana_core.docker_manager import ContainerRegistry, ModuleContainerManager
from pinchana_core.vpn import GluetunController, VpnRotationError

from .media_probe import MediaDimensionProbe
from .mobile_auth import MobileGrantError, MobileGrantStore, MobileInstallation
from .response_adapter import normalize_inspect_response, normalize_scrape_response
from .schemas import ApiErrorResponse, InspectV1Response, ScrapeV1Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TURNSTILE_SITEVERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
TURNSTILE_TEST_SECRET_KEYS = {
    "1x0000000000000000000000000000000AA",
    "2x0000000000000000000000000000000AA",
    "3x0000000000000000000000000000000AA",
}
YOUTUBE_DUB_LANGUAGES = {
    "af", "az", "id", "ms", "bs", "ca", "cs", "da", "de", "et", "en-IN", "en-GB", "en",
    "es", "es-419", "es-US", "eu", "fil", "fr", "fr-CA", "gl", "hr", "zu", "is", "it", "sw",
    "lv", "lt", "hu", "nl", "no", "uz", "pl", "pt-PT", "pt", "ro", "sq", "sk", "sl",
    "sr-Latn", "fi", "sv", "vi", "tr", "be", "bg", "ky", "kk", "mk", "mn", "ru", "sr", "uk",
    "el", "hy", "iw", "ur", "ar", "fa", "ne", "mr", "hi", "as", "bn", "pa", "gu", "or", "ta",
    "te", "kn", "ml", "si", "th", "lo", "my", "ka", "am", "km", "zh-CN", "zh-TW", "zh-HK",
    "ja", "ko",
}
FILENAME_STYLES = {"classic", "basic", "pretty", "nerdy"}
BUILD_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{7,40}$", re.IGNORECASE)
BUILD_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,31}$")
BUILD_REPOSITORY_PATTERN = re.compile(r"^https://github\.com/Pinchana/[A-Za-z0-9_.-]+$")
BUILD_VERSION_PATTERN = re.compile(
    r"^\d{2}\.(?:0[1-9]|1[0-2])\.(?:[1-9]\d*)"
    r"(?:\+dev\.[0-9a-f]{7,40})?$",
    re.IGNORECASE,
)
GIF_MAX_INPUT_BYTES = 50 * 1024 * 1024
GIF_MAX_OUTPUT_BYTES = 50 * 1024 * 1024
GIF_MAX_DURATION_SECONDS = 60.0
GIF_PROCESS_TIMEOUT_SECONDS = 45.0
GIF_FILTER = (
    "fps=12,scale='min(960,iw)':-2:flags=lanczos,split[frames][palette_source];"
    "[palette_source]palettegen=max_colors=128:stats_mode=diff[palette];"
    "[frames][palette]paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle"
)

# ---------------------------------------------------------------------------
# 1. In-process plugin discovery (optional — for local dev)
# ---------------------------------------------------------------------------
SCRAPER_MODULES = os.getenv("IN_PROCESS_PLUGINS", "").split(",")
for mod_name in SCRAPER_MODULES:
    mod_name = mod_name.strip()
    if not mod_name:
        continue
    try:
        import importlib
        importlib.import_module(mod_name)
        logger.info("Loaded in-process plugin: %s", mod_name)
    except ImportError as e:
        logger.debug("In-process plugin not available: %s (%s)", mod_name, e)

# ---------------------------------------------------------------------------
# 2. Container registry (always available — reads module endpoints from config)
# ---------------------------------------------------------------------------
container_registry = ContainerRegistry()

# ---------------------------------------------------------------------------
# 3. Container module manager (optional — for runtime build/start/stop)
# ---------------------------------------------------------------------------
container_manager: ModuleContainerManager | None = None
if os.getenv("CONTAINER_MODE", "false").lower() in ("1", "true", "yes"):
    try:
        container_manager = ModuleContainerManager()
        for name in list(container_manager.modules.keys()):
            container_manager.start(name)
        logger.info("Container manager initialized with %d modules", len(container_manager.modules))
    except Exception as e:
        logger.warning("Container manager failed to initialize: %s", e)

gluetun = GluetunController()

# ---------------------------------------------------------------------------
# 4. FastAPI app
# ---------------------------------------------------------------------------
storage = MediaStorage(
    base_path=os.getenv("CACHE_PATH", "./cache"),
    max_size_gb=float(os.getenv("CACHE_MAX_SIZE_GB", "10.0")),
)
dimension_probe = MediaDimensionProbe(storage.base_path)

forward_client: httpx.AsyncClient | None = None
internal_client: httpx.AsyncClient | None = None
gif_conversion_slots = asyncio.Semaphore(2)
gif_conversion_sessions: set[str] = set()
gif_conversion_sessions_lock = asyncio.Lock()
mobile_grant_store: MobileGrantStore | None = None


class WebVerifyRequest(BaseModel):
    token: str = Field(min_length=1, max_length=4096)


class WebSessionResponse(BaseModel):
    access_token: str
    expires_at: int


class MobileChallengeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    installation_id: str = Field(
        min_length=16,
        max_length=128,
        pattern=r"^[A-Za-z0-9_-]+$",
    )
    platform: Literal["ios", "android"]
    app_id: str = Field(
        min_length=3,
        max_length=255,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]+$",
    )


class MobileChallengeResponse(BaseModel):
    challenge_id: str
    challenge: str
    expires_at: int
    providers: list[Literal["app_attest", "play_integrity", "guest"]]
    guest_allowed: bool


class MobileGrantRequest(MobileChallengeRequest):
    challenge_id: str = Field(min_length=16, max_length=128)
    challenge: str = Field(min_length=32, max_length=256)
    provider: Literal["app_attest", "play_integrity", "guest"]
    evidence: str | None = Field(default=None, max_length=64_000)


class MobileSessionRefreshRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    refresh_token: str = Field(min_length=32, max_length=512)


class MobileSessionGrantResponse(BaseModel):
    access_token: str
    access_expires_at: int
    refresh_token: str
    refresh_expires_at: int
    installation_id: str
    trust: Literal["attested", "guest"]


class GifConversionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    platform: str = Field(min_length=1, max_length=32, pattern=r"^[a-z0-9][a-z0-9_-]*$")
    postId: str = Field(min_length=1, max_length=256)
    filename: str = Field(min_length=1, max_length=1024)

    @field_validator("postId")
    @classmethod
    def safe_post_id(cls, value: str) -> str:
        if value.startswith("/") or ".." in value or "/" in value or "\\" in value or "\x00" in value:
            raise ValueError("invalid media path")
        return value

    @field_validator("filename")
    @classmethod
    def safe_filename(cls, value: str) -> str:
        if value.startswith("/") or ".." in value or "\\" in value or "\x00" in value:
            raise ValueError("invalid media path")
        return value


class DlpCookiesEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal[2]
    keyId: str = Field(min_length=8, max_length=128)
    clientPubKey: str = Field(min_length=40, max_length=64)
    salt: str = Field(min_length=20, max_length=64)
    iv: str = Field(min_length=12, max_length=32)
    ciphertext: str = Field(min_length=20, max_length=480_000)


class DlpSubmitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: str = Field(min_length=10, max_length=2048)
    quality: Literal[
        "best", "8k", "4k", "1440p", "1080p", "720p", "480p", "360p", "240p", "144p", "audio"
    ] = "best"
    codec: Literal["auto", "h264", "av1", "vp9"] = "auto"
    container: Literal["auto", "mp4", "webm", "mkv"] = "auto"
    audioFormat: Literal["best", "mp3", "ogg", "wav", "opus"] = "best"
    audioBitrate: Literal["320", "256", "128", "96", "64", "8"] = "128"
    preferBetterAudio: bool = False
    dubLanguage: str = Field(default="original", min_length=2, max_length=16)
    filenameStyle: Literal["classic", "basic", "pretty", "nerdy"] | None = None
    subtitleLanguage: str | None = Field(default=None, min_length=2, max_length=16)
    cookiesEnc: DlpCookiesEnvelope | None = None

    @field_validator("dubLanguage")
    @classmethod
    def valid_dub_language(cls, value: str) -> str:
        if value != "original" and value not in YOUTUBE_DUB_LANGUAGES:
            raise ValueError("unsupported YouTube dub language")
        return value

    @field_validator("subtitleLanguage")
    @classmethod
    def valid_subtitle_language(cls, value: str | None) -> str | None:
        if value is not None and value != "none" and value not in YOUTUBE_DUB_LANGUAGES:
            raise ValueError("unsupported YouTube subtitle language")
        return value


def _public_build_version() -> str:
    version = os.getenv("PINCHANA_BUILD_VERSION", "").strip()
    return version if BUILD_VERSION_PATTERN.fullmatch(version) else "development"


def _public_build_manifest() -> dict[str, Any]:
    """Return only validated public source revisions from the baked manifest."""
    raw_manifest = os.getenv("PINCHANA_BUILD_COMMITS", "").strip()
    try:
        parsed = json.loads(raw_manifest) if raw_manifest else {}
    except json.JSONDecodeError:
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}

    fallback_commit = os.getenv("PINCHANA_BUILD_COMMIT", "").strip()
    if BUILD_COMMIT_PATTERN.fullmatch(fallback_commit) and "api" not in parsed:
        parsed["api"] = {
            "commit": fallback_commit,
            "repository": "https://github.com/Pinchana/pinchana-api",
        }

    commits: dict[str, dict[str, str]] = {}
    for name, value in parsed.items():
        if not isinstance(name, str) or not BUILD_NAME_PATTERN.fullmatch(name):
            continue
        if isinstance(value, str):
            commit = value
            repository = ""
        elif isinstance(value, dict):
            commit = value.get("commit", "")
            repository = value.get("repository", "")
        else:
            continue
        if not isinstance(commit, str) or not BUILD_COMMIT_PATTERN.fullmatch(commit):
            continue
        entry = {"commit": commit.lower()}
        if isinstance(repository, str) and BUILD_REPOSITORY_PATTERN.fullmatch(repository):
            entry["repository"] = repository
        commits[name] = entry

    return {"version": _public_build_version(), "commits": commits}


def _configured_api_keys() -> dict[str, str]:
    raw = os.getenv("PINCHANA_API_KEYS", "")
    if not raw:
        return {}
    try:
        keys = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("PINCHANA_API_KEYS must be a JSON object")
        raise RuntimeError("API key configuration is invalid") from exc
    if not isinstance(keys, dict) or not all(
        isinstance(name, str) and isinstance(value, str) and name and value
        for name, value in keys.items()
    ):
        raise RuntimeError("PINCHANA_API_KEYS must map client names to non-empty secrets")
    return keys


def _require_api_key(x_api_key: str | None = Header(default=None)) -> str:
    try:
        keys = _configured_api_keys()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail="API authentication is not configured") from exc
    for client_name, candidate in keys.items():
        if x_api_key and hmac.compare_digest(x_api_key, candidate):
            return client_name
    raise HTTPException(status_code=401, detail="Invalid or missing API key")


def _urlsafe_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _urlsafe_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _session_secret() -> bytes:
    secret = os.getenv("TURNSTILE_SESSION_SECRET", "")
    if len(secret) < 32:
        raise HTTPException(status_code=503, detail="Web authentication is not configured")
    return secret.encode("utf-8")


def _session_max_age() -> int:
    try:
        return max(60, min(int(os.getenv("TURNSTILE_SESSION_MAX_AGE", "43200")), 86400))
    except ValueError:
        return 43200


def _issue_web_session() -> tuple[str, int]:
    now = int(time.time())
    expires_at = now + _session_max_age()
    payload = _urlsafe_encode(
        json.dumps(
            {"iat": now, "exp": expires_at, "nonce": secrets.token_urlsafe(12)},
            separators=(",", ":"),
        ).encode("utf-8")
    )
    signature = _urlsafe_encode(
        hmac.new(_session_secret(), payload.encode("ascii"), hashlib.sha256).digest()
    )
    return f"{payload}.{signature}", expires_at


def _validate_web_session(token: str) -> dict[str, Any]:
    try:
        payload, signature = token.split(".", 1)
        expected = _urlsafe_encode(
            hmac.new(_session_secret(), payload.encode("ascii"), hashlib.sha256).digest()
        )
        if not hmac.compare_digest(signature, expected):
            raise ValueError("signature mismatch")
        claims = json.loads(_urlsafe_decode(payload))
        if not isinstance(claims, dict) or int(claims["exp"]) <= int(time.time()):
            raise ValueError("session expired")
        return claims
    except (ValueError, KeyError, TypeError, json.JSONDecodeError, binascii.Error) as exc:
        raise HTTPException(status_code=401, detail="Invalid or expired web session") from exc


def _require_web_session(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="Invalid or missing web session")
    return _validate_web_session(token)


MOBILE_ALL_SCOPES = {
    "mobile:scrape",
    "mobile:media",
    "mobile:capabilities",
    "mobile:dlp",
}
MOBILE_ANONYMOUS_SCOPES = {
    "mobile:scrape",
    "mobile:media",
    "mobile:capabilities",
}


def _bounded_int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        return max(minimum, min(int(os.getenv(name, str(default))), maximum))
    except ValueError:
        return default


def _mobile_auth_mode() -> Literal["disabled", "guest", "attested", "hybrid"]:
    mode = os.getenv("MOBILE_AUTH_MODE", "disabled").strip().lower()
    if mode not in {"disabled", "guest", "attested", "hybrid"}:
        logger.error("Invalid MOBILE_AUTH_MODE=%s", mode)
        return "disabled"
    return mode  # type: ignore[return-value]


def _mobile_auth_required() -> bool:
    return os.getenv("MOBILE_AUTH_REQUIRED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _mobile_auth_providers() -> list[Literal["app_attest", "play_integrity", "guest"]]:
    mode = _mobile_auth_mode()
    providers: list[Literal["app_attest", "play_integrity", "guest"]] = []
    verifier_url = os.getenv("MOBILE_ATTESTATION_URL", "").strip()
    verifier_token = os.getenv("MOBILE_ATTESTATION_TOKEN", "")
    if mode in {"attested", "hybrid"} and verifier_url and len(verifier_token) >= 32:
        providers.extend(["app_attest", "play_integrity"])
    if mode in {"guest", "hybrid"}:
        providers.append("guest")
    return providers


def _mobile_session_secret() -> bytes:
    secret = os.getenv("MOBILE_SESSION_SECRET", "")
    lowered = secret.lower()
    if (
        len(secret) < 32
        or "replace-with" in lowered
        or "change-me" in lowered
        or "example" in lowered
    ):
        raise HTTPException(status_code=503, detail="Mobile authentication is not configured")
    return secret.encode("utf-8")


def _mobile_store() -> MobileGrantStore:
    global mobile_grant_store
    configured_path = Path(
        os.getenv(
            "MOBILE_SESSION_DB_PATH",
            str(Path(os.getenv("CACHE_PATH", "./cache")) / "mobile-sessions.sqlite3"),
        )
    )
    if mobile_grant_store is None or mobile_grant_store.path != configured_path:
        mobile_grant_store = MobileGrantStore(configured_path)
        mobile_grant_store.initialize()
    return mobile_grant_store


def _mobile_access_max_age() -> int:
    return _bounded_int_env("MOBILE_ACCESS_TOKEN_MAX_AGE", 900, 300, 1800)


def _mobile_refresh_max_age() -> int:
    return _bounded_int_env("MOBILE_REFRESH_TOKEN_MAX_AGE", 2_592_000, 86_400, 7_776_000)


def _mobile_scopes(trust: str) -> list[str]:
    if trust == "attested":
        return sorted(MOBILE_ALL_SCOPES)
    configured = os.getenv(
        "MOBILE_GUEST_SCOPES",
        "mobile:scrape,mobile:media,mobile:capabilities",
    )
    return sorted(
        {
            scope.strip()
            for scope in configured.split(",")
            if scope.strip() in MOBILE_ALL_SCOPES
        }
    )


def _issue_mobile_access_session(installation: MobileInstallation) -> tuple[str, int]:
    now = int(time.time())
    expires_at = now + _mobile_access_max_age()
    claims = {
        "v": 1,
        "iss": "pinchana",
        "aud": "pinchana-mobile",
        "typ": "mobile_access",
        "sub": installation.installation_id,
        "platform": installation.platform,
        "app_id": installation.app_id,
        "trust": installation.trust,
        "scope": _mobile_scopes(installation.trust),
        "jti": secrets.token_urlsafe(12),
        "iat": now,
        "exp": expires_at,
    }
    payload = _urlsafe_encode(
        json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    signature = _urlsafe_encode(
        hmac.new(_mobile_session_secret(), payload.encode("ascii"), hashlib.sha256).digest()
    )
    return f"{payload}.{signature}", expires_at


def _validate_mobile_session(token: str) -> dict[str, Any]:
    try:
        payload, signature = token.split(".", 1)
        expected = _urlsafe_encode(
            hmac.new(_mobile_session_secret(), payload.encode("ascii"), hashlib.sha256).digest()
        )
        if not hmac.compare_digest(signature, expected):
            raise ValueError("signature mismatch")
        claims = json.loads(_urlsafe_decode(payload))
        now = int(time.time())
        if (
            not isinstance(claims, dict)
            or claims.get("iss") != "pinchana"
            or claims.get("aud") != "pinchana-mobile"
            or claims.get("typ") != "mobile_access"
            or not isinstance(claims.get("sub"), str)
            or not isinstance(claims.get("jti"), str)
            or not isinstance(claims.get("scope"), list)
            or not all(isinstance(scope, str) for scope in claims["scope"])
            or int(claims["iat"]) > now + 60
            or int(claims["exp"]) <= now
        ):
            raise ValueError("invalid mobile claims")
        return claims
    except (ValueError, KeyError, TypeError, json.JSONDecodeError, binascii.Error) as exc:
        raise HTTPException(status_code=401, detail="Invalid or expired mobile session") from exc


def _require_mobile_session(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="Invalid or missing mobile session")
    return _validate_mobile_session(token)


def _optional_mobile_session(
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    if authorization:
        return _require_mobile_session(authorization)
    if _mobile_auth_required():
        raise HTTPException(status_code=401, detail="Invalid or missing mobile session")
    return {
        "typ": "mobile_anonymous",
        "trust": "anonymous",
        "scope": sorted(MOBILE_ANONYMOUS_SCOPES),
    }


def _require_mobile_scope(claims: dict[str, Any], required_scope: str) -> dict[str, Any]:
    if required_scope not in claims.get("scope", []):
        raise HTTPException(status_code=403, detail="Mobile session does not grant this operation")
    return claims


def _mobile_scrape_session(
    claims: dict[str, Any] = Depends(_optional_mobile_session),
) -> dict[str, Any]:
    return _require_mobile_scope(claims, "mobile:scrape")


def _mobile_media_session(
    claims: dict[str, Any] = Depends(_optional_mobile_session),
) -> dict[str, Any]:
    return _require_mobile_scope(claims, "mobile:media")


def _mobile_capabilities_session(
    claims: dict[str, Any] = Depends(_optional_mobile_session),
) -> dict[str, Any]:
    return _require_mobile_scope(claims, "mobile:capabilities")


def _require_mobile_dlp_session(
    claims: dict[str, Any] = Depends(_require_mobile_session),
) -> dict[str, Any]:
    return _require_mobile_scope(claims, "mobile:dlp")


def _mobile_remote_hash(request: Request) -> str:
    remote = request.client.host if request.client else "unknown"
    if os.getenv("MOBILE_TRUST_PROXY_HEADERS", "false").lower() in {"1", "true", "yes"}:
        forwarded = request.headers.get("x-forwarded-for", "").partition(",")[0].strip()
        remote = request.headers.get("cf-connecting-ip", "").strip() or forwarded or remote
    return hmac.new(
        _mobile_session_secret(),
        remote.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


async def _verify_mobile_attestation(request: MobileGrantRequest) -> str:
    providers = _mobile_auth_providers()
    if request.provider not in providers:
        raise HTTPException(status_code=403, detail="This mobile grant provider is not allowed")
    if request.provider == "guest":
        if request.evidence is not None:
            raise HTTPException(status_code=400, detail="Guest grants do not accept attestation evidence")
        return "guest"
    if request.provider == "app_attest" and request.platform != "ios":
        raise HTTPException(status_code=400, detail="App Attest is only valid for iOS")
    if request.provider == "play_integrity" and request.platform != "android":
        raise HTTPException(status_code=400, detail="Play Integrity is only valid for Android")
    if not request.evidence:
        raise HTTPException(status_code=400, detail="Attestation evidence is required")

    verifier_url = os.getenv("MOBILE_ATTESTATION_URL", "").strip()
    verifier_token = os.getenv("MOBILE_ATTESTATION_TOKEN", "")
    if not verifier_url or len(verifier_token) < 32 or forward_client is None:
        raise HTTPException(status_code=503, detail="Mobile attestation verifier is not configured")
    try:
        response = await forward_client.post(
            verifier_url,
            headers={"Authorization": f"Bearer {verifier_token}"},
            json=request.model_dump(mode="json"),
            timeout=httpx.Timeout(15, connect=5),
        )
        response.raise_for_status()
        result = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("mobile_attestation_unavailable provider=%s", request.provider)
        raise HTTPException(status_code=503, detail="Mobile attestation is temporarily unavailable") from exc
    if (
        not isinstance(result, dict)
        or result.get("valid") is not True
        or result.get("platform") != request.platform
        or result.get("app_id") != request.app_id
    ):
        raise HTTPException(status_code=403, detail="Mobile attestation failed")
    return "attested"


def _mobile_grant_response(
    installation: MobileInstallation,
    refresh_token: str,
    refresh_expires_at: int,
) -> MobileSessionGrantResponse:
    access_token, access_expires_at = _issue_mobile_access_session(installation)
    return MobileSessionGrantResponse(
        access_token=access_token,
        access_expires_at=access_expires_at,
        refresh_token=refresh_token,
        refresh_expires_at=refresh_expires_at,
        installation_id=installation.installation_id,
        trust=installation.trust,  # type: ignore[arg-type]
    )


def _dlp_enabled() -> bool:
    return os.getenv("DLP_ENABLED", "false").lower() in {"1", "true", "yes"}


def _valid_dlp_secret(value: str) -> bool:
    lowered = value.lower()
    return len(value) >= 32 and "replace-with" not in lowered and "disabled-change-me" not in lowered


def _dlp_config() -> tuple[str, str]:
    url = os.getenv("DLP_URL", "").rstrip("/")
    token = os.getenv("DLP_GATEWAY_TOKEN", "")
    owner_secret = os.getenv("DLP_OWNER_SECRET", "")
    if (
        not _dlp_enabled()
        or not url
        or not _valid_dlp_secret(token)
        or not _valid_dlp_secret(owner_secret)
        or hmac.compare_digest(token, owner_secret)
    ):
        raise HTTPException(status_code=503, detail="Private downloads are unavailable")
    return url, token


def _dlp_job_owner(claims: dict[str, Any]) -> str:
    owner_id = claims.get("sub") if claims.get("typ") == "mobile_access" else claims.get("nonce")
    if not isinstance(owner_id, str) or not owner_id:
        raise HTTPException(status_code=401, detail="Invalid session owner")
    owner_secret = os.getenv("DLP_OWNER_SECRET", "").encode("utf-8") or _session_secret()
    return _urlsafe_encode(hmac.new(owner_secret, owner_id.encode("utf-8"), hashlib.sha256).digest())


async def _dlp_capabilities() -> dict[str, Any] | None:
    if forward_client is None:
        return None
    try:
        url, _token = _dlp_config()
        response = await forward_client.get(f"{url}/health", timeout=httpx.Timeout(3, connect=2))
        if response.status_code != 200:
            return None
        payload = response.json()
        if payload.get("status") != "ok" or payload.get("protocol") != 2 or payload.get("redis") is not True:
            return None
        capabilities = payload.get("capabilities")
        if not isinstance(capabilities, dict):
            return None
        required_lists = ("services", "qualities", "codecs", "containers", "audioFormats", "audioBitrates", "dubLanguages")
        if any(not isinstance(capabilities.get(field), list) for field in required_lists):
            return None
        optional_lists = ("filenameStyles", "subtitleLanguages")
        if any(field in capabilities and not isinstance(capabilities[field], list) for field in optional_lists):
            return None
        if capabilities.get("betterAudio") is not True or capabilities.get("services") != ["youtube"]:
            return None
        for field in optional_lists:
            capabilities.setdefault(field, [])
        return capabilities
    except (HTTPException, httpx.RequestError, ValueError, AttributeError):
        return None


async def _dlp_healthy() -> bool:
    return await _dlp_capabilities() is not None


def _dlp_headers(claims: dict[str, Any]) -> dict[str, str]:
    _url, token = _dlp_config()
    return {"x-dlp-service-token": token, "x-job-owner": _dlp_job_owner(claims)}


async def _dlp_json(method: str, path: str, claims: dict[str, Any], body: dict[str, Any] | None = None):
    if forward_client is None:
        raise HTTPException(status_code=503, detail="Gateway HTTP client is not ready")
    url, _token = _dlp_config()
    try:
        response = await forward_client.request(
            method,
            f"{url}{path}",
            headers=_dlp_headers(claims),
            json=body,
            timeout=httpx.Timeout(30, connect=5),
        )
    except httpx.RequestError as exc:
        logger.warning("dlp_upstream_unavailable path=%s error=%s", path, type(exc).__name__)
        raise HTTPException(status_code=503, detail="Private downloads are temporarily unavailable") from exc
    if response.status_code >= 400:
        try:
            detail = response.json().get("detail", "Private download request failed")
        except (ValueError, AttributeError):
            detail = "Private download request failed"
        if response.status_code >= 500:
            detail = "Private downloads are temporarily unavailable"
        raise HTTPException(status_code=response.status_code, detail=detail)
    return response.json()


def _turnstile_rejection_reason(result: Any, *, enforce_metadata: bool = True) -> str | None:
    if not isinstance(result, dict):
        return "invalid-response"
    if result.get("success") is not True:
        error_codes = result.get("error-codes")
        if isinstance(error_codes, list) and error_codes:
            return "cloudflare-rejected"
        return "unsuccessful"
    if not enforce_metadata:
        return None
    expected_hostname = os.getenv("TURNSTILE_EXPECTED_HOSTNAME", "").strip()
    if expected_hostname and result.get("hostname") != expected_hostname:
        return "hostname-mismatch"
    expected_action = os.getenv("TURNSTILE_EXPECTED_ACTION", "turnstile-spin-v1").strip()
    if expected_action and result.get("action") != expected_action:
        return "action-mismatch"
    return None


def _valid_turnstile_result(result: Any, *, enforce_metadata: bool = True) -> bool:
    return _turnstile_rejection_reason(result, enforce_metadata=enforce_metadata) is None


async def _initialize_mobile_auth() -> None:
    if _mobile_auth_mode() == "disabled":
        return
    try:
        _mobile_session_secret()
    except HTTPException:
        if _mobile_auth_required():
            raise
        logger.warning(
            "Mobile authentication is not configured; continuing with anonymous mobile access"
        )
        return
    await asyncio.to_thread(_mobile_store)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global forward_client, internal_client
    forward_client = httpx.AsyncClient(
        timeout=httpx.Timeout(120.0, connect=10.0),
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
    )
    internal_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://pinchana.internal",
        timeout=120.0,
    )
    await _initialize_mobile_auth()
    try:
        yield
    finally:
        await forward_client.aclose()
        await internal_client.aclose()
        await storage.close()


app = FastAPI(title="Pinchana Server", version=_public_build_version(), lifespan=lifespan)


def _is_v1_request(request: Request) -> bool:
    return request.url.path.startswith("/v1/")


def _error_message(detail: Any, fallback: str) -> str:
    if isinstance(detail, dict):
        nested = detail.get("detail")
        if nested is not None:
            return _error_message(nested, fallback)
        message = detail.get("message")
        return message if isinstance(message, str) and message else fallback
    if isinstance(detail, str):
        try:
            parsed = json.loads(detail)
        except json.JSONDecodeError:
            return detail or fallback
        return _error_message(parsed, fallback)
    return fallback


def _upstream_error_code(detail: Any) -> str | None:
    if isinstance(detail, dict):
        nested = detail.get("detail")
        if nested is not None:
            return _upstream_error_code(nested)
        code = detail.get("code")
        return code if isinstance(code, str) and code else None
    if isinstance(detail, str):
        try:
            parsed = json.loads(detail)
        except json.JSONDecodeError:
            return None
        return _upstream_error_code(parsed)
    return None


def _http_error_code(status_code: int, detail: Any) -> tuple[str, str]:
    raw_message = _error_message(detail, "Request failed")
    upstream_code = _upstream_error_code(detail)
    if upstream_code in {
        "authentication_required",
        "restricted_media",
        "not_found",
        "rate_limited",
        "extraction_failed",
    }:
        return upstream_code, raw_message
    lowered = raw_message.lower()
    if status_code == 400 and "no module handles" in lowered:
        return "unsupported_url", "No scraper supports this URL"
    if status_code == 400:
        return "invalid_url", raw_message
    if status_code == 401 and "web session" in lowered:
        return "unauthorized", "Invalid or missing web session"
    if status_code == 401 and "mobile session" in lowered:
        return "unauthorized", raw_message
    mapping = {
        401: ("unauthorized", "Invalid or missing API key"),
        403: ("forbidden", raw_message),
        404: ("not_found", raw_message),
        429: ("rate_limited", "The upstream service is rate limited"),
        500: ("internal_error", "The scraper failed to process this URL"),
        502: ("invalid_upstream_response", "The scraper returned an invalid response"),
        503: ("service_unavailable", raw_message),
    }
    return mapping.get(status_code, ("http_error", raw_message))


@app.exception_handler(StarletteHTTPException)
async def api_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if not _is_v1_request(request):
        return await http_exception_handler(request, exc)
    code, message = _http_error_code(exc.status_code, exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": code, "message": message, "details": None}},
        headers=exc.headers,
    )


@app.exception_handler(RequestValidationError)
async def api_validation_exception_handler(request: Request, exc: RequestValidationError):
    if not _is_v1_request(request):
        return await request_validation_exception_handler(request, exc)
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": "validation_error",
                "message": "Request validation failed",
                "details": jsonable_encoder(exc.errors()),
            }
        },
    )


@app.exception_handler(Exception)
async def api_unhandled_exception_handler(request: Request, exc: Exception):
    if not _is_v1_request(request):
        logger.exception("unhandled_request_error path=%s", request.url.path, exc_info=exc)
        return PlainTextResponse(status_code=500, content="Internal Server Error")
    logger.exception("v1_unhandled_request_error path=%s", request.url.path, exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "internal_error",
                "message": "An internal error occurred",
                "details": None,
            }
        },
    )

# Mount in-process plugin routers (if any)
for name, plugin in registry.items():
    app.include_router(plugin.router, prefix=f"/{name}", tags=[name])
    logger.info("Mounted in-process router: /%s", name)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _resolve_module(url: str):
    """Find the best module (in-process or container) for a URL."""
    url_lower = url.lower()

    # 1. In-process plugin match
    for name, plugin in registry.items():
        for pattern in plugin.route_patterns:
            if pattern.lower() in url_lower:
                return "in_process", name, plugin

    # 2. Container module match
    for name, module in container_registry.modules.items():
        for pattern in module.route_patterns:
            if pattern.lower() in url_lower:
                return "container", name, module

    return None, None, None


async def _forward_to_container(
    module_name: str,
    request: ScrapeRequest,
    *,
    operation: str = "scrape",
) -> dict[str, Any]:
    module = container_registry.modules.get(module_name)
    if not module:
        raise HTTPException(status_code=404, detail=f"Container module {module_name} not configured")

    endpoint = module.endpoint
    if forward_client is None:
        raise HTTPException(status_code=503, detail="Gateway HTTP client is not ready")
    logger.info(
        "scrape_forward module=%s endpoint=%s url=%s",
        module_name, endpoint, request.url,
    )
    try:
        resp = await forward_client.post(
            f"{endpoint}/{operation}",
            json={"url": str(request.url)},
        )
    except httpx.RequestError as exc:
        logger.error("Upstream module %s (%s) is unreachable: %s", module_name, endpoint, exc)
        raise HTTPException(
            status_code=503,
            detail=f"The {module_name} scraper is temporarily unavailable",
        ) from exc
    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.error(
            "Upstream module %s (%s) returned %s for /%s: %s",
            module_name, endpoint, resp.status_code, operation, resp.text,
        )
        raise HTTPException(status_code=resp.status_code, detail=resp.text) from e
    payload = resp.json()
    if not isinstance(payload, dict):
        logger.error("Upstream module %s returned a non-object scrape payload", module_name)
        raise HTTPException(status_code=502, detail="The scraper returned an invalid response")
    return payload


def _rewrite_media_urls(value: Any, prefix: str) -> Any:
    if isinstance(value, str) and value.startswith("/media/"):
        return f"{prefix}{value.removeprefix('/media')}"
    if isinstance(value, list):
        return [_rewrite_media_urls(item, prefix) for item in value]
    if isinstance(value, dict):
        return {key: _rewrite_media_urls(item, prefix) for key, item in value.items()}
    return value


async def _process_scrape_payload(
    request: ScrapeRequest,
    http_request: Request,
    *,
    operation: str = "scrape",
) -> tuple[str, dict[str, Any]]:
    """Route a scrape and retain the complete module payload for v1 adapters."""
    url = str(request.url)
    client = http_request.client
    client_address = f"{client.host}:{client.port}" if client else "unknown"
    logger.info("scrape_request client=%s url=%s", client_address, url)

    mode, name, target = _resolve_module(url)

    if mode is None:
        plugin_patterns = {
            plugin_name: plugin.route_patterns
            for plugin_name, plugin in registry.items()
        }
        container_patterns = {
            module_name: module.route_patterns
            for module_name, module in container_registry.modules.items()
        }
        logger.warning(
            "scrape_rejected reason=no_matching_module client=%s url=%s "
            "plugin_patterns=%s container_patterns=%s",
            client_address, url, plugin_patterns, container_patterns,
        )
        raise HTTPException(
            status_code=400,
            detail="No module handles this URL. "
                   f"Plugins: {plugin_patterns}  "
                   f"Containers: {container_patterns}"
        )

    logger.info(
        "scrape_route_selected client=%s url=%s module=%s mode=%s patterns=%s",
        client_address, url, name, mode, target.route_patterns,
    )
    started = time.perf_counter()
    if mode == "in_process":
        if internal_client is None:
            raise HTTPException(status_code=503, detail="Internal HTTP client is not ready")
        resp = await internal_client.post(f"/{name}/{operation}", json={"url": url})
        if resp.status_code != 200:
            logger.error(
                "scrape_upstream_error module=%s mode=%s status=%s body=%s",
                name, mode, resp.status_code, resp.text,
            )
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        payload = resp.json()
        if not isinstance(payload, dict):
            logger.error("Upstream module %s returned a non-object scrape payload", name)
            raise HTTPException(status_code=502, detail="The scraper returned an invalid response")
    else:
        payload = await _forward_to_container(name, request, operation=operation)

    logger.info(
        "scrape_complete module=%s mode=%s elapsed_ms=%.1f",
        name, mode, (time.perf_counter() - started) * 1000,
    )
    return name, payload


async def _process_scrape_request(request: ScrapeRequest, http_request: Request) -> ScrapeResponse:
    """Return the existing public response shape for legacy clients."""
    _platform, payload = await _process_scrape_payload(request, http_request)
    return ScrapeResponse(**payload)


def _resolve_media_file(post_id: str, filename: str) -> Path:
    if ".." in filename or filename.startswith("/"):
        raise HTTPException(status_code=404, detail="Invalid path")

    resolved = (storage.base_path / post_id / filename).resolve()
    base_resolved = storage.base_path.resolve()
    try:
        resolved.relative_to(base_resolved)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Invalid path") from exc

    if not resolved.exists() or not resolved.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return resolved


def _serve_media_file(post_id: str, filename: str) -> FileResponse:
    return FileResponse(_resolve_media_file(post_id, filename))


async def _run_media_process(*args: str, timeout: float) -> tuple[int, bytes, bytes]:
    try:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail="Media conversion is unavailable") from exc
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except TimeoutError as exc:
        process.kill()
        await process.wait()
        raise HTTPException(status_code=504, detail="Media conversion timed out") from exc
    return process.returncode or 0, stdout[-4096:], stderr[-4096:]


async def _probe_media_duration(source: Path) -> float:
    return_code, stdout, stderr = await _run_media_process(
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(source),
        timeout=5.0,
    )
    if return_code != 0:
        logger.info("gif_conversion_rejected reason=probe_failed stderr_bytes=%d", len(stderr))
        raise HTTPException(status_code=422, detail="The media file cannot be converted")
    try:
        duration = float(stdout.strip())
    except (ValueError, OverflowError) as exc:
        raise HTTPException(status_code=422, detail="The media duration is unavailable") from exc
    if not 0 < duration <= GIF_MAX_DURATION_SECONDS:
        raise HTTPException(status_code=422, detail="GIF conversion supports media up to 60 seconds")
    return duration


async def _convert_media_to_gif(source: Path, output: Path) -> None:
    return_code, _stdout, stderr = await _run_media_process(
        "ffmpeg",
        "-nostdin", "-y", "-loglevel", "error",
        "-i", str(source),
        "-filter_complex", GIF_FILTER,
        "-loop", "0",
        "-fs", str(GIF_MAX_OUTPUT_BYTES),
        str(output),
        timeout=GIF_PROCESS_TIMEOUT_SECONDS,
    )
    if return_code != 0:
        logger.info("gif_conversion_failed reason=ffmpeg_exit stderr_bytes=%d", len(stderr))
        raise HTTPException(status_code=422, detail="The media file cannot be converted to GIF")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/web/identity")
async def web_identity():
    """Return the project-signed, origin-bound certificate for this instance."""
    raw_certificate = os.getenv("PINCHANA_INSTANCE_CERTIFICATE", "").strip()
    certificate_file = os.getenv("PINCHANA_INSTANCE_CERTIFICATE_FILE", "").strip()
    if not raw_certificate and certificate_file:
        try:
            raw_certificate = Path(certificate_file).read_text(encoding="utf-8").strip()
        except OSError as exc:
            logger.warning("instance_certificate_unavailable: %s", exc)
    if not raw_certificate:
        raise HTTPException(status_code=503, detail="Instance certificate is not configured")
    try:
        certificate = json.loads(raw_certificate)
        if (
            not isinstance(certificate, dict)
            or not isinstance(certificate.get("payload"), str)
            or not isinstance(certificate.get("signature"), str)
        ):
            raise ValueError("invalid certificate envelope")
    except (json.JSONDecodeError, ValueError) as exc:
        logger.error("instance_certificate_invalid: %s", exc)
        raise HTTPException(status_code=503, detail="Instance certificate is invalid") from exc
    return JSONResponse(
        content=certificate,
        headers={"Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=300"},
    )


@app.post("/scrape", response_model=ScrapeResponse)
async def process_scrape_request(
    request: ScrapeRequest,
    http_request: Request,
    client_name: str = Depends(_require_api_key),
):
    """Machine-to-machine scrape endpoint protected by a named API key."""
    logger.info("authenticated_scrape client_name=%s", client_name)
    return await _process_scrape_request(request, http_request)


@app.post(
    "/v1/scrape",
    response_model=ScrapeV1Response,
    responses={
        status: {"model": ApiErrorResponse}
        for status in (400, 401, 403, 404, 422, 429, 500, 502, 503)
    },
)
async def process_v1_scrape_request(
    request: ScrapeRequest,
    http_request: Request,
    client_name: str = Depends(_require_api_key),
):
    """Return a versioned, normalized scrape response for machine clients."""
    logger.info("authenticated_v1_scrape client_name=%s", client_name)
    return await _normalized_scrape_response(request, http_request)


@app.post(
    "/v1/inspect",
    response_model=InspectV1Response,
    responses={
        status: {"model": ApiErrorResponse}
        for status in (400, 401, 403, 404, 422, 429, 500, 502, 503)
    },
)
async def process_v1_inspect_request(
    request: ScrapeRequest,
    http_request: Request,
    client_name: str = Depends(_require_api_key),
):
    """Inspect a social post and its one-level quote without downloading media."""
    logger.info("authenticated_v1_inspect client_name=%s", client_name)
    platform, payload = await _process_scrape_payload(
        request,
        http_request,
        operation="inspect",
    )
    if platform not in {"twitter", "threads"}:
        raise HTTPException(status_code=400, detail="Inspection is supported for X and Threads")
    try:
        return await normalize_inspect_response(
            payload,
            platform=platform,
            source_url=str(request.url),
            probe=dimension_probe,
        )
    except (ValueError, ValidationError) as exc:
        logger.error("inspect_normalization_failed module=%s error=%s", platform, exc)
        raise HTTPException(status_code=502, detail="The scraper returned an invalid response") from exc


async def _normalized_scrape_response(
    request: ScrapeRequest,
    http_request: Request,
) -> ScrapeV1Response:
    """Run a scrape through the shared public v1 response adapter."""
    platform, payload = await _process_scrape_payload(request, http_request)
    try:
        return await normalize_scrape_response(
            payload,
            platform=platform,
            source_url=str(request.url),
            probe=dimension_probe,
        )
    except (ValueError, ValidationError) as exc:
        logger.error("scrape_normalization_failed module=%s error=%s", platform, exc)
        raise HTTPException(status_code=502, detail="The scraper returned an invalid response") from exc


@app.post(
    "/v1/web/scrape",
    response_model=ScrapeV1Response,
    responses={
        status: {"model": ApiErrorResponse}
        for status in (400, 401, 403, 404, 422, 429, 500, 502, 503)
    },
)
async def process_v1_web_scrape_request(
    request: ScrapeRequest,
    http_request: Request,
    _claims: dict[str, Any] = Depends(_require_web_session),
):
    """Return a normalized scrape response protected by a browser web session."""
    logger.info("authenticated_v1_web_scrape")
    result = await _normalized_scrape_response(request, http_request)
    rewritten = _rewrite_media_urls(result.model_dump(), "/web/media")
    return ScrapeV1Response.model_validate(rewritten)


@app.post(
    "/v1/mobile/scrape",
    response_model=ScrapeV1Response,
    responses={
        status: {"model": ApiErrorResponse}
        for status in (400, 401, 403, 404, 422, 429, 500, 502, 503)
    },
)
async def process_v1_mobile_scrape_request(
    request: ScrapeRequest,
    http_request: Request,
    claims: dict[str, Any] = Depends(_mobile_scrape_session),
):
    """Return a normalized scrape response with optional mobile authentication."""
    logger.info(
        "v1_mobile_scrape authenticated=%s",
        claims.get("typ") == "mobile_access",
    )
    result = await _normalized_scrape_response(request, http_request)
    rewritten = _rewrite_media_urls(result.model_dump(), "/mobile/media")
    return ScrapeV1Response.model_validate(rewritten)


@app.get("/v1/mobile/auth")
async def mobile_auth_policy():
    providers = _mobile_auth_providers()
    if not providers:
        if _mobile_auth_required():
            raise HTTPException(status_code=503, detail="Mobile authentication is disabled")
        return {
            "required": False,
            "providers": [],
            "guest_allowed": False,
            "access_token_max_age": _mobile_access_max_age(),
        }
    _mobile_session_secret()
    return {
        "required": _mobile_auth_required(),
        "providers": providers,
        "guest_allowed": "guest" in providers,
        "access_token_max_age": _mobile_access_max_age(),
    }


@app.post("/v1/mobile/challenges", response_model=MobileChallengeResponse)
async def mobile_challenge(request: MobileChallengeRequest, http_request: Request):
    providers = _mobile_auth_providers()
    if not providers:
        raise HTTPException(status_code=503, detail="Mobile authentication is disabled")
    challenge_ttl = _bounded_int_env("MOBILE_CHALLENGE_TTL", 120, 60, 300)
    try:
        challenge = await asyncio.to_thread(
            _mobile_store().create_challenge,
            installation_id=request.installation_id,
            platform=request.platform,
            app_id=request.app_id,
            remote_hash=_mobile_remote_hash(http_request),
            ttl_seconds=challenge_ttl,
            rate_window_seconds=_bounded_int_env(
                "MOBILE_CHALLENGE_RATE_WINDOW",
                600,
                60,
                3600,
            ),
            rate_limit=_bounded_int_env("MOBILE_CHALLENGE_RATE_LIMIT", 10, 1, 100),
        )
    except MobileGrantError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    return MobileChallengeResponse(
        challenge_id=challenge.challenge_id,
        challenge=challenge.challenge,
        expires_at=challenge.expires_at,
        providers=providers,
        guest_allowed="guest" in providers,
    )


@app.post("/v1/mobile/grants", response_model=MobileSessionGrantResponse)
@app.post(
    "/v1/mobile/attest",
    response_model=MobileSessionGrantResponse,
    deprecated=True,
)
async def mobile_grant(request: MobileGrantRequest):
    try:
        await asyncio.to_thread(
            _mobile_store().validate_challenge,
            challenge_id=request.challenge_id,
            challenge=request.challenge,
            installation_id=request.installation_id,
            platform=request.platform,
            app_id=request.app_id,
        )
    except MobileGrantError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    trust = await _verify_mobile_attestation(request)
    try:
        installation = await asyncio.to_thread(
            _mobile_store().consume_challenge,
            challenge_id=request.challenge_id,
            challenge=request.challenge,
            installation_id=request.installation_id,
            platform=request.platform,
            app_id=request.app_id,
            trust=trust,
        )
        refresh = await asyncio.to_thread(
            _mobile_store().issue_refresh,
            installation,
            ttl_seconds=_mobile_refresh_max_age(),
        )
    except MobileGrantError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    logger.info(
        "mobile_installation_granted platform=%s trust=%s",
        installation.platform,
        installation.trust,
    )
    return _mobile_grant_response(
        installation,
        refresh.refresh_token,
        refresh.refresh_expires_at,
    )


@app.post("/v1/mobile/session/refresh", response_model=MobileSessionGrantResponse)
async def mobile_session_refresh(request: MobileSessionRefreshRequest):
    try:
        refresh = await asyncio.to_thread(
            _mobile_store().rotate_refresh,
            request.refresh_token,
            ttl_seconds=_mobile_refresh_max_age(),
        )
    except MobileGrantError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    return _mobile_grant_response(
        refresh.installation,
        refresh.refresh_token,
        refresh.refresh_expires_at,
    )


@app.delete("/v1/mobile/session", status_code=204)
async def mobile_session_revoke(request: MobileSessionRefreshRequest):
    await asyncio.to_thread(
        _mobile_store().revoke_refresh_family,
        request.refresh_token,
    )
    return Response(status_code=204)


@app.post("/v1/mobile/verify", status_code=410)
async def mobile_verify():
    """Retired static-key exchange kept only as an explicit migration signal."""
    raise HTTPException(
        status_code=410,
        detail="Bundled mobile API keys are no longer accepted; use installation grants",
    )


@app.get("/v1/mobile/session")
async def mobile_session(claims: dict[str, Any] = Depends(_require_mobile_session)):
    return {
        "valid": True,
        "expires_at": claims["exp"],
        "installation_id": claims["sub"],
        "trust": claims["trust"],
        "scope": claims["scope"],
    }


@app.get("/v1/mobile/capabilities")
async def mobile_capabilities(
    claims: dict[str, Any] = Depends(_mobile_capabilities_session),
):
    capabilities = await _dlp_capabilities() if _dlp_enabled() else None
    available = capabilities is not None and "mobile:dlp" in claims["scope"]
    return JSONResponse(
        content={
            "available": available,
            "dlp": capabilities if available else None,
        }
    )


@app.post("/web/verify", response_model=WebSessionResponse)
async def web_verify(request: WebVerifyRequest):
    """Validate a one-use Turnstile token and issue a signed web session."""
    secret_key = os.getenv("TURNSTILE_SECRET_KEY", "")
    if not secret_key or forward_client is None:
        raise HTTPException(status_code=503, detail="Web verification is not configured")
    try:
        response = await forward_client.post(
            TURNSTILE_SITEVERIFY_URL,
            data={
                "secret": secret_key,
                "response": request.token,
                "idempotency_key": str(uuid.uuid4()),
            },
            timeout=10.0,
        )
        response.raise_for_status()
        result = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("turnstile_verification_unavailable: %s", exc)
        raise HTTPException(status_code=503, detail="Verification service unavailable") from exc
    enforce_metadata = secret_key not in TURNSTILE_TEST_SECRET_KEYS
    rejection_reason = _turnstile_rejection_reason(result, enforce_metadata=enforce_metadata)
    if rejection_reason is not None:
        error_codes = result.get("error-codes", []) if isinstance(result, dict) else []
        hostname = result.get("hostname") if isinstance(result, dict) else None
        action = result.get("action") if isinstance(result, dict) else None
        logger.info(
            "turnstile_verification_rejected reason=%s error_codes=%s hostname=%s action=%s",
            rejection_reason,
            error_codes,
            hostname,
            action,
        )
        raise HTTPException(status_code=403, detail="Verification failed")
    access_token, expires_at = _issue_web_session()
    return WebSessionResponse(access_token=access_token, expires_at=expires_at)


@app.get("/web/session")
async def web_session(claims: dict[str, Any] = Depends(_require_web_session)):
    return {"valid": True, "expires_at": claims["exp"]}


@app.get("/web/build")
async def web_build():
    """Expose only public source revisions; no session is required."""
    return JSONResponse(
        content=_public_build_manifest(),
        headers={"Cache-Control": "public, max-age=300"},
    )


@app.get("/web/capabilities")
async def web_capabilities(_claims: dict[str, Any] = Depends(_require_web_session)):
    """Advertise optional browser features without exposing service topology."""
    capabilities = await _dlp_capabilities() if _dlp_enabled() else None
    available = capabilities is not None
    return {
        "dlp": {
            "available": available,
            "protocol": 2 if available else None,
            "services": capabilities["services"] if capabilities else [],
            "qualities": capabilities["qualities"] if capabilities else [],
            "codecs": capabilities["codecs"] if capabilities else [],
            "containers": capabilities["containers"] if capabilities else [],
            "audioFormats": capabilities["audioFormats"] if capabilities else [],
            "audioBitrates": capabilities["audioBitrates"] if capabilities else [],
            "dubLanguages": capabilities["dubLanguages"] if capabilities else [],
            "filenameStyles": capabilities["filenameStyles"] if capabilities else [],
            "subtitleLanguages": capabilities["subtitleLanguages"] if capabilities else [],
            "betterAudio": capabilities["betterAudio"] if capabilities else False,
        },
        "mediaConversions": {
            "gif": {"serverFallback": True},
        },
    }


async def _acquire_gif_conversion(claims: dict[str, Any]) -> str:
    owner = claims.get("nonce")
    if not isinstance(owner, str) or not owner:
        raise HTTPException(status_code=401, detail="Invalid web session")
    async with gif_conversion_sessions_lock:
        if owner in gif_conversion_sessions:
            raise HTTPException(status_code=429, detail="A GIF conversion is already running for this session")
        if gif_conversion_slots.locked():
            raise HTTPException(status_code=429, detail="GIF conversion is busy; try again shortly")
        await gif_conversion_slots.acquire()
        gif_conversion_sessions.add(owner)
    return owner


async def _release_gif_conversion(owner: str) -> None:
    async with gif_conversion_sessions_lock:
        gif_conversion_sessions.discard(owner)
        gif_conversion_slots.release()


@app.post("/web/convert/gif")
async def web_convert_gif(
    request: GifConversionRequest,
    claims: dict[str, Any] = Depends(_require_web_session),
):
    source = _resolve_media_file(request.postId, request.filename)
    source_size = source.stat().st_size
    if source_size <= 0 or source_size > GIF_MAX_INPUT_BYTES:
        logger.info("gif_conversion_rejected reason=input_size bytes=%d", source_size)
        raise HTTPException(status_code=413, detail="GIF conversion supports files up to 50 MiB")

    owner = await _acquire_gif_conversion(claims)
    temporary_directory: Path | None = None
    started = time.perf_counter()
    try:
        duration = await _probe_media_duration(source)
        temporary_directory = Path(tempfile.mkdtemp(prefix="pinchana-gif-"))
        output = temporary_directory / "output.gif"
        await _convert_media_to_gif(source, output)
        if not output.is_file() or output.stat().st_size <= 0:
            raise HTTPException(status_code=422, detail="GIF conversion produced an empty file")
        if output.stat().st_size > GIF_MAX_OUTPUT_BYTES:
            raise HTTPException(status_code=413, detail="The converted GIF exceeds 50 MiB")

        output_name = f"{source.stem}.gif"
        logger.info(
            "gif_conversion_complete input_bytes=%d output_bytes=%d duration_seconds=%.3f elapsed_ms=%.1f",
            source_size,
            output.stat().st_size,
            duration,
            (time.perf_counter() - started) * 1000,
        )
        cleanup = temporary_directory
        temporary_directory = None
        return FileResponse(
            output,
            media_type="image/gif",
            filename=output_name,
            headers={
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
            },
            background=BackgroundTask(shutil.rmtree, cleanup, ignore_errors=True),
        )
    finally:
        if temporary_directory is not None:
            shutil.rmtree(temporary_directory, ignore_errors=True)
        await _release_gif_conversion(owner)


@app.post("/web/dlp/jobs")
async def web_dlp_allocate(claims: dict[str, Any] = Depends(_require_web_session)):
    return await _dlp_json("POST", "/v2/jobs", claims)


@app.post("/web/dlp/jobs/{job_id}/submit")
async def web_dlp_submit(
    job_id: uuid.UUID,
    request: DlpSubmitRequest,
    claims: dict[str, Any] = Depends(_require_web_session),
):
    return await _dlp_json("POST", f"/v2/jobs/{job_id}/submit", claims, request.model_dump(mode="json", exclude_none=True))


@app.get("/web/dlp/jobs/{job_id}")
async def web_dlp_status(job_id: uuid.UUID, claims: dict[str, Any] = Depends(_require_web_session)):
    return await _dlp_json("GET", f"/v2/jobs/{job_id}", claims)


@app.get("/web/dlp/jobs/{job_id}/file")
async def web_dlp_file(
    job_id: uuid.UUID,
    request: Request,
    claims: dict[str, Any] = Depends(_require_web_session),
):
    if forward_client is None:
        raise HTTPException(status_code=503, detail="Gateway HTTP client is not ready")
    url, _token = _dlp_config()
    try:
        headers = _dlp_headers(claims)
        for name in ("range", "if-range"):
            if value := request.headers.get(name):
                headers[name] = value
        upstream_request = forward_client.build_request("GET", f"{url}/v2/jobs/{job_id}/file", headers=headers)
        upstream = await forward_client.send(upstream_request, stream=True)
    except httpx.RequestError as exc:
        raise HTTPException(status_code=503, detail="Private download is temporarily unavailable") from exc
    if upstream.status_code >= 400 and upstream.status_code != 416:
        await upstream.aclose()
        raise HTTPException(status_code=upstream.status_code, detail="Private download file is unavailable")
    headers = {
        name: value
        for name, value in upstream.headers.items()
        if name.lower() in {
            "accept-ranges",
            "content-range",
            "content-type",
            "content-length",
            "content-disposition",
            "etag",
            "last-modified",
        }
    }
    headers["Cache-Control"] = "no-store"
    headers["X-Content-Type-Options"] = "nosniff"
    return StreamingResponse(
        upstream.aiter_raw(),
        status_code=upstream.status_code,
        headers=headers,
        background=BackgroundTask(upstream.aclose),
    )


@app.post("/v1/mobile/dlp/jobs")
async def mobile_dlp_allocate(
    claims: dict[str, Any] = Depends(_require_mobile_dlp_session),
):
    return await _dlp_json("POST", "/v2/jobs", claims)


@app.post("/v1/mobile/dlp/jobs/{job_id}/submit")
async def mobile_dlp_submit(
    job_id: uuid.UUID,
    request: DlpSubmitRequest,
    claims: dict[str, Any] = Depends(_require_mobile_dlp_session),
):
    return await _dlp_json(
        "POST",
        f"/v2/jobs/{job_id}/submit",
        claims,
        request.model_dump(mode="json", exclude_none=True),
    )


@app.get("/v1/mobile/dlp/jobs/{job_id}")
async def mobile_dlp_status(
    job_id: uuid.UUID,
    claims: dict[str, Any] = Depends(_require_mobile_dlp_session),
):
    return await _dlp_json("GET", f"/v2/jobs/{job_id}", claims)


@app.get("/v1/mobile/dlp/jobs/{job_id}/file")
async def mobile_dlp_file(
    job_id: uuid.UUID,
    request: Request,
    claims: dict[str, Any] = Depends(_require_mobile_dlp_session),
):
    return await web_dlp_file(job_id, request, claims)


@app.post("/web/scrape", response_model=ScrapeResponse)
async def web_scrape_request(
    request: ScrapeRequest,
    http_request: Request,
    _claims: dict[str, Any] = Depends(_require_web_session),
):
    result = await _process_scrape_request(request, http_request)
    return ScrapeResponse(**_rewrite_media_urls(result.model_dump(), "/web/media"))


@app.get("/media/{platform}/{post_id}/{filename:path}")
async def serve_media(
    platform: str,
    post_id: str,
    filename: str,
    _client_name: str = Depends(_require_api_key),
):
    return _serve_media_file(post_id, filename)


@app.get("/web/media/{platform}/{post_id}/{filename:path}")
async def serve_web_media(
    platform: str,
    post_id: str,
    filename: str,
    _claims: dict[str, Any] = Depends(_require_web_session),
):
    return _serve_media_file(post_id, filename)


@app.get("/mobile/media/{platform}/{post_id}/{filename:path}")
async def serve_mobile_media(
    platform: str,
    post_id: str,
    filename: str,
    _claims: dict[str, Any] = Depends(_mobile_media_session),
):
    return _serve_media_file(post_id, filename)


@app.get("/health")
async def health_check():
    results = {}
    all_healthy = True

    # In-process plugins
    for name, plugin in registry.items():
        try:
            if internal_client is None:
                raise RuntimeError("Internal HTTP client is not ready")
            resp = await internal_client.get(f"/{name}/health")
            resp.raise_for_status()
            results[name] = {"mode": "in_process", "status": "healthy", "detail": resp.json()}
        except Exception as e:
            results[name] = {"mode": "in_process", "status": "unhealthy", "detail": str(e)}
            all_healthy = False

    # Container modules (HTTP health check via registry endpoint)
    for name, module in container_registry.modules.items():
        health = await container_registry.health(name)
        if health["status"] == "healthy":
            results[name] = {"mode": "container", "status": "healthy", "detail": health["detail"]}
        else:
            results[name] = {"mode": "container", "status": "unhealthy", "detail": health}
            all_healthy = False

    if not all_healthy:
        raise HTTPException(status_code=503, detail=results)

    return {"status": "healthy", "modules": results}


# ---------------------------------------------------------------------------
# Admin routes for VPN management
# ---------------------------------------------------------------------------
@app.post("/admin/vpn/rotate")
async def admin_rotate_vpn(_client_name: str = Depends(_require_api_key)):
    """Manually trigger a VPN IP rotation via Gluetun."""
    try:
        await gluetun.rotate_ip()
        return {"status": "rotated"}
    except VpnRotationError as e:
        raise HTTPException(status_code=503, detail=f"VPN rotation failed: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error during rotation: {e}")


@app.get("/admin/vpn/status")
async def admin_vpn_status(_client_name: str = Depends(_require_api_key)):
    """Return current Gluetun VPN connection status."""
    try:
        status = await gluetun.get_vpn_status()
        public_ip = await gluetun.get_public_ip()
        return {"vpn": status, "public_ip": public_ip}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed to query VPN status: {e}")


@app.delete("/admin/mobile/installations/{installation_id}", status_code=204)
async def admin_revoke_mobile_installation(
    installation_id: str,
    _client_name: str = Depends(_require_api_key),
):
    if not re.fullmatch(r"[A-Za-z0-9_-]{16,128}", installation_id):
        raise HTTPException(status_code=400, detail="Invalid mobile installation ID")
    revoked = await asyncio.to_thread(
        _mobile_store().revoke_installation,
        installation_id,
    )
    if not revoked:
        raise HTTPException(status_code=404, detail="Mobile installation not found")
    logger.info("mobile_installation_revoked")
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Admin routes for container management
# ---------------------------------------------------------------------------
@app.post("/admin/modules/{name}/start")
async def admin_start_module(name: str, _client_name: str = Depends(_require_api_key)):
    if not container_manager:
        raise HTTPException(status_code=501, detail="Container mode is not enabled")
    if name not in container_manager.modules:
        raise HTTPException(status_code=404, detail=f"Module {name} not in config")
    endpoint = container_manager.start(name)
    return {"status": "started", "endpoint": endpoint}


@app.post("/admin/modules/{name}/stop")
async def admin_stop_module(name: str, _client_name: str = Depends(_require_api_key)):
    if not container_manager:
        raise HTTPException(status_code=501, detail="Container mode is not enabled")
    container_manager.stop(name)
    return {"status": "stopped"}


@app.get("/admin/modules")
async def admin_list_modules(_client_name: str = Depends(_require_api_key)):
    result = {
        "in_process": {name: {"patterns": p.route_patterns} for name, p in registry.items()},
        "containers": {},
    }

    # Show all configured container modules from registry
    for name, m in container_registry.modules.items():
        result["containers"][name] = {
            "config": {
                "source_type": m.source_type,
                "source_url": m.source_url,
                "port": m.port,
                "endpoint": m.endpoint,
                "image_tag": m.image_tag,
                "route_patterns": m.route_patterns,
            },
            "running": False,
        }

    # If container manager is active, overlay running status
    if container_manager:
        for name in container_manager.running:
            if name in result["containers"]:
                result["containers"][name]["running"] = True

    return result
