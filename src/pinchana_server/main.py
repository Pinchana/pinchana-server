"""Pinchana Server — dynamically loads plugins or manages containers."""

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import logging
import mimetypes
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
import urllib.parse
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from pinchana_core.models import (
    RemoteAssetDescriptor,
    ScrapeRequest,
    ScrapeResponse,
    ScrapeV2Context,
    ScrapeV2ExtractedData,
    ScraperCapabilitiesV2,
    TelegramDeliveryDescriptor,
    TelegramAssetDelivery,
    TelegramAssetV2,
    ScrapeV2TelegramResponse,
)
from pinchana_core.plugins import registry
from pinchana_core.storage import MediaStorage
from pinchana_core.docker_manager import ContainerRegistry, ModuleContainerManager
from pinchana_core.vpn import GluetunController, VpnRotationError

from .media_probe import MediaDimensionProbe
from .response_adapter import normalize_scrape_response
from .ssrf import pinned_httpx_transport, validate_upstream_url
from .tickets import InMemoryTicketStore, RedisTicketStore, TicketData, TicketStore
from .telegram_normalizer import TelegramNormalizer
from .v2_observability import v2_observability
from .v2_runtime import normalized_filename, validate_internal_token, validate_spool_topology
from .schemas import (
    ApiErrorResponse,
    MediaDimensions,
    ScrapeAuthor,
    ScrapeContent,
    ScrapeSource,
    ScrapeV1Response,
    ScrapeV2WebProcessingResponse,
    ScrapeV2WebReadyResponse,
    ScrapeV2Content,
    WebAssetTunnelDelivery,
    WebAssetV2,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ticket_store: TicketStore = InMemoryTicketStore(check_workers=False)
normalization_redis: Any | None = None
telegram_normalizer = TelegramNormalizer()

V2_PLATFORM_FLAGS = {
    "instagram": ("PINCHANA_V2_INSTAGRAM", True),
    "tiktok": ("PINCHANA_V2_TIKTOK", False),
    "threads": ("PINCHANA_V2_THREADS", False),
    "twitter": ("PINCHANA_V2_TWITTER", False),
}
V2_TICKET_TTL_SECONDS = 7200
V2_UPSTREAM_SAFETY_MARGIN_SECONDS = 60
V2_MIN_DIRECT_TTL_SECONDS = 60

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


class WebVerifyRequest(BaseModel):
    token: str = Field(min_length=1, max_length=4096)


class WebSessionResponse(BaseModel):
    access_token: str
    expires_at: int


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

    return {"version": "preview", "commits": commits}


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


def _require_telegram_scope(
    x_api_key: str | None = Header(default=None),
) -> str:
    client_name = _require_api_key(x_api_key)

    raw_scopes = os.getenv("PINCHANA_API_KEY_SCOPES", "")
    client_scopes: set[str] = set()
    if raw_scopes:
        try:
            scopes_dict = json.loads(raw_scopes)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=503, detail="API scope configuration is invalid") from exc
        if not isinstance(scopes_dict, dict):
            raise HTTPException(status_code=503, detail="API scope configuration is invalid")
        configured = scopes_dict.get(client_name, [])
        if isinstance(configured, list) and all(isinstance(scope, str) for scope in configured):
            client_scopes.update(scope.strip() for scope in configured if scope.strip())
        elif isinstance(configured, str):
            client_scopes.update(scope.strip() for scope in configured.split(",") if scope.strip())
        else:
            raise HTTPException(status_code=503, detail="API scope configuration is invalid")

    env_specific = os.getenv(f"API_KEY_SCOPES_{client_name.upper()}", "")
    if env_specific:
        client_scopes.update(s.strip() for s in env_specific.split(",") if s.strip())

    if "delivery:telegram" not in client_scopes:
        raise HTTPException(
            status_code=403,
            detail="Client credential does not have delivery:telegram scope",
        )
    return client_name


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
    nonce = claims.get("nonce")
    if not isinstance(nonce, str) or not nonce:
        raise HTTPException(status_code=401, detail="Invalid web session")
    owner_secret = os.getenv("DLP_OWNER_SECRET", "").encode("utf-8") or _session_secret()
    return _urlsafe_encode(hmac.new(owner_secret, nonce.encode("utf-8"), hashlib.sha256).digest())


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    global forward_client, internal_client, ticket_store, normalization_redis
    validate_spool_topology()
    validate_internal_token()
    redis_url = os.getenv("REDIS_URL", "").strip()
    if redis_url:
        ticket_store = RedisTicketStore(redis_url)
        normalization_redis = ticket_store.redis
    else:
        ticket_store = InMemoryTicketStore(check_workers=True)
        normalization_redis = None
    forward_client = httpx.AsyncClient(
        timeout=httpx.Timeout(120.0, connect=10.0),
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
    )
    internal_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://pinchana.internal",
        timeout=120.0,
    )
    protected_spool_dirs = await _recover_ephemeral_jobs()
    _prune_stale_spool_directories(protected_spool_dirs)
    try:
        yield
    finally:
        await _shutdown_spool_tasks()
        await forward_client.aclose()
        await internal_client.aclose()
        if isinstance(ticket_store, RedisTicketStore):
            await ticket_store.redis.aclose()
        await storage.close()


app = FastAPI(title="Pinchana Server", version="1.0.0", lifespan=lifespan)


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


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _v2_platform_enabled(platform: str) -> bool:
    configured = V2_PLATFORM_FLAGS.get(platform)
    return bool(configured and _env_flag(*configured))


def _descriptor_ticket_ttl(descriptor: RemoteAssetDescriptor) -> int:
    if descriptor.expires_at is None:
        return V2_TICKET_TTL_SECONDS
    remaining = descriptor.expires_at - int(time.time()) - V2_UPSTREAM_SAFETY_MARGIN_SECONDS
    return min(V2_TICKET_TTL_SECONDS, remaining)


def _descriptor_can_tunnel(descriptor: RemoteAssetDescriptor) -> bool:
    if not descriptor.supports_range:
        return False
    if descriptor.credential_ref:
        return _descriptor_ticket_ttl(descriptor) >= V2_MIN_DIRECT_TTL_SECONDS
    return _descriptor_ticket_ttl(descriptor) >= V2_MIN_DIRECT_TTL_SECONDS


async def _forward_to_container(module_name: str, request: ScrapeRequest) -> dict[str, Any]:
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
            f"{endpoint}/scrape",
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
            "Upstream module %s (%s) returned %s for /scrape: %s",
            module_name, endpoint, resp.status_code, resp.text,
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
        resp = await internal_client.post(f"/{name}/scrape", json={"url": url})
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
        payload = await _forward_to_container(name, request)

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


# ---------------------------------------------------------------------------
# v2 Web Zero-Cache & Asset Streaming Endpoints
# ---------------------------------------------------------------------------
def _get_instance_id(claims: dict[str, Any]) -> str:
    return claims.get("issuer", "pinchana-project")


async def _stream_credential_asset(ticket: TicketData, request: Request):
    reference = ticket.descriptor.credential_ref or ""
    module_name = reference.split(".", 1)[0]
    plugin = registry.get(module_name)
    module = container_registry.modules.get(module_name)
    if plugin is not None:
        client = internal_client
        base = f"/{module_name}"
    elif module is not None:
        client = forward_client
        base = module.endpoint.rstrip("/")
    else:
        raise HTTPException(status_code=502, detail="Credential resolver is unavailable")
    if client is None:
        raise HTTPException(status_code=503, detail="Gateway HTTP client is not ready")

    headers = {
        "X-Pinchana-Internal-Token": os.getenv(
            "PINCHANA_INTERNAL_TOKEN", "pinchana-local-development"
        )
    }
    for name in ("range", "if-range"):
        if value := request.headers.get(name):
            headers[name] = value
    response = await client.send(
        client.build_request(
            request.method,
            f"{base}/v2/internal/assets/{urllib.parse.quote(reference, safe='._-')}",
            headers=headers,
        ),
        stream=True,
    )
    if response.status_code >= 400 and response.status_code != 416:
        await response.aclose()
        v2_observability.increment("credential_resolution_failure", platform=module_name)
        raise HTTPException(status_code=502, detail="Credentialed asset delivery failed")
    output_headers = {
        name: value
        for name in (
            "accept-ranges", "content-length", "content-range", "content-type", "etag", "last-modified"
        )
        if (value := response.headers.get(name))
    }
    output_headers["Cache-Control"] = "private, no-store"
    output_headers["X-Content-Type-Options"] = "nosniff"
    delivered_filename = normalized_filename(
        ticket.descriptor.filename, response.headers.get("content-type")
    )
    output_headers["Content-Disposition"] = f'attachment; filename="{delivered_filename}"'
    v2_observability.increment(f"upstream_{response.status_code}", platform=module_name)
    if request.method == "HEAD":
        await response.aclose()
        return Response(status_code=response.status_code, headers=output_headers)

    async def body():
        try:
            async for chunk in response.aiter_raw():
                yield chunk
        finally:
            await response.aclose()
            await ticket_store.release_lease(ticket.ticket_id)

    return StreamingResponse(body(), status_code=response.status_code, headers=output_headers)


async def _stream_v2_asset(ticket: TicketData, request: Request):
    if ticket.spool_path:
        spool_file = Path(ticket.spool_path)
        if not spool_file.is_file():
            raise HTTPException(status_code=404, detail="Spool media file expired or not found")
        return FileResponse(
            spool_file,
            filename=ticket.descriptor.filename,
            media_type=ticket.descriptor.mime_type,
            headers={
                "Cache-Control": "private, no-store",
                "X-Content-Type-Options": "nosniff",
            },
            background=(
                BackgroundTask(ticket_store.release_lease, ticket.ticket_id)
                if request.method != "HEAD"
                else None
            ),
        )

    if ticket.descriptor.credential_ref:
        return await _stream_credential_asset(ticket, request)

    current_url = ticket.descriptor.upstream_url
    if not current_url:
        raise HTTPException(status_code=502, detail="Asset has no delivery source")

    # Safe headers whitelist
    upstream_headers = {}
    if ticket.descriptor.safe_headers:
        for k, v in ticket.descriptor.safe_headers.items():
            if k.lower() in {"user-agent", "referer", "accept", "accept-language"}:
                upstream_headers[k] = str(v)

    # Forward client range headers
    for name in ("range", "if-range"):
        if val := request.headers.get(name):
            upstream_headers[name] = val

    hops = 0
    max_hops = 5

    while hops < max_hops:
        try:
            current_url, resolved_ip = validate_upstream_url(current_url)
        except HTTPException as exc:
            v2_observability.increment("ssrf_rejection")
            raise HTTPException(
                status_code=502, detail="Upstream asset rejected by network policy"
            ) from exc
        hostname = urllib.parse.urlparse(current_url).hostname
        if not hostname:
            raise HTTPException(status_code=400, detail="Missing upstream hostname")
        pinned_client = httpx.AsyncClient(
            transport=pinned_httpx_transport(hostname, resolved_ip),
            timeout=httpx.Timeout(120.0, connect=10.0),
        )
        method = request.method
        req = pinned_client.build_request(method, current_url, headers=upstream_headers)
        try:
            resp = await pinned_client.send(req, stream=True)
        except Exception:
            await pinned_client.aclose()
            raise

        if resp.status_code in (301, 302, 303, 307, 308):
            location = resp.headers.get("location")
            await resp.aclose()
            await pinned_client.aclose()
            if not location:
                raise HTTPException(status_code=502, detail="Upstream redirect missing Location header")
            next_url = urllib.parse.urljoin(current_url, location)
            # Strip auth headers on cross-origin redirects
            if urllib.parse.urlparse(next_url).netloc != urllib.parse.urlparse(current_url).netloc:
                upstream_headers.pop("authorization", None)
                upstream_headers.pop("cookie", None)
                upstream_headers.pop("Referer", None)
                upstream_headers.pop("referer", None)
            current_url = next_url
            hops += 1
            continue

        if resp.status_code >= 400 and resp.status_code != 416:
            await resp.aclose()
            await pinned_client.aclose()
            raise HTTPException(status_code=resp.status_code, detail="Upstream asset returned an error")

        out_headers = {}
        for name in (
            "accept-ranges",
            "content-length",
            "content-range",
            "content-type",
            "etag",
            "last-modified",
        ):
            if val := resp.headers.get(name):
                out_headers[name] = val

        out_headers["Cache-Control"] = "private, no-store"
        out_headers["X-Content-Type-Options"] = "nosniff"
        delivered_filename = normalized_filename(
            ticket.descriptor.filename, resp.headers.get("content-type")
        )
        out_headers["Content-Disposition"] = f'attachment; filename="{delivered_filename}"'
        v2_observability.increment(f"upstream_{resp.status_code}")

        if method == "HEAD":
            await resp.aclose()
            await pinned_client.aclose()
            return Response(status_code=resp.status_code, headers=out_headers)

        async def _body_stream():
            try:
                async for chunk in resp.aiter_raw():
                    yield chunk
            finally:
                await resp.aclose()
                await pinned_client.aclose()
                await ticket_store.release_lease(ticket.ticket_id)

        return StreamingResponse(
            _body_stream(),
            status_code=resp.status_code,
            headers=out_headers,
        )

    raise HTTPException(status_code=502, detail="Too many upstream redirects")


# ---------------------------------------------------------------------------
# v2 Ephemeral Spool Jobs (Zero Persistent Cache)
# ---------------------------------------------------------------------------
ephemeral_jobs: dict[str, dict[str, Any]] = {}
ephemeral_jobs_lock = asyncio.Lock()
spool_tasks: dict[str, asyncio.Task[Any]] = {}


def _ephemeral_job_key(job_id: str) -> str:
    return f"pinchana:v2:job:{job_id}"


async def _set_ephemeral_job(job_id: str, job: dict[str, Any]) -> None:
    if normalization_redis is not None:
        ttl = max(1, int(job.get("expires_at", int(time.time()) + 300)) - int(time.time()))
        # Retain the record briefly past logical expiry so polling can clean the
        # shared spool directory and return 410 instead of losing ownership data.
        await normalization_redis.set(
            _ephemeral_job_key(job_id), json.dumps(job), ex=ttl + 300
        )
        return
    async with ephemeral_jobs_lock:
        ephemeral_jobs[job_id] = job


async def _get_ephemeral_job(job_id: str) -> dict[str, Any] | None:
    if normalization_redis is not None:
        raw = await normalization_redis.get(_ephemeral_job_key(job_id))
        if not raw:
            return None
        return json.loads(raw)
    async with ephemeral_jobs_lock:
        job = ephemeral_jobs.get(job_id)
        return dict(job) if job else None


async def _delete_ephemeral_job(job_id: str) -> None:
    if normalization_redis is not None:
        await normalization_redis.delete(_ephemeral_job_key(job_id))
        return
    async with ephemeral_jobs_lock:
        ephemeral_jobs.pop(job_id, None)


def _track_spool_task(job_id: str, task: asyncio.Task[Any]) -> None:
    spool_tasks[job_id] = task
    task.add_done_callback(lambda _task: spool_tasks.pop(job_id, None))


async def _shutdown_spool_tasks() -> None:
    active = list(spool_tasks.items())
    for _job_id, task in active:
        task.cancel()
    if active:
        await asyncio.gather(*(task for _, task in active), return_exceptions=True)
    for job_id, _task in active:
        job = await _get_ephemeral_job(job_id)
        if not job or job.get("status") != "processing":
            continue
        await _cleanup_job_spool(job)
        job.update({
            "status": "failed",
            "error": "Processing stopped during shutdown; retry the request",
            "expires_at": int(time.time()) + 300,
            "spool_dir": None,
            "spool_files": [],
            "ticket_ids": [],
        })
        await _set_ephemeral_job(job_id, job)
        v2_observability.increment("processing_job_failure", platform=job.get("platform"))


def _spool_root() -> Path:
    return Path(os.getenv("V2_SPOOL_PATH", "./spool")).resolve()


def _safe_spool_path(raw_path: str | None) -> Path | None:
    if not raw_path:
        return None
    root = _spool_root()
    candidate = Path(raw_path).resolve()
    return candidate if candidate.is_relative_to(root) else None


async def _job_has_active_leases(job: dict[str, Any]) -> bool:
    for ticket_id in job.get("ticket_ids") or []:
        ticket = await ticket_store.get_ticket(str(ticket_id))
        if ticket and ticket.active_leases > 0:
            return True
    return False


async def _cleanup_job_spool(job: dict[str, Any]) -> bool:
    if await _job_has_active_leases(job):
        return False
    spool_dir = _safe_spool_path(job.get("spool_dir"))
    if spool_dir and spool_dir.is_dir():
        shutil.rmtree(spool_dir, ignore_errors=True)
        v2_observability.increment("spool_cleanup")
    for ticket_id in job.get("ticket_ids") or []:
        await ticket_store.delete_ticket(str(ticket_id))
    return True


def _job_spool_files_available(job: dict[str, Any]) -> bool:
    files = job.get("spool_files") or []
    if not files:
        return False
    return all(
        (candidate := _safe_spool_path(str(raw))) is not None
        and candidate.is_file()
        and candidate.stat().st_size > 0
        for raw in files
    )


async def _recover_ephemeral_jobs() -> set[Path]:
    """Recover safe ready jobs and fail interrupted work after a restart."""
    protected: set[Path] = set()
    if normalization_redis is None:
        return protected
    async for key in normalization_redis.scan_iter(match="pinchana:v2:job:*"):
        raw = await normalization_redis.get(key)
        if not raw:
            continue
        try:
            job = json.loads(raw)
        except (TypeError, ValueError):
            await normalization_redis.delete(key)
            continue
        job_id = str(key).rsplit(":", 1)[-1]
        status = job.get("status")
        expired = int(job.get("expires_at", 0)) <= int(time.time())
        if expired:
            if await _cleanup_job_spool(job):
                await _delete_ephemeral_job(job_id)
                v2_observability.increment("processing_job_expiry", platform=job.get("platform"))
            continue
        if status == "processing":
            await _cleanup_job_spool(job)
            job.update({
                "status": "failed",
                "error": "Processing was interrupted; retry the request",
                "expires_at": int(time.time()) + 300,
                "spool_dir": None,
                "spool_files": [],
                "ticket_ids": [],
            })
            await _set_ephemeral_job(job_id, job)
            v2_observability.increment("processing_job_failure", platform=job.get("platform"))
            continue
        if status == "ready" and not _job_spool_files_available(job):
            await _cleanup_job_spool(job)
            job.update({
                "status": "failed",
                "error": "Processed media is unavailable; retry the request",
                "expires_at": int(time.time()) + 300,
                "spool_dir": None,
                "spool_files": [],
                "ticket_ids": [],
            })
            await _set_ephemeral_job(job_id, job)
            v2_observability.increment("processing_job_failure", platform=job.get("platform"))
            continue
        spool_dir = _safe_spool_path(job.get("spool_dir"))
        if spool_dir:
            protected.add(spool_dir)
    return protected


def _prune_stale_spool_directories(protected: set[Path] | None = None) -> None:
    spool_root = Path(os.getenv("V2_SPOOL_PATH", "./spool")).resolve()
    if not spool_root.is_dir():
        return
    protected = protected or set()
    cutoff = time.time() - 3 * 60 * 60
    for candidate in spool_root.iterdir():
        try:
            resolved = candidate.resolve()
            if not resolved.is_relative_to(spool_root) or resolved in protected:
                continue
            if candidate.is_dir() and candidate.stat().st_mtime < cutoff:
                shutil.rmtree(candidate, ignore_errors=True)
                v2_observability.increment("spool_cleanup")
            elif candidate.is_dir():
                for partial in candidate.glob("*.part"):
                    if partial.is_file() and partial.stat().st_mtime < time.time() - 10 * 60:
                        partial.unlink(missing_ok=True)
                        v2_observability.increment("spool_cleanup")
        except OSError:
            logger.warning("ephemeral_spool_cleanup_stat_failed")


async def _expire_ephemeral_job(job_id: str, expires_at: int) -> None:
    await asyncio.sleep(max(0, expires_at - int(time.time())) + 1)
    job = await _get_ephemeral_job(job_id)
    if not job or int(job.get("expires_at", 0)) > int(time.time()):
        return
    if not await _cleanup_job_spool(job):
        asyncio.create_task(_expire_ephemeral_job(job_id, int(time.time()) + 30))
        return
    await _delete_ephemeral_job(job_id)
    v2_observability.increment("processing_job_expiry", platform=job.get("platform"))


async def _run_ephemeral_spool_job(
    job_id: str,
    url: str,
    module_name: str,
    session_nonce: str,
    instance_id: str,
    extracted: ScrapeV2ExtractedData,
):
    started_at = time.monotonic()
    spool_root = Path(os.getenv("V2_SPOOL_PATH", "./spool")).resolve()
    spool_dir = (spool_root / job_id).resolve()
    if not spool_dir.is_relative_to(spool_root):
        raise RuntimeError("invalid spool path")
    spool_dir.mkdir(parents=True, exist_ok=False)
    processing_job = await _get_ephemeral_job(job_id)
    if processing_job and processing_job.get("status") == "processing":
        processing_job["spool_dir"] = str(spool_dir)
        await _set_ephemeral_job(job_id, processing_job)
    maximum_bytes = int(os.getenv("V2_SPOOL_MAX_BYTES", str(1024 * 1024 * 1024)))

    async def open_upstream(descriptor: RemoteAssetDescriptor):
        if descriptor.credential_ref:
            reference = descriptor.credential_ref
            namespace = reference.split(".", 1)[0]
            plugin = registry.get(namespace)
            module = container_registry.modules.get(namespace)
            if plugin is not None:
                client, base = internal_client, f"/{namespace}"
            elif module is not None:
                client, base = forward_client, module.endpoint.rstrip("/")
            else:
                raise RuntimeError("credential resolver unavailable")
            if client is None:
                raise RuntimeError("gateway client unavailable")
            response = await client.send(
                client.build_request(
                    "GET",
                    f"{base}/v2/internal/assets/{urllib.parse.quote(reference, safe='._-')}",
                    headers={
                        "X-Pinchana-Internal-Token": os.getenv(
                            "PINCHANA_INTERNAL_TOKEN", "pinchana-local-development"
                        )
                    },
                ),
                stream=True,
            )
            return response, None

        current_url = descriptor.upstream_url
        if not current_url:
            raise RuntimeError("descriptor has no upstream URL")
        headers = {
            key: str(value)
            for key, value in (descriptor.safe_headers or {}).items()
            if key.lower() in {"user-agent", "referer", "accept", "accept-language"}
        }
        for _hop in range(5):
            try:
                current_url, resolved_ip = validate_upstream_url(current_url)
            except HTTPException:
                v2_observability.increment("ssrf_rejection", platform=module_name)
                raise
            hostname = urllib.parse.urlparse(current_url).hostname
            if not hostname:
                raise RuntimeError("upstream hostname missing")
            client = httpx.AsyncClient(
                transport=pinned_httpx_transport(hostname, resolved_ip),
                timeout=httpx.Timeout(120, connect=10),
            )
            response = await client.send(client.build_request("GET", current_url, headers=headers), stream=True)
            if response.status_code not in {301, 302, 303, 307, 308}:
                return response, client
            location = response.headers.get("location")
            await response.aclose()
            await client.aclose()
            if not location:
                raise RuntimeError("upstream redirect missing location")
            next_url = urllib.parse.urljoin(current_url, location)
            if urllib.parse.urlparse(next_url).netloc != urllib.parse.urlparse(current_url).netloc:
                headers.pop("Referer", None)
                headers.pop("referer", None)
            current_url = next_url
        raise RuntimeError("too many upstream redirects")

    try:
        web_assets: list[WebAssetV2] = []
        spool_files: list[str] = []
        ticket_ids: list[str] = []
        for descriptor in extracted.assets:
            response, owned_client = await open_upstream(descriptor)
            try:
                if response.status_code in {200, 206, 416}:
                    v2_observability.increment(
                        f"upstream_{response.status_code}", platform=module_name
                    )
                if response.status_code >= 400:
                    if descriptor.credential_ref:
                        v2_observability.increment(
                            "credential_resolution_failure", platform=module_name
                        )
                    raise RuntimeError("upstream media was unavailable")
                actual_mime = response.headers.get("content-type") or descriptor.mime_type
                actual_filename = normalized_filename(descriptor.filename, actual_mime)
                resolved_descriptor = descriptor.model_copy(update={
                    "filename": actual_filename,
                    "mime_type": actual_mime.split(";", 1)[0].strip() if actual_mime else None,
                })
                destination = spool_dir / f"{descriptor.index:03d}-{actual_filename}"
                partial = destination.with_suffix(destination.suffix + ".part")
                written = 0
                with partial.open("wb") as output:
                    async for chunk in response.aiter_raw():
                        written += len(chunk)
                        if written > maximum_bytes:
                            raise RuntimeError("spool asset exceeds configured size limit")
                        output.write(chunk)
                if written == 0:
                    raise RuntimeError("upstream returned an empty asset")
                partial.replace(destination)
                spool_files.append(str(destination))
                v2_observability.increment("spool_bytes", platform=module_name, amount=written)
            finally:
                await response.aclose()
                if owned_client is not None:
                    await owned_client.aclose()
            ticket = await ticket_store.create_ticket(
                session_nonce=session_nonce,
                instance_id=instance_id,
                descriptor=resolved_descriptor,
                spool_path=str(destination),
                ttl_seconds=1800,
            )
            ticket_ids.append(ticket.ticket_id)
            web_assets.append(
                WebAssetV2(
                    id=f"{extracted.shortcode}-{descriptor.index}",
                    asset_key=descriptor.asset_id or f"{module_name}:{extracted.shortcode}:{descriptor.index}:{descriptor.role}",
                    index=descriptor.index,
                    type=descriptor.media_type,
                    role=descriptor.role,
                    filename=resolved_descriptor.filename,
                    mime_type=resolved_descriptor.mime_type,
                    size=descriptor.size,
                    dimensions=descriptor.dimensions,
                    duration_seconds=descriptor.duration_seconds,
                    bitrate=descriptor.bitrate,
                    looping=descriptor.looping,
                    delivery=WebAssetTunnelDelivery(
                        kind="tunnel",
                        url=f"/v2/assets/{ticket.ticket_id}",
                        expires_at=ticket.expires_at,
                    ),
                )
            )

        ready_response = ScrapeV2WebReadyResponse(
            status="ready",
            request_id=job_id,
            source=ScrapeSource(platform=module_name, url=url),
            content=ScrapeV2Content(
                shortcode=extracted.shortcode,
                title=extracted.caption,
                text=extracted.caption,
            ),
            author=ScrapeAuthor(name=extracted.author, username=extracted.author),
            assets=web_assets,
        )
        ready_expiry = int(time.time()) + 1800
        await _set_ephemeral_job(job_id, {
            "status": "ready",
            "result": ready_response.model_dump(mode="json"),
            "expires_at": ready_expiry,
            "session_nonce": session_nonce,
            "instance_id": instance_id,
            "platform": module_name,
            "spool_dir": str(spool_dir),
            "spool_files": spool_files,
            "ticket_ids": ticket_ids,
        })
        asyncio.create_task(_expire_ephemeral_job(job_id, ready_expiry))
        v2_observability.increment("processing_job_success", platform=module_name)
        v2_observability.observe(
            "time_to_ready", time.monotonic() - started_at, platform=module_name
        )
    except Exception as exc:
        logger.error("ephemeral_spool_job_failed platform=%s error=%s", module_name, type(exc).__name__)
        shutil.rmtree(spool_dir, ignore_errors=True)
        await _set_ephemeral_job(job_id, {
            "status": "failed",
            "error": "Media processing failed",
            "expires_at": int(time.time()) + 300,
            "session_nonce": session_nonce,
            "instance_id": instance_id,
            "platform": module_name,
            "spool_dir": None,
            "spool_files": [],
            "ticket_ids": [],
        })
        v2_observability.increment("processing_job_failure", platform=module_name)


async def _run_ephemeral_spool_job_guarded(
    job_id: str,
    url: str,
    module_name: str,
    session_nonce: str,
    instance_id: str,
    extracted: ScrapeV2ExtractedData,
) -> None:
    try:
        await _run_ephemeral_spool_job(
            job_id, url, module_name, session_nonce, instance_id, extracted
        )
    except Exception as exc:
        logger.error(
            "ephemeral_spool_job_setup_failed platform=%s error=%s",
            module_name,
            type(exc).__name__,
        )
        await _set_ephemeral_job(job_id, {
            "status": "failed",
            "error": "Media processing could not start; retry the request",
            "expires_at": int(time.time()) + 300,
            "session_nonce": session_nonce,
            "instance_id": instance_id,
            "platform": module_name,
            "spool_dir": None,
            "spool_files": [],
            "ticket_ids": [],
        })
        v2_observability.increment("processing_job_failure", platform=module_name)


@app.get("/v2/jobs/{job_id}")
async def get_v2_job_status(
    job_id: str,
    claims: dict[str, Any] = Depends(_require_web_session),
):
    job = await _get_ephemeral_job(job_id)
    if not job:
        v2_observability.increment("processing_job_404")
        raise HTTPException(status_code=404, detail="Job not found or expired")
    if int(job.get("expires_at", 0)) <= int(time.time()):
        if await _cleanup_job_spool(job):
            await _delete_ephemeral_job(job_id)
        else:
            asyncio.create_task(_expire_ephemeral_job(job_id, int(time.time()) + 30))
        v2_observability.increment("processing_job_expiry", platform=job.get("platform"))
        raise HTTPException(status_code=410, detail="Job expired")
    if (
        job.get("session_nonce") != claims.get("nonce", "")
        or job.get("instance_id") != _get_instance_id(claims)
    ):
        v2_observability.increment("processing_job_403", platform=job.get("platform"))
        raise HTTPException(status_code=403, detail="Job does not belong to this web session")
    if job["status"] == "ready":
        if not _job_spool_files_available(job):
            await _cleanup_job_spool(job)
            job.update({
                "status": "failed",
                "error": "Processed media is unavailable; retry the request",
                "expires_at": int(time.time()) + 300,
                "spool_dir": None,
                "spool_files": [],
                "ticket_ids": [],
            })
            await _set_ephemeral_job(job_id, job)
            v2_observability.increment("processing_job_failure", platform=job.get("platform"))
            raise HTTPException(status_code=502, detail=job["error"])
        return JSONResponse(content=job["result"])
    if job["status"] == "failed":
        raise HTTPException(status_code=502, detail=job.get("error", "Job failed"))
    return {
        "status": "processing",
        "job_id": job_id,
        "status_url": f"/v2/jobs/{job_id}",
        "expires_at": job.get("expires_at", int(time.time()) + 300),
    }


@app.post(
    "/v2/scrape",
    response_model=ScrapeV2WebReadyResponse | ScrapeV2WebProcessingResponse,
    responses={
        status: {"model": ApiErrorResponse}
        for status in (400, 401, 403, 404, 422, 429, 500, 502, 503)
    },
)
async def process_v2_web_scrape(
    request: ScrapeRequest,
    http_request: Request,
    claims: dict[str, Any] = Depends(_require_web_session),
):
    """v2 Web resolve route returning zero-persistent-cache opaque tickets."""
    resolve_started = time.monotonic()
    logger.info("authenticated_v2_web_scrape")
    url = str(request.url)
    mode, name, target = _resolve_module(url)
    if mode is None:
        raise HTTPException(status_code=400, detail="No module handles this URL.")
    v2_observability.increment("resolve_attempt", platform=str(name))
    if not _v2_platform_enabled(str(name)):
        logger.info("v2_route_disabled platform=%s", name)
        v2_observability.increment("v1_rollback_disabled", platform=str(name))
        raise HTTPException(
            status_code=409,
            detail={"code": "v2_disabled", "message": f"Native v2 is disabled for {name}"},
        )

    session_nonce = claims.get("nonce", "")
    instance_id = _get_instance_id(claims)

    if internal_client is None or forward_client is None:
        raise HTTPException(status_code=503, detail="Gateway HTTP client is not ready")
    module_client = internal_client if mode == "in_process" else forward_client
    module_base = f"/{name}" if mode == "in_process" else target.endpoint.rstrip("/")
    try:
        cap_resp = await module_client.get(f"{module_base}/v2/capabilities", timeout=5)
        capabilities = cap_resp.json() if cap_resp.status_code == 200 else {}
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("v2_capability_failed platform=%s error=%s", name, type(exc).__name__)
        v2_observability.increment("v1_rollback_capability", platform=str(name))
        raise HTTPException(
            status_code=502,
            detail={"code": "v2_capability_unavailable", "message": f"Native v2 capability check failed for {name}"},
        ) from exc
    if not capabilities.get("supports_v2_remote"):
        logger.info("v2_capability_unavailable platform=%s status=%s", name, cap_resp.status_code)
        v2_observability.increment("v1_rollback_capability", platform=str(name))
        raise HTTPException(
            status_code=502,
            detail={"code": "v2_capability_unavailable", "message": f"Native v2 is unavailable for {name}"},
        )
    try:
        scrape_resp = await module_client.post(f"{module_base}/v2/scrape", json={"url": url})
    except httpx.HTTPError as exc:
        logger.warning("v2_extract_unreachable platform=%s error=%s", name, type(exc).__name__)
        raise HTTPException(status_code=503, detail={"code": "service_unavailable", "message": f"The {name} scraper is unavailable"}) from exc
    if scrape_resp.status_code != 200:
        try:
            upstream_detail = scrape_resp.json().get("detail")
        except (ValueError, AttributeError):
            upstream_detail = None
        raise HTTPException(
            status_code=scrape_resp.status_code,
            detail=upstream_detail or {"code": "extraction_failed", "message": f"{name} extraction failed"},
        )
    try:
        v2_extracted = ScrapeV2ExtractedData(**scrape_resp.json())
    except (ValueError, ValidationError) as exc:
        raise HTTPException(status_code=502, detail={"code": "invalid_response", "message": "The scraper returned an invalid v2 response"}) from exc
    if not v2_extracted.assets:
        raise HTTPException(status_code=404, detail={"code": "not_found", "message": "No downloadable media found"})

    if any(not _descriptor_can_tunnel(descriptor) for descriptor in v2_extracted.assets):
        job_id = secrets.token_urlsafe(18)
        job_expiry = int(time.time()) + 1800
        await _set_ephemeral_job(job_id, {
            "status": "processing",
            "expires_at": job_expiry,
            "session_nonce": session_nonce,
            "instance_id": instance_id,
            "platform": str(name),
            "spool_dir": None,
            "spool_files": [],
            "ticket_ids": [],
        })
        spool_task = asyncio.create_task(_run_ephemeral_spool_job_guarded(
            job_id,
            url,
            str(name),
            session_nonce,
            instance_id,
            v2_extracted,
        ))
        _track_spool_task(job_id, spool_task)
        v2_observability.increment("delivery_spool", platform=str(name))
        v2_observability.increment("native_v2_success", platform=str(name))
        v2_observability.observe(
            "resolve_latency", time.monotonic() - resolve_started, platform=str(name)
        )
        return ScrapeV2WebProcessingResponse(
            status="processing",
            request_id=str(uuid.uuid4()),
            job_id=job_id,
            status_url=f"/v2/jobs/{job_id}",
            expires_at=job_expiry,
        )

    # Native v2 remote assets
    web_assets: list[WebAssetV2] = []
    for desc in v2_extracted.assets:
        ttl = _descriptor_ticket_ttl(desc)
        ticket = await ticket_store.create_ticket(
            session_nonce=session_nonce,
            instance_id=instance_id,
            descriptor=desc,
            ttl_seconds=ttl,
        )
        web_assets.append(
            WebAssetV2(
                id=f"{v2_extracted.shortcode}-{desc.index}",
                asset_key=desc.asset_id or f"{name}:{v2_extracted.shortcode}:{desc.index}:{desc.role}",
                index=desc.index,
                type=desc.media_type,  # type: ignore
                role=desc.role,  # type: ignore
                filename=desc.filename,
                mime_type=desc.mime_type,
                size=desc.size,
                dimensions=desc.dimensions,
                duration_seconds=desc.duration_seconds,
                bitrate=desc.bitrate,
                looping=desc.looping,
                delivery=WebAssetTunnelDelivery(
                    kind="tunnel",
                    url=f"/v2/assets/{ticket.ticket_id}",
                    expires_at=ticket.expires_at,
                ),
            )
        )

    v2_observability.increment("delivery_tunnel", platform=str(name))
    v2_observability.increment("native_v2_success", platform=str(name))
    v2_observability.observe(
        "resolve_latency", time.monotonic() - resolve_started, platform=str(name)
    )
    return ScrapeV2WebReadyResponse(
        status="ready",
        request_id=str(uuid.uuid4()),
        source=ScrapeSource(platform=name, url=url),  # type: ignore[arg-type]
        content=ScrapeV2Content(
            shortcode=v2_extracted.shortcode,
            title=v2_extracted.caption,
            text=v2_extracted.caption,
        ),
        author=ScrapeAuthor(name=v2_extracted.author, username=v2_extracted.author),
        assets=web_assets,
    )



@app.get("/v2/assets/{ticket_id}", operation_id="get_v2_asset")
@app.head("/v2/assets/{ticket_id}", operation_id="head_v2_asset")
async def serve_v2_asset(
    ticket_id: str,
    request: Request,
    claims: dict[str, Any] = Depends(_require_web_session),
):
    """Stream or head request for a session-bound opaque asset ticket."""
    ticket = await ticket_store.get_ticket(ticket_id)
    if not ticket:
        v2_observability.increment("ticket_404")
        raise HTTPException(status_code=404, detail="Invalid or expired asset ticket")

    if ticket.is_expired() and ticket.active_leases == 0:
        await ticket_store.delete_ticket(ticket_id)
        v2_observability.increment("ticket_410")
        raise HTTPException(status_code=410, detail="Asset ticket expired")

    if ticket.session_nonce != claims.get("nonce", ""):
        v2_observability.increment("ticket_403")
        raise HTTPException(status_code=403, detail="Ticket does not belong to this web session")

    if ticket.instance_id != _get_instance_id(claims):
        v2_observability.increment("ticket_403")
        raise HTTPException(status_code=403, detail="Ticket is not valid for this API instance")

    if request.method == "HEAD":
        # For HEAD requests, lease is acquired and released inline
        acquired = await ticket_store.acquire_lease(ticket_id)
        if not acquired:
            raise HTTPException(status_code=404, detail="Ticket expired")
        try:
            return await _stream_v2_asset(ticket, request)
        finally:
            await ticket_store.release_lease(ticket_id)

    acquired = await ticket_store.acquire_lease(ticket_id)
    if not acquired:
        raise HTTPException(status_code=404, detail="Ticket expired")

    try:
        return await _stream_v2_asset(ticket, request)
    except Exception:
        await ticket_store.release_lease(ticket_id)
        raise


@app.post(
    "/v2/telegram/scrape",
    response_model=ScrapeV2TelegramResponse,
    responses={
        status: {"model": ApiErrorResponse}
        for status in (400, 401, 403, 404, 422, 429, 500, 502, 503)
    },
)
async def process_v2_telegram_scrape(
    request: ScrapeRequest,
    http_request: Request,
    client_name: str = Depends(_require_telegram_scope),
):
    """Authenticated endpoint reserved for Telegram Bot API delivery normalization."""
    logger.info("authenticated_v2_telegram_scrape client=%s", client_name)
    url = str(request.url)
    platform, payload = await _process_scrape_payload(request, http_request)
    try:
        normalized = await normalize_scrape_response(
            payload,
            platform=platform,
            source_url=url,
            probe=None,
        )
    except (ValueError, ValidationError) as exc:
        raise HTTPException(status_code=502, detail="The scraper returned an invalid response") from exc

    shortcode = normalized.data.id
    cache_root = storage.base_path.resolve()
    post_dir = (storage.base_path / shortcode).resolve()

    # Path traversal validation for post_dir
    try:
        post_dir.relative_to(cache_root)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid post directory path traversal")

    if not post_dir.is_dir():
        raise HTTPException(status_code=404, detail="Media post directory not found")

    ordered_media = sorted(normalized.data.media, key=lambda item: item.index)
    if not ordered_media:
        raise HTTPException(status_code=404, detail="No source media files found in post directory")

    telegram_assets: List[TelegramAssetV2] = []

    for asset in ordered_media:
        source_file = dimension_probe._resolve_media_path(asset.url)
        if source_file is None:
            raise HTTPException(status_code=502, detail="Scraper returned an unresolved cache path")
        if source_file.is_symlink():
            raise HTTPException(status_code=400, detail=f"Symlink media is not allowed: {source_file.name}")

        # Path traversal & symlink validation for each source file
        resolved_source = source_file.resolve()
        try:
            resolved_source.relative_to(cache_root)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Path traversal detected in media file {source_file.name}")

        if not resolved_source.is_file():
            continue

        asset_type = asset.type
        mime_type = mimetypes.guess_type(source_file.name)[0] or (
            "video/mp4" if asset_type == "video"
            else "audio/mpeg" if asset_type == "audio"
            else "image/jpeg"
        )
        asset_key = f"{platform}:{shortcode}:{asset.index}:{asset.role}"
        source_fingerprint = telegram_normalizer.source_fingerprint(resolved_source, asset_key)
        cache_key = f"{asset_key}:telegram-v1:{source_fingerprint}"

        if asset_type == "video":
            variant_path, status_str, probe = await telegram_normalizer.normalize_for_telegram(
                input_path=resolved_source,
                post_dir=post_dir,
                asset_key=asset_key,
                fingerprint=source_fingerprint,
                redis_client=normalization_redis,
            )
            rel_variant = str(variant_path.resolve().relative_to(cache_root))
            dimensions = {"width": probe.width, "height": probe.height} if probe.width and probe.height else None
            duration = probe.duration
            output_size = variant_path.stat().st_size if variant_path.is_file() else resolved_source.stat().st_size
        else:
            # Images or Audio - served directly from shared cache
            rel_variant = str(resolved_source.relative_to(cache_root))
            status_str = "compatible"
            probed_dimensions = await dimension_probe.dimensions_for(asset.url, asset_type)
            dimensions = probed_dimensions.model_dump() if probed_dimensions else None
            duration = asset.duration_seconds
            output_size = resolved_source.stat().st_size

        delivery = TelegramAssetDelivery(
            kind="shared-cache",
            normalization_profile="telegram-v1",
            relative_variant_path=rel_variant,
            asset_key=asset_key,
            source_fingerprint=source_fingerprint,
            cache_key=cache_key,
            streamability_status=status_str,
        )

        telegram_assets.append(
            TelegramAssetV2(
                id=f"{shortcode}-{asset.index}",
                asset_key=asset_key,
                index=asset.index,
                type=asset_type,  # type: ignore
                role=asset.role,  # type: ignore[arg-type]
                filename=Path(rel_variant).name,
                mime_type=mime_type,
                size=output_size,
                dimensions=dimensions,
                duration_seconds=duration,
                title=asset.title,
                artist=asset.artist,
                looping=asset.looping,
                delivery=delivery,
            )
        )

    return ScrapeV2TelegramResponse(
        status="ready",
        request_id=str(uuid.uuid4()),
        source={"platform": platform, "url": url},
        content={
            "shortcode": shortcode,
            "caption": normalized.data.content.text,
            "title": normalized.data.content.title,
            "text": normalized.data.content.text,
            "html": normalized.data.content.html,
            "published_at": normalized.data.content.published_at,
        },
        author=normalized.data.author.model_dump(mode="json"),
        engagement=normalized.data.engagement.model_dump(mode="json") if normalized.data.engagement else None,
        safety=normalized.data.safety.model_dump(mode="json") if normalized.data.safety else None,
        link=normalized.data.link.model_dump(mode="json") if normalized.data.link else None,
        assets=telegram_assets,
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


@app.get("/admin/v2/metrics")
async def admin_v2_metrics(_client_name: str = Depends(_require_api_key)):
    """Return safe low-cardinality counters for the staged v2 rollout."""
    return v2_observability.snapshot()
