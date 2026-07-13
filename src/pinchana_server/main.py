"""Pinchana Server — dynamically loads plugins or manages containers."""

import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
import httpx
from pydantic import BaseModel, Field
from pinchana_core.models import ScrapeRequest, ScrapeResponse
from pinchana_core.plugins import registry
from pinchana_core.storage import MediaStorage
from pinchana_core.docker_manager import ContainerRegistry, ModuleContainerManager
from pinchana_core.vpn import GluetunController, VpnRotationError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TURNSTILE_SITEVERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
TURNSTILE_TEST_SECRET_KEYS = {
    "1x0000000000000000000000000000000AA",
    "2x0000000000000000000000000000000AA",
    "3x0000000000000000000000000000000AA",
}

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

forward_client: httpx.AsyncClient | None = None
internal_client: httpx.AsyncClient | None = None


class WebVerifyRequest(BaseModel):
    token: str = Field(min_length=1, max_length=4096)


class WebSessionResponse(BaseModel):
    access_token: str
    expires_at: int


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
    try:
        yield
    finally:
        await forward_client.aclose()
        await internal_client.aclose()
        await storage.close()


app = FastAPI(title="Pinchana Server", version="1.0.0", lifespan=lifespan)

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


async def _forward_to_container(module_name: str, request: ScrapeRequest) -> ScrapeResponse:
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
    return ScrapeResponse(**resp.json())


def _rewrite_media_urls(value: Any, prefix: str) -> Any:
    if isinstance(value, str) and value.startswith("/media/"):
        return f"{prefix}{value.removeprefix('/media')}"
    if isinstance(value, list):
        return [_rewrite_media_urls(item, prefix) for item in value]
    if isinstance(value, dict):
        return {key: _rewrite_media_urls(item, prefix) for key, item in value.items()}
    return value


async def _process_scrape_request(request: ScrapeRequest, http_request: Request) -> ScrapeResponse:
    """Route a validated scrape request to an in-process or container module."""
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
        result = ScrapeResponse(**resp.json())
    else:
        result = await _forward_to_container(name, request)

    logger.info(
        "scrape_complete module=%s mode=%s elapsed_ms=%.1f",
        name, mode, (time.perf_counter() - started) * 1000,
    )
    return result


def _serve_media_file(post_id: str, filename: str) -> FileResponse:
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
    return FileResponse(resolved)


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
