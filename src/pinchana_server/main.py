"""Pinchana Server — dynamically loads plugins or manages containers."""

import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import httpx
from pinchana_core.models import ScrapeRequest, ScrapeResponse
from pinchana_core.plugins import registry
from pinchana_core.storage import MediaStorage
from pinchana_core.docker_manager import ContainerRegistry, ModuleContainerManager
from pinchana_core.vpn import GluetunController, VpnRotationError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    resp = await forward_client.post(f"{endpoint}/scrape", json={"url": str(request.url)})
    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.error(
            "Upstream module %s (%s) returned %s for /scrape: %s",
            module_name, endpoint, resp.status_code, resp.text,
        )
        raise HTTPException(status_code=resp.status_code, detail=resp.text) from e
    return ScrapeResponse(**resp.json())


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.post("/scrape", response_model=ScrapeResponse)
async def process_scrape_request(request: ScrapeRequest):
    """Unified scrape endpoint — routes to plugin or container module."""
    url = str(request.url)
    mode, name, target = _resolve_module(url)

    if mode is None:
        raise HTTPException(
            status_code=400,
            detail="No module handles this URL. "
                   f"Plugins: {[p.route_patterns for p in registry._plugins.values()]}  "
                   f"Containers: {[{n: m.route_patterns} for n, m in container_registry.modules.items()]}"
        )

    started = time.perf_counter()
    if mode == "in_process":
        if internal_client is None:
            raise HTTPException(status_code=503, detail="Internal HTTP client is not ready")
        resp = await internal_client.post(f"/{name}/scrape", json={"url": url})
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        result = ScrapeResponse(**resp.json())
        logger.info("scrape_complete module=%s mode=%s elapsed_ms=%.1f", name, mode, (time.perf_counter() - started) * 1000)
        return result

    # container
    result = await _forward_to_container(name, request)
    logger.info("scrape_complete module=%s mode=%s elapsed_ms=%.1f", name, mode, (time.perf_counter() - started) * 1000)
    return result


@app.get("/media/{platform}/{post_id}/{filename:path}")
async def serve_media(platform: str, post_id: str, filename: str):
    if ".." in filename or filename.startswith("/"):
        raise HTTPException(status_code=404, detail="Invalid path")

    file_path = storage.base_path / post_id / filename
    resolved = file_path.resolve()
    base_resolved = storage.base_path.resolve()
    if not str(resolved).startswith(str(base_resolved)):
        raise HTTPException(status_code=404, detail="Invalid path")

    if not resolved.exists() or not resolved.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(resolved)


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
async def admin_rotate_vpn():
    """Manually trigger a VPN IP rotation via Gluetun."""
    try:
        await gluetun.rotate_ip()
        return {"status": "rotated"}
    except VpnRotationError as e:
        raise HTTPException(status_code=503, detail=f"VPN rotation failed: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error during rotation: {e}")


@app.get("/admin/vpn/status")
async def admin_vpn_status():
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
async def admin_start_module(name: str):
    if not container_manager:
        raise HTTPException(status_code=501, detail="Container mode is not enabled")
    if name not in container_manager.modules:
        raise HTTPException(status_code=404, detail=f"Module {name} not in config")
    endpoint = container_manager.start(name)
    return {"status": "started", "endpoint": endpoint}


@app.post("/admin/modules/{name}/stop")
async def admin_stop_module(name: str):
    if not container_manager:
        raise HTTPException(status_code=501, detail="Container mode is not enabled")
    container_manager.stop(name)
    return {"status": "stopped"}


@app.get("/admin/modules")
async def admin_list_modules():
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
