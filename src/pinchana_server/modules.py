"""Load the packaged scrapers and call their handlers without an HTTP hop."""

import asyncio
from dataclasses import dataclass
from importlib import import_module

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRoute
from pinchana_core.models import ScrapeRequest
from pinchana_core.plugins import registry
from pinchana_core.storage import set_cache_scope
from pydantic import HttpUrl

PACKAGES = {
    "tiktok": "pinchana_tiktok",
    "instagram": "pinchana_inst",
    "shorts": "pinchana_shorts",
    "soundcloud": "pinchana_soundcloud",
    "ytmusic": "pinchana_ytmusic",
    "spotify": "pinchana_spotify",
    "apple_music": "pinchana_apple_music",
    "deezer": "pinchana_deezer",
    "threads": "pinchana_threads",
    "twitter": "pinchana_twitter",
    "coub": "pinchana_coub",
    "bluesky": "pinchana_bluesky",
}
TWITTER_SHARE_HOSTS = ("vxtwitter.com", "fxtwitter.com")


@dataclass(frozen=True)
class ScraperModule:
    name: str
    app: FastAPI
    patterns: tuple[str, ...]
    routes: dict[str, APIRoute]

    def matches(self, url: httpx.URL) -> bool:
        if self.name == "bluesky":
            return url.host in self.patterns
        for pattern in self.patterns:
            host, slash, path = pattern.partition("/")
            if url.host != host and not url.host.endswith(f".{host}"):
                continue
            if not slash or url.path.startswith(f"/{path}"):
                return True
        return False

    async def run(
        self, operation: str, request: ScrapeRequest, *, cache_scope: str | None = None,
    ) -> dict:
        route = self.routes.get(f"/{operation}")
        if route is None or "POST" not in route.methods:
            raise HTTPException(400, f"{operation.capitalize()} is unavailable for {self.name}")
        url = request.url
        if self.name == "twitter" and (url.host or "").removeprefix("www.") in TWITTER_SHARE_HOSTS:
            url = HttpUrl(str(httpx.URL(str(url)).copy_with(host="x.com")))
        request = request.model_copy(update={"cache_scope": cache_scope, "url": url})
        # The model copy doesn't rerun the cache-scope validator.
        set_cache_scope(request.cache_scope)
        try:
            timeout = 600 if self.name == "apple_music" else 120
            async with asyncio.timeout(timeout):
                result = jsonable_encoder(await route.endpoint(request))
            if not isinstance(result, dict):
                raise HTTPException(502, "The scraper returned an invalid response")
            return result
        except TimeoutError as exc:
            raise HTTPException(503, f"The {self.name} scraper timed out") from exc
        finally:
            set_cache_scope(None)

    async def health(self) -> dict:
        try:
            async with asyncio.timeout(5):
                detail = await self.routes["/health"].endpoint()
        except HTTPException as exc:
            return {"mode": "in_process", "status": "unhealthy", "detail": exc.detail}
        except Exception as exc:
            return {"mode": "in_process", "status": "unhealthy", "detail": str(exc)}
        return {"mode": "in_process", "status": "healthy", "detail": detail}


def _load_module(name: str, package: str) -> ScraperModule:
    app = import_module(f"{package}.main").app
    plugin = registry.get(name)
    if plugin is None:
        raise RuntimeError(f"{package} did not register the {name} scraper")
    routes = {route.path: route for route in plugin.router.routes if isinstance(route, APIRoute)}
    for path in ("/scrape", "/health"):
        if path not in routes:
            raise RuntimeError(f"{package} is missing its {path} handler")
    patterns = tuple(plugin.route_patterns)
    if name == "twitter":
        patterns += TWITTER_SHARE_HOSTS
    return ScraperModule(name, app, patterns, routes)


modules = {name: _load_module(name, package) for name, package in PACKAGES.items()}
