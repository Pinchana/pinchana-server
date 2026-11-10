"""Owner-requested media cache invalidation; never scrape or download a post."""

import asyncio
import os
import re
import shutil
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit, urlunsplit

import httpx
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field

SHORTCODE = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
HOSTS = {
    "tiktok": {"tiktok.com", "www.tiktok.com", "m.tiktok.com", "vm.tiktok.com", "vt.tiktok.com", "tt.site", "www.tt.site"},
    "instagram": {"instagram.com", "www.instagram.com"},
    "threads": {"threads.net", "www.threads.net", "threads.com", "www.threads.com"},
    "youtube": {"youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com", "youtu.be"},
    "twitter": {"x.com", "www.x.com", "twitter.com", "www.twitter.com", "mobile.twitter.com"},
    "coub": {"coub.com", "www.coub.com"},
    "apple_music": {"music.apple.com"},
    "spotify": {"open.spotify.com", "spotify.link"},
    "deezer": {"deezer.com", "www.deezer.com", "link.deezer.com", "deezer.page.link"},
}


class CacheClearRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    target: str = Field(min_length=1, max_length=2048)


class CacheClearResponse(BaseModel):
    shortcode: str | None
    deleted_posts: int = Field(ge=0)
    freed_bytes: int = Field(ge=0)


def _validated_url(url: str, platform: str | None = None) -> tuple[str, str]:
    try:
        parsed = urlsplit(url)
        valid = (
            parsed.scheme == "https"
            and not parsed.username and not parsed.password
            and parsed.port in (None, 443)
            and not any(char.isspace() or ord(char) < 32 for char in url)
            and "\\" not in url
        )
    except ValueError:
        valid = False
    if valid:
        for name, hosts in HOSTS.items():
            if parsed.hostname in hosts and platform in (None, name):
                host = "www.tt.site" if parsed.hostname == "tt.site" else parsed.hostname
                return urlunsplit(("https", host, parsed.path or "/", parsed.query, "")), name
    raise HTTPException(400, "Unsupported or unsafe cache URL")


def _url_shortcode(url: str, platform: str) -> str | None:
    parsed = urlsplit(url)
    path = parsed.path
    if platform == "instagram" and path.startswith("/share/"):
        return None  # The share token is not the canonical post shortcode.
    pattern = {
        "tiktok": r"/(?:@[^/]+/(?:video|photo)|v|share/video)/(\d+)(?:\.html)?/?",
        "instagram": r"/(?:[^/]+/)?(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)/?",
        "threads": r"/(?:@[^/]+/)?(?:post|t)/([A-Za-z0-9_-]+)/?",
        "twitter": r"/(?:[^/]+/status|i/status|i/web/status)/(\d+)(?:/.*)?",
        "coub": r"/(?:view|embed)/([A-Za-z0-9_-]+)/?",
    }.get(platform)
    if pattern and (match := re.fullmatch(pattern, path)):
        return match.group(1)
    if platform == "youtube":
        if parsed.hostname == "youtu.be":
            candidate = path.strip("/")
        elif match := re.fullmatch(r"/(?:shorts|embed)/([A-Za-z0-9_-]+)/?", path):
            candidate = match.group(1)
        else:
            candidate = parse_qs(parsed.query).get("v", [""])[0] if path == "/watch" else ""
        return candidate if re.fullmatch(r"[A-Za-z0-9_-]{11}", candidate) else None
    if platform == "apple_music":
        match = re.fullmatch(r"/(?:[a-zA-Z]{2}/)?(song|album)/(?:(?:[^/]+)/)?(\d+)/?", path)
        if match:
            kind, identifier = match.groups()
            selected = parse_qs(parsed.query, keep_blank_values=True).get("i")
            if kind == "album":
                return f"am-{selected[0]}" if selected and len(selected) == 1 and selected[0].isascii() and selected[0].isdigit() else None
            return f"am-{identifier}" if not selected else None
    if platform in {"spotify", "deezer"}:
        match = re.fullmatch(r"/(?:[a-z]{2}/|intl-[a-z]{2}/)?(track|album)/([A-Za-z0-9]+)/?", path)
        if match:
            kind, identifier = match.groups()
            prefix = "sp" if platform == "spotify" else "dz"
            return f"{prefix}-{'album-' if kind == 'album' else ''}{identifier}"
    return None


def _is_share_url(url: str, platform: str) -> bool:
    parsed = urlsplit(url)
    if platform == "tiktok":
        pattern = r"/[A-Za-z0-9_-]+/?" if parsed.hostname in {"vm.tiktok.com", "vt.tiktok.com"} else r"/t/[A-Za-z0-9_-]+/?"
        return bool(re.fullmatch(pattern, parsed.path))
    if platform in {"instagram", "threads"}:
        return bool(re.fullmatch(r"/share/(?:reel/|p/)?[A-Za-z0-9_-]+/?", parsed.path))
    return parsed.hostname in {"spotify.link", "link.deezer.com", "deezer.page.link"}


async def resolve_cache_target(target: str, client: httpx.AsyncClient | None) -> str | None:
    target = target.strip()
    if target == "all":
        return None
    if SHORTCODE.fullmatch(target):
        return target
    current, platform = _validated_url(target)
    # At most three streamed GETs, including tt.site -> vm fallback. Do not
    # fetch the final post or follow redirects to untrusted destinations.
    async with asyncio.timeout(20):
        for _ in range(3):
            if shortcode := _url_shortcode(current, platform):
                if SHORTCODE.fullmatch(shortcode):
                    return shortcode
                raise HTTPException(400, "Invalid cache shortcode")
            if not _is_share_url(current, platform):
                raise HTTPException(400, "Cannot determine shortcode; provide it directly")
            if client is None:
                raise HTTPException(503, "Gateway HTTP client is not ready")
            try:
                async with client.stream(
                    "GET", current, follow_redirects=False, timeout=5,
                    headers={"User-Agent": "Mozilla/5.0"},
                ) as response:
                    status = response.status_code
                    location = response.headers.get("location")
            except httpx.HTTPError as exc:
                if urlsplit(current).hostname == "www.tt.site":
                    current = f"https://vm.tiktok.com/{urlsplit(current).path.strip('/').split('/')[-1]}/"
                    continue
                raise HTTPException(503, "Share link is temporarily unavailable") from exc
            if status >= 500 and urlsplit(current).hostname == "www.tt.site":
                current = f"https://vm.tiktok.com/{urlsplit(current).path.strip('/').split('/')[-1]}/"
                continue
            if status in {301, 302, 303, 307, 308} and location:
                current, _ = _validated_url(urljoin(current, location), platform)
                # Allow the last response to resolve to a post, without GET #4.
                if shortcode := _url_shortcode(current, platform):
                    if SHORTCODE.fullmatch(shortcode):
                        return shortcode
                continue
            raise HTTPException(503 if status == 429 or status >= 500 else 400, "Share link could not be resolved")
    raise HTTPException(400, "Share link redirect limit exceeded")


def clear_media_cache(root: Path, shortcode: str | None) -> CacheClearResponse:
    """Remove completed post directories only, leaving DBs and service state alone.

    rmtree does not follow symlinks inside a post; symlinked post directories
    are refused too. Filesystem work runs off the event loop at the endpoint.
    Active downloads can recreate an entry after invalidation.
    """
    if shortcode is not None and not SHORTCODE.fullmatch(shortcode):
        raise ValueError("Invalid cache shortcode")
    root = root.resolve()
    if root == Path(root.anchor):
        raise ValueError("Refusing to clear a filesystem root")
    deleted = freed = 0
    if root.is_dir():
        entries = [root / shortcode] if shortcode is not None else list(root.iterdir())
        for entry in entries:
            if (
                not SHORTCODE.fullmatch(entry.name)
                or entry.is_symlink() or not entry.is_dir()
                or not (entry / "metadata.json").is_file()
            ):
                continue
            size = 0
            for directory, _, files in os.walk(entry, followlinks=False):
                for filename in files:
                    path = Path(directory) / filename
                    try:
                        if not path.is_symlink():
                            size += path.stat().st_size
                    except FileNotFoundError:
                        pass  # A concurrent eviction already removed it.
            try:
                shutil.rmtree(entry)
            except FileNotFoundError:
                continue
            deleted += 1
            freed += size
    return CacheClearResponse(shortcode=shortcode, deleted_posts=deleted, freed_bytes=freed)
