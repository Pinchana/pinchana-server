"""Bounded, web-session-owned playlist preparation using the normal scrapers.

This is intentionally independent of DLP: cached scraper media is copied into a
ZIP, not returned as a scrape result or exposed through machine/mobile routes.
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
import sys
import tempfile
import time
import uuid
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable, Literal
from urllib.parse import parse_qs, unquote, urlsplit

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator

from .schemas import ScrapeV1Response

MAX_ITEMS = 100
MAX_BYTES = 2 * 1024**3
JOB_TTL = 15 * 60
JOB_TIMEOUT = 30 * 60
MAX_JOBS = 8

HOSTS = {
    "youtube.com": "youtube", "www.youtube.com": "youtube", "m.youtube.com": "youtube",
    "youtu.be": "youtube", "music.youtube.com": "ytmusic",
    "soundcloud.com": "soundcloud", "www.soundcloud.com": "soundcloud",
    "on.soundcloud.com": "soundcloud", "open.spotify.com": "spotify",
    "deezer.com": "deezer", "www.deezer.com": "deezer",
    "deezer.page.link": "deezer", "link.deezer.com": "deezer",
    "tiktok.com": "tiktok", "www.tiktok.com": "tiktok", "vm.tiktok.com": "tiktok",
    "vt.tiktok.com": "tiktok", "tt.site": "tiktok",
    "instagram.com": "instagram", "www.instagram.com": "instagram", "instagr.am": "instagram",
    "threads.net": "threads", "www.threads.net": "threads", "threads.com": "threads",
    "www.threads.com": "threads", "x.com": "twitter", "www.x.com": "twitter",
    "twitter.com": "twitter", "www.twitter.com": "twitter",
    "vxtwitter.com": "twitter", "fxtwitter.com": "twitter", "coub.com": "coub",
    "www.coub.com": "coub", "m.tiktok.com": "tiktok",
    "mobile.twitter.com": "twitter",
}


def platform_for(url: str) -> str:
    parsed = urlsplit(url)
    platform = HOSTS.get((parsed.hostname or "").lower())
    if parsed.scheme != "https" or parsed.username or parsed.password or parsed.port not in (None, 443) or not platform:
        raise HTTPException(400, "Playlists require supported public HTTPS platform URLs")
    return platform


def is_collection(url: str) -> bool:
    platform = platform_for(url)
    parsed = urlsplit(url)
    if platform in {"youtube", "ytmusic"}:
        return bool(parse_qs(parsed.query).get("list"))
    if platform == "soundcloud":
        return bool(re.fullmatch(r"/[^/]+/sets/[^/]+/?", parsed.path))
    if platform in {"spotify", "deezer"}:
        return bool(re.fullmatch(r"/(?:intl-[a-z]{2}/|[a-z]{2}/)?(?:playlist|album)/[A-Za-z0-9]+/?", parsed.path))
    if platform == "tiktok":
        return bool(re.fullmatch(r"/@[^/]+/(?:collection|playlist)/[^/]+/?", parsed.path))
    return False


class PlaylistRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: HttpUrl | None = None
    urls: list[HttpUrl] | None = Field(default=None, min_length=1, max_length=MAX_ITEMS)
    mode: Literal["media", "audio"] = "media"
    filenameStyle: Literal["classic", "basic", "pretty", "nerdy"] = "pretty"

    @model_validator(mode="after")
    def valid_sources(self):
        if (self.url is None) == (self.urls is None):
            raise ValueError("Provide either a playlist URL or a list of media URLs")
        for url in self.urls or [self.url]:
            if len(str(url)) > 2048:
                raise ValueError("URL is too long")
            platform_for(str(url))
        if self.url is not None and not is_collection(str(self.url)):
            raise ValueError("Not a supported playlist URL; use urls for a custom playlist")
        if self.urls and any(is_collection(str(url)) for url in self.urls):
            raise ValueError("Nested playlists are not supported")
        return self


async def extract_collection(url: str, directory: Path) -> tuple[str, list[str]]:
    """Metadata-only extraction; downloads still go through platform modules."""
    output = directory / "playlist.json"
    with output.open("wb") as stream:
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "yt_dlp", "--ignore-config", "--flat-playlist",
            "--dump-single-json", "--skip-download", "--yes-playlist", "--no-warnings",
            "--playlist-end", str(MAX_ITEMS + 1), "--socket-timeout", "15", "--retries", "1",
            "--use-extractors", "youtube.*,soundcloud.*,tiktok.*", "--", url,
            stdout=stream, stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            await asyncio.wait_for(process.wait(), timeout=90)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
    if process.returncode or output.stat().st_size > 4 * 1024**2:
        raise HTTPException(400, "Could not read this public playlist")
    try:
        payload = json.loads(output.read_text())
        entries = payload["entries"]
        if not isinstance(entries, list) or not entries:
            raise ValueError("empty playlist")
        if len(entries) > MAX_ITEMS or (payload.get("playlist_count") or 0) > MAX_ITEMS:
            raise HTTPException(400, f"Playlists are limited to {MAX_ITEMS} items")
        urls = []
        source_platform = platform_for(url)
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError("invalid playlist entry")
            candidate = entry.get("webpage_url") or entry.get("url")
            # Flat YouTube entries can contain only a video ID.
            if source_platform in {"youtube", "ytmusic"}:
                identifier = entry.get("id")
                if not re.fullmatch(r"[A-Za-z0-9_-]{11}", identifier or ""):
                    raise ValueError("invalid video ID")
                host = "music.youtube.com" if source_platform == "ytmusic" else "www.youtube.com"
                candidate = f"https://{host}/watch?v={identifier}"
            if not isinstance(candidate, str) or platform_for(candidate) != source_platform:
                raise ValueError("playlist contains an unsupported URL")
            urls.append(candidate)
        return str(payload.get("title") or "Playlist"), urls
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(400, "Playlist is empty or contains unavailable items") from exc
    finally:
        output.unlink(missing_ok=True)


def clean_name(value: str) -> str:
    cleaned = re.sub(r'[\x00-\x1f\x7f/\\:*?"<>|]', "_", value).strip(" .")
    return cleaned.encode("utf-8")[:180].decode("utf-8", errors="ignore").rstrip(" .") or "Playlist"


def cached_asset(base: Path, url: str) -> Path:
    """Only copy regular cached files, never remote URLs or symlink targets."""
    parsed = urlsplit(url)
    parts = unquote(parsed.path).split("/")
    if parsed.scheme or parsed.netloc or len(parts) < 5 or parts[1] != "media":
        raise HTTPException(502, "Playlist scraper returned non-local media")
    relative = parts[3:]  # /media/<platform>/<post>/<file>
    if any(part in {"", ".", ".."} or "\\" in part or "\x00" in part for part in relative):
        raise HTTPException(502, "Playlist scraper returned an unsafe media path")
    path = base.resolve()
    for part in relative:
        path = path / part
        if path.is_symlink():
            raise HTTPException(502, "Playlist media cannot be a symlink")
    if not path.is_file():
        raise HTTPException(502, "Playlist media is no longer cached; prepare it again")
    return path


@dataclass
class PlaylistJob:
    owner: str
    directory: Path
    expires_at: int
    status: str = "PROCESSING"
    stage: str = "resolving"
    completed: int = 0
    total: int = 0
    size: int | None = None
    filename: str = "Playlist [pinchana.cc].zip"
    error: str | None = None
    task: asyncio.Task | None = field(default=None, repr=False)

    def public(self, job_id: str) -> dict:
        return {
            "jobId": job_id, "status": self.status, "stage": self.stage,
            "completed": self.completed, "total": self.total,
            "progress": 100 if self.status == "READY" else round(self.completed / self.total * 95) if self.total else None,
            "expiresAt": self.expires_at, "size": self.size,
            "filename": self.filename, "error": self.error,
        }


class PlaylistJobs:
    def __init__(self):
        self.jobs: dict[str, PlaylistJob] = {}
        self.slots = asyncio.Semaphore(2)
        self.sweeper: asyncio.Task | None = None

    async def _sweep(self):
        while True:
            await asyncio.sleep(30)
            self.prune()

    async def close(self):
        tasks = [job.task for job in self.jobs.values() if job.task]
        if self.sweeper:
            tasks.append(self.sweeper)
            self.sweeper = None
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for job in self.jobs.values():
            shutil.rmtree(job.directory, ignore_errors=True)
        self.jobs.clear()

    def prune(self):
        for job_id, job in list(self.jobs.items()):
            if job.expires_at <= time.time() and (not job.task or job.task.done()):
                shutil.rmtree(job.directory, ignore_errors=True)
                del self.jobs[job_id]

    def get(self, job_id: str, owner: str) -> PlaylistJob:
        self.prune()
        job = self.jobs.get(job_id)
        if not job or job.owner != owner:
            raise HTTPException(404, "Playlist job not found or expired")
        return job

    def submit(
        self, owner: str, request: PlaylistRequest, base: Path,
        resolve: Callable[[str, Path], Awaitable[tuple[str, list[str]]]],
        scrape: Callable[[str], Awaitable[ScrapeV1Response]],
        audio: Callable[[Path, Path], Awaitable[None]],
    ) -> dict:
        self.prune()
        if self.sweeper is None:
            self.sweeper = asyncio.create_task(self._sweep())
        if len(self.jobs) >= MAX_JOBS or any(job.owner == owner and job.status == "PROCESSING" for job in self.jobs.values()):
            raise HTTPException(429, "Playlist preparation is busy; try again shortly")
        job_id = str(uuid.uuid4())
        job = PlaylistJob(owner, Path(tempfile.mkdtemp(prefix="pinchana-playlist-")), int(time.time()) + JOB_TIMEOUT + JOB_TTL)
        self.jobs[job_id] = job
        job.task = asyncio.create_task(self._run(job, request, base, resolve, scrape, audio))
        return job.public(job_id)

    async def _run(self, job, request, base, resolve, scrape, audio):
        try:
            async with asyncio.timeout(JOB_TIMEOUT), self.slots:
                title, urls = await resolve(str(request.url), job.directory) if request.url else ("Playlist", [str(url) for url in request.urls])
                if not urls or len(urls) > MAX_ITEMS:
                    raise HTTPException(400, f"Playlists must contain 1–{MAX_ITEMS} items")
                job.total = len(urls)
                job.filename = f"{clean_name(title)} [pinchana.cc].zip"
                used_bytes = 0
                archive = job.directory / "playlist.zip"
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as bundle:
                    for index, url in enumerate(urls, 1):
                        platform_for(url)
                        if is_collection(url):
                            raise HTTPException(400, "Nested playlists are not supported")
                        job.stage = "downloading"
                        result = await scrape(url)
                        assets = sorted((asset for asset in result.data.media if asset.role != "cover"), key=lambda asset: asset.index)
                        if request.mode == "audio":
                            assets = [asset for asset in assets if asset.type in {"audio", "video"}]
                            if any(asset.type == "audio" for asset in assets):
                                assets = [asset for asset in assets if asset.type == "audio"]
                        if not assets:
                            raise HTTPException(400, f"Playlist item {index} has no downloadable {request.mode}")
                        for number, asset in enumerate(assets, 1):
                            source = cached_asset(base, asset.url)
                            if source.stat().st_size + used_bytes > MAX_BYTES:
                                raise HTTPException(400, "Playlist exceeds the 2 GiB archive limit")
                            converted = None
                            if request.mode == "audio" and asset.type == "video":
                                converted = job.directory / "audio.mp3"
                                await audio(source, converted)
                                source = converted
                            name = result.data.content.title or result.data.id
                            if request.filenameStyle == "classic":
                                name = f"{result.data.source.platform}_{result.data.id}"
                            elif request.filenameStyle == "nerdy":
                                name = f"{name} ({result.data.source.platform}, {result.data.id})"
                            elif request.filenameStyle == "pretty" and result.data.author.name:
                                name = f"{name} - {result.data.author.name}"
                            filename = f"{index:03d}-{number:02d} {clean_name(name)} [pinchana.cc]{source.suffix}"
                            # Bounded file copies yield between chunks; never block the event loop
                            # for an entire archive or leave a background writer after cancellation.
                            with source.open("rb") as incoming, bundle.open(filename, "w", force_zip64=True) as outgoing:
                                while chunk := incoming.read(1024 * 1024):
                                    used_bytes += len(chunk)
                                    if used_bytes > MAX_BYTES:
                                        raise HTTPException(400, "Playlist exceeds the 2 GiB archive limit")
                                    outgoing.write(chunk)
                                    await asyncio.sleep(0)
                            if converted:
                                converted.unlink(missing_ok=True)
                        job.completed = index
                job.stage = "ready"
                job.size = archive.stat().st_size
                job.status = "READY"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            job.status = "FAILED"
            job.error = str(exc.detail) if isinstance(exc, HTTPException) and exc.status_code < 500 else "Playlist preparation failed; try again"
            shutil.rmtree(job.directory, ignore_errors=True)
        finally:
            job.expires_at = int(time.time()) + JOB_TTL
