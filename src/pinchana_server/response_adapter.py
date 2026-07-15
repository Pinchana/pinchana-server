"""Normalize legacy scraper-module payloads into the public v1 contract."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from .media_probe import MediaDimensionProbe
from .schemas import (
    EngagementMetadata,
    LinkMetadata,
    MediaAsset,
    MusicMetadata,
    Platform,
    SafetyMetadata,
    ScrapeAuthor,
    ScrapeContent,
    ScrapeData,
    ScrapeSource,
    ScrapeV1Response,
)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _non_negative_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return None
    return normalized if normalized >= 0 else None


def _author(raw: dict[str, Any], platform: Platform) -> ScrapeAuthor:
    author = _text(raw.get("author"))
    explicit_username = _text(raw.get("username"))
    if platform == "twitter":
        return ScrapeAuthor(
            name=_text(raw.get("author_name")) or author,
            username=explicit_username or author,
        )
    if platform in {"instagram", "threads"}:
        return ScrapeAuthor(name=author, username=explicit_username or author)
    return ScrapeAuthor(name=author, username=explicit_username)


def _engagement(raw: dict[str, Any]) -> EngagementMetadata | None:
    mapping = {
        "likes": "like_count",
        "replies": "reply_count",
        "reposts": "repost_count",
        "quotes": "quote_count",
        "views": "view_count",
    }
    if not any(source in raw for source in mapping.values()):
        return None
    return EngagementMetadata(**{
        target: _non_negative_int(raw.get(source))
        for target, source in mapping.items()
    })


def _safety(raw: dict[str, Any]) -> SafetyMetadata | None:
    keys = ("spoiler", "text_spoiler", "nsfw")
    if not any(key in raw for key in keys):
        return None
    return SafetyMetadata(**{key: bool(raw.get(key, False)) for key in keys})


def _asset(
    *,
    media_type: str,
    role: str,
    url: Any,
    preview_url: Any = None,
    duration: Any = None,
    title: Any = None,
    artist: Any = None,
) -> dict[str, Any] | None:
    normalized_url = _text(url)
    if not normalized_url:
        return None
    return {
        "type": media_type,
        "role": role,
        "url": normalized_url,
        "preview_url": _text(preview_url),
        "duration_seconds": _non_negative_int(duration),
        "title": _text(title),
        "artist": _text(artist),
    }


def _media_descriptors(raw: dict[str, Any]) -> list[dict[str, Any]]:
    descriptors: list[dict[str, Any]] = []
    tracklist = raw.get("tracklist")
    if isinstance(tracklist, list) and tracklist:
        for track in tracklist:
            if not isinstance(track, dict):
                continue
            item = _asset(
                media_type="audio",
                role="content",
                url=track.get("audio_url"),
                title=track.get("title"),
                artist=track.get("artist"),
            )
            if item:
                descriptors.append(item)
    else:
        carousel = raw.get("carousel")
        if isinstance(carousel, list) and carousel:
            for item in carousel:
                if not isinstance(item, dict):
                    continue
                if _text(item.get("video_url")):
                    descriptor = _asset(
                        media_type="video",
                        role="content",
                        url=item.get("video_url"),
                        preview_url=item.get("thumbnail_url"),
                    )
                else:
                    descriptor = _asset(
                        media_type="image",
                        role="content",
                        url=item.get("thumbnail_url"),
                    )
                if descriptor:
                    descriptors.append(descriptor)
        elif _text(raw.get("video_url")):
            descriptor = _asset(
                media_type="video",
                role="content",
                url=raw.get("video_url"),
                preview_url=raw.get("thumbnail_url"),
                duration=raw.get("duration"),
            )
            if descriptor:
                descriptors.append(descriptor)
        elif _text(raw.get("audio_url")):
            descriptor = _asset(
                media_type="audio",
                role="content",
                url=raw.get("audio_url"),
                duration=raw.get("duration"),
                title=raw.get("title"),
                artist=raw.get("author"),
            )
            if descriptor:
                descriptors.append(descriptor)
        else:
            descriptor = _asset(
                media_type="image",
                role="content",
                url=raw.get("thumbnail_url"),
            )
            if descriptor:
                descriptors.append(descriptor)

        audio_url = _text(raw.get("audio_url"))
        if audio_url and not any(item["url"] == audio_url for item in descriptors):
            descriptor = _asset(
                media_type="audio",
                role="soundtrack" if descriptors else "content",
                url=audio_url,
                duration=raw.get("duration"),
                title=raw.get("title"),
                artist=raw.get("author"),
            )
            if descriptor:
                descriptors.append(descriptor)

    cover_url = _text(raw.get("cover_url"))
    if cover_url and not any(item["url"] == cover_url for item in descriptors):
        descriptor = _asset(media_type="image", role="cover", url=cover_url)
        if descriptor:
            descriptors.append(descriptor)

    music = raw.get("music")
    if isinstance(music, dict):
        music_audio = _asset(
            media_type="audio",
            role="soundtrack",
            url=music.get("audio_url"),
            duration=music.get("duration_seconds"),
            title=music.get("title"),
            artist=music.get("artist"),
        )
        if music_audio and not any(item["url"] == music_audio["url"] for item in descriptors):
            descriptors.append(music_audio)
        music_cover = _asset(
            media_type="image",
            role="cover",
            url=music.get("cover_url"),
        )
        if music_cover and not any(item["url"] == music_cover["url"] for item in descriptors):
            descriptors.append(music_cover)
    return descriptors


def _published_at(raw: dict[str, Any]) -> datetime | None:
    value = raw.get("taken_at") or raw.get("created_at")
    if value is None:
        return None
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        for parser in (
            lambda text: datetime.fromisoformat(text.replace("Z", "+00:00")),
            lambda text: datetime.strptime(text, "%a %b %d %H:%M:%S %z %Y"),
        ):
            try:
                parsed = parser(value)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


async def normalize_scrape_response(
    raw: dict[str, Any],
    *,
    platform: Platform,
    source_url: str,
    probe: MediaDimensionProbe,
) -> ScrapeV1Response:
    descriptors = _media_descriptors(raw)
    dimensions = await asyncio.gather(*(
        probe.dimensions_for(item["url"], item["type"])
        for item in descriptors
    ))
    media = [
        MediaAsset(index=index, dimensions=dimensions[index], **item)
        for index, item in enumerate(descriptors)
    ]

    identifier = _text(raw.get("shortcode"))
    if identifier is None:
        raise ValueError("Scraper response is missing its identifier")
    album = _text(raw.get("album"))
    link = _text(raw.get("link"))
    return ScrapeV1Response(data=ScrapeData(
        id=identifier,
        source=ScrapeSource(
            platform=platform,
            url=source_url,
            application=_text(raw.get("source")),
        ),
        content=ScrapeContent(
            title=_text(raw.get("title")),
            text=_text(raw.get("caption")),
            html=_text(raw.get("text_html")),
            published_at=_published_at(raw),
        ),
        author=_author(raw, platform),
        media=media,
        music=MusicMetadata(album=album) if album else None,
        engagement=_engagement(raw),
        safety=_safety(raw),
        link=LinkMetadata(url=link) if link else None,
    ))
