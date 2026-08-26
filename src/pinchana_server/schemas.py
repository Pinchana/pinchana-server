"""Public schemas for the versioned Pinchana scrape API."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl

Platform = Literal[
    "tiktok",
    "instagram",
    "shorts",
    "soundcloud",
    "ytmusic",
    "spotify",
    "deezer",
    "threads",
    "twitter",
]


class MediaDimensions(BaseModel):
    width: int = Field(gt=0)
    height: int = Field(gt=0)


class MediaAsset(BaseModel):
    index: int = Field(ge=0)
    type: Literal["image", "video", "audio"]
    role: Literal["content", "soundtrack", "cover"]
    url: str = Field(min_length=1)
    preview_url: str | None = None
    dimensions: MediaDimensions | None = None
    duration_seconds: int | None = Field(default=None, ge=0)
    title: str | None = None
    artist: str | None = None
    looping: bool = False


class ScrapeSource(BaseModel):
    platform: Platform
    url: HttpUrl
    application: str | None = None


class ScrapeContent(BaseModel):
    title: str | None = None
    text: str | None = None
    html: str | None = None
    published_at: datetime | None = None


class ScrapeAuthor(BaseModel):
    name: str | None = None
    username: str | None = None


class MusicMetadata(BaseModel):
    album: str | None = None


class EngagementMetadata(BaseModel):
    likes: int | None = Field(default=None, ge=0)
    replies: int | None = Field(default=None, ge=0)
    reposts: int | None = Field(default=None, ge=0)
    quotes: int | None = Field(default=None, ge=0)
    views: int | None = Field(default=None, ge=0)


class SafetyMetadata(BaseModel):
    spoiler: bool = False
    text_spoiler: bool = False
    nsfw: bool = False


class LinkMetadata(BaseModel):
    url: str = Field(min_length=1)


class EmbeddedPostData(BaseModel):
    id: str = Field(min_length=1)
    source: ScrapeSource
    content: ScrapeContent
    author: ScrapeAuthor
    media: list[MediaAsset]
    music: MusicMetadata | None = None
    engagement: EngagementMetadata | None = None
    safety: SafetyMetadata | None = None
    link: LinkMetadata | None = None


class ScrapeData(EmbeddedPostData):
    quote: EmbeddedPostData | None = None


class InspectedPostData(BaseModel):
    id: str = Field(min_length=1)
    source: ScrapeSource
    content: ScrapeContent
    author: ScrapeAuthor
    quote: "InspectedEmbeddedPostData | None" = None


class InspectedEmbeddedPostData(BaseModel):
    id: str = Field(min_length=1)
    source: ScrapeSource
    content: ScrapeContent
    author: ScrapeAuthor


class ResponseMetadata(BaseModel):
    api_version: Literal["1"] = "1"


class ScrapeV1Response(BaseModel):
    data: ScrapeData
    meta: ResponseMetadata = Field(default_factory=ResponseMetadata)


class InspectV1Response(BaseModel):
    data: InspectedPostData
    meta: ResponseMetadata = Field(default_factory=ResponseMetadata)


class ApiError(BaseModel):
    code: str
    message: str
    details: Any | None = None


class ApiErrorResponse(BaseModel):
    error: ApiError
