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
    role: Literal["content", "soundtrack", "preview", "cover", "artwork"]
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


class ScrapeData(BaseModel):
    id: str = Field(min_length=1)
    source: ScrapeSource
    content: ScrapeContent
    author: ScrapeAuthor
    media: list[MediaAsset]
    music: MusicMetadata | None = None
    engagement: EngagementMetadata | None = None
    safety: SafetyMetadata | None = None
    link: LinkMetadata | None = None


class ResponseMetadata(BaseModel):
    api_version: Literal["1"] = "1"


class ScrapeV1Response(BaseModel):
    data: ScrapeData
    meta: ResponseMetadata = Field(default_factory=ResponseMetadata)


class ApiError(BaseModel):
    code: str
    message: str
    details: Any | None = None


class ApiErrorResponse(BaseModel):
    error: ApiError


# ---------------------------------------------------------------------------
# v2 Public Web Response Schemas
# ---------------------------------------------------------------------------
class WebAssetTunnelDelivery(BaseModel):
    kind: Literal["tunnel"] = "tunnel"
    url: str
    expires_at: int


class WebAssetJobDelivery(BaseModel):
    kind: Literal["job"] = "job"
    job_id: str
    status_url: str
    expires_at: int


class WebAssetV2(BaseModel):
    id: str
    asset_key: str
    index: int
    type: Literal["image", "video", "audio"]
    role: Literal["content", "soundtrack", "preview", "cover", "artwork"]
    availability: Literal["full", "preview", "metadata-only"] = "full"
    filename: str
    mime_type: str | None = None
    size: int | None = None
    dimensions: MediaDimensions | None = None
    duration_seconds: float | None = Field(default=None, ge=0)
    bitrate: int | None = Field(default=None, ge=0)
    looping: bool = False
    delivery: WebAssetTunnelDelivery | WebAssetJobDelivery


class WebCollectionItemV2(BaseModel):
    index: int = Field(ge=0)
    item_id: str = Field(min_length=1)
    title: str
    artist: str | None = None
    album: str | None = None
    duration_seconds: float | None = Field(default=None, ge=0)
    availability: Literal["full", "preview", "metadata-only"]
    classifications: list[str] = Field(default_factory=list)
    asset_count: int = Field(default=0, ge=0)
    delivery_status: Literal["select-item", "processing-required", "unavailable"]


class ScrapeV2Content(BaseModel):
    shortcode: str = Field(min_length=1)
    title: str | None = None
    text: str | None = None
    html: str | None = None
    published_at: datetime | None = None
    album: str | None = None
    duration_seconds: float | None = Field(default=None, ge=0)
    availability: Literal["full", "preview", "metadata-only"] = "full"
    classifications: list[str] = Field(default_factory=list)
    item_count: int = Field(default=0, ge=0)
    resolved_item_count: int = Field(default=0, ge=0)
    collection_truncated: bool = False


class ScrapeV2WebReadyResponse(BaseModel):
    status: Literal["ready"] = "ready"
    request_id: str
    source: ScrapeSource
    content: ScrapeV2Content
    author: ScrapeAuthor
    assets: list[WebAssetV2]
    collection: list[WebCollectionItemV2] = Field(default_factory=list)


class ScrapeV2WebProcessingResponse(BaseModel):
    status: Literal["processing"] = "processing"
    request_id: str
    job_id: str
    status_url: str
    expires_at: int
    retry_after: int = 2
