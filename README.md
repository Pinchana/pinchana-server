# Pinchana Server

Pinchana Server is the central HTTP gateway for Pinchana. It authenticates clients, selects a platform module from configured URL patterns, normalizes API v1 responses, serves protected cached media, and exposes the isolated browser-session flow.

---

## Responsibilities

- Exposes normalized `/v1/scrape` and legacy `/scrape` routes for supported platforms.
- Selects the appropriate module by matching patterns in `modules.yaml`.
- Proxies requests to containerized modules or calls registered in-process plugins.
- Provides authenticated administration routes for Gluetun status and rotation.
- Serves files from the shared media cache with authentication and range support.
- Optionally manages module containers when `CONTAINER_MODE=true` and the Docker socket is mounted.

---

## Request flow

1. A machine client sends `POST /v1/scrape` with a complete HTTP(S) URL and `X-API-Key`.
2. The server validates the request and selects the first matching module from `modules.yaml`.
3. It calls an in-process plugin or forwards the request to the configured module endpoint.
4. It converts the module result to the stable v1 `{data, meta}` envelope.
5. The client retrieves protected `/media/...` paths with the same machine key.

---

## API reference

### `POST /v1/scrape`

Routes the URL to the appropriate scraper and returns the versioned contract.
```json
{
  "url": "https://www.tiktok.com/..."
}
```
This route requires the `X-API-Key` header. Named keys are supplied through the `PINCHANA_API_KEYS` JSON environment variable. The response groups source, content, author, and optional
platform metadata. All downloadable assets are ordered in `data.media`:

```json
{
  "data": {
    "id": "SHORTCODE",
    "source": {
      "platform": "instagram",
      "url": "https://www.instagram.com/p/SHORTCODE/",
      "application": null
    },
    "content": {
      "title": null,
      "text": "Example reel",
      "html": null,
      "published_at": "2026-07-15T11:56:00Z"
    },
    "author": {"name": "creator", "username": "creator"},
    "media": [
      {
        "index": 0,
        "type": "video",
        "role": "content",
        "url": "/media/instagram/SHORTCODE/video.mp4",
        "preview_url": "/media/instagram/SHORTCODE/thumbnail.jpg",
        "dimensions": {"width": 1080, "height": 1920},
        "duration_seconds": null,
        "title": null,
        "artist": null,
        "looping": false
      }
    ],
    "music": null,
    "engagement": null,
    "safety": null,
    "link": null
  },
  "meta": {"api_version": "1"}
}
```

Visual media dimensions are measured from the cached file. If an image or video
cannot be inspected, `dimensions` is `null`; audio always has null dimensions.
Carousels are ordered content items, slideshow audio uses the `soundtrack` role,
and album art uses the `cover` role. Threads music attachments are exposed as a
30-second `soundtrack` preview with title, artist, and a separate `cover` asset.
Errors have the stable shape
`{"error":{"code":"...","message":"...","details":null}}`.

### `POST /scrape`

This compatibility route uses the same authentication, request validation, routing, and forwarding limits but returns the earlier flat response. New clients must use `/v1/scrape`.

### Web routes

- `POST /web/verify` validates a Turnstile token directly with Cloudflare Siteverify and returns a signed browser-session token.
- `GET /web/identity` exposes the project-issued certificate used by the official web client to authorize a custom API origin.
- `GET /web/session` validates that token.
- `GET /web/build` publicly exposes the sanitized source revisions included in the deployment; it never returns configuration or infrastructure details.
- `POST /v1/web/scrape` returns the same normalized v1 contract as `/v1/scrape`, authenticates with the browser-session bearer token, and places protected assets under `/web/media/...`.
- `POST /web/scrape` performs a scrape with the browser-session bearer token.
- `GET /web/media/...` serves protected media to a verified web session.
- `GET /web/capabilities` advertises optional protocol-v2 DLP support.
- `POST /web/convert/gif` converts an authenticated, already-cached media file to GIF; it never accepts uploads or remote URLs. The encoder generates a full-animation palette (up to 256 colors) in a separate FFmpeg pass, uses stable ordered dithering and transparent delta frames, then runs lossless `gifsicle -O3` optimization. It targets 4 MiB, caps delivered files at 8 MiB, and retries smaller resolution/frame-rate profiles from the complete source rather than truncating an oversized GIF. Short clips start at a 640px display-edge ceiling and 15 fps; longer clips start smaller. Display aspect ratio is retained, neither display dimension is upscaled, and the finished timeline is checked against the video stream duration. The existing 50 MiB input, 60-second duration, two-conversion concurrency and one-job-per-session limits remain. All encoding attempts share one 45-second deadline. Docker includes `gifsicle`; direct installations without it retain bounded FFmpeg conversion. GIF remains less efficient than video; clips that cannot meet the size limit should be kept as video.
- `/web/dlp/jobs...` allocates, submits, monitors, and streams owner-bound private-download jobs to the internal DLP service. The gateway forwards cookie ciphertext only.

### Web-only playlists

- `POST /web/playlists/jobs` prepares an ordered playlist archive asynchronously.
- `GET /web/playlists/jobs/{job_id}` reports `PROCESSING`, `READY`, or `FAILED`,
  progress, item counts, expiry, and archive size.
- `GET /web/playlists/jobs/{job_id}/file` downloads the completed ZIP with range support.

All three routes require a verified **web bearer session**, and jobs are bound to
that session. Machine keys and mobile sessions cannot use them. There are no
machine/mobile playlist routes. `/web/capabilities` advertises
`playlists: {available: true, archive: "zip", maxItems: 100}`.

Submit either a direct public collection URL:

```json
{"url":"https://open.spotify.com/playlist/PLAYLIST_ID","mode":"audio","filenameStyle":"pretty"}
```

or a custom playlist of post/track URLs from any supported platform (mixed
platforms are allowed):

```json
{"urls":["https://www.instagram.com/p/POST/","https://x.com/user/status/123"],"mode":"media"}
```

YouTube/YouTube Music playlists, SoundCloud sets, Spotify/Deezer playlists and
albums, and publicly extractable TikTok collections use metadata-only enumeration.
For platforms without a public collection listing, supply individual URLs.
Spotify listing uses its module's existing API credentials. Private collections,
nested collections, unavailable items, and over-limit playlists fail explicitly;
no partial archive is offered. Ordinary single-post/carousel scrape routes are
unchanged.

Downloads run through the normal platform scrapers (including an internal
YouTube scraper operation), **not DLP**. Media mode preserves the scraper files;
audio mode keeps audio tracks or extracts MP3 from video. Covers are excluded.
The result is always one ZIP, even for one item; browser ZIP preferences do not
apply. Limits: 100 items, 2 GiB of archived media, two simultaneous preparations,
one active preparation per session, eight retained jobs, and a 30-minute deadline.
Completed/failed jobs expire after 15 minutes; archives are periodically cleaned
and removed on shutdown. Jobs are process-local: use the default single gateway
worker (or sticky routing); restarts invalidate prepared downloads.

DLP is a separate asynchronous service, not a scraper module. It is disabled by default with `DLP_ENABLED=false`. When enabled, set independent `DLP_GATEWAY_TOKEN` and `DLP_OWNER_SECRET` values and keep `DLP_URL` reachable only on the internal gateway network.

### `GET /health`
Returns the status of the gateway and the VPN.

### `POST /v1/admin/cache/clear`

Owner-operated media cache invalidation for the Telegram Bot:

```json
{"target": "all"}
```

`target` can also be a shortcode or a supported HTTPS post/share URL. The response
is `{"shortcode":null,"deleted_posts":3,"freed_bytes":12345}` (`shortcode` is the
resolved ID for a single post). Missing entries return zero counts. URLs are
resolved without scraping/downloading; redirects are HTTPS-only, same-platform,
host-allowlisted, and limited to three requests with a 20-second total deadline.
Temporary `tt.site` failures may fall back to `vm.tiktok.com` within that budget.
Direct links support TikTok, Instagram, Threads, X, YouTube, Coub, Spotify and
Deezer; for other URLs such as SoundCloud slugs, provide the cache shortcode.

Requires `X-API-Key` **and** a client name listed in
`PINCHANA_CACHE_ADMIN_CLIENTS` (comma-separated; default `bot,telegram-bot`). Set
it empty to disable clearing. Web/mobile sessions cannot authorize this route.
The Bot separately restricts `/cc` to its owner's Telegram ID.

Only direct, non-symlink post directories containing `metadata.json` are removed.
The root, databases, session files and unrelated service directories are retained;
symlinks are never followed. This is invalidation, not a download pause: active
requests may repopulate an entry after it is cleared.

### `POST /admin/vpn/rotate`
Triggers an immediate VPN IP rotation.

### `GET /admin/modules`
Returns a list of all configured modules and their status.

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CONTAINER_MODE` | `false` | Enable Docker container management features. |
| `MODULES_CONFIG` | `/app/config/modules.yaml` | Path to the module routing configuration. |
| `CACHE_PATH` | `./cache` | Base path for serving cached media. |
| `PINCHANA_CACHE_ADMIN_CLIENTS` | `bot,telegram-bot` | Named machine clients allowed to clear completed media posts; empty disables clearing. |
| `PINCHANA_INSTANCE_CERTIFICATE` | unset | Project-issued JSON certificate envelope for this public origin. |
| `PINCHANA_INSTANCE_CERTIFICATE_FILE` | unset | Mounted certificate file used instead of the inline value. |
| `PINCHANA_BUILD_VERSION` | `development` | Validated product CalVer baked into official images and exposed by build metadata. |
| `PINCHANA_BUILD_COMMIT` | unset | Parent API commit baked into official images; used as a manifest fallback. |
| `PINCHANA_BUILD_COMMITS` | unset | JSON map of public API and module commits, normally baked by release CI. |

See [Instance certificates](../docs/INSTANCE_TRUST.md) for issuance and security boundaries.

---

## Development

Managed by `uv`.

```bash
uv sync --frozen
uv run uvicorn pinchana_server.main:app --host 0.0.0.0 --port 8080 --reload
```

---

## License

MIT

Authenticated machine clients can use DLP through `GET /v1/dlp/capabilities`,
`POST /v1/dlp/jobs`, `POST /v1/dlp/jobs/{job_id}/submit`,
`GET /v1/dlp/jobs/{job_id}`, and `GET /v1/dlp/jobs/{job_id}/file`.
All routes require `X-API-Key` and a positive numeric `X-Pinchana-User` header.
Job ownership is isolated by authenticated client and user, independently of
web/mobile sessions. The existing `DLP_ENABLED`, `DLP_URL`, `DLP_GATEWAY_TOKEN`,
and `DLP_OWNER_SECRET` configuration applies. File responses are authenticated,
streamed, and never stored in the public scrape cache.

DLP job submissions accept `cookiesEnc` (the protocol-v2 worker-encrypted cookie
envelope) and `useHostCookies` (default `true`). User cookies take priority over
optional operator cookies configured on the DLP API. `useHostCookies: false`
requests no host fallback. Only full YouTube video URLs may use these endpoints;
Apple Music, Spotify, YouTube Music and Shorts retain their normal scraper paths.
