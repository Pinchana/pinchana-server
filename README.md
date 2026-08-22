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
- `POST /web/convert/gif` converts an authenticated, already-cached media file to GIF with bounded native FFmpeg limits; it never accepts uploads or remote URLs.
- `/web/dlp/jobs...` allocates, submits, monitors, and streams owner-bound private-download jobs to the internal DLP service. The gateway forwards cookie ciphertext only.

DLP is a separate asynchronous service, not a scraper module. It is disabled by default with `DLP_ENABLED=false`. When enabled, set independent `DLP_GATEWAY_TOKEN` and `DLP_OWNER_SECRET` values and keep `DLP_URL` reachable only on the internal gateway network.

### `GET /health`
Returns the status of the gateway and the VPN.

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
