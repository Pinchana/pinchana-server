# 🚀 Pinchana Server

> [!WARNING]
> The Live Photo implementation on this branch currently does not work at all. It is exploration and experimentation for TikTok Live Photo scraping behavior, not production-ready functionality.

**Pinchana Server** is the central gateway for the Pinchana scraping ecosystem. It acts as a unified HTTP entry point, routing incoming requests to specialized scraper modules based on URL patterns.

---

## ✨ Key Features

- **🌐 Unified Entry Point:** Exposes a single `/scrape` endpoint for all supported platforms (TikTok, Instagram, etc.).
- **🚦 Smart Routing:** Automatically directs requests to the correct module by matching URL patterns defined in `modules.yaml`.
- **🛠 Module Management:** 
    - **Container Mode:** Can dynamically manage (start/stop) scraper containers via the Docker API.
    - **Proxying:** transparently forwards requests to standalone containerized modules over HTTP.
- **🛡 VPN Integration:** Provides admin endpoints to monitor and rotate the global VPN IP (Gluetun).
- **💾 Media Serving:** Directly serves cached media files stored by the scrapers.

---

## 🏗 How it Works

1. **Request Received:** A client sends a `POST /scrape` request with a URL.
2. **Resolution:** The server checks `modules.yaml` to find a module that matches the URL pattern.
3. **Execution:** 
    - If the module is "in-process", it calls the plugin directly.
    - If the module is a "container", it proxies the request to the module's HTTP endpoint.
4. **Response:** The server returns the standardized metadata to the client.

---

## 📡 API Reference

### `POST /scrape`
Routes the URL to the appropriate scraper.
```json
{
  "url": "https://www.tiktok.com/..."
}
```

### `GET /health`
Returns the status of the gateway and the VPN.

### `POST /admin/vpn/rotate`
Triggers an immediate VPN IP rotation.

### `GET /admin/modules`
Returns a list of all configured modules and their status.

---

## ⚙️ Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CONTAINER_MODE` | `false` | Enable Docker container management features. |
| `MODULES_CONFIG` | `/app/config/modules.yaml` | Path to the module routing configuration. |
| `CACHE_PATH` | `./cache` | Base path for serving cached media. |

---

## 🛠 Development

Managed by `uv`.

```bash
uv sync
uv run uvicorn src.pinchana_server.main:app --host 0.0.0.0 --port 8080
```

---

## 📜 License

MIT
