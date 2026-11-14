# syntax=docker/dockerfile:1.20
FROM python:3.13-slim

ARG PINCHANA_BUILD_VERSION=development
ARG PINCHANA_BUILD_COMMIT=unknown
ARG PINCHANA_BUILD_COMMITS={}

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates ffmpeg gifsicle libcurl4t64 \
    libnss3 libatk-bridge2.0-0 libxss1 libgtk-3-0t64 \
    libasound2t64 libxtst6 libgbm1 libxshmfence1 \
    libxcomposite1 libxdamage1 libxrandr2 libpangocairo-1.0-0 fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /workspace/pinchana-server
COPY --parents pinchana-*/pyproject.toml pinchana-*/README.md /workspace/
COPY pinchana-server/uv.lock ./
COPY --parents --exclude=**/__pycache__ pinchana-*/src /workspace/

RUN uv sync --locked --no-dev --no-editable

ENV CACHE_PATH=/app/cache
ENV CACHE_MAX_SIZE_GB=10.0
ENV YTDLP_COOKIES_DIR=/run/pinchana-cookies
ENV CLOAKBROWSER_AUTO_UPDATE=false
ENV PINCHANA_BUILD_VERSION=$PINCHANA_BUILD_VERSION
ENV PINCHANA_BUILD_COMMIT=$PINCHANA_BUILD_COMMIT
ENV PINCHANA_BUILD_COMMITS=$PINCHANA_BUILD_COMMITS

RUN mkdir -p /app/cache /run/pinchana-cookies

EXPOSE 8080
CMD ["/workspace/pinchana-server/.venv/bin/uvicorn", "pinchana_server.main:app", "--host", "0.0.0.0", "--port", "8080"]
