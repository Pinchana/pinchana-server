FROM python:3.13-slim

ARG PINCHANA_BUILD_COMMIT=unknown
ARG PINCHANA_BUILD_COMMITS={}

WORKDIR /workspace/pinchana-server

# Install Docker CLI so the server can manage sibling containers
RUN apt-get update && apt-get install -y \
    ca-certificates curl gnupg lsb-release \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update && apt-get install -y docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy pinchana-core (local path dependency) first
COPY pinchana-core/pyproject.toml pinchana-core/uv.lock pinchana-core/README.md ../pinchana-core/
RUN mkdir -p ../pinchana-core/src
COPY pinchana-core/src ../pinchana-core/src

# Copy server package files
COPY pinchana-server/pyproject.toml pinchana-server/uv.lock pinchana-server/README.md ./
RUN uv sync --frozen --no-install-project

COPY pinchana-server/src ./src

RUN mkdir -p /app/cache
ENV CACHE_PATH=/app/cache
ENV CACHE_MAX_SIZE_GB=10.0
ENV PINCHANA_BUILD_COMMIT=$PINCHANA_BUILD_COMMIT
ENV PINCHANA_BUILD_COMMITS=$PINCHANA_BUILD_COMMITS

EXPOSE 8080
CMD ["uv", "run", "uvicorn", "pinchana_server.main:app", "--host", "0.0.0.0", "--port", "8080"]
