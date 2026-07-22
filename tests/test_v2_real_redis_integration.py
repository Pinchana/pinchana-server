"""Opt-in integration coverage for the production Redis coordination paths."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.request

import pytest

from pinchana_core.models import RemoteAssetDescriptor
from pinchana_server.distributed_lock import RedisOwnedLock
import pinchana_server.main as server_main
from pinchana_server.tickets import RedisTicketStore
from pinchana_server.v2_runtime import (
    validate_shared_spool_registry,
    validate_spool_topology,
)


pytestmark = pytest.mark.skipif(
    not os.getenv("PINCHANA_TEST_REDIS_URL"),
    reason="PINCHANA_TEST_REDIS_URL is required for real Redis integration",
)


@pytest.mark.asyncio
async def test_ticket_lease_and_job_state_cross_process_contract(monkeypatch, tmp_path):
    redis_url = os.environ["PINCHANA_TEST_REDIS_URL"]
    first = RedisTicketStore(redis_url)
    second = RedisTicketStore(redis_url)
    descriptor = RemoteAssetDescriptor(
        index=0,
        media_type="audio",
        role="content",
        availability="full",
        filename="integration.mp3",
        mime_type="audio/mpeg",
        upstream_url="https://1.1.1.1/integration.mp3?signature=server-only",
        supports_range=True,
        asset_id="integration:audio:full",
        source_fingerprint="integration-fingerprint",
    )
    ticket = await first.create_ticket("session", "instance", descriptor, ttl_seconds=2)
    try:
        recovered = await second.get_ticket(ticket.ticket_id)
        assert recovered is not None
        assert recovered.descriptor.source_fingerprint == "integration-fingerprint"
        leased = await second.acquire_lease(ticket.ticket_id)
        assert leased is not None and leased.active_leases == 1
        assert (await first.get_ticket(ticket.ticket_id)).active_leases == 1
        await first.release_lease(ticket.ticket_id)
        assert (await second.get_ticket(ticket.ticket_id)).active_leases == 0

        expiring = await first.create_ticket("session", "instance", descriptor, ttl_seconds=1)
        time.sleep(1.1)
        assert await second.acquire_lease(expiring.ticket_id) is None
        assert (await second.get_ticket(expiring.ticket_id)).is_expired()
        await first.delete_ticket(expiring.ticket_id)

        monkeypatch.setattr(server_main, "normalization_redis", first.redis)
        job_id = "phase5-real-redis-job"
        await server_main._set_ephemeral_job(job_id, {
            "status": "processing",
            "expires_at": int(time.time()) + 60,
            "session_nonce": "session",
            "instance_id": "instance",
            "platform": "threads",
            "spool_dir": None,
            "spool_files": [],
            "ticket_ids": [],
        })
        monkeypatch.setattr(server_main, "normalization_redis", second.redis)
        cross_instance = await server_main._get_ephemeral_job(job_id)
        assert cross_instance["status"] == "processing"
        await server_main._recover_ephemeral_jobs()
        recovered_job = await server_main._get_ephemeral_job(job_id)
        assert recovered_job["status"] == "failed"
        assert "http" not in json.dumps(recovered_job).lower()
        await server_main._delete_ephemeral_job(job_id)
    finally:
        await first.delete_ticket(ticket.ticket_id)
        await first.redis.aclose()
        await second.redis.aclose()


@pytest.mark.asyncio
async def test_owned_normalization_lock_acquire_renew_and_release():
    redis_url = os.environ["PINCHANA_TEST_REDIS_URL"]
    store = RedisTicketStore(redis_url)
    key = "pinchana:norm_lock:phase5-real-redis:telegram-v1"
    first = RedisOwnedLock(store.redis, key, ttl_seconds=10)
    second = RedisOwnedLock(store.redis, key, ttl_seconds=10)
    try:
        assert await first.acquire() is True
        assert await second.acquire() is False
        assert await first.renew() is True
        assert await second.release() is False
        assert await first.release() is True
        assert await second.acquire() is True
    finally:
        await store.redis.delete(key)
        await store.redis.aclose()


@pytest.mark.asyncio
async def test_shared_and_separate_spool_identity_with_real_redis(monkeypatch, tmp_path):
    redis_url = os.environ["PINCHANA_TEST_REDIS_URL"]
    store = RedisTicketStore(redis_url)
    deployment = f"phase5-real-{os.getpid()}"
    shared = tmp_path / "shared"
    separate = tmp_path / "separate"
    shared.mkdir()
    separate.mkdir()
    monkeypatch.setenv("PINCHANA_API_REPLICAS", "2")
    monkeypatch.setenv("REDIS_URL", redis_url)
    monkeypatch.setenv("V2_SPOOL_SHARED", "true")
    monkeypatch.setenv("PINCHANA_DEPLOYMENT_ID", deployment)
    monkeypatch.setenv("V2_SPOOL_MIN_FREE_BYTES", "1")
    try:
        monkeypatch.setenv("V2_SPOOL_PATH", str(shared))
        first = validate_spool_topology()
        second = validate_spool_topology()
        await validate_shared_spool_registry(store.redis, first)
        await validate_shared_spool_registry(store.redis, second)

        monkeypatch.setenv("V2_SPOOL_PATH", str(separate))
        isolated = validate_spool_topology()
        with pytest.raises(RuntimeError, match="same V2 spool"):
            await validate_shared_spool_registry(store.redis, isolated)
    finally:
        await store.redis.delete(f"pinchana:v2:spool-topology:{deployment}")
        await store.redis.aclose()


def _available_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _wait_for_liveness(port: int, timeout: float = 12.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/livez", timeout=0.5) as response:
                return response.status == 200
        except Exception:
            time.sleep(0.1)
    return False


@pytest.mark.asyncio
async def test_two_gateway_processes_share_spool_and_reject_isolated_replica(tmp_path):
    redis_url = os.environ["PINCHANA_TEST_REDIS_URL"]
    store = RedisTicketStore(redis_url)
    deployment = f"phase5-process-{os.getpid()}"
    shared = tmp_path / "shared"
    isolated = tmp_path / "isolated"
    shared.mkdir()
    isolated.mkdir()
    modules = tmp_path / "modules.yaml"
    modules.write_text("modules: {}\n", encoding="utf-8")
    base_env = {
        **os.environ,
        "REDIS_URL": redis_url,
        "PINCHANA_API_REPLICAS": "2",
        "V2_SPOOL_SHARED": "true",
        "V2_SPOOL_REQUIRE_EXISTING": "true",
        "V2_SPOOL_MIN_FREE_BYTES": "1",
        "PINCHANA_DEPLOYMENT_ID": deployment,
        "MODULES_CONFIG": str(modules),
        "CACHE_PATH": str(tmp_path / "cache"),
        "PINCHANA_ENV": "development",
    }
    processes: list[subprocess.Popen[bytes]] = []
    try:
        for _index in range(2):
            port = _available_port()
            process = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "pinchana_server.main:app", "--host", "127.0.0.1", "--port", str(port)],
                env={**base_env, "V2_SPOOL_PATH": str(shared)},
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            processes.append(process)
            assert _wait_for_liveness(port), "gateway process did not become live"

        isolated_port = _available_port()
        rejected = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "pinchana_server.main:app", "--host", "127.0.0.1", "--port", str(isolated_port)],
            env={**base_env, "V2_SPOOL_PATH": str(isolated)},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        processes.append(rejected)
        assert rejected.wait(timeout=12) != 0
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            if process.poll() is None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
        await store.redis.delete(f"pinchana:v2:spool-topology:{deployment}")
        await store.redis.aclose()
