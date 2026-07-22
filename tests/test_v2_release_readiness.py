import json
import asyncio
import os
import time

import fakeredis
import pytest
from pinchana_core.models import RemoteAssetDescriptor

import pinchana_server.main as server_main
from pinchana_server.tickets import InMemoryTicketStore
from pinchana_server.v2_runtime import normalized_filename, validate_internal_token, validate_spool_topology
from pinchana_server.v2_observability import V2Observability


def test_spool_topology_guard_distinguishes_workers_and_replicas(monkeypatch, tmp_path):
    monkeypatch.setenv("PINCHANA_API_REPLICAS", "1")
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("V2_SPOOL_SHARED", raising=False)
    validate_spool_topology()

    monkeypatch.setenv("PINCHANA_API_REPLICAS", "2")
    with pytest.raises(RuntimeError, match="REDIS_URL"):
        validate_spool_topology()

    monkeypatch.setenv("REDIS_URL", "redis://redis:6379/0")
    with pytest.raises(RuntimeError, match="V2_SPOOL_SHARED"):
        validate_spool_topology()

    monkeypatch.setenv("V2_SPOOL_SHARED", "true")
    monkeypatch.setenv("V2_SPOOL_PATH", str(tmp_path))
    validate_spool_topology()


def test_production_tiktok_requires_strong_internal_token(monkeypatch):
    monkeypatch.setenv("PINCHANA_ENV", "production")
    monkeypatch.setenv("PINCHANA_V2_TIKTOK", "true")
    monkeypatch.setenv("PINCHANA_INTERNAL_TOKEN", "weak")
    with pytest.raises(RuntimeError, match="32 characters"):
        validate_internal_token()

    monkeypatch.setenv("PINCHANA_INTERNAL_TOKEN", "x" * 32)
    validate_internal_token()


@pytest.mark.parametrize(
    ("filename", "mime", "expected"),
    [
        ("photo.jpg", "image/webp", "photo.webp"),
        ("photo.jpg", None, "photo.jpg"),
        ("clip.mp4", "video/mp4; charset=binary", "clip.mp4"),
        ('bad\r\n"name.jpg', "image/webp", "bad___name.webp"),
    ],
)
def test_filename_is_coherent_with_delivered_mime(filename, mime, expected):
    assert normalized_filename(filename, mime) == expected


def test_rollout_metrics_are_low_cardinality_and_identifier_free():
    metrics = V2Observability()
    metrics.increment("resolve_attempt", platform="twitter")
    metrics.increment("resolve_attempt", platform="https://x.com/private/status/123")
    metrics.observe("resolve_latency", 0.25, platform="threads")
    payload = metrics.snapshot()
    encoded = json.dumps(payload)
    assert payload["counters"] == {
        "resolve_attempt:twitter": 1,
        "resolve_attempt:unknown": 1,
    }
    assert payload["average_seconds"]["resolve_latency:threads"] == 0.25
    assert "private" not in encoded
    assert "123" not in encoded


def test_startup_prunes_stale_partial_and_unprotected_directories(monkeypatch, tmp_path):
    monkeypatch.setenv("V2_SPOOL_PATH", str(tmp_path))
    active = tmp_path / "active"
    active.mkdir()
    active_file = active / "asset.mp4"
    active_file.write_bytes(b"media")

    partial_dir = tmp_path / "partial"
    partial_dir.mkdir()
    partial = partial_dir / "asset.mp4.part"
    partial.write_bytes(b"partial")
    old = time.time() - 11 * 60
    os.utime(partial, (old, old))

    stale = tmp_path / "stale"
    stale.mkdir()
    very_old = time.time() - 4 * 60 * 60
    os.utime(stale, (very_old, very_old))

    server_main._prune_stale_spool_directories({active.resolve()})

    assert active_file.exists()
    assert not partial.exists()
    assert not stale.exists()


@pytest.mark.asyncio
async def test_restart_marks_processing_job_failed_without_leaking_details(monkeypatch, tmp_path):
    redis = fakeredis.FakeAsyncRedis(decode_responses=True)
    monkeypatch.setattr(server_main, "normalization_redis", redis)
    monkeypatch.setenv("V2_SPOOL_PATH", str(tmp_path))
    job = {
        "status": "processing",
        "expires_at": int(time.time()) + 1800,
        "session_nonce": "nonce",
        "instance_id": "instance",
        "platform": "threads",
        "spool_dir": None,
        "spool_files": [],
        "ticket_ids": [],
    }
    await server_main._set_ephemeral_job("interrupted", job)

    await server_main._recover_ephemeral_jobs()
    recovered = await server_main._get_ephemeral_job("interrupted")

    assert recovered["status"] == "failed"
    assert recovered["error"] == "Processing was interrupted; retry the request"
    assert "http" not in json.dumps(recovered)
    await redis.aclose()


@pytest.mark.asyncio
async def test_ready_job_with_missing_spool_file_is_failed(monkeypatch, tmp_path):
    redis = fakeredis.FakeAsyncRedis(decode_responses=True)
    monkeypatch.setattr(server_main, "normalization_redis", redis)
    monkeypatch.setenv("V2_SPOOL_PATH", str(tmp_path))
    spool_dir = tmp_path / "missing"
    spool_dir.mkdir()
    job = {
        "status": "ready",
        "result": {"status": "ready"},
        "expires_at": int(time.time()) + 1800,
        "session_nonce": "nonce",
        "instance_id": "instance",
        "platform": "twitter",
        "spool_dir": str(spool_dir),
        "spool_files": [str(spool_dir / "gone.mp4")],
        "ticket_ids": [],
    }
    await server_main._set_ephemeral_job("missing", job)

    await server_main._recover_ephemeral_jobs()
    recovered = await server_main._get_ephemeral_job("missing")

    assert recovered["status"] == "failed"
    assert recovered["error"] == "Processed media is unavailable; retry the request"
    await redis.aclose()


@pytest.mark.asyncio
async def test_cleanup_stays_inside_root_and_preserves_active_lease(monkeypatch, tmp_path):
    monkeypatch.setattr(server_main, "normalization_redis", None)
    monkeypatch.setenv("V2_SPOOL_PATH", str(tmp_path / "spool"))
    spool_dir = tmp_path / "spool" / "job"
    spool_dir.mkdir(parents=True)
    media = spool_dir / "asset.mp4"
    media.write_bytes(b"media")
    outside = tmp_path / "outside"
    outside.mkdir()

    store = InMemoryTicketStore(check_workers=False)
    monkeypatch.setattr(server_main, "ticket_store", store)
    descriptor = RemoteAssetDescriptor(
        index=0,
        media_type="video",
        role="content",
        filename="asset.mp4",
        upstream_url="https://1.1.1.1/asset.mp4",
    )
    ticket = await store.create_ticket("nonce", "instance", descriptor, str(media), 300)
    await store.acquire_lease(ticket.ticket_id)

    active_job = {"spool_dir": str(spool_dir), "ticket_ids": [ticket.ticket_id]}
    assert await server_main._cleanup_job_spool(active_job) is False
    assert media.exists()

    await store.release_lease(ticket.ticket_id)
    assert await server_main._cleanup_job_spool(active_job) is True
    assert not spool_dir.exists()

    traversal_job = {"spool_dir": str(outside), "ticket_ids": []}
    assert await server_main._cleanup_job_spool(traversal_job) is True
    assert outside.exists()


@pytest.mark.asyncio
async def test_graceful_shutdown_marks_processing_job_failed(monkeypatch):
    monkeypatch.setattr(server_main, "normalization_redis", None)
    server_main.ephemeral_jobs.clear()
    server_main.spool_tasks.clear()
    await server_main._set_ephemeral_job("shutdown-job", {
        "status": "processing",
        "expires_at": int(time.time()) + 1800,
        "session_nonce": "nonce",
        "instance_id": "instance",
        "platform": "tiktok",
        "spool_dir": None,
        "spool_files": [],
        "ticket_ids": [],
    })
    task = asyncio.create_task(asyncio.sleep(60))
    server_main._track_spool_task("shutdown-job", task)

    await server_main._shutdown_spool_tasks()
    recovered = await server_main._get_ephemeral_job("shutdown-job")

    assert recovered["status"] == "failed"
    assert recovered["error"] == "Processing stopped during shutdown; retry the request"
