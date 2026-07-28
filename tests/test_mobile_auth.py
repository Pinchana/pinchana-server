import asyncio
import os
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from pinchana_server.main import app
from pinchana_server.mobile_auth import MobileGrantError, MobileGrantStore


@pytest.fixture
def store(tmp_path: Path) -> MobileGrantStore:
    grant_store = MobileGrantStore(tmp_path / "mobile-sessions.sqlite3")
    grant_store.initialize()
    return grant_store


def test_challenge_is_context_bound_and_single_use(store: MobileGrantStore):
    challenge = store.create_challenge(
        installation_id="installation_123456",
        platform="ios",
        app_id="cc.pinchana.mobile",
        remote_hash="remote",
        ttl_seconds=120,
        rate_window_seconds=600,
        rate_limit=10,
        now=1_000,
    )
    installation = store.consume_challenge(
        challenge_id=challenge.challenge_id,
        challenge=challenge.challenge,
        installation_id="installation_123456",
        platform="ios",
        app_id="cc.pinchana.mobile",
        trust="guest",
        now=1_001,
    )
    assert installation.installation_id == "installation_123456"
    assert installation.trust == "guest"

    with pytest.raises(MobileGrantError, match="already used"):
        store.consume_challenge(
            challenge_id=challenge.challenge_id,
            challenge=challenge.challenge,
            installation_id="installation_123456",
            platform="ios",
            app_id="cc.pinchana.mobile",
            trust="guest",
            now=1_002,
        )


def test_challenge_rate_limit_is_enforced(store: MobileGrantStore):
    for _ in range(2):
        store.create_challenge(
            installation_id="installation_123456",
            platform="android",
            app_id="cc.pinchana.mobile",
            remote_hash="same-remote",
            ttl_seconds=120,
            rate_window_seconds=600,
            rate_limit=2,
            now=1_000,
        )
    with pytest.raises(MobileGrantError) as raised:
        store.create_challenge(
            installation_id="installation_123456",
            platform="android",
            app_id="cc.pinchana.mobile",
            remote_hash="same-remote",
            ttl_seconds=120,
            rate_window_seconds=600,
            rate_limit=2,
            now=1_001,
        )
    assert raised.value.status_code == 429


def test_refresh_rotation_and_reuse_revoke_the_family(store: MobileGrantStore):
    challenge = store.create_challenge(
        installation_id="installation_123456",
        platform="ios",
        app_id="cc.pinchana.mobile",
        remote_hash="remote",
        ttl_seconds=120,
        rate_window_seconds=600,
        rate_limit=10,
        now=1_000,
    )
    installation = store.consume_challenge(
        challenge_id=challenge.challenge_id,
        challenge=challenge.challenge,
        installation_id="installation_123456",
        platform="ios",
        app_id="cc.pinchana.mobile",
        trust="attested",
        now=1_001,
    )
    first = store.issue_refresh(installation, ttl_seconds=3_600, now=1_001)
    second = store.rotate_refresh(first.refresh_token, ttl_seconds=3_600, now=1_002)

    with pytest.raises(MobileGrantError, match="reuse detected"):
        store.rotate_refresh(first.refresh_token, ttl_seconds=3_600, now=1_003)
    with pytest.raises(MobileGrantError, match="reuse detected"):
        store.rotate_refresh(second.refresh_token, ttl_seconds=3_600, now=1_004)


def test_installation_revocation_invalidates_refresh_tokens(store: MobileGrantStore):
    challenge = store.create_challenge(
        installation_id="installation_123456",
        platform="android",
        app_id="cc.pinchana.mobile",
        remote_hash="remote",
        ttl_seconds=120,
        rate_window_seconds=600,
        rate_limit=10,
        now=1_000,
    )
    installation = store.consume_challenge(
        challenge_id=challenge.challenge_id,
        challenge=challenge.challenge,
        installation_id="installation_123456",
        platform="android",
        app_id="cc.pinchana.mobile",
        trust="attested",
        now=1_001,
    )
    refresh = store.issue_refresh(installation, ttl_seconds=3_600, now=1_001)
    assert store.revoke_installation(installation.installation_id, now=1_002)
    with pytest.raises(MobileGrantError):
        store.rotate_refresh(refresh.refresh_token, ttl_seconds=3_600, now=1_003)


def test_guest_installation_grant_refresh_and_session_contract(tmp_path: Path):
    environment = {
        "MOBILE_AUTH_MODE": "guest",
        "MOBILE_SESSION_SECRET": "mobile-session-secret-0123456789abcdef",
        "MOBILE_SESSION_DB_PATH": str(tmp_path / "integration.sqlite3"),
        "MOBILE_GUEST_SCOPES": "mobile:scrape,mobile:media,mobile:capabilities",
    }
    identity = {
        "installation_id": "installation_123456",
        "platform": "ios",
        "app_id": "cc.pinchana.mobile",
    }

    async def exercise_contract():
        with patch.dict(os.environ, environment, clear=False):
            transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                challenge_response = await client.post(
                    "/v1/mobile/challenges",
                    json=identity,
                )
                assert challenge_response.status_code == 200
                challenge = challenge_response.json()
                assert challenge["providers"] == ["guest"]

                grant_response = await client.post(
                    "/v1/mobile/attest",
                    json={
                        **identity,
                        "challenge_id": challenge["challenge_id"],
                        "challenge": challenge["challenge"],
                        "provider": "guest",
                        "evidence": None,
                    },
                )
                assert grant_response.status_code == 200
                grant = grant_response.json()
                assert grant["trust"] == "guest"

                session_response = await client.get(
                    "/v1/mobile/session",
                    headers={"Authorization": f"Bearer {grant['access_token']}"},
                )
                assert session_response.status_code == 200
                assert session_response.json()["installation_id"] == identity["installation_id"]

                refresh_response = await client.post(
                    "/v1/mobile/session/refresh",
                    json={"refresh_token": grant["refresh_token"]},
                )
                assert refresh_response.status_code == 200
                refreshed = refresh_response.json()
                assert refreshed["refresh_token"] != grant["refresh_token"]

                replay_response = await client.post(
                    "/v1/mobile/session/refresh",
                    json={"refresh_token": grant["refresh_token"]},
                )
                assert replay_response.status_code == 401
                revoked_family_response = await client.post(
                    "/v1/mobile/session/refresh",
                    json={"refresh_token": refreshed["refresh_token"]},
                )
                assert revoked_family_response.status_code == 401

    asyncio.run(exercise_contract())
