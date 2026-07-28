"""Stateful mobile installation grants and rotating refresh sessions."""

from __future__ import annotations

import hashlib
import os
import secrets
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path


class MobileGrantError(Exception):
    def __init__(self, message: str, *, status_code: int = 401) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class MobileInstallation:
    installation_id: str
    platform: str
    app_id: str
    trust: str


@dataclass(frozen=True)
class MobileChallenge:
    challenge_id: str
    challenge: str
    expires_at: int


@dataclass(frozen=True)
class MobileRefreshGrant:
    refresh_token: str
    refresh_expires_at: int
    installation: MobileInstallation


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class MobileGrantStore:
    """Small durable grant store.

    SQLite is intentional here: the gateway already has a persistent cache
    volume, and session rollout must not depend on the separate Redis rollout.
    Transactions use ``BEGIN IMMEDIATE`` so refresh rotation remains atomic.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS mobile_challenges (
                    challenge_id TEXT PRIMARY KEY,
                    challenge_hash TEXT NOT NULL,
                    installation_id TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    app_id TEXT NOT NULL,
                    remote_hash TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    consumed_at INTEGER
                );
                CREATE INDEX IF NOT EXISTS mobile_challenges_remote_created
                    ON mobile_challenges(remote_hash, created_at);

                CREATE TABLE IF NOT EXISTS mobile_installations (
                    installation_id TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    app_id TEXT NOT NULL,
                    trust TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    last_seen_at INTEGER NOT NULL,
                    revoked_at INTEGER
                );

                CREATE TABLE IF NOT EXISTS mobile_refresh_tokens (
                    token_hash TEXT PRIMARY KEY,
                    family_id TEXT NOT NULL,
                    installation_id TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    used_at INTEGER,
                    revoked_at INTEGER,
                    FOREIGN KEY(installation_id)
                        REFERENCES mobile_installations(installation_id)
                );
                CREATE INDEX IF NOT EXISTS mobile_refresh_family
                    ON mobile_refresh_tokens(family_id);
                """
            )
        os.chmod(self.path, 0o600)

    def create_challenge(
        self,
        *,
        installation_id: str,
        platform: str,
        app_id: str,
        remote_hash: str,
        ttl_seconds: int,
        rate_window_seconds: int,
        rate_limit: int,
        now: int | None = None,
    ) -> MobileChallenge:
        current = int(time.time()) if now is None else now
        challenge_id = secrets.token_urlsafe(18)
        challenge = secrets.token_urlsafe(32)
        expires_at = current + ttl_seconds

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "DELETE FROM mobile_challenges WHERE expires_at < ?",
                (current - rate_window_seconds,),
            )
            recent = connection.execute(
                """
                SELECT COUNT(*) FROM mobile_challenges
                WHERE remote_hash = ? AND created_at >= ?
                """,
                (remote_hash, current - rate_window_seconds),
            ).fetchone()[0]
            if recent >= rate_limit:
                raise MobileGrantError("Too many installation challenges", status_code=429)
            connection.execute(
                """
                INSERT INTO mobile_challenges (
                    challenge_id, challenge_hash, installation_id, platform,
                    app_id, remote_hash, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    challenge_id,
                    _token_hash(challenge),
                    installation_id,
                    platform,
                    app_id,
                    remote_hash,
                    current,
                    expires_at,
                ),
            )

        return MobileChallenge(challenge_id, challenge, expires_at)

    def consume_challenge(
        self,
        *,
        challenge_id: str,
        challenge: str,
        installation_id: str,
        platform: str,
        app_id: str,
        trust: str,
        now: int | None = None,
    ) -> MobileInstallation:
        current = int(time.time()) if now is None else now
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT challenge_hash, installation_id, platform, app_id,
                       expires_at, consumed_at
                FROM mobile_challenges WHERE challenge_id = ?
                """,
                (challenge_id,),
            ).fetchone()
            if row is None:
                raise MobileGrantError("Invalid installation challenge")
            if row["consumed_at"] is not None:
                raise MobileGrantError("Installation challenge was already used")
            if row["expires_at"] <= current:
                raise MobileGrantError("Installation challenge expired")
            if (
                not secrets.compare_digest(row["challenge_hash"], _token_hash(challenge))
                or row["installation_id"] != installation_id
                or row["platform"] != platform
                or row["app_id"] != app_id
            ):
                raise MobileGrantError("Installation challenge does not match this client")

            existing = connection.execute(
                """
                SELECT revoked_at FROM mobile_installations
                WHERE installation_id = ?
                """,
                (installation_id,),
            ).fetchone()
            if existing is not None and existing["revoked_at"] is not None:
                raise MobileGrantError("This installation has been revoked", status_code=403)

            connection.execute(
                "UPDATE mobile_challenges SET consumed_at = ? WHERE challenge_id = ?",
                (current, challenge_id),
            )
            connection.execute(
                """
                INSERT INTO mobile_installations (
                    installation_id, platform, app_id, trust,
                    created_at, last_seen_at, revoked_at
                ) VALUES (?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(installation_id) DO UPDATE SET
                    platform = excluded.platform,
                    app_id = excluded.app_id,
                    trust = excluded.trust,
                    last_seen_at = excluded.last_seen_at
                """,
                (installation_id, platform, app_id, trust, current, current),
            )

        return MobileInstallation(installation_id, platform, app_id, trust)

    def validate_challenge(
        self,
        *,
        challenge_id: str,
        challenge: str,
        installation_id: str,
        platform: str,
        app_id: str,
        now: int | None = None,
    ) -> None:
        current = int(time.time()) if now is None else now
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT challenge_hash, installation_id, platform, app_id,
                       expires_at, consumed_at
                FROM mobile_challenges WHERE challenge_id = ?
                """,
                (challenge_id,),
            ).fetchone()
        if row is None:
            raise MobileGrantError("Invalid installation challenge")
        if row["consumed_at"] is not None:
            raise MobileGrantError("Installation challenge was already used")
        if row["expires_at"] <= current:
            raise MobileGrantError("Installation challenge expired")
        if (
            not secrets.compare_digest(row["challenge_hash"], _token_hash(challenge))
            or row["installation_id"] != installation_id
            or row["platform"] != platform
            or row["app_id"] != app_id
        ):
            raise MobileGrantError("Installation challenge does not match this client")

    def issue_refresh(
        self,
        installation: MobileInstallation,
        *,
        ttl_seconds: int,
        family_id: str | None = None,
        now: int | None = None,
    ) -> MobileRefreshGrant:
        current = int(time.time()) if now is None else now
        refresh_token = secrets.token_urlsafe(48)
        expires_at = current + ttl_seconds
        family = family_id or secrets.token_urlsafe(18)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO mobile_refresh_tokens (
                    token_hash, family_id, installation_id,
                    created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    _token_hash(refresh_token),
                    family,
                    installation.installation_id,
                    current,
                    expires_at,
                ),
            )
        return MobileRefreshGrant(refresh_token, expires_at, installation)

    def rotate_refresh(
        self,
        refresh_token: str,
        *,
        ttl_seconds: int,
        now: int | None = None,
    ) -> MobileRefreshGrant:
        current = int(time.time()) if now is None else now
        token_digest = _token_hash(refresh_token)
        replacement = secrets.token_urlsafe(48)
        replacement_hash = _token_hash(replacement)
        replacement_expires_at = current + ttl_seconds

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT r.family_id, r.installation_id, r.expires_at,
                       r.used_at, r.revoked_at, i.platform, i.app_id,
                       i.trust, i.revoked_at AS installation_revoked_at
                FROM mobile_refresh_tokens r
                JOIN mobile_installations i
                  ON i.installation_id = r.installation_id
                WHERE r.token_hash = ?
                """,
                (token_digest,),
            ).fetchone()
            if row is None:
                raise MobileGrantError("Invalid refresh token")
            if row["used_at"] is not None or row["revoked_at"] is not None:
                connection.execute(
                    """
                    UPDATE mobile_refresh_tokens SET revoked_at = ?
                    WHERE family_id = ? AND revoked_at IS NULL
                    """,
                    (current, row["family_id"]),
                )
                connection.commit()
                raise MobileGrantError("Refresh token reuse detected")
            if row["expires_at"] <= current:
                raise MobileGrantError("Refresh token expired")
            if row["installation_revoked_at"] is not None:
                raise MobileGrantError("This installation has been revoked", status_code=403)

            connection.execute(
                "UPDATE mobile_refresh_tokens SET used_at = ? WHERE token_hash = ?",
                (current, token_digest),
            )
            connection.execute(
                """
                INSERT INTO mobile_refresh_tokens (
                    token_hash, family_id, installation_id,
                    created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    replacement_hash,
                    row["family_id"],
                    row["installation_id"],
                    current,
                    replacement_expires_at,
                ),
            )
            connection.execute(
                """
                UPDATE mobile_installations SET last_seen_at = ?
                WHERE installation_id = ?
                """,
                (current, row["installation_id"]),
            )

        installation = MobileInstallation(
            row["installation_id"],
            row["platform"],
            row["app_id"],
            row["trust"],
        )
        return MobileRefreshGrant(replacement, replacement_expires_at, installation)

    def revoke_refresh_family(self, refresh_token: str, *, now: int | None = None) -> None:
        current = int(time.time()) if now is None else now
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT family_id FROM mobile_refresh_tokens WHERE token_hash = ?",
                (_token_hash(refresh_token),),
            ).fetchone()
            if row is None:
                return
            connection.execute(
                """
                UPDATE mobile_refresh_tokens SET revoked_at = ?
                WHERE family_id = ? AND revoked_at IS NULL
                """,
                (current, row["family_id"]),
            )

    def revoke_installation(
        self,
        installation_id: str,
        *,
        now: int | None = None,
    ) -> bool:
        current = int(time.time()) if now is None else now
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE mobile_installations SET revoked_at = ?
                WHERE installation_id = ? AND revoked_at IS NULL
                """,
                (current, installation_id),
            ).rowcount
            if updated:
                connection.execute(
                    """
                    UPDATE mobile_refresh_tokens SET revoked_at = ?
                    WHERE installation_id = ? AND revoked_at IS NULL
                    """,
                    (current, installation_id),
                )
        return bool(updated)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection
