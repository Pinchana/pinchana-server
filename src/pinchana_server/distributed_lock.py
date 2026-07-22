"""Small Redis owned-lock primitive used by media normalization."""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from typing import Any


@dataclass
class RedisOwnedLock:
    redis: Any
    key: str
    ttl_seconds: int = 60
    token: str = ""

    async def acquire(self) -> bool:
        if not self.token:
            self.token = secrets.token_hex(16)
        return bool(
            await self.redis.set(
                self.key,
                self.token,
                nx=True,
                ex=max(1, int(self.ttl_seconds)),
            )
        )

    async def renew(self) -> bool:
        script = (
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
            "return redis.call('expire', KEYS[1], ARGV[2]) else return 0 end"
        )
        return bool(
            await self.redis.eval(
                script,
                1,
                self.key,
                self.token,
                max(1, int(self.ttl_seconds)),
            )
        )

    async def release(self) -> bool:
        script = (
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
            "return redis.call('del', KEYS[1]) else return 0 end"
        )
        return bool(await self.redis.eval(script, 1, self.key, self.token))
