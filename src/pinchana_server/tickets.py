"""Server-side ticket store abstraction and in-memory implementation."""

import asyncio
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import secrets
import time
from typing import Optional, Dict
from pinchana_core.models import RemoteAssetDescriptor


@dataclass
class TicketData:
    ticket_id: str
    session_nonce: str
    instance_id: str
    descriptor: RemoteAssetDescriptor
    spool_path: Optional[str] = None
    expires_at: int = 0
    active_leases: int = 0
    created_at: float = field(default_factory=time.time)

    def is_expired(self) -> bool:
        return int(time.time()) >= self.expires_at


class TicketStore(ABC):
    @abstractmethod
    async def create_ticket(
        self,
        session_nonce: str,
        instance_id: str,
        descriptor: RemoteAssetDescriptor,
        spool_path: Optional[str] = None,
        ttl_seconds: int = 7200,
    ) -> TicketData:
        pass

    @abstractmethod
    async def get_ticket(self, ticket_id: str) -> Optional[TicketData]:
        pass

    @abstractmethod
    async def acquire_lease(self, ticket_id: str) -> Optional[TicketData]:
        pass

    @abstractmethod
    async def release_lease(self, ticket_id: str) -> None:
        pass

    @abstractmethod
    async def delete_ticket(self, ticket_id: str) -> None:
        pass


class InMemoryTicketStore(TicketStore):
    def __init__(self, check_workers: bool = True):
        if check_workers:
            workers = int(os.getenv("WEB_WORKERS", os.getenv("WORKERS", "1")))
            if workers > 1:
                raise RuntimeError(
                    "InMemoryTicketStore cannot be used when multiple server workers are configured (WORKERS > 1). "
                    "Configure REDIS_URL to enable RedisTicketStore for multi-worker deployment."
                )
        self._tickets: Dict[str, TicketData] = {}
        self._lock = asyncio.Lock()

    async def create_ticket(
        self,
        session_nonce: str,
        instance_id: str,
        descriptor: RemoteAssetDescriptor,
        spool_path: Optional[str] = None,
        ttl_seconds: int = 7200,
    ) -> TicketData:
        ticket_id = secrets.token_urlsafe(24)
        effective_ttl = max(1, int(ttl_seconds))
        expires_at = int(time.time()) + effective_ttl
        ticket = TicketData(
            ticket_id=ticket_id,
            session_nonce=session_nonce,
            instance_id=instance_id,
            descriptor=descriptor,
            spool_path=spool_path,
            expires_at=expires_at,
            active_leases=0,
        )
        async with self._lock:
            self._tickets[ticket_id] = ticket
            self._clean_expired_unlocked()
        return ticket

    async def get_ticket(self, ticket_id: str) -> Optional[TicketData]:
        async with self._lock:
            ticket = self._tickets.get(ticket_id)
            if not ticket:
                return None
            return ticket

    async def acquire_lease(self, ticket_id: str) -> Optional[TicketData]:
        async with self._lock:
            ticket = self._tickets.get(ticket_id)
            if not ticket:
                return None
            if ticket.is_expired() and ticket.active_leases == 0:
                return None
            ticket.active_leases += 1
            return ticket

    async def release_lease(self, ticket_id: str) -> None:
        async with self._lock:
            ticket = self._tickets.get(ticket_id)
            if ticket:
                ticket.active_leases = max(0, ticket.active_leases - 1)
                if ticket.is_expired() and ticket.active_leases == 0:
                    self._tickets.pop(ticket_id, None)

    async def delete_ticket(self, ticket_id: str) -> None:
        async with self._lock:
            self._tickets.pop(ticket_id, None)

    def _clean_expired_unlocked(self):
        now = int(time.time())
        expired_ids = [
            tid
            for tid, t in self._tickets.items()
            if t.expires_at + 300 <= now and t.active_leases == 0
        ]
        for tid in expired_ids:
            self._tickets.pop(tid, None)


class RedisTicketStore(TicketStore):
    """Production Redis-backed ticket store for multi-replica deployment."""

    def __init__(self, redis_url: str):
        import redis.asyncio as aioredis
        self.redis = aioredis.from_url(redis_url, decode_responses=True)

    async def create_ticket(
        self,
        session_nonce: str,
        instance_id: str,
        descriptor: RemoteAssetDescriptor,
        spool_path: Optional[str] = None,
        ttl_seconds: int = 7200,
    ) -> TicketData:
        ticket_id = secrets.token_urlsafe(24)
        effective_ttl = max(1, int(ttl_seconds))
        expires_at = int(time.time()) + effective_ttl
        ticket = TicketData(
            ticket_id=ticket_id,
            session_nonce=session_nonce,
            instance_id=instance_id,
            descriptor=descriptor,
            spool_path=spool_path,
            expires_at=expires_at,
            active_leases=0,
        )
        import json
        key = f"pinchana:ticket:{ticket_id}"
        payload = {
            "ticket_id": ticket_id,
            "session_nonce": session_nonce,
            "instance_id": instance_id,
            "descriptor": descriptor.model_dump(),
            "spool_path": spool_path,
            "expires_at": expires_at,
            "active_leases": 0,
        }
        await self.redis.set(key, json.dumps(payload), ex=effective_ttl + 300)
        return ticket

    async def get_ticket(self, ticket_id: str) -> Optional[TicketData]:
        import json
        key = f"pinchana:ticket:{ticket_id}"
        raw = await self.redis.get(key)
        if not raw:
            return None
        payload = json.loads(raw)
        desc = RemoteAssetDescriptor(**payload["descriptor"])
        ticket = TicketData(
            ticket_id=payload["ticket_id"],
            session_nonce=payload["session_nonce"],
            instance_id=payload["instance_id"],
            descriptor=desc,
            spool_path=payload.get("spool_path"),
            expires_at=payload["expires_at"],
            active_leases=int(payload.get("active_leases", 0)),
        )
        return ticket

    async def acquire_lease(self, ticket_id: str) -> Optional[TicketData]:
        import json
        key = f"pinchana:ticket:{ticket_id}"
        script = """
        local raw = redis.call('get', KEYS[1])
        if not raw then return nil end
        local payload = cjson.decode(raw)
        if tonumber(payload.expires_at) <= tonumber(ARGV[1]) and (payload.active_leases or 0) == 0 then
            return nil
        end
        payload.active_leases = (payload.active_leases or 0) + 1
        local ttl = redis.call('ttl', KEYS[1])
        if ttl < 300 then ttl = 300 end
        local encoded = cjson.encode(payload)
        redis.call('set', KEYS[1], encoded, 'EX', ttl)
        return encoded
        """
        raw = await self.redis.eval(script, 1, key, int(time.time()))
        if not raw:
            return None
        payload = json.loads(raw)
        return TicketData(
            ticket_id=payload["ticket_id"],
            session_nonce=payload["session_nonce"],
            instance_id=payload["instance_id"],
            descriptor=RemoteAssetDescriptor(**payload["descriptor"]),
            spool_path=payload.get("spool_path"),
            expires_at=payload["expires_at"],
            active_leases=int(payload["active_leases"]),
        )

    async def release_lease(self, ticket_id: str) -> None:
        key = f"pinchana:ticket:{ticket_id}"
        script = """
        local raw = redis.call('get', KEYS[1])
        if not raw then return 0 end
        local payload = cjson.decode(raw)
        payload.active_leases = math.max(0, (payload.active_leases or 0) - 1)
        local now = tonumber(ARGV[1])
        if payload.active_leases == 0 and tonumber(payload.expires_at) <= now then
            redis.call('del', KEYS[1])
            return 1
        end
        local ttl = redis.call('ttl', KEYS[1])
        if ttl < 60 then ttl = 60 end
        redis.call('set', KEYS[1], cjson.encode(payload), 'EX', ttl)
        return 1
        """
        await self.redis.eval(script, 1, key, int(time.time()))

    async def delete_ticket(self, ticket_id: str) -> None:
        key = f"pinchana:ticket:{ticket_id}"
        await self.redis.delete(key)
