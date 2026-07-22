"""SSRF protection module for validating upstream URLs and redirect hops."""

import ipaddress
import socket
import urllib.parse
from typing import List, Tuple
from fastapi import HTTPException
import httpcore
import httpx
from httpcore._backends.auto import AutoBackend
from httpcore._backends.base import AsyncNetworkBackend, AsyncNetworkStream, SOCKET_OPTION
import typing


PRIVATE_IPV4_NETWORKS = [
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.0.0.0/24"),
    ipaddress.ip_network("192.0.2.0/24"),
    ipaddress.ip_network("192.88.99.0/24"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("198.18.0.0/15"),
    ipaddress.ip_network("198.51.100.0/24"),
    ipaddress.ip_network("203.0.113.0/24"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("240.0.0.0/4"),
    ipaddress.ip_network("255.255.255.255/32"),
]

PRIVATE_IPV6_NETWORKS = [
    ipaddress.ip_network("::/128"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("100::/64"),
    ipaddress.ip_network("2001:db8::/32"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("fec0::/10"),
]


def is_ip_forbidden(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Check if an IP address falls into a forbidden/private network range."""
    if isinstance(ip, ipaddress.IPv6Address):
        # Handle IPv4-mapped IPv6 (e.g. ::ffff:127.0.0.1 or ::ffff:10.0.0.1)
        if ip.ipv4_mapped:
            return is_ip_forbidden(ip.ipv4_mapped)
        for net in PRIVATE_IPV6_NETWORKS:
            if ip in net:
                return True
        return False
    elif isinstance(ip, ipaddress.IPv4Address):
        for net in PRIVATE_IPV4_NETWORKS:
            if ip in net:
                return True
        return False
    return True


def validate_upstream_url(url: str) -> Tuple[str, str]:
    """
    Validate an upstream URL for SSRF protection and return (url, validated_ip).

    DNS Rebinding Protection Mechanism:
    - Resolves all A/AAAA records for the hostname.
    - Rejects IPv4 loopback, RFC 1918 private, link-local (169.254.x.x / cloud metadata),
      multicast/broadcast, IPv6 unique-local/link-local/loopback, and IPv4-mapped IPv6 ranges.
    - Rejects embedded URL authority credentials (user:pass@host).
    - Returns the validated target IP string to enable host-pinning on upstream connection,
      preventing Time-of-Check to Time-of-Use (TOCTOU) DNS rebinding attacks.
    """
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid upstream URL format") from exc

    if parsed.scheme.lower() not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="Only HTTP and HTTPS protocols are supported")

    if parsed.username or parsed.password:
        raise HTTPException(
            status_code=400, detail="Embedded credentials in URL are disallowed for security"
        )

    hostname = parsed.hostname
    if not hostname:
        raise HTTPException(status_code=400, detail="Missing hostname in upstream URL")

    # Check if hostname is an IP string
    try:
        ip_obj = ipaddress.ip_address(hostname)
        if is_ip_forbidden(ip_obj):
            raise HTTPException(status_code=403, detail="Access to private IP range is forbidden")
        return url, hostname
    except ValueError:
        pass  # Hostname is a domain name, proceed to DNS resolution

    # Resolve domain to IP addresses via DNS
    try:
        resolved_info = socket.getaddrinfo(hostname, parsed.port or (443 if parsed.scheme == "https" else 80))
    except socket.gaierror as exc:
        raise HTTPException(status_code=502, detail=f"Failed to resolve upstream hostname {hostname}") from exc

    first_ip = None
    for family, socktype, proto, canonname, sockaddr in resolved_info:
        ip_str = sockaddr[0]
        if not first_ip:
            first_ip = ip_str
        try:
            ip_obj = ipaddress.ip_address(ip_str)
            if is_ip_forbidden(ip_obj):
                raise HTTPException(
                    status_code=403,
                    detail=f"Host {hostname} resolved to forbidden IP {ip_str}",
                )
        except ValueError:
            continue

    return url, first_ip or hostname


class PinnedAsyncNetworkBackend(AsyncNetworkBackend):
    """Connect one validated hostname to one validated address without re-resolving it."""

    def __init__(self, hostname: str, address: str):
        self.hostname = hostname.rstrip(".").lower()
        self.address = address
        self.backend = AutoBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        normalized = host.rstrip(".").lower()
        if normalized != self.hostname:
            raise OSError("Connection target does not match the validated hostname")
        return await self.backend.connect_tcp(
            self.address,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        raise OSError("Unix sockets are not valid upstream asset targets")

    async def sleep(self, seconds: float) -> None:
        await self.backend.sleep(seconds)


def pinned_httpx_transport(hostname: str, address: str) -> httpx.AsyncHTTPTransport:
    """Create an HTTP transport whose TCP dial is pinned while TLS keeps the URL hostname."""
    transport = httpx.AsyncHTTPTransport(trust_env=False, retries=0)
    ssl_context = transport._pool._ssl_context
    transport._pool = httpcore.AsyncConnectionPool(
        ssl_context=ssl_context,
        max_connections=2,
        max_keepalive_connections=0,
        keepalive_expiry=0,
        network_backend=PinnedAsyncNetworkBackend(hostname, address),
    )
    return transport
