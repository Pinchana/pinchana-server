"""Low-cardinality in-process counters for the staged v2 rollout."""

from __future__ import annotations

from collections import Counter, defaultdict
from threading import Lock


PLATFORMS = frozenset({"instagram", "tiktok", "threads", "twitter", "unknown"})


class V2Observability:
    def __init__(self) -> None:
        self._counters: Counter[tuple[str, str]] = Counter()
        self._histogram_sum: dict[tuple[str, str], float] = defaultdict(float)
        self._histogram_count: Counter[tuple[str, str]] = Counter()
        self._lock = Lock()

    @staticmethod
    def _platform(platform: str | None) -> str:
        value = str(platform or "unknown").lower()
        return value if value in PLATFORMS else "unknown"

    def increment(self, name: str, *, platform: str | None = None, amount: int = 1) -> None:
        key = (name, self._platform(platform))
        with self._lock:
            self._counters[key] += max(0, int(amount))

    def observe(self, name: str, seconds: float, *, platform: str | None = None) -> None:
        key = (name, self._platform(platform))
        with self._lock:
            self._histogram_sum[key] += max(0.0, float(seconds))
            self._histogram_count[key] += 1

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            counters = {
                f"{name}:{platform}": value
                for (name, platform), value in sorted(self._counters.items())
            }
            averages = {
                f"{name}:{platform}": self._histogram_sum[(name, platform)] / count
                for (name, platform), count in sorted(self._histogram_count.items())
                if count
            }
        return {"counters": counters, "average_seconds": averages}


v2_observability = V2Observability()
