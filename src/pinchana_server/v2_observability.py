"""Low-cardinality in-process counters for the staged v2 rollout."""

from __future__ import annotations

from collections import Counter, defaultdict
import re
from threading import Lock


PLATFORMS = frozenset({
    "instagram",
    "tiktok",
    "threads",
    "twitter",
    "soundcloud",
    "spotify",
    "deezer",
    "ytmusic",
    "unknown",
})
METRIC_NAME = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
DURATION_BUCKETS = (0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)


class V2Observability:
    def __init__(self) -> None:
        self._counters: Counter[tuple[str, str]] = Counter()
        self._histogram_sum: dict[tuple[str, str], float] = defaultdict(float)
        self._histogram_count: Counter[tuple[str, str]] = Counter()
        self._histogram_buckets: Counter[tuple[str, str, float]] = Counter()
        self._gauges: dict[tuple[str, str], float] = {}
        self._lock = Lock()

    @staticmethod
    def _platform(platform: str | None) -> str:
        value = str(platform or "unknown").lower()
        return value if value in PLATFORMS else "unknown"

    @staticmethod
    def _name(name: str) -> str:
        value = str(name).lower()
        return value if METRIC_NAME.fullmatch(value) else "unknown"

    def increment(self, name: str, *, platform: str | None = None, amount: int = 1) -> None:
        key = (self._name(name), self._platform(platform))
        with self._lock:
            self._counters[key] += max(0, int(amount))

    def observe(self, name: str, seconds: float, *, platform: str | None = None) -> None:
        key = (self._name(name), self._platform(platform))
        with self._lock:
            self._histogram_sum[key] += max(0.0, float(seconds))
            self._histogram_count[key] += 1
            for boundary in DURATION_BUCKETS:
                if seconds <= boundary:
                    self._histogram_buckets[(key[0], key[1], boundary)] += 1

    def set_gauge(self, name: str, value: float, *, platform: str | None = None) -> None:
        key = (self._name(name), self._platform(platform))
        with self._lock:
            self._gauges[key] = max(0.0, float(value))

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
            gauges = {
                f"{name}:{platform}": value
                for (name, platform), value in sorted(self._gauges.items())
            }
        return {"counters": counters, "average_seconds": averages, "gauges": gauges}

    def prometheus(self) -> str:
        """Render a collector-friendly, low-cardinality exposition payload."""
        with self._lock:
            counters = sorted(self._counters.items())
            sums = dict(self._histogram_sum)
            counts = sorted(self._histogram_count.items())
            buckets = dict(self._histogram_buckets)
            gauges = sorted(self._gauges.items())
        lines = [
            "# HELP pinchana_v2_events_total V2 rollout events.",
            "# TYPE pinchana_v2_events_total counter",
        ]
        for (name, platform), value in counters:
            lines.append(
                f'pinchana_v2_events_total{{event="{name}",platform="{platform}"}} {value}'
            )
        lines.extend([
            "# HELP pinchana_v2_duration_seconds V2 operation durations.",
            "# TYPE pinchana_v2_duration_seconds histogram",
        ])
        for (name, platform), count in counts:
            labels = f'metric="{name}",platform="{platform}"'
            for boundary in DURATION_BUCKETS:
                bucket_count = buckets.get((name, platform, boundary), 0)
                lines.append(
                    "pinchana_v2_duration_seconds_bucket"
                    f'{{{labels},le="{boundary:g}"}} {bucket_count}'
                )
            lines.append(
                f'pinchana_v2_duration_seconds_bucket{{{labels},le="+Inf"}} {count}'
            )
            lines.append(f"pinchana_v2_duration_seconds_sum{{{labels}}} {sums[(name, platform)]}")
            lines.append(f"pinchana_v2_duration_seconds_count{{{labels}}} {count}")
        lines.extend([
            "# HELP pinchana_v2_gauge V2 rollout runtime gauges.",
            "# TYPE pinchana_v2_gauge gauge",
        ])
        for (name, platform), value in gauges:
            lines.append(f'pinchana_v2_gauge{{metric="{name}",platform="{platform}"}} {value}')
        return "\n".join(lines) + "\n"


v2_observability = V2Observability()
