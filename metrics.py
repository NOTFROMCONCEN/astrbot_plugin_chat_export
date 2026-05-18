from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class MetricPoint:
    name: str
    value: float
    labels: dict[str, str] = field(default_factory=dict)
    ts: float = field(default_factory=time.time)


class MetricsCollector:
    """轻量级内存指标收集器，保留最近 1000 条点数据，零外部依赖。"""

    def __init__(self, max_points: int = 1000):
        self._points: list[MetricPoint] = []
        self._max_points = max_points
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}

    def inc(self, name: str, value: float = 1.0, labels: dict[str, str] | None = None) -> None:
        key = self._key(name, labels)
        self._counters[key] = self._counters.get(key, 0.0) + value
        self._append(MetricPoint(name, value, labels or {}))

    def record(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        self._append(MetricPoint(name, value, labels or {}))

    def gauge(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        key = self._key(name, labels)
        self._gauges[key] = value
        self._append(MetricPoint(name, value, labels or {}))

    def counter_value(self, name: str, labels: dict[str, str] | None = None) -> float:
        return self._counters.get(self._key(name, labels), 0.0)

    def gauge_value(self, name: str, labels: dict[str, str] | None = None) -> float:
        return self._gauges.get(self._key(name, labels), 0.0)

    def snapshot(self) -> dict[str, Any]:
        """返回当前指标快照（可序列化）。"""
        result: dict[str, Any] = {
            "counters": {},
            "gauges": {},
            "histogram_recent": {},
        }
        for key, value in self._counters.items():
            name, labels = self._decode_key(key)
            result["counters"][self._format_key(name, labels)] = value
        for key, value in self._gauges.items():
            name, labels = self._decode_key(key)
            result["gauges"][self._format_key(name, labels)] = value
        for name in {"sqlite_flush_latency_ms", "qdrant_flush_latency_ms", "embedding_latency_ms"}:
            vals = [p.value for p in self._points if p.name == name]
            if vals:
                result["histogram_recent"][name] = {
                    "count": len(vals),
                    "avg": round(sum(vals) / len(vals), 2),
                    "min": round(min(vals), 2),
                    "max": round(max(vals), 2),
                }
        return result

    def _append(self, point: MetricPoint) -> None:
        self._points.append(point)
        if len(self._points) > self._max_points:
            self._points = self._points[-self._max_points:]

    @staticmethod
    def _key(name: str, labels: dict[str, str] | None) -> str:
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    @staticmethod
    def _decode_key(key: str) -> tuple[str, dict[str, str]]:
        if "{" not in key:
            return key, {}
        name, rest = key.split("{", 1)
        rest = rest.rstrip("}")
        labels = {}
        for part in rest.split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                labels[k] = v
        return name, labels

    @staticmethod
    def _format_key(name: str, labels: dict[str, str]) -> str:
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"
