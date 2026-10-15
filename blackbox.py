"""Blackbox — post-incident flight recorder.

Bounded in-memory ring buffer of the last N events (requests, errors,
metric samples) with timestamps. When capacity is exceeded, the oldest
events fall off. dump() returns everything in order for post-mortems.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any, Dict, List, Optional


class Blackbox:
    """Ring buffer of the last `max_events` events, thread-safe."""

    def __init__(self, max_events: int = 1000, clock: Optional[Any] = None):
        if max_events < 1:
            raise ValueError("max_events must be >= 1")
        self._max_events = max_events
        self._clock = clock or time.time
        self._events: deque = deque(maxlen=max_events)
        self._lock = threading.Lock()
        self._error_count = 0

    def log(self, kind: str, payload: Dict[str, Any]) -> None:
        """Record an event of `kind` with arbitrary JSON-able payload."""
        self._append(kind, payload, error=False)

    def error(self, kind: str, payload: Dict[str, Any]) -> None:
        """Record an error event; also bumps the error counter."""
        self._append(kind, payload, error=True)

    def _append(self, kind: str, payload: Dict[str, Any], error: bool) -> None:
        event = {
            "ts": self._clock(),
            "kind": kind,
            "payload": payload,
            "error": error,
        }
        with self._lock:
            self._events.append(event)
            if error:
                self._error_count += 1

    def dump(self) -> List[Dict[str, Any]]:
        """All retained events, oldest first — for the post-mortem."""
        with self._lock:
            return list(self._events)

    def stats(self) -> Dict[str, Any]:
        """Counts by kind plus overall error rate."""
        with self._lock:
            counts: Dict[str, int] = {}
            for e in self._events:
                counts[e["kind"]] = counts.get(e["kind"], 0) + 1
            total = len(self._events)
            return {
                "total": total,
                "counts_by_kind": counts,
                "errors": self._error_count,
                "error_rate": (self._error_count / total) if total else 0.0,
            }
