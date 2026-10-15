# Blackbox

Post-incident flight recorder for inference services. A bounded in-memory ring buffer of the last N events — requests, errors, metric samples — with timestamps. When capacity is exceeded, the oldest events fall off. `dump()` returns everything in order for the post-mortem.

## Why

When a serving stack degrades, the first question is "what happened in the 5 minutes before the alert?" Logging everything to disk is expensive and slow; sampling loses the tail. A fixed-size ring buffer keeps the most recent window in memory at O(1) append cost, with zero I/O on the hot path — the same pattern as an aircraft's blackbox: keep the last N, overwrite the oldest, dump on demand.

## Architecture

- `Blackbox(max_events=1000)` — bounded `deque(maxlen=N)`, thread-safe via a lock.
- `log(kind, payload)` — record any event (request, metric sample, decision).
- `error(kind, payload)` — record an error event; bumps a persistent error counter (survives eviction of the events themselves).
- `dump()` — all retained events, oldest first, as a copy.
- `stats()` — total, counts by kind, error count, error rate.

## Results

- 11 tests, all passing: ordering, eviction, error counting, eviction-proof counters, thread safety, copy semantics.
- Append is O(1); memory bounded at exactly `max_events` entries.

## Usage

```python
from blackbox import Blackbox

bb = Blackbox(max_events=1000)

bb.log("request", {"path": "/v1/chat", "tenant": "tenant-a"})
bb.log("metric", {"tok_s": 45.2})
bb.error("upstream_5xx", {"status": 502})

bb.stats()
# {'total': 3, 'counts_by_kind': {'request': 1, 'metric': 1, 'upstream_5xx': 1},
#  'errors': 1, 'error_rate': 0.333...}

for event in bb.dump():  # oldest first, for the post-mortem
    print(event["ts"], event["kind"], event["payload"])
```

## Tests

```bash
uv venv && . .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install pytest
pytest -q
```
