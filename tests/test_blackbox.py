import time

import pytest

from blackbox import Blackbox


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        self.t += 1.0
        return self.t


def test_logs_and_dumps_in_order():
    bb = Blackbox(max_events=10, clock=FakeClock())
    bb.log("request", {"path": "/v1/chat"})
    bb.log("metric", {"tok_s": 45.2})
    events = bb.dump()
    assert [e["kind"] for e in events] == ["request", "metric"]
    assert events[0]["payload"] == {"path": "/v1/chat"}


def test_events_have_timestamps():
    bb = Blackbox(max_events=5, clock=FakeClock())
    bb.log("request", {})
    bb.log("request", {})
    ts = [e["ts"] for e in bb.dump()]
    assert ts[0] < ts[1]


def test_oldest_fall_off_when_full():
    bb = Blackbox(max_events=3, clock=FakeClock())
    for i in range(5):
        bb.log("request", {"i": i})
    events = bb.dump()
    assert len(events) == 3
    assert [e["payload"]["i"] for e in events] == [2, 3, 4]


def test_error_bumps_counter():
    bb = Blackbox(max_events=10, clock=FakeClock())
    bb.log("request", {})
    bb.error("upstream_5xx", {"status": 502})
    bb.error("timeout", {})
    assert bb.stats()["errors"] == 2
    assert bb.stats()["error_rate"] == pytest.approx(2 / 3)


def test_stats_counts_by_kind():
    bb = Blackbox(max_events=100, clock=FakeClock())
    for _ in range(3):
        bb.log("request", {})
    for _ in range(2):
        bb.log("metric", {})
    bb.error("boom", {})
    s = bb.stats()
    assert s["counts_by_kind"] == {"request": 3, "metric": 2, "boom": 1}
    assert s["total"] == 6


def test_error_count_survives_eviction():
    bb = Blackbox(max_events=2, clock=FakeClock())
    for i in range(10):
        bb.error("err", {"i": i})
    s = bb.stats()
    assert s["errors"] == 10
    assert s["total"] == 2


def test_empty_stats():
    bb = Blackbox(clock=FakeClock())
    s = bb.stats()
    assert s == {"total": 0, "counts_by_kind": {}, "errors": 0, "error_rate": 0.0}


def test_default_max_events_1000():
    bb = Blackbox(clock=FakeClock())
    for i in range(1500):
        bb.log("request", {"i": i})
    events = bb.dump()
    assert len(events) == 1000
    assert events[-1]["payload"]["i"] == 1499


def test_invalid_max_events():
    with pytest.raises(ValueError):
        Blackbox(max_events=0)


def test_thread_safety():
    bb = Blackbox(max_events=500, clock=FakeClock())

    def writer(n):
        for i in range(n):
            bb.log("request", {"i": i})

    import threading

    threads = [threading.Thread(target=writer, args=(100,)) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert bb.stats()["total"] == 400


def test_dump_returns_copy():
    bb = Blackbox(max_events=5, clock=FakeClock())
    bb.log("request", {})
    d = bb.dump()
    d.clear()
    assert len(bb.dump()) == 1
