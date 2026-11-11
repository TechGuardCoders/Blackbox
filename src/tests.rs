use std::sync::Arc;

use serde_json::json;

use super::Blackbox;

/// Fake clock: starts at 1000.0, +1.0 per call (mirrors the Python FakeClock).
struct TickingClock {
    t: std::sync::Mutex<f64>,
}
impl TickingClock {
    fn new() -> Self {
        TickingClock { t: std::sync::Mutex::new(1000.0) }
    }
    fn tick(&self) -> f64 {
        let mut t = self.t.lock().unwrap();
        *t += 1.0;
        *t
    }
}

fn bb_with_ticking(max_events: usize) -> (Blackbox, std::sync::Arc<TickingClock>) {
    let clock = Arc::new(TickingClock::new());
    let c = clock.clone();
    (Blackbox::with_clock(max_events, move || c.tick()), clock)
}

#[test]
fn logs_and_dumps_in_order() {
    let (bb, _clock) = bb_with_ticking(10);
    bb.log("request", json!({"path": "/v1/chat"}));
    bb.log("metric", json!({"tok_s": 45.2}));
    let events = bb.dump();
    let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
    assert_eq!(kinds, vec!["request", "metric"]);
    assert_eq!(events[0].payload, json!({"path": "/v1/chat"}));
}

#[test]
fn events_have_timestamps() {
    let (bb, _clock) = bb_with_ticking(5);
    bb.log("request", json!({}));
    bb.log("request", json!({}));
    let ts: Vec<f64> = bb.dump().iter().map(|e| e.ts).collect();
    assert!(ts[0] < ts[1]);
}

#[test]
fn oldest_fall_off_when_full() {
    let (bb, _clock) = bb_with_ticking(3);
    for i in 0..5 {
        bb.log("request", json!({"i": i}));
    }
    let events = bb.dump();
    assert_eq!(events.len(), 3);
    let is: Vec<i64> = events
        .iter()
        .map(|e| e.payload["i"].as_i64().unwrap())
        .collect();
    assert_eq!(is, vec![2, 3, 4]);
}

#[test]
fn error_bumps_counter() {
    let (bb, _clock) = bb_with_ticking(10);
    bb.log("request", json!({}));
    bb.error("upstream_5xx", json!({"status": 502}));
    bb.error("timeout", json!({}));
    let s = bb.stats();
    assert_eq!(s.errors, 2);
    assert!((s.error_rate - 2.0 / 3.0).abs() < 1e-9);
}

#[test]
fn stats_counts_by_kind() {
    let (bb, _clock) = bb_with_ticking(100);
    for _ in 0..3 {
        bb.log("request", json!({}));
    }
    for _ in 0..2 {
        bb.log("metric", json!({}));
    }
    bb.error("boom", json!({}));
    let s = bb.stats();
    assert_eq!(s.counts_by_kind.len(), 3);
    assert_eq!(s.counts_by_kind["request"], 3);
    assert_eq!(s.counts_by_kind["metric"], 2);
    assert_eq!(s.counts_by_kind["boom"], 1);
    assert_eq!(s.total, 6);
}

#[test]
fn error_count_survives_eviction() {
    let (bb, _clock) = bb_with_ticking(2);
    for i in 0..10 {
        bb.error("err", json!({"i": i}));
    }
    let s = bb.stats();
    assert_eq!(s.errors, 10);
    assert_eq!(s.total, 2);
}

#[test]
fn empty_stats() {
    let (bb, _clock) = bb_with_ticking(1000);
    let s = bb.stats();
    assert_eq!(s.total, 0);
    assert!(s.counts_by_kind.is_empty());
    assert_eq!(s.errors, 0);
    assert_eq!(s.error_rate, 0.0);
}

#[test]
fn default_max_events_1000() {
    let (bb, _clock) = bb_with_ticking(1000);
    for i in 0..1500 {
        bb.log("request", json!({"i": i}));
    }
    let events = bb.dump();
    assert_eq!(events.len(), 1000);
    assert_eq!(events.last().unwrap().payload["i"], 1499);
}

#[test]
#[should_panic(expected = "max_events must be >= 1")]
fn invalid_max_events() {
    let _ = Blackbox::new(0);
}

#[test]
fn thread_safety() {
    let (bb, _clock) = bb_with_ticking(500);
    let shared = Arc::new(bb);
    let mut handles = Vec::new();
    for _ in 0..4 {
        let bb = shared.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..100 {
                bb.log("request", json!({"i": i}));
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(shared.stats().total, 400);
}

#[test]
fn dump_returns_copy() {
    let (bb, _clock) = bb_with_ticking(5);
    bb.log("request", json!({}));
    let mut d = bb.dump();
    d.clear();
    assert_eq!(bb.dump().len(), 1);
}
