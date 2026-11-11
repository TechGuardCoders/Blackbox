//! Blackbox — post-incident flight recorder.
//!
//! Bounded in-memory ring buffer of the last N events (requests, errors,
//! metric samples) with timestamps. When capacity is exceeded, the oldest
//! events fall off. [`Blackbox::dump`] returns everything in order for
//! post-mortems.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde_json::Value;

/// A single recorded event.
#[derive(Debug, Clone, Serialize)]
pub struct Event {
    /// Timestamp (seconds since the epoch, from the clock).
    pub ts: f64,
    /// Event kind, e.g. "request", "metric", "upstream_5xx".
    pub kind: String,
    /// Arbitrary JSON-able payload.
    pub payload: Value,
    /// Whether this was recorded via [`Blackbox::error`].
    pub error: bool,
}

/// Aggregate counts returned by [`Blackbox::stats`].
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Stats {
    /// Number of events currently retained.
    pub total: usize,
    /// Counts by event kind.
    pub counts_by_kind: HashMap<String, usize>,
    /// Total errors ever recorded (survives eviction).
    pub errors: u64,
    /// `errors / total`, or 0.0 when empty.
    pub error_rate: f64,
}

struct Inner {
    max_events: usize,
    events: VecDeque<Event>,
    error_count: u64,
}

/// Ring buffer of the last `max_events` events, thread-safe.
///
/// All methods take `&self`, so wrap in an `Arc` to share across threads
/// (see [`SharedBlackbox`]).
pub struct Blackbox {
    inner: Mutex<Inner>,
    clock: Box<dyn Fn() -> f64 + Send + Sync>,
}

impl Blackbox {
    /// Create a new recorder holding at most `max_events` events.
    ///
    /// # Panics
    /// Panics if `max_events < 1` (mirrors the Python `ValueError`).
    pub fn new(max_events: usize) -> Self {
        Self::with_clock(max_events, default_clock)
    }

    /// Like [`Blackbox::new`] with a custom clock (seconds since epoch).
    ///
    /// # Panics
    /// Panics if `max_events < 1`.
    pub fn with_clock<F>(max_events: usize, clock: F) -> Self
    where
        F: Fn() -> f64 + Send + Sync + 'static,
    {
        assert!(max_events >= 1, "max_events must be >= 1");
        Blackbox {
            inner: Mutex::new(Inner {
                max_events,
                events: VecDeque::with_capacity(max_events),
                error_count: 0,
            }),
            clock: Box::new(clock),
        }
    }

    /// Record an event of `kind` with arbitrary JSON-able payload.
    pub fn log(&self, kind: impl Into<String>, payload: Value) {
        self.append(kind, payload, false);
    }

    /// Record an error event; also bumps the error counter.
    pub fn error(&self, kind: impl Into<String>, payload: Value) {
        self.append(kind, payload, true);
    }

    fn append(&self, kind: impl Into<String>, payload: Value, error: bool) {
        let event = Event {
            ts: (self.clock)(),
            kind: kind.into(),
            payload,
            error,
        };
        let mut inner = self.inner.lock().unwrap();
        if inner.events.len() == inner.max_events {
            inner.events.pop_front();
        }
        inner.events.push_back(event);
        if error {
            inner.error_count += 1;
        }
    }

    /// All retained events, oldest first — for the post-mortem.
    pub fn dump(&self) -> Vec<Event> {
        let inner = self.inner.lock().unwrap();
        inner.events.iter().cloned().collect()
    }

    /// Counts by kind plus overall error rate.
    pub fn stats(&self) -> Stats {
        let inner = self.inner.lock().unwrap();
        let mut counts: HashMap<String, usize> = HashMap::new();
        for e in &inner.events {
            *counts.entry(e.kind.clone()).or_insert(0) += 1;
        }
        let total = inner.events.len();
        Stats {
            total,
            counts_by_kind: counts,
            errors: inner.error_count,
            error_rate: if total > 0 {
                inner.error_count as f64 / total as f64
            } else {
                0.0
            },
        }
    }
}

fn default_clock() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Thread-safe shared handle, matching the Python object's usage pattern.
pub type SharedBlackbox = Arc<Blackbox>;

#[cfg(test)]
mod tests;
