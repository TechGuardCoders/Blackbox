use blackbox::Blackbox;
use serde_json::json;

fn main() {
    let bb = Blackbox::new(1000);

    bb.log("request", json!({"path": "/v1/chat", "tenant": "tenant-a"}));
    bb.log("metric", json!({"tok_s": 45.2}));
    bb.error("upstream_5xx", json!({"status": 502}));

    let stats = bb.stats();
    println!(
        "total={} errors={} error_rate={:.3}",
        stats.total, stats.errors, stats.error_rate
    );
    for (kind, n) in &stats.counts_by_kind {
        println!("  {kind}: {n}");
    }

    for event in bb.dump() {
        // oldest first, for the post-mortem
        println!("{} {} {}", event.ts, event.kind, event.payload);
    }
}
