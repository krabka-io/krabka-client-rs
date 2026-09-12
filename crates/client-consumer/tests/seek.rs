//! Integration coverage for `Consumer::seek`.
//!
//! `crates/client-consumer/src/seek.rs` carries no unit tests. This suite is
//! therefore the only coverage of the method, and it runs against a real
//! broker. It proves two properties:
//!
//! - A seek that the caller issues before the first poll survives assignment.
//!   The consumer holds the target in `pending_seeks` and applies it after the
//!   post-assignment offset prime and before it builds the first fetch. The
//!   seek thus wins over the prime. The consumer delivers no record below the
//!   sought offset and skips no record above it.
//! - The reject boundary is `offset < 0`. A negative offset gives
//!   `ConsumerError::InvalidOffset`. Offset 0 is a valid target, because it
//!   means "read this partition again from the start".
//!
//! Both cases start a Kafka container, so both carry `#[ignore]`. Run them
//! with Docker present:
//!
//! ```text
//! cargo test -p krabka-client-consumer --test seek -- --ignored --nocapture
//! ```

mod support;

use std::time::{Duration, Instant};

use assert2::{assert, check};
use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerError, ConsumerRecord};
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_units::millis;

/// How long a case waits for the records it expects.
const POLL_BUDGET: Duration = Duration::from_secs(30);
/// Per-poll wait. Several short polls let the group finish its first
/// `JoinGroup`/`SyncGroup` round inside the budget.
const POLL_WAIT_MS: u32 = 500;

/// Write `n` records to partition 0 of `topic`, one per send.
///
/// The producer is the path a caller uses, so the offsets under test come from
/// the same code the seek later reads back. Each record carries a distinct key
/// and value, which keeps a mis-ordered result readable in a failure dump.
async fn produce_n(bootstrap: &str, topic: &str, n: u32) {
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .expect("producer build");
    for i in 0..n {
        producer
            .send(ProducerRecord {
                topic: topic.to_string(),
                partition: Some(0),
                key: Some(format!("k{i}").into()),
                value: Some(format!("v{i}").into()),
                headers: vec![],
                timestamp_ms: None,
            })
            .await
            .await
            .expect("producer ack channel")
            .expect("producer ack");
    }
    producer.flush().await.expect("producer flush");
}

/// Poll until `want` records arrive or the budget runs out.
///
/// The budget is a real-time bound. The broker is a separate process and gives
/// this side no assignment signal to await, so a slow assignment must not hang
/// the case.
async fn poll_until(consumer: &mut Consumer, want: usize) -> Vec<ConsumerRecord> {
    let deadline = Instant::now() + POLL_BUDGET;
    let mut got = Vec::new();
    while got.len() < want && Instant::now() < deadline {
        let records = consumer.poll(millis(POLL_WAIT_MS)).await.expect("poll");
        got.extend(records);
    }
    got
}

/// Build a consumer with a fresh group that reads from the earliest offset.
async fn earliest_consumer(bootstrap: &str, topic: &str, group_prefix: &str) -> Consumer {
    Consumer::builder()
        .bootstrap(bootstrap)
        .group_id(support::unique(group_prefix))
        .subscribe(vec![topic.to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("consumer build")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn seek_before_first_poll_resumes_from_sought_offset() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let bootstrap = kafka.bootstrap.clone();

    let client = support::bootstrap_client(&bootstrap).await;
    let topic = support::unique("seek");
    support::create_topic(&client, &topic).await;

    // Offsets 0..=4 on partition 0.
    produce_n(&bootstrap, &topic, 5).await;

    // A fresh group with Earliest reads from offset 0 if no seek intervenes.
    let mut consumer = earliest_consumer(&bootstrap, &topic, "seek-group").await;

    // Seek to offset 2 before the first poll, that is before the partition is
    // even sure to be assigned. The consumer must hold the target and apply it
    // after assignment and before any fetch.
    consumer.seek(topic.as_str(), 0, 2).await.expect("seek");

    let got = poll_until(&mut consumer, 3).await;
    let offsets: Vec<i64> = got.iter().map(|r| r.offset).collect();
    // The consumer delivers no pre-seek record, that is no offset 0 or 1, and
    // skips nothing above the seek. The result is exactly 2, 3, 4.
    assert!(offsets == vec![2, 3, 4]);

    consumer.close().await.expect("close");
    client.close();
    drop(kafka);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn seek_rejects_negative_offset() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let bootstrap = kafka.bootstrap.clone();

    let client = support::bootstrap_client(&bootstrap).await;
    let topic = support::unique("seek-negative");
    support::create_topic(&client, &topic).await;

    let consumer = earliest_consumer(&bootstrap, &topic, "seek-negative-group").await;

    let rejected = consumer.seek(topic.as_str(), 0, -1).await;
    check!(let Err(ConsumerError::InvalidOffset(-1)) = rejected);

    // Offset 0 is a valid seek target, because it re-reads the partition from
    // the start. The reject boundary is strictly `offset < 0`, so the method
    // must accept 0.
    check!(consumer.seek(topic.as_str(), 0, 0).await.is_ok());

    consumer.close().await.expect("close");
    client.close();
    drop(kafka);
}
