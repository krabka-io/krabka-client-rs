//! Integration coverage for `Consumer::seek`.
//!
//! `crates/client-consumer/src/seek.rs` carries no unit tests. This suite is
//! therefore the only coverage of the method, and it runs against a real
//! broker. It proves three properties:
//!
//! - A seek that the caller issues before the first poll wins over the prime
//!   that `Consumer::build` already ran. The consumer delivers no record below
//!   the sought offset and skips no record above it.
//! - A seek on a partition the consumer does NOT hold waits in `pending_seeks`
//!   until the partition lands, and then beats the prime that the assignment
//!   triggers. `Consumer::build` completes `JoinGroup`, `SyncGroup` and the
//!   initial prime before it returns, so a second group member is what makes a
//!   partition unassigned at the time of the seek.
//! - The reject boundary is `offset < 0`. A negative offset gives
//!   `ConsumerError::InvalidOffset`. Offset 0 is a valid target, because it
//!   means "read this partition again from the start".
//!
//! Every case starts a Kafka container, so every case carries `#[ignore]`.
//! Run them with Docker present:
//!
//! ```text
//! cargo test -p krabka-client-consumer --test seek -- --ignored --nocapture
//! ```

mod support;

use std::time::{Duration, Instant};

use assert2::{assert, check};
use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerError, ConsumerRecord};
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_units::{millis, secs};

/// How long a case waits for the records it expects.
const POLL_BUDGET: Duration = Duration::from_secs(30);
/// Per-poll wait. Several short polls let the group finish its first
/// `JoinGroup`/`SyncGroup` round inside the budget.
const POLL_WAIT_MS: u32 = 500;

/// Write `n` records to `partition` of `topic`, one per send.
///
/// The producer is the path a caller uses, so the offsets under test come from
/// the same code the seek later reads back. Each record carries a distinct key
/// and value, which keeps a mis-ordered result readable in a failure dump.
async fn produce_n(bootstrap: &str, topic: &str, partition: i32, n: u32) {
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .expect("producer build");
    for i in 0..n {
        producer
            .send(ProducerRecord {
                topic: topic.to_string(),
                partition: Some(partition),
                key: Some(format!("k{partition}-{i}").into()),
                value: Some(format!("p{partition}v{i}").into()),
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

/// Build one member of `group` that reads from the earliest offset.
///
/// The timeouts match the other container suites. The short heartbeat keeps a
/// detect-and-rejoin round trip inside the broker's rebalance delay, which the
/// pre-assignment case depends on.
async fn earliest_consumer(bootstrap: &str, topic: &str, group: &str, client_id: &str) -> Consumer {
    Consumer::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .group_id(group)
        .session_timeout(secs(30))
        .rebalance_timeout(secs(10))
        .heartbeat_interval(millis(500))
        .subscribe(vec![topic.to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("consumer build")
}

/// Wait until `consumer` holds `want` partitions.
async fn wait_for_assignment_count(consumer: &Consumer, want: usize) {
    let deadline = Instant::now() + POLL_BUDGET;
    loop {
        let held = consumer.assignment().await.len();
        if held == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "assignment stalled at {held} partitions, wanted {want}"
        );
        // Real-time wait, not a progress poll: the rebalance runs in the
        // broker process and publishes no signal this side can await.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll until `want` records from `partition` arrive, and return them.
///
/// The loop also drives the rejoin that hands `consumer` the partition, so it
/// keeps what every poll delivers rather than dropping the records a rejoin
/// returns. Records from the consumer's other partitions are not of interest
/// here, so the filter leaves them out.
async fn poll_partition_until(
    consumer: &mut Consumer,
    partition: i32,
    want: usize,
) -> Vec<ConsumerRecord> {
    let deadline = Instant::now() + POLL_BUDGET;
    let mut got: Vec<ConsumerRecord> = Vec::new();
    while got.len() < want {
        assert!(
            Instant::now() < deadline,
            "only {} of {want} records arrived on partition {partition}: {got:?}",
            got.len()
        );
        let records = consumer.poll(millis(POLL_WAIT_MS)).await.expect("poll");
        got.extend(
            records
                .into_iter()
                .filter(|record| record.partition == partition),
        );
    }
    got
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
    produce_n(&bootstrap, &topic, 0, 5).await;

    // A fresh group with Earliest reads from offset 0 if no seek intervenes.
    let mut consumer =
        earliest_consumer(&bootstrap, &topic, &support::unique("seek-group"), "m1").await;

    // Seek to offset 2 before the first poll. `build` has already assigned the
    // partition and primed it to 0, so the seek must beat that prime before
    // the consumer builds its first fetch. The case below covers the other
    // branch, where the partition is not assigned yet.
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

/// A seek on a partition the consumer does not hold yet lands when it does.
///
/// `Consumer::build` returns only after `JoinGroup`, `SyncGroup` and the initial
/// offset prime, so a seek issued straight after `build` addresses a partition
/// the consumer already owns. A second member is what leaves a subscribed
/// partition unassigned. `m1` seeks the partition that `m2` holds, `m2` then
/// leaves, and `m1` acquires the partition. The prime that follows that
/// assignment sets the position to 0, because the group committed nothing for
/// that partition. Only the pending seek can move it to 2. A seek that the
/// consumer dropped, or that the prime overwrote, shows up at once as offsets
/// 0 and 1 among the delivered records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn seek_before_assignment_applies_when_the_partition_lands() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let bootstrap = kafka.bootstrap.clone();

    let client = support::bootstrap_client(&bootstrap).await;
    let topic = support::unique("seek-unassigned");
    let group = support::unique("seek-unassigned-group");
    support::create_topic_with_partitions(&client, &topic, 2).await;

    // Offsets 0..=4 on both partitions.
    produce_n(&bootstrap, &topic, 0, 5).await;
    produce_n(&bootstrap, &topic, 1, 5).await;

    // Two members split the two partitions 1/1. They are built together so
    // they batch into the first rebalance round, which avoids a follower that
    // waits on a late leader.
    let (mut m1, m2) = tokio::join!(
        earliest_consumer(&bootstrap, &topic, &group, "m1"),
        earliest_consumer(&bootstrap, &topic, &group, "m2"),
    );
    wait_for_assignment_count(&m1, 1).await;
    wait_for_assignment_count(&m2, 1).await;

    // The partition m1 does not hold. `apply_pending_seeks` materialises a
    // seek only for an assigned partition, so this one stays pending.
    let held = m1.assignment().await;
    let other = 1 - held[0].1;
    m1.seek(topic.as_str(), other, 2).await.expect("seek");
    assert!(!m1.assignment().await.contains(&(topic.clone(), other)));

    // m2 leaves and m1 takes the freed partition. Neither member polled or
    // committed for it, so the prime puts it at 0 under Earliest.
    m2.close().await.expect("close m2");
    let got = poll_partition_until(&mut m1, other, 3).await;
    let offsets: Vec<i64> = got.iter().map(|record| record.offset).collect();
    assert!(offsets == vec![2, 3, 4]);

    m1.close().await.expect("close m1");
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

    let consumer = earliest_consumer(
        &bootstrap,
        &topic,
        &support::unique("seek-negative-group"),
        "m1",
    )
    .await;

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
