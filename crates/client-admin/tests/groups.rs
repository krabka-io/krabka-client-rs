//! Proves that the admin client sees a live consumer group on a real broker.
//!
//! The suite produces one record, consumes it in a group, and commits. It then
//! checks two admin surfaces against that group: `ListGroups` must name the
//! group, and `OffsetFetch` must report a committed offset of 1 for the single
//! partition. Offset 1 is the next offset to read, so it proves the commit
//! covered the one record.
//!
//! The broker is `confluentinc/cp-kafka:6.1.1` in Docker, so the case carries
//! `#[ignore]`. Run it with Docker present:
//!
//! ```text
//! cargo test -p krabka-client-admin --test groups -- --ignored --nocapture
//! ```

mod support;

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_producer::{Producer, ProducerRecord};

/// How long the case waits for the group to join and the first record to
/// arrive.
const RECORD_DEADLINE: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn lists_groups_and_committed_offsets() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let mut admin = support::admin(&kafka.bootstrap).await;

    let topic = support::unique("groups-topic");
    let group = support::unique("groups-group");
    support::create_topic(&mut admin, &topic, 1).await;
    produce_one(&kafka.bootstrap, &topic).await;

    let mut consumer = Consumer::builder()
        .bootstrap(kafka.bootstrap.as_str())
        .group_id(group.as_str())
        .subscribe(vec![topic.clone()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("consumer build");
    poll_until_record(&mut consumer).await;
    consumer.commit_sync().await.expect("commit_sync");

    let groups = admin.list_groups().await.expect("list_groups");
    assert!(groups.contains(&group), "{groups:?}");

    let offsets = admin
        .list_consumer_group_offsets(&group)
        .await
        .expect("list_consumer_group_offsets");
    let committed = offsets.get(&(topic.clone(), 0)).copied();
    assert!(committed == Some(1), "{offsets:?}");

    drop(kafka);
}

/// Write one record and wait for its ack.
async fn produce_one(bootstrap: &str, topic: &str) {
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .expect("producer build");

    producer
        .send(ProducerRecord {
            topic: topic.to_owned(),
            value: Some("v".into()),
            ..Default::default()
        })
        .await
        .await
        .expect("producer ack channel closed")
        .expect("produce record");

    producer.flush().await.expect("flush");
}

/// Poll until at least one record arrives, or fail at the deadline.
///
/// The first polls of a cold group return no records while the group joins and
/// the assignment settles. A commit after such a poll records offset 0, and
/// the offset assertion then fails for a reason that is not a client defect.
async fn poll_until_record(consumer: &mut Consumer) {
    let deadline = Instant::now() + RECORD_DEADLINE;
    while Instant::now() < deadline {
        let records = consumer.poll(krabka_units::secs(2)).await.expect("poll");
        if !records.is_empty() {
            return;
        }
    }
    panic!("no record arrived within {RECORD_DEADLINE:?}");
}
