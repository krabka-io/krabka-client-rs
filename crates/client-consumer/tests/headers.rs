//! A `ConsumerRecord` carries the headers that the producer set.
//!
//! The Kafka v2 record format stores per-record headers as key and value
//! pairs. This suite proves the full round trip through a real broker. The
//! producer writes one record with one header. The consumer polls that record
//! and returns the same header key and the same header value.
//!
//! `krabka_client_consumer::Header` and `krabka_client_producer::Header` are
//! two types with the same shape, one per crate boundary. The suite therefore
//! imports the consumer type under an alias and compares against that type.
//!
//! The case starts a Kafka container, so it carries `#[ignore]`. Run it with
//! Docker present:
//!
//! ```text
//! cargo test -p krabka-client-consumer --test headers -- --ignored --nocapture
//! ```

mod support;

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord, Header as ConsumerHeader};
use krabka_client_producer::{Header, Producer, ProducerRecord};
use krabka_units::secs;

/// How long the case waits for its one record.
const POLL_BUDGET: Duration = Duration::from_secs(30);
/// Per-poll wait. Several short polls let the group finish its first
/// `JoinGroup`/`SyncGroup` round inside the budget.
const POLL_WAIT_SECS: u32 = 2;

/// Poll until a record arrives, or fail once the budget runs out.
///
/// The budget is a real-time bound. The broker is a separate process and gives
/// this side no assignment signal to await, so a slow assignment must not hang
/// the case.
async fn poll_first_batch(consumer: &mut Consumer) -> Vec<ConsumerRecord> {
    let deadline = Instant::now() + POLL_BUDGET;
    loop {
        let records = consumer.poll(secs(POLL_WAIT_SECS)).await.expect("poll");
        if !records.is_empty() {
            return records;
        }
        assert!(
            Instant::now() < deadline,
            "no record arrived within {POLL_BUDGET:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn consumer_record_carries_headers() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let bootstrap = kafka.bootstrap.clone();

    // Create the topic before the produce.
    let client = support::bootstrap_client(&bootstrap).await;
    let topic = support::unique("headers");
    support::create_topic(&client, &topic).await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.as_str())
        .build()
        .await
        .expect("producer build");
    producer
        .send(ProducerRecord {
            topic: topic.clone(),
            partition: None,
            key: None,
            value: Some("v".into()),
            headers: vec![Header {
                key: "trace".into(),
                value: Some("abc".into()),
            }],
            timestamp_ms: None,
        })
        .await
        .await
        .expect("producer ack channel")
        .expect("producer ack");
    producer.flush().await.expect("producer flush");

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.as_str())
        .group_id(support::unique("headers-group"))
        .subscribe(vec![topic.clone()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("consumer build");

    let records = poll_first_batch(&mut consumer).await;
    assert!(
        records[0].headers
            == vec![ConsumerHeader {
                key: "trace".into(),
                value: Some("abc".into()),
            }]
    );

    consumer.close().await.expect("close");
    client.close();
    drop(kafka);
}
