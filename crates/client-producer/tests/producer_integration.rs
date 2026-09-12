//! End-to-end producer coverage against a real Kafka broker in Docker.
//!
//! The suite proves three producer behaviours:
//!
//! 1. The builder rejects `enable_idempotence=true` together with `acks=0`.
//!    Kafka forbids that pair, because a producer that gets no acknowledgement
//!    cannot detect a duplicate.
//! 2. A non-idempotent `acks=0` producer sends and flushes without a stall.
//! 3. An idempotent `acks=all` producer writes a batch of records, and a
//!    consumer group reads the same number of records back.
//!
//! The monorepo version of this suite started a Krabka broker in the same
//! process. The broker moved to another repository, so the suite now starts
//! `confluentinc/cp-kafka:6.1.1` through `tests/support/mod.rs`. Every case
//! needs Docker and carries `#[ignore]`.
//!
//! `flavor = "multi_thread", worker_threads = 2` stays necessary. A
//! single-threaded runtime cannot drive the producer sender task and the test
//! body at the same time.

mod support;

use std::time::Duration;

use bytes::Bytes;
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_producer::{Acks, Producer, ProducerError, ProducerRecord};

/// Record count for the produce-then-consume case.
const PRODUCE_N: usize = 20;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn idempotence_plus_acks_zero_rejects() {
    support::init_tracing();
    // The builder validates the configuration before it opens a connection,
    // so this case needs no broker traffic. It still starts the container so
    // that the bootstrap address is a real one. A bad address would make
    // `build` fail for the wrong reason, and the case would stop proving what
    // it claims.
    let kafka = support::start_kafka().await;

    let result = Producer::builder()
        .bootstrap(kafka.bootstrap.clone())
        .enable_idempotence(true)
        .acks(Acks::Zero)
        .build()
        .await;

    assert2::assert!(matches!(result, Err(ProducerError::InvalidConfig(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn non_idempotent_acks_zero_fire_and_forget() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let client = support::bootstrap_client(&kafka.bootstrap).await;
    let topic = support::unique("producer-acks-zero");
    support::create_topic(&client, &topic).await;

    let producer = Producer::builder()
        .bootstrap(kafka.bootstrap.clone())
        .enable_idempotence(false)
        .acks(Acks::Zero)
        .linger(Duration::from_millis(5))
        .build()
        .await
        .expect("producer build");

    let acknowledgement = producer
        .send(ProducerRecord {
            topic: topic.clone(),
            value: Some(Bytes::from_static(b"x")),
            ..Default::default()
        })
        .await;
    producer.flush().await.expect("flush");
    // acks=0 is fire-and-forget. The oneshot may resolve with Ok, or the
    // sender may drop it. Both outcomes are correct. The case only proves
    // that the send does not hang.
    let _ = tokio::time::timeout(Duration::from_secs(2), acknowledgement).await;

    producer.close().await.expect("close");
    client.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn idempotent_produce_then_consume() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let client = support::bootstrap_client(&kafka.bootstrap).await;
    let topic = support::unique("producer-idempotent");
    support::create_topic(&client, &topic).await;

    produce_records(&kafka.bootstrap, &topic).await;
    let seen = consume_records(&kafka.bootstrap, &topic).await;
    assert2::assert!(seen == PRODUCE_N);

    client.close();
}

/// Write [`PRODUCE_N`] records with an idempotent `acks=all` producer and wait
/// for every acknowledgement.
async fn produce_records(bootstrap: &str, topic: &str) {
    let producer = Producer::builder()
        .bootstrap(bootstrap.to_owned())
        .enable_idempotence(true)
        .acks(Acks::All)
        .linger(Duration::from_millis(5))
        .build()
        .await
        .expect("producer build");

    let mut acknowledgements = Vec::with_capacity(PRODUCE_N);
    for i in 0..PRODUCE_N {
        acknowledgements.push(
            producer
                .send(ProducerRecord {
                    topic: topic.to_owned(),
                    value: Some(Bytes::from(format!("v{i}"))),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.expect("flush");

    for (i, acknowledgement) in acknowledgements.into_iter().enumerate() {
        let metadata = acknowledgement
            .await
            .expect("oneshot")
            .unwrap_or_else(|e| panic!("record {i} failed: {e:?}"));
        assert2::assert!(metadata.partition == 0);
    }

    producer.close().await.expect("producer close");
}

/// Read the topic back through a consumer group and return the record count.
///
/// The loop stops at [`PRODUCE_N`] records or at the deadline, whichever comes
/// first. The caller compares the count.
async fn consume_records(bootstrap: &str, topic: &str) -> usize {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_owned())
        .client_id("producer-integration-consumer")
        .group_id(support::unique("producer-integration-group"))
        .session_timeout(krabka_units::secs(30))
        .rebalance_timeout(krabka_units::secs(2))
        .heartbeat_interval(krabka_units::secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic.to_owned()])
        .build()
        .await
        .expect("consumer build");

    let mut seen = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while seen < PRODUCE_N && std::time::Instant::now() < deadline {
        seen += consumer
            .poll(krabka_units::millis(500))
            .await
            .expect("poll")
            .len();
    }

    consumer.close().await.expect("consumer close");
    seen
}
