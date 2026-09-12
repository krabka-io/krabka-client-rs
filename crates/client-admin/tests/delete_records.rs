//! Proves that `AdminClient::delete_records` moves a partition's log start
//! offset on a real broker.
//!
//! The suite writes 100 records to one partition, asks the broker to delete
//! the records below offset 50, and compares the whole
//! [`DeleteRecordsOutcome`] the client builds from the response. It then
//! fetches from offset 0 on a separate connection. The broker must answer
//! `OFFSET_OUT_OF_RANGE` (error code 1), because offset 0 is now below the log
//! start. That last step proves the deletion reached the log, not only the
//! response record.
//!
//! The broker is `confluentinc/cp-kafka:6.1.1` in Docker, so the case carries
//! `#[ignore]`. Run it with Docker present:
//!
//! ```text
//! cargo test -p krabka-client-admin --test delete_records -- --ignored --nocapture
//! ```

mod support;

use assert2::assert;
use krabka_client_admin::{AdminClient, DeleteRecordsOp, DeleteRecordsOutcome};
use krabka_client_core::{ClientError, Connection, ConnectionOptions, fetch_partition};
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;

/// Number of records written before the deletion.
const RECORD_COUNT: i64 = 100;
/// The first offset that stays readable after the deletion.
const DELETE_BEFORE_OFFSET: i64 = 50;
/// Kafka's `OFFSET_OUT_OF_RANGE`.
const OFFSET_OUT_OF_RANGE: i16 = 1;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn delete_records_truncates_log_and_maps_outcome() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let mut admin = support::admin(&kafka.bootstrap).await;

    let topic = support::unique("wal");
    support::create_topic(&mut admin, &topic, 1).await;
    produce_frames(&kafka.bootstrap, &topic).await;

    let outcomes = admin
        .delete_records(
            &[DeleteRecordsOp {
                topic: topic.clone(),
                partition: 0,
                offset: DELETE_BEFORE_OFFSET,
            }],
            krabka_units::secs(30),
        )
        .await
        .expect("delete_records");

    assert!(
        outcomes
            == vec![DeleteRecordsOutcome {
                topic: topic.clone(),
                partition: 0,
                error_code: 0,
                low_watermark: DELETE_BEFORE_OFFSET,
            }]
    );

    let topic_id = topic_id_for(&mut admin, &topic).await;
    let reader = connect_reader(&kafka.bootstrap).await;
    let fetch_error = fetch_partition(
        &reader,
        &topic,
        topic_id,
        0,
        0,
        krabka_units::millis(500),
        krabka_units::mebibytes(1),
    )
    .await
    .expect_err("a fetch below the log start must fail");

    assert!(
        matches!(
            fetch_error,
            ClientError::Server {
                error_code: OFFSET_OUT_OF_RANGE
            }
        ),
        "fetch below the log start must return OFFSET_OUT_OF_RANGE, got {fetch_error:?}"
    );

    drop(kafka);
}

/// Write [`RECORD_COUNT`] records to partition 0 and wait for every ack.
async fn produce_frames(bootstrap: &str, topic: &str) {
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .expect("producer build");

    for offset in 0..RECORD_COUNT {
        producer
            .send(ProducerRecord {
                topic: topic.to_owned(),
                partition: Some(0),
                value: Some(format!("frame-{offset}").into_bytes().into()),
                ..Default::default()
            })
            .await
            .await
            .expect("producer ack channel closed")
            .expect("produce frame");
    }

    producer.flush().await.expect("flush");
}

/// Read the topic id from Metadata.
///
/// Kafka adds the topic id to the Metadata response at v10. A broker below
/// that release reports no id, and the fetch then addresses the partition by
/// name. [`WireUuid::ZERO`] is the wire value for "no topic id".
async fn topic_id_for(admin: &mut AdminClient, topic: &str) -> WireUuid {
    admin
        .metadata(&[topic])
        .await
        .expect("metadata")
        .topics
        .into_iter()
        .find(|entry| entry.name == topic)
        .and_then(|entry| entry.topic_id)
        .map_or(WireUuid::ZERO, |id| WireUuid(*id.as_bytes()))
}

/// Open a second connection, which the fetch uses.
///
/// The admin client points its connection at the controller. The fetch needs a
/// plain broker connection with its own client id, so the broker log names the
/// reader.
async fn connect_reader(bootstrap: &str) -> Connection {
    let addr = tokio::net::lookup_host(bootstrap)
        .await
        .expect("resolve bootstrap")
        .next()
        .expect("bootstrap address");
    Connection::connect_with_options(
        addr,
        ConnectionOptions {
            client_id: "delete-records-test-reader".to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("connect reader")
}
