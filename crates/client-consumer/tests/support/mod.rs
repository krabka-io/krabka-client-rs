//! Shared helpers for the container-driven consumer suites.
//!
//! These suites came from the monorepo, where they booted a Krabka broker in
//! the same process. That broker now lives in another repository, so the
//! suites run against a real Kafka in Docker instead. The container seam is
//! the one `crates/client-core/tests/integration.rs` already uses: the
//! `confluent` module of `testcontainers-modules`, which starts
//! `confluentinc/cp-kafka:6.1.1`.
//!
//! Every case that needs the container carries `#[ignore]`, so
//! `cargo test --workspace` and `bazel test //...` compile the suite and skip
//! the case. Run one suite with Docker present:
//!
//! ```text
//! cargo test -p krabka-client-consumer --test consumer_integration -- --ignored --nocapture
//! ```
//!
//! Cargo treats `tests/support/mod.rs` as a submodule rather than as its own
//! test binary, and `//bazel:defs.bzl` globs it into the sources of every
//! integration test in this package.

// A suite declares `mod support;` and uses the helpers it needs. The rest are
// dead in that binary. This is a rustc lint on a test-only helper module, not
// a Clippy suppression.
#![allow(dead_code)]

use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use bytes::Bytes;
use krabka_client_core::{Client, ClientError};
use krabka_protocol::{
    owned::{
        api_versions_request::ApiVersionsRequest,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use testcontainers::{ContainerAsync, ContainerRequest, runners::AsyncRunner};
// `testcontainers_modules::kafka` re-exports `confluent::*`, so the bare
// `Kafka` here is the Confluent module's container type.
use testcontainers_modules::kafka::{KAFKA_PORT, Kafka};

/// The deadline for a container to start, which includes the image pull.
pub const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);
/// Maximum attempts for the initial round-trip while the broker still warms
/// up. With a 1s pause between attempts this gives ~15s of tolerance.
pub const BOOTSTRAP_MAX_ATTEMPTS: u32 = 15;
pub const BOOTSTRAP_RETRY_DELAY: Duration = Duration::from_secs(1);
/// Attempts for a produce that races `CreateTopics` metadata propagation.
pub const PRODUCE_MAX_ATTEMPTS: u32 = 10;
/// Kafka error code `UNKNOWN_TOPIC_OR_PARTITION`.
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

/// Initialise a per-test tracing subscriber so `--nocapture` runs surface
/// client-side connection and dispatch logs. It is safe to call this many
/// times.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("krabka_client_consumer=debug,krabka_client_core=debug,info")
        .with_test_writer()
        .try_init();
}

/// A name no other case in the same binary uses.
pub fn unique(prefix: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{n}", std::process::id())
}

/// A running Kafka container and the address a client bootstraps from.
pub struct KafkaBroker {
    pub container: ContainerAsync<Kafka>,
    pub bootstrap: String,
}

impl KafkaBroker {
    /// Stop the broker process and start it again on the same volumes.
    ///
    /// `ContainerAsync::start` re-runs the module's `exec_after_start`, which
    /// re-publishes `advertised.listeners` for the mapped host port. Docker
    /// keeps the published port across a stop, so the bootstrap address is
    /// re-read rather than assumed.
    pub async fn restart(&mut self) {
        self.container
            .stop_with_timeout(Some(30))
            .await
            .expect("stop kafka container");
        self.container
            .start()
            .await
            .expect("restart kafka container");
        self.bootstrap = mapped_bootstrap(&self.container).await;
    }
}

async fn mapped_bootstrap(container: &ContainerAsync<Kafka>) -> String {
    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("mapped kafka port");
    format!("127.0.0.1:{port}")
}

/// Start a Kafka container and return the handle with its bootstrap address.
pub async fn start_kafka() -> KafkaBroker {
    start_kafka_with(Kafka::default().into()).await
}

/// Start a Kafka container built from a caller-configured image.
pub async fn start_kafka_with(image: ContainerRequest<Kafka>) -> KafkaBroker {
    let container = tokio::time::timeout(CONTAINER_START_TIMEOUT, image.start())
        .await
        .expect("kafka container start timed out")
        .expect("kafka container failed to start");
    let bootstrap = mapped_bootstrap(&container).await;
    KafkaBroker {
        container,
        bootstrap,
    }
}

/// Build a `Client` and drive the bootstrap `ApiVersions` round-trip with
/// retry on `ClientError::Disconnected`.
///
/// The Confluent module returns from `start()` while the broker still finishes
/// controller election and `ApiVersions` table construction. On a slow runner
/// the first RPC lands mid-bringup and the broker resets the TCP stream. On a
/// hard `Disconnected` the reader task has exited, so this rebuilds the client
/// on each attempt to get a fresh writer/reader pair.
pub async fn bootstrap_client(bootstrap: &str) -> Client {
    for attempt in 1..=BOOTSTRAP_MAX_ATTEMPTS {
        let client = Client::builder()
            .bootstrap(bootstrap)
            .client_id("krabka-consumer-integration")
            .build()
            .await
            .expect("client build failed");

        match client.send(ApiVersionsRequest::default()).await {
            Ok(_) => return client,
            Err(ClientError::Disconnected) => {
                client.close();
                warm_up_pause(attempt).await;
            }
            Err(e) => panic!("bootstrap ApiVersions failed with non-retryable error: {e}"),
        }
    }
    panic!("broker never accepted ApiVersions after {BOOTSTRAP_MAX_ATTEMPTS} attempts");
}

/// Real-time wait, not a progress poll: the broker is a separate process and
/// publishes no readiness signal this side can await.
async fn warm_up_pause(attempt: u32) {
    tracing::warn!(
        attempt,
        max = BOOTSTRAP_MAX_ATTEMPTS,
        "broker is still warming up; retrying"
    );
    tokio::time::sleep(BOOTSTRAP_RETRY_DELAY).await;
}

/// Create `name` with `partitions` partitions and replication factor 1.
pub async fn create_topic_with_partitions(client: &Client, name: &str, partitions: i32) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 30_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert2::assert!(resp.topics[0].error_code == 0, "{resp:?}");
}

/// Create `name` with one partition and replication factor 1.
pub async fn create_topic(client: &Client, name: &str) {
    create_topic_with_partitions(client, name, 1).await;
}

/// Resolve the topic UUID through a Metadata request.
///
/// Produce and Fetch at v >= 13 carry only `topic_id` on the wire. A broker
/// that predates KIP-516 reports the zero UUID, and the negotiated version is
/// then below 13, so the name still settles the routing.
pub async fn topic_id_for(client: &Client, name: &str) -> WireUuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

/// Build a `RecordBatch` with one entry per value.
pub fn record_batch_with_values(values: &[&str]) -> RecordBatch {
    let len_i32 = i32::try_from(values.len()).expect("test fixture small enough for i32");
    let len_i64 = i64::try_from(values.len()).expect("test fixture small enough for i64");
    let mut batch = RecordBatch {
        last_offset_delta: (len_i32 - 1).max(0),
        max_timestamp: len_i64,
        ..RecordBatch::default()
    };
    for (i, v) in values.iter().enumerate() {
        batch.records.push(Record {
            offset_delta: i32::try_from(i).expect("test fixture small enough for i32"),
            value: Some(Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
    batch
}

/// Produce `values` to partition 0 of `topic`.
pub async fn produce(client: &Client, topic: &str, values: &[&str]) {
    produce_to_partition(client, topic, 0, values).await;
}

/// Produce `values` to `partition` of `topic`.
///
/// The broker publishes the new partition to its metadata cache after
/// `CreateTopics` returns, so an immediate produce can still see
/// `UNKNOWN_TOPIC_OR_PARTITION`. The bounded retry covers that window.
pub async fn produce_to_partition(client: &Client, topic: &str, partition: i32, values: &[&str]) {
    let topic_id = topic_id_for(client, topic).await;
    for attempt in 1..=PRODUCE_MAX_ATTEMPTS {
        let resp = client
            .send(ProduceRequest {
                acks: 1,
                timeout_ms: 10_000,
                topic_data: vec![TopicProduceData {
                    name: topic.into(),
                    topic_id,
                    partition_data: vec![PartitionProduceData {
                        index: partition,
                        records: Some(record_batch_with_values(values).into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("produce");
        let err = resp.responses[0].partition_responses[0].error_code;
        if err == 0 {
            return;
        }
        assert2::assert!(
            err == UNKNOWN_TOPIC_OR_PARTITION && attempt < PRODUCE_MAX_ATTEMPTS,
            "produce failed after {attempt} attempt(s): {resp:?}"
        );
        // Real-time wait, not a progress poll: the metadata refresh happens in
        // the broker process and publishes no signal this side can await.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
