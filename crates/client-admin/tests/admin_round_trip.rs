//! End-to-end test of the topic admin RPCs against a broker.
//!
//! The suite starts a `confluentinc/cp-kafka:6.1.1` container with the
//! `tests/support` harness. It drives the topic RPCs through `AdminClient` and
//! then reads the cluster state back.
//!
//! # Coverage map for the `NOT_CONTROLLER` retry
//!
//! The full retry pipeline has four steps. The first response carries
//! `NOT_CONTROLLER` (41). The admin client sends a fresh `Metadata` request.
//! The client reconnects to the reported controller. The client sends the
//! original RPC again.
//!
//! A unit test cannot drive this pipeline through `AdminClient`, because
//! `AdminClient` holds a concrete `krabka_client_core::Connection` and there is
//! no trait seam at the byte layer. A Kafka-protocol fake server for three RPCs
//! is more code than the retry itself.
//!
//! The tests split the coverage into three parts instead:
//!
//! * **Predicate**. `src/topics.rs::tests::any_not_controller_predicate_matches_code_41`
//!   and `src/topics.rs::tests::any_not_controller_ignores_other_errors` lock
//!   the retry-eligibility check to code 41 only.
//! * **Endpoint resolver**.
//!   `src/topics.rs::tests::controller_endpoint_picks_broker_with_matching_node_id`,
//!   `src/topics.rs::tests::controller_endpoint_returns_none_when_no_match` and
//!   `src/topics.rs::tests::controller_endpoint_rejects_non_dialable_ephemeral_port`
//!   lock the mapping from `MetadataResponse` to the `host:port` that the retry
//!   reconnects to.
//! * **Pipeline**. `admin_round_trip_create_alter_delete` in *this file* runs
//!   the happy path against a broker. The container holds one broker, so that
//!   broker is always the controller and the retry does not run here. The code
//!   path that retries on `NOT_CONTROLLER` is the same path that succeeds with
//!   no retry when the response is clean. The case therefore covers the
//!   integration path through `parse_create_topics`, `parse_delete_topics` and
//!   `parse_create_partitions`.

mod support;

use std::{collections::BTreeMap, time::Duration};

use assert2::assert;
use krabka_client_admin::{
    AdminClient, AlterConfigsOutcome, CreatePartitionsOp, CreatePartitionsOutcome,
    CreateTopicOutcome, CreateTopicSpec, DeleteTopicOutcome, IncrementalAlterOp,
    TopicConfigOverrides, TopicMetadataEntry,
};

/// Bound on every read-back loop. The broker applies a change and then makes
/// it visible in its own time.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Pause between two read-backs.
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Timeout that the broker gets to complete one create or delete RPC.
const RPC_TIMEOUT_SECS: u32 = 30;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn admin_round_trip_create_alter_delete() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let mut admin = support::admin(&kafka.bootstrap).await;
    let topic = support::unique("foo");

    // One container start is expensive, so the steps share one case and one
    // client. Each step is its own function to keep every function short.
    assert_topic_absent(&mut admin, &topic).await;
    create_topic_with_override(&mut admin, &topic).await;
    expand_partitions(&mut admin, &topic).await;
    read_create_override(&mut admin, &topic).await;
    set_override(&mut admin, &topic).await;
    delete_override(&mut admin, &topic).await;
    delete_topic(&mut admin, &topic).await;
}

/// Step 1. The topic does not exist yet.
async fn assert_topic_absent(admin: &mut AdminClient, topic: &str) {
    let md = admin.metadata(&[topic]).await.expect("metadata");
    let entry = md
        .topics
        .into_iter()
        .find(|t| t.name == topic)
        .expect("metadata names every requested topic");
    assert!(entry.error.is_some(), "{entry:?}");
}

/// Step 2. Create the topic with one config override, then read the metadata
/// back.
async fn create_topic_with_override(admin: &mut AdminClient, topic: &str) {
    let configs = BTreeMap::from([("retention.ms".to_owned(), "60000".to_owned())]);
    let outcomes = admin
        .create_topics(
            &[CreateTopicSpec {
                name: topic.to_owned(),
                partitions: 3,
                replicas: 1,
                configs,
            }],
            krabka_units::secs(RPC_TIMEOUT_SECS),
        )
        .await
        .expect("create_topics");
    assert!(
        outcomes
            == vec![CreateTopicOutcome {
                name: topic.to_owned(),
                topic_id: None,
                error: None,
            }]
    );

    let entry = await_topic(admin, topic, "the topic has three partitions", |e| {
        e.is_some_and(|t| t.error.is_none() && t.partition_count == 3)
    })
    .await;
    assert!(
        entry
            == Some(TopicMetadataEntry {
                name: topic.to_owned(),
                topic_id: None,
                partition_count: 3,
                replication_factor: 1,
                error: None,
            })
    );
}

/// Step 3. Raise the partition count from three to five.
async fn expand_partitions(admin: &mut AdminClient, topic: &str) {
    let outcomes = admin
        .create_partitions(
            &[CreatePartitionsOp {
                name: topic.to_owned(),
                new_total_count: 5,
            }],
            krabka_units::secs(RPC_TIMEOUT_SECS),
        )
        .await
        .expect("create_partitions");
    assert!(
        outcomes
            == vec![CreatePartitionsOutcome {
                name: topic.to_owned(),
                error: None,
            }]
    );

    let entry = await_topic(admin, topic, "the topic has five partitions", |e| {
        e.is_some_and(|t| t.partition_count == 5)
    })
    .await;
    assert!(
        entry
            == Some(TopicMetadataEntry {
                name: topic.to_owned(),
                topic_id: None,
                partition_count: 5,
                replication_factor: 1,
                error: None,
            })
    );
}

/// Step 4. `describe_configs` reports the create-time override as a dynamic
/// topic override.
async fn read_create_override(admin: &mut AdminClient, topic: &str) {
    let overrides = await_overrides(admin, topic, "retention.ms is 60000", |o| {
        override_is(o, "retention.ms", "60000")
    })
    .await;
    assert!(overrides == vec![expected_overrides(topic, &[("retention.ms", "60000")])]);
}

/// Step 5. `IncrementalAlterConfigs` sets a second key.
async fn set_override(admin: &mut AdminClient, topic: &str) {
    let outcomes = admin
        .incremental_alter_configs(&[IncrementalAlterOp::Set {
            topic: topic.to_owned(),
            key: "cleanup.policy".to_owned(),
            value: "compact".to_owned(),
        }])
        .await
        .expect("incremental_alter_configs set");
    assert!(outcomes == vec![alter_ok(topic)]);

    let overrides = await_overrides(admin, topic, "cleanup.policy is compact", |o| {
        override_is(o, "cleanup.policy", "compact")
    })
    .await;
    assert!(
        overrides
            == vec![expected_overrides(
                topic,
                &[("cleanup.policy", "compact"), ("retention.ms", "60000")],
            )]
    );
}

/// Step 6. `IncrementalAlterConfigs` deletes the create-time override. The
/// second key stays.
async fn delete_override(admin: &mut AdminClient, topic: &str) {
    let outcomes = admin
        .incremental_alter_configs(&[IncrementalAlterOp::Delete {
            topic: topic.to_owned(),
            key: "retention.ms".to_owned(),
        }])
        .await
        .expect("incremental_alter_configs delete");
    assert!(outcomes == vec![alter_ok(topic)]);

    let overrides = await_overrides(admin, topic, "retention.ms is gone", |o| {
        !o.iter().any(|t| t.overrides.contains_key("retention.ms"))
            && override_is(o, "cleanup.policy", "compact")
    })
    .await;
    assert!(overrides == vec![expected_overrides(topic, &[("cleanup.policy", "compact")])]);
}

/// Step 7. Delete the topic. The metadata then reports the topic as absent or
/// as error-marked.
async fn delete_topic(admin: &mut AdminClient, topic: &str) {
    let outcomes = admin
        .delete_topics(&[topic], krabka_units::secs(RPC_TIMEOUT_SECS))
        .await
        .expect("delete_topics");
    assert!(
        outcomes
            == vec![DeleteTopicOutcome {
                name: topic.to_owned(),
                error: None,
            }]
    );

    let entry = await_topic(admin, topic, "the topic is no longer live", |e| {
        e.is_none_or(|t| t.error.is_some())
    })
    .await;
    if let Some(entry) = entry {
        assert!(entry.error.is_some(), "{entry:?}");
    }
}

/// Read the metadata entry for `topic` until `ready` accepts it.
///
/// The pause between two reads is a real-time wait on a separate process, not
/// a poll of progress that this task makes. The broker applies the change in
/// its own time and gives this side no signal to await.
async fn await_topic(
    admin: &mut AdminClient,
    topic: &str,
    what: &str,
    ready: impl Fn(Option<&TopicMetadataEntry>) -> bool,
) -> Option<TopicMetadataEntry> {
    tokio::time::timeout(SETTLE_TIMEOUT, async {
        loop {
            let md = admin.metadata(&[topic]).await.expect("metadata");
            let entry = md.topics.into_iter().find(|t| t.name == topic);
            if ready(entry.as_ref()) {
                break entry;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the broker did not report that {what} within {SETTLE_TIMEOUT:?}"))
}

/// Read the dynamic config overrides of `topic` until `ready` accepts them.
///
/// The pause between two reads is a real-time wait on a separate process, as
/// in [`await_topic`].
async fn await_overrides(
    admin: &mut AdminClient,
    topic: &str,
    what: &str,
    ready: impl Fn(&[TopicConfigOverrides]) -> bool,
) -> Vec<TopicConfigOverrides> {
    tokio::time::timeout(SETTLE_TIMEOUT, async {
        loop {
            let overrides = admin
                .describe_configs(&[topic])
                .await
                .expect("describe_configs");
            if ready(&overrides) {
                break overrides;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the broker did not report that {what} within {SETTLE_TIMEOUT:?}"))
}

fn override_is(overrides: &[TopicConfigOverrides], key: &str, value: &str) -> bool {
    overrides
        .iter()
        .any(|t| t.overrides.get(key).map(String::as_str) == Some(value))
}

fn expected_overrides(topic: &str, entries: &[(&str, &str)]) -> TopicConfigOverrides {
    TopicConfigOverrides {
        topic: topic.to_owned(),
        overrides: entries
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
    }
}

fn alter_ok(topic: &str) -> AlterConfigsOutcome {
    AlterConfigsOutcome {
        topic: topic.to_owned(),
        error: None,
    }
}
