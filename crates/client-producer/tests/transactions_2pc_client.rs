//! Native client coverage for KIP-939 prepare/recovery and forced
//! termination.
//!
//! The suite proves three client behaviours of two-phase commit:
//!
//! 1. A producer prepares a transaction, closes, and a new producer with the
//!    same transactional id recovers it with
//!    `init_transactions_with_keep_prepared(true)`. A `complete_transaction`
//!    call with the matching [`PreparedTransactionState`] commits it, and the
//!    coordinator reports `CompleteCommit`.
//! 2. The same recovery with a state that does not match aborts instead, and
//!    the coordinator reports `CompleteAbort`.
//! 3. `AdminClient::force_terminate_transaction` clears a prepared
//!    transaction that no client comes back for. The coordinator first aborts
//!    the open generation, then installs a new fenced generation in `Empty`.
//!
//! The prepared state also goes through `Display` and `FromStr`, because a
//! real external transaction manager writes that string to its own log and
//! reads it back after a restart.
//!
//! # The pinned container image cannot run this suite
//!
//! `tests/support/mod.rs` starts `confluentinc/cp-kafka:6.1.1`, which is
//! Apache Kafka 2.7. That release has no KIP-939 two-phase commit and no
//! `transaction.version` feature, so it has no feature level 3, no
//! `InitProducerId` `keepPreparedTxn` flag, and no prepared transaction state.
//! The case therefore carries
//! `#[ignore = "requires Docker and a broker with KIP-939 two-phase commit"]`
//! and no CI job runs it today.
//!
//! A runner for this suite needs all of the following:
//!
//! - A broker built from Apache Kafka 4.0 or later, in KRaft mode.
//! - The broker property `transaction.two.phase.commit.enable=true`.
//! - The `transaction.version` feature raised to level 3. The case does that
//!   step itself with `UpdateFeaturesRequest`, which is the same path a client
//!   or an operator tool uses.
//!
//! To run the suite, point `tests/support/mod.rs` at such an image and give
//! the case its own bootstrap address.

mod support;

use std::time::Duration;

use bytes::Bytes;
use krabka_client_admin::AdminClient;
use krabka_client_core::Client;
use krabka_client_producer::{PreparedTransactionState, Producer, ProducerRecord};
use krabka_protocol::owned::{
    describe_transactions_request::DescribeTransactionsRequest,
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

/// Feature level that carries KIP-939 two-phase commit.
const TRANSACTION_VERSION_TWO_PHASE_COMMIT: i16 = 3;
/// `FeatureUpdateKey.upgrade_type` value for an upgrade.
const UPGRADE_TYPE_UPGRADE: i8 = 1;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and a broker with KIP-939 two-phase commit"]
async fn prepare_recovery_commit_abort_and_admin_termination() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let observer = support::bootstrap_client(&kafka.bootstrap).await;
    enable_two_phase_commit(&observer).await;

    let topic = support::unique("two-pc-client");
    support::create_topic(&observer, &topic).await;

    recover_and_commit(&kafka.bootstrap, &topic, &observer).await;
    recover_and_abort(&kafka.bootstrap, &topic, &observer).await;
    force_terminate(&kafka.bootstrap, &topic, &observer).await;

    observer.close();
}

/// Raise `transaction.version` to the level that carries two-phase commit.
///
/// The broker also needs `transaction.two.phase.commit.enable=true` in its own
/// configuration. This step is the client-side half, and it is the same
/// request `kafka-features` sends.
async fn enable_two_phase_commit(client: &Client) {
    let response = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "transaction.version".to_owned(),
                max_version_level: TRANSACTION_VERSION_TWO_PHASE_COMMIT,
                upgrade_type: UPGRADE_TYPE_UPGRADE,
                ..Default::default()
            }],
            timeout_ms: 30_000,
            ..Default::default()
        })
        .await
        .expect("enable transaction.version 3");
    assert2::assert!(response.error_code == 0, "{response:?}");
}

/// Prepare a transaction, recover it in a second producer, and commit it.
async fn recover_and_commit(bootstrap: &str, topic: &str, observer: &Client) {
    let transactional_id = support::unique("two-pc-recover-commit");
    let prepared = prepare_record(bootstrap, &transactional_id, topic, b"commit").await;
    // An external transaction manager keeps the prepared state as text. The
    // round-trip proves the recovered producer can commit from the text form.
    let persisted = prepared
        .to_string()
        .parse::<PreparedTransactionState>()
        .expect("persisted prepared state round-trips");

    let recovery = producer(bootstrap, &transactional_id).await;
    recovery
        .init_transactions_with_keep_prepared(true)
        .await
        .expect("recover prepared transaction for commit");
    recovery
        .complete_transaction(persisted)
        .await
        .expect("matching state commits");

    assert2::assert!(transaction_state(observer, &transactional_id).await == "CompleteCommit");
    recovery.close().await.expect("close commit recovery");
}

/// Prepare a transaction, recover it, and complete it with a state that does
/// not match. The coordinator must abort.
async fn recover_and_abort(bootstrap: &str, topic: &str, observer: &Client) {
    let transactional_id = support::unique("two-pc-recover-abort");
    let _prepared = prepare_record(bootstrap, &transactional_id, topic, b"abort").await;

    let recovery = producer(bootstrap, &transactional_id).await;
    recovery
        .init_transactions_with_keep_prepared(true)
        .await
        .expect("recover prepared transaction for abort");
    recovery
        .complete_transaction(PreparedTransactionState::default())
        .await
        .expect("mismatched state aborts");

    assert2::assert!(transaction_state(observer, &transactional_id).await == "CompleteAbort");
    recovery.close().await.expect("close abort recovery");
}

/// Prepare a transaction that no client comes back for, then clear it with the
/// admin client.
async fn force_terminate(bootstrap: &str, topic: &str, observer: &Client) {
    let transactional_id = support::unique("two-pc-admin-terminate");
    let _prepared = prepare_record(bootstrap, &transactional_id, topic, b"terminate").await;
    assert2::assert!(transaction_state(observer, &transactional_id).await == "Ongoing");

    let bootstrap_addrs = [bootstrap.to_owned()];
    let admin = AdminClient::connect(&bootstrap_addrs)
        .await
        .expect("admin connects");
    admin
        .force_terminate_transaction(&transactional_id)
        .await
        .expect("force terminate transaction");

    // InitProducerId first aborts the ongoing generation, then installs a new
    // fenced generation in Empty state.
    assert2::assert!(transaction_state(observer, &transactional_id).await == "Empty");
}

/// Build a two-phase-commit producer for `transactional_id`.
///
/// `transaction_two_phase_commit_enable(true)` forbids a transaction timeout,
/// because the external transaction manager owns the decision to commit or to
/// abort. The coordinator must not time the transaction out on its own.
async fn producer(bootstrap: &str, transactional_id: &str) -> Producer {
    Producer::builder()
        .bootstrap(bootstrap.to_owned())
        .transactional_id(transactional_id.to_owned())
        .transaction_two_phase_commit_enable(true)
        .linger(Duration::ZERO)
        .build()
        .await
        .expect("2PC producer connects")
}

/// Write one record inside a transaction, prepare the transaction, and close
/// the producer without a commit or an abort.
///
/// This leaves the transaction open on the coordinator, which is what a real
/// external transaction manager does between its prepare phase and its commit
/// phase.
async fn prepare_record(
    bootstrap: &str,
    transactional_id: &str,
    topic: &str,
    value: &'static [u8],
) -> PreparedTransactionState {
    let producer = producer(bootstrap, transactional_id).await;
    producer
        .init_transactions()
        .await
        .expect("initialize 2PC producer");
    let transaction = producer
        .begin_transaction()
        .await
        .expect("begin transaction");
    producer
        .send(ProducerRecord {
            topic: topic.to_owned(),
            partition: Some(0),
            value: Some(Bytes::from_static(value)),
            ..Default::default()
        })
        .await
        .await
        .expect("producer acknowledgement channel")
        .expect("transactional produce");
    let prepared = transaction.prepare().await.expect("prepare transaction");
    drop(transaction);
    producer.close().await.expect("close prepared producer");
    prepared
}

/// Read the coordinator's view of `transactional_id`.
async fn transaction_state(client: &Client, transactional_id: &str) -> String {
    let response = client
        .send(DescribeTransactionsRequest {
            transactional_ids: vec![transactional_id.to_owned()],
            ..Default::default()
        })
        .await
        .expect("describe transaction");
    let state = &response.transaction_states[0];
    assert2::assert!(state.error_code == 0, "{state:?}");
    state.transaction_state.clone()
}
