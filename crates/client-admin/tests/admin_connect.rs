//! Bootstrap-walk test for `AdminClient::connect`.
//!
//! The first entry of the bootstrap list refuses TCP connects. `connect` must
//! go on to the second entry and give back a usable client. The suite proves
//! that the walk happens and that the client the walk returns round-trips a
//! request.
//!
//! This suite adds to the predicate-level unit tests in
//! `crates/client-admin/src/topics.rs`. Those tests are
//! `controller_endpoint_picks_broker_with_matching_node_id` and
//! `any_not_controller_predicate_matches_code_41`. They lock the pure parts of
//! the `NOT_CONTROLLER` retry path. `tests/admin_round_trip.rs` covers the full
//! pipeline against a broker: response, then reconnect, then resend.
//!
//! The broker is a `confluentinc/cp-kafka:6.1.1` container, so the case carries
//! `#[ignore]`.

mod support;

use std::time::Duration;

use assert2::assert;
use krabka_client_admin::{AdminClient, KafkaError, TopicMetadataEntry};
use tokio::net::TcpListener;

/// Maximum attempts for `connect` while the broker still warms up. With a 1s
/// pause between attempts this gives ~15s of tolerance.
const CONNECT_MAX_ATTEMPTS: u32 = 15;
const CONNECT_RETRY_DELAY: Duration = Duration::from_secs(1);

/// The wire code and name that a broker reports for a topic it does not hold.
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

/// Spec test: `connect_walks_bootstrap_list`.
///
/// The case binds an ephemeral port and drops the listener. Connects to that
/// address get `ECONNREFUSED`. The case then starts the broker container and
/// uses its address as the second bootstrap entry. `AdminClient::connect` must
/// skip the refused address and succeed against the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn connect_walks_bootstrap_list() {
    support::init_tracing();

    // First bootstrap entry: an address whose connects get `ECONNREFUSED`.
    let refused = refused_address().await;

    // Second bootstrap entry: the broker container.
    let kafka = support::start_kafka().await;

    // `support::admin` is not used here. `connect` itself is what the case
    // proves, so the case calls it directly and keeps the warm-up retry local.
    let mut admin = connect_with_retry(&[refused, kafka.bootstrap.clone()]).await;

    // A metadata request confirms the client the walk returned is usable. The
    // admin client sends `allow_auto_topic_creation: false`, so the broker
    // reports the unknown topic and creates nothing.
    let topic = support::unique("nonexistent");
    let md = admin
        .metadata(&[topic.as_str()])
        .await
        .expect("metadata request against the second bootstrap");
    let entry = md.topics.into_iter().find(|t| t.name == topic);
    assert!(
        entry
            == Some(TopicMetadataEntry {
                name: topic.clone(),
                topic_id: None,
                partition_count: 0,
                replication_factor: 0,
                error: Some(KafkaError {
                    code: UNKNOWN_TOPIC_OR_PARTITION,
                    name: "UNKNOWN_TOPIC_OR_PARTITION",
                    message: None,
                }),
            })
    );
}

/// An address that refuses every connect.
///
/// The listener binds an ephemeral port and then goes out of scope. The kernel
/// keeps the port free, so a connect to it fails with `ECONNREFUSED`.
async fn refused_address() -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind an ephemeral port");
    let addr = listener.local_addr().expect("read the bound address");
    drop(listener);
    addr.to_string()
}

/// Call `AdminClient::connect` until the broker accepts the connection.
///
/// The container is ready before the broker finishes controller election, so
/// the first attempt can fail on a broker that is still warming up. The retry
/// lives here, not in `tests/support/mod.rs`, because this case must call
/// `connect` with its own two-entry bootstrap list.
async fn connect_with_retry(bootstrap: &[String]) -> AdminClient {
    for attempt in 1..=CONNECT_MAX_ATTEMPTS {
        match AdminClient::connect(bootstrap).await {
            Ok(admin) => return admin,
            Err(e) if attempt < CONNECT_MAX_ATTEMPTS => {
                tracing::warn!(attempt, "connect failed while the broker warms up: {e}");
                // A real-time wait on a separate process. The broker publishes
                // no readiness signal that this side can await.
                tokio::time::sleep(CONNECT_RETRY_DELAY).await;
            }
            Err(e) => panic!(
                "the second bootstrap entry must succeed although the first refuses, but connect failed after {CONNECT_MAX_ATTEMPTS} attempts: {e}"
            ),
        }
    }
    unreachable!("the loop above either returns or panics")
}
