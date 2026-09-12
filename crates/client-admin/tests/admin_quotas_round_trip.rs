//! End-to-end test of the client-quota admin RPCs.
//!
//! The suite drives `DescribeClientQuotas` (`api_key` 48) and
//! `AlterClientQuotas` (`api_key` 49) against a `confluentinc/cp-kafka:6.1.1`
//! container. KIP-546 added both RPCs in Kafka 2.6, so the 2.7 broker in that
//! image serves them.
//!
//! The pipeline matches the quota path of the operator's `KafkaUser` reconcile:
//! read the current per-user state, diff it, write the `(set, remove)` ops that
//! the diff gives, then read the state back.
//!
//! Both cases need the container, so both carry `#[ignore]`.

mod support;

use std::time::Duration;

use assert2::{assert, check};
use krabka_client_admin::{AdminClient, QuotaOp, UserQuotaConfig, diff_user_quotas};

/// Bound on a read-back loop. The broker writes a quota change and then makes
/// it visible to `DescribeClientQuotas` in its own time.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Pause between two read-backs.
const POLL_INTERVAL: Duration = Duration::from_millis(200);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn user_quotas_set_change_remove() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let mut admin = support::admin(&kafka.bootstrap).await;

    let user = support::unique("alice");

    // 1. The user has no quotas at the start.
    let initial = admin
        .describe_user_quotas(&user)
        .await
        .expect("describe_user_quotas");
    assert!(initial.is_empty());

    // 2. Set the producer rate and the request percentage. `validate_only` is
    // false, so the broker writes the change.
    let outcome = admin
        .alter_user_quotas(
            &user,
            &[
                QuotaOp::Set {
                    key: "producer_byte_rate".to_owned(),
                    value: 1_048_576.0,
                },
                QuotaOp::Set {
                    key: "request_percentage".to_owned(),
                    value: 25.0,
                },
            ],
            false,
        )
        .await
        .expect("alter_user_quotas set");
    assert!(outcome.is_none());

    let after_set = await_user_quotas(
        &mut admin,
        &user,
        &quotas(&[
            ("producer_byte_rate", 1_048_576.0),
            ("request_percentage", 25.0),
        ]),
    )
    .await;
    assert!(after_set.len() == 2);
    check!((after_set["producer_byte_rate"] - 1_048_576.0).abs() < f64::EPSILON);
    check!((after_set["request_percentage"] - 25.0).abs() < f64::EPSILON);

    // 3. A diff against the same desired state gives no ops.
    let same = after_set.clone();
    let ops = diff_user_quotas(&after_set, &same);
    assert!(ops.is_empty());

    // 4. Change the producer rate and drop the request percentage. The diff
    // gives one Set and one Remove. Apply both and read the state back.
    let desired = quotas(&[("producer_byte_rate", 2_097_152.0)]);
    let ops = diff_user_quotas(&after_set, &desired);
    assert!(ops.len() == 2);
    let outcome = admin
        .alter_user_quotas(&user, &ops, false)
        .await
        .expect("alter_user_quotas drift");
    assert!(outcome.is_none());

    let after_drift = await_user_quotas(&mut admin, &user, &desired).await;
    assert!(after_drift.len() == 1);
    check!((after_drift["producer_byte_rate"] - 2_097_152.0).abs() < f64::EPSILON);

    // 5. Remove the remaining key. The read-back is then empty.
    let outcome = admin
        .alter_user_quotas(
            &user,
            &[QuotaOp::Remove {
                key: "producer_byte_rate".to_owned(),
            }],
            false,
        )
        .await
        .expect("alter_user_quotas remove");
    assert!(outcome.is_none());

    let final_state = await_user_quotas(&mut admin, &user, &UserQuotaConfig::new()).await;
    assert!(final_state.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn validate_only_does_not_persist() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let mut admin = support::admin(&kafka.bootstrap).await;

    let user = support::unique("bob");

    let outcome = admin
        .alter_user_quotas(
            &user,
            &[QuotaOp::Set {
                key: "producer_byte_rate".to_owned(),
                value: 1.0,
            }],
            true, // validate_only
        )
        .await
        .expect("alter_user_quotas validate only");
    assert!(outcome.is_none());

    // The broker wrote nothing, so no read-back loop is needed here. A loop
    // would only hide a write that the broker made.
    let after = admin
        .describe_user_quotas(&user)
        .await
        .expect("describe_user_quotas");
    assert!(after.is_empty());
}

/// Build a quota map from key and value pairs.
fn quotas(entries: &[(&str, f64)]) -> UserQuotaConfig {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_owned(), *value))
        .collect()
}

/// Read the user's quotas until the broker reports `expected`.
///
/// The pause between two reads is a real-time wait on a separate process, not
/// a poll of progress that this task makes. The broker writes the change and
/// then refreshes its own quota cache, and it gives this side no signal to
/// await.
async fn await_user_quotas(
    admin: &mut AdminClient,
    user: &str,
    expected: &UserQuotaConfig,
) -> UserQuotaConfig {
    tokio::time::timeout(SETTLE_TIMEOUT, async {
        loop {
            let actual = admin
                .describe_user_quotas(user)
                .await
                .expect("describe_user_quotas");
            if quotas_match(&actual, expected) {
                break actual;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("the broker did not report the quotas {expected:?} within {SETTLE_TIMEOUT:?}")
    })
}

/// Compare two quota maps. Each value compares within `f64::EPSILON`, which is
/// how the cases above compare a single value.
fn quotas_match(actual: &UserQuotaConfig, expected: &UserQuotaConfig) -> bool {
    actual.len() == expected.len()
        && expected.iter().all(|(key, want)| {
            actual
                .get(key)
                .is_some_and(|got| (got - want).abs() < f64::EPSILON)
        })
}
