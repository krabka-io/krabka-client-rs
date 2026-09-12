//! KIP-932 share-group consumer suite.
//!
//! The cases prove that a [`ShareConsumer`]:
//!
//! - joins a share group, reports its member id and group id, and leaves the
//!   group on `close()`
//! - acquires records with `delivery_count == 1`, and lets the implicit
//!   auto-`Accept` move the share-partition start offset past them
//! - re-acquires a released record with an incremented `delivery_count`
//! - never gets a rejected record again after `commit()`
//! - shares one topic with a second member, where each member holds whole
//!   partitions and no record goes to both members
//! - extends an acquisition lock with `renew()` in explicit mode, and refuses
//!   `renew()` in implicit mode
//!
//! ## Why every case is ignored
//!
//! The container image that `support` pins is `confluentinc/cp-kafka:6.1.1`,
//! which is Apache Kafka 2.7. That release has no share groups. It answers
//! none of `ShareGroupHeartbeat`, `ShareFetch`, `ShareAcknowledge` or
//! `ShareGroupDescribe`, so the first heartbeat of `ShareConsumer::build()`
//! already fails. The suite is therefore compiled but not run, and every case
//! carries an `#[ignore]` that says so.
//!
//! To run the suite, point `support` at a broker with KIP-932 share groups
//! turned on. Such a broker needs:
//!
//! - Apache Kafka 4.1 or later with the `share.version=1` feature, or Apache
//!   Kafka 4.0 with `group.share.enable=true` and
//!   `unstable.api.versions.enable=true`
//! - a `group.coordinator.rebalance.protocols` list that includes `share`
//! - for `explicit_renew_prevents_redelivery` only, a one-second record lock:
//!   `group.share.record.lock.duration.ms=1000`, together with the lower bound
//!   `group.share.min.record.lock.duration.ms=1000`
//!
//! The broker creates the `__share_group_state` topic itself, so no case
//! bootstraps it.

mod support;

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use krabka_client_consumer::{
    ConsumerError, ShareAckMode, ShareAckType, ShareConsumer, ShareConsumerRecord,
};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        share_group_describe_request::ShareGroupDescribeRequest,
        share_group_describe_response::DescribedGroup,
    },
    primitives::uuid::Uuid as WireUuid,
};
use krabka_units::millis;

/// Bound for every settle loop in this suite.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a poll loop tries before it gives up on the wanted record count.
const POLL_BUDGET: Duration = Duration::from_secs(15);
/// Pause between two `ShareGroupDescribe` round trips.
const DESCRIBE_RETRY_DELAY: Duration = Duration::from_millis(200);
/// The broker-side acquisition lock that the renew case needs. The runner sets
/// it with `group.share.record.lock.duration.ms`.
const RECORD_LOCK: Duration = Duration::from_secs(1);

/// A member joins a share group and leaves it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and a broker with KIP-932 share groups"]
async fn share_consumer_joins_and_closes() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let admin = support::bootstrap_client(&kafka.bootstrap).await;

    let topic = support::unique("share-topic");
    let group = support::unique("share-group");
    support::create_topic(&admin, &topic).await;

    let mut consumer = share_consumer(
        &kafka.bootstrap,
        &group,
        "share-1",
        &topic,
        ShareAckMode::Implicit,
    )
    .await;

    assert2::assert!(!consumer.member_id().is_empty());
    assert2::assert!(consumer.group_id() == group);

    consumer.close().await.expect("close");
    drop(kafka);
}

/// Implicit mode: a poll acquires every record, and the auto-`Accept` moves the
/// share-partition start offset past them, so a later poll gets nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and a broker with KIP-932 share groups"]
async fn poll_acquires_and_implicit_accept_advances() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let admin = support::bootstrap_client(&kafka.bootstrap).await;

    let topic = support::unique("share-implicit");
    let group = support::unique("share-implicit-grp");
    support::create_topic(&admin, &topic).await;
    let topic_id = support::topic_id_for(&admin, &topic).await;
    support::produce(&admin, &topic, &["v0", "v1", "v2"]).await;

    let mut consumer = share_consumer(
        &kafka.bootstrap,
        &group,
        "share-1",
        &topic,
        ShareAckMode::Implicit,
    )
    .await;
    wait_for_coverage(&admin, &group, topic_id, &[0], 1).await;

    // First poll: acquire all three offsets, each on its first delivery.
    let first = poll_until(&mut consumer, 3, POLL_BUDGET).await;
    assert2::assert!(view(&first) == expected(&topic, 0, 0, 1, &["v0", "v1", "v2"]));

    // Second poll: the auto-`Accept` rides on this `ShareFetch` and moves the
    // start offset past 0..2, so nothing is acquired again. Poll a few times to
    // give the accept time to take effect.
    let mut second = 0usize;
    for _ in 0..5 {
        second += consumer.poll(millis(300)).await.expect("share poll").len();
    }
    assert2::assert!(second == 0);

    consumer.close().await.expect("close");
    drop(kafka);
}

/// Explicit mode: a `Release` makes the broker deliver the records again with
/// an incremented `delivery_count`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and a broker with KIP-932 share groups"]
async fn explicit_release_redelivers() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let admin = support::bootstrap_client(&kafka.bootstrap).await;

    let topic = support::unique("share-release");
    let group = support::unique("share-release-grp");
    support::create_topic(&admin, &topic).await;
    let topic_id = support::topic_id_for(&admin, &topic).await;
    support::produce(&admin, &topic, &["v0", "v1", "v2"]).await;

    let mut consumer = share_consumer(
        &kafka.bootstrap,
        &group,
        "rel-1",
        &topic,
        ShareAckMode::Explicit,
    )
    .await;
    wait_for_coverage(&admin, &group, topic_id, &[0], 1).await;

    let first = poll_until(&mut consumer, 3, POLL_BUDGET).await;
    assert2::assert!(view(&first) == expected(&topic, 0, 0, 1, &["v0", "v1", "v2"]));

    // Release every record back to the queue.
    for record in &first {
        consumer
            .acknowledge(record, ShareAckType::Release)
            .expect("release in explicit mode");
    }

    // The release rides on the next poll. The released offsets are then
    // acquired again with a higher delivery count.
    let second = poll_until(&mut consumer, 3, POLL_BUDGET).await;
    assert2::assert!(view(&second) == expected(&topic, 0, 0, 2, &["v0", "v1", "v2"]));

    consumer.close().await.expect("close");
    drop(kafka);
}

/// Explicit mode: a `Reject` plus a `commit()` archives the acquired offsets,
/// so only a record produced later comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and a broker with KIP-932 share groups"]
async fn explicit_reject_not_redelivered() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let admin = support::bootstrap_client(&kafka.bootstrap).await;

    let topic = support::unique("share-reject");
    let group = support::unique("share-reject-grp");
    support::create_topic(&admin, &topic).await;
    let topic_id = support::topic_id_for(&admin, &topic).await;
    support::produce(&admin, &topic, &["v0", "v1", "v2"]).await;

    let mut consumer = share_consumer(
        &kafka.bootstrap,
        &group,
        "rej-1",
        &topic,
        ShareAckMode::Explicit,
    )
    .await;
    wait_for_coverage(&admin, &group, topic_id, &[0], 1).await;

    let first = poll_until(&mut consumer, 3, POLL_BUDGET).await;
    assert2::assert!(view(&first) == expected(&topic, 0, 0, 1, &["v0", "v1", "v2"]));

    // Reject all three, then flush with a standalone `ShareAcknowledge`.
    for record in &first {
        consumer
            .acknowledge(record, ShareAckType::Reject)
            .expect("reject in explicit mode");
    }
    consumer.commit().await.expect("commit rejects");

    // Produce one more record. Only that record can arrive, because the
    // rejected offsets are archived and the start offset moved past them.
    support::produce(&admin, &topic, &["v3"]).await;

    let next = poll_until(&mut consumer, 1, POLL_BUDGET).await;
    assert2::assert!(view(&next) == expected(&topic, 0, 3, 1, &["v3"]));

    consumer.close().await.expect("close");
    drop(kafka);
}

/// Two members of one share group split a two-partition topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and a broker with KIP-932 share groups"]
async fn two_consumers_share_topic() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let admin = support::bootstrap_client(&kafka.bootstrap).await;

    let topic = support::unique("share-split");
    let group = support::unique("share-split-grp");
    support::create_topic_with_partitions(&admin, &topic, 2).await;
    let topic_id = support::topic_id_for(&admin, &topic).await;
    support::produce_to_partition(&admin, &topic, 0, &["p0a", "p0b"]).await;
    support::produce_to_partition(&admin, &topic, 1, &["p1a", "p1b"]).await;

    let mut c1 = share_consumer(
        &kafka.bootstrap,
        &group,
        "share-c1",
        &topic,
        ShareAckMode::Implicit,
    )
    .await;
    let mut c2 = share_consumer(
        &kafka.bootstrap,
        &group,
        "share-c2",
        &topic,
        ShareAckMode::Implicit,
    )
    .await;
    wait_for_coverage(&admin, &group, topic_id, &[0, 1], 2).await;

    // Drive both members until every produced record arrives. The assignment
    // of the second member settles over a few heartbeats, so both members poll
    // in a bounded loop. The auto-`Accept` moves each member past its own
    // partitions, so no record arrives twice.
    let mut got1: Vec<(i32, String)> = Vec::new();
    let mut got2: Vec<(i32, String)> = Vec::new();
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    while Instant::now() < deadline && got1.len() + got2.len() < 4 {
        for record in c1.poll(millis(250)).await.expect("c1 poll") {
            got1.push((record.partition, val(&record)));
        }
        for record in c2.poll(millis(250)).await.expect("c2 poll") {
            got2.push((record.partition, val(&record)));
        }
    }

    // No record went to both members.
    let values1: HashSet<&String> = got1.iter().map(|(_, v)| v).collect();
    let values2: HashSet<&String> = got2.iter().map(|(_, v)| v).collect();
    assert2::assert!(values1.is_disjoint(&values2));
    // A member holds whole partitions, so it never sees both partitions.
    let parts1: HashSet<i32> = got1.iter().map(|(p, _)| *p).collect();
    let parts2: HashSet<i32> = got2.iter().map(|(p, _)| *p).collect();
    assert2::assert!(parts1.is_disjoint(&parts2));
    // Together the two members cover all four records.
    let mut all: Vec<String> = got1
        .iter()
        .chain(got2.iter())
        .map(|(_, v)| v.clone())
        .collect();
    all.sort();
    assert2::assert!(all == vec!["p0a", "p0b", "p1a", "p1b"]);

    c1.close().await.expect("close c1");
    c2.close().await.expect("close c2");
    drop(kafka);
}

/// `close()` leaves the group, so a later describe no longer lists the member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and a broker with KIP-932 share groups"]
async fn close_leaves_group() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let admin = support::bootstrap_client(&kafka.bootstrap).await;

    let topic = support::unique("share-leave");
    let group = support::unique("share-leave-grp");
    support::create_topic(&admin, &topic).await;
    let topic_id = support::topic_id_for(&admin, &topic).await;
    support::produce(&admin, &topic, &["v0"]).await;

    let mut consumer = share_consumer(
        &kafka.bootstrap,
        &group,
        "leave-1",
        &topic,
        ShareAckMode::Implicit,
    )
    .await;
    let member_id = consumer.member_id().to_string();
    assert2::assert!(!member_id.is_empty());

    wait_for_coverage(&admin, &group, topic_id, &[0], 1).await;
    let first = poll_until(&mut consumer, 1, POLL_BUDGET).await;
    assert2::assert!(view(&first) == expected(&topic, 0, 0, 1, &["v0"]));

    consumer.close().await.expect("close");

    // The leave heartbeat that `close()` sends carries member epoch -1 and
    // removes the member. The describe repeats until the member is gone.
    wait_for_member_absent(&admin, &group, &member_id).await;
    drop(kafka);
}

/// Explicit mode: a renewed lock stops a redelivery.
///
/// The consumer polls one record and renews its lock before the short record
/// lock expires. It then waits past the original lock. The renew moved the
/// deadline, so the broker does not deliver the record again. This proves that
/// the renew reaches the broker and that the broker applies it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and a broker with KIP-932 share groups"]
async fn explicit_renew_prevents_redelivery() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let admin = support::bootstrap_client(&kafka.bootstrap).await;

    let topic = support::unique("share-renew");
    let group = support::unique("share-renew-grp");
    support::create_topic(&admin, &topic).await;
    let topic_id = support::topic_id_for(&admin, &topic).await;
    support::produce(&admin, &topic, &["v0"]).await;

    let mut consumer = share_consumer(
        &kafka.bootstrap,
        &group,
        "rn-1",
        &topic,
        ShareAckMode::Explicit,
    )
    .await;
    wait_for_coverage(&admin, &group, topic_id, &[0], 1).await;

    // Acquire the one record. Explicit mode does no auto-accept. The instant of
    // delivery is kept, because the broker-side lock starts at about that
    // moment: the poll that returns the record is the acquiring fetch.
    let (first, acquired_at) = poll_one_with_instant(&mut consumer).await;
    assert2::assert!(view(&first) == expected(&topic, 0, 0, 1, &["v0"]));

    // Renew at 40% of the lock, before the lock expires. The broker moves the
    // deadline to renew time plus one more lock, near 140% of the lock.
    sleep_until(acquired_at + RECORD_LOCK * 2 / 5).await;
    consumer
        .renew(&first[0])
        .await
        .expect("renew in explicit mode");

    // Wait to 115% of the lock. That is past the original deadline, where an
    // un-renewed record would already be swept and delivered again, and before
    // the renewed deadline near 140%.
    sleep_until(acquired_at + RECORD_LOCK * 23 / 20).await;

    // The renewed lock still holds, so there is no redelivery. Two short polls
    // end near 130% of the lock, still before the renewed deadline.
    let mut redelivered = 0usize;
    for _ in 0..2 {
        redelivered += consumer.poll(millis(60)).await.expect("share poll").len();
    }
    assert2::assert!(redelivered == 0);

    consumer.close().await.expect("close");
    drop(kafka);
}

/// Implicit mode rejects `renew()`.
///
/// Implicit mode accepts a record on the next poll or on close, so a lock
/// renewal has no meaning. `renew()` returns `ConsumerError::IllegalState` and
/// makes no wire round trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and a broker with KIP-932 share groups"]
async fn renew_errors_in_implicit_mode() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let admin = support::bootstrap_client(&kafka.bootstrap).await;

    let topic = support::unique("share-implicit-renew");
    let group = support::unique("share-implicit-renew-grp");
    support::create_topic(&admin, &topic).await;
    let topic_id = support::topic_id_for(&admin, &topic).await;
    support::produce(&admin, &topic, &["v0"]).await;

    let mut consumer = share_consumer(
        &kafka.bootstrap,
        &group,
        "imp-1",
        &topic,
        ShareAckMode::Implicit,
    )
    .await;
    wait_for_coverage(&admin, &group, topic_id, &[0], 1).await;

    let first = poll_until(&mut consumer, 1, POLL_BUDGET).await;
    assert2::assert!(view(&first) == expected(&topic, 0, 0, 1, &["v0"]));

    let result = consumer.renew(&first[0]).await;
    assert2::assert!(matches!(result, Err(ConsumerError::IllegalState(_))));

    consumer.close().await.expect("close");
    drop(kafka);
}

// ── helpers ───────────────────────────────────────────────────────────────

/// Build a share consumer that subscribes to one topic.
///
/// The 300 ms heartbeat keeps the join and the assignment inside the poll
/// budget of each case.
async fn share_consumer(
    bootstrap: &str,
    group_id: &str,
    client_id: &str,
    topic: &str,
    ack_mode: ShareAckMode,
) -> ShareConsumer {
    ShareConsumer::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .group_id(group_id)
        .subscribe([topic.to_string()])
        .ack_mode(ack_mode)
        .heartbeat_interval(millis(300))
        .build()
        .await
        .expect("build share consumer")
}

/// The UTF-8 value of a record, or an empty string when the record has none.
fn val(record: &ShareConsumerRecord) -> String {
    String::from_utf8_lossy(record.value.as_deref().unwrap_or(&[])).into_owned()
}

/// One comparable row per record: topic, partition, offset, delivery count and
/// value.
type RecordRow = (String, i32, i64, i16, String);

/// The sorted rows of a delivered batch, for one comparison against the whole
/// expected batch.
fn view(records: &[ShareConsumerRecord]) -> Vec<RecordRow> {
    let mut rows: Vec<RecordRow> = records
        .iter()
        .map(|r| {
            (
                r.topic.clone(),
                r.partition,
                r.offset,
                r.delivery_count,
                val(r),
            )
        })
        .collect();
    rows.sort();
    rows
}

/// The rows that `values` must produce, from `first_offset` upward.
fn expected(
    topic: &str,
    partition: i32,
    first_offset: i64,
    delivery_count: i16,
    values: &[&str],
) -> Vec<RecordRow> {
    values
        .iter()
        .enumerate()
        .map(|(i, value)| {
            let step = i64::try_from(i).expect("test fixture small enough for i64");
            (
                topic.to_string(),
                partition,
                first_offset + step,
                delivery_count,
                (*value).to_string(),
            )
        })
        .collect()
}

/// Poll until `want` records accumulate, or until `budget` runs out.
async fn poll_until(
    consumer: &mut ShareConsumer,
    want: usize,
    budget: Duration,
) -> Vec<ShareConsumerRecord> {
    let mut acquired: Vec<ShareConsumerRecord> = Vec::new();
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline && acquired.len() < want {
        let records = consumer.poll(millis(300)).await.expect("share poll");
        acquired.extend(records);
    }
    acquired
}

/// Poll until one record arrives, and report when it arrived.
async fn poll_one_with_instant(
    consumer: &mut ShareConsumer,
) -> (Vec<ShareConsumerRecord>, Instant) {
    let deadline = Instant::now() + POLL_BUDGET;
    while Instant::now() < deadline {
        let records = consumer.poll(millis(200)).await.expect("share poll");
        if !records.is_empty() {
            return (records, Instant::now());
        }
    }
    panic!("no record arrived in {POLL_BUDGET:?}");
}

/// Sleep until `target`. The delay is real time, and it is deliberate: the case
/// exercises a broker-side lock deadline, which is a property of time. No state
/// poll can replace it.
async fn sleep_until(target: Instant) {
    if let Some(remaining) = target.checked_duration_since(Instant::now()) {
        tokio::time::sleep(remaining).await;
    }
}

/// Send a `ShareGroupDescribe` for `group` and return its row.
///
/// Returns `None` when the broker reports no row for the group.
async fn describe_group(client: &Client, group: &str) -> Option<DescribedGroup> {
    let resp = client
        .send(ShareGroupDescribeRequest {
            group_ids: vec![group.to_string()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .expect("ShareGroupDescribe");
    resp.groups.into_iter().find(|g| g.group_id == group)
}

/// Wait until the group has `members` members that together hold every
/// partition in `partitions`.
///
/// The monorepo suite waited on a broker-internal share-state signal. A client
/// cannot see that signal. It can see the assignment, and a member acquires no
/// record before the coordinator gives it the partition, so the assignment is
/// the readiness gate this suite uses.
async fn wait_for_coverage(
    client: &Client,
    group: &str,
    topic_id: WireUuid,
    partitions: &[i32],
    members: usize,
) {
    tokio::time::timeout(SETTLE_TIMEOUT, async {
        loop {
            if group_covers(client, group, topic_id, partitions, members).await {
                return;
            }
            describe_pause().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("{group} did not assign {partitions:?} to {members} member(s) in {SETTLE_TIMEOUT:?}")
    });
}

/// True when `group` has `members` members that together hold `partitions`.
async fn group_covers(
    client: &Client,
    group: &str,
    topic_id: WireUuid,
    partitions: &[i32],
    members: usize,
) -> bool {
    let Some(described) = describe_group(client, group).await else {
        return false;
    };
    if described.members.len() != members {
        return false;
    }
    let held: HashSet<i32> = described
        .members
        .iter()
        .flat_map(|member| member.assignment.topic_partitions.iter())
        .filter(|tp| tp.topic_id == topic_id)
        .flat_map(|tp| tp.partitions.iter().copied())
        .collect();
    partitions.iter().all(|p| held.contains(p))
}

/// Wait until `group` no longer lists `member_id`.
async fn wait_for_member_absent(client: &Client, group: &str, member_id: &str) {
    tokio::time::timeout(SETTLE_TIMEOUT, async {
        loop {
            let absent = match describe_group(client, group).await {
                // The group stays but holds no such member, or the row is gone.
                Some(described) => described.members.iter().all(|m| m.member_id != member_id),
                None => true,
            };
            if absent {
                return;
            }
            describe_pause().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{member_id} stayed in {group} for {SETTLE_TIMEOUT:?}"));
}

/// Real-time pause between two describe round trips, not a progress poll: the
/// coordinator publishes no signal that this side can await.
async fn describe_pause() {
    tokio::time::sleep(DESCRIBE_RETRY_DELAY).await;
}
