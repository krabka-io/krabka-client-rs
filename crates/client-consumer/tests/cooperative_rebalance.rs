//! KIP-429 cooperative-sticky rebalance suite.
//!
//! The cases run several [`Consumer`]s with [`Assignor::CooperativeSticky`]
//! against one broker and prove three properties:
//!
//! - A single member takes every partition of its topic.
//! - Three members reach a settled assignment that covers all partitions once,
//!   with no partition held by two members and no member left empty.
//! - A rebalance stays transparent to `poll()`. The method never reports a
//!   rebalance-specific error, the member that sheds partitions loses no
//!   record, and the member that gains them replays none, because the
//!   revoke-time commit moved the committed position first.
//!
//! Every case needs the Docker container from [`support`], so every case
//! carries `#[ignore]`.

mod support;

use std::{collections::HashSet, time::Duration};

use bytes::Bytes;
use krabka_client_consumer::{Assignor, AutoOffsetReset, Consumer, ConsumerError};
use krabka_client_core::Client;
use krabka_units::{millis, secs};

/// Bound for every settle loop in this suite.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Pause between two member joins. See [`pace_join`].
const JOIN_PACE: Duration = Duration::from_millis(500);

/// Three members split a 6-partition topic without overlap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn cooperative_three_member_partial_revocation() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let producer = support::bootstrap_client(&kafka.bootstrap).await;

    let topic = support::unique("coop6");
    let group = support::unique("coop-grp");
    support::create_topic_with_partitions(&producer, &topic, 6).await;

    // m1 joins alone and takes all six partitions.
    let m1 = cooperative_consumer(&kafka.bootstrap, &group, "m1", &topic).await;
    wait_for_assignment_count(&m1, 6).await;
    assert2::assert!(m1.assignment().await.len() == 6);

    // m2 joins. Phase 1 keeps the partitions that m1 retains and gives m2
    // none. Phase 2 moves the freed half to m2. Both members must hold a
    // non-empty, disjoint share before m3 joins, which is what
    // `wait_for_settled_split` demands. A union of six alone does not prove
    // the round is over: m1 still reports all six while m2 sits at zero
    // between the two phases.
    pace_join().await;
    let m2 = cooperative_consumer(&kafka.bootstrap, &group, "m2", &topic).await;
    wait_for_settled_split(&[&m1, &m2], 6).await;

    // m3 joins after the m1 and m2 round is complete. The settle loop below
    // gates the final round.
    pace_join().await;
    let m3 = cooperative_consumer(&kafka.bootstrap, &group, "m3", &topic).await;

    let union = wait_for_settled_split(&[&m1, &m2, &m3], 6).await;
    for (name, _) in &union {
        assert2::assert!(*name == topic);
    }
    let mut partitions: Vec<i32> = union.into_iter().map(|(_, p)| p).collect();
    partitions.sort_unstable();
    assert2::assert!(partitions == vec![0, 1, 2, 3, 4, 5]);

    m1.close().await.expect("close m1");
    m2.close().await.expect("close m2");
    m3.close().await.expect("close m3");
    drop(kafka);
}

/// A rebalance stays invisible to `poll()`, and no record is lost or replayed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn cooperative_transparent_to_poll() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let producer = support::bootstrap_client(&kafka.bootstrap).await;

    let topic = support::unique("cooppoll");
    let group = support::unique("poll-grp");
    support::create_topic_with_partitions(&producer, &topic, 4).await;

    // First wave: one record per partition.
    produce_wave(&producer, &topic, 'a').await;

    // m1 starts alone and gets all four partitions.
    let mut m1 = cooperative_consumer(&kafka.bootstrap, &group, "m1", &topic).await;
    wait_for_assignment_count(&m1, 4).await;
    let first_wave = poll_until_values(&mut m1, 4, |_| true).await;
    assert2::assert!(first_wave.len() == 4);

    // Second wave. m1 still owns all four partitions and reads the whole wave
    // before any rebalance. This is the case the revoke-time commit protects:
    // m1 moves past `b0..b3`, so the committed position must stop a replay
    // when a partition later moves to m2.
    produce_wave(&producer, &topic, 'b').await;
    let m1_second_wave = poll_until_values(&mut m1, 4, |v| v.starts_with('b')).await;
    assert2::assert!(m1_second_wave.len() == 4);

    // m2 starts and triggers a cooperative rebalance that takes two partitions
    // from m1. m1 keeps polling so its coordinator can complete the rejoin.
    let mut m2 = cooperative_consumer(&kafka.bootstrap, &group, "m2", &topic).await;
    wait_for_even_split(&mut m1, &m2, 2).await;

    // Drain m2 to empty. m2 primes its two partitions at the position m1
    // committed, which is past `b*`, so m2 must deliver none of the second
    // wave. A record that comes back here means the commit was lost.
    let m2_second_wave = drain_to_empty(&mut m2, |v| v.starts_with('b')).await;

    // No loss: m1 delivered the whole second wave.
    assert2::assert!(m1_second_wave.len() == 4);
    // No replay: no second-wave value reached both members.
    assert2::assert!(m1_second_wave.is_disjoint(&m2_second_wave));

    m1.close().await.expect("close m1");
    m2.close().await.expect("close m2");
    drop(kafka);
}

/// One member takes every partition and reads every record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn cooperative_single_member_steady_state() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let producer = support::bootstrap_client(&kafka.bootstrap).await;

    let topic = support::unique("cooponly");
    let group = support::unique("only-grp");
    support::create_topic_with_partitions(&producer, &topic, 3).await;

    let mut consumer = cooperative_consumer(&kafka.bootstrap, &group, "m1", &topic).await;
    wait_for_assignment_count(&consumer, 3).await;
    let assignment = consumer.assignment().await;
    assert2::assert!(assignment.len() == 3);
    let mut parts: Vec<i32> = assignment.iter().map(|(_, p)| *p).collect();
    parts.sort_unstable();
    assert2::assert!(parts == vec![0, 1, 2]);

    for p in 0..3i32 {
        support::produce_to_partition(&producer, &topic, p, &[&format!("v{p}")]).await;
    }

    let seen = poll_until_values(&mut consumer, 3, |_| true).await;
    assert2::assert!(seen.len() == 3);

    consumer.close().await.expect("close consumer");
    drop(kafka);
}

// ── helpers ───────────────────────────────────────────────────────────────

/// Pause between two member joins. The pause is real time, and it is
/// deliberate.
///
/// A cooperative-sticky rebalance runs in two phases, and the broker holds the
/// first phase open for its initial rebalance delay. A member that joins while
/// the previous round is still open starts one more round, and the group never
/// settles on a clean snapshot. No signal on either side reports "the group is
/// stable": the member count increases at `JoinGroup`, which is before
/// `SyncGroup` completes. The joins are therefore paced in real time. This is a
/// timing property of the protocol, not a guess at a duration that makes the
/// case pass.
async fn pace_join() {
    tokio::time::sleep(JOIN_PACE).await;
}

/// The UTF-8 value of a record, or an empty string when the record has none.
fn value_string(value: Option<&Bytes>) -> String {
    String::from_utf8_lossy(value.map_or(&[], Bytes::as_ref)).into_owned()
}

/// Produce one record to each of the four partitions, named `<prefix><index>`.
async fn produce_wave(producer: &Client, topic: &str, prefix: char) {
    for p in 0..4i32 {
        support::produce_to_partition(producer, topic, p, &[&format!("{prefix}{p}")]).await;
    }
}

/// Build a consumer that uses the cooperative-sticky assignor.
///
/// The 500 ms heartbeat keeps one detect-and-rejoin round trip inside the
/// broker's initial rebalance delay: detection costs up to 500 ms and the
/// rejoin costs up to 500 ms more. A 1 s heartbeat gives a 2 s worst case,
/// which can pass the broker's wait on a loaded runner. The leader then
/// computes the next round from stale member metadata.
async fn cooperative_consumer(
    bootstrap: &str,
    group_id: &str,
    client_id: &str,
    topic: &str,
) -> Consumer {
    Consumer::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .group_id(group_id)
        .assignor(Assignor::CooperativeSticky)
        .session_timeout(secs(30))
        .rebalance_timeout(secs(2))
        .heartbeat_interval(millis(500))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic.to_string()])
        .build()
        .await
        .expect("build cooperative consumer")
}

/// Poll `consumer` until `want` distinct values that satisfy `keep` arrive.
async fn poll_until_values(
    consumer: &mut Consumer,
    want: usize,
    keep: fn(&str) -> bool,
) -> HashSet<String> {
    let mut seen: HashSet<String> = HashSet::new();
    tokio::time::timeout(SETTLE_TIMEOUT, async {
        while seen.len() < want {
            for record in consumer.poll(millis(200)).await.expect("poll") {
                let value = value_string(record.value.as_ref());
                if keep(&value) {
                    seen.insert(value);
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("consumer delivered fewer than {want} values in {SETTLE_TIMEOUT:?}")
    });
    seen
}

/// Poll `consumer` until one poll returns nothing, and keep the values that
/// satisfy `keep`.
async fn drain_to_empty(consumer: &mut Consumer, keep: fn(&str) -> bool) -> HashSet<String> {
    let mut seen: HashSet<String> = HashSet::new();
    tokio::time::timeout(SETTLE_TIMEOUT, async {
        loop {
            let records = consumer.poll(millis(200)).await.expect("poll");
            if records.is_empty() {
                return;
            }
            for record in records {
                let value = value_string(record.value.as_ref());
                if keep(&value) {
                    seen.insert(value);
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("consumer still delivered records after {SETTLE_TIMEOUT:?}"));
    seen
}

/// Wait until `consumer` holds `expected` partitions.
async fn wait_for_assignment_count(consumer: &Consumer, expected: usize) {
    tokio::time::timeout(SETTLE_TIMEOUT, async {
        loop {
            if consumer.assignment().await.len() == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("assignment count did not reach {expected} in {SETTLE_TIMEOUT:?}"));
}

/// Wait for a settled cooperative assignment and return its union.
///
/// The correctness properties are that the union covers every partition, that
/// no two members hold the same partition, and that every member holds at
/// least one partition. Those three together are what proves the round is
/// over: a union that covers every partition proves nothing on its own,
/// because a member that has not yet processed its `SyncGroup` response still
/// reports the partitions it is about to give up.
///
/// The loop does not demand an even split. The per-member `assignment()` reads
/// are not one atomic snapshot, so a phase-1 to phase-2 transition can show one
/// member with one partition and another with three. The loop condition rides
/// out that window instead of failing on it.
async fn wait_for_settled_split(
    consumers: &[&Consumer],
    expected: usize,
) -> HashSet<(String, i32)> {
    tokio::time::timeout(SETTLE_TIMEOUT, async {
        loop {
            let mut union: HashSet<(String, i32)> = HashSet::new();
            let mut overlap = false;
            let mut empty_member = false;
            for consumer in consumers {
                let assignment = consumer.assignment().await;
                empty_member |= assignment.is_empty();
                for tp in assignment {
                    overlap |= !union.insert(tp);
                }
            }
            if union.len() == expected && !overlap && !empty_member {
                return union;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("assignment did not settle on {expected} partitions in {SETTLE_TIMEOUT:?}")
    })
}

/// Wait until both members hold `each` partitions, and keep `m1` polling.
///
/// m1 must keep polling so that its coordinator completes the rejoin. KIP-429
/// makes the rebalance transparent to `poll()`, so a rebalance-specific error
/// from `poll()` fails the case at once. Other errors are transient and the
/// loop ignores them. Once m2 owns its partitions the leader has completed
/// phase 2, which runs after the revoke-time commit of m1.
async fn wait_for_even_split(m1: &mut Consumer, m2: &Consumer, each: usize) {
    tokio::time::timeout(SETTLE_TIMEOUT, async {
        loop {
            match m1.poll(millis(200)).await {
                Ok(_) => {}
                Err(ConsumerError::CommitInvalid | ConsumerError::RebalanceFailed(_)) => {
                    panic!("m1.poll reported a rebalance-specific error, which KIP-429 forbids")
                }
                Err(_) => {}
            }
            if m1.assignment().await.len() == each && m2.assignment().await.len() == each {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("members did not settle on {each} partitions each in {SETTLE_TIMEOUT:?}")
    });
}
