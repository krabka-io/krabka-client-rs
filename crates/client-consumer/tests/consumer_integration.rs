//! End-to-end consumer tests against a Kafka broker in Docker.
//!
//! A producer writes records through `krabka-client-core`. A
//! [`krabka_client_consumer::Consumer`] subscribes through a group and reads
//! the records back. The suite proves that commits survive a broker restart,
//! that an eager rebalance re-acquires and primes a freed partition, that a
//! commit after a rebalance stamps the current generation, that an
//! out-of-range fetch offset recovers by `auto.offset.reset` policy, and that a
//! member which joins before its subscribed topic exists still gets a working
//! assignment when the topic appears.
//!
//! These cases came from the monorepo, where they booted a broker in the same
//! process. The broker now lives in another repository, so `support` starts
//! `confluentinc/cp-kafka:6.1.1` instead. That image is Apache Kafka 2.7. It
//! has no topic IDs in Metadata, so `support::topic_id_for` returns the zero
//! UUID and the client negotiates Produce and Fetch below v13. It also tops
//! out at `OffsetFetch` v7, which answers with the legacy top-level `topics`
//! array instead of the v8+ `groups` array, so the reader in this file accepts
//! either shape.
//!
//! `flavor = "multi_thread", worker_threads = 2` stays. The broker is now a
//! separate process, so the original reason (a broker accept loop in the same
//! runtime) no longer applies. The flavour stays because a `Consumer` runs a
//! coordinator task and a heartbeat task next to the test body, and both must
//! make progress while the body awaits a poll.
//!
//! Every case needs the container, so every case carries `#[ignore]`. Run the
//! suite with Docker present:
//!
//! ```text
//! cargo test -p krabka-client-consumer --test consumer_integration -- --ignored --nocapture
//! ```

mod support;

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerError, ConsumerRecord};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        delete_records_request::{
            DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
        },
        offset_fetch_request::{
            OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
            OffsetFetchRequestTopics,
        },
        offset_fetch_response::OffsetFetchResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use krabka_units::{millis, secs};

/// Outer bound for any wait that depends on the container. A rebalance, a
/// metadata refresh and a log recovery all run in the broker process, and a
/// cold runner is far slower than the in-process broker this suite came from.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Pause between two attempts at a condition the broker owns.
///
/// This is a real-time wait, not a progress poll. The broker is a separate
/// process and publishes no readiness signal that this side can await, so
/// `tokio::task::yield_now` would spin a core without letting the broker
/// advance.
async fn broker_side_pause() {
    tokio::time::sleep(Duration::from_millis(200)).await;
}

/// Decode a record value as a lossy UTF-8 string.
fn value_of(record: &ConsumerRecord) -> String {
    String::from_utf8_lossy(record.value.as_deref().unwrap_or(&[])).into_owned()
}

/// Wait until `consumer` holds exactly `want` partitions.
///
/// This checks `assignment()` and never polls, so the consumer's position does
/// not advance while the wait runs.
async fn wait_for_assignment(consumer: &Consumer, want: usize, what: &str) {
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    loop {
        let held = consumer.assignment().await.len();
        if held == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: assignment stalled at {held} partitions, wanted {want}"
        );
        broker_side_pause().await;
    }
}

/// Wait until `consumer` holds exactly `want` partitions, polling meanwhile.
///
/// A rejoin needs the poll loop to run, so this variant drives `poll` on every
/// attempt and ignores what it returns.
async fn poll_until_assignment(consumer: &mut Consumer, want: usize, what: &str) {
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    loop {
        let _ = consumer.poll(millis(200)).await;
        let held = consumer.assignment().await.len();
        if held == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: assignment stalled at {held} partitions, wanted {want}"
        );
        broker_side_pause().await;
    }
}

/// Poll until `want` records arrive, keeping their order.
async fn collect_records(consumer: &mut Consumer, want: usize) -> Vec<ConsumerRecord> {
    let mut out: Vec<ConsumerRecord> = Vec::new();
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    while out.len() < want {
        out.extend(
            consumer
                .poll(millis(300))
                .await
                .expect("poll must not error"),
        );
        assert!(
            Instant::now() < deadline,
            "only {} of {want} records arrived",
            out.len()
        );
    }
    out
}

/// Poll until `want` distinct values that start with `prefix` arrive.
///
/// The prefix filter drops any record that a re-acquired partition replays, so
/// the count measures the fresh records only.
async fn collect_prefixed(consumer: &mut Consumer, prefix: char, want: usize) -> HashSet<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    while seen.len() < want {
        for record in consumer
            .poll(millis(200))
            .await
            .expect("poll must not error")
        {
            let value = value_of(&record);
            if value.starts_with(prefix) {
                seen.insert(value);
            }
        }
        assert!(
            Instant::now() < deadline,
            "only {} of {want} fresh records arrived: {seen:?}",
            seen.len()
        );
    }
    seen
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn rust_producer_to_rust_consumer_through_group() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let topic = support::unique("rrtopic");
    let group = support::unique("rrgroup");

    let producer = support::bootstrap_client(&kafka.bootstrap).await;
    support::create_topic(&producer, &topic).await;
    support::produce(&producer, &topic, &["a", "b", "c"]).await;

    let mut consumer = Consumer::builder()
        .bootstrap(&kafka.bootstrap)
        .client_id("rust-consumer")
        .group_id(&group)
        .session_timeout(secs(30))
        .rebalance_timeout(secs(10))
        .heartbeat_interval(secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic.clone()])
        .build()
        .await
        .expect("build consumer");

    let seen: Vec<String> = collect_records(&mut consumer, 3)
        .await
        .iter()
        .map(value_of)
        .collect();
    assert!(seen == vec!["a", "b", "c"]);

    consumer.commit_sync().await.expect("commit");
    consumer.close().await.expect("close consumer");
    producer.close();
    drop(kafka);
}

/// A committed offset survives a broker restart.
///
/// The first run consumes every record and commits. `KafkaBroker::restart`
/// then stops the broker process and starts it again on the same volumes, so
/// the log and the `__consumer_offsets` partitions come back. A second member
/// of the same group must start from the committed offset, which is the log
/// end, and read nothing.
///
/// The restart kills every open connection, so each client is rebuilt against
/// the bootstrap address that the support module re-reads after the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn offsets_survive_broker_restart() {
    support::init_tracing();
    let mut kafka = support::start_kafka().await;
    let topic = support::unique("persist");
    let group = support::unique("persist-grp");

    // First run: create, produce, consume, commit.
    {
        let producer = support::bootstrap_client(&kafka.bootstrap).await;
        support::create_topic(&producer, &topic).await;
        support::produce(&producer, &topic, &["x", "y", "z"]).await;

        let mut consumer = Consumer::builder()
            .bootstrap(&kafka.bootstrap)
            .client_id("c")
            .group_id(&group)
            .session_timeout(secs(30))
            .rebalance_timeout(secs(10))
            .heartbeat_interval(secs(1))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe([topic.clone()])
            .build()
            .await
            .expect("build consumer");
        assert!(collect_records(&mut consumer, 3).await.len() == 3);
        consumer.commit_sync().await.expect("commit");
        consumer.close().await.expect("close consumer");
        producer.close();
    }

    kafka.restart().await;

    // Second run: the same group reads from the committed offset, which is the
    // log end.
    {
        let mut consumer = Consumer::builder()
            .bootstrap(&kafka.bootstrap)
            .client_id("c2")
            .group_id(&group)
            .session_timeout(secs(30))
            .rebalance_timeout(secs(10))
            .heartbeat_interval(secs(1))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe([topic.clone()])
            .build()
            .await
            .expect("build consumer after the restart");

        // Wait for the assignment first. A poll issued while the join is still
        // open returns empty whatever the committed offset is, so the check
        // below would prove nothing.
        wait_for_assignment(&consumer, 1, "consumer after the restart").await;
        for _ in 0..5 {
            let records = consumer
                .poll(millis(500))
                .await
                .expect("poll must not error");
            assert!(
                records.is_empty(),
                "the committed offset was not honoured: {records:?}"
            );
        }
        consumer.close().await.expect("close consumer");
    }

    drop(kafka);
}

/// Build one group member that subscribes to `topic`.
async fn build_member(bootstrap: &str, client_id: &str, group: &str, topic: &str) -> Consumer {
    Consumer::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .group_id(group)
        .session_timeout(secs(30))
        .rebalance_timeout(secs(10))
        .heartbeat_interval(millis(500))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic.to_string()])
        .build()
        .await
        .expect("build consumer")
}

/// Bring up two members of `group` that split a 2-partition `topic` 1/1.
///
/// Both members are built at the same time so they batch into the *first*
/// rebalance round. The broker holds that round open for its initial rebalance
/// delay and completes it once both have joined. Adding a second member to an
/// already stable group instead would make the follower wait on a late leader.
async fn two_member_split(bootstrap: &str, group: &str, topic: &str) -> (Consumer, Consumer) {
    let (m1, m2) = tokio::join!(
        build_member(bootstrap, "m1", group, topic),
        build_member(bootstrap, "m2", group, topic),
    );
    wait_for_assignment(&m1, 1, "m1 in the 1/1 split").await;
    wait_for_assignment(&m2, 1, "m2 in the 1/1 split").await;
    (m1, m2)
}

/// `m2` leaves, so `m1` becomes the sole member and takes the freed partition
/// back through the coordinator's *eager* rejoin path.
///
/// That path primes the fetch offset of the re-acquired partition before it
/// publishes the assignment again. `m1` is its own leader here, so there is no
/// follower wait to race.
async fn drop_member_and_reacquire(m1: &mut Consumer, m2: Consumer) {
    m2.close().await.expect("m2 leaves the group");
    poll_until_assignment(m1, 2, "m1 re-acquires both partitions").await;
}

/// Two Range (eager) consumers share a 2-partition topic.
///
/// When the second consumer joins, the survivor gives up a partition through
/// the coordinator's *eager* rejoin path. When the second consumer leaves, the
/// survivor takes the freed partition back through that same path. That path
/// primes the fetch offset of the re-acquired partition *before* it publishes
/// the assignment again.
///
/// This case covers the prime-before-publish order in the eager branch of
/// `coordinator.rs`. `cooperative_rebalance.rs` covers the cooperative
/// branches. A poll that races the rejoin must not see a re-acquired partition
/// with no primed offset and fetch it from 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn eager_rebalance_reacquires_and_primes() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let topic = support::unique("eagerrebal");
    let group = support::unique("eager-grp");

    let producer = support::bootstrap_client(&kafka.bootstrap).await;
    support::create_topic_with_partitions(&producer, &topic, 2).await;

    let (mut m1, m2) = two_member_split(&kafka.bootstrap, &group, &topic).await;
    drop_member_and_reacquire(&mut m1, m2).await;

    // Produce a fresh record to each partition. m1 owns both again, so it must
    // deliver both. That proves the re-acquired partition primed correctly and
    // that poll did not stall on a missing next-offset entry.
    support::produce_to_partition(&producer, &topic, 0, &["b0"]).await;
    support::produce_to_partition(&producer, &topic, 1, &["b1"]).await;
    let second = collect_prefixed(&mut m1, 'b', 2).await;
    assert!(second == HashSet::from(["b0".to_string(), "b1".to_string()]));

    m1.close().await.expect("close m1");
    producer.close();
    drop(kafka);
}

/// Regression: a commit after a rebalance stamps the CURRENT generation.
///
/// A rebalance bumps the group generation. A commit issued after that rebalance
/// must use the current generation, not the start-up snapshot. The commit path
/// read a `generation_id` captured at build time and never kept it in sync as
/// the coordinator rejoined. So the first commit after any rebalance hit
/// `ILLEGAL_GENERATION (22)`, and a long-running commit loop crashed. The demo
/// metrics-compactor crash-loop showed this failure.
///
/// The coordinator now shares the generation in an `Arc<AtomicI32>` and
/// publishes it on every rejoin. A rebalance code on commit now defers the
/// commit instead of failing it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn commit_succeeds_after_rebalance_bumps_generation() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let topic = support::unique("genbump");
    let group = support::unique("genbump-grp");

    let producer = support::bootstrap_client(&kafka.bootstrap).await;
    support::create_topic_with_partitions(&producer, &topic, 2).await;

    let (mut m1, m2) = two_member_split(&kafka.bootstrap, &group, &topic).await;
    let gen_after_split = m1.generation_id();

    drop_member_and_reacquire(&mut m1, m2).await;

    // The rejoin bumped the generation, and the accessor reads it live from the
    // shared atomic. That proves the generation which the commit path stamps is
    // the current one.
    let gen_after_rejoin = m1.generation_id();
    assert!(gen_after_rejoin > gen_after_split);

    // Consume a record on each partition so there are offsets to commit, then
    // commit. Before the fix this stamped the stale start-up generation, the
    // broker returned `ILLEGAL_GENERATION (22)`, and `commit_sync` failed.
    support::produce_to_partition(&producer, &topic, 0, &["g0"]).await;
    support::produce_to_partition(&producer, &topic, 1, &["g1"]).await;
    let seen = collect_prefixed(&mut m1, 'g', 2).await;
    assert!(seen == HashSet::from(["g0".to_string(), "g1".to_string()]));

    m1.commit_sync()
        .await
        .expect("commit after a generation-bumping rebalance must succeed (current generation)");

    m1.close().await.expect("close m1");
    producer.close();
    drop(kafka);
}

// ── KIP-320 truncation detection ─────────────────────────────────────────────

/// Move a partition's `log_start_offset` forward with `DeleteRecords`.
///
/// `DeleteRecords` is `api_key=21`. It drops every record below `offset`. This
/// helper returns the resulting `low_watermark`, which is the broker's new log
/// start. A consumer positioned below that point then sees
/// `OFFSET_OUT_OF_RANGE` on its next `Fetch`. This is the deterministic way to
/// cause the truncation and divergence that KIP-320 handles.
async fn delete_records_before(client: &Client, topic: &str, partition: i32, offset: i64) -> i64 {
    let resp = client
        .send(DeleteRecordsRequest {
            topics: vec![DeleteRecordsTopic {
                name: topic.into(),
                partitions: vec![DeleteRecordsPartition {
                    partition_index: partition,
                    offset,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            timeout_ms: 30_000,
            ..Default::default()
        })
        .await
        .expect("DeleteRecords");
    let pr = &resp.topics[0].partitions[0];
    assert!(pr.error_code == 0, "{resp:?}");
    pr.low_watermark
}

/// The eight seed records, at offsets 0 to 7.
const OUT_OF_RANGE_SEED: [&str; 8] = ["a", "b", "c", "d", "e", "f", "g", "h"];
/// The new log start after the trim.
const OUT_OF_RANGE_TRIM: i64 = 5;
/// Polls that must come back empty before the `Latest` case produces again.
const RESET_SETTLE_POLLS: u32 = 3;

/// One `OFFSET_OUT_OF_RANGE` recovery scenario.
struct OutOfRangeCase {
    /// The policy the recovering consumer runs with.
    policy: AutoOffsetReset,
    /// Records produced after the reset has settled. `Latest` needs them,
    /// because its reset lands at the live log end and leaves nothing older to
    /// read. `Earliest` needs none, because its reset lands at the new log
    /// start and the records above the trim are still there.
    produced_after_reset: &'static [&'static str],
    /// The values the recovered consumer must deliver, in order.
    expected: &'static [&'static str],
}

/// Drive one `OFFSET_OUT_OF_RANGE` recovery scenario.
///
/// A consumer sits at offset 0 while `DeleteRecords` moves the log start to 5.
/// The next `Fetch` from 0 is below the log start, so the broker returns
/// `OFFSET_OUT_OF_RANGE` (code 1). The error-first poll loop then resets by
/// policy. Neither policy may surface an error, and neither may deliver a
/// record below the new log start.
async fn run_out_of_range_reset(case: &OutOfRangeCase) {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let topic = support::unique("oor");
    let group = support::unique("oor-grp");

    let producer = support::bootstrap_client(&kafka.bootstrap).await;
    support::create_topic(&producer, &topic).await;
    support::produce(&producer, &topic, &OUT_OF_RANGE_SEED).await;

    // Trim the log start to 5. A consumer that starts at 0 is now below the
    // log. The returned low watermark is the broker's own new log start.
    let low = delete_records_before(&producer, &topic, 0, OUT_OF_RANGE_TRIM).await;
    assert!(low == OUT_OF_RANGE_TRIM);

    let mut consumer = Consumer::builder()
        .bootstrap(&kafka.bootstrap)
        .client_id("c")
        .group_id(&group)
        .session_timeout(secs(30))
        .rebalance_timeout(secs(10))
        .heartbeat_interval(secs(1))
        .auto_offset_reset(case.policy)
        .subscribe([topic.clone()])
        .build()
        .await
        .expect("build consumer");

    if !case.produced_after_reset.is_empty() {
        settle_latest_reset(&mut consumer).await;
        support::produce(&producer, &topic, case.produced_after_reset).await;
    }

    let records = collect_records(&mut consumer, case.expected.len()).await;
    for record in &records {
        // An offset below the trim means the consumer fetched from 0 again
        // instead of from the recovered position. That is the fault this case
        // catches.
        assert!(
            record.offset >= OUT_OF_RANGE_TRIM,
            "record below the new log start: {record:?}"
        );
    }
    let seen: Vec<String> = records.iter().map(value_of).collect();
    assert!(seen == case.expected);

    consumer.close().await.expect("close consumer");
    producer.close();
    drop(kafka);
}

/// Wait until the `Latest` reset has taken effect.
///
/// `Latest` writes the `i64::MAX` sentinel and the next poll resolves it to the
/// live log end with `ListOffsets(-1)`. Records produced before that resolution
/// would land below the resolved position and never arrive, so the fresh
/// records must wait for it. Each poll must come back `Ok` and empty: an error
/// here means the policy did not reset.
async fn settle_latest_reset(consumer: &mut Consumer) {
    wait_for_assignment(consumer, 1, "consumer before the Latest reset").await;
    for _ in 0..RESET_SETTLE_POLLS {
        let records = consumer
            .poll(millis(400))
            .await
            .expect("OOR under Latest must reset, not error");
        assert!(
            records.is_empty(),
            "the Latest reset delivered stale records: {records:?}"
        );
    }
}

/// `OFFSET_OUT_OF_RANGE` recovery under `auto.offset.reset=latest`.
///
/// `Latest` writes the `i64::MAX` sentinel. The next poll resolves that
/// sentinel to the live log end with `ListOffsets(-1)`. The consumer then reads
/// the records produced after that point. This shows real recovery, with no
/// error and a resumed fetch, and not a silent stall.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn consumer_resets_on_offset_out_of_range_latest() {
    run_out_of_range_reset(&OutOfRangeCase {
        policy: AutoOffsetReset::Latest,
        produced_after_reset: &["NEW1", "NEW2", "NEW3"],
        expected: &["NEW1", "NEW2", "NEW3"],
    })
    .await;
}

/// `OFFSET_OUT_OF_RANGE` recovery under `auto.offset.reset=earliest`.
///
/// `Earliest` must reset to the `log_start_offset` in the response, which is 5
/// here, and NOT to the literal 0. A reset to 0 would cause
/// `OFFSET_OUT_OF_RANGE` again without end, and that is the root cause this
/// case catches. After recovery the consumer starts again from the new log
/// start and delivers the records that survived the trim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn consumer_resets_on_offset_out_of_range_earliest() {
    run_out_of_range_reset(&OutOfRangeCase {
        policy: AutoOffsetReset::Earliest,
        produced_after_reset: &[],
        expected: &["f", "g", "h"],
    })
    .await;
}

/// Seat a below-trim committed offset for `group` on partition 0 of `topic`.
///
/// An `Earliest` seed member primes `next_offset` to 0 during assignment and
/// commits that 0. The caller then trims the log past it. `assignment()` is
/// checked rather than `poll`, so the seed consumes no record and its position
/// stays at 0.
async fn seed_committed_offset_zero(bootstrap: &str, group: &str, topic: &str) {
    let seed = Consumer::builder()
        .bootstrap(bootstrap)
        .client_id("seed")
        .group_id(group)
        .session_timeout(secs(30))
        .rebalance_timeout(secs(10))
        .heartbeat_interval(secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic.to_string()])
        .build()
        .await
        .expect("build seed consumer");
    wait_for_assignment(&seed, 1, "seed consumer").await;
    seed.commit_sync().await.expect("seed commit of offset 0");
    seed.close().await.expect("close seed consumer");
}

/// `auto.offset.reset=none` reports `OFFSET_OUT_OF_RANGE` as an error.
///
/// The consumer returns `ConsumerError::LogTruncation` and does not reset
/// silently.
///
/// This case causes the same divergence as the `Latest` case: `DeleteRecords`
/// trims the log start past the consumer's offset. But the `None` policy makes
/// the out-of-range arm return an error instead of a safe offset. `poll` must
/// return `Err(ConsumerError::LogTruncation { .. })`. That error carries the
/// out-of-range fetch offset and, as the `safe_offset`, the `log_start_offset`
/// from the response.
///
/// A new `None` consumer starts at the `i64::MAX` sentinel, which resolves to
/// the live log end, so it is never out of range. To seat a below-trim position
/// deterministically, an `Earliest` seed consumer first commits offset 0 for
/// the group BEFORE the trim. `DeleteRecords` then moves the log start forward
/// past that committed 0. The `None` consumer inherits the below-trim committed
/// offset and gets `OFFSET_OUT_OF_RANGE`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn consumer_none_policy_surfaces_log_truncation() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let topic = support::unique("oor-none");
    let group = support::unique("oor-none-grp");

    let producer = support::bootstrap_client(&kafka.bootstrap).await;
    support::create_topic(&producer, &topic).await;
    support::produce(&producer, &topic, &["a", "b", "c", "d", "e", "f"]).await;

    seed_committed_offset_zero(&kafka.bootstrap, &group, &topic).await;

    // Trim past offset 0. The group's committed 0 is now below the log start.
    let low = delete_records_before(&producer, &topic, 0, 4).await;
    assert!(low == 4);

    let mut consumer = Consumer::builder()
        .bootstrap(&kafka.bootstrap)
        .client_id("c")
        .group_id(&group)
        .session_timeout(secs(30))
        .rebalance_timeout(secs(10))
        .heartbeat_interval(secs(1))
        .auto_offset_reset(AutoOffsetReset::None)
        .subscribe([topic.clone()])
        .build()
        .await
        .expect("build consumer");

    // The committed offset (0) is below the log start (4), so the fetch gets
    // `OFFSET_OUT_OF_RANGE` and `None` surfaces `LogTruncation`. Empty polls are
    // tolerated while the assignment settles, but the first non-empty outcome
    // must be the error.
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    let mut got_truncation = false;
    while Instant::now() < deadline {
        match consumer.poll(millis(300)).await {
            Ok(records) => assert!(records.is_empty(), "{records:?}"),
            Err(ConsumerError::LogTruncation {
                topic: reported_topic,
                partition,
                fetch_offset,
                ..
            }) => {
                assert!(reported_topic == topic);
                assert!(partition == 0);
                check!(
                    fetch_offset == 0,
                    "fetch_offset should be the out-of-range offset 0, got {fetch_offset}"
                );
                got_truncation = true;
                break;
            }
            Err(other) => panic!("expected LogTruncation, got {other:?}"),
        }
    }
    assert!(got_truncation);

    consumer.close().await.expect("close consumer");
    producer.close();
    drop(kafka);
}

/// Build an `OffsetFetch` request that is valid at whatever version the broker
/// negotiates.
///
/// The legacy `group_id` plus `topics` fields carry v0-7 and the `groups` array
/// carries v8+. The codegen encodes only the set the negotiated version needs,
/// so one request covers both. cp-kafka 6.1.1 is Apache Kafka 2.7, which tops
/// out at v7, so the container reads the legacy fields.
fn offset_fetch_for(group: &str, topic: &str, topic_id: WireUuid) -> OffsetFetchRequest {
    OffsetFetchRequest {
        group_id: group.into(),
        topics: Some(vec![OffsetFetchRequestTopic {
            name: topic.into(),
            partition_indexes: vec![0],
            ..Default::default()
        }]),
        groups: vec![OffsetFetchRequestGroup {
            group_id: group.into(),
            topics: Some(vec![OffsetFetchRequestTopics {
                name: topic.into(),
                topic_id,
                partition_indexes: vec![0],
                ..Default::default()
            }]),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Read partition 0's `(committed_offset, committed_leader_epoch)` out of an
/// `OffsetFetch` response, across either wire shape.
///
/// v0-7 puts the rows in the top-level `topics`, and v8+ puts them under
/// `groups`. A row with a non-zero error code is skipped, so a coordinator that
/// is still loading `__consumer_offsets` reads as "no row yet".
fn committed_row(resp: &OffsetFetchResponse) -> Option<(i64, i32)> {
    for topic in &resp.topics {
        for p in &topic.partitions {
            if p.partition_index == 0 && p.error_code == 0 {
                return Some((p.committed_offset, p.committed_leader_epoch));
            }
        }
    }
    for group in &resp.groups {
        for topic in &group.topics {
            for p in &topic.partitions {
                if p.partition_index == 0 && p.error_code == 0 {
                    return Some((p.committed_offset, p.committed_leader_epoch));
                }
            }
        }
    }
    None
}

/// Read the committed offset and leader epoch for partition 0 of `topic`.
///
/// A broker that has just restarted still loads `__consumer_offsets`, so it can
/// answer `COORDINATOR_LOAD_IN_PROGRESS` for a while. The bounded retry covers
/// that window.
async fn committed_offset_and_epoch(
    client: &Client,
    group: &str,
    topic: &str,
    topic_id: WireUuid,
) -> (i64, i32) {
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    loop {
        let resp = client
            .send(offset_fetch_for(group, topic, topic_id))
            .await
            .expect("OffsetFetch");
        if let Some(row) = committed_row(&resp) {
            return row;
        }
        assert!(
            Instant::now() < deadline,
            "OffsetFetch never returned a usable partition row for {group}"
        );
        broker_side_pause().await;
    }
}

/// The `committed_leader_epoch` survives a broker restart.
///
/// The consumer commits the epoch it consumed, and `OffsetFetch` reads that
/// epoch back. The consumer needs this round trip to seed
/// `positions[..].offset_epoch`, so that a later leader-epoch bump can start
/// KIP-320 validation.
///
/// The case produces the records at the partition's natural leader epoch, which
/// is 0. The consumer's `Fetch` carries `current_leader_epoch = 0` and matches
/// the broker, so the broker does not epoch-fence the fetch and the consumer
/// sees `ConsumerRecord.leader_epoch == 0`. After the consume and the commit,
/// the committed epoch must read back as exactly 0 across a broker restart. It
/// must NOT read back as the -1 "no epoch committed" sentinel that an
/// uncommitted partition gives.
///
/// The difference between 0 and -1 proves that the consumer sent the consumed
/// epoch through `OffsetCommit` and that the epoch came back through
/// `OffsetFetch`. The case checks an unrelated never-committed group as the -1
/// control.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn committed_leader_epoch_survives_restart() {
    support::init_tracing();
    let mut kafka = support::start_kafka().await;
    let topic = support::unique("epoch-persist");
    let group = support::unique("epoch-grp");
    let control_group = support::unique("never-committed-grp");
    let topic_uuid;

    // First run: produce, consume, check the records carry epoch 0, commit.
    {
        let producer = support::bootstrap_client(&kafka.bootstrap).await;
        support::create_topic(&producer, &topic).await;
        topic_uuid = support::topic_id_for(&producer, &topic).await;
        support::produce(&producer, &topic, &["e0", "e1", "e2"]).await;

        let mut consumer = Consumer::builder()
            .bootstrap(&kafka.bootstrap)
            .client_id("c")
            .group_id(&group)
            .session_timeout(secs(30))
            .rebalance_timeout(secs(10))
            .heartbeat_interval(secs(1))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe([topic.clone()])
            .build()
            .await
            .expect("build consumer");

        let epochs: Vec<i32> = collect_records(&mut consumer, 3)
            .await
            .iter()
            .map(|r| r.leader_epoch)
            .collect();
        assert!(epochs == vec![0, 0, 0]);

        consumer.commit_sync().await.expect("commit");
        consumer.close().await.expect("close consumer");
        producer.close();
    }

    kafka.restart().await;

    // Second run: read the committed offset back and check that the committed
    // leader epoch survived as 0, not as the -1 "absent" sentinel.
    {
        let client = support::bootstrap_client(&kafka.bootstrap).await;
        let committed = committed_offset_and_epoch(&client, &group, &topic, topic_uuid).await;
        assert!(committed == (3, 0));

        // Control: a never-committed group gives the -1 "absent" sentinel, so
        // the epoch 0 above is a real committed value and not a default.
        let control = committed_offset_and_epoch(&client, &control_group, &topic, topic_uuid).await;
        assert!(control == (-1, -1));

        client.close();
    }

    drop(kafka);
}

/// Regression for the WAL-consumer cold-start hang.
///
/// A single-member group that JOINS before its subscribed topic exists gets a
/// 0-partition assignment. Recovery needs the coordinator's metadata refresh to
/// see that the topic appeared and to rejoin. Without that loop the empty
/// assignment stayed empty for ever, which is the `logs-compactor` and
/// `profiles-block-builder` hang.
///
/// No second member ever joins. The case also guards the TOCTOU fix: the
/// coordinator seeds its rejoin baseline from the snapshot that it used for the
/// *initial* empty assignment, so a topic created at any time after the join,
/// and also during start-up, still reads as growth.
///
/// The container broker also watches the subscribed topics of a stable group
/// and can start a rebalance from its own side when the partition count grows.
/// So against this broker the case no longer isolates the client-only refresh
/// path. What it still proves is the outcome: the cold-started member ends up
/// with both partitions and delivers every record from them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn cold_start_rejoins_when_subscribed_topic_appears() {
    support::init_tracing();
    let kafka = support::start_kafka().await;
    let topic = support::unique("wal-late");
    let group = support::unique("cold-start-grp");

    let producer = support::bootstrap_client(&kafka.bootstrap).await;

    // Subscribe to a topic that does NOT exist yet. The join still succeeds and
    // the assignment is empty, which is the cold start the WAL consumers hit.
    let mut consumer = Consumer::builder()
        .bootstrap(&kafka.bootstrap)
        .client_id("c")
        .group_id(&group)
        .session_timeout(secs(30))
        .rebalance_timeout(secs(10))
        .heartbeat_interval(millis(500))
        .subscription_metadata_refresh_interval(millis(750))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic.clone()])
        .build()
        .await
        .expect("consumer builds even though its subscribed topic does not exist yet");

    // Let the initial join settle, then check it really is the empty-assignment
    // cold start: a single stable member with nothing to consume.
    let _ = consumer.poll(millis(200)).await;
    assert!(consumer.assignment().await.is_empty());

    // The distributor creates the WAL topic AFTER the consumer has joined.
    support::create_topic_with_partitions(&producer, &topic, 2).await;

    poll_until_assignment(
        &mut consumer,
        2,
        "cold-started member after the topic appears",
    )
    .await;

    // The recovered assignment must work: produce to both partitions and check
    // that every record arrives. That proves the re-acquired partitions primed
    // their fetch offsets.
    support::produce_to_partition(&producer, &topic, 0, &["p0a", "p0b"]).await;
    support::produce_to_partition(&producer, &topic, 1, &["p1a", "p1b"]).await;

    let seen = collect_prefixed(&mut consumer, 'p', 4).await;
    let expected: HashSet<String> = ["p0a", "p0b", "p1a", "p1b"]
        .into_iter()
        .map(String::from)
        .collect();
    assert!(seen == expected);

    consumer.close().await.expect("close consumer");
    producer.close();
    drop(kafka);
}
