//! End-to-end test of the KIP-113 log-dir admin RPCs.
//!
//! The suite drives `AlterReplicaLogDirs` and `DescribeLogDirs` through
//! `AdminClient`. It runs the typed wrappers in
//! `crates/client-admin/src/log_dirs.rs` against a broker.
//!
//! The broker needs two entries in `log.dirs`, which the default container
//! image does not have. The case therefore builds the image with
//! `testcontainers::ImageExt` and starts it through
//! `support::start_kafka_with`. Both directories are paths inside the
//! container, so the reported `log_dir` compares against the container path
//! directly. A host-side `canonicalize` would name a path that the broker
//! never reports.
//!
//! The case needs the container, so it carries `#[ignore]`.

mod support;

use std::{collections::BTreeMap, time::Duration};

use assert2::assert;
use krabka_client_admin::{AdminClient, LogDirInfo};
use testcontainers::ImageExt as _;
use testcontainers_modules::kafka::Kafka;

/// The first `log.dirs` entry, which is the default of the image.
const PRIMARY_LOG_DIR: &str = "/var/lib/kafka/data";
/// The second `log.dirs` entry, which is the target of the move.
const TARGET_LOG_DIR: &str = "/var/lib/kafka/data2";

/// Bound on a read-back loop. The broker moves a replica in its own time.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Pause between two read-backs.
const POLL_INTERVAL: Duration = Duration::from_millis(200);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn admin_log_dirs_alter_then_describe_converges() {
    support::init_tracing();
    let image = Kafka::default().with_env_var(
        "KAFKA_LOG_DIRS",
        format!("{PRIMARY_LOG_DIR},{TARGET_LOG_DIR}"),
    );
    let kafka = support::start_kafka_with(image).await;
    let mut admin = support::admin(&kafka.bootstrap).await;

    // A two-partition topic lets KIP-113 placement spread the partitions over
    // both configured log dirs.
    let topic = support::unique("t");
    support::create_topic(&mut admin, &topic, 2).await;

    // Wait until the broker holds both partitions on disk. The old in-process
    // harness had a `wait_until_partition_present` hook for this. The report
    // that `DescribeLogDirs` gives is the equivalent signal here.
    await_partitions(
        &mut admin,
        &topic,
        None,
        "both partitions exist in a log dir",
    )
    .await;

    assert_initial_report(&mut admin, &topic).await;
    move_partitions_to_target(&mut admin, &topic).await;

    // Poll until both partitions are current logs in the target dir with no
    // future log left behind.
    await_partitions(
        &mut admin,
        &topic,
        Some(TARGET_LOG_DIR),
        "both partitions moved into the target log dir",
    )
    .await;

    assert_filtered_report(&mut admin, &topic).await;
}

/// The report before the move names both dirs and holds no future log.
async fn assert_initial_report(admin: &mut AdminClient, topic: &str) {
    let initial = admin
        .describe_log_dirs(None)
        .await
        .expect("describe_log_dirs");
    let mut reported: Vec<&str> = initial.iter().map(|d| d.log_dir.as_str()).collect();
    reported.sort_unstable();
    assert!(reported == vec![PRIMARY_LOG_DIR, TARGET_LOG_DIR]);

    for dir in &initial {
        assert!(dir.error.is_none(), "{dir:?}");
        for entry in &dir.topics {
            for partition in &entry.partitions {
                assert!(!partition.is_future_key, "{partition:?}");
                assert!(partition.partition_size >= 0, "{partition:?}");
            }
        }
    }

    let (current, any_future) = partitions_of(&initial, topic, None);
    assert!(current == vec![0, 1]);
    assert!(!any_future);
}

/// Move both partitions into the target dir.
///
/// `AlterReplicaLogDirs` takes the last entry per topic and partition when the
/// wire message lists one twice. The request below lists each partition once.
async fn move_partitions_to_target(admin: &mut AdminClient, topic: &str) {
    let mut assignments: BTreeMap<String, Vec<(String, Vec<i32>)>> = BTreeMap::new();
    assignments.insert(
        TARGET_LOG_DIR.to_owned(),
        vec![(topic.to_owned(), vec![0, 1])],
    );
    let outcomes = admin
        .alter_replica_log_dirs(&assignments)
        .await
        .expect("alter_replica_log_dirs");
    assert!(outcomes.len() == 2);
    for outcome in &outcomes {
        assert!(outcome.error.is_none(), "{outcome:?}");
    }
}

/// A filtered describe, for the one topic, still sees both partitions in the
/// target dir.
async fn assert_filtered_report(admin: &mut AdminClient, topic: &str) {
    // An empty partition list means every partition of that topic.
    let filter = BTreeMap::from([(topic.to_owned(), Vec::new())]);
    let filtered = admin
        .describe_log_dirs(Some(&filter))
        .await
        .expect("filtered describe_log_dirs");
    let (current, any_future) = partitions_of(&filtered, topic, Some(TARGET_LOG_DIR));
    assert!(current == vec![0, 1]);
    assert!(!any_future);
}

/// Read the log-dir report until it shows partitions 0 and 1 of `topic` as
/// current logs, and no future log for that topic.
///
/// `in_dir` narrows the report to one log dir. `None` accepts any dir.
///
/// The pause between two reads is a real-time wait on a separate process, not
/// a poll of progress that this task makes. The broker copies the log segments
/// in its own time and gives this side no signal to await.
async fn await_partitions(admin: &mut AdminClient, topic: &str, in_dir: Option<&str>, what: &str) {
    tokio::time::timeout(SETTLE_TIMEOUT, async {
        loop {
            let report = admin
                .describe_log_dirs(None)
                .await
                .expect("describe_log_dirs");
            let (current, any_future) = partitions_of(&report, topic, in_dir);
            if !any_future && current == vec![0, 1] {
                break;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the broker did not report that {what} within {SETTLE_TIMEOUT:?}"));
}

/// The partitions of `topic` that the report holds as current logs, with
/// `true` when the report still holds a future log for the topic.
fn partitions_of(report: &[LogDirInfo], topic: &str, in_dir: Option<&str>) -> (Vec<i32>, bool) {
    let mut current = Vec::new();
    let mut any_future = false;
    for dir in report {
        if in_dir.is_some_and(|want| want != dir.log_dir) {
            continue;
        }
        for entry in dir.topics.iter().filter(|t| t.name == topic) {
            for partition in &entry.partitions {
                if partition.is_future_key {
                    any_future = true;
                } else {
                    current.push(partition.partition_index);
                }
            }
        }
    }
    current.sort_unstable();
    current.dedup();
    (current, any_future)
}
