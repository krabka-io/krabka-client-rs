//! KIP-516 offset wire-shape helpers.
//!
//! The consumer sends `OffsetCommit` and `OffsetFetch` at v9 or lower, where
//! each topic has its name. Apache Kafka's classic `ConsumerCoordinator`
//! builds both requests with the `forTopicNames` builder, which caps the
//! version at 9 (`sendOffsetCommitRequest`, `sendOffsetFetchRequest`). At v8+
//! `OffsetFetch` carries a per-group `groups[]` array, and the legacy
//! `group_id` + `topics` fields are v0-7 only.
//!
//! The builders populate BOTH the legacy and the new fields. The codegen
//! encodes only the set that is valid for the negotiated version, so one
//! request works regardless of what the broker negotiated. The parser flattens
//! an `OffsetFetch` response across either shape.

use std::{
    collections::{BTreeSet, HashMap},
    iter,
};

pub use krabka_client_core::TopicNameOffsetFetch;
use krabka_protocol::{
    owned::{
        offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
        offset_fetch_request::{
            OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
            OffsetFetchRequestTopics,
        },
        offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    commit::OffsetAndMetadata,
    coordinator::{COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR},
};

/// An `OffsetFetch` request that names its topics, capped at v9.
///
/// `OffsetFetch` v10 names each topic by id only. Apache Kafka's classic
/// `ConsumerCoordinator.sendOffsetFetchRequest` uses
/// `OffsetFetchRequest.Builder.forTopicNames`, which allows
/// `ApiKeys.OFFSET_FETCH.oldestVersion()` to `TOPIC_ID_MIN_VERSION - 1` (9).
/// This type gives the same range to version negotiation. When the coordinator
/// supports only v10 or higher, the send fails with
/// `ClientError::IncompatibleVersion` before the request goes out. Kafka's
/// builder fails with `UnsupportedVersionException` in that case.
/// Build an `OffsetFetch` request that covers `by_topic` and is valid at each
/// version from v1 to v9.
///
/// This function populates both the legacy `group_id`/`topics` fields for v0-7
/// and the v8+ `groups[]` array. Each topic has its name and no topic id, as
/// in Kafka's `ConsumerCoordinator.sendOffsetFetchRequest`.
///
/// The request sets `require_stable`, as Apache Kafka's consumers do
/// (`CommitRequestManager.OffsetFetchRequestState.toUnsentRequest` and
/// `ConsumerCoordinator.sendOffsetFetchRequest`). From v7 the coordinator then
/// answers `UNSTABLE_OFFSET_COMMIT` (88) for a partition with a pending
/// transactional offset commit, and `send_offset_fetch` asks again. Below v7
/// the field is not on the wire. Kafka's `OffsetFetchRequest.Builder` also
/// drops the flag there (`throwIfStableOffsetsUnsupported`), because
/// `internal.throw.on.fetch.stable.offset.unsupported` defaults to `false`.
pub fn build_offset_fetch(
    group_id: &str,
    by_topic: &HashMap<String, Vec<i32>>,
) -> TopicNameOffsetFetch {
    let legacy_topics: Vec<OffsetFetchRequestTopic> = by_topic
        .iter()
        .map(|(name, parts)| OffsetFetchRequestTopic {
            name: name.clone(),
            partition_indexes: parts.clone(),
            ..Default::default()
        })
        .collect();
    let group_topics: Vec<OffsetFetchRequestTopics> = by_topic
        .iter()
        .map(|(name, parts)| OffsetFetchRequestTopics {
            name: name.clone(),
            partition_indexes: parts.clone(),
            ..Default::default()
        })
        .collect();
    TopicNameOffsetFetch::new(OffsetFetchRequest {
        group_id: group_id.to_owned(),
        topics: Some(legacy_topics),
        groups: vec![OffsetFetchRequestGroup {
            group_id: group_id.to_owned(),
            topics: Some(group_topics),
            ..Default::default()
        }],
        require_stable: true,
        ..Default::default()
    })
}

/// Flatten an `OffsetFetch` response into `(topic_name, partition,
/// committed_offset, committed_leader_epoch)` tuples.
///
/// v8 and v9 data lives in `groups`, and v0-7 data lives in `topics`.
pub fn parse_offset_fetch(resp: &OffsetFetchResponse) -> Vec<(String, i32, i64, i32)> {
    let mut out = Vec::new();
    if resp.groups.is_empty() {
        for t in &resp.topics {
            append_topic_offsets(&mut out, &t.name, &t.partitions);
        }
    } else {
        for g in &resp.groups {
            for t in &g.topics {
                append_topic_offsets(&mut out, &t.name, &t.partitions);
            }
        }
    }
    out
}

trait OffsetFetchPartitionData {
    fn offset_fields(&self) -> (i32, i64, i32);
    fn error_code(&self) -> i16;
}

impl OffsetFetchPartitionData for OffsetFetchResponsePartition {
    fn offset_fields(&self) -> (i32, i64, i32) {
        (
            self.partition_index,
            self.committed_offset,
            self.committed_leader_epoch,
        )
    }

    fn error_code(&self) -> i16 {
        self.error_code
    }
}

impl OffsetFetchPartitionData for OffsetFetchResponsePartitions {
    fn offset_fields(&self) -> (i32, i64, i32) {
        (
            self.partition_index,
            self.committed_offset,
            self.committed_leader_epoch,
        )
    }

    fn error_code(&self) -> i16 {
        self.error_code
    }
}

fn append_topic_offsets<P: OffsetFetchPartitionData>(
    out: &mut Vec<(String, i32, i64, i32)>,
    topic: &str,
    partitions: &[P],
) {
    out.extend(partitions.iter().map(|partition| {
        let (index, offset, leader_epoch) = partition.offset_fields();
        (topic.to_owned(), index, offset, leader_epoch)
    }));
}

fn append_topic_errors<'a, P: OffsetFetchPartitionData>(
    out: &mut Vec<(&'a str, i16)>,
    topic: &'a str,
    partitions: &[P],
) {
    out.extend(
        partitions
            .iter()
            .map(|partition| (topic, partition.error_code())),
    );
}

/// The committed offsets of an `OffsetFetch` response. Kafka's
/// `ConsumerCoordinator.OffsetFetchResponseHandler`: a partition with offset
/// `-1` or an error has no committed offset.
///
/// v8 and v9 data lives in `groups`, and v0-7 data lives in `topics`.
pub fn parse_committed_offsets(
    resp: &OffsetFetchResponse,
) -> HashMap<(String, i32), Option<OffsetAndMetadata>> {
    let committed = |error_code: i16, offset: i64, leader_epoch: i32, metadata: &Option<String>| {
        (error_code == 0 && offset >= 0).then(|| OffsetAndMetadata {
            offset,
            leader_epoch: (leader_epoch >= 0).then_some(leader_epoch),
            metadata: metadata.clone().unwrap_or_default(),
        })
    };
    let mut out = HashMap::new();
    for t in &resp.topics {
        for p in &t.partitions {
            out.insert(
                (t.name.clone(), p.partition_index),
                committed(
                    p.error_code,
                    p.committed_offset,
                    p.committed_leader_epoch,
                    &p.metadata,
                ),
            );
        }
    }
    for g in &resp.groups {
        for t in &g.topics {
            for p in &t.partitions {
                out.insert(
                    (t.name.clone(), p.partition_index),
                    committed(
                        p.error_code,
                        p.committed_offset,
                        p.committed_leader_epoch,
                        &p.metadata,
                    ),
                );
            }
        }
    }
    out
}

/// `UNKNOWN_TOPIC_OR_PARTITION`: the coordinator does not know the topic.
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// `TOPIC_AUTHORIZATION_FAILED`: the principal cannot describe the topic.
const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
/// `GROUP_AUTHORIZATION_FAILED`: the principal cannot describe the group.
const GROUP_AUTHORIZATION_FAILED: i16 = 30;
/// `UNSTABLE_OFFSET_COMMIT`: a transaction or a replication holds the offset.
const UNSTABLE_OFFSET_COMMIT: i16 = 88;
/// `UNKNOWN_TOPIC_ID`: the coordinator does not hold the topic id.
const UNKNOWN_TOPIC_ID: i16 = 100;

/// What the consumer does with one `OffsetFetch` response.
///
/// The mapping follows Apache Kafka's `CommitRequestManager.OffsetFetchRequestState`
/// (`onResponse`, `onFailure` and `onSuccess`) and
/// `CommitRequestManager.fetchOffsetsWithRetries`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OffsetFetchAction {
    /// Use the committed offsets in the response.
    Complete,
    /// A partition answered 3 or 100. Send the request again until the
    /// deadline. After the deadline, use the response. Its errored partitions
    /// start from the reset policy.
    RetryUnknownTopic(i16),
    /// The group answered a retriable code, or a partition answered 88. Send
    /// the request again until the deadline, then fail with the code.
    Retry(i16),
    /// The group answered 15 or 16. Find the coordinator again and send the
    /// request again until the deadline, then fail with the code.
    FindCoordinator(i16),
    /// The group answered 30. Fail.
    GroupAuthorizationFailed,
    /// Partitions answered 29. Fail with the names of their topics.
    TopicAuthorizationFailed(BTreeSet<String>),
    /// The group or a partition answered a code that Kafka does not retry.
    /// Fail with the code.
    Fatal(i16),
}

/// Classify an `OffsetFetch` response as Apache Kafka's consumer does.
///
/// A group error code comes first. Kafka's `OffsetFetchRequestState.onFailure`
/// finds the coordinator again for 15 and 16, fails with a group authorization
/// error for 30, retries every other `RetriableException` code, and fails for
/// all other codes.
///
/// Without a group error, `OffsetFetchRequestState.onSuccess` reads each
/// partition in order. The first code other than 0, 3, 29, 88 and 100 fails
/// the fetch at once. After the loop, 29 fails the fetch with the topic names,
/// then 88 makes it retriable, then 3 and 100 make it retriable with partial
/// results.
///
/// v8 and v9 data lives in `groups`, and v0-7 data lives in the top-level
/// fields.
pub fn classify_offset_fetch(resp: &OffsetFetchResponse) -> OffsetFetchAction {
    let group_error = iter::once(resp.error_code)
        .chain(resp.groups.iter().map(|g| g.error_code))
        .find(|code| *code != 0);
    if let Some(code) = group_error {
        return classify_group_error(code);
    }

    let mut partitions = Vec::new();
    for topic in &resp.topics {
        append_topic_errors(&mut partitions, &topic.name, &topic.partitions);
    }
    for group in &resp.groups {
        for topic in &group.topics {
            append_topic_errors(&mut partitions, &topic.name, &topic.partitions);
        }
    }
    let mut unauthorized_topics = BTreeSet::new();
    let mut unstable = false;
    let mut unknown_topic = None;
    for (topic, code) in partitions {
        match code {
            0 => {}
            UNKNOWN_TOPIC_OR_PARTITION | UNKNOWN_TOPIC_ID => {
                unknown_topic.get_or_insert(code);
            }
            TOPIC_AUTHORIZATION_FAILED => {
                unauthorized_topics.insert(topic.to_owned());
            }
            UNSTABLE_OFFSET_COMMIT => unstable = true,
            code => return OffsetFetchAction::Fatal(code),
        }
    }
    if !unauthorized_topics.is_empty() {
        OffsetFetchAction::TopicAuthorizationFailed(unauthorized_topics)
    } else if unstable {
        OffsetFetchAction::Retry(UNSTABLE_OFFSET_COMMIT)
    } else if let Some(code) = unknown_topic {
        OffsetFetchAction::RetryUnknownTopic(code)
    } else {
        OffsetFetchAction::Complete
    }
}

fn classify_group_error(code: i16) -> OffsetFetchAction {
    match code {
        COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR => OffsetFetchAction::FindCoordinator(code),
        GROUP_AUTHORIZATION_FAILED => OffsetFetchAction::GroupAuthorizationFailed,
        code if is_retriable_error(code) => OffsetFetchAction::Retry(code),
        code => OffsetFetchAction::Fatal(code),
    }
}

/// Whether Apache Kafka's `common/protocol/Errors` maps `code` to an exception
/// that extends `RetriableException`.
pub fn is_retriable_error(code: i16) -> bool {
    matches!(
        code,
        2 | 3
            | 5
            | 6
            | 7
            | 9
            | 13
            | 14
            | 15
            | 16
            | 19
            | 20
            | 41
            | 51
            | 56
            | 70
            | 71
            | 72
            | 74
            | 75
            | 78
            | 80
            | 83
            | 84
            | 88
            | 89
            | 100
            | 103
            | 106
            | 122
            | 123
            | 133
    )
}

/// An `OffsetCommit` request that names its topics, capped at v9.
pub use krabka_client_core::TopicNameOffsetCommit;

/// Build the `topics` for an `OffsetCommit`.
///
/// Each topic has its name and no topic id, as in Kafka's
/// `ConsumerCoordinator.sendOffsetCommitRequest`. `offsets` maps `(topic,
/// partition)` to `(committed_offset, committed_leader_epoch)`.
pub fn build_commit_topics(
    offsets: HashMap<(String, i32), OffsetAndMetadata>,
) -> Vec<OffsetCommitRequestTopic> {
    let mut by_topic: HashMap<String, Vec<(i32, OffsetAndMetadata)>> = HashMap::new();
    for ((t, p), offset) in offsets {
        by_topic.entry(t).or_default().push((p, offset));
    }
    by_topic
        .into_iter()
        .map(|(name, parts)| OffsetCommitRequestTopic {
            name,
            partitions: parts
                .into_iter()
                .map(|(p, offset)| OffsetCommitRequestPartition {
                    partition_index: p,
                    committed_offset: offset.offset,
                    committed_leader_epoch: offset.leader_epoch.unwrap_or(-1),
                    committed_metadata: Some(offset.metadata),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect()
}

/// Build the `topic_id → name` reverse map from the consumer's `name →
/// topic_id` table. The fetch path uses it to resolve `Fetch` v13+ responses,
/// which name each topic by id only.
pub fn id_to_name(topic_ids: &HashMap<String, WireUuid>) -> HashMap<WireUuid, String> {
    topic_ids.iter().map(|(n, id)| (*id, n.clone())).collect()
}

#[cfg(test)]
mod tests {

    use krabka_protocol::{
        UnknownTaggedFields,
        owned::offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
            OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
        },
    };

    use super::*;
    use crate::test_support::wire_uuid;

    #[test]
    fn build_offset_fetch_populates_legacy_and_groups() {
        let mut by_topic = HashMap::new();
        by_topic.insert("t".to_owned(), vec![0, 1]);

        let req = build_offset_fetch("g", &by_topic);
        // Legacy single-group fields (v0-7) AND v8+ groups[] by topic name.
        assert2::assert!(
            req == TopicNameOffsetFetch::new(OffsetFetchRequest {
                group_id: "g".to_owned(),
                topics: Some(vec![OffsetFetchRequestTopic {
                    name: "t".to_owned(),
                    partition_indexes: vec![0, 1],
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }]),
                groups: vec![OffsetFetchRequestGroup {
                    group_id: "g".to_owned(),
                    member_id: None,
                    member_epoch: -1,
                    topics: Some(vec![OffsetFetchRequestTopics {
                        name: "t".to_owned(),
                        topic_id: WireUuid::ZERO,
                        partition_indexes: vec![0, 1],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }]),
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                require_stable: true,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            })
        );
    }

    /// The topic name of `n` in the `grouped` rows.
    fn topic_name(n: u8) -> String {
        match n {
            1 => "orders".into(),
            2 => "payments".into(),
            _ => "shipments".into(),
        }
    }

    trait MissingOffsetFetchPartition {
        fn with_error_code(error_code: i16) -> Self;
    }

    impl MissingOffsetFetchPartition for OffsetFetchResponsePartition {
        fn with_error_code(error_code: i16) -> Self {
            Self {
                committed_offset: -1,
                error_code,
                ..Default::default()
            }
        }
    }

    impl MissingOffsetFetchPartition for OffsetFetchResponsePartitions {
        fn with_error_code(error_code: i16) -> Self {
            Self {
                committed_offset: -1,
                error_code,
                ..Default::default()
            }
        }
    }

    /// A v8 or v9 response for group `g`. Each row is `(topic number, partition
    /// error code)`, and `topic_name` gives the name of the topic number.
    fn grouped(group_error: i16, rows: &[(u8, i16)]) -> OffsetFetchResponse {
        OffsetFetchResponse {
            groups: vec![OffsetFetchResponseGroup {
                group_id: "g".into(),
                error_code: group_error,
                topics: rows
                    .iter()
                    .map(|(topic, error_code)| OffsetFetchResponseTopics {
                        name: topic_name(*topic),
                        partitions: vec![OffsetFetchResponsePartitions::with_error_code(
                            *error_code,
                        )],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// A v2-7 response. Each row is `(topic name, partition error code)`.
    fn legacy(group_error: i16, rows: &[(&str, i16)]) -> OffsetFetchResponse {
        OffsetFetchResponse {
            error_code: group_error,
            topics: rows
                .iter()
                .map(|(name, error_code)| OffsetFetchResponseTopic {
                    name: (*name).into(),
                    partitions: vec![OffsetFetchResponsePartition::with_error_code(*error_code)],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Kafka's `CommitRequestManager.OffsetFetchRequestState` maps each group
    /// and partition error code of an `OffsetFetch` response.
    #[test]
    fn classify_offset_fetch_maps_codes_as_kafka_does() {
        use OffsetFetchAction::{
            Complete, Fatal, FindCoordinator, GroupAuthorizationFailed, Retry, RetryUnknownTopic,
            TopicAuthorizationFailed,
        };
        let topics = |names: &[&str]| {
            TopicAuthorizationFailed(names.iter().map(|name| (*name).to_owned()).collect())
        };
        for (name, response, expected) in [
            ("no error", grouped(0, &[(1, 0), (2, 0)]), Complete),
            ("no rows", grouped(0, &[]), Complete),
            // Group codes: `onFailure`.
            ("coordinator load in progress", grouped(14, &[]), Retry(14)),
            (
                "coordinator not available",
                grouped(15, &[]),
                FindCoordinator(15),
            ),
            ("not coordinator", grouped(16, &[]), FindCoordinator(16)),
            ("request timed out", grouped(7, &[]), Retry(7)),
            ("network exception", grouped(13, &[]), Retry(13)),
            (
                "group authorization failed",
                grouped(30, &[]),
                GroupAuthorizationFailed,
            ),
            ("unknown member id", grouped(25, &[]), Fatal(25)),
            ("stale member epoch", grouped(113, &[]), Fatal(113)),
            ("group id not found", grouped(69, &[]), Fatal(69)),
            ("unknown server error", grouped(-1, &[]), Fatal(-1)),
            (
                "a group code hides the partition codes",
                grouped(14, &[(1, 29)]),
                Retry(14),
            ),
            // Partition codes: `onSuccess`.
            (
                "unknown topic or partition",
                grouped(0, &[(1, 0), (2, 3)]),
                RetryUnknownTopic(3),
            ),
            (
                "unknown topic id",
                grouped(0, &[(1, 100)]),
                RetryUnknownTopic(100),
            ),
            (
                "topic authorization failed names each topic",
                grouped(0, &[(1, 29), (2, 29), (3, 29)]),
                topics(&["orders", "payments", "shipments"]),
            ),
            (
                "unstable offset commit",
                grouped(0, &[(1, 88)]),
                Retry(UNSTABLE_OFFSET_COMMIT),
            ),
            (
                "not leader or follower is unexpected",
                grouped(0, &[(1, 6)]),
                Fatal(6),
            ),
            (
                "unknown member id on a partition is unexpected",
                grouped(0, &[(1, 25)]),
                Fatal(25),
            ),
            (
                "topic authorization comes before unstable offsets",
                grouped(0, &[(1, 88), (2, 29)]),
                topics(&["payments"]),
            ),
            (
                "unstable offsets come before unknown topics",
                grouped(0, &[(1, 100), (2, 88)]),
                Retry(UNSTABLE_OFFSET_COMMIT),
            ),
            (
                "an unexpected code comes before collected codes",
                grouped(0, &[(1, 29), (2, 6)]),
                Fatal(6),
            ),
            (
                "the first unexpected code wins",
                grouped(0, &[(1, 6), (2, 25)]),
                Fatal(6),
            ),
            // v2-7 responses.
            (
                "legacy group code",
                legacy(16, &[("orders", 0)]),
                FindCoordinator(16),
            ),
            (
                "legacy topic authorization failed",
                legacy(0, &[("orders", 29)]),
                topics(&["orders"]),
            ),
            (
                "legacy unknown topic or partition",
                legacy(0, &[("orders", 3)]),
                RetryUnknownTopic(3),
            ),
        ] {
            assert2::check!(classify_offset_fetch(&response) == expected, "case {name}");
        }
    }

    #[test]
    fn is_retriable_error_matches_kafka_retriable_exceptions() {
        let retriable = (-1..=140)
            .filter(|code| is_retriable_error(*code))
            .collect::<Vec<_>>();
        assert2::assert!(
            retriable
                == vec![
                    2, 3, 5, 6, 7, 9, 13, 14, 15, 16, 19, 20, 41, 51, 56, 70, 71, 72, 74, 75, 78,
                    80, 83, 84, 88, 89, 100, 103, 106, 122, 123, 133,
                ]
        );
    }

    #[test]
    fn parse_offset_fetch_reads_groups_with_name() {
        let resp = OffsetFetchResponse {
            groups: vec![OffsetFetchResponseGroup {
                group_id: "g".into(),
                topics: vec![OffsetFetchResponseTopics {
                    name: "t".into(),
                    partitions: vec![OffsetFetchResponsePartitions {
                        partition_index: 3,
                        committed_offset: 42,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let out = parse_offset_fetch(&resp);
        assert2::assert!(out == vec![("t".to_owned(), 3, 42, -1)]);
    }

    #[test]
    fn parse_offset_fetch_falls_back_to_legacy_topics() {
        // v0-7: no groups, data in the top-level `topics` field.
        let resp = OffsetFetchResponse {
            topics: vec![OffsetFetchResponseTopic {
                name: "legacy".into(),
                partitions: vec![OffsetFetchResponsePartition {
                    partition_index: 1,
                    committed_offset: 11,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let out = parse_offset_fetch(&resp);
        assert2::assert!(out == vec![("legacy".to_owned(), 1, 11, -1)]);
    }

    #[test]
    fn build_commit_topics_names_each_topic() {
        let mut offsets = HashMap::new();
        offsets.insert(
            ("t".to_owned(), 3),
            OffsetAndMetadata {
                offset: 100,
                leader_epoch: Some(5),
                metadata: "note".into(),
            },
        );
        let topics = build_commit_topics(offsets);
        assert2::assert!(
            topics
                == vec![OffsetCommitRequestTopic {
                    name: "t".to_owned(),
                    topic_id: WireUuid::ZERO,
                    partitions: vec![OffsetCommitRequestPartition {
                        partition_index: 3,
                        committed_offset: 100,
                        committed_leader_epoch: 5,
                        committed_metadata: Some("note".into()),
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }]
        );
    }

    #[test]
    fn id_to_name_inverts_the_map() {
        let mut ids = HashMap::new();
        ids.insert("t".to_owned(), wire_uuid(7));
        let inv = id_to_name(&ids);
        assert2::assert!(inv.get(&wire_uuid(7)) == Some(&"t".to_owned()));
    }
}
