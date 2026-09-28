//! Batched share-group offset listing:
//! [`AdminClient::list_share_group_offsets`].
//!
//! The call finds the coordinator of every group with one batched
//! `FindCoordinator` request and sends one `DescribeShareGroupOffsets`
//! request to each coordinator with every group that it coordinates, as
//! Apache Kafka's `ListShareGroupOffsetsHandler` does with its
//! `CoordinatorStrategy`. A request to a coordinator that
//! fails or loses its connection finds the coordinator again.

use std::collections::BTreeMap;

use krabka_protocol::owned::{
    describe_share_group_offsets_request::{
        DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsRequestGroup,
        DescribeShareGroupOffsetsRequestTopic,
    },
    describe_share_group_offsets_response::DescribeShareGroupOffsetsResponse,
};

use crate::{
    AdminClient, KafkaError,
    group_coordinators::{GroupOutcome, group_error_outcome},
    group_descriptions::request_error_outcomes,
};

/// The partitions to list for one share group, as Apache Kafka's
/// `ListShareGroupOffsetsSpec` holds them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListShareGroupOffsetsSpec {
    /// The partitions to list, or `None` for every partition that the group
    /// has an offset for.
    pub topic_partitions: Option<Vec<(String, i32)>>,
}

/// One partition's share-group offset, as Apache Kafka's
/// `SharePartitionOffsetInfo` holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharePartitionOffsetInfo {
    pub start_offset: i64,
    /// `None` when the coordinator gives a negative epoch.
    pub leader_epoch: Option<i32>,
    /// `None` when the coordinator gives a negative lag, as
    /// `DescribeShareGroupOffsets` v0 always does.
    pub lag: Option<i64>,
}

/// The offsets of one share group: each answered partition, with `None` for a
/// partition that has no start offset.
pub type ShareGroupOffsets = BTreeMap<(String, i32), Option<SharePartitionOffsetInfo>>;

/// The `DescribeShareGroupOffsets` request for `keys`, as Kafka's
/// `ListShareGroupOffsetsHandler.buildBatchedRequest` builds it.
fn describe_share_group_offsets_request(
    keys: &[String],
    specs: &BTreeMap<String, ListShareGroupOffsetsSpec>,
) -> DescribeShareGroupOffsetsRequest {
    DescribeShareGroupOffsetsRequest {
        groups: keys
            .iter()
            .map(|group_id| DescribeShareGroupOffsetsRequestGroup {
                group_id: group_id.clone(),
                topics: specs
                    .get(group_id)
                    .and_then(|spec| spec.topic_partitions.as_ref())
                    .map(|partitions| {
                        let mut topics = BTreeMap::<&str, Vec<i32>>::new();
                        for (topic, partition) in partitions {
                            topics.entry(topic).or_default().push(*partition);
                        }
                        topics
                            .into_iter()
                            .map(
                                |(topic_name, partitions)| DescribeShareGroupOffsetsRequestTopic {
                                    topic_name: topic_name.to_owned(),
                                    partitions,
                                    ..Default::default()
                                },
                            )
                            .collect()
                    }),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// The outcome of each group of `keys` in one `DescribeShareGroupOffsets`
/// answer, as Kafka's `ListShareGroupOffsetsHandler.handleResponse` reads it.
///
/// A group error retries as `handleGroupError` does. A partition with an
/// error code gives no entry, a negative start offset gives `None`, and a
/// negative leader epoch or lag gives `None` for that field. A group that the
/// answer omits completes with no partitions, as the handler completes each
/// key without a group error.
fn share_group_offsets_outcomes(
    keys: &[String],
    response: DescribeShareGroupOffsetsResponse,
) -> BTreeMap<String, GroupOutcome<ShareGroupOffsets>> {
    let mut outcomes = keys
        .iter()
        .map(|key| {
            (
                key.clone(),
                GroupOutcome::Done(Ok(ShareGroupOffsets::new())),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for group in response.groups {
        let Some(outcome) = outcomes.get_mut(&group.group_id) else {
            continue;
        };
        if group.error_code != 0 {
            *outcome = group_error_outcome(group.error_code, group.error_message, &[]);
            continue;
        }
        let GroupOutcome::Done(Ok(offsets)) = outcome else {
            continue;
        };
        for topic in group.topics {
            for partition in topic.partitions {
                if partition.error_code != 0 {
                    tracing::debug!(
                        topic = topic.topic_name,
                        partition = partition.partition_index,
                        error_code = partition.error_code,
                        "skipping a share partition with an error"
                    );
                    continue;
                }
                let info = (partition.start_offset >= 0).then_some(SharePartitionOffsetInfo {
                    start_offset: partition.start_offset,
                    leader_epoch: (partition.leader_epoch >= 0).then_some(partition.leader_epoch),
                    lag: (partition.lag >= 0).then_some(partition.lag),
                });
                offsets.insert((topic.topic_name.clone(), partition.partition_index), info);
            }
        }
    }
    outcomes
}

impl AdminClient {
    /// Lists the start offsets of share groups with
    /// `DescribeShareGroupOffsets`, as Apache Kafka's `listShareGroupOffsets`
    /// does with its `ListShareGroupOffsetsHandler`.
    ///
    /// The call finds the coordinator of every group with one batched
    /// `FindCoordinator` request and sends one `DescribeShareGroupOffsets`
    /// request to each coordinator with every group that it coordinates, each
    /// with the partitions of its [`ListShareGroupOffsetsSpec`].
    /// `COORDINATOR_LOAD_IN_PROGRESS` (14) sends a group to the same
    /// coordinator again, and `COORDINATOR_NOT_AVAILABLE` (15) and
    /// `NOT_COORDINATOR` (16) find its coordinator again. A lost
    /// connection finds the coordinator again. The call waits with Kafka's
    /// backoff between rounds and stops at `default.api.timeout.ms` (60 s),
    /// where each pending group gets `REQUEST_TIMED_OUT` (7).
    ///
    /// Each group's offsets hold every answered partition without an error.
    /// A partition with no start offset maps to `None`.
    ///
    /// # Errors
    /// The call itself does not fail. Each group gets its own [`KafkaError`],
    /// such as `GROUP_AUTHORIZATION_FAILED` (30) or `REQUEST_TIMED_OUT` (7)
    /// at the deadline.
    pub async fn list_share_group_offsets(
        &self,
        specs: &BTreeMap<String, ListShareGroupOffsetsSpec>,
    ) -> BTreeMap<String, Result<ShareGroupOffsets, KafkaError>> {
        let groups = specs.keys().map(String::as_str).collect::<Vec<_>>();
        self.call_group_coordinators(
            "describeShareGroupOffsets",
            &groups,
            |coordinator, keys| async move {
                let connection = self.connect_coordinator(&coordinator).await?;
                let request = describe_share_group_offsets_request(&keys, specs);
                Ok(match connection.send(request).await {
                    Ok(response) => share_group_offsets_outcomes(&keys, response),
                    Err(error) => request_error_outcomes(&keys, &error.into()),
                })
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;
    use krabka_client_core::MockReply;
    use krabka_protocol::owned::{
        describe_share_group_offsets_request,
        describe_share_group_offsets_response::{
            DescribeShareGroupOffsetsResponseGroup, DescribeShareGroupOffsetsResponsePartition,
            DescribeShareGroupOffsetsResponseTopic,
        },
    };

    use super::*;
    use crate::{
        group_coordinators::{
            kafka_error,
            test_support::{Lookup, group_cluster},
        },
        partition_leaders::test_support::{decode_request, encode_response},
    };

    fn partition(
        partition_index: i32,
        start_offset: i64,
        leader_epoch: i32,
        lag: i64,
        error_code: i16,
    ) -> DescribeShareGroupOffsetsResponsePartition {
        DescribeShareGroupOffsetsResponsePartition {
            partition_index,
            start_offset,
            leader_epoch,
            lag,
            error_code,
            ..Default::default()
        }
    }

    #[test]
    fn request_names_the_partitions_of_each_spec() {
        let specs = BTreeMap::from([
            ("all".to_owned(), ListShareGroupOffsetsSpec::default()),
            (
                "some".to_owned(),
                ListShareGroupOffsetsSpec {
                    topic_partitions: Some(vec![
                        ("orders".to_owned(), 2),
                        ("audit".to_owned(), 0),
                        ("orders".to_owned(), 0),
                    ]),
                },
            ),
        ]);
        let keys = ["all".to_owned(), "some".to_owned()];
        assert!(
            describe_share_group_offsets_request(&keys, &specs)
                == DescribeShareGroupOffsetsRequest {
                    groups: vec![
                        DescribeShareGroupOffsetsRequestGroup {
                            group_id: "all".to_owned(),
                            topics: None,
                            ..Default::default()
                        },
                        DescribeShareGroupOffsetsRequestGroup {
                            group_id: "some".to_owned(),
                            topics: Some(vec![
                                DescribeShareGroupOffsetsRequestTopic {
                                    topic_name: "audit".to_owned(),
                                    partitions: vec![0],
                                    ..Default::default()
                                },
                                DescribeShareGroupOffsetsRequestTopic {
                                    topic_name: "orders".to_owned(),
                                    partitions: vec![2, 0],
                                    ..Default::default()
                                },
                            ]),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }
        );
    }

    /// Kafka's `ListShareGroupOffsetsHandler.handleResponse` skips a
    /// partition with an error, maps a negative start offset to `null` and a
    /// negative epoch or lag to `Optional.empty()`, and retries the group
    /// errors of `handleGroupError`.
    #[test]
    fn outcomes_read_each_group_as_kafka_does() {
        let keys = ["good", "loading", "moved", "denied", "absent"].map(str::to_owned);
        let response = DescribeShareGroupOffsetsResponse {
            groups: vec![
                DescribeShareGroupOffsetsResponseGroup {
                    group_id: "good".to_owned(),
                    topics: vec![DescribeShareGroupOffsetsResponseTopic {
                        topic_name: "orders".to_owned(),
                        partitions: vec![
                            partition(0, 41, 3, 5, 0),
                            partition(1, 7, -1, -1, 0),
                            partition(2, -1, 0, 0, 0),
                            partition(3, 9, 1, 1, 3),
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                DescribeShareGroupOffsetsResponseGroup {
                    group_id: "loading".to_owned(),
                    error_code: 14,
                    ..Default::default()
                },
                DescribeShareGroupOffsetsResponseGroup {
                    group_id: "moved".to_owned(),
                    error_code: 16,
                    ..Default::default()
                },
                DescribeShareGroupOffsetsResponseGroup {
                    group_id: "denied".to_owned(),
                    error_code: 30,
                    error_message: Some("denied".to_owned()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let info = |start_offset, leader_epoch, lag| {
            Some(SharePartitionOffsetInfo {
                start_offset,
                leader_epoch,
                lag,
            })
        };
        assert!(
            share_group_offsets_outcomes(&keys, response)
                == BTreeMap::from([
                    (
                        "good".to_owned(),
                        GroupOutcome::Done(Ok(BTreeMap::from([
                            (("orders".to_owned(), 0), info(41, Some(3), Some(5))),
                            (("orders".to_owned(), 1), info(7, None, None)),
                            (("orders".to_owned(), 2), None),
                        ])))
                    ),
                    (
                        "loading".to_owned(),
                        GroupOutcome::Retry("COORDINATOR_LOAD_IN_PROGRESS".to_owned())
                    ),
                    (
                        "moved".to_owned(),
                        GroupOutcome::Unmap("NOT_COORDINATOR".to_owned())
                    ),
                    (
                        "denied".to_owned(),
                        GroupOutcome::Done(Err(kafka_error(30, Some("denied".to_owned()))))
                    ),
                    ("absent".to_owned(), GroupOutcome::Done(Ok(BTreeMap::new()))),
                ])
        );
    }

    /// Two groups on one coordinator share one `DescribeShareGroupOffsets`
    /// request, and a group that is loading is sent again alone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_share_group_offsets_batches_the_groups_of_a_coordinator() {
        let loading_answers = Arc::new(Mutex::new(0_usize));
        let cluster = group_cluster(
            1,
            vec![(describe_share_group_offsets_request::API_KEY, 0, 1)],
            |_, _| Lookup::At(0),
            move |_, api_key, version, body, _| {
                if api_key != describe_share_group_offsets_request::API_KEY {
                    return MockReply::Silent;
                }
                let request: DescribeShareGroupOffsetsRequest = decode_request(body, version, true);
                let groups = request
                    .groups
                    .into_iter()
                    .map(|group| {
                        let mut loading = loading_answers.lock().expect("answers lock");
                        let error_code = if group.group_id == "loading" && *loading == 0 {
                            *loading += 1;
                            14
                        } else {
                            0
                        };
                        DescribeShareGroupOffsetsResponseGroup {
                            topics: if error_code == 0 {
                                vec![DescribeShareGroupOffsetsResponseTopic {
                                    topic_name: "orders".to_owned(),
                                    partitions: vec![partition(0, 41, 3, 5, 0)],
                                    ..Default::default()
                                }]
                            } else {
                                Vec::new()
                            },
                            group_id: group.group_id,
                            error_code,
                            ..Default::default()
                        }
                    })
                    .collect();
                MockReply::Respond(encode_response(
                    &DescribeShareGroupOffsetsResponse {
                        groups,
                        ..Default::default()
                    },
                    version,
                    true,
                ))
            },
        )
        .await;
        let admin = cluster.admin(krabka_units::secs(5)).await;
        let specs = BTreeMap::from([
            ("ready".to_owned(), ListShareGroupOffsetsSpec::default()),
            ("loading".to_owned(), ListShareGroupOffsetsSpec::default()),
        ]);

        let result = admin.list_share_group_offsets(&specs).await;

        let seen = cluster.stop();
        let requested = seen
            .requests
            .iter()
            .map(|request| {
                let body: DescribeShareGroupOffsetsRequest =
                    decode_request(&request.body, request.version, true);
                (
                    request.version,
                    body.groups
                        .into_iter()
                        .map(|group| group.group_id)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let offsets = Ok(BTreeMap::from([(
            ("orders".to_owned(), 0),
            Some(SharePartitionOffsetInfo {
                start_offset: 41,
                leader_epoch: Some(3),
                lag: Some(5),
            }),
        )]));
        assert!(
            (result, requested)
                == (
                    BTreeMap::from([
                        ("loading".to_owned(), offsets.clone()),
                        ("ready".to_owned(), offsets),
                    ]),
                    vec![
                        (1, vec!["loading".to_owned(), "ready".to_owned()]),
                        (1, vec!["loading".to_owned()]),
                    ],
                )
        );
    }
}
