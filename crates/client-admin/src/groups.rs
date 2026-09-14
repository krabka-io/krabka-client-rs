//! Consumer-group admin APIs: [`AdminClient::list_groups`] and
//! [`AdminClient::list_consumer_group_offsets`].
//!
//! These are thin wrappers over the `ListGroups` (`api_key`=16),
//! `OffsetCommit` (`api_key`=8) and `OffsetFetch` (`api_key`=9) RPCs.
//!
//! ## `OffsetCommit` version note
//!
//! [`AdminClient::alter_consumer_group_offsets`] sends `OffsetCommit` at v9 or
//! lower, where the request names each topic. Apache Kafka's
//! `AlterConsumerGroupOffsetsHandler` builds its request with
//! `OffsetCommitRequest.Builder.forTopicNames`, which caps the version at 9.
//!
//! ## `OffsetFetch` version note
//!
//! [`AdminClient::list_consumer_group_offsets`] sends `OffsetFetch` at v2 to
//! v9, where the response names each topic. Apache Kafka's
//! `ListConsumerGroupOffsetsHandler` builds its request with
//! `OffsetFetchRequest.Builder.forTopicNames`, which caps the version at 9.

use std::collections::BTreeMap;

use bytes::BufMut;
use krabka_client_core::{CoordinatorKeyType, build_find_coordinator, coordinator_endpoint};
use krabka_protocol::{
    Encode, ProtocolError, ProtocolRequest,
    owned::{
        list_groups_request::ListGroupsRequest,
        offset_commit_request::{
            self, OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_commit_response::OffsetCommitResponse,
        offset_fetch_request::{self, OffsetFetchRequest, OffsetFetchRequestGroup},
        offset_fetch_response::OffsetFetchResponse,
    },
};

use crate::{
    AdminClient, AdminError, KafkaError, format_host_port, kafka_error_if, kafka_error_name,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupOffsetOutcome {
    pub topic: String,
    pub partition: i32,
    pub error: Option<KafkaError>,
}

impl AdminClient {
    /// Commits explicit offsets for an inactive consumer group.
    ///
    /// The request names each topic, so the client negotiates `OffsetCommit`
    /// v9 or lower, as Apache Kafka's `AlterConsumerGroupOffsetsHandler` does.
    ///
    /// # Errors
    /// Returns an error when encoding, transport, or response handling fails.
    /// Returns [`ClientError::IncompatibleVersion`] when the coordinator does
    /// not support `OffsetCommit` v9 or lower.
    ///
    /// [`ClientError::IncompatibleVersion`]: krabka_client_core::ClientError::IncompatibleVersion
    pub async fn alter_consumer_group_offsets(
        &mut self,
        group: &str,
        offsets: &BTreeMap<(String, i32), i64>,
    ) -> Result<Vec<ConsumerGroupOffsetOutcome>, AdminError> {
        self.reconnect_group_coordinator(group).await?;
        let response = self
            .conn
            .send(offset_commit_request(group, offsets))
            .await?;
        Ok(response
            .topics
            .into_iter()
            .flat_map(|topic| {
                let name = topic.name;
                topic
                    .partitions
                    .into_iter()
                    .map(move |partition| ConsumerGroupOffsetOutcome {
                        topic: name.clone(),
                        partition: partition.partition_index,
                        error: kafka_error_if(partition.error_code, None),
                    })
            })
            .collect())
    }

    /// Returns the group-id of every consumer group known to the broker.
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn list_groups(&mut self) -> Result<Vec<String>, AdminError> {
        // Default request lists every group (empty state/type filters).
        let req = ListGroupsRequest::default();
        let resp = self.conn.send(req).await?;
        if resp.error_code != 0 {
            return Err(AdminError::Broker {
                api: "ListGroups",
                code: resp.error_code,
                name: kafka_error_name(resp.error_code),
                message: None,
            });
        }
        Ok(resp.groups.into_iter().map(|g| g.group_id).collect())
    }

    /// Returns `(topic, partition) → committed_offset` for the named group.
    ///
    /// The call requests all topics and partitions (`topics: None`). It skips
    /// a partition that has an error code, and a partition with a committed
    /// offset below 0, which means no committed offset.
    ///
    /// The response names each topic, so the client negotiates `OffsetFetch`
    /// v2 to v9, as Apache Kafka's `ListConsumerGroupOffsetsHandler` does.
    ///
    /// # Errors
    /// Returns an error when encoding, transport, or response handling fails.
    /// Returns [`AdminError::Broker`] when the coordinator answers with a
    /// group error code. Returns [`ClientError::IncompatibleVersion`] when the
    /// coordinator does not support `OffsetFetch` v2 to v9.
    ///
    /// [`ClientError::IncompatibleVersion`]: krabka_client_core::ClientError::IncompatibleVersion
    pub async fn list_consumer_group_offsets(
        &mut self,
        group: &str,
    ) -> Result<BTreeMap<(String, i32), i64>, AdminError> {
        self.reconnect_group_coordinator(group).await?;
        let response = self.conn.send(offset_fetch_request(group)).await?;
        committed_offsets(group, response)
    }

    async fn reconnect_group_coordinator(&mut self, group: &str) -> Result<(), AdminError> {
        let response = self
            .conn
            .send(build_find_coordinator(group, CoordinatorKeyType::Group))
            .await?;
        let coordinator = coordinator_endpoint(group, response)?;
        self.reconnect(&format_host_port(&coordinator.host, coordinator.port))
            .await
    }
}

/// An `OffsetCommit` request that names its topics, capped at v9.
///
/// `OffsetCommit` v10 names each topic by id only. Apache Kafka's
/// `AlterConsumerGroupOffsetsHandler.buildBatchedRequest` uses
/// `OffsetCommitRequest.Builder.forTopicNames`, which allows v9 at most. This
/// type gives the same cap to version negotiation.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TopicNameOffsetCommit(OffsetCommitRequest);

impl Encode for TopicNameOffsetCommit {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for TopicNameOffsetCommit {
    const API_KEY: i16 = offset_commit_request::API_KEY;
    const MIN_VERSION: i16 = offset_commit_request::MIN_VERSION;
    /// The last `OffsetCommit` version that carries topic names.
    const MAX_VERSION: i16 = 9;
    const FLEXIBLE_MIN: i16 = offset_commit_request::FLEXIBLE_MIN;
    type Response = OffsetCommitResponse;
}

/// An `OffsetFetch` request that names its topics, limited to v2 to v9.
///
/// `OffsetFetch` v10 names each topic by id only. Apache Kafka's
/// `ListConsumerGroupOffsetsHandler.buildBatchedRequest` uses
/// `OffsetFetchRequest.Builder.forTopicNames`, which allows v9 at most. The
/// admin request asks for all topics, and `Builder.build` rejects that request
/// below v2 (`TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION`). This type gives
/// the same range to version negotiation.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TopicNameOffsetFetch(OffsetFetchRequest);

impl Encode for TopicNameOffsetFetch {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for TopicNameOffsetFetch {
    const API_KEY: i16 = offset_fetch_request::API_KEY;
    /// The first `OffsetFetch` version that can ask for all topics.
    const MIN_VERSION: i16 = 2;
    /// The last `OffsetFetch` version that carries topic names.
    const MAX_VERSION: i16 = 9;
    const FLEXIBLE_MIN: i16 = offset_fetch_request::FLEXIBLE_MIN;
    type Response = OffsetFetchResponse;
}

/// Builds the admin `OffsetFetch` request for all topics of `group`.
///
/// The request fills the v2 to v7 fields (`group_id`, `topics`) and the v8+
/// `groups` array, so it is valid at each negotiated version. Kafka's
/// `OffsetFetchRequest.Builder.maybeDowngrade` moves the group into the v2 to
/// v7 fields in the same way.
fn offset_fetch_request(group: &str) -> TopicNameOffsetFetch {
    TopicNameOffsetFetch(OffsetFetchRequest {
        group_id: group.into(),
        topics: None,
        groups: vec![OffsetFetchRequestGroup {
            group_id: group.into(),
            member_id: None,
            member_epoch: -1,
            topics: None,
            ..Default::default()
        }],
        require_stable: false,
        ..Default::default()
    })
}

/// Reads the committed offsets of `group` from an `OffsetFetch` response.
///
/// v8 and v9 put the group in `groups`. v2 to v7 put the group error code and
/// the topics at the top level, which Kafka's `OffsetFetchResponse.group`
/// reads in the same way. As in Kafka's
/// `ListConsumerGroupOffsetsHandler.handleResponse`, a partition with an error
/// code gives no row.
fn committed_offsets(
    group: &str,
    response: OffsetFetchResponse,
) -> Result<BTreeMap<(String, i32), i64>, AdminError> {
    let (error_code, rows): (i16, Vec<(String, i32, i64, i16)>) = if response.groups.is_empty() {
        (
            response.error_code,
            response
                .topics
                .into_iter()
                .flat_map(|topic| {
                    let name = topic.name;
                    topic.partitions.into_iter().map(move |partition| {
                        (
                            name.clone(),
                            partition.partition_index,
                            partition.committed_offset,
                            partition.error_code,
                        )
                    })
                })
                .collect(),
        )
    } else {
        let mut error_code = 0;
        let mut rows = Vec::new();
        for entry in response
            .groups
            .into_iter()
            .filter(|entry| entry.group_id == group)
        {
            error_code = entry.error_code;
            for topic in entry.topics {
                let name = topic.name;
                rows.extend(topic.partitions.into_iter().map(|partition| {
                    (
                        name.clone(),
                        partition.partition_index,
                        partition.committed_offset,
                        partition.error_code,
                    )
                }));
            }
        }
        (error_code, rows)
    };
    if error_code != 0 {
        return Err(AdminError::Broker {
            api: "OffsetFetch",
            code: error_code,
            name: kafka_error_name(error_code),
            message: Some(format!("group={group}")),
        });
    }
    Ok(rows
        .into_iter()
        .filter_map(|(topic, partition, offset, partition_error)| {
            if partition_error != 0 {
                tracing::warn!(
                    topic,
                    partition,
                    error_code = partition_error,
                    "skipping the committed offset of a partition with an error"
                );
                None
            } else if offset < 0 {
                None
            } else {
                Some(((topic, partition), offset))
            }
        })
        .collect())
}

fn offset_commit_request(
    group: &str,
    offsets: &BTreeMap<(String, i32), i64>,
) -> TopicNameOffsetCommit {
    let mut topics = BTreeMap::<String, Vec<OffsetCommitRequestPartition>>::new();
    for ((topic, partition), offset) in offsets {
        topics
            .entry(topic.clone())
            .or_default()
            .push(OffsetCommitRequestPartition {
                partition_index: *partition,
                committed_offset: *offset,
                committed_leader_epoch: -1,
                committed_metadata: None,
                ..Default::default()
            });
    }
    TopicNameOffsetCommit(OffsetCommitRequest {
        group_id: group.into(),
        generation_id_or_member_epoch: -1,
        member_id: String::new(),
        topics: topics
            .into_iter()
            .map(|(name, partitions)| OffsetCommitRequestTopic {
                name,
                partitions,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use assert2::assert;
    use bytes::{Buf, BytesMut};
    use krabka_client_core::{ClientError, MockBroker};
    use krabka_protocol::{
        Decode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            find_coordinator_request,
            find_coordinator_response::FindCoordinatorResponse,
            metadata_request,
            metadata_response::MetadataResponse,
            offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
            offset_fetch_response::{
                OffsetFetchResponseGroup, OffsetFetchResponsePartition,
                OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
            },
        },
    };

    use super::*;

    #[test]
    fn offset_reset_builds_admin_commit() {
        let offsets = BTreeMap::from([(("orders".into(), 2), 41)]);
        let request = offset_commit_request("worker", &offsets);
        let expected = TopicNameOffsetCommit(OffsetCommitRequest {
            group_id: "worker".into(),
            topics: vec![OffsetCommitRequestTopic {
                name: "orders".into(),
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 2,
                    committed_offset: 41,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
        assert!(request == expected);
    }

    fn encode(response: &impl Encode, version: i16, flexible: bool) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        if flexible {
            bytes.extend_from_slice(&[0]);
        }
        response.encode(&mut bytes, version).unwrap();
        bytes.to_vec()
    }

    /// The `OffsetCommit` and `OffsetFetch` version ranges that a mock broker
    /// advertises.
    #[derive(Clone, Copy)]
    struct GroupRanges {
        offset_commit: (i16, i16),
        offset_fetch: (i16, i16),
    }

    /// An `ApiVersions` response that advertises `ranges`.
    fn api_versions(ranges: GroupRanges) -> Vec<u8> {
        encode(
            &ApiVersionsResponse {
                api_keys: vec![
                    ApiVersion {
                        api_key: api_versions_request::API_KEY,
                        min_version: 0,
                        max_version: 0,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: find_coordinator_request::API_KEY,
                        min_version: 0,
                        max_version: 0,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: offset_commit_request::API_KEY,
                        min_version: ranges.offset_commit.0,
                        max_version: ranges.offset_commit.1,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: offset_fetch_request::API_KEY,
                        min_version: ranges.offset_fetch.0,
                        max_version: ranges.offset_fetch.1,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: metadata_request::API_KEY,
                        min_version: 13,
                        max_version: 13,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            0,
            false,
        )
    }

    /// Decodes a request body of type `R` behind its request header.
    fn decode_request<R: ProtocolRequest + for<'de> Decode<'de>>(
        mut body: &[u8],
        version: i16,
    ) -> R {
        let client_id_len = body.get_i16();
        body.advance(usize::try_from(client_id_len).expect("client id length"));
        if version >= R::FLEXIBLE_MIN {
            body.advance(1);
        }
        R::decode(&mut body, version).expect("request decodes")
    }

    /// A bootstrap broker that sends every group RPC to `coordinator`.
    async fn bootstrap_for(
        coordinator: &MockBroker,
        ranges: GroupRanges,
        group_rpcs: Arc<AtomicUsize>,
    ) -> MockBroker {
        let coordinator_addr = coordinator.addr;
        MockBroker::start(move |api_key, version, _, _| match api_key {
            api_versions_request::API_KEY => Some(api_versions(ranges)),
            find_coordinator_request::API_KEY => Some(encode(
                &FindCoordinatorResponse {
                    node_id: 2,
                    host: coordinator_addr.ip().to_string(),
                    port: i32::from(coordinator_addr.port()),
                    ..Default::default()
                },
                version,
                false,
            )),
            offset_commit_request::API_KEY => {
                group_rpcs.fetch_add(1, Ordering::SeqCst);
                Some(encode(&OffsetCommitResponse::default(), version, true))
            }
            offset_fetch_request::API_KEY => {
                group_rpcs.fetch_add(1, Ordering::SeqCst);
                Some(encode(&OffsetFetchResponse::default(), version, true))
            }
            metadata_request::API_KEY => Some(encode(&MetadataResponse::default(), version, true)),
            _ => None,
        })
        .await
    }

    /// The negotiated `OffsetCommit` version and the result of
    /// `alter_consumer_group_offsets`, with a version error as its ranges.
    type CommitResult = Result<Vec<ConsumerGroupOffsetOutcome>, (i16, i16, i16, i16, i16)>;

    /// Apache Kafka's `AlterConsumerGroupOffsetsHandler` builds `OffsetCommit`
    /// with `OffsetCommitRequest.Builder.forTopicNames`, which caps the version
    /// at 9. The request names each topic at every negotiated version.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alter_consumer_group_offsets_sends_offset_commit_by_topic_name() {
        let sent_request = |version| {
            vec![(
                version,
                OffsetCommitRequest {
                    group_id: "workers".into(),
                    generation_id_or_member_epoch: -1,
                    topics: vec![OffsetCommitRequestTopic {
                        name: "orders".into(),
                        partitions: vec![OffsetCommitRequestPartition {
                            partition_index: 2,
                            committed_offset: 41,
                            committed_leader_epoch: -1,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )]
        };
        let committed = Ok(vec![ConsumerGroupOffsetOutcome {
            topic: "orders".into(),
            partition: 2,
            error: None,
        }]);
        for (name, offset_commit_range, expected_requests, expected_result) in [
            (
                "coordinator stops at v7",
                (2, 7),
                sent_request(7),
                committed.clone(),
            ),
            (
                "coordinator stops at v9",
                (2, 9),
                sent_request(9),
                committed.clone(),
            ),
            (
                "coordinator supports v10",
                (2, 10),
                sent_request(9),
                committed.clone(),
            ),
            (
                "coordinator supports only v10",
                (10, 10),
                Vec::new(),
                Err((offset_commit_request::API_KEY, 10, 10, 2, 9)),
            ),
        ] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_in_mock = Arc::clone(&requests);
            let ranges = GroupRanges {
                offset_commit: offset_commit_range,
                offset_fetch: (2, 10),
            };
            let coordinator = MockBroker::start(move |api_key, version, _, body| match api_key {
                api_versions_request::API_KEY => Some(api_versions(ranges)),
                offset_commit_request::API_KEY => {
                    let request: OffsetCommitRequest = decode_request(body, version);
                    let response = OffsetCommitResponse {
                        topics: request
                            .topics
                            .iter()
                            .map(|topic| OffsetCommitResponseTopic {
                                name: topic.name.clone(),
                                partitions: topic
                                    .partitions
                                    .iter()
                                    .map(|partition| OffsetCommitResponsePartition {
                                        partition_index: partition.partition_index,
                                        ..Default::default()
                                    })
                                    .collect(),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    };
                    requests_in_mock
                        .lock()
                        .expect("requests lock")
                        .push((version, request));
                    Some(encode(
                        &response,
                        version,
                        version >= offset_commit_request::FLEXIBLE_MIN,
                    ))
                }
                _ => None,
            })
            .await;
            let bootstrap = bootstrap_for(&coordinator, ranges, Arc::default()).await;
            let mut admin = AdminClient::connect(&[bootstrap.addr.to_string()])
                .await
                .expect("admin connects");

            let result: CommitResult = admin
                .alter_consumer_group_offsets(
                    "workers",
                    &BTreeMap::from([(("orders".into(), 2), 41)]),
                )
                .await
                .map_err(|error| match error {
                    AdminError::Transport(ClientError::IncompatibleVersion {
                        api_key,
                        broker_min,
                        broker_max,
                        client_min,
                        client_max,
                    }) => (api_key, broker_min, broker_max, client_min, client_max),
                    other => panic!("case {name}: unexpected error {other:?}"),
                });

            bootstrap.stop();
            coordinator.stop();
            let requests = requests.lock().expect("requests lock").clone();
            assert!(
                (requests, result) == (expected_requests, expected_result),
                "case {name}"
            );
        }
    }

    /// The `OffsetFetch` answer of the mock coordinator at `version`: one
    /// committed offset, one partition with no committed offset, and one
    /// partition with an error code. v2 to v7 put the topics at the top level.
    fn offset_fetch_response(version: i16) -> OffsetFetchResponse {
        let rows = [
            ("orders", 2, 41, 0),
            ("orders", 3, -1, 0),
            ("payments", 0, 5, 3),
        ];
        if version < 8 {
            OffsetFetchResponse {
                topics: rows
                    .iter()
                    .map(|(name, partition_index, committed_offset, error_code)| {
                        OffsetFetchResponseTopic {
                            name: (*name).into(),
                            partitions: vec![OffsetFetchResponsePartition {
                                partition_index: *partition_index,
                                committed_offset: *committed_offset,
                                error_code: *error_code,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            }
        } else {
            OffsetFetchResponse {
                groups: vec![OffsetFetchResponseGroup {
                    group_id: "workers".into(),
                    topics: rows
                        .iter()
                        .map(|(name, partition_index, committed_offset, error_code)| {
                            OffsetFetchResponseTopics {
                                name: (*name).into(),
                                partitions: vec![OffsetFetchResponsePartitions {
                                    partition_index: *partition_index,
                                    committed_offset: *committed_offset,
                                    error_code: *error_code,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }
        }
    }

    /// The negotiated `OffsetFetch` version and the result of
    /// `list_consumer_group_offsets`, with a version error as its ranges.
    type FetchResult = Result<BTreeMap<(String, i32), i64>, (i16, i16, i16, i16, i16)>;

    /// Apache Kafka's `ListConsumerGroupOffsetsHandler` builds `OffsetFetch`
    /// with `OffsetFetchRequest.Builder.forTopicNames`, which caps the version
    /// at 9. A request for all topics also needs v2. The response names each
    /// topic, so the call sends no `Metadata` request.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_consumer_group_offsets_sends_offset_fetch_by_topic_name() {
        let legacy_request = |version| {
            vec![(
                version,
                OffsetFetchRequest {
                    group_id: "workers".into(),
                    topics: None,
                    ..Default::default()
                },
            )]
        };
        let grouped_request = |version| {
            vec![(
                version,
                OffsetFetchRequest {
                    groups: vec![OffsetFetchRequestGroup {
                        group_id: "workers".into(),
                        member_id: None,
                        member_epoch: -1,
                        topics: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )]
        };
        let fetched = Ok(BTreeMap::from([(("orders".into(), 2), 41)]));
        for (name, offset_fetch_range, expected_requests, expected_result) in [
            (
                "coordinator stops at v7",
                (2, 7),
                legacy_request(7),
                fetched.clone(),
            ),
            (
                "coordinator stops at v8",
                (2, 8),
                grouped_request(8),
                fetched.clone(),
            ),
            (
                "coordinator stops at v9",
                (2, 9),
                grouped_request(9),
                fetched.clone(),
            ),
            (
                "coordinator supports v10",
                (2, 10),
                grouped_request(9),
                fetched.clone(),
            ),
            (
                "coordinator supports only v10",
                (10, 10),
                Vec::new(),
                Err((offset_fetch_request::API_KEY, 10, 10, 2, 9)),
            ),
            (
                "coordinator supports only v1",
                (1, 1),
                Vec::new(),
                Err((offset_fetch_request::API_KEY, 1, 1, 2, 9)),
            ),
        ] {
            let ranges = GroupRanges {
                offset_commit: (2, 9),
                offset_fetch: offset_fetch_range,
            };
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_in_mock = Arc::clone(&requests);
            let metadata_requests = Arc::new(AtomicUsize::new(0));
            let metadata_requests_in_mock = Arc::clone(&metadata_requests);
            let coordinator = MockBroker::start(move |api_key, version, _, body| match api_key {
                api_versions_request::API_KEY => Some(api_versions(ranges)),
                offset_fetch_request::API_KEY => {
                    let request: OffsetFetchRequest = decode_request(body, version);
                    requests_in_mock
                        .lock()
                        .expect("requests lock")
                        .push((version, request));
                    Some(encode(
                        &offset_fetch_response(version),
                        version,
                        version >= offset_fetch_request::FLEXIBLE_MIN,
                    ))
                }
                metadata_request::API_KEY => {
                    metadata_requests_in_mock.fetch_add(1, Ordering::SeqCst);
                    Some(encode(&MetadataResponse::default(), version, true))
                }
                _ => None,
            })
            .await;
            let bootstrap = bootstrap_for(&coordinator, ranges, Arc::default()).await;
            let mut admin = AdminClient::connect(&[bootstrap.addr.to_string()])
                .await
                .expect("admin connects");

            let result: FetchResult =
                admin
                    .list_consumer_group_offsets("workers")
                    .await
                    .map_err(|error| match error {
                        AdminError::Transport(ClientError::IncompatibleVersion {
                            api_key,
                            broker_min,
                            broker_max,
                            client_min,
                            client_max,
                        }) => (api_key, broker_min, broker_max, client_min, client_max),
                        other => panic!("case {name}: unexpected error {other:?}"),
                    });

            bootstrap.stop();
            coordinator.stop();
            let requests = requests.lock().expect("requests lock").clone();
            assert!(
                (requests, metadata_requests.load(Ordering::SeqCst), result)
                    == (expected_requests, 0, expected_result),
                "case {name}"
            );
        }
    }

    /// The result of `committed_offsets`, with a broker error as its fields.
    type OffsetsResult =
        Result<BTreeMap<(String, i32), i64>, (&'static str, i16, &'static str, Option<String>)>;

    /// Kafka's `OffsetFetchResponse.group` reads the group error code from the
    /// top level below v8 and from the group entry from v8.
    #[test]
    fn committed_offsets_reads_both_response_shapes() {
        let offsets = BTreeMap::from([(("orders".into(), 2), 41)]);
        let group_error =
            |code, name| Err(("OffsetFetch", code, name, Some("group=workers".into())));
        for (name, response, expected) in [
            (
                "v2 to v7 shape",
                offset_fetch_response(7),
                Ok(offsets.clone()),
            ),
            ("v8 and v9 shape", offset_fetch_response(9), Ok(offsets)),
            (
                "v2 to v7 group error",
                OffsetFetchResponse {
                    error_code: 15,
                    ..Default::default()
                },
                group_error(15, "COORDINATOR_NOT_AVAILABLE"),
            ),
            (
                "v8 and v9 group error",
                OffsetFetchResponse {
                    groups: vec![OffsetFetchResponseGroup {
                        group_id: "workers".into(),
                        error_code: 16,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                group_error(16, "NOT_COORDINATOR"),
            ),
        ] {
            let result: OffsetsResult =
                committed_offsets("workers", response).map_err(|error| match error {
                    AdminError::Broker {
                        api,
                        code,
                        name,
                        message,
                    } => (api, code, name, message),
                    other => panic!("case {name}: unexpected error {other:?}"),
                });
            assert!(result == expected, "case {name}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_offset_rpcs_use_the_group_coordinator() {
        let ranges = GroupRanges {
            offset_commit: (2, 10),
            offset_fetch: (2, 10),
        };
        let coordinator_group_rpcs = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&coordinator_group_rpcs);
        let coordinator = MockBroker::start(move |api_key, version, _, _| match api_key {
            api_versions_request::API_KEY => Some(api_versions(ranges)),
            offset_commit_request::API_KEY => {
                seen.fetch_add(1, Ordering::SeqCst);
                Some(encode(
                    &OffsetCommitResponse {
                        topics: vec![OffsetCommitResponseTopic {
                            name: "orders".into(),
                            partitions: vec![OffsetCommitResponsePartition {
                                partition_index: 2,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    version,
                    true,
                ))
            }
            offset_fetch_request::API_KEY => {
                seen.fetch_add(1, Ordering::SeqCst);
                Some(encode(&offset_fetch_response(version), version, true))
            }
            _ => None,
        })
        .await;

        let bootstrap_group_rpcs = Arc::new(AtomicUsize::new(0));
        let bootstrap =
            bootstrap_for(&coordinator, ranges, Arc::clone(&bootstrap_group_rpcs)).await;
        let mut admin = AdminClient::connect(&[bootstrap.addr.to_string()])
            .await
            .expect("admin connects");

        let committed = admin
            .alter_consumer_group_offsets("workers", &BTreeMap::from([(("orders".into(), 2), 41)]))
            .await
            .expect("offset commit succeeds");
        let mut admin = AdminClient::connect(&[bootstrap.addr.to_string()])
            .await
            .expect("second admin connects");
        let fetched = admin
            .list_consumer_group_offsets("workers")
            .await
            .expect("offset fetch succeeds");

        assert!(bootstrap_group_rpcs.load(Ordering::SeqCst) == 0);
        assert!(coordinator_group_rpcs.load(Ordering::SeqCst) == 2);
        assert!(
            committed
                == vec![ConsumerGroupOffsetOutcome {
                    topic: "orders".into(),
                    partition: 2,
                    error: None,
                }]
        );
        assert!(fetched == BTreeMap::from([(("orders".into(), 2), 41)]));
        bootstrap.stop();
        coordinator.stop();
    }
}
