//! Partition offset lookup (`ListOffsets`), as Kafka's `Admin.listOffsets`.

use std::collections::BTreeMap;

use krabka_client_core::ConnectionOptions;
use krabka_protocol::owned::{
    list_offsets_request::{self, ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    list_offsets_response::ListOffsetsResponse,
};

use crate::{
    AdminClient, AdminError, KafkaError, kafka_error_name,
    partition_leaders::{PartitionKey, PartitionResults, complete_results},
};

/// `UNSUPPORTED_VERSION`: the leader cannot answer this offset spec.
const UNSUPPORTED_VERSION: i16 = 35;
/// `ListOffsets` `replica_id` of a consumer (`CONSUMER_REPLICA_ID`).
const CONSUMER_REPLICA_ID: i32 = -1;

/// Which offset of a partition to look up, as Kafka's `OffsetSpec`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetSpec {
    /// The first offset of the log (`EARLIEST_TIMESTAMP`, -2).
    Earliest,
    /// The next offset to be written, the high watermark or, for
    /// `READ_COMMITTED`, the last stable offset (`LATEST_TIMESTAMP`, -1).
    Latest,
    /// The offset of the record with the largest timestamp
    /// (`MAX_TIMESTAMP`, -3, KIP-734). Needs `ListOffsets` v7.
    MaxTimestamp,
    /// The first offset of the local log of a tiered topic
    /// (`EARLIEST_LOCAL_TIMESTAMP`, -4, KIP-405). Needs `ListOffsets` v8.
    EarliestLocal,
    /// The highest offset copied to tiered storage
    /// (`LATEST_TIERED_TIMESTAMP`, -5, KIP-1005). Needs `ListOffsets` v9.
    LatestTiered,
    /// The first offset not yet copied to tiered storage
    /// (`EARLIEST_PENDING_UPLOAD_TIMESTAMP`, -6, KIP-1023). Needs
    /// `ListOffsets` v11.
    EarliestPendingUpload,
    /// The first offset whose timestamp is at or after this Kafka epoch
    /// millisecond.
    ForTimestamp(i64),
}

impl OffsetSpec {
    /// The `timestamp` of the `ListOffsets` partition, as Kafka's
    /// `ListOffsetsHandler` sets it.
    const fn wire_timestamp(self) -> i64 {
        match self {
            Self::Earliest => -2,
            Self::Latest => -1,
            Self::MaxTimestamp => -3,
            Self::EarliestLocal => -4,
            Self::LatestTiered => -5,
            Self::EarliestPendingUpload => -6,
            Self::ForTimestamp(timestamp) => timestamp,
        }
    }

    /// The lowest `ListOffsets` version that can answer this spec, as Kafka's
    /// `ListOffsetsRequest.Builder.forConsumer` requires it. A timestamp
    /// lookup needs v1.
    const fn min_version(self) -> i16 {
        match self {
            Self::Earliest | Self::Latest | Self::ForTimestamp(_) => 1,
            Self::MaxTimestamp => 7,
            Self::EarliestLocal => 8,
            Self::LatestTiered => 9,
            Self::EarliestPendingUpload => 11,
        }
    }
}

/// Which records a `ListOffsets` lookup sees, as Kafka's `IsolationLevel`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Every record, including those of open and aborted transactions.
    #[default]
    ReadUncommitted,
    /// Only records below the last stable offset. Needs `ListOffsets` v2.
    ReadCommitted,
}

impl IsolationLevel {
    const fn wire(self) -> i8 {
        match self {
            Self::ReadUncommitted => 0,
            Self::ReadCommitted => 1,
        }
    }

    const fn min_version(self) -> i16 {
        match self {
            Self::ReadUncommitted => 1,
            Self::ReadCommitted => 2,
        }
    }
}

/// The offset that `list_offsets` found for one partition, as Kafka's
/// `ListOffsetsResultInfo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListedOffset {
    /// The offset, or `-1` when the partition has no such offset.
    pub offset: i64,
    /// The timestamp of the record at `offset`, in Kafka epoch milliseconds,
    /// or `-1`. An instant is a coordinate, so it stays a raw integer.
    pub timestamp_ms: i64,
    /// The leader epoch of the record at `offset`, or `None` when the leader
    /// did not give one.
    pub leader_epoch: Option<i32>,
}

/// The `ListOffsets` request for `keys`, grouped by topic, as Kafka's
/// `ListOffsetsHandler.buildBatchedRequest` builds it for a consumer.
fn list_offsets_request(
    keys: &[PartitionKey],
    specs: &BTreeMap<PartitionKey, OffsetSpec>,
    isolation_level: IsolationLevel,
    timeout_ms: i32,
) -> ListOffsetsRequest {
    let mut by_topic = BTreeMap::<&str, Vec<ListOffsetsPartition>>::new();
    for key in keys {
        let spec = specs.get(key).copied().unwrap_or(OffsetSpec::Latest);
        by_topic
            .entry(key.0.as_str())
            .or_default()
            .push(ListOffsetsPartition {
                partition_index: key.1,
                timestamp: spec.wire_timestamp(),
                ..Default::default()
            });
    }
    ListOffsetsRequest {
        replica_id: CONSUMER_REPLICA_ID,
        isolation_level: isolation_level.wire(),
        topics: by_topic
            .into_iter()
            .map(|(name, partitions)| ListOffsetsTopic {
                name: name.to_owned(),
                partitions,
                ..Default::default()
            })
            .collect(),
        timeout_ms,
        ..Default::default()
    }
}

/// Maps a `ListOffsets` response to a result for each of `keys`.
fn list_offsets_results(
    keys: &[PartitionKey],
    response: ListOffsetsResponse,
) -> PartitionResults<ListedOffset> {
    let mut out = BTreeMap::new();
    for topic in response.topics {
        for partition in topic.partitions {
            let result = if partition.error_code == 0 {
                Ok(ListedOffset {
                    offset: partition.offset,
                    timestamp_ms: partition.timestamp,
                    leader_epoch: (partition.leader_epoch != -1).then_some(partition.leader_epoch),
                })
            } else {
                Err(KafkaError {
                    code: partition.error_code,
                    name: kafka_error_name(partition.error_code),
                    message: None,
                })
            };
            out.insert((topic.name.clone(), partition.partition_index), result);
        }
    }
    complete_results("ListOffsets", keys, out)
}

/// Whether a `ListOffsets` partition code retries the partition.
///
/// Kafka's `ListOffsetsHandler.handlePartitionError` sends
/// `NOT_LEADER_OR_FOLLOWER` and `LEADER_NOT_AVAILABLE` back to the leader
/// lookup and retries every other `RetriableException` on the same leader.
/// These are the codes whose exception is a `RetriableException`.
const fn list_offsets_retry_code(code: i16) -> bool {
    matches!(
        code,
        // CORRUPT_MESSAGE, UNKNOWN_TOPIC_OR_PARTITION, LEADER_NOT_AVAILABLE,
        // NOT_LEADER_OR_FOLLOWER, REQUEST_TIMED_OUT
        2 | 3 | 5 | 6 | 7
        // REPLICA_NOT_AVAILABLE, NETWORK_EXCEPTION, the coordinator errors
        | 9 | 13 | 14 | 15 | 16
        // NOT_ENOUGH_REPLICAS, NOT_ENOUGH_REPLICAS_AFTER_APPEND, NOT_CONTROLLER
        | 19 | 20 | 41
        // CONCURRENT_TRANSACTIONS, KAFKA_STORAGE_ERROR
        | 51 | 56
        // FETCH_SESSION_ID_NOT_FOUND, INVALID_FETCH_SESSION_EPOCH,
        // LISTENER_NOT_FOUND, FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH
        | 70 | 71 | 72 | 74 | 75
        // OFFSET_NOT_AVAILABLE, PREFERRED_LEADER_NOT_AVAILABLE,
        // ELIGIBLE_LEADERS_NOT_AVAILABLE, UNSTABLE_OFFSET_COMMIT
        | 78 | 80 | 83 | 88
        // UNKNOWN_TOPIC_ID, INCONSISTENT_TOPIC_ID
        | 100 | 103
    )
}

/// The lowest `ListOffsets` version that the lookup of `spec` with
/// `isolation_level` needs.
const fn required_version(spec: OffsetSpec, isolation_level: IsolationLevel) -> i16 {
    let spec_version = spec.min_version();
    let isolation_version = isolation_level.min_version();
    if spec_version > isolation_version {
        spec_version
    } else {
        isolation_version
    }
}

/// Splits `keys` by whether a leader whose highest usable `ListOffsets`
/// version is `max_version` can answer them. Each key it cannot answer gets
/// `UNSUPPORTED_VERSION`, as Kafka's `ListOffsetsHandler.handleUnsupportedError`
/// fails the partitions of an unsupported spec and keeps the others.
fn split_supported(
    keys: Vec<PartitionKey>,
    specs: &BTreeMap<PartitionKey, OffsetSpec>,
    isolation_level: IsolationLevel,
    max_version: i16,
) -> (Vec<PartitionKey>, PartitionResults<ListedOffset>) {
    let mut supported = Vec::new();
    let mut unsupported = BTreeMap::new();
    for key in keys {
        let spec = specs.get(&key).copied().unwrap_or(OffsetSpec::Latest);
        let needed = required_version(spec, isolation_level);
        if needed <= max_version {
            supported.push(key);
        } else {
            unsupported.insert(
                key,
                Err(KafkaError {
                    code: UNSUPPORTED_VERSION,
                    name: kafka_error_name(UNSUPPORTED_VERSION),
                    message: Some(format!(
                        "the leader supports ListOffsets up to v{max_version}, and {spec:?} with \
                         {isolation_level:?} needs v{needed}"
                    )),
                }),
            );
        }
    }
    (supported, unsupported)
}

/// Sends one `ListOffsets` request for `keys` to the leader at `address`.
async fn list_offsets_on_leader(
    address: String,
    options: ConnectionOptions,
    keys: Vec<PartitionKey>,
    specs: &BTreeMap<PartitionKey, OffsetSpec>,
    isolation_level: IsolationLevel,
    timeout_ms: i32,
) -> Result<PartitionResults<ListedOffset>, AdminError> {
    let connection = AdminClient::connect_one(&address, options).await?;
    let max_version = connection
        .advertised_api_range(list_offsets_request::API_KEY)
        .map_or(-1, |(_, max)| {
            max.min(list_offsets_request::LATEST_STABLE_VERSION)
        });
    let (supported, mut out) = split_supported(keys, specs, isolation_level, max_version);
    if !supported.is_empty() {
        let request = list_offsets_request(&supported, specs, isolation_level, timeout_ms);
        let response = connection.send(request).await?;
        out.extend(list_offsets_results(&supported, response));
    }
    Ok(out)
}

impl AdminClient {
    /// Looks up the offset that `specs` names for each partition, as Kafka's
    /// `listOffsets` operation does.
    ///
    /// The client finds the leader of each partition with `Metadata` and
    /// sends one `ListOffsets` request to each leader, all at the same time,
    /// as Kafka's `ListOffsetsHandler` with its `PartitionLeaderStrategy`
    /// does. The request asks as a consumer (`replica_id` -1) with
    /// `isolation_level`, and carries the call timeout as its `timeout_ms`.
    /// The result has one entry for each partition of `specs`.
    ///
    /// A partition goes back to the leader lookup, and the call finds its
    /// leader again after the backoff, when the lookup names no leader yet,
    /// when the connection to its leader fails or is lost, and when the
    /// leader answers a retriable code, such as `NOT_LEADER_OR_FOLLOWER` (6)
    /// or `LEADER_NOT_AVAILABLE` (5). The call stops at Kafka's default
    /// `default.api.timeout.ms` (60 s), where each unresolved partition gets
    /// `REQUEST_TIMED_OUT` (7).
    ///
    /// A spec that the leader's `ListOffsets` versions cannot answer, such as
    /// [`OffsetSpec::MaxTimestamp`] below v7, fails with
    /// `UNSUPPORTED_VERSION` (35) for its partition only, as Kafka's
    /// `ListOffsetsHandler.handleUnsupportedError` does.
    ///
    /// # Errors
    ///
    /// The call itself does not fail. Each partition gets its own
    /// [`KafkaError`] in the result, such as the final lookup or
    /// `ListOffsets` error of that partition.
    pub async fn list_offsets(
        &self,
        specs: &BTreeMap<(String, i32), OffsetSpec>,
        isolation_level: IsolationLevel,
    ) -> BTreeMap<(String, i32), Result<ListedOffset, KafkaError>> {
        let keys = specs.keys().cloned().collect::<Vec<_>>();
        let timeout_ms = i32::try_from(self.retry.timeout.as_millis()).unwrap_or(i32::MAX);
        self.call_partition_leaders(
            "ListOffsets",
            &keys,
            list_offsets_retry_code,
            |address, keys| {
                list_offsets_on_leader(
                    address,
                    self.options.clone(),
                    keys,
                    specs,
                    isolation_level,
                    timeout_ms,
                )
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use krabka_protocol::owned::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };

    use super::*;
    use crate::partition_leaders::test_support::{
        Seen, decode_request, encode_response, fast_admin, leader_broker, refused_address,
    };

    fn key(partition: i32) -> PartitionKey {
        ("orders".to_owned(), partition)
    }

    #[test]
    fn request_carries_each_spec_as_kafka_encodes_it() {
        let specs = BTreeMap::from([
            (key(0), OffsetSpec::Earliest),
            (key(1), OffsetSpec::Latest),
            (key(2), OffsetSpec::MaxTimestamp),
            (key(3), OffsetSpec::EarliestLocal),
            (key(4), OffsetSpec::LatestTiered),
            (key(5), OffsetSpec::EarliestPendingUpload),
            (
                ("audit".to_owned(), 0),
                OffsetSpec::ForTimestamp(1_700_000_000_000),
            ),
        ]);
        let keys = specs.keys().cloned().collect::<Vec<_>>();
        let partition = |partition_index, timestamp| ListOffsetsPartition {
            partition_index,
            timestamp,
            ..Default::default()
        };

        assert2::assert!(
            list_offsets_request(&keys, &specs, IsolationLevel::ReadCommitted, 30_000)
                == ListOffsetsRequest {
                    replica_id: -1,
                    isolation_level: 1,
                    topics: vec![
                        ListOffsetsTopic {
                            name: "audit".to_owned(),
                            partitions: vec![partition(0, 1_700_000_000_000)],
                            ..Default::default()
                        },
                        ListOffsetsTopic {
                            name: "orders".to_owned(),
                            partitions: vec![
                                partition(0, -2),
                                partition(1, -1),
                                partition(2, -3),
                                partition(3, -4),
                                partition(4, -5),
                                partition(5, -6),
                            ],
                            ..Default::default()
                        },
                    ],
                    timeout_ms: 30_000,
                    ..Default::default()
                }
        );
    }

    #[test]
    fn required_version_matches_kafka_for_consumer() {
        for (spec, isolation_level, expected) in [
            (OffsetSpec::Latest, IsolationLevel::ReadUncommitted, 1),
            (
                OffsetSpec::ForTimestamp(5),
                IsolationLevel::ReadUncommitted,
                1,
            ),
            (OffsetSpec::Earliest, IsolationLevel::ReadCommitted, 2),
            (OffsetSpec::MaxTimestamp, IsolationLevel::ReadCommitted, 7),
            (
                OffsetSpec::EarliestLocal,
                IsolationLevel::ReadUncommitted,
                8,
            ),
            (OffsetSpec::LatestTiered, IsolationLevel::ReadUncommitted, 9),
            (
                OffsetSpec::EarliestPendingUpload,
                IsolationLevel::ReadUncommitted,
                11,
            ),
        ] {
            assert2::assert!(
                required_version(spec, isolation_level) == expected,
                "{spec:?} {isolation_level:?}"
            );
        }
    }

    #[test]
    fn results_map_offsets_and_errors_of_each_partition() {
        let response = ListOffsetsResponse {
            topics: vec![ListOffsetsTopicResponse {
                name: "orders".to_owned(),
                partitions: vec![
                    ListOffsetsPartitionResponse {
                        partition_index: 0,
                        timestamp: 1_700_000_000_000,
                        offset: 42,
                        leader_epoch: 3,
                        ..Default::default()
                    },
                    ListOffsetsPartitionResponse {
                        partition_index: 1,
                        offset: 7,
                        ..Default::default()
                    },
                    ListOffsetsPartitionResponse {
                        partition_index: 2,
                        error_code: 3,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        assert2::assert!(
            list_offsets_results(&[key(0), key(1), key(2)], response)
                == BTreeMap::from([
                    (
                        key(0),
                        Ok(ListedOffset {
                            offset: 42,
                            timestamp_ms: 1_700_000_000_000,
                            leader_epoch: Some(3),
                        })
                    ),
                    (
                        key(1),
                        Ok(ListedOffset {
                            offset: 7,
                            timestamp_ms: -1,
                            leader_epoch: None,
                        })
                    ),
                    (
                        key(2),
                        Err(KafkaError {
                            code: 3,
                            name: "UNKNOWN_TOPIC_OR_PARTITION",
                            message: None,
                        })
                    ),
                ])
        );
    }

    /// The `ListOffsets` answer to `request`: offset 100 with leader epoch 4
    /// on `orders`-0, and `orders_1_code` (or offset 200) on `orders`-1.
    fn offsets_answer(request: &ListOffsetsRequest, orders_1_code: i16) -> ListOffsetsResponse {
        ListOffsetsResponse {
            topics: request
                .topics
                .iter()
                .map(|topic| ListOffsetsTopicResponse {
                    name: topic.name.clone(),
                    partitions: topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            let (error_code, offset) = if partition.partition_index == 0 {
                                (0, 100)
                            } else {
                                (orders_1_code, 200)
                            };
                            ListOffsetsPartitionResponse {
                                partition_index: partition.partition_index,
                                error_code,
                                offset,
                                leader_epoch: 4,
                                ..Default::default()
                            }
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// One mock-broker case of `list_offsets`.
    struct Case {
        name: &'static str,
        /// The highest `ListOffsets` version that the leader advertises.
        max_version: i16,
        /// The leader of each `Metadata` answer, `None` for the mock itself.
        leaders: Vec<Option<std::net::SocketAddr>>,
        /// The code of `orders`-1 in each `ListOffsets` answer.
        orders_1_codes: Vec<i16>,
        /// The spec of `orders`-1.
        orders_1_spec: OffsetSpec,
        /// The result of `orders`-1.
        expected: Result<ListedOffset, KafkaError>,
        /// The `Metadata` requests.
        metadata_requests: usize,
        /// The partitions of each `ListOffsets` request.
        requests: Vec<Vec<PartitionKey>>,
    }

    /// Apache Kafka's `ListOffsetsHandler` batches the partitions of one
    /// leader into one request, looks the leader up again after
    /// `NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE` or a lost connection,
    /// fails a partition on a non-retriable code, and fails only the
    /// partitions of a spec that the leader's versions cannot answer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_offsets_retries_and_fails_each_partition_as_kafka_does() {
        let offset_200 = || {
            Ok(ListedOffset {
                offset: 200,
                timestamp_ms: -1,
                leader_epoch: Some(4),
            })
        };
        let refused = refused_address().await;
        let cases = [
            Case {
                name: "success",
                max_version: 11,
                leaders: vec![None],
                orders_1_codes: vec![0],
                orders_1_spec: OffsetSpec::MaxTimestamp,
                expected: offset_200(),
                metadata_requests: 1,
                requests: vec![vec![key(0), key(1)]],
            },
            Case {
                name: "not leader or follower finds the leader again",
                max_version: 11,
                leaders: vec![None],
                orders_1_codes: vec![6, 0],
                orders_1_spec: OffsetSpec::Earliest,
                expected: offset_200(),
                metadata_requests: 2,
                requests: vec![vec![key(0), key(1)], vec![key(1)]],
            },
            Case {
                name: "leader not available finds the leader again",
                max_version: 11,
                leaders: vec![None],
                orders_1_codes: vec![5, 0],
                orders_1_spec: OffsetSpec::Earliest,
                expected: offset_200(),
                metadata_requests: 2,
                requests: vec![vec![key(0), key(1)], vec![key(1)]],
            },
            Case {
                name: "a refused connection finds the leader again",
                max_version: 11,
                leaders: vec![Some(refused), None],
                orders_1_codes: vec![0],
                orders_1_spec: OffsetSpec::Latest,
                expected: offset_200(),
                metadata_requests: 2,
                requests: vec![vec![key(0), key(1)]],
            },
            Case {
                name: "topic authorization failed is final",
                max_version: 11,
                leaders: vec![None],
                orders_1_codes: vec![29],
                orders_1_spec: OffsetSpec::Latest,
                expected: Err(KafkaError {
                    code: 29,
                    name: "UNKNOWN",
                    message: None,
                }),
                metadata_requests: 1,
                requests: vec![vec![key(0), key(1)]],
            },
            Case {
                name: "max timestamp below v7 fails only its partition",
                max_version: 6,
                leaders: vec![None],
                orders_1_codes: vec![0],
                orders_1_spec: OffsetSpec::MaxTimestamp,
                expected: Err(KafkaError {
                    code: UNSUPPORTED_VERSION,
                    name: "UNSUPPORTED_VERSION",
                    message: Some(
                        "the leader supports ListOffsets up to v6, and MaxTimestamp with \
                         ReadUncommitted needs v7"
                            .to_owned(),
                    ),
                }),
                metadata_requests: 1,
                requests: vec![vec![key(0)]],
            },
        ];
        for case in cases {
            let seen = Arc::new(Mutex::new(Seen::default()));
            let codes = case.orders_1_codes.clone();
            let broker = leader_broker(
                vec![(list_offsets_request::API_KEY, 1, case.max_version)],
                list_offsets_request::API_KEY,
                case.leaders.clone(),
                Arc::clone(&seen),
                move |n, version, body| {
                    let flexible = version >= list_offsets_request::FLEXIBLE_MIN;
                    let request: ListOffsetsRequest = decode_request(body, version, flexible);
                    let code = codes[n.min(codes.len() - 1)];
                    encode_response(&offsets_answer(&request, code), version, flexible)
                },
            )
            .await;
            let admin = fast_admin(broker.addr, krabka_units::secs(5)).await;
            let specs =
                BTreeMap::from([(key(0), OffsetSpec::Latest), (key(1), case.orders_1_spec)]);

            let result = admin
                .list_offsets(&specs, IsolationLevel::ReadUncommitted)
                .await;

            broker.stop();
            let seen = seen.lock().expect("seen lock");
            let requests = seen
                .requests
                .iter()
                .map(|(version, body)| {
                    let flexible = *version >= list_offsets_request::FLEXIBLE_MIN;
                    decode_request::<ListOffsetsRequest>(body, *version, flexible)
                })
                .collect::<Vec<_>>();
            let expected_requests = case
                .requests
                .iter()
                .map(|keys| {
                    // `timeout_ms` is on the wire from v10 only.
                    let timeout_ms = if case.max_version >= 10 { 5_000 } else { 0 };
                    list_offsets_request(keys, &specs, IsolationLevel::ReadUncommitted, timeout_ms)
                })
                .collect::<Vec<_>>();
            assert2::assert!(
                (result, seen.metadata_requests, requests)
                    == (
                        BTreeMap::from([
                            (
                                key(0),
                                Ok(ListedOffset {
                                    offset: 100,
                                    timestamp_ms: -1,
                                    leader_epoch: Some(4),
                                })
                            ),
                            (key(1), case.expected),
                        ]),
                        case.metadata_requests,
                        expected_requests,
                    ),
                "case {}",
                case.name
            );
        }
    }

    /// Kafka retries a partition whose topic the lookup does not know until
    /// the call deadline, and then fails it with a `TimeoutException`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_offsets_times_out_a_partition_that_never_gets_a_leader() {
        let seen = Arc::new(Mutex::new(Seen::default()));
        let broker = leader_broker(
            vec![(list_offsets_request::API_KEY, 1, 11)],
            list_offsets_request::API_KEY,
            vec![None],
            Arc::clone(&seen),
            |_, _, _| Vec::new(),
        )
        .await;
        let admin = fast_admin(broker.addr, krabka_units::millis(300)).await;

        let result = admin
            .list_offsets(
                &BTreeMap::from([(("missing".to_owned(), 0), OffsetSpec::Latest)]),
                IsolationLevel::ReadUncommitted,
            )
            .await;

        broker.stop();
        let codes = result
            .into_iter()
            .map(|(key, result)| (key, result.map_err(|error| error.code)))
            .collect::<Vec<_>>();
        let seen = seen.lock().expect("seen lock");
        assert2::assert!(codes == vec![(("missing".to_owned(), 0), Err(7))]);
        assert2::assert!(seen.metadata_requests > 1);
        assert2::assert!(seen.requests.is_empty());
    }
}
