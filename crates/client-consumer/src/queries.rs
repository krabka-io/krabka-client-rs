//! Offset and topic metadata queries: Kafka's `KafkaConsumer.beginningOffsets`,
//! `endOffsets`, `offsetsForTimes`, `currentLag`, `partitionsFor` and
//! `listTopics`.
//!
//! The queries do not need the partitions to be assigned. They send their own
//! `Metadata` request for the topics, as Kafka's `OffsetFetcher` adds the
//! topics as transient topics, and then one `ListOffsets` request per leader.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use krabka_client_core::MetadataScope;
use krabka_protocol::owned::{
    list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    list_offsets_response::ListOffsetsResponse,
    metadata_request::MetadataRequest,
    metadata_response::MetadataResponse,
};
use krabka_units::convert::TimeExt as _;

use crate::{
    IsolationLevel, consumer::Consumer, coordinator::next_backoff, error::ConsumerError,
    poll::is_reset_sentinel,
};

/// `TOPIC_AUTHORIZATION_FAILED`.
const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
/// `UNKNOWN_TOPIC_OR_PARTITION`.
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// `INVALID_TOPIC_EXCEPTION`.
const INVALID_TOPIC_EXCEPTION: i16 = 17;
/// `UNSUPPORTED_FOR_MESSAGE_FORMAT`.
const UNSUPPORTED_FOR_MESSAGE_FORMAT: i16 = 43;

/// The offset of a timestamp search. Kafka's `OffsetAndTimestamp`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OffsetAndTimestamp {
    /// The first offset whose timestamp is at or after the searched timestamp.
    pub offset: i64,
    /// The timestamp of the record at `offset`.
    pub timestamp: i64,
    /// The leader epoch of the record at `offset`, if the broker knows it.
    pub leader_epoch: Option<i32>,
}

/// A broker of the cluster. Kafka's `Node`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    pub id: i32,
    /// The host, or empty when the metadata names no broker with this id.
    pub host: String,
    /// The port, or `-1` when the metadata names no broker with this id.
    pub port: i32,
    pub rack: Option<String>,
}

/// The metadata of one partition. Kafka's `PartitionInfo`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionInfo {
    pub topic: String,
    pub partition: i32,
    /// The leader, or `None` when the partition has no leader.
    pub leader: Option<Node>,
    pub replicas: Vec<Node>,
    pub in_sync_replicas: Vec<Node>,
    pub offline_replicas: Vec<Node>,
}

/// A `ListOffsets` request that searches by timestamp, from version 1. Kafka's
/// `ListOffsetsRequest.Builder.forConsumer(requireTimestamp = true, ..)` sets
/// the oldest allowed version to 1, because version 0 has no timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TimestampListOffsets(ListOffsetsRequest);

impl krabka_protocol::Encode for TimestampListOffsets {
    fn encode<B: bytes::BufMut>(
        &self,
        buf: &mut B,
        version: i16,
    ) -> Result<(), krabka_protocol::ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl krabka_protocol::ProtocolRequest for TimestampListOffsets {
    const API_KEY: i16 = krabka_protocol::owned::list_offsets_request::API_KEY;
    const MIN_VERSION: i16 = 1;
    const MAX_VERSION: i16 = krabka_protocol::owned::list_offsets_request::MAX_VERSION;
    const LATEST_STABLE_VERSION: i16 =
        krabka_protocol::owned::list_offsets_request::LATEST_STABLE_VERSION;
    const FLEXIBLE_MIN: i16 = krabka_protocol::owned::list_offsets_request::FLEXIBLE_MIN;
    type Response = ListOffsetsResponse;
}

/// What a query takes from one `ListOffsets` partition row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryRow {
    Found(OffsetAndTimestamp),
    /// No offset, and no retry: `NONE` with offset `-1`, or
    /// `UNSUPPORTED_FOR_MESSAGE_FORMAT`.
    NotFound,
    Unauthorized,
    Retry,
}

/// Kafka's `OffsetFetcherUtils.handleListOffsetResponse` for one row.
fn classify_query_row(error_code: i16, offset: i64, timestamp: i64, leader_epoch: i32) -> QueryRow {
    match error_code {
        0 if offset != -1 => QueryRow::Found(OffsetAndTimestamp {
            offset,
            timestamp,
            leader_epoch: (leader_epoch >= 0).then_some(leader_epoch),
        }),
        0 | UNSUPPORTED_FOR_MESSAGE_FORMAT => QueryRow::NotFound,
        TOPIC_AUTHORIZATION_FAILED => QueryRow::Unauthorized,
        _ => QueryRow::Retry,
    }
}

/// Fail for a requested topic that `metadata` does not authorize or names as
/// invalid. Kafka's `Metadata.update` records these errors, and the next
/// `client.poll` of `OffsetFetcher.fetchOffsetsByTimes` throws
/// `TopicAuthorizationException` or `InvalidTopicException`. Other topic
/// errors wait for the next round.
fn requested_topic_errors(
    metadata: &MetadataResponse,
    topics: &BTreeSet<String>,
) -> Result<(), ConsumerError> {
    let requested = |topic: &&krabka_protocol::owned::metadata_response::MetadataResponseTopic| {
        topic
            .name
            .as_ref()
            .is_some_and(|name| topics.contains(name))
    };
    let unauthorized: BTreeSet<String> = metadata
        .topics
        .iter()
        .filter(requested)
        .filter(|topic| topic.error_code == TOPIC_AUTHORIZATION_FAILED)
        .filter_map(|topic| topic.name.clone())
        .collect();
    if !unauthorized.is_empty() {
        return Err(ConsumerError::TopicAuthorizationFailed(unauthorized));
    }
    if let Some(name) = metadata
        .topics
        .iter()
        .filter(requested)
        .find(|topic| topic.error_code == INVALID_TOPIC_EXCEPTION)
        .and_then(|topic| topic.name.clone())
    {
        return Err(ConsumerError::InvalidTopic(name));
    }
    Ok(())
}

/// The leader id and epoch of each partition in `metadata` that has a leader.
fn partition_leaders(metadata: &MetadataResponse) -> HashMap<(String, i32), (i32, i32)> {
    metadata
        .topics
        .iter()
        .filter(|topic| topic.error_code == 0)
        .filter_map(|topic| topic.name.as_ref().map(|name| (name, topic)))
        .flat_map(|(name, topic)| {
            topic
                .partitions
                .iter()
                .filter(|partition| partition.leader_id >= 0)
                .map(move |partition| {
                    (
                        (name.clone(), partition.partition_index),
                        (partition.leader_id, partition.leader_epoch),
                    )
                })
        })
        .collect()
}

/// One partition of a `ListOffsets` query: `((topic, partition), current
/// leader epoch, timestamp)`.
type QueryPartition = ((String, i32), i32, i64);

/// The `ListOffsets` request for `partitions`.
fn query_request(
    partitions: &[QueryPartition],
    isolation_level: IsolationLevel,
) -> ListOffsetsRequest {
    let mut by_topic: BTreeMap<&str, Vec<ListOffsetsPartition>> = BTreeMap::new();
    for ((topic, partition), leader_epoch, timestamp) in partitions {
        by_topic
            .entry(topic)
            .or_default()
            .push(ListOffsetsPartition {
                partition_index: *partition,
                current_leader_epoch: *leader_epoch,
                timestamp: *timestamp,
                ..Default::default()
            });
    }
    ListOffsetsRequest {
        replica_id: -1,
        isolation_level: isolation_level.wire(),
        topics: by_topic
            .into_iter()
            .map(|(name, partitions)| ListOffsetsTopic {
                name: name.to_owned(),
                partitions,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// Kafka's `TopicMetadataFetcher.getTopicMetadata` for one response. `None`
/// means that a topic has a retriable error and the request must be sent
/// again.
fn topic_partition_infos(
    metadata: &MetadataResponse,
) -> Result<Option<HashMap<String, Vec<PartitionInfo>>>, ConsumerError> {
    let unauthorized: BTreeSet<String> = metadata
        .topics
        .iter()
        .filter(|topic| topic.error_code == TOPIC_AUTHORIZATION_FAILED)
        .filter_map(|topic| topic.name.clone())
        .collect();
    if !unauthorized.is_empty() {
        return Err(ConsumerError::TopicAuthorizationFailed(unauthorized));
    }
    let mut retry = false;
    for topic in &metadata.topics {
        let name = topic.name.clone().unwrap_or_default();
        match topic.error_code {
            0 | UNKNOWN_TOPIC_OR_PARTITION => {}
            INVALID_TOPIC_EXCEPTION => return Err(ConsumerError::InvalidTopic(name)),
            code if crate::offset_wire::is_retriable_error(code) => retry = true,
            code => return Err(ConsumerError::Server(code)),
        }
    }
    if retry {
        return Ok(None);
    }
    let node = |id: i32| {
        metadata
            .brokers
            .iter()
            .find(|broker| broker.node_id == id)
            .map_or_else(
                || Node {
                    id,
                    host: String::new(),
                    port: -1,
                    rack: None,
                },
                |broker| Node {
                    id,
                    host: broker.host.clone(),
                    port: broker.port,
                    rack: broker.rack.clone(),
                },
            )
    };
    let nodes = |ids: &[i32]| ids.iter().map(|id| node(*id)).collect::<Vec<_>>();
    Ok(Some(
        metadata
            .topics
            .iter()
            .filter(|topic| topic.error_code == 0)
            .filter_map(|topic| topic.name.as_ref().map(|name| (name, topic)))
            .map(|(name, topic)| {
                let partitions = topic
                    .partitions
                    .iter()
                    .map(|partition| PartitionInfo {
                        topic: name.clone(),
                        partition: partition.partition_index,
                        leader: metadata
                            .brokers
                            .iter()
                            .any(|broker| broker.node_id == partition.leader_id)
                            .then(|| node(partition.leader_id)),
                        replicas: nodes(&partition.replica_nodes),
                        in_sync_replicas: nodes(&partition.isr_nodes),
                        offline_replicas: nodes(&partition.offline_replicas),
                    })
                    .collect();
                (name.clone(), partitions)
            })
            .collect(),
    ))
}

impl Consumer {
    /// The first offset of each partition. Kafka's
    /// `KafkaConsumer.beginningOffsets`.
    ///
    /// A partition without an offset is not in the result.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::TopicAuthorizationFailed`],
    /// [`ConsumerError::Timeout`] when some offsets are not known before
    /// `default_api_timeout`, or a request error that is not retriable.
    pub async fn beginning_offsets(
        &self,
        partitions: &[(String, i32)],
    ) -> Result<HashMap<(String, i32), i64>, ConsumerError> {
        self.boundary_offsets(partitions, crate::poll::EARLIEST_TIMESTAMP)
            .await
    }

    /// The end offset of each partition: the high watermark, or the last
    /// stable offset with `read_committed`. Kafka's `KafkaConsumer.endOffsets`.
    ///
    /// A partition without an offset is not in the result.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::TopicAuthorizationFailed`],
    /// [`ConsumerError::Timeout`] when some offsets are not known before
    /// `default_api_timeout`, or a request error that is not retriable.
    pub async fn end_offsets(
        &self,
        partitions: &[(String, i32)],
    ) -> Result<HashMap<(String, i32), i64>, ConsumerError> {
        self.boundary_offsets(partitions, crate::poll::LATEST_TIMESTAMP)
            .await
    }

    async fn boundary_offsets(
        &self,
        partitions: &[(String, i32)],
        timestamp: i64,
    ) -> Result<HashMap<(String, i32), i64>, ConsumerError> {
        let search = partitions
            .iter()
            .map(|partition| (partition.clone(), timestamp))
            .collect();
        Ok(self
            .search_offsets(search, false)
            .await?
            .into_iter()
            .map(|(partition, found)| (partition, found.offset))
            .collect())
    }

    /// The first offset of each partition whose timestamp is at or after the
    /// given timestamp. Kafka's `KafkaConsumer.offsetsForTimes`.
    ///
    /// A partition without such a record maps to `None`.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::InvalidArgument`] for a negative timestamp,
    /// [`ConsumerError::TopicAuthorizationFailed`], [`ConsumerError::Timeout`]
    /// when some offsets are not known before `default_api_timeout`, or a
    /// request error that is not retriable.
    pub async fn offsets_for_times(
        &self,
        timestamps: &HashMap<(String, i32), i64>,
    ) -> Result<HashMap<(String, i32), Option<OffsetAndTimestamp>>, ConsumerError> {
        if let Some(((topic, partition), timestamp)) =
            timestamps.iter().find(|(_, timestamp)| **timestamp < 0)
        {
            return Err(ConsumerError::InvalidArgument(format!(
                "The target time for partition {topic}-{partition} is {timestamp}. The target time cannot be negative."
            )));
        }
        let mut found = self.search_offsets(timestamps.clone(), true).await?;
        Ok(timestamps
            .keys()
            .map(|partition| (partition.clone(), found.remove(partition)))
            .collect())
    }

    /// The number of records between the position of an assigned partition
    /// and its end offset from the last fetch. Kafka's
    /// `KafkaConsumer.currentLag`.
    ///
    /// `None` when the position or the end offset is not known.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::NoCurrentAssignment`] if the consumer does not
    /// own the partition.
    pub async fn current_lag(
        &self,
        topic: impl Into<String>,
        partition: i32,
    ) -> Result<Option<i64>, ConsumerError> {
        let key = (topic.into(), partition);
        let assigned = self.assigned.lock().await;
        if !assigned.contains(&key) {
            let (topic, partition) = key;
            return Err(ConsumerError::NoCurrentAssignment { topic, partition });
        }
        let offsets = self.next_offsets.lock().await;
        // Kafka's `partitionLag` needs a valid position: not a pending reset,
        // and not one that waits for validation.
        let awaiting_validation = self
            .positions
            .lock()
            .await
            .get(&key)
            .is_some_and(|position| position.awaiting_validation);
        let position = offsets
            .get(&key)
            .copied()
            .filter(|offset| !is_reset_sentinel(*offset) && !awaiting_validation);
        drop(offsets);
        drop(assigned);
        let end = self.end_offsets.lock().await.get(&key).copied();
        Ok(position.zip(end).map(|(position, end)| end - position))
    }

    /// Send `ListOffsets` for each partition of `search` with its timestamp,
    /// until each partition has an answer. Kafka's
    /// `OffsetFetcher.fetchOffsetsByTimes`.
    ///
    /// Each round sends `Metadata` for the topics and one `ListOffsets` to
    /// each leader. A partition without a leader, a retriable row or a
    /// transport error waits for the next round. `require_timestamps` (a
    /// timestamp search) and `read_committed` need `ListOffsets` version 2 or
    /// higher.
    async fn search_offsets(
        &self,
        search: HashMap<(String, i32), i64>,
        require_timestamps: bool,
    ) -> Result<HashMap<(String, i32), OffsetAndTimestamp>, ConsumerError> {
        let started = tokio::time::Instant::now();
        let deadline = started + self.default_api_timeout.to_std();
        // The deadline bounds each request too, as Kafka's timer bounds each
        // `client.poll`.
        tokio::time::timeout_at(
            deadline,
            self.search_offsets_until(search, require_timestamps, started, deadline),
        )
        .await
        .unwrap_or_else(|_| {
            Err(ConsumerError::Timeout(format!(
                "Failed to get offsets by times in {}ms",
                started.elapsed().as_millis()
            )))
        })
    }

    async fn search_offsets_until(
        &self,
        mut search: HashMap<(String, i32), i64>,
        require_timestamps: bool,
        started: tokio::time::Instant,
        deadline: tokio::time::Instant,
    ) -> Result<HashMap<(String, i32), OffsetAndTimestamp>, ConsumerError> {
        let mut backoff = self.retry_policy.initial_backoff;
        let mut found = HashMap::new();
        while !search.is_empty() {
            let topics: BTreeSet<String> = search.keys().map(|(topic, _)| topic.clone()).collect();
            let request = krabka_client_core::topics_request(
                topics.iter().cloned(),
                self.allows_auto_topic_creation(),
            );
            match self.client.refresh_metadata_with(request).await {
                Ok(metadata) => {
                    requested_topic_errors(&metadata, &topics)?;
                    let leaders = partition_leaders(&metadata);
                    let mut by_leader: BTreeMap<i32, Vec<QueryPartition>> = BTreeMap::new();
                    for (partition, timestamp) in &search {
                        if let Some((leader, epoch)) = leaders.get(partition)
                            && self.client.knows_broker(*leader)
                        {
                            by_leader.entry(*leader).or_default().push((
                                partition.clone(),
                                *epoch,
                                *timestamp,
                            ));
                        }
                    }
                    let mut unauthorized = BTreeSet::new();
                    for (leader, partitions) in by_leader {
                        let request = query_request(&partitions, self.isolation_level);
                        let broker = self.client.broker(leader);
                        // Kafka's `ListOffsetsRequest.Builder.forConsumer`:
                        // version 2 for `read_committed`, version 1 for a
                        // timestamp search.
                        let answer = if self.isolation_level == IsolationLevel::ReadCommitted {
                            broker
                                .send(crate::poll::ReadCommittedListOffsets(request))
                                .await
                        } else if require_timestamps {
                            broker.send(TimestampListOffsets(request)).await
                        } else {
                            broker.send(request).await
                        };
                        match answer {
                            Ok(answer) => {
                                apply_query_answer(
                                    &answer,
                                    &mut search,
                                    &mut found,
                                    &mut unauthorized,
                                );
                            }
                            Err(error) if crate::poll::is_transient_transport_error(&error) => {
                                self.client.evict_broker(leader);
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    if !unauthorized.is_empty() {
                        return Err(ConsumerError::TopicAuthorizationFailed(unauthorized));
                    }
                }
                Err(error) if crate::poll::is_transient_transport_error(&error) => {}
                Err(error) => return Err(error.into()),
            }
            if search.is_empty() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ConsumerError::Timeout(format!(
                    "Failed to get offsets by times in {}ms",
                    started.elapsed().as_millis()
                )));
            }
            tokio::time::sleep_until((tokio::time::Instant::now() + backoff).min(deadline)).await;
            backoff = next_backoff(backoff, self.retry_policy.max_backoff);
        }
        Ok(found)
    }

    /// The partitions of `topic`. Kafka's `KafkaConsumer.partitionsFor`.
    ///
    /// An unknown topic gives an empty list. With `allow_auto_create_topics`,
    /// the `Metadata` request lets the broker create the topic.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::TopicAuthorizationFailed`],
    /// [`ConsumerError::InvalidTopic`], [`ConsumerError::Timeout`] when a
    /// retriable error lasts past `default_api_timeout`, or a request error.
    pub async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, ConsumerError> {
        let request = krabka_client_core::topics_request(
            [topic.to_owned()],
            self.allows_auto_topic_creation(),
        );
        Ok(self
            .topic_metadata(request)
            .await?
            .remove(topic)
            .unwrap_or_default())
    }

    /// The partitions of every topic of the cluster. Kafka's
    /// `KafkaConsumer.listTopics`.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::TopicAuthorizationFailed`],
    /// [`ConsumerError::Timeout`] when a retriable error lasts past
    /// `default_api_timeout`, or a request error.
    pub async fn list_topics(&self) -> Result<HashMap<String, Vec<PartitionInfo>>, ConsumerError> {
        self.topic_metadata(MetadataRequest::default()).await
    }

    /// Whether the consumer lets a `Metadata` request create a topic.
    fn allows_auto_topic_creation(&self) -> bool {
        matches!(
            self.client.metadata_topics().scope(),
            MetadataScope::Topics {
                allow_auto_topic_creation: true
            }
        )
    }

    /// Send `request` until its topics have no retriable error. Kafka's
    /// `TopicMetadataFetcher.getTopicMetadata`.
    async fn topic_metadata(
        &self,
        request: MetadataRequest,
    ) -> Result<HashMap<String, Vec<PartitionInfo>>, ConsumerError> {
        let deadline = tokio::time::Instant::now() + self.default_api_timeout.to_std();
        tokio::time::timeout_at(deadline, self.topic_metadata_until(request, deadline))
            .await
            .unwrap_or_else(|_| {
                Err(ConsumerError::Timeout(
                    "Timeout expired while fetching topic metadata".to_owned(),
                ))
            })
    }

    async fn topic_metadata_until(
        &self,
        request: MetadataRequest,
        deadline: tokio::time::Instant,
    ) -> Result<HashMap<String, Vec<PartitionInfo>>, ConsumerError> {
        let mut backoff = self.retry_policy.initial_backoff;
        loop {
            match self.client.refresh_metadata_with(request.clone()).await {
                Ok(metadata) => {
                    if let Some(topics) = topic_partition_infos(&metadata)? {
                        return Ok(topics);
                    }
                }
                Err(error) if crate::poll::is_transient_transport_error(&error) => {}
                Err(error) => return Err(error.into()),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ConsumerError::Timeout(
                    "Timeout expired while fetching topic metadata".to_owned(),
                ));
            }
            tokio::time::sleep_until((tokio::time::Instant::now() + backoff).min(deadline)).await;
            backoff = next_backoff(backoff, self.retry_policy.max_backoff);
        }
    }
}

/// Take the rows of `answer` for the partitions that `search` still has.
fn apply_query_answer(
    answer: &ListOffsetsResponse,
    search: &mut HashMap<(String, i32), i64>,
    found: &mut HashMap<(String, i32), OffsetAndTimestamp>,
    unauthorized: &mut BTreeSet<String>,
) {
    for topic in &answer.topics {
        for row in &topic.partitions {
            let key = (topic.name.clone(), row.partition_index);
            if !search.contains_key(&key) {
                continue;
            }
            match classify_query_row(row.error_code, row.offset, row.timestamp, row.leader_epoch) {
                QueryRow::Found(offset) => {
                    search.remove(&key);
                    found.insert(key, offset);
                }
                QueryRow::NotFound => {
                    search.remove(&key);
                }
                QueryRow::Unauthorized => {
                    unauthorized.insert(topic.name.clone());
                }
                QueryRow::Retry => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicU16, Ordering},
        },
    };

    use krabka_client_core::{Client, MockBroker};
    use krabka_protocol::{
        Decode as _, Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            list_offsets_request::{
                self, ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
            },
            list_offsets_response::{
                ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
            },
            metadata_request,
            metadata_response::{
                MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
                MetadataResponseTopic,
            },
        },
    };
    use krabka_units::{millis, secs};

    use super::*;
    use crate::poll::partition_error_tests::consumer_with_client;

    /// A `ListOffsets` row of the mock: `(error code, offset, timestamp,
    /// leader epoch)` for a partition and a searched timestamp.
    type Rows = fn(i32, i64) -> (i16, i64, i64, i32);

    type SentRequests = Arc<std::sync::Mutex<Vec<ListOffsetsRequest>>>;

    fn encode(response: &impl Encode, version: i16) -> Vec<u8> {
        let mut buf = bytes::BytesMut::new();
        response.encode(&mut buf, version).expect("encode");
        buf.to_vec()
    }

    fn metadata(port: i32, error_code: i16) -> MetadataResponse {
        MetadataResponse {
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port,
                rack: Some("az-1".into()),
                ..Default::default()
            }],
            topics: vec![MetadataResponseTopic {
                error_code,
                name: Some("orders".into()),
                partitions: (0..2)
                    .map(|partition_index| MetadataResponsePartition {
                        partition_index,
                        leader_id: 1,
                        leader_epoch: 4,
                        replica_nodes: vec![1, 2],
                        isr_nodes: vec![1],
                        offline_replicas: vec![2],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// A broker that leads `orders-0` and `orders-1`, answers `Metadata` with
    /// `metadata_error` for the topic, and answers each `ListOffsets`
    /// partition with `rows`, or not at all for `None`.
    async fn query_broker(
        rows: Option<Rows>,
        metadata_error: i16,
        list_offsets_max_version: i16,
        sent: SentRequests,
    ) -> MockBroker {
        use bytes::Buf as _;
        let port = Arc::new(AtomicU16::new(0));
        let port_in_mock = Arc::clone(&port);
        let broker = MockBroker::start(move |api_key, version, _corr_id, mut body| {
            if api_key == api_versions_request::API_KEY {
                let versions = ApiVersionsResponse {
                    api_keys: [
                        (api_versions_request::API_KEY, 3),
                        (metadata_request::API_KEY, 8),
                        (list_offsets_request::API_KEY, list_offsets_max_version),
                    ]
                    .into_iter()
                    .map(|(api_key, max_version)| ApiVersion {
                        api_key,
                        min_version: 0,
                        max_version,
                        ..Default::default()
                    })
                    .collect(),
                    ..Default::default()
                };
                return Some(encode(&versions, 0));
            }
            if api_key == metadata_request::API_KEY {
                let port = i32::from(port_in_mock.load(Ordering::SeqCst));
                return Some(encode(&metadata(port, metadata_error), version));
            }
            if api_key == list_offsets_request::API_KEY {
                let client_id_len = usize::try_from(body.get_i16()).expect("client id");
                body.advance(client_id_len);
                let request = ListOffsetsRequest::decode(&mut body, version).expect("decode");
                sent.lock().expect("sent lock").push(request.clone());
                let rows = rows?;
                let answer = ListOffsetsResponse {
                    topics: request
                        .topics
                        .iter()
                        .map(|topic| ListOffsetsTopicResponse {
                            name: topic.name.clone(),
                            partitions: topic
                                .partitions
                                .iter()
                                .map(|partition| {
                                    let (error_code, offset, timestamp, leader_epoch) =
                                        rows(partition.partition_index, partition.timestamp);
                                    ListOffsetsPartitionResponse {
                                        partition_index: partition.partition_index,
                                        error_code,
                                        offset,
                                        timestamp,
                                        leader_epoch,
                                        ..Default::default()
                                    }
                                })
                                .collect(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                };
                return Some(encode(&answer, version));
            }
            None
        })
        .await;
        port.store(broker.addr.port(), Ordering::SeqCst);
        broker
    }

    fn request(isolation_level: i8, timestamps: [i64; 2]) -> ListOffsetsRequest {
        ListOffsetsRequest {
            replica_id: -1,
            isolation_level,
            topics: vec![ListOffsetsTopic {
                name: "orders".into(),
                partitions: timestamps
                    .into_iter()
                    .zip(0..)
                    .map(|(timestamp, partition_index)| ListOffsetsPartition {
                        partition_index,
                        current_leader_epoch: 4,
                        timestamp,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// What one query returned, in a form that the table can compare.
    #[derive(Debug, PartialEq)]
    enum Answer {
        Offsets(HashMap<(String, i32), i64>),
        Times(HashMap<(String, i32), Option<OffsetAndTimestamp>>),
        Error(String),
    }

    #[derive(Clone, Copy, Debug)]
    enum Query {
        Beginning,
        End,
        Times([i64; 2]),
    }

    /// Kafka's `beginningOffsets`, `endOffsets` and `offsetsForTimes` send
    /// `ListOffsets` to the leader with the timestamps -2, -1 or the searched
    /// ones, and the isolation level of the consumer
    /// (`OffsetFetcher.fetchOffsetsByTimes`, `OffsetFetcherUtils.handleListOffsetResponse`).
    #[tokio::test]
    async fn offset_queries_follow_kafkas_offset_fetcher() {
        let key = |partition: i32| ("orders".to_string(), partition);
        let found: Rows = |partition, timestamp| {
            if timestamp < 0 {
                (0, i64::from(partition) + 3, -1, -1)
            } else if partition == 0 {
                (0, 42, timestamp + 5, 2)
            } else {
                (0, -1, -1, -1)
            }
        };
        let unauthorized: Rows = |_, _| (29, -1, -1, -1);
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, isolation, query, (rows, metadata_error, list_offsets_max), expected, expected_requests) in [
            (
                "beginning offsets",
                IsolationLevel::ReadUncommitted,
                Query::Beginning,
                (Some(found), 0, 5),
                Answer::Offsets(HashMap::from([(key(0), 3), (key(1), 4)])),
                vec![request(0, [-2, -2])],
            ),
            (
                "end offsets with read committed",
                IsolationLevel::ReadCommitted,
                Query::End,
                (Some(found), 0, 5),
                Answer::Offsets(HashMap::from([(key(0), 3), (key(1), 4)])),
                vec![request(1, [-1, -1])],
            ),
            (
                "offsets for times",
                IsolationLevel::ReadUncommitted,
                Query::Times([1000, 2000]),
                (Some(found), 0, 5),
                Answer::Times(HashMap::from([
                    (
                        key(0),
                        Some(OffsetAndTimestamp {
                            offset: 42,
                            timestamp: 1005,
                            leader_epoch: Some(2),
                        }),
                    ),
                    (key(1), None),
                ])),
                vec![request(0, [1000, 2000])],
            ),
            (
                "negative timestamp",
                IsolationLevel::ReadUncommitted,
                Query::Times([-1, 2000]),
                (Some(found), 0, 5),
                Answer::Error(
                    "invalid argument: The target time for partition orders-0 is -1. The target time cannot be negative."
                        .into(),
                ),
                vec![],
            ),
            (
                "unauthorized topic",
                IsolationLevel::ReadUncommitted,
                Query::Beginning,
                (Some(unauthorized), 0, 5),
                Answer::Error("not authorized to access topics: [orders]".into()),
                vec![request(0, [-2, -2])],
            ),
            (
                "metadata does not authorize the topic",
                IsolationLevel::ReadUncommitted,
                Query::Beginning,
                (Some(found), 29, 5),
                Answer::Error("not authorized to access topics: [orders]".into()),
                vec![],
            ),
            (
                "metadata names the topic invalid",
                IsolationLevel::ReadUncommitted,
                Query::End,
                (Some(found), 17, 5),
                Answer::Error("topic 'orders' is invalid".into()),
                vec![],
            ),
            (
                "a silent leader ends by the API timeout",
                IsolationLevel::ReadUncommitted,
                Query::Times([1000, 2000]),
                (None, 0, 5),
                Answer::Error("timeout".into()),
                vec![request(0, [1000, 2000])],
            ),
            (
                "offsets for times on a broker with ListOffsets v1",
                IsolationLevel::ReadUncommitted,
                Query::Times([1000, 2000]),
                (Some(found), 0, 1),
                Answer::Times(HashMap::from([
                    (
                        key(0),
                        Some(OffsetAndTimestamp {
                            offset: 42,
                            timestamp: 1005,
                            leader_epoch: None,
                        }),
                    ),
                    (key(1), None),
                ])),
                vec![ListOffsetsRequest {
                    replica_id: -1,
                    topics: vec![ListOffsetsTopic {
                        name: "orders".into(),
                        partitions: [1000, 2000]
                            .into_iter()
                            .zip(0..)
                            .map(|(timestamp, partition_index)| ListOffsetsPartition {
                                partition_index,
                                current_leader_epoch: -1,
                                timestamp,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            ),
        ] {
            let sent = SentRequests::default();
            let broker =
                query_broker(rows, metadata_error, list_offsets_max, Arc::clone(&sent)).await;
            let client = Client::builder()
                .bootstrap(broker.addr.to_string())
                .request_timeout(secs(30))
                .build()
                .await
                .expect("client");
            let mut consumer = consumer_with_client(client);
            consumer.isolation_level = isolation;
            consumer.default_api_timeout = millis(500);
            let partitions = [key(0), key(1)];
            let answer = match query {
                Query::Beginning => consumer.beginning_offsets(&partitions).await.map(Answer::Offsets),
                Query::End => consumer.end_offsets(&partitions).await.map(Answer::Offsets),
                Query::Times(timestamps) => consumer
                    .offsets_for_times(&partitions.iter().cloned().zip(timestamps).collect())
                    .await
                    .map(Answer::Times),
            }
            .unwrap_or_else(|error| match error {
                // The message names the elapsed time.
                ConsumerError::Timeout(_) => Answer::Error("timeout".into()),
                error => Answer::Error(error.to_string()),
            });
            broker.stop();
            let mut requests = sent.lock().expect("sent lock").clone();
            for request in &mut requests {
                for topic in &mut request.topics {
                    topic.partitions.sort_by_key(|partition| partition.partition_index);
                }
            }
            actual.push((name, answer, requests));
            wanted.push((name, expected, expected_requests));
        }
        assert2::assert!(actual == wanted);
    }

    /// Kafka's `TopicMetadataFetcher.getTopicMetadata`: an unauthorized topic
    /// and an invalid topic fail, an unknown topic is left out, a retriable
    /// error sends the request again, and a replica that the broker list does
    /// not name is `Node(id, "", -1)`.
    #[test]
    fn topic_partition_infos_follow_kafkas_topic_metadata_fetcher() {
        let node_1 = Node {
            id: 1,
            host: "127.0.0.1".into(),
            port: 9092,
            rack: Some("az-1".into()),
        };
        let node_2 = Node {
            id: 2,
            host: String::new(),
            port: -1,
            rack: None,
        };
        let orders = (0..2)
            .map(|partition| PartitionInfo {
                topic: "orders".into(),
                partition,
                leader: Some(node_1.clone()),
                replicas: vec![node_1.clone(), node_2.clone()],
                in_sync_replicas: vec![node_1.clone()],
                offline_replicas: vec![node_2.clone()],
            })
            .collect::<Vec<_>>();
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, error_code, expected) in [
            (
                "no error",
                0,
                Ok(Some(HashMap::from([(
                    "orders".to_string(),
                    orders.clone(),
                )]))),
            ),
            ("unknown topic", 3, Ok(Some(HashMap::new()))),
            ("leader not available", 5, Ok(None)),
            (
                "unauthorized",
                29,
                Err("not authorized to access topics: [orders]".to_string()),
            ),
            (
                "invalid topic",
                17,
                Err("topic 'orders' is invalid".to_string()),
            ),
        ] {
            let result = topic_partition_infos(&metadata(9092, error_code))
                .map_err(|error| error.to_string());
            actual.push((name, result));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// Kafka's `currentLag` is the end offset of the last fetch minus the
    /// position, empty when one of them is not known, and it throws for an
    /// unassigned partition (`SubscriptionState.partitionLag`).
    #[tokio::test]
    async fn current_lag_follows_kafkas_partition_lag() {
        let broker = query_broker(Some(|_, _| (0, 0, 0, 0)), 0, 5, SentRequests::default()).await;
        let client = Client::builder()
            .bootstrap(broker.addr.to_string())
            .build()
            .await
            .expect("client");
        let consumer = consumer_with_client(client);
        let key = ("orders".to_string(), 0);
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, partition, position, end, seek, awaiting_validation, expected) in [
            ("known", 0, 5, Some(12), None, false, Ok(Some(7))),
            ("after a seek", 0, 5, Some(12), Some(8), false, Ok(Some(4))),
            ("no end offset", 0, 5, None, None, false, Ok(None)),
            ("awaiting validation", 0, 5, Some(12), None, true, Ok(None)),
            (
                "awaiting reset",
                0,
                crate::poll::END_SENTINEL,
                Some(12),
                None,
                false,
                Ok(None),
            ),
            (
                "unassigned",
                9,
                5,
                Some(12),
                None,
                false,
                Err("no current assignment for partition orders-9".to_string()),
            ),
        ] {
            consumer
                .positions
                .lock()
                .await
                .entry(key.clone())
                .or_default()
                .awaiting_validation = awaiting_validation;
            consumer
                .next_offsets
                .lock()
                .await
                .insert(key.clone(), position);
            let mut ends = consumer.end_offsets.lock().await;
            ends.clear();
            if let Some(end) = end {
                ends.insert(key.clone(), end);
            }
            drop(ends);
            if let Some(offset) = seek {
                consumer.seek("orders", 0, offset).await.expect("seek");
            }
            let lag = consumer
                .current_lag("orders", partition)
                .await
                .map_err(|error| error.to_string());
            actual.push((name, lag));
            wanted.push((name, expected));
        }
        broker.stop();
        assert2::assert!(actual == wanted);
    }

    #[test]
    fn classify_query_row_follows_handle_list_offset_response() {
        let actual = [
            (0, 7, 100, 3),
            (0, 7, 100, -1),
            (0, -1, -1, -1),
            (43, -1, -1, -1),
            (29, -1, -1, -1),
            (6, -1, -1, -1),
        ]
        .map(|(error_code, offset, timestamp, epoch)| {
            classify_query_row(error_code, offset, timestamp, epoch)
        });
        let found = |leader_epoch| {
            QueryRow::Found(OffsetAndTimestamp {
                offset: 7,
                timestamp: 100,
                leader_epoch,
            })
        };
        assert2::assert!(
            actual
                == [
                    found(Some(3)),
                    found(None),
                    QueryRow::NotFound,
                    QueryRow::NotFound,
                    QueryRow::Unauthorized,
                    QueryRow::Retry,
                ]
        );
    }
}
