//! `Consumer::poll` issues one `Fetch` that covers every assigned partition,
//! advances next-offsets, and returns the decoded records.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use bytes::BufMut;
use krabka_ids::LeaderEpoch;
use krabka_protocol::{
    Encode, ProtocolError, ProtocolRequest,
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic, ForgottenTopic},
        fetch_response::FetchResponse,
        list_offsets_request::{self, ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        list_offsets_response::ListOffsetsResponse,
    },
    records::{Record, RecordBatch},
};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    mebibytes,
};

use crate::{
    builder::{AutoOffsetReset, IsolationLevel},
    consumer::{Consumer, ConsumerRecord, Header, TimestampType},
    error::ConsumerError,
    fetch_buffer::BufferedPartition,
    fetch_session::{
        FINAL_EPOCH, FetchSession, INVALID_SESSION_ID, SessionPartition, SessionRequest,
    },
    position::PartitionPosition,
};

/// Synthetic leader id that means "leader unknown → use the bootstrap
/// connection".
///
/// It matches `BrokerPool`'s bootstrap slot, so a fallback Fetch goes out
/// through `Client::send` and not `Client::broker(id)`.
const BOOTSTRAP_LEADER: i32 = -1;
const UNKNOWN_FETCH_OFFSET: i64 = -1;
const UNKNOWN_LEADER_ID: i32 = -1;
/// `OFFSET_OUT_OF_RANGE`.
const OFFSET_OUT_OF_RANGE: i16 = 1;
/// The offset the wire uses for "the broker did not answer this".
const UNKNOWN_OFFSET: i64 = -1;
pub(crate) const DEFAULT_FETCH_PARTITION_MAX: ByteSize = mebibytes(1);
pub(crate) const DEFAULT_FETCH_MAX: ByteSize = mebibytes(50);

/// One fetchable partition's request fields:
/// `(partition, fetch_offset, current_leader_epoch, last_fetched_epoch)`.
type FetchSpec = (i32, i64, LeaderEpoch, LeaderEpoch);

/// Partitions to fetch, grouped first by leader id, then by topic.
type FetchByLeader = HashMap<i32, HashMap<String, Vec<FetchSpec>>>;

fn fetch_leader_id(leader_id: i32, knows_leader: bool) -> i32 {
    if leader_id >= 0 && knows_leader {
        leader_id
    } else {
        BOOTSTRAP_LEADER
    }
}

fn should_use_bootstrap_leader(leader: i32) -> bool {
    leader == BOOTSTRAP_LEADER
}

fn fetch_offset_or_unknown(offsets: &HashMap<(String, i32), i64>, key: &(String, i32)) -> i64 {
    offsets.get(key).copied().unwrap_or(UNKNOWN_FETCH_OFFSET)
}

fn is_read_committed(isolation_level: IsolationLevel) -> bool {
    isolation_level == IsolationLevel::ReadCommitted
}

pub(crate) fn is_transient_transport_error(e: &krabka_client_core::ClientError) -> bool {
    matches!(
        e,
        krabka_client_core::ClientError::Connect { .. }
            | krabka_client_core::ClientError::Tls { .. }
            | krabka_client_core::ClientError::Sasl { .. }
            | krabka_client_core::ClientError::Disconnected
            | krabka_client_core::ClientError::Timeout(_)
            | krabka_client_core::ClientError::Io(_)
    )
}

fn is_transient_poll_error(e: &ConsumerError) -> bool {
    matches!(e, ConsumerError::Client(client) if is_transient_transport_error(client))
}

/// What [`Consumer::poll`] does with one partition row of a `Fetch` response,
/// chosen by the row's `error_code`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FetchPartitionAction {
    /// `NONE`: decode the records.
    Records,
    /// `OFFSET_OUT_OF_RANGE`: resolve a new position with the reset policy.
    ResetOffset,
    /// `NOT_LEADER_OR_FOLLOWER`: adopt the leader hint, or refresh metadata.
    Reroute,
    /// `FENCED_LEADER_EPOCH` or `UNKNOWN_LEADER_EPOCH`: validate the position
    /// against fresher metadata.
    RevalidateEpoch,
    /// `UNKNOWN_TOPIC_OR_PARTITION` or `UNKNOWN_TOPIC_ID`: skip the row and
    /// refresh metadata. The next poll fetches the partition again.
    RefreshMetadata,
    /// Any other code: fail the poll with `ConsumerError::Server(code)`.
    Fail(i16),
}

/// Map a `Fetch` partition `error_code` to its [`FetchPartitionAction`].
///
/// Kafka's `FetchCollector.handleInitializeErrors` also requests a metadata
/// update for `REPLICA_NOT_AVAILABLE` (9), `KAFKA_STORAGE_ERROR` (56),
/// `OFFSET_NOT_AVAILABLE` (78) and `INCONSISTENT_TOPIC_ID` (103), the errors of
/// a follower that serves a fetch (KIP-392).
///
/// Kafka's `FetchCollector.handleInitializeErrors` requests a metadata update
/// for `UNKNOWN_TOPIC_OR_PARTITION` (3) and for `UNKNOWN_TOPIC_ID` (100), and
/// does not raise an error to the application. A Fetch v13 or later names the
/// topic by id only. The broker answers 100 when its metadata image does not
/// hold that id yet, or when the topic was deleted and created again.
fn classify_fetch_partition_error(error_code: i16) -> FetchPartitionAction {
    match error_code {
        0 => FetchPartitionAction::Records,
        1 /* OFFSET_OUT_OF_RANGE */ => FetchPartitionAction::ResetOffset,
        6 /* NOT_LEADER_OR_FOLLOWER */ => FetchPartitionAction::Reroute,
        74 /* FENCED_LEADER_EPOCH */ | 75 /* UNKNOWN_LEADER_EPOCH */ => {
            FetchPartitionAction::RevalidateEpoch
        }
        3 /* UNKNOWN_TOPIC_OR_PARTITION */
        | 9 /* REPLICA_NOT_AVAILABLE */
        | 56 /* KAFKA_STORAGE_ERROR */
        | 78 /* OFFSET_NOT_AVAILABLE */
        | 100 /* UNKNOWN_TOPIC_ID */
        | 103 /* INCONSISTENT_TOPIC_ID */ => FetchPartitionAction::RefreshMetadata,
        other => FetchPartitionAction::Fail(other),
    }
}

fn aborted_txn_started(first_offset: i64, batch_base_offset: i64) -> bool {
    first_offset <= batch_base_offset
}

fn should_drop_aborted_batch(
    read_committed: bool,
    is_transactional: bool,
    producer_is_aborted: bool,
) -> bool {
    read_committed && is_transactional && producer_is_aborted
}

fn record_offset(base_offset: i64, offset_delta: i32) -> i64 {
    base_offset + i64::from(offset_delta)
}

/// The timestamp and timestamp type of `record` in `batch`.
///
/// Kafka's `DefaultRecordBatch` reads `max_timestamp` as the log append time
/// when the batch attributes say `LogAppendTime`. `DefaultRecord.readFrom`
/// then gives that time to every record and ignores the record delta.
pub(crate) fn record_timestamp(batch: &RecordBatch, record: &Record) -> (i64, TimestampType) {
    let timestamp_type = TimestampType::from(batch.attributes.timestamp_type());
    let timestamp = match timestamp_type {
        TimestampType::CreateTime => batch.base_timestamp + record.timestamp_delta,
        TimestampType::LogAppendTime => batch.max_timestamp,
    };
    (timestamp, timestamp_type)
}

/// The `max_bytes` of one partition in a Fetch request.
fn session_partition(
    topic_id: krabka_protocol::primitives::uuid::Uuid,
    (_, fetch_offset, leader_epoch, last_fetched_epoch): FetchSpec,
    partition_max: ByteSize,
) -> SessionPartition {
    SessionPartition {
        topic_id,
        fetch_offset,
        // Unwrap the leader epochs to raw wire `int32` at the FetchRequest
        // encode boundary.
        current_leader_epoch: leader_epoch.get(),
        last_fetched_epoch: last_fetched_epoch.get(),
        partition_max_bytes: partition_max.bytes_i32(),
    }
}

/// Build the Fetch request of one broker from its session request.
///
/// Kafka's `AbstractFetch.createFetchRequest`: `max_wait_ms` is
/// `fetch.max.wait.ms`, and the session id, epoch, partitions and forgotten
/// partitions come from the fetch session (KIP-227).
fn build_fetch_request(
    max_wait_ms: i32,
    isolation_level: IsolationLevel,
    min: ByteSize,
    max: ByteSize,
    rack_id: &str,
    session: SessionRequest,
) -> FetchRequest {
    let mut topics: Vec<FetchTopic> = Vec::new();
    for ((topic, partition), data) in session.partitions {
        if topics.last().is_none_or(|last| last.topic != topic) {
            topics.push(FetchTopic {
                topic: topic.clone(),
                topic_id: data.topic_id,
                ..Default::default()
            });
        }
        if let Some(last) = topics.last_mut() {
            last.partitions.push(FetchPartition {
                partition,
                fetch_offset: data.fetch_offset,
                current_leader_epoch: data.current_leader_epoch,
                last_fetched_epoch: data.last_fetched_epoch,
                partition_max_bytes: data.partition_max_bytes,
                ..Default::default()
            });
        }
    }
    let mut forgotten_topics_data: Vec<ForgottenTopic> = Vec::new();
    for ((topic, partition), topic_id) in session.forgotten {
        if forgotten_topics_data
            .last()
            .is_none_or(|last| last.topic != topic)
        {
            forgotten_topics_data.push(ForgottenTopic {
                topic: topic.clone(),
                topic_id,
                ..Default::default()
            });
        }
        if let Some(last) = forgotten_topics_data.last_mut() {
            last.partitions.push(partition);
        }
    }
    FetchRequest {
        max_wait_ms,
        min_bytes: min.bytes_i32(),
        max_bytes: max.bytes_i32(),
        isolation_level: isolation_level.wire(),
        session_id: session.session_id,
        session_epoch: session.session_epoch,
        topics,
        forgotten_topics_data,
        // KIP-392: `AbstractFetch.createFetchRequest` sends `client.rack`.
        rack_id: rack_id.to_owned(),
        ..Default::default()
    }
}

/// A Fetch that runs in a task, with the fetch offset of each partition that
/// it asked for.
struct InFlightFetch {
    handle: tokio::task::JoinHandle<()>,
    /// The result of the request. The task sends it before it notifies
    /// `Fetches::completed`, so a waiter that wakes can always take it.
    result: tokio::sync::oneshot::Receiver<Result<FetchResponse, krabka_client_core::ClientError>>,
    requested: HashMap<(String, i32), i64>,
    /// The partitions that this Fetch asked from a preferred read replica.
    from_replica: std::collections::HashSet<(String, i32)>,
}

/// A Fetch whose task ended: its broker, the fetch offsets it asked for, and
/// its result, or `None` when the task ended without a result.
struct CompletedFetch {
    leader: i32,
    requested: HashMap<(String, i32), i64>,
    from_replica: std::collections::HashSet<(String, i32)>,
    result: Option<Result<FetchResponse, krabka_client_core::ClientError>>,
}

/// The Fetch requests of a consumer: at most one in flight per broker, and the
/// fetch session of each broker.
///
/// Kafka's `AbstractFetch` sends a Fetch to every broker that has no pending
/// Fetch, and keeps a `FetchSessionHandler` per broker. A response that arrives
/// after `poll` returned waits here for the next `poll`.
#[derive(Default)]
pub(crate) struct Fetches {
    sessions: HashMap<i32, FetchSession>,
    in_flight: HashMap<i32, InFlightFetch>,
    /// The Fetch requests whose result the consumer took from `in_flight` and
    /// did not process yet.
    ready: Vec<CompletedFetch>,
    completed: Arc<tokio::sync::Notify>,
    /// KIP-392: the replica that a broker named in `preferred_read_replica`,
    /// with the time until which the consumer fetches from it. Kafka's
    /// `SubscriptionState.TopicPartitionState.preferredReadReplica`.
    preferred_read_replicas: HashMap<(String, i32), (i32, tokio::time::Instant)>,
}

impl Fetches {
    /// Move each Fetch whose task sent its result, or ended, to `ready`.
    fn collect_ready(&mut self) {
        let leaders: Vec<i32> = self.in_flight.keys().copied().collect();
        for leader in leaders {
            let Some(fetch) = self.in_flight.get_mut(&leader) else {
                continue;
            };
            let result = match fetch.result.try_recv() {
                Ok(result) => Some(result),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => None,
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => continue,
            };
            if let Some(fetch) = self.in_flight.remove(&leader) {
                self.ready.push(CompletedFetch {
                    leader,
                    requested: fetch.requested,
                    from_replica: fetch.from_replica,
                    result,
                });
            }
        }
    }
}

impl Drop for Fetches {
    fn drop(&mut self) {
        for fetch in self.in_flight.values() {
            fetch.handle.abort();
        }
    }
}

/// `ListOffsets` timestamp that asks for the log end offset.
pub(crate) const LATEST_TIMESTAMP: i64 = -1;
/// `ListOffsets` timestamp that asks for the log start offset.
pub(crate) const EARLIEST_TIMESTAMP: i64 = -2;

/// Placeholder for "reset with `ListOffsets`", resolved before the next Fetch:
/// the log end, or the offset for the timestamp of `by_duration`.
const LATEST_SENTINEL: i64 = i64::MAX;

/// Placeholder for the reset of [`Consumer::seek_to_beginning`]: Kafka's
/// `requestOffsetReset(partitions, EARLIEST)`, resolved with `ListOffsets(-2)`.
pub(crate) const BEGINNING_SENTINEL: i64 = i64::MAX - 1;

/// Placeholder for the reset of [`Consumer::seek_to_end`]: Kafka's
/// `requestOffsetReset(partitions, LATEST)`, resolved with `ListOffsets(-1)`.
pub(crate) const END_SENTINEL: i64 = i64::MAX - 2;

/// Placeholder for the position of a manually assigned partition: the
/// committed offset, or a reset when the group has none. Kafka's
/// `TopicPartitionState.shouldInitialize`.
pub(crate) const COMMITTED_SENTINEL: i64 = i64::MAX - 3;

/// Whether `next_offset` is a placeholder that a position update did not
/// resolve yet: a reset (Kafka's `TopicPartitionState.awaitingReset`) or the
/// committed offset of a new manual assignment. Such a partition has no valid
/// position, so the consumer does not fetch or commit it.
pub(crate) fn is_reset_sentinel(next_offset: i64) -> bool {
    next_offset >= COMMITTED_SENTINEL
}

/// The `ListOffsets` timestamp that resolves a [`LATEST_SENTINEL`] for the
/// reset policy at `now_ms`.
///
/// Kafka's `AutoOffsetResetStrategy.timestamp` gives `-1` for `latest` and
/// `now - duration` for `by_duration`.
fn reset_timestamp(policy: AutoOffsetReset, now_ms: i64) -> i64 {
    match policy {
        AutoOffsetReset::ByDuration(duration) => {
            now_ms.saturating_sub(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        }
        AutoOffsetReset::Earliest | AutoOffsetReset::Latest | AutoOffsetReset::None => {
            LATEST_TIMESTAMP
        }
    }
}

/// Run `future`, or return [`ConsumerError::Wakeup`] when `wakeup` comes
/// first.
async fn until_woken<F: std::future::Future>(
    wakeup: Option<&crate::control::WakeupHandle>,
    future: F,
) -> Result<F::Output, ConsumerError> {
    match wakeup {
        None => Ok(future.await),
        Some(wakeup) => tokio::select! {
            biased;
            () = wakeup.woken() => Err(ConsumerError::Wakeup),
            output = future => Ok(output),
        },
    }
}

/// Milliseconds since the Unix epoch.
fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

/// `ListOffsets` partitions for one broker, by topic:
/// `(partition, current_leader_epoch)`.
type ListOffsetsSpecs = BTreeMap<String, Vec<(i32, LeaderEpoch)>>;

/// Build the consumer's `ListOffsets` request for one broker.
///
/// Kafka's `ListOffsetsRequest.Builder.forConsumer` sends the consumer's own
/// isolation level, so a `read_committed` consumer that asks for the latest
/// offset gets the last stable offset and not the high watermark. The broker
/// reads `isolation_level` from version 2, so a `read_committed` consumer
/// sends [`ReadCommittedListOffsets`], which negotiates version 2 or higher.
/// `current_leader_epoch` (KIP-320, version 4) comes from metadata and is `-1`
/// when the epoch is unknown, as in `OffsetFetcher.groupListOffsetRequests`.
fn build_offsets_request(
    by_topic: ListOffsetsSpecs,
    timestamp: i64,
    isolation_level: IsolationLevel,
) -> ListOffsetsRequest {
    let topics: Vec<ListOffsetsTopic> = by_topic
        .into_iter()
        .map(|(name, partitions)| ListOffsetsTopic {
            name,
            partitions: partitions
                .into_iter()
                .map(|(partition_index, leader_epoch)| ListOffsetsPartition {
                    partition_index,
                    // Unwrap the leader epoch to the raw wire `int32`.
                    current_leader_epoch: leader_epoch.get(),
                    timestamp,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    ListOffsetsRequest {
        replica_id: -1,
        isolation_level: isolation_level.wire(),
        topics,
        ..Default::default()
    }
}

/// A `ListOffsets` request from a `read_committed` consumer, from version 2.
///
/// `isolation_level` exists from version 2. At version 0 or 1 the codec
/// leaves it out, and the broker answers with `read_uncommitted` semantics:
/// the high watermark and not the last stable offset. Kafka's
/// `ListOffsetsRequest.Builder.forConsumer` sets the oldest allowed version
/// to 2 for `READ_COMMITTED`. This type gives the same range to version
/// negotiation. When the broker supports only version 0 or 1, the send fails
/// with `ClientError::IncompatibleVersion` before the request goes out.
/// Kafka fails with `UnsupportedVersionException` in that case.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadCommittedListOffsets(pub(crate) ListOffsetsRequest);

impl Encode for ReadCommittedListOffsets {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for ReadCommittedListOffsets {
    const API_KEY: i16 = list_offsets_request::API_KEY;
    /// The first `ListOffsets` version that carries `isolation_level`.
    const MIN_VERSION: i16 = 2;
    const MAX_VERSION: i16 = list_offsets_request::MAX_VERSION;
    const LATEST_STABLE_VERSION: i16 = list_offsets_request::LATEST_STABLE_VERSION;
    const FLEXIBLE_MIN: i16 = list_offsets_request::FLEXIBLE_MIN;
    type Response = ListOffsetsResponse;
}

/// Group `ListOffsets` partitions by the broker that gets the request.
///
/// A partition goes to its leader when the leader id is known and the pool
/// can dial it, and to the bootstrap connection otherwise. That is the rule
/// that `Consumer::group_fetches` uses for Fetch. Each partition carries the
/// leader epoch from its position, which is `-1` when metadata has not
/// reported one.
fn group_list_offsets(
    keys: &[(String, i32)],
    positions: &HashMap<(String, i32), PartitionPosition>,
    knows_broker: impl Fn(i32) -> bool,
) -> BTreeMap<i32, ListOffsetsSpecs> {
    let mut grouped: BTreeMap<i32, ListOffsetsSpecs> = BTreeMap::new();
    for key in keys {
        let position = positions.get(key).copied().unwrap_or_default();
        let leader = fetch_leader_id(position.leader_id, knows_broker(position.leader_id));
        grouped
            .entry(leader)
            .or_default()
            .entry(key.0.clone())
            .or_default()
            .push((key.1, position.leader_epoch));
    }
    grouped
}

/// What the consumer does with one partition row of a `ListOffsets` answer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListOffsetsRowAction {
    /// `NONE` with a known offset: set the position to the offset.
    Position(i64),
    /// `TOPIC_AUTHORIZATION_FAILED`: fail the poll.
    Unauthorized,
    /// Any other row: keep the position and try again after a metadata
    /// refresh.
    Retry,
}

/// Map a `ListOffsets` partition row to its [`ListOffsetsRowAction`].
///
/// Kafka's `OffsetFetcherUtils.handleListOffsetResponse` sets a position only
/// for `NONE` with an offset other than `-1`. It throws
/// `TopicAuthorizationException` for `TOPIC_AUTHORIZATION_FAILED`. It retries
/// every other code, which includes codes that it does not name.
fn classify_list_offsets_row(error_code: i16, offset: i64) -> ListOffsetsRowAction {
    match error_code {
        0 if offset != UNKNOWN_OFFSET => ListOffsetsRowAction::Position(offset),
        29 /* TOPIC_AUTHORIZATION_FAILED */ => ListOffsetsRowAction::Unauthorized,
        _ => ListOffsetsRowAction::Retry,
    }
}

/// What the consumer takes from the `ListOffsets` answers for a set of
/// partitions.
#[derive(Debug, Default, Eq, PartialEq)]
struct ListOffsetsResult {
    /// The offset for each requested partition that got one.
    offsets: HashMap<(String, i32), i64>,
    /// `true` when a requested partition got no offset and must be tried
    /// again after a metadata refresh.
    retry: bool,
    /// The topics that answered `TOPIC_AUTHORIZATION_FAILED`.
    unauthorized: BTreeSet<String>,
}

impl ListOffsetsResult {
    /// Collect the answers for `requested`.
    ///
    /// A requested partition that no answer names is retried. An answer with
    /// an unauthorized row gives no offsets, because Kafka's
    /// `handleListOffsetResponse` throws for the whole response.
    fn collect(requested: &[(String, i32)], answers: &[ListOffsetsResponse]) -> Self {
        let mut result = Self::default();
        for answer in answers {
            let mut offsets = HashMap::new();
            let mut unauthorized = BTreeSet::new();
            for topic in &answer.topics {
                for row in &topic.partitions {
                    match classify_list_offsets_row(row.error_code, row.offset) {
                        ListOffsetsRowAction::Position(offset) => {
                            offsets.insert((topic.name.clone(), row.partition_index), offset);
                        }
                        ListOffsetsRowAction::Unauthorized => {
                            unauthorized.insert(topic.name.clone());
                        }
                        ListOffsetsRowAction::Retry => {}
                    }
                }
            }
            if unauthorized.is_empty() {
                result.offsets.extend(offsets);
            } else {
                result.unauthorized.extend(unauthorized);
            }
        }
        result.offsets.retain(|key, _| requested.contains(key));
        result.retry = requested
            .iter()
            .any(|key| !result.offsets.contains_key(key));
        result
    }

    /// `Err(TopicAuthorizationFailed)` when a topic was not authorized.
    fn authorization(&self) -> Result<(), ConsumerError> {
        if self.unauthorized.is_empty() {
            Ok(())
        } else {
            Err(ConsumerError::TopicAuthorizationFailed(
                self.unauthorized.clone(),
            ))
        }
    }
}

impl Consumer {
    /// Returns the records from every v2 batch that the broker returned per
    /// assigned partition, or an empty vec on timeout.
    ///
    /// Under `read_committed` isolation, this method filters out control
    /// batches and records that belong to aborted transactions on the client
    /// side, with the response's `aborted_transactions` list. The broker returns
    /// verbatim bytes.
    ///
    /// `poll` returns at most `max_poll_records` records. It keeps the rest of
    /// a fetch for the next call, which then sends no Fetch.
    ///
    /// A rebalance that the coordinator asks for starts only when the
    /// application calls `poll`. The internal coordinator task then runs the
    /// join and mutates the live `assigned` snapshot in place. For the eager
    /// protocol, `poll` waits for that join up to `timeout` and fetches nothing
    /// before it completes. `poll` also resets the `max_poll_interval` timer.
    #[tracing::instrument(
        name = "consumer.poll",
        level = "debug",
        skip_all,
        fields(
            group_id = %self.group_id,
            timeout_ms = timeout.millis_i64_trunc(),
            assigned_partitions = tracing::field::Empty,
            leaders = tracing::field::Empty,
            records = tracing::field::Empty,
        ),
        err
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn poll(&mut self, timeout: Time) -> Result<Vec<ConsumerRecord>, ConsumerError> {
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(
                u64::try_from(timeout.millis_i64_trunc()).unwrap_or(0),
            );
        // Kafka's `ClassicKafkaConsumer.poll` throws `IllegalStateException`
        // without a subscription.
        if self.subscription.borrow().is_none() && !self.subscription.borrow().manual_assignment {
            return Err(ConsumerError::NotSubscribed);
        }
        // Kafka's `ConsumerNetworkClient.maybeTriggerWakeup`: a pending
        // wakeup fails the `poll` before it blocks.
        if self.wakeup.take() {
            return Err(ConsumerError::Wakeup);
        }
        if !self.prepare_poll(deadline).await? {
            return Ok(Vec::new());
        }
        if !self.wait_for_rebalance(deadline).await? {
            return Ok(Vec::new());
        }
        let wakeup = self.wakeup.clone();

        // Records of an earlier fetch come first, without a new Fetch.
        let buffered = self.drain_fetch_buffer().await;
        if !buffered.is_empty() {
            tracing::Span::current().record("records", buffered.len());
            return Ok(buffered);
        }

        // 2. Build a FetchRequest covering every assigned partition.
        let assigned = self.assigned.lock().await.clone();
        tracing::Span::current().record("assigned_partitions", assigned.len());
        if assigned.is_empty() {
            tokio::select! {
                () = tokio::time::sleep_until(deadline) => return Ok(Vec::new()),
                () = wakeup.woken() => return Err(ConsumerError::Wakeup),
            }
        }

        let by_leader = self.group_fetches(&assigned).await;
        tracing::Span::current().record("leaders", by_leader.len());
        let topic_ids = self.topic_ids.lock().await.clone();
        self.send_fetches(by_leader, &topic_ids);
        self.fetches.collect_ready();
        if self.fetches.in_flight.is_empty() && self.fetches.ready.is_empty() {
            tokio::select! {
                () = self.wait_without_fetches(&assigned, deadline) => return Ok(Vec::new()),
                () = wakeup.woken() => return Err(ConsumerError::Wakeup),
            }
        }
        tokio::select! {
            biased;
            () = self.wait_for_fetches(deadline) => {}
            // The Fetch requests stay in flight. The next `poll` takes their
            // results.
            () = wakeup.woken() => return Err(ConsumerError::Wakeup),
        }
        let responses = self.take_completed_fetches(&topic_ids).await?;

        self.process_fetch_responses(responses, &topic_ids).await?;
        let records = self.drain_fetch_buffer().await;
        tracing::Span::current().record("records", records.len());
        Ok(records)
    }

    /// Wait until the join that the coordinator task runs for this `poll`
    /// completes, or until `deadline`. Return `false` when the join did not
    /// complete in time.
    ///
    /// For the eager protocol, Kafka's `ConsumerCoordinator.onJoinPrepare`
    /// revokes every partition before the `JoinGroup` inside `poll`, so that
    /// `poll` fetches nothing until the join completes. The cooperative
    /// protocol keeps the owned partitions, and `poll` fetches them while the
    /// join runs. A member without partitions has nothing to fetch either.
    ///
    /// The rebalance listener calls that the join asks for run here, inside
    /// `poll`.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::RebalanceListenerFailed`] when a listener
    /// callback failed.
    pub(crate) async fn wait_for_rebalance(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<bool, ConsumerError> {
        // Kafka's `onJoinComplete` keeps the first callback error and throws
        // it after the rebalance step. A failed callback therefore does not
        // end this `poll` while the join that queued it still runs.
        let mut first_error = self.run_pending_listener_calls().await.err();
        if !*self.rebalance_pending.borrow() {
            return first_error.map_or(Ok(true), Err);
        }
        let eager = self.rebalance_protocol == crate::assignor::RebalanceProtocol::Eager;
        if first_error.is_none() && !eager && !self.assigned.lock().await.is_empty() {
            return Ok(true);
        }
        // The coordinator task can request the rejoin after the start of this
        // `poll` signalled it. Signal again, so that this `poll` starts the
        // join that it waits for, as Kafka's `ensureActiveGroup` does.
        crate::coordinator::note_poll(&self.poll_signal);
        let wakeup = self.wakeup.clone();
        let joined = loop {
            // A slow callback must not keep this `poll` past its timeout. The
            // next `poll` runs the calls that come later.
            if tokio::time::Instant::now() >= deadline {
                break false;
            }
            let call = tokio::select! {
                biased;
                call = self.listener_calls.recv() => call,
                joined = self.rebalance_pending.wait_for(|pending| !*pending) => {
                    // A closed channel means that the coordinator task
                    // stopped. `poll` then continues with the assignment that
                    // it has.
                    let _ = joined;
                    break true;
                }
                () = tokio::time::sleep_until(deadline) => break false,
                // A wakeup ends the wait between two callbacks, never inside
                // one.
                () = wakeup.woken() => {
                    first_error.get_or_insert(ConsumerError::Wakeup);
                    break false;
                }
            };
            match call {
                Some(call) => {
                    if let Err(error) = self.complete_listener_call(call).await {
                        first_error.get_or_insert(error);
                    }
                }
                // No listener calls can come: wait for the join alone.
                None => {
                    break tokio::select! {
                        biased;
                        joined = self.rebalance_pending.wait_for(|pending| !*pending) => {
                            let _ = joined;
                            true
                        }
                        () = tokio::time::sleep_until(deadline) => false,
                        () = wakeup.woken() => {
                            first_error.get_or_insert(ConsumerError::Wakeup);
                            false
                        }
                    };
                }
            }
        };
        // The join can end with calls that are still waiting, for example the
        // assign callback.
        if let Err(error) = self.run_pending_listener_calls().await {
            first_error.get_or_insert(error);
        }
        first_error.map_or(Ok(joined), Err)
    }

    /// Return up to `max_poll_records` buffered records, and move the consumed
    /// positions past them.
    ///
    /// The `assigned` guard lives until the positions move, so the coordinator
    /// cannot revoke a partition between the ownership check and the drain.
    /// The lock order is `assigned`, then `next_offsets`, then `positions`, as
    /// in the coordinator task.
    async fn drain_fetch_buffer(&mut self) -> Vec<ConsumerRecord> {
        let ownership_ids = self.commit_identity.lock().await.ownership_ids.clone();
        let paused = self.paused_of(&ownership_ids);
        let assigned_guard = self.assigned.lock().await;
        let assigned: std::collections::HashSet<(String, i32)> =
            assigned_guard.iter().cloned().collect();
        let mut offsets = self.next_offsets.lock().await;
        let mut positions = self.positions.lock().await;
        let records = self.fetch_buffer.drain(
            self.max_poll_records,
            &assigned,
            &paused,
            &mut offsets,
            &mut positions,
        );
        drop(positions);
        drop(offsets);
        drop(assigned_guard);
        records
    }
    /// Wait when this `poll` sends no Fetch and none is in flight, for example
    /// when every partition is paused.
    ///
    /// Kafka's `ClassicKafkaConsumer.pollForFetches` waits for the poll timeout,
    /// or for the retry backoff while a partition has no valid position. A
    /// listener call that waits also ends the wait after the retry backoff, so
    /// the next `poll` runs it.
    async fn wait_without_fetches(
        &self,
        assigned: &[(String, i32)],
        deadline: tokio::time::Instant,
    ) {
        let all_positions = {
            let offsets = self.next_offsets.lock().await;
            let positions = self.positions.lock().await;
            assigned.iter().all(|key| {
                offsets
                    .get(key)
                    .is_some_and(|offset| !is_reset_sentinel(*offset))
                    && !positions
                        .get(key)
                        .is_some_and(|position| position.awaiting_validation)
            })
        };
        let until = if all_positions && self.listener_calls.is_empty() {
            deadline
        } else {
            deadline.min(tokio::time::Instant::now() + self.retry_policy.initial_backoff)
        };
        // A rebalance that starts during the wait queues listener calls that
        // this consumer must run, so it ends the wait, as Kafka's
        // `pollForFetches` waits at most `coordinator.timeToNextPoll`.
        let mut rebalance_pending = self.rebalance_pending.clone();
        rebalance_pending.borrow_and_update();
        let rebalance = async {
            // Without a coordinator task nothing changes the flag.
            if rebalance_pending.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            () = tokio::time::sleep_until(until) => {}
            () = rebalance => {}
        }
    }

    /// Decode the fetch responses into the fetch buffer, and act on the
    /// partition errors.
    async fn process_fetch_responses(
        &mut self,
        responses: Vec<krabka_protocol::owned::fetch_response::FetchResponse>,
        topic_ids: &HashMap<String, krabka_protocol::primitives::uuid::Uuid>,
    ) -> Result<(), ConsumerError> {
        // 3. Decode each partition's RecordBatches, advance next-offsets.
        //
        // The wire-level `records` field can carry multiple concatenated
        // RecordBatches; we iterate every v2 batch, emit one ConsumerRecord
        // per Record, and bump next_offsets to the highest seen offset + 1.
        // Reverse-map topic_id → name. At Fetch v ≥ 13 the response carries
        // only `topic_id`; `topic.topic` is empty.
        let id_to_name = crate::offset_wire::id_to_name(topic_ids);

        // Re-snapshot the assignment: a cooperative rebalance may have
        // revoked partitions while this Fetch was in flight. Records for
        // partitions we no longer own must be dropped — the new owner will
        // serve them from the offset we committed at revoke time. Snapshot
        // before locking `next_offsets` to keep the coordinator's
        // assigned→next_offsets lock order (avoids deadlock).
        let still_owned: std::collections::HashSet<(String, i32)> =
            self.assigned.lock().await.iter().cloned().collect();

        let mut fetched: Vec<BufferedPartition> = Vec::new();
        let mut refresh_after_processing = false;
        // The partitions that answered `OFFSET_OUT_OF_RANGE` under `Earliest`
        // or `None`, with the offset each was fetched from. Both policies need
        // the broker's real log start, and reading that is an RPC, so the loop
        // records them and the code below the guard resolves them.
        let mut out_of_range: Vec<((String, i32), i64)> = Vec::new();
        // The fetch positions that this loop resets, for the auto commit
        // before a `JoinGroup`.
        let mut polled_resets: Vec<((String, i32), Option<i64>)> = Vec::new();
        // KIP-392 preferred read replica changes: `Some(replica)` to fetch from
        // it, `None` to go back to the leader.
        let mut replica_updates: Vec<((String, i32), Option<i32>)> = Vec::new();
        let mut offsets = self.next_offsets.lock().await;
        for topic in responses.iter().flat_map(|resp| &resp.responses) {
            let topic_name = if topic.topic.is_empty() {
                id_to_name.get(&topic.topic_id).cloned().unwrap_or_default()
            } else {
                topic.topic.clone()
            };
            for part in &topic.partitions {
                // Drop records for partitions revoked while this Fetch was
                // in flight (cooperative rebalance transparency).
                if !still_owned.contains(&(topic_name.clone(), part.partition_index)) {
                    continue;
                }

                let key = (topic_name.clone(), part.partition_index);

                // KIP-320 in-band truncation: leader served no records and told
                // us where to truncate (diverging_epoch.end_offset >= 0).
                if part.diverging_epoch.end_offset >= 0 {
                    self.handle_truncation_in_poll(
                        &mut offsets,
                        &key,
                        part.diverging_epoch.end_offset,
                    )?;
                    polled_resets.push((key.clone(), Some(part.diverging_epoch.end_offset)));
                    continue;
                }
                // Error-first: inspect the partition error_code before decoding.
                let action = classify_fetch_partition_error(part.error_code);
                // Kafka's `FetchCollector.handleInitializeErrors` clears the
                // preferred read replica with each metadata update request.
                if matches!(
                    action,
                    FetchPartitionAction::Reroute
                        | FetchPartitionAction::RevalidateEpoch
                        | FetchPartitionAction::RefreshMetadata
                ) {
                    replica_updates.push((key.clone(), None));
                }
                match action {
                    FetchPartitionAction::Records => {
                        // `FetchCollector.updatePartitionState`.
                        if part.preferred_read_replica >= 0 {
                            replica_updates.push((key.clone(), Some(part.preferred_read_replica)));
                        }
                    }
                    FetchPartitionAction::ResetOffset => {
                        // The response cannot say where the log now starts. Apache
                        // Kafka builds an errored partition with `log_start_offset`,
                        // `high_watermark` and `last_stable_offset` all set to -1, so
                        // `part.log_start_offset` is -1 here whatever the real log
                        // start is. Reading it and fetching from it wedges the
                        // partition: every following Fetch asks for -1 and gets
                        // OFFSET_OUT_OF_RANGE again. A hardcoded 0 wedges it the same
                        // way once retention has moved the log start past 0.
                        //
                        // So every policy resolves its position with a ListOffsets
                        // instead. Earliest and Latest plant a sentinel that
                        // `resolve_reset_sentinels` replaces before the next Fetch.
                        // None reports the error, and `deferred_out_of_range` carries
                        // it out of this loop so the true log start can be read once
                        // the offsets guard is released.
                        match self.auto_offset_reset {
                            AutoOffsetReset::Latest | AutoOffsetReset::ByDuration(_) => {
                                offsets.insert(key.clone(), LATEST_SENTINEL);
                                polled_resets.push((key.clone(), None));
                            }
                            AutoOffsetReset::Earliest | AutoOffsetReset::None => {
                                let fetch_offset = fetch_offset_or_unknown(&offsets, &key);
                                out_of_range.push((key.clone(), fetch_offset));
                            }
                        }
                        continue;
                    }
                    FetchPartitionAction::Reroute => {
                        // A routing miss, NOT a truncation: we sent the Fetch to
                        // a broker that no longer leads this partition (e.g. a
                        // leadership change since the last metadata refresh).
                        // Re-target the leader so the next poll routes correctly;
                        // do NOT set awaiting_validation (nothing diverged).
                        let mut positions = self.positions.lock().await;
                        if part.current_leader.leader_id >= 0 {
                            // The broker handed us the new leader inline (KIP-320
                            // current_leader hint). Adopt it immediately.
                            let p = positions.entry(key.clone()).or_default();
                            p.leader_id = part.current_leader.leader_id;
                            // Wrap the KIP-320 current-leader hint (raw wire
                            // `int32`) at the Fetch-response decode boundary.
                            p.leader_epoch = LeaderEpoch(part.current_leader.leader_epoch);
                        } else {
                            // No hint: force a metadata refresh after this loop
                            // so the next poll learns the new leader. Reset the
                            // stale leader id so the bootstrap fallback (and a
                            // re-flag, if metadata advances the epoch) kicks in.
                            if let Some(p) = positions.get_mut(&key) {
                                p.leader_id = UNKNOWN_LEADER_ID;
                            }
                            drop(positions);
                            refresh_after_processing = true;
                        }
                        continue;
                    }
                    FetchPartitionAction::RevalidateEpoch => {
                        let mut positions = self.positions.lock().await;
                        if let Some(p) = positions.get_mut(&key) {
                            // Force refresh_leader_epochs to re-flag against
                            // fresher metadata next poll (any real epoch >= 0 > -1).
                            p.leader_epoch = LeaderEpoch(UNKNOWN_LEADER_ID);
                            // Only gate on validation when we have a consumed epoch
                            // to validate against. A never-consumed partition
                            // (offset_epoch < 0) has nothing to validate; flagging it
                            // would wedge it — validate_positions skips offset_epoch
                            // < 0, and the fetch builder skips awaiting_validation.
                            if p.offset_epoch.is_known() {
                                p.awaiting_validation = true;
                            }
                        }
                        continue;
                    }
                    FetchPartitionAction::RefreshMetadata => {
                        // The broker does not know the topic or the topic id. Skip the
                        // row and refresh metadata, as Kafka's consumer does. The
                        // refresh at the start of the next poll also stores a new
                        // topic id for a topic that was created again.
                        tracing::warn!(
                            topic = %topic_name,
                            partition = part.partition_index,
                            error_code = part.error_code,
                            "fetch partition names a topic the broker does not know; refreshing metadata"
                        );
                        refresh_after_processing = true;
                        continue;
                    }
                    FetchPartitionAction::Fail(code) => {
                        return Err(ConsumerError::Server(code));
                    }
                }

                record_readable_end(
                    &mut *self.end_offsets.lock().await,
                    key.clone(),
                    self.isolation_level,
                    part.high_watermark,
                    part.last_stable_offset,
                );

                if let Some(partition) =
                    self.process_partition_records(&offsets, &key, &topic_name, part)
                {
                    fetched.push(partition);
                }
            }
        }
        if let Some(auto_commit) = &self.auto_commit {
            auto_commit.reset_polled(polled_resets).await;
        }
        // Drop the offsets guard before any `.await`: refreshing metadata is an
        // RPC, and we must never hold a Mutex guard across an await point.
        drop(offsets);
        let now = tokio::time::Instant::now();
        for (key, replica) in replica_updates {
            match replica {
                // `TopicPartitionState.updatePreferredReadReplica` restarts the
                // expiry only when the replica changes.
                Some(replica) => {
                    let expires = now + self.metadata_max_age.to_std();
                    let entry = self
                        .fetches
                        .preferred_read_replicas
                        .entry(key)
                        .or_insert((replica, expires));
                    if entry.0 != replica {
                        *entry = (replica, expires);
                    }
                }
                None => {
                    self.fetches.preferred_read_replicas.remove(&key);
                }
            }
        }
        for partition in fetched {
            self.fetch_buffer.push(partition);
        }
        if !out_of_range.is_empty() && self.recover_out_of_range(&out_of_range).await? {
            refresh_after_processing = true;
        }
        if refresh_after_processing {
            // Best-effort: a NOT_LEADER_OR_FOLLOWER without a current_leader
            // hint means our cached leader is stale; learn the new one so the
            // next poll routes correctly. UNKNOWN_TOPIC_OR_PARTITION and
            // UNKNOWN_TOPIC_ID mean our topic metadata is stale. A failure is
            // non-fatal — the next refresh_leader_epochs pass retries.
            let _ = self.client.refresh_metadata().await;
        }
        Ok(())
    }

    /// Send a Fetch to each leader in `by_leader` that has no Fetch in flight.
    /// Each request runs in its own task, so the leaders answer in parallel.
    fn send_fetches(
        &mut self,
        by_leader: FetchByLeader,
        topic_ids: &HashMap<String, krabka_protocol::primitives::uuid::Uuid>,
    ) {
        let max_wait_ms = crate::consumer::protocol_millis_i32(self.fetch_max_wait);
        for (leader, by_topic) in by_leader {
            if self.fetches.in_flight.contains_key(&leader) {
                continue;
            }
            let wanted: BTreeMap<(String, i32), SessionPartition> = by_topic
                .into_iter()
                .flat_map(|(topic, specs)| {
                    let topic_id = topic_ids.get(&topic).copied().unwrap_or_default();
                    let partition_max = self.fetch_partition_max;
                    specs.into_iter().map(move |spec| {
                        (
                            (topic.clone(), spec.0),
                            session_partition(topic_id, spec, partition_max),
                        )
                    })
                })
                .collect();
            let requested = wanted
                .iter()
                .map(|(key, partition)| (key.clone(), partition.fetch_offset))
                .collect();
            let from_replica = wanted
                .keys()
                .filter(|key| {
                    self.fetches
                        .preferred_read_replicas
                        .get(*key)
                        .is_some_and(|(replica, _)| *replica == leader)
                })
                .cloned()
                .collect();
            let session = self
                .fetches
                .sessions
                .entry(leader)
                .or_default()
                .build(wanted);
            let request = build_fetch_request(
                max_wait_ms,
                self.isolation_level,
                self.fetch_min,
                self.fetch_max,
                self.client_rack.as_deref().unwrap_or_default(),
                session,
            );
            let client = self.client.clone();
            let completed = Arc::clone(&self.fetches.completed);
            let (sender, result) = tokio::sync::oneshot::channel();
            let handle = tokio::spawn(async move {
                let response = if should_use_bootstrap_leader(leader) {
                    client.send(request).await
                } else {
                    client.broker(leader).send(request).await
                };
                let _ = sender.send(response);
                completed.notify_one();
            });
            self.fetches.in_flight.insert(
                leader,
                InFlightFetch {
                    handle,
                    result,
                    requested,
                    from_replica,
                },
            );
        }
    }

    /// Wait until a Fetch in flight completes, or until `deadline`. Kafka's
    /// `ClassicKafkaConsumer.pollForFetches` bounds the network wait with the
    /// poll timer and returns as soon as a fetch is available.
    async fn wait_for_fetches(&mut self, deadline: tokio::time::Instant) {
        loop {
            self.fetches.collect_ready();
            if self.fetches.in_flight.is_empty() || !self.fetches.ready.is_empty() {
                return;
            }
            tokio::select! {
                () = self.fetches.completed.notified() => {}
                () = tokio::time::sleep_until(deadline) => return,
            }
        }
    }

    /// Take the responses of the completed Fetch requests, and update the
    /// fetch session of each broker.
    ///
    /// The function drops the data of a partition whose fetch position changed
    /// after the request, as Kafka's `FetchCollector.initialize` discards a
    /// stale fetch. A response that the fetch session rejects gives no data, as
    /// in `AbstractFetch.handleFetchSuccess`.
    ///
    /// # Errors
    ///
    /// Returns a transport error that is not transient.
    async fn take_completed_fetches(
        &mut self,
        topic_ids: &HashMap<String, krabka_protocol::primitives::uuid::Uuid>,
    ) -> Result<Vec<FetchResponse>, ConsumerError> {
        self.fetches.collect_ready();
        if self.fetches.ready.is_empty() {
            return Ok(Vec::new());
        }
        let id_to_name = crate::offset_wire::id_to_name(topic_ids);
        let offsets = self.next_offsets.lock().await.clone();
        let mut responses = Vec::new();
        let mut failure = None;
        for fetch in std::mem::take(&mut self.fetches.ready) {
            let leader = fetch.leader;
            let session = self.fetches.sessions.entry(leader).or_default();
            match fetch.result {
                Some(Ok(mut response)) => {
                    let partitions = response
                        .responses
                        .iter()
                        .map(|topic| topic.partitions.len())
                        .sum();
                    if !session.handle_response(
                        response.error_code,
                        response.session_id,
                        partitions,
                        response.throttle_time_ms,
                    ) {
                        tracing::debug!(
                            leader,
                            error_code = response.error_code,
                            "fetch session answered with an error; the next fetch is a full fetch"
                        );
                        continue;
                    }
                    let mut replica_out_of_range = Vec::new();
                    for topic in &mut response.responses {
                        let name = if topic.topic.is_empty() {
                            id_to_name.get(&topic.topic_id).cloned().unwrap_or_default()
                        } else {
                            topic.topic.clone()
                        };
                        topic.partitions.retain(|partition| {
                            let key = (name.clone(), partition.partition_index);
                            // A preferred replica that answers out of range
                            // gives no reset: Kafka's `handleInitializeErrors`
                            // clears the replica and fetches from the leader.
                            // The request decides this, as the preference can
                            // change while the Fetch is in flight.
                            if partition.error_code == OFFSET_OUT_OF_RANGE
                                && fetch.from_replica.contains(&key)
                            {
                                replica_out_of_range.push(key);
                                return false;
                            }
                            fetch.requested.get(&key).is_some_and(|requested| {
                                offsets.get(&key).copied().unwrap_or(0) == *requested
                            })
                        });
                    }
                    for key in replica_out_of_range {
                        self.fetches.preferred_read_replicas.remove(&key);
                    }
                    responses.push(response);
                }
                Some(Err(error)) => {
                    session.handle_error();
                    // `AbstractFetch.handleFetchFailure` clears the preferred read
                    // replica of each partition of the failed session.
                    for key in fetch.requested.keys() {
                        self.fetches.preferred_read_replicas.remove(key);
                    }
                    if is_transient_transport_error(&error) {
                        if should_use_bootstrap_leader(leader) {
                            self.client.reconnect_bootstrap().await;
                        } else {
                            self.client.evict_broker(leader);
                        }
                    } else if failure.is_none() {
                        failure = Some(error);
                    }
                }
                // The task ended without a result: it panicked or was aborted.
                None => session.handle_error(),
            }
        }
        match failure {
            Some(error) => Err(error.into()),
            None => Ok(responses),
        }
    }

    /// Close the fetch session of each broker. Kafka's `AbstractFetch.close`
    /// sends a Fetch with the session id, epoch `-1` and no partitions to each
    /// broker that holds a session.
    pub(crate) async fn close_fetch_sessions(&mut self, timeout: std::time::Duration) {
        let max_wait_ms = crate::consumer::protocol_millis_i32(self.fetch_max_wait);
        let closes: Vec<_> = self
            .fetches
            .sessions
            .iter()
            .filter(|(_, session)| session.session_id() != INVALID_SESSION_ID)
            .map(|(leader, session)| {
                let request = build_fetch_request(
                    max_wait_ms,
                    self.isolation_level,
                    self.fetch_min,
                    self.fetch_max,
                    self.client_rack.as_deref().unwrap_or_default(),
                    SessionRequest {
                        session_id: session.session_id(),
                        session_epoch: FINAL_EPOCH,
                        partitions: BTreeMap::new(),
                        forgotten: Vec::new(),
                    },
                );
                let client = self.client.clone();
                let leader = *leader;
                async move {
                    let _ = if should_use_bootstrap_leader(leader) {
                        client.send(request).await
                    } else {
                        client.broker(leader).send(request).await
                    };
                }
            })
            .collect();
        let _ = tokio::time::timeout(timeout, futures_util::future::join_all(closes)).await;
    }

    async fn group_fetches(&mut self, assigned: &[(String, i32)]) -> FetchByLeader {
        let subscription = self.subscription.borrow().clone();
        let awaiting_callback = self
            .assigned_callback_pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let ownership_ids = self.commit_identity.lock().await.ownership_ids.clone();
        let paused = self.paused_of(&ownership_ids);
        let buffered = self.fetch_buffer.buffered_partitions();
        let mut grouped: FetchByLeader = HashMap::new();
        let now = tokio::time::Instant::now();
        let mut refresh_metadata = false;
        {
            let offsets = self.next_offsets.lock().await;
            let positions = self.positions.lock().await;
            for (t, p) in assigned {
                if awaiting_callback.contains(&(t.clone(), *p)) {
                    continue;
                }
                // Kafka's `SubscriptionState.isFetchableAndSubscribed`: with a
                // topic subscription, a partition of a topic that the
                // consumer no longer subscribes to is not fetched.
                if !subscription.manual_assignment
                    && subscription.pattern.is_none()
                    && !subscription.contains(t)
                {
                    continue;
                }
                // Skip partitions still awaiting validation — they must not be
                // fetched until proven consistent.
                if positions
                    .get(&(t.clone(), *p))
                    .is_some_and(|x| x.awaiting_validation)
                {
                    continue;
                }
                let next = offsets.get(&(t.clone(), *p)).copied().unwrap_or(0);
                // A reset sentinel is a reset that `ListOffsets` did not
                // resolve yet. Kafka does not fetch a partition that awaits a
                // reset, and the sentinel is no offset to fetch from.
                if is_reset_sentinel(next) {
                    continue;
                }
                // Kafka's `AbstractFetch.fetchablePartitions` leaves out a
                // paused partition and a partition with buffered records.
                if paused.contains(&(t.clone(), *p)) || buffered.contains(&(t.clone(), *p)) {
                    continue;
                }
                let pos = positions.get(&(t.clone(), *p)).copied().unwrap_or_default();
                // Route to the leader when its id is known AND the pool has a
                // dialable address for it; otherwise fall back to the bootstrap
                // connection. `knows_broker` is a synchronous registry lookup
                // (no await), so it's safe to call while the offsets/positions
                // guards are held. A leader whose advertised address is unusable
                // (e.g. port 0 from an in-process test broker) is treated as
                // unknown — the bootstrap broker is the leader in that
                // single-broker case anyway.
                let mut leader =
                    fetch_leader_id(pos.leader_id, self.client.knows_broker(pos.leader_id));
                // KIP-392: Kafka's `AbstractFetch.selectReadReplica` fetches from
                // the preferred read replica until it expires. A replica that is
                // not in the metadata clears the preference and refreshes it.
                let key = (t.clone(), *p);
                match self.fetches.preferred_read_replicas.get(&key).copied() {
                    Some((_, expires)) if now > expires => {
                        self.fetches.preferred_read_replicas.remove(&key);
                    }
                    Some((replica, _)) if self.client.knows_broker(replica) => leader = replica,
                    Some(_) => {
                        self.fetches.preferred_read_replicas.remove(&key);
                        refresh_metadata = true;
                    }
                    None => {}
                }
                // Kafka's `AbstractFetch.prepareFetchRequests` skips a broker
                // with a pending Fetch.
                if self.fetches.in_flight.contains_key(&leader) {
                    continue;
                }
                grouped
                    .entry(leader)
                    .or_default()
                    .entry(t.clone())
                    .or_default()
                    .push((*p, next, pos.leader_epoch, pos.offset_epoch));
            }
        }
        if refresh_metadata {
            // Best effort, as the other metadata refreshes of the fetch path.
            let _ = self.client.refresh_metadata().await;
        }

        grouped
    }

    async fn prepare_poll(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<bool, ConsumerError> {
        // Kafka's consumer raises a fatal `OffsetFetch` error from `poll()`.
        // A rejoin in the coordinator task leaves such an error here.
        if let Some(error) = crate::coordinator::take_poll_error(&self.poll_error) {
            return Err(error);
        }
        // Kafka's `ConsumerCoordinator.poll` sends the interval auto commit
        // before `updateFetchPositions`.
        self.maybe_auto_commit_async().await;
        // This `poll` resets the poll timer of the coordinator task, and lets
        // it start a rejoin that waits for a `poll`. The positions for the
        // commit before that join are already recorded above.
        crate::coordinator::note_poll(&self.poll_signal);
        // Metadata comes first. A partition that has no position yet (a new
        // assignment without a committed offset) gets its leader id and
        // epoch here, so its first `ListOffsets` goes to the leader and not
        // to the bootstrap broker. Kafka's `OffsetFetcher.groupListOffsetRequests`
        // routes with `metadata.currentLeader(tp)` in the same way.
        self.sync_metadata_topics();
        let wakeup = self.wakeup.clone();
        self.update_fetch_positions(Some(&wakeup), Some(deadline))
            .await
    }

    /// Name the subscribed topics in the metadata requests of this consumer.
    /// The coordinator task changes the topics of a pattern subscription on
    /// its own client, so `poll` learns the leaders of a topic that a pattern
    /// adds. Kafka's `ConsumerMetadata` reads the subscription of each update.
    fn sync_metadata_topics(&self) {
        let topics = {
            let subscription = self.subscription.borrow();
            (!subscription.is_none()).then(|| subscription.topics.clone())
        };
        if let Some(topics) = topics
            && self.client.metadata_topics().names() != topics
        {
            self.client.metadata_topics().set(topics);
        }
    }

    /// Refresh the leader epochs, resolve the offset resets and validate the
    /// positions. Kafka's `KafkaConsumer.updateFetchPositions`.
    ///
    /// Return `false` after a transient error, when the caller must try again
    /// later.
    ///
    /// With `wakeup`, a wakeup ends each request of the update, as Kafka's
    /// `ConsumerNetworkClient.poll` throws `WakeupException` while it waits.
    /// The truncation that the validation found is applied without a wakeup
    /// check.
    pub(crate) async fn update_fetch_positions(
        &self,
        wakeup: Option<&crate::control::WakeupHandle>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<bool, ConsumerError> {
        if let Err(error) = until_woken(wakeup, self.refresh_leader_epochs()).await? {
            if is_transient_poll_error(&error) {
                self.client.reconnect_bootstrap().await;
                return Ok(false);
            }
            return Err(error);
        }
        if let Err(error) = until_woken(wakeup, self.resolve_committed_sentinels(deadline)).await? {
            if is_transient_poll_error(&error) {
                self.client.reconnect_bootstrap().await;
                return Ok(false);
            }
            return Err(error);
        }
        if let Err(error) = until_woken(wakeup, self.resolve_reset_sentinels()).await? {
            if is_transient_poll_error(&error) {
                self.client.reconnect_bootstrap().await;
                return Ok(false);
            }
            return Err(error);
        }
        let truncated = match until_woken(wakeup, self.validate_positions()).await? {
            Ok(truncated) => truncated,
            Err(error) if is_transient_poll_error(&error) => {
                self.client.reconnect_bootstrap().await;
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        if !truncated.is_empty() {
            self.apply_truncation(&truncated).await?;
        }
        Ok(true)
    }

    /// Decode the records of one partition row into a [`BufferedPartition`].
    /// The consumed position does not move here. It moves when `poll` returns
    /// the records.
    fn process_partition_records(
        &self,
        offsets: &HashMap<(String, i32), i64>,
        key: &(String, i32),
        topic_name: &str,
        part: &krabka_protocol::owned::fetch_response::PartitionData,
    ) -> Option<BufferedPartition> {
        // Legacy MessageSet payloads are skipped here; the consumer
        // only handles v2 batches.
        let batches = part.records.as_ref()?.as_v2()?;
        // The broker returns whole record batches whose last offset is
        // >= the requested fetch_offset, even when the batch starts
        // before it (e.g. after an OFFSET_OUT_OF_RANGE reset or when
        // a single large batch straddles log_start). Kafka's JVM
        // client skips any records below the position; we do the same.
        // Capture the position now — before `next_offset_after` updates
        // it — so the filter baseline matches the actual fetch offset.
        let fetch_floor = offsets.get(key).copied().unwrap_or(0);
        let mut records = std::collections::VecDeque::new();
        // read_committed filtering happens entirely client-side: the
        // broker returns verbatim on-disk bytes (control batches,
        // aborted records and all) plus an `aborted_transactions`
        // list. We replay Kafka's algorithm — walk batches in offset
        // order, tracking which producer_ids have an open aborted
        // transaction, and drop transactional records from those.
        let read_committed = is_read_committed(self.isolation_level);
        // Aborted txns sorted by first_offset; consumed front-to-back
        // as batch offsets advance past each entry's start.
        let mut aborted: std::collections::VecDeque<(i64, i64)> = if read_committed {
            let mut v: Vec<(i64, i64)> = part
                .aborted_transactions
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|a| (a.first_offset, a.producer_id))
                .collect();
            v.sort_unstable();
            v.into()
        } else {
            std::collections::VecDeque::new()
        };
        // producer_ids with a currently-open aborted transaction.
        let mut aborted_pids: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for batch in batches {
            // Move every aborted txn that starts at or before this
            // batch into the active set.
            if read_committed {
                while let Some(&(first_offset, pid)) = aborted.front() {
                    if aborted_txn_started(first_offset, batch.base_offset) {
                        aborted_pids.insert(pid);
                        aborted.pop_front();
                    } else {
                        break;
                    }
                }
            }
            // Control batches (commit/abort markers) carry no user
            // records. A control batch for a producer ends its aborted
            // transaction; drop the batch either way.
            if batch.attributes.is_control_batch() {
                if read_committed {
                    aborted_pids.remove(&batch.producer_id);
                }
                continue;
            }
            // Drop transactional records belonging to an aborted txn.
            if should_drop_aborted_batch(
                read_committed,
                batch.attributes.is_transactional(),
                aborted_pids.contains(&batch.producer_id),
            ) {
                continue;
            }
            for r in &batch.records {
                let offset = record_offset(batch.base_offset, r.offset_delta);
                // Skip records that precede the fetch floor: the broker
                // returned a whole batch whose base_offset < our
                // position (straddle case — see fetch_floor comment).
                if offset < fetch_floor {
                    continue;
                }
                let (timestamp, timestamp_type) = record_timestamp(batch, r);
                records.push_back(ConsumerRecord {
                    topic: topic_name.to_string(),
                    partition: part.partition_index,
                    offset,
                    leader_epoch: batch.partition_leader_epoch,
                    timestamp,
                    timestamp_type,
                    key: r.key.clone(),
                    value: r.value.clone(),
                    headers: r
                        .headers
                        .iter()
                        .map(|h| Header {
                            key: h.key.clone(),
                            value: h.value.clone(),
                        })
                        .collect(),
                });
            }
        }
        // When `poll` returns the last record, the position moves past every
        // fetched batch, and the position's offset_epoch becomes the highest
        // batch leader epoch, so the next Fetch sends the correct
        // last_fetched_epoch (KIP-320).
        let next_offset = next_offset_after(batches)?;
        Some(BufferedPartition {
            key: key.clone(),
            position: fetch_floor,
            records,
            next_offset,
            // Wrap the batch's raw wire `partition_leader_epoch` (`int32`) at
            // the RecordBatch decode boundary.
            last_epoch: batches
                .iter()
                .map(|batch| batch.partition_leader_epoch)
                .max()
                .map(LeaderEpoch),
        })
    }
}

const fn readable_end_offset(
    isolation: IsolationLevel,
    high_watermark: i64,
    last_stable_offset: i64,
) -> i64 {
    match isolation {
        IsolationLevel::ReadCommitted => last_stable_offset,
        IsolationLevel::ReadUncommitted => high_watermark,
    }
}

fn record_readable_end(
    ends: &mut HashMap<(String, i32), i64>,
    key: (String, i32),
    isolation: IsolationLevel,
    high_watermark: i64,
    last_stable_offset: i64,
) {
    ends.insert(
        key,
        readable_end_offset(isolation, high_watermark, last_stable_offset),
    );
}

/// The offset to fetch next after consuming `batches`: one past the highest
/// `base_offset + last_offset_delta` across all decoded batches.
///
/// This function returns `None` when there are no batches, which leaves the
/// offset unchanged. The consumer uses it to advance past control and aborted
/// batches that emit no records, instead of re-fetching them.
fn next_offset_after(batches: &[krabka_protocol::records::RecordBatch]) -> Option<i64> {
    batches
        .iter()
        .map(|b| b.base_offset + i64::from(b.last_offset_delta) + 1)
        .max()
}

impl Consumer {
    /// Replace each reset sentinel in `next_offsets` with the offset from
    /// `ListOffsets`.
    ///
    /// A `LATEST_SENTINEL` resolves to the log end (`timestamp=-1`), or for
    /// `by_duration` to the first offset at or after now minus the duration.
    /// `auto_offset_reset = Latest` plants those sentinels at build time, and
    /// the `OFFSET_OUT_OF_RANGE` arm of the poll loop plants them again.
    /// [`Consumer::seek_to_beginning`] plants a `BEGINNING_SENTINEL`
    /// (`timestamp=-2`) and [`Consumer::seek_to_end`] an `END_SENTINEL`
    /// (`timestamp=-1`).
    ///
    /// This runs in `prepare_poll`, after the metadata refresh. A partition
    /// whose row has an error keeps its sentinel. `group_fetches` skips it, and
    /// the metadata refresh at the start of the next `prepare_poll` lets that
    /// poll try again.
    ///
    /// # Errors
    ///
    /// Returns `TopicAuthorizationFailed` when the broker does not authorize
    /// a topic, and a transport error that is not transient.
    #[tracing::instrument(
        name = "consumer.resolve_reset_sentinels",
        level = "debug",
        skip_all,
        fields(group_id = %self.group_id, sentinels = tracing::field::Empty),
        err
    )]
    async fn resolve_reset_sentinels(&self) -> Result<(), ConsumerError> {
        let by_sentinel = {
            let offsets = self.next_offsets.lock().await;
            [
                (
                    LATEST_SENTINEL,
                    reset_timestamp(self.auto_offset_reset, unix_now_ms()),
                ),
                (BEGINNING_SENTINEL, EARLIEST_TIMESTAMP),
                (END_SENTINEL, LATEST_TIMESTAMP),
            ]
            .map(|(sentinel, timestamp)| (sentinel, timestamp, keys_at(&offsets, sentinel)))
        };
        let count: usize = by_sentinel.iter().map(|(_, _, keys)| keys.len()).sum();
        if count == 0 {
            return Ok(());
        }
        tracing::Span::current().record("sentinels", count);
        let mut results = Vec::new();
        for (sentinel, timestamp, keys) in by_sentinel {
            if !keys.is_empty() {
                results.push((sentinel, self.list_offsets(&keys, timestamp).await?));
            }
        }
        {
            // A seek or a rebalance can change a position while the request
            // is in flight. Replace only a sentinel that is still there.
            let mut offsets = self.next_offsets.lock().await;
            for (sentinel, result) in &results {
                for (key, offset) in &result.offsets {
                    if let Some(next) = offsets.get_mut(key)
                        && *next == *sentinel
                    {
                        *next = *offset;
                    }
                }
            }
        }
        results
            .into_iter()
            .try_for_each(|(_, result)| result.authorization())
    }

    /// Replace each `COMMITTED_SENTINEL` with the committed offset of the
    /// group, or with the reset of `auto_offset_reset` when the group has none
    /// or the consumer has no group id. Kafka's
    /// `refreshCommittedOffsetsIfNeeded` and `resetInitializingPositions`.
    ///
    /// # Errors
    ///
    /// Returns the error of the `OffsetFetch` request.
    pub(crate) async fn resolve_committed_sentinels(
        &self,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), ConsumerError> {
        let keys = keys_at(&*self.next_offsets.lock().await, COMMITTED_SENTINEL);
        if keys.is_empty() {
            return Ok(());
        }
        // Kafka's `updateFetchPositions` gives the committed offsets the timer
        // of the `poll`.
        let deadline = deadline
            .unwrap_or_else(|| tokio::time::Instant::now() + self.default_api_timeout.to_std());
        let committed = if self.group_id.is_empty() {
            HashMap::new()
        } else {
            self.committed_until(&keys, deadline).await?
        };
        let reset = crate::consumer::reset_starting_offset(self.auto_offset_reset);
        let mut offsets = self.next_offsets.lock().await;
        let mut positions = self.positions.lock().await;
        for key in keys {
            let Some(next) = offsets.get_mut(&key) else {
                continue;
            };
            if *next != COMMITTED_SENTINEL {
                continue;
            }
            match committed.get(&key).cloned().flatten() {
                Some(offset) => {
                    *next = offset.offset;
                    let position = positions.entry(key).or_default();
                    position.offset_epoch = LeaderEpoch(offset.leader_epoch.unwrap_or(-1));
                    // Kafka's `TopicPartitionState.seekUnvalidated` waits for
                    // `OffsetForLeaderEpoch` when the committed epoch is older
                    // than the leader epoch of the metadata.
                    position.awaiting_validation = crate::validate::should_await_validation(
                        position.leader_epoch,
                        position.offset_epoch,
                    );
                }
                None => *next = reset,
            }
        }
        Ok(())
    }

    /// Send `ListOffsets(timestamp)` for `keys` to each partition leader.
    ///
    /// This is Kafka's `OffsetFetcher.groupListOffsetRequests` and
    /// `sendListOffsetRequest`: one request per leader, with the consumer's
    /// isolation level and the leader epoch from metadata. No lock is held
    /// across a request. A transient transport error drops that connection,
    /// and its partitions are retried, as Kafka retries a
    /// `RetriableException`.
    ///
    /// # Errors
    ///
    /// Returns a transport error that is not transient.
    #[cfg_attr(test, mutants::skip)] // cargo-mutants: ListOffsets RPC orchestration, exercised by the mock-broker test
    async fn list_offsets(
        &self,
        keys: &[(String, i32)],
        timestamp: i64,
    ) -> Result<ListOffsetsResult, ConsumerError> {
        let by_leader = group_list_offsets(keys, &*self.positions.lock().await, |id| {
            self.client.knows_broker(id)
        });
        let mut answers = Vec::with_capacity(by_leader.len());
        for (leader, by_topic) in by_leader {
            let request = build_offsets_request(by_topic, timestamp, self.isolation_level);
            let answer = if is_read_committed(self.isolation_level) {
                self.send_list_offsets(leader, ReadCommittedListOffsets(request))
                    .await
            } else {
                self.send_list_offsets(leader, request).await
            };
            match answer {
                Ok(answer) => answers.push(answer),
                Err(e) if is_transient_transport_error(&e) => {
                    if should_use_bootstrap_leader(leader) {
                        self.client.reconnect_bootstrap().await;
                    } else {
                        self.client.evict_broker(leader);
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(ListOffsetsResult::collect(keys, &answers))
    }

    /// Send one `ListOffsets` request to `leader`, or to the bootstrap
    /// connection when the leader is not known.
    async fn send_list_offsets<R>(
        &self,
        leader: i32,
        request: R,
    ) -> Result<ListOffsetsResponse, krabka_client_core::ClientError>
    where
        R: ProtocolRequest<Response = ListOffsetsResponse>,
    {
        if should_use_bootstrap_leader(leader) {
            self.client.send(request).await
        } else {
            self.client.broker(leader).send(request).await
        }
    }

    /// Recover the partitions that answered `OFFSET_OUT_OF_RANGE`.
    ///
    /// An errored Fetch partition carries no usable `log_start_offset`, so one
    /// `ListOffsets(timestamp=-2)` reads the real log start for the whole set.
    /// `Earliest` then fetches from there. `None` reports the first partition
    /// as a truncation, and the log start is the safe offset the caller seeks
    /// to.
    ///
    /// The caller has already released the `next_offsets` guard, so the RPC
    /// here holds no lock. A partition whose row has an error keeps its
    /// position, so the next Fetch gets `OFFSET_OUT_OF_RANGE` again and this
    /// path runs again. The return value is `true` when the caller must
    /// refresh metadata before that.
    ///
    /// # Errors
    ///
    /// Returns `TopicAuthorizationFailed` when the broker does not authorize
    /// a topic, and `LogTruncation` under `auto.offset.reset=none`.
    #[tracing::instrument(
        name = "consumer.recover_out_of_range",
        level = "debug",
        skip_all,
        fields(group_id = %self.group_id, partitions = out_of_range.len()),
        err
    )]
    async fn recover_out_of_range(
        &self,
        out_of_range: &[((String, i32), i64)],
    ) -> Result<bool, ConsumerError> {
        let keys: Vec<(String, i32)> = out_of_range.iter().map(|(key, _)| key.clone()).collect();
        let result = self.list_offsets(&keys, EARLIEST_TIMESTAMP).await?;
        result.authorization()?;

        if let AutoOffsetReset::None = self.auto_offset_reset {
            if let Some(truncation) = out_of_range_truncation(out_of_range, &result.offsets) {
                return Err(truncation);
            }
            return Ok(false);
        }

        let mut offsets = self.next_offsets.lock().await;
        let log_starts = out_of_range_positions(out_of_range, &result.offsets);
        for (key, log_start) in &log_starts {
            offsets.insert(key.clone(), *log_start);
        }
        if let Some(auto_commit) = &self.auto_commit {
            auto_commit
                .reset_polled(
                    log_starts
                        .into_iter()
                        .map(|(key, log_start)| (key, Some(log_start))),
                )
                .await;
        }
        Ok(result.retry)
    }
}

/// The fetch position to write for each out-of-range partition.
///
/// A partition the answer does not mention keeps the position it has. The next
/// poll fetches it again and gets `OFFSET_OUT_OF_RANGE` again, which retries
/// this path. That is better than inventing an offset for it.
fn out_of_range_positions(
    out_of_range: &[((String, i32), i64)],
    log_starts: &HashMap<(String, i32), i64>,
) -> Vec<((String, i32), i64)> {
    out_of_range
        .iter()
        .filter_map(|(key, _)| log_starts.get(key).map(|start| (key.clone(), *start)))
        .collect()
}

/// The truncation that `auto.offset.reset=none` reports.
///
/// It names the first out-of-range partition, the offset the fetch asked for,
/// and the log start the caller seeks to. `UNKNOWN_OFFSET` as the safe offset
/// says the broker gave no answer for that partition.
fn out_of_range_truncation(
    out_of_range: &[((String, i32), i64)],
    log_starts: &HashMap<(String, i32), i64>,
) -> Option<ConsumerError> {
    let ((topic, partition), fetch_offset) = out_of_range.first()?;
    Some(ConsumerError::LogTruncation {
        topic: topic.clone(),
        partition: *partition,
        fetch_offset: *fetch_offset,
        safe_offset: log_starts
            .get(&(topic.clone(), *partition))
            .copied()
            .unwrap_or(UNKNOWN_OFFSET),
    })
}

/// The keys whose next offset is exactly `sentinel`.
fn keys_at(offsets: &HashMap<(String, i32), i64>, sentinel: i64) -> Vec<(String, i32)> {
    offsets
        .iter()
        .filter(|(_, value)| **value == sentinel)
        .map(|(key, _)| key.clone())
        .collect()
}

impl Consumer {
    /// Apply the truncations that the proactive validate pass detected to
    /// `next_offsets`.
    ///
    /// This method honors `auto.offset.reset`. With `None` it errors on the
    /// first truncated partition.
    #[tracing::instrument(
        name = "consumer.apply_truncation",
        level = "debug",
        skip_all,
        fields(group_id = %self.group_id, truncated = truncated.len()),
        err
    )]
    async fn apply_truncation(
        &self,
        truncated: &HashMap<(String, i32), i64>,
    ) -> Result<(), ConsumerError> {
        let mut offsets = self.next_offsets.lock().await;
        for (key, safe_offset) in truncated {
            if let AutoOffsetReset::None = self.auto_offset_reset {
                let fetch_offset = fetch_offset_or_unknown(&offsets, key);
                return Err(ConsumerError::LogTruncation {
                    topic: key.0.clone(),
                    partition: key.1,
                    fetch_offset,
                    safe_offset: *safe_offset,
                });
            }
            offsets.insert(key.clone(), *safe_offset);
        }
        if let Some(auto_commit) = &self.auto_commit {
            auto_commit
                .reset_polled(
                    truncated
                        .iter()
                        .map(|(key, safe_offset)| (key.clone(), Some(*safe_offset))),
                )
                .await;
        }
        Ok(())
    }

    /// In-band `diverging_epoch` handler for use inside the poll loop, while the
    /// `next_offsets` guard is already held.
    fn handle_truncation_in_poll(
        &self,
        offsets: &mut HashMap<(String, i32), i64>,
        key: &(String, i32),
        safe_offset: i64,
    ) -> Result<(), ConsumerError> {
        if let AutoOffsetReset::None = self.auto_offset_reset {
            let fetch_offset = fetch_offset_or_unknown(offsets, key);
            return Err(ConsumerError::LogTruncation {
                topic: key.0.clone(),
                partition: key.1,
                fetch_offset,
                safe_offset,
            });
        }
        offsets.insert(key.clone(), safe_offset);
        Ok(())
    }
}

#[cfg(test)]
mod offset_advance_tests {
    use std::collections::HashMap;

    use assert2::check;
    use krabka_protocol::{
        owned::{
            fetch_request::ReplicaState,
            list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
        },
        primitives::uuid::Uuid as WireUuid,
        records::{RecordBatch, RecordsPayload},
        tagged_fields::UnknownTaggedFields,
    };

    use super::*;

    fn id(n: u8) -> WireUuid {
        let mut b = [0u8; 16];
        b[15] = n;
        WireUuid(b)
    }

    #[test]
    fn fetch_partition_error_codes_map_to_kafka_consumer_actions() {
        for (name, error_code, expected) in [
            ("none", 0, FetchPartitionAction::Records),
            ("offset out of range", 1, FetchPartitionAction::ResetOffset),
            (
                "unknown topic or partition",
                3,
                FetchPartitionAction::RefreshMetadata,
            ),
            ("not leader or follower", 6, FetchPartitionAction::Reroute),
            (
                "fenced leader epoch",
                74,
                FetchPartitionAction::RevalidateEpoch,
            ),
            (
                "unknown leader epoch",
                75,
                FetchPartitionAction::RevalidateEpoch,
            ),
            (
                "unknown topic id",
                100,
                FetchPartitionAction::RefreshMetadata,
            ),
            (
                "replica not available",
                9,
                FetchPartitionAction::RefreshMetadata,
            ),
            (
                "kafka storage error",
                56,
                FetchPartitionAction::RefreshMetadata,
            ),
            (
                "offset not available",
                78,
                FetchPartitionAction::RefreshMetadata,
            ),
            (
                "inconsistent topic id",
                103,
                FetchPartitionAction::RefreshMetadata,
            ),
            (
                "topic authorization failed",
                29,
                FetchPartitionAction::Fail(29),
            ),
        ] {
            check!(
                classify_fetch_partition_error(error_code) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn fetch_leader_id_uses_known_non_negative_leader_or_bootstrap() {
        check!(BOOTSTRAP_LEADER == -1);
        check!(UNKNOWN_LEADER_ID == -1);
        check!(UNKNOWN_FETCH_OFFSET == -1);
        for (name, leader, known, expected) in [
            ("known non-negative", 3, true, 3),
            ("negative leader", -1, true, BOOTSTRAP_LEADER),
            ("unknown broker", 3, false, BOOTSTRAP_LEADER),
        ] {
            check!(fetch_leader_id(leader, known) == expected, "case {name}");
        }
        for (name, leader, expected) in [
            ("bootstrap sentinel", BOOTSTRAP_LEADER, true),
            ("broker leader", 3, false),
        ] {
            check!(
                should_use_bootstrap_leader(leader) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn readable_end_respects_consumer_isolation() {
        check!(readable_end_offset(IsolationLevel::ReadUncommitted, 12, 9) == 12);
        check!(readable_end_offset(IsolationLevel::ReadCommitted, 12, 9) == 9);

        let mut ends = HashMap::new();
        record_readable_end(
            &mut ends,
            ("topic-a".into(), 0),
            IsolationLevel::ReadCommitted,
            12,
            9,
        );
        check!(ends.get(&("topic-a".into(), 0)) == Some(&9));
    }

    #[test]
    fn poll_helpers_preserve_sentinel_filter_and_record_math_boundaries() {
        let key = ("topic-a".to_string(), 2);
        let mut offsets = HashMap::new();

        assert2::assert!(fetch_offset_or_unknown(&offsets, &key) == UNKNOWN_FETCH_OFFSET);
        offsets.insert(key.clone(), 42);
        check!(fetch_offset_or_unknown(&offsets, &key) == 42);

        for (name, isolation, expected) in [
            ("read committed", IsolationLevel::ReadCommitted, true),
            ("read uncommitted", IsolationLevel::ReadUncommitted, false),
        ] {
            check!(is_read_committed(isolation) == expected, "case {name}");
        }
        for (name, first, offset, expected) in [
            ("at first offset", 10, 10, true),
            ("after first offset", 10, 11, true),
            ("before first offset", 11, 10, false),
        ] {
            check!(
                aborted_txn_started(first, offset) == expected,
                "case {name}"
            );
        }
        for (name, committed, aborted, started, expected) in [
            ("drop aborted", true, true, true, true),
            ("uncommitted isolation", false, true, true, false),
            ("not aborted", true, false, true, false),
            ("transaction not started", true, true, false, false),
        ] {
            check!(
                should_drop_aborted_batch(committed, aborted, started) == expected,
                "case {name}"
            );
        }
        check!(record_offset(100, 7) == 107);
    }

    #[test]
    fn transient_transport_error_classification_is_narrow() {
        use std::io;

        use krabka_client_core::ClientError;

        let transport_cases = vec![
            ("disconnected", ClientError::Disconnected, true),
            (
                "timeout",
                ClientError::Timeout(krabka_units::millis(10)),
                true,
            ),
            (
                "connection reset",
                ClientError::Io(io::Error::new(io::ErrorKind::ConnectionReset, "reset")),
                true,
            ),
            (
                "connect refused",
                ClientError::Connect {
                    addr: "127.0.0.1:9092".parse().unwrap(),
                    source: io::Error::new(io::ErrorKind::ConnectionRefused, "refused"),
                },
                true,
            ),
            ("server error", ClientError::Server { error_code: 6 }, false),
            (
                "incompatible version",
                ClientError::IncompatibleVersion {
                    api_key: 1,
                    broker_min: 0,
                    broker_max: 1,
                    client_min: 2,
                    client_max: 3,
                },
                false,
            ),
        ];
        for (name, error, expected) in &transport_cases {
            check!(
                is_transient_transport_error(error) == *expected,
                "case {name}"
            );
        }
        let poll_cases = vec![
            (
                "client connect",
                ConsumerError::Client(ClientError::Connect {
                    addr: "127.0.0.1:9092".parse().unwrap(),
                    source: io::Error::new(io::ErrorKind::ConnectionRefused, "refused"),
                }),
                true,
            ),
            ("server", ConsumerError::Server(6), false),
        ];
        for (name, error, expected) in &poll_cases {
            check!(is_transient_poll_error(error) == *expected, "case {name}");
        }
    }

    /// Kafka's `AbstractFetch.createFetchRequest` and `FetchRequest.Builder`:
    /// the limits, the isolation level, the session fields, the partitions by
    /// topic, and the forgotten partitions by topic.
    #[test]
    fn build_fetch_request_carries_limits_session_partitions_and_forgotten_topics() {
        let partition = |fetch_offset| crate::fetch_session::SessionPartition {
            topic_id: id(7),
            fetch_offset,
            current_leader_epoch: 5,
            last_fetched_epoch: 4,
            partition_max_bytes: 128 * 1024,
        };
        let req = build_fetch_request(
            500,
            IsolationLevel::ReadCommitted,
            krabka_units::bytes(7),
            mebibytes(2),
            "az-1",
            SessionRequest {
                session_id: 77,
                session_epoch: 3,
                partitions: BTreeMap::from([
                    (("topic-a".to_owned(), 2), partition(42)),
                    (("topic-a".to_owned(), 3), partition(43)),
                ]),
                forgotten: vec![
                    (("topic-b".to_owned(), 0), id(8)),
                    (("topic-b".to_owned(), 1), id(8)),
                ],
            },
        );
        let fetch_partition = |partition, fetch_offset| FetchPartition {
            partition,
            current_leader_epoch: 5,
            fetch_offset,
            last_fetched_epoch: 4,
            log_start_offset: -1,
            partition_max_bytes: 128 * 1024,
            replica_directory_id: WireUuid::default(),
            high_watermark: i64::MAX,
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert2::assert!(
            req == FetchRequest {
                replica_id: -1,
                max_wait_ms: 500,
                min_bytes: 7,
                max_bytes: 2 * 1024 * 1024,
                isolation_level: 1, // read_committed wire value
                session_id: 77,
                session_epoch: 3,
                topics: vec![FetchTopic {
                    topic: "topic-a".into(),
                    topic_id: id(7),
                    partitions: vec![fetch_partition(2, 42), fetch_partition(3, 43)],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }],
                forgotten_topics_data: vec![ForgottenTopic {
                    topic: "topic-b".into(),
                    topic_id: id(8),
                    partitions: vec![0, 1],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }],
                rack_id: "az-1".into(),
                cluster_id: None,
                replica_state: ReplicaState {
                    replica_id: -1,
                    replica_epoch: -1,
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }
        );
    }

    /// The request carries the caller's timestamp verbatim, the consumer's
    /// isolation level and the leader epoch, and asks as a consumer, which
    /// is `replica_id = -1`.
    ///
    /// `-1` asks for the log end and `-2` for the log start. The
    /// `OFFSET_OUT_OF_RANGE` recovery path depends on the second one, because
    /// an errored Fetch partition reports no usable `log_start_offset`.
    #[test]
    fn offsets_request_carries_the_timestamp_isolation_and_leader_epoch() {
        for (timestamp, isolation, isolation_level, leader_epoch) in [
            (LATEST_TIMESTAMP, IsolationLevel::ReadCommitted, 1, 4),
            (LATEST_TIMESTAMP, IsolationLevel::ReadUncommitted, 0, -1),
            (EARLIEST_TIMESTAMP, IsolationLevel::ReadCommitted, 1, -1),
            (EARLIEST_TIMESTAMP, IsolationLevel::ReadUncommitted, 0, 9),
        ] {
            let by_topic =
                BTreeMap::from([("topic-a".to_string(), vec![(3, LeaderEpoch(leader_epoch))])]);
            let req = build_offsets_request(by_topic, timestamp, isolation);

            check!(
                req == ListOffsetsRequest {
                    replica_id: -1,
                    isolation_level,
                    topics: vec![ListOffsetsTopic {
                        name: "topic-a".into(),
                        partitions: vec![ListOffsetsPartition {
                            partition_index: 3,
                            current_leader_epoch: leader_epoch,
                            timestamp,
                            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    }],
                    timeout_ms: 0,
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
                "timestamp {timestamp}, isolation {isolation:?}"
            );
        }
    }

    /// A partition goes to its known leader with the metadata epoch. A
    /// partition without a known, dialable leader goes to the bootstrap
    /// connection with the epoch that its position has.
    #[test]
    fn list_offsets_partitions_group_by_leader_with_the_leader_epoch() {
        let position = |leader_id, leader_epoch| PartitionPosition {
            leader_id,
            leader_epoch: LeaderEpoch(leader_epoch),
            ..Default::default()
        };
        let positions = HashMap::from([
            (("topic-a".to_string(), 0), position(1, 5)),
            (("topic-a".to_string(), 1), position(2, 6)),
            (("topic-b".to_string(), 0), position(1, 3)),
            (("topic-b".to_string(), 1), position(3, 8)),
        ]);
        let keys = [
            ("topic-a".to_string(), 0),
            ("topic-a".to_string(), 1),
            ("topic-b".to_string(), 0),
            ("topic-b".to_string(), 1),
            ("topic-c".to_string(), 0),
        ];
        let grouped = group_list_offsets(&keys, &positions, |id| id != 3);
        assert2::assert!(
            grouped
                == BTreeMap::from([
                    (
                        BOOTSTRAP_LEADER,
                        BTreeMap::from([
                            ("topic-b".to_string(), vec![(1, LeaderEpoch(8))]),
                            ("topic-c".to_string(), vec![(0, LeaderEpoch(-1))]),
                        ]),
                    ),
                    (
                        1,
                        BTreeMap::from([
                            ("topic-a".to_string(), vec![(0, LeaderEpoch(5))]),
                            ("topic-b".to_string(), vec![(0, LeaderEpoch(3))]),
                        ]),
                    ),
                    (
                        2,
                        BTreeMap::from([("topic-a".to_string(), vec![(1, LeaderEpoch(6))])]),
                    ),
                ])
        );
    }

    /// `OffsetFetcherUtils.handleListOffsetResponse` sets a position only for
    /// `NONE` with a known offset, fails for `TOPIC_AUTHORIZATION_FAILED`, and
    /// retries every other code.
    #[test]
    fn list_offsets_rows_map_to_kafka_consumer_actions() {
        for (name, error_code, offset, expected) in [
            ("none", 0, 40, ListOffsetsRowAction::Position(40)),
            ("none at zero", 0, 0, ListOffsetsRowAction::Position(0)),
            ("none without an offset", 0, -1, ListOffsetsRowAction::Retry),
            ("not leader or follower", 6, -1, ListOffsetsRowAction::Retry),
            (
                "unknown topic or partition",
                3,
                -1,
                ListOffsetsRowAction::Retry,
            ),
            ("leader not available", 5, -1, ListOffsetsRowAction::Retry),
            ("replica not available", 9, -1, ListOffsetsRowAction::Retry),
            ("offset not available", 78, -1, ListOffsetsRowAction::Retry),
            ("fenced leader epoch", 74, -1, ListOffsetsRowAction::Retry),
            ("unknown leader epoch", 75, -1, ListOffsetsRowAction::Retry),
            ("kafka storage error", 56, -1, ListOffsetsRowAction::Retry),
            (
                "unsupported for message format",
                43,
                -1,
                ListOffsetsRowAction::Retry,
            ),
            ("unexpected code", 2, -1, ListOffsetsRowAction::Retry),
            (
                "topic authorization failed",
                29,
                -1,
                ListOffsetsRowAction::Unauthorized,
            ),
        ] {
            check!(
                classify_list_offsets_row(error_code, offset) == expected,
                "case {name}"
            );
        }
    }

    /// The answers become offsets for the requested partitions. A partition
    /// with an error row or without a row is retried. An answer with an
    /// unauthorized row gives no offsets.
    #[test]
    fn list_offsets_result_collects_offsets_retries_and_unauthorized_topics() {
        let row = |partition_index, error_code, offset| ListOffsetsPartitionResponse {
            partition_index,
            error_code,
            offset,
            ..Default::default()
        };
        let answer = |name: &str, rows| ListOffsetsResponse {
            topics: vec![ListOffsetsTopicResponse {
                name: name.into(),
                partitions: rows,
                ..Default::default()
            }],
            ..Default::default()
        };
        let requested = [("topic-a".to_string(), 0), ("topic-a".to_string(), 1)];
        for (name, answers, expected) in [
            (
                "every partition answered",
                vec![answer("topic-a", vec![row(0, 0, 5), row(1, 0, 9)])],
                ListOffsetsResult {
                    offsets: HashMap::from([
                        (("topic-a".to_string(), 0), 5),
                        (("topic-a".to_string(), 1), 9),
                    ]),
                    retry: false,
                    unauthorized: BTreeSet::new(),
                },
            ),
            (
                "an error row is retried",
                vec![answer("topic-a", vec![row(0, 0, 5), row(1, 6, -1)])],
                ListOffsetsResult {
                    offsets: HashMap::from([(("topic-a".to_string(), 0), 5)]),
                    retry: true,
                    unauthorized: BTreeSet::new(),
                },
            ),
            (
                "a missing row is retried and an unrequested row is ignored",
                vec![answer("topic-a", vec![row(0, 0, 5), row(2, 0, 3)])],
                ListOffsetsResult {
                    offsets: HashMap::from([(("topic-a".to_string(), 0), 5)]),
                    retry: true,
                    unauthorized: BTreeSet::new(),
                },
            ),
            (
                "an unauthorized answer gives no offsets",
                vec![answer("topic-a", vec![row(0, 0, 5), row(1, 29, -1)])],
                ListOffsetsResult {
                    offsets: HashMap::new(),
                    retry: true,
                    unauthorized: BTreeSet::from(["topic-a".to_string()]),
                },
            ),
        ] {
            check!(
                ListOffsetsResult::collect(&requested, &answers) == expected,
                "case {name}"
            );
        }
    }

    /// Only an unauthorized topic fails the reset.
    #[test]
    fn list_offsets_result_authorization_names_the_unauthorized_topics() {
        let mut result = ListOffsetsResult::default();
        check!(result.authorization().map_err(|e| e.to_string()) == Ok(()));
        result.unauthorized.insert("topic-a".to_string());
        check!(
            result.authorization().map_err(|e| e.to_string())
                == Err(ConsumerError::TopicAuthorizationFailed(BTreeSet::from([
                    "topic-a".to_string()
                ]))
                .to_string())
        );
    }

    /// The recovery writes the broker's log start, and leaves a partition the
    /// broker did not answer for alone.
    ///
    /// Inventing a position for an unanswered partition is what the old code
    /// did: it wrote the -1 that an errored Fetch reports, and every following
    /// Fetch then asked for -1 and was out of range again.
    #[test]
    fn out_of_range_positions_uses_the_log_start_and_skips_the_unanswered() {
        let out_of_range = vec![
            (("topic-a".to_string(), 0), 0),
            (("topic-a".to_string(), 1), 3),
        ];
        let log_starts = HashMap::from([(("topic-a".to_string(), 0), 5)]);
        assert2::assert!(
            out_of_range_positions(&out_of_range, &log_starts)
                == vec![(("topic-a".to_string(), 0), 5)]
        );
    }

    /// Under `auto.offset.reset=none` the first out-of-range partition becomes
    /// the error, with the log start as the offset the caller seeks to.
    ///
    /// An answer that omits the partition gives `UNKNOWN_OFFSET`, which says
    /// the broker did not report one. It is not a position to fetch from.
    #[test]
    fn out_of_range_truncation_names_the_first_partition_and_its_log_start() {
        for (answered, expected_safe) in [(true, 5), (false, UNKNOWN_OFFSET)] {
            let log_starts = if answered {
                HashMap::from([(("topic-a".to_string(), 0), 5)])
            } else {
                HashMap::new()
            };
            let out_of_range = vec![
                (("topic-a".to_string(), 0), 2),
                (("topic-a".to_string(), 1), 3),
            ];
            assert2::assert!(
                out_of_range_truncation(&out_of_range, &log_starts).map(|e| e.to_string())
                    == Some(
                        ConsumerError::LogTruncation {
                            topic: "topic-a".to_string(),
                            partition: 0,
                            fetch_offset: 2,
                            safe_offset: expected_safe,
                        }
                        .to_string()
                    )
            );
        }
    }

    /// Nothing out of range means nothing to report.
    #[test]
    fn out_of_range_truncation_is_none_for_an_empty_set() {
        assert2::assert!(out_of_range_truncation(&[], &HashMap::new()).is_none());
    }

    /// `keys_at` picks exactly the partitions parked on a sentinel.
    #[test]
    fn keys_at_selects_only_the_sentinel_partitions() {
        let mut offsets: HashMap<(String, i32), i64> = HashMap::new();
        offsets.insert(("topic-a".to_string(), 0), LATEST_SENTINEL);
        offsets.insert(("topic-a".to_string(), 1), 42);
        assert2::assert!(keys_at(&offsets, LATEST_SENTINEL) == vec![("topic-a".to_string(), 0)]);
    }

    #[test]
    fn advance_target_uses_last_offset_delta_not_record_count() {
        // A batch spanning offsets 10..=14 (last_offset_delta = 4) but carrying
        // zero surviving records must still advance the fetch offset to 15.
        let batch = RecordBatch {
            base_offset: 10,
            last_offset_delta: 4,
            records: vec![],
            ..Default::default()
        };
        let payload = RecordsPayload::V2(vec![batch]);
        let batches = payload.as_v2().unwrap();
        assert2::assert!(super::next_offset_after(batches) == Some(15));
    }

    #[test]
    fn advance_target_none_for_empty() {
        let payload = RecordsPayload::V2(vec![]);
        assert2::assert!(super::next_offset_after(payload.as_v2().unwrap()) == None);
    }
}

#[cfg(test)]
pub(crate) mod partition_error_tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicI32, AtomicU8, AtomicU16, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use assert2::check;
    use krabka_client_core::{Client, MockBroker};
    use krabka_protocol::{
        Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            fetch_response::{
                EpochEndOffset, FetchResponse, FetchableTopicResponse, LeaderIdAndEpoch,
                PartitionData,
            },
            metadata_request,
            metadata_response::MetadataResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    };
    use krabka_units::secs;
    use tokio::sync::{Mutex, Notify};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        Assignor,
        consumer::{CommitIdentity, ConsumerRetryPolicy},
    };

    const TOPIC_ID: WireUuid = WireUuid([7; 16]);

    fn encode(response: &impl Encode, version: i16) -> Vec<u8> {
        let mut buf = bytes::BytesMut::new();
        response.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    /// A mock broker that answers `ApiVersions` and a non-flexible `Metadata`,
    /// and counts the `Metadata` requests.
    async fn metadata_counting_broker(metadata_requests: Arc<AtomicUsize>) -> MockBroker {
        MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                let versions = ApiVersionsResponse {
                    error_code: 0,
                    api_keys: vec![
                        ApiVersion {
                            api_key: api_versions_request::API_KEY,
                            min_version: 0,
                            max_version: 3,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: metadata_request::API_KEY,
                            min_version: 0,
                            max_version: 8,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                };
                return Some(encode(&versions, 0));
            }
            if api_key == metadata_request::API_KEY {
                metadata_requests.fetch_add(1, Ordering::SeqCst);
                return Some(encode(&MetadataResponse::default(), version));
            }
            None
        })
        .await
    }

    async fn consumer_on(broker: &MockBroker) -> Consumer {
        let client = Client::builder()
            .bootstrap(broker.addr.to_string())
            .build()
            .await
            .unwrap();
        consumer_with_client(client)
    }

    /// A consumer of `group-a` on `client` that owns `orders-0` at offset 5.
    pub(crate) fn consumer_with_client(client: Client) -> Consumer {
        Consumer {
            client,
            group_id: "group-a".into(),
            coordinator_id: Arc::new(AtomicI32::new(0)),
            retry_policy: ConsumerRetryPolicy::default().into(),
            member_id: tokio::sync::watch::channel("member-a".to_owned()).1,
            commit_identity: Arc::new(Mutex::new(CommitIdentity {
                generation: 1,
                member_id: "member-a".into(),
                ownership_ids: HashMap::from([(("orders".into(), 0), 1)]),
                rejoin_on_poll: false,
            })),
            commit_serialization: Arc::new(Mutex::new(())),
            commit_async_state: Arc::new(AtomicU8::new(0)),
            commit_async_callbacks: Arc::default(),
            group_instance_id: None,
            current_generation: Arc::new(AtomicI32::new(1)),
            subscription: crate::subscription::shared(vec!["orders".into()], None, true),
            group_protocol: crate::GroupProtocol::Classic,
            assigned: Arc::new(Mutex::new(vec![("orders".into(), 0)])),
            assignment_changed: Arc::new(Notify::new()),
            next_offsets: Arc::new(Mutex::new(HashMap::from([(("orders".into(), 0), 5)]))),
            end_offsets: Arc::new(Mutex::new(HashMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
            topic_ids: Arc::new(Mutex::new(HashMap::from([("orders".into(), TOPIC_ID)]))),
            session_timeout: secs(45),
            heartbeat_interval: secs(3),
            rebalance_protocol: crate::assignor::RebalanceProtocol::Eager,
            coordinator_shutdown: CancellationToken::new(),
            coordinator_handle: None,
            isolation_level: IsolationLevel::ReadUncommitted,
            fetch_min: krabka_client_core::DEFAULT_FETCH_MIN,
            fetch_max: DEFAULT_FETCH_MAX,
            fetch_partition_max: DEFAULT_FETCH_PARTITION_MAX,
            fetch_max_wait: crate::consumer::DEFAULT_CONSUMER_FETCH_MAX_WAIT,
            fetches: crate::poll::Fetches::default(),
            client_rack: None,
            metadata_max_age: crate::consumer::DEFAULT_CONSUMER_METADATA_MAX_AGE,
            request_timeout: krabka_units::secs(30),
            wakeup: crate::control::WakeupHandle::default(),
            enforced_rebalances: tokio::sync::mpsc::unbounded_channel().0,
            default_api_timeout: crate::consumer::DEFAULT_CONSUMER_DEFAULT_API_TIMEOUT,
            paused: std::sync::Mutex::default(),
            auto_offset_reset: AutoOffsetReset::Latest,
            poll_error: crate::coordinator::PollErrorSlot::default(),
            auto_commit: None,
            poll_signal: crate::coordinator::PollSignal::default(),
            rebalance_pending: tokio::sync::watch::channel(false).1,
            max_poll_records: crate::consumer::DEFAULT_CONSUMER_MAX_POLL_RECORDS,
            fetch_buffer: crate::fetch_buffer::FetchBuffer::default(),
            close_operation: tokio::sync::watch::Sender::new(
                crate::control::CloseRequest::default(),
            ),
            rebalance_listener: None,
            listener_calls: tokio::sync::mpsc::unbounded_channel().1,
            assigned_callback_pending: Arc::default(),
        }
    }

    /// A Fetch v13 response row: the topic by id only, one partition with
    /// `error_code`, no records and no leader hint.
    fn fetch_response(error_code: i16) -> FetchResponse {
        FetchResponse {
            responses: vec![FetchableTopicResponse {
                topic: String::new(),
                topic_id: TOPIC_ID,
                partitions: vec![PartitionData {
                    partition_index: 0,
                    error_code,
                    high_watermark: -1,
                    last_stable_offset: -1,
                    log_start_offset: -1,
                    diverging_epoch: EpochEndOffset {
                        epoch: -1,
                        end_offset: -1,
                        ..Default::default()
                    },
                    current_leader: LeaderIdAndEpoch {
                        leader_id: -1,
                        leader_epoch: -1,
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// How one poll handled a partition error row.
    #[derive(Debug, PartialEq)]
    struct PollOutcome {
        /// `Ok(record_count)`, `Err(Some(code))` for
        /// `ConsumerError::Server(code)`, or `Err(None)` for any other error.
        result: Result<usize, Option<i16>>,
        metadata_requests: usize,
        next_offset: Option<i64>,
    }

    /// Kafka's consumer raises a fatal `OffsetFetch` error from `poll()`. A
    /// rejoin leaves the error for `poll`, which returns it once. The next poll
    /// runs as usual.
    #[tokio::test]
    async fn poll_returns_a_fatal_rejoin_error_once() {
        let broker = metadata_counting_broker(Arc::default()).await;
        let mut consumer = consumer_on(&broker).await;
        let topics = std::collections::BTreeSet::from(["orders".to_string()]);
        crate::coordinator::report_rejoin_error(
            &consumer.poll_error,
            ConsumerError::TopicAuthorizationFailed(topics.clone()),
        );

        let first = consumer
            .prepare_poll(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .map_err(|error| match error {
                ConsumerError::TopicAuthorizationFailed(topics) => Some(topics),
                _ => None,
            });
        let second = consumer
            .prepare_poll(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .map_err(|_| None::<std::collections::BTreeSet<String>>);

        broker.stop();
        assert2::assert!((first, second) == (Err(Some(topics)), Ok(true)));
    }

    /// A partition that the coordinator revokes while `poll` drains the fetch
    /// buffer is either still owned for the whole drain, or not drained at
    /// all. The drain never returns records of a partition that left the
    /// assignment before the drain completed.
    #[tokio::test]
    async fn drain_never_returns_records_of_a_partition_revoked_during_the_drain() {
        let broker = metadata_counting_broker(Arc::default()).await;
        let mut consumer = consumer_on(&broker).await;
        consumer
            .fetch_buffer
            .push(crate::fetch_buffer::BufferedPartition {
                key: ("orders".into(), 0),
                position: 5,
                records: std::collections::VecDeque::from([ConsumerRecord {
                    topic: "orders".into(),
                    partition: 0,
                    offset: 5,
                    leader_epoch: 0,
                    timestamp: 0,
                    timestamp_type: crate::TimestampType::CreateTime,
                    key: None,
                    value: None,
                    headers: Vec::new(),
                }]),
                next_offset: 6,
                last_epoch: None,
            });
        let assigned = Arc::clone(&consumer.assigned);
        let next_offsets = Arc::clone(&consumer.next_offsets);
        // Block the drain between the ownership check and the position update.
        let offsets_guard = next_offsets.lock().await;
        let drain = tokio::spawn(async move {
            let records = consumer.drain_fetch_buffer().await;
            (consumer, records)
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The coordinator revokes the partition, if it can take the lock.
        let revoked = match tokio::time::timeout(Duration::from_millis(50), assigned.lock()).await {
            Ok(mut owned) => {
                owned.clear();
                true
            }
            Err(_) => false,
        };
        drop(offsets_guard);
        let (consumer, records) = drain.await.expect("drain task");
        drop(consumer);
        broker.stop();
        assert2::assert!(!revoked || records.is_empty());
    }

    /// While a join runs, Kafka's eager `onJoinPrepare` has revoked every
    /// partition, so `poll` fetches nothing until the join completes or the
    /// poll timeout passes. A cooperative member keeps fetching its owned
    /// partitions.
    #[tokio::test(start_paused = true)]
    async fn poll_waits_for_a_pending_eager_join_but_not_for_a_cooperative_one() {
        let broker = metadata_counting_broker(Arc::default()).await;
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, assignor, owns_partitions, pending, join_completes_after, expected) in [
            ("no join", Assignor::Range, true, false, None, (true, 0)),
            (
                "eager join completes in time",
                Assignor::Range,
                true,
                true,
                Some(100),
                (true, 100),
            ),
            (
                "eager join does not complete in time",
                Assignor::Range,
                true,
                true,
                None,
                (false, 500),
            ),
            (
                "cooperative join with owned partitions",
                Assignor::CooperativeSticky,
                true,
                true,
                None,
                (true, 0),
            ),
            (
                "cooperative join without partitions",
                Assignor::CooperativeSticky,
                false,
                true,
                Some(200),
                (true, 200),
            ),
        ] {
            let mut consumer = consumer_on(&broker).await;
            consumer.rebalance_protocol =
                crate::assignor::rebalance_protocol_of(&[assignor]).expect("assignor protocol");
            if !owns_partitions {
                consumer.assigned.lock().await.clear();
            }
            let pending_sender = tokio::sync::watch::Sender::new(pending);
            consumer.rebalance_pending = pending_sender.subscribe();
            let completion = join_completes_after.map(|millis| {
                let pending_sender = pending_sender.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(millis)).await;
                    pending_sender.send_replace(false);
                })
            });
            let start = tokio::time::Instant::now();
            let joined = consumer
                .wait_for_rebalance(start + Duration::from_millis(500))
                .await
                .expect("no listener");
            let elapsed_ms = u64::try_from(start.elapsed().as_millis()).expect("millis");
            if let Some(completion) = completion {
                completion.abort();
            }
            actual.push((name, (joined, elapsed_ms)));
            wanted.push((name, expected));
        }
        broker.stop();
        assert2::assert!(actual == wanted);
    }

    /// One `poll` of the record cap case: the number of records, the first and
    /// the last offset, and the fetch position after the `poll`.
    #[derive(Debug, PartialEq)]
    struct CappedPoll {
        records: usize,
        first_offset: Option<i64>,
        last_offset: Option<i64>,
        next_offset: Option<i64>,
    }

    /// Kafka's `FetchCollector.collectFetch` returns at most
    /// `max.poll.records` records, and keeps the rest of the completed fetch for
    /// the next `poll`. The position moves only past the returned records, and
    /// the next `poll` sends no Fetch while buffered records remain.
    #[tokio::test]
    async fn poll_returns_at_most_max_poll_records_and_keeps_the_rest_without_a_fetch() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let fetches_in_mock = Arc::clone(&fetches);
        let broker = MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                let versions = ApiVersionsResponse {
                    api_keys: vec![
                        ApiVersion {
                            api_key: api_versions_request::API_KEY,
                            min_version: 0,
                            max_version: 3,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: metadata_request::API_KEY,
                            min_version: 0,
                            max_version: 8,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: krabka_protocol::owned::fetch_request::API_KEY,
                            min_version: 4,
                            max_version: 11,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                };
                return Some(encode(&versions, 0));
            }
            if api_key == metadata_request::API_KEY {
                return Some(encode(&MetadataResponse::default(), version));
            }
            if api_key != krabka_protocol::owned::fetch_request::API_KEY {
                return None;
            }
            fetches_in_mock.fetch_add(1, Ordering::SeqCst);
            let batch = krabka_protocol::records::RecordBatch {
                base_offset: 5,
                last_offset_delta: 1199,
                records: (0..1200)
                    .map(|offset_delta| krabka_protocol::records::Record {
                        offset_delta,
                        value: Some(bytes::Bytes::from_static(b"v")),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            let mut response = fetch_response(0);
            response.responses[0].topic = "orders".into();
            response.responses[0].partitions[0].high_watermark = 1205;
            response.responses[0].partitions[0].records =
                Some(krabka_protocol::records::RecordsPayload::V2(vec![batch]));
            Some(encode(&response, version))
        })
        .await;
        let mut consumer = consumer_on(&broker).await;

        let mut polls = Vec::new();
        for _ in 0..3 {
            let records = consumer.poll(secs(5)).await.expect("poll");
            polls.push(CappedPoll {
                records: records.len(),
                first_offset: records.first().map(|record| record.offset),
                last_offset: records.last().map(|record| record.offset),
                next_offset: consumer
                    .next_offsets
                    .lock()
                    .await
                    .get(&("orders".to_string(), 0))
                    .copied(),
            });
        }

        broker.stop();
        assert2::assert!(
            (polls, fetches.load(Ordering::SeqCst))
                == (
                    vec![
                        CappedPoll {
                            records: 500,
                            first_offset: Some(5),
                            last_offset: Some(504),
                            next_offset: Some(505),
                        },
                        CappedPoll {
                            records: 500,
                            first_offset: Some(505),
                            last_offset: Some(1004),
                            next_offset: Some(1005),
                        },
                        CappedPoll {
                            records: 200,
                            first_offset: Some(1005),
                            last_offset: Some(1204),
                            next_offset: Some(1205),
                        },
                    ],
                    1,
                )
        );
    }

    /// Kafka's `FetchCollector.handleInitializeErrors` requests a metadata
    /// update for 3, 6 and 100 and does not raise an error. The poll returns
    /// no records, keeps the fetch position, and refreshes metadata. Any code
    /// that Kafka does not handle fails the poll.
    #[tokio::test]
    async fn fetch_partition_errors_refresh_metadata_or_fail_the_poll() {
        for (name, error_code, expected) in [
            (
                "unknown topic or partition",
                3,
                PollOutcome {
                    result: Ok(0),
                    metadata_requests: 1,
                    next_offset: Some(5),
                },
            ),
            (
                "not leader or follower",
                6,
                PollOutcome {
                    result: Ok(0),
                    metadata_requests: 1,
                    next_offset: Some(5),
                },
            ),
            (
                "unknown topic id",
                100,
                PollOutcome {
                    result: Ok(0),
                    metadata_requests: 1,
                    next_offset: Some(5),
                },
            ),
            (
                "topic authorization failed",
                29,
                PollOutcome {
                    result: Err(Some(29)),
                    metadata_requests: 0,
                    next_offset: Some(5),
                },
            ),
        ] {
            let metadata_requests = Arc::new(AtomicUsize::new(0));
            let broker = metadata_counting_broker(Arc::clone(&metadata_requests)).await;
            let mut consumer = consumer_on(&broker).await;
            let topic_ids = consumer.topic_ids.lock().await.clone();

            let result = consumer
                .process_fetch_responses(vec![fetch_response(error_code)], &topic_ids)
                .await
                .map(|()| consumer.fetch_buffer.len())
                .map_err(|error| match error {
                    ConsumerError::Server(code) => Some(code),
                    _ => None,
                });
            let outcome = PollOutcome {
                result,
                metadata_requests: metadata_requests.load(Ordering::SeqCst),
                next_offset: consumer
                    .next_offsets
                    .lock()
                    .await
                    .get(&("orders".to_string(), 0))
                    .copied(),
            };

            broker.stop();
            check!(outcome == expected, "case {name}");
        }
    }

    /// How the test resets the fetch position of `orders-0` after the start of
    /// `poll`.
    #[derive(Clone, Copy, Debug)]
    enum PollReset {
        /// A Fetch row without an error and without records.
        None,
        /// A Fetch row with `OFFSET_OUT_OF_RANGE` under `auto.offset.reset=latest`.
        LatestOutOfRange,
        /// A Fetch row with `OFFSET_OUT_OF_RANGE` under
        /// `auto.offset.reset=earliest`. The leader answers `ListOffsets` with
        /// log start 2.
        EarliestOutOfRange,
        /// A Fetch row with a diverging epoch that ends at offset 3.
        FetchTruncation,
        /// The validation pass finds a truncation to offset 3.
        ValidationTruncation,
    }

    /// Kafka's `onJoinPrepare` commits `SubscriptionState.allConsumed`, which
    /// reads the current fetch positions. A reset in `poll` therefore changes
    /// what the commit before a `JoinGroup` sends. A position that waits for
    /// `ListOffsets` is not valid, and `allConsumed` skips it.
    #[tokio::test]
    async fn a_reset_in_poll_moves_the_position_of_the_commit_before_join() {
        let orders_0 = ("orders".to_string(), 0);
        let cases = [
            (
                "no reset: the position at the start of poll",
                PollReset::None,
                HashMap::from([(orders_0.clone(), (5, -1))]),
            ),
            (
                "out of range under latest: no position until ListOffsets",
                PollReset::LatestOutOfRange,
                HashMap::new(),
            ),
            (
                "out of range under earliest: the log start",
                PollReset::EarliestOutOfRange,
                HashMap::from([(orders_0.clone(), (2, -1))]),
            ),
            (
                "truncation in a Fetch response: the end offset of the diverging epoch",
                PollReset::FetchTruncation,
                HashMap::from([(orders_0.clone(), (3, -1))]),
            ),
            (
                "truncation from validation: the safe offset",
                PollReset::ValidationTruncation,
                HashMap::from([(orders_0.clone(), (3, -1))]),
            ),
        ];
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, reset, expected) in cases {
            let leader_port = Arc::new(AtomicU16::new(0));
            let broker = list_offsets_broker(
                "leader",
                Arc::clone(&leader_port),
                (0, 2),
                list_offsets_request::MAX_VERSION,
                Arc::default(),
                Arc::default(),
            )
            .await;
            leader_port.store(broker.addr.port(), Ordering::SeqCst);
            let mut consumer = consumer_on(&broker).await;
            let auto_commit = crate::commit::AutoCommit::new(std::time::Duration::from_secs(5));
            consumer.auto_commit = Some(auto_commit.clone());
            // The start of `poll` records the positions. The interval has not
            // passed, so it sends no commit.
            consumer.maybe_auto_commit_async().await;
            let topic_ids = consumer.topic_ids.lock().await.clone();

            let result = match reset {
                PollReset::None => {
                    consumer
                        .process_fetch_responses(vec![fetch_response(0)], &topic_ids)
                        .await
                }
                PollReset::LatestOutOfRange => {
                    consumer.auto_offset_reset = AutoOffsetReset::Latest;
                    consumer
                        .process_fetch_responses(vec![fetch_response(1)], &topic_ids)
                        .await
                }
                PollReset::EarliestOutOfRange => {
                    consumer.auto_offset_reset = AutoOffsetReset::Earliest;
                    consumer
                        .refresh_leader_epochs()
                        .await
                        .expect("metadata for the leader route");
                    consumer
                        .process_fetch_responses(vec![fetch_response(1)], &topic_ids)
                        .await
                }
                PollReset::FetchTruncation => {
                    let mut response = fetch_response(0);
                    response.responses[0].partitions[0].diverging_epoch = EpochEndOffset {
                        epoch: 2,
                        end_offset: 3,
                        ..Default::default()
                    };
                    consumer
                        .process_fetch_responses(vec![response], &topic_ids)
                        .await
                }
                PollReset::ValidationTruncation => {
                    consumer
                        .apply_truncation(&HashMap::from([(orders_0.clone(), 3)]))
                        .await
                }
            };
            result.expect("the reset succeeds");
            let offsets = auto_commit
                .polled_offsets(&HashMap::from([(orders_0.clone(), 1)]))
                .await;

            broker.stop();
            actual.push((name, offsets));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// The broker id that the metadata names as the leader of `orders-0`.
    const LEADER_ID: i32 = 1;
    /// The leader epoch that the metadata reports for `orders-0`.
    const LEADER_EPOCH: i32 = 7;

    /// The `ListOffsets` requests that the mock brokers decoded, with the
    /// name of the broker that received each one and the request version.
    type SentListOffsets = Arc<std::sync::Mutex<Vec<(&'static str, i16, ListOffsetsRequest)>>>;

    /// The response bytes for a `ListOffsets` request at `version` with one
    /// `orders-0` row.
    fn list_offsets_answer_bytes(version: i16, error_code: i16, offset: i64) -> Vec<u8> {
        use krabka_protocol::owned::{
            list_offsets_request::FLEXIBLE_MIN,
            list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
        };
        let answer = ListOffsetsResponse {
            topics: vec![ListOffsetsTopicResponse {
                name: "orders".into(),
                partitions: vec![ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code,
                    offset,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut bytes = Vec::new();
        if version >= FLEXIBLE_MIN {
            // The flexible response header has an empty tagged-field section.
            bytes.push(0);
        }
        bytes.extend(encode(&answer, version));
        bytes
    }

    /// A mock broker that answers `ApiVersions`, `Metadata` and `ListOffsets`.
    ///
    /// It records each decoded `ListOffsets` request under `name` and answers
    /// it with one `orders-0` row. The `Metadata` answer names broker
    /// `LEADER_ID` at `leader_port` as the leader of `orders-0`, with epoch
    /// `LEADER_EPOCH`.
    async fn list_offsets_broker(
        name: &'static str,
        leader_port: Arc<AtomicU16>,
        row: (i16, i64),
        list_offsets_max_version: i16,
        sent: SentListOffsets,
        metadata_requests: Arc<AtomicUsize>,
    ) -> MockBroker {
        use bytes::Buf as _;
        use krabka_protocol::{
            Decode as _,
            owned::{
                list_offsets_request::{self, FLEXIBLE_MIN},
                metadata_response::{
                    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
                },
            },
        };
        MockBroker::start(move |api_key, version, _corr_id, mut body| {
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
                metadata_requests.fetch_add(1, Ordering::SeqCst);
                let metadata = MetadataResponse {
                    brokers: vec![MetadataResponseBroker {
                        node_id: LEADER_ID,
                        host: "127.0.0.1".into(),
                        port: i32::from(leader_port.load(Ordering::SeqCst)),
                        ..Default::default()
                    }],
                    topics: vec![MetadataResponseTopic {
                        name: Some("orders".into()),
                        topic_id: TOPIC_ID,
                        partitions: vec![MetadataResponsePartition {
                            partition_index: 0,
                            leader_id: LEADER_ID,
                            leader_epoch: LEADER_EPOCH,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                return Some(encode(&metadata, version));
            }
            if api_key == list_offsets_request::API_KEY {
                let client_id_len = usize::try_from(body.get_i16()).expect("client id length");
                body.advance(client_id_len);
                if version >= FLEXIBLE_MIN {
                    body.advance(1);
                }
                let request =
                    ListOffsetsRequest::decode(&mut body, version).expect("ListOffsets decodes");
                sent.lock()
                    .expect("sent lock")
                    .push((name, version, request));
                return Some(list_offsets_answer_bytes(version, row.0, row.1));
            }
            None
        })
        .await
    }

    /// The `ListOffsets` request that the consumer sends for `orders-0`.
    fn expected_list_offsets(isolation_level: i8, timestamp: i64) -> ListOffsetsRequest {
        ListOffsetsRequest {
            replica_id: -1,
            isolation_level,
            topics: vec![ListOffsetsTopic {
                name: "orders".into(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: 0,
                    current_leader_epoch: LEADER_EPOCH,
                    timestamp,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Why a poll step failed, in a form that tests can compare.
    #[derive(Debug, PartialEq)]
    enum PollFailure {
        /// `ConsumerError::TopicAuthorizationFailed` with its topics.
        TopicAuthorizationFailed(std::collections::BTreeSet<String>),
        /// `ClientError::IncompatibleVersion` as
        /// `(api_key, broker_min, broker_max, client_min, client_max)`.
        IncompatibleVersion(i16, i16, i16, i16, i16),
        /// Any other error.
        Other,
    }

    impl From<ConsumerError> for PollFailure {
        fn from(error: ConsumerError) -> Self {
            match error {
                ConsumerError::TopicAuthorizationFailed(topics) => {
                    Self::TopicAuthorizationFailed(topics)
                }
                ConsumerError::Client(krabka_client_core::ClientError::IncompatibleVersion {
                    api_key,
                    broker_min,
                    broker_max,
                    client_min,
                    client_max,
                }) => Self::IncompatibleVersion(
                    api_key, broker_min, broker_max, client_min, client_max,
                ),
                _ => Self::Other,
            }
        }
    }

    /// How the consumer handled one `ListOffsets` exchange.
    #[derive(Debug, PartialEq)]
    struct ListOffsetsOutcome {
        /// Each decoded request, with the name of the broker that received it
        /// and the version.
        requests: Vec<(&'static str, i16, ListOffsetsRequest)>,
        result: Result<(), PollFailure>,
        /// The `Metadata` requests after the setup.
        metadata_requests: usize,
        next_offset: Option<i64>,
    }

    /// How the test reaches the `ListOffsets` path.
    #[derive(Clone, Copy, Debug)]
    enum Reset {
        /// `auto.offset.reset=latest`: a sentinel that `prepare_poll` resolves.
        Latest,
        /// `auto.offset.reset=by_duration:PT1H`: a sentinel that
        /// `prepare_poll` resolves with the timestamp of one hour ago.
        ByOneHour,
        /// `auto.offset.reset=earliest`: a Fetch row with
        /// `OFFSET_OUT_OF_RANGE`.
        EarliestOutOfRange,
        /// `seek_to_beginning` of every partition, then `position`.
        SeekToBeginning,
        /// `seek_to_end` of every partition, then `position`.
        SeekToEnd,
    }

    /// One `ListOffsets` scenario on two mock brokers.
    #[derive(Clone, Copy, Debug)]
    struct Exchange {
        isolation: IsolationLevel,
        reset: Reset,
        /// The `(error_code, offset)` row that the leader answers.
        row: (i16, i64),
        /// The highest `ListOffsets` version that both brokers advertise.
        list_offsets_max_version: i16,
        /// `true` when a metadata refresh fills the positions map before the
        /// exchange, as after an earlier poll. `false` is a new assignment
        /// without a committed offset, whose position has no leader yet.
        primed: bool,
    }

    /// Run `exchange` with a consumer that bootstraps from a broker that is
    /// not the leader of `orders-0`.
    ///
    /// The bootstrap broker answers each `ListOffsets` with
    /// `NOT_LEADER_OR_FOLLOWER`.
    async fn run_list_offsets_exchange(exchange: Exchange) -> ListOffsetsOutcome {
        let sent: SentListOffsets = Arc::default();
        let metadata_requests = Arc::new(AtomicUsize::new(0));
        let leader_port = Arc::new(AtomicU16::new(0));
        let leader = list_offsets_broker(
            "leader",
            Arc::clone(&leader_port),
            exchange.row,
            exchange.list_offsets_max_version,
            Arc::clone(&sent),
            Arc::clone(&metadata_requests),
        )
        .await;
        leader_port.store(leader.addr.port(), Ordering::SeqCst);
        let bootstrap = list_offsets_broker(
            "bootstrap",
            Arc::clone(&leader_port),
            (6, -1),
            exchange.list_offsets_max_version,
            Arc::clone(&sent),
            Arc::clone(&metadata_requests),
        )
        .await;
        let mut consumer = consumer_on(&bootstrap).await;
        consumer.isolation_level = exchange.isolation;
        if exchange.primed {
            consumer
                .refresh_leader_epochs()
                .await
                .expect("setup metadata");
            metadata_requests.store(0, Ordering::SeqCst);
        }

        let result = match exchange.reset {
            Reset::Latest => {
                consumer.auto_offset_reset = AutoOffsetReset::Latest;
                consumer
                    .next_offsets
                    .lock()
                    .await
                    .insert(("orders".into(), 0), LATEST_SENTINEL);
                consumer
                    .prepare_poll(tokio::time::Instant::now() + Duration::from_secs(5))
                    .await
                    .map(|_| ())
            }
            Reset::ByOneHour => {
                consumer.auto_offset_reset =
                    AutoOffsetReset::ByDuration(std::time::Duration::from_hours(1));
                consumer
                    .next_offsets
                    .lock()
                    .await
                    .insert(("orders".into(), 0), LATEST_SENTINEL);
                consumer
                    .prepare_poll(tokio::time::Instant::now() + Duration::from_secs(5))
                    .await
                    .map(|_| ())
            }
            Reset::EarliestOutOfRange => {
                consumer.auto_offset_reset = AutoOffsetReset::Earliest;
                let topic_ids = consumer.topic_ids.lock().await.clone();
                consumer
                    .process_fetch_responses(vec![fetch_response(1)], &topic_ids)
                    .await
            }
            Reset::SeekToBeginning | Reset::SeekToEnd => {
                // Kafka's `seekToBeginning` and `seekToEnd` ignore
                // `auto.offset.reset`.
                consumer.auto_offset_reset =
                    AutoOffsetReset::ByDuration(std::time::Duration::from_hours(1));
                consumer.default_api_timeout = krabka_units::millis(300);
                let reset = if matches!(exchange.reset, Reset::SeekToBeginning) {
                    consumer.seek_to_beginning(&[]).await
                } else {
                    consumer.seek_to_end(&[]).await
                };
                match reset {
                    Ok(()) => consumer.position("orders", 0).await.map(|_| ()),
                    Err(error) => Err(error),
                }
            }
        };
        let requests = sent.lock().expect("sent lock").clone();
        let outcome = ListOffsetsOutcome {
            requests,
            result: result.map_err(PollFailure::from),
            metadata_requests: metadata_requests.load(Ordering::SeqCst),
            next_offset: consumer
                .next_offsets
                .lock()
                .await
                .get(&("orders".to_string(), 0))
                .copied(),
        };

        bootstrap.stop();
        leader.stop();
        outcome
    }

    /// Kafka's consumer sends `ListOffsets` to the partition leader with its
    /// own isolation level and the leader epoch from metadata
    /// (`OffsetFetcher.groupListOffsetRequests` and
    /// `ListOffsetsRequest.Builder.forConsumer`). A row with an error never
    /// sets a position. `TOPIC_AUTHORIZATION_FAILED` fails the poll, and any
    /// other code retries after a metadata refresh
    /// (`OffsetFetcherUtils.handleListOffsetResponse`).
    #[tokio::test]
    async fn list_offsets_goes_to_the_leader_with_the_isolation_level_and_honours_row_errors() {
        use std::collections::BTreeSet;

        let max = list_offsets_request::MAX_VERSION;
        let latest_request =
            |isolation| vec![("leader", max, expected_list_offsets(isolation, -1))];
        let latest = |isolation, row| Exchange {
            isolation,
            reset: Reset::Latest,
            row,
            list_offsets_max_version: max,
            primed: true,
        };
        let earliest = |isolation, row| Exchange {
            reset: Reset::EarliestOutOfRange,
            ..latest(isolation, row)
        };
        for (name, exchange, expected) in [
            (
                "read committed latest reads the last stable offset",
                latest(IsolationLevel::ReadCommitted, (0, 40)),
                ListOffsetsOutcome {
                    requests: latest_request(1),
                    result: Ok(()),
                    metadata_requests: 1,
                    next_offset: Some(40),
                },
            ),
            (
                "read uncommitted latest reads the high watermark",
                latest(IsolationLevel::ReadUncommitted, (0, 50)),
                ListOffsetsOutcome {
                    requests: latest_request(0),
                    result: Ok(()),
                    metadata_requests: 1,
                    next_offset: Some(50),
                },
            ),
            (
                "not leader or follower keeps the sentinel and refreshes metadata",
                latest(IsolationLevel::ReadCommitted, (6, -1)),
                ListOffsetsOutcome {
                    requests: latest_request(1),
                    result: Ok(()),
                    metadata_requests: 1,
                    next_offset: Some(LATEST_SENTINEL),
                },
            ),
            (
                "topic authorization failed fails the poll",
                latest(IsolationLevel::ReadCommitted, (29, -1)),
                ListOffsetsOutcome {
                    requests: latest_request(1),
                    result: Err(PollFailure::TopicAuthorizationFailed(BTreeSet::from([
                        "orders".to_string(),
                    ]))),
                    metadata_requests: 1,
                    next_offset: Some(LATEST_SENTINEL),
                },
            ),
            (
                "seek to beginning reads the log start",
                Exchange {
                    reset: Reset::SeekToBeginning,
                    ..latest(IsolationLevel::ReadUncommitted, (0, 7))
                },
                ListOffsetsOutcome {
                    requests: vec![("leader", max, expected_list_offsets(0, -2))],
                    result: Ok(()),
                    metadata_requests: 1,
                    next_offset: Some(7),
                },
            ),
            (
                "seek to end reads the last stable offset",
                Exchange {
                    reset: Reset::SeekToEnd,
                    ..latest(IsolationLevel::ReadCommitted, (0, 40))
                },
                ListOffsetsOutcome {
                    requests: latest_request(1),
                    result: Ok(()),
                    metadata_requests: 1,
                    next_offset: Some(40),
                },
            ),
            (
                "earliest after out of range reads the log start",
                earliest(IsolationLevel::ReadUncommitted, (0, 7)),
                ListOffsetsOutcome {
                    requests: vec![("leader", max, expected_list_offsets(0, -2))],
                    result: Ok(()),
                    metadata_requests: 0,
                    next_offset: Some(7),
                },
            ),
            (
                "earliest with a retriable row keeps the position and refreshes metadata",
                earliest(IsolationLevel::ReadCommitted, (74, -1)),
                ListOffsetsOutcome {
                    requests: vec![("leader", max, expected_list_offsets(1, -2))],
                    result: Ok(()),
                    metadata_requests: 1,
                    next_offset: Some(5),
                },
            ),
        ] {
            let outcome = run_list_offsets_exchange(exchange).await;
            check!(outcome == expected, "case {name}");
        }
    }

    /// Kafka's `AutoOffsetResetStrategy.timestamp` for `by_duration` is now
    /// minus the duration, and the reset sends `ListOffsets` with it (KIP-1106).
    #[tokio::test]
    async fn by_duration_resets_with_the_timestamp_of_now_minus_the_duration() {
        let before = unix_now_ms() - 3_600_000;
        let mut outcome = run_list_offsets_exchange(Exchange {
            isolation: IsolationLevel::ReadUncommitted,
            reset: Reset::ByOneHour,
            row: (0, 40),
            list_offsets_max_version: list_offsets_request::MAX_VERSION,
            primed: true,
        })
        .await;
        let after = unix_now_ms() - 3_600_000;
        // The wall clock moves during the exchange: check the range, then
        // compare the whole outcome with the timestamp of the start.
        let mut in_range = Vec::new();
        for (_, _, request) in &mut outcome.requests {
            for topic in &mut request.topics {
                for partition in &mut topic.partitions {
                    in_range.push((before..=after).contains(&partition.timestamp));
                    partition.timestamp = before;
                }
            }
        }

        assert2::assert!(
            (in_range, outcome)
                == (
                    vec![true],
                    ListOffsetsOutcome {
                        requests: vec![(
                            "leader",
                            list_offsets_request::MAX_VERSION,
                            expected_list_offsets(0, before),
                        )],
                        result: Ok(()),
                        metadata_requests: 1,
                        next_offset: Some(40),
                    }
                )
        );
    }

    #[test]
    fn reset_timestamp_follows_the_reset_policy() {
        let now = 1_700_000_000_000;
        let actual = [
            AutoOffsetReset::Latest,
            AutoOffsetReset::None,
            AutoOffsetReset::ByDuration(std::time::Duration::from_hours(1)),
            AutoOffsetReset::ByDuration(std::time::Duration::from_millis(1500)),
        ]
        .map(|policy| reset_timestamp(policy, now));
        assert2::assert!(actual == [-1, -1, now - 3_600_000, now - 1500]);
    }

    /// A new assignment without a committed offset has no leader in the
    /// positions map yet. Kafka's `OffsetFetcher.groupListOffsetRequests`
    /// routes with `metadata.currentLeader(tp)`, so the first `ListOffsets`
    /// of the first poll goes to the leader and not to the bootstrap broker.
    #[tokio::test]
    async fn first_poll_sends_list_offsets_to_the_leader_of_a_new_partition() {
        let outcome = run_list_offsets_exchange(Exchange {
            isolation: IsolationLevel::ReadCommitted,
            reset: Reset::Latest,
            row: (0, 40),
            list_offsets_max_version: list_offsets_request::MAX_VERSION,
            primed: false,
        })
        .await;

        assert2::assert!(
            outcome
                == ListOffsetsOutcome {
                    requests: vec![(
                        "leader",
                        list_offsets_request::MAX_VERSION,
                        expected_list_offsets(1, -1),
                    )],
                    result: Ok(()),
                    metadata_requests: 1,
                    next_offset: Some(40),
                }
        );
    }

    /// `isolation_level` exists from `ListOffsets` version 2. Kafka's
    /// `ListOffsetsRequest.Builder.forConsumer` sets the oldest allowed
    /// version to 2 for `READ_COMMITTED`, and to 1 (the oldest version) for
    /// `READ_UNCOMMITTED`. A `read_committed` consumer on a broker that
    /// supports only version 1 fails the poll with an unsupported version. It
    /// does not reset to the high watermark.
    #[tokio::test]
    async fn read_committed_list_offsets_needs_version_2() {
        let at = |isolation, list_offsets_max_version| Exchange {
            isolation,
            reset: Reset::Latest,
            row: (0, 40),
            list_offsets_max_version,
            primed: true,
        };
        // Below version 4 the request has no `current_leader_epoch`, and the
        // decoder gives the default.
        let without_epoch = |isolation_level| {
            let mut request = expected_list_offsets(isolation_level, -1);
            request.topics[0].partitions[0].current_leader_epoch =
                ListOffsetsPartition::default().current_leader_epoch;
            request
        };
        for (name, exchange, expected) in [
            (
                "read committed on a version 1 broker fails the poll",
                at(IsolationLevel::ReadCommitted, 1),
                ListOffsetsOutcome {
                    requests: vec![],
                    result: Err(PollFailure::IncompatibleVersion(
                        list_offsets_request::API_KEY,
                        0,
                        1,
                        2,
                        list_offsets_request::MAX_VERSION,
                    )),
                    metadata_requests: 1,
                    next_offset: Some(LATEST_SENTINEL),
                },
            ),
            (
                "read committed on a version 2 broker sends version 2",
                at(IsolationLevel::ReadCommitted, 2),
                ListOffsetsOutcome {
                    requests: vec![("leader", 2, without_epoch(1))],
                    result: Ok(()),
                    metadata_requests: 1,
                    next_offset: Some(40),
                },
            ),
            (
                "read uncommitted on a version 1 broker sends version 1",
                at(IsolationLevel::ReadUncommitted, 1),
                ListOffsetsOutcome {
                    requests: vec![("leader", 1, without_epoch(0))],
                    result: Ok(()),
                    metadata_requests: 1,
                    next_offset: Some(40),
                },
            ),
        ] {
            let outcome = run_list_offsets_exchange(exchange).await;
            check!(outcome == expected, "case {name}");
        }
    }

    /// Kafka's consumer gives each record the timestamp and the timestamp type
    /// of its record batch. A `LogAppendTime` batch gives every record the
    /// batch `max_timestamp`.
    #[tokio::test]
    async fn fetched_records_take_the_batch_timestamp_type() {
        let broker = metadata_counting_broker(Arc::default()).await;
        let consumer = consumer_on(&broker).await;
        let key: (String, i32) = ("orders".into(), 0);

        for case in &timestamp_cases::CASES {
            let part = PartitionData {
                partition_index: 0,
                records: Some(timestamp_cases::batch(case).into()),
                ..Default::default()
            };
            let offsets = HashMap::from([(key.clone(), timestamp_cases::BASE_OFFSET)]);

            let records: Vec<ConsumerRecord> = consumer
                .process_partition_records(&offsets, &key, "orders", &part)
                .map(|partition| partition.records.into_iter().collect())
                .unwrap_or_default();

            let expected: Vec<ConsumerRecord> = case
                .expected
                .iter()
                .zip(timestamp_cases::BASE_OFFSET..)
                .zip(timestamp_cases::VALUES)
                .map(
                    |(((timestamp, timestamp_type), offset), value)| ConsumerRecord {
                        topic: "orders".into(),
                        partition: 0,
                        offset,
                        leader_epoch: timestamp_cases::LEADER_EPOCH,
                        timestamp: *timestamp,
                        timestamp_type: *timestamp_type,
                        key: None,
                        value: Some(bytes::Bytes::from_static(value)),
                        headers: Vec::new(),
                    },
                )
                .collect();
            check!(records == expected, "case {}", case.name);
        }
    }
}

/// Record batch timestamp cases that the classic and the share consumer tests
/// share.
#[cfg(test)]
pub(crate) mod timestamp_cases {
    use bytes::Bytes;
    use krabka_protocol::records::{Attributes, Record, RecordBatch};

    use crate::consumer::TimestampType;

    pub(crate) const BASE_OFFSET: i64 = 5;
    pub(crate) const LEADER_EPOCH: i32 = 3;
    pub(crate) const VALUES: [&[u8]; 2] = [b"v0", b"v1"];

    /// One batch with two records and the timestamps Kafka gives them.
    pub(crate) struct TimestampCase {
        pub(crate) name: &'static str,
        pub(crate) attributes: Attributes,
        pub(crate) base_timestamp: i64,
        pub(crate) max_timestamp: i64,
        pub(crate) timestamp_deltas: [i64; 2],
        pub(crate) expected: [(i64, TimestampType); 2],
    }

    pub(crate) const CASES: [TimestampCase; 3] = [
        TimestampCase {
            name: "create time adds the delta to the base timestamp",
            attributes: Attributes(0),
            base_timestamp: 1000,
            max_timestamp: 1005,
            timestamp_deltas: [0, 5],
            expected: [
                (1000, TimestampType::CreateTime),
                (1005, TimestampType::CreateTime),
            ],
        },
        TimestampCase {
            name: "log append time uses the max timestamp",
            attributes: Attributes(Attributes::TIMESTAMP_TYPE_BIT),
            base_timestamp: 1000,
            max_timestamp: 9000,
            timestamp_deltas: [0, 5],
            expected: [
                (9000, TimestampType::LogAppendTime),
                (9000, TimestampType::LogAppendTime),
            ],
        },
        TimestampCase {
            name: "log append time in a transactional batch uses the max timestamp",
            attributes: Attributes(Attributes::TIMESTAMP_TYPE_BIT | Attributes::TRANSACTIONAL_BIT),
            base_timestamp: 1000,
            max_timestamp: 9000,
            timestamp_deltas: [0, 5],
            expected: [
                (9000, TimestampType::LogAppendTime),
                (9000, TimestampType::LogAppendTime),
            ],
        },
    ];

    pub(crate) fn batch(case: &TimestampCase) -> RecordBatch {
        RecordBatch {
            base_offset: BASE_OFFSET,
            partition_leader_epoch: LEADER_EPOCH,
            attributes: case.attributes,
            last_offset_delta: 1,
            base_timestamp: case.base_timestamp,
            max_timestamp: case.max_timestamp,
            records: case
                .timestamp_deltas
                .iter()
                .zip(0..)
                .zip(VALUES)
                .map(|((timestamp_delta, offset_delta), value)| Record {
                    attributes: 0,
                    timestamp_delta: *timestamp_delta,
                    offset_delta,
                    key: None,
                    value: Some(Bytes::from_static(value)),
                    headers: Vec::new(),
                })
                .collect(),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod fetch_path_tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU16, Ordering},
        },
        time::Duration,
    };

    use bytes::Buf as _;
    use krabka_client_core::{Client, MockBroker};
    use krabka_protocol::{
        Decode as _, Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            fetch_request,
            fetch_response::{FetchableTopicResponse, PartitionData},
            metadata_request,
            metadata_response::{
                MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
                MetadataResponseTopic,
            },
        },
        primitives::uuid::Uuid as WireUuid,
    };
    use krabka_units::{millis, secs};

    use super::*;

    const TOPIC_ID: WireUuid = WireUuid([7; 16]);

    /// What a mock broker does with a Fetch request.
    #[derive(Clone, Copy, Debug)]
    enum FetchAnswer {
        /// No response.
        Silent,
        /// A response with this top-level error code, session id and no
        /// partition data.
        Respond { error_code: i16, session_id: i32 },
        /// A response with one `orders-0` row without records.
        Partition {
            error_code: i16,
            preferred_read_replica: i32,
        },
    }

    /// The Fetch requests that the mock brokers received: `(broker id,
    /// request)`.
    type SentFetches = Arc<std::sync::Mutex<Vec<(i32, FetchRequest)>>>;

    fn encode(response: &impl Encode, version: i16) -> Vec<u8> {
        let mut buf = bytes::BytesMut::new();
        response.encode(&mut buf, version).expect("encode");
        buf.to_vec()
    }

    /// Start one mock broker per entry of `answers`, with node ids from 1.
    /// Metadata names broker `i + 1` as the leader of `orders-i`. Each broker
    /// answers Fetch v4 to v11 with its entry of `answers`.
    async fn start_brokers(answers: &[Vec<FetchAnswer>], sent: &SentFetches) -> Vec<MockBroker> {
        let ports: Arc<Vec<AtomicU16>> =
            Arc::new(answers.iter().map(|_| AtomicU16::new(0)).collect());
        let mut brokers = Vec::new();
        for (index, answers) in answers.iter().enumerate() {
            let node_id = i32::try_from(index).expect("index") + 1;
            let ports_in_mock = Arc::clone(&ports);
            let sent = Arc::clone(sent);
            let answers = std::sync::Mutex::new(answers.clone().into_iter());
            let last = std::sync::Mutex::new(FetchAnswer::Silent);
            let broker = MockBroker::start(move |api_key, version, _corr_id, mut body| {
                if api_key == api_versions_request::API_KEY {
                    let versions = ApiVersionsResponse {
                        api_keys: [
                            (api_versions_request::API_KEY, 0, 3),
                            (metadata_request::API_KEY, 0, 8),
                            (fetch_request::API_KEY, 4, 11),
                        ]
                        .into_iter()
                        .map(|(api_key, min_version, max_version)| ApiVersion {
                            api_key,
                            min_version,
                            max_version,
                            ..Default::default()
                        })
                        .collect(),
                        ..Default::default()
                    };
                    return Some(encode(&versions, 0));
                }
                if api_key == metadata_request::API_KEY {
                    let ports = &ports_in_mock;
                    let count = ports.len();
                    let metadata = MetadataResponse {
                        brokers: (0..count)
                            .map(|index| MetadataResponseBroker {
                                node_id: i32::try_from(index).expect("index") + 1,
                                host: "127.0.0.1".into(),
                                port: i32::from(ports[index].load(Ordering::SeqCst)),
                                ..Default::default()
                            })
                            .collect(),
                        topics: vec![MetadataResponseTopic {
                            name: Some("orders".into()),
                            topic_id: TOPIC_ID,
                            partitions: (0..count)
                                .map(|index| {
                                    let index = i32::try_from(index).expect("index");
                                    MetadataResponsePartition {
                                        partition_index: index,
                                        leader_id: index + 1,
                                        leader_epoch: 1,
                                        ..Default::default()
                                    }
                                })
                                .collect(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    return Some(encode(&metadata, version));
                }
                if api_key == fetch_request::API_KEY {
                    let client_id_len = usize::try_from(body.get_i16()).expect("client id");
                    body.advance(client_id_len);
                    let request = FetchRequest::decode(&mut body, version).expect("decode fetch");
                    sent.lock().expect("sent lock").push((node_id, request));
                    let mut last = last.lock().expect("last lock");
                    if let Some(answer) = answers.lock().expect("answers lock").next() {
                        *last = answer;
                    }
                    return match *last {
                        FetchAnswer::Silent => None,
                        FetchAnswer::Respond {
                            error_code,
                            session_id,
                        } => Some(encode(
                            &FetchResponse {
                                error_code,
                                session_id,
                                ..Default::default()
                            },
                            version,
                        )),
                        FetchAnswer::Partition {
                            error_code,
                            preferred_read_replica,
                        } => Some(encode(
                            &FetchResponse {
                                responses: vec![FetchableTopicResponse {
                                    topic: "orders".into(),
                                    partitions: vec![PartitionData {
                                        partition_index: 0,
                                        error_code,
                                        high_watermark: 5,
                                        last_stable_offset: 5,
                                        preferred_read_replica,
                                        ..Default::default()
                                    }],
                                    ..Default::default()
                                }],
                                ..Default::default()
                            },
                            version,
                        )),
                    };
                }
                None
            })
            .await;
            ports[index].store(broker.addr.port(), Ordering::SeqCst);
            brokers.push(broker);
        }
        brokers
    }

    /// A consumer that bootstraps from the first broker and owns one partition
    /// of `orders` per broker, each at offset 5.
    async fn consumer_on(brokers: &[MockBroker]) -> Consumer {
        let client = Client::builder()
            .bootstrap(brokers[0].addr.to_string())
            .request_timeout(secs(30))
            .build()
            .await
            .expect("client");
        let partitions: Vec<(String, i32)> = (0..brokers.len())
            .map(|index| ("orders".to_owned(), i32::try_from(index).expect("index")))
            .collect();
        let consumer = crate::poll::partition_error_tests::consumer_with_client(client);
        *consumer.assigned.lock().await = partitions.clone();
        *consumer.next_offsets.lock().await =
            partitions.iter().map(|key| (key.clone(), 5)).collect();
        consumer.commit_identity.lock().await.ownership_ids =
            partitions.into_iter().zip(1..).collect();
        consumer
    }

    fn stop(brokers: Vec<MockBroker>) {
        for broker in brokers {
            broker.stop();
        }
    }

    /// Kafka's `FetchRequest.max_wait_ms` is `fetch.max.wait.ms` (default
    /// 500), whatever the poll timeout.
    #[tokio::test]
    async fn fetch_waits_fetch_max_wait_and_not_the_poll_timeout() {
        let sent = SentFetches::default();
        let brokers = start_brokers(
            &[vec![FetchAnswer::Respond {
                error_code: 0,
                session_id: 0,
            }]],
            &sent,
        )
        .await;
        let mut consumer = consumer_on(&brokers).await;
        consumer.poll(secs(30)).await.expect("poll");
        let max_waits: Vec<i32> = sent
            .lock()
            .expect("sent lock")
            .iter()
            .map(|(_, request)| request.max_wait_ms)
            .collect();
        drop(consumer);
        stop(brokers);
        assert2::assert!(max_waits == vec![500]);
    }

    /// Kafka sends the Fetch of every leader at once, and `poll` returns by its
    /// timeout, also when no leader answers.
    #[tokio::test]
    async fn fetches_go_to_all_leaders_at_once_and_poll_returns_by_its_timeout() {
        let sent = SentFetches::default();
        let silent = vec![FetchAnswer::Silent];
        let brokers = start_brokers(&[silent.clone(), silent.clone(), silent], &sent).await;
        let mut consumer = consumer_on(&brokers).await;
        consumer.refresh_leader_epochs().await.expect("metadata");
        let started = tokio::time::Instant::now();
        let records = consumer.poll(millis(300)).await.expect("poll");
        let elapsed = started.elapsed();
        // Let the requests reach the brokers.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut leaders: Vec<i32> = sent
            .lock()
            .expect("sent lock")
            .iter()
            .map(|(node_id, _)| *node_id)
            .collect();
        leaders.sort_unstable();
        drop(consumer);
        stop(brokers);
        assert2::assert!(
            (
                records.len(),
                elapsed < Duration::from_millis(1500),
                leaders
            ) == (0, true, vec![1, 2, 3])
        );
    }

    /// A partition that waits for its assign callback gets no Fetch. Kafka's
    /// `SubscriptionState.isFetchable` is false while
    /// `pendingOnAssignedCallback` is set.
    #[tokio::test]
    async fn fetch_leaves_out_a_partition_that_waits_for_its_assign_callback() {
        let respond = vec![FetchAnswer::Respond {
            error_code: 0,
            session_id: 0,
        }];
        let sent = SentFetches::default();
        let brokers = start_brokers(&[respond.clone(), respond], &sent).await;
        let mut consumer = consumer_on(&brokers).await;
        consumer.refresh_leader_epochs().await.expect("metadata");
        let gate = crate::rebalance_listener::AssignedCallbackGate::new(
            &consumer.assigned_callback_pending,
            &[("orders".to_owned(), 0)],
        );
        consumer.poll(millis(200)).await.expect("poll");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let leaders = |sent: &SentFetches| {
            let mut leaders: Vec<i32> = sent
                .lock()
                .expect("sent lock")
                .drain(..)
                .map(|(node_id, _)| node_id)
                .collect();
            leaders.sort_unstable();
            leaders
        };
        let waiting = leaders(&sent);
        drop(gate);
        consumer.poll(millis(200)).await.expect("poll");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let after = leaders(&sent);
        drop(consumer);
        stop(brokers);
        assert2::assert!((waiting, after) == (vec![2], vec![1, 2]));
    }

    /// Kafka's `seek` sets the position of an assigned partition at once, and
    /// `SubscriptionState.assignedState` throws for a partition that the
    /// consumer does not own. The next Fetch uses the sought offset.
    #[tokio::test]
    async fn seek_moves_an_owned_position_and_rejects_an_unowned_partition() {
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, partition, expected_error, expected_offset) in [
            ("owned partition", 0, None, 2),
            (
                "unowned partition",
                9,
                Some("no current assignment for partition orders-9"),
                5,
            ),
        ] {
            let sent = SentFetches::default();
            let brokers = start_brokers(
                &[vec![FetchAnswer::Respond {
                    error_code: 0,
                    session_id: 0,
                }]],
                &sent,
            )
            .await;
            let mut consumer = consumer_on(&brokers).await;
            let error = consumer
                .seek("orders", partition, 2)
                .await
                .err()
                .map(|error| error.to_string());
            consumer.poll(millis(200)).await.expect("poll");
            let offsets: Vec<i64> = sent
                .lock()
                .expect("sent lock")
                .iter()
                .flat_map(|(_, request)| &request.topics)
                .flat_map(|topic| &topic.partitions)
                .map(|partition| partition.fetch_offset)
                .collect();
            drop(consumer);
            stop(brokers);
            actual.push((name, error, offsets));
            wanted.push((
                name,
                expected_error.map(str::to_owned),
                vec![expected_offset],
            ));
        }
        assert2::assert!(actual == wanted);
    }

    /// Kafka's `AbstractFetch.fetchablePartitions` leaves out a paused
    /// partition, so its leader gets no Fetch until `resume`.
    #[tokio::test]
    async fn fetch_leaves_out_a_paused_partition_until_resume() {
        let respond = vec![FetchAnswer::Respond {
            error_code: 0,
            session_id: 0,
        }];
        let sent = SentFetches::default();
        let brokers = start_brokers(&[respond.clone(), respond], &sent).await;
        let mut consumer = consumer_on(&brokers).await;
        consumer.refresh_leader_epochs().await.expect("metadata");
        let fetched = |sent: &SentFetches| {
            let mut partitions: Vec<(i32, i32)> = sent
                .lock()
                .expect("sent lock")
                .drain(..)
                .flat_map(|(node_id, request)| {
                    request
                        .topics
                        .into_iter()
                        .flat_map(|topic| topic.partitions)
                        .map(move |partition| (node_id, partition.partition))
                })
                .collect();
            partitions.sort_unstable();
            partitions
        };
        consumer
            .pause(&[("orders".to_owned(), 0)])
            .await
            .expect("pause");
        consumer.poll(millis(200)).await.expect("poll");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let paused = fetched(&sent);
        consumer
            .resume(&[("orders".to_owned(), 0)])
            .await
            .expect("resume");
        consumer.poll(millis(200)).await.expect("poll");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let resumed = fetched(&sent);
        let unowned = consumer
            .position("orders", 9)
            .await
            .map_err(|error| error.to_string());
        drop(consumer);
        stop(brokers);
        assert2::assert!(
            (paused, resumed, unowned)
                == (
                    vec![(2, 1)],
                    vec![(1, 0), (2, 1)],
                    Err("no current assignment for partition orders-9".to_owned())
                )
        );
    }

    /// Kafka's `pollForFetches` waits for the poll timeout when no partition
    /// can be fetched, for example when all are paused.
    #[tokio::test]
    async fn poll_waits_for_its_timeout_when_every_partition_is_paused() {
        let sent = SentFetches::default();
        let brokers = start_brokers(
            &[vec![FetchAnswer::Respond {
                error_code: 0,
                session_id: 0,
            }]],
            &sent,
        )
        .await;
        let mut consumer = consumer_on(&brokers).await;
        consumer
            .pause(&[("orders".to_owned(), 0)])
            .await
            .expect("pause");
        let started = tokio::time::Instant::now();
        let records = consumer.poll(millis(300)).await.expect("poll").len();
        let waited = started.elapsed() >= Duration::from_millis(250);
        let fetches = sent.lock().expect("sent lock").len();
        drop(consumer);
        stop(brokers);
        assert2::assert!((records, waited, fetches) == (0, true, 0));
    }

    /// Kafka's `wakeup` makes the blocked `poll`, or the next one, throw
    /// `WakeupException` once.
    #[tokio::test]
    async fn wakeup_ends_the_poll_that_waits_or_the_next_one() {
        let sent = SentFetches::default();
        let brokers = start_brokers(&[vec![FetchAnswer::Silent]], &sent).await;
        let mut consumer = consumer_on(&brokers).await;
        consumer.wakeup();
        let pending = consumer
            .poll(millis(100))
            .await
            .map_err(|error| error.to_string());
        let handle = consumer.wakeup_handle();
        let waker = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            handle.wakeup();
        });
        let started = tokio::time::Instant::now();
        let blocked = consumer
            .poll(secs(10))
            .await
            .map_err(|error| error.to_string());
        let woken_fast = started.elapsed() < Duration::from_secs(5);
        waker.await.expect("waker");
        let next = consumer
            .poll(millis(100))
            .await
            .map(|records| records.len());
        drop(consumer);
        stop(brokers);
        let wakeup = Err("the consumer was woken up".to_owned());
        assert2::assert!(
            (
                pending,
                blocked,
                woken_fast,
                next.map_err(|error| error.to_string())
            ) == (wakeup.clone(), wakeup, true, Ok(0))
        );
    }

    /// Kafka's `wakeup` ends a `poll` that waits for a metadata request of
    /// `updateFetchPositions`, as `ConsumerNetworkClient.poll` throws
    /// `WakeupException` while it waits.
    #[tokio::test]
    async fn wakeup_ends_a_poll_that_waits_for_metadata() {
        let silent = MockBroker::start(|api_key, _version, _corr_id, _body| {
            (api_key == api_versions_request::API_KEY).then(|| {
                encode(
                    &ApiVersionsResponse {
                        api_keys: [
                            (api_versions_request::API_KEY, 0, 3),
                            (metadata_request::API_KEY, 0, 8),
                        ]
                        .into_iter()
                        .map(|(api_key, min_version, max_version)| ApiVersion {
                            api_key,
                            min_version,
                            max_version,
                            ..Default::default()
                        })
                        .collect(),
                        ..Default::default()
                    },
                    0,
                )
            })
        })
        .await;
        let client = Client::builder()
            .bootstrap(silent.addr.to_string())
            .request_timeout(secs(30))
            .build()
            .await
            .expect("client");
        let mut consumer = crate::poll::partition_error_tests::consumer_with_client(client);
        let handle = consumer.wakeup_handle();
        let waker = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            handle.wakeup();
        });
        let polled = tokio::time::timeout(Duration::from_secs(5), consumer.poll(secs(10)))
            .await
            .map(|result| {
                result
                    .map(|records| records.len())
                    .map_err(|error| error.to_string())
            });
        waker.await.expect("waker");
        drop(consumer);
        silent.stop();
        assert2::assert!(polled == Ok(Err("the consumer was woken up".to_owned())));
    }

    /// Kafka's `pollForFetches` waits at most until the coordinator needs the
    /// application thread: a rebalance that starts while every partition is
    /// paused ends the wait.
    #[tokio::test]
    async fn a_rebalance_ends_the_wait_of_a_poll_with_nothing_to_fetch() {
        let sent = SentFetches::default();
        let brokers = start_brokers(
            &[vec![FetchAnswer::Respond {
                error_code: 0,
                session_id: 0,
            }]],
            &sent,
        )
        .await;
        let mut consumer = consumer_on(&brokers).await;
        consumer
            .pause(&[("orders".to_owned(), 0)])
            .await
            .expect("pause");
        let (pending, pending_rx) = tokio::sync::watch::channel(false);
        consumer.rebalance_pending = pending_rx;
        let rebalance = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            pending.send_replace(true);
            // Keep the sender, as the coordinator task does.
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        let started = tokio::time::Instant::now();
        consumer.poll(secs(5)).await.expect("poll");
        let ended_early = started.elapsed() < Duration::from_secs(2);
        rebalance.abort();
        drop(consumer);
        stop(brokers);
        assert2::assert!(ended_early);
    }

    /// Kafka's `ConsumerMetadata` names the current subscription, so the
    /// metadata of `poll` covers a topic that a pattern added in the
    /// coordinator task.
    #[tokio::test]
    async fn poll_names_the_topics_that_a_pattern_added() {
        let sent = SentFetches::default();
        let brokers = start_brokers(&[vec![FetchAnswer::Silent]], &sent).await;
        let consumer = consumer_on(&brokers).await;
        consumer.client.metadata_topics().set(["orders".to_owned()]);
        consumer.subscription.send_modify(|subscription| {
            subscription.pattern = Some(crate::TopicPattern::new(|_| true));
            subscription.topics = vec!["orders".to_owned(), "payments".to_owned()];
        });
        consumer.sync_metadata_topics();
        let names = consumer.client.metadata_topics().names();
        drop(consumer);
        stop(brokers);
        assert2::assert!(names == vec!["orders".to_owned(), "payments".to_owned()]);
    }

    /// A listener that records its calls. A consumer without a group runs no
    /// callback.
    struct CallRecorder {
        calls: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl crate::ConsumerRebalanceListener for CallRecorder {
        async fn on_partitions_revoked(
            &mut self,
            _consumer: &Consumer,
            partitions: &[(String, i32)],
        ) -> Result<(), crate::RebalanceListenerError> {
            self.calls
                .lock()
                .expect("calls lock")
                .push(format!("revoked {partitions:?}"));
            Ok(())
        }

        async fn on_partitions_assigned(
            &mut self,
            _consumer: &Consumer,
            partitions: &[(String, i32)],
        ) -> Result<(), crate::RebalanceListenerError> {
            self.calls
                .lock()
                .expect("calls lock")
                .push(format!("assigned {partitions:?}"));
            Ok(())
        }
    }

    /// Kafka's `updateFetchPositions` gives the committed offsets the timer of
    /// the `poll`, so an unresponsive coordinator does not hold a short poll
    /// for the whole `default.api.timeout.ms`.
    #[tokio::test]
    async fn a_short_poll_does_not_wait_for_the_committed_offsets() {
        let sent = SentFetches::default();
        let brokers = start_brokers(&[vec![FetchAnswer::Silent]], &sent).await;
        let mut consumer = consumer_on(&brokers).await;
        consumer
            .next_offsets
            .lock()
            .await
            .insert(("orders".to_owned(), 0), COMMITTED_SENTINEL);
        let started = tokio::time::Instant::now();
        let result = consumer
            .poll(millis(200))
            .await
            .map(|records| records.len());
        let elapsed = started.elapsed();
        drop(consumer);
        stop(brokers);
        assert2::assert!((result.is_err(), elapsed < Duration::from_secs(5)) == (true, true));
    }

    /// Kafka's consumer without `group.id` fetches the partitions of
    /// `assign` from the reset position, and throws `InvalidGroupIdException`
    /// for the group calls.
    #[tokio::test]
    async fn a_consumer_without_a_group_fetches_its_manual_assignment() {
        let sent = SentFetches::default();
        let brokers = start_brokers(
            &[vec![FetchAnswer::Respond {
                error_code: 0,
                session_id: 0,
            }]],
            &sent,
        )
        .await;
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut consumer = Consumer::builder()
            .bootstrap(brokers[0].addr.to_string())
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .rebalance_listener(Box::new(CallRecorder {
                calls: std::sync::Arc::clone(&calls),
            }))
            .build()
            .await
            .expect("build");
        let before_assign = consumer
            .poll(millis(20))
            .await
            .map(|records| records.len())
            .map_err(|error| error.to_string());
        consumer
            .assign(&[("orders".to_owned(), 0)])
            .await
            .expect("assign");
        consumer.poll(millis(300)).await.expect("poll");
        let fetched: Vec<(i32, String, i32, i64)> = sent
            .lock()
            .expect("sent lock")
            .iter()
            .flat_map(|(node_id, request)| {
                request.topics.iter().flat_map(move |topic| {
                    topic.partitions.iter().map(move |partition| {
                        (
                            *node_id,
                            topic.topic.clone(),
                            partition.partition,
                            partition.fetch_offset,
                        )
                    })
                })
            })
            .collect();
        let group_calls = (
            consumer
                .commit_sync()
                .await
                .map_err(|error| error.to_string()),
            consumer
                .subscribe(["orders"])
                .await
                .map_err(|error| error.to_string()),
            consumer
                .enforce_rebalance(None)
                .map_err(|error| error.to_string()),
        );
        consumer.unsubscribe().await.expect("unsubscribe");
        let after_unsubscribe = (
            consumer.assignment().await,
            consumer.next_offsets.lock().await.len(),
        );
        let listener_calls = calls.lock().expect("calls lock").clone();
        consumer.close().await.expect("close");
        stop(brokers);
        let invalid_group = Err(ConsumerError::InvalidGroupId.to_string());
        assert2::assert!(
            (
                before_assign,
                fetched.first().cloned(),
                group_calls,
                after_unsubscribe,
                listener_calls
            ) == (
                Err("not subscribed to any topic".to_owned()),
                Some((1, "orders".to_owned(), 0, 0)),
                (invalid_group.clone(), invalid_group.clone(), invalid_group),
                (Vec::new(), 0),
                Vec::<String>::new()
            )
        );
    }

    /// One step of a read replica case.
    #[derive(Clone, Copy, Debug)]
    enum ReplicaStep {
        Poll,
        /// Wait longer than the metadata max age of the case.
        WaitForExpiry,
    }

    /// KIP-392: Kafka's Fetch carries `client.rack`, and a
    /// `preferred_read_replica` in a response sends the next Fetch of the
    /// partition to that replica (`AbstractFetch.selectReadReplica`) until it
    /// expires after `metadata.max.age.ms` or the replica answers an error
    /// (`FetchCollector.handleInitializeErrors`).
    #[tokio::test]
    async fn fetch_sends_the_rack_and_follows_the_preferred_read_replica() {
        use FetchAnswer::Partition;
        use ReplicaStep::{Poll, WaitForExpiry};
        let records = Partition {
            error_code: 0,
            preferred_read_replica: -1,
        };
        let prefer_2 = Partition {
            error_code: 0,
            preferred_read_replica: 2,
        };
        let not_leader = Partition {
            error_code: 6,
            preferred_read_replica: -1,
        };
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, rack, leader, replica, steps, expected) in [
            (
                "no rack",
                None,
                vec![records],
                vec![records],
                vec![Poll],
                vec![(1, String::new())],
            ),
            (
                "rack",
                Some("az-1"),
                vec![records],
                vec![records],
                vec![Poll],
                vec![(1, "az-1".to_owned())],
            ),
            (
                "preferred read replica",
                Some("az-1"),
                vec![prefer_2],
                vec![records],
                vec![Poll, Poll],
                vec![(1, "az-1".to_owned()), (2, "az-1".to_owned())],
            ),
            (
                "the replica answers not leader or follower",
                Some("az-1"),
                vec![prefer_2, records],
                vec![not_leader],
                vec![Poll, Poll, Poll],
                vec![
                    (1, "az-1".to_owned()),
                    (2, "az-1".to_owned()),
                    (1, "az-1".to_owned()),
                ],
            ),
            (
                "the preference expires",
                Some("az-1"),
                vec![prefer_2, records],
                vec![records],
                vec![Poll, Poll, WaitForExpiry, Poll],
                vec![
                    (1, "az-1".to_owned()),
                    (2, "az-1".to_owned()),
                    (1, "az-1".to_owned()),
                ],
            ),
        ] {
            let sent = SentFetches::default();
            let brokers = start_brokers(&[leader, replica], &sent).await;
            let mut consumer = consumer_on(&brokers).await;
            *consumer.assigned.lock().await = vec![("orders".to_owned(), 0)];
            consumer.client_rack = rack.map(str::to_owned);
            consumer.metadata_max_age = millis(300);
            for step in steps {
                match step {
                    Poll => {
                        let before = sent.lock().expect("sent lock").len();
                        consumer.poll(secs(2)).await.expect("poll");
                        // The fetch sessions of the two brokers are independent;
                        // wait until the Fetch of this poll is recorded.
                        let _ = tokio::time::timeout(Duration::from_secs(2), async {
                            while sent.lock().expect("sent lock").len() == before {
                                tokio::time::sleep(Duration::from_millis(5)).await;
                            }
                        })
                        .await;
                    }
                    WaitForExpiry => tokio::time::sleep(Duration::from_millis(400)).await,
                }
            }
            let requests: Vec<(i32, String)> = sent
                .lock()
                .expect("sent lock")
                .iter()
                .map(|(node_id, request)| (*node_id, request.rack_id.clone()))
                .collect();
            drop(consumer);
            stop(brokers);
            actual.push((name, requests));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// One Fetch request of a session case: `(session_id, session_epoch,
    /// fetched partitions, forgotten partitions)`.
    type SessionFields = (i32, i32, Vec<i32>, Vec<i32>);

    fn session_fields(request: &FetchRequest) -> SessionFields {
        (
            request.session_id,
            request.session_epoch,
            request
                .topics
                .iter()
                .flat_map(|topic| topic.partitions.iter().map(|p| p.partition))
                .collect(),
            request
                .forgotten_topics_data
                .iter()
                .flat_map(|topic| topic.partitions.iter().copied())
                .collect(),
        )
    }

    /// Kafka's `FetchSessionHandler`: the first Fetch is full and creates a
    /// session, the next one is incremental and names no unchanged partition.
    /// `FETCH_SESSION_ID_NOT_FOUND` starts a new session with a full Fetch.
    #[tokio::test]
    async fn fetch_sessions_send_incremental_requests_and_recover_a_lost_session() {
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, answers, expected) in [
            (
                "session",
                vec![
                    FetchAnswer::Respond {
                        error_code: 0,
                        session_id: 77,
                    },
                    FetchAnswer::Respond {
                        error_code: 0,
                        session_id: 77,
                    },
                ],
                vec![(0, 0, vec![0], vec![]), (77, 1, vec![], vec![])],
            ),
            (
                "session lost",
                vec![
                    FetchAnswer::Respond {
                        error_code: 0,
                        session_id: 77,
                    },
                    FetchAnswer::Respond {
                        error_code: 70,
                        session_id: 0,
                    },
                    FetchAnswer::Respond {
                        error_code: 0,
                        session_id: 78,
                    },
                ],
                vec![
                    (0, 0, vec![0], vec![]),
                    (77, 1, vec![], vec![]),
                    (0, 0, vec![0], vec![]),
                ],
            ),
        ] {
            let sent = SentFetches::default();
            let rounds = answers.len();
            let brokers = start_brokers(&[answers], &sent).await;
            let mut consumer = consumer_on(&brokers).await;
            for _ in 0..rounds {
                consumer.poll(secs(5)).await.expect("poll");
            }
            let requests: Vec<SessionFields> = sent
                .lock()
                .expect("sent lock")
                .iter()
                .map(|(_, request)| session_fields(request))
                .collect();
            drop(consumer);
            stop(brokers);
            actual.push((name, requests));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// A Fetch result counts as soon as the task sent it, also while the task
    /// has not ended yet, so `poll` does not sleep until its deadline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fetch_result_wakes_poll_before_the_task_ends() {
        let sent = SentFetches::default();
        let brokers = start_brokers(&[vec![FetchAnswer::Silent]], &sent).await;
        let mut consumer = consumer_on(&brokers).await;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let completed = Arc::clone(&consumer.fetches.completed);
        consumer.fetches.in_flight.insert(
            -1,
            InFlightFetch {
                handle: tokio::spawn(async move {
                    let _ = tx.send(Ok(FetchResponse::default()));
                    completed.notify_one();
                    // The task ends late.
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }),
                result: rx,
                requested: HashMap::new(),
                from_replica: std::collections::HashSet::new(),
            },
        );
        let started = tokio::time::Instant::now();
        consumer
            .wait_for_fetches(started + Duration::from_secs(2))
            .await;
        let elapsed = started.elapsed();
        drop(consumer);
        stop(brokers);
        assert2::assert!(elapsed < Duration::from_millis(500));
    }

    /// A preferred read replica that answers `OFFSET_OUT_OF_RANGE` gives no
    /// offset reset, also when the preference expired while the Fetch was in
    /// flight: the request decides.
    #[tokio::test]
    async fn out_of_range_from_a_replica_request_gives_no_reset() {
        let sent = SentFetches::default();
        let brokers = start_brokers(&[vec![FetchAnswer::Silent]], &sent).await;
        let mut consumer = consumer_on(&brokers).await;
        consumer.auto_offset_reset = AutoOffsetReset::Latest;
        let key = ("orders".to_owned(), 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        consumer.fetches.in_flight.insert(
            2,
            InFlightFetch {
                handle: tokio::spawn(async {}),
                result: rx,
                requested: HashMap::from([(key.clone(), 5)]),
                from_replica: std::collections::HashSet::from([key.clone()]),
            },
        );
        tx.send(Ok(FetchResponse {
            responses: vec![FetchableTopicResponse {
                topic: "orders".into(),
                partitions: vec![PartitionData {
                    partition_index: 0,
                    error_code: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }))
        .expect("send response");
        let topic_ids = consumer.topic_ids.lock().await.clone();
        let responses = consumer
            .take_completed_fetches(&topic_ids)
            .await
            .expect("take");
        consumer
            .process_fetch_responses(responses, &topic_ids)
            .await
            .expect("process");
        let position = consumer.next_offsets.lock().await.get(&key).copied();
        drop(consumer);
        stop(brokers);
        assert2::assert!(position == Some(5));
    }

    /// Kafka's `FetchCollector` discards the data of a partition whose position
    /// changed after the Fetch went out, for example after a seek.
    #[tokio::test]
    async fn a_fetch_response_for_a_moved_position_gives_no_records() {
        let sent = SentFetches::default();
        let brokers = start_brokers(&[vec![FetchAnswer::Silent]], &sent).await;
        let mut consumer = consumer_on(&brokers).await;
        let (tx, rx) = tokio::sync::oneshot::channel();
        consumer.fetches.in_flight.insert(
            -1,
            InFlightFetch {
                handle: tokio::spawn(async {}),
                result: rx,
                requested: HashMap::from([(("orders".to_owned(), 0), 5)]),
                from_replica: std::collections::HashSet::new(),
            },
        );
        // A seek moves the position while the Fetch is in flight.
        consumer
            .next_offsets
            .lock()
            .await
            .insert(("orders".to_owned(), 0), 40);
        let record = krabka_protocol::records::RecordBatch {
            base_offset: 5,
            last_offset_delta: 0,
            records: vec![krabka_protocol::records::Record {
                offset_delta: 0,
                value: Some(bytes::Bytes::from_static(b"old")),
                ..Default::default()
            }],
            ..Default::default()
        };
        tx.send(Ok(FetchResponse {
            responses: vec![FetchableTopicResponse {
                topic: "orders".into(),
                partitions: vec![PartitionData {
                    partition_index: 0,
                    high_watermark: 6,
                    records: Some(krabka_protocol::records::RecordsPayload::V2(vec![record])),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }))
        .expect("send response");
        consumer
            .wait_for_fetches(tokio::time::Instant::now() + Duration::from_secs(5))
            .await;
        let topic_ids = consumer.topic_ids.lock().await.clone();
        let responses = consumer
            .take_completed_fetches(&topic_ids)
            .await
            .expect("take");
        consumer
            .process_fetch_responses(responses, &topic_ids)
            .await
            .expect("process");
        let records = consumer.drain_fetch_buffer().await;
        let position = consumer
            .next_offsets
            .lock()
            .await
            .get(&("orders".to_owned(), 0))
            .copied();
        drop(consumer);
        stop(brokers);
        assert2::assert!((records.len(), position) == (0, Some(40)));
    }
}
