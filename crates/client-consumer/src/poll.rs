//! `Consumer::poll` issues one `Fetch` that covers every assigned partition,
//! advances next-offsets, and returns the decoded records.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::BufMut;
use krabka_ids::LeaderEpoch;
use krabka_protocol::{
    Encode, ProtocolError, ProtocolRequest,
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        list_offsets_request::{self, ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        list_offsets_response::ListOffsetsResponse,
    },
};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    mebibytes,
};

use crate::{
    builder::{AutoOffsetReset, IsolationLevel},
    consumer::{Consumer, ConsumerRecord, Header},
    error::ConsumerError,
    fetch_buffer::BufferedPartition,
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

fn is_transient_transport_error(e: &krabka_client_core::ClientError) -> bool {
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
        3 /* UNKNOWN_TOPIC_OR_PARTITION */ | 100 /* UNKNOWN_TOPIC_ID */ => {
            FetchPartitionAction::RefreshMetadata
        }
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

fn record_timestamp(base_timestamp: i64, timestamp_delta: i64) -> i64 {
    base_timestamp + timestamp_delta
}

fn build_fetch_topic(
    name: String,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
    partitions: Vec<FetchSpec>,
    partition_max: ByteSize,
) -> FetchTopic {
    FetchTopic {
        topic: name,
        topic_id,
        partitions: partitions
            .into_iter()
            .map(
                |(p, off, leader_epoch, last_fetched_epoch)| FetchPartition {
                    partition: p,
                    fetch_offset: off,
                    // Unwrap the leader epochs to raw wire `int32` at the
                    // FetchRequest encode boundary.
                    current_leader_epoch: leader_epoch.get(),
                    last_fetched_epoch: last_fetched_epoch.get(),
                    partition_max_bytes: partition_max.bytes_i32(),
                    ..Default::default()
                },
            )
            .collect(),
        ..Default::default()
    }
}

fn build_fetch_request(
    timeout_ms: i32,
    isolation_level: IsolationLevel,
    min: ByteSize,
    max: ByteSize,
    topics: Vec<FetchTopic>,
) -> FetchRequest {
    FetchRequest {
        max_wait_ms: timeout_ms,
        min_bytes: min.bytes_i32(),
        max_bytes: max.bytes_i32(),
        isolation_level: isolation_level.wire(),
        topics,
        ..Default::default()
    }
}

/// `ListOffsets` timestamp that asks for the log end offset.
const LATEST_TIMESTAMP: i64 = -1;
/// `ListOffsets` timestamp that asks for the log start offset.
const EARLIEST_TIMESTAMP: i64 = -2;

/// Placeholder for "fetch from the log end", resolved before the next Fetch.
const LATEST_SENTINEL: i64 = i64::MAX;

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
struct ReadCommittedListOffsets(ListOffsetsRequest);

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
        if !self.prepare_poll().await? {
            return Ok(Vec::new());
        }
        if !self.wait_for_rebalance(deadline).await {
            return Ok(Vec::new());
        }

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
            tokio::time::sleep_until(deadline).await;
            return Ok(Vec::new());
        }

        let by_leader = self.group_fetches(&assigned).await;
        tracing::Span::current().record("leaders", by_leader.len());
        let topic_ids = self.topic_ids.lock().await.clone();
        let remaining =
            Time::from_std(deadline.saturating_duration_since(tokio::time::Instant::now()));
        let responses = self.send_fetches(remaining, by_leader, &topic_ids).await?;

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
    async fn wait_for_rebalance(&mut self, deadline: tokio::time::Instant) -> bool {
        if !*self.rebalance_pending.borrow() {
            return true;
        }
        let eager = self.assignor.rebalance_protocol() == crate::assignor::RebalanceProtocol::Eager;
        if !eager && !self.assigned.lock().await.is_empty() {
            return true;
        }
        let joined = tokio::time::timeout_at(
            deadline,
            self.rebalance_pending.wait_for(|pending| !*pending),
        )
        .await;
        // A closed channel means that the coordinator task stopped. `poll`
        // then continues with the assignment that it has.
        joined.is_ok()
    }

    /// Return up to `max_poll_records` buffered records, and move the consumed
    /// positions past them.
    async fn drain_fetch_buffer(&mut self) -> Vec<ConsumerRecord> {
        let assigned: std::collections::HashSet<(String, i32)> =
            self.assigned.lock().await.iter().cloned().collect();
        let mut offsets = self.next_offsets.lock().await;
        let mut positions = self.positions.lock().await;
        self.fetch_buffer.drain(
            self.max_poll_records,
            &assigned,
            &mut offsets,
            &mut positions,
        )
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
                match classify_fetch_partition_error(part.error_code) {
                    FetchPartitionAction::Records => {}
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
                            AutoOffsetReset::Latest => {
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

    async fn send_fetches(
        &self,
        timeout: Time,
        by_leader: FetchByLeader,
        topic_ids: &HashMap<String, krabka_protocol::primitives::uuid::Uuid>,
    ) -> Result<Vec<krabka_protocol::owned::fetch_response::FetchResponse>, ConsumerError> {
        // Truncate rather than round: `max_wait_ms` is a wire field, and a
        // fractional millisecond rounded up would ask the broker to hold the
        // Fetch open past the caller's budget. A negative budget — a deadline
        // already passed — means "do not wait", as `Duration` did before.
        let timeout_ms = i32::try_from(timeout.millis_i64_trunc().max(0)).unwrap_or(i32::MAX);

        // Issue one Fetch per leader. All guards are released; we collect every
        // response before re-locking to process them. Sent sequentially so a
        // single parked leader can't starve the others' deadlines beyond the
        // per-request timeout (and to keep the borrow on `self.client` simple).
        let mut responses = Vec::with_capacity(by_leader.len());
        for (leader, by_topic) in by_leader {
            let topics: Vec<FetchTopic> = by_topic
                .into_iter()
                .map(|(name, plist)| {
                    let topic_id = topic_ids.get(&name).copied().unwrap_or_default();
                    build_fetch_topic(name, topic_id, plist, self.fetch_partition_max)
                })
                .collect();
            let req = build_fetch_request(
                timeout_ms,
                self.isolation_level,
                self.fetch_min,
                self.fetch_max,
                topics,
            );
            let resp = if should_use_bootstrap_leader(leader) {
                match self.client.send(req).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        if is_transient_transport_error(&e) {
                            self.client.reconnect_bootstrap().await;
                            continue;
                        }
                        return Err(e.into());
                    }
                }
            } else {
                match self.client.broker(leader).send(req).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        if is_transient_transport_error(&e) {
                            self.client.evict_broker(leader);
                            continue;
                        }
                        return Err(e.into());
                    }
                }
            };
            responses.push(resp);
        }

        Ok(responses)
    }

    async fn group_fetches(&self, assigned: &[(String, i32)]) -> FetchByLeader {
        let mut grouped: FetchByLeader = HashMap::new();
        {
            let offsets = self.next_offsets.lock().await;
            let positions = self.positions.lock().await;
            for (t, p) in assigned {
                // Skip partitions still awaiting validation — they must not be
                // fetched until proven consistent.
                if positions
                    .get(&(t.clone(), *p))
                    .is_some_and(|x| x.awaiting_validation)
                {
                    continue;
                }
                let next = offsets.get(&(t.clone(), *p)).copied().unwrap_or(0);
                // A `LATEST_SENTINEL` is a reset that `ListOffsets` did not
                // resolve yet. Kafka does not fetch a partition that awaits a
                // reset, and the sentinel is no offset to fetch from.
                if next == LATEST_SENTINEL {
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
                let leader =
                    fetch_leader_id(pos.leader_id, self.client.knows_broker(pos.leader_id));
                grouped
                    .entry(leader)
                    .or_default()
                    .entry(t.clone())
                    .or_default()
                    .push((*p, next, pos.leader_epoch, pos.offset_epoch));
            }
        }

        grouped
    }

    async fn prepare_poll(&mut self) -> Result<bool, ConsumerError> {
        // Kafka's consumer raises a fatal `OffsetFetch` error from `poll()`.
        // A rejoin in the coordinator task leaves such an error here.
        if let Some(error) = crate::coordinator::take_poll_error(&self.poll_error) {
            return Err(error);
        }
        self.apply_pending_seeks().await;
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
        if let Err(error) = self.refresh_leader_epochs().await {
            if is_transient_poll_error(&error) {
                self.client.reconnect_bootstrap().await;
                return Ok(false);
            }
            return Err(error);
        }
        if let Err(error) = self.resolve_latest_sentinels().await {
            if is_transient_poll_error(&error) {
                self.client.reconnect_bootstrap().await;
                return Ok(false);
            }
            return Err(error);
        }
        let truncated = match self.validate_positions().await {
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
                records.push_back(ConsumerRecord {
                    topic: topic_name.to_string(),
                    partition: part.partition_index,
                    offset,
                    leader_epoch: batch.partition_leader_epoch,
                    timestamp: record_timestamp(batch.base_timestamp, r.timestamp_delta),
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
    /// Replace any `LATEST_SENTINEL` in `next_offsets` with the real log-end
    /// offset from `ListOffsets(timestamp=-1)`.
    ///
    /// `auto_offset_reset = Latest` plants those sentinels at build time, and
    /// the `OFFSET_OUT_OF_RANGE` arm of the poll loop plants them again. This
    /// runs in `prepare_poll`, after the metadata refresh. A partition whose
    /// row has an error keeps its sentinel. `group_fetches` skips it, and the
    /// metadata refresh at the start of the next `prepare_poll` lets that
    /// poll try again.
    ///
    /// # Errors
    ///
    /// Returns `TopicAuthorizationFailed` when the broker does not authorize
    /// a topic, and a transport error that is not transient.
    #[tracing::instrument(
        name = "consumer.resolve_latest_sentinels",
        level = "debug",
        skip_all,
        fields(group_id = %self.group_id, sentinels = tracing::field::Empty),
        err
    )]
    async fn resolve_latest_sentinels(&self) -> Result<(), ConsumerError> {
        let sentinels = keys_at(&*self.next_offsets.lock().await, LATEST_SENTINEL);
        if sentinels.is_empty() {
            return Ok(());
        }
        tracing::Span::current().record("sentinels", sentinels.len());
        let result = self.list_offsets(&sentinels, LATEST_TIMESTAMP).await?;
        {
            // A seek or a rebalance can change a position while the request
            // is in flight. Replace only a sentinel that is still there.
            let mut offsets = self.next_offsets.lock().await;
            for (key, offset) in &result.offsets {
                if let Some(next) = offsets.get_mut(key)
                    && *next == LATEST_SENTINEL
                {
                    *next = *offset;
                }
            }
        }
        result.authorization()
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
    use krabka_units::kibibytes;

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
        check!(record_timestamp(1000, 33) == 1033);
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

    #[test]
    fn build_fetch_request_preserves_topic_partition_and_limits() {
        let topic = build_fetch_topic(
            "topic-a".into(),
            id(7),
            vec![(2, 42, LeaderEpoch(5), LeaderEpoch(4))],
            kibibytes(128),
        );
        assert2::assert!(
            topic
                == FetchTopic {
                    topic: "topic-a".into(),
                    topic_id: id(7),
                    partitions: vec![FetchPartition {
                        partition: 2,
                        current_leader_epoch: 5,
                        fetch_offset: 42,
                        last_fetched_epoch: 4,
                        log_start_offset: -1,
                        partition_max_bytes: 128 * 1024,
                        replica_directory_id: WireUuid::default(),
                        high_watermark: i64::MAX,
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }
        );

        let req = build_fetch_request(
            123,
            IsolationLevel::ReadCommitted,
            krabka_units::bytes(7),
            mebibytes(2),
            vec![topic.clone()],
        );
        assert2::assert!(
            req == FetchRequest {
                replica_id: -1,
                max_wait_ms: 123,
                min_bytes: 7,
                max_bytes: 2 * 1024 * 1024,
                isolation_level: 1, // read_committed wire value
                session_id: 0,
                session_epoch: -1,
                topics: vec![topic],
                forgotten_topics_data: Vec::new(),
                rack_id: String::new(),
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
mod partition_error_tests {
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
        Consumer {
            client,
            group_id: "group-a".into(),
            coordinator_id: Arc::new(AtomicI32::new(0)),
            retry_policy: ConsumerRetryPolicy::default().into(),
            member_id: "member-a".into(),
            commit_identity: Arc::new(Mutex::new(CommitIdentity {
                generation: 1,
                member_id: "member-a".into(),
                ownership_ids: HashMap::from([(("orders".into(), 0), 1)]),
                rejoin_on_poll: false,
            })),
            commit_serialization: Arc::new(Mutex::new(())),
            commit_async_state: Arc::new(AtomicU8::new(0)),
            group_instance_id: None,
            current_generation: Arc::new(AtomicI32::new(1)),
            subscribed_topics: vec!["orders".into()],
            assigned: Arc::new(Mutex::new(vec![("orders".into(), 0)])),
            assignment_changed: Arc::new(Notify::new()),
            next_offsets: Arc::new(Mutex::new(HashMap::from([(("orders".into(), 0), 5)]))),
            end_offsets: Arc::new(Mutex::new(HashMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
            pending_seeks: Arc::new(Mutex::new(HashMap::new())),
            topic_ids: Arc::new(Mutex::new(HashMap::from([("orders".into(), TOPIC_ID)]))),
            session_timeout: secs(45),
            heartbeat_interval: secs(3),
            assignor: Assignor::Range,
            coordinator_shutdown: CancellationToken::new(),
            coordinator_handle: None,
            isolation_level: IsolationLevel::ReadUncommitted,
            fetch_min: krabka_client_core::DEFAULT_FETCH_MIN,
            fetch_max: DEFAULT_FETCH_MAX,
            fetch_partition_max: DEFAULT_FETCH_PARTITION_MAX,
            auto_offset_reset: AutoOffsetReset::Latest,
            poll_error: crate::coordinator::PollErrorSlot::default(),
            auto_commit: None,
            poll_signal: crate::coordinator::PollSignal::default(),
            rebalance_pending: tokio::sync::watch::channel(false).1,
            max_poll_records: crate::consumer::DEFAULT_CONSUMER_MAX_POLL_RECORDS,
            fetch_buffer: crate::fetch_buffer::FetchBuffer::default(),
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

        let first = consumer.prepare_poll().await.map_err(|error| match error {
            ConsumerError::TopicAuthorizationFailed(topics) => Some(topics),
            _ => None,
        });
        let second = consumer
            .prepare_poll()
            .await
            .map_err(|_| None::<std::collections::BTreeSet<String>>);

        broker.stop();
        assert2::assert!((first, second) == (Err(Some(topics)), Ok(true)));
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
            consumer.assignor = assignor;
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
                .await;
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
        /// `auto.offset.reset=earliest`: a Fetch row with
        /// `OFFSET_OUT_OF_RANGE`.
        EarliestOutOfRange,
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
                consumer.prepare_poll().await.map(|_| ())
            }
            Reset::EarliestOutOfRange => {
                consumer.auto_offset_reset = AutoOffsetReset::Earliest;
                let topic_ids = consumer.topic_ids.lock().await.clone();
                consumer
                    .process_fetch_responses(vec![fetch_response(1)], &topic_ids)
                    .await
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
}
