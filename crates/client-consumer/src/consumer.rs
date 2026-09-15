//! `Consumer`, the public lifecycle handle. Build it with
//! [`Consumer::builder`].
//!
//! The consumer is subscribe-only and has no `assign()`. Use
//! `krabka-client-core` directly for manual partition consumption.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI32, AtomicU8, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use krabka_client_core::{Client, FetchMinBytes};
use krabka_protocol::{
    owned::{
        join_group_response::JoinGroupResponse,
        sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
        sync_group_response::SyncGroupResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, StdDurationExt as _, TimeExt as _},
    millis, minutes, secs,
};
use refined_type::rule::{GreaterI32, GreaterI64, MinMaxI64, MinMaxU128};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    assignor::Assignor,
    builder::{
        AutoOffsetReset, IsolationLevel, decode_assignment, decode_subscription, encode_assignment,
        encode_subscription,
    },
    coordinator::{
        CoordinatorRetryPolicy, CoordinatorState, find_coordinator, with_coordinator_refind,
    },
    error::ConsumerError,
    group_metadata::ConsumerGroupMetadata,
};

/// Subscribe-style consumer handle. Construct via [`Consumer::builder`].
#[allow(dead_code)] // `session_timeout` / `heartbeat_interval`
// are captured for diagnostics; the live values are owned
// by the coordinator task post-start.
pub struct Consumer {
    pub(crate) client: Client,
    pub(crate) group_id: String,
    /// Node id of the group's coordinator broker. `FindCoordinator` discovers it
    /// at build time, and the coordinator task keeps it current through the
    /// shared `Arc<AtomicI32>`. The commit path (`commit.rs`) reads it to route
    /// `OffsetCommit` to the coordinator over this data-path client.
    pub(crate) coordinator_id: Arc<AtomicI32>,
    pub(crate) retry_policy: CoordinatorRetryPolicy,
    /// The live member id. The coordinator task publishes each change, for
    /// example the new id of a join from scratch.
    pub(crate) member_id: tokio::sync::watch::Receiver<String>,
    pub(crate) commit_identity: Arc<Mutex<CommitIdentity>>,
    pub(crate) commit_serialization: Arc<Mutex<()>>,
    pub(crate) commit_async_state: Arc<AtomicU8>,
    pub(crate) group_instance_id: Option<String>,
    /// The current group generation exposed by [`Consumer::generation`].
    /// Commit RPCs use the generation atomically paired with membership and
    /// partition ownership in `commit_identity`.
    pub(crate) current_generation: Arc<AtomicI32>,
    pub(crate) subscribed_topics: Vec<String>,
    /// Current assigned partitions: `(topic, partition_index)`.
    pub(crate) assigned: Arc<Mutex<Vec<(String, i32)>>>,
    /// Wakes selected-offset commits after assignment publication.
    pub(crate) assignment_changed: Arc<Notify>,
    /// Next offset to fetch per partition.
    pub(crate) next_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    /// Last broker-reported readable end offset per partition.
    pub(crate) end_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    /// KIP-320 per-partition leader-epoch metadata, keyed like `next_offsets`.
    pub(crate) positions: Arc<Mutex<HashMap<(String, i32), crate::position::PartitionPosition>>>,
    /// Pending [`seek`](Consumer::seek) targets: `(topic, partition) -> next
    /// offset to fetch`. `poll` applies them at its top once the partition is
    /// assigned, *after* the coordinator's post-assignment prime. The prime
    /// therefore does not overwrite a seek requested before assignment. See
    /// `seek.rs`. The map is empty in steady state.
    pub(crate) pending_seeks: Arc<Mutex<HashMap<(String, i32), i64>>>,
    /// Topic UUIDs resolved at build time. Fetch v ≥ 13 needs them, because it
    /// carries `topic_id` instead of the topic name.
    pub(crate) topic_ids: Arc<Mutex<HashMap<String, WireUuid>>>,
    pub(crate) session_timeout: Time,
    pub(crate) heartbeat_interval: Time,
    #[allow(dead_code)]
    pub(crate) assignor: Assignor,
    pub(crate) coordinator_shutdown: CancellationToken,
    pub(crate) coordinator_handle: Option<JoinHandle<()>>,
    /// Controls which records `poll` returns.
    pub(crate) isolation_level: IsolationLevel,
    pub(crate) fetch_min: ByteSize,
    pub(crate) fetch_max: ByteSize,
    pub(crate) fetch_partition_max: ByteSize,
    /// What `poll` does on a missing offset or a detected truncation. `None`
    /// surfaces `ConsumerError::LogTruncation`. Any other value makes `poll`
    /// apply the safe offset, per KIP-320.
    pub(crate) auto_offset_reset: AutoOffsetReset,
    /// A fatal error from a coordinator rejoin that the next `poll` returns.
    /// The coordinator task holds the same slot.
    pub(crate) poll_error: crate::coordinator::PollErrorSlot,
    /// Kafka's `enable.auto.commit` state. `None` when auto commit is off.
    /// The coordinator task holds a clone.
    pub(crate) auto_commit: Option<crate::commit::AutoCommit>,
    /// Counts the `poll` calls that got no error. The coordinator task waits
    /// for it after a fatal error.
    pub(crate) poll_signal: crate::coordinator::PollSignal,
    /// `true` while the member must join the group. The coordinator task
    /// writes it.
    pub(crate) rebalance_pending: tokio::sync::watch::Receiver<bool>,
    /// Kafka's `max.poll.records`.
    pub(crate) max_poll_records: usize,
    /// Fetched records that `poll` did not return yet.
    pub(crate) fetch_buffer: crate::fetch_buffer::FetchBuffer,
    /// The membership operation of `close`. The coordinator task reads it
    /// when it stops.
    pub(crate) close_operation: tokio::sync::watch::Sender<GroupMembershipOperation>,
}

/// What a closing consumer does with its group membership. Kafka's
/// `CloseOptions.GroupMembershipOperation` (KIP-1092).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GroupMembershipOperation {
    /// Send `LeaveGroup`, also for a static member.
    LeaveGroup,
    /// Stay in the group until the session timeout, also for a dynamic
    /// member.
    RemainInGroup,
    /// A dynamic member leaves the group. A static member (with a
    /// `group_instance_id`) stays in the group, so that a restart with the
    /// same instance id does not start a rebalance (KIP-345).
    #[default]
    Default,
}

#[derive(Clone)]
pub(crate) struct CommitIdentity {
    pub generation: i32,
    pub member_id: String,
    pub ownership_ids: HashMap<(String, i32), u64>,
    /// `true` after a fatal coordinator error removed the member from the
    /// group, until the join that the next `poll` starts completes. A commit
    /// fails with `CommitFailed` while it is set.
    pub rejoin_on_poll: bool,
}

#[derive(Clone)]
struct StartConfig {
    bootstrap: String,
    client_id: String,
    group_id: String,
    session_timeout: Time,
    /// Kafka's `max.poll.interval.ms`, also the `JoinGroup` rebalance timeout.
    max_poll_interval: Time,
    /// Kafka's `max.poll.records`.
    max_poll_records: usize,
    heartbeat_interval: Time,
    subscription_metadata_refresh_interval: Time,
    subscribe: Vec<String>,
    group_instance_id: Option<String>,
    auto_offset_reset: AutoOffsetReset,
    isolation_level: IsolationLevel,
    assignor: Assignor,
    fetch_min: ByteSize,
    fetch_max: ByteSize,
    fetch_partition_max: ByteSize,
    request_timeout: Time,
    dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    frame_max: krabka_client_core::ClientFrameMax,
    metadata_recovery_strategy: krabka_client_core::MetadataRecoveryStrategy,
    metadata_recovery_rebootstrap_trigger: Time,
    leave_group_timeout: Time,
    client_rack: Option<String>,
    security: Option<krabka_client_core::security::ClientSecurity>,
    retry_policy: ConsumerRetryPolicy,
    /// Kafka's `auto.commit.interval.ms`, or `None` when
    /// `enable.auto.commit` is off.
    auto_commit_interval: Option<Duration>,
    /// Kafka's `allow.auto.create.topics`.
    allow_auto_create_topics: bool,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RetryTime(Duration);

impl RetryTime {
    fn new(name: &str, value: Time) -> Result<Self, String> {
        let milliseconds = GreaterI64::<0>::new(value.millis_i64())
            .map_err(|error| format!("{name}: {error}"))?
            .into_value();
        if !value.secs_f64().is_finite() || Time::from_millis(milliseconds) != value {
            return Err(format!("{name} must be a whole number of milliseconds"));
        }
        Ok(Self(Duration::from_millis(
            u64::try_from(milliseconds).expect("validated retry time is positive"),
        )))
    }

    fn time(self) -> Time {
        Time::from_std(self.0)
    }
}

/// Validated classic Consumer startup and coordinator retry timing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerRetryPolicy {
    startup_attempt_timeout: RetryTime,
    startup_deadline: RetryTime,
    startup_initial_backoff: RetryTime,
    startup_max_backoff: RetryTime,
    coordinator_retry_timeout: RetryTime,
    coordinator_initial_backoff: RetryTime,
    coordinator_max_backoff: RetryTime,
}

impl ConsumerRetryPolicy {
    /// Construct a validated retry policy.
    ///
    /// # Errors
    ///
    /// Returns an error for non-positive, non-finite, fractional-millisecond,
    /// or inconsistently ordered values.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        startup_attempt_timeout: Time,
        startup_deadline: Time,
        startup_initial_backoff: Time,
        startup_max_backoff: Time,
        coordinator_retry_timeout: Time,
        coordinator_initial_backoff: Time,
        coordinator_max_backoff: Time,
    ) -> Result<Self, String> {
        let policy = Self {
            startup_attempt_timeout: RetryTime::new(
                "consumer startup attempt timeout",
                startup_attempt_timeout,
            )?,
            startup_deadline: RetryTime::new("consumer startup deadline", startup_deadline)?,
            startup_initial_backoff: RetryTime::new(
                "consumer startup initial backoff",
                startup_initial_backoff,
            )?,
            startup_max_backoff: RetryTime::new(
                "consumer startup maximum backoff",
                startup_max_backoff,
            )?,
            coordinator_retry_timeout: RetryTime::new(
                "consumer coordinator retry timeout",
                coordinator_retry_timeout,
            )?,
            coordinator_initial_backoff: RetryTime::new(
                "consumer coordinator initial backoff",
                coordinator_initial_backoff,
            )?,
            coordinator_max_backoff: RetryTime::new(
                "consumer coordinator maximum backoff",
                coordinator_max_backoff,
            )?,
        };
        if policy.startup_attempt_timeout > policy.startup_deadline {
            return Err(
                "consumer startup attempt timeout must not exceed startup deadline".to_owned(),
            );
        }
        if policy.startup_initial_backoff > policy.startup_max_backoff {
            return Err(
                "consumer startup initial backoff must not exceed startup maximum backoff"
                    .to_owned(),
            );
        }
        if policy.coordinator_initial_backoff > policy.coordinator_max_backoff {
            return Err(
                "consumer coordinator initial backoff must not exceed coordinator maximum backoff"
                    .to_owned(),
            );
        }
        Ok(policy)
    }

    /// Per-attempt startup timeout.
    #[must_use]
    pub fn startup_attempt_timeout(self) -> Time {
        self.startup_attempt_timeout.time()
    }

    /// Wall-clock startup deadline.
    #[must_use]
    pub fn startup_deadline(self) -> Time {
        self.startup_deadline.time()
    }

    /// Initial startup retry backoff.
    #[must_use]
    pub fn startup_initial_backoff(self) -> Time {
        self.startup_initial_backoff.time()
    }

    /// Maximum startup retry backoff.
    #[must_use]
    pub fn startup_max_backoff(self) -> Time {
        self.startup_max_backoff.time()
    }

    /// Coordinator operation retry timeout.
    #[must_use]
    pub fn coordinator_retry_timeout(self) -> Time {
        self.coordinator_retry_timeout.time()
    }

    /// Initial coordinator retry backoff.
    #[must_use]
    pub fn coordinator_initial_backoff(self) -> Time {
        self.coordinator_initial_backoff.time()
    }

    /// Maximum coordinator retry backoff.
    #[must_use]
    pub fn coordinator_max_backoff(self) -> Time {
        self.coordinator_max_backoff.time()
    }
}

impl Default for ConsumerRetryPolicy {
    fn default() -> Self {
        Self::new(
            secs(90),
            minutes(5),
            millis(500),
            secs(5),
            secs(30),
            millis(100),
            secs(1),
        )
        .expect("default consumer retry policy is valid")
    }
}

/// Default deadline for classic Consumer best-effort group departure.
pub const DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT: Time = secs(5);

/// Positive, whole-millisecond classic Consumer leave-group deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerLeaveGroupTimeout(Duration);

impl ConsumerLeaveGroupTimeout {
    /// Validate a leave-group timeout.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value whose
    /// milliseconds cannot be represented as `u64`.
    pub fn new(value: Duration) -> Result<Self, String> {
        let milliseconds = MinMaxU128::<1, { u64::MAX as u128 }>::new(value.as_millis())
            .map_err(|error| format!("consumer leave-group timeout: {error}"))?
            .into_value();
        let milliseconds = u64::try_from(milliseconds)
            .map_err(|error| format!("consumer leave-group timeout: {error}"))?;
        if Duration::from_millis(milliseconds) != value {
            return Err(
                "consumer leave-group timeout must be a whole number of milliseconds".to_owned(),
            );
        }
        Ok(Self(value))
    }

    /// Return the validated duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    /// Return the validated duration in milliseconds.
    ///
    /// # Panics
    ///
    /// Panics only if the invariant established by [`Self::new`] is violated.
    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis()).expect("validated consumer leave-group timeout fits u64")
    }
}

impl Default for ConsumerLeaveGroupTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT.to_std())
            .expect("default consumer leave-group timeout is valid")
    }
}

/// Default cadence for checking subscribed-topic metadata changes.
pub const DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL: Time = secs(5);

/// Positive total response-byte budget for one classic consumer fetch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerFetchMaxBytes(i32);

impl ConsumerFetchMaxBytes {
    /// Validate a total fetch byte budget.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or negative.
    pub fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("consumer fetch max bytes: {error}"))
    }

    /// Return the validated protocol byte count.
    #[must_use]
    pub const fn bytes(self) -> i32 {
        self.0
    }

    /// Return the validated byte count as a dimensioned quantity.
    #[must_use]
    pub fn size(self) -> ByteSize {
        ByteSize::from_bytes_i64(i64::from(self.0))
    }
}

impl TryFrom<ByteSize> for ConsumerFetchMaxBytes {
    type Error = String;

    fn try_from(value: ByteSize) -> Result<Self, Self::Error> {
        let bytes = value.bytes_f64();
        if !bytes.is_finite()
            || bytes.fract() != 0.0
            || !(1.0..=f64::from(i32::MAX)).contains(&bytes)
        {
            return Err(
                "consumer fetch max must be a positive whole-byte value that fits i32".to_owned(),
            );
        }
        Self::new(value.bytes_i32())
    }
}

/// Positive per-partition response-byte budget for one classic consumer fetch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerFetchPartitionMaxBytes(i32);

impl ConsumerFetchPartitionMaxBytes {
    /// Validate a per-partition fetch byte budget.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or negative.
    pub fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("consumer fetch partition max bytes: {error}"))
    }

    /// Return the validated protocol byte count.
    #[must_use]
    pub const fn bytes(self) -> i32 {
        self.0
    }

    /// Return the validated byte count as a dimensioned quantity.
    #[must_use]
    pub fn size(self) -> ByteSize {
        ByteSize::from_bytes_i64(i64::from(self.0))
    }
}

impl TryFrom<ByteSize> for ConsumerFetchPartitionMaxBytes {
    type Error = String;

    fn try_from(value: ByteSize) -> Result<Self, Self::Error> {
        let bytes = value.bytes_f64();
        if !bytes.is_finite()
            || bytes.fract() != 0.0
            || !(1.0..=f64::from(i32::MAX)).contains(&bytes)
        {
            return Err(
                "consumer fetch partition max must be a positive whole-byte value that fits i32"
                    .to_owned(),
            );
        }
        Self::new(value.bytes_i32())
    }
}

/// Positive, whole-millisecond subscribed-topic metadata refresh cadence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerSubscriptionMetadataRefreshInterval(Duration);

impl ConsumerSubscriptionMetadataRefreshInterval {
    /// Validate a subscribed-topic metadata refresh interval.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value whose
    /// milliseconds cannot be represented as `u64`.
    pub fn new(value: Duration) -> Result<Self, String> {
        let milliseconds = MinMaxU128::<1, { u64::MAX as u128 }>::new(value.as_millis())
            .map_err(|error| format!("consumer subscription metadata refresh interval: {error}"))?
            .into_value();
        let milliseconds = u64::try_from(milliseconds)
            .map_err(|error| format!("consumer subscription metadata refresh interval: {error}"))?;
        if Duration::from_millis(milliseconds) != value {
            return Err(
                "consumer subscription metadata refresh interval must be a whole number of milliseconds"
                    .to_owned(),
            );
        }
        Ok(Self(value))
    }

    /// Return the validated duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    /// Return the validated duration in milliseconds.
    ///
    /// # Panics
    ///
    /// Panics only if the invariant established by [`Self::new`] is violated.
    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis())
            .expect("validated consumer subscription metadata refresh interval fits u64")
    }
}

impl Default for ConsumerSubscriptionMetadataRefreshInterval {
    fn default() -> Self {
        Self::new(DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL.to_std())
            .expect("default consumer subscription metadata refresh interval is valid")
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.coordinator_shutdown.cancel();
    }
}

/// A per-record header key/value pair, as the Kafka v2 record format defines
/// it. The key is a UTF-8 string. The value is optional raw bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Bytes>,
}

/// The meaning of a record timestamp, as bit 3 of the v2 record batch
/// attributes gives it.
///
/// Kafka's `TimestampType` also has `NoTimestampType` for v0 messages. This
/// consumer reads only v2 record batches, so that value cannot occur.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampType {
    /// The producer set the timestamp when it created the record.
    CreateTime,
    /// The broker set the timestamp when it appended the batch to the log.
    LogAppendTime,
}

impl From<krabka_protocol::records::TimestampType> for TimestampType {
    fn from(value: krabka_protocol::records::TimestampType) -> Self {
        match value {
            krabka_protocol::records::TimestampType::CreateTime => Self::CreateTime,
            krabka_protocol::records::TimestampType::LogAppendTime => Self::LogAppendTime,
        }
    }
}

/// One record returned by `Consumer::poll`.
///
/// The key and the value are the raw record bytes. Kafka's
/// `serializedKeySize` and `serializedValueSize` are their lengths, or -1 for
/// `None`, so this type does not keep them as separate fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub leader_epoch: i32,
    /// The create time, or for a `LogAppendTime` batch the broker append
    /// time (the batch `max_timestamp`).
    pub timestamp: i64,
    /// The timestamp type from the attributes of the record batch.
    pub timestamp_type: TimestampType,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
}

fn initial_subscription_bytes(subscribe: &[String], client_rack: Option<&str>) -> bytes::Bytes {
    encode_subscription(subscribe, &[], -1, client_rack)
}

/// The `JoinGroup` fields of the startup join. Kafka's first
/// `AbstractCoordinator.rejoinReason` is empty.
fn startup_join_fields(
    group_id: &str,
    group_instance_id: Option<&str>,
    protocol_name: &str,
    subscription: bytes::Bytes,
    (session_timeout_ms, rebalance_timeout_ms): (i32, i32),
) -> crate::coordinator::JoinRequestFields {
    crate::coordinator::JoinRequestFields {
        group_id: group_id.to_owned(),
        group_instance_id: group_instance_id.map(str::to_owned),
        session_timeout_ms,
        rebalance_timeout_ms,
        protocol_name: protocol_name.to_owned(),
        subscription,
        reason: String::new(),
    }
}

fn first_join_member_id(resp: &JoinGroupResponse) -> Result<String, ConsumerError> {
    let member_id = if resp.error_code == 79 || resp.error_code == 0 {
        resp.member_id.clone()
    } else {
        return Err(ConsumerError::Server(resp.error_code));
    };
    if member_id.is_empty() {
        return Err(ConsumerError::RebalanceFailed(
            "broker did not assign a member_id".into(),
        ));
    }
    Ok(member_id)
}

/// The metadata scope of a consumer with a topic list subscription. Kafka's
/// `ConsumerMetadata.newMetadataRequestBuilder` names the subscribed topics
/// and asks for all topics only for a client-side pattern subscription.
fn subscription_metadata_scope(
    allow_auto_create_topics: bool,
) -> krabka_client_core::MetadataScope {
    krabka_client_core::MetadataScope::Topics {
        allow_auto_topic_creation: allow_auto_create_topics,
    }
}

fn is_subscribed_topic(subscribe: &[String], name: &str) -> bool {
    subscribe.iter().any(|s| s == name)
}

fn is_group_leader(leader: &str, member_id: &str) -> bool {
    leader == member_id
}

fn build_sync_assignment(
    member_id: String,
    partitions: &[(String, i32)],
) -> SyncGroupRequestAssignment {
    SyncGroupRequestAssignment {
        member_id,
        assignment: encode_assignment(partitions),
        ..Default::default()
    }
}

fn build_sync_request(
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
    protocol_name: String,
    assignments: Vec<SyncGroupRequestAssignment>,
) -> SyncGroupRequest {
    SyncGroupRequest {
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        protocol_type: Some("consumer".into()),
        protocol_name: Some(protocol_name),
        assignments,
        ..Default::default()
    }
}

async fn leave_startup_member(
    client: &Client,
    coordinator_id: &AtomicI32,
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<String>,
    leave_group_timeout: Time,
) {
    if !crate::coordinator::should_send_leave_group(
        member_id,
        group_instance_id.as_deref(),
        GroupMembershipOperation::Default,
    ) {
        return;
    }
    let broker = client.broker(coordinator_id.load(Ordering::Relaxed));
    let send = broker.send(crate::coordinator::build_leave_group_request(
        group_id.to_string(),
        member_id.to_string(),
        None,
    ));
    let _ = tokio::time::timeout(leave_group_timeout.to_std(), send).await;
}

fn has_assigned_partitions(assigned_partitions: &[(String, i32)]) -> bool {
    !assigned_partitions.is_empty()
}

pub(crate) fn starting_offset(committed: i64, auto_offset_reset: AutoOffsetReset) -> i64 {
    if committed >= 0 {
        committed
    } else {
        reset_starting_offset(auto_offset_reset)
    }
}

pub(crate) fn reset_starting_offset(auto_offset_reset: AutoOffsetReset) -> i64 {
    match auto_offset_reset {
        AutoOffsetReset::Earliest => 0,
        // Resolved by poll() on first call.
        AutoOffsetReset::Latest | AutoOffsetReset::None => i64::MAX,
    }
}

/// Kafka's default `max.poll.interval.ms`.
pub const DEFAULT_CONSUMER_MAX_POLL_INTERVAL: Time = minutes(5);

/// Kafka's default `max.poll.records`.
pub const DEFAULT_CONSUMER_MAX_POLL_RECORDS: usize = 500;

/// The validated `max_poll_records`.
///
/// Kafka's `ConsumerConfig` defines `max.poll.records` as an `int` of at least
/// 1.
fn validated_max_poll_records(records: usize) -> Result<usize, String> {
    if records == 0 || i32::try_from(records).is_err() {
        return Err("consumer max poll records must be from 1 to i32::MAX".to_owned());
    }
    Ok(records)
}

/// The validated `max_poll_interval`.
///
/// Kafka's `ConsumerConfig` defines `max.poll.interval.ms` as an `int` of at
/// least 1. The interval must therefore be a whole number of milliseconds from
/// 1 to `i32::MAX`.
fn validated_max_poll_interval(interval: Time) -> Result<Time, String> {
    let milliseconds = MinMaxI64::<1, { i32::MAX as i64 }>::new(interval.millis_i64())
        .map_err(|error| format!("consumer max poll interval: {error}"))?
        .into_value();
    if !interval.secs_f64().is_finite() || Time::from_millis(milliseconds) != interval {
        return Err("consumer max poll interval must be a whole number of milliseconds".to_owned());
    }
    Ok(interval)
}

/// The auto commit interval, or `None` when auto commit is off.
///
/// Kafka's `ConsumerConfig` defines `auto.commit.interval.ms` as an `int` of at
/// least 0. The interval must therefore be a whole number of milliseconds from
/// 0 to `i32::MAX`.
fn validated_auto_commit_interval(
    enable_auto_commit: bool,
    interval: Time,
) -> Result<Option<Duration>, String> {
    if !enable_auto_commit {
        return Ok(None);
    }
    let milliseconds = MinMaxI64::<0, { i32::MAX as i64 }>::new(interval.millis_i64())
        .map_err(|error| format!("consumer auto commit interval: {error}"))?
        .into_value();
    if !interval.secs_f64().is_finite() || Time::from_millis(milliseconds) != interval {
        return Err(
            "consumer auto commit interval must be a whole number of milliseconds".to_owned(),
        );
    }
    Ok(Some(interval.to_std()))
}

fn primed_position(committed_epoch: i32) -> crate::position::PartitionPosition {
    crate::position::PartitionPosition {
        // Wrap the committed leader epoch (raw wire `int32` from OffsetFetch) at
        // the decode boundary.
        offset_epoch: krabka_ids::LeaderEpoch(committed_epoch),
        ..Default::default()
    }
}

/// A protocol `int32` millisecond field, truncated rather than rounded.
///
/// The coordinator compares every Kafka request field that this feeds against
/// its own configured bounds. The pre-quantity code reached those fields
/// through `Duration::as_millis`, which truncates. Rounding to nearest would
/// push a fractional-millisecond timeout one millisecond wider on the wire.
pub(crate) fn protocol_millis_i32(value: Time) -> i32 {
    i32::try_from(value.millis_i64_trunc()).unwrap_or(i32::MAX)
}

#[bon::bon]
impl Consumer {
    /// Build a [`Consumer`] subscribed to the given topics.
    ///
    /// This function validates the configuration eagerly and fails fast before
    /// any network I/O. It then calls the internal `start_once` operation with a
    /// per-attempt timeout. An attempt can stall on a lost wakeup during
    /// cold-boot group-join contention, or return a transient error. The
    /// function then drops the timed-out future, which cancels its in-flight
    /// connections, and starts a fresh attempt. A genuine misconfiguration or a
    /// persistent error surfaces immediately.
    ///
    /// With `enable_auto_commit` on (the default, as in Kafka), the consumer
    /// commits its positions as Kafka's `enable.auto.commit` does: from `poll`
    /// each `auto_commit_interval` (default 5 s), before each `JoinGroup`, and
    /// in [`close`](Self::close). The commit before a `JoinGroup` includes only
    /// records that the application received before its latest `poll`. Set
    /// `enable_auto_commit(false)` to commit manually, for example in a
    /// transactional consume-process-produce loop. The consumer then commits
    /// nothing that the application does not ask for.
    ///
    /// `max_poll_interval` (default 5 min) is Kafka's `max.poll.interval.ms`.
    /// When the application does not call `poll` for longer, a dynamic member
    /// sends `LeaveGroup` and a static member stops its heartbeats. The next
    /// `poll` joins the group again. The consumer also sends the value as the
    /// `JoinGroup` rebalance timeout. The consumer starts a rebalance that the
    /// coordinator asks for only in `poll`, so the assignment does not change
    /// for the eager protocol while the application processes the records of
    /// the last `poll`.
    #[builder(start_fn = builder, finish_fn = build)]
    #[tracing::instrument(
        name = "consumer.start",
        level = "info",
        skip_all,
        fields(group_id = %group_id, client_id = %client_id),
        err
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into, default = "krabka-consumer".to_string())] client_id: String,
        #[builder(into)] group_id: String,
        #[builder(default = secs(45))] session_timeout: Time,
        #[builder(default = DEFAULT_CONSUMER_MAX_POLL_INTERVAL)] max_poll_interval: Time,
        #[builder(default = DEFAULT_CONSUMER_MAX_POLL_RECORDS)] max_poll_records: usize,
        #[builder(default = secs(3))] heartbeat_interval: Time,
        #[builder(default = DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL)]
        subscription_metadata_refresh_interval: Time,
        #[builder(into)] subscribe: Vec<String>,
        #[builder(into)] group_instance_id: Option<String>,
        #[builder(default = AutoOffsetReset::Latest)] auto_offset_reset: AutoOffsetReset,
        #[builder(default = IsolationLevel::ReadUncommitted)] isolation_level: IsolationLevel,
        #[builder(default = Assignor::Range)] assignor: Assignor,
        #[builder(default = krabka_client_core::DEFAULT_FETCH_MIN)] fetch_min: ByteSize,
        #[builder(default = crate::poll::DEFAULT_FETCH_MAX)] fetch_max: ByteSize,
        #[builder(default = crate::poll::DEFAULT_FETCH_PARTITION_MAX)]
        fetch_partition_max: ByteSize,
        #[builder(default = secs(30))] request_timeout: Time,
        #[builder(default = krabka_client_core::DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY)]
        dispatch_queue_capacity: usize,
        #[builder(default = krabka_client_core::DEFAULT_CLIENT_FRAME_MAX)] frame_max: ByteSize,
        #[builder(default)]
        metadata_recovery_strategy: krabka_client_core::MetadataRecoveryStrategy,
        #[builder(default = krabka_client_core::DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER)]
        metadata_recovery_rebootstrap_trigger: Time,
        #[builder(default = DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT)] leave_group_timeout: Time,
        #[builder(into)] client_rack: Option<String>,
        security: Option<krabka_client_core::security::ClientSecurity>,
        #[builder(default = ConsumerRetryPolicy::default())] retry_policy: ConsumerRetryPolicy,
        #[builder(default = true)] enable_auto_commit: bool,
        #[builder(default = secs(5))] auto_commit_interval: Time,
        /// Kafka's `allow.auto.create.topics`: the metadata requests name the
        /// subscribed topics and let a broker with
        /// `auto.create.topics.enable=true` create a missing one.
        #[builder(default = true)]
        allow_auto_create_topics: bool,
    ) -> Result<Self, ConsumerError> {
        // Fail fast on misconfig — before any retry loop.
        if subscribe.is_empty() {
            return Err(ConsumerError::NotSubscribed);
        }
        if group_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed("group_id required".into()));
        }
        if group_instance_id.as_deref().is_some_and(str::is_empty) {
            return Err(ConsumerError::RebalanceFailed(
                "group_instance_id must not be empty".into(),
            ));
        }
        let fetch_min = FetchMinBytes::try_from(fetch_min)
            .map_err(ConsumerError::RebalanceFailed)?
            .size();
        let fetch_max = ConsumerFetchMaxBytes::try_from(fetch_max)
            .map_err(ConsumerError::RebalanceFailed)?
            .size();
        if fetch_min.bytes_i32() > fetch_max.bytes_i32() {
            return Err(ConsumerError::RebalanceFailed(
                "consumer fetch min must not exceed consumer fetch max".to_owned(),
            ));
        }
        let fetch_partition_max = ConsumerFetchPartitionMaxBytes::try_from(fetch_partition_max)
            .map_err(ConsumerError::RebalanceFailed)?
            .size();
        // The two validated newtypes below still speak `Duration`: both derive
        // `Eq`, which an `f64`-backed quantity cannot satisfy. Their whole job
        // is to police the whole-millisecond invariant, so the quantity meets
        // them at their own boundary and comes straight back.
        let leave_group_timeout = ConsumerLeaveGroupTimeout::new(leave_group_timeout.to_std())
            .map_err(ConsumerError::RebalanceFailed)?;
        let subscription_metadata_refresh_interval =
            ConsumerSubscriptionMetadataRefreshInterval::new(
                subscription_metadata_refresh_interval.to_std(),
            )
            .map_err(ConsumerError::RebalanceFailed)?;
        let dispatch_queue_capacity =
            krabka_client_core::ConnectionDispatchQueueCapacity::new(dispatch_queue_capacity)
                .map_err(ConsumerError::RebalanceFailed)?;
        let frame_max = krabka_client_core::ClientFrameMax::try_from(frame_max)
            .map_err(ConsumerError::RebalanceFailed)?;
        let metadata_recovery_rebootstrap_trigger =
            krabka_client_core::MetadataRecoveryRebootstrapTrigger::new(
                metadata_recovery_rebootstrap_trigger,
            )
            .map_err(ConsumerError::RebalanceFailed)?
            .time();
        let auto_commit_interval =
            validated_auto_commit_interval(enable_auto_commit, auto_commit_interval)
                .map_err(ConsumerError::RebalanceFailed)?;
        let max_poll_interval = validated_max_poll_interval(max_poll_interval)
            .map_err(ConsumerError::RebalanceFailed)?;
        let max_poll_records =
            validated_max_poll_records(max_poll_records).map_err(ConsumerError::RebalanceFailed)?;

        let config = StartConfig {
            bootstrap,
            client_id,
            group_id,
            session_timeout,
            max_poll_interval,
            max_poll_records,
            heartbeat_interval,
            subscription_metadata_refresh_interval: Time::from_std(
                subscription_metadata_refresh_interval.duration(),
            ),
            subscribe,
            group_instance_id,
            auto_offset_reset,
            isolation_level,
            assignor,
            fetch_min,
            fetch_max,
            fetch_partition_max,
            request_timeout,
            dispatch_queue_capacity,
            frame_max,
            metadata_recovery_strategy,
            metadata_recovery_rebootstrap_trigger,
            leave_group_timeout: Time::from_std(leave_group_timeout.duration()),
            client_rack,
            security,
            retry_policy,
            auto_commit_interval,
            allow_auto_create_topics,
        };

        let started = tokio::time::Instant::now();
        let mut backoff = config.retry_policy.startup_initial_backoff().to_std();
        loop {
            match tokio::time::timeout(
                config.retry_policy.startup_attempt_timeout().to_std(),
                Box::pin(Self::start_once(config.clone())),
            )
            .await
            {
                Ok(Ok(consumer)) => return Ok(consumer),
                Ok(Err(error)) => {
                    if started.elapsed().as_time() < config.retry_policy.startup_deadline()
                        && is_retriable_consumer_start_error(&error)
                    {
                        tracing::warn!(
                        group = %config.group_id,
                                %error,
                                "consumer startup failed transiently; retrying with a fresh connection"
                            );
                    } else {
                        return Err(error);
                    }
                }
                Err(_elapsed) => {
                    if started.elapsed().as_time() >= config.retry_policy.startup_deadline() {
                        return Err(ConsumerError::Client(
                            krabka_client_core::ClientError::Timeout(
                                config.retry_policy.startup_attempt_timeout(),
                            ),
                        ));
                    }
                    tracing::warn!(
                        group = %config.group_id,
                        timeout = ?config.retry_policy.startup_attempt_timeout(),
                        "consumer startup exceeded attempt timeout \
                         (likely a cold-boot group-join stall); \
                         retrying with a fresh connection"
                    );
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(
                backoff * 2,
                config.retry_policy.startup_max_backoff().to_std(),
            );
        }
    }

    /// Single attempt to build a [`Consumer`]: resolve bootstrap, `JoinGroup`
    /// twice, compute the assignment if this member is the elected leader,
    /// `SyncGroup`, prime offsets, then spawn the coordinator task.
    ///
    /// [`Self::start`] calls this under a per-attempt timeout. A drop of the
    /// returned future at any point before the final `tokio::spawn` cancels all
    /// in-flight connections cleanly. This function spawns the coordinator task
    /// at its very end, with no `.await` after it, so a timed-out attempt can
    /// never orphan a coordinator task.
    #[tracing::instrument(
        name = "consumer.start_once",
        level = "info",
        skip_all,
        fields(
            group_id = %config.group_id,
            coordinator_id = tracing::field::Empty,
            member_id = tracing::field::Empty,
            generation = tracing::field::Empty,
            is_leader = tracing::field::Empty,
            assigned_partitions = tracing::field::Empty,
        ),
        err
    )]
    async fn start_once(config: StartConfig) -> Result<Self, ConsumerError> {
        let finish_config = config.clone();
        let StartConfig {
            bootstrap,
            client_id,
            group_id,
            session_timeout,
            max_poll_interval,
            subscribe,
            group_instance_id,
            assignor,
            request_timeout,
            dispatch_queue_capacity,
            frame_max,
            metadata_recovery_strategy,
            metadata_recovery_rebootstrap_trigger,
            client_rack,
            security,
            allow_auto_create_topics,
            ..
        } = config;
        let client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id(client_id.clone())
            .request_timeout(request_timeout)
            .dispatch_queue_capacity(dispatch_queue_capacity.get())
            .frame_max(frame_max.size())
            .metadata_recovery_strategy(metadata_recovery_strategy)
            .metadata_recovery_rebootstrap_trigger(metadata_recovery_rebootstrap_trigger)
            .maybe_security(security.clone())
            .metadata_scope(subscription_metadata_scope(allow_auto_create_topics))
            .build()
            .await?;
        client.metadata_topics().set(subscribe.iter().cloned());

        // `JoinGroupRequest` carries these as `int32` milliseconds, and the
        // JVM coordinator compares them against its own configured bounds.
        // `Duration::as_millis` truncated here before the conversion, so keep
        // truncating: `millis_i32`'s round-to-nearest would widen a fractional
        // session timeout by a millisecond on the wire.
        let session_timeout_ms = protocol_millis_i32(session_timeout);
        // Kafka's `ClassicKafkaConsumer` sends `max.poll.interval.ms` as the
        // rebalance timeout.
        let rebalance_timeout_ms = protocol_millis_i32(max_poll_interval);

        // 0. Discover the group's coordinator broker. Real Kafka (Strimzi)
        //    returns NOT_COORDINATOR (16) for any group RPC that doesn't reach
        //    the group's actual coordinator, so every Join/Sync/Heartbeat/Commit/
        //    Fetch/Leave must target it — not the arbitrary bootstrap broker.
        //    `find_coordinator` also `refresh_metadata`s the main client's pool
        //    so it learns the coordinator broker's address (needed by
        //    `client.broker(coordinator_id)` here and by `commit.rs`).
        let coordinator_retry = CoordinatorRetryPolicy::from(config.retry_policy);
        let coordinator_id = Arc::new(AtomicI32::new(
            find_coordinator(&client, &group_id, coordinator_retry).await?,
        ));
        tracing::Span::current().record("coordinator_id", coordinator_id.load(Ordering::Relaxed));

        // First JoinGroup uses empty `owned_partitions` + `generation_id=-1`:
        // we've never been in the group before, so we have nothing to claim
        // and no prior generation to defend against zombie ownership.
        let subscription_bytes = initial_subscription_bytes(&subscribe, client_rack.as_deref());
        let protocol_name = assignor.protocol_name().to_string();

        // 1. First JoinGroup — empty member_id, expect MEMBER_ID_REQUIRED (79)
        //    or a regular response; either way the broker hands us a member_id.
        //    Routed to the coordinator broker, re-discovering it on a
        //    cold/relocating-coordinator code (14/15/16) before each retry.
        let join_fields = startup_join_fields(
            &group_id,
            group_instance_id.as_deref(),
            &protocol_name,
            subscription_bytes.clone(),
            (session_timeout_ms, rebalance_timeout_ms),
        );
        let r1 = with_coordinator_refind(
            &client,
            &group_id,
            &coordinator_id,
            coordinator_retry,
            |r: &JoinGroupResponse| r.error_code,
            || {
                let request = join_fields.request(String::new());
                let client = &client;
                let target = coordinator_id.load(Ordering::Relaxed);
                async move {
                    client
                        .broker(target)
                        .send(request)
                        .await
                        .map_err(ConsumerError::from)
                }
            },
        )
        .await?;
        let member_id = first_join_member_id(&r1)?;
        tracing::Span::current().record("member_id", member_id.as_str());

        let cleanup_client = client.clone();
        let cleanup_coordinator_id = Arc::clone(&coordinator_id);
        let cleanup_group_id = group_id.clone();
        let cleanup_member_id = member_id.clone();
        let cleanup_group_instance_id = group_instance_id.clone();
        let cleanup_leave_group_timeout = finish_config.leave_group_timeout;
        let start_result = finish_startup(
            finish_config,
            client,
            coordinator_id,
            member_id,
            subscription_bytes,
            protocol_name,
            (session_timeout_ms, rebalance_timeout_ms),
        )
        .await;
        match start_result {
            Ok(consumer) => Ok(consumer),
            Err(error) => {
                leave_startup_member(
                    &cleanup_client,
                    &cleanup_coordinator_id,
                    &cleanup_group_id,
                    &cleanup_member_id,
                    cleanup_group_instance_id,
                    cleanup_leave_group_timeout,
                )
                .await;
                Err(ConsumerError::StartupAfterJoin(Box::new(error)))
            }
        }
    }
}

async fn finish_startup(
    config: StartConfig,
    client: Client,
    coordinator_id: Arc<AtomicI32>,
    member_id: String,
    subscription_bytes: bytes::Bytes,
    protocol_name: String,
    timeouts_ms: (i32, i32),
) -> Result<Consumer, ConsumerError> {
    let spawn_config = config.clone();
    let coordinator_retry = CoordinatorRetryPolicy::from(config.retry_policy);
    let StartConfig {
        group_id,
        group_instance_id,
        auto_offset_reset,
        ..
    } = config;
    // Kafka's `JoinGroupResponseHandler` requests the second join with this
    // reason.
    let mut join_fields = startup_join_fields(
        &group_id,
        group_instance_id.as_deref(),
        &protocol_name,
        subscription_bytes,
        timeouts_ms,
    );
    join_fields.reason = format!("need to re-join with the given member-id: {member_id}");
    // 2. Second JoinGroup with the assigned member_id, on the coordinator.
    let r2 = with_coordinator_refind(
        &client,
        &group_id,
        &coordinator_id,
        coordinator_retry,
        |r: &JoinGroupResponse| r.error_code,
        || {
            let request = join_fields.request(member_id.clone());
            let client = &client;
            let target = coordinator_id.load(Ordering::Relaxed);
            async move {
                client
                    .broker(target)
                    .send(request)
                    .await
                    .map_err(ConsumerError::from)
            }
        },
    )
    .await?;
    if r2.error_code != 0 {
        return Err(ConsumerError::Server(r2.error_code));
    }

    let InitialAssignment {
        assigned_partitions,
        topic_ids,
        topic_partitions,
    } = resolve_initial_assignment(
        &spawn_config,
        &client,
        &coordinator_id,
        &member_id,
        &protocol_name,
        &r2,
    )
    .await?;

    // 5. Fetch existing committed offsets so poll() resumes correctly.
    let mut next_offsets: HashMap<(String, i32), i64> = HashMap::new();
    let mut positions: HashMap<(String, i32), crate::position::PartitionPosition> = HashMap::new();
    if has_assigned_partitions(&assigned_partitions) {
        let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for (t, p) in &assigned_partitions {
            by_topic.entry(t.clone()).or_default().push(*p);
        }
        // OffsetFetch is a coordinator RPC — route it to the coordinator
        // broker (its id is fresh from the join/sync above).
        let of = crate::coordinator::send_offset_fetch(
            &client,
            &group_id,
            &coordinator_id,
            &crate::offset_wire::build_offset_fetch(&group_id, &by_topic),
            coordinator_retry,
        )
        .await?;
        for (name, partition_index, committed, committed_epoch) in
            crate::offset_wire::parse_offset_fetch(&of)
        {
            let starting = starting_offset(committed, auto_offset_reset);
            next_offsets.insert((name.clone(), partition_index), starting);
            positions.insert((name, partition_index), primed_position(committed_epoch));
        }
    }

    spawn_consumer(
        spawn_config,
        client,
        coordinator_id,
        member_id,
        StartupState {
            generation_id: r2.generation_id,
            assigned_partitions,
            next_offsets,
            positions,
            topic_ids,
            topic_partitions,
        },
    )
    .await
}

struct InitialAssignment {
    assigned_partitions: Vec<(String, i32)>,
    topic_ids: HashMap<String, WireUuid>,
    topic_partitions: HashMap<String, i32>,
}

async fn resolve_initial_assignment(
    config: &StartConfig,
    client: &Client,
    coordinator_id: &Arc<AtomicI32>,
    member_id: &str,
    protocol_name: &str,
    r2: &JoinGroupResponse,
) -> Result<InitialAssignment, ConsumerError> {
    let group_id = &config.group_id;
    let subscribe = &config.subscribe;
    let group_instance_id = &config.group_instance_id;
    let assignor = config.assignor;
    // 3. Always issue a Metadata to resolve topic_ids (needed for
    //    Fetch v ≥ 13). If we are the leader, also use the partition
    //    counts to compute the assignment.
    //    `refresh_metadata` (not a bare `send`) so the main client's
    //    BrokerPool learns each broker's (id → addr) mapping up front,
    //    letting `poll`/`validate` route to partition leaders immediately
    //    rather than waiting for the first `refresh_leader_epochs` pass.
    let md = client.refresh_metadata().await?;
    let mut topic_ids: HashMap<String, WireUuid> = HashMap::new();
    let mut topic_partitions: HashMap<String, i32> = HashMap::new();
    for t in &md.topics {
        let Some(name) = &t.name else { continue };
        if is_subscribed_topic(subscribe, name) {
            let count = i32::try_from(t.partitions.len()).unwrap_or(i32::MAX);
            topic_partitions.insert(name.clone(), count);
            topic_ids.insert(name.clone(), t.topic_id);
        }
    }

    let is_leader = is_group_leader(&r2.leader, member_id);
    tracing::Span::current().record("generation", r2.generation_id);
    tracing::Span::current().record("is_leader", is_leader);
    let assignments_for_sync: Vec<SyncGroupRequestAssignment> = if is_leader {
        let assignments = match assignor {
            Assignor::Range => {
                let inputs: Vec<(String, Vec<String>)> = r2
                    .members
                    .iter()
                    .map(|m| {
                        let ds = decode_subscription(&m.metadata);
                        (m.member_id.clone(), ds.topics)
                    })
                    .collect();
                crate::assignor::range::assign(inputs, &topic_partitions)
            }
            Assignor::CooperativeSticky => {
                let inputs: Vec<crate::assignor::cooperative_sticky::MemberInput> = r2
                    .members
                    .iter()
                    .map(|m| {
                        let ds = decode_subscription(&m.metadata);
                        (m.member_id.clone(), ds.topics, ds.owned, ds.generation_id)
                    })
                    .collect();
                crate::assignor::cooperative_sticky::assign(&inputs, &topic_partitions)
            }
        };
        assignments
            .into_iter()
            .map(|(m, partitions)| build_sync_assignment(m, &partitions))
            .collect()
    } else {
        Vec::new()
    };

    // 4. SyncGroup — leader installs assignments; everyone receives their
    //    own assignment in the response. On the coordinator broker, with
    //    re-discovery on a cold/relocating-coordinator code.
    let r3 = with_coordinator_refind(
        client,
        group_id,
        coordinator_id,
        config.retry_policy.into(),
        |r: &SyncGroupResponse| r.error_code,
        || {
            let group_id = group_id.clone();
            let protocol_name = protocol_name.to_string();
            let member_id = member_id.to_string();
            let assignments_for_sync = assignments_for_sync.clone();
            let generation_id = r2.generation_id;
            let group_instance_id = group_instance_id.clone();
            let target = coordinator_id.load(Ordering::Relaxed);
            async move {
                client
                    .broker(target)
                    .send(build_sync_request(
                        group_id,
                        generation_id,
                        member_id,
                        group_instance_id.clone(),
                        protocol_name,
                        assignments_for_sync,
                    ))
                    .await
                    .map_err(ConsumerError::from)
            }
        },
    )
    .await?;
    if r3.error_code != 0 {
        return Err(ConsumerError::Server(r3.error_code));
    }
    let assigned_partitions = decode_assignment(&r3.assignment);
    tracing::Span::current().record("assigned_partitions", assigned_partitions.len());

    Ok(InitialAssignment {
        assigned_partitions,
        topic_ids,
        topic_partitions,
    })
}

struct StartupState {
    generation_id: i32,
    assigned_partitions: Vec<(String, i32)>,
    next_offsets: HashMap<(String, i32), i64>,
    positions: HashMap<(String, i32), crate::position::PartitionPosition>,
    topic_ids: HashMap<String, WireUuid>,
    topic_partitions: HashMap<String, i32>,
}

async fn spawn_consumer(
    config: StartConfig,
    client: Client,
    coordinator_id: Arc<AtomicI32>,
    member_id: String,
    startup: StartupState,
) -> Result<Consumer, ConsumerError> {
    let StartConfig {
        bootstrap,
        client_id,
        group_id,
        session_timeout,
        max_poll_interval,
        max_poll_records,
        heartbeat_interval,
        subscription_metadata_refresh_interval,
        subscribe,
        group_instance_id,
        auto_offset_reset,
        isolation_level,
        assignor,
        fetch_min,
        fetch_max,
        fetch_partition_max,
        request_timeout,
        dispatch_queue_capacity,
        frame_max,
        metadata_recovery_strategy,
        metadata_recovery_rebootstrap_trigger,
        leave_group_timeout,
        client_rack,
        security,
        retry_policy,
        auto_commit_interval,
        allow_auto_create_topics,
    } = config;
    let StartupState {
        generation_id,
        assigned_partitions,
        next_offsets,
        positions,
        topic_ids,
        topic_partitions,
    } = startup;
    // 6. Spawn the coordinator task (heartbeat + rebalance loop) on its
    //    own connection.
    //
    //    The broker processes requests serially per TCP connection: a
    //    JoinGroup parked in the rebalance-join purgatory (up to
    //    INITIAL_REBALANCE_DELAY per round, and cooperative rounds
    //    cascade) blocks every later request on that same socket. If the
    //    coordinator shared `poll()`'s connection, a `Fetch` issued
    //    mid-rebalance would head-of-line-block behind the parked
    //    JoinGroup and stall until the client request timeout. A dedicated
    //    coordinator connection keeps the data path (`poll`/commit)
    //    independent of the group-protocol path. (The JVM client never
    //    hits this because real brokers serve a connection's requests
    //    concurrently.)
    let coordinator_client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id(client_id.clone())
        .request_timeout(request_timeout)
        .dispatch_queue_capacity(dispatch_queue_capacity.get())
        .frame_max(frame_max.size())
        .metadata_recovery_strategy(metadata_recovery_strategy)
        .metadata_recovery_rebootstrap_trigger(metadata_recovery_rebootstrap_trigger)
        .maybe_security(security.clone())
        .metadata_scope(subscription_metadata_scope(allow_auto_create_topics))
        .build()
        .await?;
    coordinator_client
        .metadata_topics()
        .set(subscribe.iter().cloned());

    let ownership_ids = assigned_partitions
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, partition)| (partition, index as u64 + 1))
        .collect();
    let next_ownership_id = assigned_partitions.len() as u64 + 1;
    let assigned = Arc::new(Mutex::new(assigned_partitions));
    let assignment_changed = Arc::new(Notify::new());
    let commit_identity = Arc::new(Mutex::new(CommitIdentity {
        generation: generation_id,
        member_id: member_id.clone(),
        ownership_ids,
        rejoin_on_poll: false,
    }));
    let commit_serialization = Arc::new(Mutex::new(()));
    let commit_async_state = Arc::new(AtomicU8::new(0));
    let next_offsets = Arc::new(Mutex::new(next_offsets));
    let end_offsets = Arc::new(Mutex::new(HashMap::new()));
    let positions = Arc::new(Mutex::new(positions));
    let pending_seeks = Arc::new(Mutex::new(HashMap::new()));
    let topic_ids = Arc::new(Mutex::new(topic_ids));
    // Shared with the coordinator task so the commit path always stamps the
    // current generation; the coordinator publishes to it on every (re)join.
    let current_generation = Arc::new(AtomicI32::new(generation_id));

    let poll_error = crate::coordinator::PollErrorSlot::default();
    let poll_signal = crate::coordinator::PollSignal::default();
    let rebalance_pending = tokio::sync::watch::Sender::new(false);
    let rebalance_pending_receiver = rebalance_pending.subscribe();
    let close_operation = tokio::sync::watch::Sender::new(GroupMembershipOperation::Default);
    let auto_commit = auto_commit_interval.map(crate::commit::AutoCommit::new);

    let shutdown = CancellationToken::new();
    let published_member_id = tokio::sync::watch::Sender::new(member_id.clone());
    let live_member_id = published_member_id.subscribe();
    let state = CoordinatorState {
        client: coordinator_client,
        group_id: group_id.clone(),
        coordinator_id: Arc::clone(&coordinator_id),
        member_id,
        published_member_id,
        commit_identity: Arc::clone(&commit_identity),
        group_instance_id: group_instance_id.clone(),
        generation_id,
        current_generation: Arc::clone(&current_generation),
        assignor,
        subscribed_topics: subscribe.clone(),
        assigned: Arc::clone(&assigned),
        assignment_changed: Arc::clone(&assignment_changed),
        next_ownership_id,
        next_offsets: Arc::clone(&next_offsets),
        end_offsets: Arc::clone(&end_offsets),
        positions: Arc::clone(&positions),
        topic_ids: Arc::clone(&topic_ids),
        session_timeout,
        max_poll_interval,
        heartbeat_interval,
        subscription_metadata_refresh_interval,
        leave_group_timeout,
        auto_offset_reset,
        client_rack: client_rack.clone(),
        // The metadata snapshot this initial assignment was computed against,
        // threaded to the coordinator so its rejoin baseline starts from
        // exactly what we saw here — not a fresh fetch that could already
        // include a topic created during start-up (which would strand a
        // cold-start empty assignment permanently).
        initial_subscribed_counts: topic_partitions,
        retry_policy: retry_policy.into(),
        poll_error: Arc::clone(&poll_error),
        auto_commit: auto_commit.clone(),
        commit_serialization: Arc::clone(&commit_serialization),
        join_prepared: false,
        polls: poll_signal.subscribe(),
        rebalance_pending,
        close_operation: close_operation.subscribe(),
        rejoin_reason: String::new(),
    };
    // IMPORTANT: `tokio::spawn` is the very last operation — no `.await`
    // follows it.  Dropping a timed-out `start_once` future before this
    // point cancels all in-flight connections without spawning anything.
    let coord_handle = tokio::spawn(crate::coordinator::run(state, shutdown.clone()));

    Ok(Consumer {
        client,
        group_id,
        coordinator_id,
        retry_policy: retry_policy.into(),
        member_id: live_member_id,
        commit_identity,
        commit_serialization,
        commit_async_state,
        group_instance_id: group_instance_id.clone(),
        current_generation,
        subscribed_topics: subscribe,
        assigned,
        assignment_changed,
        next_offsets,
        end_offsets,
        positions,
        pending_seeks,
        topic_ids,
        session_timeout,
        heartbeat_interval,
        assignor,
        coordinator_shutdown: shutdown,
        coordinator_handle: Some(coord_handle),
        isolation_level,
        fetch_min,
        fetch_max,
        fetch_partition_max,
        auto_offset_reset,
        poll_error,
        auto_commit,
        poll_signal,
        rebalance_pending: rebalance_pending_receiver,
        max_poll_records,
        fetch_buffer: crate::fetch_buffer::FetchBuffer::default(),
        close_operation,
    })
}

/// Returns `true` for a transient startup error, where a fresh build after a
/// drop of the half-built consumer is likely to succeed.
///
/// Returns `false` for a permanent misconfig or decode error, so those surface
/// immediately without pointless retries.
fn is_retriable_consumer_start_error(error: &ConsumerError) -> bool {
    // Retry only conditions that a fresh attempt a moment later is likely to
    // clear: transient group-protocol codes (the group is mid-rebalance or the
    // coordinator is warming up / relocating), and a connection dropped
    // mid-join. The lost-wakeup *hang* the retry loop exists to survive is NOT
    // an error here — it never returns; it is caught by the per-attempt
    // timeout. We deliberately do NOT retry `Connect`/`Timeout`: an unreachable
    // or non-responding broker is a genuine fault that must surface promptly
    // (and is the broker's `depends_on: healthy` to prevent at cold boot), not
    // be masked by a long retry storm.
    matches!(
        error,
        // 14 COORDINATOR_LOAD_IN_PROGRESS, 15 COORDINATOR_NOT_AVAILABLE,
        // 16 NOT_COORDINATOR, 22 ILLEGAL_GENERATION, 25 UNKNOWN_MEMBER_ID,
        // 27 REBALANCE_IN_PROGRESS, 79 MEMBER_ID_REQUIRED.
        ConsumerError::Server(14 | 15 | 16 | 22 | 25 | 27 | 79)
            | ConsumerError::Client(krabka_client_core::ClientError::Disconnected)
    ) || matches!(
        error,
        ConsumerError::StartupAfterJoin(inner)
            if is_retriable_consumer_start_error(inner)
                || matches!(
                    inner.as_ref(),
                    ConsumerError::Client(krabka_client_core::ClientError::Timeout(_))
                )
    )
}

impl Consumer {
    /// The consumer's group id.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// The current member id. It is empty while the member is not in the group,
    /// and it changes when the member joins the group again from scratch.
    #[must_use]
    pub fn member_id(&self) -> String {
        self.member_id.borrow().clone()
    }

    /// The current group generation. The coordinator task keeps it live across
    /// rejoins through the shared `Arc<AtomicI32>`.
    #[must_use]
    pub fn generation_id(&self) -> i32 {
        self.current_generation.load(Ordering::Relaxed)
    }

    /// KIP-447 group metadata to hand to a transactional producer's
    /// `send_offsets_to_transaction`.
    ///
    /// The generation id is the coordinator's live generation, which the shared
    /// `Arc<AtomicI32>` keeps current across rejoins.
    #[must_use]
    pub fn group_metadata(&self) -> ConsumerGroupMetadata {
        ConsumerGroupMetadata {
            group_id: self.group_id.clone(),
            generation_id: self.current_generation.load(Ordering::Relaxed),
            member_id: self.member_id(),
            group_instance_id: self.group_instance_id.clone(),
        }
    }

    /// Topics this consumer subscribed to at build time.
    #[must_use]
    pub fn subscribed_topics(&self) -> &[String] {
        &self.subscribed_topics
    }

    /// Snapshot of currently assigned `(topic, partition)` pairs.
    pub async fn assignment(&self) -> Vec<(String, i32)> {
        self.assigned.lock().await.clone()
    }

    /// Whether every assigned partition has consumed through the last end
    /// offset reported by a successful fetch.
    pub async fn at_log_end(&self) -> bool {
        let assigned = self.assigned.lock().await;
        if assigned.is_empty() {
            return false;
        }
        let positions = self.next_offsets.lock().await;
        let ends = self.end_offsets.lock().await;
        assigned.iter().all(|partition| {
            positions
                .get(partition)
                .zip(ends.get(partition))
                .is_some_and(|(position, end)| position >= end)
        })
    }

    /// Close the consumer with [`GroupMembershipOperation::Default`]: commit
    /// with auto commit on, stop the coordinator task, and send `LeaveGroup`
    /// for a dynamic member. A static member stays in the group. See
    /// [`close_with`](Self::close_with).
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn close(self) -> Result<(), ConsumerError> {
        self.close_with(GroupMembershipOperation::Default).await
    }

    /// Close the consumer, and leave the group or stay in it as `operation`
    /// says. Kafka's `KafkaConsumer.close(CloseOptions)`.
    ///
    /// The coordinator itself sends the best-effort `LeaveGroup` as the last
    /// thing it does on shutdown. See `crate::coordinator::run`. It uses its
    /// *live* `member_id`, which can differ from the one captured at build
    /// time, because a from-scratch rejoin (`UNKNOWN_MEMBER_ID`) replaces it.
    /// The cancel and join are prompt, because the coordinator races its
    /// in-tick RPCs against the shutdown token.
    #[tracing::instrument(
        name = "consumer.close",
        level = "info",
        skip_all,
        fields(group_id = %self.group_id, member_id = %self.member_id(), ?operation),
        err
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn close_with(
        mut self,
        operation: GroupMembershipOperation,
    ) -> Result<(), ConsumerError> {
        // Kafka's `ConsumerCoordinator.close` runs `maybeAutoCommitOffsetsSync`
        // before the coordinator leaves the group.
        self.auto_commit_on_close().await;
        self.close_operation.send_replace(operation);
        self.coordinator_shutdown.cancel();
        if let Some(h) = self.coordinator_handle.take() {
            let _ = h.await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod protocol_millis_tests {
    use assert2::check;
    use krabka_units::{Time, convert::TimeExt as _, millis, minutes, secs};

    use super::protocol_millis_i32;

    /// `session_timeout_ms` and `rebalance_timeout_ms` are `int32` wire fields
    /// that the group coordinator range-checks. The pre-quantity code reached
    /// them through `Duration::as_millis`, which truncates, so a fractional
    /// millisecond must still round *down*. `TimeExt::millis_i32` rounds to
    /// nearest and would report one millisecond more.
    #[test]
    fn protocol_millis_truncates_rather_than_rounding() {
        for (_name, value, expected) in [
            ("whole default session timeout", secs(45), 45_000),
            ("whole default rebalance timeout", minutes(1), 60_000),
            ("whole millisecond", millis(37), 37),
            (
                "fractional millisecond truncates down",
                Time::from_secs_f64(0.0016),
                1,
            ),
            (
                "fractional millisecond that would round up",
                Time::from_secs_f64(1.9999),
                1_999,
            ),
            ("below one millisecond", Time::from_secs_f64(0.0009), 0),
        ] {
            check!(protocol_millis_i32(value) == expected);
        }
    }

    /// The code keeps the saturation that the
    /// `i32::try_from(...).unwrap_or(i32::MAX)` guard provided. An absurd
    /// timeout clamps and does not wrap negative on the wire.
    #[test]
    fn protocol_millis_saturates_at_i32_max() {
        check!(protocol_millis_i32(Time::from_secs(10_000_000_000)) == i32::MAX);
    }
}

#[cfg(test)]
mod consumer_retry_policy_tests {
    use assert2::{assert, check};
    use krabka_units::{Time, convert::TimeExt as _, millis, minutes, secs};

    use super::ConsumerRetryPolicy;

    #[test]
    fn consumer_retry_policy_defaults_and_overrides() {
        let defaults = ConsumerRetryPolicy::default();
        check!(defaults.startup_attempt_timeout() == secs(90));
        check!(defaults.startup_deadline() == minutes(5));
        check!(defaults.startup_initial_backoff() == millis(500));
        check!(defaults.startup_max_backoff() == secs(5));
        check!(defaults.coordinator_retry_timeout() == secs(30));
        check!(defaults.coordinator_initial_backoff() == millis(100));
        check!(defaults.coordinator_max_backoff() == secs(1));

        let configured = ConsumerRetryPolicy::new(
            secs(11),
            secs(12),
            millis(13),
            millis(14),
            secs(15),
            millis(16),
            millis(17),
        )
        .expect("valid retry policy");
        check!(configured.startup_attempt_timeout() == secs(11));
        check!(configured.startup_deadline() == secs(12));
        check!(configured.startup_initial_backoff() == millis(13));
        check!(configured.startup_max_backoff() == millis(14));
        check!(configured.coordinator_retry_timeout() == secs(15));
        check!(configured.coordinator_initial_backoff() == millis(16));
        check!(configured.coordinator_max_backoff() == millis(17));
    }

    #[test]
    fn consumer_retry_policy_rejects_invalid_values() {
        let valid = [
            secs(90),
            minutes(5),
            millis(500),
            secs(5),
            secs(30),
            millis(100),
            secs(1),
        ];
        for (index, invalid) in [
            Time::ZERO,
            Time::from_secs_f64(0.0005),
            Time::from_secs_f64(f64::INFINITY),
        ]
        .into_iter()
        .enumerate()
        {
            let mut values = valid;
            values[index] = invalid;
            assert!(
                ConsumerRetryPolicy::new(
                    values[0], values[1], values[2], values[3], values[4], values[5], values[6],
                )
                .is_err()
            );
        }

        for values in [
            [
                secs(13),
                secs(12),
                millis(1),
                millis(2),
                secs(3),
                millis(1),
                millis(2),
            ],
            [
                secs(1),
                secs(2),
                millis(3),
                millis(2),
                secs(3),
                millis(1),
                millis(2),
            ],
            [
                secs(1),
                secs(2),
                millis(1),
                millis(2),
                secs(3),
                millis(3),
                millis(2),
            ],
        ] {
            assert!(
                ConsumerRetryPolicy::new(
                    values[0], values[1], values[2], values[3], values[4], values[5], values[6],
                )
                .is_err()
            );
        }
    }
}

#[cfg(test)]
mod security_arg_tests {
    use assert2::check;
    use krabka_client_core::{
        ClientError, MockBroker,
        security::{ClientSecurity, SaslCredentials},
    };
    use krabka_protocol::{
        Encode, UnknownTaggedFields,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
            leave_group_request,
            leave_group_response::{self, LeaveGroupResponse},
        },
    };
    use krabka_security::ListenerProtocol;

    use super::*;
    use crate::builder::DecodedSubscription;

    fn api_versions_for_startup_cleanup() -> Vec<u8> {
        let resp = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 3,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: leave_group_request::API_KEY,
                    min_version: 0,
                    max_version: leave_group_request::MAX_VERSION,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    fn leave_group_response_at(version: i16) -> Vec<u8> {
        let mut buf = bytes::BytesMut::new();
        if version >= leave_group_response::FLEXIBLE_MIN {
            buf.extend_from_slice(&[0x00]);
        }
        LeaveGroupResponse::default()
            .encode(&mut buf, version)
            .unwrap();
        buf.to_vec()
    }

    #[test]
    fn leave_group_timeout_uses_default_and_valid_override() {
        let default = ConsumerLeaveGroupTimeout::default();
        assert2::assert!(default.duration() == Duration::from_secs(5));
        assert2::assert!(default.milliseconds() == 5_000);

        let timeout = ConsumerLeaveGroupTimeout::new(Duration::from_millis(37))
            .expect("positive whole milliseconds");
        assert2::assert!(timeout.duration() == Duration::from_millis(37));
        assert2::assert!(timeout.milliseconds() == 37);
    }

    #[test]
    fn leave_group_timeout_validates_millisecond_boundaries() {
        assert2::assert!(ConsumerLeaveGroupTimeout::new(Duration::ZERO).is_err());
        assert2::assert!(
            ConsumerLeaveGroupTimeout::new(Duration::from_millis(1) + Duration::from_nanos(1))
                .is_err()
        );
        assert2::assert!(ConsumerLeaveGroupTimeout::new(Duration::from_millis(u64::MAX)).is_ok());
        assert2::assert!(ConsumerLeaveGroupTimeout::new(Duration::from_secs(u64::MAX)).is_err());
    }

    #[test]
    fn subscription_metadata_refresh_interval_uses_default_and_valid_override() {
        let default = ConsumerSubscriptionMetadataRefreshInterval::default();
        assert2::assert!(default.duration() == Duration::from_secs(5));
        assert2::assert!(default.milliseconds() == 5_000);

        let interval = ConsumerSubscriptionMetadataRefreshInterval::new(Duration::from_millis(37))
            .expect("positive whole milliseconds");
        assert2::assert!(interval.duration() == Duration::from_millis(37));
        assert2::assert!(interval.milliseconds() == 37);
    }

    #[test]
    fn subscription_metadata_refresh_interval_validates_millisecond_boundaries() {
        assert2::assert!(ConsumerSubscriptionMetadataRefreshInterval::new(Duration::ZERO).is_err());
        assert2::assert!(
            ConsumerSubscriptionMetadataRefreshInterval::new(
                Duration::from_millis(1) + Duration::from_nanos(1)
            )
            .is_err()
        );
        assert2::assert!(
            ConsumerSubscriptionMetadataRefreshInterval::new(Duration::from_millis(u64::MAX))
                .is_ok()
        );
        assert2::assert!(
            ConsumerSubscriptionMetadataRefreshInterval::new(Duration::from_secs(u64::MAX))
                .is_err()
        );
    }

    #[tokio::test]
    async fn invalid_subscription_metadata_refresh_interval_fails_before_broker_lookup() {
        let error = Consumer::builder()
            .bootstrap("invalid.invalid:9092")
            .group_id("metadata-refresh-validation")
            .subscribe(["topic".to_owned()])
            .subscription_metadata_refresh_interval(krabka_units::secs(0))
            .build()
            .await
            .err()
            .expect("invalid configuration");

        assert2::assert!(
            error
                .to_string()
                .contains("consumer subscription metadata refresh interval")
        );
    }

    #[tokio::test]
    async fn invalid_leave_group_timeout_fails_before_broker_lookup() {
        let error = Consumer::builder()
            .bootstrap("invalid.invalid:9092")
            .group_id("leave-validation")
            .subscribe(["topic".to_owned()])
            .leave_group_timeout(krabka_units::secs(0))
            .build()
            .await
            .err()
            .expect("invalid configuration");

        assert2::assert!(error.to_string().contains("consumer leave-group timeout"));
    }

    #[tokio::test]
    async fn invalid_client_resource_policy_fails_before_broker_lookup() {
        let error = Consumer::builder()
            .bootstrap("invalid.invalid:9092")
            .group_id("client-policy-validation")
            .subscribe(["topic".to_owned()])
            .dispatch_queue_capacity(0)
            .build()
            .await
            .err()
            .expect("invalid configuration");

        assert2::assert!(error.to_string().contains("client dispatch queue capacity"));
    }

    #[tokio::test]
    async fn startup_member_cleanup_leaves_the_group_for_a_dynamic_member_only() {
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, group_instance_id, expected) in [
            ("dynamic member", None, true),
            ("static member", Some("instance-a".to_owned()), false),
        ] {
            let saw_leave = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let saw_leave_in_mock = Arc::clone(&saw_leave);
            let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    Some(api_versions_for_startup_cleanup())
                } else if api_key == leave_group_request::API_KEY {
                    saw_leave_in_mock.store(true, Ordering::SeqCst);
                    Some(leave_group_response_at(version))
                } else {
                    None
                }
            })
            .await;

            let client = Client::builder()
                .bootstrap(mock.addr.to_string())
                .request_timeout(krabka_units::millis(100))
                .build()
                .await
                .unwrap();
            let coordinator_id = AtomicI32::new(0);

            leave_startup_member(
                &client,
                &coordinator_id,
                "group-a",
                "member-a",
                group_instance_id,
                krabka_units::millis(37),
            )
            .await;

            mock.stop();
            actual.push((name, saw_leave.load(Ordering::SeqCst)));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    #[tokio::test]
    async fn startup_member_cleanup_bounds_stalled_leave_with_configured_timeout() {
        let saw_leave = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_leave_in_mock = Arc::clone(&saw_leave);
        let mock = MockBroker::start(move |api_key, _version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                Some(api_versions_for_startup_cleanup())
            } else if api_key == leave_group_request::API_KEY {
                saw_leave_in_mock.store(true, Ordering::SeqCst);
                None
            } else {
                None
            }
        })
        .await;

        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(krabka_units::secs(5))
            .build()
            .await
            .expect("client");
        let coordinator_id = AtomicI32::new(0);

        tokio::time::timeout(
            Duration::from_secs(1),
            leave_startup_member(
                &client,
                &coordinator_id,
                "group-a",
                "member-a",
                None,
                krabka_units::millis(37),
            ),
        )
        .await
        .expect("configured leave deadline bounds cleanup");

        mock.stop();
        assert2::assert!(saw_leave.load(Ordering::SeqCst));
    }

    #[test]
    fn initial_subscription_uses_unknown_generation_and_empty_owned_partitions() {
        let bytes = initial_subscription_bytes(&["topic".into()], Some("rack-a"));
        let decoded = decode_subscription(&bytes);

        check!(
            decoded
                == DecodedSubscription {
                    topics: vec!["topic".into()],
                    owned: Vec::new(),
                    generation_id: -1,
                    rack_id: Some("rack-a".into()),
                }
        );
    }

    #[test]
    fn startup_join_request_preserves_group_member_timeouts_protocol_and_empty_reason() {
        let metadata = bytes::Bytes::from_static(b"metadata");
        let req = startup_join_fields(
            "group-a",
            Some("instance-a"),
            "range",
            metadata.clone(),
            (45_000, 60_000),
        )
        .request("member-a".into());

        assert2::assert!(
            req == JoinGroupRequest {
                group_id: "group-a".into(),
                session_timeout_ms: 45_000,
                rebalance_timeout_ms: 60_000,
                member_id: "member-a".into(),
                group_instance_id: Some("instance-a".into()),
                protocol_type: "consumer".into(),
                protocols: vec![JoinGroupRequestProtocol {
                    name: "range".into(),
                    metadata,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                reason: Some(String::new()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
        );
    }

    #[test]
    fn first_join_member_id_accepts_required_or_success_and_rejects_errors() {
        enum Expected<'a> {
            Member(&'a str),
            Server(i16),
            RebalanceFailed,
        }
        for (_name, error_code, member_id, expected) in [
            (
                "member id required",
                79,
                "member-a",
                Expected::Member("member-a"),
            ),
            ("success", 0, "member-b", Expected::Member("member-b")),
            ("server error", 42, "member-c", Expected::Server(42)),
            ("empty member", 0, "", Expected::RebalanceFailed),
        ] {
            let response = JoinGroupResponse {
                error_code,
                member_id: member_id.into(),
                ..Default::default()
            };
            let actual = first_join_member_id(&response);
            let matches = match expected {
                Expected::Member(expected) => matches!(actual, Ok(actual) if actual == expected),
                Expected::Server(expected) => {
                    matches!(actual, Err(ConsumerError::Server(code)) if code == expected)
                }
                Expected::RebalanceFailed => {
                    matches!(actual, Err(ConsumerError::RebalanceFailed(_)))
                }
            };
            assert2::assert!(matches);
        }
    }

    #[test]
    fn is_subscribed_topic_matches_exact_topic_names() {
        let subscribe = vec!["orders".to_string(), "payments".to_string()];

        for (name, expected) in [
            ("orders", true),
            ("payments", true),
            ("shipments", false),
            ("order", false),
        ] {
            assert2::assert!(is_subscribed_topic(&subscribe, name) == expected);
        }
    }

    #[test]
    fn is_group_leader_matches_exact_member_id() {
        for (_name, leader, member_id, expected) in [
            ("exact leader", "member-a", "member-a", true),
            ("different member", "member-a", "member-b", false),
            ("empty member", "member-a", "", false),
        ] {
            assert2::assert!(is_group_leader(leader, member_id) == expected);
        }
    }

    #[test]
    fn build_sync_assignment_preserves_member_and_assignment_payload() {
        let assignment = build_sync_assignment("member-a".into(), &[("orders".into(), 3)]);
        let decoded = decode_assignment(&assignment.assignment);

        assert2::assert!(
            (assignment.member_id.as_str(), decoded) == ("member-a", vec![("orders".into(), 3)])
        );
    }

    #[test]
    fn build_sync_request_preserves_group_generation_member_protocol_and_assignments() {
        let assignment = build_sync_assignment("member-a".into(), &[("orders".into(), 3)]);
        let req = build_sync_request(
            "group-a".into(),
            7,
            "member-a".into(),
            Some("instance-a".into()),
            "range".into(),
            vec![assignment.clone()],
        );

        assert2::assert!(
            req == SyncGroupRequest {
                group_id: "group-a".into(),
                generation_id: 7,
                member_id: "member-a".into(),
                group_instance_id: Some("instance-a".into()),
                protocol_type: Some("consumer".into()),
                protocol_name: Some("range".into()),
                assignments: vec![assignment],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
        );
    }

    #[test]
    fn offset_prime_helpers_preserve_assignment_presence_offsets_and_epochs() {
        for (_name, partitions, expected) in [
            ("empty assignment", vec![], false),
            ("assigned partition", vec![("orders".to_string(), 0)], true),
        ] {
            assert2::assert!(has_assigned_partitions(&partitions) == expected);
        }

        for (_name, committed, reset, expected) in [
            ("committed offset", 12, AutoOffsetReset::Earliest, 12),
            ("missing earliest", -1, AutoOffsetReset::Earliest, 0),
            ("missing latest", -1, AutoOffsetReset::Latest, i64::MAX),
            ("missing none", -1, AutoOffsetReset::None, i64::MAX),
        ] {
            assert2::assert!(starting_offset(committed, reset) == expected);
        }

        let position = primed_position(9);
        assert2::assert!(
            position
                == crate::position::PartitionPosition {
                    offset_epoch: krabka_ids::LeaderEpoch(9),
                    leader_id: -1,
                    leader_epoch: krabka_ids::LeaderEpoch(-1),
                    awaiting_validation: false,
                }
        );
    }

    #[tokio::test]
    async fn consumer_builder_uses_request_timeout_for_initial_connect_handshake() {
        let mock = MockBroker::start(|_api_key, _version, _corr_id, _body| None).await;

        let build = Consumer::builder()
            .bootstrap(mock.addr.to_string())
            .client_id("timeout-consumer")
            .group_id("timeout-group")
            .subscribe(vec!["orders".to_string()])
            .request_timeout(krabka_units::millis(100))
            .build();
        let res = tokio::time::timeout(Duration::from_millis(500), build).await;

        mock.stop();

        let Err(err) = res.expect("consumer build should not retain the 30s connect timeout")
        else {
            panic!("silent broker must time out during build")
        };
        assert2::assert!(matches!(
            err,
            ConsumerError::Client(ClientError::Timeout(d))
                if d == krabka_units::millis(100)
        ));
    }

    #[tokio::test]
    async fn consumer_builder_rejects_non_positive_fetch_budgets_before_network_io() {
        let min = Consumer::builder()
            .bootstrap("127.0.0.1:1")
            .group_id("timeout-group")
            .subscribe(vec!["orders".to_string()])
            .fetch_min(krabka_units::bytes(0))
            .build()
            .await;
        assert2::assert!(matches!(min, Err(ConsumerError::RebalanceFailed(_))));

        let max = Consumer::builder()
            .bootstrap("127.0.0.1:1")
            .group_id("timeout-group")
            .subscribe(vec!["orders".to_string()])
            .fetch_max(krabka_units::bytes(0))
            .build()
            .await;
        assert2::assert!(matches!(max, Err(ConsumerError::RebalanceFailed(_))));

        let partition_max = Consumer::builder()
            .bootstrap("127.0.0.1:1")
            .group_id("timeout-group")
            .subscribe(vec!["orders".to_string()])
            .fetch_partition_max(krabka_units::bytes(0))
            .build()
            .await;
        assert2::assert!(matches!(
            partition_max,
            Err(ConsumerError::RebalanceFailed(_))
        ));

        let inverted = Consumer::builder()
            .bootstrap("127.0.0.1:1")
            .group_id("timeout-group")
            .subscribe(vec!["orders".to_string()])
            .fetch_min(krabka_units::bytes(2))
            .fetch_max(krabka_units::bytes(1))
            .build()
            .await;
        assert2::assert!(matches!(
            inverted,
            Err(ConsumerError::RebalanceFailed(message))
                if message == "consumer fetch min must not exceed consumer fetch max"
        ));
    }

    #[test]
    fn consumer_fetch_byte_settings_validate_and_convert_to_uom() {
        for value in [1, i32::MAX] {
            let size = ByteSize::from_bytes_i64(i64::from(value));
            let max = ConsumerFetchMaxBytes::try_from(size).expect("positive fetch maximum");
            let partition_max = ConsumerFetchPartitionMaxBytes::try_from(size)
                .expect("positive partition fetch maximum");

            check!(max.bytes() == value);
            check!(partition_max.bytes() == value);
            check!(max.size().bytes_i32() == value);
            check!(partition_max.size().bytes_i32() == value);
        }

        for invalid in [
            ByteSize::ZERO,
            ByteSize::from_bytes_f64(-1.0),
            ByteSize::from_bytes_f64(1.5),
            ByteSize::from_bytes_f64(f64::from(i32::MAX) + 1.0),
        ] {
            check!(ConsumerFetchMaxBytes::try_from(invalid).is_err());
            check!(ConsumerFetchPartitionMaxBytes::try_from(invalid).is_err());
        }
    }

    async fn test_consumer() -> Consumer {
        let client = Client::builder()
            .bootstrap("127.0.0.1:1")
            .client_id("test-client")
            .build()
            .await
            .unwrap();
        Consumer {
            client,
            group_id: "group-a".into(),
            coordinator_id: Arc::new(AtomicI32::new(3)),
            retry_policy: ConsumerRetryPolicy::default().into(),
            member_id: tokio::sync::watch::channel("member-a".to_owned()).1,
            commit_identity: Arc::new(Mutex::new(CommitIdentity {
                generation: 7,
                member_id: "member-a".into(),
                ownership_ids: HashMap::from([(("orders".into(), 0), 1)]),
                rejoin_on_poll: false,
            })),
            commit_serialization: Arc::new(Mutex::new(())),
            commit_async_state: Arc::new(AtomicU8::new(0)),
            group_instance_id: Some("instance-a".into()),
            current_generation: Arc::new(AtomicI32::new(7)),
            subscribed_topics: vec!["orders".into(), "payments".into()],
            assigned: Arc::new(Mutex::new(vec![("orders".into(), 0)])),
            assignment_changed: Arc::new(Notify::new()),
            next_offsets: Arc::new(Mutex::new(HashMap::new())),
            end_offsets: Arc::new(Mutex::new(HashMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
            pending_seeks: Arc::new(Mutex::new(HashMap::new())),
            topic_ids: Arc::new(Mutex::new(HashMap::new())),
            session_timeout: secs(45),
            heartbeat_interval: secs(3),
            assignor: Assignor::Range,
            coordinator_shutdown: CancellationToken::new(),
            coordinator_handle: None,
            isolation_level: IsolationLevel::ReadUncommitted,
            fetch_min: krabka_client_core::DEFAULT_FETCH_MIN,
            fetch_max: crate::poll::DEFAULT_FETCH_MAX,
            fetch_partition_max: crate::poll::DEFAULT_FETCH_PARTITION_MAX,
            auto_offset_reset: AutoOffsetReset::Latest,
            poll_error: crate::coordinator::PollErrorSlot::default(),
            auto_commit: None,
            poll_signal: crate::coordinator::PollSignal::default(),
            rebalance_pending: tokio::sync::watch::channel(false).1,
            max_poll_records: DEFAULT_CONSUMER_MAX_POLL_RECORDS,
            fetch_buffer: crate::fetch_buffer::FetchBuffer::default(),
            close_operation: tokio::sync::watch::Sender::new(GroupMembershipOperation::Default),
        }
    }

    #[tokio::test]
    async fn accessors_return_consumer_identity_subscription_and_assignment() {
        let consumer = test_consumer().await;

        check!(
            (
                consumer.group_id(),
                consumer.member_id(),
                consumer.generation_id(),
                consumer.subscribed_topics(),
                consumer.assignment().await,
            ) == (
                "group-a",
                "member-a".to_owned(),
                7,
                &["orders".to_string(), "payments".to_string()][..],
                vec![("orders".into(), 0)],
            )
        );

        let metadata = consumer.group_metadata();
        assert2::assert!(
            metadata
                == ConsumerGroupMetadata {
                    group_id: "group-a".into(),
                    generation_id: 7,
                    member_id: "member-a".into(),
                    group_instance_id: Some("instance-a".into()),
                }
        );
    }

    #[tokio::test]
    async fn log_end_requires_every_assigned_position_to_reach_a_reported_end() {
        let consumer = test_consumer().await;
        assert2::assert!(!consumer.at_log_end().await);

        consumer
            .next_offsets
            .lock()
            .await
            .insert(("orders".into(), 0), 12);
        consumer
            .end_offsets
            .lock()
            .await
            .insert(("orders".into(), 0), 12);
        assert2::assert!(consumer.at_log_end().await);

        consumer.seek("orders", 0, 0).await.unwrap();
        assert2::assert!(!consumer.at_log_end().await);
    }

    /// Regression: the generation that the commit path stamps must track the
    /// coordinator's joins and rejoins, not a start-up snapshot. The coordinator
    /// is the sole writer and publishes through the shared `current_generation`
    /// atomic. The accessor, the group metadata, and the commit path all read it
    /// live, so a commit issued after a rebalance carries the CURRENT generation
    /// instead of the stale one that the broker rejects with
    /// `ILLEGAL_GENERATION (22)`.
    #[tokio::test]
    async fn generation_tracks_coordinator_rejoins_via_shared_atomic() {
        let consumer = test_consumer().await;
        assert2::assert!(
            (
                consumer.generation_id(),
                consumer.group_metadata().generation_id
            ) == (7, 7)
        );

        // Simulate the coordinator publishing a new generation on rejoin.
        consumer.current_generation.store(11, Ordering::Relaxed);

        assert2::assert!(
            (
                consumer.generation_id(),
                consumer.group_metadata().generation_id
            ) == (11, 11)
        );
    }

    #[tokio::test]
    async fn close_cancels_shutdown_token_even_without_spawned_handle() {
        let consumer = test_consumer().await;
        let shutdown = consumer.coordinator_shutdown.clone();

        consumer.close().await.unwrap();

        assert2::assert!(shutdown.is_cancelled());
    }

    // --- is_retriable_consumer_start_error ---

    #[test]
    fn retriable_error_classification() {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        use krabka_client_core::ClientError;

        // Transient group-protocol server codes.
        let transient_codes: &[i16] = &[14, 15, 16, 22, 25, 27, 79];
        for &code in transient_codes {
            assert2::assert!(is_retriable_consumer_start_error(&ConsumerError::Server(
                code
            )));
        }

        // Non-transient server code (e.g. INVALID_REQUEST = 42).
        assert2::assert!(!is_retriable_consumer_start_error(&ConsumerError::Server(
            42
        )));

        for (_name, error, expected) in [
            // A connection dropped mid-join is transient — retry a fresh attempt.
            (
                "disconnected",
                ConsumerError::Client(ClientError::Disconnected),
                true,
            ),
            // Connect/Timeout are NOT retried: an unreachable or non-responding
            // broker is a genuine fault that must surface promptly (the lost-wakeup
            // hang the retry loop survives never returns Timeout — it is caught by
            // the per-attempt timeout, not classified here).
            (
                "timeout",
                ConsumerError::Client(ClientError::Timeout(krabka_units::secs(1))),
                false,
            ),
            (
                "startup after join",
                ConsumerError::StartupAfterJoin(Box::new(ConsumerError::Client(
                    ClientError::Timeout(krabka_units::secs(1)),
                ))),
                true,
            ),
            (
                "connect refused",
                ConsumerError::Client(ClientError::Connect {
                    addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092),
                    source: std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused"),
                }),
                false,
            ),
            // Permanent misconfig errors — must NOT be retriable.
            ("not subscribed", ConsumerError::NotSubscribed, false),
            (
                "rebalance failed",
                ConsumerError::RebalanceFailed("group_id required".into()),
                false,
            ),
            (
                "incompatible version",
                ConsumerError::Client(ClientError::IncompatibleVersion {
                    api_key: 0,
                    broker_min: 0,
                    broker_max: 5,
                    client_min: 7,
                    client_max: 10,
                }),
                false,
            ),
        ] {
            assert2::assert!(is_retriable_consumer_start_error(&error) == expected);
        }
    }

    #[tokio::test]
    async fn consumer_builder_accepts_security() {
        let security = ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain {
                username: "u".into(),
                password: "p".into(),
            }),
            sasl_host: None,
        };
        // 127.0.0.1:1 is unroutable; the consumer build connects eagerly
        // (JoinGroup), so it must fail — proving the security arg is
        // threaded (not a type error).
        let res = Consumer::builder()
            .bootstrap("127.0.0.1:1")
            .group_id("g")
            .subscribe(vec!["t".to_string()])
            .security(security)
            .build()
            .await;
        assert2::assert!(res.is_err());
    }
}

#[cfg(test)]
mod auto_commit_tests {
    use std::{collections::VecDeque, sync::atomic::AtomicI16};

    use bytes::Buf as _;
    use krabka_client_core::MockBroker;
    use krabka_protocol::{
        Decode as _, Encode, UnknownTaggedFields,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            fetch_request,
            fetch_response::FetchResponse,
            find_coordinator_request,
            find_coordinator_response::FindCoordinatorResponse,
            heartbeat_request,
            heartbeat_response::HeartbeatResponse,
            join_group_request::{self, JoinGroupRequest},
            leave_group_request::{self, LeaveGroupRequest},
            leave_group_response::LeaveGroupResponse,
            metadata_request,
            metadata_response::MetadataResponse,
            offset_commit_request::{
                self, OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
            },
            offset_commit_response::{
                OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
            },
            offset_fetch_request,
            offset_fetch_response::OffsetFetchResponse,
            sync_group_request,
        },
    };

    use super::*;

    const GROUP: &str = "group-a";
    pub(super) const MEMBER: &str = "member-a";
    pub(super) const TOPIC: &str = "orders";
    /// `REBALANCE_IN_PROGRESS`.
    const REBALANCE_IN_PROGRESS: i16 = 27;
    const AUTO_COMMIT_INTERVAL: Duration = Duration::from_secs(5);
    const SESSION_TIMEOUT: Duration = Duration::from_secs(45);
    /// How long a rebalance step waits for the `SyncGroup` requests. The first
    /// heartbeat comes after 3 s.
    const REBALANCE_WAIT: Duration = Duration::from_secs(30);

    /// A group request that the mock coordinator received, or an event of the
    /// application, in arrival order.
    #[derive(Clone, Debug, PartialEq)]
    pub(super) enum GroupRequest {
        OffsetCommit(OffsetCommitRequest),
        JoinGroup,
        SyncGroup,
        LeaveGroup,
        /// `commit_sync` returned to the application.
        CommitSyncReturned,
        /// The coordinator did not receive the expected `SyncGroup` requests in
        /// the time of the step.
        SyncGroupLate,
        /// `close` did not return in the time of the step.
        CloseTimedOut,
        /// The `JoinGroup` came more than the session timeout after the last
        /// heartbeat. A broker would have removed the member.
        SessionExpired,
    }

    /// How the mock coordinator answers one `OffsetCommit`.
    #[derive(Clone, Copy, Debug)]
    enum CommitReply {
        /// Send no response.
        Drop,
        /// Answer each partition with this error code.
        Error(i16),
    }

    /// The API versions that the mock advertises: `(api_key, min, max)`. Each
    /// maximum is below the flexible version of its API, so no response needs a
    /// tagged response header.
    const API_VERSIONS: [(i16, i16, i16); 10] = [
        (api_versions_request::API_KEY, 0, 3),
        (metadata_request::API_KEY, 0, 8),
        (find_coordinator_request::API_KEY, 0, 2),
        (join_group_request::API_KEY, 0, 8),
        (sync_group_request::API_KEY, 0, 3),
        (heartbeat_request::API_KEY, 0, 3),
        (leave_group_request::API_KEY, 0, 5),
        (offset_commit_request::API_KEY, 2, 7),
        (offset_fetch_request::API_KEY, 1, 5),
        (fetch_request::API_KEY, 4, 11),
    ];

    /// A group coordinator that records every group request. It answers each
    /// one with success, except the `OffsetCommit` requests in
    /// `commit_replies`. It does not answer `FindCoordinator`.
    pub(super) struct MockCoordinator {
        protocol: &'static str,
        requests: std::sync::Mutex<Vec<GroupRequest>>,
        /// The error code of the next `Heartbeat` response. The mock sends it
        /// once and then answers `0`.
        pub(super) heartbeat_error: AtomicI16,
        /// The assignment of each `SyncGroup` response. The mock repeats the
        /// last one.
        assignments: std::sync::Mutex<VecDeque<Vec<(String, i32)>>>,
        generation: AtomicI32,
        /// The answers to the next `OffsetCommit` requests. An empty queue
        /// answers with success.
        commit_replies: std::sync::Mutex<VecDeque<CommitReply>>,
        /// When `true`, the next `OffsetCommit` makes the next `Heartbeat`
        /// answer `REBALANCE_IN_PROGRESS`.
        rebalance_on_commit: std::sync::atomic::AtomicBool,
        /// When the last `Heartbeat` or `JoinGroup` came.
        pub(super) last_heartbeat: std::sync::Mutex<tokio::time::Instant>,
        /// When `true`, the mock does not answer a `Heartbeat` without an error
        /// in `heartbeat_error`.
        drop_heartbeats: std::sync::atomic::AtomicBool,
        /// Each decoded `JoinGroup` request.
        pub(super) joins: std::sync::Mutex<Vec<JoinGroupRequest>>,
        /// Each decoded `LeaveGroup` request.
        pub(super) leaves: std::sync::Mutex<Vec<LeaveGroupRequest>>,
        /// When `true`, the mock does not answer `LeaveGroup`.
        pub(super) drop_leaves: std::sync::atomic::AtomicBool,
    }

    fn encode(response: &impl Encode, version: i16) -> Vec<u8> {
        let mut buf = bytes::BytesMut::new();
        response.encode(&mut buf, version).expect("encode response");
        buf.to_vec()
    }

    impl MockCoordinator {
        /// A coordinator at generation 1 whose `SyncGroup` responses carry
        /// `assignments`.
        pub(super) fn new(assignor: Assignor, assignments: Vec<Vec<(String, i32)>>) -> Arc<Self> {
            Arc::new(Self {
                protocol: assignor.protocol_name(),
                requests: std::sync::Mutex::new(Vec::new()),
                heartbeat_error: AtomicI16::new(0),
                assignments: std::sync::Mutex::new(assignments.into()),
                generation: AtomicI32::new(1),
                commit_replies: std::sync::Mutex::new(VecDeque::new()),
                rebalance_on_commit: std::sync::atomic::AtomicBool::new(false),
                last_heartbeat: std::sync::Mutex::new(tokio::time::Instant::now()),
                drop_heartbeats: std::sync::atomic::AtomicBool::new(false),
                joins: std::sync::Mutex::new(Vec::new()),
                leaves: std::sync::Mutex::new(Vec::new()),
                drop_leaves: std::sync::atomic::AtomicBool::new(false),
            })
        }

        fn record(&self, request: GroupRequest) {
            self.requests.lock().expect("requests lock").push(request);
        }

        pub(super) fn requests(&self) -> Vec<GroupRequest> {
            self.requests.lock().expect("requests lock").clone()
        }

        fn sync_groups(&self) -> usize {
            self.requests()
                .iter()
                .filter(|request| **request == GroupRequest::SyncGroup)
                .count()
        }

        pub(super) fn respond(
            &self,
            api_key: i16,
            version: i16,
            mut body: &[u8],
        ) -> Option<Vec<u8>> {
            match api_key {
                api_versions_request::API_KEY => Some(encode(
                    &ApiVersionsResponse {
                        api_keys: API_VERSIONS
                            .iter()
                            .map(|(api_key, min_version, max_version)| ApiVersion {
                                api_key: *api_key,
                                min_version: *min_version,
                                max_version: *max_version,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    },
                    0,
                )),
                metadata_request::API_KEY => Some(encode(&MetadataResponse::default(), version)),
                heartbeat_request::API_KEY => {
                    *self.last_heartbeat.lock().expect("last heartbeat lock") =
                        tokio::time::Instant::now();
                    let error_code = self.heartbeat_error.swap(0, Ordering::SeqCst);
                    if error_code == 0 && self.drop_heartbeats.load(Ordering::SeqCst) {
                        return None;
                    }
                    Some(encode(
                        &HeartbeatResponse {
                            error_code,
                            ..Default::default()
                        },
                        version,
                    ))
                }
                join_group_request::API_KEY => {
                    let mut last_heartbeat =
                        self.last_heartbeat.lock().expect("last heartbeat lock");
                    if last_heartbeat.elapsed() > SESSION_TIMEOUT {
                        self.record(GroupRequest::SessionExpired);
                    }
                    *last_heartbeat = tokio::time::Instant::now();
                    drop(last_heartbeat);
                    self.record(GroupRequest::JoinGroup);
                    let client_id_len = body.get_i16();
                    body.advance(usize::try_from(client_id_len.max(0)).expect("client id length"));
                    let flexible = version >= join_group_request::FLEXIBLE_MIN;
                    if flexible {
                        // The empty tagged fields of the request header.
                        body.advance(1);
                    }
                    self.joins
                        .lock()
                        .expect("joins lock")
                        .push(JoinGroupRequest::decode(&mut body, version).expect("decode join"));
                    let mut response = if flexible { vec![0] } else { Vec::new() };
                    response.extend(encode(
                        &JoinGroupResponse {
                            generation_id: self.generation.fetch_add(1, Ordering::SeqCst) + 1,
                            protocol_name: Some(self.protocol.to_owned()),
                            protocol_type: Some("consumer".into()),
                            leader: "leader".into(),
                            member_id: MEMBER.into(),
                            ..Default::default()
                        },
                        version,
                    ));
                    Some(response)
                }
                sync_group_request::API_KEY => {
                    self.record(GroupRequest::SyncGroup);
                    let mut assignments = self.assignments.lock().expect("assignments lock");
                    let assignment = if assignments.len() > 1 {
                        assignments.pop_front().expect("assignment")
                    } else {
                        assignments.front().cloned().expect("assignment")
                    };
                    drop(assignments);
                    Some(encode(
                        &SyncGroupResponse {
                            assignment: crate::builder::encode_assignment(&assignment),
                            ..Default::default()
                        },
                        version,
                    ))
                }
                // No partition has records.
                fetch_request::API_KEY => Some(encode(&FetchResponse::default(), version)),
                // No partition has a committed offset.
                offset_fetch_request::API_KEY => {
                    Some(encode(&OffsetFetchResponse::default(), version))
                }
                leave_group_request::API_KEY => {
                    self.record(GroupRequest::LeaveGroup);
                    let client_id_len = body.get_i16();
                    body.advance(usize::try_from(client_id_len.max(0)).expect("client id length"));
                    let flexible = version >= leave_group_request::FLEXIBLE_MIN;
                    if flexible {
                        // The empty tagged fields of the request header.
                        body.advance(1);
                    }
                    self.leaves
                        .lock()
                        .expect("leaves lock")
                        .push(LeaveGroupRequest::decode(&mut body, version).expect("decode leave"));
                    if self.drop_leaves.load(Ordering::SeqCst) {
                        return None;
                    }
                    let mut response = if flexible { vec![0] } else { Vec::new() };
                    response.extend(encode(&LeaveGroupResponse::default(), version));
                    Some(response)
                }
                find_coordinator_request::API_KEY => Some(encode(
                    &FindCoordinatorResponse {
                        node_id: 0,
                        ..Default::default()
                    },
                    version,
                )),
                offset_commit_request::API_KEY => {
                    let client_id_len = body.get_i16();
                    body.advance(usize::try_from(client_id_len.max(0)).expect("client id length"));
                    let mut request =
                        OffsetCommitRequest::decode(&mut body, version).expect("decode commit");
                    request.topics.sort_by(|a, b| a.name.cmp(&b.name));
                    for topic in &mut request.topics {
                        topic.partitions.sort_by_key(|p| p.partition_index);
                    }
                    if self.rebalance_on_commit.swap(false, Ordering::SeqCst) {
                        self.heartbeat_error
                            .store(REBALANCE_IN_PROGRESS, Ordering::SeqCst);
                    }
                    let error_code = match self
                        .commit_replies
                        .lock()
                        .expect("commit replies lock")
                        .pop_front()
                    {
                        None => 0,
                        Some(CommitReply::Error(code)) => code,
                        Some(CommitReply::Drop) => {
                            self.record(GroupRequest::OffsetCommit(request));
                            return None;
                        }
                    };
                    let response = OffsetCommitResponse {
                        topics: request
                            .topics
                            .iter()
                            .map(|topic| OffsetCommitResponseTopic {
                                name: topic.name.clone(),
                                partitions: topic
                                    .partitions
                                    .iter()
                                    .map(|p| OffsetCommitResponsePartition {
                                        partition_index: p.partition_index,
                                        error_code,
                                        ..Default::default()
                                    })
                                    .collect(),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    };
                    self.record(GroupRequest::OffsetCommit(request));
                    Some(encode(&response, version))
                }
                _ => None,
            }
        }
    }

    pub(super) fn partition(index: i32) -> (String, i32) {
        (TOPIC.to_owned(), index)
    }

    /// The `OffsetCommit` that Kafka's `SubscriptionState.allConsumed` makes:
    /// each `(partition, offset, leader_epoch)` with metadata `""`.
    fn commit(generation: i32, offsets: &[(i32, i64, i32)]) -> GroupRequest {
        GroupRequest::OffsetCommit(OffsetCommitRequest {
            group_id: GROUP.into(),
            generation_id_or_member_epoch: generation,
            member_id: MEMBER.into(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: vec![OffsetCommitRequestTopic {
                name: TOPIC.into(),
                partitions: offsets
                    .iter()
                    .map(
                        |(partition_index, committed_offset, committed_leader_epoch)| {
                            OffsetCommitRequestPartition {
                                partition_index: *partition_index,
                                committed_offset: *committed_offset,
                                committed_leader_epoch: *committed_leader_epoch,
                                committed_metadata: Some(String::new()),
                                unknown_tagged_fields: UnknownTaggedFields::default(),
                            }
                        },
                    )
                    .collect(),
                ..Default::default()
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
    }

    /// Kafka's `ConsumerMetadata.newMetadataRequestBuilder` names the
    /// subscribed topics, with `allowAutoTopicCreation` from
    /// `allow.auto.create.topics`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_requests_name_the_subscribed_topics() {
        use krabka_protocol::owned::{
            find_coordinator_response::FindCoordinatorResponse, metadata_request::MetadataRequest,
        };

        let named = |topics: &[&str], allow| {
            krabka_client_core::topics_request(topics.iter().map(|&t| t.to_owned()), allow)
        };
        for (name, subscribe, allow_auto_create_topics, expected) in [
            (
                "subscribe a and b",
                vec!["a", "b"],
                true,
                named(&["a", "b"], true),
            ),
            ("auto creation off", vec!["a"], false, named(&["a"], false)),
        ] {
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let handler_requests = Arc::clone(&requests);
            let mock = MockBroker::start(move |api_key, version, _corr_id, mut body| {
                match api_key {
                    api_versions_request::API_KEY => Some(encode(
                        &ApiVersionsResponse {
                            api_keys: API_VERSIONS
                                .iter()
                                .map(|(api_key, min_version, max_version)| ApiVersion {
                                    api_key: *api_key,
                                    min_version: *min_version,
                                    max_version: *max_version,
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        },
                        0,
                    )),
                    find_coordinator_request::API_KEY => Some(encode(
                        &FindCoordinatorResponse {
                            node_id: 1,
                            host: "127.0.0.1".into(),
                            port: 0,
                            ..Default::default()
                        },
                        version,
                    )),
                    metadata_request::API_KEY => {
                        let client_id_len = body.get_i16();
                        body.advance(usize::try_from(client_id_len.max(0)).expect("length"));
                        let request =
                            MetadataRequest::decode(&mut body, version).expect("decode Metadata");
                        handler_requests
                            .lock()
                            .expect("requests lock")
                            .push(request);
                        Some(encode(&MetadataResponse::default(), version))
                    }
                    // The test stops at the first `JoinGroup`.
                    _ => None,
                }
            })
            .await;
            let mut config =
                start_config(mock.addr.to_string(), Assignor::Range, false, minutes(1));
            config.subscribe = subscribe.into_iter().map(str::to_owned).collect();
            config.allow_auto_create_topics = allow_auto_create_topics;
            let start = tokio::spawn(Consumer::start_once(config));
            let first = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(first) = requests.lock().expect("requests lock").first().cloned() {
                        break first;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("the consumer sends Metadata");
            start.abort();
            mock.stop();
            assert2::check!(first == expected, "{name}");
        }
    }

    pub(super) fn start_config(
        bootstrap: String,
        assignor: Assignor,
        auto_commit: bool,
        rebalance_timeout: Time,
    ) -> StartConfig {
        StartConfig {
            bootstrap,
            client_id: "auto-commit-test".into(),
            group_id: GROUP.into(),
            session_timeout: Time::from_std(SESSION_TIMEOUT),
            max_poll_interval: rebalance_timeout,
            max_poll_records: DEFAULT_CONSUMER_MAX_POLL_RECORDS,
            heartbeat_interval: secs(3),
            subscription_metadata_refresh_interval: minutes(60),
            subscribe: vec![TOPIC.into()],
            group_instance_id: None,
            auto_offset_reset: AutoOffsetReset::Earliest,
            isolation_level: IsolationLevel::ReadUncommitted,
            assignor,
            fetch_min: krabka_client_core::DEFAULT_FETCH_MIN,
            fetch_max: crate::poll::DEFAULT_FETCH_MAX,
            fetch_partition_max: crate::poll::DEFAULT_FETCH_PARTITION_MAX,
            request_timeout: secs(30),
            dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity::new(
                krabka_client_core::DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
            )
            .expect("dispatch queue capacity"),
            frame_max: krabka_client_core::ClientFrameMax::try_from(
                krabka_client_core::DEFAULT_CLIENT_FRAME_MAX,
            )
            .expect("frame max"),
            metadata_recovery_strategy: krabka_client_core::MetadataRecoveryStrategy::default(),
            metadata_recovery_rebootstrap_trigger:
                krabka_client_core::DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER,
            leave_group_timeout: secs(5),
            client_rack: None,
            security: None,
            retry_policy: ConsumerRetryPolicy::default(),
            auto_commit_interval: auto_commit.then_some(AUTO_COMMIT_INTERVAL),
            allow_auto_create_topics: true,
        }
    }

    /// One application or group event of a scenario.
    #[derive(Clone, Copy, Debug)]
    enum Step {
        /// Run the start of `poll`, and wait for a commit that it sends.
        Poll,
        /// Advance the clock by the auto commit interval.
        AdvanceInterval,
        /// `poll` returns records of partition 0 up to offset 19. The
        /// application has not processed them until it polls again.
        ReceiveRecords,
        /// The next heartbeat answers `REBALANCE_IN_PROGRESS`. Wait until the
        /// coordinator received `syncs` `SyncGroup` requests in total, or
        /// record `SyncGroupLate` after `within`.
        Rebalance { syncs: usize, within: Duration },
        /// Run `commit_sync`.
        CommitSync,
        /// The mock answers the next `OffsetCommit` with this reply, and the
        /// next heartbeat after that commit answers `REBALANCE_IN_PROGRESS`.
        /// Run `commit_sync`, and wait as `Rebalance` does.
        CommitSyncDuringRebalance {
            reply: CommitReply,
            syncs: usize,
            within: Duration,
        },
        /// The mock answers the next `OffsetCommit` with this reply.
        ReplyToNextCommit(CommitReply),
        /// Take the commit lock and keep it until the scenario ends.
        HoldCommitLock,
        /// The mock stops to answer heartbeats, except a heartbeat that
        /// answers an error.
        DropHeartbeats,
        /// Run `close`, or record `CloseTimedOut` after two minutes.
        Close,
    }

    /// Wait until the coordinator received `syncs` `SyncGroup` requests in
    /// total, or record `SyncGroupLate` after `within`. The application polls
    /// each 100 ms meanwhile, so that the coordinator task starts the joins.
    /// These polls record no new positions.
    pub(super) async fn wait_for_sync_groups(
        coordinator: &MockCoordinator,
        polls: &crate::coordinator::PollSignal,
        syncs: usize,
        within: Duration,
    ) {
        let waited = tokio::time::timeout(within, async {
            while coordinator.sync_groups() < syncs {
                crate::coordinator::note_poll(polls);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        if waited.is_err() {
            coordinator.record(GroupRequest::SyncGroupLate);
        }
        // Let a commit that comes after the last SyncGroup arrive.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    /// Start a consumer that owns partitions 0 (next offset 12, leader epoch 3)
    /// and 1 (next offset 7, no epoch) at generation 1, run `steps`, and return
    /// the group requests that the coordinator received.
    async fn run(
        auto_commit: bool,
        assignor: Assignor,
        assignments: Vec<Vec<(String, i32)>>,
        steps: &[Step],
    ) -> Vec<GroupRequest> {
        run_with_rebalance_timeout(auto_commit, assignor, assignments, minutes(1), steps).await
    }

    /// [`run`] with a rebalance timeout.
    async fn run_with_rebalance_timeout(
        auto_commit: bool,
        assignor: Assignor,
        assignments: Vec<Vec<(String, i32)>>,
        rebalance_timeout: Time,
        steps: &[Step],
    ) -> Vec<GroupRequest> {
        let coordinator = MockCoordinator::new(assignor, assignments);
        let in_mock = Arc::clone(&coordinator);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            in_mock.respond(api_key, version, body)
        })
        .await;
        let config = start_config(
            mock.addr.to_string(),
            assignor,
            auto_commit,
            rebalance_timeout,
        );
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .build()
            .await
            .expect("client");
        let mut consumer = Some(
            spawn_consumer(
                config,
                client,
                Arc::new(AtomicI32::new(0)),
                MEMBER.into(),
                StartupState {
                    generation_id: 1,
                    assigned_partitions: vec![partition(0), partition(1)],
                    next_offsets: HashMap::from([(partition(0), 12), (partition(1), 7)]),
                    positions: HashMap::from([(partition(0), primed_position(3))]),
                    topic_ids: HashMap::new(),
                    topic_partitions: HashMap::from([(TOPIC.to_owned(), 2)]),
                },
            )
            .await
            .expect("spawn consumer"),
        );
        let mut held_commit_lock = None;

        for step in steps {
            match step {
                Step::Poll => {
                    let consumer = consumer.as_ref().expect("open consumer");
                    consumer.maybe_auto_commit_async().await;
                    drop(consumer.commit_serialization.lock().await);
                }
                Step::AdvanceInterval => tokio::time::advance(AUTO_COMMIT_INTERVAL).await,
                Step::ReceiveRecords => {
                    let consumer = consumer.as_ref().expect("open consumer");
                    consumer.next_offsets.lock().await.insert(partition(0), 20);
                }
                Step::Rebalance { syncs, within } => {
                    let consumer = consumer.as_ref().expect("open consumer");
                    coordinator
                        .heartbeat_error
                        .store(REBALANCE_IN_PROGRESS, Ordering::SeqCst);
                    wait_for_sync_groups(&coordinator, &consumer.poll_signal, *syncs, *within)
                        .await;
                }
                Step::CommitSync => {
                    let consumer = consumer.as_ref().expect("open consumer");
                    consumer.commit_sync().await.expect("commit_sync");
                    coordinator.record(GroupRequest::CommitSyncReturned);
                }
                Step::CommitSyncDuringRebalance {
                    reply,
                    syncs,
                    within,
                } => {
                    let consumer = consumer.as_ref().expect("open consumer");
                    coordinator
                        .commit_replies
                        .lock()
                        .expect("commit replies lock")
                        .push_back(*reply);
                    coordinator
                        .rebalance_on_commit
                        .store(true, Ordering::SeqCst);
                    let commit = async {
                        // The result does not matter here: the scenario checks
                        // the order of the requests.
                        let _ = consumer.commit_sync().await;
                        coordinator.record(GroupRequest::CommitSyncReturned);
                    };
                    tokio::join!(
                        commit,
                        wait_for_sync_groups(&coordinator, &consumer.poll_signal, *syncs, *within)
                    );
                }
                Step::ReplyToNextCommit(reply) => {
                    coordinator
                        .commit_replies
                        .lock()
                        .expect("commit replies lock")
                        .push_back(*reply);
                }
                Step::DropHeartbeats => {
                    coordinator.drop_heartbeats.store(true, Ordering::SeqCst);
                }
                Step::HoldCommitLock => {
                    let consumer = consumer.as_ref().expect("open consumer");
                    held_commit_lock = Some(
                        Arc::clone(&consumer.commit_serialization)
                            .lock_owned()
                            .await,
                    );
                }
                Step::Close => {
                    let close = consumer.take().expect("open consumer").close();
                    match tokio::time::timeout(Duration::from_mins(2), close).await {
                        Ok(result) => result.expect("close"),
                        Err(_) => coordinator.record(GroupRequest::CloseTimedOut),
                    }
                }
            }
        }

        let requests = coordinator.requests();
        drop(held_commit_lock);
        drop(consumer);
        mock.stop();
        requests
    }

    /// One scenario: name, `enable.auto.commit`, assignor, the `SyncGroup`
    /// assignments, the steps, and the group requests that Kafka sends.
    type Case = (
        &'static str,
        bool,
        Assignor,
        Vec<Vec<(String, i32)>>,
        Vec<Step>,
        Vec<GroupRequest>,
    );

    /// Kafka commits the consumed positions from `poll` each
    /// `auto.commit.interval.ms`, before each `JoinGroup`, and in `close`, but
    /// only when `enable.auto.commit` is on. With auto commit off, the consumer
    /// commits nothing that the application did not ask for.
    #[tokio::test(start_paused = true)]
    async fn auto_commit_sends_offset_commit_where_kafka_does() {
        use GroupRequest::{JoinGroup, LeaveGroup, SyncGroup};
        use Step::{AdvanceInterval, Close, Poll, Rebalance, ReceiveRecords};

        let both = [(0, 12, 3), (1, 7, -1)];
        let cases: Vec<Case> = vec![
            (
                "on: a poll after the interval commits the positions",
                true,
                Assignor::Range,
                vec![],
                vec![Poll, AdvanceInterval, Poll],
                vec![commit(1, &both)],
            ),
            (
                "on: a poll before the interval commits nothing",
                true,
                Assignor::Range,
                vec![],
                vec![Poll, Poll],
                vec![],
            ),
            (
                "off: a poll after the interval commits nothing",
                false,
                Assignor::Range,
                vec![],
                vec![Poll, AdvanceInterval, Poll],
                vec![],
            ),
            (
                "on, eager: a rebalance commits the positions of the last poll before JoinGroup",
                true,
                Assignor::Range,
                vec![vec![partition(0), partition(1)]],
                vec![
                    Poll,
                    ReceiveRecords,
                    Rebalance {
                        syncs: 1,
                        within: REBALANCE_WAIT,
                    },
                ],
                vec![commit(1, &both), JoinGroup, SyncGroup],
            ),
            (
                "off, eager: a rebalance commits nothing",
                false,
                Assignor::Range,
                vec![vec![partition(0), partition(1)]],
                vec![
                    Poll,
                    ReceiveRecords,
                    Rebalance {
                        syncs: 1,
                        within: REBALANCE_WAIT,
                    },
                ],
                vec![JoinGroup, SyncGroup],
            ),
            (
                "on, cooperative: a revoke of partition 1 commits before each JoinGroup",
                true,
                Assignor::CooperativeSticky,
                vec![vec![partition(0)]],
                vec![
                    Poll,
                    ReceiveRecords,
                    Rebalance {
                        syncs: 2,
                        within: REBALANCE_WAIT,
                    },
                ],
                vec![
                    commit(1, &both),
                    JoinGroup,
                    SyncGroup,
                    commit(2, &[(0, 12, 3)]),
                    JoinGroup,
                    SyncGroup,
                ],
            ),
            (
                "off, cooperative: a revoke of partition 1 commits nothing",
                false,
                Assignor::CooperativeSticky,
                vec![vec![partition(0)]],
                vec![
                    Poll,
                    ReceiveRecords,
                    Rebalance {
                        syncs: 2,
                        within: REBALANCE_WAIT,
                    },
                ],
                vec![JoinGroup, SyncGroup, JoinGroup, SyncGroup],
            ),
            (
                "on: close commits the current positions, then leaves the group",
                true,
                Assignor::Range,
                vec![],
                vec![Poll, ReceiveRecords, Close],
                vec![commit(1, &[(0, 20, 3), (1, 7, -1)]), LeaveGroup],
            ),
            (
                "off: close only leaves the group",
                false,
                Assignor::CooperativeSticky,
                vec![],
                vec![Poll, ReceiveRecords, Close],
                vec![LeaveGroup],
            ),
        ];
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, auto_commit, assignor, assignments, steps, expected) in cases {
            actual.push((name, run(auto_commit, assignor, assignments, &steps).await));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// The commit before a `JoinGroup` and the commit of `close` wait for the
    /// other commits of the consumer, so the committed offset does not move
    /// back. The rebalance timeout bounds the commit before a `JoinGroup`, and
    /// the close timeout bounds the commit of `close`. A commit that waits for
    /// the rebalance does not block the rebalance.
    #[tokio::test(start_paused = true)]
    async fn auto_commit_keeps_the_commit_order_within_the_group_timeouts() {
        use CommitReply::{Drop, Error};
        use GroupRequest::{CommitSyncReturned, JoinGroup, LeaveGroup, SyncGroup};
        use Step::{
            Close, CommitSync, CommitSyncDuringRebalance, DropHeartbeats, HoldCommitLock, Poll,
            Rebalance, ReceiveRecords, ReplyToNextCommit,
        };

        let polled = [(0, 12, 3), (1, 7, -1)];
        let received = [(0, 20, 3), (1, 7, -1)];
        let both_partitions = vec![vec![partition(0), partition(1)]];
        let cases: Vec<(&str, Time, Vec<Step>, Vec<GroupRequest>)> = vec![
            (
                "commit_sync, then a rebalance: the commit before JoinGroup does not go back",
                minutes(1),
                vec![
                    Poll,
                    ReceiveRecords,
                    CommitSync,
                    Rebalance {
                        syncs: 1,
                        within: REBALANCE_WAIT,
                    },
                ],
                vec![
                    commit(1, &received),
                    CommitSyncReturned,
                    commit(1, &received),
                    JoinGroup,
                    SyncGroup,
                ],
            ),
            (
                "a rebalance while commit_sync waits for its response: the commit before \
                 JoinGroup comes after it",
                minutes(1),
                vec![
                    Poll,
                    ReceiveRecords,
                    CommitSyncDuringRebalance {
                        reply: Drop,
                        syncs: 1,
                        within: Duration::from_mins(1),
                    },
                ],
                vec![
                    commit(1, &received),
                    CommitSyncReturned,
                    commit(1, &received),
                    JoinGroup,
                    SyncGroup,
                ],
            ),
            (
                "a rebalance while commit_sync waits for the rebalance: the rebalance does not \
                 wait for commit_sync",
                minutes(1),
                vec![
                    Poll,
                    ReceiveRecords,
                    CommitSyncDuringRebalance {
                        reply: Error(REBALANCE_IN_PROGRESS),
                        syncs: 1,
                        within: REBALANCE_WAIT,
                    },
                ],
                vec![
                    commit(1, &received),
                    commit(1, &received),
                    JoinGroup,
                    SyncGroup,
                    CommitSyncReturned,
                ],
            ),
            (
                "another commit holds the commit lock: JoinGroup at the rebalance timeout, \
                 without a commit",
                secs(10),
                vec![
                    Poll,
                    HoldCommitLock,
                    Rebalance {
                        syncs: 1,
                        within: REBALANCE_WAIT,
                    },
                ],
                vec![JoinGroup, SyncGroup],
            ),
            (
                "another commit holds the commit lock past the session timeout: heartbeats keep \
                 the member in the group",
                minutes(1),
                vec![
                    Poll,
                    HoldCommitLock,
                    Rebalance {
                        syncs: 1,
                        within: Duration::from_secs(90),
                    },
                ],
                vec![JoinGroup, SyncGroup],
            ),
            (
                "the coordinator does not answer a heartbeat during the commit before JoinGroup: \
                 JoinGroup at the rebalance timeout",
                secs(10),
                vec![
                    Poll,
                    HoldCommitLock,
                    DropHeartbeats,
                    Rebalance {
                        syncs: 1,
                        within: Duration::from_secs(20),
                    },
                ],
                vec![JoinGroup, SyncGroup],
            ),
            (
                "the coordinator does not answer the commit before JoinGroup: JoinGroup at the \
                 rebalance timeout",
                secs(10),
                vec![
                    Poll,
                    ReplyToNextCommit(Drop),
                    Rebalance {
                        syncs: 1,
                        within: REBALANCE_WAIT,
                    },
                ],
                vec![commit(1, &polled), JoinGroup, SyncGroup],
            ),
            (
                "another commit holds the commit lock: close leaves the group after the close \
                 timeout",
                minutes(1),
                vec![Poll, ReceiveRecords, HoldCommitLock, Close],
                vec![LeaveGroup],
            ),
        ];
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, rebalance_timeout, steps, expected) in cases {
            actual.push((
                name,
                run_with_rebalance_timeout(
                    true,
                    Assignor::Range,
                    both_partitions.clone(),
                    rebalance_timeout,
                    &steps,
                )
                .await,
            ));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    #[test]
    fn auto_commit_interval_follows_kafka_config_bounds() {
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, enable, interval, expected) in [
            ("off", false, Time::from_millis(-1), Ok(None)),
            ("default", true, secs(5), Ok(Some(Duration::from_secs(5)))),
            ("zero", true, Time::from_millis(0), Ok(Some(Duration::ZERO))),
            (
                "i32::MAX milliseconds",
                true,
                Time::from_millis(i64::from(i32::MAX)),
                Ok(Some(Duration::from_millis(u64::from(
                    i32::MAX.unsigned_abs(),
                )))),
            ),
            ("negative", true, Time::from_millis(-1), Err(())),
            (
                "above i32::MAX milliseconds",
                true,
                Time::from_millis(i64::from(i32::MAX) + 1),
                Err(()),
            ),
            (
                "half a millisecond",
                true,
                Time::from_secs_f64(0.0005),
                Err(()),
            ),
            (
                "i32::MAX milliseconds and a fraction",
                true,
                Time::from_secs_f64((f64::from(i32::MAX) + 0.25) / 1e3),
                Err(()),
            ),
            ("not a number", true, Time::from_secs_f64(f64::NAN), Err(())),
            (
                "infinite",
                true,
                Time::from_secs_f64(f64::INFINITY),
                Err(()),
            ),
        ] {
            let result = validated_auto_commit_interval(enable, interval).map_err(|_| ());
            actual.push((name, result));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }
}

#[cfg(test)]
mod group_rejoin_tests {
    use std::sync::atomic::Ordering;

    use krabka_client_core::MockBroker;

    use super::{
        auto_commit_tests::{
            GroupRequest, MEMBER, MockCoordinator, TOPIC, partition, start_config,
            wait_for_sync_groups,
        },
        *,
    };

    /// `FENCED_INSTANCE_ID`.
    const FENCED_INSTANCE_ID: i16 = 82;

    /// What the application saw around a fence of its static member.
    #[derive(Debug, PartialEq)]
    struct FenceObservation {
        first_poll: Result<usize, String>,
        commit_after_fence: Result<(), String>,
        requests_before_next_poll: Vec<GroupRequest>,
        next_poll: Result<usize, String>,
        requests_after_next_poll: Vec<GroupRequest>,
        assignment: Vec<(String, i32)>,
        generation: i32,
    }

    /// The coordinator fences the static member on a heartbeat. `poll` returns
    /// the error once, a commit fails, and the member sends no `JoinGroup`
    /// until the application polls again. That `poll` joins the group and gets
    /// a new assignment. Kafka's `AbstractCoordinator.pollHeartbeat` raises the
    /// failure cause once, and the next `poll` calls `ensureActiveGroup`.
    #[tokio::test(start_paused = true)]
    async fn poll_after_a_fenced_instance_id_joins_the_group_again() {
        let coordinator = MockCoordinator::new(Assignor::Range, vec![vec![partition(1)]]);
        coordinator
            .heartbeat_error
            .store(FENCED_INSTANCE_ID, Ordering::SeqCst);
        let in_mock = Arc::clone(&coordinator);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            in_mock.respond(api_key, version, body)
        })
        .await;
        let mut config = start_config(mock.addr.to_string(), Assignor::Range, false, minutes(1));
        config.group_instance_id = Some("instance-a".into());
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .build()
            .await
            .expect("client");
        let mut consumer = spawn_consumer(
            config,
            client,
            Arc::new(AtomicI32::new(0)),
            MEMBER.into(),
            StartupState {
                generation_id: 1,
                assigned_partitions: vec![partition(0), partition(1)],
                next_offsets: HashMap::from([(partition(0), 12), (partition(1), 7)]),
                positions: HashMap::new(),
                topic_ids: HashMap::new(),
                topic_partitions: HashMap::from([(TOPIC.to_owned(), 2)]),
            },
        )
        .await
        .expect("spawn consumer");
        // Wait for the heartbeat that gets the fence.
        tokio::time::timeout(Duration::from_secs(30), async {
            while !crate::coordinator::poll_error_pending(&consumer.poll_error) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the coordinator fences the member");

        let first_poll = consumer
            .poll(millis(100))
            .await
            .map(|records| records.len())
            .map_err(|error| error.to_string());
        // Several heartbeat intervals without a poll.
        tokio::time::sleep(Duration::from_secs(20)).await;
        let commit_after_fence = consumer
            .commit_sync()
            .await
            .map_err(|error| error.to_string());
        let requests_before_next_poll = coordinator.requests();
        let next_poll = consumer
            .poll(millis(100))
            .await
            .map(|records| records.len())
            .map_err(|error| error.to_string());
        wait_for_sync_groups(
            &coordinator,
            &consumer.poll_signal,
            1,
            Duration::from_secs(30),
        )
        .await;
        let observation = FenceObservation {
            first_poll,
            commit_after_fence,
            requests_before_next_poll,
            next_poll,
            requests_after_next_poll: coordinator
                .requests()
                .into_iter()
                .filter(|request| *request != GroupRequest::SessionExpired)
                .collect(),
            assignment: consumer.assignment().await,
            generation: consumer.generation_id(),
        };
        consumer.close().await.expect("close");
        mock.stop();

        assert2::assert!(
            observation
                == FenceObservation {
                    first_poll: Err(
                        ConsumerError::FencedInstanceId("instance-a".into()).to_string()
                    ),
                    commit_after_fence: Err(ConsumerError::CommitFailed.to_string()),
                    requests_before_next_poll: Vec::new(),
                    next_poll: Ok(0),
                    requests_after_next_poll: vec![
                        GroupRequest::JoinGroup,
                        GroupRequest::SyncGroup
                    ],
                    assignment: vec![partition(1)],
                    generation: 2,
                }
        );
    }
}

#[cfg(test)]
mod poll_interval_tests {
    use std::sync::atomic::Ordering;

    use krabka_client_core::MockBroker;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::leave_group_request::{LeaveGroupRequest, MemberIdentity},
    };

    use super::{
        auto_commit_tests::{MEMBER, MockCoordinator, TOPIC, partition, start_config},
        *,
    };

    /// How the application polls in a poll timeout case, with a
    /// `max_poll_interval` of 500 ms. The polls before the timeout only signal
    /// the coordinator task, as the start of `poll` does. The case runs in real
    /// time: with paused time, the network wait of a request lets the clock
    /// jump past the poll deadline.
    #[derive(Clone, Copy, Debug)]
    enum Polls {
        /// One `poll` each 100 ms.
        EachTenthOfASecond,
        /// No `poll` for 700 ms.
        Late,
    }

    /// What the group saw in a poll timeout case.
    #[derive(Debug, PartialEq)]
    struct PollTimeoutObservation {
        leaves: Vec<LeaveGroupRequest>,
        heartbeats_after_timeout: bool,
        commit: Result<(), String>,
        /// `Consumer::member_id` before the next `poll`.
        member_id_before_next_poll: String,
        /// `(member_id, group_instance_id)` of each `JoinGroup` after the next
        /// `poll`.
        joins_after_next_poll: Vec<(String, Option<String>)>,
        /// `Consumer::member_id` after the join of the next `poll`.
        member_id_after_next_poll: String,
    }

    async fn run_poll_timeout_case(
        group_instance_id: Option<&str>,
        polls: Polls,
    ) -> PollTimeoutObservation {
        let coordinator = MockCoordinator::new(Assignor::Range, vec![vec![partition(0)]]);
        let in_mock = Arc::clone(&coordinator);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            in_mock.respond(api_key, version, body)
        })
        .await;
        let mut config = start_config(mock.addr.to_string(), Assignor::Range, false, millis(500));
        config.group_instance_id = group_instance_id.map(str::to_owned);
        config.heartbeat_interval = millis(100);
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .build()
            .await
            .expect("client");
        let mut consumer = spawn_consumer(
            config,
            client,
            Arc::new(AtomicI32::new(0)),
            MEMBER.into(),
            StartupState {
                generation_id: 1,
                assigned_partitions: vec![partition(0)],
                next_offsets: HashMap::from([(partition(0), 12)]),
                positions: HashMap::new(),
                topic_ids: HashMap::new(),
                topic_partitions: HashMap::from([(TOPIC.to_owned(), 1)]),
            },
        )
        .await
        .expect("spawn consumer");
        // The case starts after the first heartbeat, with a `poll`.
        let spawned_at = *coordinator
            .last_heartbeat
            .lock()
            .expect("last heartbeat lock");
        while *coordinator
            .last_heartbeat
            .lock()
            .expect("last heartbeat lock")
            == spawned_at
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        crate::coordinator::note_poll(&consumer.poll_signal);

        let poller = matches!(polls, Polls::EachTenthOfASecond).then(|| {
            let poll_signal = consumer.poll_signal.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    crate::coordinator::note_poll(&poll_signal);
                }
            })
        });
        tokio::time::sleep(Duration::from_millis(700)).await;
        let heartbeat_at_timeout = *coordinator
            .last_heartbeat
            .lock()
            .expect("last heartbeat lock");
        // Four heartbeat intervals.
        tokio::time::sleep(Duration::from_millis(400)).await;
        let heartbeats_after_timeout = *coordinator
            .last_heartbeat
            .lock()
            .expect("last heartbeat lock")
            != heartbeat_at_timeout;
        let commit = consumer
            .commit_sync()
            .await
            .map_err(|error| error.to_string());
        let leaves = coordinator.leaves.lock().expect("leaves lock").clone();
        let member_id_before_next_poll = consumer.member_id();
        let joins_before = coordinator.joins.lock().expect("joins lock").len();
        consumer.poll(millis(0)).await.expect("next poll");
        // The application keeps polling while the join runs.
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            while consumer.member_id().is_empty() {
                crate::coordinator::note_poll(&consumer.poll_signal);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let joins_after_next_poll = coordinator.joins.lock().expect("joins lock")[joins_before..]
            .iter()
            .map(|join| (join.member_id.clone(), join.group_instance_id.clone()))
            .collect();
        let member_id_after_next_poll = consumer.member_id();
        if let Some(poller) = poller {
            poller.abort();
        }
        coordinator.heartbeat_error.store(0, Ordering::SeqCst);
        drop(consumer);
        mock.stop();
        PollTimeoutObservation {
            leaves,
            heartbeats_after_timeout,
            commit,
            member_id_before_next_poll,
            joins_after_next_poll,
            member_id_after_next_poll,
        }
    }

    /// Kafka's heartbeat thread sends `LeaveGroup` with the reason "consumer
    /// poll timeout has expired." when no `poll` came for
    /// `max.poll.interval.ms`, for a dynamic member only
    /// (`AbstractCoordinator.handlePollTimeoutExpiry`, `maybeLeaveGroup`). Every
    /// member resets its generation, so heartbeats stop, a commit fails with
    /// `CommitFailedException`, and the next `poll` joins with an empty member
    /// id.
    #[tokio::test]
    async fn a_member_leaves_the_group_when_poll_does_not_come_within_max_poll_interval() {
        let commit_failed = Err(ConsumerError::CommitFailed.to_string());
        let leave = LeaveGroupRequest {
            group_id: "group-a".into(),
            // Version 5 has no top-level member id.
            member_id: String::new(),
            members: vec![MemberIdentity {
                member_id: MEMBER.into(),
                group_instance_id: None,
                reason: Some("consumer poll timeout has expired.".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, group_instance_id, polls, expected) in [
            (
                "dynamic member polls in time",
                None,
                Polls::EachTenthOfASecond,
                PollTimeoutObservation {
                    leaves: Vec::new(),
                    heartbeats_after_timeout: true,
                    commit: Ok(()),
                    member_id_before_next_poll: MEMBER.into(),
                    joins_after_next_poll: Vec::new(),
                    member_id_after_next_poll: MEMBER.into(),
                },
            ),
            (
                "dynamic member polls late",
                None,
                Polls::Late,
                PollTimeoutObservation {
                    leaves: vec![leave],
                    heartbeats_after_timeout: false,
                    commit: commit_failed.clone(),
                    member_id_before_next_poll: String::new(),
                    joins_after_next_poll: vec![(String::new(), None)],
                    member_id_after_next_poll: MEMBER.into(),
                },
            ),
            (
                "static member polls late",
                Some("instance-a"),
                Polls::Late,
                PollTimeoutObservation {
                    leaves: Vec::new(),
                    heartbeats_after_timeout: false,
                    commit: commit_failed,
                    member_id_before_next_poll: String::new(),
                    joins_after_next_poll: vec![(String::new(), Some("instance-a".into()))],
                    member_id_after_next_poll: MEMBER.into(),
                },
            ),
        ] {
            actual.push((name, run_poll_timeout_case(group_instance_id, polls).await));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted, "{actual:#?}");
    }

    /// The poll timeout clears the assignment before the `LeaveGroup` goes out.
    /// A `poll` while the coordinator does not answer the `LeaveGroup` fetches
    /// nothing, and it starts the join at once.
    #[tokio::test]
    async fn a_poll_during_the_poll_timeout_leave_fetches_nothing_and_starts_the_join() {
        let coordinator = MockCoordinator::new(Assignor::Range, vec![vec![partition(0)]]);
        coordinator
            .drop_leaves
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let in_mock = Arc::clone(&coordinator);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            in_mock.respond(api_key, version, body)
        })
        .await;
        let mut config = start_config(mock.addr.to_string(), Assignor::Range, false, millis(300));
        config.heartbeat_interval = millis(50);
        config.leave_group_timeout = secs(10);
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .build()
            .await
            .expect("client");
        let mut consumer = spawn_consumer(
            config,
            client,
            Arc::new(AtomicI32::new(0)),
            MEMBER.into(),
            StartupState {
                generation_id: 1,
                assigned_partitions: vec![partition(0)],
                next_offsets: HashMap::from([(partition(0), 12)]),
                positions: HashMap::new(),
                topic_ids: HashMap::new(),
                topic_partitions: HashMap::from([(TOPIC.to_owned(), 1)]),
            },
        )
        .await
        .expect("spawn consumer");
        tokio::time::timeout(Duration::from_secs(10), async {
            while coordinator.leaves.lock().expect("leaves lock").is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the member sends LeaveGroup");

        let records = consumer.poll(millis(100)).await.expect("poll");
        let joined = tokio::time::timeout(Duration::from_secs(5), async {
            while coordinator.joins.lock().expect("joins lock").is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        let observation = (
            records.len(),
            joined,
            coordinator.leaves.lock().expect("leaves lock").len(),
        );
        drop(consumer);
        mock.stop();
        assert2::assert!(observation == (0, true, 1));
    }

    /// Kafka's `ClassicKafkaConsumer` sends `max.poll.interval.ms` as the
    /// `JoinGroup` rebalance timeout. The default is 300000 ms.
    #[tokio::test]
    async fn join_group_sends_max_poll_interval_as_the_rebalance_timeout() {
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, max_poll_interval, expected) in [
            ("default", None, 300_000),
            ("configured", Some(secs(10)), 10_000),
        ] {
            let coordinator = MockCoordinator::new(Assignor::Range, vec![Vec::new()]);
            let in_mock = Arc::clone(&coordinator);
            let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
                in_mock.respond(api_key, version, body)
            })
            .await;
            let consumer = Consumer::builder()
                .bootstrap(mock.addr.to_string())
                .group_id("group-a")
                .subscribe(vec![TOPIC.to_owned()])
                .maybe_max_poll_interval(max_poll_interval)
                .build()
                .await
                .expect("build");
            let rebalance_timeouts: Vec<i32> = coordinator
                .joins
                .lock()
                .expect("joins lock")
                .iter()
                .map(|join| join.rebalance_timeout_ms)
                .collect();
            drop(consumer);
            mock.stop();
            actual.push((name, rebalance_timeouts));
            wanted.push((name, vec![expected, expected]));
        }
        assert2::assert!(actual == wanted);
    }

    #[test]
    fn max_poll_settings_follow_kafka_config_bounds() {
        let interval = |value: Time| validated_max_poll_interval(value).map_err(|_| ());
        let records = |value: usize| validated_max_poll_records(value).map_err(|_| ());
        let actual = (
            [
                interval(Time::from_millis(0)),
                interval(Time::from_millis(1)),
                interval(DEFAULT_CONSUMER_MAX_POLL_INTERVAL),
                interval(Time::from_millis(i64::from(i32::MAX))),
                interval(Time::from_millis(i64::from(i32::MAX) + 1)),
                interval(Time::from_secs_f64(0.0015)),
            ],
            [
                records(0),
                records(1),
                records(DEFAULT_CONSUMER_MAX_POLL_RECORDS),
                records(usize::try_from(i32::MAX).expect("fits")),
                records(usize::try_from(i32::MAX).expect("fits") + 1),
            ],
        );
        let expected = (
            [
                Err(()),
                Ok(Time::from_millis(1)),
                Ok(minutes(5)),
                Ok(Time::from_millis(i64::from(i32::MAX))),
                Err(()),
                Err(()),
            ],
            [
                Err(()),
                Ok(1),
                Ok(500),
                Ok(usize::try_from(i32::MAX).expect("fits")),
                Err(()),
            ],
        );
        assert2::assert!(actual == expected);
    }
}

#[cfg(test)]
mod group_membership_tests {
    use std::sync::atomic::Ordering;

    use krabka_client_core::MockBroker;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::leave_group_request::{LeaveGroupRequest, MemberIdentity},
    };

    use super::{
        auto_commit_tests::{MEMBER, MockCoordinator, TOPIC, partition, start_config},
        *,
    };

    async fn started_consumer(mock: &MockBroker, group_instance_id: Option<&str>) -> Consumer {
        let mut config = start_config(mock.addr.to_string(), Assignor::Range, false, minutes(1));
        config.group_instance_id = group_instance_id.map(str::to_owned);
        config.heartbeat_interval = millis(50);
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .build()
            .await
            .expect("client");
        spawn_consumer(
            config,
            client,
            Arc::new(AtomicI32::new(0)),
            MEMBER.into(),
            StartupState {
                generation_id: 1,
                assigned_partitions: vec![partition(0)],
                next_offsets: HashMap::from([(partition(0), 12)]),
                positions: HashMap::new(),
                topic_ids: HashMap::new(),
                topic_partitions: HashMap::from([(TOPIC.to_owned(), 1)]),
            },
        )
        .await
        .expect("spawn consumer")
    }

    /// Kafka's `AbstractCoordinator.close` calls `maybeLeaveGroup` with the
    /// `CloseOptions` operation. `shouldSendLeaveGroupRequest` sends for
    /// `LEAVE_GROUP`, and for a dynamic member with `DEFAULT`. The member is
    /// named by its member id and the reason only.
    #[tokio::test]
    async fn close_leaves_the_group_as_the_membership_operation_says() {
        let leave = LeaveGroupRequest {
            group_id: "group-a".into(),
            // Version 5 has no top-level member id.
            member_id: String::new(),
            members: vec![MemberIdentity {
                member_id: MEMBER.into(),
                group_instance_id: None,
                reason: Some("the consumer is being closed".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, group_instance_id, operation, expected) in [
            (
                "dynamic member, default",
                None,
                GroupMembershipOperation::Default,
                vec![leave.clone()],
            ),
            (
                "static member, default",
                Some("i-1"),
                GroupMembershipOperation::Default,
                Vec::new(),
            ),
            (
                "static member, leave group",
                Some("i-1"),
                GroupMembershipOperation::LeaveGroup,
                vec![leave.clone()],
            ),
            (
                "dynamic member, remain in group",
                None,
                GroupMembershipOperation::RemainInGroup,
                Vec::new(),
            ),
        ] {
            let coordinator = MockCoordinator::new(Assignor::Range, vec![vec![partition(0)]]);
            let in_mock = Arc::clone(&coordinator);
            let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
                in_mock.respond(api_key, version, body)
            })
            .await;
            let consumer = started_consumer(&mock, group_instance_id).await;
            consumer.close_with(operation).await.expect("close");
            mock.stop();
            actual.push((
                name,
                coordinator.leaves.lock().expect("leaves lock").clone(),
            ));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// A heartbeat can request a rebalance after the start of `poll` signalled
    /// the coordinator task. The eager `poll` that then waits for the join
    /// must still start it, or it waits until its timeout.
    #[tokio::test]
    async fn a_poll_that_waits_for_an_eager_join_starts_a_join_requested_after_it_began() {
        let coordinator = MockCoordinator::new(Assignor::Range, vec![vec![partition(0)]]);
        let in_mock = Arc::clone(&coordinator);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            in_mock.respond(api_key, version, body)
        })
        .await;
        let mut consumer = started_consumer(&mock, None).await;
        // The start of `poll` signals the task.
        crate::coordinator::note_poll(&consumer.poll_signal);
        coordinator.heartbeat_error.store(27, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !*consumer.rebalance_pending.borrow() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the heartbeat requests a rebalance");

        let started = tokio::time::Instant::now();
        let joined = consumer
            .wait_for_rebalance(started + Duration::from_secs(5))
            .await;
        let within_timeout = started.elapsed() < Duration::from_secs(4);
        let joins = coordinator.joins.lock().expect("joins lock").len();
        drop(consumer);
        mock.stop();
        assert2::assert!((joined, within_timeout, joins) == (true, true, 1));
    }

    /// Kafka's `JoinGroup` carries `AbstractCoordinator.rejoinReason`
    /// (KIP-800): "group is already rebalancing" after a heartbeat
    /// `REBALANCE_IN_PROGRESS`, and "encountered <error> from HEARTBEAT
    /// response" after a heartbeat error that resets the member.
    #[tokio::test]
    async fn join_group_carries_the_rejoin_reason() {
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, heartbeat_error, expected) in [
            (
                "rebalance in progress",
                27,
                ("member-a", "group is already rebalancing"),
            ),
            (
                "illegal generation",
                22,
                (
                    "member-a",
                    "encountered ILLEGAL_GENERATION from HEARTBEAT response",
                ),
            ),
            (
                "unknown member id",
                25,
                ("", "encountered UNKNOWN_MEMBER_ID from HEARTBEAT response"),
            ),
        ] {
            let coordinator = MockCoordinator::new(Assignor::Range, vec![vec![partition(0)]]);
            let in_mock = Arc::clone(&coordinator);
            let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
                in_mock.respond(api_key, version, body)
            })
            .await;
            let consumer = started_consumer(&mock, None).await;
            coordinator
                .heartbeat_error
                .store(heartbeat_error, Ordering::SeqCst);
            let join = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    crate::coordinator::note_poll(&consumer.poll_signal);
                    if let Some(join) = coordinator.joins.lock().expect("joins lock").first() {
                        break (join.member_id.clone(), join.reason.clone());
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("the consumer joins again");
            drop(consumer);
            mock.stop();
            actual.push((name, join));
            wanted.push((name, (expected.0.to_owned(), Some(expected.1.to_owned()))));
        }
        assert2::assert!(actual == wanted);
    }
}
