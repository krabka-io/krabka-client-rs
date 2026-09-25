//! `Consumer::commit_sync` and `commit_async`.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use krabka_protocol::owned::{
    offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestTopic},
    offset_commit_response::OffsetCommitResponse,
};
use tokio::sync::Mutex;

use crate::{
    consumer::{CommitIdentity, Consumer},
    coordinator::{
        COORDINATOR_LOAD_IN_PROGRESS, COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR, find_coordinator,
        is_retriable_coordinator_code, is_retriable_transport_error, next_backoff,
        retry_deadline_elapsed,
    },
    error::ConsumerError,
    offset_wire::{TopicNameOffsetCommit, build_commit_topics},
    position::PartitionPosition,
};

const ASYNC_COMMIT_IDLE: u8 = 0;
const ASYNC_COMMIT_RUNNING: u8 = 1;
const ASYNC_COMMIT_DIRTY: u8 = 2;

/// `UNKNOWN_TOPIC_OR_PARTITION`: the coordinator does not know the topic.
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// `REQUEST_TIMED_OUT`.
const REQUEST_TIMED_OUT: i16 = 7;
/// `ILLEGAL_GENERATION`.
const ILLEGAL_GENERATION: i16 = 22;
/// `UNKNOWN_MEMBER_ID`.
const UNKNOWN_MEMBER_ID: i16 = 25;
/// `REBALANCE_IN_PROGRESS`.
const REBALANCE_IN_PROGRESS: i16 = 27;
/// `TOPIC_AUTHORIZATION_FAILED`.
const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
/// `GROUP_AUTHORIZATION_FAILED`.
const GROUP_AUTHORIZATION_FAILED: i16 = 30;
/// `FENCED_INSTANCE_ID`.
const FENCED_INSTANCE_ID: i16 = 82;

/// The offset that a commit stores for one partition. Kafka's
/// `OffsetAndMetadata`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OffsetAndMetadata {
    /// The next offset to read, that is the last consumed offset plus 1.
    pub offset: i64,
    /// The leader epoch of the last consumed record, if known.
    pub leader_epoch: Option<i32>,
    /// A string that the coordinator stores with the offset. Kafka's default
    /// is the empty string.
    pub metadata: String,
}

impl OffsetAndMetadata {
    /// An offset with no leader epoch and empty metadata.
    #[must_use]
    pub fn new(offset: i64) -> Self {
        Self {
            offset,
            leader_epoch: None,
            metadata: String::new(),
        }
    }

    /// The commit of a fetch position with its raw wire leader epoch. `-1`
    /// means no known epoch.
    fn of_position(offset: i64, leader_epoch: i32) -> Self {
        Self {
            offset,
            leader_epoch: (leader_epoch >= 0).then_some(leader_epoch),
            metadata: String::new(),
        }
    }
}

/// The commits of fetch positions, as `(offset, leader_epoch)` pairs.
pub(crate) fn position_commits(
    offsets: HashMap<(String, i32), (i64, i32)>,
) -> HashMap<(String, i32), OffsetAndMetadata> {
    offsets
        .into_iter()
        .map(|(partition, (offset, leader_epoch))| {
            (
                partition,
                OffsetAndMetadata::of_position(offset, leader_epoch),
            )
        })
        .collect()
}

fn commit_offsets(
    raw_offsets: HashMap<(String, i32), i64>,
    positions: &HashMap<(String, i32), PartitionPosition>,
) -> HashMap<(String, i32), (i64, i32)> {
    raw_offsets
        .into_iter()
        .map(|(k, v)| {
            // Unwrap the position's leader epoch to raw wire `int32` for the
            // OffsetCommit `committed_leader_epoch` field.
            let epoch = positions.get(&k).map_or(-1, |p| p.offset_epoch.get());
            (k, (v, epoch))
        })
        .collect()
}

/// Check the offsets of [`Consumer::commit_offsets_sync`].
///
/// Kafka's `OffsetAndMetadata` rejects a negative offset. Kafka accepts any
/// other offset, also one past the consumed position.
fn validate_selected_offsets(
    offsets: &HashMap<(String, i32), OffsetAndMetadata>,
    assigned: &HashMap<(String, i32), u64>,
) -> Result<(), ConsumerError> {
    for ((topic, partition), offset) in offsets {
        if offset.offset < 0 {
            return Err(ConsumerError::InvalidOffset(offset.offset));
        }
        if !assigned.contains_key(&(topic.clone(), *partition)) {
            return Err(ConsumerError::IllegalState(format!(
                "cannot commit unassigned partition {topic}-{partition}"
            )));
        }
    }
    Ok(())
}

/// Take the offsets of an asynchronous commit.
///
/// With `auto_commit`, the function also raises the positions for the commit
/// before a `JoinGroup` to these offsets. See [`AutoCommit::record_sent`].
async fn snapshot_commit_offsets(
    commit_identity: &Arc<Mutex<CommitIdentity>>,
    offsets: &Arc<Mutex<HashMap<(String, i32), i64>>>,
    positions: &Arc<Mutex<HashMap<(String, i32), PartitionPosition>>>,
    auto_commit: Option<&AutoCommit>,
) -> (HashMap<(String, i32), OffsetAndMetadata>, (i32, String)) {
    let identity = commit_identity.lock().await.clone();
    let mut raw_offsets = offsets.lock().await.clone();
    raw_offsets.retain(|partition, offset| {
        identity.ownership_ids.contains_key(partition) && has_valid_position(*offset)
    });
    if raw_offsets.is_empty() {
        return (HashMap::new(), (identity.generation, identity.member_id));
    }
    let pos = positions.lock().await;
    let offsets = commit_offsets(raw_offsets, &pos);
    drop(pos);
    if let Some(auto_commit) = auto_commit {
        auto_commit
            .record_sent(sent_positions(&offsets, &identity.ownership_ids))
            .await;
    }
    (
        position_commits(offsets),
        (identity.generation, identity.member_id),
    )
}

/// The callback of an asynchronous commit. Kafka's `OffsetCommitCallback`.
///
/// The consumer calls it once with the offsets that the commit sent and the
/// result. An empty map means that the consumer had no position to commit.
pub type OffsetCommitCallback =
    Box<dyn FnOnce(&HashMap<(String, i32), OffsetAndMetadata>, Result<(), &ConsumerError>) + Send>;

/// The callbacks of the asynchronous commits that wait for the next snapshot.
pub(crate) type OffsetCommitCallbacks = Arc<std::sync::Mutex<Vec<OffsetCommitCallback>>>;

/// Take the callbacks that the next asynchronous commit snapshot covers.
fn take_callbacks(callbacks: &OffsetCommitCallbacks) -> Vec<OffsetCommitCallback> {
    std::mem::take(
        &mut *callbacks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
}

/// The result that an asynchronous commit gives to its callbacks.
///
/// This follows Kafka's `ConsumerCoordinator.OffsetCommitResponseHandler` and
/// `doCommitOffsetsAsync`. A retriable error becomes
/// [`ConsumerError::RetriableCommitFailed`]. When partitions have errors of
/// more than one class, the class with the largest [`PartitionCommitError`]
/// wins, as in [`auto_commit_outcome`].
fn async_commit_result(
    result: Result<OffsetCommitResponse, ConsumerError>,
    group_id: &str,
    group_instance_id: Option<&str>,
) -> Result<(), ConsumerError> {
    let response = match result {
        Ok(response) => response,
        Err(ConsumerError::Client(error))
            if is_retriable_transport_error(&error)
                || matches!(error, krabka_client_core::ClientError::Timeout(_)) =>
        {
            return Err(ConsumerError::RetriableCommitFailed(Box::new(
                ConsumerError::Client(error),
            )));
        }
        // `CommitRoute::send` finds the coordinator again after 15 or 16.
        // Kafka gives a failed lookup to the callback as
        // `RetriableCommitFailedException` (`commitOffsetsAsync`).
        Err(ConsumerError::CoordinatorUnavailable) => {
            return Err(ConsumerError::RetriableCommitFailed(Box::new(
                ConsumerError::CoordinatorUnavailable,
            )));
        }
        Err(ConsumerError::Server(code)) if is_retriable_coordinator_code(code) => {
            return Err(ConsumerError::RetriableCommitFailed(Box::new(
                ConsumerError::Server(code),
            )));
        }
        Err(error) => return Err(error),
    };
    let partitions = || {
        response.topics.iter().flat_map(|topic| {
            topic
                .partitions
                .iter()
                .map(move |partition| (topic, partition.error_code))
        })
    };
    let class = PartitionCommitError::of_response(&response);
    let code = partitions()
        .map(|(_, code)| code)
        .find(|code| PartitionCommitError::of(*code) == class)
        .unwrap_or_default();
    match class {
        PartitionCommitError::None => Ok(()),
        PartitionCommitError::TopicAuthorization => Err(ConsumerError::TopicAuthorizationFailed(
            partitions()
                .filter(|(_, code)| *code == TOPIC_AUTHORIZATION_FAILED)
                .map(|(topic, _)| topic.name.clone())
                .collect(),
        )),
        PartitionCommitError::Retriable | PartitionCommitError::CoordinatorUnknown => Err(
            ConsumerError::RetriableCommitFailed(Box::new(ConsumerError::Server(code))),
        ),
        PartitionCommitError::Rebalance => Err(ConsumerError::CommitFailed),
        PartitionCommitError::FencedInstanceId => Err(ConsumerError::FencedInstanceId(
            group_instance_id.unwrap_or_default().to_owned(),
        )),
        PartitionCommitError::GroupAuthorization => {
            Err(ConsumerError::GroupAuthorizationFailed(group_id.to_owned()))
        }
        PartitionCommitError::Fatal => Err(ConsumerError::Server(code)),
    }
}

/// Build the `OffsetCommit` request of a commit. The request names each
/// topic, so the version is v9 or lower. See [`TopicNameOffsetCommit`].
pub(crate) fn build_commit_request(
    group_id: String,
    generation_id_or_member_epoch: i32,
    member_id: String,
    group_instance_id: Option<String>,
    topics: Vec<OffsetCommitRequestTopic>,
) -> TopicNameOffsetCommit {
    TopicNameOffsetCommit(OffsetCommitRequest {
        group_id,
        generation_id_or_member_epoch,
        member_id,
        group_instance_id,
        topics,
        ..Default::default()
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CommitOutcome {
    Acked(HashSet<(String, i32)>),
    /// Kafka's `OffsetCommitResponseHandler` raises a retriable error, and
    /// `commitOffsetsSync` sends the commit again until the timeout.
    Retriable {
        code: i16,
        acknowledged: HashSet<(String, i32)>,
        /// Kafka calls `markCoordinatorUnknown` before the retry, so the next
        /// attempt finds the coordinator again.
        find_coordinator: bool,
    },
}

/// What a rebalance-class partition error (`ILLEGAL_GENERATION`,
/// `UNKNOWN_MEMBER_ID`, `REBALANCE_IN_PROGRESS` and a `FENCED_INSTANCE_ID`
/// that followed a generation change) raises, as Kafka's
/// `ConsumerCoordinator.OffsetCommitResponseHandler.handle` does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RebalanceCommitFailure {
    /// The generation changed since the request: the group already moved to a
    /// new generation, and this commit's offsets were never recorded. Kafka's
    /// `CommitFailedException`.
    CommitFailed,
    /// `REBALANCE_IN_PROGRESS`, or a generation-changing code seen while the
    /// generation is still the request's: the group is still
    /// `PREPARING_REBALANCE`. Kafka's `RebalanceInProgressException`.
    RebalanceInProgress,
}

/// The consumer state that decides what an `OffsetCommit` partition error
/// means for a synchronous commit.
#[derive(Clone, Copy, Debug)]
struct CommitResponseContext<'a> {
    group_id: &'a str,
    group_instance_id: Option<&'a str>,
    /// The coordinator task runs, so it can join the group again.
    coordinator_alive: bool,
    /// The generation and the member id are still the ones of the request.
    /// Kafka's `CoordinatorResponseHandler.generationUnchanged`.
    generation_unchanged: bool,
}

/// Map an `OffsetCommit` response of a synchronous commit to a result, as
/// Kafka's `ConsumerCoordinator.OffsetCommitResponseHandler.handle` and
/// `commitOffsetsSync` do.
///
/// - `0`: the partition is acknowledged.
/// - `GROUP_AUTHORIZATION_FAILED (30)`: [`ConsumerError::GroupAuthorizationFailed`].
/// - `TOPIC_AUTHORIZATION_FAILED (29)`: the function collects the topics and
///   returns [`ConsumerError::TopicAuthorizationFailed`] when no other partition
///   has an error.
/// - `UNKNOWN_TOPIC_OR_PARTITION (3)`, `COORDINATOR_LOAD_IN_PROGRESS (14)`:
///   retriable.
/// - `REQUEST_TIMED_OUT (7)`, `COORDINATOR_NOT_AVAILABLE (15)`,
///   `NOT_COORDINATOR (16)`: retriable after the consumer finds the coordinator
///   again.
/// - `FENCED_INSTANCE_ID (82)` with the generation of the request:
///   [`ConsumerError::FencedInstanceId`].
/// - `ILLEGAL_GENERATION (22)`, `UNKNOWN_MEMBER_ID (25)` and `82` after the
///   generation changed: [`ConsumerError::CommitFailed`].
/// - `REBALANCE_IN_PROGRESS (27)`, and `22`/`25` seen while the generation is
///   still the request's (the group is still `PREPARING_REBALANCE`):
///   [`ConsumerError::RebalanceInProgress`].
/// - Any other code, for example `OFFSET_METADATA_TOO_LARGE (12)` and
///   `INVALID_COMMIT_OFFSET_SIZE (28)`: [`ConsumerError::Server`].
///
/// This follows Kafka's classic consumer exactly: `commitOffsetsSync` does not
/// retry a rebalance-class error, and it commits nothing for the request that
/// hit it. The krabka coordinator task still joins the group again in the
/// background regardless of this outcome, so a caller that calls `poll()`
/// after the error and commits again finds a consumer that is at, or well on
/// its way to, the next generation.
///
/// Kafka stops at the first partition error other than `29`, so its result
/// for a response with errors of more than one class depends on the partition
/// order. This function gives a final error precedence over a rebalance-class
/// error, a rebalance-class error over a retriable error, and a retriable
/// error over `29`.
fn commit_response_outcome(
    resp: &OffsetCommitResponse,
    context: CommitResponseContext<'_>,
) -> Result<CommitOutcome, ConsumerError> {
    let mut rebalance = None;
    let mut retriable = None;
    let mut find_coordinator = false;
    let mut unauthorized_topics = BTreeSet::new();
    let mut acknowledged = HashSet::new();
    for topic in &resp.topics {
        for partition in &topic.partitions {
            let code = partition.error_code;
            match PartitionCommitError::of(code) {
                PartitionCommitError::None => {
                    acknowledged.insert((topic.name.clone(), partition.partition_index));
                }
                PartitionCommitError::TopicAuthorization => {
                    unauthorized_topics.insert(topic.name.clone());
                }
                PartitionCommitError::Retriable => {
                    retriable.get_or_insert(code);
                }
                PartitionCommitError::CoordinatorUnknown => {
                    retriable.get_or_insert(code);
                    find_coordinator = true;
                }
                PartitionCommitError::FencedInstanceId if context.generation_unchanged => {
                    return Err(ConsumerError::FencedInstanceId(
                        context.group_instance_id.unwrap_or_default().to_owned(),
                    ));
                }
                PartitionCommitError::Rebalance | PartitionCommitError::FencedInstanceId => {
                    let failure = if context.coordinator_alive
                        && (code == REBALANCE_IN_PROGRESS || context.generation_unchanged)
                    {
                        RebalanceCommitFailure::RebalanceInProgress
                    } else {
                        RebalanceCommitFailure::CommitFailed
                    };
                    rebalance.get_or_insert(failure);
                }
                PartitionCommitError::GroupAuthorization => {
                    return Err(ConsumerError::GroupAuthorizationFailed(
                        context.group_id.to_owned(),
                    ));
                }
                PartitionCommitError::Fatal => return Err(ConsumerError::Server(code)),
            }
        }
    }
    match (rebalance, retriable) {
        (Some(RebalanceCommitFailure::CommitFailed), _) => Err(ConsumerError::CommitFailed),
        (Some(RebalanceCommitFailure::RebalanceInProgress), _) => Err(
            ConsumerError::RebalanceInProgress(context.group_id.to_owned()),
        ),
        (None, Some(code)) => Ok(CommitOutcome::Retriable {
            code,
            acknowledged,
            find_coordinator,
        }),
        (None, None) if !unauthorized_topics.is_empty() => {
            Err(ConsumerError::TopicAuthorizationFailed(unauthorized_topics))
        }
        (None, None) => Ok(CommitOutcome::Acked(acknowledged)),
    }
}

fn retain_continuously_owned(
    pending: &mut HashMap<(String, i32), (OffsetAndMetadata, u64)>,
    current: &HashMap<(String, i32), u64>,
) {
    pending.retain(|partition, (_, ownership_id)| current.get(partition) == Some(ownership_id));
}

/// The committable position of one owned partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ConsumedPosition {
    pub offset: i64,
    pub leader_epoch: i32,
    /// The ownership that the position belongs to. A commit sends the position
    /// only while the consumer still holds this ownership.
    pub ownership_id: u64,
}

/// Committable positions, keyed like `Consumer::next_offsets`.
pub(crate) type ConsumedPositions = HashMap<(String, i32), ConsumedPosition>;

/// Whether `next_offset` is a fetch position that Kafka can commit.
///
/// A reset sentinel is a reset that `poll` has not resolved yet. Kafka's
/// `SubscriptionState.allConsumed` skips a partition without a valid position.
fn has_valid_position(next_offset: i64) -> bool {
    next_offset >= 0 && !crate::poll::is_reset_sentinel(next_offset)
}

/// Kafka's `SubscriptionState.allConsumed`: the position and leader epoch of
/// each owned partition that has a valid position.
pub(crate) fn all_consumed(
    ownership_ids: &HashMap<(String, i32), u64>,
    next_offsets: &HashMap<(String, i32), i64>,
    positions: &HashMap<(String, i32), PartitionPosition>,
) -> ConsumedPositions {
    next_offsets
        .iter()
        .filter(|(_, offset)| has_valid_position(**offset))
        .filter_map(|(partition, offset)| {
            ownership_ids.get(partition).map(|ownership_id| {
                let leader_epoch = positions
                    .get(partition)
                    .map_or(-1, |position| position.offset_epoch.get());
                (
                    partition.clone(),
                    ConsumedPosition {
                        offset: *offset,
                        leader_epoch,
                        ownership_id: *ownership_id,
                    },
                )
            })
        })
        .collect()
}

/// The `(offset, leader_epoch)` of each position whose ownership is still
/// current.
fn owned_offsets(
    consumed: &ConsumedPositions,
    ownership_ids: &HashMap<(String, i32), u64>,
) -> HashMap<(String, i32), (i64, i32)> {
    consumed
        .iter()
        .filter(|(partition, position)| {
            ownership_ids.get(*partition) == Some(&position.ownership_id)
        })
        .map(|(partition, position)| (partition.clone(), (position.offset, position.leader_epoch)))
        .collect()
}

/// The committable positions of the `(offset, leader_epoch)` pairs that a
/// commit sends, with the ownership of each partition.
fn sent_positions(
    offsets: &HashMap<(String, i32), (i64, i32)>,
    ownership_ids: &HashMap<(String, i32), u64>,
) -> Vec<((String, i32), ConsumedPosition)> {
    offsets
        .iter()
        .filter_map(|(partition, (offset, leader_epoch))| {
            ownership_ids.get(partition).map(|ownership_id| {
                (
                    partition.clone(),
                    ConsumedPosition {
                        offset: *offset,
                        leader_epoch: *leader_epoch,
                        ownership_id: *ownership_id,
                    },
                )
            })
        })
        .collect()
}

/// The committable positions of the offsets that a synchronous commit sends.
fn pending_positions(
    pending: &HashMap<(String, i32), (OffsetAndMetadata, u64)>,
) -> Vec<((String, i32), ConsumedPosition)> {
    pending
        .iter()
        .map(|(partition, (offset, ownership_id))| {
            (
                partition.clone(),
                ConsumedPosition {
                    offset: offset.offset,
                    leader_epoch: offset.leader_epoch.unwrap_or(-1),
                    ownership_id: *ownership_id,
                },
            )
        })
        .collect()
}

/// When a synchronous commit raises the positions for the commit before a
/// `JoinGroup`. See [`AutoCommit::record_sent`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecordSent {
    /// A commit of the fetch positions records them before it sends, so a
    /// commit before a `JoinGroup` cannot move the committed offset back
    /// behind a request whose result is not known.
    BeforeSend,
    /// A commit of caller-selected offsets records only what the coordinator
    /// acknowledged. Such an offset can be past the fetch position, and a
    /// rejected one must not reach the commit before a `JoinGroup`. Kafka's
    /// `onJoinPrepare` commits `allConsumed`, the fetch positions.
    AfterAck,
}

/// How an automatic commit ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AutoCommitOutcome {
    Committed,
    /// Kafka's `RetriableCommitFailedException`: the coordinator is loading,
    /// moved or timed out, the topic is unknown, or the connection failed.
    Retriable,
    Failed,
}

/// What Kafka's `ConsumerCoordinator.OffsetCommitResponseHandler.handle` does
/// with the error code of one partition. A larger class takes precedence in
/// [`auto_commit_outcome`] when a response has errors of more than one class.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum PartitionCommitError {
    None,
    /// `TOPIC_AUTHORIZATION_FAILED`. Kafka collects the unauthorized topics
    /// and raises `TopicAuthorizationException` only when no other partition
    /// has an error. Any other error takes precedence.
    TopicAuthorization,
    /// `UNKNOWN_TOPIC_OR_PARTITION` and `COORDINATOR_LOAD_IN_PROGRESS`. Kafka
    /// retries.
    Retriable,
    /// `REQUEST_TIMED_OUT`, `COORDINATOR_NOT_AVAILABLE` and `NOT_COORDINATOR`.
    /// Kafka calls `markCoordinatorUnknown` and retries.
    CoordinatorUnknown,
    /// `ILLEGAL_GENERATION`, `UNKNOWN_MEMBER_ID` and `REBALANCE_IN_PROGRESS`.
    /// Kafka raises `CommitFailedException` or `RebalanceInProgressException`.
    Rebalance,
    /// `FENCED_INSTANCE_ID`. Kafka raises `FencedInstanceIdException` while the
    /// generation is unchanged, and a rebalance error after it changed.
    FencedInstanceId,
    /// `GROUP_AUTHORIZATION_FAILED`. Kafka raises
    /// `GroupAuthorizationException`.
    GroupAuthorization,
    /// Any other code. Kafka raises the error to the application:
    /// `OFFSET_METADATA_TOO_LARGE` and `INVALID_COMMIT_OFFSET_SIZE` as
    /// themselves, other codes as "Unexpected error in commit".
    Fatal,
}

impl PartitionCommitError {
    fn of(code: i16) -> Self {
        match code {
            0 => Self::None,
            TOPIC_AUTHORIZATION_FAILED => Self::TopicAuthorization,
            UNKNOWN_TOPIC_OR_PARTITION | COORDINATOR_LOAD_IN_PROGRESS => Self::Retriable,
            REQUEST_TIMED_OUT | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR => {
                Self::CoordinatorUnknown
            }
            ILLEGAL_GENERATION | UNKNOWN_MEMBER_ID | REBALANCE_IN_PROGRESS => Self::Rebalance,
            FENCED_INSTANCE_ID => Self::FencedInstanceId,
            GROUP_AUTHORIZATION_FAILED => Self::GroupAuthorization,
            _ => Self::Fatal,
        }
    }
}

impl PartitionCommitError {
    /// The class of `response` in [`auto_commit_outcome`]: the largest class of
    /// its partitions.
    fn of_response(response: &OffsetCommitResponse) -> Self {
        response
            .topics
            .iter()
            .flat_map(|topic| topic.partitions.iter())
            .map(|partition| Self::of(partition.error_code))
            .max()
            .unwrap_or(Self::None)
    }
}

/// Whether `response` makes Kafka's `OffsetCommitResponseHandler` call
/// `markCoordinatorUnknown`, so the consumer finds the coordinator again and
/// the commit is retriable. A partition with a final error or a rebalance
/// error takes precedence, as in [`auto_commit_outcome`].
pub(crate) fn names_moved_coordinator(response: &OffsetCommitResponse) -> bool {
    PartitionCommitError::of_response(response) == PartitionCommitError::CoordinatorUnknown
}

/// Classify the result of one automatic `OffsetCommit`, as Kafka's
/// `ConsumerCoordinator.OffsetCommitResponseHandler` does.
///
/// The function reads the error of every partition. Kafka's handler stops at
/// the first partition error other than `TOPIC_AUTHORIZATION_FAILED`, so its
/// result for a response with a retriable and a fatal partition error depends
/// on the partition order. That order comes from hash maps, here and in Kafka
/// (`ConsumerCoordinator.sendOffsetCommitRequest`). This function gives the
/// fatal error precedence, so the result does not depend on that order. The
/// commit before a `JoinGroup` then does not retry until the rebalance timeout
/// when a partition can never commit.
pub(crate) fn auto_commit_outcome(
    result: &Result<OffsetCommitResponse, ConsumerError>,
) -> AutoCommitOutcome {
    match result {
        Ok(response) => match PartitionCommitError::of_response(response) {
            PartitionCommitError::None => AutoCommitOutcome::Committed,
            PartitionCommitError::Retriable | PartitionCommitError::CoordinatorUnknown => {
                AutoCommitOutcome::Retriable
            }
            PartitionCommitError::TopicAuthorization
            | PartitionCommitError::Rebalance
            | PartitionCommitError::FencedInstanceId
            | PartitionCommitError::GroupAuthorization
            | PartitionCommitError::Fatal => AutoCommitOutcome::Failed,
        },
        Err(ConsumerError::Client(error))
            if is_retriable_transport_error(error)
                || matches!(error, krabka_client_core::ClientError::Timeout(_)) =>
        {
            AutoCommitOutcome::Retriable
        }
        Err(ConsumerError::Server(code)) if is_retriable_coordinator_code(*code) => {
            AutoCommitOutcome::Retriable
        }
        Err(_) => AutoCommitOutcome::Failed,
    }
}

/// Kafka's `enable.auto.commit` state for one consumer. The consumer and its
/// coordinator task share it.
#[derive(Clone, Debug)]
pub(crate) struct AutoCommit {
    /// Kafka's `auto.commit.interval.ms`.
    interval: Duration,
    /// When the next interval commit from `poll` is due. Kafka's
    /// `ConsumerCoordinator.nextAutoCommitTimer`.
    next_due: Arc<Mutex<tokio::time::Instant>>,
    /// The positions at the start of the latest `poll`, raised to each offset
    /// that a commit sent after that `poll` for the same ownership.
    ///
    /// Kafka runs `onJoinPrepare` inside `poll`, so its pre-rebalance commit
    /// includes only records that the application received before that `poll`.
    /// The krabka coordinator task rebalances between two polls, while the
    /// application can still process the records of the last `poll`. The task
    /// therefore commits these positions and not the live ones.
    ///
    /// A commit of the application can send a newer offset than the positions
    /// of the latest `poll`. The pre-rebalance commit must not move the
    /// committed offset back behind it. Kafka's `allConsumed` is never behind a
    /// `commitSync()` of the same ownership, because both read the positions.
    polled: Arc<Mutex<ConsumedPositions>>,
}

impl AutoCommit {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            interval,
            next_due: Arc::new(Mutex::new(tokio::time::Instant::now() + interval)),
            polled: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Move the positions for the commit before a `JoinGroup` after `poll`
    /// resets fetch positions.
    ///
    /// Kafka's `allConsumed` reads the current position, so a reset in `poll`
    /// changes what `onJoinPrepare` commits. `Some(offset)` is a truncation or
    /// an out-of-range reset: the commit sends that offset. The position of a
    /// reset has no known leader epoch here, so the commit sends none (`-1`).
    /// `None` is a reset that waits for `ListOffsets`: the partition has no
    /// valid position, and the commit skips it, as `allConsumed` does. A
    /// partition without a position of the latest `poll` stays without one.
    pub(crate) async fn reset_polled(
        &self,
        resets: impl IntoIterator<Item = ((String, i32), Option<i64>)>,
    ) {
        let mut polled = self.polled.lock().await;
        for (partition, offset) in resets {
            match offset {
                Some(offset) => {
                    if let Some(position) = polled.get_mut(&partition) {
                        position.offset = offset;
                        position.leader_epoch = -1;
                    }
                }
                None => {
                    polled.remove(&partition);
                }
            }
        }
    }

    /// Raise the positions for the commit before a `JoinGroup` to the offsets
    /// that a commit sends. Call it while you hold `commit_serialization`,
    /// before the commit sends its request.
    pub(crate) async fn record_sent(
        &self,
        sent: impl IntoIterator<Item = ((String, i32), ConsumedPosition)>,
    ) {
        let mut polled = self.polled.lock().await;
        for (partition, position) in sent {
            let known = polled.entry(partition).or_insert(position);
            if known.ownership_id != position.ownership_id || known.offset < position.offset {
                *known = position;
            }
        }
    }

    /// Start the interval again. Kafka's `onJoinComplete` resets
    /// `nextAutoCommitTimer` after each assignment.
    pub(crate) async fn restart_interval(&self) {
        *self.next_due.lock().await = tokio::time::Instant::now() + self.interval;
    }

    /// The `(offset, leader_epoch)` of each position of the latest `poll`
    /// whose ownership is still current.
    pub(crate) async fn polled_offsets(
        &self,
        ownership_ids: &HashMap<(String, i32), u64>,
    ) -> HashMap<(String, i32), (i64, i32)> {
        owned_offsets(&*self.polled.lock().await, ownership_ids)
    }
}

/// The route of a background commit to the group coordinator.
#[derive(Clone)]
struct CommitRoute {
    client: krabka_client_core::Client,
    group_id: String,
    group_instance_id: Option<String>,
    coordinator_id: Arc<std::sync::atomic::AtomicI32>,
    retry_policy: crate::coordinator::CoordinatorRetryPolicy,
}

impl CommitRoute {
    /// Send one `OffsetCommit` to the coordinator.
    ///
    /// If the coordinator moved or the connection failed, find the coordinator
    /// again and send once more. A background commit does not wait through the
    /// full retry loop.
    async fn send(
        &self,
        topics: Vec<OffsetCommitRequestTopic>,
        generation: i32,
        member_id: &str,
    ) -> Result<OffsetCommitResponse, ConsumerError> {
        let request = |topics| {
            build_commit_request(
                self.group_id.clone(),
                generation,
                member_id.to_owned(),
                self.group_instance_id.clone(),
                topics,
            )
        };
        let target = self.coordinator_id.load(Ordering::Relaxed);
        let result = self
            .client
            .broker(target)
            .send(request(topics.clone()))
            .await;
        let moved = match &result {
            Ok(response) => names_moved_coordinator(response),
            Err(error) => is_retriable_transport_error(error),
        };
        if !moved {
            return result.map_err(ConsumerError::from);
        }
        let id = find_coordinator(&self.client, &self.group_id, self.retry_policy).await?;
        self.coordinator_id.store(id, Ordering::Relaxed);
        self.client
            .broker(id)
            .send(request(topics))
            .await
            .map_err(ConsumerError::from)
    }
}

impl Consumer {
    fn commit_route(&self) -> CommitRoute {
        CommitRoute {
            client: self.client.clone(),
            group_id: self.group_id.clone(),
            group_instance_id: self.group_instance_id.clone(),
            coordinator_id: Arc::clone(&self.coordinator_id),
            retry_policy: self.retry_policy,
        }
    }

    /// The identity and the committable positions of this consumer now.
    async fn consumed_positions(&self) -> (CommitIdentity, ConsumedPositions) {
        let identity = self.commit_identity.lock().await.clone();
        let next_offsets = self.next_offsets.lock().await.clone();
        let positions = self.positions.lock().await.clone();
        let consumed = all_consumed(&identity.ownership_ids, &next_offsets, &positions);
        (identity, consumed)
    }

    /// The auto commit of `poll`: Kafka's
    /// `ConsumerCoordinator.maybeAutoCommitOffsetsAsync(now)`.
    ///
    /// This method records the positions for a later pre-rebalance commit. When
    /// the interval has passed, it sends an asynchronous commit of the
    /// positions. A failure goes to the log. A retriable failure makes the next
    /// commit due after the retry backoff, as Kafka's `autoCommitOffsetsAsync`
    /// does.
    pub(crate) async fn maybe_auto_commit_async(&self) {
        let Some(auto_commit) = &self.auto_commit else {
            return;
        };
        let (identity, consumed) = self.consumed_positions().await;
        auto_commit.polled.lock().await.clone_from(&consumed);
        let mut next_due = auto_commit.next_due.lock().await;
        let now = tokio::time::Instant::now();
        if now < *next_due {
            return;
        }
        // Take the commit lock before `poll` continues, so a later
        // `commit_sync` cannot commit newer offsets before this older commit.
        // If another commit holds the lock, `poll` does not wait for it: the
        // commit stays due and the next `poll` tries again.
        let Ok(commit_guard) = Arc::clone(&self.commit_serialization).try_lock_owned() else {
            return;
        };
        *next_due = now + auto_commit.interval;
        drop(next_due);
        let offsets = owned_offsets(&consumed, &identity.ownership_ids);
        if offsets.is_empty() {
            return;
        }
        let route = self.commit_route();
        let next_due = Arc::clone(&auto_commit.next_due);
        let retry_backoff = self.retry_policy.initial_backoff;
        tokio::spawn(async move {
            let _commit_guard = commit_guard;
            let result = route
                .send(
                    build_commit_topics(position_commits(offsets)),
                    identity.generation,
                    &identity.member_id,
                )
                .await;
            match auto_commit_outcome(&result) {
                AutoCommitOutcome::Committed => {}
                AutoCommitOutcome::Retriable => {
                    tracing::debug!(
                        group = %route.group_id,
                        ?result,
                        "asynchronous auto commit failed with a retriable error"
                    );
                    *next_due.lock().await = tokio::time::Instant::now() + retry_backoff;
                }
                AutoCommitOutcome::Failed => {
                    tracing::warn!(
                        group = %route.group_id,
                        ?result,
                        "asynchronous auto commit failed"
                    );
                }
            }
        });
    }

    /// The auto commit of `close`: Kafka's
    /// `ConsumerCoordinator.maybeAutoCommitOffsetsSync(timer)`.
    ///
    /// This method commits the current positions and waits for the result, up
    /// to Kafka's default close timeout. A failure goes to the log, and `close`
    /// continues.
    /// Wait until no asynchronous commit is queued or running, or until
    /// `deadline`. Kafka's `ConsumerCoordinator.close` polls while
    /// `pendingAsyncCommits > 0 && timer.notExpired()` and invokes the
    /// completed callbacks.
    pub(crate) async fn wait_for_async_commits(&self, deadline: tokio::time::Instant) {
        let wait = async {
            loop {
                if self.commit_async_state.load(Ordering::Acquire) == ASYNC_COMMIT_IDLE {
                    return;
                }
                // The worker holds the lock while it snapshots and sends.
                drop(self.commit_serialization.lock().await);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        if tokio::time::timeout_at(deadline, wait).await.is_err() {
            tracing::warn!(
                group = %self.group_id,
                "asynchronous commits were still pending when the close timeout expired"
            );
        }
    }

    pub(crate) async fn auto_commit_on_close(&self, deadline: tokio::time::Instant) {
        if self.auto_commit.is_none() {
            return;
        }
        // The close timeout also bounds the wait for another commit that holds
        // the commit lock.
        let commit = async {
            let _commit_guard = self.commit_serialization.lock().await;
            let (_, consumed) = self.consumed_positions().await;
            let pending = consumed
                .into_iter()
                .map(|(partition, position)| {
                    (
                        partition,
                        (
                            OffsetAndMetadata::of_position(position.offset, position.leader_epoch),
                            position.ownership_id,
                        ),
                    )
                })
                .collect::<HashMap<_, _>>();
            if pending.is_empty() {
                return Ok(());
            }
            self.commit_pending_offsets(pending, RecordSent::BeforeSend)
                .await
        };
        match tokio::time::timeout_at(deadline, commit).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(group = %self.group_id, %error, "synchronous auto commit on close failed");
            }
            Err(_) => {
                tracing::warn!(group = %self.group_id, "synchronous auto commit on close timed out");
            }
        }
    }
}

impl Consumer {
    /// Commit the current next-offsets for every assigned partition.
    ///
    /// This method blocks until the broker acks.
    #[cfg_attr(test, mutants::skip)] // cargo-mutants: I/O-bound coordinator RPC, exercised by integration tests
    #[tracing::instrument(
        name = "consumer.commit_sync",
        level = "debug",
        skip_all,
        fields(
            group_id = %self.group_id,
            member_id = %self.member_id(),
            generation = self.current_generation.load(Ordering::Relaxed),
            partitions = tracing::field::Empty,
        ),
        err
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn commit_sync(&self) -> Result<(), ConsumerError> {
        self.require_group_id()?;
        let _commit_guard = self.commit_serialization.lock().await;
        let pending = {
            let identity = self.commit_identity.lock().await;
            self.ensure_active_group(&identity)?;
            let offsets = self.next_offsets.lock().await;
            let positions = self.positions.lock().await;
            // Kafka's `commitSync()` commits `SubscriptionState.allConsumed`:
            // the position and the leader epoch at the call.
            all_consumed(&identity.ownership_ids, &offsets, &positions)
                .into_iter()
                .map(|(partition, position)| {
                    (
                        partition,
                        (
                            OffsetAndMetadata::of_position(position.offset, position.leader_epoch),
                            position.ownership_id,
                        ),
                    )
                })
                .collect::<HashMap<_, _>>()
        };
        if pending.is_empty() {
            return Ok(());
        }
        tracing::Span::current().record("partitions", pending.len());

        self.commit_pending_offsets(pending, RecordSent::BeforeSend)
            .await
    }

    /// Commit caller-selected offsets for currently assigned partitions.
    ///
    /// This is Kafka's `commitSync(Map<TopicPartition, OffsetAndMetadata>)`.
    /// The call commits each offset with its leader epoch and metadata. It
    /// does not commit other partitions and does not change the fetch
    /// positions. An offset can be past the consumed position, as in Kafka.
    ///
    /// # Errors
    ///
    /// `Ok(())` means each requested offset was broker-acknowledged while this
    /// consumer continuously owned its partition, or that ownership ended and
    /// the new owner will safely replay from the prior committed offset.
    ///
    /// Returns an error if an offset is negative, targets an unassigned
    /// partition, or the coordinator rejects the commit. A rebalance error
    /// ([`ConsumerError::CommitFailed`] or
    /// [`ConsumerError::RebalanceInProgress`]) is not retried and commits
    /// nothing: call `poll()` and commit again, as Kafka's
    /// `commitSync` requires.
    pub async fn commit_offsets_sync(
        &self,
        offsets: HashMap<(String, i32), OffsetAndMetadata>,
    ) -> Result<(), ConsumerError> {
        self.require_group_id()?;
        let _commit_guard = self.commit_serialization.lock().await;
        if offsets.is_empty() {
            return Ok(());
        }
        let pending = {
            let identity = self.commit_identity.lock().await;
            self.ensure_active_group(&identity)?;
            validate_selected_offsets(&offsets, &identity.ownership_ids)?;
            offsets
                .into_iter()
                .map(|(partition, offset)| {
                    let ownership_id = identity.ownership_ids[&partition];
                    (partition, (offset, ownership_id))
                })
                .collect::<HashMap<_, _>>()
        };

        self.commit_pending_offsets(pending, RecordSent::AfterAck)
            .await
    }

    /// Fail a synchronous commit when the consumer is not part of an active
    /// group.
    ///
    /// That is the case after a fatal coordinator error removed the member,
    /// until the next `poll` joins the group again, and after the coordinator
    /// task stopped. Kafka's `ConsumerCoordinator.sendOffsetCommitRequest`
    /// raises `CommitFailedException` in this state and sends no request.
    ///
    /// A fence sets `rejoin_on_poll` in the same critical section in which it
    /// clears the ownership. Pass an `identity` that you read under the
    /// `commit_identity` lock, so an ownership that the fence cleared always
    /// comes with the flag.
    fn ensure_active_group(&self, identity: &CommitIdentity) -> Result<(), ConsumerError> {
        if identity.rejoin_on_poll
            || self.coordinator_shutdown.is_cancelled()
            || self
                .coordinator_handle
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            return Err(ConsumerError::CommitFailed);
        }
        Ok(())
    }

    async fn commit_pending_offsets(
        &self,
        mut pending: HashMap<(String, i32), (OffsetAndMetadata, u64)>,
        record: RecordSent,
    ) -> Result<(), ConsumerError> {
        let retry_start = tokio::time::Instant::now();
        let mut retry_backoff = self.retry_policy.initial_backoff;
        loop {
            let identity = self.commit_identity.lock().await.clone();
            self.ensure_active_group(&identity)?;
            retain_continuously_owned(&mut pending, &identity.ownership_ids);
            if pending.is_empty() {
                return Ok(());
            }

            if record == RecordSent::BeforeSend
                && let Some(auto_commit) = &self.auto_commit
            {
                auto_commit.record_sent(pending_positions(&pending)).await;
            }
            let topics = build_commit_topics(
                pending
                    .iter()
                    .map(|(partition, (offset, _))| (partition.clone(), offset.clone()))
                    .collect(),
            );
            let outcome = match self
                .commit_topics_once(topics, (identity.generation, identity.member_id.clone()))
                .await
            {
                Ok(outcome) => outcome,
                // Kafka's `CoordinatorResponseHandler.onFailure` marks the
                // coordinator unknown after a disconnect, and
                // `commitOffsetsSync` retries until the timeout. A request
                // timeout is a disconnect in Kafka's `NetworkClient`.
                Err(ConsumerError::Client(error))
                    if is_retriable_transport_error(&error)
                        || matches!(error, krabka_client_core::ClientError::Timeout(_)) =>
                {
                    if retry_deadline_elapsed(retry_start, self.retry_policy.timeout) {
                        return Err(ConsumerError::CoordinatorUnavailable);
                    }
                    tracing::warn!(
                        group = %self.group_id,
                        %error,
                        "offset commit request failed; finding the coordinator again and retrying",
                    );
                    self.client
                        .evict_broker(self.coordinator_id.load(Ordering::Relaxed));
                    self.find_coordinator_again(retry_start).await;
                    if !self.sleep_before_retry(retry_start, retry_backoff).await {
                        return Err(ConsumerError::CoordinatorUnavailable);
                    }
                    retry_backoff = next_backoff(retry_backoff, self.retry_policy.max_backoff);
                    continue;
                }
                Err(error) => return Err(error),
            };
            match outcome {
                CommitOutcome::Acked(acknowledged) => {
                    self.record_acknowledged(record, &pending, &acknowledged)
                        .await;
                    pending.retain(|partition, _| !acknowledged.contains(partition));
                    if pending.is_empty() {
                        return Ok(());
                    }
                    return Err(ConsumerError::IllegalState(
                        "offset commit response omitted requested partitions".into(),
                    ));
                }
                CommitOutcome::Retriable {
                    code,
                    acknowledged,
                    find_coordinator,
                } => {
                    // Kafka's `ConsumerCoordinator.commitOffsetsSync` sends the
                    // commit again while it fails with a retriable error and
                    // the timeout has not passed.
                    self.record_acknowledged(record, &pending, &acknowledged)
                        .await;
                    pending.retain(|partition, _| !acknowledged.contains(partition));
                    if pending.is_empty() {
                        return Ok(());
                    }
                    if retry_deadline_elapsed(retry_start, self.retry_policy.timeout) {
                        return Err(ConsumerError::Server(code));
                    }
                    tracing::warn!(
                        group = %self.group_id,
                        error_code = code,
                        "offset commit failed with a retriable error; retrying",
                    );
                    if find_coordinator {
                        self.find_coordinator_again(retry_start).await;
                    }
                    if !self.sleep_before_retry(retry_start, retry_backoff).await {
                        return Err(ConsumerError::Server(code));
                    }
                    retry_backoff = next_backoff(retry_backoff, self.retry_policy.max_backoff);
                }
            }
        }
    }

    /// Raise the positions for the commit before a `JoinGroup` to the
    /// `acknowledged` offsets of `pending`, for a [`RecordSent::AfterAck`]
    /// commit.
    async fn record_acknowledged(
        &self,
        record: RecordSent,
        pending: &HashMap<(String, i32), (OffsetAndMetadata, u64)>,
        acknowledged: &HashSet<(String, i32)>,
    ) {
        if record != RecordSent::AfterAck {
            return;
        }
        let Some(auto_commit) = &self.auto_commit else {
            return;
        };
        let acked = pending
            .iter()
            .filter(|(partition, _)| acknowledged.contains(*partition))
            .map(|(partition, value)| (partition.clone(), value.clone()))
            .collect();
        auto_commit.record_sent(pending_positions(&acked)).await;
    }

    /// Wait `backoff` before the next attempt of a synchronous commit that
    /// started at `retry_start`, but not past the retry timeout. Return whether
    /// time is left for the next attempt. Kafka's `commitOffsetsSync` does
    /// `timer.sleep(backoff)` and then sends again only while
    /// `timer.notExpired()`.
    async fn sleep_before_retry(
        &self,
        retry_start: tokio::time::Instant,
        backoff: Duration,
    ) -> bool {
        let remaining = self
            .retry_policy
            .timeout
            .saturating_sub(retry_start.elapsed());
        tokio::time::sleep(backoff.min(remaining)).await;
        !retry_deadline_elapsed(retry_start, self.retry_policy.timeout)
    }

    /// Find the group coordinator again, within the time that is left of a
    /// synchronous commit that started at `retry_start`. Kafka's
    /// `commitOffsetsSync` does this in `coordinatorUnknownAndUnreadySync` after
    /// `markCoordinatorUnknown`. If the lookup fails, the next attempt uses the
    /// last known coordinator.
    async fn find_coordinator_again(&self, retry_start: tokio::time::Instant) {
        let retry_policy = crate::coordinator::CoordinatorRetryPolicy {
            timeout: self
                .retry_policy
                .timeout
                .saturating_sub(retry_start.elapsed()),
            ..self.retry_policy
        };
        match find_coordinator(&self.client, &self.group_id, retry_policy).await {
            Ok(id) => self.coordinator_id.store(id, Ordering::Relaxed),
            Err(error) => {
                tracing::warn!(
                    group = %self.group_id,
                    %error,
                    "coordinator lookup for an offset commit failed; retrying with the last known coordinator",
                );
            }
        }
    }

    /// Send one `OffsetCommit` to the coordinator and map the response.
    ///
    /// [`Consumer::commit_pending_offsets`] retries a retriable result.
    async fn commit_topics_once(
        &self,
        topics: Vec<OffsetCommitRequestTopic>,
        identity: (i32, String),
    ) -> Result<CommitOutcome, ConsumerError> {
        let (generation, member_id) = identity;
        let target = self.coordinator_id.load(Ordering::Relaxed);
        let resp = self
            .client
            .broker(target)
            .send(build_commit_request(
                self.group_id.clone(),
                generation,
                member_id.clone(),
                self.group_instance_id.clone(),
                topics,
            ))
            .await?;

        let current = self.commit_identity.lock().await.clone();
        // Without a subscription no coordinator task joins the group again, so
        // a rebalance error fails the commit, as in Kafka.
        let coordinator_alive = !self.subscription.borrow().is_none()
            && self
                .coordinator_handle
                .as_ref()
                .is_none_or(|h| !h.is_finished());
        let outcome = commit_response_outcome(
            &resp,
            CommitResponseContext {
                group_id: &self.group_id,
                group_instance_id: self.group_instance_id.as_deref(),
                coordinator_alive,
                generation_unchanged: current.generation == generation
                    && current.member_id == member_id,
            },
        )?;
        Ok(outcome)
    }

    /// Fire-and-forget commit.
    ///
    /// This method returns after scheduling the latest offsets for a background
    /// commit. Calls made while one is queued or running are coalesced into its
    /// snapshot or one follow-up snapshot. It does NOT wait for the broker ack.
    /// It logs errors and does not return them. Kafka's `commitAsync()` with
    /// its `DefaultOffsetCommitCallback`.
    #[cfg_attr(test, mutants::skip)] // cargo-mutants: fire-and-forget I/O spawn, exercised by integration tests
    #[tracing::instrument(
        name = "consumer.commit_async",
        level = "debug",
        skip_all,
        fields(
            group_id = %self.group_id,
            member_id = %self.member_id(),
            generation = self.current_generation.load(Ordering::Relaxed),
        )
    )]
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::InvalidGroupId`] without a group id.
    pub fn commit_async(&self) -> Result<(), ConsumerError> {
        self.require_group_id()?;
        self.schedule_commit_async();
        Ok(())
    }

    /// Commit the positions in the background, and call `callback` with the
    /// result. Kafka's `commitAsync(OffsetCommitCallback)`.
    ///
    /// The commit is coalesced with other asynchronous commits as
    /// [`Consumer::commit_async`] describes. The callback gets the offsets and
    /// the result of the commit that covers this call. The consumer calls it
    /// on a background task.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::InvalidGroupId`] without a group id. The
    /// callback then does not run.
    pub fn commit_async_with_callback<F>(&self, callback: F) -> Result<(), ConsumerError>
    where
        F: FnOnce(&HashMap<(String, i32), OffsetAndMetadata>, Result<(), &ConsumerError>)
            + Send
            + 'static,
    {
        self.require_group_id()?;
        self.commit_async_callbacks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Box::new(callback));
        self.schedule_commit_async();
        Ok(())
    }

    fn schedule_commit_async(&self) {
        loop {
            match self.commit_async_state.load(Ordering::Acquire) {
                ASYNC_COMMIT_IDLE => {
                    if self
                        .commit_async_state
                        .compare_exchange(
                            ASYNC_COMMIT_IDLE,
                            ASYNC_COMMIT_RUNNING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                ASYNC_COMMIT_RUNNING => {
                    if self
                        .commit_async_state
                        .compare_exchange(
                            ASYNC_COMMIT_RUNNING,
                            ASYNC_COMMIT_DIRTY,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
                ASYNC_COMMIT_DIRTY => return,
                _ => unreachable!("invalid async commit state"),
            }
        }

        let route = self.commit_route();
        let commit_identity = Arc::clone(&self.commit_identity);
        let commit_serialization = Arc::clone(&self.commit_serialization);
        let offsets = Arc::clone(&self.next_offsets);
        let positions = Arc::clone(&self.positions);
        let commit_async_state = Arc::clone(&self.commit_async_state);
        let auto_commit = self.auto_commit.clone();
        let callbacks = Arc::clone(&self.commit_async_callbacks);
        tokio::spawn(async move {
            loop {
                {
                    let _commit_guard = commit_serialization.lock().await;
                    // Calls queued before this snapshot are represented by the
                    // current offsets, so collapse them into this request.
                    commit_async_state.store(ASYNC_COMMIT_RUNNING, Ordering::Release);
                    let waiting = take_callbacks(&callbacks);
                    let (sent, (generation, member_id)) = snapshot_commit_offsets(
                        &commit_identity,
                        &offsets,
                        &positions,
                        auto_commit.as_ref(),
                    )
                    .await;
                    // Kafka completes an asynchronous commit of no offsets
                    // locally with success.
                    let result = if sent.is_empty() {
                        Ok(())
                    } else {
                        let response = route
                            .send(build_commit_topics(sent.clone()), generation, &member_id)
                            .await;
                        async_commit_result(
                            response,
                            &route.group_id,
                            route.group_instance_id.as_deref(),
                        )
                    };
                    // Kafka's `DefaultOffsetCommitCallback` logs a failed
                    // asynchronous commit, also one with a retriable error.
                    if let Err(error) = &result {
                        tracing::warn!(%error, "commit_async failed");
                    }
                    for callback in waiting {
                        // A panic in one callback must not stop the worker in
                        // the running state, which would disable later
                        // asynchronous commits.
                        let call = std::panic::AssertUnwindSafe(|| {
                            callback(&sent, result.as_ref().copied());
                        });
                        if std::panic::catch_unwind(call).is_err() {
                            tracing::error!("an offset commit callback panicked");
                        }
                    }
                }

                if commit_async_state
                    .compare_exchange(
                        ASYNC_COMMIT_RUNNING,
                        ASYNC_COMMIT_IDLE,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    return;
                }
                // A caller marked the worker dirty during the RPC. Claim that
                // single coalesced follow-up and rejoin the FIFO behind any
                // synchronous commit already waiting on the mutex.
                if commit_async_state
                    .compare_exchange(
                        ASYNC_COMMIT_DIRTY,
                        ASYNC_COMMIT_RUNNING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_err()
                {
                    return;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicI32, AtomicUsize},
        time::Duration,
    };

    use assert2::check;
    use krabka_client_core::{Client, MockBroker, MockReply};
    use krabka_protocol::{
        Encode, UnknownTaggedFields,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            find_coordinator_request, metadata_request, offset_commit_request,
            offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
            offset_commit_response::{
                OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
            },
        },
        primitives::uuid::Uuid,
    };
    use krabka_units::secs;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        AutoOffsetReset, IsolationLevel, consumer::ConsumerRetryPolicy,
        coordinator::CoordinatorRetryPolicy,
    };

    fn response(errors: &[i16]) -> OffsetCommitResponse {
        OffsetCommitResponse {
            throttle_time_ms: 0,
            topics: vec![OffsetCommitResponseTopic {
                name: "topic".into(),
                topic_id: Uuid::ZERO,
                partitions: errors
                    .iter()
                    .enumerate()
                    .map(
                        |(partition_index, error_code)| OffsetCommitResponsePartition {
                            partition_index: i32::try_from(partition_index).unwrap(),
                            error_code: *error_code,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                    )
                    .collect(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }

    fn encode_response(response: &OffsetCommitResponse, version: i16) -> Vec<u8> {
        let mut buf = bytes::BytesMut::new();
        response.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    /// An `ApiVersions` response that advertises `OffsetCommit` in
    /// `offset_commit_range`, and the `FindCoordinator` and `Metadata` requests
    /// of a coordinator lookup.
    fn api_versions_for_offset_commit(offset_commit_range: (i16, i16)) -> Vec<u8> {
        let response = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 3,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: offset_commit_request::API_KEY,
                    min_version: offset_commit_range.0,
                    max_version: offset_commit_range.1,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: find_coordinator_request::API_KEY,
                    min_version: 0,
                    max_version: 0,
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
        let mut buf = bytes::BytesMut::new();
        response.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    fn request_generation(body: &[u8]) -> i32 {
        fn string_end(body: &[u8], start: usize) -> usize {
            let len = i16::from_be_bytes([body[start], body[start + 1]]);
            start + 2 + usize::try_from(len.max(0)).unwrap()
        }
        let group_start = string_end(body, 0);
        let generation_start = string_end(body, group_start);
        i32::from_be_bytes(
            body[generation_start..generation_start + 4]
                .try_into()
                .unwrap(),
        )
    }

    fn request_member_id(body: &[u8]) -> String {
        fn string(body: &[u8], start: usize) -> (usize, String) {
            let len =
                usize::try_from(i16::from_be_bytes([body[start], body[start + 1]]).max(0)).unwrap();
            let end = start + 2 + len;
            (
                end,
                String::from_utf8(body[start + 2..end].to_vec()).unwrap(),
            )
        }
        let (group_start, _) = string(body, 0);
        let (generation_start, _) = string(body, group_start);
        let (_, member_id) = string(body, generation_start + 4);
        member_id
    }

    fn request_offsets(body: &[u8]) -> Vec<(i32, i64)> {
        fn string_end(body: &[u8], start: usize) -> usize {
            let len = i16::from_be_bytes([body[start], body[start + 1]]);
            start + 2 + usize::try_from(len.max(0)).unwrap()
        }
        fn i32_at(body: &[u8], start: usize) -> i32 {
            i32::from_be_bytes(body[start..start + 4].try_into().unwrap())
        }
        fn i64_at(body: &[u8], start: usize) -> i64 {
            i64::from_be_bytes(body[start..start + 8].try_into().unwrap())
        }

        let mut cursor = string_end(body, 0);
        cursor = string_end(body, cursor);
        cursor += 4;
        cursor = string_end(body, cursor);
        cursor += 8;
        let topic_count = usize::try_from(i32_at(body, cursor)).unwrap();
        cursor += 4;
        let mut offsets = Vec::new();
        for _ in 0..topic_count {
            cursor = string_end(body, cursor);
            let partition_count = usize::try_from(i32_at(body, cursor)).unwrap();
            cursor += 4;
            for _ in 0..partition_count {
                offsets.push((i32_at(body, cursor), i64_at(body, cursor + 4)));
                cursor += 12;
                cursor = string_end(body, cursor);
            }
        }
        offsets
    }

    fn commit_identity(generation: i32, member_id: &str) -> Arc<Mutex<CommitIdentity>> {
        Arc::new(Mutex::new(CommitIdentity {
            generation,
            member_id: member_id.into(),
            ownership_ids: HashMap::from([(("topic".into(), 0), 1)]),
            rejoin_on_poll: false,
        }))
    }

    async fn selected_commit_consumer(
        commit_identity: Arc<Mutex<CommitIdentity>>,
        assignment_changed: Arc<tokio::sync::Notify>,
        generation: Arc<AtomicI32>,
        requests: Arc<AtomicUsize>,
        mixed_response: bool,
        on_first_request: impl Fn(&Arc<Mutex<CommitIdentity>>, &Arc<AtomicI32>, i32, &str)
        + Send
        + 'static,
    ) -> (Consumer, MockBroker, Arc<std::sync::Mutex<Vec<(i32, i64)>>>) {
        let identity_in_mock = Arc::clone(&commit_identity);
        let generation_in_mock = Arc::clone(&generation);
        let changed_in_mock = Arc::clone(&assignment_changed);
        let requests_in_mock = Arc::clone(&requests);
        let seen_offsets = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_offsets_in_mock = Arc::clone(&seen_offsets);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_offset_commit((2, 2)));
            }
            if api_key != offset_commit_request::API_KEY {
                return None;
            }
            let attempt = requests_in_mock.fetch_add(1, Ordering::SeqCst);
            let offsets = request_offsets(body);
            seen_offsets_in_mock.lock().unwrap().extend(&offsets);
            let partitions = offsets
                .iter()
                .map(|(partition, _)| *partition)
                .collect::<Vec<_>>();
            if attempt == 0 {
                on_first_request(
                    &identity_in_mock,
                    &generation_in_mock,
                    request_generation(body),
                    &request_member_id(body),
                );
                changed_in_mock.notify_waiters();
                let errors = if mixed_response {
                    vec![0, 27]
                } else {
                    vec![27]
                };
                Some(encode_response(&response(&errors), version))
            } else {
                let errors = if mixed_response && partitions != [1] {
                    vec![42; partitions.len()]
                } else {
                    vec![0; partitions.len()]
                };
                let mut response = response(&errors);
                for (partition, partition_index) in
                    response.topics[0].partitions.iter_mut().zip(partitions)
                {
                    partition.partition_index = partition_index;
                }
                Some(encode_response(&response, version))
            }
        })
        .await;
        let consumer =
            commit_consumer(&mock, commit_identity, assignment_changed, generation).await;
        (consumer, mock, seen_offsets)
    }

    /// A consumer whose client talks to `mock`, which owns each partition of
    /// `commit_identity` at next offset 12.
    async fn commit_consumer(
        mock: &MockBroker,
        commit_identity: Arc<Mutex<CommitIdentity>>,
        assignment_changed: Arc<tokio::sync::Notify>,
        generation: Arc<AtomicI32>,
    ) -> Consumer {
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .build()
            .await
            .unwrap();
        let ownership = commit_identity.lock().await.ownership_ids.clone();
        let assigned = ownership.keys().cloned().collect::<Vec<_>>();
        let next_offsets = ownership
            .keys()
            .cloned()
            .map(|partition| (partition, 12))
            .collect();
        Consumer {
            client,
            group_id: "group-a".into(),
            coordinator_id: Arc::new(AtomicI32::new(0)),
            retry_policy: ConsumerRetryPolicy::default().into(),
            member_id: tokio::sync::watch::channel("member-a".to_owned()).1,
            commit_identity,
            commit_serialization: Arc::new(Mutex::new(())),
            commit_async_state: Arc::new(std::sync::atomic::AtomicU8::new(ASYNC_COMMIT_IDLE)),
            commit_async_callbacks: Arc::default(),
            group_instance_id: None,
            current_generation: generation,
            subscription: crate::subscription::shared(vec!["topic".into()], None, true),
            group_protocol: crate::GroupProtocol::Classic,
            assigned: Arc::new(Mutex::new(assigned)),
            assignment_changed,
            next_offsets: Arc::new(Mutex::new(next_offsets)),
            end_offsets: Arc::new(Mutex::new(HashMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
            topic_ids: Arc::new(Mutex::new(HashMap::new())),
            session_timeout: secs(45),
            heartbeat_interval: secs(3),
            rebalance_protocol: crate::assignor::RebalanceProtocol::Eager,
            coordinator_shutdown: CancellationToken::new(),
            coordinator_handle: None,
            isolation_level: IsolationLevel::ReadUncommitted,
            fetch_min: krabka_client_core::DEFAULT_FETCH_MIN,
            fetch_max: crate::poll::DEFAULT_FETCH_MAX,
            fetch_partition_max: crate::poll::DEFAULT_FETCH_PARTITION_MAX,
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

    /// Kafka's `OffsetAndMetadata` rejects a negative offset, and Kafka's
    /// `commitSync(offsets)` accepts an offset past the consumed position.
    #[test]
    fn selected_offset_validation_follows_kafka() {
        let assigned = HashMap::from([(("topic".to_string(), 2), 1)]);

        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, partition, offset, expected) in [
            (
                "negative",
                ("topic", 2),
                -1,
                Some("invalid seek offset -1: must be non-negative"),
            ),
            (
                "unassigned",
                ("other", 2),
                1,
                Some("illegal state: cannot commit unassigned partition other-2"),
            ),
            ("at the position", ("topic", 2), 11, None),
            ("past the position", ("topic", 2), 100, None),
        ] {
            let offsets = HashMap::from([(
                (partition.0.to_string(), partition.1),
                OffsetAndMetadata::new(offset),
            )]);
            let result = validate_selected_offsets(&offsets, &assigned).err();
            actual.push((name, result.map(|error| error.to_string())));
            wanted.push((name, expected.map(str::to_owned)));
        }
        assert2::assert!(actual == wanted);
    }

    #[test]
    fn commit_offsets_use_position_epoch_or_unknown_epoch() {
        let mut raw = HashMap::new();
        raw.insert(("known".into(), 0), 11);
        raw.insert(("unknown".into(), 1), 22);

        let mut positions = HashMap::new();
        positions.insert(
            ("known".into(), 0),
            PartitionPosition {
                offset_epoch: krabka_ids::LeaderEpoch(7),
                ..Default::default()
            },
        );

        let offsets = commit_offsets(raw, &positions);
        assert2::assert!(
            offsets
                == HashMap::from([
                    (("known".into(), 0), (11, 7)),
                    (("unknown".into(), 1), (22, -1)),
                ])
        );
    }

    /// Kafka's `SubscriptionState.allConsumed` commits each owned partition
    /// that has a valid position, with its leader epoch. A later commit sends a
    /// position only while its ownership is still current.
    #[test]
    fn consumed_positions_keep_owned_valid_positions_of_the_current_ownership() {
        let ownership_ids = HashMap::from([
            (("orders".into(), 0), 1),
            (("orders".into(), 1), 2),
            (("orders".into(), 2), 3),
            (("orders".into(), 3), 4),
        ]);
        let next_offsets = HashMap::from([
            (("orders".into(), 0), 12),
            (("orders".into(), 1), 0),
            (("orders".into(), 2), i64::MAX),
            (("orders".into(), 3), -1),
            (("unowned".into(), 0), 5),
        ]);
        let positions = HashMap::from([(
            ("orders".into(), 0),
            PartitionPosition {
                offset_epoch: krabka_ids::LeaderEpoch(4),
                ..Default::default()
            },
        )]);

        let consumed = all_consumed(&ownership_ids, &next_offsets, &positions);
        assert2::assert!(
            consumed
                == HashMap::from([
                    (
                        ("orders".into(), 0),
                        ConsumedPosition {
                            offset: 12,
                            leader_epoch: 4,
                            ownership_id: 1,
                        },
                    ),
                    (
                        ("orders".into(), 1),
                        ConsumedPosition {
                            offset: 0,
                            leader_epoch: -1,
                            ownership_id: 2,
                        },
                    ),
                ])
        );

        let reassigned = HashMap::from([(("orders".into(), 0), 9), (("orders".into(), 1), 2)]);
        assert2::assert!(
            owned_offsets(&consumed, &reassigned)
                == HashMap::from([(("orders".into(), 1), (0, -1))])
        );
    }

    /// Kafka's `OffsetCommitResponseHandler` raises a retriable error for 3, 7,
    /// 14, 15 and 16, and for a failed connection. Every other error is final.
    #[test]
    fn auto_commit_outcome_follows_kafka_commit_error_classes() {
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, result, expected) in [
            (
                "success",
                Ok(response(&[0, 0])),
                AutoCommitOutcome::Committed,
            ),
            (
                "unknown topic or partition",
                Ok(response(&[0, 3])),
                AutoCommitOutcome::Retriable,
            ),
            (
                "request timed out",
                Ok(response(&[7])),
                AutoCommitOutcome::Retriable,
            ),
            (
                "coordinator loading",
                Ok(response(&[14])),
                AutoCommitOutcome::Retriable,
            ),
            (
                "coordinator not available",
                Ok(response(&[15])),
                AutoCommitOutcome::Retriable,
            ),
            (
                "not coordinator",
                Ok(response(&[16])),
                AutoCommitOutcome::Retriable,
            ),
            (
                "illegal generation",
                Ok(response(&[22])),
                AutoCommitOutcome::Failed,
            ),
            (
                "rebalance in progress",
                Ok(response(&[27])),
                AutoCommitOutcome::Failed,
            ),
            (
                "group authorization failed",
                Ok(response(&[30])),
                AutoCommitOutcome::Failed,
            ),
            (
                "disconnected",
                Err(ConsumerError::Client(
                    krabka_client_core::ClientError::Disconnected,
                )),
                AutoCommitOutcome::Retriable,
            ),
            (
                "client timeout",
                Err(ConsumerError::Client(
                    krabka_client_core::ClientError::Timeout(secs(30)),
                )),
                AutoCommitOutcome::Retriable,
            ),
            (
                "coordinator lookup not available",
                Err(ConsumerError::Server(15)),
                AutoCommitOutcome::Retriable,
            ),
            (
                "coordinator lookup refused",
                Err(ConsumerError::Server(30)),
                AutoCommitOutcome::Failed,
            ),
            (
                "topic authorization failed",
                Ok(response(&[0, 29])),
                AutoCommitOutcome::Failed,
            ),
            (
                "retriable partition, then a fatal partition",
                Ok(response(&[3, 30])),
                AutoCommitOutcome::Failed,
            ),
            (
                "fatal partition, then a retriable partition",
                Ok(response(&[30, 3])),
                AutoCommitOutcome::Failed,
            ),
            (
                "retriable coordinator partition, then a fatal partition",
                Ok(response(&[16, 22])),
                AutoCommitOutcome::Failed,
            ),
            (
                "topic authorization failure, then a retriable partition",
                Ok(response(&[29, 3])),
                AutoCommitOutcome::Retriable,
            ),
            (
                "retriable partition, then a topic authorization failure",
                Ok(response(&[3, 29])),
                AutoCommitOutcome::Retriable,
            ),
        ] {
            actual.push((name, auto_commit_outcome(&result)));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// The automatic commit route finds the coordinator again and resends only
    /// when the response is retriable because of `7`, `15` or `16`, the codes
    /// where Kafka's `OffsetCommitResponseHandler` calls
    /// `markCoordinatorUnknown`. A final error or a rebalance error on another
    /// partition takes precedence.
    #[test]
    fn names_moved_coordinator_only_for_a_retriable_coordinator_response() {
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, errors, expected) in [
            ("success", &[0, 0][..], false),
            ("request timed out", &[0, 7][..], true),
            ("coordinator not available", &[15][..], true),
            ("not coordinator with a retriable code", &[3, 16][..], true),
            ("coordinator load in progress", &[14][..], false),
            ("unknown topic or partition", &[3][..], false),
            (
                "request timed out and metadata too large",
                &[7, 12][..],
                false,
            ),
            (
                "not coordinator and group authorization",
                &[30, 16][..],
                false,
            ),
            ("not coordinator and a rebalance", &[16, 27][..], false),
            (
                "not coordinator and topic authorization",
                &[29, 16][..],
                true,
            ),
        ] {
            actual.push((name, names_moved_coordinator(&response(errors))));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// `poll` does not wait for a commit that holds the commit lock. The
    /// interval commit stays due, and the next `poll` sends it.
    #[tokio::test(start_paused = true)]
    async fn busy_commit_lock_moves_the_interval_commit_to_the_next_poll() {
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_in_mock = Arc::clone(&requests);
        let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_offset_commit((2, 2)));
            }
            if api_key != offset_commit_request::API_KEY {
                return None;
            }
            requests_in_mock.fetch_add(1, Ordering::SeqCst);
            Some(encode_response(&response(&[0]), version))
        })
        .await;
        let mut consumer = commit_consumer(
            &mock,
            commit_identity(7, "member-a"),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(AtomicI32::new(7)),
        )
        .await;
        let interval = Duration::from_hours(1);
        consumer.auto_commit = Some(AutoCommit::new(interval));
        tokio::time::advance(interval).await;

        let busy = consumer.commit_serialization.lock().await;
        consumer.maybe_auto_commit_async().await;
        let while_busy = requests.load(Ordering::SeqCst);
        drop(busy);
        consumer.maybe_auto_commit_async().await;
        drop(consumer.commit_serialization.lock().await);
        let after = requests.load(Ordering::SeqCst);

        mock.stop();
        assert2::assert!((while_busy, after) == (0, 1));
    }

    /// After a retriable failure, Kafka's `autoCommitOffsetsAsync` makes the
    /// next auto commit due after `retry.backoff.ms`, and not after a full
    /// `auto.commit.interval.ms`.
    #[tokio::test(start_paused = true)]
    async fn retriable_auto_commit_failure_commits_again_after_the_retry_backoff() {
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_in_mock = Arc::clone(&requests);
        let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_offset_commit((2, 2)));
            }
            if api_key != offset_commit_request::API_KEY {
                return None;
            }
            let error = if requests_in_mock.fetch_add(1, Ordering::SeqCst) == 0 {
                7
            } else {
                0
            };
            Some(encode_response(&response(&[error]), version))
        })
        .await;
        let mut consumer = commit_consumer(
            &mock,
            commit_identity(7, "member-a"),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(AtomicI32::new(7)),
        )
        .await;
        let interval = Duration::from_hours(1);
        consumer.auto_commit = Some(AutoCommit::new(interval));
        let backoff = consumer.retry_policy.initial_backoff;
        // The paused clock jumps to the next timer while a response is on the
        // loopback socket. A short timer keeps each jump far below the request
        // timeout.
        let ticker = tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        let mut sent = Vec::new();
        for advance in [interval, Duration::ZERO, backoff, Duration::ZERO] {
            tokio::time::advance(advance).await;
            consumer.maybe_auto_commit_async().await;
            drop(consumer.commit_serialization.lock().await);
            sent.push(requests.load(Ordering::SeqCst));
        }

        ticker.abort();
        mock.stop();
        assert2::assert!(sent == vec![1, 1, 2, 2]);
    }

    #[tokio::test]
    async fn snapshot_commit_offsets_is_empty_without_offsets() {
        let identity = commit_identity(7, "member-a");
        let offsets = Arc::new(Mutex::new(HashMap::new()));
        let positions = Arc::new(Mutex::new(HashMap::new()));

        let snapshot = snapshot_commit_offsets(&identity, &offsets, &positions, None).await;

        assert2::assert!(snapshot == (HashMap::new(), (7, "member-a".into())));
    }

    #[tokio::test]
    async fn snapshot_commit_offsets_preserves_offsets_and_epochs() {
        let identity = Arc::new(Mutex::new(CommitIdentity {
            generation: 7,
            member_id: "member-a".into(),
            ownership_ids: HashMap::from([(("alpha".into(), 0), 1), (("alpha".into(), 1), 2)]),
            rejoin_on_poll: false,
        }));
        let offsets = Arc::new(Mutex::new(HashMap::from([
            (("alpha".to_string(), 0), 10),
            (("alpha".to_string(), 1), 20),
        ])));
        let positions = Arc::new(Mutex::new(HashMap::from([(
            ("alpha".to_string(), 1),
            PartitionPosition {
                offset_epoch: krabka_ids::LeaderEpoch(7),
                ..Default::default()
            },
        )])));
        let snapshot = snapshot_commit_offsets(&identity, &offsets, &positions, None).await;

        assert2::assert!(
            snapshot
                == (
                    HashMap::from([
                        (("alpha".to_string(), 0), OffsetAndMetadata::new(10)),
                        (
                            ("alpha".to_string(), 1),
                            OffsetAndMetadata {
                                offset: 20,
                                leader_epoch: Some(7),
                                metadata: String::new(),
                            }
                        ),
                    ]),
                    (7, "member-a".into())
                )
        );
    }

    #[test]
    fn build_commit_request_preserves_group_member_generation_and_topics() {
        let topics = vec![
            krabka_protocol::owned::offset_commit_request::OffsetCommitRequestTopic {
                name: "topic".into(),
                topic_id: Uuid::ZERO,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 3,
                    committed_offset: 99,
                    committed_leader_epoch: 5,
                    committed_metadata: Some(String::new()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ];

        let req = build_commit_request(
            "group-a".into(),
            42,
            "member-a".into(),
            Some("instance-a".into()),
            topics.clone(),
        );

        assert2::assert!(
            req == TopicNameOffsetCommit(OffsetCommitRequest {
                group_id: "group-a".into(),
                generation_id_or_member_epoch: 42,
                member_id: "member-a".into(),
                group_instance_id: Some("instance-a".into()),
                retention_time_ms: -1,
                topics,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            })
        );
    }

    /// `commit_response_outcome` gives a final error precedence over a
    /// rebalance-class error, a rebalance-class error over a retriable error,
    /// and a retriable error over `TOPIC_AUTHORIZATION_FAILED`, where Kafka's
    /// result depends on the partition order. Whether a rebalance-class error
    /// raises `CommitFailed` or `RebalanceInProgress` depends on the code, the
    /// generation and the coordinator task, exactly as Kafka's
    /// `commitOffsetsSync` never retries either and raises the matching
    /// exception straight through.
    #[test]
    fn commit_response_outcome_orders_errors_and_reads_the_consumer_state() {
        let running = CommitResponseContext {
            group_id: "group-a",
            group_instance_id: Some("instance-a"),
            coordinator_alive: true,
            generation_unchanged: true,
        };
        let stopped = CommitResponseContext {
            coordinator_alive: false,
            ..running
        };
        let rejoined = CommitResponseContext {
            generation_unchanged: false,
            ..running
        };
        let rejoined_and_stopped = CommitResponseContext {
            coordinator_alive: false,
            ..rejoined
        };
        let commit_failed = ConsumerError::CommitFailed.to_string();
        let rebalance_in_progress =
            ConsumerError::RebalanceInProgress("group-a".into()).to_string();
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, errors, context, expected) in [
            (
                "success",
                &[0, 0][..],
                running,
                Ok(CommitOutcome::Acked(HashSet::from([
                    ("topic".into(), 0),
                    ("topic".into(), 1),
                ]))),
            ),
            (
                "retriable code keeps the acknowledged partitions",
                &[0, 3][..],
                running,
                Ok(CommitOutcome::Retriable {
                    code: 3,
                    acknowledged: HashSet::from([("topic".into(), 0)]),
                    find_coordinator: false,
                }),
            ),
            (
                "a later coordinator code finds the coordinator",
                &[14, 7][..],
                running,
                Ok(CommitOutcome::Retriable {
                    code: 14,
                    acknowledged: HashSet::new(),
                    find_coordinator: true,
                }),
            ),
            (
                "a rebalance error takes precedence over a retriable code",
                &[3, 27][..],
                running,
                Err(rebalance_in_progress.clone()),
            ),
            (
                "final code takes precedence over a rebalance error",
                &[27, 42][..],
                running,
                Err(ConsumerError::Server(42).to_string()),
            ),
            (
                "group authorization takes precedence over a rebalance error",
                &[22, 30][..],
                running,
                Err("not authorized to access group: group-a".into()),
            ),
            (
                "retriable code takes precedence over topic authorization",
                &[29, 16][..],
                running,
                Ok(CommitOutcome::Retriable {
                    code: 16,
                    acknowledged: HashSet::new(),
                    find_coordinator: true,
                }),
            ),
            (
                "topic authorization after a success",
                &[0, 29][..],
                running,
                Err("not authorized to access topics: [topic]".into()),
            ),
            (
                "illegal generation while the group is still preparing a rebalance",
                &[22][..],
                running,
                Err(rebalance_in_progress.clone()),
            ),
            (
                "unknown member id while the group is still preparing a rebalance",
                &[25][..],
                running,
                Err(rebalance_in_progress.clone()),
            ),
            (
                "rebalance in progress",
                &[27][..],
                running,
                Err(rebalance_in_progress.clone()),
            ),
            (
                "illegal generation after a rejoin",
                &[22][..],
                rejoined,
                Err(commit_failed.clone()),
            ),
            (
                "unknown member id after a rejoin",
                &[25][..],
                rejoined,
                Err(commit_failed.clone()),
            ),
            (
                "rebalance in progress after a rejoin is still a rebalance error",
                &[27][..],
                rejoined,
                Err(rebalance_in_progress.clone()),
            ),
            (
                "unknown member id after the task stopped",
                &[25][..],
                stopped,
                Err(commit_failed.clone()),
            ),
            (
                "rebalance in progress after the task stopped",
                &[27][..],
                stopped,
                Err(commit_failed.clone()),
            ),
            (
                "fenced instance id with the same generation",
                &[82][..],
                running,
                Err(ConsumerError::FencedInstanceId("instance-a".into()).to_string()),
            ),
            (
                "fenced instance id after a rejoin",
                &[82][..],
                rejoined,
                Err(commit_failed.clone()),
            ),
            (
                "fenced instance id after a rejoin and a task stop",
                &[82][..],
                rejoined_and_stopped,
                Err(commit_failed.clone()),
            ),
        ] {
            actual.push((
                name,
                commit_response_outcome(&response(errors), context)
                    .map_err(|error| error.to_string()),
            ));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// The negotiated `OffsetCommit` version and the request that the
    /// coordinator decoded at that version.
    type SentOffsetCommits = Vec<(i16, OffsetCommitRequest)>;

    /// The result of `commit_offsets_sync`, with a version error as its
    /// ranges.
    type CommitResult = Result<(), (i16, i16, i16, i16, i16)>;

    /// Apache Kafka's classic `ConsumerCoordinator.sendOffsetCommitRequest`
    /// builds `OffsetCommit` with `OffsetCommitRequest.Builder.forTopicNames`,
    /// which caps the version at 9. The request names each topic and carries
    /// no topic id at every negotiated version.
    #[tokio::test]
    async fn commit_sends_offset_commit_by_topic_name_at_v9_or_lower() {
        let sent_request = |version| {
            vec![(
                version,
                OffsetCommitRequest {
                    group_id: "group-a".into(),
                    generation_id_or_member_epoch: 7,
                    member_id: "member-a".into(),
                    group_instance_id: None,
                    retention_time_ms: -1,
                    topics: vec![OffsetCommitRequestTopic {
                        name: "topic".into(),
                        topic_id: Uuid::ZERO,
                        partitions: vec![OffsetCommitRequestPartition {
                            partition_index: 0,
                            committed_offset: 12,
                            committed_leader_epoch: -1,
                            committed_metadata: Some(String::new()),
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            )]
        };
        for (name, offset_commit_range, expected_requests, expected_result) in [
            ("coordinator stops at v7", (2, 7), sent_request(7), Ok(())),
            ("coordinator stops at v9", (2, 9), sent_request(9), Ok(())),
            ("coordinator supports v10", (2, 10), sent_request(9), Ok(())),
            (
                "coordinator supports only v10",
                (10, 10),
                Vec::new(),
                Err((offset_commit_request::API_KEY, 10, 10, 2, 9)),
            ),
        ] {
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let requests_in_mock = Arc::clone(&requests);
            let mock = MockBroker::start(move |api_key, version, _corr_id, mut body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_for_offset_commit(offset_commit_range));
                }
                if api_key != offset_commit_request::API_KEY {
                    return None;
                }
                let client_id_len = bytes::Buf::get_i16(&mut body);
                bytes::Buf::advance(
                    &mut body,
                    usize::try_from(client_id_len).expect("client id length"),
                );
                if version >= offset_commit_request::FLEXIBLE_MIN {
                    bytes::Buf::advance(&mut body, 1);
                }
                let request = krabka_protocol::Decode::decode(&mut body, version)
                    .expect("offset commit request decodes");
                requests_in_mock
                    .lock()
                    .expect("requests lock")
                    .push((version, request));
                let mut body = Vec::new();
                if version >= offset_commit_request::FLEXIBLE_MIN {
                    // The flexible response header carries empty tagged fields.
                    body.push(0);
                }
                body.extend(encode_response(&response(&[0]), version));
                Some(body)
            })
            .await;
            let consumer = commit_consumer(
                &mock,
                commit_identity(7, "member-a"),
                Arc::new(tokio::sync::Notify::new()),
                Arc::new(AtomicI32::new(7)),
            )
            .await;

            let result: CommitResult = consumer
                .commit_offsets_sync(HashMap::from([(
                    ("topic".into(), 0),
                    OffsetAndMetadata::new(12),
                )]))
                .await
                .map_err(|error| match error {
                    ConsumerError::Client(
                        krabka_client_core::ClientError::IncompatibleVersion {
                            api_key,
                            broker_min,
                            broker_max,
                            client_min,
                            client_max,
                        },
                    ) => (api_key, broker_min, broker_max, client_min, client_max),
                    other => panic!("case {name}: unexpected error {other:?}"),
                });

            mock.stop();
            let requests: SentOffsetCommits = requests.lock().expect("requests lock").clone();
            check!(
                (requests, result) == (expected_requests, expected_result),
                "case {name}"
            );
        }
    }

    /// One scripted answer of the mock coordinator to an `OffsetCommit`.
    #[derive(Clone, Copy, Debug)]
    enum CommitAnswer {
        /// Answer each partition of a topic with the error code of that topic,
        /// and `0` for a topic that is not in the list.
        Codes(&'static [(&'static str, i16)]),
        /// Close the connection without a response.
        Close,
        /// Send no response, so the request times out.
        Silent,
    }

    /// The result of a synchronous commit, the `OffsetCommit` requests and the
    /// `FindCoordinator` requests that the coordinator received.
    type CommitCodeResult = (Result<(), String>, usize, usize);

    /// `commit_sync` handles each `OffsetCommit` error code as Kafka's
    /// `ConsumerCoordinator.OffsetCommitResponseHandler.handle` and
    /// `commitOffsetsSync` do. Retriable codes and failed requests retry until
    /// the timeout. `7`, `15`, `16` and a failed request find the coordinator
    /// again first. The rebalance codes (`22`, `25`, `27`, `82` after a rejoin)
    /// are not retried: `commit_sync` raises `RebalanceInProgress` or
    /// `CommitFailed` straight through, with no offsets committed, exactly as
    /// Kafka's `commitOffsetsSync` does.
    #[tokio::test]
    async fn commit_sync_handles_each_offset_commit_error_code_as_kafka_does() {
        use CommitAnswer::{Close, Codes, Silent};

        const RETRY: CoordinatorRetryPolicy = CoordinatorRetryPolicy {
            timeout: Duration::from_secs(2),
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
        };
        const EXPIRED: CoordinatorRetryPolicy = CoordinatorRetryPolicy {
            timeout: Duration::ZERO,
            ..RETRY
        };
        /// The backoff is longer than the time that is left.
        const LONG_BACKOFF: CoordinatorRetryPolicy = CoordinatorRetryPolicy {
            timeout: Duration::from_millis(100),
            initial_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(5),
        };
        let fenced = ConsumerError::FencedInstanceId("instance-a".into()).to_string();
        let commit_failed = ConsumerError::CommitFailed.to_string();
        let rebalance_in_progress =
            ConsumerError::RebalanceInProgress("group-a".into()).to_string();
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, answers, rejoin, retry_policy, expected) in [
            ("success", vec![Codes(&[])], false, RETRY, (Ok(()), 1, 0)),
            (
                "unknown topic or partition retries",
                vec![Codes(&[("orders", 3)]), Codes(&[])],
                false,
                RETRY,
                (Ok(()), 2, 0),
            ),
            (
                "coordinator load in progress retries",
                vec![Codes(&[("orders", 14)]), Codes(&[])],
                false,
                RETRY,
                (Ok(()), 2, 0),
            ),
            (
                "request timed out finds the coordinator and retries",
                vec![Codes(&[("orders", 7)]), Codes(&[])],
                false,
                RETRY,
                (Ok(()), 2, 1),
            ),
            (
                "coordinator not available finds the coordinator and retries",
                vec![Codes(&[("orders", 15)]), Codes(&[])],
                false,
                RETRY,
                (Ok(()), 2, 1),
            ),
            (
                "not coordinator finds the coordinator and retries",
                vec![Codes(&[("orders", 16)]), Codes(&[])],
                false,
                RETRY,
                (Ok(()), 2, 1),
            ),
            (
                "unknown topic or partition past the timeout",
                vec![Codes(&[("orders", 3)])],
                false,
                EXPIRED,
                (Err(ConsumerError::Server(3).to_string()), 1, 0),
            ),
            (
                "not coordinator with a backoff past the timeout",
                vec![Codes(&[("orders", 16)]), Codes(&[])],
                false,
                LONG_BACKOFF,
                (Err(ConsumerError::Server(16).to_string()), 1, 1),
            ),
            (
                "disconnect finds the coordinator and retries",
                vec![Close, Codes(&[])],
                false,
                RETRY,
                (Ok(()), 2, 1),
            ),
            (
                "request timeout finds the coordinator and retries",
                vec![Silent, Codes(&[])],
                false,
                RETRY,
                (Ok(()), 2, 1),
            ),
            (
                "disconnect past the timeout",
                vec![Close],
                false,
                EXPIRED,
                (Err(ConsumerError::CoordinatorUnavailable.to_string()), 1, 0),
            ),
            (
                "group authorization failed",
                vec![Codes(&[("orders", 30)])],
                false,
                RETRY,
                (Err("not authorized to access group: group-a".into()), 1, 0),
            ),
            (
                "topic authorization failed collects every topic",
                vec![Codes(&[("orders", 29), ("payments", 29)])],
                false,
                RETRY,
                (
                    Err("not authorized to access topics: [orders, payments]".into()),
                    1,
                    0,
                ),
            ),
            (
                "topic authorization failed with a retriable code retries",
                vec![Codes(&[("orders", 29), ("payments", 3)]), Codes(&[])],
                false,
                RETRY,
                (Ok(()), 2, 0),
            ),
            (
                "offset metadata too large",
                vec![Codes(&[("orders", 12)])],
                false,
                RETRY,
                (Err(ConsumerError::Server(12).to_string()), 1, 0),
            ),
            (
                "invalid commit offset size",
                vec![Codes(&[("orders", 28)])],
                false,
                RETRY,
                (Err(ConsumerError::Server(28).to_string()), 1, 0),
            ),
            (
                "unknown topic id is unexpected for a request by topic name",
                vec![Codes(&[("orders", 100)])],
                false,
                RETRY,
                (Err(ConsumerError::Server(100).to_string()), 1, 0),
            ),
            (
                "not leader or follower is unexpected",
                vec![Codes(&[("orders", 6)])],
                false,
                RETRY,
                (Err(ConsumerError::Server(6).to_string()), 1, 0),
            ),
            (
                "fenced instance id with the same generation",
                vec![Codes(&[("orders", 82)])],
                false,
                RETRY,
                (Err(fenced.clone()), 1, 0),
            ),
            (
                "fenced instance id fails the commit after a rejoin",
                vec![Codes(&[("orders", 82)]), Codes(&[])],
                true,
                RETRY,
                (Err(commit_failed.clone()), 1, 0),
            ),
            (
                "illegal generation while the group is still preparing a rebalance",
                vec![Codes(&[("orders", 22)])],
                false,
                RETRY,
                (Err(rebalance_in_progress.clone()), 1, 0),
            ),
            (
                "illegal generation fails the commit after a rejoin",
                vec![Codes(&[("orders", 22)]), Codes(&[])],
                true,
                RETRY,
                (Err(commit_failed.clone()), 1, 0),
            ),
            (
                "unknown member id while the group is still preparing a rebalance",
                vec![Codes(&[("orders", 25)])],
                false,
                RETRY,
                (Err(rebalance_in_progress.clone()), 1, 0),
            ),
            (
                "unknown member id fails the commit after a rejoin",
                vec![Codes(&[("orders", 25)]), Codes(&[])],
                true,
                RETRY,
                (Err(commit_failed.clone()), 1, 0),
            ),
            (
                "rebalance in progress fails the commit",
                vec![Codes(&[("orders", 27)])],
                false,
                RETRY,
                (Err(rebalance_in_progress.clone()), 1, 0),
            ),
            (
                "rebalance in progress fails the commit even after a rejoin",
                vec![Codes(&[("orders", 27)]), Codes(&[])],
                true,
                RETRY,
                (Err(rebalance_in_progress.clone()), 1, 0),
            ),
        ] {
            actual.push((
                name,
                scripted_commit_sync(answers, rejoin, retry_policy).await,
            ));
            wanted.push((name, expected));
        }
        let wanted: Vec<(&str, CommitCodeResult)> = wanted;
        assert2::assert!(actual == wanted);
    }

    /// Run `commit_sync` against a mock coordinator that gives `answers` to
    /// the `OffsetCommit` requests in order and repeats the last answer. With
    /// `rejoin`, the coordinator task joins the group again with generation 8
    /// before the first answer.
    async fn scripted_commit_sync(
        answers: Vec<CommitAnswer>,
        rejoin: bool,
        retry_policy: CoordinatorRetryPolicy,
    ) -> CommitCodeResult {
        use CommitAnswer::{Close, Codes, Silent};
        use krabka_protocol::owned::{
            find_coordinator_response::FindCoordinatorResponse, metadata_response::MetadataResponse,
        };

        let identity = Arc::new(Mutex::new(CommitIdentity {
            generation: 7,
            member_id: "member-a".into(),
            ownership_ids: HashMap::from([(("orders".into(), 0), 1), (("payments".into(), 0), 2)]),
            rejoin_on_poll: false,
        }));
        let assignment_changed = Arc::new(tokio::sync::Notify::new());
        let offset_commits = Arc::new(AtomicUsize::new(0));
        let find_coordinators = Arc::new(AtomicUsize::new(0));
        let identity_in_mock = Arc::clone(&identity);
        let changed_in_mock = Arc::clone(&assignment_changed);
        let offset_commits_in_mock = Arc::clone(&offset_commits);
        let find_coordinators_in_mock = Arc::clone(&find_coordinators);
        let mock = MockBroker::start_with_replies(move |api_key, version, _corr_id, mut body| {
            let mut buffer = bytes::BytesMut::new();
            match api_key {
                api_versions_request::API_KEY => {
                    MockReply::Respond(api_versions_for_offset_commit((7, 7)))
                }
                find_coordinator_request::API_KEY => {
                    find_coordinators_in_mock.fetch_add(1, Ordering::SeqCst);
                    FindCoordinatorResponse::default()
                        .encode(&mut buffer, version)
                        .unwrap();
                    MockReply::Respond(buffer.to_vec())
                }
                metadata_request::API_KEY => {
                    MetadataResponse::default()
                        .encode(&mut buffer, version)
                        .unwrap();
                    MockReply::Respond(buffer.to_vec())
                }
                offset_commit_request::API_KEY => {
                    let attempt = offset_commits_in_mock.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 && rejoin {
                        identity_in_mock.try_lock().unwrap().generation = 8;
                        changed_in_mock.notify_waiters();
                    }
                    let client_id_len = bytes::Buf::get_i16(&mut body);
                    bytes::Buf::advance(&mut body, usize::try_from(client_id_len).unwrap());
                    let request: OffsetCommitRequest =
                        krabka_protocol::Decode::decode(&mut body, version).unwrap();
                    let codes = match answers[attempt.min(answers.len() - 1)] {
                        Codes(codes) => codes,
                        Close => return MockReply::Close,
                        Silent => return MockReply::Silent,
                    };
                    let code_of = |topic: &str| {
                        codes
                            .iter()
                            .find(|(name, _)| *name == topic)
                            .map_or(0, |(_, code)| *code)
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
                                    .map(|partition| OffsetCommitResponsePartition {
                                        partition_index: partition.partition_index,
                                        error_code: code_of(&topic.name),
                                        ..Default::default()
                                    })
                                    .collect(),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    };
                    MockReply::Respond(encode_response(&response, version))
                }
                _ => MockReply::Silent,
            }
        })
        .await;
        let mut consumer = commit_consumer(
            &mock,
            Arc::clone(&identity),
            assignment_changed,
            Arc::new(AtomicI32::new(7)),
        )
        .await;
        consumer.client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(krabka_units::millis(300))
            .build()
            .await
            .unwrap();
        consumer.group_instance_id = Some("instance-a".into());
        consumer.retry_policy = retry_policy;

        let result = tokio::time::timeout(Duration::from_secs(10), consumer.commit_sync())
            .await
            .map_err(|_| "commit never finished".to_string())
            .and_then(|result| result.map_err(|error| error.to_string()));

        mock.stop();
        (
            result,
            offset_commits.load(Ordering::SeqCst),
            find_coordinators.load(Ordering::SeqCst),
        )
    }

    /// A commit fails without a request after the coordinator task stopped, and
    /// after the coordinator fenced the static member until the next `poll`
    /// joins the group again. Kafka's
    /// `ConsumerCoordinator.sendOffsetCommitRequest` raises
    /// `CommitFailedException` when the member is not part of an active group.
    #[tokio::test]
    async fn commit_fails_without_a_request_after_the_coordinator_task_stops() {
        #[derive(Clone, Copy)]
        enum Commit {
            All,
            Selected,
        }
        #[derive(Clone, Copy)]
        enum TaskState {
            Running,
            /// The task runs and waits for the next `poll` after a fence.
            Fenced,
            Stopping,
            Stopped,
        }
        let commit_failed = "offset commit failed: the consumer is not part of an active group; it is likely that the consumer was kicked out of the group";
        for (name, commit, task_state, owned, expected) in [
            (
                "commit sync while the task runs",
                Commit::All,
                TaskState::Running,
                true,
                (Ok(()), 1),
            ),
            (
                "commit sync after close cancelled the task but before it ends",
                Commit::All,
                TaskState::Stopping,
                true,
                (Err(commit_failed.to_string()), 0),
            ),
            (
                "commit sync while the task waits for a poll after a fence",
                Commit::All,
                TaskState::Fenced,
                false,
                (Err(commit_failed.to_string()), 0),
            ),
            (
                "selected commit while the task waits for a poll after a fence",
                Commit::Selected,
                TaskState::Fenced,
                false,
                (Err(commit_failed.to_string()), 0),
            ),
            (
                "commit sync after the task stops",
                Commit::All,
                TaskState::Stopped,
                true,
                (Err(commit_failed.to_string()), 0),
            ),
            (
                "commit sync after a fence cleared the assignment",
                Commit::All,
                TaskState::Stopped,
                false,
                (Err(commit_failed.to_string()), 0),
            ),
            (
                "selected commit after the task stops",
                Commit::Selected,
                TaskState::Stopped,
                true,
                (Err(commit_failed.to_string()), 0),
            ),
        ] {
            let requests = Arc::new(AtomicUsize::new(0));
            let requests_in_mock = Arc::clone(&requests);
            let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_for_offset_commit((2, 2)));
                }
                if api_key != offset_commit_request::API_KEY {
                    return None;
                }
                requests_in_mock.fetch_add(1, Ordering::SeqCst);
                Some(encode_response(&response(&[0]), version))
            })
            .await;
            let identity = commit_identity(7, "member-a");
            if !owned {
                identity.lock().await.ownership_ids.clear();
            }
            if matches!(task_state, TaskState::Fenced) {
                identity.lock().await.rejoin_on_poll = true;
            }
            let mut consumer = commit_consumer(
                &mock,
                identity,
                Arc::new(tokio::sync::Notify::new()),
                Arc::new(AtomicI32::new(7)),
            )
            .await;
            let task = match task_state {
                TaskState::Running | TaskState::Fenced => {
                    tokio::spawn(std::future::pending::<()>())
                }
                TaskState::Stopping => {
                    consumer.coordinator_shutdown.cancel();
                    tokio::spawn(std::future::pending::<()>())
                }
                TaskState::Stopped => {
                    let task = tokio::spawn(async {});
                    while !task.is_finished() {
                        tokio::task::yield_now().await;
                    }
                    task
                }
            };
            consumer.coordinator_handle = Some(task);

            let commit_future = async {
                match commit {
                    Commit::All => consumer.commit_sync().await,
                    Commit::Selected => {
                        consumer
                            .commit_offsets_sync(HashMap::from([(
                                ("topic".into(), 0),
                                OffsetAndMetadata::new(12),
                            )]))
                            .await
                    }
                }
            };
            let result = tokio::time::timeout(Duration::from_secs(5), commit_future)
                .await
                .unwrap_or_else(|_| panic!("case {name}: commit never finished"))
                .map_err(|error| error.to_string());

            if let Some(task) = consumer.coordinator_handle.take() {
                task.abort();
            }
            mock.stop();
            check!(
                (result, requests.load(Ordering::SeqCst)) == expected,
                "case {name}"
            );
        }
    }

    /// Issue #116: a rebalance-class code does not wait for the coordinator
    /// task to rejoin and resend. `commit_offsets_sync` raises the error from
    /// the one response it got, even though a rejoin happened concurrently.
    #[tokio::test]
    async fn selected_commit_fails_immediately_on_rebalance_in_progress_without_retrying() {
        let identity = commit_identity(7, "member-a");
        let changed = Arc::new(tokio::sync::Notify::new());
        let generation = Arc::new(AtomicI32::new(7));
        let requests = Arc::new(AtomicUsize::new(0));
        let (consumer, mock, _) = selected_commit_consumer(
            identity,
            changed,
            generation,
            Arc::clone(&requests),
            false,
            |identity, generation, _request_generation, _request_member_id| {
                // The coordinator task rejoins concurrently with the response.
                identity.try_lock().unwrap().generation = 8;
                generation.store(8, Ordering::Relaxed);
            },
        )
        .await;

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            consumer.commit_offsets_sync(HashMap::from([(
                ("topic".into(), 0),
                OffsetAndMetadata::new(12),
            )])),
        )
        .await
        .expect("commit does not wait for a rejoin");

        mock.stop();
        assert2::assert!(
            result.map_err(|error| error.to_string())
                == Err(ConsumerError::RebalanceInProgress("group-a".into()).to_string())
        );
        assert2::assert!(requests.load(Ordering::SeqCst) == 1);
    }

    /// The request that fails with a rebalance code still carries the
    /// generation and the member id that `commit_offsets_sync` snapshotted at
    /// its call, not the ones a concurrent rejoin published afterward.
    #[tokio::test]
    async fn selected_commit_uses_snapshot_generation_if_ownership_changes_before_rpc() {
        let identity = commit_identity(7, "member-a");
        let changed = Arc::new(tokio::sync::Notify::new());
        let generation = Arc::new(AtomicI32::new(7));
        let requests = Arc::new(AtomicUsize::new(0));
        let seen_generation = Arc::new(AtomicI32::new(-1));
        let seen_in_mock = Arc::clone(&seen_generation);
        let (consumer, mock, _) = selected_commit_consumer(
            Arc::clone(&identity),
            changed,
            Arc::clone(&generation),
            requests,
            false,
            move |_ownership, _generation, request_generation, _request_member_id| {
                seen_in_mock.store(request_generation, Ordering::Relaxed);
            },
        )
        .await;
        let mut changed_identity = identity.lock().await;
        changed_identity
            .ownership_ids
            .insert(("topic".into(), 0), 2);
        changed_identity.generation = 8;
        drop(changed_identity);
        generation.store(8, Ordering::Relaxed);
        let topics = build_commit_topics(HashMap::from([(
            ("topic".into(), 0),
            OffsetAndMetadata::new(12),
        )]));

        let error = consumer
            .commit_topics_once(topics, (7, "member-a".into()))
            .await
            .expect_err("rebalance in progress fails the commit");

        mock.stop();
        assert2::assert!(
            error.to_string() == ConsumerError::RebalanceInProgress("group-a".into()).to_string()
        );
        assert2::assert!(seen_generation.load(Ordering::Relaxed) == 7);
    }

    /// The one request a selected commit sends carries the live member id from
    /// `commit_identity`, even though it is never retried.
    #[tokio::test]
    async fn selected_commit_uses_live_member_id_even_without_a_retry() {
        let identity = commit_identity(8, "member-new");
        let changed = Arc::new(tokio::sync::Notify::new());
        let generation = Arc::new(AtomicI32::new(7));
        let requests = Arc::new(AtomicUsize::new(0));
        let seen_new_member = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen_in_mock = Arc::clone(&seen_new_member);
        let (consumer, mock, _) = selected_commit_consumer(
            identity,
            changed,
            generation,
            requests,
            false,
            move |_identity, generation, _request_generation, request_member_id| {
                seen_in_mock.store(request_member_id == "member-new", Ordering::Relaxed);
                generation.store(8, Ordering::Release);
            },
        )
        .await;
        let result = consumer
            .commit_offsets_sync(HashMap::from([(
                ("topic".into(), 0),
                OffsetAndMetadata::new(12),
            )]))
            .await;

        mock.stop();
        assert2::assert!(result.is_err());
        assert2::assert!(seen_new_member.load(Ordering::Relaxed));
    }

    /// A rebalance code on any one partition of a mixed response fails the
    /// whole commit, even though another partition of the same response
    /// acknowledged.
    #[tokio::test]
    async fn commit_sync_fails_on_any_rebalance_code_in_a_mixed_response() {
        let identity = Arc::new(Mutex::new(CommitIdentity {
            generation: 7,
            member_id: "member-a".into(),
            ownership_ids: HashMap::from([(("topic".into(), 0), 1), (("topic".into(), 1), 2)]),
            rejoin_on_poll: false,
        }));
        let changed = Arc::new(tokio::sync::Notify::new());
        let generation = Arc::new(AtomicI32::new(7));
        let requests = Arc::new(AtomicUsize::new(0));
        let (consumer, mock, _) = selected_commit_consumer(
            identity,
            changed,
            generation,
            Arc::clone(&requests),
            true,
            |identity, generation, _request_generation, _request_member_id| {
                identity.try_lock().unwrap().generation = 8;
                generation.store(8, Ordering::Relaxed);
            },
        )
        .await;

        let result = consumer.commit_sync().await;

        mock.stop();
        assert2::assert!(
            result.map_err(|error| error.to_string())
                == Err(ConsumerError::RebalanceInProgress("group-a".into()).to_string())
        );
        assert2::assert!(requests.load(Ordering::SeqCst) == 1);
    }

    /// A commit that fails with a rebalance error still releases
    /// `commit_serialization` so a newer commit for the same partition sends
    /// only after it, never before.
    #[tokio::test]
    async fn a_failed_commit_releases_the_lock_before_a_newer_commit_for_the_same_partition() {
        let identity = commit_identity(7, "member-a");
        let changed = Arc::new(tokio::sync::Notify::new());
        let generation = Arc::new(AtomicI32::new(7));
        let requests = Arc::new(AtomicUsize::new(0));
        let (consumer, mock, seen_offsets) = selected_commit_consumer(
            identity,
            changed,
            generation,
            requests,
            false,
            |identity, generation, _request_generation, _request_member_id| {
                identity.try_lock().unwrap().generation = 8;
                generation.store(8, Ordering::Relaxed);
            },
        )
        .await;
        let consumer = Arc::new(consumer);
        let blocker = Arc::clone(&consumer.commit_serialization)
            .lock_owned()
            .await;
        let older = {
            let consumer = Arc::clone(&consumer);
            tokio::spawn(async move {
                consumer
                    .commit_offsets_sync(HashMap::from([(
                        ("topic".into(), 0),
                        OffsetAndMetadata::new(10),
                    )]))
                    .await
            })
        };
        tokio::task::yield_now().await;
        let newer = {
            let consumer = Arc::clone(&consumer);
            tokio::spawn(async move {
                consumer
                    .commit_offsets_sync(HashMap::from([(
                        ("topic".into(), 0),
                        OffsetAndMetadata::new(12),
                    )]))
                    .await
            })
        };
        tokio::task::yield_now().await;
        drop(blocker);

        let older_error = older
            .await
            .unwrap()
            .expect_err("the response for the older commit is a rebalance error");
        newer
            .await
            .unwrap()
            .expect("the newer commit sends once the older one released the lock");

        mock.stop();
        assert2::assert!(
            older_error.to_string()
                == ConsumerError::RebalanceInProgress("group-a".into()).to_string()
        );
        assert2::assert!(*seen_offsets.lock().unwrap() == vec![(0, 10), (0, 12)]);
    }

    #[tokio::test]
    async fn async_commits_queued_behind_an_rpc_are_coalesced() {
        let identity = commit_identity(7, "member-a");
        let changed = Arc::new(tokio::sync::Notify::new());
        let generation = Arc::new(AtomicI32::new(7));
        let requests = Arc::new(AtomicUsize::new(0));
        let (consumer, mock, _) = selected_commit_consumer(
            identity,
            changed,
            generation,
            Arc::clone(&requests),
            false,
            |_identity, _generation, _request_generation, _request_member_id| {},
        )
        .await;
        let blocker = consumer.commit_serialization.lock().await;

        for _ in 0..100 {
            consumer.commit_async().expect("commit_async");
        }
        tokio::task::yield_now().await;
        assert2::assert!(consumer.commit_async_state.load(Ordering::Acquire) == ASYNC_COMMIT_DIRTY);
        drop(blocker);

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while consumer.commit_async_state.load(Ordering::Acquire) != ASYNC_COMMIT_IDLE {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("coalesced async commit completes");
        mock.stop();

        assert2::assert!(requests.load(Ordering::SeqCst) == 1);
    }

    #[test]
    fn selected_commit_topics_exclude_unrequested_assignment_positions() {
        let topics = build_commit_topics(HashMap::from([(
            ("topic".into(), 0),
            OffsetAndMetadata::new(12),
        )]));

        assert2::assert!(topics.len() == 1);
        assert2::assert!(topics[0].partitions.len() == 1);
        assert2::assert!(topics[0].partitions[0].partition_index == 0);
    }

    /// The `OffsetCommit` requests that [`recording_coordinator`] received.
    type SentCommits = Arc<std::sync::Mutex<Vec<OffsetCommitRequest>>>;

    /// A coordinator that records each `OffsetCommit` v7 request and answers
    /// each partition with `error_code`.
    async fn recording_coordinator(error_code: i16) -> (MockBroker, SentCommits) {
        use bytes::Buf as _;
        use krabka_protocol::Decode as _;

        let sent = SentCommits::default();
        let sent_in_mock = Arc::clone(&sent);
        let mock = MockBroker::start(move |api_key, version, _corr_id, mut body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_offset_commit((7, 7)));
            }
            if api_key != offset_commit_request::API_KEY {
                return None;
            }
            let client_id_len = usize::try_from(body.get_i16()).unwrap();
            body.advance(client_id_len);
            let request = OffsetCommitRequest::decode(&mut body, version).unwrap();
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
                                error_code,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            sent_in_mock.lock().unwrap().push(request);
            Some(encode_response(&response, version))
        })
        .await;
        (mock, sent)
    }

    fn sent_partitions(sent: &SentCommits) -> Vec<OffsetCommitRequestPartition> {
        sent.lock()
            .unwrap()
            .iter()
            .flat_map(|request| request.topics.iter())
            .flat_map(|topic| topic.partitions.iter().cloned())
            .collect()
    }

    /// Kafka's `commitSync(offsets)` sends each offset with its leader epoch
    /// and metadata, also an offset past the consumed position. Kafka's
    /// `commitSync()` sends the position with its leader epoch.
    #[tokio::test]
    async fn synchronous_commits_send_kafkas_offsets_epochs_and_metadata() {
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, selected, expected) in [
            (
                "selected offset past the position",
                Some(OffsetAndMetadata {
                    offset: 100,
                    leader_epoch: Some(4),
                    metadata: "note".into(),
                }),
                (100, 4, "note"),
            ),
            (
                "selected offset without epoch",
                Some(OffsetAndMetadata::new(3)),
                (3, -1, ""),
            ),
            ("positions", None, (12, 9, "")),
        ] {
            let (mock, sent) = recording_coordinator(0).await;
            let consumer = commit_consumer(
                &mock,
                commit_identity(7, "member-a"),
                Arc::new(tokio::sync::Notify::new()),
                Arc::new(AtomicI32::new(7)),
            )
            .await;
            consumer.positions.lock().await.insert(
                ("topic".into(), 0),
                PartitionPosition {
                    offset_epoch: krabka_ids::LeaderEpoch(9),
                    ..Default::default()
                },
            );
            let result = match selected {
                Some(offset) => {
                    consumer
                        .commit_offsets_sync(HashMap::from([(("topic".into(), 0), offset)]))
                        .await
                }
                None => consumer.commit_sync().await,
            };
            mock.stop();
            actual.push((
                name,
                result.map_err(|error| error.to_string()),
                sent_partitions(&sent),
            ));
            wanted.push((
                name,
                Ok(()),
                vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: expected.0,
                    committed_leader_epoch: expected.1,
                    committed_metadata: Some(expected.2.into()),
                    ..Default::default()
                }],
            ));
        }
        assert2::assert!(actual == wanted);
    }

    /// A failed coordinator lookup of an asynchronous commit comes to the
    /// callback as `RetriableCommitFailedException` (Kafka's
    /// `ConsumerCoordinator.commitOffsetsAsync`). Other errors stay as they
    /// are.
    #[test]
    fn asynchronous_commit_wraps_a_failed_coordinator_lookup() {
        let actual = [
            ConsumerError::Server(15),
            ConsumerError::Server(14),
            ConsumerError::CoordinatorUnavailable,
            ConsumerError::Server(30),
            ConsumerError::CommitFailed,
        ]
        .map(|error| {
            async_commit_result(Err(error), "group-a", None).map_err(|error| error.to_string())
        });
        let retriable = |cause: &str| {
            Err(format!(
                "offset commit failed with a retriable exception: {cause}"
            ))
        };
        assert2::assert!(
            actual
                == [
                    retriable("broker error_code 15"),
                    retriable("broker error_code 14"),
                    retriable("coordinator unavailable"),
                    Err("broker error_code 30".to_owned()),
                    Err("offset commit failed: the consumer is not part of an active group; it is likely that the consumer was kicked out of the group".to_owned()),
                ]
        );
    }

    /// Kafka's `commitAsync(callback)` calls the callback with the offsets and
    /// the result. A retriable error comes as `RetriableCommitFailedException`.
    #[tokio::test]
    async fn asynchronous_commit_calls_the_callback_with_offsets_and_result() {
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, error_code, expected) in [
            ("success", 0, Ok(())),
            (
                "coordinator loading",
                14,
                Err("offset commit failed with a retriable exception: broker error_code 14"),
            ),
            (
                "group authorization",
                30,
                Err("not authorized to access group: group-a"),
            ),
            ("metadata too large", 12, Err("broker error_code 12")),
        ] {
            let (mock, _sent) = recording_coordinator(error_code).await;
            let consumer = commit_consumer(
                &mock,
                commit_identity(7, "member-a"),
                Arc::new(tokio::sync::Notify::new()),
                Arc::new(AtomicI32::new(7)),
            )
            .await;
            let (tx, rx) = tokio::sync::oneshot::channel();
            consumer
                .commit_async_with_callback(move |offsets, result| {
                    let _ = tx.send((offsets.clone(), result.map_err(ToString::to_string)));
                })
                .expect("commit_async_with_callback");
            let completed = tokio::time::timeout(Duration::from_secs(5), rx)
                .await
                .expect("callback runs")
                .expect("callback sends");
            mock.stop();
            actual.push((name, completed));
            wanted.push((
                name,
                (
                    HashMap::from([(("topic".to_string(), 0), OffsetAndMetadata::new(12))]),
                    expected.map_err(str::to_owned),
                ),
            ));
        }
        assert2::assert!(actual == wanted);
    }

    /// Kafka completes an asynchronous commit of no offsets locally with
    /// success, and calls the callback.
    #[tokio::test]
    async fn asynchronous_commit_without_positions_calls_the_callback() {
        let (mock, sent) = recording_coordinator(0).await;
        let consumer = commit_consumer(
            &mock,
            commit_identity(7, "member-a"),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(AtomicI32::new(7)),
        )
        .await;
        consumer.next_offsets.lock().await.clear();
        let (tx, rx) = tokio::sync::oneshot::channel();
        consumer
            .commit_async_with_callback(move |offsets, result| {
                let _ = tx.send((offsets.clone(), result.map_err(ToString::to_string)));
            })
            .expect("commit_async_with_callback");
        let completed = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("callback runs")
            .expect("callback sends");
        mock.stop();
        assert2::assert!((completed, sent_partitions(&sent)) == ((HashMap::new(), Ok(())), vec![]));
    }

    /// Kafka's `onJoinPrepare` commits the fetch positions. An explicit offset
    /// reaches the commit before a `JoinGroup` only after the coordinator
    /// acknowledged it, so a rejected offset past the position is never
    /// committed by the auto commit.
    #[tokio::test]
    async fn explicit_offsets_reach_the_pre_join_commit_only_after_an_ack() {
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, error_code, expected) in [
            (
                "acknowledged",
                0,
                HashMap::from([(("topic".to_string(), 0), (100, 4))]),
            ),
            ("metadata too large", 12, HashMap::new()),
        ] {
            let (mock, _sent) = recording_coordinator(error_code).await;
            let mut consumer = commit_consumer(
                &mock,
                commit_identity(7, "member-a"),
                Arc::new(tokio::sync::Notify::new()),
                Arc::new(AtomicI32::new(7)),
            )
            .await;
            let auto_commit = AutoCommit::new(Duration::from_secs(5));
            consumer.auto_commit = Some(auto_commit.clone());
            let _ = consumer
                .commit_offsets_sync(HashMap::from([(
                    ("topic".into(), 0),
                    OffsetAndMetadata {
                        offset: 100,
                        leader_epoch: Some(4),
                        metadata: "note".into(),
                    },
                )]))
                .await;
            mock.stop();
            let ownership = consumer.commit_identity.lock().await.ownership_ids.clone();
            actual.push((name, auto_commit.polled_offsets(&ownership).await));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// A panic in an application callback does not stop later asynchronous
    /// commits.
    #[tokio::test]
    async fn asynchronous_commits_continue_after_a_callback_panics() {
        let (mock, _sent) = recording_coordinator(0).await;
        let consumer = commit_consumer(
            &mock,
            commit_identity(7, "member-a"),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(AtomicI32::new(7)),
        )
        .await;
        consumer
            .commit_async_with_callback(|_, _| panic!("callback panics"))
            .expect("commit_async_with_callback");
        tokio::time::timeout(Duration::from_secs(5), async {
            while consumer.commit_async_state.load(Ordering::Acquire) != ASYNC_COMMIT_IDLE {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the worker becomes idle");
        let (tx, rx) = tokio::sync::oneshot::channel();
        consumer
            .commit_async_with_callback(move |_, result| {
                let _ = tx.send(result.map_err(ToString::to_string));
            })
            .expect("commit_async_with_callback");
        let second = tokio::time::timeout(Duration::from_secs(5), rx).await;
        mock.stop();
        assert2::assert!(let Ok(Ok(Ok(()))) = second);
    }

    /// A consumer with a manual assignment has no coordinator task that joins
    /// the group again, so a rebalance error fails its commit with
    /// `CommitFailed`, as Kafka's `commitOffsetsSync` throws
    /// `CommitFailedException`.
    #[tokio::test]
    async fn a_rebalance_error_fails_the_commit_of_a_manual_assignment() {
        let (mock, _sent) = recording_coordinator(ILLEGAL_GENERATION).await;
        let mut consumer = commit_consumer(
            &mock,
            commit_identity(-1, ""),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(AtomicI32::new(-1)),
        )
        .await;
        consumer.subscription = crate::subscription::shared(Vec::new(), None, true);
        consumer
            .subscription
            .send_modify(|subscription| subscription.manual_assignment = true);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            consumer.commit_offsets_sync(HashMap::from([(
                ("topic".into(), 0),
                OffsetAndMetadata::new(12),
            )])),
        )
        .await
        .map(|result| result.map_err(|error| error.to_string()));
        mock.stop();
        assert2::assert!(result == Ok(Err(ConsumerError::CommitFailed.to_string())));
    }
}
