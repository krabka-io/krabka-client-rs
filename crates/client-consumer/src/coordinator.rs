//! Background coordinator task. It owns the join/sync/heartbeat/rebalance
//! lifecycle for a [`Consumer`](crate::consumer::Consumer).
//!
//! On each tick the task either sends a steady-state `Heartbeat` or runs a
//! full `JoinGroup` + `SyncGroup` round when `needs_rejoin` is set. The broker
//! signals a rebalance with `error_code = 27 (REBALANCE_IN_PROGRESS)`
//! on heartbeat. `25 (UNKNOWN_MEMBER_ID)` forces a from-scratch
//! handshake, which clears `member_id` and sets `generation_id = -1`.
//!
//! Cooperative rebalance (KIP-429) runs phase-1 and phase-2 in place. Phase 1
//! reduces the owned set to the partitions the member kept. The task then
//! re-Joins and re-Syncs at once, so the leader can place the freshly
//! freed partitions onto whoever needs them. Eager (`range`) drops the
//! whole assignment and reinstalls it in a single round.
//!
//! While a rejoin is in flight the task deliberately does *not* heartbeat.
//! `JoinGroup` resets the broker-side session timer.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        find_coordinator_request::FindCoordinatorRequest,
        find_coordinator_response::FindCoordinatorResponse,
        heartbeat_request::HeartbeatRequest,
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        join_group_response::JoinGroupResponse,
        leave_group_request::{LeaveGroupRequest, MemberIdentity},
        offset_fetch_response::OffsetFetchResponse,
        sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
        sync_group_response::SyncGroupResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use krabka_units::{
    Time,
    convert::{StdDurationExt as _, TimeExt as _},
};
use tokio::sync::{Mutex, Notify, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;

use crate::{
    GroupMembershipOperation,
    assignor::{Assignor, RebalanceProtocol},
    builder::{
        AutoOffsetReset, decode_assignment, decode_subscription, encode_assignment,
        encode_subscription,
    },
    commit::{
        AutoCommitOutcome, auto_commit_outcome, build_commit_request, names_moved_coordinator,
    },
    consumer::{CommitIdentity, ConsumerRetryPolicy, reset_starting_offset, starting_offset},
    error::ConsumerError,
    offset_wire::{
        OffsetFetchAction, TopicNameOffsetFetch, build_commit_topics, build_offset_fetch,
        classify_offset_fetch, parse_offset_fetch,
    },
};

/// Retriable group-coordinator error codes. The coordinator is loading its
/// state (`14`), not yet available (`15`), or has moved to another broker
/// (`16`). Kafka clients retry these with backoff rather than failing.
pub(crate) const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
pub(crate) const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub(crate) const NOT_COORDINATOR: i16 = 16;

/// `FENCED_INSTANCE_ID`: another consumer joined the group with the same
/// `group.instance.id` (KIP-345).
const FENCED_INSTANCE_ID: i16 = 82;
/// `UNKNOWN_MEMBER_ID`.
const UNKNOWN_MEMBER_ID: i16 = 25;

/// The rejoin reason after a heartbeat error.
///
/// Kafka's `HeartbeatResponseHandler` requests a rejoin with "group is
/// already rebalancing" for `REBALANCE_IN_PROGRESS`. For the other errors it
/// calls `resetStateOnResponseError`, whose reason is "encountered <error>
/// from HEARTBEAT response".
fn heartbeat_rejoin_reason(error_code: i16) -> String {
    let error = match error_code {
        27 => return "group is already rebalancing".into(),
        22 => "ILLEGAL_GENERATION",
        UNKNOWN_MEMBER_ID => "UNKNOWN_MEMBER_ID",
        FENCED_INSTANCE_ID => "FENCED_INSTANCE_ID",
        _ => "UNKNOWN_SERVER_ERROR",
    };
    format!("encountered {error} from HEARTBEAT response")
}

/// The rejoin reason after a failed join. Kafka's
/// `AbstractCoordinator.joinGroupIfNeeded` uses "rebalance failed due to
/// <exception class>".
fn rebalance_failure_reason(error: &ConsumerError) -> String {
    let exception = match error {
        ConsumerError::Server(14) => "CoordinatorLoadInProgressException",
        ConsumerError::Server(15) | ConsumerError::CoordinatorUnavailable => {
            "CoordinatorNotAvailableException"
        }
        ConsumerError::Server(16) => "NotCoordinatorException",
        ConsumerError::Server(22) => "IllegalGenerationException",
        ConsumerError::Server(23) => "InconsistentGroupProtocolException",
        ConsumerError::Server(24) => "InvalidGroupIdException",
        ConsumerError::Server(UNKNOWN_MEMBER_ID) => "UnknownMemberIdException",
        ConsumerError::Server(26) => "InvalidSessionTimeoutException",
        ConsumerError::Server(27) => "RebalanceInProgressException",
        ConsumerError::Server(30) | ConsumerError::GroupAuthorizationFailed(_) => {
            "GroupAuthorizationException"
        }
        ConsumerError::Server(81) => "GroupMaxSizeReachedException",
        ConsumerError::FencedInstanceId(_) => "FencedInstanceIdException",
        ConsumerError::TopicAuthorizationFailed(_) => "TopicAuthorizationException",
        ConsumerError::Client(krabka_client_core::ClientError::Timeout(_)) => "TimeoutException",
        ConsumerError::Client(_) => "DisconnectException",
        _ => "KafkaException",
    };
    format!("rebalance failed due to {exception}")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CoordinatorRetryPolicy {
    pub timeout: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl From<ConsumerRetryPolicy> for CoordinatorRetryPolicy {
    fn from(value: ConsumerRetryPolicy) -> Self {
        Self {
            timeout: value.coordinator_retry_timeout().to_std(),
            initial_backoff: value.coordinator_initial_backoff().to_std(),
            max_backoff: value.coordinator_max_backoff().to_std(),
        }
    }
}

pub(crate) fn is_retriable_coordinator_code(code: i16) -> bool {
    matches!(
        code,
        COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR
    )
}

pub(crate) fn is_retriable_transport_error(e: &krabka_client_core::ClientError) -> bool {
    matches!(
        e,
        krabka_client_core::ClientError::Connect { .. }
            | krabka_client_core::ClientError::Tls { .. }
            | krabka_client_core::ClientError::Sasl { .. }
            | krabka_client_core::ClientError::Disconnected
            | krabka_client_core::ClientError::Io(_)
    )
}

/// Read the effective `error_code` from a `FindCoordinatorResponse` across wire
/// shapes.
///
/// v4+ carries per-key rows in `coordinators`, and this function uses the
/// first row. v0-v3 uses the top-level field. krabka's broker populates both,
/// so either read is correct against it. This function stays correct against
/// real Kafka at any negotiated version.
fn coordinator_error_code(r: &FindCoordinatorResponse) -> i16 {
    r.coordinators
        .first()
        .map_or(r.error_code, |c| c.error_code)
}

/// Read the coordinator `node_id` from a `FindCoordinatorResponse` across wire
/// shapes: v4+ uses `coordinators[0].node_id`, and older versions use the
/// top-level `node_id`.
fn coordinator_node_id(r: &FindCoordinatorResponse) -> i32 {
    r.coordinators.first().map_or(r.node_id, |c| c.node_id)
}

/// Discover the broker that currently coordinates `group_id` and return its
/// node id.
///
/// This function sends `FindCoordinator(key = group_id)` over the bootstrap
/// connection, because any broker can answer `FindCoordinator`. It retries the
/// cold and loading coordinator codes (14/15/16) with backoff. On success it
/// calls `refresh_metadata`, so the pool learns the coordinator broker's
/// address from the cluster's broker list. Without that refresh,
/// [`Client::broker`](krabka_client_core::Client::broker) for the coordinator
/// id would fail with `Disconnected`.
///
/// This function returns the coordinator's `node_id`. It errors with
/// `Server(code)` if the lookup keeps returning a non-zero, non-retriable code.
/// It errors with `CoordinatorUnavailable` if, after the refresh, the pool
/// still has no dialable address for that id.
#[tracing::instrument(
    name = "consumer.find_coordinator",
    level = "info",
    skip_all,
    fields(group_id = %group_id, coordinator_id = tracing::field::Empty),
    err
)]
pub(crate) async fn find_coordinator(
    client: &Client,
    group_id: &str,
    retry: CoordinatorRetryPolicy,
) -> Result<i32, ConsumerError> {
    let resp = with_coordinator_retry(retry, coordinator_error_code, || {
        let group_id = group_id.to_string();
        async move {
            match client.send(build_find_coordinator_request(group_id)).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if is_retriable_transport_error(&e) {
                        client.reconnect_bootstrap().await;
                    }
                    Err(ConsumerError::from(e))
                }
            }
        }
    })
    .await?;

    let code = coordinator_error_code(&resp);
    if code != 0 {
        return Err(ConsumerError::Server(code));
    }
    let node_id = coordinator_node_id(&resp);

    // Refresh the pool's (id → addr) registry so a multi-broker cluster learns
    // the coordinator broker's real address and `client.broker(node_id)` dials
    // it directly. A single-broker cluster advertises the coordinator on port 0
    // (deliberately skipped by `refresh_brokers`), leaving the id unknown — but
    // `BrokerHandle::send` then falls back to the bootstrap connection, which on
    // a single-broker cluster IS the coordinator. So we no longer hard-fail when
    // the coordinator isn't a separately dialable broker.
    client.refresh_metadata().await?;
    tracing::Span::current().record("coordinator_id", node_id);
    Ok(node_id)
}

fn build_find_coordinator_request(group_id: String) -> FindCoordinatorRequest {
    FindCoordinatorRequest {
        key: group_id.clone(),
        // v4+ carries the key(s) in `coordinator_keys`; older versions ignore
        // it and use `key`. Populating both keeps us version-agnostic on the
        // negotiated wire form.
        coordinator_keys: vec![group_id],
        ..Default::default()
    }
}

pub(crate) fn retry_deadline_elapsed(start: tokio::time::Instant, timeout: Duration) -> bool {
    start.elapsed() >= timeout
}

pub(crate) fn next_backoff(backoff: Duration, max_backoff: Duration) -> Duration {
    (backoff * 2).min(max_backoff)
}

/// Build the `LeaveGroup` request of one member.
///
/// Kafka's `AbstractCoordinator.maybeLeaveGroup` names the member by its
/// member id and a reason only, also for a static member. The request carries
/// no `group_instance_id`.
pub(crate) fn build_leave_group_request(
    group_id: String,
    member_id: String,
    reason: Option<&str>,
) -> LeaveGroupRequest {
    LeaveGroupRequest {
        group_id,
        member_id: member_id.clone(),
        members: vec![MemberIdentity {
            member_id,
            // KIP-800. Version 5 and later carry the reason.
            reason: reason.map(|reason| truncate_reason(reason).to_owned()),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn build_heartbeat_request(
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
) -> HeartbeatRequest {
    HeartbeatRequest {
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        ..Default::default()
    }
}

/// Send a group-coordinator RPC to the *current* coordinator broker, and
/// re-discover the coordinator before a retry on a cold or relocating
/// coordinator code (14/15/16).
///
/// This chases a moved coordinator to its new home instead of looping forever
/// on the stale id. Real Kafka returns `NOT_COORDINATOR` when an RPC reaches
/// the wrong broker. The plain `with_coordinator_retry` re-sends the identical
/// request to the same broker.
///
/// `coordinator_id` is the shared cell that `make` reads, so each retry targets
/// the latest id. Re-discovery updates that cell in place on success. `make`
/// does the `client.broker(id).send(...)` routing itself. This function mirrors
/// the deadline and backoff of `with_coordinator_retry`. The only addition is
/// the re-find between retriable attempts.
pub(crate) async fn with_coordinator_refind<R, F, Fut>(
    client: &Client,
    group_id: &str,
    coordinator_id: &AtomicI32,
    retry: CoordinatorRetryPolicy,
    code: impl Fn(&R) -> i16,
    make: F,
) -> Result<R, ConsumerError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<R, ConsumerError>>,
{
    let start = tokio::time::Instant::now();
    let mut backoff = retry.initial_backoff;
    loop {
        let needs_refind = match make().await {
            Ok(r) if !is_retriable_coordinator_code(code(&r)) => return Ok(r),
            Ok(r) => {
                if retry_deadline_elapsed(start, retry.timeout) {
                    return Ok(r);
                }
                // Retriable broker code: the coordinator likely moved.
                true
            }
            Err(ConsumerError::Client(e)) if is_retriable_transport_error(&e) => {
                if retry_deadline_elapsed(start, retry.timeout) {
                    return Err(ConsumerError::CoordinatorUnavailable);
                }
                // The socket to the coordinator is gone (bounced / failed
                // over); evict it so re-discovery + the next attempt reconnect
                // to the current coordinator's address.
                client.evict_broker(coordinator_id.load(Ordering::Relaxed));
                true
            }
            Err(e) => return Err(e),
        };
        if needs_refind {
            // Best-effort re-discovery. A transient failure here just means the
            // next attempt reuses the last-known id; the outer deadline (and
            // find_coordinator's own retry) still bound us.
            match find_coordinator(client, group_id, retry).await {
                Ok(id) => coordinator_id.store(id, Ordering::Relaxed),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "coordinator re-discovery failed; retrying with last-known id"
                    );
                }
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = next_backoff(backoff, retry.max_backoff);
    }
}

/// Send a group-coordinator RPC and retry on the cold-coordinator codes
/// (14/15/16) and on transient transport errors.
///
/// The retry uses capped exponential backoff until `timeout` elapses. `make`
/// rebuilds the request on each attempt, so the function can re-send it. `code`
/// reads the response's `error_code`. On the deadline this function returns the
/// last response, so the caller's `error_code` handling runs. It returns
/// `CoordinatorUnavailable` instead if the last attempt was a transport
/// failure.
pub(crate) async fn with_coordinator_retry<R, F, Fut>(
    retry: CoordinatorRetryPolicy,
    code: impl Fn(&R) -> i16,
    make: F,
) -> Result<R, ConsumerError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<R, ConsumerError>>,
{
    let start = tokio::time::Instant::now();
    let mut backoff = retry.initial_backoff;
    loop {
        match make().await {
            Ok(r) if !is_retriable_coordinator_code(code(&r)) => return Ok(r),
            Ok(r) => {
                if retry_deadline_elapsed(start, retry.timeout) {
                    return Ok(r);
                }
            }
            Err(ConsumerError::Client(e)) if is_retriable_transport_error(&e) => {
                if retry_deadline_elapsed(start, retry.timeout) {
                    return Err(ConsumerError::CoordinatorUnavailable);
                }
            }
            Err(e) => return Err(e),
        }
        tokio::time::sleep(backoff).await;
        backoff = next_backoff(backoff, retry.max_backoff);
    }
}

/// Mutable state owned exclusively by the coordinator task.
///
/// The `Arc<Mutex<...>>` fields are shared with the parent `Consumer`, so
/// `poll()` and `assignment()` see live updates as rebalances land. The plain
/// non-`Arc` fields belong to the coordinator alone, and it can mutate them
/// freely. `member_id` and `generation_id` change on a from-scratch rejoin.
pub(crate) struct CoordinatorState {
    pub client: Client,
    pub group_id: String,
    /// Node id of the broker that currently coordinates this group, discovered
    /// with `FindCoordinator`. Every group RPC (Join/Sync/Heartbeat/Commit/
    /// Fetch/Leave) routes here with `client.broker(coordinator_id)`. A
    /// coordinator RPC that returns 14/15/16 triggers re-discovery.
    ///
    /// This `Arc<AtomicI32>` is shared with the parent `Consumer`, so its commit
    /// path (`commit.rs`, on the data-path client) routes `OffsetCommit` to the
    /// same coordinator and sees re-discovery updates the moment they land.
    pub coordinator_id: Arc<AtomicI32>,
    pub member_id: String,
    /// The member id that the parent `Consumer` reports. The task publishes
    /// `member_id` here whenever it publishes a new generation.
    pub published_member_id: tokio::sync::watch::Sender<String>,
    pub commit_identity: Arc<Mutex<CommitIdentity>>,
    pub group_instance_id: Option<String>,
    pub generation_id: i32,
    /// Published copy of `generation_id`. This `Arc<AtomicI32>` is shared with
    /// the parent `Consumer`, so its commit path (`commit.rs`) stamps the
    /// CURRENT generation onto `OffsetCommit`. The coordinator is the sole
    /// writer. Always update both fields together with [`set_generation`], so a
    /// commit after a rebalance never carries the stale generation that the
    /// broker rejects with `ILLEGAL_GENERATION`.
    pub current_generation: Arc<AtomicI32>,
    /// Kafka's `partition.assignment.strategy`, in the order of preference.
    pub assignors: Vec<Assignor>,
    /// The newest rebalance protocol that every assignor supports.
    pub rebalance_protocol: RebalanceProtocol,
    /// The subscription, shared with the `Consumer`.
    pub subscription: crate::subscription::SharedSubscription,
    /// Changes of the subscription that the task did not handle yet.
    pub subscription_changes: tokio::sync::watch::Receiver<crate::subscription::Subscription>,
    /// The topics of the last `JoinGroup`. Kafka's
    /// `ConsumerCoordinator.joinedSubscription`.
    pub joined_topics: Vec<String>,
    /// The `unsubscribe` calls that the task handled.
    pub seen_unsubscribes: u64,
    pub assigned: Arc<Mutex<Vec<(String, i32)>>>,
    pub assignment_changed: Arc<Notify>,
    pub next_ownership_id: u64,
    pub next_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    pub end_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    pub positions: Arc<Mutex<HashMap<(String, i32), crate::position::PartitionPosition>>>,
    pub topic_ids: Arc<Mutex<HashMap<String, WireUuid>>>,
    pub session_timeout: Time,
    /// Kafka's `max.poll.interval.ms`. It is also the rebalance timeout.
    pub max_poll_interval: Time,
    pub heartbeat_interval: Time,
    pub subscription_metadata_refresh_interval: Time,
    pub leave_group_timeout: Time,
    pub auto_offset_reset: AutoOffsetReset,
    pub client_rack: Option<String>,
    /// Subscribed-topic partition counts that the INITIAL assignment was
    /// computed against. This is the metadata snapshot that `start_once` already
    /// fetched. The coordinator seeds its rejoin baseline from this and not from
    /// a fresh post-spawn `Metadata` fetch. A fresh fetch could already include
    /// a topic created in the window between the initial assignment and the
    /// start of this task. It would then compare equal to the baseline forever
    /// and strand the empty cold-start assignment permanently.
    pub initial_subscribed_counts: HashMap<String, i32>,
    pub retry_policy: CoordinatorRetryPolicy,
    /// A fatal error from a rejoin that the next `poll()` returns. This slot is
    /// shared with the parent `Consumer`. Kafka's consumer raises such an
    /// `OffsetFetch` error from `poll()`.
    pub poll_error: PollErrorSlot,
    /// Kafka's `enable.auto.commit` state, shared with the parent `Consumer`.
    /// `None` when auto commit is off.
    pub auto_commit: Option<crate::commit::AutoCommit>,
    /// The commit lock of the parent `Consumer`. The commit before a
    /// `JoinGroup` takes it, so the commits of the consumer reach the
    /// coordinator in order.
    pub commit_serialization: Arc<Mutex<()>>,
    /// `true` after the pre-join auto commit ran and before the join completes.
    /// Kafka's `AbstractCoordinator.needsJoinPrepare` is the inverse flag: a
    /// retry of a failed `JoinGroup` does not commit again.
    pub join_prepared: bool,
    /// The `poll` count of the parent `Consumer`. After a fatal error the task
    /// waits for a `poll` that comes after the one that returned the error.
    pub polls: tokio::sync::watch::Receiver<u64>,
    /// `true` while the member must join the group, from the request of a
    /// rebalance until the join completes. `poll` reads it.
    pub rebalance_pending: tokio::sync::watch::Sender<bool>,
    /// What the member does with its group membership when the task stops.
    /// `close_with` writes it before it stops the task.
    pub close_operation: tokio::sync::watch::Receiver<crate::control::CloseRequest>,
    /// The reason of the next `JoinGroup` (KIP-800). Kafka's
    /// `AbstractCoordinator.rejoinReason`: empty for the first join, set by
    /// each request to join again, and cleared after a completed sync.
    pub rejoin_reason: String,
    /// The calls of the rebalance listener, or `None` without a listener.
    pub listener_calls: Option<crate::rebalance_listener::ListenerCalls>,
    /// The reasons of the rebalances that `Consumer::enforce_rebalance` asks
    /// for.
    pub enforced_rebalances: tokio::sync::mpsc::UnboundedReceiver<(String, u64)>,
    /// The partitions that the member lost with its generation. The next join
    /// gives them to `on_partitions_lost`, as Kafka's `onJoinPrepare` does.
    pub lost_partitions: Vec<(String, i32)>,
    /// The added partitions that wait for `on_partitions_assigned`. `poll`
    /// does not fetch them.
    pub assigned_callback_pending: crate::rebalance_listener::AssignedCallbackPending,
    /// Kafka's `Heartbeat.pollTimer`: `poll` and each completed join reset it.
    pub poll_timer: PollTimer,
    /// `true` when the poll timer expired while the task waited for a listener
    /// callback. The task then leaves the group, as after a poll timeout.
    pub poll_timeout_in_callback: bool,
}

/// Remember the owned partitions as lost before a reset clears them.
async fn remember_lost_partitions(state: &mut CoordinatorState) {
    let owned = state.assigned.lock().await.clone();
    for partition in owned {
        if !state.lost_partitions.contains(&partition) {
            state.lost_partitions.push(partition);
        }
    }
}

/// Ask `poll` to run a rebalance listener callback and wait until it ran.
///
/// Kafka runs the callbacks on the application thread inside `poll`, and its
/// heartbeat thread keeps the member in the group meanwhile. The task sends a
/// heartbeat each heartbeat interval while it waits.
///
/// # Errors
///
/// Returns `FencedInstanceId` when a heartbeat says that another consumer
/// joined with the same `group.instance.id`.
async fn call_listener(
    state: &mut CoordinatorState,
    kind: crate::rebalance_listener::ListenerCallKind,
    partitions: Vec<(String, i32)>,
) -> Result<(), ConsumerError> {
    loop {
        let Some(calls) = &state.listener_calls else {
            return Ok(());
        };
        let (done, ran) = tokio::sync::oneshot::channel();
        if calls
            .send(crate::rebalance_listener::ListenerCall {
                kind,
                partitions: partitions.clone(),
                done,
            })
            .is_err()
        {
            // The consumer is gone.
            return Ok(());
        }
        let (ran_in_time, heartbeat) = {
            let state = &*state;
            let call_done = CancellationToken::new();
            let wait = async {
                let ran_in_time = wait_for_listener_call(state, ran).await;
                call_done.cancel();
                ran_in_time
            };
            tokio::join!(wait, heartbeat_during_join_prepare(state, &call_done))
        };
        let resend = match ran_in_time {
            CallWait::Ran => {
                state.poll_timer.reset();
                false
            }
            // A cancelled `poll` dropped the call before the callback ended.
            // The next `poll` runs it again, as Kafka runs each callback to
            // its end before the rebalance continues.
            CallWait::Dropped(deadline) => {
                state.poll_timer.deadline = deadline;
                true
            }
            CallWait::PollTimeout => {
                // Kafka's heartbeat thread leaves the group when the poll timer
                // expires, also while a callback runs (`AbstractCoordinator.
                // HeartbeatThread.run`, `handlePollTimeoutExpiry`).
                state.poll_timeout_in_callback = true;
                return Err(ConsumerError::RebalanceFailed(
                    "max_poll_interval expired while a rebalance listener callback waited".into(),
                ));
            }
        };
        match heartbeat {
            Some(HeartbeatOutcome::Fenced) => {
                return Err(ConsumerError::FencedInstanceId(
                    state.group_instance_id.clone().unwrap_or_default(),
                ));
            }
            Some(HeartbeatOutcome::RejoinFromScratch) => {
                forget_member(state).await;
                state.rejoin_reason = heartbeat_rejoin_reason(UNKNOWN_MEMBER_ID);
                return Ok(());
            }
            _ if resend => {}
            _ => return Ok(()),
        }
    }
}

/// How the wait for a listener call ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CallWait {
    /// The callback ran. `poll` ran it, so the poll timer starts again.
    Ran,
    /// The consumer dropped the call before the callback ended, with the poll
    /// timer deadline after the last `poll`.
    Dropped(tokio::time::Instant),
    /// No `poll` came before the poll timer expired.
    PollTimeout,
}

/// Wait until `poll` ran a listener call, or until the poll timer expires
/// without a `poll`.
async fn wait_for_listener_call(
    state: &CoordinatorState,
    ran: tokio::sync::oneshot::Receiver<()>,
) -> CallWait {
    let mut ran = ran;
    let mut deadline = state.poll_timer.deadline;
    let mut polls = *state.polls.borrow();
    loop {
        tokio::select! {
            biased;
            result = &mut ran => {
                return if result.is_ok() {
                    CallWait::Ran
                } else {
                    CallWait::Dropped(deadline)
                };
            }
            () = tokio::time::sleep_until(deadline) => {
                let now_polls = *state.polls.borrow();
                if now_polls == polls {
                    return CallWait::PollTimeout;
                }
                polls = now_polls;
                deadline = tokio::time::Instant::now() + state.poll_timer.interval;
            }
        }
    }
}

/// Kafka's `ConsumerCoordinator.onJoinPrepare`: the auto commit, then the
/// listener. It runs once per join; a retry of a failed `JoinGroup` does not
/// run it again.
///
/// A member without a generation gives its lost partitions to
/// `on_partitions_lost`. An eager member gives all owned partitions to
/// `on_partitions_revoked`. A cooperative member gives only the partitions of
/// topics that it no longer subscribes to.
async fn join_prepare(state: &mut CoordinatorState) -> Result<(), ConsumerError> {
    if state.join_prepared {
        return Ok(());
    }
    commit_before_join(state).await?;
    state.join_prepared = true;
    if state.member_id.is_empty() || state.generation_id < 0 {
        let lost = std::mem::take(&mut state.lost_partitions);
        if !lost.is_empty() {
            call_listener(
                state,
                crate::rebalance_listener::ListenerCallKind::Lost,
                lost,
            )
            .await?;
        }
    } else if state.rebalance_protocol == RebalanceProtocol::Eager {
        let owned = state.assigned.lock().await.clone();
        call_listener(
            state,
            crate::rebalance_listener::ListenerCallKind::Revoked,
            owned,
        )
        .await?;
    } else {
        // Kafka's cooperative `onJoinPrepare` revokes only the partitions of
        // topics that the consumer no longer subscribes to.
        let owned = state.assigned.lock().await.clone();
        let (kept, revoked): (Vec<_>, Vec<_>) = {
            let subscription = state.subscription.borrow();
            owned
                .into_iter()
                .partition(|(topic, _)| subscription.contains(topic))
        };
        if !revoked.is_empty() {
            call_listener(
                state,
                crate::rebalance_listener::ListenerCallKind::Revoked,
                revoked,
            )
            .await?;
            let generation = state.generation_id;
            publish_assignment(state, &kept, true, generation).await;
        }
    }
    Ok(())
}

/// A fatal coordinator error that waits for the next `poll()`.
///
/// The guard of this `std::sync::Mutex` never lives across an `.await` or
/// while another lock is held.
pub(crate) type PollErrorSlot = Arc<std::sync::Mutex<Option<ConsumerError>>>;

/// The `poll` count of a `Consumer`. `poll` increments it when it returns no
/// error that a coordinator failure left. See [`note_poll`].
pub(crate) type PollSignal = tokio::sync::watch::Sender<u64>;

/// Tell the coordinator task that the application called `poll`, and that the
/// `poll` returned no error that a coordinator failure left.
pub(crate) fn note_poll(signal: &PollSignal) {
    signal.send_modify(|polls| *polls = polls.wrapping_add(1));
}

/// Whether an error waits in `slot` for the next `poll`.
pub(crate) fn poll_error_pending(slot: &PollErrorSlot) -> bool {
    slot.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_some()
}

/// Wait until the application calls `poll` again after `poll` returned the
/// error in `poll_error`.
///
/// Kafka's `AbstractCoordinator.pollHeartbeat` raises the failure cause of the
/// heartbeat thread once. The next `poll` then calls `ensureActiveGroup`, which
/// joins the group again. The function returns `false` when `shutdown` fires
/// or when the `Consumer` is gone.
async fn wait_for_poll_after_error(
    polls: &mut tokio::sync::watch::Receiver<u64>,
    poll_error: &PollErrorSlot,
    shutdown: &CancellationToken,
) -> bool {
    polls.borrow_and_update();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return false,
            changed = polls.changed() => {
                if changed.is_err() {
                    return false;
                }
                // A `poll` that ran before it took the error does not count.
                if !poll_error_pending(poll_error) {
                    return true;
                }
            }
        }
    }
}

/// Keep `error` for the next `poll()` if the application must see it.
///
/// An authentication failure is such an error. Kafka's heartbeat thread makes
/// an `AuthenticationException` its failure cause, and `poll` raises it
/// (`AbstractCoordinator.HeartbeatThread.run`).
pub(crate) fn report_rejoin_error(slot: &PollErrorSlot, error: ConsumerError) {
    if error.is_fatal_offset_fetch_error()
        || matches!(&error, ConsumerError::Client(client) if client.is_authentication_failure())
    {
        *slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
    }
}

/// Take the error that a rejoin left for `poll()`, if any.
pub(crate) fn take_poll_error(slot: &PollErrorSlot) -> Option<ConsumerError> {
    slot.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

/// Set the coordinator's working generation AND publish it to the shared atomic
/// that the commit path reads.
///
/// Use this for EVERY generation change: join, rejoin, and from-scratch reset.
/// The parent `Consumer`'s `OffsetCommit` then always stamps the current
/// generation. The broker rejects a stale generation with
/// `ILLEGAL_GENERATION`.
fn set_generation(state: &mut CoordinatorState, generation_id: i32) {
    state.generation_id = generation_id;
    state
        .current_generation
        .store(generation_id, Ordering::Release);
}

/// Install `generation_id` for commits and heartbeats, and keep the
/// assignment. The member id of the join goes with it.
async fn install_generation(state: &mut CoordinatorState, generation_id: i32) {
    let mut identity = state.commit_identity.lock().await;
    identity.generation = generation_id;
    identity.member_id.clone_from(&state.member_id);
    drop(identity);
    set_generation(state, generation_id);
}

/// Keep `poll` from fetching `partitions` until the returned gate drops after
/// their assign callback. Without a listener there is no callback to wait for.
fn assigned_callback_gate(
    state: &CoordinatorState,
    partitions: &[(String, i32)],
) -> Option<crate::rebalance_listener::AssignedCallbackGate> {
    state.listener_calls.as_ref().map(|_| {
        crate::rebalance_listener::AssignedCallbackGate::new(
            &state.assigned_callback_pending,
            partitions,
        )
    })
}

async fn publish_assignment(
    state: &mut CoordinatorState,
    assignment: &[(String, i32)],
    preserve_retained: bool,
    generation_id: i32,
) {
    install_assignment(state, assignment, preserve_retained, generation_id, false).await;
}

/// Publish `assignment` at `generation_id` to the shared state of the
/// `Consumer`. `rejoin_on_poll` goes to the commit identity in the same
/// critical section as the ownership.
async fn install_assignment(
    state: &mut CoordinatorState,
    assignment: &[(String, i32)],
    preserve_retained: bool,
    generation_id: i32,
    rejoin_on_poll: bool,
) {
    let mut next_id = state.next_ownership_id;
    let mut assigned = state.assigned.lock().await;
    let mut identity = state.commit_identity.lock().await;
    update_ownership(
        &mut identity.ownership_ids,
        assignment,
        preserve_retained,
        &mut next_id,
    );
    assigned.clear();
    assigned.extend_from_slice(assignment);
    identity.generation = generation_id;
    identity.member_id.clone_from(&state.member_id);
    identity.rejoin_on_poll = rejoin_on_poll;
    drop(identity);
    state
        .published_member_id
        .send_replace(state.member_id.clone());
    drop(assigned);
    // A high watermark belongs to an ownership snapshot.  Re-learn it from
    // the next successful fetch after any assignment publication.
    state.end_offsets.lock().await.clear();
    state.next_ownership_id = next_id;
    set_generation(state, generation_id);
    state.assignment_changed.notify_waiters();
}

fn update_ownership(
    ownership: &mut HashMap<(String, i32), u64>,
    assignment: &[(String, i32)],
    preserve_retained: bool,
    next_id: &mut u64,
) {
    if preserve_retained {
        let assigned = assignment.iter().collect::<HashSet<_>>();
        ownership.retain(|partition, _| assigned.contains(partition));
    } else {
        ownership.clear();
    }
    for partition in assignment {
        ownership.entry(partition.clone()).or_insert_with(|| {
            let id = *next_id;
            *next_id = next_id.wrapping_add(1);
            id
        });
    }
}

/// Outcome of a single heartbeat RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeartbeatOutcome {
    /// `error_code == 0`.
    Ok,
    /// `REBALANCE_IN_PROGRESS (27)` or `ILLEGAL_GENERATION (22)`. Rejoin with
    /// the current `member_id`. `ILLEGAL_GENERATION` fires when the heartbeat
    /// tick lands after the broker has already advanced past the generation the
    /// member last synced on, for example when a rebalance completed between
    /// two heartbeat windows. Without a rejoin the member would keep
    /// heartbeating the dead generation forever and would never pick up the new
    /// assignment.
    NeedRejoin(i16),
    /// `UNKNOWN_MEMBER_ID (25)`. Clear `member_id` and rejoin from scratch.
    RejoinFromScratch,
    /// `FENCED_INSTANCE_ID (82)`. Another consumer joined with the same
    /// `group.instance.id`. The member leaves the group until the next `poll`.
    /// Kafka's `AbstractCoordinator.HeartbeatResponseHandler` resets the member
    /// and raises `FencedInstanceIdException`, and the heartbeat thread keeps
    /// it as its failure cause.
    Fenced,
    /// Transport error or unexpected non-fatal broker code. Retry on the next tick.
    Transient,
}

fn heartbeat_outcome(error_code: i16) -> HeartbeatOutcome {
    match error_code {
        0 => HeartbeatOutcome::Ok,
        27 | 22 => HeartbeatOutcome::NeedRejoin(error_code),
        25 => HeartbeatOutcome::RejoinFromScratch,
        FENCED_INSTANCE_ID => HeartbeatOutcome::Fenced,
        _ => HeartbeatOutcome::Transient,
    }
}

/// Drive the heartbeat + rebalance loop until `shutdown` fires.
///
/// On entry the caller has already done one initial Join+Sync, so the loop
/// begins in steady-state heartbeating. `needs_rejoin` becomes `true` as soon
/// as the broker signals a rebalance. The next tick then does the rejoin in
/// place of the heartbeat.
#[cfg_attr(test, mutants::skip)] // cargo-mutants: long-running I/O event loop, exercised by integration tests
fn subscription_metadata_refresh_due(last_check: tokio::time::Instant, interval: Time) -> bool {
    last_check.elapsed().as_time() >= interval
}

/// Current partition count of each subscribed topic that exists in broker
/// metadata.
///
/// A subscribed topic that does not exist yet is absent from the map. It shows
/// up later as growth once someone creates the topic.
#[tracing::instrument(
    name = "consumer.subscribed_partition_counts",
    level = "debug",
    skip_all,
    fields(group_id = %state.group_id, topics = tracing::field::Empty),
    err
)]
async fn subscribed_partition_counts(
    state: &CoordinatorState,
) -> Result<HashMap<String, i32>, ConsumerError> {
    let md = state.client.refresh_metadata().await?;
    let mut counts = HashMap::new();
    for t in &md.topics {
        let Some(name) = &t.name else { continue };
        if state.subscription.borrow().contains(name) {
            counts.insert(
                name.clone(),
                i32::try_from(t.partitions.len()).unwrap_or(i32::MAX),
            );
        }
    }
    tracing::Span::current().record("topics", counts.len());
    Ok(counts)
}

/// True when any subscribed topic now has more partitions than the assignment
/// was last computed against, which means a topic appeared or grew.
///
/// Such a change means the group must rejoin, so the assignor redistributes the
/// new partitions. Without the rejoin, a consumer that joined before its WAL
/// topic existed keeps an EMPTY assignment forever. The broker never sends a
/// rebalance to a single-member Stable group, and that rebalance is the only
/// other thing that sets `needs_rejoin`.
fn subscribed_topics_grew(known: &HashMap<String, i32>, current: &HashMap<String, i32>) -> bool {
    current
        .iter()
        .any(|(topic, count)| *count > known.get(topic).copied().unwrap_or(0))
}

/// Fold `current` into `known` and take the per-topic max.
///
/// Kafka partition counts are monotonic, because a topic never loses
/// partitions. The rejoin baseline must therefore only ever ADVANCE. A
/// transient metadata under-report from a controller failover or a partial
/// response must never lower it and re-trigger a spurious rejoin. A non-leader
/// rejoin, whose snapshot is empty, must leave the baseline untouched and must
/// not erase it.
fn merge_counts(known: &mut HashMap<String, i32>, current: &HashMap<String, i32>) {
    for (topic, &count) in current {
        let entry = known.entry(topic.clone()).or_insert(0);
        *entry = (*entry).max(count);
    }
}

pub(crate) async fn run(mut state: CoordinatorState, shutdown: CancellationToken) {
    // The coordinator task runs on its own `Client` (separate pool from the
    // build/data-path client), so its pool's (id → addr) registry starts empty.
    // Populate it once up front so the very first heartbeat's
    // `client.broker(coordinator_id)` resolves an address instead of failing
    // `Disconnected` and burning a heartbeat interval on re-discovery. The id
    // was already discovered at build time; this just teaches *this* pool where
    // that broker lives. Best-effort: a failure here is recovered by the
    // heartbeat path's Disconnected → re-discover handling.
    if let Err(e) = state.client.refresh_metadata().await {
        tracing::warn!(error = %e, "coordinator client metadata refresh failed at startup");
    }

    let mut ticker = tokio::time::interval(state.heartbeat_interval.to_std());
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut rejoin = RejoinRequest::default();
    // Subscribed-topic partition counts the current assignment was computed
    // against. We rejoin when these GROW (a topic created after we joined, or a
    // topic that gains partitions) so the assignor distributes the new
    // partitions. Seeded from the snapshot the INITIAL assignment used (threaded
    // in as `initial_subscribed_counts`), NOT a fresh fetch here: a fresh fetch
    // could already include a topic created in the window between that initial
    // Metadata and this task starting, comparing equal to the baseline forever
    // and stranding the empty cold-start assignment permanently. Advanced only
    // ever via `merge_counts` (monotonic max), so a transient metadata blip
    // can't lower it. This is what lets a consumer that joined before its WAL
    // topic existed (empty assignment) recover once the topic is created.
    let mut known_counts = std::mem::take(&mut state.initial_subscribed_counts);
    let mut last_meta_check = tokio::time::Instant::now();
    // Kafka's `Heartbeat.pollTimer`: `poll` and each completed join reset it.
    state.poll_timer = PollTimer::new(state.max_poll_interval);
    let mut polls_open = true;

    loop {
        let event = tokio::select! {
            // The ticker goes before a `poll`, so frequent polls cannot starve
            // the heartbeats. The ticker fires once per heartbeat interval, so
            // it cannot starve the polls either.
            biased;
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => TaskEvent::Tick,
            changed = state.polls.changed(), if polls_open => {
                if changed.is_ok() {
                    TaskEvent::Poll
                } else {
                    polls_open = false;
                    continue;
                }
            }
            // Kafka's `enforceRebalance` calls `requestRejoin`: the next `poll`
            // joins the group again with the reason.
            Some((reason, polls)) = state.enforced_rebalances.recv() => {
                state.rejoin_reason = reason;
                rejoin.request_after_poll(polls, &state.rebalance_pending);
                continue;
            }
            // `subscribe`, `subscribe_pattern` and `unsubscribe`.
            Ok(()) = state.subscription_changes.changed() => {
                handle_subscription_change(&mut state, &mut rejoin).await;
                continue;
            }
            // Kafka's heartbeat thread checks `pollTimeoutExpired` each retry
            // backoff. The task wakes at the deadline of the poll timer.
            () = tokio::time::sleep_until(state.poll_timer.deadline), if !state.member_id.is_empty() => {
                TaskEvent::PollTimeout
            }
        };
        if event == TaskEvent::Poll {
            state.poll_timer.reset();
            if !rejoin.due(&state.polls) {
                continue;
            }
        } else if state.polls.has_changed().unwrap_or(false) {
            // A `poll` that came while an RPC was in flight resets the timer
            // before the expiry check.
            state.polls.borrow_and_update();
            state.poll_timer.reset();
        }

        if event != TaskEvent::Poll && !state.member_id.is_empty() && state.poll_timer.expired() {
            // Take the `poll` count before the `LeaveGroup` goes out: a `poll`
            // during the request starts the join.
            state.rejoin_reason = "consumer pro-actively leaving the group".into();
            rejoin.request_after_next_poll(&state);
            leave_on_poll_timeout(&mut state).await;
            continue;
        }

        // Detect a subscribed topic appearing / gaining partitions after we
        // joined (the cold-start race that otherwise strands an empty
        // assignment) and rejoin to distribute it. Throttled, and only when not
        // already rejoining. Best-effort: a failed metadata RPC just retries.
        if !rejoin.requested()
            && subscription_metadata_refresh_due(
                last_meta_check,
                state.subscription_metadata_refresh_interval,
            )
        {
            last_meta_check = tokio::time::Instant::now();
            // Kafka's `ConsumerCoordinator.maybeUpdateSubscriptionMetadata`
            // matches a pattern against each metadata update.
            if refresh_pattern_topics(&mut state).await
                && state.subscription.borrow().topics != state.joined_topics
            {
                let topics = state.subscription.borrow().topics.clone();
                state.rejoin_reason =
                    crate::subscription::subscription_changed_reason(&state.joined_topics, &topics);
                rejoin.request_after_next_poll(&state);
            } else if let Ok(current) = subscribed_partition_counts(&state).await
                && subscribed_topics_grew(&known_counts, &current)
            {
                tracing::info!(
                    group = %state.group_id,
                    "subscribed-topic partitions changed; rejoining to update assignment"
                );
                // Don't advance `known_counts` here against this fresh fetch —
                // it could record partitions the rejoin doesn't end up assigning
                // (e.g. a leader whose Metadata lags this read). Advance only
                // once the rejoin lands, from the snapshot its assignment was
                // actually computed against (the Ok branch below).
                state.rejoin_reason = "cached metadata has changed".into();
                rejoin.request_after_next_poll(&state);
            }
        }

        // Race the per-tick RPCs against shutdown so `close()` returns
        // promptly even when we're mid-rebalance and the broker is holding
        // a JoinGroup / SyncGroup open. Without this, cancellation is only
        // observed *between* ticks, so a `rejoin()` in flight against an
        // open broker call would stall `close()` for up to session_timeout.
        // The RPC futures are cancellation-safe: `Client` multiplexes on
        // correlation ids, so dropping an in-flight send only abandons its
        // pending response — it can't corrupt the connection.
        if rejoin.due(&state.polls) {
            tokio::select! {
                () = shutdown.cancelled() => break,
                result = rejoin_group(&mut state) => match result {
                    Ok(snapshot) => {
                        rejoin.complete(&state);
                        state.poll_timer.reset();
                        // Re-baseline from the metadata the rejoin's assignment
                        // was actually computed against (the leader's snapshot;
                        // empty for a non-leader, which `merge_counts` leaves
                        // untouched) — NOT a third independent fetch, which could
                        // read a newer count than was assigned and strand the
                        // difference. Monotonic max-merge: only ever advance.
                        merge_counts(&mut known_counts, &snapshot);
                    }
                    Err(ConsumerError::FencedInstanceId(group_instance_id)) => {
                        fence_member(&mut state, group_instance_id).await;
                        if !wait_for_poll_after_error(&mut state.polls, &state.poll_error, &shutdown).await {
                            break;
                        }
                        state.poll_timer.reset();
                        state.rejoin_reason = rebalance_failure_reason(&ConsumerError::FencedInstanceId(String::new()));
                        rejoin.request_now(&state);
                    }
                    Err(_) if std::mem::take(&mut state.poll_timeout_in_callback) => {
                        state.rejoin_reason = "consumer pro-actively leaving the group".into();
                        // The join of the stalled `poll` ends here. The next
                        // `poll` starts a new one.
                        rejoin = RejoinRequest::None;
                        rejoin.request_after_next_poll(&state);
                        leave_on_poll_timeout(&mut state).await;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "rejoin failed; will retry on next tick");
                        state.rejoin_reason = rebalance_failure_reason(&e);
                        report_rejoin_error(&state.poll_error, e);
                    }
                },
            }
        } else if event == TaskEvent::Tick && !state.member_id.is_empty() {
            tokio::select! {
                () = shutdown.cancelled() => break,
                outcome = heartbeat_once(&state) => match outcome {
                    HeartbeatOutcome::Ok | HeartbeatOutcome::Transient => {}
                    HeartbeatOutcome::NeedRejoin(error_code) => {
                        state.rejoin_reason = heartbeat_rejoin_reason(error_code);
                        rejoin.request_after_next_poll(&state);
                    }
                    HeartbeatOutcome::RejoinFromScratch => {
                        forget_member(&mut state).await;
                        state.rejoin_reason = heartbeat_rejoin_reason(UNKNOWN_MEMBER_ID);
                        rejoin.request_after_next_poll(&state);
                    }
                    HeartbeatOutcome::Fenced => {
                        let group_instance_id = state.group_instance_id.clone().unwrap_or_default();
                        fence_member(&mut state, group_instance_id).await;
                        if !wait_for_poll_after_error(&mut state.polls, &state.poll_error, &shutdown).await {
                            break;
                        }
                        state.poll_timer.reset();
                        state.rejoin_reason = heartbeat_rejoin_reason(FENCED_INSTANCE_ID);
                        rejoin.request_now(&state);
                    }
                },
            }
        }
    }

    // Graceful departure: tell the broker to evict us *now* rather than
    // waiting out `session_timeout`. This MUST use `state.member_id`, which
    // is the live id — a from-scratch rejoin (`UNKNOWN_MEMBER_ID`) replaces
    // it mid-life, so the `Consumer`'s build-time copy can be stale. Leaving
    // with a stale id is a silent no-op that orphans the real member until
    // its session expires, stalling the rest of the group's rebalance.
    // Best-effort and bounded: a hung broker must not block `close()`.
    let close = *state.close_operation.borrow();
    leave_group(
        &state,
        &state.member_id,
        close.operation,
        CLOSE_LEAVE_REASON,
    )
    .await;
}

/// What woke the coordinator task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TaskEvent {
    /// The heartbeat interval passed.
    Tick,
    /// The application called `poll`.
    Poll,
    /// The poll timer reached its deadline.
    PollTimeout,
}

/// Kafka's `Heartbeat.pollTimer`: it expires when no `poll` and no completed
/// join came for `max.poll.interval.ms`.
pub(crate) struct PollTimer {
    interval: Duration,
    deadline: tokio::time::Instant,
}

impl PollTimer {
    pub(crate) fn new(interval: Time) -> Self {
        let interval = interval.to_std();
        Self {
            interval,
            deadline: tokio::time::Instant::now() + interval,
        }
    }

    fn reset(&mut self) {
        self.deadline = tokio::time::Instant::now() + self.interval;
    }

    /// Kafka's `Timer.isExpired`: the deadline is inclusive.
    fn expired(&self) -> bool {
        tokio::time::Instant::now() >= self.deadline
    }
}

/// A rejoin that the coordinator task starts in the next `poll`.
///
/// Kafka's `ConsumerCoordinator.poll` calls `ensureActiveGroup` only when the
/// application polls. A rebalance that the coordinator asks for therefore does
/// not start while the application processes the records of the last `poll`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum RejoinRequest {
    /// No rejoin is needed.
    #[default]
    None,
    /// A rejoin is due when a `poll` changed the `poll` count from this value.
    AfterPoll(u64),
    /// A rejoin is due at once, because a `poll` already came.
    Now,
}

impl RejoinRequest {
    fn requested(self) -> bool {
        self != Self::None
    }

    /// Request a rejoin that starts when the application polls again.
    fn request_after_next_poll(&mut self, state: &CoordinatorState) {
        self.request_after_poll(*state.polls.borrow(), &state.rebalance_pending);
    }

    /// Request a rejoin that starts with the first `poll` after the `poll`
    /// count `polls`.
    fn request_after_poll(
        &mut self,
        polls: u64,
        rebalance_pending: &tokio::sync::watch::Sender<bool>,
    ) {
        if *self == Self::None {
            *self = Self::AfterPoll(polls);
        }
        rebalance_pending.send_replace(true);
    }

    /// Request a rejoin that starts at once, because a `poll` already came.
    fn request_now(&mut self, state: &CoordinatorState) {
        *self = Self::Now;
        state.rebalance_pending.send_replace(true);
    }

    fn due(self, polls: &tokio::sync::watch::Receiver<u64>) -> bool {
        match self {
            Self::None => false,
            Self::Now => true,
            Self::AfterPoll(count) => *polls.borrow() != count,
        }
    }

    fn complete(&mut self, state: &CoordinatorState) {
        *self = Self::None;
        state.rebalance_pending.send_replace(false);
    }
}

/// The `LeaveGroup` reason of `close`. Kafka's `AbstractCoordinator.close`.
const CLOSE_LEAVE_REASON: &str = "the consumer is being closed";

/// The `LeaveGroup` reason after `max.poll.interval.ms`. Kafka's
/// `AbstractCoordinator.handlePollTimeoutExpiry`.
const POLL_TIMEOUT_LEAVE_REASON: &str = "consumer poll timeout has expired.";

/// Leave the group because the application did not call `poll` within
/// `max.poll.interval.ms`.
///
/// Kafka's `AbstractCoordinator.handlePollTimeoutExpiry` calls
/// `maybeLeaveGroup`. It sends `LeaveGroup` for a dynamic member only, and it
/// resets the generation and the member id for every member. The heartbeats
/// then stop, and the next `poll` joins the group again. This function does the
/// same, clears the assignment, and marks the commit identity with
/// `rejoin_on_poll`, so a commit fails with `CommitFailed` until the join.
///
/// The function clears the member and the assignment before it sends the
/// `LeaveGroup`, so a `poll` during the request fetches nothing. The task
/// sends the join that such a `poll` requests after the `LeaveGroup`, so the
/// coordinator never sees the new member before the old one leaves.
async fn leave_on_poll_timeout(state: &mut CoordinatorState) {
    tracing::warn!(
        group = %state.group_id,
        max_poll_interval = ?state.max_poll_interval,
        "consumer poll timeout has expired: the time between two poll calls was longer than \
         max_poll_interval; the member leaves the group and joins again on the next poll"
    );
    let member_id = std::mem::take(&mut state.member_id);
    state.rebalance_pending.send_replace(true);
    remember_lost_partitions(state).await;
    install_assignment(state, &[], false, -1, true).await;
    if should_send_leave_group(
        &member_id,
        state.group_instance_id.as_deref(),
        GroupMembershipOperation::Default,
    ) {
        leave_group(
            state,
            &member_id,
            GroupMembershipOperation::Default,
            POLL_TIMEOUT_LEAVE_REASON,
        )
        .await;
    }
}

/// Act on a change of the subscription.
///
/// An empty subscription is Kafka's `unsubscribe`: the member leaves the group
/// with the reason `the consumer unsubscribed from all topics`, and its
/// generation and assignment are reset (`maybeLeaveGroup`,
/// `resetGenerationOnLeaveGroup`). The `Consumer` already ran the listener.
/// Any other change makes the next `poll` join the group when the topics are
/// not the joined ones, as `rejoinNeededOrPending` does.
async fn handle_subscription_change(state: &mut CoordinatorState, rejoin: &mut RejoinRequest) {
    let subscription = state.subscription_changes.borrow_and_update().clone();
    if subscription.unsubscribes != state.seen_unsubscribes {
        state.seen_unsubscribes = subscription.unsubscribes;
        let member_id = std::mem::take(&mut state.member_id);
        state.lost_partitions.clear();
        install_assignment(state, &[], false, -1, false).await;
        *rejoin = RejoinRequest::None;
        state.rebalance_pending.send_replace(false);
        // Kafka's `resetGenerationOnLeaveGroup` requests the next join with
        // this reason.
        state.rejoin_reason = "consumer pro-actively leaving the group".into();
        leave_group(
            state,
            &member_id,
            GroupMembershipOperation::Default,
            UNSUBSCRIBE_LEAVE_REASON,
        )
        .await;
    }
    if subscription.is_none() {
        return;
    }
    refresh_pattern_topics(state).await;
    let topics = state.subscription.borrow().topics.clone();
    // The task has its own client. Its metadata requests name the new topics,
    // so the leader assigns them.
    state.client.metadata_topics().set(topics.iter().cloned());
    if topics != state.joined_topics {
        state.rejoin_reason =
            crate::subscription::subscription_changed_reason(&state.joined_topics, &topics);
        rejoin.request_after_next_poll(state);
    } else if state.member_id.is_empty() {
        rejoin.request_after_next_poll(state);
    }
}

/// The `LeaveGroup` reason of `unsubscribe`. Kafka's
/// `ClassicKafkaConsumer.unsubscribe`.
const UNSUBSCRIBE_LEAVE_REASON: &str = "the consumer unsubscribed from all topics";

/// Match the pattern of a pattern subscription against all topics of the
/// cluster, and store the matched topics. Return whether they changed.
async fn refresh_pattern_topics(state: &mut CoordinatorState) -> bool {
    let (pattern, version) = {
        let subscription = state.subscription.borrow();
        let Some(pattern) = subscription.pattern.clone() else {
            return false;
        };
        (pattern, subscription.version)
    };
    let Ok(metadata) = state
        .client
        .refresh_metadata_with(krabka_protocol::owned::metadata_request::MetadataRequest::default())
        .await
    else {
        return false;
    };
    let matched = crate::subscription::Subscription {
        pattern: Some(pattern),
        ..crate::subscription::Subscription::topics(
            Vec::new(),
            state.subscription.borrow().exclude_internal_topics,
        )
    }
    .matching_topics(&metadata)
    .unwrap_or_default();
    // Store the topics only for the subscription that the request matched. A
    // `subscribe`, `subscribe_pattern` or `unsubscribe` of the application
    // during the request stays a change that the task handles.
    let stored = state
        .subscription
        .send_if_modified(|subscription| subscription.store_pattern_topics(version, &matched));
    if state.subscription.borrow().version == version {
        state.client.metadata_topics().set(matched.iter().cloned());
        if stored {
            // The task made this change itself.
            state.subscription_changes.borrow_and_update();
        }
    }
    stored
}

/// Forget the member id, the generation and the partition ownership after
/// `UNKNOWN_MEMBER_ID`, so the next join starts from scratch.
///
/// Kafka's `ConsumerCoordinator.onJoinPrepare` treats the owned partitions of
/// a member without a generation as lost, for the eager and the cooperative
/// protocol. The function therefore clears the assignment too, so no `poll`
/// fetches a partition that the coordinator can give to another member.
async fn forget_member(state: &mut CoordinatorState) {
    state.member_id.clear();
    state.rebalance_pending.send_replace(true);
    remember_lost_partitions(state).await;
    install_assignment(state, &[], false, -1, false).await;
}

/// Remove the member from the group after the coordinator fenced its
/// `group.instance.id`.
///
/// The function marks the commit identity with `rejoin_on_poll`, so a commit
/// fails with `CommitFailed` until the member joins again. In the same step it
/// clears the assignment, the member id and the generation, so `poll()` fetches
/// nothing. It then keeps the fenced error for the next `poll()`. The caller
/// waits for the `poll` after that, and then joins the group from scratch with
/// an empty member id. Kafka's `AbstractCoordinator.resetStateOnResponseError`
/// resets the generation and requests a rejoin in the same way. A shutdown
/// before the rejoin sends no `LeaveGroup`, because the member id is empty.
async fn fence_member(state: &mut CoordinatorState, group_instance_id: String) {
    tracing::error!(
        group = %state.group_id,
        group_instance_id = %group_instance_id,
        "another consumer joined with the same group.instance.id; the member joins again on the next poll"
    );
    state.member_id.clear();
    state.rebalance_pending.send_replace(true);
    remember_lost_partitions(state).await;
    install_assignment(state, &[], false, -1, true).await;
    *state
        .poll_error
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) =
        Some(ConsumerError::FencedInstanceId(group_instance_id));
}

/// The error for a non-zero `JoinGroup` or `SyncGroup` `error_code`.
///
/// Kafka's `JoinGroupResponseHandler` and `SyncGroupResponseHandler` raise
/// `FENCED_INSTANCE_ID` as a fatal error.
fn group_response_error(error_code: i16, group_instance_id: Option<&str>) -> ConsumerError {
    if error_code == FENCED_INSTANCE_ID {
        ConsumerError::FencedInstanceId(group_instance_id.unwrap_or_default().to_string())
    } else {
        ConsumerError::Server(error_code)
    }
}

/// Kafka's `JoinGroupRequest.maybeTruncateReason`: a reason is at most 255
/// characters.
pub(crate) fn truncate_reason(reason: &str) -> &str {
    reason
        .char_indices()
        .nth(255)
        .map_or(reason, |(end, _)| &reason[..end])
}

/// Whether a member sends `LeaveGroup` for `operation`.
///
/// Kafka's `AbstractCoordinator.shouldSendLeaveGroupRequest`: a member leaves
/// for `LeaveGroup`, and a dynamic member also for `Default`. A static member
/// stays in the group by default (KIP-345), so a restart with the same
/// `group.instance.id` does not start a rebalance.
pub(crate) fn should_send_leave_group(
    member_id: &str,
    group_instance_id: Option<&str>,
    operation: GroupMembershipOperation,
) -> bool {
    !member_id.is_empty()
        && match operation {
            GroupMembershipOperation::LeaveGroup => true,
            GroupMembershipOperation::RemainInGroup => false,
            GroupMembershipOperation::Default => group_instance_id.is_none(),
        }
}

/// Best-effort `LeaveGroup` for the coordinator's *current* member id.
///
/// The task sends it once, on shutdown, with a short timeout. A broker that
/// falls back to session-timeout eviction is harmless on close, but a stalled
/// send would hang `close()`, which awaits this task. This mirrors the Java
/// client, which leaves the group on close for dynamic members. This function
/// skips a cleared id, which comes from a from-scratch rejoin that never
/// re-completed, and which the broker would not recognize anyway.
#[cfg_attr(test, mutants::skip)] // cargo-mutants: best-effort shutdown I/O, exercised by integration tests
#[tracing::instrument(
    name = "consumer.leave_group",
    level = "info",
    skip_all,
    fields(group_id = %state.group_id, member_id = %state.member_id)
)]
async fn leave_group(
    state: &CoordinatorState,
    member_id: &str,
    operation: GroupMembershipOperation,
    reason: &str,
) {
    if !should_send_leave_group(member_id, state.group_instance_id.as_deref(), operation) {
        return;
    }
    // `member_id` is populated for both the v0–v2 (top-level) and v3+
    // (`members` array) wire shapes so the negotiated version picks up
    // whichever it serializes. Routed to the coordinator broker like every
    // other group RPC; on close it's best-effort, so a stale/unknown
    // coordinator id (Disconnected) just falls back to session-timeout
    // eviction — no re-discovery is worth the wall-clock on shutdown.
    let coordinator = state
        .client
        .broker(state.coordinator_id.load(Ordering::Relaxed));
    let send = coordinator.send(build_leave_group_request(
        state.group_id.clone(),
        member_id.to_owned(),
        Some(reason),
    ));
    let _ = tokio::time::timeout(state.leave_group_timeout.to_std(), send).await;
}

/// Send one `Heartbeat` to the coordinator broker and translate the response
/// into a directive.
///
/// A cold or relocating coordinator code (14/15/16) triggers in-place
/// re-discovery, because the coordinator moved and the next tick's heartbeat or
/// rejoin must target the new broker. This function reports such a code as
/// `Transient`, so the task simply retries on the next tick.
#[tracing::instrument(
    name = "consumer.heartbeat",
    level = "debug",
    skip_all,
    fields(
        group_id = %state.group_id,
        member_id = %state.member_id,
        generation = state.generation_id,
    )
)]
pub(crate) async fn heartbeat_once(state: &CoordinatorState) -> HeartbeatOutcome {
    let result = send_heartbeat(state).await;
    heartbeat_result_outcome(state, result).await
}

/// The `Heartbeat` RPC of the current member. The future owns its inputs, so
/// a task can run it to its end.
fn send_heartbeat(
    state: &CoordinatorState,
) -> impl std::future::Future<
    Output = Result<
        krabka_protocol::owned::heartbeat_response::HeartbeatResponse,
        krabka_client_core::ClientError,
    >,
> + Send
+ 'static {
    let client = state.client.clone();
    let coordinator = state.coordinator_id.load(Ordering::Relaxed);
    let request = build_heartbeat_request(
        state.group_id.clone(),
        state.generation_id,
        state.member_id.clone(),
        state.group_instance_id.clone(),
    );
    async move { client.broker(coordinator).send(request).await }
}

/// Classify a `Heartbeat` result, and find the coordinator again after a
/// coordinator or transport error.
async fn heartbeat_result_outcome(
    state: &CoordinatorState,
    result: Result<
        krabka_protocol::owned::heartbeat_response::HeartbeatResponse,
        krabka_client_core::ClientError,
    >,
) -> HeartbeatOutcome {
    match result {
        Ok(r) => {
            let outcome = heartbeat_outcome(r.error_code);
            if matches!(outcome, HeartbeatOutcome::Transient) {
                if is_retriable_coordinator_code(r.error_code) {
                    refind_after(state, "heartbeat").await;
                } else {
                    tracing::warn!(error_code = r.error_code, "unexpected heartbeat error");
                }
            }
            outcome
        }
        Err(e) if is_retriable_transport_error(&e) => {
            // Lost the socket to the coordinator (bounced / failed over):
            // evict + re-discover so the next tick reconnects to its current
            // address.
            state
                .client
                .evict_broker(state.coordinator_id.load(Ordering::Relaxed));
            refind_after(state, "heartbeat").await;
            HeartbeatOutcome::Transient
        }
        Err(e) => {
            tracing::warn!(error = %e, "heartbeat send failed");
            report_rejoin_error(&state.poll_error, ConsumerError::Client(e));
            HeartbeatOutcome::Transient
        }
    }
}

/// Best-effort coordinator re-discovery for use off the heartbeat path, which
/// cannot surface an error.
///
/// On success this function publishes the new id into the shared
/// `coordinator_id` cell. On failure it logs and keeps the last-known id, and
/// the next tick retries.
#[cfg_attr(test, mutants::skip)] // cargo-mutants: best-effort discovery I/O, exercised by integration tests
async fn refind_after(state: &CoordinatorState, ctx: &str) {
    match find_coordinator(&state.client, &state.group_id, state.retry_policy).await {
        Ok(id) => state.coordinator_id.store(id, Ordering::Relaxed),
        Err(e) => {
            tracing::warn!(error = %e, context = ctx, "coordinator re-discovery failed");
        }
    }
}

/// Run one complete rebalance round, Join and Sync, then mutate the shared
/// `assigned` and `next_offsets` snapshots in place.
///
/// For [`RebalanceProtocol::Cooperative`] this can issue *two* Join+Sync rounds
/// back-to-back. The first installs the kept partitions only. The second, phase
/// 2, receives the freshly placed ones. See KIP-429.
#[tracing::instrument(
    name = "consumer.rejoin",
    level = "info",
    skip_all,
    fields(
        group_id = %state.group_id,
        member_id = %state.member_id,
        protocol = ?state.rebalance_protocol,
        generation = tracing::field::Empty,
        revoked = tracing::field::Empty,
        added = tracing::field::Empty,
    ),
    err
)]
async fn rejoin_group(state: &mut CoordinatorState) -> Result<HashMap<String, i32>, ConsumerError> {
    let JoinOutcome {
        owned,
        assignment: new_assignment,
        generation: new_generation,
        topic_partitions,
    } = join_and_sync(state).await?;

    let old_set: HashSet<(String, i32)> = owned.iter().cloned().collect();
    let new_set: HashSet<(String, i32)> = new_assignment.iter().cloned().collect();
    let revoked: Vec<(String, i32)> = old_set.difference(&new_set).cloned().collect();
    let added: Vec<(String, i32)> = new_set.difference(&old_set).cloned().collect();
    let span = tracing::Span::current();
    span.record("generation", new_generation);
    span.record("revoked", revoked.len());
    span.record("added", added.len());

    // The subscribed-topic partition snapshot the FINAL published assignment was
    // computed against — returned so the coordinator re-baselines against exactly
    // what it assigned (eager / pure-add use the round-1 snapshot; a cooperative
    // revoke uses phase 2's).
    let final_counts = match state.rebalance_protocol {
        RebalanceProtocol::Eager => {
            // Drop everything and reinstall in a single round. Prime the
            // added partitions' fetch offsets *before* publishing the new
            // assignment: `poll()` defaults an assigned-but-unprimed
            // partition to offset 0 (poll.rs's `unwrap_or(0)`), so a poll
            // racing between the `assigned` publish and the prime would
            // re-fetch from 0 and re-deliver already-consumed records. Prime
            // first → a partition is only visible in `assigned` once its
            // next_offset is established.
            prime_offsets(state, &added).await?;
            let _gate = assigned_callback_gate(state, &new_assignment);
            publish_assignment(state, &new_assignment, false, new_generation).await;
            {
                let mut off = state.next_offsets.lock().await;
                off.retain(|k, _| new_set.contains(k));
                // Prune the KIP-320 position sidecar in lockstep so stale
                // epoch metadata for dropped partitions doesn't accumulate.
                let mut pos = state.positions.lock().await;
                pos.retain(|k, _| new_set.contains(k));
            }
            // Kafka's eager `onJoinPrepare` revoked every partition, so all of
            // the new assignment is added.
            call_listener(
                state,
                crate::rebalance_listener::ListenerCallKind::Assigned,
                new_assignment.clone(),
            )
            .await?;
            topic_partitions
        }
        RebalanceProtocol::Cooperative => {
            if revoked.is_empty() {
                // Pure additions: merge into the existing assigned set.
                // No phase 2 needed because no member needed to revoke.
                //
                // Prime the added partitions' fetch offsets *before*
                // publishing them into `assigned`: a `poll()` racing the
                // rebalance would otherwise see an assigned-but-unprimed
                // partition and fetch it from offset 0 (poll.rs's
                // `unwrap_or(0)`), re-delivering records the previous owner
                // already committed past at revoke time.
                prime_offsets(state, &added).await?;
                let _gate = assigned_callback_gate(state, &added);
                publish_assignment(state, &new_assignment, true, new_generation).await;
                call_listener(
                    state,
                    crate::rebalance_listener::ListenerCallKind::Assigned,
                    added.clone(),
                )
                .await?;
                topic_partitions
            } else {
                // Phase 1: drop the partitions we're losing, then
                // immediately rejoin so the leader can place them on
                // whoever needs them in phase 2. Keeping kept partitions
                // active throughout is the whole point of KIP-429.
                // Kafka's cooperative `onJoinComplete` runs the revoke callback
                // before it changes the assignment, installs the whole new
                // assignment, then runs the assign callback with the added
                // partitions.
                // Kafka's `onJoinComplete` runs after the join installed the
                // new generation, so a commit in the callback carries it.
                install_generation(state, new_generation).await;
                call_listener(
                    state,
                    crate::rebalance_listener::ListenerCallKind::Revoked,
                    revoked.clone(),
                )
                .await?;
                prime_offsets(state, &added).await?;
                let gate = assigned_callback_gate(state, &added);
                publish_assignment(state, &new_assignment, true, new_generation).await;
                call_listener(
                    state,
                    crate::rebalance_listener::ListenerCallKind::Assigned,
                    added.clone(),
                )
                .await?;
                drop(gate);
                // With auto commit on, `commit_before_join` committed the
                // positions of the revoked partitions before round 1. With
                // auto commit off, the application commits. The consumer does
                // not commit a position that the application did not ask for.
                {
                    let mut off = state.next_offsets.lock().await;
                    let mut pos = state.positions.lock().await;
                    for p in &revoked {
                        off.remove(p);
                        // Prune the KIP-320 position sidecar in lockstep.
                        pos.remove(p);
                    }
                }

                // Phase 2: rejoin with the reduced owned-set. Kafka's
                // `ConsumerCoordinator.onJoinComplete` requests it with this
                // reason.
                state.rejoin_reason = "need to revoke partitions and re-join".into();
                let JoinOutcome {
                    owned: owned_after_revoke,
                    assignment: assignment2,
                    generation: gen2,
                    topic_partitions: topic_partitions2,
                } = join_and_sync(state).await?;
                let owned_after_revoke_set: HashSet<(String, i32)> =
                    owned_after_revoke.iter().cloned().collect();
                let added2: Vec<(String, i32)> = assignment2
                    .iter()
                    .filter(|p| !owned_after_revoke_set.contains(*p))
                    .cloned()
                    .collect();
                // Prime the freshly placed partitions *before* publishing the
                // phase-2 assignment, so a poll racing the rebalance can't
                // observe them in `assigned` without a primed next_offset and
                // fetch from 0 (poll.rs). That primed value is the offset the
                // revoking member committed at revoke time; fetching from 0
                // instead would re-deliver the records it already consumed.
                let assignment2_set: HashSet<(String, i32)> = assignment2.iter().cloned().collect();
                let revoked2: Vec<(String, i32)> = owned_after_revoke
                    .iter()
                    .filter(|p| !assignment2_set.contains(*p))
                    .cloned()
                    .collect();
                if !revoked2.is_empty() {
                    install_generation(state, gen2).await;
                    call_listener(
                        state,
                        crate::rebalance_listener::ListenerCallKind::Revoked,
                        revoked2,
                    )
                    .await?;
                }
                prime_offsets(state, &added2).await?;
                let _gate = assigned_callback_gate(state, &added2);
                publish_assignment(state, &assignment2, true, gen2).await;
                call_listener(
                    state,
                    crate::rebalance_listener::ListenerCallKind::Assigned,
                    added2,
                )
                .await?;
                topic_partitions2
            }
        }
    };
    Ok(final_counts)
}

/// The `JoinGroup` protocols of a member: one subscription per assignor, in
/// the configured order.
///
/// This is Kafka's `ConsumerCoordinator.metadata`. Each subscription carries
/// the owned partitions, the generation, the rack and the `userData` of its
/// assignor. `last_assignment` is the assignment of the last completed join,
/// which the `sticky` user data carries.
pub(crate) fn join_protocols(
    assignors: &[Assignor],
    topics: &[String],
    owned: &[(String, i32)],
    generation_id: i32,
    rack_id: Option<&str>,
    last_assignment: Option<&[(String, i32)]>,
) -> Vec<(String, Bytes)> {
    assignors
        .iter()
        .map(|assignor| {
            (
                assignor.protocol_name().to_owned(),
                encode_subscription(
                    topics,
                    owned,
                    generation_id,
                    rack_id,
                    crate::assignor::subscription_user_data(
                        *assignor,
                        last_assignment,
                        generation_id,
                    ),
                ),
            )
        })
        .collect()
}

/// The fields of the `JoinGroup` requests of one join. Only the member id
/// differs between the requests of the `MEMBER_ID_REQUIRED` handshake.
#[derive(Clone, Debug)]
pub(crate) struct JoinRequestFields {
    pub group_id: String,
    pub group_instance_id: Option<String>,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    /// One `(name, subscription)` per assignor, in the configured order.
    pub protocols: Vec<(String, Bytes)>,
    /// Kafka's `AbstractCoordinator.rejoinReason`.
    pub reason: String,
}

impl JoinRequestFields {
    pub(crate) fn request(&self, member_id: String) -> JoinGroupRequest {
        JoinGroupRequest {
            group_id: self.group_id.clone(),
            protocol_type: "consumer".into(),
            member_id,
            group_instance_id: self.group_instance_id.clone(),
            session_timeout_ms: self.session_timeout_ms,
            rebalance_timeout_ms: self.rebalance_timeout_ms,
            protocols: self
                .protocols
                .iter()
                .map(|(name, metadata)| JoinGroupRequestProtocol {
                    name: name.clone(),
                    metadata: metadata.clone(),
                    ..Default::default()
                })
                .collect(),
            // KIP-800. Version 8 and later carry the reason.
            reason: Some(truncate_reason(&self.reason).to_owned()),
            ..Default::default()
        }
    }
}

pub(crate) fn build_sync_group_assignment(
    member_id: String,
    partitions: &[(String, i32)],
) -> SyncGroupRequestAssignment {
    SyncGroupRequestAssignment {
        member_id,
        assignment: encode_assignment(partitions),
        ..Default::default()
    }
}

fn build_sync_group_request(
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
    chosen_protocol: String,
    assignments: Vec<SyncGroupRequestAssignment>,
) -> SyncGroupRequest {
    SyncGroupRequest {
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        protocol_type: Some("consumer".into()),
        protocol_name: Some(chosen_protocol),
        assignments,
        ..Default::default()
    }
}

/// The auto commit before a `JoinGroup`: Kafka's
/// `ConsumerCoordinator.onJoinPrepare` with `enable.auto.commit` on.
///
/// Kafka commits the consumed positions of every owned partition before each
/// `JoinGroup`, for the eager and the cooperative protocol. It retries a
/// retriable failure until the rebalance timeout expires. After a non-retriable
/// failure or at the timeout, it writes an error to the log and joins the
/// group. This function commits the positions of the latest `poll`; see
/// [`crate::commit::AutoCommit`]. It runs once per join: a retry of a failed
/// `JoinGroup` does not commit again.
///
/// Kafka's heartbeat thread keeps the session alive while `onJoinPrepare`
/// waits. This function sends a heartbeat each heartbeat interval while the
/// commit runs. When a heartbeat says that the coordinator does not know the
/// member, the function stops the commit, forgets the member and joins from
/// scratch.
///
/// # Errors
///
/// Returns `FencedInstanceId` when a heartbeat says that another consumer
/// joined with the same `group.instance.id`.
#[tracing::instrument(
    name = "consumer.commit_before_join",
    level = "debug",
    skip_all,
    fields(group_id = %state.group_id, generation = state.generation_id)
)]
async fn commit_before_join(state: &mut CoordinatorState) -> Result<(), ConsumerError> {
    let Some(auto_commit) = state.auto_commit.clone() else {
        return Ok(());
    };
    let heartbeat = {
        let state = &*state;
        let commit_done = CancellationToken::new();
        let stop_commit = CancellationToken::new();
        let commit = async {
            tokio::select! {
                () = commit_consumed_before_join(state, &auto_commit) => {}
                () = stop_commit.cancelled() => {}
            }
            commit_done.cancel();
        };
        let heartbeats = async {
            let outcome = heartbeat_during_join_prepare(state, &commit_done).await;
            stop_commit.cancel();
            outcome
        };
        tokio::join!(commit, heartbeats).1
    };
    match heartbeat {
        Some(HeartbeatOutcome::Fenced) => Err(ConsumerError::FencedInstanceId(
            state.group_instance_id.clone().unwrap_or_default(),
        )),
        Some(HeartbeatOutcome::RejoinFromScratch) => {
            tracing::warn!(
                "the coordinator does not know the member; joining the group from scratch"
            );
            forget_member(state).await;
            state.rejoin_reason = heartbeat_rejoin_reason(UNKNOWN_MEMBER_ID);
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Send a heartbeat each heartbeat interval until `commit_done` is cancelled,
/// or until a heartbeat says that the member is fenced or unknown. Return that
/// outcome.
///
/// The function returns as soon as `commit_done` is cancelled, also while a
/// heartbeat is in flight, so the heartbeat does not delay the `JoinGroup`, as
/// in Kafka, where the heartbeat thread and the `JoinGroup` do not wait for
/// each other. A task runs the `Heartbeat` RPC, so the request runs to its end
/// on the connection.
async fn heartbeat_during_join_prepare(
    state: &CoordinatorState,
    commit_done: &CancellationToken,
) -> Option<HeartbeatOutcome> {
    let interval = state.heartbeat_interval.to_std();
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            () = commit_done.cancelled() => return None,
            _ = ticker.tick() => {}
        }
        let mut request = tokio::spawn(send_heartbeat(state));
        let result = tokio::select! {
            biased;
            () = commit_done.cancelled() => return None,
            result = &mut request => result,
        };
        let Ok(result) = result else {
            // The heartbeat task panicked. The next tick sends a new heartbeat.
            continue;
        };
        let outcome = tokio::select! {
            biased;
            () = commit_done.cancelled() => return None,
            outcome = heartbeat_result_outcome(state, result) => outcome,
        };
        match outcome {
            HeartbeatOutcome::Ok
            | HeartbeatOutcome::NeedRejoin(_)
            | HeartbeatOutcome::Transient => {}
            outcome @ (HeartbeatOutcome::RejoinFromScratch | HeartbeatOutcome::Fenced) => {
                return Some(outcome);
            }
        }
    }
}

/// Commit the positions of the latest `poll` before a `JoinGroup`.
///
/// The commit takes `commit_serialization` before it reads the positions, so
/// it cannot reach the coordinator after a newer commit of the consumer. The
/// rebalance timeout bounds the wait for that lock, each `OffsetCommit` and
/// each coordinator lookup.
async fn commit_consumed_before_join(
    state: &CoordinatorState,
    auto_commit: &crate::commit::AutoCommit,
) {
    let deadline = tokio::time::Instant::now() + state.max_poll_interval.to_std();
    let Some(_turn) = commit_turn(&state.commit_serialization, auto_commit, deadline).await else {
        tracing::error!(
            "auto commit before the rebalance timed out waiting for another commit; joining the group"
        );
        return;
    };
    loop {
        let identity = state.commit_identity.lock().await.clone();
        let offsets = auto_commit.polled_offsets(&identity.ownership_ids).await;
        if offsets.is_empty() {
            return;
        }
        let coordinator = state.coordinator_id.load(Ordering::Relaxed);
        let broker = state.client.broker(coordinator);
        let send = broker.send(build_commit_request(
            state.group_id.clone(),
            identity.generation,
            identity.member_id,
            state.group_instance_id.clone(),
            build_commit_topics(crate::commit::position_commits(offsets)),
        ));
        let Ok(result) = tokio::time::timeout_at(deadline, send).await else {
            tracing::error!("auto commit before the rebalance timed out; joining the group");
            return;
        };
        let result = result.map_err(ConsumerError::from);
        match auto_commit_outcome(&result) {
            AutoCommitOutcome::Committed => return,
            AutoCommitOutcome::Failed => {
                tracing::error!(
                    ?result,
                    "auto commit before the rebalance failed; joining the group"
                );
                return;
            }
            AutoCommitOutcome::Retriable => {
                let moved = match &result {
                    Ok(response) => names_moved_coordinator(response),
                    Err(_) => true,
                };
                if moved {
                    state.client.evict_broker(coordinator);
                    if tokio::time::timeout_at(deadline, refind_after(state, "auto commit"))
                        .await
                        .is_err()
                    {
                        tracing::error!(
                            ?result,
                            "auto commit before the rebalance timed out; joining the group"
                        );
                        return;
                    }
                }
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    tracing::error!(
                        ?result,
                        "auto commit before the rebalance timed out; joining the group"
                    );
                    return;
                }
                tokio::time::sleep(state.retry_policy.initial_backoff.min(deadline - now)).await;
            }
        }
    }
}

/// Wait for the turn of the commit before a `JoinGroup` in the commit order.
///
/// The function returns `Some(Some(guard))` when the commit holds
/// `commit_serialization`. It returns `Some(None)` when the commit that holds
/// the lock waits for this rebalance. That commit sends nothing before the
/// rebalance publishes the next assignment, so the commit before the
/// `JoinGroup` goes first. The function returns `None` at `deadline`.
async fn commit_turn(
    commit_serialization: &Arc<Mutex<()>>,
    auto_commit: &crate::commit::AutoCommit,
    deadline: tokio::time::Instant,
) -> Option<Option<OwnedMutexGuard<()>>> {
    let mut parked_commits = auto_commit.parked_commits();
    tokio::select! {
        biased;
        guard = Arc::clone(commit_serialization).lock_owned() => Some(Some(guard)),
        parked = parked_commits.wait_for(|parked| *parked) => parked.is_ok().then_some(None),
        () = tokio::time::sleep_until(deadline) => None,
    }
}

struct JoinOutcome {
    /// The partitions that the member owned when it sent the `JoinGroup`,
    /// after the join preparation.
    owned: Vec<(String, i32)>,
    assignment: Vec<(String, i32)>,
    generation: i32,
    topic_partitions: HashMap<String, i32>,
}

async fn perform_join(
    state: &mut CoordinatorState,
    owned: &[(String, i32)],
) -> Result<JoinGroupResponse, ConsumerError> {
    // Kafka's `ConsumerCoordinator.metadata` names the subscription at the
    // join, and `joinedSubscription` keeps it.
    let topics = state.subscription.borrow().topics.clone();
    state.joined_topics.clone_from(&topics);
    // Truncating, not rounding: these are `JoinGroupRequest` `int32`
    // milliseconds the coordinator range-checks, and `Duration::as_millis`
    // truncated here before the conversion.
    let mut fields = JoinRequestFields {
        group_id: state.group_id.clone(),
        group_instance_id: state.group_instance_id.clone(),
        session_timeout_ms: crate::consumer::protocol_millis_i32(state.session_timeout),
        rebalance_timeout_ms: crate::consumer::protocol_millis_i32(state.max_poll_interval),
        // Kafka's eager `onJoinPrepare` revokes every partition before the
        // join, so an eager subscription owns nothing. The `sticky` user data
        // still carries the last assignment.
        protocols: join_protocols(
            &state.assignors,
            &topics,
            match state.rebalance_protocol {
                RebalanceProtocol::Eager => &[],
                RebalanceProtocol::Cooperative => owned,
            },
            state.generation_id,
            state.client_rack.as_deref(),
            (state.generation_id >= 0).then_some(owned),
        ),
        reason: state.rejoin_reason.clone(),
    };

    // Pull the pieces every retry closure needs out of `&mut state` into locals
    // so `with_coordinator_refind` can borrow the shared coordinator cell
    // alongside the closures' borrows without aliasing `state`. `Client` and
    // the `Arc<AtomicI32>` are both cheap to clone; the atomic is the same cell
    // `state.coordinator_id` points at, so re-discovery updates are visible to
    // the rest of the task and the parent `Consumer` immediately.
    let client = state.client.clone();
    let group_id = state.group_id.clone();
    let group_instance_id = state.group_instance_id.clone();
    let coordinator_id = Arc::clone(&state.coordinator_id);

    // First join: if we have no member_id, expect MEMBER_ID_REQUIRED (79) and
    // capture the broker-assigned id, then issue a second join. Retry a cold or
    // relocating coordinator (14/15/16) with backoff, re-discovering the
    // coordinator before each retry so a moved coordinator is chased rather
    // than re-hit on the stale broker.
    let r1 = with_coordinator_refind(
        &client,
        &group_id,
        &coordinator_id,
        state.retry_policy,
        |r: &JoinGroupResponse| r.error_code,
        || {
            let request = fields.request(state.member_id.clone());
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
    let join_resp = if r1.error_code == 0 {
        r1
    } else if r1.error_code == 79 {
        let assigned_id = r1.member_id.clone();
        if assigned_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed(
                "broker did not assign a member_id".into(),
            ));
        }
        state.member_id.clone_from(&assigned_id);
        // Kafka's `JoinGroupResponseHandler` requests the rejoin with this
        // reason.
        state.rejoin_reason = format!("need to re-join with the given member-id: {assigned_id}");
        fields.reason.clone_from(&state.rejoin_reason);
        let r2 = with_coordinator_refind(
            &client,
            &group_id,
            &coordinator_id,
            state.retry_policy,
            |r: &JoinGroupResponse| r.error_code,
            || {
                let request = fields.request(assigned_id.clone());
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
            return Err(group_response_error(
                r2.error_code,
                group_instance_id.as_deref(),
            ));
        }
        r2
    } else {
        return Err(group_response_error(
            r1.error_code,
            group_instance_id.as_deref(),
        ));
    };

    Ok(join_resp)
}

/// Issue `JoinGroup`, assign as leader if this member won the election, then
/// issue `SyncGroup`.
///
/// This function handles the `MEMBER_ID_REQUIRED` two-step when `member_id` is
/// empty. It returns `(assignment, generation_id, protocol_name)`.
// Sequential join/sync state machine; splitting fragments the linear
// MEMBER_ID_REQUIRED → leader-assign → SyncGroup flow.
#[tracing::instrument(
    name = "consumer.join_and_sync",
    level = "info",
    skip_all,
    fields(
        group_id = %state.group_id,
        member_id = tracing::field::Empty,
        generation = tracing::field::Empty,
        is_leader = tracing::field::Empty,
        protocol = tracing::field::Empty,
        assigned_partitions = tracing::field::Empty,
    ),
    err
)]
async fn join_and_sync(state: &mut CoordinatorState) -> Result<JoinOutcome, ConsumerError> {
    join_prepare(state).await?;
    // The preparation can reset the member and clear its partitions, as
    // Kafka's `onJoinPrepare` does. The subscription names what is left.
    let owned = state.assigned.lock().await.clone();
    let join_resp = perform_join(state, &owned).await?;
    // The broker may have refreshed our member_id on this join too.
    if !join_resp.member_id.is_empty() {
        state.member_id.clone_from(&join_resp.member_id);
    }
    let chosen_protocol = join_resp.protocol_name.clone().unwrap_or_default();
    let generation_id = join_resp.generation_id;

    // Leader: resolve partition counts via Metadata and run the assignor.
    let is_leader = join_resp.leader == state.member_id;
    {
        let span = tracing::Span::current();
        span.record("member_id", state.member_id.as_str());
        span.record("generation", generation_id);
        span.record("is_leader", is_leader);
        span.record("protocol", chosen_protocol.as_str());
    }
    // Subscribed-topic partition counts the assignment is computed against,
    // captured from the SAME Metadata the leader runs the assignor on (empty for
    // a non-leader). Returned so the coordinator's rejoin baseline tracks exactly
    // what was assigned rather than a divergent later fetch.
    let leader = compute_leader_assignment(state, &join_resp, is_leader).await?;
    let my_assignment =
        sync_assignment(state, generation_id, &chosen_protocol, leader.assignments).await?;
    tracing::Span::current().record("assigned_partitions", my_assignment.len());
    state.join_prepared = false;
    // Kafka's `SyncGroupResponseHandler` clears the reason after a sync.
    state.rejoin_reason.clear();
    if let Some(auto_commit) = &state.auto_commit {
        auto_commit.restart_interval().await;
    }
    Ok(JoinOutcome {
        owned,
        assignment: my_assignment,
        generation: generation_id,
        topic_partitions: leader.topic_partitions,
    })
}

/// The `SyncGroup` assignments of the group leader.
///
/// Kafka's `ConsumerCoordinator.onLeaderElected` looks up the assignor that the
/// coordinator selected, decodes each member subscription, and assigns the
/// partitions of every topic that a member subscribes to.
///
/// # Errors
///
/// Returns `IllegalState` when the coordinator selected a protocol that this
/// member did not offer.
pub(crate) fn leader_assignments(
    assignors: &[Assignor],
    response: &JoinGroupResponse,
    metadata: &krabka_protocol::owned::metadata_response::MetadataResponse,
) -> Result<Vec<SyncGroupRequestAssignment>, ConsumerError> {
    let name = response.protocol_name.as_deref().unwrap_or_default();
    let assignor = Assignor::by_protocol_name(assignors, name).ok_or_else(|| {
        ConsumerError::IllegalState(format!(
            "Coordinator selected invalid assignment protocol: {name}"
        ))
    })?;
    let members: Vec<crate::assignor::GroupMember> = response
        .members
        .iter()
        .map(|member| crate::assignor::GroupMember {
            key: crate::assignor::MemberKey {
                member_id: member.member_id.clone(),
                group_instance_id: member.group_instance_id.clone(),
            },
            subscription: decode_subscription(&member.metadata),
        })
        .collect();
    let all_topics: HashSet<&String> = members
        .iter()
        .flat_map(|member| &member.subscription.topics)
        .collect();
    let topic_partitions: HashMap<String, i32> = metadata
        .topics
        .iter()
        .filter_map(|topic| {
            let name = topic.name.as_ref()?;
            all_topics.contains(name).then(|| {
                (
                    name.clone(),
                    i32::try_from(topic.partitions.len()).unwrap_or(i32::MAX),
                )
            })
        })
        .collect();
    let mut assignments: Vec<_> = crate::assignor::assign(assignor, &members, &topic_partitions)
        .into_iter()
        .collect();
    assignments.sort();
    Ok(assignments
        .into_iter()
        .map(|(member, partitions)| build_sync_group_assignment(member, &partitions))
        .collect())
}

struct LeaderAssignment {
    assignments: Vec<SyncGroupRequestAssignment>,
    topic_partitions: HashMap<String, i32>,
}

async fn compute_leader_assignment(
    state: &CoordinatorState,
    response: &JoinGroupResponse,
    is_leader: bool,
) -> Result<LeaderAssignment, ConsumerError> {
    if !is_leader {
        return Ok(LeaderAssignment {
            assignments: Vec::new(),
            topic_partitions: HashMap::new(),
        });
    }
    let metadata = state.client.refresh_metadata().await?;
    let mut topic_partitions = HashMap::new();
    let mut resolved_ids = HashMap::new();
    for topic in &metadata.topics {
        let Some(name) = &topic.name else { continue };
        if state.subscription.borrow().contains(name) {
            topic_partitions.insert(
                name.clone(),
                i32::try_from(topic.partitions.len()).unwrap_or(i32::MAX),
            );
            resolved_ids.insert(name.clone(), topic.topic_id);
        }
    }
    state.topic_ids.lock().await.extend(resolved_ids);
    let assignments = leader_assignments(&state.assignors, response, &metadata)?;
    Ok(LeaderAssignment {
        assignments,
        topic_partitions,
    })
}

async fn sync_assignment(
    state: &CoordinatorState,
    generation_id: i32,
    protocol: &str,
    assignments: Vec<SyncGroupRequestAssignment>,
) -> Result<Vec<(String, i32)>, ConsumerError> {
    let response = with_coordinator_refind(
        &state.client,
        &state.group_id,
        &state.coordinator_id,
        state.retry_policy,
        |response: &SyncGroupResponse| response.error_code,
        || {
            let request = build_sync_group_request(
                state.group_id.clone(),
                generation_id,
                state.member_id.clone(),
                state.group_instance_id.clone(),
                protocol.to_string(),
                assignments.clone(),
            );
            let target = state.coordinator_id.load(Ordering::Relaxed);
            async move {
                state
                    .client
                    .broker(target)
                    .send(request)
                    .await
                    .map_err(ConsumerError::from)
            }
        },
    )
    .await?;
    if response.error_code != 0 {
        return Err(group_response_error(
            response.error_code,
            state.group_instance_id.as_deref(),
        ));
    }
    Ok(decode_assignment(&response.assignment))
}

/// Populate `next_offsets` for newly added partitions with a batch fetch of the
/// committed offsets.
///
/// When no commit exists, this function falls back to `auto.offset.reset`
/// semantics. It mirrors the initial prime in `consumer.rs::start` step 5.
#[tracing::instrument(
    name = "consumer.prime_offsets",
    level = "debug",
    skip_all,
    fields(group_id = %state.group_id, partitions = partitions.len()),
    err
)]
async fn prime_offsets(
    state: &CoordinatorState,
    partitions: &[(String, i32)],
) -> Result<(), ConsumerError> {
    if partitions.is_empty() {
        return Ok(());
    }
    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    for (t, p) in partitions {
        by_topic.entry(t.clone()).or_default().push(*p);
    }
    // OffsetFetch is a coordinator RPC. `prime_offsets` only runs right after a
    // successful join/sync (which just discovered/refreshed `coordinator_id`),
    // so the id is fresh; route straight to it.
    let of = send_offset_fetch(
        &state.client,
        &state.group_id,
        &state.coordinator_id,
        &build_offset_fetch(&state.group_id, &by_topic),
        state.retry_policy,
    )
    .await?;

    let mut offsets = state.next_offsets.lock().await;
    let mut positions = state.positions.lock().await;
    let mut seen: HashSet<(String, i32)> = HashSet::new();
    for (name, partition_index, committed, committed_epoch) in parse_offset_fetch(&of) {
        let starting = starting_offset(committed, state.auto_offset_reset);
        let key = (name, partition_index);
        seen.insert(key.clone());
        offsets.insert(key.clone(), starting);
        // Wrap the committed leader epoch (raw wire `int32` from OffsetFetch) at
        // the decode boundary.
        positions.entry(key).or_default().offset_epoch = krabka_ids::LeaderEpoch(committed_epoch);
    }
    // The broker may omit partitions that have no commit record at all;
    // ensure every requested partition has an entry so poll() can find it.
    for tp in partitions {
        if should_prime_missing_partition(seen.contains(tp)) {
            let starting = reset_starting_offset(state.auto_offset_reset);
            offsets.insert(tp.clone(), starting);
            positions.entry(tp.clone()).or_default();
        }
    }
    Ok(())
}

/// Send an `OffsetFetch` to the coordinator and act on its error codes as
/// Apache Kafka's consumer does.
///
/// This follows Kafka's `CommitRequestManager.fetchOffsets`. The function
/// classifies each response with [`classify_offset_fetch`]:
///
/// - A partition that answers 3 or 100 makes the function send the request
///   again until `retry.timeout` elapses. After the deadline the function
///   returns the last response. Its errored partitions carry committed offset
///   -1, so the caller starts them from the reset policy, as Kafka's
///   `OffsetFetchResult.toOffsetMapWithNulls` does.
/// - A retriable group code, or 88 on a partition, makes the function send the
///   request again until the deadline. 15 and 16 also make it find the
///   coordinator again first. After the deadline the function returns
///   [`ConsumerError::Server`] with the code.
/// - Other codes fail at once with [`ConsumerError::GroupAuthorizationFailed`],
///   [`ConsumerError::TopicAuthorizationFailed`] or
///   [`ConsumerError::OffsetFetchFailed`].
///
/// The request names each topic, so the version is v9 or lower. See
/// [`TopicNameOffsetFetch`].
pub(crate) async fn send_offset_fetch(
    client: &Client,
    group_id: &str,
    coordinator_id: &AtomicI32,
    request: &TopicNameOffsetFetch,
    retry: CoordinatorRetryPolicy,
) -> Result<OffsetFetchResponse, ConsumerError> {
    let start = tokio::time::Instant::now();
    let mut backoff = retry.initial_backoff;
    loop {
        let response = client
            .broker(coordinator_id.load(Ordering::Relaxed))
            .send(request.clone())
            .await?;
        let deadline_elapsed = retry_deadline_elapsed(start, retry.timeout);
        match classify_offset_fetch(&response) {
            OffsetFetchAction::Complete => return Ok(response),
            OffsetFetchAction::RetryUnknownTopic(code) => {
                if deadline_elapsed {
                    tracing::warn!(
                        error_code = code,
                        "offset fetch still names a topic the coordinator does not know; \
                         using the reset policy for those partitions"
                    );
                    return Ok(response);
                }
                tracing::debug!(
                    error_code = code,
                    "offset fetch names an unknown topic; retrying"
                );
            }
            OffsetFetchAction::Retry(code) => {
                if deadline_elapsed {
                    return Err(ConsumerError::Server(code));
                }
                tracing::debug!(error_code = code, "offset fetch failed; retrying");
            }
            OffsetFetchAction::FindCoordinator(code) => {
                if deadline_elapsed {
                    return Err(ConsumerError::Server(code));
                }
                tracing::debug!(
                    error_code = code,
                    "offset fetch reached no coordinator; finding the coordinator again"
                );
                match find_coordinator(client, group_id, retry).await {
                    Ok(id) => coordinator_id.store(id, Ordering::Relaxed),
                    Err(error) => tracing::warn!(
                        error = %error,
                        "coordinator re-discovery failed; retrying with last-known id"
                    ),
                }
            }
            OffsetFetchAction::GroupAuthorizationFailed => {
                return Err(ConsumerError::GroupAuthorizationFailed(
                    group_id.to_string(),
                ));
            }
            OffsetFetchAction::TopicAuthorizationFailed(topics) => {
                return Err(ConsumerError::TopicAuthorizationFailed(topics));
            }
            OffsetFetchAction::Fatal(code) => return Err(ConsumerError::OffsetFetchFailed(code)),
        }
        tokio::time::sleep(backoff).await;
        backoff = next_backoff(backoff, retry.max_backoff);
    }
}

fn should_prime_missing_partition(seen: bool) -> bool {
    !seen
}

#[cfg(test)]
mod retry_tests {
    use std::{
        collections::BTreeSet,
        io,
        net::SocketAddr,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use assert2::check;
    use bytes::Buf;
    use krabka_client_core::MockBroker;
    use krabka_protocol::{
        Decode, Encode, UnknownTaggedFields,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            find_coordinator_request, heartbeat_request,
            heartbeat_response::HeartbeatResponse,
            join_group_request, leave_group_request, metadata_request,
            metadata_response::{MetadataResponse, MetadataResponseBroker},
            offset_fetch_request::{
                self, OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
                OffsetFetchRequestTopics,
            },
            offset_fetch_response::{
                OffsetFetchResponseGroup, OffsetFetchResponsePartition,
                OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
            },
            sync_group_request,
        },
    };
    use krabka_units::{millis, minutes, secs};

    use super::*;

    const ORDERS: &str = "orders";

    const PAYMENTS: &str = "payments";

    /// An `ApiVersions` response that advertises `OffsetFetch` in
    /// `offset_fetch_range`.
    fn api_versions_for_offset_fetch(offset_fetch_range: (i16, i16)) -> Vec<u8> {
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
                    api_key: offset_fetch_request::API_KEY,
                    min_version: offset_fetch_range.0,
                    max_version: offset_fetch_range.1,
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
        let mut buffer = bytes::BytesMut::new();
        response
            .encode(&mut buffer, 0)
            .expect("encode API versions");
        buffer.to_vec()
    }

    /// One scripted `OffsetFetch` answer: the group error code and, for
    /// partition 0 of each topic, the partition error code and the committed
    /// offset.
    struct Answer {
        group_error: i16,
        rows: Vec<(&'static str, i16, i64)>,
    }

    fn answer(group_error: i16, rows: &[(&'static str, i16, i64)]) -> Answer {
        Answer {
            group_error,
            rows: rows.to_vec(),
        }
    }

    /// The `OffsetFetch` response body for `answer` at `version`, behind the
    /// flexible response header's empty tagged fields from v6. v2 to v7 put the
    /// group error code and the topics at the top level. v8 and v9 put them in
    /// the group entry.
    fn offset_fetch_response(answer: &Answer, version: i16) -> Vec<u8> {
        let partition = |error_code: i16, committed_offset: i64| OffsetFetchResponsePartitions {
            partition_index: 0,
            committed_offset,
            committed_leader_epoch: -1,
            error_code,
            ..Default::default()
        };
        let response = if version < 8 {
            OffsetFetchResponse {
                error_code: answer.group_error,
                topics: answer
                    .rows
                    .iter()
                    .map(
                        |(topic, error_code, committed_offset)| OffsetFetchResponseTopic {
                            name: (*topic).into(),
                            partitions: vec![OffsetFetchResponsePartition {
                                partition_index: 0,
                                committed_offset: *committed_offset,
                                committed_leader_epoch: -1,
                                error_code: *error_code,
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                    )
                    .collect(),
                ..Default::default()
            }
        } else {
            OffsetFetchResponse {
                groups: vec![OffsetFetchResponseGroup {
                    group_id: "group-a".into(),
                    error_code: answer.group_error,
                    topics: answer
                        .rows
                        .iter()
                        .map(
                            |(topic, error_code, committed_offset)| OffsetFetchResponseTopics {
                                name: (*topic).into(),
                                partitions: vec![partition(*error_code, *committed_offset)],
                                ..Default::default()
                            },
                        )
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }
        };
        let mut buffer = bytes::BytesMut::new();
        if version >= offset_fetch_request::FLEXIBLE_MIN {
            buffer.extend_from_slice(&[0]);
        }
        response
            .encode(&mut buffer, version)
            .expect("encode offset fetch");
        buffer.to_vec()
    }

    /// The result of `send_offset_fetch` in a form that tests can compare.
    #[derive(Debug, PartialEq, Eq)]
    enum Outcome {
        Offsets(Vec<(String, i32, i64, i32)>),
        Server(i16),
        GroupAuthorizationFailed(String),
        TopicAuthorizationFailed(BTreeSet<String>),
        OffsetFetchFailed(i16),
        /// The coordinator supports no `OffsetFetch` version from the client's
        /// minimum to 9. The fields are the broker range and the client range.
        IncompatibleVersion((i16, i16), (i16, i16)),
    }

    fn outcome(result: Result<OffsetFetchResponse, ConsumerError>, name: &str) -> Outcome {
        match result {
            Ok(response) => Outcome::Offsets(parse_offset_fetch(&response)),
            Err(ConsumerError::Server(code)) => Outcome::Server(code),
            Err(ConsumerError::GroupAuthorizationFailed(group)) => {
                Outcome::GroupAuthorizationFailed(group)
            }
            Err(ConsumerError::TopicAuthorizationFailed(topics)) => {
                Outcome::TopicAuthorizationFailed(topics)
            }
            Err(ConsumerError::OffsetFetchFailed(code)) => Outcome::OffsetFetchFailed(code),
            Err(ConsumerError::Client(krabka_client_core::ClientError::IncompatibleVersion {
                api_key: offset_fetch_request::API_KEY,
                broker_min,
                broker_max,
                client_min,
                client_max,
            })) => Outcome::IncompatibleVersion((broker_min, broker_max), (client_min, client_max)),
            Err(error) => panic!("case {name}: unexpected error {error:?}"),
        }
    }

    /// Kafka's `CommitRequestManager` and `OffsetFetchRequestState` map each
    /// `OffsetFetch` group and partition error code to an action: retry, find
    /// the coordinator again and retry, use partial results at the deadline, or
    /// fail. The mock coordinator answers each attempt from a script and
    /// repeats its last answer.
    #[tokio::test]
    async fn offset_fetch_error_codes_map_to_kafka_consumer_actions() {
        for (name, answers, timeout, expected) in offset_fetch_cases() {
            let (outcome, requests, find_coordinators) =
                run_offset_fetch((1, 10), &["orders", "payments"], answers, timeout, name).await;
            check!(
                (outcome, requests.len(), find_coordinators) == expected,
                "case {name}"
            );
        }
    }

    /// Apache Kafka's classic `ConsumerCoordinator.sendOffsetFetchRequest`
    /// builds `OffsetFetch` with `OffsetFetchRequest.Builder.forTopicNames`,
    /// which caps the version at 9, so each topic has its name. Kafka's
    /// consumers also set `requireStable`
    /// (`CommitRequestManager.OffsetFetchRequestState.toUnsentRequest`,
    /// `ConsumerCoordinator.sendOffsetFetchRequest`). The field exists from
    /// v7. Below v7 `OffsetFetchRequest.Builder.throwIfStableOffsetsUnsupported`
    /// drops it. A coordinator with a pending transactional offset commit
    /// answers `UNSTABLE_OFFSET_COMMIT` (88), and the consumer asks again.
    #[tokio::test]
    async fn offset_fetch_names_topics_at_v9_or_lower_and_requires_stable_offsets() {
        let by_name_before_v8 = |require_stable| OffsetFetchRequest {
            group_id: "group-a".into(),
            topics: Some(vec![OffsetFetchRequestTopic {
                name: "orders".into(),
                partition_indexes: vec![0],
                ..Default::default()
            }]),
            require_stable,
            ..Default::default()
        };
        let grouped = || OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: "group-a".into(),
                member_id: None,
                member_epoch: -1,
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: "orders".into(),
                    topic_id: WireUuid::ZERO,
                    partition_indexes: vec![0],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            require_stable: true,
            ..Default::default()
        };
        let committed = || Outcome::Offsets(vec![("orders".to_string(), 0, 42, -1)]);
        let ok = || answer(0, &[(ORDERS, 0, 42)]);
        for (name, offset_fetch_range, answers, expected) in [
            (
                "v6 has no require_stable field",
                (1, 6),
                vec![ok()],
                (committed(), vec![(6, by_name_before_v8(false))]),
            ),
            (
                "v7 requires stable offsets",
                (1, 7),
                vec![ok()],
                (committed(), vec![(7, by_name_before_v8(true))]),
            ),
            (
                "v9 names the topic",
                (1, 9),
                vec![ok()],
                (committed(), vec![(9, grouped())]),
            ),
            (
                "a coordinator with v10 gets v9",
                (1, 10),
                vec![ok()],
                (committed(), vec![(9, grouped())]),
            ),
            (
                "a coordinator with only v10 gets no request",
                (10, 10),
                vec![ok()],
                (Outcome::IncompatibleVersion((10, 10), (1, 9)), Vec::new()),
            ),
            (
                "a pending transaction answers 88 and the consumer asks again",
                (1, 10),
                vec![answer(0, &[(ORDERS, 88, -1)]), ok()],
                (committed(), vec![(9, grouped()), (9, grouped())]),
            ),
        ] {
            let (outcome, requests, find_coordinators) = run_offset_fetch(
                offset_fetch_range,
                &["orders"],
                answers,
                Duration::from_secs(5),
                name,
            )
            .await;
            check!(
                ((outcome, requests), find_coordinators) == (expected, 0),
                "case {name}"
            );
        }
    }

    /// One `send_offset_fetch` case: its name, the scripted answers, the retry
    /// timeout, and the expected outcome with the `OffsetFetch` and
    /// `FindCoordinator` request counts.
    type OffsetFetchCase = (&'static str, Vec<Answer>, Duration, (Outcome, usize, usize));

    fn offset_fetch_cases() -> Vec<OffsetFetchCase> {
        const LONG: Duration = Duration::from_secs(5);
        const NOW: Duration = Duration::ZERO;
        let committed = |orders: i64, payments: i64| {
            Outcome::Offsets(vec![
                ("orders".to_string(), 0, orders, -1),
                ("payments".to_string(), 0, payments, -1),
            ])
        };
        let ok = || answer(0, &[(ORDERS, 0, 42), (PAYMENTS, 0, 7)]);
        let partition_error = |orders: i16, payments: i16| {
            answer(0, &[(ORDERS, orders, -1), (PAYMENTS, payments, -1)])
        };
        let topics = |names: &[&str]| {
            Outcome::TopicAuthorizationFailed(names.iter().map(|n| (*n).to_string()).collect())
        };
        vec![
            ("no error", vec![ok()], LONG, (committed(42, 7), 1, 0)),
            // Group codes.
            (
                "coordinator load in progress retries on the same coordinator",
                vec![answer(14, &[]), ok()],
                LONG,
                (committed(42, 7), 2, 0),
            ),
            (
                "coordinator not available finds the coordinator and retries",
                vec![answer(15, &[]), ok()],
                LONG,
                (committed(42, 7), 2, 1),
            ),
            (
                "not coordinator finds the coordinator and retries",
                vec![answer(16, &[]), ok()],
                LONG,
                (committed(42, 7), 2, 1),
            ),
            (
                "another retriable group code retries",
                vec![answer(7, &[]), ok()],
                LONG,
                (committed(42, 7), 2, 0),
            ),
            (
                "a retriable group code past the deadline fails",
                vec![answer(14, &[])],
                NOW,
                (Outcome::Server(14), 1, 0),
            ),
            (
                "not coordinator past the deadline fails",
                vec![answer(16, &[])],
                NOW,
                (Outcome::Server(16), 1, 0),
            ),
            (
                "group authorization failed is fatal",
                vec![answer(30, &[])],
                LONG,
                (
                    Outcome::GroupAuthorizationFailed("group-a".to_string()),
                    1,
                    0,
                ),
            ),
            (
                "unknown member id is fatal",
                vec![answer(25, &[])],
                LONG,
                (Outcome::OffsetFetchFailed(25), 1, 0),
            ),
            (
                "stale member epoch is fatal without a member epoch",
                vec![answer(113, &[])],
                LONG,
                (Outcome::OffsetFetchFailed(113), 1, 0),
            ),
            (
                "another group code is fatal",
                vec![answer(69, &[])],
                LONG,
                (Outcome::OffsetFetchFailed(69), 1, 0),
            ),
            (
                "a group code hides the partition codes",
                vec![answer(14, &[(ORDERS, 29, -1)]), ok()],
                LONG,
                (committed(42, 7), 2, 0),
            ),
            // Partition codes.
            (
                "unknown topic or partition then committed offsets",
                vec![partition_error(3, 0), ok()],
                LONG,
                (committed(42, 7), 2, 0),
            ),
            (
                "unknown topic id then committed offsets",
                vec![partition_error(0, 100), ok()],
                LONG,
                (committed(42, 7), 2, 0),
            ),
            (
                "unknown topic id past the deadline gives no committed offset",
                vec![partition_error(100, 0)],
                NOW,
                (committed(-1, -1), 1, 0),
            ),
            (
                "topic authorization failed names every unauthorized topic",
                vec![partition_error(29, 29)],
                LONG,
                (topics(&["orders", "payments"]), 1, 0),
            ),
            (
                "unstable offset commit retries",
                vec![partition_error(88, 0), ok()],
                LONG,
                (committed(42, 7), 2, 0),
            ),
            (
                "unstable offset commit past the deadline fails",
                vec![partition_error(88, 0)],
                NOW,
                (Outcome::Server(88), 1, 0),
            ),
            (
                "not leader or follower is fatal",
                vec![partition_error(6, 0)],
                LONG,
                (Outcome::OffsetFetchFailed(6), 1, 0),
            ),
            (
                "topic authorization comes before unstable offsets",
                vec![partition_error(88, 29)],
                LONG,
                (topics(&["payments"]), 1, 0),
            ),
            (
                "unstable offsets come before unknown topics",
                vec![partition_error(100, 88)],
                NOW,
                (Outcome::Server(88), 1, 0),
            ),
            (
                "an unexpected partition code comes before unknown topics",
                vec![partition_error(100, 6)],
                LONG,
                (Outcome::OffsetFetchFailed(6), 1, 0),
            ),
        ]
    }

    /// The decoded `OffsetFetch` requests that a mock coordinator received,
    /// each with its version.
    type SentOffsetFetches = Vec<(i16, OffsetFetchRequest)>;

    /// Run `send_offset_fetch` for partition 0 of each of `topics` against a
    /// mock coordinator that advertises `offset_fetch_range` and answers from
    /// `answers`. Return the outcome, the decoded `OffsetFetch` requests, and
    /// the `FindCoordinator` request count.
    async fn run_offset_fetch(
        offset_fetch_range: (i16, i16),
        topics: &[&str],
        answers: Vec<Answer>,
        timeout: Duration,
        name: &str,
    ) -> (Outcome, SentOffsetFetches, usize) {
        let offset_fetches = Arc::new(std::sync::Mutex::new(Vec::new()));
        let find_coordinators = Arc::new(AtomicUsize::new(0));
        let offset_fetches_in_mock = Arc::clone(&offset_fetches);
        let find_coordinators_in_mock = Arc::clone(&find_coordinators);
        let mock = MockBroker::start(move |api_key, version, _corr_id, mut body| {
            let mut buffer = bytes::BytesMut::new();
            match api_key {
                api_versions_request::API_KEY => {
                    Some(api_versions_for_offset_fetch(offset_fetch_range))
                }
                offset_fetch_request::API_KEY => {
                    let client_id_len = body.get_i16();
                    body.advance(usize::try_from(client_id_len).expect("client id length"));
                    if version >= offset_fetch_request::FLEXIBLE_MIN {
                        body.advance(1);
                    }
                    let request = OffsetFetchRequest::decode(&mut body, version)
                        .expect("offset fetch request decodes");
                    let mut sent = offset_fetches_in_mock.lock().expect("requests lock");
                    let attempt = sent.len();
                    sent.push((version, request));
                    Some(offset_fetch_response(
                        &answers[attempt.min(answers.len() - 1)],
                        version,
                    ))
                }
                find_coordinator_request::API_KEY => {
                    find_coordinators_in_mock.fetch_add(1, Ordering::SeqCst);
                    FindCoordinatorResponse::default()
                        .encode(&mut buffer, version)
                        .expect("encode find coordinator");
                    Some(buffer.to_vec())
                }
                metadata_request::API_KEY => {
                    MetadataResponse::default()
                        .encode(&mut buffer, version)
                        .expect("encode metadata");
                    Some(buffer.to_vec())
                }
                _ => None,
            }
        })
        .await;
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .build()
            .await
            .expect("client");
        let request = build_offset_fetch(
            "group-a",
            &topics
                .iter()
                .map(|topic| ((*topic).to_string(), vec![0]))
                .collect(),
        );
        let coordinator_id = AtomicI32::new(0);

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            send_offset_fetch(
                &client,
                "group-a",
                &coordinator_id,
                &request,
                CoordinatorRetryPolicy {
                    timeout,
                    initial_backoff: Duration::from_millis(1),
                    max_backoff: Duration::from_millis(1),
                },
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("case {name}: offset fetch never finished"));

        mock.stop();
        let sent = offset_fetches.lock().expect("requests lock").clone();
        (
            outcome(result, name),
            sent,
            find_coordinators.load(Ordering::SeqCst),
        )
    }

    /// A rejoin keeps only the errors that Kafka's consumer raises from
    /// `poll()`, and `poll()` takes each one once.
    /// Kafka's `enforceRebalance` sets `rejoinNeeded` at the call, so the
    /// first `poll` after the call joins. The task can see the request after
    /// that `poll`, so the request keeps the `poll` count of the call.
    #[test]
    fn an_enforced_rebalance_is_due_after_a_poll_that_came_before_the_task_saw_it() {
        let (pending, _pending_rx) = tokio::sync::watch::channel(false);
        let (polls, polls_rx) = tokio::sync::watch::channel(5_u64);
        let mut request = RejoinRequest::None;
        // The application polled after `enforce_rebalance` at count 5.
        polls.send_replace(6);
        request.request_after_poll(5, &pending);
        assert2::assert!((request.due(&polls_rx), *pending.borrow()) == (true, true));
    }

    #[test]
    fn rejoin_errors_reach_poll_only_when_fatal() {
        for (name, error, expected) in [
            (
                "group authorization failed",
                ConsumerError::GroupAuthorizationFailed("group-a".into()),
                true,
            ),
            (
                "topic authorization failed",
                ConsumerError::TopicAuthorizationFailed(BTreeSet::from(["orders".to_string()])),
                true,
            ),
            (
                "offset fetch failed",
                ConsumerError::OffsetFetchFailed(6),
                true,
            ),
            (
                "retriable code past the deadline",
                ConsumerError::Server(14),
                false,
            ),
            (
                "coordinator unavailable",
                ConsumerError::CoordinatorUnavailable,
                false,
            ),
        ] {
            let slot = PollErrorSlot::default();
            report_rejoin_error(&slot, error);
            let first = take_poll_error(&slot).is_some();
            let second = take_poll_error(&slot).is_some();
            check!((first, second) == (expected, false), "case {name}");
        }
    }

    fn retry(timeout: Duration) -> CoordinatorRetryPolicy {
        CoordinatorRetryPolicy {
            timeout,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        }
    }

    struct Resp {
        error_code: i16,
    }

    fn refused_connect_error() -> ConsumerError {
        ConsumerError::Client(krabka_client_core::ClientError::Connect {
            addr: SocketAddr::from(([127, 0, 0, 1], 9092)),
            source: io::Error::new(io::ErrorKind::ConnectionRefused, "refused"),
        })
    }

    fn api_versions_for_leave_group() -> Vec<u8> {
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
                    api_key: leave_group_request::API_KEY,
                    min_version: 0,
                    max_version: leave_group_request::MAX_VERSION,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut buffer = bytes::BytesMut::new();
        response
            .encode(&mut buffer, 0)
            .expect("encode API versions");
        buffer.to_vec()
    }

    #[tokio::test(start_paused = true)]
    async fn subscription_metadata_refresh_due_uses_configured_inclusive_boundary() {
        let last_check = tokio::time::Instant::now();
        let interval = millis(37);

        tokio::time::advance(Duration::from_millis(36)).await;
        assert2::assert!(!subscription_metadata_refresh_due(last_check, interval));

        tokio::time::advance(Duration::from_millis(1)).await;
        assert2::assert!(subscription_metadata_refresh_due(last_check, interval));
    }

    #[tokio::test]
    async fn coordinator_leave_group_uses_configured_timeout() {
        let saw_leave = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_leave_in_mock = Arc::clone(&saw_leave);
        let mock = MockBroker::start(move |api_key, _version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                Some(api_versions_for_leave_group())
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
        let state = CoordinatorState {
            client,
            group_id: "group-a".into(),
            coordinator_id: Arc::new(AtomicI32::new(0)),
            member_id: "member-a".into(),
            published_member_id: tokio::sync::watch::Sender::new("member-a".to_owned()),
            commit_identity: Arc::new(Mutex::new(CommitIdentity {
                generation: 1,
                member_id: "member-a".into(),
                ownership_ids: HashMap::new(),
                rejoin_on_poll: false,
            })),
            group_instance_id: None,
            generation_id: 1,
            current_generation: Arc::new(AtomicI32::new(1)),
            assignors: vec![Assignor::Range],
            rebalance_protocol: RebalanceProtocol::Eager,
            subscription: crate::subscription::shared(vec!["topic".into()], None, true),
            subscription_changes: crate::subscription::shared(Vec::new(), None, true).subscribe(),
            joined_topics: Vec::new(),
            seen_unsubscribes: 0,
            assigned: Arc::new(Mutex::new(Vec::new())),
            assignment_changed: Arc::new(Notify::new()),
            next_ownership_id: 1,
            next_offsets: Arc::new(Mutex::new(HashMap::new())),
            end_offsets: Arc::new(Mutex::new(HashMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
            topic_ids: Arc::new(Mutex::new(HashMap::new())),
            session_timeout: secs(45),
            max_poll_interval: minutes(1),
            heartbeat_interval: secs(3),
            subscription_metadata_refresh_interval: millis(37),
            leave_group_timeout: millis(37),
            auto_offset_reset: AutoOffsetReset::Latest,
            client_rack: None,
            initial_subscribed_counts: HashMap::new(),
            retry_policy: retry(Duration::from_secs(30)),
            poll_error: PollErrorSlot::default(),
            auto_commit: None,
            commit_serialization: Arc::new(Mutex::new(())),
            join_prepared: false,
            polls: PollSignal::default().subscribe(),
            rebalance_pending: tokio::sync::watch::Sender::new(false),
            close_operation: tokio::sync::watch::channel(crate::control::CloseRequest::default()).1,
            rejoin_reason: String::new(),
            listener_calls: None,
            enforced_rebalances: tokio::sync::mpsc::unbounded_channel().1,
            lost_partitions: Vec::new(),
            assigned_callback_pending: Arc::default(),
            poll_timer: PollTimer::new(secs(300)),
            poll_timeout_in_callback: false,
        };

        tokio::time::timeout(
            Duration::from_secs(1),
            leave_group(
                &state,
                &state.member_id,
                GroupMembershipOperation::Default,
                CLOSE_LEAVE_REASON,
            ),
        )
        .await
        .expect("configured leave deadline bounds coordinator shutdown");
        mock.stop();
        assert2::assert!(saw_leave.load(Ordering::SeqCst));
    }

    #[test]
    fn find_coordinator_request_populates_legacy_and_batched_group_keys() {
        let req = build_find_coordinator_request("group-a".into());

        assert2::assert!(
            req == FindCoordinatorRequest {
                key: "group-a".into(),
                key_type: 0,
                coordinator_keys: vec!["group-a".into()],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn subscribed_topics_grew_detects_appearance_and_partition_growth() {
        let empty: HashMap<String, i32> = HashMap::new();
        let one: HashMap<String, i32> = [("logs".to_string(), 1)].into_iter().collect();
        let three: HashMap<String, i32> = [("logs".to_string(), 3)].into_iter().collect();

        for (_name, known, current, expected) in [
            // Cold-start race: topic absent at join, created later -> growth -> rejoin.
            ("topic appears", &empty, &one, true),
            // Topic gained partitions -> rejoin to (re)distribute them.
            ("partition count grows", &one, &three, true),
            // Steady state: unchanged -> no spurious rejoin.
            ("steady state", &one, &one, false),
            // A topic shrinking/disappearing is not "growth" -> no rejoin.
            ("partition count shrinks", &three, &one, false),
            ("topic disappears", &one, &empty, false),
        ] {
            assert2::assert!(subscribed_topics_grew(known, current) == expected);
        }
    }

    #[test]
    fn merge_counts_advances_monotonically_and_ignores_transient_under_reports() {
        let one: HashMap<String, i32> = [("logs".to_string(), 1)].into_iter().collect();
        let three: HashMap<String, i32> = [("logs".to_string(), 3)].into_iter().collect();
        let five: HashMap<String, i32> = [("logs".to_string(), 5)].into_iter().collect();

        // Empty baseline + a topic appears -> baseline records it.
        let mut known: HashMap<String, i32> = HashMap::new();
        merge_counts(&mut known, &one);
        assert2::assert!(known.get("logs") == Some(&1));

        // Growth advances the baseline; after merging the new count the SAME
        // count is no longer seen as growth (so the rejoin doesn't re-fire).
        merge_counts(&mut known, &three);
        assert2::assert!(
            (known.get("logs"), subscribed_topics_grew(&known, &three)) == (Some(&3), false)
        );

        // A transient metadata under-report (controller failover / partial
        // response) must NOT lower the baseline: Kafka partition counts are
        // monotonic, so dropping to 1 then recovering to 3 would otherwise churn
        // a spurious rejoin. max-merge pins it at 3.
        merge_counts(&mut known, &one);
        assert2::assert!(
            (known.get("logs"), subscribed_topics_grew(&known, &three)) == (Some(&3), false)
        );

        // A non-leader rejoin's snapshot is empty -> max-merge is a no-op, so the
        // baseline survives (the next tick sees no phantom growth).
        merge_counts(&mut known, &HashMap::new());
        assert2::assert!(known.get("logs") == Some(&3));

        // A genuinely larger count still advances.
        merge_counts(&mut known, &five);
        assert2::assert!(known.get("logs") == Some(&5));
    }

    #[tokio::test(start_paused = true)]
    async fn retry_deadline_elapsed_uses_elapsed_timeout_boundary() {
        let start = tokio::time::Instant::now();

        assert2::assert!(!retry_deadline_elapsed(start, Duration::from_millis(1)));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert2::assert!(retry_deadline_elapsed(start, Duration::from_millis(1)));
    }

    #[test]
    fn next_backoff_doubles_until_cap() {
        for (_name, backoff, max_backoff, expected) in [
            (
                "doubling below cap",
                Duration::from_millis(100),
                Duration::from_secs(1),
                Duration::from_millis(200),
            ),
            (
                "doubling reaches cap",
                Duration::from_millis(800),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
            (
                "already capped",
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
        ] {
            assert2::assert!(next_backoff(backoff, max_backoff) == expected);
        }
    }

    #[test]
    fn leave_group_request_populates_legacy_and_batched_member_fields() {
        let req = build_leave_group_request(
            "group-a".into(),
            "member-a".into(),
            Some(CLOSE_LEAVE_REASON),
        );

        assert2::assert!(
            req == LeaveGroupRequest {
                group_id: "group-a".into(),
                member_id: "member-a".into(),
                members: vec![MemberIdentity {
                    member_id: "member-a".into(),
                    group_instance_id: None,
                    reason: Some("the consumer is being closed".into()),
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    /// Kafka's `ConsumerCoordinator.onLeaderElected` runs the assignor that the
    /// coordinator selected over the partitions of every topic that a member
    /// subscribes to, and fails for a protocol that this member did not offer.
    #[test]
    fn leader_runs_the_selected_assignor_over_all_member_topics() {
        use krabka_protocol::owned::{
            join_group_response::JoinGroupResponseMember,
            metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
        };
        let topic = |name: &str, partitions: i32| MetadataResponseTopic {
            name: Some(name.into()),
            partitions: (0..partitions)
                .map(|partition_index| MetadataResponsePartition {
                    partition_index,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let metadata = MetadataResponse {
            topics: vec![topic("a", 2), topic("b", 2), topic("c", 1)],
            ..Default::default()
        };
        let member = |id: &str, topics: &[&str]| JoinGroupResponseMember {
            member_id: id.into(),
            metadata: encode_subscription(
                &topics.iter().map(|t| (*t).to_owned()).collect::<Vec<_>>(),
                &[],
                -1,
                None,
                None,
            ),
            ..Default::default()
        };
        let response = |protocol: &str| JoinGroupResponse {
            protocol_name: Some(protocol.into()),
            members: vec![member("m1", &["a"]), member("m2", &["a", "b"])],
            ..Default::default()
        };
        let decoded = |result: Result<Vec<SyncGroupRequestAssignment>, ConsumerError>| {
            result
                .map(|assignments| {
                    assignments
                        .into_iter()
                        .map(|assignment| {
                            (
                                assignment.member_id,
                                decode_assignment(&assignment.assignment),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .map_err(|error| error.to_string())
        };
        let p = |topic: &str, partition: i32| (topic.to_owned(), partition);
        let assignors = [Assignor::Range, Assignor::RoundRobin];
        let actual = [
            decoded(leader_assignments(
                &assignors,
                &response("roundrobin"),
                &metadata,
            )),
            decoded(leader_assignments(
                &assignors,
                &response("range"),
                &metadata,
            )),
            decoded(leader_assignments(
                &assignors,
                &response("sticky"),
                &metadata,
            )),
        ];
        assert2::assert!(
            actual
                == [
                    Ok(vec![
                        ("m1".to_owned(), vec![p("a", 0)]),
                        ("m2".to_owned(), vec![p("a", 1), p("b", 0), p("b", 1)]),
                    ]),
                    Ok(vec![
                        ("m1".to_owned(), vec![p("a", 0)]),
                        ("m2".to_owned(), vec![p("a", 1), p("b", 0), p("b", 1)]),
                    ]),
                    Err(
                        "illegal state: Coordinator selected invalid assignment protocol: sticky"
                            .to_owned()
                    ),
                ]
        );
    }

    #[test]
    fn join_group_request_preserves_group_member_timeouts_protocol_and_reason() {
        let fields = JoinRequestFields {
            group_id: "group-a".into(),
            group_instance_id: Some("instance-a".into()),
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 30_000,
            protocols: vec![("range".into(), vec![1, 2, 3].into())],
            reason: "group is already rebalancing".into(),
        };

        assert2::assert!(
            fields.request("member-a".into())
                == JoinGroupRequest {
                    group_id: "group-a".into(),
                    session_timeout_ms: 10_000,
                    rebalance_timeout_ms: 30_000,
                    member_id: "member-a".into(),
                    group_instance_id: Some("instance-a".into()),
                    protocol_type: "consumer".into(),
                    protocols: vec![JoinGroupRequestProtocol {
                        name: "range".into(),
                        metadata: vec![1, 2, 3].into(),
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }],
                    reason: Some("group is already rebalancing".into()),
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }
        );
    }

    #[test]
    fn sync_group_request_preserves_member_generation_protocol_and_assignments() {
        let assignment = build_sync_group_assignment(
            "member-a".into(),
            &[("topic-a".to_string(), 0), ("topic-a".to_string(), 1)],
        );
        assert2::assert!(
            (
                assignment.member_id.as_str(),
                decode_assignment(&assignment.assignment),
            ) == (
                "member-a",
                vec![("topic-a".to_string(), 0), ("topic-a".to_string(), 1)],
            )
        );

        let req = build_sync_group_request(
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
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn heartbeat_request_preserves_group_generation_and_member() {
        let req = build_heartbeat_request(
            "group-a".into(),
            42,
            "member-a".into(),
            Some("instance-a".into()),
        );

        assert2::assert!(
            req == HeartbeatRequest {
                group_id: "group-a".into(),
                generation_id: 42,
                member_id: "member-a".into(),
                group_instance_id: Some("instance-a".into()),
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn prime_offset_helpers_preserve_committed_and_reset_boundaries() {
        for (_name, committed, reset, expected) in [
            ("committed positive", 12, AutoOffsetReset::Earliest, 12),
            ("committed zero", 0, AutoOffsetReset::Latest, 0),
            ("missing earliest", -1, AutoOffsetReset::Earliest, 0),
            ("missing latest", -1, AutoOffsetReset::Latest, i64::MAX),
            ("missing none", -1, AutoOffsetReset::None, i64::MAX),
        ] {
            assert2::assert!(starting_offset(committed, reset) == expected);
        }

        for (_name, reset, expected) in [
            ("earliest", AutoOffsetReset::Earliest, 0),
            ("latest", AutoOffsetReset::Latest, i64::MAX),
            ("none", AutoOffsetReset::None, i64::MAX),
        ] {
            assert2::assert!(reset_starting_offset(reset) == expected);
        }

        for (_name, has_position, expected) in [
            ("missing position", false, true),
            ("existing position", true, false),
        ] {
            assert2::assert!(should_prime_missing_partition(has_position) == expected);
        }
    }

    #[test]
    fn heartbeat_outcome_classifies_success_rejoin_and_transient_errors() {
        for (_name, error_code, expected) in [
            ("success", 0, HeartbeatOutcome::Ok),
            (
                "rebalance in progress",
                27,
                HeartbeatOutcome::NeedRejoin(27),
            ),
            ("illegal generation", 22, HeartbeatOutcome::NeedRejoin(22)),
            ("unknown member", 25, HeartbeatOutcome::RejoinFromScratch),
            ("fenced instance id", 82, HeartbeatOutcome::Fenced),
            ("loading coordinator", 14, HeartbeatOutcome::Transient),
            ("unknown transient", 99, HeartbeatOutcome::Transient),
        ] {
            assert2::assert!(heartbeat_outcome(error_code) == expected);
        }
    }

    /// Encode `response` at `version` behind the flexible response header's
    /// empty tagged fields when `version` is flexible.
    fn group_response(response: &impl Encode, version: i16, flexible_min: i16) -> Vec<u8> {
        let mut buffer = bytes::BytesMut::new();
        if version >= flexible_min {
            buffer.extend_from_slice(&[0]);
        }
        response
            .encode(&mut buffer, version)
            .expect("encode response");
        buffer.to_vec()
    }

    /// An `ApiVersions` response that advertises the lowest client version of
    /// each group API that the coordinator task sends.
    fn api_versions_for_coordinator_task() -> Vec<u8> {
        let version = |api_key: i16, version: i16| ApiVersion {
            api_key,
            min_version: version,
            max_version: version,
            ..Default::default()
        };
        let response = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 3,
                    ..Default::default()
                },
                version(metadata_request::API_KEY, metadata_request::MIN_VERSION),
                version(
                    find_coordinator_request::API_KEY,
                    find_coordinator_request::MIN_VERSION,
                ),
                version(heartbeat_request::API_KEY, heartbeat_request::MIN_VERSION),
                version(join_group_request::API_KEY, join_group_request::MIN_VERSION),
                version(sync_group_request::API_KEY, sync_group_request::MIN_VERSION),
                version(
                    leave_group_request::API_KEY,
                    leave_group_request::MIN_VERSION,
                ),
            ],
            ..Default::default()
        };
        let mut buffer = bytes::BytesMut::new();
        response
            .encode(&mut buffer, 0)
            .expect("encode API versions");
        buffer.to_vec()
    }

    /// The broker answers for one coordinator task case. `None` leaves the
    /// request without a response.
    #[derive(Clone, Copy)]
    struct GroupAnswers {
        /// Whether the application polls each 10 ms during the case.
        polls: bool,
        heartbeat: i16,
        join_group: Option<i16>,
        sync_group: Option<i16>,
    }

    /// What the coordinator task did after the heartbeat answer.
    #[derive(Debug, PartialEq, Eq)]
    struct TaskObservation {
        task_exited: bool,
        shutdown_cancelled: bool,
        poll_error: Option<String>,
        assigned: Vec<(String, i32)>,
        generation: i32,
        commit_member_id: String,
        rejoin_on_poll: bool,
        group_requests: BTreeSet<&'static str>,
    }

    fn group_request_name(api_key: i16) -> Option<&'static str> {
        match api_key {
            find_coordinator_request::API_KEY => Some("FindCoordinator"),
            heartbeat_request::API_KEY => Some("Heartbeat"),
            join_group_request::API_KEY => Some("JoinGroup"),
            sync_group_request::API_KEY => Some("SyncGroup"),
            leave_group_request::API_KEY => Some("LeaveGroup"),
            _ => None,
        }
    }

    /// Start a mock coordinator that answers with `answers`, run the
    /// coordinator task of a static member that owns `orders-0` against it,
    /// and return what the task did once the observation equals `expected`
    /// or two seconds elapse.
    async fn run_coordinator_task(
        answers: GroupAnswers,
        expected: &TaskObservation,
    ) -> TaskObservation {
        let port = Arc::new(std::sync::atomic::AtomicU16::new(0));
        let port_in_mock = Arc::clone(&port);
        let group_requests = Arc::new(std::sync::Mutex::new(BTreeSet::new()));
        let group_requests_in_mock = Arc::clone(&group_requests);
        let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
            if let Some(name) = group_request_name(api_key) {
                group_requests_in_mock
                    .lock()
                    .expect("requests lock")
                    .insert(name);
            }
            let port = i32::from(port_in_mock.load(Ordering::SeqCst));
            match api_key {
                api_versions_request::API_KEY => Some(api_versions_for_coordinator_task()),
                metadata_request::API_KEY => Some(group_response(
                    &MetadataResponse {
                        brokers: vec![MetadataResponseBroker {
                            node_id: 0,
                            host: "127.0.0.1".into(),
                            port,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    version,
                    metadata_request::FLEXIBLE_MIN,
                )),
                find_coordinator_request::API_KEY => Some(group_response(
                    &FindCoordinatorResponse {
                        node_id: 0,
                        host: "127.0.0.1".into(),
                        port,
                        ..Default::default()
                    },
                    version,
                    find_coordinator_request::FLEXIBLE_MIN,
                )),
                heartbeat_request::API_KEY => Some(group_response(
                    &HeartbeatResponse {
                        error_code: answers.heartbeat,
                        ..Default::default()
                    },
                    version,
                    heartbeat_request::FLEXIBLE_MIN,
                )),
                join_group_request::API_KEY => answers.join_group.map(|error_code| {
                    group_response(
                        &JoinGroupResponse {
                            error_code,
                            generation_id: 2,
                            protocol_name: Some("range".into()),
                            leader: "member-b".into(),
                            member_id: "member-a".into(),
                            ..Default::default()
                        },
                        version,
                        join_group_request::FLEXIBLE_MIN,
                    )
                }),
                sync_group_request::API_KEY => answers.sync_group.map(|error_code| {
                    group_response(
                        &SyncGroupResponse {
                            error_code,
                            ..Default::default()
                        },
                        version,
                        sync_group_request::FLEXIBLE_MIN,
                    )
                }),
                _ => None,
            }
        })
        .await;
        port.store(mock.addr.port(), Ordering::SeqCst);
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(secs(5))
            .build()
            .await
            .expect("client");
        let orders_0 = (ORDERS.to_string(), 0);
        // The application does not poll during the case.
        let poll_signal = PollSignal::default();
        let state = CoordinatorState {
            client,
            group_id: "group-a".into(),
            coordinator_id: Arc::new(AtomicI32::new(0)),
            member_id: "member-a".into(),
            published_member_id: tokio::sync::watch::Sender::new("member-a".to_owned()),
            commit_identity: Arc::new(Mutex::new(CommitIdentity {
                generation: 1,
                member_id: "member-a".into(),
                ownership_ids: HashMap::from([(orders_0.clone(), 1)]),
                rejoin_on_poll: false,
            })),
            group_instance_id: Some("instance-a".into()),
            generation_id: 1,
            current_generation: Arc::new(AtomicI32::new(1)),
            assignors: vec![Assignor::Range],
            rebalance_protocol: RebalanceProtocol::Eager,
            subscription: crate::subscription::shared(vec![ORDERS.into()], None, true),
            subscription_changes: crate::subscription::shared(Vec::new(), None, true).subscribe(),
            joined_topics: Vec::new(),
            seen_unsubscribes: 0,
            assigned: Arc::new(Mutex::new(vec![orders_0.clone()])),
            assignment_changed: Arc::new(Notify::new()),
            next_ownership_id: 2,
            next_offsets: Arc::new(Mutex::new(HashMap::from([(orders_0, 5)]))),
            end_offsets: Arc::new(Mutex::new(HashMap::new())),
            positions: Arc::new(Mutex::new(HashMap::new())),
            topic_ids: Arc::new(Mutex::new(HashMap::new())),
            session_timeout: secs(45),
            max_poll_interval: minutes(1),
            heartbeat_interval: millis(20),
            subscription_metadata_refresh_interval: minutes(10),
            leave_group_timeout: millis(200),
            auto_offset_reset: AutoOffsetReset::Latest,
            client_rack: None,
            initial_subscribed_counts: HashMap::new(),
            retry_policy: CoordinatorRetryPolicy {
                timeout: Duration::from_secs(1),
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
            },
            poll_error: PollErrorSlot::default(),
            auto_commit: None,
            commit_serialization: Arc::new(Mutex::new(())),
            join_prepared: false,
            polls: poll_signal.subscribe(),
            rebalance_pending: tokio::sync::watch::Sender::new(false),
            close_operation: tokio::sync::watch::channel(crate::control::CloseRequest::default()).1,
            rejoin_reason: String::new(),
            listener_calls: None,
            enforced_rebalances: tokio::sync::mpsc::unbounded_channel().1,
            lost_partitions: Vec::new(),
            assigned_callback_pending: Arc::default(),
            poll_timer: PollTimer::new(secs(300)),
            poll_timeout_in_callback: false,
        };
        let poll_error = Arc::clone(&state.poll_error);
        let assigned = Arc::clone(&state.assigned);
        let generation = Arc::clone(&state.current_generation);
        let commit_identity = Arc::clone(&state.commit_identity);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(state, shutdown.clone()));
        let poller = answers.polls.then(|| {
            let poll_signal = poll_signal.clone();
            tokio::spawn(async move {
                loop {
                    note_poll(&poll_signal);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
        });

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        // After the observation first equals `expected`, the case watches for
        // ten more heartbeat intervals, so a late request shows.
        let mut settle_until = None;
        let observation = loop {
            let poll_error = poll_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .map(ToString::to_string);
            let identity = commit_identity.lock().await.clone();
            let observation = TaskObservation {
                task_exited: task.is_finished(),
                shutdown_cancelled: shutdown.is_cancelled(),
                poll_error,
                assigned: assigned.lock().await.clone(),
                generation: generation.load(Ordering::SeqCst),
                commit_member_id: identity.member_id,
                rejoin_on_poll: identity.rejoin_on_poll,
                group_requests: group_requests.lock().expect("requests lock").clone(),
            };
            let now = tokio::time::Instant::now();
            if now >= deadline || settle_until.is_some_and(|until| now >= until) {
                break observation;
            }
            if observation == *expected {
                settle_until.get_or_insert(now + Duration::from_millis(200));
            } else if settle_until.is_some() {
                break observation;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        if let Some(poller) = poller {
            poller.abort();
        }
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("coordinator task stops on shutdown")
            .expect("coordinator task does not panic");
        drop(poll_signal);
        mock.stop();
        observation
    }

    /// A heartbeat, `JoinGroup` or `SyncGroup` answer of
    /// `FENCED_INSTANCE_ID (82)` clears the assignment and the member, marks
    /// the commit identity with `rejoin_on_poll`, and leaves the fenced error
    /// for the next `poll()`. The task keeps running, sends no `LeaveGroup`,
    /// and sends no further group request before a `poll` takes the error.
    /// A rebalance starts only when the application polls.
    /// Kafka's `AbstractCoordinator.HeartbeatResponseHandler` resets the member
    /// and raises `FencedInstanceIdException`, which the heartbeat thread keeps
    /// as its failure cause. Other heartbeat answers keep the assignment.
    #[tokio::test]
    async fn coordinator_task_removes_a_fenced_static_member_until_the_next_poll() {
        let fenced = "fenced group.instance.id instance-a: another consumer with the same group.instance.id joined the group";
        let owned = vec![(ORDERS.to_string(), 0)];
        let requests = |names: &[&'static str]| names.iter().copied().collect::<BTreeSet<_>>();
        for (name, answers, expected) in [
            (
                "success",
                GroupAnswers {
                    polls: false,
                    heartbeat: 0,
                    join_group: None,
                    sync_group: None,
                },
                TaskObservation {
                    task_exited: false,
                    shutdown_cancelled: false,
                    poll_error: None,
                    assigned: owned.clone(),
                    generation: 1,
                    commit_member_id: "member-a".into(),
                    rejoin_on_poll: false,
                    group_requests: requests(&["Heartbeat"]),
                },
            ),
            (
                "heartbeat fenced instance id",
                GroupAnswers {
                    polls: true,
                    heartbeat: 82,
                    join_group: None,
                    sync_group: None,
                },
                TaskObservation {
                    task_exited: false,
                    shutdown_cancelled: false,
                    poll_error: Some(fenced.into()),
                    assigned: Vec::new(),
                    generation: -1,
                    commit_member_id: String::new(),
                    rejoin_on_poll: true,
                    group_requests: requests(&["Heartbeat"]),
                },
            ),
            (
                "rebalance in progress",
                GroupAnswers {
                    polls: false,
                    heartbeat: 27,
                    join_group: None,
                    sync_group: None,
                },
                TaskObservation {
                    task_exited: false,
                    shutdown_cancelled: false,
                    poll_error: None,
                    assigned: owned.clone(),
                    generation: 1,
                    commit_member_id: "member-a".into(),
                    rejoin_on_poll: false,
                    group_requests: requests(&["Heartbeat"]),
                },
            ),
            (
                "rebalance in progress and the application polls",
                GroupAnswers {
                    polls: true,
                    heartbeat: 27,
                    join_group: None,
                    sync_group: None,
                },
                TaskObservation {
                    task_exited: false,
                    shutdown_cancelled: false,
                    poll_error: None,
                    assigned: owned.clone(),
                    generation: 1,
                    commit_member_id: "member-a".into(),
                    rejoin_on_poll: false,
                    group_requests: requests(&["Heartbeat", "JoinGroup"]),
                },
            ),
            (
                "unknown member id",
                GroupAnswers {
                    polls: false,
                    heartbeat: 25,
                    join_group: None,
                    sync_group: None,
                },
                TaskObservation {
                    task_exited: false,
                    shutdown_cancelled: false,
                    poll_error: None,
                    assigned: Vec::new(),
                    generation: -1,
                    commit_member_id: String::new(),
                    rejoin_on_poll: false,
                    group_requests: requests(&["Heartbeat"]),
                },
            ),
            (
                "coordinator not available",
                GroupAnswers {
                    polls: false,
                    heartbeat: 15,
                    join_group: None,
                    sync_group: None,
                },
                TaskObservation {
                    task_exited: false,
                    shutdown_cancelled: false,
                    poll_error: None,
                    assigned: owned.clone(),
                    generation: 1,
                    commit_member_id: "member-a".into(),
                    rejoin_on_poll: false,
                    group_requests: requests(&["FindCoordinator", "Heartbeat"]),
                },
            ),
            (
                "join group fenced instance id",
                GroupAnswers {
                    polls: true,
                    heartbeat: 27,
                    join_group: Some(82),
                    sync_group: None,
                },
                TaskObservation {
                    task_exited: false,
                    shutdown_cancelled: false,
                    poll_error: Some(fenced.into()),
                    assigned: Vec::new(),
                    generation: -1,
                    commit_member_id: String::new(),
                    rejoin_on_poll: true,
                    group_requests: requests(&["Heartbeat", "JoinGroup"]),
                },
            ),
            (
                "sync group fenced instance id",
                GroupAnswers {
                    polls: true,
                    heartbeat: 27,
                    join_group: Some(0),
                    sync_group: Some(82),
                },
                TaskObservation {
                    task_exited: false,
                    shutdown_cancelled: false,
                    poll_error: Some(fenced.into()),
                    assigned: Vec::new(),
                    generation: -1,
                    commit_member_id: String::new(),
                    rejoin_on_poll: true,
                    group_requests: requests(&["Heartbeat", "JoinGroup", "SyncGroup"]),
                },
            ),
        ] {
            let observation = run_coordinator_task(answers, &expected).await;
            check!(observation == expected, "case {name}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retries_until_coordinator_finishes_loading() {
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_retry(
            retry(Duration::from_secs(30)),
            |r: &Resp| r.error_code,
            || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    // COORDINATOR_LOAD_IN_PROGRESS (14) thrice, then success.
                    Ok::<_, ConsumerError>(Resp {
                        error_code: if n < 3 { 14 } else { 0 },
                    })
                }
            },
        )
        .await
        .unwrap();
        assert2::assert!(r.error_code == 0);
        assert2::assert!(calls.load(Ordering::SeqCst) == 4);
    }

    #[tokio::test(start_paused = true)]
    async fn configured_coordinator_retry_policy_controls_backoff_and_timeout() {
        let calls = AtomicUsize::new(0);
        let retry = CoordinatorRetryPolicy {
            timeout: Duration::from_millis(35),
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(10),
        };
        let r = with_coordinator_retry(
            retry,
            |r: &Resp| r.error_code,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, ConsumerError>(Resp { error_code: 15 }) }
            },
        )
        .await
        .unwrap();
        assert2::assert!(r.error_code == 15);
        assert2::assert!(calls.load(Ordering::SeqCst) == 5);
    }

    #[tokio::test(start_paused = true)]
    async fn surfaces_last_response_after_deadline() {
        let r = with_coordinator_retry(
            retry(Duration::from_secs(1)),
            |r: &Resp| r.error_code,
            || async { Ok::<_, ConsumerError>(Resp { error_code: 15 }) },
        )
        .await
        .unwrap();
        // Deadline hit while still retriable: return the last response so the
        // caller's `error_code != 0` handling surfaces it.
        assert2::assert!(r.error_code == 15);
    }

    #[tokio::test(start_paused = true)]
    async fn non_retriable_code_returns_immediately() {
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_retry(
            retry(Duration::from_secs(30)),
            |r: &Resp| r.error_code,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok::<_, ConsumerError>(Resp { error_code: 25 }) } // UNKNOWN_MEMBER_ID
            },
        )
        .await
        .unwrap();
        assert2::assert!(r.error_code == 25);
        assert2::assert!(calls.load(Ordering::SeqCst) == 1);
    }

    #[tokio::test(start_paused = true)]
    async fn disconnect_past_deadline_surfaces_coordinator_unavailable() {
        let r = with_coordinator_retry(
            retry(Duration::from_secs(1)),
            |r: &Resp| r.error_code,
            || async {
                Err::<Resp, _>(ConsumerError::Client(
                    krabka_client_core::ClientError::Disconnected,
                ))
            },
        )
        .await;
        assert2::assert!(matches!(r, Err(ConsumerError::CoordinatorUnavailable)));
    }

    #[tokio::test(start_paused = true)]
    async fn connect_past_deadline_surfaces_coordinator_unavailable() {
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_retry(
            retry(Duration::from_millis(1)),
            |r: &Resp| r.error_code,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err::<Resp, _>(refused_connect_error()) }
            },
        )
        .await;

        assert2::assert!(matches!(r, Err(ConsumerError::CoordinatorUnavailable)));
        assert2::assert!(calls.load(Ordering::SeqCst) > 1);
    }
}

#[cfg(test)]
mod find_coordinator_parse_tests {

    use krabka_protocol::owned::find_coordinator_response::Coordinator;

    use super::*;

    #[test]
    fn parses_legacy_and_batched_coordinator_shapes() {
        for (_name, resp, expected) in [
            (
                "batched success",
                FindCoordinatorResponse {
                    node_id: -1,
                    error_code: 99,
                    coordinators: vec![Coordinator {
                        key: "g".into(),
                        node_id: 7,
                        host: "h".into(),
                        port: 9092,
                        error_code: 0,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                (7, 0, false),
            ),
            (
                "legacy success",
                FindCoordinatorResponse {
                    node_id: 3,
                    error_code: 0,
                    coordinators: vec![],
                    ..Default::default()
                },
                (3, 0, false),
            ),
            (
                "batched not coordinator",
                FindCoordinatorResponse {
                    node_id: 1,
                    error_code: 0,
                    coordinators: vec![Coordinator {
                        key: "g".into(),
                        node_id: -1,
                        error_code: NOT_COORDINATOR,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                (-1, NOT_COORDINATOR, true),
            ),
        ] {
            let code = coordinator_error_code(&resp);
            assert2::assert!(
                (
                    coordinator_node_id(&resp),
                    code,
                    is_retriable_coordinator_code(code)
                ) == expected
            );
        }
    }
}

#[cfg(test)]
mod refind_tests {
    use std::sync::atomic::AtomicUsize;

    use assert2::check;

    use super::*;

    fn retry(timeout: Duration) -> CoordinatorRetryPolicy {
        CoordinatorRetryPolicy {
            timeout,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        }
    }

    struct Resp {
        error_code: i16,
    }

    // Without a live broker we can't exercise the real re-find (it sends RPCs),
    // but we can prove the retry/backoff/deadline behaviour matches
    // `with_coordinator_retry` for the no-broker code paths. A purely
    // successful response returns immediately without touching the
    // coordinator cell.
    #[tokio::test(start_paused = true)]
    async fn returns_immediately_on_success_without_refind() {
        // 127.0.0.1:1 is unroutable, so any re-find attempt would fail — but a
        // success on the first attempt must never re-find.
        let client = Client::builder()
            .bootstrap("127.0.0.1:1")
            .build()
            .await
            .unwrap();
        let coord = AtomicI32::new(5);
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_refind(
            &client,
            "g",
            &coord,
            retry(Duration::from_secs(30)),
            |r: &Resp| r.error_code,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, ConsumerError>(Resp { error_code: 0 }) }
            },
        )
        .await
        .unwrap();
        check!(r.error_code == 0);
        check!(calls.load(Ordering::SeqCst) == 1);
        // Coordinator cell untouched — no re-find on success.
        check!(coord.load(Ordering::Relaxed) == 5);
    }

    // A non-retriable broker code (e.g. UNKNOWN_MEMBER_ID 25) is returned to the
    // caller on the first attempt, no re-find.
    #[tokio::test(start_paused = true)]
    async fn non_retriable_code_returns_without_refind() {
        let client = Client::builder()
            .bootstrap("127.0.0.1:1")
            .build()
            .await
            .unwrap();
        let coord = AtomicI32::new(2);
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_refind(
            &client,
            "g",
            &coord,
            retry(Duration::from_secs(30)),
            |r: &Resp| r.error_code,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, ConsumerError>(Resp { error_code: 25 }) }
            },
        )
        .await
        .unwrap();
        check!(r.error_code == 25);
        check!(calls.load(Ordering::SeqCst) == 1);
        check!(coord.load(Ordering::Relaxed) == 2);
    }

    #[tokio::test(start_paused = true)]
    async fn connect_error_refinds_until_deadline() {
        let client = Client::builder()
            .bootstrap("127.0.0.1:1")
            .socket_connection_setup_timeout(krabka_units::millis(10))
            .request_timeout(krabka_units::millis(10))
            .build()
            .await
            .unwrap();
        let coord = AtomicI32::new(5);
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_refind(
            &client,
            "g",
            &coord,
            retry(Duration::from_millis(1)),
            |r: &Resp| r.error_code,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async {
                    Err::<Resp, _>(ConsumerError::Client(
                        krabka_client_core::ClientError::Connect {
                            addr: "127.0.0.1:9092".parse().unwrap(),
                            source: std::io::Error::new(
                                std::io::ErrorKind::ConnectionRefused,
                                "refused",
                            ),
                        },
                    ))
                }
            },
        )
        .await;

        assert2::assert!(matches!(r, Err(ConsumerError::CoordinatorUnavailable)));
        assert2::assert!(calls.load(Ordering::SeqCst) > 1);
    }

    #[test]
    fn cooperative_ownership_preserves_retained_and_rotates_reassigned_partitions() {
        let p0 = ("topic".to_string(), 0);
        let p1 = ("topic".to_string(), 1);
        let mut ownership = HashMap::from([(p0.clone(), 7), (p1.clone(), 8)]);
        let mut next_id = 9;

        update_ownership(
            &mut ownership,
            std::slice::from_ref(&p0),
            true,
            &mut next_id,
        );
        assert2::assert!(ownership == HashMap::from([(p0.clone(), 7)]));

        update_ownership(
            &mut ownership,
            &[p0.clone(), p1.clone()],
            true,
            &mut next_id,
        );
        assert2::assert!(ownership == HashMap::from([(p0, 7), (p1, 9)]));
    }

    #[test]
    fn eager_assignment_rotates_even_retained_partition_ownership() {
        let p0 = ("topic".to_string(), 0);
        let mut ownership = HashMap::from([(p0.clone(), 7)]);
        let mut next_id = 8;

        update_ownership(
            &mut ownership,
            std::slice::from_ref(&p0),
            false,
            &mut next_id,
        );

        assert2::assert!(ownership == HashMap::from([(p0, 8)]));
    }
}
