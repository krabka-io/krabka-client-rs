//! The consumer group protocol of KIP-848: `ConsumerGroupHeartbeat`.
//!
//! With `group_protocol(GroupProtocol::Consumer)` the member sends
//! `ConsumerGroupHeartbeat` instead of `JoinGroup`, `SyncGroup` and
//! `Heartbeat`. The group coordinator assigns the partitions and the member
//! reconciles them: it gives up the partitions that the assignment no longer
//! holds, takes the new ones, and acknowledges the assignment with the next
//! heartbeat. This is Kafka's `ConsumerHeartbeatRequestManager` and
//! `ConsumerMembershipManager`.

use std::collections::{BTreeMap, HashMap, HashSet};

use krabka_protocol::{
    owned::{
        consumer_group_heartbeat_request::{ConsumerGroupHeartbeatRequest, TopicPartitions},
        consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use krabka_units::convert::TimeExt as _;

/// The group protocol of a consumer. Kafka's `group.protocol`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GroupProtocol {
    /// `JoinGroup`, `SyncGroup` and `Heartbeat`, with client-side assignment.
    #[default]
    Classic,
    /// `ConsumerGroupHeartbeat` with server-side assignment (KIP-848).
    Consumer,
}

/// Kafka's `ConsumerGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH`.
pub(crate) const JOIN_GROUP_MEMBER_EPOCH: i32 = 0;
/// Kafka's `ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH`.
pub(crate) const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;
/// Kafka's `ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH`.
pub(crate) const LEAVE_GROUP_STATIC_MEMBER_EPOCH: i32 = -2;

/// `FENCED_MEMBER_EPOCH`.
pub(crate) const FENCED_MEMBER_EPOCH: i16 = 110;
/// `UNKNOWN_MEMBER_ID`.
const UNKNOWN_MEMBER_ID: i16 = 25;

/// The partitions of an assignment, and the topic ids that the metadata does
/// not name yet.
pub(crate) type ResolvedAssignment = (Vec<(String, i32)>, HashSet<WireUuid>);

/// The partitions that a member owns, by topic id and sorted. The wire uuid
/// has no order, so the map is keyed by its bytes.
pub(crate) type OwnedByTopicId = BTreeMap<[u8; 16], Vec<i32>>;

/// The fields of the last `ConsumerGroupHeartbeat`. Kafka's
/// `ConsumerHeartbeatRequestManager.HeartbeatState.SentFields`: a field goes
/// out again only when it changed, or when the member joins.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SentFields {
    rebalance_timeout_ms: Option<i32>,
    subscribed_topic_names: Option<Vec<String>>,
    server_assignor: Option<String>,
    topic_partitions: Option<OwnedByTopicId>,
}

/// What the member sends in its next heartbeat.
#[derive(Clone, Debug)]
pub(crate) struct HeartbeatFields<'a> {
    pub group_id: &'a str,
    pub member_id: &'a str,
    pub member_epoch: i32,
    pub instance_id: Option<&'a str>,
    pub rack_id: Option<&'a str>,
    pub rebalance_timeout_ms: i32,
    /// The subscribed topics, sorted.
    pub subscribed_topic_names: Vec<String>,
    pub server_assignor: Option<&'a str>,
    /// The partitions that the member owns, by topic id.
    pub owned: OwnedByTopicId,
    /// `true` for the first heartbeat of a join, which sends every field.
    pub joining: bool,
}

/// Build the next `ConsumerGroupHeartbeat` and record the fields that it
/// carries.
pub(crate) fn build_heartbeat(
    fields: &HeartbeatFields<'_>,
    sent: &mut SentFields,
) -> ConsumerGroupHeartbeatRequest {
    let mut request = ConsumerGroupHeartbeatRequest {
        group_id: fields.group_id.to_owned(),
        member_id: fields.member_id.to_owned(),
        member_epoch: fields.member_epoch,
        instance_id: fields.instance_id.map(str::to_owned),
        ..Default::default()
    };
    let all = fields.joining;
    if all {
        // Kafka sends the rack only when the member joins.
        request.rack_id = fields.rack_id.map(str::to_owned);
        *sent = SentFields::default();
    }
    if all || sent.rebalance_timeout_ms != Some(fields.rebalance_timeout_ms) {
        request.rebalance_timeout_ms = fields.rebalance_timeout_ms;
        sent.rebalance_timeout_ms = Some(fields.rebalance_timeout_ms);
    } else {
        request.rebalance_timeout_ms = -1;
    }
    if all || sent.subscribed_topic_names.as_ref() != Some(&fields.subscribed_topic_names) {
        request.subscribed_topic_names = Some(fields.subscribed_topic_names.clone());
        sent.subscribed_topic_names = Some(fields.subscribed_topic_names.clone());
    }
    if let Some(assignor) = fields.server_assignor
        && (all || sent.server_assignor.as_deref() != Some(assignor))
    {
        request.server_assignor = Some(assignor.to_owned());
        sent.server_assignor = Some(assignor.to_owned());
    }
    if all || sent.topic_partitions.as_ref() != Some(&fields.owned) {
        request.topic_partitions = Some(
            fields
                .owned
                .iter()
                .map(|(topic_id, partitions)| TopicPartitions {
                    topic_id: WireUuid(*topic_id),
                    partitions: partitions.clone(),
                    ..Default::default()
                })
                .collect(),
        );
        sent.topic_partitions = Some(fields.owned.clone());
    }
    request
}

/// What the member does with the error code of a heartbeat response. Kafka's
/// `HeartbeatRequestManager.onErrorResponse`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HeartbeatAction {
    /// The response carries the member state.
    Ok,
    /// Send the heartbeat again after the retry backoff.
    Retry,
    /// Find the coordinator again, then send the heartbeat again.
    FindCoordinator,
    /// The coordinator does not know the member any more: give up the
    /// partitions and join again with epoch 0.
    Fenced,
    /// The application must see the error.
    Fatal,
}

/// Map the error code of a `ConsumerGroupHeartbeat` response, as Kafka's
/// `ConsumerHeartbeatRequestManager` does.
///
/// `COORDINATOR_NOT_AVAILABLE (15)` and `NOT_COORDINATOR (16)` make the member
/// find the coordinator again. `COORDINATOR_LOAD_IN_PROGRESS (14)` retries.
/// `FENCED_MEMBER_EPOCH (110)` and `UNKNOWN_MEMBER_ID (25)` make the member
/// join again from epoch 0. `UNSUPPORTED_ASSIGNOR (112)`,
/// `UNRELEASED_INSTANCE_ID (111)`, `GROUP_MAX_SIZE_REACHED (81)`,
/// `INVALID_REQUEST (42)`, `UNSUPPORTED_VERSION (35)` and the authorization
/// codes are fatal.
pub(crate) fn heartbeat_action(error_code: i16) -> HeartbeatAction {
    match error_code {
        0 => HeartbeatAction::Ok,
        crate::coordinator::COORDINATOR_NOT_AVAILABLE | crate::coordinator::NOT_COORDINATOR => {
            HeartbeatAction::FindCoordinator
        }
        crate::coordinator::COORDINATOR_LOAD_IN_PROGRESS => HeartbeatAction::Retry,
        FENCED_MEMBER_EPOCH | UNKNOWN_MEMBER_ID => HeartbeatAction::Fenced,
        // `UNSUPPORTED_ASSIGNOR (112)`, `UNRELEASED_INSTANCE_ID (111)`,
        // `GROUP_MAX_SIZE_REACHED (81)`, `INVALID_REQUEST (42)`,
        // `UNSUPPORTED_VERSION (35)`, the authorization codes and every other
        // code reach the application.
        _ => HeartbeatAction::Fatal,
    }
}

/// The epoch of the heartbeat that leaves the group. Kafka's
/// `ConsumerMembershipManager.leaveGroupEpoch`: a static member that closes
/// with the default operation keeps its place in the group with `-2`.
pub(crate) fn leave_group_epoch(
    group_instance_id: Option<&str>,
    operation: crate::consumer::GroupMembershipOperation,
) -> i32 {
    if operation == crate::consumer::GroupMembershipOperation::LeaveGroup {
        return LEAVE_GROUP_MEMBER_EPOCH;
    }
    if group_instance_id.is_some() {
        LEAVE_GROUP_STATIC_MEMBER_EPOCH
    } else {
        LEAVE_GROUP_MEMBER_EPOCH
    }
}

/// Whether a closing member sends the heartbeat that leaves the group. Kafka's
/// `ConsumerHeartbeatRequestManager.shouldSendLeaveHeartbeatNow`: a dynamic
/// member that closes with `REMAIN_IN_GROUP` sends none and waits for the
/// session timeout.
pub(crate) fn should_send_leave_heartbeat(
    member_id: &str,
    group_instance_id: Option<&str>,
    operation: crate::consumer::GroupMembershipOperation,
) -> bool {
    if member_id.is_empty() {
        return false;
    }
    !(group_instance_id.is_none()
        && operation == crate::consumer::GroupMembershipOperation::RemainInGroup)
}

/// The partitions of an assignment, by topic name.
///
/// A topic id that the metadata does not name yet stays unresolved, and the
/// member reconciles it after the next metadata refresh, as Kafka's
/// `AbstractMembershipManager` keeps unresolved assignments.
pub(crate) fn assignment_partitions(
    response: &ConsumerGroupHeartbeatResponse,
    names: &HashMap<WireUuid, String>,
) -> Option<ResolvedAssignment> {
    let assignment = response.assignment.as_ref()?;
    let mut partitions = Vec::new();
    let mut unresolved = HashSet::new();
    for topic in &assignment.topic_partitions {
        match names.get(&topic.topic_id) {
            Some(name) => partitions.extend(
                topic
                    .partitions
                    .iter()
                    .map(|partition| (name.clone(), *partition)),
            ),
            None => {
                unresolved.insert(topic.topic_id);
            }
        }
    }
    partitions.sort();
    Some((partitions, unresolved))
}

/// The owned partitions by topic id, for the next heartbeat.
pub(crate) fn owned_by_topic_id(
    owned: &[(String, i32)],
    topic_ids: &HashMap<String, WireUuid>,
) -> OwnedByTopicId {
    let mut by_id: OwnedByTopicId = BTreeMap::new();
    for (topic, partition) in owned {
        if let Some(topic_id) = topic_ids.get(topic) {
            by_id.entry(topic_id.0).or_default().push(*partition);
        }
    }
    for partitions in by_id.values_mut() {
        partitions.sort_unstable();
    }
    by_id
}

/// Run the member of a consumer group with the KIP-848 protocol.
///
/// The task sends a `ConsumerGroupHeartbeat` at once and then each heartbeat
/// interval that the coordinator returns. Each response can carry a new
/// assignment, which the task reconciles before the next heartbeat
/// acknowledges it. Kafka's `ConsumerHeartbeatRequestManager` and
/// `ConsumerMembershipManager` do the same.
pub(crate) async fn run(
    mut state: crate::coordinator::CoordinatorState,
    shutdown: tokio_util::sync::CancellationToken,
) {
    if let Err(error) = state.client.refresh_metadata().await {
        tracing::warn!(%error, "coordinator client metadata refresh failed at startup");
    }
    let mut interval = state.heartbeat_interval.to_std();
    let mut send_now = true;
    loop {
        if !send_now {
            let waited = tokio::select! {
                () = shutdown.cancelled() => break,
                () = tokio::time::sleep(interval) => true,
                changed = state.subscription_changes.changed() => changed.is_ok(),
            };
            if !waited {
                break;
            }
        }
        send_now = false;
        match heartbeat_once(&mut state).await {
            HeartbeatOutcome::Interval(next) => interval = next,
            HeartbeatOutcome::Reconciled(next) => {
                interval = next;
                // Kafka acknowledges a new assignment with the next heartbeat,
                // which it sends at once.
                send_now = true;
            }
            HeartbeatOutcome::Backoff => {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    () = tokio::time::sleep(state.retry_policy.initial_backoff) => {}
                }
                send_now = true;
            }
            HeartbeatOutcome::Stop => break,
        }
    }
    let close = *state.close_operation.borrow();
    if should_send_leave_heartbeat(
        &state.member_id,
        state.group_instance_id.as_deref(),
        close.operation,
    ) {
        let epoch = leave_group_epoch(state.group_instance_id.as_deref(), close.operation);
        state.member_epoch = epoch;
        let leave_timeout = state.leave_group_timeout.to_std();
        let leave = send_heartbeat(&mut state, HeartbeatKind::Leave).await;
        let _ = tokio::time::timeout(leave_timeout, leave.send()).await;
    }
}

/// What the task does after one heartbeat.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeartbeatOutcome {
    /// Wait this interval for the next heartbeat.
    Interval(std::time::Duration),
    /// A new assignment was reconciled: acknowledge it at once, then wait this
    /// interval.
    Reconciled(std::time::Duration),
    /// Wait the retry backoff, then send again.
    Backoff,
    /// Stop the task.
    Stop,
}

/// Send one heartbeat and act on the response.
async fn heartbeat_once(state: &mut crate::coordinator::CoordinatorState) -> HeartbeatOutcome {
    let kind = if state.member_epoch == JOIN_GROUP_MEMBER_EPOCH {
        HeartbeatKind::Join
    } else {
        HeartbeatKind::Interval
    };
    let response = match send_heartbeat(state, kind).await.send().await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, group = %state.group_id, "consumer group heartbeat failed");
            // The member does not know what reached the coordinator, so the
            // next heartbeat sends every field again. Kafka's
            // `ConsumerHeartbeatRequestManager.resetHeartbeatState`.
            state.sent_heartbeat_fields = SentFields::default();
            if crate::coordinator::is_retriable_transport_error(&error) {
                state.client.evict_broker(
                    state
                        .coordinator_id
                        .load(std::sync::atomic::Ordering::Relaxed),
                );
            }
            return HeartbeatOutcome::Backoff;
        }
    };
    let interval = heartbeat_interval(response.heartbeat_interval_ms, state.heartbeat_interval);
    match heartbeat_action(response.error_code) {
        HeartbeatAction::Ok => {}
        HeartbeatAction::Retry => return HeartbeatOutcome::Backoff,
        HeartbeatAction::FindCoordinator => {
            crate::coordinator::find_coordinator_again(state).await;
            return HeartbeatOutcome::Backoff;
        }
        HeartbeatAction::Fenced => {
            tracing::warn!(
                group = %state.group_id,
                error_code = response.error_code,
                "the coordinator fenced the member; giving up the partitions and joining again"
            );
            state.member_epoch = JOIN_GROUP_MEMBER_EPOCH;
            state.sent_heartbeat_fields = SentFields::default();
            crate::coordinator::forget_member_keeping_id(state).await;
            return HeartbeatOutcome::Backoff;
        }
        HeartbeatAction::Fatal => {
            tracing::error!(
                group = %state.group_id,
                error_code = response.error_code,
                error = ?response.error_message,
                "the consumer group heartbeat failed with a fatal error"
            );
            crate::coordinator::report_rejoin_error(
                &state.poll_error,
                crate::error::ConsumerError::Server(response.error_code),
            );
            return HeartbeatOutcome::Stop;
        }
    }
    if let Some(member_id) = response.member_id.clone().filter(|id| !id.is_empty()) {
        state.member_id = member_id;
    }
    state.member_epoch = response.member_epoch;
    crate::coordinator::publish_member_epoch(state).await;
    let names = crate::offset_wire::id_to_name(&state.topic_ids.lock().await.clone());
    let Some((mut target, mut unresolved)) = assignment_partitions(&response, &names) else {
        state.rebalance_pending.send_replace(false);
        return HeartbeatOutcome::Interval(interval);
    };
    if !unresolved.is_empty() {
        // The assignment names its topics by id, so the member needs the ids
        // of the metadata. Kafka's `Metadata` holds them for the consumer, and
        // `ConsumerMembershipManager` keeps an unresolved assignment until a
        // metadata update names its topics.
        refresh_topic_ids(state).await;
        let names = crate::offset_wire::id_to_name(&state.topic_ids.lock().await.clone());
        if let Some(resolved) = assignment_partitions(&response, &names) {
            (target, unresolved) = resolved;
        }
    }
    if !unresolved.is_empty() {
        tracing::info!(
            group = %state.group_id,
            topics = unresolved.len(),
            "the assignment names topic ids that the metadata does not name yet"
        );
    }
    match reconcile(state, &target).await {
        // Kafka acknowledges only a reconciliation that moved partitions. An
        // assignment that repeats what the member owns goes out again with the
        // next interval heartbeat.
        Ok(true) => HeartbeatOutcome::Reconciled(interval),
        Ok(false) => HeartbeatOutcome::Interval(interval),
        Err(error) => {
            tracing::warn!(%error, group = %state.group_id, "the reconciliation of the assignment failed");
            HeartbeatOutcome::Backoff
        }
    }
}

/// Which heartbeat the member sends. Kafka sends every field only while the
/// member joins (`MemberState.JOINING`), and a member that leaves gives up its
/// assignment before the request
/// (`AbstractMembershipManager.transitionToSendingLeaveGroup` sets
/// `currentAssignment` to `NONE`), so the leave heartbeat carries an empty
/// partition list.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeartbeatKind {
    /// The first heartbeat of a member that joins the group.
    Join,
    /// A heartbeat of a member of the group.
    Interval,
    /// The heartbeat that leaves the group.
    Leave,
}

/// Send the heartbeat of the current member state.
///
/// The fields that the request carried stay in `sent_heartbeat_fields`, so the
/// next heartbeat sends only what changed. Kafka's
/// `ConsumerHeartbeatRequestManager.HeartbeatState.buildRequestData` keeps them
/// in the same way.
async fn send_heartbeat(
    state: &mut crate::coordinator::CoordinatorState,
    kind: HeartbeatKind,
) -> HeartbeatCall<'_> {
    let topic_ids = state.topic_ids.lock().await.clone();
    let owned = if kind == HeartbeatKind::Leave {
        OwnedByTopicId::new()
    } else {
        owned_by_topic_id(&state.assigned.lock().await, &topic_ids)
    };
    let subscribed_topic_names = state.subscription.borrow().topics.clone();
    let mut sent = std::mem::take(&mut state.sent_heartbeat_fields);
    let request = build_heartbeat(
        &HeartbeatFields {
            group_id: &state.group_id,
            member_id: &state.member_id,
            member_epoch: state.member_epoch,
            instance_id: state.group_instance_id.as_deref(),
            rack_id: state.client_rack.as_deref(),
            rebalance_timeout_ms: crate::consumer::protocol_millis_i32(state.max_poll_interval),
            subscribed_topic_names,
            server_assignor: state.server_assignor.as_deref(),
            owned,
            joining: kind == HeartbeatKind::Join,
        },
        &mut sent,
    );
    state.sent_heartbeat_fields = sent;
    let broker = state.client.broker(
        state
            .coordinator_id
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    HeartbeatCall { broker, request }
}

/// One `ConsumerGroupHeartbeat` that is ready to go out.
struct HeartbeatCall<'a> {
    broker: krabka_client_core::BrokerHandle<'a>,
    request: ConsumerGroupHeartbeatRequest,
}

impl HeartbeatCall<'_> {
    async fn send(self) -> Result<ConsumerGroupHeartbeatResponse, krabka_client_core::ClientError> {
        self.broker.send(self.request).await
    }
}

/// Store the topic ids of the metadata, so the member can resolve the topics
/// of its assignment.
async fn refresh_topic_ids(state: &crate::coordinator::CoordinatorState) {
    match state.client.refresh_metadata().await {
        Ok(metadata) => {
            crate::validate::refresh_tracked_topic_ids(
                &mut *state.topic_ids.lock().await,
                &metadata,
            );
        }
        Err(error) => {
            tracing::warn!(%error, group = %state.group_id, "metadata refresh failed");
        }
    }
}

/// The heartbeat interval of a response, or the configured one when the
/// response names none.
fn heartbeat_interval(
    heartbeat_interval_ms: i32,
    configured: krabka_units::Time,
) -> std::time::Duration {
    if heartbeat_interval_ms > 0 {
        std::time::Duration::from_millis(u64::try_from(heartbeat_interval_ms).unwrap_or(0))
    } else {
        configured.to_std()
    }
}

/// Reconcile `target` with the partitions that the member owns. Returns
/// `true` when the member gave up or took a partition.
///
/// Kafka's `AbstractMembershipManager.reconcile` gives up the partitions that
/// the assignment no longer holds, with `onPartitionsRevoked`, and then takes
/// the new ones with `onPartitionsAssigned`.
async fn reconcile(
    state: &mut crate::coordinator::CoordinatorState,
    target: &[(String, i32)],
) -> Result<bool, crate::error::ConsumerError> {
    let owned = state.assigned.lock().await.clone();
    let target_set: HashSet<(String, i32)> = target.iter().cloned().collect();
    let owned_set: HashSet<(String, i32)> = owned.iter().cloned().collect();
    let mut revoked: Vec<(String, i32)> = owned
        .iter()
        .filter(|partition| !target_set.contains(*partition))
        .cloned()
        .collect();
    let mut added: Vec<(String, i32)> = target
        .iter()
        .filter(|partition| !owned_set.contains(*partition))
        .cloned()
        .collect();
    revoked.sort();
    added.sort();
    if revoked.is_empty() && added.is_empty() {
        state.rebalance_pending.send_replace(false);
        return Ok(false);
    }
    state.rebalance_pending.send_replace(true);
    let epoch = state.member_epoch;
    if !revoked.is_empty() {
        crate::coordinator::call_listener_kind(
            state,
            crate::rebalance_listener::ListenerCallKind::Revoked,
            revoked.clone(),
        )
        .await?;
        let kept: Vec<(String, i32)> = owned
            .iter()
            .filter(|partition| target_set.contains(*partition))
            .cloned()
            .collect();
        crate::coordinator::publish_group_assignment(state, &kept, epoch).await;
    }
    crate::coordinator::prime_added_offsets(state, &added).await?;
    let gate = crate::coordinator::assigned_callback_gate(state, &added);
    crate::coordinator::publish_group_assignment(state, target, epoch).await;
    let result = crate::coordinator::call_listener_kind(
        state,
        crate::rebalance_listener::ListenerCallKind::Assigned,
        added,
    )
    .await;
    drop(gate);
    state.rebalance_pending.send_replace(false);
    result.map(|()| true)
}

#[cfg(test)]
mod group_protocol_tests {
    use std::sync::{Arc, Mutex};

    use bytes::Buf as _;
    use krabka_client_core::MockBroker;
    use krabka_protocol::{
        Decode as _,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions as AssignedPartitions,
            consumer_group_heartbeat_request,
            consumer_group_heartbeat_response::Assignment,
            find_coordinator_request,
            find_coordinator_response::FindCoordinatorResponse,
            metadata_request,
            metadata_response::{
                MetadataResponse, MetadataResponsePartition, MetadataResponseTopic,
            },
            offset_fetch_request,
            offset_fetch_response::{
                OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
            },
        },
    };
    use krabka_units::{millis, secs};

    use super::*;
    use crate::{CloseOptions, Consumer, consumer::GroupMembershipOperation};

    const TOPIC: &str = "orders";
    const TOPIC_ID: WireUuid = WireUuid([3; 16]);

    /// One `ConsumerGroupHeartbeat` answer of the mock coordinator.
    #[derive(Clone, Debug)]
    struct Answer {
        error_code: i16,
        member_epoch: i32,
        /// The assigned partitions of `orders`, or `None` for a response
        /// without an assignment.
        assignment: Option<Vec<i32>>,
    }

    /// An answer that assigns `partitions` at `member_epoch`.
    fn assigns(member_epoch: i32, partitions: &[i32]) -> Answer {
        Answer {
            error_code: 0,
            member_epoch,
            assignment: Some(partitions.to_vec()),
        }
    }

    /// An answer that fails with `error_code`.
    fn fails(error_code: i16) -> Answer {
        Answer {
            error_code,
            member_epoch: 0,
            assignment: None,
        }
    }

    fn encode(response: &impl krabka_protocol::Encode, version: i16) -> Vec<u8> {
        let mut buffer = bytes::BytesMut::new();
        response.encode(&mut buffer, version).expect("encode");
        buffer.to_vec()
    }

    /// The response of a flexible api, after the tagged fields of its header.
    /// `ApiVersions` keeps the header of version 0, as Kafka does.
    fn flexible(response: &impl krabka_protocol::Encode, version: i16) -> Vec<u8> {
        let mut frame = vec![0];
        frame.extend(encode(response, version));
        frame
    }

    /// The `ApiVersions` of a coordinator that speaks `ConsumerGroupHeartbeat`.
    fn api_versions() -> ApiVersionsResponse {
        ApiVersionsResponse {
            api_keys: [
                (api_versions_request::API_KEY, 3),
                (metadata_request::API_KEY, 12),
                (find_coordinator_request::API_KEY, 2),
                (offset_fetch_request::API_KEY, 5),
                (consumer_group_heartbeat_request::API_KEY, 0),
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
        }
    }

    /// The metadata of `orders`, which names the topic id of the assignment.
    fn metadata() -> MetadataResponse {
        MetadataResponse {
            topics: vec![MetadataResponseTopic {
                name: Some(TOPIC.into()),
                topic_id: TOPIC_ID,
                partitions: (0..2)
                    .map(|partition_index| MetadataResponsePartition {
                        partition_index,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// The committed offsets of the group: `orders` has none, so the member
    /// starts its partitions at the `auto_offset_reset`.
    fn no_committed_offsets() -> OffsetFetchResponse {
        OffsetFetchResponse {
            topics: vec![OffsetFetchResponseTopic {
                name: TOPIC.into(),
                partitions: (0..2)
                    .map(|partition_index| OffsetFetchResponsePartition {
                        partition_index,
                        committed_offset: -1,
                        committed_leader_epoch: -1,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// The `ConsumerGroupHeartbeat` request of a frame, without its header.
    fn heartbeat_request(mut body: &[u8], version: i16) -> ConsumerGroupHeartbeatRequest {
        let client_id_len = usize::try_from(body.get_i16()).expect("client id length");
        body.advance(client_id_len);
        // The flexible request header ends with its tagged fields.
        body.advance(1);
        ConsumerGroupHeartbeatRequest::decode(&mut body, version).expect("decode")
    }

    /// A coordinator that answers each `ConsumerGroupHeartbeat` with the next
    /// entry of `answers`, repeats the last entry after that, and records every
    /// request that it decoded.
    async fn heartbeat_coordinator(
        answers: Vec<Answer>,
        sent: &Arc<Mutex<Vec<ConsumerGroupHeartbeatRequest>>>,
    ) -> MockBroker {
        let sent = Arc::clone(sent);
        let answers = Mutex::new(answers.into_iter());
        let last = Mutex::new(assigns(1, &[]));
        MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(encode(&api_versions(), 0));
            }
            if api_key == find_coordinator_request::API_KEY {
                return Some(encode(&FindCoordinatorResponse::default(), version));
            }
            if api_key == metadata_request::API_KEY {
                return Some(flexible(&metadata(), version));
            }
            if api_key == offset_fetch_request::API_KEY {
                return Some(encode(&no_committed_offsets(), version));
            }
            if api_key != consumer_group_heartbeat_request::API_KEY {
                return None;
            }
            sent.lock()
                .expect("sent lock")
                .push(heartbeat_request(body, version));
            let mut last = last.lock().expect("last lock");
            if let Some(next) = answers.lock().expect("answers lock").next() {
                *last = next;
            }
            let response = ConsumerGroupHeartbeatResponse {
                error_code: last.error_code,
                member_epoch: last.member_epoch,
                heartbeat_interval_ms: 20,
                assignment: last.assignment.clone().map(|partitions| Assignment {
                    topic_partitions: vec![AssignedPartitions {
                        topic_id: TOPIC_ID,
                        partitions,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            };
            Some(flexible(&response, version))
        })
        .await
    }

    /// What one case observed of the member.
    #[derive(Debug, Eq, PartialEq)]
    struct Observed {
        /// The owned partitions that the heartbeats carried. A heartbeat that
        /// repeats the field of the heartbeat before it carries none, so the
        /// list holds one entry for each change.
        acknowledged: Vec<Vec<i32>>,
        /// The partitions of `orders` that the member owned before it closed.
        owned: Vec<i32>,
        /// The member epoch of the last heartbeat.
        last_epoch: i32,
        /// How many heartbeats after the first one joined the group again.
        rejoins: usize,
    }

    /// The owned partitions that each heartbeat of `requests` carried.
    fn acknowledged(requests: &[ConsumerGroupHeartbeatRequest]) -> Vec<Vec<i32>> {
        requests
            .iter()
            .filter_map(|request| request.topic_partitions.as_ref())
            .map(|topics| {
                topics
                    .iter()
                    .flat_map(|topic| topic.partitions.clone())
                    .collect()
            })
            .collect()
    }

    /// Run one member against `answers` until its heartbeats carried `acks`
    /// partition lists, then close it.
    async fn run_member(answers: Vec<Answer>, instance_id: Option<&str>, acks: usize) -> Observed {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let mock = heartbeat_coordinator(answers, &sent).await;
        let mut consumer = Consumer::builder()
            .bootstrap(mock.addr.to_string())
            .group_id("group-a")
            .subscribe([TOPIC.to_owned()])
            .group_protocol(GroupProtocol::Consumer)
            .group_remote_assignor("uniform")
            .heartbeat_interval(millis(20))
            .request_timeout(secs(5))
            .maybe_group_instance_id(instance_id.map(str::to_owned))
            .build()
            .await
            .expect("build");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        while acknowledged(&sent.lock().expect("sent lock")).len() < acks
            && tokio::time::Instant::now() < deadline
        {
            let _ = consumer.poll(millis(20)).await;
        }
        let owned: Vec<i32> = consumer
            .assignment()
            .await
            .into_iter()
            .map(|(_, partition)| partition)
            .collect();
        consumer
            .close_with(CloseOptions::group_membership_operation(
                GroupMembershipOperation::Default,
            ))
            .await
            .expect("close");
        mock.stop();
        let requests = sent.lock().expect("sent lock").clone();
        Observed {
            acknowledged: acknowledged(&requests),
            owned,
            last_epoch: *requests
                .last()
                .map(|request| &request.member_epoch)
                .expect("a last heartbeat"),
            rejoins: requests
                .iter()
                .skip(1)
                .filter(|request| request.member_epoch == JOIN_GROUP_MEMBER_EPOCH)
                .count(),
        }
    }

    /// The first heartbeat of a member joins with epoch 0 and carries every
    /// field: Kafka's `ConsumerHeartbeatRequestManager.HeartbeatState`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_first_heartbeat_joins_the_group_with_every_field() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let mock = heartbeat_coordinator(vec![assigns(1, &[0, 1])], &sent).await;
        let mut consumer = Consumer::builder()
            .bootstrap(mock.addr.to_string())
            .group_id("group-a")
            .subscribe([TOPIC.to_owned()])
            .group_protocol(GroupProtocol::Consumer)
            .group_remote_assignor("uniform")
            .heartbeat_interval(millis(20))
            .request_timeout(secs(5))
            .build()
            .await
            .expect("build");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        while consumer.assignment().await.is_empty() && tokio::time::Instant::now() < deadline {
            let _ = consumer.poll(millis(20)).await;
        }
        consumer.close().await.expect("close");
        mock.stop();
        let first = sent.lock().expect("sent lock").first().cloned();
        let first = first.expect("a first heartbeat");
        assert2::assert!(
            ConsumerGroupHeartbeatRequest {
                member_id: String::new(),
                ..first.clone()
            } == ConsumerGroupHeartbeatRequest {
                group_id: "group-a".to_owned(),
                member_id: String::new(),
                member_epoch: JOIN_GROUP_MEMBER_EPOCH,
                rebalance_timeout_ms: 300_000,
                subscribed_topic_names: Some(vec![TOPIC.to_owned()]),
                server_assignor: Some("uniform".to_owned()),
                topic_partitions: Some(Vec::new()),
                ..Default::default()
            }
        );
        // Kafka's `AsyncKafkaConsumer` generates a v4 uuid for the member id.
        assert2::assert!(uuid::Uuid::parse_str(&first.member_id).is_ok());
    }

    /// The member reconciles each assignment of the coordinator, acknowledges
    /// it with the partitions that it owns, gives up every partition when the
    /// coordinator fences it, and leaves the group with the epoch of its
    /// membership.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_member_reconciles_and_leaves_as_kafka_does() {
        struct Case {
            name: &'static str,
            answers: Vec<Answer>,
            instance_id: Option<&'static str>,
            /// How many partition lists the heartbeats carry before the close.
            acks: usize,
        }
        let cases = [
            Case {
                name: "an assignment",
                answers: vec![assigns(1, &[0, 1])],
                instance_id: None,
                acks: 2,
            },
            Case {
                name: "an assignment that revokes a partition",
                answers: vec![assigns(1, &[0, 1]), assigns(2, &[0])],
                instance_id: None,
                acks: 3,
            },
            Case {
                name: "a fenced member",
                answers: vec![
                    assigns(1, &[0, 1]),
                    fails(FENCED_MEMBER_EPOCH),
                    assigns(1, &[0, 1]),
                ],
                instance_id: None,
                acks: 4,
            },
            Case {
                name: "a static member",
                answers: vec![assigns(1, &[0, 1])],
                instance_id: Some("member-a"),
                acks: 2,
            },
        ];
        let mut actual = Vec::new();
        for case in cases {
            actual.push((
                case.name,
                run_member(case.answers, case.instance_id, case.acks).await,
            ));
        }
        assert2::assert!(
            actual
                == vec![
                    (
                        "an assignment",
                        Observed {
                            // The join carries no partitions, the next
                            // heartbeat acknowledges the assignment, and the
                            // heartbeat that leaves gives the partitions up.
                            acknowledged: vec![vec![], vec![0, 1], vec![]],
                            owned: vec![0, 1],
                            last_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                            rejoins: 0,
                        },
                    ),
                    (
                        "an assignment that revokes a partition",
                        Observed {
                            acknowledged: vec![vec![], vec![0, 1], vec![0], vec![]],
                            owned: vec![0],
                            last_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                            rejoins: 0,
                        },
                    ),
                    (
                        "a fenced member",
                        Observed {
                            // The fence releases the partitions, and the member
                            // joins again with epoch 0 and no partitions.
                            acknowledged: vec![vec![], vec![0, 1], vec![], vec![0, 1], vec![]],
                            owned: vec![0, 1],
                            last_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                            rejoins: 1,
                        },
                    ),
                    (
                        "a static member",
                        Observed {
                            acknowledged: vec![vec![], vec![0, 1], vec![]],
                            owned: vec![0, 1],
                            // A static member that closes with the default
                            // operation keeps its place in the group.
                            last_epoch: LEAVE_GROUP_STATIC_MEMBER_EPOCH,
                            rejoins: 0,
                        },
                    ),
                ]
        );
    }
}

#[cfg(test)]
mod tests {
    use krabka_protocol::owned::{
        common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions as ResponseTopicPartitions,
        consumer_group_heartbeat_response::Assignment,
    };

    use super::*;
    use crate::consumer::GroupMembershipOperation;

    const TOPIC_ID: WireUuid = WireUuid([7; 16]);

    fn fields<'a>(member_epoch: i32, joining: bool, owned: OwnedByTopicId) -> HeartbeatFields<'a> {
        HeartbeatFields {
            group_id: "group-a",
            member_id: "member-a",
            member_epoch,
            instance_id: None,
            rack_id: Some("az-1"),
            rebalance_timeout_ms: 300_000,
            subscribed_topic_names: vec!["orders".to_owned()],
            server_assignor: Some("uniform"),
            owned,
            joining,
        }
    }

    /// Kafka's `HeartbeatState.buildRequestData` sends every field when the
    /// member joins, and after that only the fields that changed.
    #[test]
    fn a_heartbeat_repeats_only_the_fields_that_changed() {
        let mut sent = SentFields::default();
        let join = build_heartbeat(&fields(0, true, BTreeMap::new()), &mut sent);
        let steady = build_heartbeat(&fields(1, false, BTreeMap::new()), &mut sent);
        let owned = build_heartbeat(
            &fields(1, false, BTreeMap::from([(TOPIC_ID.0, vec![0, 1])])),
            &mut sent,
        );
        assert2::assert!(
            (
                join.rack_id.clone(),
                join.rebalance_timeout_ms,
                join.subscribed_topic_names.clone(),
                join.server_assignor.clone(),
                join.topic_partitions.clone(),
            ) == (
                Some("az-1".to_owned()),
                300_000,
                Some(vec!["orders".to_owned()]),
                Some("uniform".to_owned()),
                Some(Vec::new()),
            )
        );
        assert2::assert!(
            (
                steady.rack_id,
                steady.rebalance_timeout_ms,
                steady.subscribed_topic_names,
                steady.server_assignor,
                steady.topic_partitions,
            ) == (None, -1, None, None, None)
        );
        assert2::assert!(
            owned.topic_partitions
                == Some(vec![TopicPartitions {
                    topic_id: TOPIC_ID,
                    partitions: vec![0, 1],
                    ..Default::default()
                }])
        );
    }

    /// Kafka's heartbeat error classes.
    #[test]
    fn heartbeat_errors_follow_kafkas_classes() {
        let actual = [0, 14, 15, 16, 25, 110, 111, 112, 81, 42, 35, 30].map(heartbeat_action);
        assert2::assert!(
            actual
                == [
                    HeartbeatAction::Ok,
                    HeartbeatAction::Retry,
                    HeartbeatAction::FindCoordinator,
                    HeartbeatAction::FindCoordinator,
                    HeartbeatAction::Fenced,
                    HeartbeatAction::Fenced,
                    HeartbeatAction::Fatal,
                    HeartbeatAction::Fatal,
                    HeartbeatAction::Fatal,
                    HeartbeatAction::Fatal,
                    HeartbeatAction::Fatal,
                    HeartbeatAction::Fatal,
                ]
        );
    }

    /// Kafka's `leaveGroupEpoch` and `shouldSendLeaveHeartbeatNow`.
    #[test]
    fn a_leaving_member_follows_kafkas_epochs() {
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, instance_id, operation, expected) in [
            (
                "dynamic, default",
                None,
                GroupMembershipOperation::Default,
                (-1, true),
            ),
            (
                "static, default",
                Some("i-1"),
                GroupMembershipOperation::Default,
                (-2, true),
            ),
            (
                "static, leave group",
                Some("i-1"),
                GroupMembershipOperation::LeaveGroup,
                (-1, true),
            ),
            (
                "dynamic, remain in group",
                None,
                GroupMembershipOperation::RemainInGroup,
                (-1, false),
            ),
            (
                "static, remain in group",
                Some("i-1"),
                GroupMembershipOperation::RemainInGroup,
                (-2, true),
            ),
        ] {
            actual.push((
                name,
                (
                    leave_group_epoch(instance_id, operation),
                    should_send_leave_heartbeat("member-a", instance_id, operation),
                ),
            ));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// The assignment names its topics by id. An id that the metadata does not
    /// name yet stays unresolved.
    #[test]
    fn an_assignment_resolves_the_topic_ids_of_the_metadata() {
        let other = WireUuid([9; 16]);
        let response = ConsumerGroupHeartbeatResponse {
            assignment: Some(Assignment {
                topic_partitions: vec![
                    ResponseTopicPartitions {
                        topic_id: TOPIC_ID,
                        partitions: vec![1, 0],
                        ..Default::default()
                    },
                    ResponseTopicPartitions {
                        topic_id: other,
                        partitions: vec![0],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        let names = HashMap::from([(TOPIC_ID, "orders".to_owned())]);
        assert2::assert!(
            assignment_partitions(&response, &names)
                == Some((
                    vec![("orders".to_owned(), 0), ("orders".to_owned(), 1)],
                    HashSet::from([other])
                ))
        );
        assert2::assert!(
            assignment_partitions(&ConsumerGroupHeartbeatResponse::default(), &names) == None
        );
    }
}
