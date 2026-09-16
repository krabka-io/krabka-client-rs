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
        metadata_request::{MetadataRequest, MetadataRequestTopic},
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
/// `UNSUPPORTED_ASSIGNOR`.
#[cfg(test)]
const UNSUPPORTED_ASSIGNOR: i16 = 112;

/// Kafka's message for a regular expression subscription of a classic member:
/// `ClassicKafkaConsumer.subscribe(SubscriptionPattern)`.
pub(crate) const RE2J_NEEDS_CONSUMER_PROTOCOL: &str = "Subscribe to RE2/J pattern is not supported when using the CLASSIC protocol defined in config group.protocol";

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
    regex: Option<String>,
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
    /// The subscribed topics, sorted. A regular expression subscription sends
    /// none, because the coordinator matches the expression.
    pub subscribed_topic_names: Vec<String>,
    /// The regular expression of a broker-side pattern subscription.
    pub regex: Option<String>,
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
    // Kafka sends an empty expression to remove a pattern subscription, and
    // sends the expression again only when it changed or when the member
    // joins with one.
    let pattern_changed = sent.regex != fields.regex;
    if (all && fields.regex.is_some()) || pattern_changed {
        request.subscribed_topic_regex = Some(fields.regex.clone().unwrap_or_default());
        sent.regex.clone_from(&fields.regex);
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
    // Kafka's `Heartbeat.pollTimer`: `poll` resets it, and the member leaves
    // the group when it expires.
    state.poll_timer = crate::coordinator::PollTimer::new(state.max_poll_interval);
    let mut interval = state.heartbeat_interval.to_std();
    let mut send_now = true;
    let mut polls_open = true;
    // `true` after the poll timer expired: the member left the group and waits
    // for a `poll` before it joins again. Kafka's `MemberState.STALE`.
    let mut left_on_poll_timeout = false;
    let mut last_pattern_check: Option<tokio::time::Instant> = None;
    loop {
        if !send_now {
            let event = tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                () = tokio::time::sleep(interval), if !left_on_poll_timeout => TaskEvent::Heartbeat,
                changed = state.polls.changed(), if polls_open => {
                    if changed.is_err() {
                        polls_open = false;
                        continue;
                    }
                    TaskEvent::Poll
                }
                changed = state.subscription_changes.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    TaskEvent::Subscription
                }
                () = tokio::time::sleep_until(state.poll_timer.deadline()), if !left_on_poll_timeout => {
                    TaskEvent::PollTimeout
                }
            };
            match event {
                TaskEvent::Poll => {
                    state.poll_timer.reset();
                    if !left_on_poll_timeout {
                        continue;
                    }
                    // The `poll` came back: join the group again, as Kafka's
                    // member leaves the stale state when the poll timer is
                    // reset.
                    left_on_poll_timeout = false;
                    state.member_epoch = JOIN_GROUP_MEMBER_EPOCH;
                    state.sent_heartbeat_fields = SentFields::default();
                }
                TaskEvent::PollTimeout => {
                    leave_after_poll_timeout(&mut state).await;
                    left_on_poll_timeout = true;
                    continue;
                }
                TaskEvent::Subscription => {
                    // The task has its own client. Its metadata requests name
                    // the topics of the new subscription, so it can resolve
                    // the topic ids of the next assignment.
                    let topics = state.subscription.borrow().topics.clone();
                    if !topics.is_empty() {
                        state.client.metadata_topics().set(topics);
                    }
                }
                TaskEvent::Heartbeat => {}
            }
        }
        // Kafka's `AsyncKafkaConsumer.updateAssignmentMetadataIfNeeded` matches
        // a client-side pattern against the metadata in each poll. The member
        // sends the topics that matched, and the coordinator assigns them.
        if state.subscription.borrow().pattern.is_some()
            && last_pattern_check.is_none_or(|last| {
                crate::coordinator::subscription_metadata_refresh_due(
                    last,
                    state.subscription_metadata_refresh_interval,
                )
            })
        {
            last_pattern_check = Some(tokio::time::Instant::now());
            crate::coordinator::refresh_pattern_topics(&mut state).await;
        }
        send_now = false;
        let outcome = tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            outcome = heartbeat_once(&mut state) => outcome,
        };
        match outcome {
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

/// What wakes the heartbeat task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TaskEvent {
    /// The heartbeat interval elapsed.
    Heartbeat,
    /// A `poll` came: reset the poll timer.
    Poll,
    /// The subscription changed.
    Subscription,
    /// The poll timer expired: leave the group.
    PollTimeout,
}

/// Leave the group because the time between two `poll` calls was longer than
/// `max_poll_interval`, and give up every partition.
///
/// Kafka's `AbstractHeartbeatRequestManager.poll` calls
/// `transitionToSendingLeaveGroup(true)` for an expired poll timer, sends the
/// leave heartbeat and then makes the member stale until the next `poll`.
async fn leave_after_poll_timeout(state: &mut crate::coordinator::CoordinatorState) {
    tracing::warn!(
        group = %state.group_id,
        max_poll_interval = ?state.max_poll_interval,
        "consumer poll timeout has expired: the time between two poll calls was longer than \
         max_poll_interval; the member leaves the group and joins again on the next poll"
    );
    state.member_epoch = leave_group_epoch(
        state.group_instance_id.as_deref(),
        crate::consumer::GroupMembershipOperation::Default,
    );
    let leave_timeout = state.leave_group_timeout.to_std();
    let leave = send_heartbeat(state, HeartbeatKind::Leave).await;
    let _ = tokio::time::timeout(leave_timeout, leave.send()).await;
    crate::coordinator::release_partitions_after_poll_timeout(state).await;
    if let Err(error) = crate::coordinator::call_lost_listener(state).await {
        tracing::warn!(%error, group = %state.group_id, "the lost callback failed");
    }
    state.member_epoch = JOIN_GROUP_MEMBER_EPOCH;
    state.sent_heartbeat_fields = SentFields::default();
}

/// Keep the member in the group while `poll` runs a rebalance listener
/// callback. Kafka's heartbeat manager keeps sending `ConsumerGroupHeartbeat`
/// while the application thread runs the callback.
///
/// The request carries only the fields that changed, so it repeats the member
/// state and asks for nothing new. The heartbeat after the callback acts on
/// the response of the member state.
pub(crate) async fn heartbeat_during_callback(
    state: &crate::coordinator::CoordinatorState,
    done: &tokio_util::sync::CancellationToken,
) {
    let interval = state.heartbeat_interval.to_std();
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            () = done.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let mut sent = state.sent_heartbeat_fields.clone();
        let fields = heartbeat_fields(state, HeartbeatKind::Interval).await;
        let request = build_heartbeat(&fields, &mut sent);
        let broker = state.client.broker(
            state
                .coordinator_id
                .load(std::sync::atomic::Ordering::Relaxed),
        );
        let call = HeartbeatCall {
            broker,
            request,
            needs_regex: state.subscription.borrow().regex.is_some(),
        };
        let result = tokio::select! {
            biased;
            () = done.cancelled() => return,
            result = call.send() => result,
        };
        if let Err(error) = result {
            tracing::warn!(%error, group = %state.group_id, "the heartbeat during a rebalance callback failed");
        }
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
            if !crate::coordinator::is_retriable_transport_error(&error) {
                // Kafka's `AbstractHeartbeatRequestManager.onFailure` retries
                // only a `RetriableException`, and hands every other failure
                // to the application. A broker without api key 68 fails here.
                tracing::error!(
                    %error,
                    group = %state.group_id,
                    "the consumer group heartbeat failed for good; the member stops"
                );
                crate::coordinator::report_fatal_error(
                    &state.poll_error,
                    crate::error::ConsumerError::Client(error),
                );
                return HeartbeatOutcome::Stop;
            }
            state.client.evict_broker(
                state
                    .coordinator_id
                    .load(std::sync::atomic::Ordering::Relaxed),
            );
            return HeartbeatOutcome::Backoff;
        }
    };
    let interval = heartbeat_interval(response.heartbeat_interval_ms, state.heartbeat_interval);
    if response.error_code != 0 {
        // Kafka's `AbstractHeartbeatRequestManager.onErrorResponse` resets the
        // sent fields, so the heartbeat after an error carries them again.
        state.sent_heartbeat_fields = SentFields::default();
    }
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
            crate::coordinator::forget_member_keeping_id(state).await;
            // Kafka's `transitionToFenced` gives the partitions to
            // `onPartitionsLost` before the member joins again.
            if let Err(error) = crate::coordinator::call_lost_listener(state).await {
                tracing::warn!(%error, group = %state.group_id, "the lost callback failed");
            }
            return HeartbeatOutcome::Backoff;
        }
        HeartbeatAction::Fatal => {
            tracing::error!(
                group = %state.group_id,
                error_code = response.error_code,
                error = ?response.error_message,
                "the consumer group heartbeat failed with a fatal error"
            );
            crate::coordinator::report_fatal_error(
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
        refresh_topic_ids(state, &unresolved).await;
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
    // The topics of a regular expression subscription come from the
    // assignment, because the coordinator matches the expression. They give
    // `poll` the topics of its metadata and its fetch filter.
    let topics: Vec<String> = target.iter().map(|(topic, _)| topic.clone()).collect();
    state
        .subscription
        .send_if_modified(|subscription| subscription.store_assigned_topics(&topics));
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
    let mut sent = state.sent_heartbeat_fields.clone();
    let needs_regex = state.subscription.borrow().regex.is_some();
    let request = build_heartbeat(&heartbeat_fields(state, kind).await, &mut sent);
    state.sent_heartbeat_fields = sent;
    let broker = state.client.broker(
        state
            .coordinator_id
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    HeartbeatCall {
        broker,
        request,
        needs_regex,
    }
}

/// The heartbeat fields of the current member state.
async fn heartbeat_fields(
    state: &crate::coordinator::CoordinatorState,
    kind: HeartbeatKind,
) -> HeartbeatFields<'_> {
    let topic_ids = state.topic_ids.lock().await.clone();
    let owned = if kind == HeartbeatKind::Leave {
        OwnedByTopicId::new()
    } else {
        owned_by_topic_id(&state.assigned.lock().await, &topic_ids)
    };
    let (subscribed_topic_names, regex) = {
        let subscription = state.subscription.borrow();
        // A regular expression subscription names no topics: Kafka's
        // `SubscriptionState.subscription()` is empty for
        // `AUTO_PATTERN_RE2J`, and the coordinator matches the expression.
        let topics = if subscription.regex.is_some() {
            Vec::new()
        } else {
            subscription.topics.clone()
        };
        (topics, subscription.regex.clone())
    };
    HeartbeatFields {
        group_id: &state.group_id,
        member_id: &state.member_id,
        member_epoch: state.member_epoch,
        instance_id: state.group_instance_id.as_deref(),
        rack_id: state.client_rack.as_deref(),
        rebalance_timeout_ms: crate::consumer::protocol_millis_i32(state.max_poll_interval),
        subscribed_topic_names,
        regex,
        server_assignor: state.server_assignor.as_deref(),
        owned,
        joining: kind == HeartbeatKind::Join,
    }
}

/// A `ConsumerGroupHeartbeat` that carries `subscribed_topic_regex`. The
/// field exists from version 1, so a broker that speaks only version 0 must
/// fail the call in place of dropping the expression.
struct RegexHeartbeat(ConsumerGroupHeartbeatRequest);

impl krabka_protocol::Encode for RegexHeartbeat {
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

impl krabka_protocol::ProtocolRequest for RegexHeartbeat {
    const API_KEY: i16 = krabka_protocol::owned::consumer_group_heartbeat_request::API_KEY;
    const MIN_VERSION: i16 = 1;
    const MAX_VERSION: i16 = krabka_protocol::owned::consumer_group_heartbeat_request::MAX_VERSION;
    const LATEST_STABLE_VERSION: i16 =
        krabka_protocol::owned::consumer_group_heartbeat_request::LATEST_STABLE_VERSION;
    const FLEXIBLE_MIN: i16 =
        krabka_protocol::owned::consumer_group_heartbeat_request::FLEXIBLE_MIN;
    type Response = ConsumerGroupHeartbeatResponse;
}

/// One `ConsumerGroupHeartbeat` that is ready to go out.
struct HeartbeatCall<'a> {
    broker: krabka_client_core::BrokerHandle<'a>,
    request: ConsumerGroupHeartbeatRequest,
    /// `true` when the member subscribes to a regular expression, which needs
    /// version 1.
    needs_regex: bool,
}

impl HeartbeatCall<'_> {
    async fn send(self) -> Result<ConsumerGroupHeartbeatResponse, krabka_client_core::ClientError> {
        if self.needs_regex {
            return self.broker.send(RegexHeartbeat(self.request)).await;
        }
        self.broker.send(self.request).await
    }
}

/// Store the topic ids of the metadata, so the member can resolve the topics
/// of its assignment.
///
/// A regular expression subscription knows no topic names, so the request
/// names the `unresolved` topic ids. Kafka's
/// `ConsumerMetadata.newMetadataRequestBuilder` asks for the assigned topic
/// ids of such a subscription, and for the topic names of every other one.
async fn refresh_topic_ids(
    state: &crate::coordinator::CoordinatorState,
    unresolved: &HashSet<WireUuid>,
) {
    let by_id = state.subscription.borrow().regex.is_some();
    let request = if by_id {
        MetadataRequest {
            topics: Some(
                unresolved
                    .iter()
                    .map(|topic_id| MetadataRequestTopic {
                        topic_id: *topic_id,
                        name: None,
                        ..Default::default()
                    })
                    .collect(),
            ),
            allow_auto_topic_creation: false,
            ..Default::default()
        }
    } else {
        state.client.metadata_topics().request()
    };
    match state.client.refresh_metadata_with(request).await {
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
            fetch_request,
            fetch_response::FetchResponse,
            find_coordinator_request,
            find_coordinator_response::FindCoordinatorResponse,
            list_offsets_request,
            list_offsets_response::{
                ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
            },
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
        /// The member id of the response. The coordinator can name another one
        /// than the request.
        member_id: Option<String>,
    }

    /// An answer that assigns `partitions` at `member_epoch`.
    fn assigns(member_epoch: i32, partitions: &[i32]) -> Answer {
        Answer {
            error_code: 0,
            member_epoch,
            assignment: Some(partitions.to_vec()),
            member_id: None,
        }
    }

    /// `answer` with the member id that the coordinator names.
    fn named(answer: Answer, member_id: &str) -> Answer {
        Answer {
            member_id: Some(member_id.to_owned()),
            ..answer
        }
    }

    /// An answer that fails with `error_code`.
    fn fails(error_code: i16) -> Answer {
        Answer {
            error_code,
            member_epoch: 0,
            assignment: None,
            member_id: None,
        }
    }

    /// What the mock coordinator recorded.
    #[derive(Clone, Default)]
    struct Recorder {
        /// Each `ConsumerGroupHeartbeat` that the mock decoded.
        heartbeats: Arc<Mutex<Vec<ConsumerGroupHeartbeatRequest>>>,
        /// The topic names of each `Metadata` request.
        metadata_topics: Arc<Mutex<Vec<String>>>,
        /// Whether a `Metadata` request named a topic by id.
        asked_by_id: Arc<Mutex<bool>>,
        /// The classic `Heartbeat` requests. A member of the consumer group
        /// protocol sends none.
        classic_heartbeats: Arc<Mutex<usize>>,
    }

    impl Recorder {
        fn heartbeats(&self) -> Vec<ConsumerGroupHeartbeatRequest> {
            self.heartbeats.lock().expect("heartbeats lock").clone()
        }

        /// The owned partitions that each heartbeat carried.
        fn acknowledged(&self) -> Vec<Vec<i32>> {
            acknowledged(&self.heartbeats())
        }

        fn epochs(&self) -> Vec<i32> {
            self.heartbeats()
                .iter()
                .map(|request| request.member_epoch)
                .collect()
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
                (list_offsets_request::API_KEY, 7),
                (fetch_request::API_KEY, 12),
                (consumer_group_heartbeat_request::API_KEY, 1),
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

    /// The log start and end of `orders`: both partitions are empty.
    fn list_offsets() -> ListOffsetsResponse {
        ListOffsetsResponse {
            topics: vec![ListOffsetsTopicResponse {
                name: TOPIC.into(),
                partitions: (0..2)
                    .map(|partition_index| ListOffsetsPartitionResponse {
                        partition_index,
                        timestamp: -1,
                        offset: 0,
                        leader_epoch: -1,
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

    /// The `Metadata` request of a frame, without its header.
    fn metadata_request_of(mut body: &[u8], version: i16) -> MetadataRequest {
        let client_id_len = usize::try_from(body.get_i16()).expect("client id length");
        body.advance(client_id_len);
        if version >= 9 {
            body.advance(1);
        }
        MetadataRequest::decode(&mut body, version).expect("decode")
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
    async fn heartbeat_coordinator(answers: Vec<Answer>, recorder: &Recorder) -> MockBroker {
        heartbeat_coordinator_with(answers, recorder, false).await
    }

    /// The same coordinator. With `by_id_only` it names the topic only for a
    /// `Metadata` request that asks for its id, as a broker does for a group
    /// with a broker-side pattern subscription.
    async fn heartbeat_coordinator_with(
        answers: Vec<Answer>,
        recorder: &Recorder,
        by_id_only: bool,
    ) -> MockBroker {
        let recorder = recorder.clone();
        let answers = Mutex::new(answers.into_iter());
        let last = Mutex::new(assigns(1, &[]));
        MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(encode(&api_versions(), 0));
            }
            if api_key == find_coordinator_request::API_KEY {
                return Some(encode(&FindCoordinatorResponse::default(), version));
            }
            if api_key == CLASSIC_HEARTBEAT_API_KEY {
                *recorder
                    .classic_heartbeats
                    .lock()
                    .expect("classic heartbeats lock") += 1;
                return None;
            }
            if api_key == metadata_request::API_KEY {
                let request = metadata_request_of(body, version);
                let topics = request.topics.unwrap_or_default();
                let by_id = topics
                    .iter()
                    .any(|topic| topic.topic_id != WireUuid::default());
                if by_id {
                    *recorder.asked_by_id.lock().expect("asked lock") = true;
                }
                recorder
                    .metadata_topics
                    .lock()
                    .expect("metadata topics lock")
                    .extend(topics.iter().filter_map(|topic| topic.name.clone()));
                if by_id_only && !by_id {
                    return Some(flexible(&MetadataResponse::default(), version));
                }
                return Some(flexible(&metadata(), version));
            }
            if api_key == offset_fetch_request::API_KEY {
                return Some(encode(&no_committed_offsets(), version));
            }
            if api_key == list_offsets_request::API_KEY {
                return Some(flexible(&list_offsets(), version));
            }
            if api_key == fetch_request::API_KEY {
                return Some(flexible(&FetchResponse::default(), version));
            }
            if api_key != consumer_group_heartbeat_request::API_KEY {
                return None;
            }
            recorder
                .heartbeats
                .lock()
                .expect("heartbeats lock")
                .push(heartbeat_request(body, version));
            let mut last = last.lock().expect("last lock");
            if let Some(next) = answers.lock().expect("answers lock").next() {
                *last = next;
            }
            let response = ConsumerGroupHeartbeatResponse {
                error_code: last.error_code,
                member_epoch: last.member_epoch,
                member_id: last.member_id.clone(),
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

    /// Kafka's `Heartbeat` api key, which the classic protocol uses.
    const CLASSIC_HEARTBEAT_API_KEY: i16 = 12;

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
        let recorder = Recorder::default();
        let mock = heartbeat_coordinator(answers, &recorder).await;
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
        while recorder.acknowledged().len() < acks && tokio::time::Instant::now() < deadline {
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
        let epochs = recorder.epochs();
        Observed {
            acknowledged: recorder.acknowledged(),
            owned,
            last_epoch: *epochs.last().expect("a last heartbeat"),
            rejoins: epochs
                .iter()
                .skip(1)
                .filter(|epoch| **epoch == JOIN_GROUP_MEMBER_EPOCH)
                .count(),
        }
    }

    /// The first heartbeat of a member joins with epoch 0 and carries every
    /// field: Kafka's `ConsumerHeartbeatRequestManager.HeartbeatState`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_first_heartbeat_joins_the_group_with_every_field() {
        let recorder = Recorder::default();
        let mock = heartbeat_coordinator(vec![assigns(1, &[0, 1])], &recorder).await;
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
        let first = recorder.heartbeats().first().cloned();
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

    /// A broker-side pattern subscription sends the regular expression and no
    /// topic names, and the member learns the topic of its assignment from a
    /// metadata request that names the topic id. Kafka's
    /// `subscribe(SubscriptionPattern)` and
    /// `ConsumerMetadata.newMetadataRequestBuilder`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_regular_expression_subscription_resolves_its_topics_by_id() {
        let recorder = Recorder::default();
        let mock = heartbeat_coordinator_with(vec![assigns(1, &[0, 1])], &recorder, true).await;
        let mut consumer = Consumer::builder()
            .bootstrap(mock.addr.to_string())
            .group_id("group-a")
            .subscribe_regex("orders-.*")
            .group_protocol(GroupProtocol::Consumer)
            .heartbeat_interval(millis(20))
            .request_timeout(secs(5))
            .build()
            .await
            .expect("build");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        while consumer.assignment().await.is_empty() && tokio::time::Instant::now() < deadline {
            let _ = consumer.poll(millis(20)).await;
        }
        let assignment = consumer.assignment().await;
        let subscription = consumer.subscription();
        consumer.close().await.expect("close");
        mock.stop();
        let first = recorder.heartbeats().first().cloned();
        let first = first.expect("a first heartbeat");
        assert2::assert!(
            (
                first.subscribed_topic_regex,
                first.subscribed_topic_names,
                *recorder.asked_by_id.lock().expect("asked lock"),
                assignment,
                subscription,
            ) == (
                Some("orders-.*".to_owned()),
                Some(Vec::new()),
                true,
                vec![(TOPIC.to_owned(), 0), (TOPIC.to_owned(), 1)],
                vec![TOPIC.to_owned()],
            )
        );
    }

    /// A client-side pattern subscription matches the metadata itself, and the
    /// member sends the topics that matched. Kafka's
    /// `AsyncKafkaConsumer.updateAssignmentMetadataIfNeeded`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_client_side_pattern_sends_the_topics_that_matched() {
        let recorder = Recorder::default();
        let mock = heartbeat_coordinator(vec![assigns(1, &[0, 1])], &recorder).await;
        let mut consumer = Consumer::builder()
            .bootstrap(mock.addr.to_string())
            .group_id("group-a")
            .subscribe_pattern(crate::TopicPattern::new(|topic| topic.starts_with("ord")))
            .group_protocol(GroupProtocol::Consumer)
            .heartbeat_interval(millis(20))
            .request_timeout(secs(5))
            .build()
            .await
            .expect("build");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        while consumer.assignment().await.is_empty() && tokio::time::Instant::now() < deadline {
            let _ = consumer.poll(millis(20)).await;
        }
        let assignment = consumer.assignment().await;
        consumer.close().await.expect("close");
        mock.stop();
        let first = recorder.heartbeats().first().cloned();
        let first = first.expect("a first heartbeat");
        assert2::assert!(
            (
                first.subscribed_topic_names,
                first.subscribed_topic_regex,
                assignment,
            ) == (
                Some(vec![TOPIC.to_owned()]),
                None,
                vec![(TOPIC.to_owned(), 0), (TOPIC.to_owned(), 1)],
            )
        );
    }

    /// The kind and partitions of one rebalance listener call.
    type ListenerCall = (crate::rebalance_listener::ListenerCallKind, Vec<i32>);

    /// The listener calls of one run.
    type ListenerCalls = Arc<Mutex<Vec<ListenerCall>>>;

    /// A rebalance listener that records its calls, and that can hold the
    /// assign callback for a while.
    struct RecordingListener {
        calls: ListenerCalls,
        assign_delay: std::time::Duration,
    }

    #[async_trait::async_trait]
    impl crate::ConsumerRebalanceListener for RecordingListener {
        async fn on_partitions_revoked(
            &mut self,
            _consumer: &Consumer,
            partitions: &[(String, i32)],
        ) -> Result<(), crate::RebalanceListenerError> {
            self.record(
                crate::rebalance_listener::ListenerCallKind::Revoked,
                partitions,
            );
            Ok(())
        }

        async fn on_partitions_assigned(
            &mut self,
            _consumer: &Consumer,
            partitions: &[(String, i32)],
        ) -> Result<(), crate::RebalanceListenerError> {
            self.record(
                crate::rebalance_listener::ListenerCallKind::Assigned,
                partitions,
            );
            tokio::time::sleep(self.assign_delay).await;
            Ok(())
        }

        async fn on_partitions_lost(
            &mut self,
            _consumer: &Consumer,
            partitions: &[(String, i32)],
        ) -> Result<(), crate::RebalanceListenerError> {
            self.record(
                crate::rebalance_listener::ListenerCallKind::Lost,
                partitions,
            );
            Ok(())
        }
    }

    impl RecordingListener {
        fn record(
            &self,
            kind: crate::rebalance_listener::ListenerCallKind,
            partitions: &[(String, i32)],
        ) {
            self.calls
                .lock()
                .expect("calls lock")
                .push((kind, partitions.iter().map(|(_, index)| *index).collect()));
        }
    }

    /// The first lost callback of `calls`.
    fn first_lost(calls: &ListenerCalls) -> Option<ListenerCall> {
        calls
            .lock()
            .expect("calls lock")
            .iter()
            .find(|(kind, _)| *kind == crate::rebalance_listener::ListenerCallKind::Lost)
            .cloned()
    }

    /// A fatal heartbeat response and a heartbeat that cannot go out both end
    /// the member and reach the application through `poll`. Kafka's
    /// `AbstractHeartbeatRequestManager.onErrorResponse` and `onFailure` hand
    /// such an error to the application thread.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fatal_heartbeat_error_reaches_poll() {
        let mut actual = Vec::new();
        for (name, answers, speaks_heartbeat) in [
            (
                "an unsupported assignor",
                vec![fails(UNSUPPORTED_ASSIGNOR)],
                true,
            ),
            ("a broker without the api", Vec::new(), false),
        ] {
            let recorder = Recorder::default();
            let mock = if speaks_heartbeat {
                heartbeat_coordinator(answers, &recorder).await
            } else {
                classic_only_coordinator().await
            };
            let mut consumer = Consumer::builder()
                .bootstrap(mock.addr.to_string())
                .group_id("group-a")
                .subscribe([TOPIC.to_owned()])
                .group_protocol(GroupProtocol::Consumer)
                .heartbeat_interval(millis(20))
                .request_timeout(secs(5))
                .build()
                .await
                .expect("build");
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
            let mut error = None;
            while error.is_none() && tokio::time::Instant::now() < deadline {
                error = consumer
                    .poll(millis(20))
                    .await
                    .err()
                    .map(|error| error.to_string());
            }
            consumer.close().await.expect("close");
            mock.stop();
            actual.push((name, error));
        }
        assert2::assert!(
            actual
                == vec![
                    (
                        "an unsupported assignor",
                        Some("broker error_code 112".to_owned()),
                    ),
                    (
                        "a broker without the api",
                        Some(
                            "client: incompatible version: broker supports 2..=2, client wants 0..=1 for api_key 68"
                                .to_owned()
                        ),
                    ),
                ]
        );
    }

    /// A broker whose `ConsumerGroupHeartbeat` versions do not overlap the
    /// ones of the client.
    async fn classic_only_coordinator() -> MockBroker {
        coordinator_with_heartbeat_versions(2, 2).await
    }

    /// A coordinator that speaks `ConsumerGroupHeartbeat` in this version
    /// range and answers no heartbeat.
    async fn coordinator_with_heartbeat_versions(min_version: i16, max_version: i16) -> MockBroker {
        MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                let mut versions = api_versions();
                for api in &mut versions.api_keys {
                    if api.api_key == consumer_group_heartbeat_request::API_KEY {
                        api.min_version = min_version;
                        api.max_version = max_version;
                    }
                }
                return Some(encode(&versions, 0));
            }
            if api_key == find_coordinator_request::API_KEY {
                return Some(encode(&FindCoordinatorResponse::default(), version));
            }
            if api_key == metadata_request::API_KEY {
                return Some(flexible(&metadata(), version));
            }
            None
        })
        .await
    }

    /// A regular expression subscription needs `ConsumerGroupHeartbeat` v1,
    /// which added `subscribed_topic_regex`. A coordinator that speaks only
    /// v0 fails the call, because a v0 request would name no topic at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_regular_expression_needs_heartbeat_version_1() {
        let mock = coordinator_with_heartbeat_versions(0, 0).await;
        let mut consumer = Consumer::builder()
            .bootstrap(mock.addr.to_string())
            .group_id("group-a")
            .subscribe_regex("orders-.*")
            .group_protocol(GroupProtocol::Consumer)
            .heartbeat_interval(millis(20))
            .request_timeout(secs(5))
            .build()
            .await
            .expect("build");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        let mut error = None;
        while error.is_none() && tokio::time::Instant::now() < deadline {
            error = consumer
                .poll(millis(20))
                .await
                .err()
                .map(|error| error.to_string());
        }
        consumer.close().await.expect("close");
        mock.stop();
        assert2::assert!(
            error
                == Some(
                    "client: incompatible version: broker supports 0..=0, client wants 1..=1 for api_key 68"
                        .to_owned()
                )
        );
    }

    /// The member fetches the partitions of a regular expression assignment
    /// also after the application subscribes to the same expression again,
    /// because the topics of such a subscription come from the assignment.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_regular_expression_subscription_keeps_fetching_after_a_resubscribe() {
        let recorder = Recorder::default();
        let mock = heartbeat_coordinator_with(vec![assigns(1, &[0, 1])], &recorder, true).await;
        let mut consumer = Consumer::builder()
            .bootstrap(mock.addr.to_string())
            .group_id("group-a")
            .subscribe_regex("orders-.*")
            .group_protocol(GroupProtocol::Consumer)
            .heartbeat_interval(millis(20))
            .request_timeout(secs(5))
            .build()
            .await
            .expect("build");
        // Poll until the member owns the partitions and their positions are
        // ready, so that the fetch of the next poll would go out.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        while tokio::time::Instant::now() < deadline {
            let _ = consumer.poll(millis(20)).await;
            let assigned = consumer.assignment().await;
            if !assigned.is_empty() && !consumer.group_fetches(&assigned).await.is_empty() {
                break;
            }
        }
        consumer.subscribe_regex("orders-.*").expect("subscribe");
        let assigned = consumer.assignment().await;
        let mut fetched: Vec<(String, i32)> = consumer
            .group_fetches(&assigned)
            .await
            .into_values()
            .flatten()
            .flat_map(|(topic, partitions)| {
                partitions
                    .into_iter()
                    .map(move |(partition, ..)| (topic.clone(), partition))
            })
            .collect();
        fetched.sort();
        consumer.close().await.expect("close");
        mock.stop();
        assert2::assert!(
            (assigned, fetched)
                == (
                    vec![(TOPIC.to_owned(), 0), (TOPIC.to_owned(), 1)],
                    vec![(TOPIC.to_owned(), 0), (TOPIC.to_owned(), 1)],
                )
        );
    }

    /// The member leaves the group when the time between two `poll` calls is
    /// longer than `max_poll_interval`, gives its partitions to the lost
    /// callback, and joins again with the next `poll`. Kafka's
    /// `AbstractHeartbeatRequestManager.poll` and `transitionToStale`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_member_that_stops_polling_leaves_and_joins_again() {
        let recorder = Recorder::default();
        let calls: ListenerCalls = Arc::default();
        let mock = heartbeat_coordinator(vec![assigns(1, &[0, 1])], &recorder).await;
        let mut consumer = Consumer::builder()
            .bootstrap(mock.addr.to_string())
            .group_id("group-a")
            .subscribe([TOPIC.to_owned()])
            .group_protocol(GroupProtocol::Consumer)
            .heartbeat_interval(millis(20))
            .max_poll_interval(millis(300))
            .request_timeout(secs(5))
            .rebalance_listener(Box::new(RecordingListener {
                calls: Arc::clone(&calls),
                assign_delay: std::time::Duration::ZERO,
            }))
            .build()
            .await
            .expect("build");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        while consumer.assignment().await.is_empty() && tokio::time::Instant::now() < deadline {
            let _ = consumer.poll(millis(20)).await;
        }
        // The application stops polling for longer than `max_poll_interval`.
        tokio::time::sleep(std::time::Duration::from_millis(700)).await;
        let left = consumer.assignment().await;
        let epochs_after_leave = recorder.epochs();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        while consumer.assignment().await.is_empty() && tokio::time::Instant::now() < deadline {
            let _ = consumer.poll(millis(20)).await;
        }
        let joined_again = consumer.assignment().await;
        consumer.close().await.expect("close");
        mock.stop();
        // A `poll` that ends before its callback ran makes the next `poll`
        // run the callback again, so the list can hold more than one call.
        let lost = first_lost(&calls);
        assert2::assert!(
            (
                left,
                epochs_after_leave.contains(&LEAVE_GROUP_MEMBER_EPOCH),
                joined_again,
                lost,
            ) == (
                Vec::new(),
                true,
                vec![(TOPIC.to_owned(), 0), (TOPIC.to_owned(), 1)],
                Some((
                    crate::rebalance_listener::ListenerCallKind::Lost,
                    vec![0, 1]
                )),
            )
        );
    }

    /// A fenced member gives its partitions to the lost callback, and the
    /// member keeps its id. Kafka's `transitionToFenced`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fenced_member_gives_its_partitions_to_the_lost_callback() {
        let recorder = Recorder::default();
        let calls: ListenerCalls = Arc::default();
        let mock = heartbeat_coordinator(
            vec![
                assigns(1, &[0, 1]),
                fails(FENCED_MEMBER_EPOCH),
                assigns(1, &[0, 1]),
            ],
            &recorder,
        )
        .await;
        let mut consumer = Consumer::builder()
            .bootstrap(mock.addr.to_string())
            .group_id("group-a")
            .subscribe([TOPIC.to_owned()])
            .group_protocol(GroupProtocol::Consumer)
            .heartbeat_interval(millis(20))
            .request_timeout(secs(5))
            .rebalance_listener(Box::new(RecordingListener {
                calls: Arc::clone(&calls),
                assign_delay: std::time::Duration::ZERO,
            }))
            .build()
            .await
            .expect("build");
        let member_id = consumer.member_id();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        while first_lost(&calls).is_none() && tokio::time::Instant::now() < deadline {
            let _ = consumer.poll(millis(20)).await;
        }
        let kept_id = consumer.member_id();
        consumer.close().await.expect("close");
        mock.stop();
        assert2::assert!(
            (first_lost(&calls), kept_id == member_id)
                == (
                    Some((
                        crate::rebalance_listener::ListenerCallKind::Lost,
                        vec![0, 1]
                    )),
                    true,
                )
        );
    }

    /// The member keeps its membership with `ConsumerGroupHeartbeat` while a
    /// rebalance listener callback runs, and never sends the classic
    /// `Heartbeat`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heartbeats_go_on_during_a_rebalance_callback() {
        let recorder = Recorder::default();
        let calls: ListenerCalls = Arc::default();
        let mock = heartbeat_coordinator(vec![assigns(1, &[0, 1])], &recorder).await;
        let mut consumer = Consumer::builder()
            .bootstrap(mock.addr.to_string())
            .group_id("group-a")
            .subscribe([TOPIC.to_owned()])
            .group_protocol(GroupProtocol::Consumer)
            .heartbeat_interval(millis(20))
            .request_timeout(secs(5))
            .rebalance_listener(Box::new(RecordingListener {
                calls: Arc::clone(&calls),
                assign_delay: std::time::Duration::from_millis(200),
            }))
            .build()
            .await
            .expect("build");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        while consumer.assignment().await.is_empty() && tokio::time::Instant::now() < deadline {
            let _ = consumer.poll(millis(200)).await;
        }
        let during_callback = recorder.heartbeats().len();
        consumer.close().await.expect("close");
        mock.stop();
        let classic = *recorder
            .classic_heartbeats
            .lock()
            .expect("classic heartbeats lock");
        // The join, the acknowledgement and the heartbeats of the 200 ms
        // callback at a 20 ms interval.
        assert2::assert!((during_callback >= 4, classic) == (true, 0));
    }

    /// The member id of the coordinator reaches the consumer, and a new
    /// subscription reaches the metadata of the heartbeat task.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_member_id_and_a_new_subscription_reach_the_consumer() {
        let recorder = Recorder::default();
        let mock =
            heartbeat_coordinator(vec![named(assigns(1, &[0, 1]), "server-id")], &recorder).await;
        let mut consumer = Consumer::builder()
            .bootstrap(mock.addr.to_string())
            .group_id("group-a")
            .subscribe([TOPIC.to_owned()])
            .group_protocol(GroupProtocol::Consumer)
            .heartbeat_interval(millis(20))
            .request_timeout(secs(5))
            .build()
            .await
            .expect("build");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        while consumer.assignment().await.is_empty() && tokio::time::Instant::now() < deadline {
            let _ = consumer.poll(millis(20)).await;
        }
        let member_id = consumer.member_id();
        consumer
            .subscribe([TOPIC.to_owned(), "payments".to_owned()])
            .await
            .expect("subscribe");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        while !recorder
            .metadata_topics
            .lock()
            .expect("metadata topics lock")
            .iter()
            .any(|topic| topic == "payments")
            && tokio::time::Instant::now() < deadline
        {
            let _ = consumer.poll(millis(20)).await;
        }
        let asked_for_payments = recorder
            .metadata_topics
            .lock()
            .expect("metadata topics lock")
            .iter()
            .any(|topic| topic == "payments");
        consumer.close().await.expect("close");
        mock.stop();
        assert2::assert!((member_id, asked_for_payments) == ("server-id".to_owned(), true));
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
            regex: None,
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

    /// Kafka's `HeartbeatState.buildRequestData` sends the regular expression
    /// of a broker-side pattern subscription when the member joins with one
    /// and when it changed, and sends an empty expression to remove it.
    #[test]
    fn a_heartbeat_carries_the_regular_expression_of_the_subscription() {
        let with_regex = |regex: Option<&'static str>, joining: bool| HeartbeatFields {
            subscribed_topic_names: Vec::new(),
            regex: regex.map(str::to_owned),
            ..fields(i32::from(!joining), joining, BTreeMap::new())
        };
        let mut sent = SentFields::default();
        let join = build_heartbeat(&with_regex(Some("orders-.*"), true), &mut sent);
        let steady = build_heartbeat(&with_regex(Some("orders-.*"), false), &mut sent);
        let changed = build_heartbeat(&with_regex(Some("orders-eu-.*"), false), &mut sent);
        let removed = build_heartbeat(&with_regex(None, false), &mut sent);
        let actual = [join, steady, changed, removed].map(|request| {
            (
                request.subscribed_topic_regex,
                request.subscribed_topic_names,
            )
        });
        assert2::assert!(
            actual
                == [
                    (Some("orders-.*".to_owned()), Some(Vec::new())),
                    (None, None),
                    (Some("orders-eu-.*".to_owned()), None),
                    (Some(String::new()), None),
                ]
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
