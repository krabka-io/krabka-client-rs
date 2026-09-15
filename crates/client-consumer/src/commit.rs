//! `Consumer::commit_sync` and `commit_async`.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::Ordering},
};

use krabka_protocol::owned::{
    offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestTopic},
    offset_commit_response::OffsetCommitResponse,
};
use tokio::sync::Mutex;

use crate::{
    consumer::{CommitIdentity, Consumer},
    coordinator::{
        find_coordinator, is_retriable_transport_error, next_backoff, retry_deadline_elapsed,
        with_coordinator_refind,
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
/// `UNKNOWN_TOPIC_ID`: a topic id that the coordinator does not hold. Kafka's
/// `CommitRequestManager` retries it as an `InvalidMetadataException`.
const UNKNOWN_TOPIC_ID: i16 = 100;

/// First non-zero per-partition `error_code` in an `OffsetCommitResponse`, or
/// `0` if every partition committed cleanly.
///
/// `with_coordinator_refind` reads this to decide whether to re-discover the
/// coordinator and retry.
fn first_commit_error(resp: &OffsetCommitResponse) -> i16 {
    for t in &resp.topics {
        for p in &t.partitions {
            if p.error_code != 0 {
                return p.error_code;
            }
        }
    }
    0
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

fn validate_selected_offsets(
    offsets: &HashMap<(String, i32), i64>,
    assigned: &[(String, i32)],
    consumed_positions: &HashMap<(String, i32), i64>,
) -> Result<(), ConsumerError> {
    for ((topic, partition), offset) in offsets {
        if *offset < 0 {
            return Err(ConsumerError::InvalidOffset(*offset));
        }
        let key = (topic.clone(), *partition);
        if !assigned.contains(&key) {
            return Err(ConsumerError::IllegalState(format!(
                "cannot commit unassigned partition {topic}-{partition}"
            )));
        }
        let consumed = consumed_positions.get(&key).copied().unwrap_or(0);
        if *offset > consumed {
            return Err(ConsumerError::IllegalState(format!(
                "cannot commit offset {offset} past consumed position {consumed} for {topic}-{partition}"
            )));
        }
    }
    Ok(())
}

async fn snapshot_commit_topics(
    commit_identity: &Arc<Mutex<CommitIdentity>>,
    offsets: &Arc<Mutex<HashMap<(String, i32), i64>>>,
    positions: &Arc<Mutex<HashMap<(String, i32), PartitionPosition>>>,
) -> Option<(usize, Vec<OffsetCommitRequestTopic>, (i32, String))> {
    let identity = commit_identity.lock().await.clone();
    let mut raw_offsets = offsets.lock().await.clone();
    raw_offsets.retain(|partition, _| identity.ownership_ids.contains_key(partition));
    if raw_offsets.is_empty() {
        return None;
    }
    let partitions = raw_offsets.len();
    let pos = positions.lock().await;
    let offsets = commit_offsets(raw_offsets, &pos);
    drop(pos);
    Some((
        partitions,
        build_commit_topics(offsets),
        (identity.generation, identity.member_id),
    ))
}

/// Build the `OffsetCommit` request of a commit. The request names each
/// topic, so the version is v9 or lower. See [`TopicNameOffsetCommit`].
fn build_commit_request(
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

/// Map an `OffsetCommit` response to a result, from the response and from
/// whether the coordinator task is still alive.
///
/// `0` is success. This function DEFERS the rebalance codes
/// `ILLEGAL_GENERATION (22)` and `REBALANCE_IN_PROGRESS (27)`, that is it
/// classifies them as deferred ONLY while the coordinator task is alive to
/// rejoin. The synchronous commit loop then retries continuously-owned
/// partitions under the newly-published identity. A long-running block-builder
/// or compactor commit loop therefore survives a routine rebalance without
/// reporting an unacknowledged commit as successful.
///
/// If the coordinator task has EXITED it can never republish a fresh
/// generation, so deferral would silently never advance. This function then
/// surfaces those codes as fatal, so the process restarts and rejoins from
/// scratch.
///
/// `UNKNOWN_TOPIC_OR_PARTITION (3)` and `UNKNOWN_TOPIC_ID (100)` are retriable.
/// The synchronous commit loop sends them again until the coordinator retry
/// timeout elapses. Any other non-zero code is always fatal.
#[cfg(test)]
fn commit_response_result(
    resp: &OffsetCommitResponse,
    coordinator_alive: bool,
) -> Result<(), ConsumerError> {
    commit_response_outcome(resp, coordinator_alive).map(|_| ())
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CommitOutcome {
    Acked(HashSet<(String, i32)>),
    Deferred {
        code: i16,
        acknowledged: HashSet<(String, i32)>,
    },
    /// The coordinator does not know the topic or the topic id of a partition.
    /// Kafka's `CommitRequestManager` retries these codes until the commit
    /// deadline.
    Retriable {
        code: i16,
        acknowledged: HashSet<(String, i32)>,
    },
}

fn commit_response_outcome(
    resp: &OffsetCommitResponse,
    coordinator_alive: bool,
) -> Result<CommitOutcome, ConsumerError> {
    let mut deferred = None;
    let mut retriable = None;
    let mut acknowledged = HashSet::new();
    for topic in &resp.topics {
        for partition in &topic.partitions {
            match partition.error_code {
                0 => {
                    acknowledged.insert((topic.name.clone(), partition.partition_index));
                }
                code @ (22 | 25 | 27) if coordinator_alive => {
                    deferred.get_or_insert(code);
                }
                code @ (UNKNOWN_TOPIC_OR_PARTITION | UNKNOWN_TOPIC_ID) => {
                    retriable.get_or_insert(code);
                }
                code => return Err(ConsumerError::Server(code)),
            }
        }
    }
    Ok(match (deferred, retriable) {
        (Some(code), _) => CommitOutcome::Deferred { code, acknowledged },
        (None, Some(code)) => CommitOutcome::Retriable { code, acknowledged },
        (None, None) => CommitOutcome::Acked(acknowledged),
    })
}

fn retain_continuously_owned(
    pending: &mut HashMap<(String, i32), (i64, u64)>,
    current: &HashMap<(String, i32), u64>,
) {
    pending.retain(|partition, (_, ownership_id)| current.get(partition) == Some(ownership_id));
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
            member_id = %self.member_id,
            generation = self.current_generation.load(Ordering::Relaxed),
            partitions = tracing::field::Empty,
        ),
        err
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn commit_sync(&self) -> Result<(), ConsumerError> {
        self.ensure_active_group()?;
        let _commit_guard = self.commit_serialization.lock().await;
        let pending = {
            let identity = self.commit_identity.lock().await;
            let offsets = self.next_offsets.lock().await;
            offsets
                .iter()
                .filter_map(|(partition, offset)| {
                    identity
                        .ownership_ids
                        .get(partition)
                        .map(|ownership_id| (partition.clone(), (*offset, *ownership_id)))
                })
                .collect::<HashMap<_, _>>()
        };
        if pending.is_empty() {
            return Ok(());
        }
        tracing::Span::current().record("partitions", pending.len());

        self.commit_pending_offsets(pending).await
    }

    /// Commit caller-selected next offsets for currently assigned partitions.
    /// Unlike [`Consumer::commit_sync`], this does not commit unrelated
    /// partitions and does not change the consumer's fetch positions.
    ///
    /// # Errors
    ///
    /// `Ok(())` means each requested offset was broker-acknowledged while this
    /// consumer continuously owned its partition, or that ownership ended and
    /// the new owner will safely replay from the prior committed offset.
    ///
    /// Returns an error if an offset is negative, targets an unassigned
    /// partition, is ahead of the current consumed position, or the coordinator
    /// rejects the commit with a non-rebalance error.
    pub async fn commit_offsets_sync(
        &self,
        offsets: HashMap<(String, i32), i64>,
    ) -> Result<(), ConsumerError> {
        self.ensure_active_group()?;
        let _commit_guard = self.commit_serialization.lock().await;
        if offsets.is_empty() {
            return Ok(());
        }
        let pending = {
            let identity = self.commit_identity.lock().await;
            let consumed_positions = self.next_offsets.lock().await;
            let owned = identity.ownership_ids.keys().cloned().collect::<Vec<_>>();
            validate_selected_offsets(&offsets, &owned, &consumed_positions)?;
            offsets
                .into_iter()
                .map(|(partition, offset)| {
                    let ownership_id = identity.ownership_ids[&partition];
                    (partition, (offset, ownership_id))
                })
                .collect::<HashMap<_, _>>()
        };

        self.commit_pending_offsets(pending).await
    }

    /// Fail a synchronous commit when the coordinator task has stopped.
    ///
    /// The task stops when the coordinator fences the static member. The
    /// consumer is then not part of an active group, and no later generation
    /// can make a commit valid. Kafka's
    /// `ConsumerCoordinator.sendOffsetCommitRequest` raises
    /// `CommitFailedException` in this state and sends no request.
    fn ensure_active_group(&self) -> Result<(), ConsumerError> {
        if self
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
        mut pending: HashMap<(String, i32), (i64, u64)>,
    ) -> Result<(), ConsumerError> {
        let retry_start = tokio::time::Instant::now();
        let mut retry_backoff = self.retry_policy.initial_backoff;
        loop {
            let identity = self.commit_identity.lock().await.clone();
            retain_continuously_owned(&mut pending, &identity.ownership_ids);
            if pending.is_empty() {
                return Ok(());
            }

            let mut assignment_changed = Box::pin(self.assignment_changed.notified());
            assignment_changed.as_mut().enable();
            let position_epochs = self.positions.lock().await.clone();
            let offsets = commit_offsets(
                pending
                    .iter()
                    .map(|(partition, (offset, _))| (partition.clone(), *offset))
                    .collect(),
                &position_epochs,
            );
            let topics = build_commit_topics(offsets);
            match self
                .commit_topics_once(topics, (identity.generation, identity.member_id.clone()))
                .await?
            {
                CommitOutcome::Acked(acknowledged) => {
                    pending.retain(|partition, _| !acknowledged.contains(partition));
                    if pending.is_empty() {
                        return Ok(());
                    }
                    return Err(ConsumerError::IllegalState(
                        "offset commit response omitted requested partitions".into(),
                    ));
                }
                CommitOutcome::Deferred { code, acknowledged } => {
                    tracing::warn!(
                        group = %self.group_id,
                        error_code = code,
                        "offset commit deferred until the coordinator rejoins",
                    );
                    pending.retain(|partition, _| !acknowledged.contains(partition));
                    let current_identity = self.commit_identity.lock().await.clone();
                    retain_continuously_owned(&mut pending, &current_identity.ownership_ids);
                    if pending.is_empty() {
                        return Ok(());
                    }
                    if current_identity.generation == identity.generation
                        && current_identity.member_id == identity.member_id
                    {
                        tokio::select! {
                            () = &mut assignment_changed => {}
                            () = self.coordinator_shutdown.cancelled() => {
                                return Err(ConsumerError::Server(code));
                            }
                        }
                    }
                }
                CommitOutcome::Retriable { code, acknowledged } => {
                    // Kafka's `CommitRequestManager.commitSyncWithRetries` sends
                    // the commit again while it fails with a retriable error and
                    // the deadline has not passed.
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
                        "offset commit names a topic the coordinator does not know; retrying",
                    );
                    tokio::time::sleep(retry_backoff).await;
                    retry_backoff = next_backoff(retry_backoff, self.retry_policy.max_backoff);
                }
            }
        }
    }

    async fn commit_topics_once(
        &self,
        topics: Vec<OffsetCommitRequestTopic>,
        identity: (i32, String),
    ) -> Result<CommitOutcome, ConsumerError> {
        // OffsetCommit is a coordinator RPC: route it to the coordinator broker
        // (discovered at build time, kept current by the coordinator task), and
        // re-discover on a cold/relocating-coordinator code so a coordinator
        // move is chased rather than looping NOT_COORDINATOR on the stale id.
        let resp = with_coordinator_refind(
            &self.client,
            &self.group_id,
            &self.coordinator_id,
            self.retry_policy,
            first_commit_error,
            || {
                let group_id = self.group_id.clone();
                let group_instance_id = self.group_instance_id.clone();
                let topics = topics.clone();
                let client = &self.client;
                let target = self.coordinator_id.load(Ordering::Relaxed);
                let identity = identity.clone();
                async move {
                    let (generation, member_id) = identity;
                    client
                        .broker(target)
                        .send(build_commit_request(
                            group_id,
                            generation,
                            member_id,
                            group_instance_id,
                            topics,
                        ))
                        .await
                        .map_err(ConsumerError::from)
                }
            },
        )
        .await?;

        // A rebalance can move the group out from under this commit (22
        // ILLEGAL_GENERATION / 27 REBALANCE_IN_PROGRESS). While the coordinator
        // task is alive it rejoins, republishes the generation, and the offsets
        // recommit next round, so `commit_response_result` defers (Ok) and the
        // commit loop survives the rebalance. If the coordinator task has exited
        // it returns fatal instead — a dead coordinator can never recover the
        // generation, so a loud restart beats a silent never-advance.
        let coordinator_alive = self
            .coordinator_handle
            .as_ref()
            .is_none_or(|h| !h.is_finished());
        let outcome = commit_response_outcome(&resp, coordinator_alive)?;
        if let CommitOutcome::Deferred { code, .. } = outcome {
            tracing::warn!(
                group = %self.group_id,
                error_code = code,
                "offset commit deferred: group rebalancing; will recommit after the coordinator rejoins",
            );
        }
        Ok(outcome)
    }

    /// Fire-and-forget commit.
    ///
    /// This method returns after scheduling the latest offsets for a background
    /// commit. Calls made while one is queued or running are coalesced into its
    /// snapshot or one follow-up snapshot. It does NOT wait for the broker ack.
    /// It logs errors and does not return them.
    #[cfg_attr(test, mutants::skip)] // cargo-mutants: fire-and-forget I/O spawn, exercised by integration tests
    #[tracing::instrument(
        name = "consumer.commit_async",
        level = "debug",
        skip_all,
        fields(
            group_id = %self.group_id,
            member_id = %self.member_id,
            generation = self.current_generation.load(Ordering::Relaxed),
        )
    )]
    pub fn commit_async(&self) {
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

        let client = self.client.clone();
        let group_id = self.group_id.clone();
        let commit_identity = Arc::clone(&self.commit_identity);
        let commit_serialization = Arc::clone(&self.commit_serialization);
        let group_instance_id = self.group_instance_id.clone();
        let offsets = Arc::clone(&self.next_offsets);
        let positions = Arc::clone(&self.positions);
        let coordinator_id = Arc::clone(&self.coordinator_id);
        let commit_async_state = Arc::clone(&self.commit_async_state);
        let retry_policy = self.retry_policy;
        tokio::spawn(async move {
            loop {
                {
                    let _commit_guard = commit_serialization.lock().await;
                    // Calls queued before this snapshot are represented by the
                    // current offsets, so collapse them into this request.
                    commit_async_state.store(ASYNC_COMMIT_RUNNING, Ordering::Release);
                    if let Some((_, topics, (generation, member_id))) =
                        snapshot_commit_topics(&commit_identity, &offsets, &positions).await
                    {
                        // Route to the coordinator broker. If it returns a moved/cold
                        // coordinator code (or the socket is gone), re-discover once and
                        // retry — but don't block a background commit on the full retry
                        // loop; one re-find recovers a coordinator move at-least-once.
                        let make_req = |topics: Vec<_>| {
                            build_commit_request(
                                group_id.clone(),
                                generation,
                                member_id.clone(),
                                group_instance_id.clone(),
                                topics,
                            )
                        };
                        let target = coordinator_id.load(Ordering::Relaxed);
                        let res = client.broker(target).send(make_req(topics.clone())).await;
                        let moved = match &res {
                            Ok(resp) => crate::coordinator::is_retriable_coordinator_code(
                                first_commit_error(resp),
                            ),
                            Err(e) if is_retriable_transport_error(e) => true,
                            Err(_) => false,
                        };
                        if moved {
                            match find_coordinator(&client, &group_id, retry_policy).await {
                                Ok(id) => {
                                    coordinator_id.store(id, Ordering::Relaxed);
                                    if let Err(e) = client.broker(id).send(make_req(topics)).await {
                                        tracing::warn!(error = %e, "commit_async retry after re-find failed");
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(error = %e, "commit_async coordinator re-discovery failed");
                                }
                            }
                        } else if let Err(e) = res {
                            tracing::warn!(error = %e, "commit_async failed");
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
    use krabka_client_core::{Client, MockBroker};
    use krabka_protocol::{
        Encode, UnknownTaggedFields,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            offset_commit_request,
            offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
            offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
        },
        primitives::uuid::Uuid,
    };
    use krabka_units::secs;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        Assignor, AutoOffsetReset, IsolationLevel, consumer::ConsumerRetryPolicy,
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
    /// `offset_commit_range`.
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
            member_id: "member-a".into(),
            commit_identity,
            commit_serialization: Arc::new(Mutex::new(())),
            commit_async_state: Arc::new(std::sync::atomic::AtomicU8::new(ASYNC_COMMIT_IDLE)),
            group_instance_id: None,
            current_generation: generation,
            subscribed_topics: vec!["topic".into()],
            assigned: Arc::new(Mutex::new(assigned)),
            assignment_changed,
            next_offsets: Arc::new(Mutex::new(next_offsets)),
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
        }
    }

    #[test]
    fn first_commit_error_returns_first_non_zero_partition_error() {
        for (_name, errors, want) in [
            ("all successful", &[0, 0][..], 0),
            ("later first error", &[0, 27, 42][..], 27),
            ("first partition errors", &[16, 27][..], 16),
        ] {
            assert2::assert!(first_commit_error(&response(errors)) == want);
        }
    }

    #[test]
    fn selected_offset_validation_rejects_invalid_targets_and_future_offsets() {
        let assigned = vec![("topic".to_string(), 2)];
        let positions = HashMap::from([(("topic".to_string(), 2), 11)]);

        for (offsets, expected) in [
            (HashMap::from([(("topic".to_string(), 2), -1)]), "negative"),
            (HashMap::from([(("other".to_string(), 2), 1)]), "unassigned"),
            (HashMap::from([(("topic".to_string(), 2), 12)]), "future"),
        ] {
            check!(
                validate_selected_offsets(&offsets, &assigned, &positions).is_err(),
                "{expected} selected offset must fail"
            );
        }

        check!(
            validate_selected_offsets(
                &HashMap::from([(("topic".to_string(), 2), 11)]),
                &assigned,
                &positions,
            )
            .is_ok()
        );
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

    #[tokio::test]
    async fn snapshot_commit_topics_returns_none_for_empty_offsets() {
        let identity = commit_identity(7, "member-a");
        let offsets = Arc::new(Mutex::new(HashMap::new()));
        let positions = Arc::new(Mutex::new(HashMap::new()));

        let snapshot = snapshot_commit_topics(&identity, &offsets, &positions).await;

        assert2::assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn snapshot_commit_topics_preserves_count_topics_offsets_and_epochs() {
        let identity = Arc::new(Mutex::new(CommitIdentity {
            generation: 7,
            member_id: "member-a".into(),
            ownership_ids: HashMap::from([(("alpha".into(), 0), 1), (("alpha".into(), 1), 2)]),
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
        let (partition_count, topics, seen_identity) =
            snapshot_commit_topics(&identity, &offsets, &positions)
                .await
                .expect("non-empty offsets are snapshotted");
        let mut topics = topics;
        topics[0].partitions.sort_by_key(|p| p.partition_index);

        assert2::assert!(
            (partition_count, topics)
                == (
                    2,
                    vec![OffsetCommitRequestTopic {
                        name: "alpha".into(),
                        topic_id: Uuid::ZERO,
                        partitions: vec![
                            OffsetCommitRequestPartition {
                                partition_index: 0,
                                committed_offset: 10,
                                committed_leader_epoch: -1,
                                committed_metadata: Some(String::new()),
                                unknown_tagged_fields: UnknownTaggedFields::default(),
                            },
                            OffsetCommitRequestPartition {
                                partition_index: 1,
                                committed_offset: 20,
                                committed_leader_epoch: 7,
                                committed_metadata: Some(String::new()),
                                unknown_tagged_fields: UnknownTaggedFields::default(),
                            },
                        ],
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }]
                )
        );
        assert2::assert!(seen_identity == (7, "member-a".into()));
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

    #[test]
    fn commit_response_result_defers_rebalance_codes_only_while_coordinator_alive() {
        for (name, errors, coordinator_alive, expected_error) in [
            ("success while alive", &[0, 0][..], true, None),
            ("success after coordinator exit", &[0, 0][..], false, None),
            ("non-rebalance error", &[0, 42][..], true, Some(42)),
            ("illegal generation deferred", &[22][..], true, None),
            ("unknown member deferred", &[25][..], true, None),
            ("rebalance deferred", &[27][..], true, None),
            (
                "later illegal generation deferred",
                &[0, 22][..],
                true,
                None,
            ),
            ("illegal generation after exit", &[22][..], false, Some(22)),
            ("unknown member after exit", &[25][..], false, Some(25)),
            ("rebalance after exit", &[27][..], false, Some(27)),
            (
                "fatal error takes precedence",
                &[27, 42][..],
                true,
                Some(42),
            ),
        ] {
            let actual = commit_response_result(&response(errors), coordinator_alive);
            let actual_error = match actual {
                Ok(()) => None,
                Err(ConsumerError::Server(code)) => Some(code),
                Err(other) => panic!("case {name}: unexpected error {other:?}"),
            };
            check!(actual_error == expected_error, "case {name}");
        }
    }

    #[test]
    fn commit_partition_error_codes_map_to_kafka_commit_actions() {
        for (name, errors, expected) in [
            (
                "unknown topic or partition is retriable",
                &[0, 3][..],
                Ok(CommitOutcome::Retriable {
                    code: 3,
                    acknowledged: HashSet::from([("topic".into(), 0)]),
                }),
            ),
            ("not leader or follower is fatal", &[6][..], Err(6)),
            (
                "unknown topic id is retriable",
                &[100][..],
                Ok(CommitOutcome::Retriable {
                    code: 100,
                    acknowledged: HashSet::new(),
                }),
            ),
            (
                "rebalance deferral takes precedence over a retriable code",
                &[100, 27][..],
                Ok(CommitOutcome::Deferred {
                    code: 27,
                    acknowledged: HashSet::new(),
                }),
            ),
            (
                "fatal error takes precedence over a retriable code",
                &[100, 42][..],
                Err(42),
            ),
        ] {
            let actual =
                commit_response_outcome(&response(errors), true).map_err(|error| match error {
                    ConsumerError::Server(code) => code,
                    other => panic!("case {name}: unexpected error {other:?}"),
                });
            check!(actual == expected, "case {name}");
        }
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
                .commit_offsets_sync(HashMap::from([(("topic".into(), 0), 12)]))
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

    /// `commit_offsets_sync` sends the commit again after a retriable topic
    /// error and succeeds when the coordinator acks, as Kafka's
    /// `CommitRequestManager.commitSyncWithRetries` does. A fatal code fails at
    /// once, and a retriable code fails when the retry timeout has elapsed.
    #[tokio::test]
    async fn commit_sync_retries_unknown_topic_errors_until_the_deadline() {
        for (name, responses, timeout, expected) in [
            (
                "unknown topic id then success",
                vec![vec![100], vec![0]],
                Duration::from_secs(5),
                (Ok(()), 2),
            ),
            (
                "unknown topic or partition then success",
                vec![vec![3], vec![0]],
                Duration::from_secs(5),
                (Ok(()), 2),
            ),
            (
                "not leader or follower fails at once",
                vec![vec![6]],
                Duration::from_secs(5),
                (Err(Some(6)), 1),
            ),
            (
                "unknown topic id past the deadline",
                vec![vec![100]],
                Duration::ZERO,
                (Err(Some(100)), 1),
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
                let attempt = requests_in_mock.fetch_add(1, Ordering::SeqCst);
                let errors = &responses[attempt.min(responses.len() - 1)];
                Some(encode_response(&response(errors), version))
            })
            .await;
            let mut consumer = commit_consumer(
                &mock,
                commit_identity(7, "member-a"),
                Arc::new(tokio::sync::Notify::new()),
                Arc::new(AtomicI32::new(7)),
            )
            .await;
            consumer.retry_policy = CoordinatorRetryPolicy {
                timeout,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
            };

            let result = tokio::time::timeout(
                Duration::from_secs(5),
                consumer.commit_offsets_sync(HashMap::from([(("topic".into(), 0), 12)])),
            )
            .await
            .unwrap_or_else(|_| panic!("case {name}: commit never finished"))
            .map_err(|error| match error {
                ConsumerError::Server(code) => Some(code),
                _ => None,
            });

            mock.stop();
            check!(
                (result, requests.load(Ordering::SeqCst)) == expected,
                "case {name}"
            );
        }
    }

    /// A commit fails without a request after the coordinator task stopped,
    /// for example after the coordinator fenced the static member. Kafka's
    /// `ConsumerCoordinator.sendOffsetCommitRequest` raises
    /// `CommitFailedException` when the member is not part of an active group.
    #[tokio::test]
    async fn commit_fails_without_a_request_after_the_coordinator_task_stops() {
        #[derive(Clone, Copy)]
        enum Commit {
            All,
            Selected,
        }
        let commit_failed = "offset commit failed: the consumer is not part of an active group; it is likely that the consumer was kicked out of the group";
        for (name, commit, task_stopped, owned, expected) in [
            (
                "commit sync while the task runs",
                Commit::All,
                false,
                true,
                (Ok(()), 1),
            ),
            (
                "commit sync after the task stops",
                Commit::All,
                true,
                true,
                (Err(commit_failed.to_string()), 0),
            ),
            (
                "commit sync after a fence cleared the assignment",
                Commit::All,
                true,
                false,
                (Err(commit_failed.to_string()), 0),
            ),
            (
                "selected commit after the task stops",
                Commit::Selected,
                true,
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
            let mut consumer = commit_consumer(
                &mock,
                identity,
                Arc::new(tokio::sync::Notify::new()),
                Arc::new(AtomicI32::new(7)),
            )
            .await;
            let task = if task_stopped {
                let task = tokio::spawn(async {});
                while !task.is_finished() {
                    tokio::task::yield_now().await;
                }
                task
            } else {
                tokio::spawn(std::future::pending::<()>())
            };
            consumer.coordinator_handle = Some(task);

            let commit_future = async {
                match commit {
                    Commit::All => consumer.commit_sync().await,
                    Commit::Selected => {
                        consumer
                            .commit_offsets_sync(HashMap::from([(("topic".into(), 0), 12)]))
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

    #[tokio::test]
    async fn selected_commit_retries_a_continuously_owned_partition_after_rejoin() {
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
                identity.try_lock().unwrap().generation = 8;
                generation.store(8, Ordering::Relaxed);
            },
        )
        .await;

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            consumer.commit_offsets_sync(HashMap::from([(("topic".into(), 0), 12)])),
        )
        .await
        .expect("selected commit completes after retained rejoin")
        .expect("retained offset is acknowledged");

        mock.stop();
        assert2::assert!(requests.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn selected_commit_drops_revoked_or_reassigned_ownership_without_stale_retry() {
        for reassigned in [false, true] {
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
                move |identity, generation, _request_generation, _request_member_id| {
                    let mut identity = identity.try_lock().expect("identity lock available");
                    identity.ownership_ids.clear();
                    if reassigned {
                        identity.ownership_ids.insert(("topic".into(), 0), 2);
                    }
                    identity.generation = 8;
                    generation.store(8, Ordering::Relaxed);
                },
            )
            .await;

            consumer
                .commit_offsets_sync(HashMap::from([(("topic".into(), 0), 12)]))
                .await
                .expect("revocation safely ends the old ownership commit");

            mock.stop();
            check!(
                requests.load(Ordering::SeqCst) == 1,
                "reassigned={reassigned}"
            );
        }
    }

    #[tokio::test]
    async fn selected_commit_registers_rejoin_notification_before_sending() {
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
            |_ownership, _generation, _request_generation, _request_member_id| {},
        )
        .await;

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            consumer.commit_offsets_sync(HashMap::from([(("topic".into(), 0), 12)])),
        )
        .await
        .expect("notification sent before response is not lost")
        .expect("selected offset is acknowledged on retry");

        mock.stop();
        assert2::assert!(requests.load(Ordering::SeqCst) == 2);
    }

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
        let topics = build_commit_topics(commit_offsets(
            HashMap::from([(("topic".into(), 0), 12)]),
            &HashMap::new(),
        ));

        let outcome = consumer
            .commit_topics_once(topics, (7, "member-a".into()))
            .await
            .expect("rebalance response is deferred");

        mock.stop();
        assert2::assert!(
            outcome
                == CommitOutcome::Deferred {
                    code: 27,
                    acknowledged: HashSet::new(),
                }
        );
        assert2::assert!(seen_generation.load(Ordering::Relaxed) == 7);
    }

    #[tokio::test]
    async fn selected_commit_uses_live_member_id_after_from_scratch_rejoin() {
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
        consumer
            .commit_offsets_sync(HashMap::from([(("topic".into(), 0), 12)]))
            .await
            .expect("selected commit retries with live member identity");

        mock.stop();
        assert2::assert!(seen_new_member.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn bulk_commit_retries_only_deferred_partitions_from_a_mixed_response() {
        let identity = Arc::new(Mutex::new(CommitIdentity {
            generation: 7,
            member_id: "member-a".into(),
            ownership_ids: HashMap::from([(("topic".into(), 0), 1), (("topic".into(), 1), 2)]),
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

        consumer
            .commit_sync()
            .await
            .expect("the deferred partition is acknowledged on retry");

        mock.stop();
        assert2::assert!(requests.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn deferred_commit_finishes_before_a_newer_commit_for_the_same_partition() {
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
                    .commit_offsets_sync(HashMap::from([(("topic".into(), 0), 10)]))
                    .await
            })
        };
        tokio::task::yield_now().await;
        let newer = {
            let consumer = Arc::clone(&consumer);
            tokio::spawn(async move {
                consumer
                    .commit_offsets_sync(HashMap::from([(("topic".into(), 0), 12)]))
                    .await
            })
        };
        tokio::task::yield_now().await;
        drop(blocker);

        older.await.unwrap().unwrap();
        newer.await.unwrap().unwrap();

        mock.stop();
        assert2::assert!(*seen_offsets.lock().unwrap() == vec![(0, 10), (0, 10), (0, 12)]);
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
            consumer.commit_async();
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
        let selected = commit_offsets(HashMap::from([(("topic".into(), 0), 12)]), &HashMap::new());
        let topics = build_commit_topics(selected);

        assert2::assert!(topics.len() == 1);
        assert2::assert!(topics[0].partitions.len() == 1);
        assert2::assert!(topics[0].partitions[0].partition_index == 0);
    }
}
