//! Background sender task. It drains ready batches from every accumulator and
//! ships them as `ProduceRequest`s through `krabka-client-core`.
//!
//! The builder `tokio::spawn`s the sender. The sender owns the `wake_rx`
//! `Receiver` end of the wake channel, while the `Producer` holds the `wake_tx`
//! `Sender`. It also owns the `flush_notify`, the `accumulators` map, and the
//! `next_seq` map. Per-batch deadlines seal only expired current batches, ready
//! wakes drain completed rollover batches, and forced wakes stay active until
//! zero-linger, flush, or shutdown work settles. Drained batches become v2
//! `RecordBatch`es, which allocates their `base_sequence`. Each batch becomes
//! its own single-partition `ProduceRequest`, sent through `Client::broker(id)`,
//! and it falls back to the bootstrap `Client::send` when the leader is
//! unknown. All of a cycle's requests are sent **concurrently**, to keep every
//! broker busy.
//!
//! ## Per-partition pipelining (idempotence-critical)
//!
//! Brokers stay busy because independent partitions send **concurrently**. Up
//! to [`SenderConfig::max_in_flight`] Produce requests overlap on the wire per
//! drain cycle. But each *single* partition keeps **at most one** request in
//! flight, which is [`MAX_IN_FLIGHT_PER_PARTITION`]. Its next batch is not
//! drained until the previous one is acked. Per-partition idempotent
//! `base_sequence` ordering therefore holds **by construction**. The broker
//! never sees two outstanding sequences for one partition, so requests issued
//! concurrently cannot reach it out of `base_sequence` order and trip
//! `OUT_OF_ORDER_SEQUENCE_NUMBER`.
//!
//! Recovery is correspondingly simple. A batch that fails, through a transport
//! error, a routing miss, or a retriable broker code, is parked in its
//! partition's single **retry slot**. On the next cycle the sender resends it
//! verbatim, with the same allocated `base_sequence` and the same bytes, and
//! ahead of any new batch for that partition. The broker dedups a re-landed
//! write with `DUPLICATE_SEQUENCE_NUMBER`. The retry slots persist across
//! cycles, and [`run`] owns them.
//!
//! ## Broker error codes
//!
//! [`classify_verdict`] follows Apache Kafka's `Sender.completeBatch` and
//! `TransactionManager.canRetry`. A code whose exception is a
//! `RetriableException` resends the batch, and an `InvalidMetadataException`
//! also refreshes metadata. The idempotence codes follow the producer mode. For
//! an idempotent producer, `OUT_OF_ORDER_SEQUENCE_NUMBER` and
//! `UNKNOWN_PRODUCER_ID` raise the producer epoch and resend the batch at
//! sequence 0, and a failed batch also raises the epoch, so its sequence leaves
//! no gap.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::{BuildHasher as _, RandomState},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI16, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use krabka_protocol::{
    owned::{
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid,
    records::{Attributes, Record, RecordBatch, RecordHeader},
};
use krabka_units::{
    Time,
    convert::{StdDurationExt as _, TimeExt as _},
};
use tokio::{
    sync::{Mutex, Notify},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{
    accumulator::{Accumulator, AccumulatorMap, InProgressBatch, PendingRecord},
    buffer_pool::MemoryReservation,
    compression::Compression,
    error::ProducerError,
    error_class::{self, ErrorClass},
    partitioner::{BuiltInPartitioner, QueueSizes},
    producer::{Acks, STATE_ACTIVE, STATE_FENCED, TopicMetadata},
    record::RecordMetadata,
    transactional::{AbortableErrorSlot, TxnState},
    transport::ProduceTransport,
};

/// Wire error codes referenced when interpreting `PartitionProduceResponse`.
mod codes {
    pub const NONE: i16 = 0;
    /// `MESSAGE_TOO_LARGE`. Kafka splits a batch of more than one record and
    /// sends the parts again.
    pub const MESSAGE_TOO_LARGE: i16 = 10;
    /// `CLUSTER_AUTHORIZATION_FAILED`. Kafka's `TransactionManager` makes it
    /// fatal for an idempotent producer.
    pub const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
    /// `UNSUPPORTED_VERSION`. Fatal for an idempotent producer.
    pub const UNSUPPORTED_VERSION: i16 = 35;
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
    pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
    /// `INVALID_PRODUCER_ID_MAPPING`. Fatal for an idempotent producer.
    pub const INVALID_PRODUCER_ID_MAPPING: i16 = 49;
    /// `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`. Fatal for an idempotent
    /// producer.
    pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
    /// `UNKNOWN_PRODUCER_ID`. The broker holds no state for the producer id.
    pub const UNKNOWN_PRODUCER_ID: i16 = 59;
    /// `PRODUCER_FENCED`. Fatal for an idempotent producer.
    pub const PRODUCER_FENCED: i16 = 90;
}

/// Wire error codes that only the tests name. The sender handles them through
/// their [`ErrorClass`].
#[cfg(test)]
mod test_codes {
    /// The Produce reached a broker that does not lead the partition, which
    /// means the routing is stale.
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    /// The Produce reached a broker that does not lead the partition.
    pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    /// The Produce named a topic id that the receiving broker does not hold.
    pub const UNKNOWN_TOPIC_ID: i16 = 100;
}

/// Synthetic leader id that means the leader is unknown, so the sender uses the
/// bootstrap connection.
///
/// It matches the consumer's convention in `poll.rs`. A partition whose leader
/// id is `< 0`, or whose advertised address the pool cannot dial, falls back to
/// the bootstrap `Client::send` rather than `Client::broker(id)`.
const BOOTSTRAP_LEADER: i32 = -1;

/// Maximum Produce requests in flight **per partition** at once.
///
/// This is pinned to `1`: a partition's next batch is not sent until its
/// previous batch is acked. That preserves idempotent per-partition
/// `base_sequence` ordering *by construction*. The broker only ever sees one
/// sequence outstanding for a partition, so there is no window in which
/// requests issued concurrently can reach the broker out of `base_sequence`
/// order and trip `OUT_OF_ORDER_SEQUENCE_NUMBER`.
///
/// ## Why not `> 1` (same-partition pipelining)?
///
/// The previous design drained up to `max_in_flight` batches per partition and
/// fired them through `futures::future::join_all`. But the `send` of
/// [`krabka_client_core::Client`] writes the request frame **and** awaits its
/// response in a single future. When several same-partition futures are polled
/// concurrently, their frame writes race on the connection's writer channel, so
/// the broker can receive `base_sequence` 16 before 0. The broker rejects the
/// gap with `OUT_OF_ORDER_SEQUENCE_NUMBER`, and the producer resends
/// concurrently again, which re-triggers the reorder. Under sustained load this
/// livelocks: some batch never converges, its records' ack-oneshots never
/// resolve, and the caller hangs.
///
/// True same-partition pipelining (`> 1`) requires a client-core API that
/// guarantees **ordered frame writes** for a partition's in-flight requests
/// (write 0, 1, 2 to the wire in order, then await their responses
/// concurrently) — e.g. a pipelined `Connection::send_batch` or a write-then-await
/// split. Until acknowledgements carry that finer identity, one in-flight batch
/// per partition is the required ordering policy. Cross-partition pipelining is unaffected:
/// independent partitions still send concurrently, bounded by
/// [`SenderConfig::max_in_flight`].
const MAX_IN_FLIGHT_PER_PARTITION: usize = 1;

// The one-slot-per-partition pipeline (a single retry slot per partition, no
// ordered drain) is only sound while a partition never has more than one request
// outstanding. If this is ever raised above `1`, that model is insufficient — a
// partition could have several outstanding sequences needing an ordered drain —
// and the recovery path must be redesigned. Enforce the dependency at compile
// time so the assumption can't silently drift.
const _: [(); 1] = [(); MAX_IN_FLIGHT_PER_PARTITION];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DrainIntent {
    /// Send batches already completed by rollover without sealing young
    /// in-progress batches.
    Ready,
    /// Seal only in-progress batches whose own linger deadline elapsed.
    Expired,
    /// Seal every in-progress batch for zero linger, explicit flush, or shutdown.
    Force,
}

/// All the bits of state the sender task needs. The builder constructs
/// one of these, hands it to [`run`], and drops it.
pub(crate) struct SenderConfig {
    /// Broker-facing transport. Production uses a real `Client`, and tests use
    /// a deterministic in-process broker model. See [`crate::transport`].
    pub transport: Box<dyn ProduceTransport>,
    pub producer_id: i64,
    /// The idempotent producer epoch, shared with `Producer`. The sender raises
    /// it where Kafka's idempotent producer bumps its epoch.
    pub producer_epoch: Arc<AtomicI16>,
    pub acks: Acks,
    pub compression: Compression,
    pub linger: Time,
    pub request_timeout_ms: i32,
    pub retries: i32,
    /// The wait before each resend of a batch. See [`RetryBackoff`].
    pub retry_backoff: RetryBackoff,
    /// The time from the creation of a batch to its failure with
    /// [`ProducerError::SendTimeout`], when no send acknowledged it. Kafka's
    /// `delivery.timeout.ms`.
    pub delivery_timeout: Time,
    /// Maximum number of Produce requests fired **concurrently per drain
    /// cycle**, across all partitions. This is the cross-partition, or
    /// per-connection, pipelining bound, which Kafka calls
    /// `max.in.flight.requests.per.connection`.
    ///
    /// Per-partition in-flight is pinned separately to
    /// [`MAX_IN_FLIGHT_PER_PARTITION`], which is `1`, for ordering. This field
    /// bounds how many *distinct partitions'* requests overlap on the wire at
    /// once.
    pub max_in_flight: usize,
    pub metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>>,
    /// Per-`(topic, partition)` leader-id cache, shared with the `Producer`.
    /// `Metadata` fills it; see `Producer::partition_count`. The sender reads it
    /// to route each Produce to the partition leader, and refreshes it on
    /// `NOT_LEADER_OR_FOLLOWER` and on `UNKNOWN_TOPIC_OR_PARTITION`.
    pub partition_leaders: Arc<DashMap<(String, i32), i32>>,
    /// The built-in partitioner, shared with `Producer`. The sender gives it
    /// the queue size of each partition for the adaptive pick.
    pub partitioner: Arc<BuiltInPartitioner>,
    pub accumulators: AccumulatorMap,
    pub next_seq: Arc<DashMap<(String, i32), i32>>,
    pub state: Arc<AtomicU8>,
    pub wake_rx: tokio::sync::mpsc::Receiver<DrainIntent>,
    pub flush_notify: Arc<Notify>,
    /// Shared with `Producer`. It tracks batches popped from an accumulator
    /// that are still being sent, so `flush` can wait for them. See the field
    /// doc on [`crate::producer::Producer`].
    pub in_flight: Arc<AtomicUsize>,
    pub shutdown: CancellationToken,
    /// `transactional_id` from the producer config. It is `None` for a
    /// non-transactional producer.
    pub transactional_id: Option<String>,
    /// Shared with `Producer`. The sender snapshots it at send time, to decide
    /// whether to stamp batches as transactional.
    pub txn_state: Arc<Mutex<TxnState>>,
    /// Shared with `Producer`. It holds the `(producer_id, producer_epoch)`
    /// that the transaction coordinator assigned through `InitProducerId`. The
    /// sender reads it when it stamps transactional batches.
    pub txn_pid_epoch: Arc<Mutex<(i64, i16)>>,
    pub txn_recovery_required: Arc<AtomicBool>,
    pub txn_recovery_generation: Arc<AtomicU64>,
    /// Shared with `Producer`. The sender sets it when a batch of the
    /// transaction fails, so a later commit fails and the application must
    /// abort. Kafka's `TransactionManager.handleFailedBatch` moves to
    /// `ABORTABLE_ERROR` in the same place.
    pub txn_abortable_error: Arc<AbortableErrorSlot>,
}

/// Mutable per-partition pipeline state, owned by [`run`] and threaded into
/// every [`drain_once`] so it persists across drain cycles.
///
/// With [`MAX_IN_FLIGHT_PER_PARTITION`] pinned to `1`, the only state a
/// partition can carry between cycles is a single failed batch that awaits a
/// verbatim resend. There is never more than one request outstanding, so there
/// is nothing to "drain" and no resend *set* to order. Each partition therefore
/// has exactly one slot.
#[derive(Default)]
struct PipelineState {
    /// Per-`(topic, partition)` retry slot. It holds a batch that failed its
    /// last send and must be resent verbatim, with the same `base_sequence` and
    /// the same bytes, ahead of any new batch for that partition.
    ///
    /// `in_flight` already counts the batch, from when it was first drained
    /// from the accumulator, so a resend does NOT count it again. A batch in
    /// this slot means the partition's single in-flight slot is occupied.
    retry: HashMap<(String, i32), PreparedBatch>,
    /// Per-`(topic, partition)` offset of the last record the broker acked.
    /// Kafka's `TxnPartitionEntry.lastAckedOffset` holds the same value, and
    /// `UNKNOWN_PRODUCER_ID` compares it with the log start offset.
    last_acked_offset: HashMap<(String, i32), i64>,
    /// Per-broker drain times for `partitioner.availability.timeout.ms`.
    /// Kafka's `RecordAccumulator.nodeStats`.
    node_latency: HashMap<i32, NodeLatency>,
}

/// When the sender last found ready records for a broker, and when it last
/// sent records to it. Kafka's `RecordAccumulator.NodeLatencyStats`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NodeLatency {
    ready_at: Instant,
    drained_at: Instant,
}

impl NodeLatency {
    /// Tell if records waited longer than `timeout` for the broker. Kafka's
    /// `RecordAccumulator.partitionReady` then leaves the partitions of the
    /// broker out of the adaptive pick.
    fn unavailable(self, timeout: Duration) -> bool {
        self.ready_at.saturating_duration_since(self.drained_at) > timeout
    }
}

/// Note that `key` had ready records at `now`, and whether this cycle sent
/// them. Kafka's `RecordAccumulator.updateNodeLatencyStats`. It does nothing
/// when the availability timeout is zero, or when the partition has no leader.
fn note_node_latency(
    cfg: &SenderConfig,
    state: &mut PipelineState,
    key: &(String, i32),
    now: Instant,
    drained: bool,
) {
    if cfg.partitioner.config().availability_timeout.is_zero() {
        return;
    }
    let Some(leader) = cfg
        .partition_leaders
        .get(key)
        .map(|leader| *leader)
        .filter(|leader| *leader >= 0)
    else {
        return;
    };
    let latency = state.node_latency.entry(leader).or_insert(NodeLatency {
        ready_at: now,
        drained_at: now,
    });
    if drained {
        latency.drained_at = now;
    }
    latency.ready_at = now;
}

/// A partition of a topic and its accumulator.
type PartitionAccumulator = (i32, Arc<Mutex<Accumulator>>);

/// Give the partitioner the queue size of each partition, for the adaptive
/// pick of a new sticky partition. Kafka's `RecordAccumulator.partitionReady`.
///
/// A topic gets queue sizes only when every partition has an accumulator, so
/// the pick can see each partition. A partition without a leader has no queue
/// size. With `partitioner.availability.timeout.ms`, a partition whose leader
/// had ready records and no send for longer than the timeout has none either.
/// A parked resend counts in the queue, as Kafka puts a retried batch back in
/// the partition queue.
async fn update_partition_load_stats(cfg: &SenderConfig, state: &PipelineState) {
    let config = cfg.partitioner.config();
    if !config.adaptive_partitioning {
        return;
    }
    let mut topics: HashMap<String, Vec<PartitionAccumulator>> = HashMap::new();
    for entry in cfg.accumulators.iter() {
        let (topic, partition) = entry.key();
        topics
            .entry(topic.clone())
            .or_default()
            .push((*partition, Arc::clone(entry.value())));
    }
    for (topic, mut partitions) in topics {
        let complete = topic_partition_count(cfg, &topic)
            .await
            .and_then(|count| usize::try_from(count).ok())
            .is_some_and(|count| partitions.len() >= count);
        if !complete {
            cfg.partitioner.update_load_stats(&topic, None);
            continue;
        }
        partitions.sort_unstable_by_key(|(partition, _)| *partition);
        let mut queues = QueueSizes {
            sizes: Vec::with_capacity(partitions.len()),
            partition_ids: Vec::with_capacity(partitions.len()),
            all: partitions.len(),
        };
        for (partition, accumulator) in partitions {
            let key = (topic.clone(), partition);
            let Some(leader) = cfg
                .partition_leaders
                .get(&key)
                .map(|leader| *leader)
                .filter(|leader| *leader >= 0)
            else {
                continue;
            };
            let queue_size =
                accumulator.lock().await.queue_size() + usize::from(state.retry.contains_key(&key));
            if !config.availability_timeout.is_zero()
                && state
                    .node_latency
                    .get(&leader)
                    .is_some_and(|latency| latency.unavailable(config.availability_timeout))
            {
                continue;
            }
            queues
                .sizes
                .push(u32::try_from(queue_size).unwrap_or(u32::MAX));
            queues.partition_ids.push(partition);
        }
        cfg.partitioner.update_load_stats(&topic, Some(queues));
    }
}

#[derive(Debug)]
struct Schedule {
    immediate: bool,
    deadline: Option<Instant>,
    settled: bool,
}

/// The multiplier of each retry backoff step. Kafka's
/// `CommonClientConfigs.RETRY_BACKOFF_EXP_BASE`.
const RETRY_BACKOFF_EXP_BASE: f64 = 2.0;
/// The random spread of a retry backoff. Kafka's
/// `CommonClientConfigs.RETRY_BACKOFF_JITTER`.
const RETRY_BACKOFF_JITTER: f64 = 0.2;

/// The retry backoff of Kafka's `ExponentialBackoff`, with the settings that
/// `RecordAccumulator` gives it.
///
/// The backoff after `attempts` earlier retries is
/// `initial * 2^attempts * random(0.8, 1.2)`, and never more than `max`. When
/// `max` is not more than `initial`, the backoff is `max` with no jitter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RetryBackoff {
    initial: Duration,
    max: Duration,
}

impl RetryBackoff {
    /// Kafka's `ExponentialBackoff` uses the smaller of `initial` and `max` as
    /// the first interval.
    pub(crate) fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial: initial.min(max),
            max,
        }
    }

    /// The backoff after `attempts` earlier retries. `unit` is a random value
    /// from 0 (inclusive) to 1 (exclusive), which picks the jitter factor.
    fn backoff(self, attempts: u32, unit: f64) -> Duration {
        if self.max <= self.initial {
            return self.initial;
        }
        // Kafka counts in whole milliseconds and divides by `max(initial, 1)`.
        // This backoff accepts a sub-millisecond initial interval, so it
        // divides by the interval itself, and only a zero interval takes the
        // smallest unit.
        let smallest = self.initial.max(Duration::from_nanos(1));
        let exp_max =
            (self.max.as_secs_f64() / smallest.as_secs_f64()).ln() / RETRY_BACKOFF_EXP_BASE.ln();
        let exp = f64::from(attempts).min(exp_max);
        let factor = 1.0 - RETRY_BACKOFF_JITTER + 2.0 * RETRY_BACKOFF_JITTER * unit;
        let value = self.initial.as_secs_f64() * RETRY_BACKOFF_EXP_BASE.powf(exp) * factor;
        Duration::try_from_secs_f64(value).map_or(self.max, |backoff| backoff.min(self.max))
    }
}

/// A random value from 0 (inclusive) to 1 (exclusive) for the backoff jitter.
///
/// Each `RandomState` has new random keys, so its hash of a fixed value is a
/// random number. This needs no extra dependency.
fn jitter_unit() -> f64 {
    let bits = RandomState::new().hash_one(0_u8) >> 32;
    f64::from(u32::try_from(bits).unwrap_or(u32::MAX)) / 4_294_967_296.0
}

/// The instant a batch created at `created_at` reaches the delivery timeout,
/// or `None` when that instant is out of range.
fn delivery_deadline(created_at: Instant, delivery_timeout: Time) -> Option<Instant> {
    created_at.checked_add(delivery_timeout.to_std())
}

/// Kafka's `ProducerBatch.hasReachedDeliveryTimeout`:
/// `deliveryTimeoutMs <= now - createdMs`.
fn reached_delivery_timeout(created_at: Instant, delivery_timeout: Time, now: Instant) -> bool {
    delivery_deadline(created_at, delivery_timeout).is_some_and(|deadline| deadline <= now)
}

fn include_deadline(schedule: &mut Schedule, deadline: Instant, now: Instant) {
    if deadline <= now {
        schedule.immediate = true;
    } else if schedule.deadline.is_none_or(|current| deadline < current) {
        schedule.deadline = Some(deadline);
    }
}

async fn schedule(cfg: &SenderConfig, state: &PipelineState, force: bool) -> Schedule {
    let now = Instant::now();
    let mut schedule = Schedule {
        immediate: false,
        deadline: None,
        settled: state.retry.is_empty() && cfg.in_flight.load(Ordering::Acquire) == 0,
    };

    for batch in state.retry.values() {
        schedule.settled = false;
        if batch_crosses_recovery_barrier(cfg, batch.transaction_generation) {
            schedule.immediate = true;
            continue;
        }
        if let Some(deadline) = delivery_deadline(batch.created_at, cfg.delivery_timeout) {
            include_deadline(&mut schedule, deadline, now);
        }
        if let Some(backoff_until) = batch.backoff_until {
            include_deadline(&mut schedule, backoff_until, now);
        } else {
            schedule.immediate = true;
        }
    }

    let keys = cfg
        .accumulators
        .iter()
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for key in keys {
        let Some(accumulator) = cfg
            .accumulators
            .get(&key)
            .map(|entry| Arc::clone(entry.value()))
        else {
            continue;
        };
        let accumulator = accumulator.lock().await;
        let has_current = accumulator
            .current
            .as_ref()
            .is_some_and(|batch| !batch.is_empty());
        let has_ready = !accumulator.ready.is_empty();
        if !has_current && !has_ready {
            continue;
        }
        schedule.settled = false;
        // The oldest batch of the partition reaches the delivery timeout
        // first, also while the partition waits for its resend.
        if let Some(deadline) = accumulator
            .ready
            .front()
            .or(accumulator.current.as_ref())
            .and_then(|batch| delivery_deadline(batch.first_append_at, cfg.delivery_timeout))
        {
            include_deadline(&mut schedule, deadline, now);
        }
        let has_recovery_invalid =
            accumulator.current.as_ref().is_some_and(|batch| {
                batch_crosses_recovery_barrier(cfg, batch.transaction_generation)
            }) || accumulator
                .ready
                .iter()
                .any(|batch| batch_crosses_recovery_barrier(cfg, batch.transaction_generation));
        if has_recovery_invalid {
            schedule.immediate = true;
            continue;
        }
        if state.retry.contains_key(&key) {
            continue;
        }
        if has_ready {
            schedule.immediate = true;
        }
        if let Some(batch) = accumulator
            .current
            .as_ref()
            .filter(|batch| !batch.is_empty())
        {
            if force || batch_crosses_recovery_barrier(cfg, batch.transaction_generation) {
                schedule.immediate = true;
            } else {
                include_deadline(
                    &mut schedule,
                    batch
                        .first_append_at
                        .checked_add(cfg.linger.to_std())
                        .unwrap_or(now),
                    now,
                );
            }
        }
    }

    schedule
}

#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(producer_id = cfg.producer_id, max_in_flight = cfg.max_in_flight),
)]
pub(crate) async fn run(mut cfg: SenderConfig) {
    let mut state = PipelineState::default();
    let mut force = false;
    let mut stopping = false;
    loop {
        let next = schedule(&cfg, &state, force).await;
        if next.immediate {
            let intent = if force {
                DrainIntent::Force
            } else {
                DrainIntent::Expired
            };
            drain_once(&mut cfg, &mut state, intent).await;
            continue;
        }
        if force && next.settled {
            force = false;
            if stopping {
                break;
            }
        }
        if stopping && next.settled {
            break;
        }
        if stopping {
            tokio::time::sleep_until(
                next.deadline
                    .expect("unsettled stopped sender has a retry deadline"),
            )
            .await;
            continue;
        }

        let received = if let Some(deadline) = next.deadline {
            tokio::select! {
                () = cfg.shutdown.cancelled() => None,
                received = cfg.wake_rx.recv() => received,
                () = tokio::time::sleep_until(deadline) => continue,
            }
        } else {
            tokio::select! {
                () = cfg.shutdown.cancelled() => None,
                received = cfg.wake_rx.recv() => received,
            }
        };
        match received {
            Some(DrainIntent::Force) => force = true,
            Some(DrainIntent::Ready | DrainIntent::Expired) => {}
            None => {
                force = true;
                stopping = true;
            }
        }
    }
}

/// One drained partition's batch, prepared for sending. It holds the encoded v2
/// `RecordBatch`, with its `base_sequence` already allocated, the topic id, and
/// the `PendingRecord`s whose oneshot acks the response resolves.
///
/// The `record_batch` is built **once**, which allocates the sequence once, so
/// a re-route or a resend ships the identical bytes. That preserves
/// per-partition idempotent sequencing: the leader sees each partition's
/// `base_sequence` exactly once, in increasing order, whichever broker it
/// reaches.
struct PreparedBatch {
    topic: String,
    partition: i32,
    topic_id: Uuid,
    /// The allocated base sequence for this batch. It is cached here, rather
    /// than re-read from `record_batch.base_sequence`, so that it is
    /// unambiguous for a transactional batch, and so that debug logging can
    /// name the batch.
    base_sequence: i32,
    record_batch: RecordBatch,
    records: Vec<PendingRecord>,
    /// The time the first record went into the batch. The delivery timeout
    /// counts from it, as Kafka's `ProducerBatch.createdMs`.
    created_at: Instant,
    /// When `Some`, the batch must not be resent until this instant after a
    /// transport failure, missing response, or retriable/routing broker
    /// response. This prevents failed sends from hot-looping the drain
    /// scheduler.
    backoff_until: Option<Instant>,
    /// Resends already admitted after the initial send.
    retries_used: i32,
    /// Backoffs already taken. The next backoff grows with it, as Kafka's
    /// `ProducerBatch.attempts`.
    backoff_attempts: u32,
    /// Why the last send did not ack the batch. It becomes the error of the
    /// records when the retries or the delivery timeout ends.
    last_failure: Option<SendFailure>,
    transaction_generation: Option<u64>,
    /// The buffer memory of the batch. It returns to the pool when the batch
    /// completes and is dropped, as Kafka's `Sender` deallocates a batch when
    /// it completes.
    _memory: Option<MemoryReservation>,
}

/// Why one send of a batch did not ack it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendFailure {
    /// The broker answered with this error code.
    Code(i16),
    /// The request did not reach the broker, or its answer did not name the
    /// partition.
    Transport,
}

impl SendFailure {
    /// The error that the records of a batch get when its retries run out.
    /// Kafka's `Sender.failBatch` gives the records the last error, and its
    /// expiry path gives them a `TimeoutException`.
    const fn error(self) -> ProducerError {
        match self {
            Self::Code(code) => ProducerError::Server(code),
            Self::Transport => ProducerError::SendTimeout,
        }
    }
}

/// One drain cycle.
///
/// It builds the send list: each partition's pending resend first, then one
/// newly drained batch for each *idle* partition. It sends every batch as its
/// own single-partition `ProduceRequest` **concurrently**. It then dispatches
/// each [`BatchVerdict`]: ack, park for resend, terminal fail, or fence.
///
/// **Per-partition ordering / idempotence.** A partition contributes at most one
/// batch per cycle. It contributes either its pending resend, held in
/// [`PipelineState::retry`], *or* one new batch when idle, and never both. The
/// `occupied` set enforces the "never both". The broker therefore never sees
/// two outstanding sequences for a partition, and a failing partition can never
/// interleave a fresh batch ahead of its pending resend. Each batch's
/// `record_batch`, and therefore its `base_sequence`, is built once and resent
/// verbatim, so the leader sees each sequence exactly once, in order. Ordering
/// is preserved by construction.
///
/// `in_flight` accounting works like this. `fetch_add` runs only when a NEW
/// batch is drained from an accumulator, because resends were counted when they
/// were first drained. `fetch_sub` runs only when a batch reaches a terminal
/// outcome: an ack, a terminal failure, a fence, or a reached delivery timeout.
/// `flush_notify` wakes when `in_flight` hits zero, and when there is nothing to
/// send.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(batches = tracing::field::Empty),
)]
async fn drain_once(cfg: &mut SenderConfig, state: &mut PipelineState, intent: DrainIntent) {
    if cfg.state.load(Ordering::Acquire) != STATE_ACTIVE {
        fence(cfg, state, Vec::new());
        return;
    }
    let now = Instant::now();

    // 1. Fail undrained batches that crossed transaction recovery, then process
    //    resends: each partition's single failed batch must precede any new
    //    batch for that partition. `collect_retries` drains the retry slots
    //    and returns batches that reached the delivery timeout, which we fail here
    //    (their in-flight slot was counted at first drain, so `finish_in_flight`
    //    once).
    update_partition_load_stats(cfg, state).await;
    fail_recovered_accumulator_batches(cfg).await;
    fail_recovered_retry_slots(cfg, &mut state.retry);
    fail_expired_accumulator_batches(cfg, now).await;
    let (mut to_send, expired) = collect_retries(
        &mut state.retry,
        now,
        cfg.delivery_timeout,
        cfg.max_in_flight,
    );
    if !expired.is_empty() {
        expire_batches(cfg, state, expired, to_send);
        return;
    }
    fail_recovered_batches(cfg, &mut to_send);

    // A partition with a pending resend is "occupied": it must not also send a
    // new batch (two same-partition requests on the wire could reorder and trip
    // `OUT_OF_ORDER_SEQUENCE_NUMBER`). Its next batch waits in the accumulator
    // until the resend acks and the slot frees. This covers both batches
    // resending this cycle (`to_send`) and ones still parked in their retry slot
    // backing off (`state.retry`) — a backed-off slot is still occupied.
    let mut occupied: HashSet<(String, i32)> = to_send
        .iter()
        .map(|pb| (pb.topic.clone(), pb.partition))
        .chain(state.retry.keys().cloned())
        .collect();

    // 2. One eligible new batch per idle partition. Across partitions we fan out
    //    concurrently, but bound the cycle's total fan-out to `max_in_flight`
    //    (the per-connection pipelining bound); partitions not reached this cycle
    //    are picked up on the next drain cycle (their retry slots carry forward,
    //    so none is starved). `in_flight` is incremented per new batch while the
    //    accumulator lock is held, so a concurrent `flush` never sees a batch
    //    that is neither in the accumulator nor counted in flight.
    let keys: Vec<(String, i32)> = cfg.accumulators.iter().map(|e| e.key().clone()).collect();
    let track_latency = !cfg.partitioner.config().availability_timeout.is_zero();
    for pb in &to_send {
        note_node_latency(cfg, state, &(pb.topic.clone(), pb.partition), now, true);
    }
    for key in keys {
        let capped = to_send.len() >= cfg.max_in_flight;
        if capped && !track_latency {
            break;
        }
        let acc = match cfg.accumulators.get(&key) {
            Some(a) => Arc::clone(a.value()),
            None => continue,
        };
        // A partition with a resend in flight keeps its one slot; skip it.
        let waits_for_resend = occupied.contains(&key);
        if capped || waits_for_resend {
            // Records wait behind a resend to their broker. Kafka's
            // `Sender.sendProducerData` notes the same case when
            // `NetworkClient.ready` refuses a node with ready data. The cap of
            // this cycle alone does not note the broker: the cycle did not
            // try it, and its connection can be idle.
            if track_latency && waits_for_resend && acc.lock().await.queue_size() > 0 {
                note_node_latency(cfg, state, &key, now, false);
            }
            continue;
        }
        // Seal only when this drain's cause permits it, then take a single
        // ready batch. A rollover wake must not pull an unrelated young
        // partition into the same send.
        {
            let mut a = acc.lock().await;
            let should_seal = a.current.as_ref().is_some_and(|batch| {
                !batch.is_empty()
                    && (matches!(intent, DrainIntent::Force)
                        || batch_crosses_recovery_barrier(cfg, batch.transaction_generation)
                        || (matches!(intent, DrainIntent::Expired)
                            && now
                                .saturating_duration_since(batch.first_append_at)
                                .as_time()
                                >= cfg.linger))
            });
            if should_seal {
                a.seal_current();
            }
        }
        let batch = {
            let mut a = acc.lock().await;
            let b = a.ready.pop_front();
            if b.is_some() {
                cfg.in_flight.fetch_add(1, Ordering::AcqRel);
            }
            b
        };
        let Some(batch) = batch else { continue };
        note_node_latency(cfg, state, &key, now, true);
        if batch_crosses_recovery_barrier(cfg, batch.transaction_generation) {
            fail_batch(batch.records, ProducerError::RecoveryRequired);
            finish_in_flight(cfg);
            continue;
        }
        let pb = prepare_batch(cfg, &key.0, key.1, batch).await;
        occupied.insert(key);
        to_send.push(pb);
    }

    tracing::Span::current().record("batches", to_send.len());
    if to_send.is_empty() {
        cfg.flush_notify.notify_waiters();
        return;
    }

    // 3. Send every batch concurrently, then apply each verdict to its window.
    send_batches(cfg, state, to_send).await;
}

/// Fail every batch that reached the delivery timeout, and park the rest of
/// this cycle's send list for the next cycle.
///
/// Kafka's `Sender.sendProducerData` fails an expired batch with a
/// `TimeoutException` and calls `TransactionManager.handleFailedBatch`, which
/// raises the epoch of an idempotent producer, gives the sequences of a
/// transactional batch back, and moves a transactional producer to
/// `ABORTABLE_ERROR` (applied by `terminal_fail_batch`). It does not fence the
/// producer. The epoch bump starts at the next batch that the sender builds:
/// Kafka's `maybeUpdateProducerIdAndEpoch` moves a partition to the new
/// identity only while it has no batch in flight.
fn expire_batches(
    cfg: &SenderConfig,
    state: &mut PipelineState,
    expired: Vec<PreparedBatch>,
    to_send: Vec<PreparedBatch>,
) {
    let mut bump_epoch = false;
    let mut repairs: Vec<((String, i32), SequenceRepair, i32)> = Vec::new();
    let mut failed: Vec<PreparedBatch> = Vec::new();
    for pb in expired {
        let repair = BatchMode::of(&pb.record_batch).repair_after_failure();
        record_repair(&mut bump_epoch, &mut repairs, &pb, repair);
        tracing::warn!(
            topic = %pb.topic,
            partition = pb.partition,
            base_sequence = pb.base_sequence,
            "the batch reached the delivery timeout; failing its records",
        );
        failed.push(pb);
    }
    if bump_epoch && bump_idempotent_epoch(cfg).is_none() {
        // Kafka gets a new producer id with `InitProducerId` when the epoch
        // overflows. This sender cannot send that request, so it fences.
        tracing::error!("idempotent producer epoch overflow; fencing the producer");
        for pb in failed {
            terminal_fail_batch(cfg, pb, ProducerError::SendTimeout);
        }
        fence(cfg, state, to_send);
        return;
    }
    if !bump_epoch {
        for (key, repair, base_sequence) in repairs {
            if repair == SequenceRepair::GiveBack {
                cfg.next_seq.insert(key, base_sequence);
            } else {
                cfg.next_seq.remove(&key);
            }
        }
    }
    // Every other batch of the cycle keeps its epoch, its sequence and its
    // bytes, and it goes back into its retry slot. A batch whose answer was
    // lost can already be on the log, and the broker dedups only a resend that
    // carries the same identity. The epoch bump applies to the batches that
    // the sender builds after it.
    for pb in to_send {
        state.retry.insert((pb.topic.clone(), pb.partition), pb);
    }
    for pb in failed {
        terminal_fail_batch(cfg, pb, ProducerError::SendTimeout);
    }
}

/// Fail the batches in the accumulators that reached the delivery timeout
/// before the sender drained them.
///
/// Kafka's `RecordAccumulator.expiredBatches` takes the batches at the front
/// of each partition queue while they reached the delivery timeout, and
/// `Sender.failExpiredBatches` fails them with a `TimeoutException`. These
/// batches have no sequence yet, so no sequence repair applies. A batch of a
/// transaction moves the transaction to the abortable error, as
/// `TransactionManager.handleFailedBatch` does.
async fn fail_expired_accumulator_batches(cfg: &SenderConfig, now: Instant) {
    let accumulators = cfg
        .accumulators
        .iter()
        .map(|entry| Arc::clone(entry.value()))
        .collect::<Vec<_>>();
    let mut expired = Vec::new();
    for accumulator in accumulators {
        let mut accumulator = accumulator.lock().await;
        while accumulator.ready.front().is_some_and(|batch| {
            reached_delivery_timeout(batch.first_append_at, cfg.delivery_timeout, now)
        }) {
            expired.extend(accumulator.ready.pop_front());
        }
        if accumulator.ready.is_empty()
            && accumulator.current.as_ref().is_some_and(|batch| {
                !batch.is_empty()
                    && reached_delivery_timeout(batch.first_append_at, cfg.delivery_timeout, now)
            })
        {
            expired.extend(accumulator.current.take());
        }
    }
    if expired.is_empty() {
        return;
    }
    for batch in expired {
        tracing::warn!(
            records = batch.records.len(),
            "a batch reached the delivery timeout before its first send; failing its records",
        );
        if batch.transaction_generation.is_some() {
            cfg.txn_abortable_error.set_timeout();
        }
        fail_batch(batch.records, ProducerError::SendTimeout);
    }
    cfg.flush_notify.notify_waiters();
}

/// Drain the per-partition retry slots into an ordered send list, in the
/// one-slot model.
///
/// Each partition holds **at most one** failed batch that awaits a verbatim
/// resend. A batch that reached the delivery timeout, measured from its
/// creation, goes into `expired`, and the caller fails it instead of resending
/// it. Every resent batch keeps its allocated `base_sequence` and its bytes,
/// and the broker dedups a re-landed write with `DUPLICATE_SEQUENCE_NUMBER`.
///
/// The function is pure over the retry map, with no `Client` and no I/O, so the
/// expiry logic is unit-testable without a broker.
fn collect_retries(
    retry: &mut HashMap<(String, i32), PreparedBatch>,
    now: Instant,
    delivery_timeout: Time,
    max_to_send: usize,
) -> (Vec<PreparedBatch>, Vec<PreparedBatch>) {
    let mut to_send: Vec<PreparedBatch> = Vec::new();
    let mut expired: Vec<PreparedBatch> = Vec::new();
    // Batches still backing off after a transport failure are re-parked here so
    // a down/refusing leader doesn't hot-loop the drain scheduler.
    let mut parked: Vec<((String, i32), PreparedBatch)> = Vec::new();

    for (key, mut pb) in retry.drain() {
        if reached_delivery_timeout(pb.created_at, delivery_timeout, now) {
            expired.push(pb);
            continue;
        }
        // Honour a retry backoff: the batch waits in its slot until its
        // `backoff_until` passes. Set after a transport failure and after a
        // routing rejection (NOT_LEADER / UNKNOWN_TOPIC) so a partition that is
        // still settling at cold boot isn't hammered in a tight resend loop.
        if pb.backoff_until.is_some_and(|t| now < t) {
            parked.push((key, pb));
            continue;
        }
        if to_send.len() >= max_to_send {
            parked.push((key, pb));
            continue;
        }
        pb.backoff_until = None;
        to_send.push(pb);
    }

    for (key, pb) in parked {
        retry.insert(key, pb);
    }

    (to_send, expired)
}

/// Per-batch verdict that [`send_batches`] consumes in the one-slot model. The
/// broker durably accepted the batch, or the batch must be resent, or it failed
/// with a server code, or it fatally fenced the producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchVerdict {
    /// Durably written, with `NONE`, or already present, with
    /// `DUPLICATE_SEQUENCE_NUMBER`.
    Acked {
        base_offset: i64,
    },
    /// Resend verbatim on the next cycle, after a transport failure, a
    /// retriable code, or a routing error.
    Retry,
    /// Raise the idempotent producer epoch, rewrite the batch at the new epoch
    /// and sequence 0, and resend it. Kafka's idempotent producer does this for
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER` and `UNKNOWN_PRODUCER_ID`
    /// (`requestIdempotentEpochBumpForPartition`, then `canRetry` gives
    /// `true`).
    BumpEpochAndRetry,
    /// Rewrite the batch at sequence 0 with the same epoch, and resend it.
    /// Kafka's transactional producer does this for `UNKNOWN_PRODUCER_ID` after
    /// the log start moved past its last acked offset
    /// (`TxnPartitionMap.startSequencesAtBeginning`).
    RestartSequenceAndRetry,
    /// Fail the records with `Server(code)`, then repair the partition
    /// sequence.
    Terminal {
        code: i16,
        repair: SequenceRepair,
    },
    /// Split the batch in two, put both parts back at the front of the
    /// accumulator, and send them again. Kafka's `Sender.completeBatch` splits
    /// a batch of more than one record that gets `MESSAGE_TOO_LARGE`
    /// (`RecordAccumulator.splitAndReenqueue`), and it does not count the
    /// attempt.
    Split,
    /// Fail the records and fence the producer. Kafka's
    /// `TransactionManager.maybeTransitionToErrorState` makes these codes
    /// fatal.
    Fatal(i16),
    RecoveryRequired,
}

/// How the producer stamped a batch. Kafka's `Sender` handles a failed batch
/// differently in each mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchMode {
    /// Idempotence is off, and the batch carries no producer id. Kafka's
    /// `Sender` has no `TransactionManager` in this mode.
    Plain,
    /// The idempotent producer stamped the batch.
    Idempotent,
    /// The batch belongs to a transaction.
    Transactional,
}

impl BatchMode {
    fn of(batch: &RecordBatch) -> Self {
        if batch.attributes.is_transactional() {
            Self::Transactional
        } else if batch.producer_id >= 0 {
            Self::Idempotent
        } else {
            Self::Plain
        }
    }

    /// The sequence repair after a batch fails with a code that no rule
    /// retries. Kafka's `Sender.failBatch` calls
    /// `TransactionManager.handleFailedBatch`, which bumps the epoch of an
    /// idempotent producer and gives the sequences back in a transaction.
    const fn repair_after_failure(self) -> SequenceRepair {
        match self {
            Self::Plain => SequenceRepair::Keep,
            Self::Idempotent => SequenceRepair::BumpEpoch,
            Self::Transactional => SequenceRepair::GiveBack,
        }
    }
}

/// What happens to the partition sequence after the sender fails a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SequenceRepair {
    /// Keep the sequence. The batch carries no producer id.
    Keep,
    /// Raise the idempotent producer epoch, which starts every partition again
    /// at sequence 0 (`requestIdempotentEpochBumpForPartition`).
    BumpEpoch,
    /// Give the sequences of the failed batch back to the partition, so the
    /// next batch takes them (`TxnPartitionMap.adjustSequencesDueToFailedBatch`).
    GiveBack,
    /// Start the partition again at sequence 0 (`resetSequenceForPartition`).
    Reset,
}

/// Classification of a per-partition `error_code`. It is either a direct
/// [`BatchVerdict`], or [`Classification::Routing`] for a code whose Kafka
/// exception extends `InvalidMetadataException`. Routing means a retry, plus
/// the leader-hint adoption and metadata refresh side effects that
/// [`interpret_response`] applies. The classification is kept separate so the
/// pure code-to-verdict mapping is unit-testable without a `Client`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Classification {
    Verdict(BatchVerdict),
    Routing,
}

/// The fields of one partition answer that decide its [`Classification`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionAnswer {
    error_code: i16,
    base_offset: i64,
    log_start_offset: i64,
}

/// Map a per-partition answer to its [`Classification`]. The function is pure
/// and does no I/O. `records_in_batch` is the number of records of the batch,
/// which decides whether `MESSAGE_TOO_LARGE` splits it.
///
/// The rules follow Kafka's `Sender.completeBatch`, `Sender.canRetry`,
/// `TransactionManager.canRetry` and `TransactionManager.handleFailedBatch`.
/// `last_acked_offset` is the offset of the last record the broker acked for
/// the partition.
///
/// With one batch in flight per partition, a batch that gets
/// `OUT_OF_ORDER_SEQUENCE_NUMBER` is always the next sequence after the last
/// acked batch. Kafka's "not the next sequence, so retry" case therefore never
/// applies.
fn classify_verdict(
    answer: PartitionAnswer,
    mode: BatchMode,
    last_acked_offset: Option<i64>,
    records_in_batch: usize,
) -> Classification {
    let code = answer.error_code;
    let verdict = match (code, mode) {
        // A batch of more than one record splits, and the parts go out again.
        // A single-record batch fails, because no split can make it smaller.
        (codes::MESSAGE_TOO_LARGE, _) if records_in_batch > 1 => BatchVerdict::Split,
        // The broker durably wrote the batch (NONE) or already had it
        // (DUPLICATE_SEQUENCE_NUMBER). `Sender.completeBatch` completes both.
        (codes::NONE | codes::DUPLICATE_SEQUENCE_NUMBER, _) => BatchVerdict::Acked {
            base_offset: answer.base_offset,
        },
        (
            codes::CLUSTER_AUTHORIZATION_FAILED
            | codes::UNSUPPORTED_VERSION
            | codes::INVALID_PRODUCER_ID_MAPPING
            | codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED
            | codes::PRODUCER_FENCED,
            BatchMode::Idempotent | BatchMode::Transactional,
        ) => BatchVerdict::Fatal(code),
        (codes::OUT_OF_ORDER_SEQUENCE_NUMBER, BatchMode::Idempotent) => {
            BatchVerdict::BumpEpochAndRetry
        }
        // The broker did not know the log start offset yet. Kafka retries
        // until it does.
        (codes::UNKNOWN_PRODUCER_ID, BatchMode::Idempotent | BatchMode::Transactional)
            if answer.log_start_offset < 0 =>
        {
            BatchVerdict::Retry
        }
        (codes::UNKNOWN_PRODUCER_ID, BatchMode::Idempotent) => BatchVerdict::BumpEpochAndRetry,
        (codes::UNKNOWN_PRODUCER_ID, BatchMode::Transactional)
            if last_acked_offset.unwrap_or(-1) < answer.log_start_offset =>
        {
            BatchVerdict::RestartSequenceAndRetry
        }
        (codes::UNKNOWN_PRODUCER_ID, BatchMode::Transactional) => BatchVerdict::Terminal {
            code,
            repair: SequenceRepair::Reset,
        },
        _ => match error_class::class(code) {
            ErrorClass::InvalidMetadata => return Classification::Routing,
            ErrorClass::Retriable => BatchVerdict::Retry,
            ErrorClass::None | ErrorClass::NotRetriable => BatchVerdict::Terminal {
                code,
                repair: mode.repair_after_failure(),
            },
        },
    };
    Classification::Verdict(verdict)
}

/// Resolve a partition's leader id from the cache.
///
/// It returns [`BOOTSTRAP_LEADER`] when the leader is unknown, that is `< 0`,
/// when the leader is uncached, or when the pool has no dialable address for
/// it, such as a port-0 in-process test broker. Those cases fall back to the
/// bootstrap connection.
fn resolve_leader(cfg: &SenderConfig, topic: &str, partition: i32) -> i32 {
    match cfg
        .partition_leaders
        .get(&(topic.to_string(), partition))
        .map(|e| *e.value())
    {
        Some(id) if id >= 0 && cfg.transport.knows_broker(id) => id,
        _ => BOOTSTRAP_LEADER,
    }
}

async fn topic_partition_count(cfg: &SenderConfig, topic: &str) -> Option<i32> {
    cfg.metadata_cache
        .lock()
        .await
        .get(topic)
        .and_then(|meta| positive_partition_count(meta.num_partitions))
}

fn positive_partition_count(count: i32) -> Option<i32> {
    (count > 0).then_some(count)
}

/// Build the v2 `RecordBatch` for a drained partition batch, and allocate its
/// `base_sequence` range from `next_seq`. The sender sends the result verbatim,
/// and resends it verbatim on a retry, so it allocates the sequence exactly
/// once per batch.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        topic = %topic,
        partition,
        batch_records = batch.records.len(),
        base_sequence = tracing::field::Empty,
    ),
)]
async fn prepare_batch(
    cfg: &SenderConfig,
    topic: &str,
    partition: i32,
    batch: InProgressBatch,
) -> PreparedBatch {
    // Allocate the base_sequence range for this batch (once).
    let base_sequence = {
        let mut entry = cfg
            .next_seq
            .entry((topic.to_string(), partition))
            .or_insert(0);
        let cur = *entry;
        let count = i32::try_from(batch.records.len()).unwrap_or(i32::MAX);
        *entry = cur.wrapping_add(count);
        cur
    };
    tracing::Span::current().record("base_sequence", base_sequence);

    // Resolve the topic_id from the metadata cache (zero is fine — the broker
    // falls back to the `name` field for v ≤ 12).
    let topic_id = cfg
        .metadata_cache
        .lock()
        .await
        .get(topic)
        .map_or(Uuid::ZERO, |m| m.topic_id);

    // Snapshot the transactional pid/epoch once per batch.
    let txn_snapshot = txn_pid_snapshot(cfg).await;

    let record_batch = build_record_batch(cfg, &batch, base_sequence, txn_snapshot);

    PreparedBatch {
        topic: topic.to_string(),
        partition,
        topic_id,
        base_sequence,
        record_batch,
        records: batch.records,
        created_at: batch.first_append_at,
        backoff_until: None,
        retries_used: 0,
        backoff_attempts: 0,
        last_failure: None,
        transaction_generation: batch.transaction_generation,
        _memory: batch.memory,
    }
}

/// One batch's send result: the batch, its [`BatchVerdict`], and whether a
/// metadata refresh is needed before any resend can route correctly.
struct BatchSendResult {
    pb: PreparedBatch,
    verdict: BatchVerdict,
    /// A metadata refresh is required before a resend can route correctly. The
    /// partition came back mis-routed with no usable inline leader hint, or the
    /// hinted leader's address is unknown.
    refresh_needed: bool,
}

/// Send every batch in `to_send` as its own single-partition `ProduceRequest`,
/// **concurrently**, then dispatch each [`BatchVerdict`]: ack the records, fail
/// them terminally, park the batch in its partition's retry slot for a verbatim
/// resend on the next cycle, or fence the producer.
///
/// The concurrency is cross-partition request pipelining. Every batch in
/// `to_send` is for a *distinct* partition, which the `occupied` set of
/// [`drain_once`] guarantees, and the brokers are independent. Overlapping the
/// round-trips therefore keeps every broker busy, never puts two same-partition
/// requests on the wire, and leaves per-partition ordering undisturbed. The
/// futures are polled on this one task, with no spawn, so they share `&cfg`
/// safely.
#[tracing::instrument(level = "debug", skip_all, fields(batches = to_send.len()))]
async fn send_batches(cfg: &SenderConfig, state: &mut PipelineState, to_send: Vec<PreparedBatch>) {
    let mut to_send = to_send;
    fail_recovered_batches(cfg, &mut to_send);
    let mut results: FuturesUnordered<_> = to_send
        .into_iter()
        .map(|pb| {
            let last_acked_offset = state
                .last_acked_offset
                .get(&(pb.topic.clone(), pb.partition))
                .copied();
            send_one_batch(cfg, pb, last_acked_offset)
        })
        .collect();

    let mut needs_refresh = false;
    let mut fenced: Option<Vec<PreparedBatch>> = None;
    // Kafka's idempotent producer bumps its epoch once for all the partitions
    // that asked for it (`bumpIdempotentEpochAndResetIdIfNeeded`). The sender
    // does the same after it has every verdict of the cycle.
    let mut bump_epoch = false;
    let mut repairs: Vec<((String, i32), SequenceRepair, i32)> = Vec::new();
    let mut restarts: Vec<PreparedBatch> = Vec::new();
    let mut rewrites: Vec<PreparedBatch> = Vec::new();
    let mut splits: Vec<PreparedBatch> = Vec::new();
    // Failed batches resolve their records after the sequence repair, so a
    // caller that sees the error already sees the repaired producer.
    let mut failed: Vec<(PreparedBatch, ProducerError)> = Vec::new();
    while let Some(res) = results.next().await {
        let BatchSendResult {
            mut pb,
            verdict,
            refresh_needed,
        } = res;
        needs_refresh |= refresh_needed;

        if let Some(to_fail) = &mut fenced {
            match verdict {
                BatchVerdict::Acked { base_offset } => ack_batch(cfg, pb, base_offset),
                BatchVerdict::Terminal { code, .. } => {
                    terminal_fail_batch(cfg, pb, ProducerError::Server(code));
                }
                BatchVerdict::Fatal(code) => {
                    fail_batch(pb.records, fatal_error(code));
                    finish_in_flight(cfg);
                }
                BatchVerdict::RecoveryRequired => {
                    fail_batch(pb.records, ProducerError::RecoveryRequired);
                    finish_in_flight(cfg);
                }
                BatchVerdict::Retry
                | BatchVerdict::BumpEpochAndRetry
                | BatchVerdict::RestartSequenceAndRetry
                | BatchVerdict::Split => to_fail.push(pb),
            }
            continue;
        }

        match verdict {
            // Durable: resolve the records with their offsets, free the slot.
            BatchVerdict::Acked { base_offset } => {
                state.record_ack(&pb, base_offset);
                ack_batch(cfg, pb, base_offset);
            }
            // Terminal server error: fail the records, free the slot, and
            // repair the partition sequence after the cycle.
            BatchVerdict::Terminal { code, repair } => {
                record_repair(&mut bump_epoch, &mut repairs, &pb, repair);
                failed.push((pb, ProducerError::Server(code)));
            }
            // The broker rejected the batch as too large. Split it, and send
            // the parts again.
            BatchVerdict::Split => splits.push(pb),
            // Transport failure, routing error or retriable code: park in the
            // partition's single retry slot, resent next cycle. The batch is
            // still outstanding, so its in-flight slot stays counted, and there
            // is no `finish_in_flight` here.
            //
            // The batch has no retry left. Kafka's `Sender.failBatch` fails
            // the records and calls `TransactionManager.handleFailedBatch`,
            // which raises the epoch of an idempotent producer, gives the
            // sequences of a transactional batch back, and moves a
            // transactional producer to `ABORTABLE_ERROR` (applied by
            // `terminal_fail_batch`). It does not fence the producer.
            BatchVerdict::Retry
            | BatchVerdict::BumpEpochAndRetry
            | BatchVerdict::RestartSequenceAndRetry
                if take_retry(&mut pb, cfg.retries) =>
            {
                let repair = BatchMode::of(&pb.record_batch).repair_after_failure();
                record_repair(&mut bump_epoch, &mut repairs, &pb, repair);
                let error = pb.last_failure.unwrap_or(SendFailure::Transport).error();
                tracing::warn!(
                    topic = %pb.topic,
                    partition = pb.partition,
                    base_sequence = pb.base_sequence,
                    "batch has no retry left; failing its records",
                );
                failed.push((pb, error));
            }
            BatchVerdict::Retry => {
                tracing::debug!(
                    topic = %pb.topic,
                    partition = pb.partition,
                    base_sequence = pb.base_sequence,
                    "parking batch for verbatim resend",
                );
                state.retry.insert((pb.topic.clone(), pb.partition), pb);
            }
            BatchVerdict::BumpEpochAndRetry => rewrites.push(pb),
            BatchVerdict::RestartSequenceAndRetry => restarts.push(pb),
            // A fatal error. Fail this batch plus every batch we have not yet
            // processed (their in-flight slots are counted, so they must be
            // released), then fence the producer and stop sending.
            BatchVerdict::Fatal(code) => {
                fail_batch(pb.records, fatal_error(code));
                finish_in_flight(cfg);
                fenced = Some(Vec::new());
            }
            BatchVerdict::RecoveryRequired => {
                fail_batch(pb.records, ProducerError::RecoveryRequired);
                finish_in_flight(cfg);
            }
        }
    }
    drop(results);

    if let Some(mut to_fail) = fenced {
        for (pb, error) in failed {
            terminal_fail_batch(cfg, pb, error);
        }
        to_fail.append(&mut restarts);
        to_fail.append(&mut rewrites);
        to_fail.append(&mut splits);
        fence(cfg, state, to_fail);
        return;
    }

    let mut epoch_bumped = false;
    if bump_epoch || !rewrites.is_empty() {
        let Some(epoch) = bump_idempotent_epoch(cfg) else {
            // Kafka gets a new producer id with `InitProducerId` when the epoch
            // overflows. This sender cannot send that request, so it fences.
            tracing::error!("idempotent producer epoch overflow; fencing the producer");
            for (pb, error) in failed {
                terminal_fail_batch(cfg, pb, error);
            }
            rewrites.append(&mut splits);
            fence(cfg, state, rewrites);
            return;
        };
        epoch_bumped = true;
        for mut pb in rewrites {
            pb.record_batch.producer_epoch = epoch;
            restart_sequence(cfg, &mut pb);
            state.retry.insert((pb.topic.clone(), pb.partition), pb);
        }
    }
    for (key, repair, base_sequence) in repairs {
        if repair == SequenceRepair::GiveBack {
            cfg.next_seq.insert(key, base_sequence);
        } else {
            cfg.next_seq.remove(&key);
        }
    }
    for mut pb in restarts {
        restart_sequence(cfg, &mut pb);
        state.retry.insert((pb.topic.clone(), pb.partition), pb);
    }
    for pb in splits {
        split_and_requeue(cfg, pb, epoch_bumped).await;
    }
    for (pb, error) in failed {
        terminal_fail_batch(cfg, pb, error);
    }

    if needs_refresh {
        update_leaders_from_metadata(cfg).await;
    }
}

impl PipelineState {
    /// Keep the offset of the last record of an acked batch, as Kafka's
    /// `TransactionManager.updateLastAckedOffset` does.
    fn record_ack(&mut self, pb: &PreparedBatch, base_offset: i64) {
        if base_offset < 0 {
            return;
        }
        let last_offset = base_offset + i64::from(pb.record_batch.last_offset_delta);
        self.last_acked_offset
            .entry((pb.topic.clone(), pb.partition))
            .and_modify(|offset| *offset = (*offset).max(last_offset))
            .or_insert(last_offset);
    }
}

/// The error for the records of a batch that failed with a fatal code.
const fn fatal_error(code: i16) -> ProducerError {
    if code == codes::PRODUCER_FENCED {
        ProducerError::FencedProducer
    } else {
        ProducerError::Server(code)
    }
}

/// Raise the idempotent producer epoch by one, and start every partition
/// again at sequence 0. It returns the new epoch, or `None` when the epoch is
/// at its maximum.
///
/// Kafka's `TransactionManager.bumpIdempotentProducerEpoch` raises the epoch
/// on the client, and `maybeUpdateProducerIdAndEpoch` starts a partition at
/// sequence 0 once it has no batch in flight. A broker accepts a higher epoch
/// with sequence 0 from an idempotent producer. This sender keeps at most one
/// batch per partition in flight, and it builds that batch before this call,
/// so the next batch it builds for any partition is the first one at the new
/// epoch.
fn bump_idempotent_epoch(cfg: &SenderConfig) -> Option<i16> {
    let epoch = cfg.producer_epoch.load(Ordering::Acquire).checked_add(1)?;
    cfg.producer_epoch.store(epoch, Ordering::Release);
    cfg.next_seq.clear();
    tracing::info!(
        producer_id = cfg.producer_id,
        producer_epoch = epoch,
        "bumped the idempotent producer epoch; sequences start again at 0"
    );
    Some(epoch)
}

/// Rewrite `pb` at sequence 0, and give the partition the sequences after it.
fn restart_sequence(cfg: &SenderConfig, pb: &mut PreparedBatch) {
    let count = i32::try_from(pb.records.len()).unwrap_or(i32::MAX);
    cfg.next_seq.insert((pb.topic.clone(), pb.partition), count);
    pb.base_sequence = 0;
    pb.record_batch.base_sequence = 0;
}

fn take_retry(batch: &mut PreparedBatch, retries: i32) -> bool {
    if batch.retries_used >= retries {
        true
    } else {
        batch.retries_used += 1;
        false
    }
}

/// Ack a batch's records with their broker-assigned offsets, and release its
/// in-flight slot. The per-record offset is `base_offset + offset_delta`.
fn ack_batch(cfg: &SenderConfig, pb: PreparedBatch, base_offset: i64) {
    let partition = pb.partition;
    for r in pb.records {
        let _ = r.ack.send(Ok(RecordMetadata {
            topic_index: 0,
            partition,
            offset: base_offset + i64::from(r.offset_delta),
            timestamp_ms: r.timestamp_ms,
        }));
    }
    finish_in_flight(cfg);
}

/// Terminally fail a batch. It resolves the batch's records with `error` and
/// releases the in-flight slot. It is the single owner of the slot release for
/// the batch.
fn terminal_fail_batch(cfg: &SenderConfig, pb: PreparedBatch, error: ProducerError) {
    if BatchMode::of(&pb.record_batch) == BatchMode::Transactional {
        // Kafka's `Sender.failBatch` calls
        // `TransactionManager.handleFailedBatch`, which moves a transactional
        // producer to `ABORTABLE_ERROR`. The application must abort the
        // transaction; `commit` fails until it does. A broker answer stores
        // its code, and a batch that timed out with no answer stores that it
        // timed out, as Kafka stores the raised `TimeoutException` there.
        match error {
            ProducerError::Server(code) => cfg.txn_abortable_error.set(code),
            _ => cfg.txn_abortable_error.set_timeout(),
        }
    }
    fail_batch(pb.records, error);
    finish_in_flight(cfg);
}

/// Note the sequence repair of a failed batch, so the cycle applies it after
/// every answer is in.
fn record_repair(
    bump_epoch: &mut bool,
    repairs: &mut Vec<((String, i32), SequenceRepair, i32)>,
    pb: &PreparedBatch,
    repair: SequenceRepair,
) {
    match repair {
        SequenceRepair::Keep => {}
        SequenceRepair::BumpEpoch => *bump_epoch = true,
        SequenceRepair::GiveBack | SequenceRepair::Reset => {
            repairs.push(((pb.topic.clone(), pb.partition), repair, pb.base_sequence));
        }
    }
}

/// Split a batch that the broker rejected as too large, and put both parts
/// back at the front of its accumulator.
///
/// Kafka's `RecordAccumulator.splitAndReenqueue` splits the batch at the
/// configured batch size. This sender seals a batch at that size already, so a
/// rejected batch is at most one batch size, and the same rule would give the
/// same batch back. It splits the records in half instead, so each rejection
/// halves the parts until each part holds one record. A single-record batch
/// fails with `MESSAGE_TOO_LARGE`, as it does in Kafka.
///
/// The partition takes the sequences of the batch back, so the parts get them
/// again when the sender drains them. An epoch bump in the same cycle already
/// started every partition at sequence 0, and then the parts take the
/// sequences of the new epoch.
async fn split_and_requeue(cfg: &SenderConfig, pb: PreparedBatch, epoch_bumped: bool) {
    let key = (pb.topic.clone(), pb.partition);
    let mode = BatchMode::of(&pb.record_batch);
    if mode != BatchMode::Plain && !epoch_bumped {
        cfg.next_seq.insert(key.clone(), pb.base_sequence);
    }
    let records = pb.records;
    let middle = records.len() / 2;
    let mut records = records;
    let second = records.split_off(middle);
    tracing::warn!(
        topic = %pb.topic,
        partition = pb.partition,
        base_sequence = pb.base_sequence,
        parts = 2,
        first_part_records = records.len(),
        "the broker rejected the batch as too large; splitting it",
    );
    let Some(accumulator) = cfg
        .accumulators
        .get(&key)
        .map(|entry| Arc::clone(entry.value()))
    else {
        fail_batch(records, ProducerError::Server(codes::MESSAGE_TOO_LARGE));
        fail_batch(second, ProducerError::Server(codes::MESSAGE_TOO_LARGE));
        finish_in_flight(cfg);
        return;
    };
    {
        let mut accumulator = accumulator.lock().await;
        // The front of the queue takes the parts in reverse, so the first part
        // ends up first.
        accumulator.push_front(second, pb.transaction_generation, pb.created_at);
        accumulator.push_front(records, pb.transaction_generation, pb.created_at);
    }
    // The batch itself is no longer in flight. Each part counts itself when
    // the sender drains it.
    finish_in_flight(cfg);
}

/// Fence the producer.
///
/// This marks `STATE_FENCED` and fails, with `FencedProducer`, `to_fail`, which
/// holds this cycle's still-live batches, every batch parked in a retry slot,
/// and everything in the accumulators. It releases each in-flight slot. The
/// sender calls it on a fatal error code, such as `PRODUCER_FENCED`, and when a
/// batch runs out of retries.
fn fence(cfg: &SenderConfig, state: &mut PipelineState, to_fail: Vec<PreparedBatch>) {
    cfg.state
        .compare_exchange(
            STATE_ACTIVE,
            STATE_FENCED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .ok();

    // Fail every batch from this cycle we were still holding.
    for batch in to_fail {
        fail_batch(batch.records, ProducerError::FencedProducer);
        finish_in_flight(cfg);
    }
    // Fail everything parked in the retry slots; the producer is dead.
    for (_, batch) in state.retry.drain() {
        fail_batch(batch.records, ProducerError::FencedProducer);
        finish_in_flight(cfg);
    }

    // Fail anything still sitting in the accumulators (current + ready) so no
    // caller's oneshot hangs. We use try_lock to avoid blocking — a record
    // being appended concurrently will observe STATE_FENCED on its next send.
    for entry in cfg.accumulators.iter() {
        if let Ok(mut a) = entry.value().try_lock() {
            if let Some(b) = a.current.take() {
                fail_batch(b.records, ProducerError::FencedProducer);
            }
            while let Some(b) = a.ready.pop_front() {
                fail_batch(b.records, ProducerError::FencedProducer);
            }
        }
    }
}

/// Park `pb` until its next backoff ends, and count the backoff.
///
/// Kafka's `RecordAccumulator` holds a retried batch back for
/// `ExponentialBackoff.backoff(attempts - 1)` after `ProducerBatch.reenqueued`
/// counts the attempt.
fn back_off(pb: &mut PreparedBatch, retry_backoff: RetryBackoff, now: Instant) {
    let backoff = retry_backoff.backoff(pb.backoff_attempts, jitter_unit());
    pb.backoff_until = Some(now.checked_add(backoff).unwrap_or(now));
    pb.backoff_attempts = pb.backoff_attempts.saturating_add(1);
}

/// Send a single batch as its own single-partition `ProduceRequest`, and
/// resolve its transport or broker result to a [`BatchVerdict`].
///
/// The function returns the batch alongside the verdict, because the caller
/// still owns its records. On a connection error it evicts the broker, so a
/// reconnect targets that broker's current address.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        topic = %pb.topic,
        partition = pb.partition,
        base_sequence = pb.base_sequence,
        leader = tracing::field::Empty,
    ),
)]
async fn send_one_batch(
    cfg: &SenderConfig,
    mut pb: PreparedBatch,
    last_acked_offset: Option<i64>,
) -> BatchSendResult {
    if batch_crosses_recovery_barrier(cfg, pb.transaction_generation) {
        return BatchSendResult {
            pb,
            verdict: BatchVerdict::RecoveryRequired,
            refresh_needed: false,
        };
    }
    // Produce v13 and later key topics by id and drop the name on the wire, so
    // each send must carry the id that the metadata cache holds now. Kafka's
    // `Sender.topicIdsForBatches` reads the id from the current metadata for
    // every request in the same way.
    //
    // Two cases need this. A batch prepared before its topic existed carries a
    // ZERO `topic_id`, and `update_leaders_from_metadata` backfills the cache once the topic
    // exists. A batch whose topic was deleted and created again carries the old
    // id, and the broker answers UNKNOWN_TOPIC_ID until the resend carries the
    // id that `update_leaders_from_metadata` stored. The batch resends in
    // place with the same `base_sequence`, so the idempotent sequence stays
    // gap-free and no records are dropped.
    if let Some(resolved) = cfg
        .metadata_cache
        .lock()
        .await
        .get(&pb.topic)
        .map(|m| m.topic_id)
        && resolved != Uuid::ZERO
    {
        pb.topic_id = resolved;
    }

    let leader = resolve_leader(cfg, &pb.topic, pb.partition);
    tracing::Span::current().record("leader", leader);
    let req = build_single_batch_request(cfg, &pb);

    let route = if leader == BOOTSTRAP_LEADER {
        None
    } else {
        Some(leader)
    };

    if cfg.acks == Acks::Zero {
        return match cfg.transport.send_produce_no_response(route, req).await {
            Ok(()) => BatchSendResult {
                pb,
                verdict: BatchVerdict::Acked { base_offset: -1 },
                refresh_needed: false,
            },
            Err(error) => {
                if leader != BOOTSTRAP_LEADER {
                    cfg.transport.evict_broker(leader);
                }
                tracing::warn!(
                    leader,
                    partition = pb.partition,
                    base_sequence = pb.base_sequence,
                    error = %error,
                    "acks=0 produce enqueue failed; will re-route",
                );
                back_off(&mut pb, cfg.retry_backoff, Instant::now());
                pb.last_failure = Some(SendFailure::Transport);
                BatchSendResult {
                    pb,
                    verdict: BatchVerdict::Retry,
                    refresh_needed: true,
                }
            }
        };
    }

    let resp: ProduceResponse = match cfg.transport.send_produce(route, req).await {
        Ok(response) => response,
        Err(error) => {
            // The cached connection is likely dead (broker bounced / failed
            // over). Evict it so a reconnect targets the broker's current
            // address; never evict the shared bootstrap connection.
            if leader != BOOTSTRAP_LEADER {
                cfg.transport.evict_broker(leader);
            }
            tracing::warn!(
                leader,
                partition = pb.partition,
                base_sequence = pb.base_sequence,
                error = %error,
                "produce to leader failed; will re-route",
            );
            // Park the batch for a verbatim resend after backoff, and refresh
            // metadata so the resend targets the current leader.
            back_off(&mut pb, cfg.retry_backoff, Instant::now());
            pb.last_failure = Some(SendFailure::Transport);
            return BatchSendResult {
                pb,
                verdict: BatchVerdict::Retry,
                refresh_needed: true,
            };
        }
    };

    interpret_response(cfg, pb, &resp, last_acked_offset)
}

/// Interpret a single-partition `ProduceResponse` into a [`BatchSendResult`].
/// It applies the leader-hint side effects of the routing case. The pure
/// code-to-verdict mapping lives in [`classify_verdict`].
fn interpret_response(
    cfg: &SenderConfig,
    mut pb: PreparedBatch,
    resp: &ProduceResponse,
    last_acked_offset: Option<i64>,
) -> BatchSendResult {
    let part_resp = resp
        .responses
        .iter()
        .find(|t| t.name == pb.topic || (pb.topic_id != Uuid::ZERO && t.topic_id == pb.topic_id))
        .and_then(|t| {
            t.partition_responses
                .iter()
                .find(|p| p.index == pb.partition)
        });

    let Some(part_resp) = part_resp else {
        // No matching partition in the response: treat as a retriable failure so
        // the batch resends verbatim rather than being dropped. This happens at
        // cold boot when a Produce for a not-yet-existing topic comes back as
        // UNKNOWN_TOPIC with an empty (name="", topic_id=ZERO) identity that
        // can't be correlated to our batch; the resend re-resolves `topic_id`
        // (see `send_one_batch`) once the topic exists.
        tracing::debug!(
            topic = %pb.topic,
            partition = pb.partition,
            base_sequence = pb.base_sequence,
            "produce response carried no matching partition; resending"
        );
        back_off(&mut pb, cfg.retry_backoff, Instant::now());
        pb.last_failure = Some(SendFailure::Transport);
        return BatchSendResult {
            pb,
            verdict: BatchVerdict::Retry,
            refresh_needed: true,
        };
    };

    // Surface the actual broker error code (and any inline leader hint) for a
    // rejected produce, so a retry loop can be diagnosed from the code rather
    // than inferred from the resend pattern. Error-gated to stay off the
    // happy-path hot loop; enable with RUST_LOG=krabka_client_producer=debug.
    if part_resp.error_code != 0 {
        tracing::debug!(
            topic = %pb.topic,
            partition = pb.partition,
            base_sequence = pb.base_sequence,
            error_code = part_resp.error_code,
            base_offset = part_resp.base_offset,
            leader_hint = part_resp.current_leader.leader_id,
            "produce partition rejected"
        );
    }
    let answer = PartitionAnswer {
        error_code: part_resp.error_code,
        base_offset: part_resp.base_offset,
        log_start_offset: part_resp.log_start_offset,
    };
    match classify_verdict(
        answer,
        BatchMode::of(&pb.record_batch),
        last_acked_offset,
        pb.records.len(),
    ) {
        Classification::Verdict(verdict) => {
            // Back off before a resend (e.g. NOT_ENOUGH_REPLICAS) so a
            // partition that keeps rejecting isn't hammered in a tight loop.
            if matches!(
                verdict,
                BatchVerdict::Retry
                    | BatchVerdict::BumpEpochAndRetry
                    | BatchVerdict::RestartSequenceAndRetry
            ) {
                back_off(&mut pb, cfg.retry_backoff, Instant::now());
                pb.last_failure = Some(SendFailure::Code(answer.error_code));
            }
            BatchSendResult {
                pb,
                verdict,
                refresh_needed: false,
            }
        }
        Classification::Routing => {
            // Adopt any inline leader hint immediately; otherwise (or if the
            // hinted leader's address is unknown) force a metadata refresh. The
            // batch resends verbatim, so sequencing stays monotonic.
            let hint = part_resp.current_leader.leader_id;
            let refresh_needed = if hint >= 0 {
                cfg.partition_leaders
                    .insert((pb.topic.clone(), pb.partition), hint);
                !cfg.transport.knows_broker(hint)
            } else {
                true
            };
            // Back off before re-routing. A NOT_LEADER/UNKNOWN_TOPIC at cold boot
            // (the partition's leader/writer-actor still settling) otherwise spins
            // a tight refresh+resend loop that hammers the broker and can itself
            // starve the reconcile that would make the partition writable —
            // leaving the producer stuck (observed: traces/logs WAL never advances
            // on some cold boots). Backing off lets the partition become ready.
            back_off(&mut pb, cfg.retry_backoff, Instant::now());
            pb.last_failure = Some(SendFailure::Code(part_resp.error_code));
            BatchSendResult {
                pb,
                verdict: BatchVerdict::Retry,
                refresh_needed,
            }
        }
    }
}

/// Build a single-partition, single-batch `ProduceRequest`. The transactional
/// state comes from the batch's own attributes, which are set at build time, so
/// the request-level `transactional_id` matches the batch exactly.
fn build_single_batch_request(cfg: &SenderConfig, pb: &PreparedBatch) -> ProduceRequest {
    let is_txn = pb.record_batch.attributes.is_transactional();
    let req_txn_id = if is_txn {
        cfg.transactional_id.clone()
    } else {
        None
    };

    ProduceRequest {
        transactional_id: req_txn_id,
        acks: cfg.acks.wire(),
        timeout_ms: cfg.request_timeout_ms,
        topic_data: vec![TopicProduceData {
            name: pb.topic.clone(),
            topic_id: pb.topic_id,
            partition_data: vec![PartitionProduceData {
                index: pb.partition,
                records: Some(pb.record_batch.clone().into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Refresh cluster metadata and adopt the fresh partition-to-leader map. The
/// refresh also refills the pool's broker-address registry, so a leader
/// re-elected onto a broker the pool had not dialed becomes routable.
#[tracing::instrument(level = "debug", skip_all)]
async fn update_leaders_from_metadata(cfg: &SenderConfig) {
    if let Ok(md) = cfg.transport.refresh_metadata().await {
        // Hold the cache lock across the loop so a tracked topic's correction is
        // applied atomically alongside the leader-map update.
        let mut cache = cfg.metadata_cache.lock().await;
        for t in &md.topics {
            let Some(name) = &t.name else { continue };
            if t.error_code != 0 {
                continue;
            }
            for p in &t.partitions {
                cfg.partition_leaders
                    .insert((name.clone(), p.partition_index), p.leader_id);
            }
            // Take the count and id of a tracked topic from the refresh. A
            // topic can get a new id when it is deleted and created again, and a
            // parked batch backfills the id on resend (see `send_one_batch`).
            // A topic without partitions has no count, as in Kafka's
            // `Cluster.partitionCountForTopic`, so it keeps the cached entry.
            // Only update topics we already track, so a full-cluster refresh
            // doesn't bloat the cache.
            if let Some(entry) = cache.get_mut(name)
                && let Ok(num_partitions) = i32::try_from(t.partitions.len())
                && num_partitions > 0
            {
                entry.num_partitions = num_partitions;
                entry.topic_id = t.topic_id;
            }
        }
    }
}

fn batch_crosses_recovery_barrier(cfg: &SenderConfig, generation: Option<u64>) -> bool {
    generation.is_some_and(|batch_generation| {
        cfg.txn_recovery_required.load(Ordering::Acquire)
            || batch_generation != cfg.txn_recovery_generation.load(Ordering::Acquire)
    })
}

fn fail_recovered_batches(cfg: &SenderConfig, batches: &mut Vec<PreparedBatch>) {
    let mut retained = Vec::with_capacity(batches.len());
    for batch in batches.drain(..) {
        if batch_crosses_recovery_barrier(cfg, batch.transaction_generation) {
            fail_batch(batch.records, ProducerError::RecoveryRequired);
            finish_in_flight(cfg);
        } else {
            retained.push(batch);
        }
    }
    batches.extend(retained);
}

fn fail_recovered_retry_slots(
    cfg: &SenderConfig,
    retry: &mut HashMap<(String, i32), PreparedBatch>,
) {
    let recovered_keys = retry
        .iter()
        .filter(|(_, batch)| batch_crosses_recovery_barrier(cfg, batch.transaction_generation))
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in recovered_keys {
        let batch = retry
            .remove(&key)
            .expect("recovered retry key remains present");
        fail_batch(batch.records, ProducerError::RecoveryRequired);
        finish_in_flight(cfg);
    }
}

async fn fail_recovered_accumulator_batches(cfg: &SenderConfig) {
    let accumulators = cfg
        .accumulators
        .iter()
        .map(|entry| Arc::clone(entry.value()))
        .collect::<Vec<_>>();
    let mut failed_any = false;
    for accumulator in accumulators {
        let mut accumulator = accumulator.lock().await;
        if accumulator
            .current
            .as_ref()
            .is_some_and(|batch| batch_crosses_recovery_barrier(cfg, batch.transaction_generation))
            && let Some(batch) = accumulator.current.take()
        {
            fail_batch(batch.records, ProducerError::RecoveryRequired);
            failed_any = true;
        }

        let mut retained = VecDeque::with_capacity(accumulator.ready.len());
        while let Some(batch) = accumulator.ready.pop_front() {
            if batch_crosses_recovery_barrier(cfg, batch.transaction_generation) {
                fail_batch(batch.records, ProducerError::RecoveryRequired);
                failed_any = true;
            } else {
                retained.push_back(batch);
            }
        }
        accumulator.ready = retained;
    }
    if failed_any {
        cfg.flush_notify.notify_waiters();
    }
}

/// Decrement `in_flight` for a completed batch, and wake any `flush` waiter
/// when that batch was the last one outstanding.
fn finish_in_flight(cfg: &SenderConfig) {
    if cfg.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
        cfg.flush_notify.notify_waiters();
    }
}

/// Snapshot the transactional `(producer_id, producer_epoch)` if and only if
/// the producer is currently inside an active transaction.
///
/// It returns `Some((pid, epoch))` when the sender should emit a transactional
/// batch, and `None` for a non-transactional send or a send outside a
/// transaction.
async fn txn_pid_snapshot(cfg: &SenderConfig) -> Option<(i64, i16)> {
    cfg.transactional_id.as_ref()?;
    let state = *cfg.txn_state.lock().await;
    if matches!(state, TxnState::InTransaction | TxnState::Preparing) {
        Some(*cfg.txn_pid_epoch.lock().await)
    } else {
        None
    }
}

/// Build a v2 `RecordBatch` from a sealed `InProgressBatch`. The batch
/// attributes encode the compression, and the actual compress step runs inside
/// `RecordBatch::encode`.
///
/// `txn_snapshot` is `Some((pid, epoch))` when the sender sends the batch inside
/// an active transaction. In that case the function sets the `is_transactional`
/// attribute bit, and it uses the pid and epoch the txn coordinator assigned
/// instead of the idempotence pid and epoch.
fn build_record_batch(
    cfg: &SenderConfig,
    batch: &InProgressBatch,
    base_sequence: i32,
    txn_snapshot: Option<(i64, i16)>,
) -> RecordBatch {
    let is_transactional = txn_snapshot.is_some();
    let attributes = Attributes::default()
        .with_compression(cfg.compression.compression_type())
        .with_transactional(is_transactional);

    // Use the txn pid/epoch when inside a transaction; fall back to the
    // idempotence pid/epoch for non-transactional batches.
    let (producer_id, producer_epoch) =
        txn_snapshot.unwrap_or((cfg.producer_id, cfg.producer_epoch.load(Ordering::Acquire)));

    let base_timestamp = batch.records.first().map_or(0, |r| r.timestamp_ms);
    let max_timestamp = batch
        .records
        .iter()
        .map(|r| r.timestamp_ms)
        .max()
        .unwrap_or(0);
    let last_offset_delta =
        i32::try_from(batch.records.len().saturating_sub(1)).unwrap_or(i32::MAX);

    let mut records: Vec<Record> = Vec::with_capacity(batch.records.len());
    for r in &batch.records {
        let headers: Vec<RecordHeader> = r
            .headers
            .iter()
            .map(|h| RecordHeader {
                key: h.key.clone(),
                value: h.value.clone(),
            })
            .collect();
        records.push(Record {
            attributes: 0,
            timestamp_delta: r.timestamp_ms - base_timestamp,
            offset_delta: r.offset_delta,
            key: r.key.clone(),
            value: r.value.clone(),
            headers,
        });
    }

    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: 0,
        attributes,
        last_offset_delta,
        base_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records,
    }
}

/// Resolve every record in `records` with an error.
///
/// `ClientError`, `Protocol`, and `Compression` are not `Clone`, so for those
/// variants only the first record receives the real error. Variants that clone
/// trivially, such as `Server`, `FencedProducer` and `Closed`, reach every
/// record, so callers see the true error code.
fn fail_batch(records: Vec<PendingRecord>, err: ProducerError) {
    fn clone_if_possible(e: &ProducerError) -> Option<ProducerError> {
        match e {
            ProducerError::Server(c) => Some(ProducerError::Server(*c)),
            ProducerError::FencedProducer => Some(ProducerError::FencedProducer),
            ProducerError::RecoveryRequired => Some(ProducerError::RecoveryRequired),
            ProducerError::Closed => Some(ProducerError::Closed),
            ProducerError::FlushTimeout => Some(ProducerError::FlushTimeout),
            ProducerError::SendTimeout => Some(ProducerError::SendTimeout),
            ProducerError::BufferExhausted {
                size,
                max_block,
                total,
                available,
                poolable,
            } => Some(ProducerError::BufferExhausted {
                size: *size,
                max_block: *max_block,
                total: *total,
                available: *available,
                poolable: *poolable,
            }),
            ProducerError::BatchTooLarge { batch_size } => Some(ProducerError::BatchTooLarge {
                batch_size: *batch_size,
            }),
            ProducerError::RecordTooLarge { record_size } => Some(ProducerError::RecordTooLarge {
                record_size: *record_size,
            }),
            ProducerError::InvalidConfig(s) => Some(ProducerError::InvalidConfig(s.clone())),
            _ => None, // Client, Protocol, Compression — not Clone.
        }
    }

    let clone = clone_if_possible(&err);
    let mut iter = records.into_iter();
    if let Some(first) = iter.next() {
        let _ = first.ack.send(Err(err));
    }
    for r in iter {
        let e = clone
            .as_ref()
            .and_then(clone_if_possible)
            .unwrap_or(ProducerError::Closed);
        let _ = r.ack.send(Err(e));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::check;
    use krabka_units::{millis, secs};
    use tokio::sync::oneshot;

    use super::*;

    /// Build a `PreparedBatch` for `(topic, partition)` with `base_sequence` and
    /// a single record, returning the batch and the record's ack receiver so a
    /// test can observe how it resolves.
    fn prepared(
        topic: &str,
        partition: i32,
        base_sequence: i32,
        created_at: Instant,
    ) -> (
        PreparedBatch,
        oneshot::Receiver<Result<RecordMetadata, ProducerError>>,
    ) {
        let (tx, rx) = oneshot::channel();
        let record = PendingRecord {
            offset_delta: 0,
            timestamp_ms: 0,
            key: None,
            value: None,
            headers: Vec::new(),
            ack: tx,
        };
        let pb = PreparedBatch {
            topic: topic.to_string(),
            partition,
            topic_id: Uuid::ZERO,
            base_sequence,
            record_batch: RecordBatch {
                base_offset: 0,
                partition_leader_epoch: 0,
                attributes: Attributes::default(),
                last_offset_delta: 0,
                base_timestamp: 0,
                max_timestamp: 0,
                producer_id: 1,
                producer_epoch: 0,
                base_sequence,
                records: Vec::new(),
            },
            records: vec![record],
            created_at,
            backoff_until: None,
            retries_used: 0,
            backoff_attempts: 0,
            last_failure: None,
            transaction_generation: None,
            _memory: None,
        };
        (pb, rx)
    }

    /// The classification of each code that has a rule of its own, in each
    /// batch mode: `(Plain, Idempotent, Transactional)`.
    ///
    /// The answer carries `base_offset` 7 and `log_start_offset` 5, and the
    /// partition has no acked offset.
    fn special_code_rows() -> Vec<(&'static str, i16, [Classification; 3])> {
        use BatchVerdict::{Acked, BumpEpochAndRetry, Fatal, RestartSequenceAndRetry, Terminal};
        use Classification::Verdict;
        use SequenceRepair::{BumpEpoch, GiveBack, Keep};
        let fatal = |code| {
            [
                Verdict(Terminal { code, repair: Keep }),
                Verdict(Fatal(code)),
                Verdict(Fatal(code)),
            ]
        };
        let failed = |code| {
            [
                Verdict(Terminal { code, repair: Keep }),
                Verdict(Terminal {
                    code,
                    repair: BumpEpoch,
                }),
                Verdict(Terminal {
                    code,
                    repair: GiveBack,
                }),
            ]
        };
        let acked = Verdict(Acked { base_offset: 7 });
        vec![
            ("NONE", codes::NONE, [acked; 3]),
            (
                "DUPLICATE_SEQUENCE_NUMBER",
                codes::DUPLICATE_SEQUENCE_NUMBER,
                [acked; 3],
            ),
            (
                "OUT_OF_ORDER_SEQUENCE_NUMBER",
                codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
                [
                    Verdict(Terminal {
                        code: 45,
                        repair: Keep,
                    }),
                    Verdict(BumpEpochAndRetry),
                    Verdict(Terminal {
                        code: 45,
                        repair: GiveBack,
                    }),
                ],
            ),
            ("INVALID_PRODUCER_EPOCH", 47, failed(47)),
            (
                "UNKNOWN_PRODUCER_ID",
                codes::UNKNOWN_PRODUCER_ID,
                [
                    Verdict(Terminal {
                        code: 59,
                        repair: Keep,
                    }),
                    Verdict(BumpEpochAndRetry),
                    Verdict(RestartSequenceAndRetry),
                ],
            ),
            (
                "CLUSTER_AUTHORIZATION_FAILED",
                codes::CLUSTER_AUTHORIZATION_FAILED,
                fatal(31),
            ),
            ("UNSUPPORTED_VERSION", codes::UNSUPPORTED_VERSION, fatal(35)),
            (
                "INVALID_PRODUCER_ID_MAPPING",
                codes::INVALID_PRODUCER_ID_MAPPING,
                fatal(49),
            ),
            (
                "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
                codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                fatal(53),
            ),
            ("PRODUCER_FENCED", codes::PRODUCER_FENCED, fatal(90)),
        ]
    }

    const MODES: [BatchMode; 3] = [
        BatchMode::Plain,
        BatchMode::Idempotent,
        BatchMode::Transactional,
    ];

    /// Every row of Kafka's `Errors` table maps to the action of Kafka's
    /// producer in each batch mode. A code without a rule of its own follows
    /// its exception class: `InvalidMetadataException` refreshes and resends,
    /// another `RetriableException` resends, and any other code fails the
    /// batch and repairs the sequence as `TransactionManager.handleFailedBatch`
    /// does.
    #[test]
    fn classify_verdict_follows_kafka_for_every_error_code_and_mode() {
        let special = special_code_rows();
        let answer = |error_code| PartitionAnswer {
            error_code,
            base_offset: 7,
            log_start_offset: 5,
        };
        for (code, name, class) in crate::error_class::tests::KAFKA_ERRORS {
            let expected = special
                .iter()
                .find(|(_, special_code, _)| *special_code == code)
                .map_or_else(
                    || {
                        MODES.map(|mode| match class {
                            ErrorClass::InvalidMetadata => Classification::Routing,
                            ErrorClass::Retriable => Classification::Verdict(BatchVerdict::Retry),
                            ErrorClass::None | ErrorClass::NotRetriable => {
                                Classification::Verdict(BatchVerdict::Terminal {
                                    code,
                                    repair: mode.repair_after_failure(),
                                })
                            }
                        })
                    },
                    |(_, _, classifications)| *classifications,
                );
            // One record in the batch, so `MESSAGE_TOO_LARGE` cannot split.
            let actual = MODES.map(|mode| classify_verdict(answer(code), mode, None, 1));
            assert2::assert!(actual == expected, "{code} {name}");
        }
    }

    /// Kafka's `Sender.completeBatch` splits a batch of more than one record
    /// that gets `MESSAGE_TOO_LARGE`, and it fails a single-record batch.
    #[test]
    fn message_too_large_splits_only_a_batch_of_more_than_one_record() {
        let answer = PartitionAnswer {
            error_code: codes::MESSAGE_TOO_LARGE,
            base_offset: 7,
            log_start_offset: 5,
        };
        for (name, records_in_batch, expected) in [
            (
                "one record fails",
                1,
                MODES.map(|mode| {
                    Classification::Verdict(BatchVerdict::Terminal {
                        code: codes::MESSAGE_TOO_LARGE,
                        repair: mode.repair_after_failure(),
                    })
                }),
            ),
            (
                "two records split",
                2,
                MODES.map(|_| Classification::Verdict(BatchVerdict::Split)),
            ),
            (
                "many records split",
                17,
                MODES.map(|_| Classification::Verdict(BatchVerdict::Split)),
            ),
        ] {
            let actual = MODES.map(|mode| classify_verdict(answer, mode, None, records_in_batch));
            assert2::assert!(actual == expected, "{name}");
        }
    }

    /// `UNKNOWN_PRODUCER_ID` depends on the log start offset and on the last
    /// acked offset of the partition (`TransactionManager.canRetry`).
    #[test]
    fn unknown_producer_id_follows_the_log_start_offset() {
        use BatchVerdict::{BumpEpochAndRetry, RestartSequenceAndRetry, Retry, Terminal};
        use Classification::Verdict;
        let cases = [
            (
                "log start unknown",
                -1,
                None,
                [
                    Verdict(Terminal {
                        code: 59,
                        repair: SequenceRepair::Keep,
                    }),
                    Verdict(Retry),
                    Verdict(Retry),
                ],
            ),
            (
                "log start past the last ack",
                5,
                Some(4),
                [
                    Verdict(Terminal {
                        code: 59,
                        repair: SequenceRepair::Keep,
                    }),
                    Verdict(BumpEpochAndRetry),
                    Verdict(RestartSequenceAndRetry),
                ],
            ),
            (
                "log start at the last ack",
                5,
                Some(5),
                [
                    Verdict(Terminal {
                        code: 59,
                        repair: SequenceRepair::Keep,
                    }),
                    Verdict(BumpEpochAndRetry),
                    Verdict(Terminal {
                        code: 59,
                        repair: SequenceRepair::Reset,
                    }),
                ],
            ),
        ];
        for (name, log_start_offset, last_acked_offset, expected) in cases {
            let answer = PartitionAnswer {
                error_code: codes::UNKNOWN_PRODUCER_ID,
                base_offset: -1,
                log_start_offset,
            };
            let actual = MODES.map(|mode| classify_verdict(answer, mode, last_acked_offset, 1));
            assert2::assert!(actual == expected, "{name}");
        }
    }

    #[test]
    fn batch_mode_follows_the_batch_stamp() {
        let cases = [
            ("no producer id", -1, false, BatchMode::Plain),
            ("idempotent", 1, false, BatchMode::Idempotent),
            ("transactional", 1, true, BatchMode::Transactional),
        ];
        for (name, producer_id, transactional, expected) in cases {
            let batch = RecordBatch {
                producer_id,
                attributes: Attributes::default().with_transactional(transactional),
                ..Default::default()
            };
            assert2::assert!(BatchMode::of(&batch) == expected, "{name}");
        }
    }

    #[test]
    fn collect_retries_splits_expired_and_drains_map() {
        // Two partitions, each holding one retry batch (one slot per partition).
        // The batch past its delivery timeout is split off as expired; the recent
        // one is returned to send. The map is fully drained either way.
        let mut retry: HashMap<(String, i32), PreparedBatch> = HashMap::new();
        let long_ago = Instant::now()
            .checked_sub(Duration::from_secs(31))
            .expect("instant in range");
        let (old, _rx_old) = prepared("t", 0, 0, long_ago);
        let (recent, _rx_recent) = prepared("t", 1, 16, Instant::now());
        retry.insert(("t".to_string(), 0), old);
        retry.insert(("t".to_string(), 1), recent);

        let (to_send, expired) = collect_retries(&mut retry, Instant::now(), secs(30), usize::MAX);

        check!(
            (
                expired.len(),
                expired[0].base_sequence,
                to_send.len(),
                to_send[0].base_sequence,
                retry.is_empty(),
            ) == (1, 0, 1, 16, true)
        );
    }

    #[test]
    fn collect_retries_caps_sends_but_still_extracts_every_expired_batch() {
        let now = Instant::now();
        let long_ago = now
            .checked_sub(Duration::from_secs(31))
            .expect("instant in range");
        let mut retry = HashMap::new();
        for partition in 0..5 {
            let created_at = if partition == 4 { long_ago } else { now };
            let (batch, _rx) = prepared("t", partition, partition * 16, created_at);
            retry.insert(("t".to_owned(), partition), batch);
        }

        let (to_send, expired) = collect_retries(&mut retry, now, secs(30), 2);

        assert_eq!(to_send.len(), 2);
        assert_eq!(expired.len(), 1);
        assert_eq!(retry.len(), 2);
    }

    #[test]
    fn retry_count_exhausts_after_configured_resends() {
        let (mut batch, _rx) = prepared("t", 0, 0, Instant::now());

        assert2::assert!(!take_retry(&mut batch, 1));
        assert2::assert!(take_retry(&mut batch, 1));
    }

    /// `ProducerBatch.hasReachedDeliveryTimeout` is
    /// `deliveryTimeoutMs <= now - createdMs`, so a batch expires exactly at
    /// the timeout.
    #[test]
    fn delivery_timeout_uses_configured_duration() {
        let now = Instant::now();
        let collect = |age: u64| {
            let mut retry = HashMap::new();
            let created_at = now
                .checked_sub(Duration::from_millis(age))
                .expect("instant in range");
            let (batch, _rx) = prepared("t", 0, 0, created_at);
            retry.insert(("t".to_owned(), 0), batch);
            let (to_send, expired) = collect_retries(&mut retry, now, millis(10), usize::MAX);
            (age, to_send.len(), expired.len())
        };

        assert2::assert!([9, 10, 11].map(collect) == [(9, 1, 0), (10, 0, 1), (11, 0, 1)]);
    }

    #[test]
    fn collect_retries_honours_connection_backoff_until() {
        // A batch parked with `backoff_until` set (after a transport failure)
        // must NOT be resent until that instant passes — otherwise a leader
        // whose pod is down and refusing connections hot-loops the drain
        // scheduler. The three sample points (before / exactly at / after the
        // backoff instant) pin the `now < backoff_until` comparison so no
        // `<` → `<=`/`>`/`>=`/`==`/`!=` mutant survives.
        let backoff = Duration::from_millis(100);
        let now = Instant::now();
        // (to_send.len(), retry.len()) collected `elapsed` after a batch that is
        // backing off until `now + backoff`.
        let collect_after = |elapsed: Duration| -> (usize, usize) {
            let mut retry: HashMap<(String, i32), PreparedBatch> = HashMap::new();
            let (mut pb, _rx) = prepared("t", 0, 0, now);
            pb.backoff_until = Some(now + backoff);
            retry.insert(("t".to_string(), 0), pb);
            let (to_send, expired) =
                collect_retries(&mut retry, now + elapsed, secs(30), usize::MAX);
            assert2::assert!(expired.is_empty());
            (to_send.len(), retry.len())
        };

        for (_name, elapsed, want) in [
            // Before the backoff instant: parked in its slot, nothing sent.
            ("before deadline", Duration::from_millis(40), (0, 1)),
            // Exactly at the backoff instant: eligible — `now < t` is false
            // here, so `<` resends while `<=` would keep it parked.
            ("at deadline", backoff, (1, 0)),
            // After the backoff instant: eligible and drained out to send.
            ("after deadline", Duration::from_millis(160), (1, 0)),
        ] {
            assert2::assert!(collect_after(elapsed) == want);
        }
    }

    /// Kafka's `ExponentialBackoff.backoff`, with `retry.backoff.ms` 100,
    /// `retry.backoff.max.ms` 1000, base 2 and jitter 0.2. The lowest random
    /// value gives the factor 0.8, and the middle value gives 1.0.
    #[test]
    fn retry_backoff_matches_kafka_exponential_backoff() {
        let ms = Duration::from_millis;
        let policy = RetryBackoff::new(ms(100), ms(1000));
        let cases = [
            (0, 0.0, ms(80)),
            (0, 0.5, ms(100)),
            (1, 0.5, ms(200)),
            (2, 0.0, ms(320)),
            (3, 0.5, ms(800)),
            (4, 0.0, ms(800)),
            (4, 0.5, ms(1000)),
            (30, 0.999, ms(1000)),
        ];
        let actual =
            cases.map(|(attempts, unit, _)| (attempts, policy.backoff(attempts, unit).as_millis()));
        let expected = cases.map(|(attempts, _, want)| (attempts, want.as_millis()));
        assert2::assert!(actual == expected);

        // A maximum at or below the initial backoff gives a constant backoff
        // of the maximum, with no jitter.
        let constant = RetryBackoff::new(ms(100), ms(50));
        assert2::assert!(
            [0, 5].map(|attempts| constant.backoff(attempts, 0.0)) == [ms(50), ms(50)]
        );

        // A sub-millisecond initial backoff grows from its own value up to the
        // maximum.
        let us = Duration::from_micros;
        let small = RetryBackoff::new(us(500), us(750));
        assert2::assert!(
            [(0, 0.5), (1, 0.0), (5, 0.999)]
                .map(|(attempts, unit)| small.backoff(attempts, unit).as_micros())
                == [500, 600, 750]
        );
    }

    #[test]
    fn jitter_unit_is_in_the_unit_interval() {
        let units = (0..1000).map(|_| jitter_unit()).collect::<Vec<_>>();
        assert2::assert!(units.iter().all(|unit| (0.0..1.0).contains(unit)));
        assert2::assert!(
            units
                .iter()
                .any(|unit| (unit - units[0]).abs() > f64::EPSILON)
        );
    }

    #[test]
    fn positive_partition_count_filters_boundary_values() {
        for (_name, input, want) in [
            ("negative", -1, None),
            ("zero", 0, None),
            ("one", 1, Some(1)),
            ("positive", 2, Some(2)),
        ] {
            assert2::assert!(positive_partition_count(input) == want);
        }
    }
}

/// Deterministic in-process integration harness.
///
/// It drives the real [`run`] sender loop against a [`MockTransport`] that
/// models a broker's per-partition idempotent sequencing, with no socket and no
/// real `Client`. It reproduces, and then guards against, the same-partition
/// pipelining hang the module docs describe.
#[cfg(test)]
mod harness {
    use std::{
        collections::VecDeque,
        sync::{
            Mutex as StdMutex,
            atomic::{AtomicBool, AtomicI64, AtomicU64},
        },
        time::Duration,
    };

    use assert2::check;
    use krabka_client_core::ClientError;
    use krabka_protocol::{
        owned::{
            metadata_response::{
                MetadataResponse, MetadataResponsePartition, MetadataResponseTopic,
            },
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
            produce_response::{
                LeaderIdAndEpoch, PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
            },
        },
        records::{Attributes, Record, RecordBatch},
    };
    use krabka_units::{millis, minutes, secs};
    use tokio::sync::oneshot;

    use super::*;
    use crate::{
        accumulator::Accumulator,
        partitioner::{PartitionerConfig, TopicPartitions},
        producer::{STATE_ACTIVE, STATE_FENCED, TopicMetadata},
        transactional::{AbortableError, TxnState},
    };

    /// Adapter that lets a sender own a `Box<dyn ProduceTransport>` while the
    /// test keeps a clone of the same `Arc<MockTransport>` to inspect.
    struct ArcTransport(Arc<MockTransport>);

    #[async_trait::async_trait]
    impl ProduceTransport for ArcTransport {
        async fn send_produce(
            &self,
            leader: Option<i32>,
            req: ProduceRequest,
        ) -> Result<ProduceResponse, ClientError> {
            self.0.send_produce(leader, req).await
        }
        async fn send_produce_no_response(
            &self,
            leader: Option<i32>,
            req: ProduceRequest,
        ) -> Result<(), ClientError> {
            self.0.send_produce_no_response(leader, req).await
        }
        fn evict_broker(&self, id: i32) {
            self.0.evict_broker(id);
        }
        fn knows_broker(&self, id: i32) -> bool {
            self.0.knows_broker(id)
        }
        async fn refresh_metadata(&self) -> Result<MetadataResponse, ClientError> {
            self.0.refresh_metadata().await
        }
    }

    /// Per-partition broker sequencing state.
    #[derive(Default)]
    struct PartitionState {
        /// The producer epoch the partition holds. A batch at a higher epoch
        /// must start at sequence 0, and a batch at a lower epoch gets
        /// `INVALID_PRODUCER_EPOCH`, as Kafka's `ProducerAppendInfo` checks.
        epoch: i16,
        /// Next `base_sequence` the broker accepts. It is strictly increasing
        /// with no gaps, as in the Kafka idempotent producer.
        expected: i32,
        /// Running log-end offset. Each accepted batch takes the current value
        /// as its `base_offset`, and then the offset advances by the record
        /// count.
        next_offset: i64,
        /// `base_sequence -> base_offset` for sequences already accepted. A
        /// resend of a written batch can then be answered with
        /// `DUPLICATE_SEQUENCE_NUMBER` and its original offset. The broker
        /// dedups, and the sender maps that answer to an ack.
        accepted: HashMap<i32, i64>,
    }

    /// A broker model with faithful per-partition idempotent sequencing.
    ///
    /// When `reorder_delay` is non-zero, `send_produce` sleeps for
    /// `reorder_delay * (REORDER_SPREAD - min(base_sequence, REORDER_SPREAD))`
    /// before it applies the broker logic. Several same-partition requests
    /// issued *concurrently* then complete **higher-`base_sequence`-first**,
    /// because a lower sequence waits longer. This models the on-the-wire write
    /// race of the old `join_all` same-partition pipelining deterministically.
    ///
    /// With the fix, at most one same-partition request is in flight, so only
    /// one request is ever outstanding per partition. The staggered delay then
    /// cannot reorder anything, and the broker sees a clean increasing
    /// sequence.
    struct MockTransport {
        partitions: StdMutex<HashMap<(String, i32), PartitionState>>,
        /// Arrival order of `(topic, partition, base_sequence)` as the broker
        /// *applied* them, after the delay. It serves assertions and
        /// debugging.
        arrivals: StdMutex<Vec<(String, i32, i32)>>,
        reorder_delay: Duration,
        /// Total Produce requests applied. A test uses it to bound livelock
        /// churn.
        applied: AtomicUsize,
        /// One-shot transport error. The next send to this `base_sequence`
        /// returns `Err(Disconnected)` exactly once, and then the flag
        /// clears.
        fail_once_seq: StdMutex<Option<i32>>,
        /// One-shot transport error for the next send to a specific broker id.
        fail_once_leader: StdMutex<Option<i32>>,
        /// Artificial per-leader delay before producing a response/error.
        leader_delay: StdMutex<HashMap<i32, Duration>>,
        /// One-shot injected broker response, keyed by `base_sequence`. The
        /// next send to that sequence returns a synthesized `ProduceResponse`
        /// once, with a custom name, `topic_id`, error code, offset and leader
        /// hint, and then the entry clears. It drives the terminal, routing and
        /// topic-correlation paths.
        inject_once: StdMutex<Option<Inject>>,
        /// `broker_id`s passed to `evict_broker`, in order.
        evicted: StdMutex<Vec<i32>>,
        /// `timeout_ms` of the most recent Produce request the broker received.
        last_timeout_ms: AtomicI64,
        /// Response that `refresh_metadata` returns. It is empty by
        /// default.
        refresh_response: StdMutex<MetadataResponse>,
        /// Broker ids the transport claims to have a dialable address for.
        /// This drives [`resolve_leader`]. The set is empty by default, so
        /// every send falls back to the bootstrap connection, as the original
        /// harness assumed.
        known_brokers: StdMutex<HashSet<i32>>,
        /// The `leader` argument of every `send_produce` call, in order, so a
        /// test can assert how a batch was routed.
        sent_leaders: StdMutex<Vec<Option<i32>>>,
        /// The `topic_id` of every `send_produce` call, in order, so a test can
        /// assert which id a resend carried.
        sent_topic_ids: StdMutex<Vec<Uuid>>,
        /// The `(producer_epoch, base_sequence)` of every `send_produce` call,
        /// in order.
        sent_batches: StdMutex<Vec<(i16, i32)>>,
        /// Signals each entry into `send_produce`, including injected failures
        /// before the broker model applies a request.
        send_started: Notify,
        active_sends: AtomicUsize,
        peak_active_sends: AtomicUsize,
        fail_next_sends: AtomicUsize,
        /// Count of `refresh_metadata` calls, so a test can assert the sender
        /// refreshed after a routing/transport failure.
        refreshes: AtomicUsize,
        offsets_seen: AtomicI64,
        /// Calls made through the dedicated one-way Produce transport path.
        no_response_sends: AtomicUsize,
        /// The instant of every `send_produce` call, in order.
        sent_at: StdMutex<Vec<Instant>>,
    }

    /// A one-shot synthesized broker response, keyed by `base_sequence`. A
    /// `name` or `topic_id` of `None` echoes the request's value. A
    /// `leader_hint >= 0` sets the partition response's `current_leader`.
    #[derive(Clone)]
    struct Inject {
        seq: i32,
        name: Option<String>,
        topic_id: Option<Uuid>,
        error_code: i16,
        base_offset: i64,
        leader_hint: i32,
        /// `log_start_offset` of the partition answer. Kafka's default is -1.
        log_start_offset: i64,
        /// Drop the producer state of the partition before the answer, as a
        /// broker does when it answers `UNKNOWN_PRODUCER_ID`.
        forget_producer_state: bool,
    }

    /// Caps the per-request reorder stagger to a bounded number of delay
    /// units, so the total sleep stays small, at
    /// `reorder_delay * REORDER_SPREAD` in the worst case. Higher sequences
    /// still complete ahead of lower ones within a single concurrent `join_all`
    /// poll.
    const REORDER_SPREAD: i32 = 32;

    impl MockTransport {
        fn new(reorder_delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                partitions: StdMutex::new(HashMap::new()),
                arrivals: StdMutex::new(Vec::new()),
                reorder_delay,
                applied: AtomicUsize::new(0),
                fail_once_seq: StdMutex::new(None),
                fail_once_leader: StdMutex::new(None),
                leader_delay: StdMutex::new(HashMap::new()),
                inject_once: StdMutex::new(None),
                evicted: StdMutex::new(Vec::new()),
                last_timeout_ms: AtomicI64::new(0),
                refresh_response: StdMutex::new(MetadataResponse::default()),
                known_brokers: StdMutex::new(HashSet::new()),
                sent_leaders: StdMutex::new(Vec::new()),
                sent_topic_ids: StdMutex::new(Vec::new()),
                sent_batches: StdMutex::new(Vec::new()),
                send_started: Notify::new(),
                active_sends: AtomicUsize::new(0),
                peak_active_sends: AtomicUsize::new(0),
                fail_next_sends: AtomicUsize::new(0),
                refreshes: AtomicUsize::new(0),
                offsets_seen: AtomicI64::new(0),
                no_response_sends: AtomicUsize::new(0),
                sent_at: StdMutex::new(Vec::new()),
            })
        }

        fn sent_at(self: &Arc<Self>) -> Vec<Instant> {
            self.sent_at.lock().unwrap().clone()
        }

        fn fail_once_on(self: &Arc<Self>, seq: i32) {
            *self.fail_once_seq.lock().unwrap() = Some(seq);
        }

        fn fail_next(self: &Arc<Self>, count: usize) {
            self.fail_next_sends.store(count, Ordering::Release);
        }

        fn peak_active_sends(self: &Arc<Self>) -> usize {
            self.peak_active_sends.load(Ordering::Acquire)
        }

        fn fail_once_on_leader(self: &Arc<Self>, leader: i32) {
            *self.fail_once_leader.lock().unwrap() = Some(leader);
        }

        fn delay_leader(self: &Arc<Self>, leader: i32, delay: Duration) {
            self.leader_delay.lock().unwrap().insert(leader, delay);
        }

        /// Make the next send to `seq` return, once, a `ProduceResponse` that
        /// carries `error_code`. It echoes the request's topic and gives no
        /// leader hint.
        fn inject_code_once(self: &Arc<Self>, seq: i32, error_code: i16) {
            self.inject(Inject {
                seq,
                name: None,
                topic_id: None,
                error_code,
                base_offset: -1,
                leader_hint: -1,
                log_start_offset: -1,
                forget_producer_state: false,
            });
        }

        /// Arm a fully-specified one-shot injected response.
        fn inject(self: &Arc<Self>, inject: Inject) {
            *self.inject_once.lock().unwrap() = Some(inject);
        }

        /// `broker_id`s the sender asked to evict, in order.
        fn evicted(self: &Arc<Self>) -> Vec<i32> {
            self.evicted.lock().unwrap().clone()
        }

        /// `timeout_ms` carried by the most recent Produce request.
        fn last_timeout_ms(self: &Arc<Self>) -> i64 {
            self.last_timeout_ms.load(Ordering::Relaxed)
        }

        /// Set the `MetadataResponse` returned by `refresh_metadata`.
        fn set_refresh_response(self: &Arc<Self>, md: MetadataResponse) {
            *self.refresh_response.lock().unwrap() = md;
        }

        /// Mark `id` as a broker the transport can dial, so `resolve_leader`
        /// routes to it instead of to the bootstrap connection.
        fn add_known_broker(self: &Arc<Self>, id: i32) {
            self.known_brokers.lock().unwrap().insert(id);
        }

        /// The `leader` argument of every `send_produce` call, in order.
        fn sent_leaders(self: &Arc<Self>) -> Vec<Option<i32>> {
            self.sent_leaders.lock().unwrap().clone()
        }

        /// The `topic_id` of every `send_produce` call, in order.
        fn sent_topic_ids(self: &Arc<Self>) -> Vec<Uuid> {
            self.sent_topic_ids.lock().unwrap().clone()
        }

        /// The `(producer_epoch, base_sequence)` of every `send_produce` call.
        fn sent_batches(self: &Arc<Self>) -> Vec<(i16, i32)> {
            self.sent_batches.lock().unwrap().clone()
        }

        /// Total Produce transport calls, including failures before the broker
        /// model applies the request.
        fn send_count(self: &Arc<Self>) -> usize {
            self.sent_leaders.lock().unwrap().len()
        }

        /// How many times the sender refreshed cluster metadata.
        fn refresh_count(self: &Arc<Self>) -> usize {
            self.refreshes.load(Ordering::Relaxed)
        }

        fn applied_count(self: &Arc<Self>) -> usize {
            self.applied.load(Ordering::Relaxed)
        }

        fn no_response_count(self: &Arc<Self>) -> usize {
            self.no_response_sends.load(Ordering::Relaxed)
        }

        /// Apply one single-partition, single-batch `ProduceRequest` to the
        /// broker model and synthesize the matching `ProduceResponse`.
        fn apply(&self, req: &ProduceRequest) -> ProduceResponse {
            let topic = &req.topic_data[0];
            let part = &topic.partition_data[0];
            let batch = part
                .records
                .as_ref()
                .and_then(|p| p.as_v2())
                .and_then(|b| b.first())
                .expect("single v2 record batch");
            let base_sequence = batch.base_sequence;
            let count = i32::try_from(batch.records.len().max(1)).unwrap_or(1);
            let key = (topic.name.clone(), part.index);

            self.arrivals
                .lock()
                .unwrap()
                .push((topic.name.clone(), part.index, base_sequence));
            self.applied.fetch_add(1, Ordering::Relaxed);

            let mut parts = self.partitions.lock().unwrap();
            let st = parts.entry(key).or_default();

            let (error_code, base_offset) = if batch.producer_id < 0 {
                // No producer id: the broker does not check sequences.
                let base_offset = st.next_offset;
                st.next_offset += i64::from(count);
                (codes::NONE, base_offset)
            } else if batch.producer_epoch < st.epoch {
                (47, -1)
            } else if batch.producer_epoch > st.epoch && base_sequence != 0 {
                (codes::OUT_OF_ORDER_SEQUENCE_NUMBER, -1)
            } else if batch.producer_epoch > st.epoch {
                // A higher epoch at sequence 0 replaces the producer state.
                st.epoch = batch.producer_epoch;
                st.expected = count;
                st.accepted.clear();
                let base_offset = st.next_offset;
                st.accepted.insert(0, base_offset);
                st.next_offset += i64::from(count);
                (codes::NONE, base_offset)
            } else if base_sequence == st.expected {
                // In-order: accept, assign offset, advance.
                let base_offset = st.next_offset;
                st.accepted.insert(base_sequence, base_offset);
                st.next_offset += i64::from(count);
                st.expected = st.expected.wrapping_add(count);
                self.offsets_seen.fetch_max(base_offset, Ordering::Relaxed);
                (codes::NONE, base_offset)
            } else if let Some(&prev) = st.accepted.get(&base_sequence) {
                // Already written (a resend of a durable batch): dedup.
                (codes::DUPLICATE_SEQUENCE_NUMBER, prev)
            } else {
                // A gap: a lower sequence hasn't been accepted yet.
                (codes::OUT_OF_ORDER_SEQUENCE_NUMBER, -1)
            };

            ProduceResponse {
                responses: vec![TopicProduceResponse {
                    name: topic.name.clone(),
                    topic_id: topic.topic_id,
                    partition_responses: vec![PartitionProduceResponse {
                        index: part.index,
                        error_code,
                        base_offset,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
        }
    }

    #[async_trait::async_trait]
    impl ProduceTransport for MockTransport {
        async fn send_produce(
            &self,
            leader: Option<i32>,
            req: ProduceRequest,
        ) -> Result<ProduceResponse, ClientError> {
            struct ActiveSend<'a>(&'a AtomicUsize);
            impl Drop for ActiveSend<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::AcqRel);
                }
            }

            let active = self.active_sends.fetch_add(1, Ordering::AcqRel) + 1;
            self.peak_active_sends.fetch_max(active, Ordering::AcqRel);
            let _active_send = ActiveSend(&self.active_sends);
            self.sent_leaders.lock().unwrap().push(leader);
            self.sent_at.lock().unwrap().push(Instant::now());
            self.sent_topic_ids
                .lock()
                .unwrap()
                .push(req.topic_data[0].topic_id);
            if let Some(batch) = req.topic_data[0].partition_data[0]
                .records
                .as_ref()
                .and_then(|p| p.as_v2())
                .and_then(|b| b.first())
            {
                self.sent_batches
                    .lock()
                    .unwrap()
                    .push((batch.producer_epoch, batch.base_sequence));
            }
            self.send_started.notify_one();
            self.last_timeout_ms
                .store(i64::from(req.timeout_ms), Ordering::Relaxed);

            if self
                .fail_next_sends
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(ClientError::Disconnected);
            }

            if let Some(delay) =
                leader.and_then(|id| self.leader_delay.lock().unwrap().get(&id).copied())
            {
                tokio::time::sleep(delay).await;
            }

            {
                let mut guard = self.fail_once_leader.lock().unwrap();
                if let (Some(target), Some(actual)) = (*guard, leader)
                    && target == actual
                {
                    *guard = None;
                    drop(guard);
                    return Err(ClientError::Disconnected);
                }
            }

            let batch_seq = req.topic_data[0].partition_data[0]
                .records
                .as_ref()
                .and_then(|p| p.as_v2())
                .and_then(|b| b.first())
                .map(|b| b.base_sequence);

            // One-shot injected transport error.
            {
                let mut guard = self.fail_once_seq.lock().unwrap();
                if let (Some(target), Some(seq)) = (*guard, batch_seq)
                    && target == seq
                {
                    *guard = None;
                    drop(guard);
                    return Err(ClientError::Disconnected);
                }
            }

            // One-shot injected broker response (terminal / routing / correlation).
            {
                let inj = {
                    let mut guard = self.inject_once.lock().unwrap();
                    match (guard.as_ref(), batch_seq) {
                        (Some(i), Some(seq)) if i.seq == seq => guard.take(),
                        _ => None,
                    }
                };
                if let Some(inj) = inj {
                    let topic = &req.topic_data[0];
                    let part = &topic.partition_data[0];
                    if inj.forget_producer_state
                        && let Some(state) = self
                            .partitions
                            .lock()
                            .unwrap()
                            .get_mut(&(topic.name.clone(), part.index))
                    {
                        state.expected = 0;
                        state.accepted.clear();
                    }
                    let current_leader = if inj.leader_hint >= 0 {
                        LeaderIdAndEpoch {
                            leader_id: inj.leader_hint,
                            ..Default::default()
                        }
                    } else {
                        LeaderIdAndEpoch::default()
                    };
                    return Ok(ProduceResponse {
                        responses: vec![TopicProduceResponse {
                            name: inj.name.unwrap_or_else(|| topic.name.clone()),
                            topic_id: inj.topic_id.unwrap_or(topic.topic_id),
                            partition_responses: vec![PartitionProduceResponse {
                                index: part.index,
                                error_code: inj.error_code,
                                base_offset: inj.base_offset,
                                log_start_offset: inj.log_start_offset,
                                current_leader,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    });
                }
            }

            // Reorder model: higher base_sequence completes first when several
            // same-partition requests are issued concurrently.
            if !self.reorder_delay.is_zero() {
                let units =
                    u32::try_from(REORDER_SPREAD - batch_seq.unwrap_or(0).min(REORDER_SPREAD))
                        .unwrap_or(0);
                tokio::time::sleep(self.reorder_delay * units).await;
            }

            Ok(self.apply(&req))
        }

        async fn send_produce_no_response(
            &self,
            leader: Option<i32>,
            req: ProduceRequest,
        ) -> Result<(), ClientError> {
            self.no_response_sends.fetch_add(1, Ordering::Relaxed);
            self.send_produce(leader, req).await.map(drop)
        }

        fn evict_broker(&self, broker_id: i32) {
            self.evicted.lock().unwrap().push(broker_id);
        }

        fn knows_broker(&self, broker_id: i32) -> bool {
            self.known_brokers.lock().unwrap().contains(&broker_id)
        }

        async fn refresh_metadata(&self) -> Result<MetadataResponse, ClientError> {
            self.refreshes.fetch_add(1, Ordering::Relaxed);
            Ok(self.refresh_response.lock().unwrap().clone())
        }
    }

    /// Shared handles a test needs to drive and observe a sender.
    struct Harness {
        accumulators: AccumulatorMap,
        next_seq: Arc<DashMap<(String, i32), i32>>,
        partition_leaders: Arc<DashMap<(String, i32), i32>>,
        metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>>,
        state: Arc<AtomicU8>,
        wake_tx: tokio::sync::mpsc::Sender<DrainIntent>,
        flush_notify: Arc<Notify>,
        in_flight: Arc<AtomicUsize>,
        shutdown: CancellationToken,
        partitioner: Arc<BuiltInPartitioner>,
        transport: Arc<MockTransport>,
        recovery_required: Arc<AtomicBool>,
        recovery_generation: Arc<AtomicU64>,
        producer_epoch: Arc<AtomicI16>,
        txn_abortable_error: Arc<AbortableErrorSlot>,
        handle: tokio::task::JoinHandle<()>,
    }

    /// Spawn a sender backed by `transport`, with `max_in_flight` and a 1ms
    /// linger, so batch deadlines expire quickly.
    fn spawn_sender(transport: Arc<MockTransport>, max_in_flight: usize) -> Harness {
        spawn_sender_with(transport, max_in_flight, millis(1))
    }

    /// Spawn a sender with an explicit `linger`. A long linger keeps the batch
    /// deadline in the future, so a test can observe wake-triggered drains in
    /// isolation.
    fn spawn_sender_with(
        transport: Arc<MockTransport>,
        max_in_flight: usize,
        linger: Time,
    ) -> Harness {
        spawn_sender_with_retries(transport, max_in_flight, linger, i32::MAX)
    }

    fn spawn_sender_with_retries(
        transport: Arc<MockTransport>,
        max_in_flight: usize,
        linger: Time,
        retries: i32,
    ) -> Harness {
        spawn_sender_with_policy(transport, max_in_flight, linger, retries, secs(30))
    }

    fn spawn_sender_with_policy(
        transport: Arc<MockTransport>,
        max_in_flight: usize,
        linger: Time,
        retries: i32,
        delivery_timeout: Time,
    ) -> Harness {
        spawn_sender_with_acks(
            transport,
            max_in_flight,
            linger,
            retries,
            delivery_timeout,
            Acks::All,
        )
    }

    fn spawn_sender_with_acks(
        transport: Arc<MockTransport>,
        max_in_flight: usize,
        linger: Time,
        retries: i32,
        delivery_timeout: Time,
        acks: Acks,
    ) -> Harness {
        spawn_sender_full(
            transport,
            max_in_flight,
            linger,
            retries,
            delivery_timeout,
            acks,
            BatchMode::Idempotent,
        )
    }

    /// The transactional `(producer_id, producer_epoch)` of a harness in
    /// [`BatchMode::Transactional`].
    const TXN_PID_EPOCH: (i64, i16) = (7, 2);

    /// Spawn a sender that stamps its batches in `mode`, with a 1ms linger.
    ///
    /// [`BatchMode::Plain`] has no producer id, and
    /// [`BatchMode::Transactional`] is inside a transaction with
    /// [`TXN_PID_EPOCH`].
    fn spawn_sender_in_mode(transport: Arc<MockTransport>, mode: BatchMode) -> Harness {
        spawn_sender_full(transport, 1, millis(1), i32::MAX, secs(30), Acks::All, mode)
    }

    fn spawn_sender_full(
        transport: Arc<MockTransport>,
        max_in_flight: usize,
        linger: Time,
        retries: i32,
        delivery_timeout: Time,
        acks: Acks,
        mode: BatchMode,
    ) -> Harness {
        spawn_sender_policy(
            transport,
            HarnessPolicy {
                max_in_flight,
                linger,
                retries,
                delivery_timeout,
                acks,
                mode,
                ..HarnessPolicy::default()
            },
        )
    }

    /// The sender settings of a harness.
    #[derive(Clone, Copy)]
    struct HarnessPolicy {
        max_in_flight: usize,
        linger: Time,
        retries: i32,
        delivery_timeout: Time,
        retry_backoff: Time,
        /// The default equals `retry_backoff`, so the backoff is constant
        /// with no jitter, and paused-time tests see exact instants.
        retry_backoff_max: Time,
        acks: Acks,
        mode: BatchMode,
    }

    impl Default for HarnessPolicy {
        fn default() -> Self {
            Self {
                max_in_flight: 1,
                linger: millis(1),
                retries: i32::MAX,
                delivery_timeout: secs(30),
                retry_backoff: millis(1),
                retry_backoff_max: millis(1),
                acks: Acks::All,
                mode: BatchMode::Idempotent,
            }
        }
    }

    fn spawn_sender_policy(transport: Arc<MockTransport>, policy: HarnessPolicy) -> Harness {
        let HarnessPolicy {
            max_in_flight,
            linger,
            retries,
            delivery_timeout,
            retry_backoff,
            retry_backoff_max,
            acks,
            mode,
        } = policy;
        let (producer_id, producer_epoch) = if mode == BatchMode::Plain {
            (-1, -1)
        } else {
            (1, 0)
        };
        let (transactional_id, txn_state) = if mode == BatchMode::Transactional {
            (Some("txn".to_owned()), TxnState::InTransaction)
        } else {
            (None, TxnState::Uninitialized)
        };
        let producer_epoch = Arc::new(AtomicI16::new(producer_epoch));
        let accumulators: AccumulatorMap = Arc::new(DashMap::new());
        let next_seq: Arc<DashMap<(String, i32), i32>> = Arc::new(DashMap::new());
        let (wake_tx, wake_rx) = tokio::sync::mpsc::channel(64);
        let flush_notify = Arc::new(Notify::new());
        let in_flight = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let partition_leaders: Arc<DashMap<(String, i32), i32>> = Arc::new(DashMap::new());
        let partitioner = Arc::new(BuiltInPartitioner::new(PartitionerConfig::default()));
        let state = Arc::new(AtomicU8::new(STATE_ACTIVE));
        let recovery_required = Arc::new(AtomicBool::new(false));
        let recovery_generation = Arc::new(AtomicU64::new(0));
        let abortable_error = Arc::new(AbortableErrorSlot::default());

        // Box the same Arc<MockTransport> for the sender; keep a clone for the
        // test to inspect.
        let cfg = SenderConfig {
            transport: Box::new(ArcTransport(transport.clone())),
            producer_id,
            producer_epoch: Arc::clone(&producer_epoch),
            acks,
            compression: Compression::None,
            linger,
            request_timeout_ms: 5_000,
            retries,
            retry_backoff: RetryBackoff::new(retry_backoff.to_std(), retry_backoff_max.to_std()),
            delivery_timeout,
            max_in_flight,
            metadata_cache: Arc::clone(&metadata_cache),
            partition_leaders: Arc::clone(&partition_leaders),
            partitioner: Arc::clone(&partitioner),
            accumulators: Arc::clone(&accumulators),
            next_seq: Arc::clone(&next_seq),
            state: Arc::clone(&state),
            wake_rx,
            flush_notify: Arc::clone(&flush_notify),
            in_flight: Arc::clone(&in_flight),
            shutdown: shutdown.clone(),
            transactional_id,
            txn_state: Arc::new(Mutex::new(txn_state)),
            txn_pid_epoch: Arc::new(Mutex::new(if mode == BatchMode::Transactional {
                TXN_PID_EPOCH
            } else {
                (1, 0)
            })),
            txn_recovery_required: Arc::clone(&recovery_required),
            txn_recovery_generation: Arc::clone(&recovery_generation),
            txn_abortable_error: Arc::clone(&abortable_error),
        };

        let handle = tokio::spawn(run(cfg));
        Harness {
            accumulators,
            next_seq,
            partition_leaders,
            metadata_cache,
            state,
            wake_tx,
            flush_notify,
            in_flight,
            shutdown,
            partitioner,
            transport,
            recovery_required,
            recovery_generation,
            producer_epoch,
            txn_abortable_error: abortable_error,
            handle,
        }
    }

    /// Append `n` records to `(topic, partition)`, each in its own batch, and
    /// return the ack receivers. The sender then allocates distinct
    /// `base_sequence`s and may pipeline them. A seal after each append forces
    /// one record per batch.
    async fn produce_burst(
        h: &Harness,
        topic: &str,
        partition: i32,
        n: usize,
    ) -> Vec<oneshot::Receiver<Result<RecordMetadata, ProducerError>>> {
        let key = (topic.to_string(), partition);
        let acc = h
            .accumulators
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(Accumulator::new(16 * 1024))))
            .value()
            .clone();

        let mut rxs = Vec::with_capacity(n);
        for _ in 0..n {
            let mut a = acc.lock().await;
            let crate::accumulator::AppendResult { receiver: rx, .. } =
                a.try_append(None, Some(bytes::Bytes::from_static(b"x")), vec![], 0, None);
            // Seal so each record becomes its own ready batch with a distinct
            // base_sequence — maximizing same-partition pipelining pressure.
            a.seal_current();
            rxs.push(rx);
        }
        let _ = h.wake_tx.try_send(DrainIntent::Ready);
        rxs
    }

    /// Append `n` records to `(topic, partition)` as a SINGLE batch, with no
    /// seal between the appends, and return the ack receivers in append order.
    /// The sender seals the batch on its next drain, so the records share one
    /// `base_sequence` with `offset_delta` 0..n-1. This exercises the per-record
    /// offset arithmetic, `base_offset + offset_delta`.
    async fn produce_single_batch(
        h: &Harness,
        topic: &str,
        partition: i32,
        n: usize,
    ) -> Vec<oneshot::Receiver<Result<RecordMetadata, ProducerError>>> {
        let rxs = produce_single_batch_without_wake(h, topic, partition, n).await;
        let _ = h.wake_tx.try_send(DrainIntent::Force);
        rxs
    }

    async fn produce_single_batch_without_wake(
        h: &Harness,
        topic: &str,
        partition: i32,
        n: usize,
    ) -> Vec<oneshot::Receiver<Result<RecordMetadata, ProducerError>>> {
        let key = (topic.to_string(), partition);
        let acc = h
            .accumulators
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(Accumulator::new(16 * 1024))))
            .value()
            .clone();

        let mut rxs = Vec::with_capacity(n);
        {
            let mut a = acc.lock().await;
            for _ in 0..n {
                let crate::accumulator::AppendResult { receiver: rx, .. } =
                    a.try_append(None, Some(bytes::Bytes::from_static(b"x")), vec![], 0, None);
                rxs.push(rx);
            }
        }
        rxs
    }

    async fn produce_ready_batches_without_wake(
        h: &Harness,
        topic: &str,
        partition: i32,
        n: usize,
    ) -> Vec<oneshot::Receiver<Result<RecordMetadata, ProducerError>>> {
        let key = (topic.to_owned(), partition);
        let accumulator = h
            .accumulators
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(Accumulator::new(16 * 1024))))
            .value()
            .clone();
        let mut receivers = Vec::with_capacity(n);
        let mut accumulator = accumulator.lock().await;
        for _ in 0..n {
            let crate::accumulator::AppendResult { receiver, .. } = accumulator.try_append(
                None,
                Some(bytes::Bytes::from_static(b"x")),
                vec![],
                0,
                None,
            );
            accumulator.seal_current();
            receivers.push(receiver);
        }
        receivers
    }

    async fn shutdown(h: Harness) {
        h.shutdown.cancel();
        let _ = h.handle.await;
    }

    #[tokio::test(start_paused = true)]
    async fn nonzero_linger_coalesces_until_the_batch_expires() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, millis(100));
        let rxs = produce_single_batch_without_wake(&h, "t", 0, 2).await;

        tokio::task::yield_now().await;
        assert_eq!(transport.send_count(), 0, "young batch sent before linger");

        tokio::time::advance(Duration::from_millis(99)).await;
        tokio::task::yield_now().await;
        assert_eq!(transport.send_count(), 0, "young batch sent before linger");

        tokio::time::advance(Duration::from_millis(1)).await;
        let mut offsets = Vec::new();
        for rx in rxs {
            offsets.push(
                rx.await
                    .expect("ack channel remains connected")
                    .expect("coalesced batch is acknowledged")
                    .offset,
            );
        }
        assert_eq!(offsets, vec![0, 1]);
        assert_eq!(transport.send_count(), 1);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn rollover_wake_sends_ready_only_and_leaves_young_currents_open() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, millis(100));
        tokio::task::yield_now().await;

        let rollover = Arc::new(Mutex::new(Accumulator::new(20)));
        h.accumulators
            .insert(("t".to_owned(), 0), Arc::clone(&rollover));
        let (ready_rx, current_rx) = {
            let mut accumulator = rollover.lock().await;
            let crate::accumulator::AppendResult {
                receiver: ready, ..
            } = accumulator.try_append(
                None,
                Some(bytes::Bytes::from_static(b"a")),
                vec![],
                0,
                None,
            );
            let crate::accumulator::AppendResult {
                receiver: current, ..
            } = accumulator.try_append(
                None,
                Some(bytes::Bytes::from_static(b"b")),
                vec![],
                0,
                None,
            );
            (ready, current)
        };

        let unrelated = Arc::new(Mutex::new(Accumulator::new(1024)));
        h.accumulators
            .insert(("t".to_owned(), 1), Arc::clone(&unrelated));
        let crate::accumulator::AppendResult {
            receiver: unrelated_rx,
            ..
        } = unrelated.lock().await.try_append(
            None,
            Some(bytes::Bytes::from_static(b"young")),
            vec![],
            0,
            None,
        );

        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");
        ready_rx
            .await
            .expect("ready ack channel remains connected")
            .expect("ready rollover batch is acknowledged");

        assert_eq!(transport.send_count(), 1);
        assert!(rollover.lock().await.current.is_some());
        assert!(unrelated.lock().await.current.is_some());

        drop((current_rx, unrelated_rx));
        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn zero_linger_force_wake_sends_without_advancing_time() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, secs(0));
        tokio::task::yield_now().await;
        let mut rxs = produce_single_batch_without_wake(&h, "t", 0, 1).await;

        h.wake_tx
            .send(DrainIntent::Force)
            .await
            .expect("sender is running");
        rxs.remove(0)
            .await
            .expect("ack channel remains connected")
            .expect("zero-linger batch is acknowledged");
        assert_eq!(transport.send_count(), 1);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn explicit_flush_intent_bypasses_nonzero_linger() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, minutes(1));
        tokio::task::yield_now().await;
        let mut rxs = produce_single_batch_without_wake(&h, "t", 0, 1).await;

        h.wake_tx
            .send(DrainIntent::Force)
            .await
            .expect("sender is running");
        rxs.remove(0)
            .await
            .expect("ack channel remains connected")
            .expect("explicitly flushed batch is acknowledged");
        assert_eq!(transport.send_count(), 1);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn off_phase_append_sends_at_its_own_linger_deadline() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, millis(100));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(50)).await;
        let mut receivers = produce_single_batch_without_wake(&h, "t", 0, 1).await;
        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");

        tokio::time::advance(Duration::from_millis(99)).await;
        tokio::task::yield_now().await;
        assert_eq!(transport.send_count(), 0);

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(transport.send_count(), 1);
        receivers
            .remove(0)
            .await
            .expect("ack channel remains connected")
            .expect("batch is acknowledged at its own deadline");

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn force_drains_more_partitions_than_max_in_flight_without_linger_wait() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 2, minutes(1));
        tokio::task::yield_now().await;
        let start = Instant::now();
        let mut receivers = Vec::new();
        for partition in 0..6 {
            receivers.extend(produce_single_batch_without_wake(&h, "t", partition, 1).await);
        }

        h.wake_tx
            .send(DrainIntent::Force)
            .await
            .expect("sender is running");
        for receiver in receivers {
            receiver
                .await
                .expect("ack channel remains connected")
                .expect("forced batch is acknowledged");
        }
        assert_eq!(Instant::now(), start);
        assert_eq!(transport.send_count(), 6);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn eligible_retries_never_exceed_max_in_flight() {
        let transport = MockTransport::new(Duration::from_millis(1));
        transport.fail_next(6);
        let h = spawn_sender_with(transport.clone(), 2, minutes(1));
        let mut receivers = Vec::new();
        for partition in 0..6 {
            receivers.extend(produce_ready_batches_without_wake(&h, "t", partition, 1).await);
        }

        h.wake_tx
            .send(DrainIntent::Force)
            .await
            .expect("sender is running");
        while transport.send_count() < 6 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(1)).await;

        for receiver in receivers {
            receiver
                .await
                .expect("ack channel remains connected")
                .expect("retry is acknowledged");
        }
        assert_eq!(transport.send_count(), 12);
        assert_eq!(transport.peak_active_sends(), 2);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn force_drains_multiple_ready_batches_from_one_partition_without_linger_wait() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, minutes(1));
        tokio::task::yield_now().await;
        let start = Instant::now();
        let receivers = produce_ready_batches_without_wake(&h, "t", 0, 3).await;

        h.wake_tx
            .send(DrainIntent::Force)
            .await
            .expect("sender is running");
        for receiver in receivers {
            receiver
                .await
                .expect("ack channel remains connected")
                .expect("forced batch is acknowledged");
        }
        assert_eq!(Instant::now(), start);
        assert_eq!(transport.send_count(), 3);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn one_ready_wake_drains_coalesced_backlog_past_the_cycle_cap() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 2, minutes(1));
        tokio::task::yield_now().await;
        let start = Instant::now();
        let mut receivers = Vec::new();
        for partition in 0..6 {
            receivers.extend(produce_ready_batches_without_wake(&h, "t", partition, 1).await);
        }

        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");
        for receiver in receivers {
            receiver
                .await
                .expect("ack channel remains connected")
                .expect("ready batch is acknowledged");
        }
        assert_eq!(Instant::now(), start);
        assert_eq!(transport.send_count(), 6);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn retry_release_resumes_same_partition_ready_backlog() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.fail_once_on(0);
        let h = spawn_sender_with(transport.clone(), 5, minutes(1));
        tokio::task::yield_now().await;
        let start = Instant::now();
        let receivers = produce_ready_batches_without_wake(&h, "t", 0, 2).await;
        let first_send = transport.send_started.notified();
        tokio::pin!(first_send);
        first_send.as_mut().enable();

        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");
        first_send.await;
        for receiver in receivers {
            receiver
                .await
                .expect("ack channel remains connected")
                .expect("retry and queued batch are acknowledged");
        }
        assert_eq!(
            Instant::now().duration_since(start),
            Duration::from_millis(1)
        );
        assert_eq!(transport.send_count(), 3);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_drains_multiple_batches_without_waiting_for_linger() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, minutes(1));
        tokio::task::yield_now().await;
        let start = Instant::now();
        let receivers = produce_ready_batches_without_wake(&h, "t", 0, 3).await;

        h.shutdown.cancel();
        h.handle.await.expect("sender shuts down cleanly");
        assert_eq!(Instant::now(), start);
        assert_eq!(transport.send_count(), 3);
        drop(receivers);
    }

    #[tokio::test(start_paused = true)]
    async fn channel_close_waits_for_retry_deadline_not_linger() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.fail_once_on(0);
        let h = spawn_sender_with(transport.clone(), 5, minutes(1));
        tokio::task::yield_now().await;
        let mut receivers = produce_ready_batches_without_wake(&h, "t", 0, 1).await;
        let first_send = transport.send_started.notified();
        tokio::pin!(first_send);
        first_send.as_mut().enable();
        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");
        first_send.await;
        let start = Instant::now();

        drop(h.wake_tx);
        h.handle.await.expect("closed sender drains cleanly");
        assert_eq!(
            Instant::now().duration_since(start),
            Duration::from_millis(1)
        );
        assert_eq!(transport.send_count(), 2);
        receivers
            .remove(0)
            .await
            .expect("ack channel remains connected")
            .expect("retry is acknowledged before close");
    }

    /// THE REGRESSION TEST for the same-partition pipelining hang.
    ///
    /// The test bursts many single-record batches at ONE partition through the
    /// real sender loop. The broker enforces strict per-partition sequencing AND
    /// a reorder model that completes same-partition requests issued
    /// *concurrently* higher-`base_sequence`-first. That models the on-the-wire
    /// write race of the old `join_all` same-partition pipelining.
    ///
    /// With the fix, one in flight per partition, a partition only ever has one
    /// request outstanding, so the staggered transport delay cannot reorder
    /// anything. The broker sees `base_sequence` 0,1,2,… exactly once each,
    /// every record acks `Ok` with offsets in order, and there is **zero retry
    /// churn**, that is exactly `N` broker applies.
    ///
    /// The `applied == N` assertion is the teeth. The old multi-in-flight design
    /// fed reordered concurrent requests to the broker, drew
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER`, drained, and resent, so it applied
    /// strictly more than `N`. Under sustained load on a cluster it churned long
    /// enough that the caller's time-boxed window saw a record's ack-oneshot
    /// still unresolved. That was the reported hang.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_partition_burst_all_acks_resolve_in_order() {
        const N: usize = 40;
        let transport = MockTransport::new(Duration::from_millis(2));
        let h = spawn_sender(transport.clone(), 5);

        let rxs = produce_burst(&h, "t", 0, N).await;

        let mut offsets = Vec::with_capacity(N);
        for (i, rx) in rxs.into_iter().enumerate() {
            let md = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .unwrap_or_else(|_| panic!("record {i} ack-oneshot never resolved (HANG)"))
                .expect("oneshot sender dropped")
                .expect("record must be acked Ok, not failed");
            assert2::assert!(md.partition == 0);
            offsets.push(md.offset);
        }

        // Offsets must be the clean increasing sequence 0..N — proof the broker
        // saw each base_sequence exactly once, in order.
        let expected: Vec<i64> = (0..i64::try_from(N).unwrap()).collect();
        assert2::assert!(offsets == expected);
        // Zero churn: with one in-flight per partition there is never an
        // out-of-order arrival, so the broker applies each batch exactly once.
        assert2::assert!(h.transport.applied_count() == N);

        shutdown(h).await;
    }

    /// Cross-partition pipelining still works concurrently and all acks resolve.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_partition_burst_all_acks_resolve() {
        const PARTS: i32 = 6;
        const PER: usize = 10;
        let transport = MockTransport::new(Duration::from_millis(1));
        let h = spawn_sender(transport.clone(), 5);

        let mut all = Vec::new();
        for p in 0..PARTS {
            let rxs = produce_burst(&h, "t", p, PER).await;
            all.push((p, rxs));
        }

        for (p, rxs) in all {
            let mut offsets = Vec::new();
            for (i, rx) in rxs.into_iter().enumerate() {
                let md = tokio::time::timeout(Duration::from_secs(10), rx)
                    .await
                    .unwrap_or_else(|_| panic!("part {p} record {i} never resolved (HANG)"))
                    .expect("oneshot dropped")
                    .expect("must be acked Ok");
                assert2::assert!(md.partition == p);
                offsets.push(md.offset);
            }
            let expected: Vec<i64> = (0..i64::try_from(PER).unwrap()).collect();
            assert2::assert!(offsets == expected);
        }

        shutdown(h).await;
    }

    /// A drain does not move the sticky partition. Kafka's `BuiltInPartitioner`
    /// switches only after `batch.size` bytes went to the partition.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_drained_batch_does_not_move_the_sticky_partition() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender(transport.clone(), 5);
        h.metadata_cache.lock().await.insert(
            "t".to_string(),
            TopicMetadata {
                num_partitions: 3,
                topic_id: Uuid::ZERO,
            },
        );
        let has_leader = |_: i32| true;
        let partitions = TopicPartitions {
            count: 3,
            has_leader: &has_leader,
        };
        let before = h.partitioner.peek("t", &partitions);

        let mut rxs = produce_single_batch(&h, "t", before.partition(), 1).await;
        tokio::time::timeout(Duration::from_secs(5), rxs.remove(0))
            .await
            .expect("record ack should resolve")
            .expect("oneshot sender should stay alive")
            .expect("record should ack");

        let after = h.partitioner.peek("t", &partitions);
        shutdown(h).await;
        assert2::assert!(after == before);
    }

    /// A sender configuration with `partitioner`, for a direct call into one
    /// sender function.
    fn direct_config(
        partitioner: Arc<BuiltInPartitioner>,
        max_in_flight: usize,
    ) -> (SenderConfig, Arc<MockTransport>) {
        let transport = MockTransport::new(Duration::ZERO);
        let (_wake_tx, wake_rx) = tokio::sync::mpsc::channel(1);
        let cfg = SenderConfig {
            transport: Box::new(ArcTransport(Arc::clone(&transport))),
            producer_id: -1,
            producer_epoch: Arc::new(AtomicI16::new(-1)),
            acks: Acks::All,
            compression: Compression::None,
            linger: millis(1),
            request_timeout_ms: 5_000,
            retries: i32::MAX,
            retry_backoff: RetryBackoff::new(Duration::from_millis(1), Duration::from_secs(1)),
            delivery_timeout: secs(30),
            max_in_flight,
            metadata_cache: Arc::new(Mutex::new(HashMap::new())),
            partition_leaders: Arc::new(DashMap::new()),
            partitioner,
            accumulators: Arc::new(DashMap::new()),
            next_seq: Arc::new(DashMap::new()),
            state: Arc::new(AtomicU8::new(STATE_ACTIVE)),
            wake_rx,
            flush_notify: Arc::new(Notify::new()),
            in_flight: Arc::new(AtomicUsize::new(0)),
            shutdown: CancellationToken::new(),
            transactional_id: None,
            txn_state: Arc::new(Mutex::new(TxnState::Uninitialized)),
            txn_pid_epoch: Arc::new(Mutex::new((-1, -1))),
            txn_recovery_required: Arc::new(AtomicBool::new(false)),
            txn_recovery_generation: Arc::new(AtomicU64::new(0)),
            txn_abortable_error: Arc::new(AbortableErrorSlot::default()),
        };
        (cfg, transport)
    }

    /// A partitioner that takes its random values from `values` in order.
    fn scripted_partitioner(
        config: PartitionerConfig,
        values: Vec<u32>,
    ) -> Arc<BuiltInPartitioner> {
        let values = Arc::new(StdMutex::new(VecDeque::from(values)));
        Arc::new(BuiltInPartitioner::with_random(
            config,
            Arc::new(move || {
                values
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("the test scripts enough random values")
            }),
        ))
    }

    /// Put `batches` sealed batches of one record each in the accumulator of
    /// `(topic, partition)`.
    async fn queue_batches(cfg: &SenderConfig, topic: &str, partition: i32, batches: usize) {
        let accumulator = Arc::clone(
            cfg.accumulators
                .entry((topic.to_owned(), partition))
                .or_insert_with(|| Arc::new(Mutex::new(Accumulator::new(16 * 1024))))
                .value(),
        );
        let mut accumulator = accumulator.lock().await;
        for _ in 0..batches {
            let _ = accumulator.try_append(
                None,
                Some(bytes::Bytes::from_static(b"v")),
                vec![],
                0,
                None,
            );
            accumulator.seal_current();
        }
    }

    /// The sender gives the partitioner the queue size of each partition, as
    /// Kafka's `RecordAccumulator.partitionReady` does, and the new sticky
    /// partition follows the weights of `BuiltInPartitioner.nextPartition`.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn queue_sizes_weight_the_adaptive_pick() {
        struct Row {
            name: &'static str,
            /// The leader of each partition of the topic `t`.
            leaders: &'static [i32],
            /// The sealed batches of each partition. A partition past the end
            /// has no accumulator.
            batches: &'static [usize],
            /// A partition with a parked resend.
            parked: Option<i32>,
            /// A broker whose records waited this long with no send.
            waited: Option<(i32, Duration)>,
            adaptive: bool,
            randoms: &'static [u32],
            picks: &'static [i32],
        }
        let rows = [
            Row {
                // Sizes 2, 0, 1 give the table 1, 4, 6.
                name: "queue sizes weight the pick",
                leaders: &[1, 1, 1],
                batches: &[2, 0, 1],
                parked: None,
                waited: None,
                adaptive: true,
                randoms: &[0, 1, 3, 4, 5, 6],
                picks: &[0, 1, 1, 2, 2, 0],
            },
            Row {
                // Sizes 2, 1, 1 give the table 1, 3, 5.
                name: "a parked resend counts in the queue",
                leaders: &[1, 1, 1],
                batches: &[2, 0, 1],
                parked: Some(1),
                waited: None,
                adaptive: true,
                randoms: &[0, 1, 2, 3, 4],
                picks: &[0, 1, 1, 2, 2],
            },
            Row {
                // Sizes 2 and 1 on partitions 0 and 2 give the table 1, 3.
                name: "a partition without a leader is left out",
                leaders: &[1, -1, 1],
                batches: &[2, 0, 1],
                parked: None,
                waited: None,
                adaptive: true,
                randoms: &[0, 1, 2, 3],
                picks: &[0, 2, 2, 0],
            },
            Row {
                name: "a broker past the availability timeout is left out",
                leaders: &[1, 2, 1],
                batches: &[2, 0, 1],
                parked: None,
                waited: Some((2, Duration::from_millis(101))),
                adaptive: true,
                randoms: &[0, 1, 2, 3],
                picks: &[0, 2, 2, 0],
            },
            Row {
                // Sizes 2, 0, 1 give the table 1, 4, 6.
                name: "a broker inside the availability timeout stays in",
                leaders: &[1, 2, 1],
                batches: &[2, 0, 1],
                parked: None,
                waited: Some((2, Duration::from_millis(100))),
                adaptive: true,
                randoms: &[0, 1, 3, 4],
                picks: &[0, 1, 1, 2],
            },
            Row {
                name: "a partition with no queue gives a uniform pick",
                leaders: &[1, 1, 1],
                batches: &[2, 0],
                parked: None,
                waited: None,
                adaptive: true,
                randoms: &[0, 1, 2, 3],
                picks: &[0, 1, 2, 0],
            },
            Row {
                name: "equal queues give a uniform pick",
                leaders: &[1, 1, 1],
                batches: &[1, 1, 1],
                parked: None,
                waited: None,
                adaptive: true,
                randoms: &[0, 1, 2, 3],
                picks: &[0, 1, 2, 0],
            },
            Row {
                name: "adaptive partitioning off gives a uniform pick",
                leaders: &[1, 1, 1],
                batches: &[2, 0, 1],
                parked: None,
                waited: None,
                adaptive: false,
                randoms: &[0, 1, 2, 3],
                picks: &[0, 1, 2, 0],
            },
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for row in rows {
            let mut randoms = row.randoms.to_vec();
            randoms.push(0);
            let partitioner = scripted_partitioner(
                PartitionerConfig {
                    sticky_batch_size: 1,
                    ignore_keys: false,
                    adaptive_partitioning: row.adaptive,
                    availability_timeout: Duration::from_millis(100),
                },
                randoms,
            );
            let (cfg, _transport) = direct_config(partitioner, 5);
            cfg.metadata_cache.lock().await.insert(
                "t".to_owned(),
                TopicMetadata {
                    num_partitions: 3,
                    topic_id: Uuid::ZERO,
                },
            );
            for (partition, &leader) in (0..).zip(row.leaders) {
                cfg.partition_leaders
                    .insert(("t".to_owned(), partition), leader);
            }
            for (partition, &batches) in (0..).zip(row.batches) {
                queue_batches(&cfg, "t", partition, batches).await;
            }
            let mut state = PipelineState::default();
            if let Some(partition) = row.parked {
                let (pb, _rx) = idempotent_batch("t", partition, 0, 0);
                state.retry.insert(("t".to_owned(), partition), pb);
            }
            if let Some((broker, waited)) = row.waited {
                let now = Instant::now();
                state.node_latency.insert(
                    broker,
                    NodeLatency {
                        ready_at: now,
                        drained_at: now.checked_sub(waited).expect("instant in range"),
                    },
                );
            }

            update_partition_load_stats(&cfg, &state).await;

            let leaders = row.leaders;
            let has_leader =
                |partition: i32| usize::try_from(partition).is_ok_and(|index| leaders[index] >= 0);
            let partitions = TopicPartitions {
                count: 3,
                has_leader: &has_leader,
            };
            let picks: Vec<i32> = row
                .randoms
                .iter()
                .map(|_| {
                    let sticky = cfg.partitioner.peek("t", &partitions);
                    cfg.partitioner.update("t", sticky, 1, &partitions, true);
                    sticky.partition()
                })
                .collect();
            actual.push((row.name, picks));
            expected.push((row.name, row.picks.to_vec()));
        }
        assert2::assert!(actual == expected);
    }

    /// With `partitioner.availability.timeout.ms`, a drain cycle notes for
    /// each leader when records were ready and when the cycle sent them.
    /// Kafka's `Sender.sendProducerData` calls
    /// `RecordAccumulator.updateNodeLatencyStats` in the same way.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_drain_notes_ready_and_drained_times_per_leader() {
        let partitioner = Arc::new(BuiltInPartitioner::new(PartitionerConfig {
            sticky_batch_size: 16 * 1024,
            ignore_keys: false,
            adaptive_partitioning: true,
            availability_timeout: Duration::from_millis(100),
        }));
        let (mut cfg, _transport) = direct_config(partitioner, 5);
        for (partition, leader) in [(0, 1), (1, 2), (2, 3)] {
            cfg.partition_leaders
                .insert(("t".to_owned(), partition), leader);
            queue_batches(&cfg, "t", partition, 1).await;
        }
        let earlier = Instant::now();
        tokio::time::advance(Duration::from_secs(1)).await;
        let now = Instant::now();
        let mut state = PipelineState::default();
        // Partition 1 waits for a resend, so its new batch cannot go out.
        let (mut parked, _rx) = idempotent_batch("t", 1, 0, 0);
        parked.backoff_until = Some(now + Duration::from_secs(10));
        state.retry.insert(("t".to_owned(), 1), parked);
        let seen = NodeLatency {
            ready_at: earlier,
            drained_at: earlier,
        };
        state.node_latency.insert(2, seen);
        state.node_latency.insert(3, seen);

        drain_once(&mut cfg, &mut state, DrainIntent::Force).await;

        let drained = NodeLatency {
            ready_at: now,
            drained_at: now,
        };
        assert2::assert!(
            state.node_latency
                == HashMap::from([
                    (1, drained),
                    (
                        2,
                        NodeLatency {
                            ready_at: now,
                            drained_at: earlier,
                        },
                    ),
                    (3, drained),
                ])
        );
    }

    /// The cap of one drain cycle does not note a broker as waiting. The
    /// cycle did not try the broker, so its records did not wait for it.
    /// Kafka notes a node only when `NetworkClient.ready` refuses it
    /// (`Sender.sendProducerData`).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn the_cycle_cap_does_not_note_an_untried_broker() {
        let partitioner = Arc::new(BuiltInPartitioner::new(PartitionerConfig {
            sticky_batch_size: 16 * 1024,
            ignore_keys: false,
            adaptive_partitioning: true,
            availability_timeout: Duration::from_millis(100),
        }));
        // One Produce per cycle, and three partitions on three brokers.
        let (mut cfg, _transport) = direct_config(partitioner, 1);
        for (partition, leader) in [(0, 1), (1, 2), (2, 3)] {
            cfg.partition_leaders
                .insert(("t".to_owned(), partition), leader);
            queue_batches(&cfg, "t", partition, 1).await;
        }
        let earlier = Instant::now();
        tokio::time::advance(Duration::from_secs(1)).await;
        let now = Instant::now();
        let seen = NodeLatency {
            ready_at: earlier,
            drained_at: earlier,
        };
        let mut state = PipelineState::default();
        for leader in [1, 2, 3] {
            state.node_latency.insert(leader, seen);
        }

        drain_once(&mut cfg, &mut state, DrainIntent::Force).await;

        // The cycle sends one batch. The broker that got it is drained now,
        // and the two brokers that the cap left out keep their old times.
        let drained = NodeLatency {
            ready_at: now,
            drained_at: now,
        };
        let mut latencies: Vec<NodeLatency> = state.node_latency.values().copied().collect();
        latencies.sort_unstable_by_key(|latency| latency.drained_at);
        assert2::assert!(latencies == vec![seen, seen, drained]);
    }

    /// A one-shot transport error mid-stream must NOT drop or reorder. The
    /// sender resends the failed batch. The broker dedups it with DUPLICATE if
    /// it had landed, or accepts it fresh. All acks resolve, and the offsets
    /// stay a clean increasing run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn transport_error_mid_stream_recovers_in_order() {
        const N: usize = 12;
        let transport = MockTransport::new(Duration::ZERO);
        // Fail the batch at base_sequence 3 exactly once.
        transport.fail_once_on(3);
        let h = spawn_sender(transport.clone(), 5);

        let rxs = produce_burst(&h, "t", 0, N).await;

        let mut offsets = Vec::with_capacity(N);
        for (i, rx) in rxs.into_iter().enumerate() {
            let md = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .unwrap_or_else(|_| panic!("record {i} never resolved after transport error"))
                .expect("oneshot dropped")
                .expect("must be acked Ok after recovery");
            offsets.push(md.offset);
        }
        let expected: Vec<i64> = (0..i64::try_from(N).unwrap()).collect();
        assert2::assert!(offsets == expected);

        // in_flight must fully drain back to zero (it lags the last ack-oneshot
        // by the `finish_in_flight` decrement, so poll via `flush_notify`).
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while h.in_flight.load(Ordering::Acquire) != 0 {
                let _ = tokio::time::timeout(Duration::from_millis(20), h.flush_notify.notified())
                    .await;
            }
        })
        .await;
        assert2::assert!(drained.is_ok());
        // A transport failure forces a metadata refresh so the resend re-resolves
        // the leader; the sender must have refreshed at least once.
        assert2::assert!(h.transport.refresh_count() >= 1);
        shutdown(h).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dead_leader_failover_refreshes_and_reroutes_before_timeout_churn() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(0);
        transport.add_known_broker(1);
        transport.fail_once_on_leader(0);
        transport.set_refresh_response(MetadataResponse {
            brokers: Vec::new(),
            topics: vec![MetadataResponseTopic {
                name: Some("t".to_string()),
                partitions: vec![MetadataResponsePartition {
                    partition_index: 0,
                    leader_id: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });

        let h = spawn_sender(transport.clone(), 5);
        h.partition_leaders.insert(("t".to_string(), 0), 0);

        let mut rxs = produce_single_batch(&h, "t", 0, 1).await;
        let md = tokio::time::timeout(Duration::from_secs(5), rxs.remove(0))
            .await
            .expect("record ack should resolve after failover reroute")
            .expect("oneshot sender should stay alive")
            .expect("record should ack after reroute");

        let refresh_count = h.transport.refresh_count();
        check!(
            (
                md.partition,
                md.offset,
                (1..=2).contains(&refresh_count),
                h.transport.sent_leaders(),
                h.transport.evicted(),
            ) == (0, 0, true, vec![Some(0), Some(1)], vec![0]),
            "failover must refresh once without churn, evict the stale leader, and reroute"
        );

        shutdown(h).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn slow_dead_leader_does_not_block_live_partition_ack() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(0);
        transport.add_known_broker(1);
        transport.add_known_broker(6);
        transport.fail_once_on_leader(0);
        transport.delay_leader(0, Duration::from_millis(250));
        transport.set_refresh_response(MetadataResponse {
            brokers: Vec::new(),
            topics: vec![MetadataResponseTopic {
                name: Some("t".to_string()),
                partitions: vec![MetadataResponsePartition {
                    partition_index: 0,
                    leader_id: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });

        let h = spawn_sender(transport.clone(), 5);
        h.partition_leaders.insert(("t".to_string(), 0), 0);
        h.partition_leaders.insert(("t".to_string(), 1), 6);

        let mut dead_rx = produce_single_batch_without_wake(&h, "t", 0, 1).await;
        let mut live_rx = produce_single_batch_without_wake(&h, "t", 1, 1).await;
        let _ = h.wake_tx.try_send(DrainIntent::Force);

        let live_md = tokio::time::timeout(Duration::from_millis(100), live_rx.remove(0))
            .await
            .expect("live partition ack should not wait for a slow dead leader")
            .expect("oneshot sender should stay alive")
            .expect("live partition should ack Ok");
        assert2::assert!((live_md.partition, live_md.offset) == (1, 0));

        let dead_md = tokio::time::timeout(Duration::from_secs(5), dead_rx.remove(0))
            .await
            .expect("dead leader partition should resolve after reroute")
            .expect("oneshot sender should stay alive")
            .expect("dead leader partition should ack after reroute");
        assert2::assert!((dead_md.partition, dead_md.offset) == (0, 0));

        let sent = h.transport.sent_leaders();
        assert2::assert!(sent.len() == 3 && sent[2] == Some(1));
        assert2::assert!(sent[..2].contains(&Some(0)) && sent[..2].contains(&Some(6)));

        shutdown(h).await;
    }

    /// `flush_notify` fires and `in_flight` returns to zero once the burst drains.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_flight_drains_to_zero() {
        let transport = MockTransport::new(Duration::from_millis(1));
        let h = spawn_sender(transport.clone(), 5);
        let rxs = produce_burst(&h, "t", 0, 20).await;
        for rx in rxs {
            let _ = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .expect("no hang")
                .expect("oneshot")
                .expect("acked");
        }
        // `in_flight` is decremented just AFTER a batch's ack-oneshots are sent
        // (see `ack_batch`), so it can briefly lag the last `rx.await`. Wait for
        // it to settle to zero via `flush_notify`, mirroring `Producer::flush`.
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while h.in_flight.load(Ordering::Acquire) != 0 {
                let _ = tokio::time::timeout(Duration::from_millis(20), h.flush_notify.notified())
                    .await;
            }
        })
        .await;
        assert2::assert!(drained.is_ok());
        // And the broker applied each batch exactly once (no churn).
        assert2::assert!(h.transport.applied_count() == 20);
        let _ = &h.next_seq;
        shutdown(h).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn acks_zero_uses_one_way_transport_and_returns_unknown_offset() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with_acks(
            transport.clone(),
            1,
            millis(1),
            i32::MAX,
            secs(30),
            Acks::Zero,
        );

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let metadata = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("record resolves")
            .expect("sender remains")
            .expect("enqueue succeeds");

        check!(metadata.offset == -1);
        check!(transport.no_response_count() == 1);
        check!(transport.applied_count() == 1);

        shutdown(h).await;
    }

    /// Mechanism proof: issuing several same-partition requests CONCURRENTLY (as
    /// the old `send_batches` did via `join_all`) against the staggered-reorder
    /// broker makes the broker apply them higher-`base_sequence`-first, so every
    /// request except the lowest draws `OUT_OF_ORDER_SEQUENCE_NUMBER`. This is
    /// the gap-and-resend trigger the fix eliminates by never issuing more than
    /// one same-partition request at a time. (Pure transport-level check; no
    /// sender loop — it isolates the reorder mechanism.)
    ///
    /// The test uses paused virtual time, so the staggered sleeps order the
    /// arrivals deterministically and do not rely on the OS scheduler.
    #[tokio::test(start_paused = true)]
    async fn concurrent_same_partition_sends_reorder_and_trip_out_of_order() {
        let transport = MockTransport::new(Duration::from_millis(5));

        // Build single-partition Produce requests for base_sequences 0,1,2,3,4.
        let make_req = |base_sequence: i32| ProduceRequest {
            acks: -1,
            topic_data: vec![TopicProduceData {
                name: "t".to_string(),
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(
                        RecordBatch {
                            attributes: Attributes::default(),
                            producer_id: 1,
                            producer_epoch: 0,
                            base_sequence,
                            records: vec![Record {
                                attributes: 0,
                                timestamp_delta: 0,
                                offset_delta: 0,
                                key: None,
                                value: Some(bytes::Bytes::from_static(b"x")),
                                headers: vec![],
                            }],
                            ..Default::default()
                        }
                        .into(),
                    ),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        // Fire all five concurrently — exactly what the buggy join_all did.
        let results =
            futures::future::join_all((0..5).map(|s| transport.send_produce(None, make_req(s))))
                .await;

        // The lowest (0) is accepted; every higher one trips OUT_OF_ORDER because
        // it reached the broker ahead of 0.
        let codes: Vec<i16> = results
            .into_iter()
            .map(|r| r.expect("no transport error").responses[0].partition_responses[0].error_code)
            .collect();
        assert2::assert!(codes[0] == codes::NONE);
        for c in &codes[1..] {
            assert2::assert!(*c == codes::OUT_OF_ORDER_SEQUENCE_NUMBER);
        }

        // Arrivals were applied highest-first (the reorder), confirming the race.
        let arrivals: Vec<i32> = transport
            .arrivals
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, s)| *s)
            .collect();
        assert2::assert!(arrivals == vec![4, 3, 2, 1, 0]);
    }

    /// A partition with a batch pending resend must NOT also send its next
    /// batch in the same cycle. Under a broker that reorders concurrent
    /// same-partition requests, the new batch could otherwise overtake the
    /// resend and trip `OUT_OF_ORDER_SEQUENCE_NUMBER` churn.
    ///
    /// A one-shot transport error parks one batch for resend mid-stream. With
    /// the reorder model active, the test still expects each batch applied
    /// exactly once, with no churn, and offsets in a clean run. This guards the
    /// "ordering preserved by construction" property of the one-slot-per-
    /// partition pipeline against a same-partition send race.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn retry_does_not_race_new_batch_under_reorder() {
        const N: usize = 16;
        let transport = MockTransport::new(Duration::from_millis(2));
        // Fail the batch at base_sequence 5 once: it parks for resend while
        // later batches are still queued behind it.
        transport.fail_once_on(5);
        let h = spawn_sender(transport.clone(), 5);

        let rxs = produce_burst(&h, "t", 0, N).await;
        let mut offsets = Vec::with_capacity(N);
        for (i, rx) in rxs.into_iter().enumerate() {
            let md = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .unwrap_or_else(|_| panic!("record {i} never resolved"))
                .expect("oneshot dropped")
                .expect("acked Ok");
            offsets.push(md.offset);
        }
        let expected: Vec<i64> = (0..i64::try_from(N).unwrap()).collect();
        assert2::assert!(offsets == expected);
        // The failed send errored at the transport before the broker applied it,
        // so each of the N batches is applied exactly once: a new batch never
        // raced (and reordered ahead of) the pending resend.
        assert2::assert!(h.transport.applied_count() == N);

        shutdown(h).await;
    }

    /// Routing decision in `resolve_leader`. A partition whose cached leader is
    /// a known, dialable broker goes to that broker. A partition whose leader is
    /// unknown, or whose address the pool cannot dial, falls back to the
    /// bootstrap connection. The test drives the real sender, so it observes the
    /// `leader` argument handed to the transport directly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn routes_to_known_leader_else_bootstrap() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(5); // 5 is dialable; 7 is not.
        let h = spawn_sender(transport.clone(), 5);

        // Partition 0 → leader 5 (known): must route to Some(5).
        h.partition_leaders.insert(("t".to_string(), 0), 5);
        // Partition 1 → leader 7 (unknown address): must fall back to bootstrap.
        h.partition_leaders.insert(("t".to_string(), 1), 7);

        let rx0 = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let rx1 = produce_burst(&h, "t", 1, 1).await.pop().expect("one rx");
        for (i, rx) in [rx0, rx1].into_iter().enumerate() {
            let _ = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .unwrap_or_else(|_| panic!("record {i} never resolved"))
                .expect("oneshot dropped")
                .expect("acked Ok");
        }

        let leaders = h.transport.sent_leaders();
        check!(
            (
                leaders.contains(&Some(5)),
                leaders.contains(&None),
                leaders.contains(&Some(7)),
            ) == (true, true, false),
            "known, bootstrap-fallback, and unknown-address leader routing: {leaders:?}"
        );

        shutdown(h).await;
    }

    /// A terminal but non-fatal server error, that is an unmodeled code, fails
    /// the record with `Server(code)` and releases its in-flight slot. It must
    /// not fence, it must not hang, and it must not be retried forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_server_error_fails_record() {
        const MESSAGE_TOO_LARGE: i16 = 10;
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject_code_once(0, MESSAGE_TOO_LARGE);
        let h = spawn_sender(transport.clone(), 5);

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let res = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved (HANG)")
            .expect("oneshot dropped");
        let err = res.expect_err("terminal error must fail the record, not ack it");
        assert2::assert!(matches!(err, ProducerError::Server(MESSAGE_TOO_LARGE)));

        // The slot is released: in_flight drains back to zero.
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while h.in_flight.load(Ordering::Acquire) != 0 {
                let _ = tokio::time::timeout(Duration::from_millis(20), h.flush_notify.notified())
                    .await;
            }
        })
        .await;
        assert2::assert!(drained.is_ok());

        shutdown(h).await;
    }

    /// How one record resolved, and what the sender did on the way.
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct PartitionErrorOutcome {
        /// `Ok(offset)` for an acked record, `Err(Some(code))` for
        /// `ProducerError::Server(code)`, and `Err(None)` for any other error.
        result: Result<i64, Option<i16>>,
        sends: usize,
        refreshes: usize,
        /// The idempotent producer epoch after the record resolved.
        epoch: i16,
    }

    /// A code whose Kafka exception extends `InvalidMetadataException`
    /// refreshes metadata once and resends the batch, which the broker then
    /// acks. Another `RetriableException` resends without a refresh. A code
    /// that is not retriable fails the record with no resend, and the
    /// idempotent producer bumps its epoch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partition_error_codes_follow_kafka_retry_classes() {
        let refreshed = PartitionErrorOutcome {
            result: Ok(0),
            sends: 2,
            refreshes: 1,
            epoch: 0,
        };
        let resent = PartitionErrorOutcome {
            refreshes: 0,
            ..refreshed
        };
        let failed = |code| PartitionErrorOutcome {
            result: Err(Some(code)),
            sends: 1,
            refreshes: 0,
            epoch: 1,
        };
        for (name, error_code, want) in [
            (
                "UNKNOWN_TOPIC_OR_PARTITION",
                test_codes::UNKNOWN_TOPIC_OR_PARTITION,
                refreshed,
            ),
            ("LEADER_NOT_AVAILABLE", 5, refreshed),
            (
                "NOT_LEADER_OR_FOLLOWER",
                test_codes::NOT_LEADER_OR_FOLLOWER,
                refreshed,
            ),
            ("KAFKA_STORAGE_ERROR", 56, refreshed),
            ("FENCED_LEADER_EPOCH", 74, refreshed),
            ("UNKNOWN_TOPIC_ID", test_codes::UNKNOWN_TOPIC_ID, refreshed),
            ("INCONSISTENT_TOPIC_ID", 103, refreshed),
            ("CORRUPT_MESSAGE", 2, resent),
            ("REQUEST_TIMED_OUT", 7, resent),
            ("NOT_ENOUGH_REPLICAS", 19, resent),
            ("NOT_ENOUGH_REPLICAS_AFTER_APPEND", 20, resent),
            ("UNKNOWN_LEADER_EPOCH", 75, resent),
            ("THROTTLING_QUOTA_EXCEEDED", 89, resent),
            ("MESSAGE_TOO_LARGE", 10, failed(10)),
            ("TOPIC_AUTHORIZATION_FAILED", 29, failed(29)),
            ("INVALID_RECORD", 87, failed(87)),
            ("UNKNOWN_SERVER_ERROR", -1, failed(-1)),
        ] {
            let transport = MockTransport::new(Duration::ZERO);
            transport.inject_code_once(0, error_code);
            let h = spawn_sender(transport.clone(), 5);

            let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
            let result = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .unwrap_or_else(|_| panic!("case {name}: record never resolved"))
                .expect("oneshot dropped")
                .map(|md| md.offset)
                .map_err(|error| match error {
                    ProducerError::Server(code) => Some(code),
                    _ => None,
                });
            let got = PartitionErrorOutcome {
                result,
                sends: transport.send_count(),
                refreshes: transport.refresh_count(),
                epoch: h.producer_epoch.load(Ordering::Acquire),
            };
            check!(got == want, "case {name}");

            shutdown(h).await;
        }
    }

    /// How one record of a scenario resolved.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum RecordOutcome {
        Acked(i64),
        Server(i16),
        Fenced,
        Other,
    }

    /// The observable result of one idempotence scenario.
    #[derive(Debug, PartialEq, Eq)]
    struct IdempotenceOutcome {
        records: Vec<RecordOutcome>,
        /// `(producer_epoch, base_sequence)` of each Produce, in order.
        sent: Vec<(i16, i32)>,
        idempotent_epoch: i16,
        fenced: bool,
    }

    /// Produce `records` records one after the other on one partition. The
    /// broker gives `inject` once, and the test waits for each record before it
    /// produces the next.
    async fn run_idempotence_scenario(
        mode: BatchMode,
        records: usize,
        inject: Inject,
    ) -> IdempotenceOutcome {
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject(inject);
        let h = spawn_sender_in_mode(transport.clone(), mode);
        let mut outcomes = Vec::with_capacity(records);
        for _ in 0..records {
            let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
            let outcome = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .expect("record resolves")
                .expect("oneshot dropped");
            outcomes.push(match outcome {
                Ok(metadata) => RecordOutcome::Acked(metadata.offset),
                Err(ProducerError::Server(code)) => RecordOutcome::Server(code),
                Err(ProducerError::FencedProducer) => RecordOutcome::Fenced,
                Err(_) => RecordOutcome::Other,
            });
        }
        let outcome = IdempotenceOutcome {
            records: outcomes,
            sent: transport.sent_batches(),
            idempotent_epoch: h.producer_epoch.load(Ordering::Acquire),
            fenced: h.state.load(Ordering::Acquire) == STATE_FENCED,
        };
        shutdown(h).await;
        outcome
    }

    /// A one-shot answer with `error_code` to `seq`, with `log_start_offset`.
    fn answer(seq: i32, error_code: i16, log_start_offset: i64) -> Inject {
        Inject {
            seq,
            name: None,
            topic_id: None,
            error_code,
            base_offset: -1,
            leader_hint: -1,
            log_start_offset,
            forget_producer_state: false,
        }
    }

    /// The idempotence codes follow Kafka's `Sender.completeBatch`,
    /// `TransactionManager.canRetry` and
    /// `TransactionManager.handleFailedBatch` in each batch mode.
    ///
    /// An idempotent producer raises its epoch and starts the sequences again
    /// at 0 after `OUT_OF_ORDER_SEQUENCE_NUMBER`, `UNKNOWN_PRODUCER_ID` and a
    /// failed batch. A transactional producer gives the sequences of a failed
    /// batch back. A fatal code fences the producer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idempotence_errors_follow_kafka_in_each_mode() {
        use BatchMode::{Idempotent, Plain, Transactional};
        use RecordOutcome::{Acked, Fenced, Server};
        let (_, txn) = TXN_PID_EPOCH;
        let outcome =
            |records: Vec<RecordOutcome>, sent: Vec<(i16, i32)>, idempotent_epoch, fenced| {
                IdempotenceOutcome {
                    records,
                    sent,
                    idempotent_epoch,
                    fenced,
                }
            };
        let cases = [
            (
                "idempotent out of order bumps the epoch and resends",
                Idempotent,
                2,
                answer(0, 45, -1),
                outcome(
                    vec![Acked(0), Acked(1)],
                    vec![(0, 0), (1, 0), (1, 1)],
                    1,
                    false,
                ),
            ),
            (
                "idempotent unknown producer id bumps the epoch and resends",
                Idempotent,
                2,
                answer(0, 59, 0),
                outcome(
                    vec![Acked(0), Acked(1)],
                    vec![(0, 0), (1, 0), (1, 1)],
                    1,
                    false,
                ),
            ),
            (
                "unknown producer id without a log start resends",
                Idempotent,
                2,
                answer(0, 59, -1),
                outcome(
                    vec![Acked(0), Acked(1)],
                    vec![(0, 0), (0, 0), (0, 1)],
                    0,
                    false,
                ),
            ),
            (
                "idempotent invalid epoch fails the batch and bumps the epoch",
                Idempotent,
                2,
                answer(0, 47, -1),
                outcome(vec![Server(47), Acked(0)], vec![(0, 0), (1, 0)], 1, false),
            ),
            (
                "idempotent failed batch bumps the epoch",
                Idempotent,
                2,
                answer(0, 10, -1),
                outcome(vec![Server(10), Acked(0)], vec![(0, 0), (1, 0)], 1, false),
            ),
            (
                "producer fenced is fatal",
                Idempotent,
                2,
                answer(0, 90, -1),
                outcome(vec![Fenced, Fenced], vec![(0, 0)], 0, true),
            ),
            (
                "cluster authorization is fatal",
                Idempotent,
                2,
                answer(0, 31, -1),
                outcome(vec![Server(31), Fenced], vec![(0, 0)], 0, true),
            ),
            (
                "transactional out of order gives the sequence back",
                Transactional,
                2,
                answer(0, 45, -1),
                outcome(
                    vec![Server(45), Acked(0)],
                    vec![(txn, 0), (txn, 0)],
                    0,
                    false,
                ),
            ),
            (
                "transactional invalid epoch gives the sequence back",
                Transactional,
                2,
                answer(0, 47, -1),
                outcome(
                    vec![Server(47), Acked(0)],
                    vec![(txn, 0), (txn, 0)],
                    0,
                    false,
                ),
            ),
            (
                "transactional unknown producer id past the log start restarts at 0",
                Transactional,
                2,
                answer(0, 59, 0),
                outcome(
                    vec![Acked(0), Acked(1)],
                    vec![(txn, 0), (txn, 0), (txn, 1)],
                    0,
                    false,
                ),
            ),
            (
                "transactional unknown producer id at the log start resets the partition",
                Transactional,
                3,
                Inject {
                    forget_producer_state: true,
                    ..answer(1, 59, 0)
                },
                outcome(
                    vec![Acked(0), Server(59), Acked(1)],
                    vec![(txn, 0), (txn, 1), (txn, 0)],
                    0,
                    false,
                ),
            ),
            (
                "transactional producer fenced is fatal",
                Transactional,
                2,
                answer(0, 90, -1),
                outcome(vec![Fenced, Fenced], vec![(txn, 0)], 0, true),
            ),
            (
                "plain out of order fails the batch and keeps sequences",
                Plain,
                2,
                answer(0, 45, -1),
                outcome(
                    vec![Server(45), Acked(0)],
                    vec![(-1, 0), (-1, 1)],
                    -1,
                    false,
                ),
            ),
        ];
        for (name, mode, records, inject, expected) in cases {
            let actual = run_idempotence_scenario(mode, records, inject).await;
            check!(actual == expected, "{name}");
        }
    }

    /// A topic that was deleted and created again keeps its name and gets a
    /// new id. The leader answers `UNKNOWN_TOPIC_ID` to the old id. The sender
    /// refreshes metadata, and the resend carries the new id, which the broker
    /// acks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unknown_topic_id_resends_with_the_refreshed_topic_id() {
        let stale_id = Uuid([1u8; 16]);
        let fresh_id = Uuid([2u8; 16]);
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject(Inject {
            seq: 0,
            name: Some(String::new()),
            topic_id: Some(stale_id),
            error_code: test_codes::UNKNOWN_TOPIC_ID,
            base_offset: -1,
            leader_hint: -1,
            log_start_offset: -1,
            forget_producer_state: false,
        });
        transport.set_refresh_response(MetadataResponse {
            topics: vec![MetadataResponseTopic {
                error_code: codes::NONE,
                name: Some("t".to_string()),
                topic_id: fresh_id,
                partitions: vec![MetadataResponsePartition {
                    error_code: codes::NONE,
                    partition_index: 0,
                    leader_id: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
        let h = spawn_sender(transport.clone(), 5);
        h.metadata_cache.lock().await.insert(
            "t".to_string(),
            TopicMetadata {
                num_partitions: 1,
                topic_id: stale_id,
            },
        );

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let offset = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved")
            .expect("oneshot dropped")
            .expect("acked Ok after the refresh and resend")
            .offset;

        check!(
            (
                offset,
                transport.sent_topic_ids(),
                transport.refresh_count()
            ) == (0, vec![stale_id, fresh_id], 1)
        );

        shutdown(h).await;
    }

    /// Kafka's `Sender.failBatch` fails a batch that has no retry left and
    /// calls `TransactionManager.handleFailedBatch`, which raises the epoch of
    /// an idempotent producer and starts every partition at sequence 0. It
    /// does not fence, so the next record still goes out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exhausted_retry_fails_the_batch_and_bumps_the_epoch() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject_code_once(0, test_codes::NOT_LEADER_OR_FOLLOWER);
        let h = spawn_sender_with_retries(transport.clone(), 1, millis(1), 0);

        let first = produce_burst(&h, "t", 0, 1).await.pop().expect("first ack");
        let first_error = tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .expect("first ack resolves")
            .expect("first sender remains")
            .expect_err("exhausted batch must fail");
        assert2::assert!(
            matches!(
                first_error,
                ProducerError::Server(test_codes::NOT_LEADER_OR_FOLLOWER)
            ),
            "{first_error:?}"
        );
        assert2::assert!(h.state.load(Ordering::Acquire) == STATE_ACTIVE);
        assert2::assert!(h.producer_epoch.load(Ordering::Acquire) == 1);

        let second = produce_burst(&h, "t", 0, 1)
            .await
            .pop()
            .expect("second ack");
        let metadata = tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .expect("second ack resolves")
            .expect("second sender remains")
            .expect("the producer still sends after the failed batch");
        assert2::assert!(metadata.partition == 0);
        // The failed batch at (epoch 0, sequence 0), then the next record at
        // the new epoch, which starts the partition again at sequence 0.
        assert2::assert!(transport.sent_batches() == vec![(0, 0), (1, 0)]);

        shutdown(h).await;
    }

    /// A transactional batch that has no retry left moves the producer to the
    /// abortable-error state, not to a fence. Kafka's
    /// `Sender.failBatch` calls `TransactionManager.handleFailedBatch`, which
    /// moves a transactional producer to `ABORTABLE_ERROR` there, so that a
    /// later `commitTransaction` fails and the application must abort. The
    /// producer itself stays active: it is the transaction, not the producer,
    /// that can no longer commit, and Kafka's idempotent producer never fences
    /// on a plain failed-batch retry exhaustion either.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_transactional_batch_with_no_retry_left_sets_the_abortable_error() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject_code_once(0, test_codes::NOT_LEADER_OR_FOLLOWER);
        let h = spawn_sender_full(
            transport.clone(),
            1,
            millis(1),
            0,
            secs(30),
            Acks::All,
            BatchMode::Transactional,
        );

        let ack = produce_burst(&h, "t", 0, 1).await.pop().expect("ack");
        let error = tokio::time::timeout(Duration::from_secs(1), ack)
            .await
            .expect("ack resolves")
            .expect("sender remains")
            .expect_err("the exhausted batch must fail");

        // NOT_LEADER_OR_FOLLOWER (6) is the code the mock injected; the
        // records get it, not a synthesized error.
        assert2::assert!(matches!(error, ProducerError::Server(6)), "{error:?}");
        assert2::assert!(h.state.load(Ordering::Acquire) == STATE_ACTIVE);
        assert2::assert!(h.txn_abortable_error.get() == Some(AbortableError::Server(6)));

        // The producer itself is not fenced: a later send from a fresh
        // partition still goes through the sender and gets acknowledged.
        let next = produce_burst(&h, "t", 1, 1).await.pop().expect("next ack");
        let metadata = tokio::time::timeout(Duration::from_secs(1), next)
            .await
            .expect("next ack resolves")
            .expect("sender remains")
            .expect("the sender keeps accepting sends after the abortable error");
        assert2::assert!(metadata.partition == 1);
        shutdown(h).await;
    }

    /// The same rule for a batch that reached the delivery timeout with no
    /// broker code at all: the abortable slot stores that the transaction
    /// timed out, since there is no code to report.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_transactional_batch_that_times_out_sets_the_abortable_timeout() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject_code_once(0, test_codes::NOT_LEADER_OR_FOLLOWER);
        let h = spawn_sender_full(
            transport.clone(),
            1,
            millis(1),
            i32::MAX,
            millis(1),
            Acks::All,
            BatchMode::Transactional,
        );

        let ack = produce_burst(&h, "t", 0, 1).await.pop().expect("ack");
        let error = tokio::time::timeout(Duration::from_secs(1), ack)
            .await
            .expect("ack resolves")
            .expect("sender remains")
            .expect_err("the expired batch must fail");

        assert2::assert!(matches!(error, ProducerError::SendTimeout), "{error:?}");
        assert2::assert!(h.state.load(Ordering::Acquire) == STATE_ACTIVE);
        assert2::assert!(h.txn_abortable_error.get() == Some(AbortableError::Timeout));
        shutdown(h).await;
    }

    /// Kafka's `Sender.completeBatch` splits a batch of more than one record
    /// that gets `MESSAGE_TOO_LARGE` and sends the parts again
    /// (`RecordAccumulator.splitAndReenqueue`). The parts take the sequences
    /// of the failed batch, so the partition keeps a gap-free sequence.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn message_too_large_splits_the_batch_and_sends_the_parts() {
        const MESSAGE_TOO_LARGE: i16 = 10;
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject_code_once(0, MESSAGE_TOO_LARGE);
        let h = spawn_sender(transport.clone(), 5);

        let mut offsets = Vec::new();
        for (index, rx) in produce_single_batch(&h, "t", 0, 2)
            .await
            .into_iter()
            .enumerate()
        {
            let metadata = tokio::time::timeout(Duration::from_secs(5), rx)
                .await
                .unwrap_or_else(|_| panic!("record {index} never resolved"))
                .expect("sender remains")
                .expect("the split parts are acknowledged");
            offsets.push(metadata.offset);
        }

        // The whole batch at sequence 0, then the two parts at sequences 0 and
        // 1, at the same epoch. The epoch does not change, and the producer
        // stays active.
        assert2::assert!(transport.sent_batches() == vec![(0, 0), (0, 0), (0, 1)]);
        assert2::assert!(h.producer_epoch.load(Ordering::Acquire) == 0);
        assert2::assert!(h.state.load(Ordering::Acquire) == STATE_ACTIVE);
        assert2::assert!(offsets.len() == 2);

        shutdown(h).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_exhaustion_preserves_concurrent_successful_ack() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(0);
        transport.add_known_broker(1);
        transport.fail_once_on_leader(0);
        transport.delay_leader(1, Duration::from_millis(20));
        let h = spawn_sender_with_retries(transport.clone(), 2, secs(1), 0);
        h.partition_leaders.insert(("t".to_owned(), 0), 0);
        h.partition_leaders.insert(("t".to_owned(), 1), 1);

        let mut failed = produce_single_batch_without_wake(&h, "t", 0, 1).await;
        let mut accepted = produce_single_batch_without_wake(&h, "t", 1, 1).await;
        let _ = h.wake_tx.try_send(DrainIntent::Force);

        let failed_error = tokio::time::timeout(Duration::from_secs(1), failed.remove(0))
            .await
            .expect("failed ack resolves")
            .expect("failed sender remains")
            .expect_err("the exhausted partition must fail");
        let accepted_metadata = tokio::time::timeout(Duration::from_secs(1), accepted.remove(0))
            .await
            .expect("accepted ack resolves")
            .expect("accepted sender remains")
            .expect("broker-accepted partition must remain acknowledged");

        // A transport loss carries no broker code, so the records get the
        // timeout error that Kafka's expiry path raises.
        assert2::assert!(
            matches!(failed_error, ProducerError::SendTimeout),
            "{failed_error:?}"
        );
        assert2::assert!((accepted_metadata.partition, accepted_metadata.offset) == (1, 0));
        assert2::assert!(h.state.load(Ordering::Acquire) == STATE_ACTIVE);
        assert2::assert!(transport.send_count() == 2);
        shutdown(h).await;
    }

    /// Kafka's `Sender.sendProducerData` fails an expired batch with a
    /// timeout and raises the epoch of an idempotent producer. It does not
    /// fence.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reached_delivery_timeout_fails_the_batch_and_bumps_the_epoch() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject_code_once(0, test_codes::NOT_LEADER_OR_FOLLOWER);
        let h = spawn_sender_with_policy(transport.clone(), 1, millis(1), i32::MAX, millis(1));

        let ack = produce_burst(&h, "t", 0, 1).await.pop().expect("ack");
        let error = tokio::time::timeout(Duration::from_secs(1), ack)
            .await
            .expect("ack resolves")
            .expect("sender remains")
            .expect_err("expired batch must fail");

        assert2::assert!(matches!(error, ProducerError::SendTimeout), "{error:?}");
        assert2::assert!(h.state.load(Ordering::Acquire) == STATE_ACTIVE);
        assert2::assert!(h.producer_epoch.load(Ordering::Acquire) == 1);

        let next = produce_burst(&h, "t", 0, 1).await.pop().expect("next ack");
        let metadata = tokio::time::timeout(Duration::from_secs(1), next)
            .await
            .expect("next ack resolves")
            .expect("sender remains")
            .expect("the producer still sends after the expired batch");
        assert2::assert!(metadata.partition == 0);
        shutdown(h).await;
    }

    /// Kafka's `RecordAccumulator.expiredBatches` and
    /// `ProducerBatch.hasReachedDeliveryTimeout` measure the delivery timeout
    /// from the creation of the batch. A batch that waits in the accumulator
    /// behind a retrying batch of the same partition expires with it, before
    /// its first send.
    #[tokio::test(start_paused = true)]
    async fn delivery_timeout_counts_from_batch_creation() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.fail_next(usize::MAX);
        let h = spawn_sender_policy(
            transport.clone(),
            HarnessPolicy {
                retry_backoff: millis(100),
                retry_backoff_max: millis(100),
                delivery_timeout: millis(300),
                ..HarnessPolicy::default()
            },
        );
        let start = Instant::now();

        let mut elapsed = Vec::new();
        for ack in produce_burst(&h, "t", 0, 2).await {
            let result = ack.await.expect("sender remains");
            let waited = Instant::now().duration_since(start);
            elapsed.push((
                matches!(result, Err(ProducerError::SendTimeout)),
                waited.as_millis(),
            ));
        }

        assert2::assert!(elapsed == vec![(true, 300), (true, 300)]);
        shutdown(h).await;
    }

    /// Kafka's `RecordAccumulator` waits
    /// `ExponentialBackoff.backoff(attempts - 1)` before a retry:
    /// `retry.backoff.ms * 2^attempts`, with a random factor from 0.8 to 1.2,
    /// and never more than `retry.backoff.max.ms`.
    #[tokio::test(start_paused = true)]
    async fn produce_retries_back_off_exponentially_with_jitter() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.fail_next(5);
        let h = spawn_sender_policy(
            transport.clone(),
            HarnessPolicy {
                retry_backoff: millis(100),
                retry_backoff_max: secs(1),
                ..HarnessPolicy::default()
            },
        );

        let ack = produce_burst(&h, "t", 0, 1).await.pop().expect("ack");
        ack.await
            .expect("sender remains")
            .expect("the sixth send is acknowledged");

        let sent_at = transport.sent_at();
        let gaps = sent_at
            .windows(2)
            .map(|pair| pair[1].duration_since(pair[0]).as_millis())
            .collect::<Vec<_>>();
        let bounds = [(80, 120), (160, 240), (320, 480), (640, 960), (800, 1000)];
        let outside = gaps
            .iter()
            .zip(bounds)
            .filter(|(gap, (low, high))| !(*low..=*high).contains(*gap))
            .collect::<Vec<_>>();
        assert2::assert!((gaps.len(), outside) == (5, Vec::new()), "gaps: {gaps:?}");
        shutdown(h).await;
    }

    /// Build a one-record idempotent `PreparedBatch` directly, bypassing the
    /// sender loop, so `expire_batches` can be exercised as a plain function.
    fn idempotent_batch(
        topic: &str,
        partition: i32,
        base_sequence: i32,
        producer_epoch: i16,
    ) -> (
        PreparedBatch,
        oneshot::Receiver<Result<RecordMetadata, ProducerError>>,
    ) {
        let (tx, rx) = oneshot::channel();
        let record = PendingRecord {
            offset_delta: 0,
            timestamp_ms: 0,
            key: None,
            value: None,
            headers: Vec::new(),
            ack: tx,
        };
        let pb = PreparedBatch {
            topic: topic.to_string(),
            partition,
            topic_id: Uuid::ZERO,
            base_sequence,
            record_batch: RecordBatch {
                base_offset: 0,
                partition_leader_epoch: 0,
                attributes: Attributes::default(),
                last_offset_delta: 0,
                base_timestamp: 0,
                max_timestamp: 0,
                producer_id: 1,
                producer_epoch,
                base_sequence,
                records: Vec::new(),
            },
            records: vec![record],
            created_at: Instant::now(),
            backoff_until: None,
            retries_used: 0,
            backoff_attempts: 0,
            last_failure: None,
            transaction_generation: None,
            _memory: None,
        };
        (pb, rx)
    }

    /// `expire_batches` must not rewrite the epoch, sequence, or bytes of a
    /// batch that did not itself reach the delivery timeout. Kafka's
    /// `maybeUpdateProducerIdAndEpoch` moves a partition to the new identity
    /// only once it has no batch in flight, so an unrelated parked batch keeps
    /// its original identity: a broker that already durably wrote it must
    /// still dedup a resend with `DUPLICATE_SEQUENCE_NUMBER`, which a rewrite
    /// at a new epoch would defeat by making the resend look like a new batch.
    ///
    /// This is a plain, synchronous call into `expire_batches` (no sender loop,
    /// no timing), so the two-partition interleaving that
    /// `retry_exhaustion_preserves_concurrent_successful_ack` cannot pin
    /// deterministically is exact and immediate here.
    #[test]
    fn expire_batches_does_not_rewrite_an_unrelated_parked_batch() {
        let transport = MockTransport::new(Duration::ZERO);
        let (_wake_tx, wake_rx) = tokio::sync::mpsc::channel(1);
        let cfg = SenderConfig {
            transport: Box::new(ArcTransport(transport)),
            producer_id: 1,
            producer_epoch: Arc::new(AtomicI16::new(3)),
            acks: Acks::All,
            compression: Compression::None,
            linger: millis(1),
            request_timeout_ms: 5_000,
            retries: i32::MAX,
            retry_backoff: RetryBackoff::new(Duration::from_millis(1), Duration::from_secs(1)),
            delivery_timeout: secs(30),
            max_in_flight: 5,
            metadata_cache: Arc::new(Mutex::new(HashMap::new())),
            partition_leaders: Arc::new(DashMap::new()),
            partitioner: Arc::new(BuiltInPartitioner::new(PartitionerConfig::default())),
            accumulators: Arc::new(DashMap::new()),
            next_seq: Arc::new(DashMap::new()),
            state: Arc::new(AtomicU8::new(STATE_ACTIVE)),
            wake_rx,
            flush_notify: Arc::new(Notify::new()),
            in_flight: Arc::new(AtomicUsize::new(2)),
            shutdown: CancellationToken::new(),
            transactional_id: None,
            txn_state: Arc::new(Mutex::new(TxnState::Uninitialized)),
            txn_pid_epoch: Arc::new(Mutex::new((1, 0))),
            txn_recovery_required: Arc::new(AtomicBool::new(false)),
            txn_recovery_generation: Arc::new(AtomicU64::new(0)),
            txn_abortable_error: Arc::new(AbortableErrorSlot::default()),
        };
        let mut state = PipelineState::default();

        // Partition 0's batch reached the delivery timeout; it is the one
        // passed as `expired`.
        let (expired_batch, mut expired_rx) = idempotent_batch("t", 0, 0, 3);

        // Partition 1's batch is unrelated: still within budget, mid-retry,
        // passed as `to_send`. Its epoch (3) and sequence (16) must survive.
        let (parked_batch, _parked_rx) = idempotent_batch("t", 1, 16, 3);
        let original_record_batch = parked_batch.record_batch.clone();

        expire_batches(&cfg, &mut state, vec![expired_batch], vec![parked_batch]);

        // The epoch bump happened (it applies to batches built after this
        // point)...
        assert2::assert!(cfg.producer_epoch.load(Ordering::Acquire) == 4);
        // ...but partition 1's already-built batch was parked byte-identical:
        // same epoch, same sequence, same encoded bytes as before the call.
        let reparked = state
            .retry
            .get(&("t".to_string(), 1))
            .expect("partition 1 stays parked, not sent as a fresh batch");
        assert2::assert!(reparked.record_batch == original_record_batch);
        assert2::assert!(reparked.base_sequence == 16);

        // Partition 0's record failed with the expiry error, not silently
        // rewritten and resent.
        let expired_result = expired_rx
            .try_recv()
            .expect("partition 0 is resolved, not parked");
        assert2::assert!(matches!(expired_result, Err(ProducerError::SendTimeout)));
    }

    /// A batch with several records gives each record
    /// `base_offset + offset_delta`. The other tests use one record per batch,
    /// where `offset_delta` is always 0. This test pins the per-record offset
    /// arithmetic.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multi_record_batch_offsets_use_base_plus_delta() {
        const N: usize = 4;
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender(transport.clone(), 5);

        let rxs = produce_single_batch(&h, "t", 0, N).await;
        let mut offsets = Vec::with_capacity(N);
        for (i, rx) in rxs.into_iter().enumerate() {
            let md = tokio::time::timeout(Duration::from_secs(10), rx)
                .await
                .unwrap_or_else(|_| panic!("record {i} never resolved"))
                .expect("oneshot dropped")
                .expect("acked Ok");
            assert2::assert!(md.partition == 0);
            offsets.push(md.offset);
        }
        // One batch at base_offset 0, records at deltas 0..N-1 → offsets 0,1,2,3.
        // Under `base_offset - offset_delta` these would be 0,-1,-2,-3.
        let expected: Vec<i64> = (0..i64::try_from(N).unwrap()).collect();
        assert2::assert!(offsets == expected);

        shutdown(h).await;
    }

    /// A transport failure to a *known* leader evicts that broker's connection,
    /// so a reconnect targets its current address. The batch then resends and
    /// acks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transport_error_evicts_known_leader() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(5);
        transport.fail_once_on(0);
        let h = spawn_sender(transport.clone(), 5);
        h.partition_leaders.insert(("t".to_string(), 0), 5);

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved")
            .expect("oneshot dropped")
            .expect("acked Ok after recovery");

        assert2::assert!(h.transport.evicted().contains(&5));

        shutdown(h).await;
    }

    /// On `NOT_LEADER_OR_FOLLOWER` with an inline `current_leader` hint to a
    /// *known* broker, the sender adopts the hint WITHOUT a metadata refresh. It
    /// routes the resend there and updates its leader cache.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn not_leader_adopts_known_inline_hint_without_refresh() {
        const NOT_LEADER_OR_FOLLOWER: i16 = 6;
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(5);
        transport.add_known_broker(8);
        transport.inject(Inject {
            seq: 0,
            name: None,
            topic_id: None,
            error_code: NOT_LEADER_OR_FOLLOWER,
            base_offset: -1,
            leader_hint: 8,
            log_start_offset: -1,
            forget_producer_state: false,
        });
        let h = spawn_sender(transport.clone(), 5);
        h.partition_leaders.insert(("t".to_string(), 0), 5);

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved")
            .expect("oneshot dropped")
            .expect("acked Ok after re-route");

        let leaders = h.transport.sent_leaders();
        check!(
            (
                leaders.contains(&Some(5)),
                leaders.contains(&Some(8)),
                h.partition_leaders
                    .get(&("t".to_string(), 0))
                    .map(|e| *e.value()),
                h.transport.refresh_count(),
            ) == (true, true, Some(8), 0),
            "inline hint must reroute, update the cache, and avoid metadata refresh: {leaders:?}"
        );

        shutdown(h).await;
    }

    /// The sender correlates a Produce response to its batch by `topic_id` when
    /// the response's topic NAME differs, because Kafka v13+ omits the name. The
    /// injected response carries the matching `topic_id` and a distinctive
    /// offset.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn correlates_response_by_topic_id_when_name_differs() {
        let topic_id = Uuid([7u8; 16]);
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject(Inject {
            seq: 0,
            name: Some(String::new()), // name does NOT match "t"
            topic_id: Some(topic_id),  // but topic_id does
            error_code: codes::NONE,
            base_offset: 42,
            leader_hint: -1,
            log_start_offset: -1,
            forget_producer_state: false,
        });
        let h = spawn_sender(transport.clone(), 5);
        // Give "t" a non-zero topic_id so the batch carries it.
        h.metadata_cache.lock().await.insert(
            "t".to_string(),
            TopicMetadata {
                num_partitions: 1,
                topic_id,
            },
        );

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let md = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved")
            .expect("oneshot dropped")
            .expect("acked Ok via topic_id correlation");
        // The injected response's base_offset (42) proves the sender matched by
        // topic_id; failing to correlate would resend and ack at the broker's 0.
        assert2::assert!(md.offset == 42);

        shutdown(h).await;
    }

    /// `&&`, not `||`, gates the `topic_id` fallback. A response whose name
    /// does NOT match, and whose `topic_id` is ZERO, must NOT be correlated. The
    /// batch has no `topic_id`, that is ZERO, so only an exact name match binds
    /// a response, and a wrong-name response forces a resend.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn does_not_correlate_mismatched_name_with_zero_topic_id() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.inject(Inject {
            seq: 0,
            name: Some("other".to_string()), // wrong name
            topic_id: Some(Uuid::ZERO),      // zero topic_id
            error_code: codes::NONE,
            base_offset: 99, // a bogus offset that must NOT be adopted
            leader_hint: -1,
            log_start_offset: -1,
            forget_producer_state: false,
        });
        let h = spawn_sender(transport.clone(), 5);
        // No metadata → the batch's topic_id is ZERO.

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let md = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved")
            .expect("oneshot dropped")
            .expect("acked Ok after resend");
        // Correct code ignores the mismatched response and resends, acking at the
        // broker's real offset 0 — never the bogus 99 the wrong response carried.
        assert2::assert!(md.offset == 0);

        shutdown(h).await;
    }

    /// `update_leaders_from_metadata` adopts leaders only from HEALTHY topics,
    /// that is `error_code == 0`. A transport error triggers a refresh whose
    /// response advertises a new leader for a healthy topic, and the cache picks
    /// it up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_adopts_leader_for_healthy_topic() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.add_known_broker(9);
        transport.fail_once_on(0); // transport error → forces a metadata refresh
        transport.set_refresh_response(MetadataResponse {
            topics: vec![MetadataResponseTopic {
                error_code: 0,
                name: Some("t".to_string()),
                partitions: vec![MetadataResponsePartition {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 9,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
        let h = spawn_sender(transport.clone(), 5);

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved")
            .expect("oneshot dropped")
            .expect("acked Ok after refresh + resend");

        assert2::assert!(
            h.partition_leaders
                .get(&("t".to_string(), 0))
                .map(|e| *e.value())
                == Some(9)
        );

        shutdown(h).await;
    }

    /// The Produce request carries the configured `request_timeout` as
    /// `timeout_ms`, so 5s becomes 5000ms on the wire.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn request_carries_configured_timeout() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender(transport.clone(), 5);

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("record never resolved")
            .expect("oneshot dropped")
            .expect("acked Ok");

        assert2::assert!(h.transport.last_timeout_ms() == 5000);

        shutdown(h).await;
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_fails_accumulator_batches_even_behind_same_partition_retry() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.fail_next(1);
        let h = spawn_sender_with(transport.clone(), 1, minutes(1));
        let retry_receiver = produce_ready_batches_without_wake(&h, "t", 0, 1)
            .await
            .remove(0);
        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");
        while transport.send_count() < 1 {
            tokio::task::yield_now().await;
        }

        let accumulator = h
            .accumulators
            .get(&("t".to_owned(), 0))
            .expect("accumulator exists")
            .value()
            .clone();
        let (ready_receiver, current_receiver) = {
            let mut accumulator = accumulator.lock().await;
            let crate::accumulator::AppendResult {
                receiver: ready_receiver,
                ..
            } = accumulator.try_append(
                None,
                Some(bytes::Bytes::from_static(b"old-ready")),
                vec![],
                0,
                Some(0),
            );
            accumulator.seal_current();
            let crate::accumulator::AppendResult {
                receiver: current_receiver,
                ..
            } = accumulator.try_append(
                None,
                Some(bytes::Bytes::from_static(b"old-current")),
                vec![],
                0,
                Some(0),
            );
            (ready_receiver, current_receiver)
        };

        h.recovery_generation.store(1, Ordering::Release);
        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");
        for receiver in [ready_receiver, current_receiver] {
            let result = receiver
                .await
                .expect("recovery acknowledgement channel remains connected");
            assert!(matches!(result, Err(ProducerError::RecoveryRequired)));
        }
        assert_eq!(
            transport.send_count(),
            1,
            "old transactional accumulator batches must fail before retry release"
        );
        assert_eq!(
            h.in_flight.load(Ordering::Acquire),
            1,
            "undrained batches must not decrement the retry's in-flight slot"
        );

        tokio::time::advance(Duration::from_millis(1)).await;
        retry_receiver
            .await
            .expect("retry acknowledgement channel remains connected")
            .expect("nontransactional retry is acknowledged");
        shutdown(h).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_transactional_batch_is_failed_after_recovery_before_reinitialization() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 1, secs(30));
        let accumulator = Arc::new(Mutex::new(Accumulator::new(1024)));
        h.accumulators
            .insert(("t".to_string(), 0), Arc::clone(&accumulator));
        let crate::accumulator::AppendResult { receiver: rx, .. } =
            accumulator.lock().await.try_append(
                None,
                Some(bytes::Bytes::from_static(b"old")),
                vec![],
                0,
                Some(0),
            );

        h.recovery_required.store(true, Ordering::Release);
        h.recovery_generation.store(1, Ordering::Release);
        // Simulate a completed InitProducerId before the sender gets to drain:
        // the old generation must still be rejected under the new epoch.
        h.recovery_required.store(false, Ordering::Release);
        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");

        let acknowledgement = tokio::time::timeout(Duration::from_secs(3), rx)
            .await
            .expect("recovery must resolve queued acknowledgement")
            .expect("acknowledgement channel remains connected");
        assert!(matches!(
            acknowledgement,
            Err(ProducerError::RecoveryRequired)
        ));
        assert_eq!(transport.applied.load(Ordering::Acquire), 0);

        shutdown(h).await;
    }

    /// A transport-failed transactional batch occupies the retry slot. Once
    /// reinitialization advances the recovery generation, that slot must fail
    /// locally instead of resending a batch from the prior transaction epoch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_slot_transactional_batch_is_failed_after_recovery_without_resend() {
        let transport = MockTransport::new(Duration::ZERO);
        transport.fail_once_on(0);
        let h = spawn_sender_with(transport.clone(), 1, secs(30));
        let accumulator = Arc::new(Mutex::new(Accumulator::new(1024)));
        h.accumulators
            .insert(("t".to_string(), 0), Arc::clone(&accumulator));
        let crate::accumulator::AppendResult { receiver: rx, .. } =
            accumulator.lock().await.try_append(
                None,
                Some(bytes::Bytes::from_static(b"old")),
                vec![],
                0,
                Some(0),
            );

        let initial_send = transport.send_started.notified();
        h.wake_tx
            .send(DrainIntent::Force)
            .await
            .expect("sender is running");
        tokio::time::timeout(Duration::from_secs(3), initial_send)
            .await
            .expect("transactional batch should reach the controlled transport failure");
        assert_eq!(
            transport.send_count(),
            1,
            "initial send must fail exactly once"
        );

        // Mirror successful reinitialization: the epoch generation advances and
        // the recovery barrier is lifted before the sender examines its retry slot.
        h.recovery_required.store(true, Ordering::Release);
        h.recovery_generation.store(1, Ordering::Release);
        h.recovery_required.store(false, Ordering::Release);
        h.wake_tx
            .send(DrainIntent::Ready)
            .await
            .expect("sender is running");

        let acknowledgement = tokio::time::timeout(Duration::from_secs(3), rx)
            .await
            .expect("recovery must resolve retry-slot acknowledgement")
            .expect("acknowledgement channel remains connected");
        assert!(matches!(
            acknowledgement,
            Err(ProducerError::RecoveryRequired)
        ));
        assert_eq!(
            transport.send_count(),
            1,
            "a retry-slot batch from the old generation must not resend"
        );

        shutdown(h).await;
    }

    /// `finish_in_flight` notifies flush waiters exactly when `in_flight`
    /// reaches zero. With a long linger, wakes trigger the only early drains, so
    /// this notify is the only one a registered waiter can receive.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finish_in_flight_notifies_when_drained() {
        let transport = MockTransport::new(Duration::ZERO);
        let h = spawn_sender_with(transport.clone(), 5, secs(30));

        let flush = Arc::clone(&h.flush_notify);
        // Register the flush waiter synchronously: a `Notified` future only
        // registers once enabled/polled, and `notify_waiters` wakes only
        // already-registered waiters, so `enable()` removes the registration
        // race deterministically (no settle needed). From here only
        // finish_in_flight can notify it.
        let watcher = flush.notified();
        tokio::pin!(watcher);
        watcher.as_mut().enable();

        let rx = produce_burst(&h, "t", 0, 1).await.pop().expect("one rx");
        let fired = tokio::time::timeout(Duration::from_secs(3), watcher).await;
        assert2::assert!(fired.is_ok());

        let _ = tokio::time::timeout(Duration::from_secs(2), rx).await;
        shutdown(h).await;
    }
}
