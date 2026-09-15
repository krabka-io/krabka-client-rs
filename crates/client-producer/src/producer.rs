//! The public `Producer` type. The builder lives in `builder.rs`, and the
//! sender task lives in `sender.rs`.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI16, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::BufMut;
use dashmap::DashMap;
use krabka_client_consumer::ConsumerGroupMetadata;
use krabka_client_core::{
    Client, ClientError, ClientFrameMax, ConnectionDispatchQueueCapacity, security::ClientSecurity,
};
use krabka_protocol::{
    Encode, ProtocolError, ProtocolRequest,
    owned::{
        add_offsets_to_txn_request::AddOffsetsToTxnRequest,
        add_partitions_to_txn_request::{self, AddPartitionsToTxnRequest},
        add_partitions_to_txn_response::AddPartitionsToTxnResponse,
        common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
        end_txn_request::EndTxnRequest,
        find_coordinator_request::FindCoordinatorRequest,
        init_producer_id_request::InitProducerIdRequest,
        init_producer_id_response::InitProducerIdResponse,
        txn_offset_commit_request::{
            TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
        },
        txn_offset_commit_response::TxnOffsetCommitResponse,
    },
};
use krabka_units::{Time, convert::TimeExt};
use tokio::{
    sync::{Mutex, Notify, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::{
    accumulator::{
        Accumulator, AccumulatorMap, AppendResult, approx_record_size, record_size_upper_bound,
    },
    buffer_pool::BufferPool,
    builder::{ProducerFlushTimeout, send_init_producer_id},
    compression::Compression,
    error::{ProducerError, RecordSizeLimit},
    metadata_wait::{MetadataRefresh, MetadataWait, metadata_request},
    partitioner::{BuiltInPartitioner, StickyPartition, TopicPartitions},
    record::{ProducerRecord, RecordMetadata},
    sender::DrainIntent,
    transactional::{
        AbortableError, FatalError, OwnedTransaction, PreparedTransactionState, Transaction,
        TxnErrorSlot, TxnState,
    },
    txn_retry::{
        self, AddPartitionsDecision, CoordinatorAttempt, EndTxnDecision, TxnRequestDecision,
    },
};

/// The last `AddPartitionsToTxn` version that a client sends.
const ADD_PARTITIONS_LAST_CLIENT_VERSION: i16 = 3;

/// An `AddPartitionsToTxn` request from a client, capped at v3.
///
/// v4 and later carry the broker-to-broker form of the request, which a Kafka
/// broker answers only for a principal with `CLUSTER_ACTION` on the cluster.
/// Apache Kafka's client builds every such request with
/// `AddPartitionsToTxnRequest.Builder.forClient`, which allows
/// `ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion()` to `LAST_CLIENT_VERSION`
/// (3), and fills only the `v3_and_below` fields. This type gives the same cap
/// to version negotiation.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ClientAddPartitionsToTxn(AddPartitionsToTxnRequest);

impl Encode for ClientAddPartitionsToTxn {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for ClientAddPartitionsToTxn {
    const API_KEY: i16 = add_partitions_to_txn_request::API_KEY;
    const MIN_VERSION: i16 = add_partitions_to_txn_request::MIN_VERSION;
    const MAX_VERSION: i16 = ADD_PARTITIONS_LAST_CLIENT_VERSION;
    const FLEXIBLE_MIN: i16 = add_partitions_to_txn_request::FLEXIBLE_MIN;
    type Response = AddPartitionsToTxnResponse;
}

/// The deadline and the backoff of the retries of one coordinator request.
struct CoordinatorRetry {
    deadline: tokio::time::Instant,
    backoff: std::time::Duration,
    max_backoff: std::time::Duration,
}

impl CoordinatorRetry {
    /// Wait before the next attempt, and give `false` when the deadline has
    /// passed. The wait doubles up to the maximum backoff. A wait that reaches
    /// the deadline gives `false`, so no attempt starts after it.
    async fn wait(&mut self) -> bool {
        let remaining = self
            .deadline
            .saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        tokio::time::sleep(self.backoff.min(remaining)).await;
        self.backoff = self.backoff.saturating_mul(2).min(self.max_backoff);
        tokio::time::Instant::now() < self.deadline
    }
}

/// Whether a request can have reached the broker, so the producer can send it
/// again. A failure before the send, such as a version error or a codec error,
/// repeats on every attempt, so the caller reports it at once.
fn request_may_have_reached_the_broker(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Disconnected | ClientError::Timeout(_) | ClientError::Io(_)
    )
}

/// The partition error code that decides a `TxnOffsetCommit` answer.
///
/// A code that no rule retries decides the answer, wherever its row is.
/// Kafka's `TxnOffsetCommitHandler` leaves its loop at such a row, and it
/// sends the request again only when every failed row is retriable. Among
/// retriable rows, a row that asks for the coordinator again wins, because one
/// such row makes Kafka's handler look the coordinator up for the whole
/// request.
fn txn_offset_commit_error_code(response: &TxnOffsetCommitResponse) -> i16 {
    let codes = response
        .topics
        .iter()
        .flat_map(|topic| {
            topic
                .partitions
                .iter()
                .map(|partition| partition.error_code)
        })
        .filter(|code| *code != 0);
    let mut rediscover = None;
    let mut resend = None;
    for code in codes {
        match txn_retry::decide_txn_offset_commit(CoordinatorAttempt::Answered(code)) {
            TxnRequestDecision::Retry { rediscover: true } => {
                rediscover.get_or_insert(code);
            }
            TxnRequestDecision::Retry { rediscover: false } => {
                resend.get_or_insert(code);
            }
            _ => return code,
        }
    }
    rediscover.or(resend).unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    Zero,
    One,
    All,
}

impl Acks {
    #[must_use]
    pub fn wire(self) -> i16 {
        match self {
            Acks::Zero => 0,
            Acks::One => 1,
            Acks::All => -1,
        }
    }
}

/// Tri-state lifecycle.
pub(crate) const STATE_ACTIVE: u8 = 0;
pub(crate) const STATE_FENCED: u8 = 1;
pub(crate) const STATE_CLOSED: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopicMetadata {
    pub num_partitions: i32,
    /// Topic UUID. Produce v13+ needs it, because that version encodes only
    /// the `topic_id` on the wire. Zero, `Uuid::ZERO`, is a valid sentinel that
    /// means "not yet known". For older wire versions the broker falls back to
    /// the `name` field.
    pub topic_id: krabka_protocol::primitives::uuid::Uuid,
}

#[derive(Clone, Debug)]
pub(crate) struct ProducerIdentity {
    pub id: i64,
    /// The idempotent producer epoch. The sender task shares it, and it raises
    /// the epoch when Kafka's idempotent producer does.
    pub epoch: Arc<AtomicI16>,
}

fn wake_sender_after_append(
    wake_tx: &tokio::sync::mpsc::Sender<DrainIntent>,
    linger: Time,
    wakes_sender: bool,
) {
    if linger == <Time as TimeExt>::ZERO {
        let _ = wake_tx.try_send(DrainIntent::Force);
    } else if wakes_sender {
        let _ = wake_tx.try_send(DrainIntent::Ready);
    }
}

// accumulators map is inherently complex
pub struct Producer {
    pub(crate) client: Client,
    pub(crate) client_id: String,
    /// TLS and SASL security policy used for the bootstrap connection.
    ///
    /// The producer retains it so that every secondary connection it opens
    /// after construction carries the same credentials. Those are the
    /// transaction-coordinator and group-coordinator dials in the transactional
    /// path. Without it, those connections would be plaintext and
    /// unauthenticated, a secured listener would drop them, and the
    /// transactional flow would fail with `Client(Disconnected)`.
    pub(crate) security: Option<ClientSecurity>,
    pub(crate) dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    pub(crate) frame_max: ClientFrameMax,
    pub(crate) identity: ProducerIdentity,
    // The following config knobs are also copied into `SenderConfig` at
    // construction time. They live on `Producer` for diagnostic
    // introspection and to support future reconnect / re-init flows.
    // Suppressing the dead-code warning is honest about
    // their current role.
    #[allow(dead_code)]
    pub(crate) acks: Acks,
    pub(crate) compression: Compression,
    pub(crate) batch_size: usize,
    #[allow(dead_code)]
    pub(crate) linger: Time,
    #[allow(dead_code)]
    pub(crate) request_timeout: Time,
    pub(crate) flush_timeout: ProducerFlushTimeout,
    /// The longest time that `send` waits for the metadata of its topic.
    /// Kafka's `max.block.ms` gives the same limit to `waitOnMetadata`.
    pub(crate) max_block: Duration,
    /// The buffer memory of the batches. `send` waits for it, as Kafka's
    /// `RecordAccumulator.append` waits for its `BufferPool`.
    pub(crate) buffer_pool: BufferPool,
    /// The largest serialized record that `send` accepts, in bytes. Kafka's
    /// `max.request.size`.
    pub(crate) max_request_size: usize,
    #[allow(dead_code)]
    pub(crate) max_in_flight: usize,
    pub(crate) metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>>,
    /// The metadata refreshes that concurrent sends share while they wait for
    /// a topic. See `MetadataWait`.
    pub(crate) metadata_refresh: MetadataRefresh,
    /// Per-`(topic, partition)` leader-id cache. The sender uses it to route
    /// each Produce to the broker that actually leads the partition.
    ///
    /// `Metadata` fills it alongside the partition count; see `partition_count`.
    /// A missing entry means the leader is unknown, and the sender falls back
    /// to the bootstrap connection. A leader id `< 0` also counts as unknown.
    /// An `Arc` shares the cache with the sender task.
    pub(crate) partition_leaders: Arc<DashMap<(String, i32), i32>>,
    pub(crate) accumulators: AccumulatorMap,
    pub(crate) next_seq: Arc<DashMap<(String, i32), i32>>,
    pub(crate) partitioner: Arc<BuiltInPartitioner>,
    pub(crate) state: Arc<AtomicU8>,
    pub(crate) wake_tx: tokio::sync::mpsc::Sender<DrainIntent>,
    pub(crate) flush_notify: Arc<Notify>,
    /// Count of batches the sender has popped from an accumulator but has not
    /// yet finished sending, that is, the Produce is in flight and awaits the
    /// broker ack.
    ///
    /// A batch that has left the accumulator but is still in flight is
    /// invisible to `all_empty`, so `flush` must also wait for this count to
    /// reach zero. Otherwise `commit_transaction` can race ahead of the Produce
    /// that drives the txn to `Ongoing`, and the coordinator rejects `EndTxn`
    /// with `INVALID_TXN_STATE`.
    pub(crate) in_flight: Arc<AtomicUsize>,
    pub(crate) sender_shutdown: CancellationToken,
    pub(crate) sender_handle: Option<JoinHandle<()>>,
    pub(crate) transactional_id: Option<String>,
    pub(crate) transaction_timeout_ms: i32,
    pub(crate) two_phase_commit_enabled: bool,
    pub(crate) init_retry_timeout: Time,
    pub(crate) init_retry_backoff: Time,
    pub(crate) retry_backoff_max: Time,
    /// An `Arc` wraps it, so the sender task can share the same state without
    /// more synchronization structures.
    pub(crate) txn_state: Arc<Mutex<TxnState>>,
    /// Set synchronously when an unresolved transaction guard is dropped, or
    /// when `EndTxn` loses its response. This is separate from `txn_state`,
    /// because `Drop` cannot await its async mutex.
    pub(crate) txn_recovery_required: Arc<AtomicBool>,
    pub(crate) txn_recovery_generation: Arc<AtomicU64>,
    /// Odd values identify a live transaction guard; resolving or abandoning
    /// that guard advances the counter to the following even value. Keeping
    /// the generation on each guard prevents an old prepared guard from
    /// poisoning a later transaction when it is eventually dropped.
    pub(crate) txn_guard_generation: Arc<AtomicU64>,
    /// Cached connection to the transaction coordinator broker.
    /// `init_transactions` fills it, and begin, commit and abort reuse it.
    pub(crate) txn_coord_client: Mutex<Option<Client>>,
    /// Authoritative `(producer_id, producer_epoch)` for the transactional
    /// flow. `init_transactions` sets it, and the sender reads it when it
    /// builds transactional `ProduceRequest`s.
    pub(crate) txn_pid_epoch: Arc<Mutex<(i64, i16)>>,
    /// The error state of the transaction.
    ///
    /// The sender sets the abortable error when a transactional batch fails,
    /// and a coordinator answer of an abortable code sets it too. `send`,
    /// `prepare_transaction`, `commit` and `send_offsets_to_transaction` then
    /// fail until the application aborts the transaction. A fence or a fatal
    /// coordinator code sets the fatal error, and every later transactional
    /// operation fails. Kafka's `TransactionManager` keeps the same states in
    /// `ABORTABLE_ERROR` and `FATAL_ERROR` with `lastError`.
    pub(crate) txn_error: Arc<TxnErrorSlot>,
    /// Identity of the transaction that was prepared locally or recovered via
    /// `InitProducerId(keepPreparedTxn=true)`. Recovery deliberately keeps this
    /// separate from `txn_pid_epoch`, which is the newly staged identity used
    /// to send `EndTxn`.
    pub(crate) prepared_transaction_state: Arc<Mutex<Option<PreparedTransactionState>>>,
}

/// Reads a minted transactional identity out of the stored pair.
///
/// The coordinator has minted a pair only when both halves are non-negative.
/// `InitProducerId` stores `(-1, -1)` before the first call, and Kafka writes
/// `-1` in either half for "no producer", so both halves are checked. A check
/// of one half alone would report a half-written pair as a real identity, and
/// a lease written under it would carry an epoch no broker can fence on.
fn minted_identity(pair: (i64, i16)) -> Option<(i64, i16)> {
    let (id, epoch) = pair;
    (id >= 0 && epoch >= 0).then_some(pair)
}

/// Read the error code of the one partition in an `AddPartitionsToTxn`
/// response.
///
/// Versions 0 to 3 carry the partition results in
/// `results_by_topic_v3_and_below`, and versions 4 and later carry them in
/// `results_by_transaction`. Kafka's `AddPartitionsToTxnResponse.errors` reads
/// both. A response without a partition row gives the top-level code, which
/// versions 4 and later carry.
fn add_partitions_error_code(response: &AddPartitionsToTxnResponse) -> i16 {
    response
        .results_by_topic_v3_and_below
        .first()
        .or_else(|| {
            response
                .results_by_transaction
                .first()
                .and_then(|transaction| transaction.topic_results.first())
        })
        .and_then(|topic| topic.results_by_partition.first())
        .map_or(response.error_code, |partition| {
            partition.partition_error_code
        })
}

impl Producer {
    /// Add `topic`/`partition` to the current transaction with
    /// `AddPartitionsToTxn`.
    ///
    /// A fence or a fatal code sets the fatal error in `txn_error`. It does not
    /// change `txn_state`: the send path holds that lock during this call, so
    /// the caller moves the state to [`FatalError::state`].
    pub(crate) async fn register_transaction_partition(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<(), ProducerError> {
        let Some(transactional_id) = &self.transactional_id else {
            return Ok(());
        };
        let mut coordinator = self.txn_coord_client.lock().await.clone().ok_or(
            ProducerError::InvalidTransactionState(
                "no txn coordinator cached — did init_transactions succeed?",
            ),
        )?;
        let (producer_id, producer_epoch) = *self.txn_pid_epoch.lock().await;
        let topic = AddPartitionsToTxnTopic {
            name: topic.to_owned(),
            partitions: vec![partition],
            ..Default::default()
        };
        let request = ClientAddPartitionsToTxn(AddPartitionsToTxnRequest {
            v3_and_below_transactional_id: transactional_id.clone(),
            v3_and_below_producer_id: producer_id,
            v3_and_below_producer_epoch: producer_epoch,
            v3_and_below_topics: vec![topic],
            ..Default::default()
        });
        // Adding a partition twice has no effect, so a lost request or a
        // retriable code is safe to retry. Kafka's `AddPartitionsToTxnHandler`
        // does the same.
        let mut retry = self.coordinator_retry();
        loop {
            let (attempt, last_error) = match coordinator.send(request.clone()).await {
                Ok(response) => {
                    let code = add_partitions_error_code(&response);
                    (
                        CoordinatorAttempt::Answered(code),
                        ProducerError::Server(code),
                    )
                }
                // Kafka's `Sender.runOnce` makes an authentication failure
                // fatal for every pending transactional request
                // (`TransactionManager.authenticationFailed`).
                Err(error) if error.is_authentication_failure() => {
                    return Err(ProducerError::Client(error));
                }
                Err(error) => (CoordinatorAttempt::Lost, ProducerError::Client(error)),
            };
            let rediscover = match txn_retry::decide_add_partitions(attempt) {
                AddPartitionsDecision::Added => return Ok(()),
                AddPartitionsDecision::Fenced => return Err(self.fatal_error(FatalError::Fenced)),
                AddPartitionsDecision::Abortable(code) => {
                    return Err(self.abortable_error(code));
                }
                AddPartitionsDecision::Fatal(code) => {
                    return Err(self.fatal_error(FatalError::Server(code)));
                }
                AddPartitionsDecision::Retry { rediscover } => rediscover,
            };
            if !retry.wait().await {
                return Err(last_error);
            }
            if rediscover {
                coordinator = self
                    .rediscovered_txn_coordinator(transactional_id, coordinator)
                    .await;
            }
        }
    }

    #[must_use]
    pub fn producer_id(&self) -> i64 {
        self.identity.id
    }

    #[must_use]
    pub fn producer_epoch(&self) -> i16 {
        self.identity.epoch.load(Ordering::Acquire)
    }

    /// Give the identity that the transaction coordinator minted for this
    /// producer's `transactional.id`.
    ///
    /// [`Producer::init_transactions`] sets this pair, and the broker fences a
    /// write that carries an older one. It is the fencing token of the
    /// transactional flow.
    ///
    /// This is not the pair that [`Producer::producer_id`] and
    /// [`Producer::producer_epoch`] report. Those come from the idempotent
    /// `InitProducerId` that the builder sends with no transactional id, and
    /// the coordinator never advances them.
    ///
    /// Returns `None` before `init_transactions` runs, and for a producer that
    /// carries no `transactional_id`.
    pub async fn transactional_identity(&self) -> Option<(i64, i16)> {
        minted_identity(*self.txn_pid_epoch.lock().await)
    }

    // ── Transactional API ────────────────────────────────────────────────────

    /// Begin a new transaction, returning a borrowed guard that must be
    /// finished with [`Transaction::commit`] or [`Transaction::abort`].
    ///
    /// The caller must call this after [`init_transactions`] has completed,
    /// and before any transactional [`send`] call. It transitions the producer
    /// from `Ready` to `InTransaction`.
    ///
    /// # Errors
    ///
    /// - [`ProducerError::NotTransactional`] — `transactional_id` was not set.
    /// - [`ProducerError::InvalidTransactionState`] — the producer is not in
    ///   the `Ready` state. For example, the caller has not yet called
    ///   `init_transactions`, or a transaction is already in flight.
    ///
    /// [`init_transactions`]: Self::init_transactions
    /// [`send`]: Self::send
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(transactional_id = self.transactional_id.as_deref()),
        err,
    )]
    pub async fn begin_transaction(&self) -> Result<Transaction<'_>, ProducerError> {
        let guard_generation = self.begin_transaction_state().await?;
        Ok(Transaction {
            producer: self,
            finished: false,
            guard_generation,
        })
    }

    /// Begin a new transaction, returning an owning guard that must be
    /// finished with [`OwnedTransaction::commit`] or
    /// [`OwnedTransaction::abort`].
    ///
    /// The semantics are identical to
    /// [`begin_transaction`](Self::begin_transaction), but the returned guard
    /// owns an `Arc<Producer>` instead of borrowing `&self`. Use it when the
    /// guard must survive across an owned or `'static` boundary that a borrow
    /// cannot, for example when it is stored behind a `dyn Trait` object. It
    /// mirrors `tokio::sync::Mutex::lock_owned`.
    ///
    /// # Errors
    ///
    /// Same as [`begin_transaction`](Self::begin_transaction).
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(transactional_id = self.transactional_id.as_deref()),
        err,
    )]
    pub async fn begin_transaction_owned(
        self: Arc<Self>,
    ) -> Result<OwnedTransaction, ProducerError> {
        let guard_generation = self.begin_transaction_state().await?;
        Ok(OwnedTransaction {
            producer: self,
            finished: false,
            guard_generation,
        })
    }

    async fn begin_transaction_state(&self) -> Result<u64, ProducerError> {
        if self.transactional_id.is_none() {
            return Err(ProducerError::NotTransactional);
        }
        if let Some(error) = self.fatal_error_state() {
            return Err(error);
        }
        if self.transaction_recovery_required() {
            return Err(ProducerError::RecoveryRequired);
        }
        let mut state = self.txn_state.lock().await;
        match *state {
            TxnState::Ready => {
                *state = TxnState::InTransaction;
                Ok(self
                    .txn_guard_generation
                    .fetch_add(1, Ordering::AcqRel)
                    .wrapping_add(1))
            }
            _ => Err(ProducerError::InvalidTransactionState(
                "begin_transaction must be called after init_transactions and not while another txn is in flight",
            )),
        }
    }

    /// Flushes the current transaction and locks it against further writes.
    ///
    /// The returned identity has a stable Kafka-compatible string form and can
    /// be persisted by an external transaction coordinator. After this method
    /// succeeds, only commit, abort, or [`complete_transaction`] may finish the
    /// transaction.
    ///
    /// # Errors
    ///
    /// - [`ProducerError::NotTransactional`] — `transactional_id` was not set.
    /// - [`ProducerError::InvalidTransactionState`] — 2PC is disabled or no
    ///   transaction is currently in flight.
    /// - [`ProducerError::FlushTimeout`] — pending records did not flush before
    ///   the configured deadline; the transaction remains open and can retry.
    ///
    /// [`complete_transaction`]: Self::complete_transaction
    pub async fn prepare_transaction(&self) -> Result<PreparedTransactionState, ProducerError> {
        if self.transactional_id.is_none() {
            return Err(ProducerError::NotTransactional);
        }
        if !self.two_phase_commit_enabled {
            return Err(ProducerError::InvalidTransactionState(
                "prepare_transaction requires transaction_two_phase_commit_enable=true",
            ));
        }
        if let Some(error) = self.fatal_error_state() {
            return Err(error);
        }
        if self.transaction_recovery_required() {
            return Err(ProducerError::RecoveryRequired);
        }

        {
            let mut state = self.txn_state.lock().await;
            if *state != TxnState::InTransaction {
                return Err(ProducerError::InvalidTransactionState(
                    "prepare_transaction must follow begin_transaction",
                ));
            }
            *state = TxnState::Preparing;
        }

        if let Err(error) = self.flush().await {
            *self.txn_state.lock().await = TxnState::InTransaction;
            return Err(error);
        }
        // Kafka's `KafkaProducer.prepareTransaction` flushes first, and then
        // `TransactionManager.prepareTransaction` calls `maybeFailWithError`,
        // which throws in both error states. A batch that failed during the
        // flush has set the error by now.
        if let Some(error) = self.transaction_error_state() {
            *self.txn_state.lock().await = TxnState::InTransaction;
            return Err(error);
        }

        let (producer_id, producer_epoch) = *self.txn_pid_epoch.lock().await;
        let Ok(prepared) = PreparedTransactionState::new(producer_id, producer_epoch) else {
            *self.txn_state.lock().await = TxnState::InTransaction;
            return Err(ProducerError::InvalidTransactionState(
                "transaction coordinator returned an invalid producer identity",
            ));
        };
        *self.prepared_transaction_state.lock().await = Some(prepared);
        *self.txn_state.lock().await = TxnState::Prepared;
        self.resolve_transaction_guard();
        Ok(prepared)
    }

    /// Completes the current prepared transaction.
    ///
    /// A token matching the prepared transaction commits it. A stale, empty,
    /// or otherwise different token aborts it, so recovery never commits work
    /// that belongs to another producer incarnation.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::InvalidTransactionState`] unless a local
    /// [`prepare_transaction`] or
    /// [`init_transactions_with_keep_prepared`](Self::init_transactions_with_keep_prepared)
    /// established a prepared transaction. Other failures are the same as
    /// committing or aborting a normal transaction.
    ///
    /// [`prepare_transaction`]: Self::prepare_transaction
    pub async fn complete_transaction(
        &self,
        prepared: PreparedTransactionState,
    ) -> Result<(), ProducerError> {
        if let Some(error) = self.fatal_error_state() {
            return Err(error);
        }
        if *self.txn_state.lock().await != TxnState::Prepared {
            return Err(ProducerError::InvalidTransactionState(
                "complete_transaction requires a prepared transaction",
            ));
        }
        let current = *self.prepared_transaction_state.lock().await;
        self.end_transaction(current == Some(prepared)).await
    }

    /// Finish the current transaction. This flushes all in-flight records,
    /// then sends `EndTxn(committed)` to the transaction coordinator. On
    /// success it transitions the producer from `InTransaction` to `Ready`.
    ///
    /// [`Transaction::commit`], [`Transaction::abort`],
    /// [`OwnedTransaction::commit`] and [`OwnedTransaction::abort`] call it.
    /// They are the only ways to finish a transaction opened with
    /// `begin_transaction` or `begin_transaction_owned`.
    ///
    /// # Errors
    ///
    /// - [`ProducerError::NotTransactional`]: `transactional_id` was not set.
    /// - [`ProducerError::InvalidTransactionState`]: not currently in a transaction.
    /// - [`ProducerError::FencedProducer`]: broker returned `INVALID_PRODUCER_EPOCH (47)` or `PRODUCER_FENCED (90)`, now or before.
    /// - [`ProducerError::FatalTransactionError`]: the coordinator answered with a fatal code, now or before. An abort that gets `TRANSACTION_ABORTABLE (120)` is fatal too.
    /// - [`ProducerError::ConcurrentTransactions`]: broker still returned `CONCURRENT_TRANSACTIONS (51)` when the retry deadline ended; caller may retry.
    /// - [`ProducerError::Server`]: an abortable code, or a retriable code that the broker still returned when the retry deadline ended.
    /// - [`ProducerError::RecoveryRequired`]: a request was lost in transport,
    ///   and the retries did not learn the outcome before the deadline.
    ///
    /// The producer sends `EndTxn` again after a retriable code, until the
    /// producer-ID initialization retry timeout ends.
    ///
    /// Every error except `RecoveryRequired` means that no request changed the
    /// transaction.
    ///
    /// A commit fails with the stored error when a batch of the transaction
    /// failed, or when a coordinator answered with an abortable code. The
    /// transaction stays open, and the application must abort it. Kafka's
    /// `TransactionManager` keeps the same rule: a batch failure moves it to
    /// `ABORTABLE_ERROR` (`handleFailedBatch`), and `beginCommit` then fails
    /// in `maybeFailWithError`.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(committed, error_code = tracing::field::Empty),
        err,
    )]
    pub(crate) async fn end_transaction(&self, committed: bool) -> Result<(), ProducerError> {
        let tid = self
            .transactional_id
            .clone()
            .ok_or(ProducerError::NotTransactional)?;

        // Kafka's `beginCommit` and `beginAbort` call `maybeFailWithError`,
        // and a fatal error fails both.
        if let Some(error) = self.fatal_error_state() {
            return Err(error);
        }

        // 1. Flush all in-flight records (block until acks).
        self.flush().await?;

        // 2. A failed batch, or an abortable coordinator answer, stops a
        //    commit. Only an abort can clear it.
        if committed && let Some(error) = self.abortable_error_state() {
            return Err(error);
        }

        let mut state = self.txn_state.lock().await;
        let previous_state = *state;
        if !matches!(previous_state, TxnState::InTransaction | TxnState::Prepared) {
            return Err(ProducerError::InvalidTransactionState(
                "commit/abort_transaction must follow begin_transaction",
            ));
        }
        *state = TxnState::CommittingOrAborting;
        drop(state);

        // 3. Retrieve the cached coordinator connection.
        let coord_guard = self.txn_coord_client.lock().await;
        let Some(coord) = coord_guard.as_ref().cloned() else {
            drop(coord_guard);
            *self.txn_state.lock().await = previous_state;
            return Err(ProducerError::InvalidTransactionState(
                "no txn coordinator cached — did init_transactions succeed?",
            ));
        };
        drop(coord_guard);

        let (pid, epoch) = *self.txn_pid_epoch.lock().await;

        // 4. Send EndTxn to the coordinator until it gives an answer that
        //    decides the outcome, or the retry deadline ends.
        let request = EndTxnRequest {
            transactional_id: tid,
            producer_id: pid,
            producer_epoch: epoch,
            committed,
            ..Default::default()
        };
        let end_txn = self
            .send_end_txn_until_decided(coord, request, committed)
            .await;

        let mut state = self.txn_state.lock().await;
        let (decision, producer_identity) = match end_txn {
            Ok(answer) => answer,
            Err(error) => {
                // Kafka's `TransactionManager.authenticationFailed` makes the
                // producer fail. An earlier attempt may have taken effect, so
                // a fresh InitProducerId epoch is required before any reuse.
                self.require_transaction_recovery();
                *state = TxnState::RecoveryRequired;
                return Err(ProducerError::Client(error));
            }
        };
        match decision {
            EndTxnDecision::Complete => {
                // KIP-890 (transaction.version 2): the coordinator bumps the
                // producer epoch on transaction completion and returns the new
                // (producer_id, producer_epoch) in the EndTxn v5 response. Adopt
                // it so the next transaction (and its record batches, which read
                // this shared pair) use the un-fenced epoch. A pre-KIP-890
                // coordinator leaves these at -1, in which case we keep the
                // current pair unchanged.
                if let Some(identity) = producer_identity {
                    self.adopt_transactional_identity(identity).await;
                }
                *self.prepared_transaction_state.lock().await = None;
                self.txn_error.clear_abortable();
                *state = TxnState::Ready;
                self.resolve_transaction_guard();
                Ok(())
            }
            EndTxnDecision::Fenced => {
                *state = TxnState::Fenced;
                drop(state);
                Err(self.fatal_error(FatalError::Fenced))
            }
            EndTxnDecision::Fatal(code) => {
                *state = TxnState::FatalError;
                drop(state);
                Err(self.fatal_error(FatalError::Server(code)))
            }
            EndTxnDecision::ConcurrentTransactions => {
                *state = previous_state; // Caller can retry the same decision.
                Err(ProducerError::ConcurrentTransactions)
            }
            EndTxnDecision::Abortable(code) => {
                *state = previous_state;
                drop(state);
                Err(self.abortable_error(code))
            }
            EndTxnDecision::TimedOut(code) => {
                *state = previous_state;
                Err(ProducerError::Server(code))
            }
            EndTxnDecision::OutcomeUnknown | EndTxnDecision::Retry { .. } => {
                // A lost attempt may have taken effect. A fresh InitProducerId
                // epoch is required before any reuse.
                self.require_transaction_recovery();
                *state = TxnState::RecoveryRequired;
                Err(ProducerError::RecoveryRequired)
            }
        }
    }

    /// Use a new transactional `(producer_id, producer_epoch)`.
    ///
    /// A new identity starts every partition again at sequence 0. Kafka's
    /// `TransactionManager` calls `resetSequenceNumbers` in the same two places,
    /// after `InitProducerId` and after an `EndTxn` v5 epoch bump. A broker
    /// that rebuilds producer state from its log sees the transaction-version-2
    /// end marker raise the epoch and clear the last sequence. It then rejects a
    /// batch at the new epoch whose sequence is not 0 with
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER`. Every batch is flushed before either
    /// call, so no in-flight batch holds a sequence from the old identity.
    async fn adopt_transactional_identity(&self, identity: (i64, i16)) {
        let mut current = self.txn_pid_epoch.lock().await;
        if *current != identity {
            self.next_seq.clear();
        }
        *current = identity;
    }

    /// Send one `EndTxn` request until its answer decides the outcome.
    ///
    /// This follows Kafka's `EndTxnHandler`. A transport failure, or a
    /// retriable code such as `CONCURRENT_TRANSACTIONS` or
    /// `COORDINATOR_LOAD_IN_PROGRESS`, causes a retry of the same request with
    /// the same producer id and epoch. The retry uses capped
    /// exponential backoff, and the producer-ID initialization retry timeout
    /// bounds it. A retry after a transport failure first finds the coordinator
    /// again and opens a new connection, because a broker restart leaves the
    /// cached connection closed.
    ///
    /// It returns the decision, and the producer identity from a `NONE` answer
    /// when the coordinator sent one.
    ///
    /// # Errors
    ///
    /// Returns a failed authentication with the coordinator at once. Kafka's
    /// `Sender.runOnce` does not retry it.
    async fn send_end_txn_until_decided(
        &self,
        mut coordinator: Client,
        request: EndTxnRequest,
        committed: bool,
    ) -> Result<(EndTxnDecision, Option<(i64, i16)>), ClientError> {
        let deadline = tokio::time::Instant::now() + self.init_retry_timeout.to_std();
        let max_backoff = self.retry_backoff_max.to_std();
        let mut backoff = self.init_retry_backoff.to_std();
        let mut earlier_attempt_lost = false;
        loop {
            let (attempt, identity) = match coordinator.send(request.clone()).await {
                Ok(response) => {
                    tracing::Span::current().record("error_code", response.error_code);
                    let identity = (response.producer_id >= 0)
                        .then_some((response.producer_id, response.producer_epoch));
                    (CoordinatorAttempt::Answered(response.error_code), identity)
                }
                Err(error) if error.is_authentication_failure() => return Err(error),
                Err(error) => {
                    tracing::warn!(
                        %error,
                        committed = request.committed,
                        "EndTxn request lost in transport; retrying to learn the outcome"
                    );
                    (CoordinatorAttempt::Lost, None)
                }
            };
            let decision = txn_retry::decide_end_txn(attempt, earlier_attempt_lost, committed);
            earlier_attempt_lost |= attempt == CoordinatorAttempt::Lost;
            let EndTxnDecision::Retry { rediscover } = decision else {
                return Ok((decision, identity));
            };
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok((
                    txn_retry::decide_end_txn_at_deadline(attempt, earlier_attempt_lost),
                    None,
                ));
            }
            tokio::time::sleep(backoff.min(remaining)).await;
            backoff = backoff.saturating_mul(2).min(max_backoff);
            if rediscover {
                match self
                    .reconnect_txn_coordinator(&request.transactional_id)
                    .await
                {
                    Ok(fresh) => coordinator = fresh,
                    Err(error) => {
                        tracing::warn!(%error, "transaction coordinator lookup failed; retrying");
                        // The next send on the old client fails fast and
                        // leads here again.
                        coordinator.reconnect_bootstrap().await;
                    }
                }
            }
        }
    }

    /// Find the transaction coordinator again and cache a new connection to it.
    async fn reconnect_txn_coordinator(&self, tid: &str) -> Result<Client, ProducerError> {
        let address = match self.find_txn_coordinator(tid).await {
            Ok(address) => address,
            Err(error @ ProducerError::Client(_)) => {
                // The bootstrap connection can also be closed by the restart.
                self.client.reconnect_bootstrap().await;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let coordinator = self.connect_txn_coordinator(address).await?;
        *self.txn_coord_client.lock().await = Some(coordinator.clone());
        Ok(coordinator)
    }

    /// Open a client for the transaction coordinator at `address`.
    async fn connect_txn_coordinator(&self, address: String) -> Result<Client, ProducerError> {
        Ok(Client::builder()
            .bootstrap(address)
            .client_id(self.client_id.clone())
            .maybe_security(self.security.clone())
            .dispatch_queue_capacity(self.dispatch_queue_capacity.get())
            .frame_max(self.frame_max.size())
            .request_timeout(self.request_timeout)
            .build()
            .await?)
    }

    /// Initialize the transactional producer.
    ///
    /// The caller must call this before any transactional operation. It
    /// discovers the transaction coordinator with `FindCoordinator`, opens a
    /// dedicated connection to it, and calls `InitProducerId` to get a fenced
    /// `(producer_id, producer_epoch)` pair.
    ///
    /// # Errors
    ///
    /// - [`ProducerError::NotTransactional`] — `transactional_id` was not set.
    /// - [`ProducerError::InvalidTransactionState`] — called while a
    ///   transaction is in flight.
    /// - [`ProducerError::FencedProducer`] — the broker returned
    ///   `INVALID_PRODUCER_EPOCH (47)` or `PRODUCER_FENCED (90)`, now or in an
    ///   earlier transactional operation.
    /// - [`ProducerError::FatalTransactionError`] — the coordinator answered
    ///   with a fatal code, now or in an earlier transactional operation.
    ///   Kafka's `initTransactions` does not clear a fatal error either: close
    ///   the producer.
    /// - [`ProducerError::Server`] — an abortable code
    ///   (`TRANSACTIONAL_ID_AUTHORIZATION_FAILED`,
    ///   `CLUSTER_AUTHORIZATION_FAILED`, `TRANSACTION_ABORTABLE`), or a
    ///   retriable code at the retry deadline. The call can be made again.
    /// - [`ProducerError::Client`] — transport-level failure.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            transactional_id = self.transactional_id.as_deref(),
            coordinator = tracing::field::Empty,
            producer_id = tracing::field::Empty,
            producer_epoch = tracing::field::Empty,
            error_code = tracing::field::Empty,
        ),
        err,
    )]
    pub async fn init_transactions(&self) -> Result<(), ProducerError> {
        self.init_transactions_with_keep_prepared(false).await
    }

    /// Initializes the producer and optionally preserves an ongoing prepared
    /// transaction from an earlier producer incarnation.
    ///
    /// When `keep_prepared` is `true` and the broker reports an ongoing
    /// transaction, the producer enters the prepared state. The only allowed
    /// transaction-ending operations are commit, abort, and
    /// [`complete_transaction`](Self::complete_transaction). The broker returns
    /// both identities: the original identity becomes the persisted comparison
    /// token, while the newly staged identity is used for the eventual `EndTxn`.
    ///
    /// # Errors
    ///
    /// The errors are the same as [`init_transactions`](Self::init_transactions).
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            transactional_id = self.transactional_id.as_deref(),
            keep_prepared,
            coordinator = tracing::field::Empty,
            producer_id = tracing::field::Empty,
            producer_epoch = tracing::field::Empty,
            error_code = tracing::field::Empty,
        ),
        err,
    )]
    pub async fn init_transactions_with_keep_prepared(
        &self,
        keep_prepared: bool,
    ) -> Result<(), ProducerError> {
        let Some(tid) = self.transactional_id.as_deref() else {
            return Err(ProducerError::NotTransactional);
        };
        // Kafka's `TransactionManager.initializeTransactions` calls
        // `maybeFailWithError`: no transition leaves `FATAL_ERROR`, so a new
        // `initTransactions` does not clear it.
        if let Some(error) = self.fatal_error_state() {
            return Err(error);
        }

        let previous_state = {
            let mut state = self.txn_state.lock().await;
            if !matches!(
                *state,
                TxnState::Uninitialized | TxnState::Ready | TxnState::RecoveryRequired
            ) && !self.transaction_recovery_required()
            {
                return Err(ProducerError::InvalidTransactionState(
                    "init_transactions called while a transaction is in flight",
                ));
            }
            let previous_state = *state;
            *state = TxnState::Initializing;
            previous_state
        };

        let initialized = async {
            let coord_addr = self.find_txn_coordinator(tid).await?;
            tracing::Span::current().record("coordinator", coord_addr.as_str());

            let coord = self.connect_txn_coordinator(coord_addr).await?;

            self.init_producer_id_on_coordinator(
                tid,
                &InitProducerIdRequest {
                    transactional_id: Some(tid.to_owned()),
                    transaction_timeout_ms: self.transaction_timeout_ms,
                    enable2_pc: self.two_phase_commit_enabled,
                    keep_prepared_txn: keep_prepared,
                    ..Default::default()
                },
                coord,
            )
            .await
        }
        .await;
        let (coord, resp) = match initialized {
            Ok(initialized) => initialized,
            Err(error) => {
                *self.txn_state.lock().await = previous_state;
                return Err(error);
            }
        };

        tracing::Span::current().record("error_code", resp.error_code);
        match resp.error_code {
            0 => {
                let recovered = if keep_prepared && resp.ongoing_txn_producer_id >= 0 {
                    let Ok(recovered) = PreparedTransactionState::new(
                        resp.ongoing_txn_producer_id,
                        resp.ongoing_txn_producer_epoch,
                    ) else {
                        *self.txn_state.lock().await = previous_state;
                        return Err(ProducerError::InvalidTransactionState(
                            "transaction coordinator returned an invalid ongoing identity",
                        ));
                    };
                    Some(recovered)
                } else {
                    None
                };
                tracing::Span::current().record("producer_id", resp.producer_id);
                tracing::Span::current().record("producer_epoch", resp.producer_epoch);
                self.adopt_transactional_identity((resp.producer_id, resp.producer_epoch))
                    .await;
                self.txn_error.clear_abortable();
                *self.txn_coord_client.lock().await = Some(coord);
                *self.prepared_transaction_state.lock().await = recovered;
                *self.txn_state.lock().await = if recovered.is_some() {
                    TxnState::Prepared
                } else {
                    TxnState::Ready
                };
                self.txn_recovery_required.store(false, Ordering::Release);
                self.resolve_transaction_guard();
                Ok(())
            }
            code => match txn_retry::decide_init_producer_id(CoordinatorAttempt::Answered(code)) {
                TxnRequestDecision::Fenced => Err(self.fence_transaction().await),
                TxnRequestDecision::Fatal(code) => Err(self.fatal_transaction(code).await),
                // An abortable code, or a retriable code at the retry
                // deadline, leaves the producer as it was.
                TxnRequestDecision::Done
                | TxnRequestDecision::Abortable(_)
                | TxnRequestDecision::Retry { .. } => {
                    *self.txn_state.lock().await = previous_state;
                    Err(ProducerError::Server(code))
                }
            },
        }
    }

    /// Send `InitProducerId` to the transaction coordinator until it answers,
    /// or until the retry deadline ends. It gives back the coordinator that
    /// answered, which is a new connection after a coordinator move.
    ///
    /// The retries follow Kafka's `InitProducerIdHandler`. A transport loss,
    /// `COORDINATOR_NOT_AVAILABLE` and `NOT_COORDINATOR` find the coordinator
    /// again. Every other retriable code sends the request again on the same
    /// connection. The caller reads the error code of the answer.
    async fn init_producer_id_on_coordinator(
        &self,
        transactional_id: &str,
        request: &InitProducerIdRequest,
        coordinator: Client,
    ) -> Result<(Client, InitProducerIdResponse), ProducerError> {
        let mut coordinator = coordinator;
        let mut retry = self.coordinator_retry();
        loop {
            let (attempt, last_outcome) = match send_init_producer_id(&coordinator, request).await {
                Ok(response) => (
                    CoordinatorAttempt::Answered(response.error_code),
                    Ok(response),
                ),
                Err(error @ ClientError::Disconnected) => (CoordinatorAttempt::Lost, Err(error)),
                Err(error) => return Err(ProducerError::Client(error)),
            };
            let TxnRequestDecision::Retry { rediscover } =
                txn_retry::decide_init_producer_id(attempt)
            else {
                return last_outcome
                    .map(|response| (coordinator, response))
                    .map_err(ProducerError::Client);
            };
            if !retry.wait().await {
                return last_outcome
                    .map(|response| (coordinator, response))
                    .map_err(ProducerError::Client);
            }
            if rediscover {
                coordinator = self
                    .rediscovered_txn_coordinator(transactional_id, coordinator)
                    .await;
            }
        }
    }

    /// Discover the transaction coordinator for `tid` with `FindCoordinator`.
    ///
    /// It handles both the legacy top-level response, versions 0–3, and the
    /// `coordinators` array that version 4 introduced.
    #[tracing::instrument(level = "debug", skip_all, fields(transactional_id = %tid), err)]
    async fn find_txn_coordinator(&self, tid: &str) -> Result<String, ProducerError> {
        self.find_coordinator(tid, 1).await
    }

    async fn find_coordinator(&self, key: &str, key_type: i8) -> Result<String, ProducerError> {
        let resp = self
            .client
            .send(FindCoordinatorRequest {
                // v0-3: the `key` field carries the lookup key
                key: key.to_owned(),
                key_type,
                // v4+: repeated coordinator_keys list
                coordinator_keys: vec![key.to_owned()],
                ..Default::default()
            })
            .await?;

        // v4+ returns a `coordinators` array; prefer it when present.
        if let Some(coord) = resp.coordinators.first() {
            if coord.error_code != 0 {
                return Err(ProducerError::Server(coord.error_code));
            }
            return Ok(format!("{}:{}", coord.host, coord.port));
        }

        // Fallback: legacy top-level host/port (versions 0–3).
        if resp.error_code != 0 {
            return Err(ProducerError::Server(resp.error_code));
        }
        Ok(format!("{}:{}", resp.host, resp.port))
    }

    /// Enroll a consumer group's offsets in the current transaction, and fence
    /// zombie producers with the supplied [`ConsumerGroupMetadata`], as
    /// KIP-447 defines.
    ///
    /// This does two broker round-trips:
    ///
    /// 1. `AddOffsetsToTxn` to the transaction coordinator. This registers the
    ///    group offset commit as part of the ongoing transaction.
    /// 2. `TxnOffsetCommit` to the group coordinator. This commits the actual
    ///    offsets transactionally. It carries the generation, member and
    ///    instance of `group_meta`, so the coordinator can fence stale
    ///    producers.
    ///
    /// Each request goes out again after a transport loss and after a
    /// retriable code, until the producer-ID initialization retry timeout
    /// ends. A code that shows the coordinator moved also finds the
    /// coordinator again. The rules follow Kafka's `AddOffsetsToTxnHandler`
    /// and `TxnOffsetCommitHandler`.
    ///
    /// # Errors
    ///
    /// - [`ProducerError::NotTransactional`] — `transactional_id` was not set.
    /// - [`ProducerError::InvalidTransactionState`] — there is no cached
    ///   transaction coordinator. Call [`init_transactions`] first.
    /// - [`ProducerError::FencedProducer`] — the coordinator answered
    ///   `INVALID_PRODUCER_EPOCH (47)` or `PRODUCER_FENCED (90)`.
    /// - [`ProducerError::Server`] — any other broker error code. A code that
    ///   Kafka calls abortable also makes every later commit fail, so the
    ///   application must abort the transaction.
    /// - [`ProducerError::Client`] — transport-level failure.
    ///
    /// [`init_transactions`]: Self::init_transactions
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            transactional_id = self.transactional_id.as_deref(),
            group_id = %group_meta.group_id,
            generation_id = group_meta.generation_id,
            offset_count = tracing::field::Empty,
        ),
        err,
    )]
    pub async fn send_offsets_to_transaction(
        &self,
        offsets: impl IntoIterator<Item = ((String, i32), i64)>,
        group_meta: &ConsumerGroupMetadata,
    ) -> Result<(), ProducerError> {
        let tid = self
            .transactional_id
            .as_deref()
            .ok_or(ProducerError::NotTransactional)?
            .to_string();
        if let Some(error) = self.fatal_error_state() {
            return Err(error);
        }
        if matches!(
            *self.txn_state.lock().await,
            TxnState::Preparing | TxnState::Prepared
        ) {
            return Err(ProducerError::InvalidTransactionState(
                "send_offsets_to_transaction is not allowed after prepare_transaction",
            ));
        }
        let offsets_vec: Vec<_> = offsets.into_iter().collect();
        tracing::Span::current().record("offset_count", offsets_vec.len());
        if let Some(error) = self.abortable_error_state() {
            return Err(error);
        }

        let (pid, epoch) = *self.txn_pid_epoch.lock().await;

        // 1. AddOffsetsToTxn → transaction coordinator.
        self.add_offsets_to_txn(&tid, pid, epoch, &group_meta.group_id)
            .await?;

        // 2. FindCoordinator(group_id, key_type=0 GROUP), then TxnOffsetCommit
        //    → group coordinator, carrying the consumer group metadata
        //    (generation id / member id / instance id) so the coordinator can
        //    fence zombie producers through the group's own state rather than
        //    requiring one producer per input partition (KIP-447).
        self.txn_offset_commit(&tid, pid, epoch, group_meta, &offsets_vec)
            .await
    }

    /// Send `AddOffsetsToTxn` to the transaction coordinator until it answers,
    /// or until the retry deadline ends.
    ///
    /// The retries follow Kafka's `AddOffsetsToTxnHandler`. A transport loss,
    /// `COORDINATOR_NOT_AVAILABLE` and `NOT_COORDINATOR` find the coordinator
    /// again. Every other retriable code sends the request again on the same
    /// connection. The coordinator adds the offsets topic partition to the
    /// transaction once, so a retry is safe.
    async fn add_offsets_to_txn(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<(), ProducerError> {
        let mut coordinator = self.txn_coord_client.lock().await.clone().ok_or(
            ProducerError::InvalidTransactionState(
                "no txn coordinator cached — did init_transactions succeed?",
            ),
        )?;
        let request = AddOffsetsToTxnRequest {
            transactional_id: transactional_id.to_owned(),
            producer_id,
            producer_epoch,
            group_id: group_id.to_owned(),
            ..Default::default()
        };
        let mut retry = self.coordinator_retry();
        loop {
            let (attempt, last_error) = match coordinator.send(request.clone()).await {
                Ok(response) => (
                    CoordinatorAttempt::Answered(response.error_code),
                    ProducerError::Server(response.error_code),
                ),
                Err(error) if request_may_have_reached_the_broker(&error) => {
                    (CoordinatorAttempt::Lost, ProducerError::Client(error))
                }
                Err(error) => return Err(ProducerError::Client(error)),
            };
            let rediscover = match txn_retry::decide_add_offsets_to_txn(attempt) {
                TxnRequestDecision::Done => return Ok(()),
                TxnRequestDecision::Fenced => return Err(self.fence_transaction().await),
                TxnRequestDecision::Abortable(code) => return Err(self.abortable_error(code)),
                TxnRequestDecision::Fatal(code) => return Err(self.fatal_transaction(code).await),
                TxnRequestDecision::Retry { rediscover } => rediscover,
            };
            if !retry.wait().await {
                return Err(last_error);
            }
            if rediscover {
                coordinator = self
                    .rediscovered_txn_coordinator(transactional_id, coordinator)
                    .await;
            }
        }
    }

    /// Send `TxnOffsetCommit` to the group coordinator until it answers, or
    /// until the retry deadline ends.
    ///
    /// The retries follow Kafka's `TxnOffsetCommitHandler`. A transport loss,
    /// `COORDINATOR_NOT_AVAILABLE`, `NOT_COORDINATOR` and `REQUEST_TIMED_OUT`
    /// find the group coordinator again. Every other retriable code sends the
    /// request again on the same connection. Kafka sends only the partitions
    /// that failed with a retriable code; this producer sends the whole
    /// request again, and the coordinator writes the same offsets.
    async fn txn_offset_commit(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_meta: &ConsumerGroupMetadata,
        offsets: &[((String, i32), i64)],
    ) -> Result<(), ProducerError> {
        let request = TxnOffsetCommitRequest {
            transactional_id: transactional_id.to_owned(),
            producer_id,
            producer_epoch,
            group_id: group_meta.group_id.clone(),
            generation_id: group_meta.generation_id,
            member_id: group_meta.member_id.clone(),
            group_instance_id: group_meta.group_instance_id.clone(),
            topics: build_topics_payload(offsets),
            ..Default::default()
        };
        let mut group_client = self.connect_group_coordinator(&group_meta.group_id).await?;
        let mut retry = self.coordinator_retry();
        loop {
            let (attempt, last_error) = match group_client.send(request.clone()).await {
                Ok(response) => {
                    let code = txn_offset_commit_error_code(&response);
                    (
                        CoordinatorAttempt::Answered(code),
                        ProducerError::Server(code),
                    )
                }
                Err(error) if request_may_have_reached_the_broker(&error) => {
                    (CoordinatorAttempt::Lost, ProducerError::Client(error))
                }
                Err(error) => return Err(ProducerError::Client(error)),
            };
            let rediscover = match txn_retry::decide_txn_offset_commit(attempt) {
                TxnRequestDecision::Done => return Ok(()),
                TxnRequestDecision::Fenced => return Err(self.fence_transaction().await),
                TxnRequestDecision::Abortable(code) => return Err(self.abortable_error(code)),
                TxnRequestDecision::Fatal(code) => return Err(self.fatal_transaction(code).await),
                TxnRequestDecision::Retry { rediscover } => rediscover,
            };
            if !retry.wait().await {
                return Err(last_error);
            }
            if rediscover {
                match self.connect_group_coordinator(&group_meta.group_id).await {
                    Ok(fresh) => group_client = fresh,
                    Err(error) => {
                        tracing::warn!(%error, "group coordinator lookup failed; retrying");
                        // `FindCoordinator` goes out on the bootstrap
                        // connection, which the same restart can have closed.
                        self.client.reconnect_bootstrap().await;
                    }
                }
            }
        }
    }

    /// Connect to the group coordinator of `group_id`.
    async fn connect_group_coordinator(&self, group_id: &str) -> Result<Client, ProducerError> {
        let group_addr = self.find_group_coordinator(group_id).await?;
        Ok(Client::builder()
            .bootstrap(group_addr)
            .client_id(self.client_id.clone())
            .maybe_security(self.security.clone())
            .dispatch_queue_capacity(self.dispatch_queue_capacity.get())
            .frame_max(self.frame_max.size())
            .build()
            .await?)
    }

    /// Discover the group coordinator for `group_id` with `FindCoordinator`
    /// and `key_type = 0`, which is GROUP.
    ///
    /// This mirrors [`find_txn_coordinator`], but it uses `key_type = 0` and
    /// looks up the group coordinator rather than the transaction
    /// coordinator.
    ///
    /// [`find_txn_coordinator`]: Self::find_txn_coordinator
    #[tracing::instrument(level = "debug", skip_all, fields(group_id = %group_id), err)]
    async fn find_group_coordinator(&self, group_id: &str) -> Result<String, ProducerError> {
        self.find_coordinator(group_id, 0).await
    }

    // ── Internal lifecycle ───────────────────────────────────────────────────

    /// The retry limits of a coordinator request. The producer-ID
    /// initialization retry timeout limits every such loop.
    fn coordinator_retry(&self) -> CoordinatorRetry {
        CoordinatorRetry {
            deadline: tokio::time::Instant::now() + self.init_retry_timeout.to_std(),
            backoff: self.init_retry_backoff.to_std(),
            max_backoff: self.retry_backoff_max.to_std(),
        }
    }

    /// Find the transaction coordinator again and connect to it. It gives
    /// `current` back when the lookup fails, after it reconnects the bootstrap
    /// connection.
    async fn rediscovered_txn_coordinator(
        &self,
        transactional_id: &str,
        current: Client,
    ) -> Client {
        match self.reconnect_txn_coordinator(transactional_id).await {
            Ok(fresh) => fresh,
            Err(error) => {
                tracing::warn!(%error, "transaction coordinator lookup failed; retrying");
                current.reconnect_bootstrap().await;
                current
            }
        }
    }

    /// Mark the transaction fenced, and give the error to report. A newer
    /// epoch owns the transactional id, so no request of this producer can
    /// change the transaction. Kafka's `TransactionManager.fatalError` leaves
    /// the producer in the same place.
    async fn fence_transaction(&self) -> ProducerError {
        *self.txn_state.lock().await = TxnState::Fenced;
        self.fatal_error(FatalError::Fenced)
    }

    /// Move the transaction to the fatal error state for `code`, and give the
    /// error to report. Kafka's `TransactionManager.fatalError` does the same.
    async fn fatal_transaction(&self, code: i16) -> ProducerError {
        *self.txn_state.lock().await = TxnState::FatalError;
        self.fatal_error(FatalError::Server(code))
    }

    /// Record `error` as the fatal error of the transaction, and give the
    /// error to report. The caller moves `txn_state`.
    ///
    /// The open transaction guard is resolved, because no request of this
    /// producer can change the transaction again: a dropped guard must not
    /// ask for recovery. The sender fails every transactional batch that it
    /// has not sent, as Kafka's `Sender.runOnce` calls `maybeAbortBatches`
    /// in `FATAL_ERROR`, so the sender is woken.
    fn fatal_error(&self, error: FatalError) -> ProducerError {
        self.txn_error.set_fatal(error);
        self.resolve_transaction_guard();
        tracing::error!(
            ?error,
            "the transactional producer is in a fatal error state; close it"
        );
        let _ = self.wake_tx.try_send(DrainIntent::Ready);
        error.error()
    }

    /// The fatal error of the transaction, as the error that every
    /// transactional operation reports.
    fn fatal_error_state(&self) -> Option<ProducerError> {
        self.txn_error.fatal().map(FatalError::error)
    }

    /// The error that `maybeFailWithError` throws in Kafka's
    /// `TransactionManager`: the fatal error, or else the abortable error.
    fn transaction_error_state(&self) -> Option<ProducerError> {
        self.fatal_error_state()
            .or_else(|| self.abortable_error_state())
    }

    /// Record `code` as the error that only an abort can clear, and give the
    /// error to report. Kafka's `TransactionManager.abortableError` does the
    /// same.
    fn abortable_error(&self, code: i16) -> ProducerError {
        self.txn_error.set_abortable(code);
        tracing::warn!(
            error_code = code,
            "the transaction can no longer commit; abort it"
        );
        ProducerError::Server(code)
    }

    /// The error that a commit reports while the transaction is abort-only.
    fn abortable_error_state(&self) -> Option<ProducerError> {
        self.txn_error.abortable().map(|error| match error {
            AbortableError::Server(code) => ProducerError::Server(code),
            AbortableError::Timeout => ProducerError::SendTimeout,
        })
    }

    pub(crate) fn is_active(&self) -> Result<(), ProducerError> {
        match self.state.load(Ordering::Acquire) {
            STATE_ACTIVE => Ok(()),
            STATE_FENCED => Err(ProducerError::FencedProducer),
            _ => Err(ProducerError::Closed),
        }
    }

    pub(crate) fn require_transaction_recovery(&self) {
        if !self.txn_recovery_required.swap(true, Ordering::AcqRel) {
            self.txn_recovery_generation.fetch_add(1, Ordering::AcqRel);
        }
        let _ = self.wake_tx.try_send(DrainIntent::Ready);
    }

    fn transaction_recovery_required(&self) -> bool {
        self.txn_recovery_required.load(Ordering::Acquire)
    }

    pub(crate) fn abandon_transaction_guard(&self, generation: u64) {
        if self
            .txn_guard_generation
            .compare_exchange(
                generation,
                generation.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.require_transaction_recovery();
        }
    }

    fn resolve_transaction_guard(&self) {
        let generation = self.txn_guard_generation.load(Ordering::Acquire);
        if generation % 2 == 1 {
            let _ = self.txn_guard_generation.compare_exchange(
                generation,
                generation.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    #[allow(dead_code)] // wired by sender on INVALID_PRODUCER_EPOCH; kept for symmetry
    pub(crate) fn fence(&self) {
        self.state
            .compare_exchange(
                STATE_ACTIVE,
                STATE_FENCED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .ok();
    }

    /// Enqueue a record and return a future that resolves when the broker
    /// acks, or when the producer fences or closes.
    ///
    /// A record with a negative partition or timestamp fails at once with
    /// [`ProducerError::InvalidPartition`] or
    /// [`ProducerError::InvalidTimestamp`], and the producer sends nothing.
    ///
    /// A transactional producer fails the record at once, and sends nothing,
    /// when its transaction holds an error: the abortable error until the
    /// application aborts, and the fatal error for good. Kafka's
    /// `KafkaProducer.doSend` fails the same way in
    /// `TransactionManager.maybeAddPartition`.
    ///
    /// This returns a `oneshot::Receiver`. The outer call is `async` because
    /// it waits for metadata that holds the topic, for at most `max_block`, as
    /// Kafka's `KafkaProducer.waitOnMetadata` does. When the wait fails, the
    /// receiver holds the error and the producer sends no Produce for the
    /// record.
    pub async fn send(
        &self,
        record: ProducerRecord,
    ) -> oneshot::Receiver<Result<RecordMetadata, ProducerError>> {
        let span = tracing::debug_span!(
            "producer.send",
            topic = %record.topic,
            partition = tracing::field::Empty,
        );
        self.send_inner(record).instrument(span).await
    }

    async fn send_inner(
        &self,
        record: ProducerRecord,
    ) -> oneshot::Receiver<Result<RecordMetadata, ProducerError>> {
        // Kafka checks the record when the application creates it, before
        // `send` makes any other check.
        if let Err(e) = record.validate() {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Err(e));
            return rx;
        }
        if let Err(e) = self.is_active() {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Err(e));
            return rx;
        }
        if self.transactional_id.is_some() && self.transaction_recovery_required() {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Err(ProducerError::RecoveryRequired));
            return rx;
        }

        let failed = |error: ProducerError| {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Err(error));
            rx
        };

        // Kafka's `KafkaProducer.doSend` calls `throwIfInPreparedState` before
        // `waitOnMetadata`, so a send after `prepare_transaction` does not
        // wait for metadata. The check before the append covers a
        // `prepare_transaction` that starts during the wait.
        if self.transactional_id.is_some() {
            let state = *self.txn_state.lock().await;
            if let Err(error) = self.transaction_generation(Some(state)) {
                return failed(error);
            }
        }

        // Produce v13 carries only the `topic_id` on the wire, so the cache
        // must hold the topic also when the caller names the partition.
        let metadata_started = tokio::time::Instant::now();
        let partition_count = match self.partition_count(&record.topic, record.partition).await {
            Ok(count) => count,
            Err(error) => return failed(error),
        };
        // A record that names no partition and has no key that picks one goes
        // to the sticky partition. Kafka's `KafkaProducer.partition` and
        // `RecordAccumulator.append`.
        let fixed_partition = record.partition.or_else(|| {
            self.partitioner
                .keyed_partition(record.key.as_deref(), partition_count)
        });
        let has_leader = |partition: i32| {
            self.partition_leaders
                .get(&(record.topic.clone(), partition))
                .is_some_and(|leader| *leader >= 0)
        };
        let partitions = TopicPartitions {
            count: partition_count,
            has_leader: &has_leader,
        };
        // Kafka's `KafkaProducer.doSend` gives the buffer the part of
        // `max.block.ms` that the metadata wait left, in whole milliseconds.
        let waited_on_metadata = Duration::from_millis(
            u64::try_from(metadata_started.elapsed().as_millis()).unwrap_or(u64::MAX),
        );
        let remaining_block = self.max_block.saturating_sub(waited_on_metadata);

        // Kafka's `KafkaProducer.ensureValidRecordSize`. It compares its
        // upper bound of the serialized size with `max.request.size` first,
        // and then with `buffer.memory`.
        let serialized_size = record_size_upper_bound(
            record.key.as_deref(),
            record.value.as_deref(),
            &record.headers,
        );
        if let Some(limit) = self.record_size_limit(serialized_size) {
            return failed(ProducerError::RecordTooLarge {
                record_size: serialized_size,
                limit,
            });
        }
        let record_size = approx_record_size(
            record.key.as_deref(),
            record.value.as_deref(),
            &record.headers,
        );

        let timestamp = record.timestamp_ms.unwrap_or_else(current_millis);
        let mut memory = None;
        loop {
            // Kafka's `RecordAccumulator.append` peeks the sticky partition
            // before it takes the partition lock, and checks under the lock
            // that no other send switched it. A switch starts the loop again.
            // Memory is not bound to a partition, so it serves the new one.
            let sticky = fixed_partition
                .is_none()
                .then(|| self.partitioner.peek(&record.topic, &partitions));
            let partition = fixed_partition
                .or(sticky.map(StickyPartition::partition))
                .unwrap_or_default();
            tracing::Span::current().record("partition", partition);
            let acc = Arc::clone(
                self.accumulators
                    .entry((record.topic.clone(), partition))
                    .or_insert_with(|| Arc::new(Mutex::new(Accumulator::new(self.batch_size))))
                    .value(),
            );

            // A record that starts a new batch needs buffer memory. The wait
            // holds no lock, so the sender can complete batches and free
            // memory. Kafka's `RecordAccumulator.append` also allocates
            // outside the partition lock, and tries the append again after.
            //
            // The state check comes first, so a send that cannot append fails
            // at once. It releases the state lock before it takes the
            // accumulator lock: the append below takes the two locks in the
            // other order.
            if memory.is_none() {
                let state = match &self.transactional_id {
                    Some(_) => Some(*self.txn_state.lock().await),
                    None => None,
                };
                let expected_generation = match self.append_transaction_generation(state) {
                    Ok(generation) => generation,
                    Err(error) => return failed(error),
                };
                let needs_memory = {
                    let mut a = acc.lock().await;
                    if self.sticky_partition_changed(&record.topic, sticky, &a, &partitions) {
                        continue;
                    }
                    let needs_memory = a.needs_new_batch(record_size, expected_generation);
                    // Kafka closes a batch that the record does not fit, so
                    // the sender can send it and free its memory during the
                    // wait.
                    if needs_memory && a.current.as_ref().is_some_and(|batch| !batch.is_empty()) {
                        a.seal_current();
                        let _ = self.wake_tx.try_send(DrainIntent::Ready);
                    }
                    needs_memory
                };
                if needs_memory {
                    let size = self.batch_size.max(record_size);
                    match self.buffer_pool.allocate(size, remaining_block).await {
                        Ok(reservation) => memory = Some(reservation),
                        Err(error) => return failed(error),
                    }
                }
            }

            // Keep the state lock until the record is registered and appended.
            // `prepare_transaction` takes the same lock before it flushes, so
            // it cannot race past a send that has already joined this
            // transaction.
            let mut transaction_state = if self.transactional_id.is_some() {
                Some(self.txn_state.lock().await)
            } else {
                None
            };
            let transaction_generation =
                match self.append_transaction_generation(transaction_state.as_deref().copied()) {
                    Ok(generation) => generation,
                    Err(error) => return failed(error),
                };
            if transaction_generation.is_some()
                && let Err(error) = self
                    .register_transaction_partition(&record.topic, partition)
                    .await
            {
                // The state lock is held here, so the state moves under it.
                if let (Some(state), Some(fatal)) =
                    (transaction_state.as_deref_mut(), self.txn_error.fatal())
                {
                    *state = fatal.state();
                }
                return failed(error);
            }
            let mut a = acc.lock().await;
            if self.sticky_partition_changed(&record.topic, sticky, &a, &partitions) {
                // Another send switched the sticky partition. Adding this
                // partition to the transaction has no effect on the outcome:
                // the transaction ends with no record on it.
                continue;
            }
            if memory.is_none() && a.needs_new_batch(record_size, transaction_generation) {
                // Another send filled the batch since the check above. Wait
                // for memory again, with no lock held. Adding the partition to
                // the transaction again has no effect.
                continue;
            }
            if let Err(error) = self.is_active() {
                return failed(error);
            }
            let AppendResult {
                receiver: rx,
                wakes_sender,
            } = a.append(
                record.key,
                record.value,
                record.headers,
                timestamp,
                transaction_generation,
                memory,
            );
            if let Some(sticky) = sticky {
                // Kafka's `RecordAccumulator.updatePartitionInfoOnAppend`.
                self.partitioner.update(
                    &record.topic,
                    sticky,
                    record_size,
                    &partitions,
                    a.all_batches_full(),
                );
            }
            drop(a);
            drop(transaction_state);
            wake_sender_after_append(&self.wake_tx, self.linger, wakes_sender);
            return rx;
        }
    }

    /// Tell if the append must pick its partition again. Kafka's
    /// `RecordAccumulator.partitionChanged`.
    ///
    /// Another send can have switched the sticky partition. When no batch of
    /// the partition is open, a switch that an open batch deferred happens
    /// now. The caller holds the lock of the accumulator `accumulator`.
    fn sticky_partition_changed(
        &self,
        topic: &str,
        sticky: Option<StickyPartition>,
        accumulator: &Accumulator,
        partitions: &TopicPartitions<'_>,
    ) -> bool {
        let Some(sticky) = sticky else {
            return false;
        };
        if self.partitioner.is_changed(topic, sticky) {
            return true;
        }
        if accumulator.all_batches_full() {
            self.partitioner.update(topic, sticky, 0, partitions, true);
            return self.partitioner.is_changed(topic, sticky);
        }
        false
    }

    /// The limit that a record of `serialized_size` bytes is larger than, in
    /// the order of Kafka's `KafkaProducer.ensureValidRecordSize`.
    fn record_size_limit(&self, serialized_size: usize) -> Option<RecordSizeLimit> {
        if serialized_size > self.max_request_size {
            Some(RecordSizeLimit::MaxRequestSize(self.max_request_size))
        } else if serialized_size > self.buffer_pool.total() {
            Some(RecordSizeLimit::BufferMemory)
        } else {
            None
        }
    }

    /// The transaction generation that a send appends with in `state`.
    ///
    /// This is [`Self::transaction_generation`], after the check that Kafka's
    /// `TransactionManager.maybeAddPartition` makes with `maybeFailWithError`:
    /// a transactional producer in the fatal or the abortable error state
    /// fails the send with the stored error. Kafka makes this check after
    /// `waitOnMetadata`, so `send` calls this after its metadata wait.
    ///
    /// # Errors
    ///
    /// Returns the stored transaction error, or an error of
    /// [`Self::transaction_generation`].
    fn append_transaction_generation(
        &self,
        state: Option<TxnState>,
    ) -> Result<Option<u64>, ProducerError> {
        if state.is_some()
            && let Some(error) = self.transaction_error_state()
        {
            return Err(error);
        }
        self.transaction_generation(state)
    }

    /// The transaction generation that a send takes in `state`: the recovery
    /// generation inside a transaction, and `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error when a transaction needs recovery, or when the state
    /// does not accept a send.
    fn transaction_generation(
        &self,
        state: Option<TxnState>,
    ) -> Result<Option<u64>, ProducerError> {
        if self.transaction_recovery_required() {
            return Err(ProducerError::RecoveryRequired);
        }
        match state {
            Some(TxnState::InTransaction) => {
                Ok(Some(self.txn_recovery_generation.load(Ordering::Acquire)))
            }
            Some(TxnState::Preparing | TxnState::Prepared) => {
                Err(ProducerError::InvalidTransactionState(
                    "send is not allowed after prepare_transaction",
                ))
            }
            _ => Ok(None),
        }
    }

    /// Return the partition count of `topic`. On a cache miss, or when
    /// `partition` is not below the cached count, wait for metadata for at
    /// most `max_block`. See [`MetadataWait::partition_count`].
    ///
    /// The wait uses [`Client::refresh_metadata_with`] rather than a bare
    /// `send(MetadataRequest)`. The request names the topics of the producer
    /// (see [`metadata_request`]). `refresh_metadata_with` also teaches the
    /// client's `BrokerPool` each broker's `(id → addr)` mapping, and that is
    /// what lets the sender route a Produce to the partition *leader* with
    /// `Client::broker(id)`. The wait records each partition's `leader_id` in
    /// `partition_leaders` for the sender to consult.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(topic = %topic, num_partitions = tracing::field::Empty),
    )]
    async fn partition_count(
        &self,
        topic: &str,
        partition: Option<i32>,
    ) -> Result<i32, ProducerError> {
        // Kafka's `KafkaProducer.waitOnMetadata` calls `ProducerMetadata.add`
        // for each send, so the periodic and error refreshes name the topic.
        self.client.metadata_topics().add(topic);
        let count = MetadataWait {
            cache: &self.metadata_cache,
            partition_leaders: &self.partition_leaders,
            refresh: &self.metadata_refresh,
            max_block: self.max_block,
            retry_backoff: self.init_retry_backoff.to_std(),
            max_backoff: self.retry_backoff_max.to_std(),
        }
        .partition_count(topic, partition, |topics| {
            self.client.refresh_metadata_with(metadata_request(topics))
        })
        .await?;
        tracing::Span::current().record("num_partitions", count);
        Ok(count)
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(client_id = %self.client_id, transactional_id = self.transactional_id.as_deref()),
        err,
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn close(mut self) -> Result<(), ProducerError> {
        self.flush().await?;
        self.state.store(STATE_CLOSED, Ordering::Release);
        self.sender_shutdown.cancel();
        if let Some(h) = self.sender_handle.take() {
            let _ = h.await;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, err)]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn flush(&self) -> Result<(), ProducerError> {
        self.is_active()?;
        let deadline = tokio::time::Instant::now()
            .checked_add(self.flush_timeout.duration())
            .ok_or(ProducerError::FlushTimeout)?;

        tokio::time::timeout_at(deadline, async {
            let _ = self.wake_tx.send(DrainIntent::Force).await;
            loop {
                let notified = self.flush_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.all_empty().await && self.in_flight.load(Ordering::Acquire) == 0 {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| ProducerError::FlushTimeout)
    }

    async fn all_empty(&self) -> bool {
        for entry in self.accumulators.iter() {
            let a = entry.value().lock().await;
            if a.current.as_ref().is_some_and(|b| !b.is_empty()) {
                return false;
            }
            if !a.ready.is_empty() {
                return false;
            }
        }
        true
    }
}

impl std::fmt::Debug for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("producer_id", &self.identity.id)
            .field("producer_epoch", &self.producer_epoch())
            .field("transactional_id", &self.transactional_id)
            .field("compression", &self.compression)
            .finish_non_exhaustive()
    }
}

fn current_millis() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0)
}

/// Group `((topic, partition), offset)` pairs by topic name into the nested
/// structure required by [`TxnOffsetCommitRequest`].
fn build_topics_payload(offsets: &[((String, i32), i64)]) -> Vec<TxnOffsetCommitRequestTopic> {
    let mut by_topic: std::collections::HashMap<&str, Vec<TxnOffsetCommitRequestPartition>> =
        std::collections::HashMap::new();
    for ((topic, partition), offset) in offsets {
        by_topic
            .entry(topic.as_str())
            .or_default()
            .push(TxnOffsetCommitRequestPartition {
                partition_index: *partition,
                committed_offset: *offset,
                ..Default::default()
            });
    }
    by_topic
        .into_iter()
        .map(|(name, partitions)| TxnOffsetCommitRequestTopic {
            name: name.to_owned(),
            partitions,
            ..Default::default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use bytes::{Bytes, BytesMut};
    use krabka_client_core::MockBroker;
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            find_coordinator_request::{self, FindCoordinatorRequest},
            find_coordinator_response::{self, Coordinator, FindCoordinatorResponse},
            metadata_request::{self, MetadataRequest},
            metadata_response::{
                self, MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
                MetadataResponseTopic,
            },
            produce_request::{self, ProduceRequest},
            produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
        },
    };

    use super::{
        DrainIntent, Producer, TxnOffsetCommitResponse, minted_identity,
        request_may_have_reached_the_broker, txn_offset_commit_error_code,
        wake_sender_after_append,
    };
    use crate::{
        ProducerRecord,
        accumulator::{Accumulator, AppendResult},
        error::ProducerError,
        partitioner::partition_for_key,
    };

    /// Both halves of the pair must be non-negative. Kafka writes `-1` in
    /// either half for "no producer", so a check of one half alone would
    /// report a half-written pair as a minted identity.
    #[test]
    fn a_minted_identity_needs_both_halves_of_the_pair() {
        assert2::check!(minted_identity((4242, 7)) == Some((4242, 7)));
        assert2::check!(minted_identity((0, 0)) == Some((0, 0)));
        assert2::check!(minted_identity((-1, -1)).is_none());
        // Either half alone being negative is not a minted identity.
        assert2::check!(minted_identity((4242, -1)).is_none());
        assert2::check!(minted_identity((-1, 7)).is_none());
    }

    const CLIENT_ID: &str = "producer-test";

    #[test]
    fn only_new_deadlines_and_rollovers_wake_nonzero_linger() {
        let (wake_tx, mut wake_rx) = tokio::sync::mpsc::channel(4);

        let mut coalesced = Accumulator::new(1024);
        let AppendResult { wakes_sender, .. } =
            coalesced.try_append(None, Some(Bytes::from_static(b"a")), vec![], 0, None);
        wake_sender_after_append(&wake_tx, krabka_units::millis(10), wakes_sender);
        assert_eq!(wake_rx.try_recv(), Ok(DrainIntent::Ready));
        let AppendResult { wakes_sender, .. } =
            coalesced.try_append(None, Some(Bytes::from_static(b"b")), vec![], 0, None);
        wake_sender_after_append(&wake_tx, krabka_units::millis(10), wakes_sender);
        assert_eq!(
            wake_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        );

        let mut rollover = Accumulator::new(20);
        let _ = rollover.try_append(None, Some(Bytes::from_static(b"a")), vec![], 0, None);
        let AppendResult { wakes_sender, .. } =
            rollover.try_append(None, Some(Bytes::from_static(b"b")), vec![], 0, None);
        wake_sender_after_append(&wake_tx, krabka_units::millis(10), wakes_sender);
        assert_eq!(wake_rx.try_recv(), Ok(DrainIntent::Ready));

        let mut immediate = Accumulator::new(1024);
        let _ = immediate.try_append(None, Some(Bytes::from_static(b"a")), vec![], 0, None);
        wake_sender_after_append(&wake_tx, krabka_units::secs(0), false);
        assert_eq!(wake_rx.try_recv(), Ok(DrainIntent::Force));
    }

    fn encode_v0(resp: &impl Encode) -> Vec<u8> {
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    fn encode_find_coordinator_response(version: i16) -> Vec<u8> {
        let resp = if version >= 4 {
            FindCoordinatorResponse {
                coordinators: vec![Coordinator {
                    key: "group-a".into(),
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 19092,
                    ..Default::default()
                }],
                ..Default::default()
            }
        } else {
            FindCoordinatorResponse {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: 19092,
                ..Default::default()
            }
        };
        let mut buf = BytesMut::new();
        if version >= find_coordinator_response::FLEXIBLE_MIN {
            buf.extend_from_slice(&[0]);
        }
        resp.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    fn api_versions_response(find_coordinator_version: i16) -> Vec<u8> {
        encode_v0(&ApiVersionsResponse {
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 0,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: find_coordinator_request::API_KEY,
                    min_version: find_coordinator_version,
                    max_version: find_coordinator_version,
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
    }

    fn decode_request_body(body: &[u8], version: i16) -> FindCoordinatorRequest {
        let header_len = 2 + CLIENT_ID.len() + usize::from(version >= 3);
        let mut request_body = &body[header_len..];
        FindCoordinatorRequest::decode(&mut request_body, version).unwrap()
    }

    async fn producer_with_find_coordinator_version(
        version: i16,
        seen: Arc<Mutex<Vec<FindCoordinatorRequest>>>,
    ) -> (MockBroker, Producer) {
        let seen_by_handler = Arc::clone(&seen);
        let find_coordinator_version = version;
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_response(find_coordinator_version));
            }
            if api_key == find_coordinator_request::API_KEY {
                seen_by_handler
                    .lock()
                    .unwrap()
                    .push(decode_request_body(body, version));
                return Some(encode_find_coordinator_response(version));
            }
            None
        })
        .await;
        let producer = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .client_id(CLIENT_ID)
            .enable_idempotence(false)
            .build()
            .await
            .expect("producer connects to mock broker");
        (mock, producer)
    }

    async fn producer_with_flush_timeout(flush_timeout: Duration) -> (MockBroker, Producer) {
        let mock = MockBroker::start(|api_key, _version, _corr_id, _body| {
            (api_key == api_versions_request::API_KEY).then(|| api_versions_response(4))
        })
        .await;
        let producer = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .client_id(CLIENT_ID)
            .enable_idempotence(false)
            .flush_timeout(flush_timeout)
            .build()
            .await
            .expect("producer connects to mock broker");
        (mock, producer)
    }

    #[derive(Clone, Copy)]
    enum LookupKind {
        Group,
        Transaction,
    }

    async fn find_coordinator(producer: &Producer, kind: LookupKind, key: &str) -> String {
        match kind {
            LookupKind::Group => producer.find_group_coordinator(key).await,
            LookupKind::Transaction => producer.find_txn_coordinator(key).await,
        }
        .expect("coordinator is returned")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn find_coordinator_sends_expected_request_for_legacy_and_batched_versions() {
        for (_name, version, kind, key, expected_request) in [
            (
                "legacy group",
                3,
                LookupKind::Group,
                "group-a",
                FindCoordinatorRequest {
                    key: "group-a".into(),
                    ..Default::default()
                },
            ),
            (
                "batched group",
                4,
                LookupKind::Group,
                "group-a",
                FindCoordinatorRequest {
                    coordinator_keys: vec!["group-a".into()],
                    ..Default::default()
                },
            ),
            (
                "legacy transaction",
                3,
                LookupKind::Transaction,
                "txn-a",
                FindCoordinatorRequest {
                    key: "txn-a".into(),
                    key_type: 1,
                    ..Default::default()
                },
            ),
            (
                "batched transaction",
                4,
                LookupKind::Transaction,
                "txn-a",
                FindCoordinatorRequest {
                    key_type: 1,
                    coordinator_keys: vec!["txn-a".into()],
                    ..Default::default()
                },
            ),
        ] {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let (mock, producer) =
                producer_with_find_coordinator_version(version, Arc::clone(&seen)).await;

            let addr = find_coordinator(&producer, kind, key).await;
            assert2::assert!(addr == "127.0.0.1:19092");
            let requests = seen.lock().unwrap();
            assert2::assert!(*requests == vec![expected_request]);
            mock.stop();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn flush_times_out_at_the_configured_deadline() {
        let (mock, producer) = producer_with_flush_timeout(Duration::from_millis(7)).await;
        producer.in_flight.store(1, Ordering::Release);

        let flush = producer.flush();
        tokio::pin!(flush);
        assert!(futures::poll!(flush.as_mut()).is_pending());

        tokio::time::advance(Duration::from_millis(6)).await;
        assert!(futures::poll!(flush.as_mut()).is_pending());

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(matches!(
            futures::poll!(flush.as_mut()),
            std::task::Poll::Ready(Err(ProducerError::FlushTimeout))
        ));
        mock.stop();
    }

    #[tokio::test(start_paused = true)]
    async fn flush_timeout_bounds_a_blocked_force_wake() {
        let (mock, mut producer) = producer_with_flush_timeout(Duration::from_millis(7)).await;
        let (wake_tx, _wake_rx) = tokio::sync::mpsc::channel(16);
        producer.wake_tx = wake_tx;
        for _ in 0..16 {
            producer
                .wake_tx
                .try_send(DrainIntent::Force)
                .expect("wake channel has capacity");
        }

        let flush = producer.flush();
        tokio::pin!(flush);
        assert!(futures::poll!(flush.as_mut()).is_pending());

        tokio::time::advance(Duration::from_millis(6)).await;
        assert!(futures::poll!(flush.as_mut()).is_pending());

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(matches!(
            futures::poll!(flush.as_mut()),
            std::task::Poll::Ready(Err(ProducerError::FlushTimeout))
        ));
        mock.stop();
    }

    #[tokio::test]
    async fn flush_does_not_miss_notification_during_state_check() {
        let (mock, producer) = producer_with_flush_timeout(Duration::from_millis(20)).await;
        let accumulator = Arc::new(tokio::sync::Mutex::new(Accumulator::new(1024)));
        producer
            .accumulators
            .insert(("held".to_owned(), 0), Arc::clone(&accumulator));
        let mut guard = accumulator.lock().await;

        let flush = producer.flush();
        tokio::pin!(flush);
        assert!(futures::poll!(flush.as_mut()).is_pending());

        guard.current = None;
        guard.ready.clear();
        producer.flush_notify.notify_waiters();
        drop(guard);

        tokio::time::timeout(Duration::from_millis(20), flush)
            .await
            .expect("flush must not wait for another notification")
            .expect("empty producer flushes");
        mock.stop();
    }

    /// A `TxnOffsetCommit` answer of several rows gives one code. A code that
    /// no rule retries wins. Among retriable rows, a row that asks for the
    /// coordinator again wins, because Kafka's `TxnOffsetCommitHandler` looks
    /// the coordinator up for the whole request at such a row.
    #[test]
    fn txn_offset_commit_reads_the_code_that_decides_the_answer() {
        use krabka_protocol::owned::txn_offset_commit_response::{
            TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
        };
        let response = |codes: &[i16]| TxnOffsetCommitResponse {
            topics: vec![TxnOffsetCommitResponseTopic {
                name: "topic".into(),
                partitions: codes
                    .iter()
                    .enumerate()
                    .map(|(index, error_code)| TxnOffsetCommitResponsePartition {
                        partition_index: i32::try_from(index).expect("partition index"),
                        error_code: *error_code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };
        for (name, codes, expected) in [
            ("every row committed", &[0, 0][..], 0),
            ("one retriable row", &[0, 14][..], 14),
            ("the coordinator moved", &[14, 16][..], 16),
            ("a timed out row asks for the coordinator", &[3, 7][..], 7),
            ("a code that no rule retries wins", &[16, 48][..], 48),
            ("an abortable code wins", &[14, 22][..], 22),
            ("the first row that resends stands", &[3, 14][..], 3),
        ] {
            assert2::assert!(
                txn_offset_commit_error_code(&response(codes)) == expected,
                "{name}"
            );
        }
    }

    /// One answer of the scripted broker to a `Metadata` request.
    #[derive(Clone, Copy)]
    enum MetadataAnswer {
        /// The topic with this error code and this number of partitions.
        Topic { error_code: i16, partitions: i32 },
        /// No answer, so the request times out.
        Silent,
    }

    /// What one send gave: the delivered partition or the error text, the
    /// partitions of every Produce that the broker received, and the distinct
    /// `Metadata` requests.
    #[derive(Debug, PartialEq, Eq)]
    struct SendOutcome {
        delivered: Result<i32, String>,
        produced_partitions: Vec<i32>,
        metadata_requests: BTreeSet<RequestedTopics>,
    }

    /// The topics that a `Metadata` request names (`None` for all topics),
    /// and its `allow_auto_topic_creation`.
    type RequestedTopics = (Option<Vec<String>>, bool);

    fn requested_topics(body: &[u8], version: i16) -> MetadataRequest {
        let header_len =
            2 + CLIENT_ID.len() + usize::from(version >= metadata_request::FLEXIBLE_MIN);
        let mut request_body = &body[header_len..];
        MetadataRequest::decode(&mut request_body, version).expect("decode Metadata")
    }

    const METADATA_TOPIC: &str = "orders";

    /// Encode `answer`. A broker lists a topic with an error only when the
    /// request names it: a request for all topics gives the topics that exist
    /// and that the client may describe.
    fn metadata_answer(
        version: i16,
        port: u16,
        answer: MetadataAnswer,
        named: bool,
    ) -> Option<Vec<u8>> {
        let MetadataAnswer::Topic {
            error_code,
            partitions,
        } = answer
        else {
            return None;
        };
        let listed = named || error_code == 0;
        let response = MetadataResponse {
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: i32::from(port),
                ..Default::default()
            }],
            topics: listed
                .then(|| MetadataResponseTopic {
                    error_code,
                    name: Some(METADATA_TOPIC.into()),
                    partitions: (0..partitions)
                        .map(|partition_index| MetadataResponsePartition {
                            partition_index,
                            leader_id: 1,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        if version >= metadata_response::FLEXIBLE_MIN {
            buf.extend_from_slice(&[0]);
        }
        response.encode(&mut buf, version).expect("encode Metadata");
        Some(buf.to_vec())
    }

    fn produce_answer(request: &ProduceRequest) -> Vec<u8> {
        let response = ProduceResponse {
            responses: request
                .topic_data
                .iter()
                .map(|topic| TopicProduceResponse {
                    name: topic.name.clone(),
                    partition_responses: topic
                        .partition_data
                        .iter()
                        .map(|partition| PartitionProduceResponse {
                            index: partition.index,
                            base_offset: 0,
                            log_start_offset: -1,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        response
            .encode(&mut buf, PRODUCE_VERSION)
            .expect("encode Produce");
        buf.to_vec()
    }

    const PRODUCE_VERSION: i16 = 3;

    /// Start a broker that answers `Metadata` from `answers` in order, and
    /// repeats the last answer. It answers every Produce with success.
    async fn send_against_scripted_metadata(
        answers: Vec<MetadataAnswer>,
        max_block: Duration,
        record: ProducerRecord,
    ) -> SendOutcome {
        let port = Arc::new(AtomicU16::new(0));
        let handler_port = Arc::clone(&port);
        let metadata_requests = AtomicUsize::new(0);
        let metadata_log = Arc::new(Mutex::new(BTreeSet::new()));
        let handler_metadata_log = Arc::clone(&metadata_log);
        let produce_log = Arc::new(Mutex::new(Vec::new()));
        let handler_produce_log = Arc::clone(&produce_log);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(encode_v0(&ApiVersionsResponse {
                    api_keys: vec![
                        ApiVersion {
                            api_key: metadata_request::API_KEY,
                            min_version: 0,
                            max_version: 12,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: produce_request::API_KEY,
                            min_version: PRODUCE_VERSION,
                            max_version: PRODUCE_VERSION,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }));
            }
            if api_key == metadata_request::API_KEY {
                let index = metadata_requests.fetch_add(1, Ordering::SeqCst);
                let answer = answers[index.min(answers.len() - 1)];
                let request = requested_topics(body, version);
                let topics = request.topics.map(|topics| {
                    topics
                        .into_iter()
                        .map(|topic| topic.name.unwrap_or_default())
                        .collect::<Vec<_>>()
                });
                let named = topics
                    .as_ref()
                    .is_some_and(|topics| topics.iter().any(|topic| topic == METADATA_TOPIC));
                handler_metadata_log
                    .lock()
                    .unwrap()
                    .insert((topics, request.allow_auto_topic_creation));
                return metadata_answer(
                    version,
                    handler_port.load(Ordering::SeqCst),
                    answer,
                    named,
                );
            }
            if api_key == produce_request::API_KEY {
                let mut request_body = &body[2 + CLIENT_ID.len()..];
                let request =
                    ProduceRequest::decode(&mut request_body, version).expect("decode Produce");
                handler_produce_log.lock().unwrap().extend(
                    request
                        .topic_data
                        .iter()
                        .flat_map(|topic| topic.partition_data.iter().map(|p| p.index)),
                );
                return Some(produce_answer(&request));
            }
            None
        })
        .await;
        port.store(mock.addr.port(), Ordering::SeqCst);
        let producer = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .client_id(CLIENT_ID)
            .enable_idempotence(false)
            .request_timeout(Duration::from_millis(300))
            // Without rebootstrap, a silent `Metadata` fails the refresh, and
            // the producer's own wait must ask again.
            .metadata_recovery_strategy(krabka_client_core::MetadataRecoveryStrategy::None)
            .max_block(max_block)
            .build()
            .await
            .expect("producer connects to mock broker");
        let delivered = producer
            .send(record)
            .await
            .await
            .expect("the producer answers the send")
            .map(|metadata| metadata.partition)
            .map_err(|error| error.to_string());
        producer.flush().await.expect("flush");
        let outcome = SendOutcome {
            delivered,
            produced_partitions: produce_log.lock().unwrap().clone(),
            metadata_requests: metadata_log.lock().unwrap().clone(),
        };
        producer.close().await.expect("close producer");
        mock.stop();
        outcome
    }

    /// `send` waits for metadata that holds the topic, as Kafka's
    /// `KafkaProducer.waitOnMetadata` does. It never picks a partition from a
    /// guessed partition count. A send that fails sends no Produce.
    ///
    /// The broker is a real socket, so the test runs in real time: paused
    /// Tokio time moves on while socket I/O is pending. The unit tests of
    /// `metadata_wait` check the backoff and the limit in paused time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_waits_for_topic_metadata_up_to_max_block() {
        const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
        const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
        let keyed = |key: &'static [u8]| ProducerRecord {
            topic: METADATA_TOPIC.into(),
            key: Some(Bytes::from_static(key)),
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        };
        let pinned = |partition: i32| ProducerRecord {
            topic: METADATA_TOPIC.into(),
            partition: Some(partition),
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        };
        let topic = |error_code: i16, partitions: i32| MetadataAnswer::Topic {
            error_code,
            partitions,
        };
        // Kafka's `ProducerMetadata.newMetadataRequestBuilder` names the
        // producer's topics and allows auto topic creation.
        let orders_request = || BTreeSet::from([(Some(vec![METADATA_TOPIC.to_owned()]), true)]);
        let delivered = |partition: i32| SendOutcome {
            delivered: Ok(partition),
            produced_partitions: vec![partition],
            metadata_requests: orders_request(),
        };
        let failed = |error: &str| SendOutcome {
            delivered: Err(error.to_owned()),
            produced_partitions: vec![],
            metadata_requests: orders_request(),
        };
        let default_block = crate::builder::DEFAULT_PRODUCER_MAX_BLOCK;
        let short_block = Duration::from_millis(100);
        let cases = [
            (
                "topic appears on the second refresh",
                vec![topic(UNKNOWN_TOPIC_OR_PARTITION, 0), topic(0, 12)],
                default_block,
                keyed(b"kafka"),
                delivered(partition_for_key(b"kafka", 12)),
            ),
            (
                "topic never appears",
                vec![topic(UNKNOWN_TOPIC_OR_PARTITION, 0)],
                short_block,
                keyed(b"kafka"),
                failed("Topic orders not present in metadata after 100 ms."),
            ),
            (
                "metadata transport error, then the topic",
                vec![MetadataAnswer::Silent, topic(0, 12)],
                default_block,
                keyed(b"my-key"),
                delivered(9),
            ),
            (
                "explicit partition beyond the count",
                vec![topic(0, 4)],
                short_block,
                pinned(7),
                failed(
                    "Partition 7 of topic orders with partition count 4 is not present in metadata after 100 ms.",
                ),
            ),
            (
                "explicit partition after the partition count grows",
                vec![topic(0, 4), topic(0, 12)],
                default_block,
                pinned(7),
                delivered(7),
            ),
            (
                "topic authorization failed fails at once",
                vec![topic(TOPIC_AUTHORIZATION_FAILED, 0)],
                default_block,
                keyed(b"kafka"),
                failed("broker error_code 29"),
            ),
        ];
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, answers, max_block, record, expected) in cases {
            let outcome = send_against_scripted_metadata(answers, max_block, record).await;
            actual.push((name, outcome));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// What a send gave while the buffer memory was full.
    #[derive(Debug, PartialEq, Eq)]
    struct BlockedSendOutcome {
        /// `send` did not return before the broker answered the earlier
        /// batches, or before `max_block` ended.
        blocked: bool,
        /// The outcome of the record: delivered, the error text, or `pending`.
        delivered: Result<(), String>,
    }

    /// Start a broker with the topic `orders` of three partitions. It answers
    /// Produce only while `answer_produce` is set. It gives back the number of
    /// Produce requests that it got.
    async fn three_partition_broker(
        answer_produce: Arc<AtomicBool>,
    ) -> (MockBroker, Arc<AtomicUsize>) {
        let port = Arc::new(AtomicU16::new(0));
        let handler_port = Arc::clone(&port);
        let handler_answer = answer_produce;
        let produce_requests = Arc::new(AtomicUsize::new(0));
        let handler_produce_requests = Arc::clone(&produce_requests);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(encode_v0(&ApiVersionsResponse {
                    api_keys: vec![
                        ApiVersion {
                            api_key: metadata_request::API_KEY,
                            min_version: 0,
                            max_version: 12,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: produce_request::API_KEY,
                            min_version: PRODUCE_VERSION,
                            max_version: PRODUCE_VERSION,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }));
            }
            if api_key == metadata_request::API_KEY {
                return metadata_answer(
                    version,
                    handler_port.load(Ordering::SeqCst),
                    MetadataAnswer::Topic {
                        error_code: 0,
                        partitions: 3,
                    },
                    true,
                );
            }
            if api_key == produce_request::API_KEY {
                handler_produce_requests.fetch_add(1, Ordering::SeqCst);
            }
            if api_key == produce_request::API_KEY && handler_answer.load(Ordering::SeqCst) {
                let mut request_body = &body[2 + CLIENT_ID.len()..];
                let request =
                    ProduceRequest::decode(&mut request_body, version).expect("decode Produce");
                return Some(produce_answer(&request));
            }
            None
        })
        .await;
        port.store(mock.addr.port(), Ordering::SeqCst);
        (mock, produce_requests)
    }

    /// Kafka's `ProducerMetadata.newMetadataRequestBuilder` names the topics
    /// that the producer sends to, with `allowAutoTopicCreation` true. The
    /// periodic and error refreshes of the client use the same request.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_metadata_refreshes_name_the_topics_of_the_sends() {
        let named = |topics: &[&str]| {
            krabka_client_core::topics_request(topics.iter().map(|&t| t.to_owned()), true)
        };
        for (name, sends, expected) in [
            ("no send", vec![], named(&[])),
            ("send to t1", vec!["t1"], named(&["t1"])),
            (
                "send to t1, then t2",
                vec!["t1", "t2", "t1"],
                named(&["t1", "t2"]),
            ),
        ] {
            let port = Arc::new(AtomicU16::new(0));
            let handler_port = Arc::clone(&port);
            let requests = Arc::new(Mutex::new(Vec::new()));
            let handler_requests = Arc::clone(&requests);
            let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(encode_v0(&ApiVersionsResponse {
                        api_keys: vec![ApiVersion {
                            api_key: metadata_request::API_KEY,
                            min_version: 0,
                            max_version: 12,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }));
                }
                if api_key != metadata_request::API_KEY {
                    return None;
                }
                let request = requested_topics(body, version);
                let topics = request.topics.clone().unwrap_or_default();
                handler_requests.lock().unwrap().push(request);
                let response = MetadataResponse {
                    brokers: vec![MetadataResponseBroker {
                        node_id: 1,
                        host: "127.0.0.1".into(),
                        port: i32::from(handler_port.load(Ordering::SeqCst)),
                        ..Default::default()
                    }],
                    topics: topics
                        .into_iter()
                        .map(|topic| MetadataResponseTopic {
                            name: topic.name,
                            partitions: vec![MetadataResponsePartition {
                                leader_id: 1,
                                ..Default::default()
                            }],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                };
                let mut buf = BytesMut::new();
                if version >= metadata_response::FLEXIBLE_MIN {
                    buf.extend_from_slice(&[0]);
                }
                response.encode(&mut buf, version).expect("encode Metadata");
                Some(buf.to_vec())
            })
            .await;
            port.store(mock.addr.port(), Ordering::SeqCst);
            let producer = Producer::builder()
                .bootstrap(mock.addr.to_string())
                .client_id(CLIENT_ID)
                .enable_idempotence(false)
                .max_block(Duration::from_secs(2))
                .build()
                .await
                .expect("producer connects to mock broker");
            for topic in sends {
                producer
                    .partition_count(topic, None)
                    .await
                    .expect("topic metadata");
            }
            producer
                .client
                .refresh_metadata()
                .await
                .expect("refresh metadata");
            let last = requests.lock().unwrap().last().cloned();
            mock.stop();
            drop(producer);
            assert2::check!(last == Some(expected), "{name}");
        }
    }

    /// What a send of one record gave, and the Produce requests that the
    /// broker got.
    #[derive(Debug, PartialEq, Eq)]
    struct ValidatedSend {
        delivered: Result<(), String>,
        produce_requests: usize,
    }

    /// Kafka's `ProducerRecord` constructor rejects a negative timestamp, and
    /// then a negative partition, with `IllegalArgumentException`. The record
    /// never reaches `send`, so no request goes to the broker.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_rejects_a_negative_partition_or_timestamp() {
        let cases = [
            (
                "negative partition",
                Some(-1),
                None,
                ValidatedSend {
                    delivered: Err("Invalid partition: -1. Partition number should always be \
                                    non-negative or null."
                        .to_owned()),
                    produce_requests: 0,
                },
            ),
            (
                "negative timestamp",
                None,
                Some(-5),
                ValidatedSend {
                    delivered: Err("Invalid timestamp: -5. Timestamp should always be \
                                    non-negative or null."
                        .to_owned()),
                    produce_requests: 0,
                },
            ),
            (
                "both negative reports the timestamp first",
                Some(-2),
                Some(-3),
                ValidatedSend {
                    delivered: Err("Invalid timestamp: -3. Timestamp should always be \
                                    non-negative or null."
                        .to_owned()),
                    produce_requests: 0,
                },
            ),
            (
                "partition 0 and timestamp 0",
                Some(0),
                Some(0),
                ValidatedSend {
                    delivered: Ok(()),
                    produce_requests: 1,
                },
            ),
        ];
        for (name, partition, timestamp_ms, expected) in cases {
            let (mock, produce_requests) =
                three_partition_broker(Arc::new(AtomicBool::new(true))).await;
            let producer = Producer::builder()
                .bootstrap(mock.addr.to_string())
                .client_id(CLIENT_ID)
                .enable_idempotence(false)
                .max_block(Duration::from_secs(2))
                .build()
                .await
                .expect("producer connects to mock broker");
            let receiver = producer
                .send(ProducerRecord {
                    topic: METADATA_TOPIC.into(),
                    partition,
                    timestamp_ms,
                    value: Some(Bytes::from_static(b"v")),
                    ..Default::default()
                })
                .await;
            let delivered = tokio::time::timeout(Duration::from_secs(5), receiver)
                .await
                .map_or_else(
                    |_| Err("pending".to_owned()),
                    |answer| {
                        answer
                            .expect("the producer answers the send")
                            .map(drop)
                            .map_err(|error| error.to_string())
                    },
                );
            let actual = ValidatedSend {
                delivered,
                produce_requests: produce_requests.load(Ordering::SeqCst),
            };
            mock.stop();
            drop(producer);
            assert2::assert!(actual == expected, "{name}");
        }
    }

    /// A record that does not fit the current batch closes that batch before
    /// it waits for memory, as Kafka's `RecordAccumulator.append` closes a full
    /// batch. The sender then sends the closed batch before its linger ends,
    /// and the memory it frees serves the new batch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_record_that_does_not_fit_sends_the_full_batch_to_free_memory() {
        let (mock, _) = three_partition_broker(Arc::new(AtomicBool::new(true))).await;
        let producer = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .client_id(CLIENT_ID)
            .enable_idempotence(false)
            .batch_size(64)
            // Kafka's size check counts a record of 40 value bytes as 125
            // bytes, so the memory must hold that. Two batches of 64 bytes
            // do not fit in it, so the second record still waits for memory.
            .buffer_memory(125)
            .linger(Duration::from_secs(30))
            .max_block(Duration::from_secs(2))
            .build()
            .await
            .expect("producer connects to mock broker");
        let record = || ProducerRecord {
            topic: METADATA_TOPIC.into(),
            partition: Some(0),
            value: Some(Bytes::from_static(&[0; 40])),
            ..Default::default()
        };

        let first = producer.send(record()).await;
        let second = producer.send(record()).await;
        let delivered = tokio::time::timeout(Duration::from_secs(5), async {
            let first = first.await.expect("first is resolved").map(drop);
            let second = producer
                .flush()
                .await
                .and(second.await.expect("second is resolved").map(drop));
            (
                first.map_err(|e| e.to_string()),
                second.map_err(|e| e.to_string()),
            )
        })
        .await;
        mock.stop();
        drop(producer);

        assert2::assert!(delivered == Ok((Ok(()), Ok(()))));
    }

    /// The limits of one row of
    /// `send_fails_a_record_larger_than_max_request_size_or_buffer_memory`.
    struct SizeLimits {
        max_request_size: Option<usize>,
        buffer_memory: usize,
    }

    /// What one send of a large record gave.
    #[derive(Debug, PartialEq, Eq)]
    struct LargeRecordOutcome {
        /// The value bytes of the record.
        value_bytes: usize,
        /// Delivered, or the error text.
        delivered: Result<(), String>,
        /// The Produce requests that the broker got for the record.
        produce_requests: usize,
    }

    /// Kafka's `KafkaProducer.ensureValidRecordSize` fails a record whose
    /// serialized size is larger than `max.request.size`, and then one that
    /// is larger than `buffer.memory`, before the accumulator. The size is
    /// Kafka's upper bound: 61 bytes of batch header, 21 bytes of record
    /// overhead, one byte for the null key, the varint length and the value,
    /// and one byte for the header count.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_fails_a_record_larger_than_max_request_size_or_buffer_memory() {
        const MIB: usize = 1024 * 1024;
        let max_request_error = |size: usize, limit: usize| {
            format!(
                "The message is {size} bytes when serialized which is larger than {limit}, \
                 which is the value of the max_request_size configuration."
            )
        };
        let buffer_memory_error = |size: usize| {
            format!(
                "The message is {size} bytes when serialized which is larger than the total \
                 memory buffer you have configured with the buffer_memory configuration."
            )
        };
        let default_limits = || SizeLimits {
            max_request_size: None,
            buffer_memory: 32 * MIB,
        };
        // A value of 1048489 bytes has a 3-byte varint length, so the record
        // is 87 + 1048489 = 1048576 bytes. A value of 938 bytes has a 2-byte
        // varint length, so the record is 86 + 938 = 1024 bytes.
        let cases = [
            (
                "record of max_request_size bytes, default limit",
                default_limits(),
                1_048_489,
                LargeRecordOutcome {
                    value_bytes: 1_048_489,
                    delivered: Ok(()),
                    produce_requests: 1,
                },
            ),
            (
                "one byte over max_request_size, default limit",
                default_limits(),
                1_048_490,
                LargeRecordOutcome {
                    value_bytes: 1_048_490,
                    delivered: Err(max_request_error(1_048_577, MIB)),
                    produce_requests: 0,
                },
            ),
            (
                "record of buffer_memory bytes",
                SizeLimits {
                    max_request_size: None,
                    buffer_memory: 1024,
                },
                938,
                LargeRecordOutcome {
                    value_bytes: 938,
                    delivered: Ok(()),
                    produce_requests: 1,
                },
            ),
            (
                "one byte over buffer_memory",
                SizeLimits {
                    max_request_size: None,
                    buffer_memory: 1024,
                },
                939,
                LargeRecordOutcome {
                    value_bytes: 939,
                    delivered: Err(buffer_memory_error(1025)),
                    produce_requests: 0,
                },
            ),
            (
                "over both limits reports max_request_size first",
                SizeLimits {
                    max_request_size: Some(500),
                    buffer_memory: 400,
                },
                939,
                LargeRecordOutcome {
                    value_bytes: 939,
                    delivered: Err(max_request_error(1025, 500)),
                    produce_requests: 0,
                },
            ),
            (
                "a larger max_request_size accepts a larger record",
                SizeLimits {
                    max_request_size: Some(2 * MIB),
                    buffer_memory: 32 * MIB,
                },
                1_048_490,
                LargeRecordOutcome {
                    value_bytes: 1_048_490,
                    delivered: Ok(()),
                    produce_requests: 1,
                },
            ),
        ];

        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, limits, value_bytes, expected) in cases {
            let (mock, produce_count) =
                three_partition_broker(Arc::new(AtomicBool::new(true))).await;
            let producer = Producer::builder()
                .bootstrap(mock.addr.to_string())
                .client_id(CLIENT_ID)
                .enable_idempotence(false)
                .batch_size(64)
                .buffer_memory(limits.buffer_memory)
                .maybe_max_request_size(limits.max_request_size)
                .build()
                .await
                .expect("producer connects to mock broker");
            let delivered = producer
                .send(ProducerRecord {
                    topic: METADATA_TOPIC.into(),
                    partition: Some(0),
                    value: Some(Bytes::from(vec![0; value_bytes])),
                    ..Default::default()
                })
                .await
                .await
                .expect("the producer answers the send")
                .map(drop)
                .map_err(|error| error.to_string());
            mock.stop();
            drop(producer);
            actual.push((
                name,
                LargeRecordOutcome {
                    value_bytes,
                    delivered,
                    produce_requests: produce_count.load(Ordering::SeqCst),
                },
            ));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// Fill the buffer memory with two batches of 16 MiB for partitions 0 and
    /// 1, which the broker does not answer, and send a third record to
    /// partition 2. When `answer_after` is set, the broker starts to answer
    /// Produce after that time, so the first batches complete and free their
    /// memory.
    async fn send_with_full_buffer_memory(
        answer_after: Option<Duration>,
        max_block: Duration,
    ) -> BlockedSendOutcome {
        const BATCH_BYTES: usize = 16 * 1024 * 1024;
        let answer_produce = Arc::new(AtomicBool::new(false));
        let (mock, _) = three_partition_broker(Arc::clone(&answer_produce)).await;
        let producer = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .client_id(CLIENT_ID)
            .enable_idempotence(false)
            .batch_size(BATCH_BYTES)
            .request_timeout(Duration::from_millis(300))
            .retry_backoff(Duration::from_millis(20))
            .max_block(max_block)
            .build()
            .await
            .expect("producer connects to mock broker");
        let pinned = |partition: i32| ProducerRecord {
            topic: METADATA_TOPIC.into(),
            partition: Some(partition),
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        };
        let _first = producer.send(pinned(0)).await;
        let _second = producer.send(pinned(1)).await;
        if let Some(delay) = answer_after {
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                answer_produce.store(true, Ordering::SeqCst);
            });
        }
        let started = std::time::Instant::now();
        let mut third = producer.send(pinned(2)).await;
        let blocked = started.elapsed() >= Duration::from_millis(80);
        let delivered = match third.try_recv() {
            Ok(result) => result.map(drop).map_err(|error| error.to_string()),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => Err("closed".to_owned()),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                match tokio::time::timeout(Duration::from_secs(3), third).await {
                    Ok(Ok(result)) => result.map(drop).map_err(|error| error.to_string()),
                    Ok(Err(_)) => Err("closed".to_owned()),
                    Err(_) => Err("pending".to_owned()),
                }
            }
        };
        mock.stop();
        drop(producer);
        BlockedSendOutcome { blocked, delivered }
    }

    /// Kafka's `BufferPool.allocate` blocks a send while `buffer.memory` is in
    /// use, for at most the rest of `max.block.ms`. It fails the record with
    /// `BufferExhaustedException` when that time ends.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_waits_for_buffer_memory_up_to_max_block() {
        let cases = [
            (
                "memory stays full",
                None,
                Duration::from_millis(200),
                BlockedSendOutcome {
                    blocked: true,
                    delivered: Err(
                        "Failed to allocate 16777216 bytes within the configured max \
                                    blocking time 200 ms. Total memory: 33554432 bytes. Available \
                                    memory: 0 bytes. Poolable size: 16777216 bytes"
                            .to_owned(),
                    ),
                },
            ),
            (
                "earlier batches complete",
                Some(Duration::from_millis(100)),
                Duration::from_secs(5),
                BlockedSendOutcome {
                    blocked: true,
                    delivered: Ok(()),
                },
            ),
        ];
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, answer_after, max_block, expected) in cases {
            actual.push((
                name,
                send_with_full_buffer_memory(answer_after, max_block).await,
            ));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// The producer sends a coordinator request again only when the request
    /// can have reached the broker. A failure before the send repeats on every
    /// attempt.
    #[test]
    fn only_a_transport_failure_can_have_reached_the_broker() {
        use krabka_client_core::ClientError;
        use krabka_units::secs;
        for (name, error, expected) in [
            ("closed connection", ClientError::Disconnected, true),
            ("request timeout", ClientError::Timeout(secs(1)), true),
            (
                "io failure",
                ClientError::Io(std::io::Error::other("broken pipe")),
                true,
            ),
            (
                "incompatible version",
                ClientError::IncompatibleVersion {
                    api_key: 25,
                    broker_min: 4,
                    broker_max: 5,
                    client_min: 0,
                    client_max: 3,
                },
                false,
            ),
            (
                "no coordinator",
                ClientError::NoCoordinator {
                    key: "group-a".into(),
                },
                false,
            ),
        ] {
            assert2::assert!(
                request_may_have_reached_the_broker(&error) == expected,
                "{name}"
            );
        }
    }

    /// Start a broker with the topic `orders`, whose partition `p` has the
    /// leader `leaders[p]`. A leader of -1 means no leader. It answers every
    /// Produce with success, and logs the partition of each Produce.
    async fn leader_broker(leaders: &'static [i32]) -> (MockBroker, Arc<Mutex<Vec<i32>>>) {
        let port = Arc::new(AtomicU16::new(0));
        let handler_port = Arc::clone(&port);
        let produce_log = Arc::new(Mutex::new(Vec::new()));
        let handler_produce_log = Arc::clone(&produce_log);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(encode_v0(&ApiVersionsResponse {
                    api_keys: vec![
                        ApiVersion {
                            api_key: metadata_request::API_KEY,
                            min_version: 0,
                            max_version: 12,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: produce_request::API_KEY,
                            min_version: PRODUCE_VERSION,
                            max_version: PRODUCE_VERSION,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }));
            }
            if api_key == metadata_request::API_KEY {
                let response = MetadataResponse {
                    brokers: vec![MetadataResponseBroker {
                        node_id: 1,
                        host: "127.0.0.1".into(),
                        port: i32::from(handler_port.load(Ordering::SeqCst)),
                        ..Default::default()
                    }],
                    topics: vec![MetadataResponseTopic {
                        name: Some(METADATA_TOPIC.into()),
                        partitions: (0..)
                            .zip(leaders)
                            .map(|(partition_index, &leader_id)| MetadataResponsePartition {
                                partition_index,
                                leader_id,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                let mut buf = BytesMut::new();
                if version >= metadata_response::FLEXIBLE_MIN {
                    buf.extend_from_slice(&[0]);
                }
                response.encode(&mut buf, version).expect("encode Metadata");
                return Some(buf.to_vec());
            }
            if api_key == produce_request::API_KEY {
                let mut request_body = &body[2 + CLIENT_ID.len()..];
                let request =
                    ProduceRequest::decode(&mut request_body, version).expect("decode Produce");
                handler_produce_log.lock().unwrap().extend(
                    request
                        .topic_data
                        .iter()
                        .flat_map(|topic| topic.partition_data.iter().map(|p| p.index)),
                );
                return Some(produce_answer(&request));
            }
            None
        })
        .await;
        port.store(mock.addr.port(), Ordering::SeqCst);
        (mock, produce_log)
    }

    /// Where the keyless records of one row went.
    #[derive(Debug, PartialEq, Eq)]
    struct KeylessSpread {
        /// The number of distinct partitions of the keyless records.
        keyless_partitions: usize,
        /// A keyless record went to a partition with no leader.
        keyless_on_a_partition_without_leader: bool,
        /// The partition of the keyed record of the row, if it has one.
        keyed_partition: Option<i32>,
        /// Each Produce carried one record.
        produce_requests: usize,
    }

    /// Kafka's `BuiltInPartitioner` (KIP-480, KIP-794) keeps keyless records
    /// on one partition until about `batch.size` bytes go there. A drain does
    /// not move it, a keyed record does not move it, and it picks only
    /// partitions with a leader. Each record here waits for its ack, so each
    /// drain sends one record, and the records are much smaller than
    /// `batch.size`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keyless_records_stay_on_one_partition_with_a_leader() {
        const KEYED: &[u8] = b"kafka";
        let mut actual_rows = Vec::new();
        let mut expected_rows = Vec::new();
        for (name, leaders, keyed_at, expected) in [
            (
                "drains do not move the sticky partition",
                &[1, 1, 1, 1][..],
                None,
                KeylessSpread {
                    keyless_partitions: 1,
                    keyless_on_a_partition_without_leader: false,
                    keyed_partition: None,
                    produce_requests: 6,
                },
            ),
            (
                "partitions without a leader are skipped",
                &[1, -1, -1, 1][..],
                None,
                KeylessSpread {
                    keyless_partitions: 1,
                    keyless_on_a_partition_without_leader: false,
                    keyed_partition: None,
                    produce_requests: 6,
                },
            ),
            (
                "a keyed record does not move the sticky partition",
                &[1, 1, 1, 1][..],
                Some(2),
                KeylessSpread {
                    keyless_partitions: 1,
                    keyless_on_a_partition_without_leader: false,
                    keyed_partition: Some(partition_for_key(KEYED, 4)),
                    produce_requests: 6,
                },
            ),
        ] {
            let (mock, produce_log) = leader_broker(leaders).await;
            let producer = Producer::builder()
                .bootstrap(mock.addr.to_string())
                .client_id(CLIENT_ID)
                .enable_idempotence(false)
                .linger(Duration::ZERO)
                .build()
                .await
                .expect("producer connects to mock broker");
            let mut keyless = BTreeSet::new();
            let mut keyed_partition = None;
            for index in 0..6 {
                let key = (keyed_at == Some(index)).then_some(Bytes::from_static(KEYED));
                let partition = producer
                    .send(ProducerRecord {
                        topic: METADATA_TOPIC.into(),
                        key: key.clone(),
                        value: Some(Bytes::from_static(b"value")),
                        ..Default::default()
                    })
                    .await
                    .await
                    .expect("the producer answers the send")
                    .expect("the record is delivered")
                    .partition;
                if key.is_some() {
                    keyed_partition = Some(partition);
                } else {
                    keyless.insert(partition);
                }
            }
            let actual = KeylessSpread {
                keyless_partitions: keyless.len(),
                keyless_on_a_partition_without_leader: keyless.iter().any(|&partition| {
                    usize::try_from(partition).is_ok_and(|index| leaders[index] < 0)
                }),
                keyed_partition,
                produce_requests: produce_log.lock().unwrap().len(),
            };
            producer.close().await.expect("close producer");
            mock.stop();
            actual_rows.push((name, actual));
            expected_rows.push((name, expected));
        }
        assert2::assert!(actual_rows == expected_rows);
    }
}
