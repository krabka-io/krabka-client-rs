//! Client-side transactional state machine. It drives the
//! `init_transactions` / `begin` / `commit` / `abort` / `send_offsets_to_transaction`
//! flow.

use std::{
    fmt,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
};

use crate::{error::ProducerError, producer::Producer};

/// The slot holds no abortable error. An error code fits in an `i16`, and
/// `-1` is `UNKNOWN_SERVER_ERROR`, so the empty value is outside that range.
const NO_ABORTABLE_ERROR: i32 = i32::MIN;

/// The sentinel that marks the slot as holding a timeout rather than a
/// broker error code. It is one below `NO_ABORTABLE_ERROR`
/// (`i32::MIN`), so it stays outside the `i16` code range and cannot
/// collide with a real, sign-extended code.
const ABORTABLE_TIMEOUT: i32 = i32::MIN + 1;

/// The error that makes the current transaction abort-only.
///
/// Apache Kafka's `TransactionManager` moves to the `ABORTABLE_ERROR` state
/// when a batch of the transaction fails, or when a coordinator answers with a
/// code that only an abort can clear (`abortableError`). Every later
/// `commitTransaction` then fails with the stored error
/// (`maybeFailWithError`), and only `abortTransaction` clears the state. Kafka
/// stores the raised exception itself, which is either a broker error code
/// wrapped in a `KafkaException`, or a client-side `TimeoutException` that
/// carries no code (a batch that ran out of retries or its routing budget
/// with no broker answer).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AbortableError {
    /// A coordinator, or a Produce response, answered with this code.
    Server(i16),
    /// A batch of the transaction was never acknowledged, and carries no
    /// broker code. Kafka raises `TimeoutException` for the same case.
    Timeout,
}

/// The error code that makes the current transaction abort-only.
///
/// The sender task writes this slot from a synchronous context, so it holds an
/// atomic rather than a mutex.
#[derive(Debug)]
pub(crate) struct AbortableErrorSlot(AtomicI32);

impl Default for AbortableErrorSlot {
    fn default() -> Self {
        Self(AtomicI32::new(NO_ABORTABLE_ERROR))
    }
}

impl AbortableErrorSlot {
    /// Store `code` as the error that the application must abort. A later
    /// error replaces an earlier one, as Kafka's `transitionTo` replaces
    /// `lastError`.
    pub(crate) fn set(&self, code: i16) {
        self.0.store(i32::from(code), Ordering::Release);
    }

    /// Store that the transaction can no longer commit because a batch of it
    /// timed out with no broker code. A later error replaces an earlier one,
    /// same as [`Self::set`].
    pub(crate) fn set_timeout(&self) {
        self.0.store(ABORTABLE_TIMEOUT, Ordering::Release);
    }

    /// The stored error, or `None` when the transaction can still commit.
    pub(crate) fn get(&self) -> Option<AbortableError> {
        match self.0.load(Ordering::Acquire) {
            NO_ABORTABLE_ERROR => None,
            ABORTABLE_TIMEOUT => Some(AbortableError::Timeout),
            code => Some(AbortableError::Server(
                i16::try_from(code).unwrap_or(i16::MIN),
            )),
        }
    }

    /// Forget the stored error. An abort and a new producer identity both
    /// clear it, as Kafka's `resetTransactionState` and `InitProducerIdHandler`
    /// clear `lastError`.
    pub(crate) fn clear(&self) {
        self.0.store(NO_ABORTABLE_ERROR, Ordering::Release);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum TxnState {
    /// The caller has not yet called `init_transactions`.
    Uninitialized,
    /// `init_transactions` succeeded, and no txn is in flight.
    Ready,
    /// Inside `begin_transaction` ... `commit/abort`.
    InTransaction,
    /// `prepare_transaction` has stopped new writes and is flushing records.
    Preparing,
    /// A 2PC transaction has been flushed and awaits its external decision.
    Prepared,
    /// `init_transactions_with_keep_prepared` is in flight.
    Initializing,
    /// A `commit` or an `abort` is in progress.
    CommittingOrAborting,
    /// A guard was dropped or `EndTxn` had an uncertain transport outcome.
    /// `init_transactions` must establish a new epoch before reuse.
    RecoveryRequired,
    /// The producer is fenced. No further txn is possible without a
    /// re-init.
    Fenced,
}

/// Stable identity of a transaction prepared for external two-phase commit.
///
/// Its string form is Kafka-compatible: `"producer_id:producer_epoch"`.
/// The empty string represents no transaction and round-trips through
/// [`Default`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PreparedTransactionState {
    producer_id: i64,
    producer_epoch: i16,
}

impl PreparedTransactionState {
    /// Creates a prepared transaction identity.
    ///
    /// # Errors
    ///
    /// Returns [`PreparedTransactionStateParseError`] when either identity
    /// component is negative.
    pub fn new(
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<Self, PreparedTransactionStateParseError> {
        if producer_id < 0 || producer_epoch < 0 {
            return Err(PreparedTransactionStateParseError);
        }
        Ok(Self {
            producer_id,
            producer_epoch,
        })
    }

    /// Producer ID of the prepared transaction.
    #[must_use]
    pub const fn producer_id(self) -> i64 {
        self.producer_id
    }

    /// Producer epoch of the prepared transaction.
    #[must_use]
    pub const fn producer_epoch(self) -> i16 {
        self.producer_epoch
    }

    /// Whether this token identifies a real transaction.
    #[must_use]
    pub const fn has_transaction(self) -> bool {
        self.producer_id >= 0
    }
}

impl Default for PreparedTransactionState {
    fn default() -> Self {
        Self {
            producer_id: -1,
            producer_epoch: -1,
        }
    }
}

impl fmt::Display for PreparedTransactionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.has_transaction() {
            write!(formatter, "{}:{}", self.producer_id, self.producer_epoch)
        } else {
            Ok(())
        }
    }
}

impl FromStr for PreparedTransactionState {
    type Err = PreparedTransactionStateParseError;

    fn from_str(serialized: &str) -> Result<Self, Self::Err> {
        if serialized.is_empty() {
            return Ok(Self::default());
        }
        let (producer_id, producer_epoch) = serialized
            .split_once(':')
            .ok_or(PreparedTransactionStateParseError)?;
        if producer_epoch.contains(':') {
            return Err(PreparedTransactionStateParseError);
        }
        Self::new(
            producer_id
                .parse()
                .map_err(|_| PreparedTransactionStateParseError)?,
            producer_epoch
                .parse()
                .map_err(|_| PreparedTransactionStateParseError)?,
        )
    }
}

/// Error returned when a prepared transaction token is malformed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid prepared transaction state; expected producer_id:producer_epoch")]
pub struct PreparedTransactionStateParseError;

/// An open transaction, borrowing the [`Producer`] that opened it.
///
/// [`Producer::begin_transaction`] returns it. [`commit`](Self::commit) and
/// [`abort`](Self::abort) each consume `self` on success, so a transaction
/// cannot be silently reused or finished twice.
///
/// On failure the call hands the guard back through
/// [`EndTransactionError::transaction`] instead of dropping it. Kafka's
/// `EndTxn` contract makes some failures, such as `CONCURRENT_TRANSACTIONS`,
/// retryable against the very same broker-side transaction, so the caller can
/// retry `commit()` on the returned guard, or switch to `abort()`. For
/// non-retryable failures the producer's transaction state has already moved
/// on, and the returned guard's next `commit` or `abort` attempt fails
/// immediately.
///
/// A dropped unresolved guard marks the producer as recovery-required. The
/// producer never guesses whether Kafka committed or aborted the transaction.
/// The caller must call `init_transactions` before the producer can send or
/// begin again.
#[derive(Debug)]
#[must_use = "a transaction must be finished with `commit()` or `abort()`"]
pub struct Transaction<'p> {
    pub(crate) producer: &'p Producer,
    pub(crate) finished: bool,
    pub(crate) guard_generation: u64,
}

impl Transaction<'_> {
    /// Flushes and prepares this transaction for external two-phase commit.
    ///
    /// # Errors
    ///
    /// See [`Producer::prepare_transaction`].
    pub async fn prepare(&self) -> Result<PreparedTransactionState, ProducerError> {
        self.producer.prepare_transaction().await
    }

    /// Commit this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    /// On failure, `self` is returned via [`EndTransactionError::transaction`]
    /// so a retryable failure can be retried or aborted.
    pub async fn commit(mut self) -> Result<(), EndTransactionError<Self>> {
        match self.producer.end_transaction(true).await {
            Ok(()) => {
                self.finished = true;
                Ok(())
            }
            Err(source) => Err(EndTransactionError {
                transaction: self,
                source,
            }),
        }
    }

    /// Abort this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    /// On failure, `self` is returned via [`EndTransactionError::transaction`]
    /// so a retryable failure can be retried or aborted.
    pub async fn abort(mut self) -> Result<(), EndTransactionError<Self>> {
        match self.producer.end_transaction(false).await {
            Ok(()) => {
                self.finished = true;
                Ok(())
            }
            Err(source) => Err(EndTransactionError {
                transaction: self,
                source,
            }),
        }
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.producer
                .abandon_transaction_guard(self.guard_generation);
        }
    }
}

/// Same contract as [`Transaction`], but owns an `Arc<Producer>` instead of
/// borrowing it.
///
/// Use it when the caller must hold the guard across an owned or `'static`
/// boundary that a borrow cannot survive, for example behind a `dyn Trait`
/// object stored in a struct field across many separate async calls.
/// [`Producer::begin_transaction_owned`] returns it. It mirrors
/// `tokio::sync::Mutex::{lock, lock_owned}` and `MutexGuard`/`OwnedMutexGuard`.
#[derive(Debug)]
#[must_use = "a transaction must be finished with `commit()` or `abort()`"]
pub struct OwnedTransaction {
    pub(crate) producer: Arc<Producer>,
    pub(crate) finished: bool,
    pub(crate) guard_generation: u64,
}

impl OwnedTransaction {
    /// Flushes and prepares this transaction for external two-phase commit.
    ///
    /// # Errors
    ///
    /// See [`Producer::prepare_transaction`].
    pub async fn prepare(&self) -> Result<PreparedTransactionState, ProducerError> {
        self.producer.prepare_transaction().await
    }

    /// Commit this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    /// On failure, `self` is returned via [`EndTransactionError::transaction`]
    /// so a retryable failure can be retried or aborted.
    pub async fn commit(mut self) -> Result<(), EndTransactionError<Self>> {
        match self.producer.end_transaction(true).await {
            Ok(()) => {
                self.finished = true;
                Ok(())
            }
            Err(source) => Err(EndTransactionError {
                transaction: self,
                source,
            }),
        }
    }

    /// Abort this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    /// On failure, `self` is returned via [`EndTransactionError::transaction`]
    /// so a retryable failure can be retried or aborted.
    pub async fn abort(mut self) -> Result<(), EndTransactionError<Self>> {
        match self.producer.end_transaction(false).await {
            Ok(()) => {
                self.finished = true;
                Ok(())
            }
            Err(source) => Err(EndTransactionError {
                transaction: self,
                source,
            }),
        }
    }
}

impl Drop for OwnedTransaction {
    fn drop(&mut self) {
        if !self.finished {
            self.producer
                .abandon_transaction_guard(self.guard_generation);
        }
    }
}

/// Error returned by [`Transaction::commit`], [`abort`](Transaction::abort),
/// and the [`OwnedTransaction`] equivalents.
///
/// It carries the guard back, so the caller can retry or abort a retryable
/// failure, such as `CONCURRENT_TRANSACTIONS`, on the same underlying
/// transaction instead of stranding it.
#[derive(Debug, thiserror::Error)]
#[error("{source}")]
pub struct EndTransactionError<T> {
    /// The guard the `commit` or `abort` call was made on, handed back so the
    /// caller can retry `commit()` or call `abort()` on the same
    /// transaction.
    pub transaction: T,
    /// The underlying failure.
    #[source]
    pub source: ProducerError,
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicI16, AtomicU16, Ordering},
        },
        time::Duration,
    };

    use bytes::BytesMut;
    use krabka_client_core::{ClientError, MockBroker};
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            add_offsets_to_txn_request,
            add_offsets_to_txn_response::AddOffsetsToTxnResponse,
            add_partitions_to_txn_request,
            add_partitions_to_txn_response::{self, AddPartitionsToTxnResponse},
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            common::add_partitions_to_txn_response::{
                add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult,
                add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
            },
            end_txn_request,
            end_txn_response::{self, EndTxnResponse},
            find_coordinator_request,
            find_coordinator_response::FindCoordinatorResponse,
            init_producer_id_request::{self, InitProducerIdRequest},
            init_producer_id_response::{self, InitProducerIdResponse},
            metadata_request,
            metadata_response::{
                self, MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
                MetadataResponseTopic,
            },
            produce_request,
            produce_response::{
                self, PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
            },
            txn_offset_commit_request,
            txn_offset_commit_response::{
                TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition,
                TxnOffsetCommitResponseTopic,
            },
        },
    };

    use super::{AbortableError, PreparedTransactionState, TxnState};
    use crate::{ProducerRecord, error::ProducerError, producer::Producer};

    #[test]
    fn prepared_transaction_state_has_stable_string_round_trip() {
        let state = PreparedTransactionState::new(42, 7).expect("valid transaction identity");

        assert2::assert!(state.producer_id() == 42);
        assert2::assert!(state.producer_epoch() == 7);
        assert2::assert!(state.has_transaction());
        assert2::assert!(state.to_string() == "42:7");
        assert2::assert!("42:7".parse::<PreparedTransactionState>() == Ok(state));
        assert2::assert!(PreparedTransactionState::default().to_string().is_empty());
        assert2::assert!(
            "".parse::<PreparedTransactionState>() == Ok(PreparedTransactionState::default())
        );
    }

    #[test]
    fn prepared_transaction_state_rejects_malformed_or_negative_identity() {
        for serialized in ["42", "-1:0", "1:-1", "1:2:3", "a:2", "1:b"] {
            assert2::assert!(serialized.parse::<PreparedTransactionState>().is_err());
        }
    }

    /// Tell if an `InitProducerId` request body carries a `transactional.id`.
    ///
    /// The builder of an idempotent producer sends one request without it, and
    /// `init_transactions` sends the requests with it. The body starts with the
    /// request header client id, and a flexible version adds a tagged-field
    /// byte after it.
    fn is_transactional_init(body: &[u8], version: i16) -> bool {
        let client_id_len = usize::try_from(i16::from_be_bytes([body[0], body[1]]).max(0))
            .expect("non-negative client id length");
        let flexible = usize::from(version >= init_producer_id_request::FLEXIBLE_MIN);
        let mut request = &body[2 + client_id_len + flexible..];
        InitProducerIdRequest::decode(&mut request, version)
            .expect("decode InitProducerId")
            .transactional_id
            .is_some()
    }

    fn encode_v0(resp: &impl Encode) -> Vec<u8> {
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    /// Boots a mock broker that also answers as its own transaction
    /// coordinator, so `FindCoordinator` resolves back to the mock's own
    /// address. It returns a transactional `Producer` with `init_transactions`
    /// already completed against that broker.
    ///
    /// `end_txn_error` lets each test steer the `error_code` of the `EndTxn`
    /// response independently per call, where 0 means success. A test can
    /// therefore fail a `commit` or `abort`, and then flip the mock so that a
    /// retry on the same guard succeeds.
    async fn transactional_producer(end_txn_error: Arc<AtomicI16>) -> (MockBroker, Producer) {
        transactional_producer_with_end_txn_timeout(end_txn_error, Arc::new(AtomicBool::new(false)))
            .await
    }

    #[tokio::test]
    async fn init_transactions_retries_with_configured_policy() {
        let port_cell = Arc::new(AtomicU16::new(0));
        let handler_port = Arc::clone(&port_cell);
        let attempts = Arc::new(AtomicU16::new(0));
        let observed = Arc::clone(&attempts);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(encode_v0(&ApiVersionsResponse::default()));
            }
            if api_key == find_coordinator_request::API_KEY {
                return Some(encode_v0(&FindCoordinatorResponse {
                    error_code: 0,
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: i32::from(handler_port.load(Ordering::SeqCst)),
                    ..Default::default()
                }));
            }
            if api_key == init_producer_id_request::API_KEY {
                if !is_transactional_init(body, version) {
                    return Some(encode_v0(&InitProducerIdResponse {
                        producer_id: 1,
                        ..Default::default()
                    }));
                }
                let attempt = observed.fetch_add(1, Ordering::SeqCst);
                return Some(encode_v0(&InitProducerIdResponse {
                    error_code: if attempt == 0 { 14 } else { 0 },
                    producer_id: 7,
                    producer_epoch: 3,
                    ..Default::default()
                }));
            }
            None
        })
        .await;
        port_cell.store(mock.addr.port(), Ordering::SeqCst);

        let producer = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .transactional_id("test-txn")
            .request_timeout(Duration::from_millis(100))
            .retry_backoff(Duration::from_millis(1))
            .init_retry_timeout(Duration::from_millis(100))
            .retry_backoff_max(Duration::from_millis(1))
            .build()
            .await
            .expect("producer connects");

        producer
            .init_transactions()
            .await
            .expect("cold coordinator retry succeeds");
        assert2::assert!(attempts.load(Ordering::SeqCst) == 2);
        mock.stop();
    }

    async fn transactional_producer_with_end_txn_timeout(
        end_txn_error: Arc<AtomicI16>,
        end_txn_silent: Arc<AtomicBool>,
    ) -> (MockBroker, Producer) {
        transactional_producer_configured(end_txn_error, end_txn_silent, false).await
    }

    async fn transactional_producer_configured(
        end_txn_error: Arc<AtomicI16>,
        end_txn_silent: Arc<AtomicBool>,
        two_phase_commit_enabled: bool,
    ) -> (MockBroker, Producer) {
        let port_cell = Arc::new(AtomicU16::new(0));
        let handler_port = port_cell.clone();
        let next_epoch = Arc::new(AtomicI16::new(3));
        let handler_epoch = Arc::clone(&next_epoch);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(encode_v0(&ApiVersionsResponse {
                    api_keys: vec![ApiVersion {
                        api_key: init_producer_id_request::API_KEY,
                        min_version: 0,
                        max_version: if two_phase_commit_enabled { 6 } else { 0 },
                        ..Default::default()
                    }],
                    ..Default::default()
                }));
            }
            if api_key == find_coordinator_request::API_KEY {
                return Some(encode_v0(&FindCoordinatorResponse {
                    error_code: 0,
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: i32::from(handler_port.load(Ordering::SeqCst)),
                    ..Default::default()
                }));
            }
            if api_key == init_producer_id_request::API_KEY {
                let response = if is_transactional_init(body, version) {
                    InitProducerIdResponse {
                        error_code: 0,
                        producer_id: 7,
                        producer_epoch: handler_epoch.fetch_add(1, Ordering::SeqCst),
                        ..Default::default()
                    }
                } else {
                    InitProducerIdResponse {
                        producer_id: 1,
                        ..Default::default()
                    }
                };
                let mut buf = BytesMut::new();
                if version >= init_producer_id_response::FLEXIBLE_MIN {
                    buf.extend_from_slice(&[0]);
                }
                response.encode(&mut buf, version).unwrap();
                return Some(buf.to_vec());
            }
            if api_key == end_txn_request::API_KEY {
                if end_txn_silent.load(Ordering::SeqCst) {
                    return None;
                }
                return Some(encode_v0(&EndTxnResponse {
                    error_code: end_txn_error.load(Ordering::SeqCst),
                    ..Default::default()
                }));
            }
            None
        })
        .await;
        port_cell.store(mock.addr.port(), Ordering::SeqCst);

        let producer = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .transactional_id("test-txn")
            .transaction_two_phase_commit_enable(two_phase_commit_enabled)
            .request_timeout(std::time::Duration::from_millis(100))
            .retry_backoff(Duration::from_millis(1))
            .init_retry_timeout(Duration::from_millis(400))
            .retry_backoff_max(Duration::from_millis(20))
            .build()
            .await
            .expect("producer connects to the mock");
        producer
            .init_transactions()
            .await
            .expect("init_transactions against the mock coordinator");
        (mock, producer)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prepared_guard_can_drop_without_poisoning_the_next_transaction() {
        let end_txn_error = Arc::new(AtomicI16::new(0));
        let (mock, producer) = transactional_producer_configured(
            end_txn_error,
            Arc::new(AtomicBool::new(false)),
            true,
        )
        .await;
        let transaction = producer
            .begin_transaction()
            .await
            .expect("begin transaction");
        let prepared = transaction.prepare().await.expect("prepare transaction");

        assert2::assert!(*producer.txn_state.lock().await == TxnState::Prepared);
        drop(transaction);
        assert2::assert!(!producer.txn_recovery_required.load(Ordering::Acquire));
        producer
            .complete_transaction(prepared)
            .await
            .expect("complete prepared transaction");
        producer
            .begin_transaction()
            .await
            .expect("begin next transaction")
            .abort()
            .await
            .expect("abort next transaction");
        mock.stop();
    }

    macro_rules! end_txn_retry_test {
        ($name:ident, borrowed, $finish:ident) => {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $name() {
                let end_txn_error = Arc::new(AtomicI16::new(51));
                let (mock, producer) = transactional_producer(end_txn_error.clone()).await;
                let txn = producer
                    .begin_transaction()
                    .await
                    .expect("begin_transaction");

                let err = txn
                    .$finish()
                    .await
                    .expect_err("broker reported CONCURRENT_TRANSACTIONS");
                assert2::assert!(matches!(err.source, ProducerError::ConcurrentTransactions));

                end_txn_error.store(0, Ordering::SeqCst);
                err.transaction.$finish().await.expect("retry succeeds");
                mock.stop();
            }
        };
        ($name:ident, owned, $finish:ident) => {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $name() {
                let end_txn_error = Arc::new(AtomicI16::new(51));
                let (mock, producer) = transactional_producer(end_txn_error.clone()).await;
                let producer = Arc::new(producer);
                let txn = producer
                    .clone()
                    .begin_transaction_owned()
                    .await
                    .expect("begin_transaction_owned");

                let err = txn
                    .$finish()
                    .await
                    .expect_err("broker reported CONCURRENT_TRANSACTIONS");
                assert2::assert!(matches!(err.source, ProducerError::ConcurrentTransactions));

                end_txn_error.store(0, Ordering::SeqCst);
                err.transaction.$finish().await.expect("retry succeeds");
                mock.stop();
            }
        };
    }

    // CONCURRENT_TRANSACTIONS (51) proves `commit`/`abort` drive the broker
    // round trip, and the returned guard remains usable once the broker clears
    // the condition.
    end_txn_retry_test!(
        transaction_commit_reports_broker_error_and_retries_on_the_same_guard,
        borrowed,
        commit
    );
    end_txn_retry_test!(
        transaction_abort_reports_broker_error_and_retries_on_the_same_guard,
        borrowed,
        abort
    );
    end_txn_retry_test!(
        owned_transaction_commit_reports_broker_error_and_retries_on_the_same_guard,
        owned,
        commit
    );
    end_txn_retry_test!(
        owned_transaction_abort_reports_broker_error_and_retries_on_the_same_guard,
        owned,
        abort
    );

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uncertain_end_txn_requires_reinitialization_before_reuse() {
        let end_txn_error = Arc::new(AtomicI16::new(0));
        let end_txn_silent = Arc::new(AtomicBool::new(true));
        let (mock, producer) =
            transactional_producer_with_end_txn_timeout(end_txn_error, end_txn_silent.clone())
                .await;

        let error = producer
            .begin_transaction()
            .await
            .expect("begin transaction")
            .commit()
            .await
            .expect_err(
                "EndTxn that stays silent past the retry deadline has an uncertain outcome",
            );
        assert2::assert!(let ProducerError::RecoveryRequired = error.source);
        drop(error.transaction);

        assert2::assert!(let Err(ProducerError::RecoveryRequired) = producer.begin_transaction().await);
        let acknowledgement = producer.send(ProducerRecord::default()).await;
        assert2::assert!(
            let Err(ProducerError::RecoveryRequired) =
                acknowledgement.await.expect("recovery error is delivered")
        );

        end_txn_silent.store(false, Ordering::SeqCst);
        producer.next_seq.insert(("topic".to_owned(), 0), 5);
        producer
            .init_transactions()
            .await
            .expect("reinitialization obtains a new epoch");
        assert2::assert!(*producer.txn_pid_epoch.lock().await == (7, 4));
        // Kafka's `TransactionManager` starts the sequences again at a new epoch.
        assert2::assert!(producer.next_seq.is_empty());
        assert2::assert!(producer.transactional_identity().await == Some((7, 4)));
        producer
            .begin_transaction()
            .await
            .expect("new epoch permits a transaction")
            .abort()
            .await
            .expect("abort after recovery");
        mock.stop();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_an_open_transaction_requires_explicit_recovery() {
        let end_txn_error = Arc::new(AtomicI16::new(0));
        let (mock, producer) = transactional_producer(end_txn_error).await;
        drop(
            producer
                .begin_transaction()
                .await
                .expect("begin transaction before drop"),
        );

        assert!(matches!(
            producer.begin_transaction().await,
            Err(ProducerError::RecoveryRequired)
        ));
        producer
            .init_transactions()
            .await
            .expect("explicit initialization recovers dropped transaction");
        producer
            .begin_transaction()
            .await
            .expect("begin after recovery")
            .abort()
            .await
            .expect("abort after recovery");
        mock.stop();
    }

    /// One scripted answer from the mock coordinator.
    #[derive(Debug, Clone, Copy)]
    enum Reply {
        /// Send no response, so the request times out in transport.
        Silent,
        /// Answer with this error code.
        Code(i16),
    }

    /// The scripted answers for one API, and the number of requests it got.
    #[derive(Debug)]
    struct Script {
        replies: std::collections::VecDeque<Reply>,
        exhausted: Reply,
        requests: usize,
    }

    impl Default for Script {
        fn default() -> Self {
            Self::new(&[], Reply::Code(0))
        }
    }

    impl Script {
        fn new(replies: &[Reply], exhausted: Reply) -> Self {
            Self {
                replies: replies.iter().copied().collect(),
                exhausted,
                requests: 0,
            }
        }

        fn next(&mut self) -> Reply {
            self.requests += 1;
            self.replies.pop_front().unwrap_or(self.exhausted)
        }
    }

    /// The scripts of a mock transaction coordinator. The same mock is the
    /// group coordinator and the partition leader.
    #[derive(Debug, Default)]
    struct Coordinator {
        end_txn: Script,
        add_partitions: Script,
        add_offsets: Script,
        txn_offset_commit: Script,
        produce: Script,
        find_coordinator_requests: usize,
        /// The negotiated version of each `AddPartitionsToTxn` request.
        add_partitions_versions: Vec<i16>,
        /// The `AddPartitionsToTxn` version range that the mock advertises.
        /// A Kafka broker advertises 0 to 5.
        add_partitions_range: Option<(i16, i16)>,
    }

    type SharedCoordinator = Arc<std::sync::Mutex<Coordinator>>;

    /// How a coordinator request ended, in a form that tests can compare.
    #[derive(Debug, PartialEq, Eq)]
    enum TxnResult {
        Ok,
        Fenced,
        ConcurrentTransactions,
        Server(i16),
        OutcomeUnknown,
        /// The broker supports no version that the client sends. The fields
        /// are the broker range and the client range.
        IncompatibleVersion((i16, i16), (i16, i16)),
        Other(String),
    }

    impl From<Result<(), ProducerError>> for TxnResult {
        fn from(result: Result<(), ProducerError>) -> Self {
            match result {
                Ok(()) => Self::Ok,
                Err(ProducerError::FencedProducer) => Self::Fenced,
                Err(ProducerError::ConcurrentTransactions) => Self::ConcurrentTransactions,
                Err(ProducerError::Server(code)) => Self::Server(code),
                Err(ProducerError::RecoveryRequired) => Self::OutcomeUnknown,
                Err(ProducerError::Client(ClientError::IncompatibleVersion {
                    broker_min,
                    broker_max,
                    client_min,
                    client_max,
                    ..
                })) => {
                    Self::IncompatibleVersion((broker_min, broker_max), (client_min, client_max))
                }
                Err(other) => Self::Other(other.to_string()),
            }
        }
    }

    /// Boot a mock coordinator that answers `EndTxn` and `AddPartitionsToTxn`
    /// from `coordinator`.
    async fn scripted_producer(
        coordinator: Coordinator,
    ) -> (MockBroker, Producer, SharedCoordinator) {
        let port_cell = Arc::new(AtomicU16::new(0));
        let handler_port = Arc::clone(&port_cell);
        let shared = Arc::new(std::sync::Mutex::new(coordinator));
        let handler_shared = Arc::clone(&shared);
        let add_partitions_range = shared
            .lock()
            .expect("scripted coordinator")
            .add_partitions_range
            .unwrap_or((0, 5));
        let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(encode_v0(&ApiVersionsResponse {
                    api_keys: vec![
                        ApiVersion {
                            api_key: add_partitions_to_txn_request::API_KEY,
                            min_version: add_partitions_range.0,
                            max_version: add_partitions_range.1,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: metadata_request::API_KEY,
                            min_version: 0,
                            max_version: 12,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: produce_request::API_KEY,
                            min_version: 3,
                            max_version: 13,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }));
            }
            let mut coordinator = handler_shared.lock().expect("scripted coordinator");
            if api_key == metadata_request::API_KEY {
                // The producer waits for metadata that holds the topic, so the
                // answer must decode at the negotiated version.
                let response = MetadataResponse {
                    brokers: vec![MetadataResponseBroker {
                        node_id: 1,
                        host: "127.0.0.1".into(),
                        port: i32::from(handler_port.load(Ordering::SeqCst)),
                        ..Default::default()
                    }],
                    topics: vec![MetadataResponseTopic {
                        name: Some("topic".into()),
                        partitions: vec![MetadataResponsePartition {
                            partition_index: 0,
                            leader_id: 1,
                            ..Default::default()
                        }],
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
                let Reply::Code(error_code) = coordinator.produce.next() else {
                    return None;
                };
                let response = ProduceResponse {
                    responses: vec![TopicProduceResponse {
                        name: "topic".into(),
                        partition_responses: vec![PartitionProduceResponse {
                            index: 0,
                            error_code,
                            base_offset: 0,
                            log_start_offset: -1,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                let mut buf = BytesMut::new();
                if version >= produce_response::FLEXIBLE_MIN {
                    buf.extend_from_slice(&[0]);
                }
                response.encode(&mut buf, version).expect("encode Produce");
                return Some(buf.to_vec());
            }
            if api_key == add_offsets_to_txn_request::API_KEY {
                return match coordinator.add_offsets.next() {
                    Reply::Silent => None,
                    Reply::Code(error_code) => Some(encode_v0(&AddOffsetsToTxnResponse {
                        error_code,
                        ..Default::default()
                    })),
                };
            }
            if api_key == txn_offset_commit_request::API_KEY {
                return match coordinator.txn_offset_commit.next() {
                    Reply::Silent => None,
                    Reply::Code(error_code) => Some(encode_v0(&TxnOffsetCommitResponse {
                        topics: vec![TxnOffsetCommitResponseTopic {
                            name: "topic".into(),
                            partitions: vec![TxnOffsetCommitResponsePartition {
                                partition_index: 0,
                                error_code,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })),
                };
            }
            if api_key == find_coordinator_request::API_KEY {
                coordinator.find_coordinator_requests += 1;
                return Some(encode_v0(&FindCoordinatorResponse {
                    error_code: 0,
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: i32::from(handler_port.load(Ordering::SeqCst)),
                    ..Default::default()
                }));
            }
            if api_key == init_producer_id_request::API_KEY {
                return Some(encode_v0(&InitProducerIdResponse {
                    error_code: 0,
                    producer_id: 7,
                    producer_epoch: 3,
                    ..Default::default()
                }));
            }
            if api_key == end_txn_request::API_KEY {
                return match coordinator.end_txn.next() {
                    Reply::Silent => None,
                    Reply::Code(error_code) => Some(encode_v0(&EndTxnResponse {
                        error_code,
                        ..Default::default()
                    })),
                };
            }
            if api_key == add_partitions_to_txn_request::API_KEY {
                coordinator.add_partitions_versions.push(version);
                return match coordinator.add_partitions.next() {
                    Reply::Silent => None,
                    Reply::Code(partition_error_code) => {
                        let response = AddPartitionsToTxnResponse {
                            results_by_topic_v3_and_below: vec![AddPartitionsToTxnTopicResult {
                                name: "topic".into(),
                                results_by_partition: vec![AddPartitionsToTxnPartitionResult {
                                    partition_index: 0,
                                    partition_error_code,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }],
                            ..Default::default()
                        };
                        let mut buf = BytesMut::new();
                        if version >= add_partitions_to_txn_response::FLEXIBLE_MIN {
                            buf.extend_from_slice(&[0]);
                        }
                        response
                            .encode(&mut buf, version)
                            .expect("encode AddPartitionsToTxn");
                        Some(buf.to_vec())
                    }
                };
            }
            None
        })
        .await;
        port_cell.store(mock.addr.port(), Ordering::SeqCst);
        let producer = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .transactional_id("test-txn")
            .request_timeout(Duration::from_millis(100))
            .retry_backoff(Duration::from_millis(1))
            .init_retry_timeout(Duration::from_millis(1500))
            .retry_backoff_max(Duration::from_millis(20))
            .build()
            .await
            .expect("producer connects to the mock");
        producer
            .init_transactions()
            .await
            .expect("init_transactions against the mock coordinator");
        shared
            .lock()
            .expect("scripted coordinator")
            .find_coordinator_requests = 0;
        (mock, producer, shared)
    }

    /// Boot a mock coordinator that answers `EndTxn` from `script`, and then
    /// with `exhausted` when the script is empty.
    async fn scripted_end_txn_producer(
        script: &[Reply],
        exhausted: Reply,
    ) -> (MockBroker, Producer, SharedCoordinator) {
        scripted_producer(Coordinator {
            end_txn: Script::new(script, exhausted),
            ..Coordinator::default()
        })
        .await
    }

    /// The observable result of one scripted commit.
    #[derive(Debug, PartialEq, Eq)]
    struct ScriptedCommit {
        result: TxnResult,
        end_txn_requests: usize,
        state: TxnState,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_learns_the_outcome_of_a_lost_end_txn() {
        use Reply::{Code, Silent};
        use TxnResult::{Fenced, OutcomeUnknown, Server};
        let cases = [
            ("answered", vec![Code(0)], TxnResult::Ok, 1, TxnState::Ready),
            (
                "lost then committed",
                vec![Silent, Code(0)],
                TxnResult::Ok,
                2,
                TxnState::Ready,
            ),
            (
                "lost, prepare in progress, then committed",
                vec![Silent, Code(51), Code(51), Code(0)],
                TxnResult::Ok,
                4,
                TxnState::Ready,
            ),
            (
                "lost, coordinator loading, then committed",
                vec![Silent, Code(14), Code(15), Code(16), Code(0)],
                TxnResult::Ok,
                5,
                TxnState::Ready,
            ),
            (
                "loading then committed",
                vec![Code(14), Code(0)],
                TxnResult::Ok,
                2,
                TxnState::Ready,
            ),
            (
                "concurrent without a loss, then committed",
                vec![Code(51), Code(0)],
                TxnResult::Ok,
                2,
                TxnState::Ready,
            ),
            (
                "request timed out, then committed",
                vec![Code(7), Code(0)],
                TxnResult::Ok,
                2,
                TxnState::Ready,
            ),
            ("fenced", vec![Code(47)], Fenced, 1, TxnState::Fenced),
            (
                "producer fenced",
                vec![Code(90)],
                Fenced,
                1,
                TxnState::Fenced,
            ),
            (
                "refused",
                vec![Code(48)],
                Server(48),
                1,
                TxnState::InTransaction,
            ),
            (
                "lost then fenced",
                vec![Silent, Code(47)],
                OutcomeUnknown,
                2,
                TxnState::RecoveryRequired,
            ),
            (
                "lost then producer fenced",
                vec![Silent, Code(90)],
                OutcomeUnknown,
                2,
                TxnState::RecoveryRequired,
            ),
            (
                "lost then invalid state",
                vec![Silent, Code(48)],
                OutcomeUnknown,
                2,
                TxnState::RecoveryRequired,
            ),
        ];
        for (name, script, result, end_txn_requests, state) in cases {
            let (mock, producer, coordinator) = scripted_end_txn_producer(&script, Code(0)).await;
            let outcome = producer
                .begin_transaction()
                .await
                .expect("begin transaction")
                .commit()
                .await
                .map_err(|error| error.source);
            let state_after = *producer.txn_state.lock().await;
            let actual = ScriptedCommit {
                result: outcome.into(),
                end_txn_requests: coordinator
                    .lock()
                    .expect("scripted coordinator")
                    .end_txn
                    .requests,
                state: state_after,
            };
            let expected = ScriptedCommit {
                result,
                end_txn_requests,
                state,
            };
            assert2::assert!(actual == expected, "{name}");
            mock.stop();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn committed_after_a_lost_end_txn_permits_the_next_transaction() {
        let (mock, producer, coordinator) =
            scripted_end_txn_producer(&[Reply::Silent], Reply::Code(0)).await;
        producer
            .begin_transaction()
            .await
            .expect("begin first transaction")
            .commit()
            .await
            .expect("retry learns that the first transaction committed");
        producer
            .begin_transaction()
            .await
            .expect("no reinitialization is needed after a learned outcome")
            .commit()
            .await
            .expect("second transaction commits");
        assert2::assert!(
            coordinator
                .lock()
                .expect("scripted coordinator")
                .end_txn
                .requests
                == 3
        );
        mock.stop();
    }

    /// The observable result of a coordinator request that retried until the
    /// deadline.
    #[derive(Debug, PartialEq, Eq)]
    struct DeadlineResult {
        result: TxnResult,
        retried: bool,
        state: TxnState,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn end_txn_that_stays_unanswered_reports_the_last_answer_at_the_deadline() {
        use Reply::{Code, Silent};
        let cases = [
            (
                "stays lost",
                Silent,
                TxnResult::OutcomeUnknown,
                TxnState::RecoveryRequired,
            ),
            (
                "stays concurrent",
                Code(51),
                TxnResult::ConcurrentTransactions,
                TxnState::InTransaction,
            ),
            (
                "stays loading",
                Code(14),
                TxnResult::Server(14),
                TxnState::InTransaction,
            ),
        ];
        for (name, exhausted, result, state) in cases {
            let (mock, producer, coordinator) = scripted_end_txn_producer(&[], exhausted).await;
            let outcome = producer
                .begin_transaction()
                .await
                .expect("begin transaction")
                .commit()
                .await
                .map_err(|error| error.source);
            let state_after = *producer.txn_state.lock().await;
            let end_txn_requests = coordinator
                .lock()
                .expect("scripted coordinator")
                .end_txn
                .requests;
            let actual = DeadlineResult {
                result: outcome.into(),
                retried: end_txn_requests > 1,
                state: state_after,
            };
            let expected = DeadlineResult {
                result,
                retried: true,
                state,
            };
            assert2::assert!(actual == expected, "{name}");
            mock.stop();
        }
    }

    /// The observable result of one scripted `AddPartitionsToTxn`.
    #[derive(Debug, PartialEq, Eq)]
    struct ScriptedAddPartitions {
        result: TxnResult,
        add_partitions_requests: usize,
        coordinator_lookups: usize,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn add_partitions_retries_the_codes_that_kafka_retries() {
        use Reply::{Code, Silent};
        use TxnResult::{Fenced, Server};
        let cases = [
            ("added", vec![Code(0)], TxnResult::Ok, 1, 0),
            (
                "concurrent, then added",
                vec![Code(51), Code(0)],
                TxnResult::Ok,
                2,
                0,
            ),
            (
                "loading, then added",
                vec![Code(14), Code(0)],
                TxnResult::Ok,
                2,
                0,
            ),
            (
                "moved, then added",
                vec![Code(16), Code(0)],
                TxnResult::Ok,
                2,
                1,
            ),
            (
                "lost, then added",
                vec![Silent, Code(0)],
                TxnResult::Ok,
                2,
                1,
            ),
            ("fenced", vec![Code(47)], Fenced, 1, 0),
            ("producer fenced", vec![Code(90)], Fenced, 1, 0),
            ("topic authorization", vec![Code(29)], Server(29), 1, 0),
            ("invalid state", vec![Code(48)], Server(48), 1, 0),
        ];
        for (name, script, result, add_partitions_requests, coordinator_lookups) in cases {
            let (mock, producer, coordinator) = scripted_producer(Coordinator {
                add_partitions: Script::new(&script, Code(0)),
                ..Coordinator::default()
            })
            .await;
            let outcome = producer.register_transaction_partition("topic", 0).await;
            let actual = {
                let coordinator = coordinator.lock().expect("scripted coordinator");
                ScriptedAddPartitions {
                    result: outcome.into(),
                    add_partitions_requests: coordinator.add_partitions.requests,
                    coordinator_lookups: coordinator.find_coordinator_requests,
                }
            };
            let expected = ScriptedAddPartitions {
                result,
                add_partitions_requests,
                coordinator_lookups,
            };
            assert2::assert!(actual == expected, "{name}");
            mock.stop();
        }
    }

    /// The observable result of one scripted `AddPartitionsToTxn` version
    /// negotiation.
    #[derive(Debug, PartialEq, Eq)]
    struct ScriptedAddPartitionsVersion {
        result: TxnResult,
        versions: Vec<i16>,
    }

    /// Apache Kafka's client builds `AddPartitionsToTxn` with
    /// `AddPartitionsToTxnRequest.Builder.forClient`, which allows v3 at most.
    /// v4 and later carry the broker-to-broker form, which a broker answers
    /// only for a principal with `CLUSTER_ACTION`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn add_partitions_to_txn_stops_at_the_last_client_version() {
        let cases = [
            ("broker stops at v3", (0, 3), TxnResult::Ok, vec![3]),
            ("broker supports v5", (0, 5), TxnResult::Ok, vec![3]),
            (
                "broker supports only the broker versions",
                (4, 5),
                TxnResult::IncompatibleVersion((4, 5), (0, 3)),
                Vec::new(),
            ),
        ];
        for (name, range, result, versions) in cases {
            let (mock, producer, coordinator) = scripted_producer(Coordinator {
                add_partitions_range: Some(range),
                ..Coordinator::default()
            })
            .await;
            let outcome = producer.register_transaction_partition("topic", 0).await;
            let actual = {
                let coordinator = coordinator.lock().expect("scripted coordinator");
                ScriptedAddPartitionsVersion {
                    result: outcome.into(),
                    versions: coordinator.add_partitions_versions.clone(),
                }
            };
            assert2::assert!(
                actual == ScriptedAddPartitionsVersion { result, versions },
                "{name}"
            );
            mock.stop();
        }
    }

    /// The observable result of one scripted `send_offsets_to_transaction`.
    #[derive(Debug, PartialEq, Eq)]
    struct ScriptedSendOffsets {
        result: TxnResult,
        add_offsets_requests: usize,
        txn_offset_commit_requests: usize,
        coordinator_lookups: usize,
        abortable_error: Option<i16>,
        state: TxnState,
    }

    /// Kafka's `AddOffsetsToTxnHandler` and `TxnOffsetCommitHandler` send the
    /// request again after a transport loss and after every retriable code,
    /// and they find the coordinator again for 15, 16 and, for
    /// `TxnOffsetCommit`, `REQUEST_TIMED_OUT`. An abortable code stops the
    /// transaction from committing, and 47 and 90 fence the producer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_offsets_to_transaction_retries_the_codes_that_kafka_retries() {
        use Reply::{Code, Silent};
        use TxnResult::{Fenced, Server};
        let group = krabka_client_consumer::ConsumerGroupMetadata {
            group_id: "group-a".into(),
            generation_id: 3,
            member_id: "member-a".into(),
            group_instance_id: None,
        };
        // (name, add offsets script, offset commit script, result, add offsets
        // requests, offset commit requests, coordinator lookups, abortable,
        // transaction state)
        let cases = [
            (
                "committed",
                vec![Code(0)],
                vec![Code(0)],
                TxnResult::Ok,
                1,
                1,
                1,
                None,
                TxnState::InTransaction,
            ),
            (
                "add offsets loading, then committed",
                vec![Code(14), Code(0)],
                vec![Code(0)],
                TxnResult::Ok,
                2,
                1,
                1,
                None,
                TxnState::InTransaction,
            ),
            (
                "add offsets moved, then committed",
                vec![Code(16), Code(0)],
                vec![Code(0)],
                TxnResult::Ok,
                2,
                1,
                2,
                None,
                TxnState::InTransaction,
            ),
            (
                "add offsets lost, then committed",
                vec![Silent, Code(0)],
                vec![Code(0)],
                TxnResult::Ok,
                2,
                1,
                2,
                None,
                TxnState::InTransaction,
            ),
            (
                "add offsets fenced",
                vec![Code(90)],
                vec![Code(0)],
                Fenced,
                1,
                0,
                0,
                None,
                TxnState::Fenced,
            ),
            (
                "add offsets abortable",
                vec![Code(120)],
                vec![Code(0)],
                Server(120),
                1,
                0,
                0,
                Some(120),
                TxnState::InTransaction,
            ),
            (
                "add offsets refused",
                vec![Code(48)],
                vec![Code(0)],
                Server(48),
                1,
                0,
                0,
                None,
                TxnState::InTransaction,
            ),
            (
                "offset commit unknown topic, then committed",
                vec![Code(0)],
                vec![Code(3), Code(0)],
                TxnResult::Ok,
                1,
                2,
                1,
                None,
                TxnState::InTransaction,
            ),
            (
                "offset commit timed out, then committed",
                vec![Code(0)],
                vec![Code(7), Code(0)],
                TxnResult::Ok,
                1,
                2,
                2,
                None,
                TxnState::InTransaction,
            ),
            (
                "offset commit moved, then committed",
                vec![Code(0)],
                vec![Code(16), Code(0)],
                TxnResult::Ok,
                1,
                2,
                2,
                None,
                TxnState::InTransaction,
            ),
            (
                "offset commit group metadata mismatch",
                vec![Code(0)],
                vec![Code(22)],
                Server(22),
                1,
                1,
                1,
                Some(22),
                TxnState::InTransaction,
            ),
            (
                "offset commit fenced",
                vec![Code(0)],
                vec![Code(47)],
                Fenced,
                1,
                1,
                1,
                None,
                TxnState::Fenced,
            ),
        ];
        for (
            name,
            add_offsets,
            txn_offset_commit,
            result,
            add_offsets_requests,
            txn_offset_commit_requests,
            coordinator_lookups,
            abortable_error,
            expected_state,
        ) in cases
        {
            let (mock, producer, coordinator) = scripted_producer(Coordinator {
                add_offsets: Script::new(&add_offsets, Code(0)),
                txn_offset_commit: Script::new(&txn_offset_commit, Code(0)),
                ..Coordinator::default()
            })
            .await;
            let transaction = producer
                .begin_transaction()
                .await
                .expect("begin transaction");
            let outcome = producer
                .send_offsets_to_transaction([(("topic".to_owned(), 0), 42)], &group)
                .await;
            let state = *producer.txn_state.lock().await;
            drop(transaction);
            let actual = {
                let coordinator = coordinator.lock().expect("scripted coordinator");
                ScriptedSendOffsets {
                    result: outcome.into(),
                    add_offsets_requests: coordinator.add_offsets.requests,
                    txn_offset_commit_requests: coordinator.txn_offset_commit.requests,
                    coordinator_lookups: coordinator.find_coordinator_requests,
                    abortable_error: producer.txn_abortable_error.get().map(|error| match error {
                        AbortableError::Server(code) => code,
                        AbortableError::Timeout => i16::MIN,
                    }),
                    state,
                }
            };
            let expected = ScriptedSendOffsets {
                result,
                add_offsets_requests,
                txn_offset_commit_requests,
                coordinator_lookups,
                abortable_error,
                state: expected_state,
            };
            assert2::assert!(actual == expected, "{name}");
            mock.stop();
        }
    }

    /// The observable result of a commit after a failed transactional batch.
    #[derive(Debug, PartialEq, Eq)]
    struct CommitAfterFailedBatch {
        record: TxnResult,
        commit: TxnResult,
        end_txn_requests: usize,
        abort: TxnResult,
        abortable_error_after_abort: Option<i16>,
        next_transaction: TxnResult,
    }

    /// Kafka's `Sender.failBatch` calls
    /// `TransactionManager.handleFailedBatch`, which moves a transactional
    /// producer to `ABORTABLE_ERROR`. `commitTransaction` then fails in
    /// `maybeFailWithError`, and it sends no `EndTxn`. Only
    /// `abortTransaction` clears the state.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_transactional_batch_stops_a_later_commit() {
        let (mock, producer, coordinator) = scripted_producer(Coordinator {
            // 42 is INVALID_REQUEST, which Kafka does not retry.
            produce: Script::new(&[Reply::Code(42)], Reply::Code(0)),
            ..Coordinator::default()
        })
        .await;
        let transaction = producer
            .begin_transaction()
            .await
            .expect("begin transaction");
        let record = producer
            .send(ProducerRecord {
                topic: "topic".to_owned(),
                partition: Some(0),
                value: Some(bytes::Bytes::from_static(b"v")),
                ..Default::default()
            })
            .await
            .await
            .expect("the record is resolved");

        let commit = transaction.commit().await;
        let (commit_result, transaction) = match commit {
            Ok(()) => (TxnResult::Ok, None),
            Err(error) => (TxnResult::from(Err(error.source)), Some(error.transaction)),
        };
        let end_txn_requests = coordinator
            .lock()
            .expect("scripted coordinator")
            .end_txn
            .requests;
        let abort = match transaction {
            Some(transaction) => {
                TxnResult::from(transaction.abort().await.map_err(|error| error.source))
            }
            None => TxnResult::Other("the commit succeeded".to_owned()),
        };
        let abortable_error_after_abort =
            producer.txn_abortable_error.get().map(|error| match error {
                AbortableError::Server(code) => code,
                AbortableError::Timeout => i16::MIN,
            });
        let next_transaction = match producer.begin_transaction().await {
            Ok(transaction) => {
                TxnResult::from(transaction.abort().await.map_err(|error| error.source))
            }
            Err(error) => TxnResult::from(Err(error)),
        };

        let actual = CommitAfterFailedBatch {
            record: TxnResult::from(record.map(drop)),
            commit: commit_result,
            end_txn_requests,
            abort,
            abortable_error_after_abort,
            next_transaction,
        };
        assert2::assert!(
            actual
                == CommitAfterFailedBatch {
                    record: TxnResult::Server(42),
                    commit: TxnResult::Server(42),
                    end_txn_requests: 0,
                    abort: TxnResult::Ok,
                    abortable_error_after_abort: None,
                    next_transaction: TxnResult::Ok,
                }
        );
        mock.stop();
    }

    /// The producer state after a commit whose `EndTxn` v5 answer carried an
    /// identity.
    #[derive(Debug, PartialEq, Eq)]
    struct IdentityAfterCommit {
        identity: (i64, i16),
        sequences: Vec<((String, i32), i32)>,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_new_producer_identity_resets_sequence_numbers() {
        let kept = vec![(("topic".to_owned(), 0), 5)];
        let cases = [
            ("epoch bump", (7, 4), (7, 4), vec![]),
            ("new producer id", (8, 0), (8, 0), vec![]),
            ("same identity", (7, 3), (7, 3), kept.clone()),
            ("no identity in the answer", (-1, -1), (7, 3), kept),
        ];
        for (name, answered, identity, sequences) in cases {
            let port_cell = Arc::new(AtomicU16::new(0));
            let handler_port = Arc::clone(&port_cell);
            let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(encode_v0(&ApiVersionsResponse {
                        api_keys: vec![ApiVersion {
                            api_key: end_txn_request::API_KEY,
                            min_version: 0,
                            max_version: 5,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }));
                }
                if api_key == find_coordinator_request::API_KEY {
                    return Some(encode_v0(&FindCoordinatorResponse {
                        error_code: 0,
                        node_id: 1,
                        host: "127.0.0.1".into(),
                        port: i32::from(handler_port.load(Ordering::SeqCst)),
                        ..Default::default()
                    }));
                }
                if api_key == init_producer_id_request::API_KEY {
                    return Some(encode_v0(&InitProducerIdResponse {
                        error_code: 0,
                        producer_id: 7,
                        producer_epoch: 3,
                        ..Default::default()
                    }));
                }
                if api_key == end_txn_request::API_KEY {
                    let mut buf = BytesMut::new();
                    if version >= end_txn_response::FLEXIBLE_MIN {
                        buf.extend_from_slice(&[0]);
                    }
                    EndTxnResponse {
                        error_code: 0,
                        producer_id: answered.0,
                        producer_epoch: answered.1,
                        ..Default::default()
                    }
                    .encode(&mut buf, version)
                    .expect("encode EndTxn response");
                    return Some(buf.to_vec());
                }
                None
            })
            .await;
            port_cell.store(mock.addr.port(), Ordering::SeqCst);
            let producer = Producer::builder()
                .bootstrap(mock.addr.to_string())
                .transactional_id("test-txn")
                .request_timeout(Duration::from_millis(100))
                .build()
                .await
                .expect("producer connects to the mock");
            producer
                .init_transactions()
                .await
                .expect("init_transactions against the mock coordinator");
            let transaction = producer
                .begin_transaction()
                .await
                .expect("begin transaction");
            producer.next_seq.insert(("topic".to_owned(), 0), 5);
            transaction.commit().await.expect("commit");
            let actual = IdentityAfterCommit {
                identity: *producer.txn_pid_epoch.lock().await,
                sequences: producer
                    .next_seq
                    .iter()
                    .map(|entry| (entry.key().clone(), *entry.value()))
                    .collect(),
            };
            let expected = IdentityAfterCommit {
                identity,
                sequences,
            };
            assert2::assert!(actual == expected, "{name}");
            mock.stop();
        }
    }

    /// How a coordinator call with a rejected authentication ended.
    #[derive(Debug, PartialEq, Eq)]
    struct RejectedCoordinator {
        rejected: bool,
        rejecting_handshakes: usize,
        state: TxnState,
    }

    /// Kafka's `Sender.runOnce` catches an `AuthenticationException` from the
    /// transaction coordinator and fails every pending transactional request
    /// (`TransactionManager.authenticationFailed`). The producer does not
    /// retry `AddPartitionsToTxn` or `EndTxn`, and it does not find another
    /// coordinator.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coordinator_sasl_rejection_is_not_retried() {
        use krabka_client_core::{
            Client, MockReply, MockSaslAnswer,
            security::{ClientSecurity, SaslCredentials},
        };
        use krabka_security::ListenerProtocol;

        #[derive(Clone, Copy, Debug)]
        enum Call {
            AddPartitions,
            Commit,
        }

        for (call, state) in [
            (Call::AddPartitions, TxnState::InTransaction),
            (Call::Commit, TxnState::RecoveryRequired),
        ] {
            let (mock, producer, _coordinator) = scripted_producer(Coordinator::default()).await;
            let handshakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter = Arc::clone(&handshakes);
            let answer = MockSaslAnswer::AuthenticateError(58);
            let rejecting =
                MockBroker::start_with_replies(move |api_key, version, _corr, _body| {
                    if api_key == krabka_protocol::owned::sasl_handshake_request::API_KEY {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                    answer.reply(api_key, version).unwrap_or(MockReply::Silent)
                })
                .await;
            let rejecting_client = Client::builder()
                .bootstrap(rejecting.addr.to_string())
                .security(ClientSecurity {
                    protocol: ListenerProtocol::SaslPlaintext,
                    tls: None,
                    sasl: Some(SaslCredentials::Plain {
                        username: "alice".into(),
                        password: "secret".into(),
                    }),
                    sasl_host: None,
                })
                .build()
                .await
                .expect("client");
            let transaction = producer
                .begin_transaction()
                .await
                .expect("begin transaction");
            *producer.txn_coord_client.lock().await = Some(rejecting_client);

            let result = match call {
                Call::AddPartitions => producer.register_transaction_partition("topic", 0).await,
                Call::Commit => transaction.commit().await.map_err(|error| error.source),
            };
            let actual = RejectedCoordinator {
                rejected: matches!(
                    result,
                    Err(ProducerError::Client(ClientError::Authentication { .. }))
                ),
                rejecting_handshakes: handshakes.load(Ordering::SeqCst),
                state: *producer.txn_state.lock().await,
            };

            rejecting.stop();
            mock.stop();
            assert2::assert!(
                actual
                    == RejectedCoordinator {
                        rejected: true,
                        rejecting_handshakes: 1,
                        state,
                    },
                "{call:?}"
            );
        }
    }
}
