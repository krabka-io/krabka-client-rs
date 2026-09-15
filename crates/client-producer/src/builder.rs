//! `Producer::builder()`: the `bon`-generated builder for `Producer::start`.

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
use krabka_client_core::{
    Client, ClientDnsTimeout, ClientError, ClientFrameMax, ConnectionDispatchQueueCapacity,
    DEFAULT_CLIENT_DNS_TIMEOUT, DEFAULT_CLIENT_FRAME_MAX,
    DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use krabka_protocol::{
    Encode, ProtocolError, ProtocolRequest,
    owned::{
        init_producer_id_request::{self, InitProducerIdRequest},
        init_producer_id_response::InitProducerIdResponse,
    },
};
use krabka_units::{
    ByteSize, Time,
    convert::{StdDurationExt as _, TimeExt as _},
};
use refined_type::rule::{GreaterI32, GreaterUsize, MinMaxU128, MinMaxUsize};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    buffer_pool::BufferPool,
    compression::Compression,
    error::ProducerError,
    partitioner::{BuiltInPartitioner, PartitionerConfig},
    producer::{Acks, Producer, ProducerIdentity},
    sender,
    transactional::{AbortableErrorSlot, TxnState},
    transport::ClientTransport,
    txn_retry::{self, CoordinatorAttempt, TxnRequestDecision},
};

/// The first `InitProducerId` version that holds the two-phase-commit fields.
const INIT_PRODUCER_ID_2PC_MIN_VERSION: i16 = 6;
/// The last released `InitProducerId` version.
const INIT_PRODUCER_ID_STABLE_MAX_VERSION: i16 = 5;

/// An `InitProducerId` request that negotiates released versions only.
///
/// `InitProducerIdRequest.json` marks v6 (KIP-939) as
/// `"latestVersionUnstable": true`. Apache Kafka's producer builds the request
/// with `InitProducerIdRequest.Builder(data)`, which calls
/// `AbstractRequest.Builder(ApiKeys.INIT_PRODUCER_ID)`. That constructor uses
/// `latestVersion(false)`, which leaves out the unstable v6. The generated
/// `InitProducerIdRequest` allows v6, so this type gives the cap to version
/// negotiation. A request that needs the two-phase commit fields still sends
/// v6.
#[derive(Clone, Debug, PartialEq, Eq)]
struct StableInitProducerId(InitProducerIdRequest);

impl Encode for StableInitProducerId {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for StableInitProducerId {
    const API_KEY: i16 = init_producer_id_request::API_KEY;
    const MIN_VERSION: i16 = init_producer_id_request::MIN_VERSION;
    const MAX_VERSION: i16 = INIT_PRODUCER_ID_STABLE_MAX_VERSION;
    const FLEXIBLE_MIN: i16 = init_producer_id_request::FLEXIBLE_MIN;
    type Response = InitProducerIdResponse;
}

/// Default producer compression.
pub const DEFAULT_PRODUCER_COMPRESSION: Compression = Compression::None;
/// Default delay before sending a partial producer batch.
///
/// Kafka's `linger.ms` default is 5 (KIP-1030).
pub const DEFAULT_PRODUCER_LINGER: Duration = Duration::from_millis(5);
/// Default producer acknowledgement mode. Kafka's `acks` default is `all`.
pub const DEFAULT_PRODUCER_ACKS: Acks = Acks::All;
/// The largest `max_in_flight_per_connection` an idempotent producer accepts.
///
/// Kafka's `ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE`.
const MAX_IN_FLIGHT_FOR_IDEMPOTENCE: usize = 5;

/// The number of the next generated `producer-<n>` client id.
///
/// Kafka's `ProducerConfig.PRODUCER_CLIENT_ID_SEQUENCE` starts at 1.
static PRODUCER_CLIENT_ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);
/// Default producer batch size in bytes.
pub const DEFAULT_PRODUCER_BATCH_BYTES: usize = 16 * 1024;
/// Default of `partitioner_ignore_keys`. Kafka's `partitioner.ignore.keys`
/// default is `false`.
pub const DEFAULT_PRODUCER_PARTITIONER_IGNORE_KEYS: bool = false;
/// Default of `partitioner_adaptive_partitioning_enable`. Kafka's
/// `partitioner.adaptive.partitioning.enable` default is `true`.
pub const DEFAULT_PRODUCER_PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE: bool = true;
/// Default of `partitioner_availability_timeout`. Kafka's
/// `partitioner.availability.timeout.ms` default is 0, which turns the check
/// off.
pub const DEFAULT_PRODUCER_PARTITIONER_AVAILABILITY_TIMEOUT: Duration = Duration::ZERO;
/// Default cross-partition in-flight request limit.
pub const DEFAULT_PRODUCER_MAX_IN_FLIGHT: usize = 5;
/// Default producer request timeout.
pub const DEFAULT_PRODUCER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Default deadline for flushing all buffered and in-flight records.
pub const DEFAULT_PRODUCER_FLUSH_TIMEOUT: Duration = Duration::from_secs(50);
/// Default retries after a batch's initial send.
pub const DEFAULT_PRODUCER_RETRIES: i32 = i32::MAX;
/// Default producer retry backoff.
pub const DEFAULT_PRODUCER_RETRY_BACKOFF: Duration = Duration::from_millis(100);
/// Default producer delivery timeout. Kafka's `delivery.timeout.ms` default is
/// 120000.
pub const DEFAULT_PRODUCER_DELIVERY_TIMEOUT: Duration = Duration::from_mins(2);
/// Default producer-ID initialization retry timeout.
///
/// The same timeout limits the retries of a transaction coordinator request:
/// `AddPartitionsToTxn`, and an `EndTxn` whose outcome a transport failure hid.
pub const DEFAULT_PRODUCER_INIT_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
/// Default upper bound of the exponential retry backoff. Kafka's
/// `retry.backoff.max.ms` default is 1000.
pub const DEFAULT_PRODUCER_RETRY_BACKOFF_MAX: Duration = Duration::from_secs(1);
/// Default transaction timeout.
pub const DEFAULT_PRODUCER_TRANSACTION_TIMEOUT: Duration = Duration::from_mins(1);
/// Default longest time that `send` waits for the metadata of its topic.
///
/// Kafka's `max.block.ms` has the same default of 60 s.
pub const DEFAULT_PRODUCER_MAX_BLOCK: Duration = Duration::from_mins(1);
/// Default producer buffer memory in bytes.
///
/// Kafka's `buffer.memory` default is 33554432 (32 MiB).
pub const DEFAULT_PRODUCER_BUFFER_MEMORY: usize = 32 * 1024 * 1024;

/// Default largest serialized record that `send` accepts, in bytes.
///
/// Kafka's `max.request.size` default is 1048576 (1 MiB).
pub const DEFAULT_PRODUCER_MAX_REQUEST_SIZE: usize = 1024 * 1024;

/// Bounded backlog for coalescing internal sender wakeups.
const SENDER_WAKE_CHANNEL_CAPACITY: usize = 16;

/// Validated deadline for flushing all buffered and in-flight records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerFlushTimeout(Duration);

impl ProducerFlushTimeout {
    /// Validate a producer flush timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when the timeout is zero, fractional milliseconds, or
    /// exceeds `i32::MAX` milliseconds.
    pub fn new(value: Duration) -> Result<Self, String> {
        validated_protocol_duration(value, "producer flush timeout").map(Self)
    }

    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    #[must_use]
    pub fn milliseconds(self) -> i32 {
        protocol_milliseconds(self.0)
    }
}

impl Default for ProducerFlushTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_PRODUCER_FLUSH_TIMEOUT).expect("default producer flush timeout is valid")
    }
}

/// Validated producer batching and compression policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerThroughputPolicy {
    compression: Compression,
    linger: Duration,
    linger_ms: i32,
    batch_bytes: usize,
    max_in_flight: usize,
}

impl ProducerThroughputPolicy {
    /// Validate producer batching and compression settings.
    ///
    /// # Errors
    ///
    /// Returns an error when linger is fractional or exceeds `i32::MAX`
    /// milliseconds, batch bytes are outside `1..=i32::MAX`, or max in flight
    /// is zero.
    pub fn new(
        compression: Compression,
        linger: Duration,
        batch_bytes: usize,
        max_in_flight: usize,
    ) -> Result<Self, String> {
        let milliseconds = MinMaxU128::<0, { i32::MAX as u128 }>::new(linger.as_millis())
            .map_err(|error| format!("producer linger: {error}"))?
            .into_value();
        let milliseconds =
            u64::try_from(milliseconds).map_err(|error| format!("producer linger: {error}"))?;
        if Duration::from_millis(milliseconds) != linger {
            return Err("producer linger must be a whole number of milliseconds".to_owned());
        }
        let linger_ms =
            i32::try_from(milliseconds).map_err(|error| format!("producer linger: {error}"))?;
        let batch_bytes = MinMaxUsize::<1, { i32::MAX as usize }>::new(batch_bytes)
            .map_err(|error| format!("producer batch bytes: {error}"))?
            .into_value();
        let max_in_flight = GreaterUsize::<0>::new(max_in_flight)
            .map_err(|error| format!("producer max in flight: {error}"))?
            .into_value();
        Ok(Self {
            compression,
            linger,
            linger_ms,
            batch_bytes,
            max_in_flight,
        })
    }

    #[must_use]
    pub const fn compression(self) -> Compression {
        self.compression
    }

    #[must_use]
    pub const fn linger(self) -> Duration {
        self.linger
    }

    #[must_use]
    pub const fn batch_bytes(self) -> usize {
        self.batch_bytes
    }

    #[must_use]
    pub const fn max_in_flight(self) -> usize {
        self.max_in_flight
    }

    #[must_use]
    pub const fn linger_ms(self) -> i32 {
        self.linger_ms
    }
}

impl Default for ProducerThroughputPolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_PRODUCER_COMPRESSION,
            DEFAULT_PRODUCER_LINGER,
            DEFAULT_PRODUCER_BATCH_BYTES,
            DEFAULT_PRODUCER_MAX_IN_FLIGHT,
        )
        .expect("default producer throughput policy is valid")
    }
}

/// Validated producer retry and transaction timing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerRetryPolicy {
    request_timeout: Duration,
    retries: i32,
    retry_backoff: Duration,
    retry_backoff_max: Duration,
    init_retry_timeout: Duration,
    transaction_timeout: Duration,
}

impl ProducerRetryPolicy {
    /// Validate producer retry and transaction timing.
    ///
    /// # Errors
    ///
    /// Returns an error for non-positive durations, retry durations above
    /// `i32::MAX` milliseconds, negative retries, or protocol timeouts that are
    /// not whole milliseconds.
    ///
    /// A `retry_backoff` above `retry_backoff_max` is valid. Kafka's
    /// `ExponentialBackoff` then uses `retry_backoff_max` for every retry.
    pub fn new(
        request_timeout: Duration,
        retries: i32,
        retry_backoff: Duration,
        retry_backoff_max: Duration,
        init_retry_timeout: Duration,
        transaction_timeout: Duration,
    ) -> Result<Self, String> {
        let request_timeout = validated_protocol_duration(request_timeout, "request timeout")?;
        let retries = GreaterI32::<-1>::new(retries)
            .map_err(|error| format!("producer retries: {error}"))?
            .into_value();
        let retry_backoff = validated_duration(retry_backoff, "producer retry backoff")?;
        let retry_backoff_max =
            validated_duration(retry_backoff_max, "producer retry backoff maximum")?;
        let init_retry_timeout = validated_duration(
            init_retry_timeout,
            "producer-ID initialization retry timeout",
        )?;
        let transaction_timeout =
            validated_protocol_duration(transaction_timeout, "transaction timeout")?;
        Ok(Self {
            request_timeout,
            retries,
            retry_backoff,
            retry_backoff_max,
            init_retry_timeout,
            transaction_timeout,
        })
    }

    #[must_use]
    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    #[must_use]
    pub const fn retries(self) -> i32 {
        self.retries
    }

    #[must_use]
    pub const fn retry_backoff(self) -> Duration {
        self.retry_backoff
    }

    #[must_use]
    pub const fn init_retry_timeout(self) -> Duration {
        self.init_retry_timeout
    }

    #[must_use]
    pub const fn retry_backoff_max(self) -> Duration {
        self.retry_backoff_max
    }

    #[must_use]
    pub const fn transaction_timeout(self) -> Duration {
        self.transaction_timeout
    }

    #[must_use]
    pub fn request_timeout_ms(self) -> i32 {
        protocol_milliseconds(self.request_timeout)
    }

    #[must_use]
    pub fn transaction_timeout_ms(self) -> i32 {
        protocol_milliseconds(self.transaction_timeout)
    }
}

impl Default for ProducerRetryPolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_PRODUCER_REQUEST_TIMEOUT,
            DEFAULT_PRODUCER_RETRIES,
            DEFAULT_PRODUCER_RETRY_BACKOFF,
            DEFAULT_PRODUCER_RETRY_BACKOFF_MAX,
            DEFAULT_PRODUCER_INIT_RETRY_TIMEOUT,
            DEFAULT_PRODUCER_TRANSACTION_TIMEOUT,
        )
        .expect("default producer retry policy is valid")
    }
}

/// Validate `max_request_size`. Kafka defines `max.request.size` as an `INT`
/// with `atLeast(0)`, so the value must fit an `i32`.
fn validated_max_request_size(max_request_size: usize) -> Result<usize, ProducerError> {
    Ok(
        MinMaxUsize::<0, { i32::MAX as usize }>::new(max_request_size)
            .map_err(|error| {
                ProducerError::InvalidConfig(format!("producer max request size: {error}"))
            })?
            .into_value(),
    )
}

fn validated_duration(value: Duration, name: &str) -> Result<Duration, String> {
    MinMaxU128::<1, { i32::MAX as u128 * 1_000_000 }>::new(value.as_nanos())
        .map(|_| value)
        .map_err(|error| format!("{name}: {error}"))
}

/// Give the delivery timeout, with the rule of Kafka's
/// `KafkaProducer.configureDeliveryTimeout`.
///
/// The delivery timeout must be at least `linger + request_timeout`. When the
/// application set it, a smaller value is an error. When it did not, a default
/// smaller than that sum becomes the sum, with a warning.
///
/// # Errors
///
/// Returns an error when the configured delivery timeout is out of range or
/// smaller than `linger + request_timeout`.
fn resolve_delivery_timeout(
    delivery_timeout: Option<Duration>,
    linger: Duration,
    request_timeout: Duration,
) -> Result<Duration, String> {
    let smallest = linger
        .saturating_add(request_timeout)
        .min(Duration::from_millis(i32::MAX.unsigned_abs().into()));
    match delivery_timeout {
        Some(configured) => {
            let configured = validated_duration(configured, "delivery timeout")?;
            if configured < smallest {
                return Err(
                    "delivery_timeout should be equal to or larger than linger + request_timeout"
                        .to_owned(),
                );
            }
            Ok(configured)
        }
        None if DEFAULT_PRODUCER_DELIVERY_TIMEOUT < smallest => {
            tracing::warn!(
                delivery_timeout = ?smallest,
                "delivery_timeout should be equal to or larger than linger + request_timeout. \
                 Setting it to linger + request_timeout."
            );
            Ok(smallest)
        }
        None => Ok(DEFAULT_PRODUCER_DELIVERY_TIMEOUT),
    }
}

fn validated_protocol_duration(value: Duration, name: &str) -> Result<Duration, String> {
    let milliseconds = MinMaxU128::<1, { i32::MAX as u128 }>::new(value.as_millis())
        .map_err(|error| format!("{name}: {error}"))?
        .into_value();
    let milliseconds = u64::try_from(milliseconds).map_err(|error| format!("{name}: {error}"))?;
    if Duration::from_millis(milliseconds) != value {
        return Err(format!("{name} must be a whole number of milliseconds"));
    }
    Ok(value)
}

fn protocol_milliseconds(value: Duration) -> i32 {
    i32::try_from(value.as_millis()).expect("validated protocol duration")
}

// cargo-mutants: protocol-default InitProducerId shape, so `-> Default` is equivalent.
#[cfg_attr(test, mutants::skip)]
fn build_init_producer_id_request() -> InitProducerIdRequest {
    InitProducerIdRequest {
        transactional_id: None,
        transaction_timeout_ms: 0,
        ..Default::default()
    }
}

fn next_backoff(backoff: Duration, max_backoff: Duration) -> Duration {
    backoff.saturating_mul(2).min(max_backoff)
}

/// Decide if the producer is idempotent, with the rules of Kafka's
/// `ProducerConfig.postProcessAndValidateIdempotenceConfigs`.
///
/// `enable_idempotence` is `None` when the application did not set it. Then
/// idempotence is on, and `retries=0` or an `acks` other than `All` turns it
/// off. When the application set it to `true`, those settings are errors. An
/// idempotent producer accepts at most 5 in-flight requests per connection,
/// and a `transactional_id` requires idempotence.
///
/// # Errors
///
/// Returns [`ProducerError::InvalidConfig`] with Kafka's `ConfigException`
/// message for each conflict.
fn resolve_idempotence(
    enable_idempotence: Option<bool>,
    acks: Acks,
    retries: i32,
    max_in_flight: usize,
    transactional_id: Option<&str>,
) -> Result<bool, ProducerError> {
    let configured = enable_idempotence.is_some();
    let mut enabled = enable_idempotence.unwrap_or(true);
    if enabled {
        let mut disable = false;
        if retries == 0 {
            if configured {
                return Err(ProducerError::InvalidConfig(
                    "Must set retries to non-zero when using the idempotent producer.".to_owned(),
                ));
            }
            tracing::info!("Idempotence will be disabled because retries is set to 0.");
            disable = true;
        }
        if acks != Acks::All {
            if configured {
                return Err(ProducerError::InvalidConfig(
                    "Must set acks to all in order to use the idempotent producer. \
                     Otherwise we cannot guarantee idempotence."
                        .to_owned(),
                ));
            }
            tracing::info!(
                acks = ?acks,
                "Idempotence will be disabled because acks is not set to all."
            );
            disable = true;
        }
        if max_in_flight > MAX_IN_FLIGHT_FOR_IDEMPOTENCE {
            return Err(ProducerError::InvalidConfig(format!(
                "To use the idempotent producer, max_in_flight_per_connection must be set to \
                 at most {MAX_IN_FLIGHT_FOR_IDEMPOTENCE}. Current value is {max_in_flight}."
            )));
        }
        enabled = !disable;
    }
    if !enabled && transactional_id.is_some() {
        return Err(ProducerError::InvalidConfig(
            "Cannot set a transactional_id without also enabling idempotence.".to_owned(),
        ));
    }
    Ok(enabled)
}

/// Give the client id, with the rule of Kafka's
/// `ProducerConfig.maybeOverrideClientId`.
///
/// A configured client id is kept, also when it is empty. Otherwise the id is
/// `producer-<transactional_id>`, or `producer-<n>` from a process-wide
/// sequence.
fn resolve_client_id(client_id: Option<String>, transactional_id: Option<&str>) -> String {
    client_id.unwrap_or_else(|| match transactional_id {
        Some(transactional_id) => format!("producer-{transactional_id}"),
        None => format!(
            "producer-{}",
            PRODUCER_CLIENT_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ),
    })
}

fn disabled_idempotence_identity() -> (i64, i16) {
    (-1, -1)
}

fn initial_txn_pid_epoch() -> (i64, i16) {
    (-1, -1)
}

fn producer_identity_from_init(init: &InitProducerIdResponse) -> Result<(i64, i16), ProducerError> {
    if init.error_code != 0 {
        return Err(ProducerError::Server(init.error_code));
    }
    Ok((init.producer_id, init.producer_epoch))
}

/// Send one `InitProducerId` request.
///
/// A request that carries a two-phase-commit field needs v6, which holds those
/// fields. Every other request negotiates a released version, which is v5 at
/// most. See [`StableInitProducerId`].
pub(crate) async fn send_init_producer_id(
    client: &Client,
    request: &InitProducerIdRequest,
) -> Result<InitProducerIdResponse, ClientError> {
    if request.enable2_pc || request.keep_prepared_txn {
        client
            .send_at_least(request.clone(), INIT_PRODUCER_ID_2PC_MIN_VERSION)
            .await
    } else {
        client.send(StableInitProducerId(request.clone())).await
    }
}

/// Send `InitProducerId`, and retry every code that Kafka's
/// `InitProducerIdHandler` retries, and a transient `Disconnected` transport
/// error, with capped exponential backoff until the deadline elapses.
///
/// Kafka's handler finds the coordinator again for `COORDINATOR_NOT_AVAILABLE`
/// and `NOT_COORDINATOR`, and after a disconnect. This producer has no
/// transaction coordinator for the idempotent `InitProducerId`, which goes to
/// the bootstrap connection, so it reconnects that connection instead.
/// [`Producer::init_transactions`] has a coordinator, and it runs its own loop
/// that finds the coordinator again.
///
/// On the deadline it returns the last response, so the caller's
/// `error_code != 0` handling runs. If the final attempt disconnected, it
/// surfaces the transport error instead.
#[tracing::instrument(level = "info", skip_all, err)]
pub(crate) async fn init_producer_id_with_retry(
    client: &Client,
    request: InitProducerIdRequest,
    retry_timeout: Time,
    initial_backoff: Time,
    max_backoff: Time,
) -> Result<InitProducerIdResponse, ProducerError> {
    let deadline = tokio::time::Instant::now()
        .checked_add(retry_timeout.to_std())
        .ok_or_else(|| {
            ProducerError::InvalidConfig("producer-ID retry timeout is too large".to_owned())
        })?;
    let mut backoff = initial_backoff.to_std();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let response = tokio::time::timeout(remaining, send_init_producer_id(client, &request))
            .await
            .map_err(|_| ProducerError::Client(ClientError::Timeout(retry_timeout)))?;
        let (attempt, last_outcome) = match response {
            Ok(resp) => (CoordinatorAttempt::Answered(resp.error_code), Ok(resp)),
            Err(error @ ClientError::Disconnected) => (CoordinatorAttempt::Lost, Err(error)),
            Err(e) => return Err(ProducerError::Client(e)),
        };
        let TxnRequestDecision::Retry { rediscover } = txn_retry::decide_init_producer_id(attempt)
        else {
            return last_outcome.map_err(ProducerError::Client);
        };
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return last_outcome.map_err(ProducerError::Client);
        }
        if rediscover {
            client.reconnect_bootstrap().await;
        }
        let sleep_for = backoff.min(remaining);
        tokio::time::sleep(sleep_for).await;
        if tokio::time::Instant::now() >= deadline {
            return last_outcome.map_err(ProducerError::Client);
        }
        backoff = next_backoff(backoff, max_backoff.to_std());
    }
}

/// Kafka's `ProducerMetadata` names the topics that the producer sends to,
/// and lets the broker create a missing one.
const PRODUCER_METADATA_SCOPE: krabka_client_core::MetadataScope =
    krabka_client_core::MetadataScope::Topics {
        allow_auto_topic_creation: true,
    };

#[bon::bon]
impl Producer {
    /// Build a [`Producer`] pointed at the given bootstrap address.
    ///
    /// The defaults are Kafka's: `acks=All`, a linger of 5 ms, and
    /// idempotence on. The builder applies the rules of Kafka's
    /// `ProducerConfig`. When `enable_idempotence` is not set, `retries=0` or
    /// an `acks` other than `All` turns idempotence off. When it is set to
    /// `true`, those settings fail with [`ProducerError::InvalidConfig`]. An
    /// idempotent producer accepts at most 5 in-flight requests per
    /// connection, and a `transactional_id` requires idempotence.
    ///
    /// The builder has no `enable_metrics_push` option. The producer does not
    /// push client metrics (KIP-714), so it never sends
    /// `GetTelemetrySubscriptions` or `PushTelemetry`. Kafka's
    /// `enable.metrics.push` is `true` by default.
    ///
    /// `send` fails a record whose serialized size is larger than
    /// `max_request_size` (default 1 MiB, Kafka's `max.request.size`) or
    /// larger than `buffer_memory` with [`ProducerError::RecordTooLarge`], and
    /// sends no request for it.
    ///
    /// When `client_id` is not set, the client id is
    /// `producer-<transactional_id>`, or `producer-<n>` with a process-wide
    /// sequence number.
    #[builder(start_fn = builder, finish_fn = build)]
    // bon builder; each arg is an independent knob
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            bootstrap = %bootstrap,
            client_id = client_id.as_deref(),
            acks = ?acks,
            enable_idempotence = ?enable_idempotence,
            transactional_id = transactional_id.as_deref(),
        ),
        err,
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into)] client_id: Option<String>,
        #[builder(default = DEFAULT_PRODUCER_COMPRESSION)] compression: Compression,
        enable_idempotence: Option<bool>,
        #[builder(default = DEFAULT_PRODUCER_ACKS)] acks: Acks,
        #[builder(default = DEFAULT_PRODUCER_LINGER)] linger: Duration,
        #[builder(default = DEFAULT_PRODUCER_BATCH_BYTES)] batch_size: usize,
        #[builder(default = DEFAULT_PRODUCER_PARTITIONER_IGNORE_KEYS)]
        partitioner_ignore_keys: bool,
        #[builder(default = DEFAULT_PRODUCER_PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE)]
        partitioner_adaptive_partitioning_enable: bool,
        #[builder(default = DEFAULT_PRODUCER_PARTITIONER_AVAILABILITY_TIMEOUT)]
        partitioner_availability_timeout: Duration,
        #[builder(default = DEFAULT_CLIENT_DNS_TIMEOUT)] dns_timeout: Time,
        #[builder(default = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY)]
        dispatch_queue_capacity: usize,
        #[builder(default = DEFAULT_CLIENT_FRAME_MAX)] frame_max: ByteSize,
        #[builder(default = DEFAULT_PRODUCER_REQUEST_TIMEOUT)] request_timeout: Duration,
        #[builder(default = DEFAULT_PRODUCER_FLUSH_TIMEOUT)] flush_timeout: Duration,
        #[builder(default = DEFAULT_PRODUCER_RETRIES)] retries: i32,
        #[builder(default = DEFAULT_PRODUCER_RETRY_BACKOFF)] retry_backoff: Duration,
        #[builder(default = DEFAULT_PRODUCER_RETRY_BACKOFF_MAX)] retry_backoff_max: Duration,
        delivery_timeout: Option<Duration>,
        #[builder(default = DEFAULT_PRODUCER_INIT_RETRY_TIMEOUT)] init_retry_timeout: Duration,
        #[builder(default = DEFAULT_PRODUCER_MAX_IN_FLIGHT)] max_in_flight_per_connection: usize,
        #[builder(default = DEFAULT_PRODUCER_MAX_BLOCK)] max_block: Duration,
        #[builder(default = DEFAULT_PRODUCER_BUFFER_MEMORY)] buffer_memory: usize,
        #[builder(default = DEFAULT_PRODUCER_MAX_REQUEST_SIZE)] max_request_size: usize,
        #[builder(default)]
        metadata_recovery_strategy: krabka_client_core::MetadataRecoveryStrategy,
        #[builder(default = krabka_client_core::DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER)]
        metadata_recovery_rebootstrap_trigger: Time,
        #[builder(into)] transactional_id: Option<String>,
        transaction_timeout: Option<Duration>,
        #[builder(default)] transaction_two_phase_commit_enable: bool,
        security: Option<krabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, ProducerError> {
        let enable_idempotence = resolve_idempotence(
            enable_idempotence,
            acks,
            retries,
            max_in_flight_per_connection,
            transactional_id.as_deref(),
        )?;
        let client_id = resolve_client_id(client_id, transactional_id.as_deref());
        if transaction_two_phase_commit_enable && transaction_timeout.is_some() {
            return Err(ProducerError::InvalidConfig(
                "transaction_timeout cannot be set when transaction_two_phase_commit_enable=true"
                    .to_owned(),
            ));
        }
        if transaction_two_phase_commit_enable && transactional_id.is_none() {
            return Err(ProducerError::InvalidConfig(
                "transaction_two_phase_commit_enable=true requires transactional_id".to_owned(),
            ));
        }
        let transaction_timeout =
            transaction_timeout.unwrap_or(DEFAULT_PRODUCER_TRANSACTION_TIMEOUT);
        let dns_timeout =
            ClientDnsTimeout::new(dns_timeout).map_err(ProducerError::InvalidConfig)?;
        let dispatch_queue_capacity = ConnectionDispatchQueueCapacity::new(dispatch_queue_capacity)
            .map_err(ProducerError::InvalidConfig)?;
        let frame_max =
            ClientFrameMax::try_from(frame_max).map_err(ProducerError::InvalidConfig)?;
        let metadata_recovery_rebootstrap_trigger =
            krabka_client_core::MetadataRecoveryRebootstrapTrigger::new(
                metadata_recovery_rebootstrap_trigger,
            )
            .map_err(ProducerError::InvalidConfig)?;
        let throughput_policy = ProducerThroughputPolicy::new(
            compression,
            linger,
            batch_size,
            max_in_flight_per_connection,
        )
        .map_err(ProducerError::InvalidConfig)?;
        let compression = throughput_policy.compression();
        let linger = throughput_policy.linger().as_time();
        let batch_size = throughput_policy.batch_bytes();
        let max_in_flight_per_connection = throughput_policy.max_in_flight();
        let buffer_pool =
            BufferPool::new(buffer_memory, batch_size).map_err(ProducerError::InvalidConfig)?;
        let max_request_size = validated_max_request_size(max_request_size)?;

        let retry_policy = ProducerRetryPolicy::new(
            request_timeout,
            retries,
            retry_backoff,
            retry_backoff_max,
            init_retry_timeout,
            transaction_timeout,
        )
        .map_err(ProducerError::InvalidConfig)?;
        let delivery_timeout = resolve_delivery_timeout(
            delivery_timeout,
            throughput_policy.linger(),
            retry_policy.request_timeout(),
        )
        .map_err(ProducerError::InvalidConfig)?;
        // The validated retry policy derives `Eq`, so it holds `Duration`s;
        // the domain past this point holds quantities.
        let request_timeout = retry_policy.request_timeout().as_time();
        let retries = retry_policy.retries();
        let retry_backoff = sender::RetryBackoff::new(
            retry_policy.retry_backoff(),
            retry_policy.retry_backoff_max(),
        );
        // A coordinator retry starts at the first backoff of the same policy.
        let first_backoff = retry_policy
            .retry_backoff()
            .min(retry_policy.retry_backoff_max())
            .as_time();
        let flush_timeout =
            ProducerFlushTimeout::new(flush_timeout).map_err(ProducerError::InvalidConfig)?;

        // 1. Build inner client. `security` is cloned (not moved) so it can be
        //    retained on the `Producer` and reused for the secondary
        //    coordinator connections opened by the transactional path.
        let client = Client::builder()
            .bootstrap(bootstrap)
            .client_id(client_id.clone())
            .dns_timeout(dns_timeout.time())
            .dispatch_queue_capacity(dispatch_queue_capacity.get())
            .frame_max(frame_max.size())
            .connect_timeout(request_timeout)
            .request_timeout(request_timeout)
            .metadata_recovery_strategy(metadata_recovery_strategy)
            .metadata_recovery_rebootstrap_trigger(metadata_recovery_rebootstrap_trigger.time())
            .maybe_security(security.clone())
            .metadata_scope(PRODUCER_METADATA_SCOPE)
            .build()
            .await?;

        // 2. InitProducerId if idempotence on.
        //
        // Idempotent-only producers (no transactional_id) allocate a producer
        // id from *any* broker — no FindCoordinator routing to a transaction
        // coordinator is required. But at cluster startup the broker can still
        // transiently return COORDINATOR_LOAD_IN_PROGRESS (14) /
        // COORDINATOR_NOT_AVAILABLE (15) / NOT_COORDINATOR (16) while its
        // internal state loads, and a conformant client retries these with
        // backoff rather than surfacing the first error. `init_producer_id`
        // does that retry and returns the final response.
        let (producer_id, producer_epoch) = if enable_idempotence {
            let init = init_producer_id_with_retry(
                &client,
                build_init_producer_id_request(),
                retry_policy.init_retry_timeout().as_time(),
                first_backoff,
                retry_policy.retry_backoff_max().as_time(),
            )
            .await?;
            producer_identity_from_init(&init)?
        } else {
            disabled_idempotence_identity()
        };

        // 3. Spawn the sender.
        let (wake_tx, wake_rx) = mpsc::channel(SENDER_WAKE_CHANNEL_CAPACITY);
        let shutdown = CancellationToken::new();
        let state = Arc::new(AtomicU8::new(0));
        let metadata_cache = Arc::new(Mutex::new(HashMap::new()));
        let partition_leaders = Arc::new(DashMap::new());
        let accumulators = Arc::new(DashMap::new());
        let next_seq = Arc::new(DashMap::new());
        let partitioner = Arc::new(BuiltInPartitioner::new(PartitionerConfig {
            sticky_batch_size: batch_size,
            ignore_keys: partitioner_ignore_keys,
            adaptive_partitioning: partitioner_adaptive_partitioning_enable,
            availability_timeout: partitioner_availability_timeout,
        }));
        let flush_notify = Arc::new(Notify::new());
        let in_flight = Arc::new(AtomicUsize::new(0));
        let producer_epoch = Arc::new(AtomicI16::new(producer_epoch));

        let txn_state = Arc::new(Mutex::new(TxnState::Uninitialized));
        let txn_recovery_required = Arc::new(AtomicBool::new(false));
        let txn_recovery_generation = Arc::new(AtomicU64::new(0));
        let txn_guard_generation = Arc::new(AtomicU64::new(0));
        let txn_pid_epoch = Arc::new(Mutex::new(initial_txn_pid_epoch()));
        let prepared_transaction_state = Arc::new(Mutex::new(None));
        let txn_abortable_error = Arc::new(AbortableErrorSlot::default());

        let sender_handle = tokio::spawn(sender::run(sender::SenderConfig {
            transport: Box::new(ClientTransport::new(client.clone())),
            producer_id,
            producer_epoch: Arc::clone(&producer_epoch),
            acks,
            compression,
            linger,
            request_timeout_ms: retry_policy.request_timeout_ms(),
            retries,
            retry_backoff,
            delivery_timeout: delivery_timeout.as_time(),
            max_in_flight: max_in_flight_per_connection,
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
            transactional_id: transactional_id.clone(),
            txn_state: Arc::clone(&txn_state),
            txn_pid_epoch: Arc::clone(&txn_pid_epoch),
            txn_recovery_required: Arc::clone(&txn_recovery_required),
            txn_recovery_generation: Arc::clone(&txn_recovery_generation),
            txn_abortable_error: Arc::clone(&txn_abortable_error),
        }));

        Ok(Producer {
            client,
            client_id,
            security,
            dispatch_queue_capacity,
            frame_max,
            identity: ProducerIdentity {
                id: producer_id,
                epoch: producer_epoch,
            },
            acks,
            compression,
            batch_size,
            linger,
            request_timeout,
            flush_timeout,
            max_block,
            buffer_pool,
            max_request_size,
            max_in_flight: max_in_flight_per_connection,
            metadata_cache,
            metadata_refresh: crate::metadata_wait::MetadataRefresh::default(),
            partition_leaders,
            accumulators,
            next_seq,
            partitioner,
            state,
            wake_tx,
            flush_notify,
            in_flight,
            sender_shutdown: shutdown,
            sender_handle: Some(sender_handle),
            transactional_id,
            transaction_timeout_ms: retry_policy.transaction_timeout_ms(),
            two_phase_commit_enabled: transaction_two_phase_commit_enable,
            init_retry_timeout: retry_policy.init_retry_timeout().as_time(),
            init_retry_backoff: first_backoff,
            retry_backoff_max: retry_policy.retry_backoff_max().as_time(),
            txn_state,
            txn_recovery_required,
            txn_recovery_generation,
            txn_guard_generation,
            txn_coord_client: Mutex::new(None),
            txn_pid_epoch,
            txn_abortable_error,
            prepared_transaction_state,
        })
    }
}

#[cfg(test)]
mod security_arg_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use bytes::BytesMut;
    use krabka_client_core::{
        MockBroker,
        security::{ClientSecurity, SaslCredentials},
    };
    use krabka_protocol::{
        Decode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            init_producer_id_response,
        },
    };
    use krabka_security::ListenerProtocol;
    use krabka_units::{micros, millis};

    use super::*;

    fn encode_v0(response: &impl Encode) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        response.encode(&mut bytes, 0).expect("encode response");
        bytes.to_vec()
    }

    #[test]
    fn producer_retry_policy_defaults_and_distinct_values_are_exact() {
        let defaults = ProducerRetryPolicy::default();
        assert2::assert!(
            (
                defaults.request_timeout(),
                defaults.retries(),
                defaults.retry_backoff(),
                defaults.retry_backoff_max(),
                defaults.init_retry_timeout(),
                defaults.transaction_timeout(),
                defaults.request_timeout_ms(),
                defaults.transaction_timeout_ms(),
            ) == (
                Duration::from_secs(30),
                i32::MAX,
                Duration::from_millis(100),
                Duration::from_secs(1),
                Duration::from_secs(30),
                Duration::from_mins(1),
                30_000,
                60_000,
            )
        );

        let policy = ProducerRetryPolicy::new(
            Duration::from_millis(11),
            12,
            Duration::from_millis(13),
            Duration::from_millis(14),
            Duration::from_millis(15),
            Duration::from_millis(17),
        )
        .expect("distinct policy");
        assert2::assert!(
            (
                policy.request_timeout(),
                policy.retries(),
                policy.retry_backoff(),
                policy.retry_backoff_max(),
                policy.init_retry_timeout(),
                policy.transaction_timeout(),
                policy.request_timeout_ms(),
                policy.transaction_timeout_ms(),
            ) == (
                Duration::from_millis(11),
                12,
                Duration::from_millis(13),
                Duration::from_millis(14),
                Duration::from_millis(15),
                Duration::from_millis(17),
                11,
                17,
            )
        );
    }

    #[test]
    fn producer_flush_timeout_defaults_and_distinct_values_are_exact() {
        assert_eq!(
            (
                ProducerFlushTimeout::default().duration(),
                ProducerFlushTimeout::default().milliseconds(),
            ),
            (Duration::from_secs(50), 50_000),
        );

        let timeout =
            ProducerFlushTimeout::new(Duration::from_millis(11)).expect("distinct timeout");
        assert_eq!(
            (timeout.duration(), timeout.milliseconds()),
            (Duration::from_millis(11), 11),
        );
    }

    #[test]
    fn producer_flush_timeout_rejects_invalid_protocol_durations() {
        for timeout in [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_millis(i32::MAX as u64 + 1),
        ] {
            assert!(
                ProducerFlushTimeout::new(timeout).is_err(),
                "{timeout:?} must be rejected"
            );
        }
    }

    #[test]
    fn producer_throughput_policy_defaults_and_distinct_values_are_exact() {
        let defaults = ProducerThroughputPolicy::default();
        assert2::assert!(
            (
                defaults.compression(),
                defaults.linger(),
                defaults.batch_bytes(),
                defaults.max_in_flight(),
                defaults.linger_ms(),
            ) == (Compression::None, Duration::from_millis(5), 16_384, 5, 5)
        );

        let policy =
            ProducerThroughputPolicy::new(Compression::Zstd, Duration::from_millis(11), 12, 13)
                .expect("distinct throughput policy");
        assert_eq!(
            (
                policy.compression(),
                policy.linger(),
                policy.batch_bytes(),
                policy.max_in_flight(),
                policy.linger_ms(),
            ),
            (Compression::Zstd, Duration::from_millis(11), 12, 13, 11,)
        );
    }

    #[test]
    fn producer_throughput_policy_enforces_protocol_bounds() {
        assert!(
            ProducerThroughputPolicy::new(Compression::None, Duration::ZERO, 1, 1).is_ok(),
            "zero linger is valid"
        );
        for (error, field) in [
            (
                ProducerThroughputPolicy::new(
                    Compression::None,
                    Duration::from_millis(i32::MAX as u64 + 1),
                    1,
                    1,
                )
                .expect_err("overflow linger"),
                "producer linger",
            ),
            (
                ProducerThroughputPolicy::new(Compression::None, Duration::from_nanos(1), 1, 1)
                    .expect_err("fractional linger"),
                "producer linger",
            ),
            (
                ProducerThroughputPolicy::new(Compression::None, Duration::ZERO, 0, 1)
                    .expect_err("zero batch bytes"),
                "producer batch bytes",
            ),
            (
                ProducerThroughputPolicy::new(
                    Compression::None,
                    Duration::ZERO,
                    i32::MAX as usize + 1,
                    1,
                )
                .expect_err("overflow batch bytes"),
                "producer batch bytes",
            ),
            (
                ProducerThroughputPolicy::new(Compression::None, Duration::ZERO, 1, 0)
                    .expect_err("zero max in flight"),
                "producer max in flight",
            ),
        ] {
            assert!(error.contains(field), "{error:?} does not name {field:?}");
        }
    }

    #[test]
    fn producer_retry_policy_rejects_invalid_bounds() {
        let one = Duration::from_millis(1);
        let zero = Duration::ZERO;
        let overflow = Duration::from_millis(i32::MAX as u64 + 1);
        let cases = [
            ("zero request", zero, 0, one, one, one, one),
            ("negative retries", one, -1, one, one, one, one),
            ("zero backoff", one, 0, zero, one, one, one),
            ("zero backoff maximum", one, 0, one, zero, one, one),
            ("zero init", one, 0, one, one, zero, one),
            ("zero transaction", one, 0, one, one, one, zero),
            ("request protocol overflow", overflow, 0, one, one, one, one),
            (
                "transaction protocol overflow",
                one,
                0,
                one,
                one,
                one,
                overflow,
            ),
            (
                "backoff maximum overflow",
                one,
                0,
                one,
                Duration::MAX,
                one,
                one,
            ),
        ];
        let rejected = cases
            .iter()
            .map(
                |&(name, request, retries, backoff, max, init, transaction)| {
                    (
                        name,
                        ProducerRetryPolicy::new(request, retries, backoff, max, init, transaction)
                            .is_err(),
                    )
                },
            )
            .collect::<Vec<_>>();
        let expected = cases
            .iter()
            .map(|&(name, ..)| (name, true))
            .collect::<Vec<_>>();
        assert2::assert!(rejected == expected);

        // Kafka's `ExponentialBackoff` accepts an initial backoff above the
        // maximum, and uses the maximum.
        assert2::assert!(
            ProducerRetryPolicy::new(one, 0, Duration::from_millis(2), one, one, one).is_ok()
        );
    }

    #[test]
    fn producer_retry_policy_names_invalid_retry_duration() {
        let valid = Duration::from_millis(1);
        let oversized = Duration::from_millis(i32::MAX as u64 + 1);
        let error = |backoff, max, init| {
            ProducerRetryPolicy::new(valid, 0, backoff, max, init, valid)
                .expect_err("invalid retry duration")
        };

        let messages = [
            error(oversized, valid, valid),
            error(valid, oversized, valid),
            error(valid, valid, oversized),
        ]
        .map(|message| message.split(':').next().unwrap_or_default().to_owned());
        assert2::assert!(
            messages
                == [
                    "producer retry backoff",
                    "producer retry backoff maximum",
                    "producer-ID initialization retry timeout",
                ]
        );
    }

    /// Kafka's `KafkaProducer.configureDeliveryTimeout` requires
    /// `delivery.timeout.ms >= linger.ms + request.timeout.ms`. A configured
    /// value below it is an error, and the default grows to it.
    #[test]
    fn delivery_timeout_follows_kafka_rule() {
        let ms = Duration::from_millis;
        let rule = "delivery_timeout should be equal to or larger than linger + request_timeout";
        let cases = [
            ("default", None, ms(5), ms(30_000), Ok(ms(120_000))),
            (
                "configured equal",
                Some(ms(30_005)),
                ms(5),
                ms(30_000),
                Ok(ms(30_005)),
            ),
            (
                "configured below",
                Some(ms(30_004)),
                ms(5),
                ms(30_000),
                Err(rule.to_owned()),
            ),
            ("default below", None, ms(5), ms(200_000), Ok(ms(200_005))),
            (
                "sum above i32 range",
                None,
                ms(i32::MAX as u64),
                ms(i32::MAX as u64),
                Ok(ms(i32::MAX as u64)),
            ),
        ];
        let actual = cases
            .iter()
            .map(|(name, configured, linger, request, _)| {
                (
                    *name,
                    resolve_delivery_timeout(*configured, *linger, *request),
                )
            })
            .collect::<Vec<_>>();
        let expected = cases
            .into_iter()
            .map(|(name, .., want)| (name, want))
            .collect::<Vec<_>>();
        assert2::assert!(actual == expected);
    }

    /// A configured delivery timeout below `linger + request_timeout` fails
    /// `build()`. The default linger of 5 ms counts in the sum, as in Kafka.
    #[tokio::test]
    async fn producer_builder_rejects_delivery_timeout_below_linger_and_request_timeout() {
        let rule = "invalid config: delivery_timeout should be equal to or larger than linger \
                    + request_timeout";
        let cases = [
            ("configured linger", Some(Duration::from_millis(5)), 1_004),
            ("default linger", None, 1_004),
            ("default linger, delivery equal to request", None, 1_000),
        ];
        let mut actual = Vec::with_capacity(cases.len());
        let mut expected = Vec::with_capacity(cases.len());
        for (name, linger, delivery_timeout_ms) in cases {
            let error = Producer::builder()
                .bootstrap("127.0.0.1:1")
                .maybe_linger(linger)
                .request_timeout(Duration::from_secs(1))
                .delivery_timeout(Duration::from_millis(delivery_timeout_ms))
                .build()
                .await
                .err()
                .map(|error| error.to_string());
            actual.push((name, error));
            expected.push((name, Some(rule.to_owned())));
        }
        assert2::assert!(actual == expected);
    }

    #[test]
    fn init_producer_id_request_is_idempotent_only_shape() {
        let req = build_init_producer_id_request();

        assert2::assert!((req.transactional_id.as_ref(), req.transaction_timeout_ms) == (None, 0));
    }

    #[test]
    fn retry_backoff_doubles_and_caps() {
        for (_name, current, want) in [
            (
                "double below cap",
                Duration::from_millis(100),
                Duration::from_millis(200),
            ),
            (
                "double to cap",
                Duration::from_millis(800),
                Duration::from_secs(1),
            ),
            (
                "remain capped",
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
        ] {
            assert2::assert!(next_backoff(current, Duration::from_secs(1)) == want);
        }
    }

    /// The settings that Kafka's `ProducerConfig.postProcessParsedConfig`
    /// derives from the producer inputs.
    #[derive(Debug, PartialEq, Eq)]
    struct ResolvedConfig {
        client_id: String,
        enable_idempotence: bool,
        acks: Acks,
        linger: Duration,
    }

    /// The inputs of one builder case. `None` leaves the setter uncalled.
    #[derive(Default)]
    struct BuilderInputs {
        client_id: Option<&'static str>,
        enable_idempotence: Option<bool>,
        acks: Option<Acks>,
        retries: Option<i32>,
        max_in_flight: Option<usize>,
        transactional_id: Option<&'static str>,
    }

    /// Replaces the sequence number of a generated `producer-<n>` client id,
    /// because tests that run in parallel also take numbers.
    fn without_sequence(client_id: String) -> String {
        match client_id.strip_prefix("producer-") {
            Some(rest) if !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()) => {
                "producer-<n>".to_owned()
            }
            _ => client_id,
        }
    }

    async fn resolved_config(
        bootstrap: &str,
        inputs: BuilderInputs,
    ) -> Result<ResolvedConfig, String> {
        let result = Producer::builder()
            .bootstrap(bootstrap)
            .maybe_client_id(inputs.client_id)
            .maybe_enable_idempotence(inputs.enable_idempotence)
            .maybe_acks(inputs.acks)
            .maybe_retries(inputs.retries)
            .maybe_max_in_flight_per_connection(inputs.max_in_flight)
            .maybe_transactional_id(inputs.transactional_id)
            .request_timeout(Duration::from_millis(500))
            .build()
            .await;
        match result {
            Ok(producer) => {
                let resolved = ResolvedConfig {
                    client_id: without_sequence(producer.client_id.clone()),
                    enable_idempotence: producer.producer_id() >= 0,
                    acks: producer.acks,
                    linger: producer.linger.to_std(),
                };
                producer.close().await.expect("close producer");
                Ok(resolved)
            }
            Err(ProducerError::InvalidConfig(message)) => Err(message),
            Err(error) => panic!("unexpected build error: {error:?}"),
        }
    }

    /// The builder applies Kafka's producer defaults and the rules of
    /// `ProducerConfig.postProcessAndValidateIdempotenceConfigs` and
    /// `ProducerConfig.maybeOverrideClientId`.
    #[tokio::test]
    async fn producer_builder_resolves_kafka_defaults_and_idempotence_rules() {
        let mock = MockBroker::start(|api_key, _version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(encode_v0(&ApiVersionsResponse::default()));
            }
            (api_key == init_producer_id_request::API_KEY).then(|| {
                encode_v0(&InitProducerIdResponse {
                    producer_id: 1,
                    ..Default::default()
                })
            })
        })
        .await;
        let bootstrap = mock.addr.to_string();
        let five_ms = Duration::from_millis(5);
        let acks_error = "Must set acks to all in order to use the idempotent producer. \
                          Otherwise we cannot guarantee idempotence.";
        let retries_error = "Must set retries to non-zero when using the idempotent producer.";
        let in_flight_error = "To use the idempotent producer, max_in_flight_per_connection \
                               must be set to at most 5. Current value is 6.";
        let transactional_error =
            "Cannot set a transactional_id without also enabling idempotence.";
        let cases: Vec<(&str, BuilderInputs, Result<ResolvedConfig, String>)> = vec![
            (
                "nothing set",
                BuilderInputs::default(),
                Ok(ResolvedConfig {
                    client_id: "producer-<n>".to_owned(),
                    enable_idempotence: true,
                    acks: Acks::All,
                    linger: five_ms,
                }),
            ),
            (
                "idempotence set, acks one",
                BuilderInputs {
                    enable_idempotence: Some(true),
                    acks: Some(Acks::One),
                    ..BuilderInputs::default()
                },
                Err(acks_error.to_owned()),
            ),
            (
                "idempotence set, acks zero",
                BuilderInputs {
                    enable_idempotence: Some(true),
                    acks: Some(Acks::Zero),
                    ..BuilderInputs::default()
                },
                Err(acks_error.to_owned()),
            ),
            (
                "idempotence not set, acks one",
                BuilderInputs {
                    acks: Some(Acks::One),
                    ..BuilderInputs::default()
                },
                Ok(ResolvedConfig {
                    client_id: "producer-<n>".to_owned(),
                    enable_idempotence: false,
                    acks: Acks::One,
                    linger: five_ms,
                }),
            ),
            (
                "idempotence not set, acks zero",
                BuilderInputs {
                    acks: Some(Acks::Zero),
                    ..BuilderInputs::default()
                },
                Ok(ResolvedConfig {
                    client_id: "producer-<n>".to_owned(),
                    enable_idempotence: false,
                    acks: Acks::Zero,
                    linger: five_ms,
                }),
            ),
            (
                "idempotence set, retries zero",
                BuilderInputs {
                    enable_idempotence: Some(true),
                    acks: Some(Acks::All),
                    retries: Some(0),
                    ..BuilderInputs::default()
                },
                Err(retries_error.to_owned()),
            ),
            (
                "idempotence not set, retries zero",
                BuilderInputs {
                    acks: Some(Acks::All),
                    retries: Some(0),
                    ..BuilderInputs::default()
                },
                Ok(ResolvedConfig {
                    client_id: "producer-<n>".to_owned(),
                    enable_idempotence: false,
                    acks: Acks::All,
                    linger: five_ms,
                }),
            ),
            (
                "idempotence set, six in flight",
                BuilderInputs {
                    enable_idempotence: Some(true),
                    acks: Some(Acks::All),
                    max_in_flight: Some(6),
                    ..BuilderInputs::default()
                },
                Err(in_flight_error.to_owned()),
            ),
            (
                "idempotence not set, six in flight",
                BuilderInputs {
                    max_in_flight: Some(6),
                    ..BuilderInputs::default()
                },
                Err(in_flight_error.to_owned()),
            ),
            (
                "idempotence not set, retries zero, six in flight",
                BuilderInputs {
                    retries: Some(0),
                    max_in_flight: Some(6),
                    ..BuilderInputs::default()
                },
                Err(in_flight_error.to_owned()),
            ),
            (
                "idempotence off, six in flight",
                BuilderInputs {
                    enable_idempotence: Some(false),
                    max_in_flight: Some(6),
                    ..BuilderInputs::default()
                },
                Ok(ResolvedConfig {
                    client_id: "producer-<n>".to_owned(),
                    enable_idempotence: false,
                    acks: Acks::All,
                    linger: five_ms,
                }),
            ),
            (
                "idempotence off, transactional id",
                BuilderInputs {
                    enable_idempotence: Some(false),
                    acks: Some(Acks::All),
                    transactional_id: Some("t"),
                    ..BuilderInputs::default()
                },
                Err(transactional_error.to_owned()),
            ),
            (
                "idempotence turned off by acks, transactional id",
                BuilderInputs {
                    acks: Some(Acks::One),
                    transactional_id: Some("t"),
                    ..BuilderInputs::default()
                },
                Err(transactional_error.to_owned()),
            ),
            (
                "transactional id names the client",
                BuilderInputs {
                    transactional_id: Some("t"),
                    ..BuilderInputs::default()
                },
                Ok(ResolvedConfig {
                    client_id: "producer-t".to_owned(),
                    enable_idempotence: true,
                    acks: Acks::All,
                    linger: five_ms,
                }),
            ),
            (
                "configured client id wins",
                BuilderInputs {
                    client_id: Some("app"),
                    transactional_id: Some("t"),
                    ..BuilderInputs::default()
                },
                Ok(ResolvedConfig {
                    client_id: "app".to_owned(),
                    enable_idempotence: true,
                    acks: Acks::All,
                    linger: five_ms,
                }),
            ),
        ];
        let mut actual = Vec::with_capacity(cases.len());
        let mut expected = Vec::with_capacity(cases.len());
        for (name, inputs, want) in cases {
            actual.push((name, resolved_config(&bootstrap, inputs).await));
            expected.push((name, want));
        }
        mock.stop();
        assert2::assert!(actual == expected);
    }

    #[test]
    fn generated_client_ids_take_distinct_sequence_numbers() {
        let first = resolve_client_id(None, None);
        let second = resolve_client_id(None, None);
        let number = |id: &str| {
            id.strip_prefix("producer-")
                .and_then(|n| n.parse::<u64>().ok())
                .expect("generated client id")
        };
        assert2::assert!(number(&second) > number(&first));
    }

    #[test]
    fn disabled_idempotence_identity_uses_kafka_sentinel_values() {
        assert2::assert!(disabled_idempotence_identity() == (-1, -1));
        assert2::assert!(initial_txn_pid_epoch() == (-1, -1));
    }

    #[test]
    fn producer_identity_from_init_maps_error_and_success() {
        let success = InitProducerIdResponse {
            error_code: 0,
            producer_id: 42,
            producer_epoch: 7,
            ..Default::default()
        };
        assert2::assert!(producer_identity_from_init(&success).unwrap() == (42, 7));

        let error = InitProducerIdResponse {
            error_code: 51,
            ..Default::default()
        };
        assert2::assert!(matches!(
            producer_identity_from_init(&error),
            Err(ProducerError::Server(51))
        ));
    }

    #[tokio::test]
    async fn producer_builder_accepts_security() {
        let security = ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain {
                username: "u".into(),
                password: "p".into(),
            }),
            sasl_host: None,
        };
        // 127.0.0.1:1 is unroutable for a listener; with idempotence on the
        // build issues an InitProducerId, which must fail at connect —
        // proving the security arg is threaded (not a type error). A lazy
        // (non-idempotent) build would not connect, so keep idempotence on.
        let res = Producer::builder()
            .bootstrap("127.0.0.1:1")
            .request_timeout(std::time::Duration::from_millis(500))
            .security(security)
            .build()
            .await;
        assert2::assert!(res.is_err());
    }

    #[tokio::test]
    async fn producer_builder_uses_request_timeout_for_initial_connect_handshake() {
        let mock = MockBroker::start(|_api_key, _version, _corr_id, _body| None).await;

        let build = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(Duration::from_millis(100))
            .build();
        let res = tokio::time::timeout(Duration::from_millis(500), build).await;

        mock.stop();

        let err = res
            .expect("producer build should not retain the 30s connect timeout")
            .expect_err("silent broker must time out during build");
        assert2::assert!(matches!(
            err,
            ProducerError::Client(ClientError::Timeout(d))
                if d == krabka_units::millis(100)
        ));
    }

    #[tokio::test]
    async fn producer_builder_uses_configured_init_retry_timeout() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);
        let mock = MockBroker::start(move |api_key, _version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(encode_v0(&ApiVersionsResponse::default()));
            }
            if api_key == init_producer_id_request::API_KEY {
                observed.fetch_add(1, Ordering::Relaxed);
                return Some(encode_v0(&InitProducerIdResponse {
                    error_code: 14, // COORDINATOR_LOAD_IN_PROGRESS
                    ..Default::default()
                }));
            }
            None
        })
        .await;

        let build = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(Duration::from_millis(100))
            .retry_backoff(Duration::from_millis(10))
            .retry_backoff_max(Duration::from_millis(10))
            .init_retry_timeout(Duration::from_millis(100))
            .build();
        let error = tokio::time::timeout(Duration::from_millis(500), build)
            .await
            .expect("configured init retry timeout must bound the build")
            .expect_err("cold coordinator must remain an error");

        mock.stop();
        // 14 is COORDINATOR_LOAD_IN_PROGRESS.
        assert2::assert!(matches!(error, ProducerError::Server(14)));
        assert2::assert!(attempts.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn two_phase_init_fields_require_version_six_before_dispatch() {
        for (name, request) in [
            (
                "enable 2PC",
                InitProducerIdRequest {
                    enable2_pc: true,
                    ..Default::default()
                },
            ),
            (
                "keep prepared transaction",
                InitProducerIdRequest {
                    keep_prepared_txn: true,
                    ..Default::default()
                },
            ),
        ] {
            let attempts = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&attempts);
            let mock = MockBroker::start(move |api_key, _version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(encode_v0(&ApiVersionsResponse {
                        api_keys: vec![ApiVersion {
                            api_key: init_producer_id_request::API_KEY,
                            min_version: 0,
                            max_version: 5,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }));
                }
                if api_key == init_producer_id_request::API_KEY {
                    observed.fetch_add(1, Ordering::Relaxed);
                }
                None
            })
            .await;
            let client = Client::builder()
                .bootstrap(mock.addr.to_string())
                .request_timeout(millis(100))
                .build()
                .await
                .unwrap();

            let error =
                init_producer_id_with_retry(&client, request, millis(100), millis(1), millis(1))
                    .await
                    .expect_err(name);

            assert2::assert!(matches!(
                error,
                ProducerError::Client(ClientError::IncompatibleVersion {
                    api_key: init_producer_id_request::API_KEY,
                    broker_min: 0,
                    broker_max: 5,
                    client_min: 6,
                    client_max: 6,
                })
            ));
            assert2::assert!(attempts.load(Ordering::Relaxed) == 0, "{name}");
            mock.stop();
        }
    }

    /// A request without the two-phase commit fields negotiates v5 at most, as
    /// Kafka's producer does. A two-phase commit request still sends v6.
    #[tokio::test]
    async fn init_producer_id_sends_the_unstable_version_only_for_two_phase_commit() {
        const CLIENT_ID: &str = "p";
        let transactional = InitProducerIdRequest {
            transactional_id: Some("txn".into()),
            transaction_timeout_ms: 60_000,
            ..Default::default()
        };
        let two_phase = InitProducerIdRequest {
            enable2_pc: true,
            ..transactional.clone()
        };
        let cases = [
            (
                "idempotent, broker max 3",
                build_init_producer_id_request(),
                3,
                3,
            ),
            (
                "idempotent, broker max 5",
                build_init_producer_id_request(),
                5,
                5,
            ),
            (
                "idempotent, broker max 6",
                build_init_producer_id_request(),
                6,
                5,
            ),
            ("transactional, broker max 6", transactional, 6, 5),
            ("two-phase commit, broker max 6", two_phase, 6, 6),
        ];
        for (name, request, broker_max, expected_version) in cases {
            let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
            let handler_seen = Arc::clone(&seen);
            let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(encode_v0(&ApiVersionsResponse {
                        api_keys: vec![ApiVersion {
                            api_key: init_producer_id_request::API_KEY,
                            min_version: 0,
                            max_version: broker_max,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }));
                }
                if api_key == init_producer_id_request::API_KEY {
                    let flexible = version >= init_producer_id_request::FLEXIBLE_MIN;
                    let mut request_body = &body[2 + CLIENT_ID.len() + usize::from(flexible)..];
                    let decoded = InitProducerIdRequest::decode(&mut request_body, version)
                        .expect("decode InitProducerId");
                    handler_seen.lock().unwrap().push((version, decoded));
                    let mut response = BytesMut::new();
                    if version >= init_producer_id_response::FLEXIBLE_MIN {
                        response.extend_from_slice(&[0]);
                    }
                    InitProducerIdResponse {
                        producer_id: 1,
                        ..Default::default()
                    }
                    .encode(&mut response, version)
                    .expect("encode InitProducerId response");
                    return Some(response.to_vec());
                }
                None
            })
            .await;
            let client = Client::builder()
                .bootstrap(mock.addr.to_string())
                .client_id(CLIENT_ID)
                .request_timeout(millis(500))
                .build()
                .await
                .unwrap();

            init_producer_id_with_retry(
                &client,
                request.clone(),
                millis(500),
                millis(1),
                millis(1),
            )
            .await
            .expect(name);

            let sent = seen.lock().unwrap().clone();
            assert2::assert!(sent == vec![(expected_version, request)], "{name}");
            mock.stop();
        }
    }

    #[tokio::test]
    async fn init_retry_timeout_bounds_an_unresponsive_request() {
        let mock = MockBroker::start(|api_key, _version, _corr_id, _body| {
            (api_key == api_versions_request::API_KEY)
                .then(|| encode_v0(&ApiVersionsResponse::default()))
        })
        .await;

        let build = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(Duration::from_secs(5))
            .retry_backoff(Duration::from_millis(1))
            .retry_backoff_max(Duration::from_millis(1))
            .init_retry_timeout(Duration::from_millis(10))
            .build();
        let error = tokio::time::timeout(Duration::from_millis(200), build)
            .await
            .expect("init retry timeout must bound an in-flight request")
            .expect_err("unresponsive InitProducerId must time out");

        mock.stop();
        assert2::assert!(matches!(
            error,
            ProducerError::Client(ClientError::Timeout(timeout))
                if timeout == krabka_units::millis(10)
        ));
    }

    #[tokio::test]
    async fn producer_builder_rejects_retry_policy_before_connection_io() {
        macro_rules! invalid {
            ($setter:ident, $value:expr) => {
                Producer::builder()
                    .bootstrap("127.0.0.1:1")
                    .$setter($value)
                    .build()
                    .await
                    .expect_err("invalid retry policy must fail before connection I/O")
            };
        }

        let zero = "[the value must be equal to 1, but received 0 || the value must be greater than 1, but received 0]";
        for (error, expected) in [
            (
                invalid!(request_timeout, Duration::ZERO),
                format!("invalid config: request timeout: {zero}"),
            ),
            (
                invalid!(retries, -1),
                "invalid config: producer retries: the value must be greater than -1, but received -1"
                    .to_owned(),
            ),
            (
                invalid!(retry_backoff, Duration::ZERO),
                format!("invalid config: producer retry backoff: {zero}"),
            ),
            (
                invalid!(delivery_timeout, Duration::ZERO),
                format!("invalid config: delivery timeout: {zero}"),
            ),
            (
                invalid!(init_retry_timeout, Duration::ZERO),
                format!("invalid config: producer-ID initialization retry timeout: {zero}"),
            ),
            (
                invalid!(retry_backoff_max, Duration::ZERO),
                format!("invalid config: producer retry backoff maximum: {zero}"),
            ),
            (
                invalid!(transaction_timeout, Duration::ZERO),
                format!("invalid config: transaction timeout: {zero}"),
            ),
        ] {
            let message = error.to_string();
            assert2::assert!(message == expected);
        }
    }

    #[tokio::test]
    async fn two_phase_commit_rejects_conflicting_or_incomplete_configuration() {
        let conflict = Producer::builder()
            .bootstrap("127.0.0.1:1")
            .transactional_id("payments")
            .transaction_two_phase_commit_enable(true)
            .transaction_timeout(Duration::from_secs(30))
            .build()
            .await
            .expect_err("2PC owns the transaction timeout");
        assert2::assert!(matches!(
            conflict,
            ProducerError::InvalidConfig(message)
                if message == "transaction_timeout cannot be set when transaction_two_phase_commit_enable=true"
        ));

        let missing_id = Producer::builder()
            .bootstrap("127.0.0.1:1")
            .transaction_two_phase_commit_enable(true)
            .build()
            .await
            .expect_err("2PC requires a transactional id");
        assert2::assert!(matches!(
            missing_id,
            ProducerError::InvalidConfig(message)
                if message == "transaction_two_phase_commit_enable=true requires transactional_id"
        ));
    }

    #[tokio::test]
    async fn producer_builder_rejects_invalid_dns_timeout_before_connection_io() {
        for timeout in [Time::ZERO, micros(1), Time::from_secs_f64(f64::INFINITY)] {
            let error = Producer::builder()
                .bootstrap("127.0.0.1:1")
                .dns_timeout(timeout)
                .build()
                .await
                .expect_err("invalid DNS timeout must fail before connection I/O");
            assert2::assert!(matches!(
                error,
                ProducerError::InvalidConfig(message)
                    if message.starts_with("client DNS timeout")
            ));
        }
    }

    #[tokio::test]
    async fn producer_builder_rejects_invalid_metadata_rebootstrap_trigger_before_io() {
        for trigger in [Time::from_millis(-1), krabka_units::micros(1)] {
            let error = Producer::builder()
                .bootstrap("127.0.0.1:1")
                .metadata_recovery_rebootstrap_trigger(trigger)
                .build()
                .await
                .expect_err("invalid metadata recovery trigger");
            assert2::assert!(matches!(error, ProducerError::InvalidConfig(_)));
        }
    }

    #[tokio::test]
    async fn producer_builder_accepts_a_distinct_dns_timeout() {
        let producer = Producer::builder()
            .bootstrap("127.0.0.1:1")
            .dns_timeout(millis(37))
            .enable_idempotence(false)
            .build()
            .await
            .expect("valid DNS timeout");
        producer.close().await.expect("close producer");
    }

    #[tokio::test]
    async fn producer_builder_carries_client_resource_policy() {
        let producer = Producer::builder()
            .bootstrap("127.0.0.1:1")
            .dispatch_queue_capacity(7)
            .frame_max(krabka_units::kibibytes(32))
            .enable_idempotence(false)
            .build()
            .await
            .expect("valid client resource policy");

        assert2::assert!(producer.dispatch_queue_capacity.get() == 7);
        assert2::assert!(producer.frame_max.size() == krabka_units::kibibytes(32));
        producer.close().await.expect("close producer");
    }

    #[tokio::test]
    async fn producer_builder_rejects_flush_timeout_before_connection_io() {
        macro_rules! invalid {
            ($setter:ident, $value:expr, $field:literal) => {
                let error = Producer::builder()
                    .bootstrap("127.0.0.1:1")
                    .$setter($value)
                    .build()
                    .await
                    .expect_err("invalid flush timeout must fail before connection I/O");
                assert!(
                    matches!(
                        error,
                        ProducerError::InvalidConfig(ref message) if message.starts_with($field)
                    ),
                    "{error:?} does not name {:?}",
                    $field
                );
            };
        }

        invalid!(flush_timeout, Duration::ZERO, "producer flush timeout");
    }

    #[tokio::test]
    async fn producer_builder_rejects_throughput_policy_before_connection_io() {
        macro_rules! invalid {
            ($setter:ident, $value:expr) => {
                Producer::builder()
                    .bootstrap("127.0.0.1:1")
                    .$setter($value)
                    .build()
                    .await
                    .expect_err("invalid throughput policy must fail before connection I/O")
            };
        }

        for (error, field) in [
            (invalid!(linger, Duration::from_nanos(1)), "producer linger"),
            (
                invalid!(linger, Duration::from_millis(i32::MAX as u64 + 1)),
                "producer linger",
            ),
            (invalid!(batch_size, 0), "producer batch bytes"),
            (
                invalid!(batch_size, i32::MAX as usize + 1),
                "producer batch bytes",
            ),
            (
                invalid!(max_in_flight_per_connection, 0),
                "producer max in flight",
            ),
            (
                invalid!(max_request_size, i32::MAX as usize + 1),
                "producer max request size",
            ),
        ] {
            assert!(
                matches!(
                    error,
                    ProducerError::InvalidConfig(ref message) if message.starts_with(field)
                ),
                "{error:?} does not name {field:?}"
            );
        }
    }
}
