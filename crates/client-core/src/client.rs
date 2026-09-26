//! Top-level [`Client`].
//!
//! [`Client`] wraps a [`BrokerPool`] and exposes a typed-request `send` API.

use std::sync::{Arc, Mutex};

use krabka_protocol::owned::metadata_request::MetadataRequest;
use krabka_units::{Time, convert::TimeExt as _, minutes};
use refined_type::rule::GreaterEqualI64;

use crate::{
    bootstrap,
    connection::{
        ClientDnsTimeout, ClientFrameMax, ConnectionDispatchQueueCapacity, ConnectionOptions,
    },
    error::ClientError,
    metadata_topics::{MetadataScope, MetadataTopics},
    pool::{BrokerInfo, BrokerPool},
    request::ProtocolRequest,
    telemetry::{
        ClientMetrics, ClientTelemetry, ClientTelemetryConfig, NetworkMetrics,
        TelemetryConnections, TelemetryTiming,
    },
};

/// A Kafka client backed by a [`BrokerPool`].
///
/// Construct a `Client` with [`Client::builder`].
///
/// A clone of a `Client` is cheap. The clone shares the underlying
/// [`BrokerPool`] through an `Arc` and copies the connection options by value.
#[derive(Clone)]
pub struct Client {
    bootstrap: String,
    pool: Arc<BrokerPool>,
    options: ConnectionOptions,
    metadata_recovery: Arc<MetadataRecovery>,
    metadata_topics: Arc<MetadataTopics>,
    /// The metric registry, for a client that pushes its metrics.
    metrics: Option<ClientMetrics>,
    /// The KIP-714 reporter. The last clone that drops it starts its close.
    /// The reporter itself holds a clone without it.
    telemetry: Option<Arc<ClientTelemetry>>,
}

/// Kafka client behavior when its last-known broker metadata can no longer be
/// refreshed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MetadataRecoveryStrategy {
    /// Return the metadata error without contacting the configured bootstrap
    /// addresses again.
    None,
    /// Re-resolve the configured bootstrap addresses and discard stale broker
    /// connections and addresses.
    #[default]
    Rebootstrap,
}

/// KIP-1102's default interval from the first unsuccessful metadata attempt
/// until the client repeats bootstrap discovery.
pub const DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER: Time = minutes(5);

/// Non-negative, whole-millisecond KIP-1102 rebootstrap trigger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataRecoveryRebootstrapTrigger(i64);

impl MetadataRecoveryRebootstrapTrigger {
    /// Validate a rebootstrap trigger.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is not a non-negative whole number of
    /// milliseconds.
    pub fn new(value: Time) -> Result<Self, String> {
        let milliseconds = GreaterEqualI64::<0>::new(value.millis_i64())
            .map_err(|error| format!("metadata recovery rebootstrap trigger: {error}"))?
            .into_value();
        if !value.secs_f64().is_finite() || Time::from_millis(milliseconds) != value {
            return Err(
                "metadata recovery rebootstrap trigger must be a whole number of milliseconds"
                    .to_owned(),
            );
        }
        Ok(Self(milliseconds))
    }

    /// Return the validated trigger interval.
    #[must_use]
    pub fn time(self) -> Time {
        Time::from_millis(self.0)
    }
}

impl Default for MetadataRecoveryRebootstrapTrigger {
    fn default() -> Self {
        Self::new(DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER)
            .expect("default metadata recovery trigger is valid")
    }
}

#[derive(Debug)]
struct MetadataRecovery {
    strategy: MetadataRecoveryStrategy,
    trigger: MetadataRecoveryRebootstrapTrigger,
    first_attempt: Mutex<Option<tokio::time::Instant>>,
}

impl MetadataRecovery {
    fn begin_attempt(&self) {
        if self.strategy == MetadataRecoveryStrategy::None {
            return;
        }
        let mut first_attempt = self
            .first_attempt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        first_attempt.get_or_insert_with(tokio::time::Instant::now);
    }

    fn complete_attempt(&self) {
        *self
            .first_attempt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    fn timed_out(&self) -> bool {
        if self.strategy == MetadataRecoveryStrategy::None {
            return false;
        }
        self.first_attempt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some_and(|started| started.elapsed() >= self.trigger.time().to_std())
    }
}

const REBOOTSTRAP_REQUIRED: i16 = 129;

#[bon::bon]
impl Client {
    /// Build a [`Client`] pointed at the given bootstrap address.
    #[builder(start_fn = builder, finish_fn = build)]
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(bootstrap = %bootstrap, client_id = %client_id),
        err,
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into, default = "krabka".to_string())] client_id: String,
        #[builder(default = crate::DEFAULT_CLIENT_DNS_TIMEOUT)] dns_timeout: Time,
        #[builder(default = crate::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT)]
        socket_connection_setup_timeout: Time,
        #[builder(default = crate::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX)]
        socket_connection_setup_timeout_max: Time,
        #[builder(default = crate::DEFAULT_RECONNECT_BACKOFF)] reconnect_backoff: Time,
        #[builder(default = crate::DEFAULT_RECONNECT_BACKOFF_MAX)] reconnect_backoff_max: Time,
        #[builder(default = crate::DEFAULT_CONNECTIONS_MAX_IDLE)] connections_max_idle: Time,
        #[builder(default)] client_dns_lookup: crate::ClientDnsLookup,
        #[builder(required, default = Some(crate::DEFAULT_SEND_BUFFER))] send_buffer: Option<
            krabka_units::ByteSize,
        >,
        #[builder(required, default = Some(crate::DEFAULT_RECEIVE_BUFFER))] receive_buffer: Option<
            krabka_units::ByteSize,
        >,
        #[builder(default = crate::DEFAULT_CLIENT_REQUEST_TIMEOUT)] request_timeout: Time,
        #[builder(default = crate::DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY)]
        dispatch_queue_capacity: usize,
        #[builder(default = crate::DEFAULT_CLIENT_FRAME_MAX)] frame_max: krabka_units::ByteSize,
        #[builder(default)] metadata_recovery_strategy: MetadataRecoveryStrategy,
        #[builder(default = crate::DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER)]
        metadata_recovery_rebootstrap_trigger: Time,
        security: Option<crate::security::ClientSecurity>,
        /// The topics of [`Client::refresh_metadata`]. Kafka's producer and
        /// consumer name their topics. The default asks for all topics.
        #[builder(default)]
        metadata_scope: MetadataScope,
        /// Push client metrics (KIP-714) as this kind of client: Kafka's
        /// `enable.metrics.push`. `None`, the default of this transport
        /// client, sends neither `GetTelemetrySubscriptions` nor
        /// `PushTelemetry`. The producer, consumer and admin builders set it.
        telemetry: Option<ClientTelemetryConfig>,
    ) -> Result<Self, ClientError> {
        let dns_timeout = ClientDnsTimeout::new(dns_timeout).map_err(ClientError::InvalidConfig)?;
        let dispatch_queue_capacity = ConnectionDispatchQueueCapacity::new(dispatch_queue_capacity)
            .map_err(ClientError::InvalidConfig)?;
        let frame_max = ClientFrameMax::try_from(frame_max).map_err(ClientError::InvalidConfig)?;
        let metadata_recovery_rebootstrap_trigger =
            MetadataRecoveryRebootstrapTrigger::new(metadata_recovery_rebootstrap_trigger)
                .map_err(ClientError::InvalidConfig)?;
        // Kafka builds the SSL engine factory when it builds the client, so a
        // store that does not load fails the client before any connection.
        // The connections reuse this configuration.
        if let Some(tls) = security.as_ref().and_then(|security| security.tls.as_ref()) {
            tls.connector()
                .map_err(|error| ClientError::InvalidConfig(error.to_string()))?;
        }
        let options = ConnectionOptions {
            client_id,
            dns_timeout,
            socket_connection_setup_timeout,
            socket_connection_setup_timeout_max,
            reconnect_backoff,
            reconnect_backoff_max,
            connections_max_idle,
            client_dns_lookup,
            send_buffer,
            receive_buffer,
            request_timeout,
            dispatch_queue_capacity,
            frame_max,
            security: security.map(Box::new),
            // `start_with_options` sets it for a client that pushes metrics.
            network_metrics: None,
        };
        Self::start_with_options(
            bootstrap,
            options,
            (
                metadata_recovery_strategy,
                metadata_recovery_rebootstrap_trigger,
            ),
            metadata_scope,
            telemetry,
        )
        .await
    }
}

impl Client {
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(bootstrap = %bootstrap, client_id = %options.client_id),
        err,
    )]
    async fn start_with_options(
        bootstrap: String,
        mut options: ConnectionOptions,
        (metadata_recovery_strategy, metadata_recovery_rebootstrap_trigger): (
            MetadataRecoveryStrategy,
            MetadataRecoveryRebootstrapTrigger,
        ),
        metadata_scope: MetadataScope,
        telemetry: Option<ClientTelemetryConfig>,
    ) -> Result<Self, ClientError> {
        let addrs = bootstrap::resolve_with_server_names(
            &bootstrap,
            options.dns_timeout,
            options.client_dns_lookup,
        )
        .await?;
        let metrics = telemetry.as_ref().map(|_| ClientMetrics::new());
        options.network_metrics =
            telemetry
                .as_ref()
                .zip(metrics.as_ref())
                .map(|(config, metrics)| {
                    NetworkMetrics::register(metrics, config.client_type.metric_group())
                });
        let pool = Arc::new(BrokerPool::new_with_server_names(addrs, options.clone()));
        let client = Client {
            bootstrap,
            pool,
            options,
            metadata_recovery: Arc::new(MetadataRecovery {
                strategy: metadata_recovery_strategy,
                trigger: metadata_recovery_rebootstrap_trigger,
                first_attempt: Mutex::new(None),
            }),
            metadata_topics: Arc::new(MetadataTopics::new(metadata_scope)),
            metrics,
            telemetry: None,
        };
        let telemetry = telemetry
            .zip(client.metrics.clone())
            .map(|(config, metrics)| {
                let timing = TelemetryTiming {
                    request_timeout: client.options.request_timeout.to_std(),
                    reconnect_backoff: client.options.reconnect_backoff.to_std(),
                };
                Arc::new(ClientTelemetry::start(
                    client.clone(),
                    &config,
                    metrics,
                    timing,
                ))
            });
        Ok(Client {
            telemetry,
            ..client
        })
    }

    /// The metric registry of a client that pushes its metrics (KIP-714).
    /// A client crate registers its own metrics in it.
    #[must_use]
    pub fn metrics(&self) -> Option<&ClientMetrics> {
        self.metrics.as_ref()
    }

    /// Send the terminating telemetry push (KIP-714), if the client pushes
    /// metrics and has a subscription, and wait for it. Kafka's clients
    /// send it from `close`. Without this call, the push goes out in the
    /// background once the last clone of the client drops.
    pub async fn close_telemetry(&self) {
        if let Some(telemetry) = &self.telemetry {
            telemetry.close().await;
        }
    }

    /// Send a request that has no fixed target broker.
    ///
    /// Before the first metadata response, the request goes to a bootstrap
    /// address. After it, the request goes to the least loaded known broker,
    /// as Kafka's `NetworkClient.leastLoadedNode` picks the node.
    #[tracing::instrument(level = "debug", skip_all, fields(api_key = R::API_KEY), err)]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError> {
        let conn = self.pool.least_loaded().await?;
        conn.send(req).await
    }

    /// Send a request, as [`send`](Self::send) routes it, only when the
    /// broker and client share a version at or above `min_version`.
    ///
    /// # Errors
    /// Returns [`ClientError::IncompatibleVersion`] before dispatch when the
    /// shared range is below `min_version`. Other errors match [`send`](Self::send).
    pub async fn send_at_least<R: ProtocolRequest>(
        &self,
        req: R,
        min_version: i16,
    ) -> Result<R::Response, ClientError> {
        let conn = self.pool.least_loaded().await?;
        let (broker_min, broker_max) = conn.advertised_api_range(R::API_KEY).unwrap_or((0, 0));
        let client_min = R::MIN_VERSION.max(min_version);
        let chosen = R::LATEST_STABLE_VERSION.min(broker_max);
        if chosen < client_min || chosen < broker_min {
            return Err(ClientError::IncompatibleVersion {
                api_key: R::API_KEY,
                broker_min,
                broker_max,
                client_min,
                client_max: R::LATEST_STABLE_VERSION,
            });
        }
        conn.send(req).await
    }

    /// Send a request, as [`send`](Self::send) routes it, without registering
    /// a pending response. Intended for protocol operations such as Produce
    /// `acks=0`.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be opened or cannot accept
    /// the encoded request.
    pub async fn send_no_response<R: ProtocolRequest>(&self, req: R) -> Result<(), ClientError> {
        let conn = self.pool.least_loaded().await?;
        conn.send_no_response(req).await
    }

    /// Drop the cached bootstrap connection and refresh the bootstrap address
    /// list from the original bootstrap string.
    ///
    /// Callers that know their bootstrap request is safe to retry can call
    /// this after a transport error and before they send again.
    #[tracing::instrument(level = "debug", skip_all, fields(bootstrap = %self.bootstrap))]
    pub async fn reconnect_bootstrap(&self) {
        self.pool.evict_bootstrap();
        // The retry goes to the bootstrap addresses even when the DNS refresh
        // below fails and the pool keeps the addresses that it has.
        self.pool.prefer_bootstrap();
        if let Ok(addrs) = bootstrap::resolve_with_server_names(
            &self.bootstrap,
            self.options.dns_timeout,
            self.options.client_dns_lookup,
        )
        .await
        {
            self.pool.replace_bootstrap_with_server_names(addrs);
        }
    }

    async fn rebootstrap_metadata(&self) -> Result<(), ClientError> {
        let addrs = bootstrap::resolve_with_server_names(
            &self.bootstrap,
            self.options.dns_timeout,
            self.options.client_dns_lookup,
        )
        .await?;
        self.pool.rebootstrap_with_server_names(addrs);
        self.metadata_recovery.complete_attempt();
        Ok(())
    }

    async fn request_metadata_from_current_cluster(
        &self,
        request: &MetadataRequest,
    ) -> Result<krabka_protocol::owned::metadata_response::MetadataResponse, ClientError> {
        let broker_ids = self.pool.broker_ids();
        if broker_ids.is_empty() {
            let connection = self.pool.bootstrap_connection().await?;
            return connection.send(request.clone()).await;
        }

        let mut last_error = None;
        let mut empty_response = None;
        for broker_id in broker_ids {
            let connection = match self.pool.get(broker_id).await {
                Ok(connection) => connection,
                Err(
                    error @ (ClientError::Connect { .. }
                    | ClientError::Tls { .. }
                    | ClientError::Sasl { .. }
                    | ClientError::Timeout(_)
                    | ClientError::Disconnected
                    | ClientError::Io(_)),
                ) => {
                    self.pool.evict(broker_id);
                    last_error = Some(error);
                    continue;
                }
                Err(error) => return Err(error),
            };
            match connection.send(request.clone()).await {
                Ok(response)
                    if response.error_code == REBOOTSTRAP_REQUIRED
                        || !response.brokers.is_empty() =>
                {
                    return Ok(response);
                }
                Ok(response) => empty_response = Some(response),
                Err(
                    error @ (ClientError::Connect { .. }
                    | ClientError::Timeout(_)
                    | ClientError::Disconnected
                    | ClientError::Io(_)),
                ) => {
                    self.pool.evict(broker_id);
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }

        empty_response.map_or_else(|| Err(last_error.unwrap_or(ClientError::Disconnected)), Ok)
    }

    /// The finalized feature levels (KIP-584) with the highest epoch that a
    /// connection of this client received in `ApiVersions`. Kafka's producer
    /// reads `transaction.version` from them. The epoch is
    /// [`UNKNOWN_FINALIZED_FEATURES_EPOCH`](crate::UNKNOWN_FINALIZED_FEATURES_EPOCH)
    /// before a connection to a broker that sends features.
    #[must_use]
    pub fn finalized_features(&self) -> crate::FinalizedFeatures {
        self.pool.finalized_features()
    }

    /// Whether the pool knows a dialable address for `broker_id`.
    ///
    /// The pool knows one when
    /// [`refresh_metadata`](Client::refresh_metadata) learned it and the port
    /// was not `0`. A caller can then choose between
    /// [`broker`](Client::broker) routing and the bootstrap
    /// [`send`](Client::send) without a speculative connect.
    // cargo-mutants: one-line delegation to BrokerPool::knows_broker
    #[must_use]
    #[cfg_attr(test, mutants::skip)]
    pub fn knows_broker(&self, broker_id: i32) -> bool {
        self.pool.knows_broker(broker_id)
    }

    /// Return a [`BrokerHandle`] that routes requests to a specific broker by id.
    ///
    /// [`refresh_metadata`] must have registered the broker first.
    ///
    /// [`refresh_metadata`]: Client::refresh_metadata
    #[must_use]
    pub fn broker(&self, broker_id: i32) -> BrokerHandle<'_> {
        BrokerHandle {
            client: self,
            broker_id,
        }
    }

    /// Drop the pooled connection to `broker_id` so the next request to it
    /// reconnects to its current advertised address.
    ///
    /// Call this after a send fails, so a bounced or failed-over broker is not
    /// retried over a dead socket.
    // cargo-mutants: one-line delegation to BrokerPool::evict
    #[cfg_attr(test, mutants::skip)]
    pub fn evict_broker(&self, broker_id: i32) {
        self.pool.evict(broker_id);
    }

    /// The topics that [`refresh_metadata`](Client::refresh_metadata) names.
    /// The clones of this client share them.
    #[must_use]
    pub fn metadata_topics(&self) -> &MetadataTopics {
        &self.metadata_topics
    }

    /// Send the `MetadataRequest` of [`metadata_topics`](Client::metadata_topics),
    /// parse the broker list from the response, refresh the pool's address
    /// registry, and return the typed response.
    ///
    /// With [`MetadataScope::AllTopics`] the request asks for all topics.
    /// With [`MetadataScope::Topics`] it names the current topics, as Kafka's
    /// producer and consumer do. Use
    /// [`refresh_metadata_with`](Client::refresh_metadata_with) for another
    /// request.
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn refresh_metadata(
        &self,
    ) -> Result<krabka_protocol::owned::metadata_response::MetadataResponse, ClientError> {
        let response = self
            .refresh_metadata_with(self.metadata_topics.request())
            .await?;
        self.metadata_topics.mark_refreshed();
        Ok(response)
    }

    /// Send `request`, parse the broker list from the response, refresh the
    /// pool's address registry, and return the typed response.
    ///
    /// Kafka's producer names its topics in the request
    /// (`ProducerMetadata.newMetadataRequestBuilder`). A request that names a
    /// topic lets the broker create the topic when
    /// `allow_auto_topic_creation` is set, and returns a per-topic error such
    /// as `TOPIC_AUTHORIZATION_FAILED` for it. Every retry and rebootstrap in
    /// this call sends the same request.
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    // cargo-mutants: live-broker metadata round-trip; not unit-testable
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(brokers = tracing::field::Empty),
        err,
    )]
    pub async fn refresh_metadata_with(
        &self,
        request: MetadataRequest,
    ) -> Result<krabka_protocol::owned::metadata_response::MetadataResponse, ClientError> {
        self.metadata_recovery.begin_attempt();
        let first = self.request_metadata_from_current_cluster(&request).await;
        let resp = match first {
            Ok(resp)
                if resp.error_code == REBOOTSTRAP_REQUIRED
                    && self.metadata_recovery.strategy == MetadataRecoveryStrategy::Rebootstrap =>
            {
                self.rebootstrap_metadata().await?;
                self.metadata_recovery.begin_attempt();
                self.request_metadata_from_current_cluster(&request).await?
            }
            Ok(resp) if resp.error_code == REBOOTSTRAP_REQUIRED => {
                return Err(ClientError::Server {
                    error_code: REBOOTSTRAP_REQUIRED,
                });
            }
            Ok(resp) if resp.brokers.is_empty() && self.metadata_recovery.timed_out() => {
                self.rebootstrap_metadata().await?;
                self.metadata_recovery.begin_attempt();
                self.request_metadata_from_current_cluster(&request).await?
            }
            Ok(resp) => resp,
            // `request_metadata_from_current_cluster` has exhausted every
            // broker in the last-known metadata. KIP-1102 permits immediate
            // rebootstrap at that boundary, and additionally covers a
            // reachable cluster that returns no usable metadata until the
            // configured trigger expires.
            Err(
                error @ (ClientError::Connect { .. }
                | ClientError::Tls { .. }
                | ClientError::Sasl { .. }
                | ClientError::Timeout(_)
                | ClientError::Disconnected
                | ClientError::Io(_)),
            ) => {
                if self.metadata_recovery.strategy == MetadataRecoveryStrategy::None {
                    return Err(error);
                }
                self.rebootstrap_metadata().await?;
                self.metadata_recovery.begin_attempt();
                self.request_metadata_from_current_cluster(&request).await?
            }
            Err(error) => return Err(error),
        };
        if resp.error_code == REBOOTSTRAP_REQUIRED {
            return Err(ClientError::Server {
                error_code: REBOOTSTRAP_REQUIRED,
            });
        }
        if !resp.brokers.is_empty() {
            self.metadata_recovery.complete_attempt();
        }
        let brokers: Vec<BrokerInfo> = resp
            .brokers
            .iter()
            .map(|b| BrokerInfo {
                id: b.node_id,
                host: b.host.clone(),
                port: b.port,
                rack: b.rack.clone(),
            })
            .collect();
        tracing::Span::current().record("brokers", brokers.len());
        self.pool.refresh_brokers(&brokers).await;
        Ok(resp)
    }

    /// Send a single-partition `OffsetForLeaderEpoch` (`api_key=23`) over the
    /// bootstrap connection.
    ///
    /// This is a thin wrapper over the free
    /// [`offset_for_leader_epoch`](crate::offset_for_leader_epoch) helper that
    /// the consumer's KIP-320 position-validation pass uses. `Client` does not
    /// otherwise expose its connection, so this method borrows the same
    /// bootstrap connection that `send` uses.
    ///
    /// # Errors
    /// Returns an error on transport or version-negotiation failure, or when
    /// the response does not contain the partition.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(topic = %topic, partition, current_leader_epoch, leader_epoch),
        err,
    )]
    pub async fn offset_for_leader_epoch(
        &self,
        topic: &str,
        partition: i32,
        current_leader_epoch: i32,
        leader_epoch: i32,
    ) -> Result<crate::offset_for_leader_epoch::EpochEndOffset, ClientError> {
        let conn = match self.pool.bootstrap_connection().await {
            Ok(conn) => conn,
            Err(
                ClientError::Connect { .. }
                | ClientError::Tls { .. }
                | ClientError::Sasl { .. }
                | ClientError::Timeout(_)
                | ClientError::Disconnected
                | ClientError::Io(_),
            ) => {
                self.reconnect_bootstrap().await;
                self.pool.bootstrap_connection().await?
            }
            Err(e) => return Err(e),
        };
        crate::offset_for_leader_epoch::offset_for_leader_epoch(
            &conn,
            topic,
            partition,
            current_leader_epoch,
            leader_epoch,
        )
        .await
    }

    /// Send a single-partition `OffsetForLeaderEpoch` (`api_key=23`) to a
    /// *specific* broker by id, through [`BrokerPool::get`].
    ///
    /// This method mirrors
    /// [`offset_for_leader_epoch`](Client::offset_for_leader_epoch) but
    /// targets the partition leader instead of the bootstrap connection.
    /// KIP-320 requires the validation RPC to reach the partition leader,
    /// which is the only replica with the authoritative epoch→end-offset
    /// history.
    ///
    /// The broker must already be in the pool's registry, which
    /// [`refresh_metadata`](Client::refresh_metadata) populates.
    ///
    /// # Errors
    /// Returns `Disconnected` if `broker_id` is not in the registry. Returns
    /// an error on transport or version-negotiation failure. Returns an error
    /// when the response does not contain the partition.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(broker_id, topic = %topic, partition, current_leader_epoch, leader_epoch),
        err,
    )]
    pub async fn offset_for_leader_epoch_on(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        current_leader_epoch: i32,
        leader_epoch: i32,
    ) -> Result<crate::offset_for_leader_epoch::EpochEndOffset, ClientError> {
        let conn = match self.pool.get(broker_id).await {
            Ok(conn) => conn,
            Err(
                ClientError::Connect { .. }
                | ClientError::Tls { .. }
                | ClientError::Sasl { .. }
                | ClientError::Timeout(_)
                | ClientError::Io(_),
            ) if self.pool.knows_broker(broker_id) => {
                self.pool.evict(broker_id);
                self.refresh_metadata().await?;
                self.pool.get(broker_id).await?
            }
            Err(e) => return Err(e),
        };
        crate::offset_for_leader_epoch::offset_for_leader_epoch(
            &conn,
            topic,
            partition,
            current_leader_epoch,
            leader_epoch,
        )
        .await
    }

    /// Fetch one partition from a specific broker with the requested isolation
    /// level. The broker address must have been learned through
    /// [`refresh_metadata`](Client::refresh_metadata).
    ///
    /// # Errors
    ///
    /// Returns a connection, protocol, or broker error. A failed connection to
    /// a known broker is evicted, metadata is refreshed, and the request is
    /// retried once against the broker's current advertised address.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            broker_id,
            topic = %fetch.topic,
            partition = fetch.partition,
            fetch_offset = fetch.fetch_offset,
            isolation_level = fetch.isolation_level,
        ),
        err,
    )]
    pub async fn fetch_partition_with_isolation_on(
        &self,
        broker_id: i32,
        fetch: crate::fetch::IsolatedFetch<'_>,
    ) -> Result<Vec<crate::fetch::FetchedRecord>, ClientError> {
        let conn = match self.pool.get(broker_id).await {
            Ok(conn) => conn,
            Err(ClientError::Disconnected) if !self.pool.knows_broker(broker_id) => {
                self.pool.bootstrap_connection().await?
            }
            Err(
                ClientError::Connect { .. }
                | ClientError::Tls { .. }
                | ClientError::Sasl { .. }
                | ClientError::Timeout(_)
                | ClientError::Io(_),
            ) if self.pool.knows_broker(broker_id) => {
                self.pool.evict(broker_id);
                self.refresh_metadata().await?;
                match self.pool.get(broker_id).await {
                    Ok(conn) => conn,
                    Err(ClientError::Disconnected) if !self.pool.knows_broker(broker_id) => {
                        self.pool.bootstrap_connection().await?
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        crate::fetch::fetch_partition_with_isolation(&conn, fetch).await
    }

    /// Close the client and all pooled connections.
    // cargo-mutants: teardown; delegates to BrokerPool::close_all
    #[cfg_attr(test, mutants::skip)]
    pub fn close(self) {
        if let Some(pool) = Arc::into_inner(self.pool) {
            pool.close_all();
        }
    }
}

impl TelemetryConnections for Client {
    fn open_connection(
        &self,
        preferred: Option<i32>,
    ) -> Option<(i32, Arc<crate::connection::Connection>)> {
        self.pool.open_connection(preferred)
    }
}

/// A handle to a specific broker within a [`Client`]'s pool.
///
/// [`Client::broker`] returns this handle.
pub struct BrokerHandle<'a> {
    client: &'a Client,
    broker_id: i32,
}

impl BrokerHandle<'_> {
    /// Send a request to this specific broker.
    ///
    /// When the pool has no dialable address for `broker_id`, this method
    /// falls back to the bootstrap connection. That happens when the broker
    /// advertises port `0`, which occurs on a single-broker cluster whose
    /// OS-assigned port never got rewritten in metadata, so
    /// [`BrokerPool::refresh_brokers`] deliberately skipped it. On such a
    /// cluster the bootstrap broker *is* this broker, for example the group
    /// coordinator a consumer routes to. The request therefore still reaches
    /// its intended target instead of failing with `Disconnected`.
    ///
    /// This fallback does not mask a *known* broker whose connect fails. It
    /// only triggers when the id was never in the registry.
    // cargo-mutants: live-broker send path; not unit-testable
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(broker_id = self.broker_id, api_key = R::API_KEY),
        err,
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError> {
        let conn = match self.client.pool.get(self.broker_id).await {
            Ok(conn) => conn,
            Err(ClientError::Disconnected) if !self.client.pool.knows_broker(self.broker_id) => {
                self.client.pool.bootstrap_connection().await?
            }
            Err(
                ClientError::Connect { .. }
                | ClientError::Tls { .. }
                | ClientError::Sasl { .. }
                | ClientError::Timeout(_)
                | ClientError::Io(_),
            ) if self.client.pool.knows_broker(self.broker_id) => {
                self.client.pool.evict(self.broker_id);
                self.client.refresh_metadata().await?;
                match self.client.pool.get(self.broker_id).await {
                    Ok(conn) => conn,
                    Err(ClientError::Disconnected)
                        if !self.client.pool.knows_broker(self.broker_id) =>
                    {
                        self.client.pool.bootstrap_connection().await?
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(e) => return Err(e),
        };
        conn.send(req).await
    }

    /// Send a request to this specific broker without waiting for a response.
    ///
    /// # Errors
    ///
    /// Returns an error if routing, connection setup, encoding, or writer
    /// enqueue fails.
    pub async fn send_no_response<R: ProtocolRequest>(&self, req: R) -> Result<(), ClientError> {
        let conn = match self.client.pool.get(self.broker_id).await {
            Ok(conn) => conn,
            Err(ClientError::Disconnected) if !self.client.pool.knows_broker(self.broker_id) => {
                self.client.pool.bootstrap_connection().await?
            }
            Err(
                ClientError::Connect { .. }
                | ClientError::Tls { .. }
                | ClientError::Sasl { .. }
                | ClientError::Timeout(_)
                | ClientError::Io(_),
            ) if self.client.pool.knows_broker(self.broker_id) => {
                self.client.pool.evict(self.broker_id);
                self.client.refresh_metadata().await?;
                match self.client.pool.get(self.broker_id).await {
                    Ok(conn) => conn,
                    Err(ClientError::Disconnected)
                        if !self.client.pool.knows_broker(self.broker_id) =>
                    {
                        self.client.pool.bootstrap_connection().await?
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        conn.send_no_response(req).await
    }
}

#[cfg(test)]
mod bootstrap_failover_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    };

    use bytes::BytesMut;
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            metadata_request::{self, MetadataRequestTopic},
            metadata_response::{
                FLEXIBLE_MIN as META_FLEXIBLE_MIN, MetadataResponse, MetadataResponseBroker,
            },
        },
    };
    use krabka_units::{bytes, mebibytes, millis};

    use super::*;
    use crate::mock::MockBroker;

    #[tokio::test]
    async fn zero_dns_timeout_is_rejected_before_resolution() {
        let result = Client::builder()
            .bootstrap("unused.invalid:9092")
            .dns_timeout(millis(0))
            .build()
            .await;
        assert2::assert!(matches!(result, Err(ClientError::InvalidConfig(_))));
    }

    #[tokio::test]
    async fn invalid_connection_resource_policy_fails_before_resolution() {
        let queue_result = Client::builder()
            .bootstrap("unused.invalid:9092")
            .dispatch_queue_capacity(0)
            .build()
            .await;
        let Err(queue_error) = queue_result else {
            panic!("zero queue capacity must fail");
        };
        assert2::assert!(queue_error.to_string().contains("dispatch queue capacity"));

        let frame_result = Client::builder()
            .bootstrap("unused.invalid:9092")
            .frame_max(mebibytes(100) + bytes(1))
            .build()
            .await;
        let Err(frame_error) = frame_result else {
            panic!("frame limit above fixed ceiling must fail");
        };
        assert2::assert!(frame_error.to_string().contains("client frame max"));

        assert2::assert!(
            MetadataRecoveryRebootstrapTrigger::new(Time::ZERO)
                .expect("zero is an immediate KIP-1102 trigger")
                .time()
                == Time::ZERO
        );
        let recovery_result = Client::builder()
            .bootstrap("unused.invalid:9092")
            .metadata_recovery_rebootstrap_trigger(Time::from_millis(-1))
            .build()
            .await;
        let Err(recovery_error) = recovery_result else {
            panic!("negative metadata recovery trigger must fail");
        };
        assert2::assert!(recovery_error.to_string().contains("metadata recovery"));
    }

    fn api_versions_v0() -> Vec<u8> {
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
                    api_key: metadata_request::API_KEY,
                    min_version: 0,
                    max_version: 13,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    fn metadata_v(version: i16, node_id: i32) -> Vec<u8> {
        metadata_v_with_port(version, node_id, 9092)
    }

    fn metadata_v_with_port(version: i16, node_id: i32, port: u16) -> Vec<u8> {
        let resp = MetadataResponse {
            brokers: vec![MetadataResponseBroker {
                node_id,
                host: "127.0.0.1".into(),
                port: i32::from(port),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        if version >= META_FLEXIBLE_MIN {
            buf.extend_from_slice(&[0x00u8]); // empty tagged fields
        }
        resp.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    fn empty_metadata_v(version: i16, error_code: i16) -> Vec<u8> {
        let resp = MetadataResponse {
            error_code,
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        if version >= META_FLEXIBLE_MIN {
            buf.extend_from_slice(&[0x00u8]);
        }
        resp.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    fn handler(
        node_id: i32,
    ) -> impl FnMut(i16, i16, i32, &[u8]) -> Option<Vec<u8>> + Send + 'static {
        move |api_key, version, _corr, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_v0());
            }
            if api_key == metadata_request::API_KEY {
                return Some(metadata_v(version, node_id));
            }
            None
        }
    }

    fn dynamic_port_handler(
        node_id: i32,
        port: Arc<AtomicU16>,
    ) -> impl FnMut(i16, i16, i32, &[u8]) -> Option<Vec<u8>> + Send + 'static {
        move |api_key, version, _corr, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_v0());
            }
            if api_key == metadata_request::API_KEY {
                return Some(metadata_v_with_port(
                    version,
                    node_id,
                    port.load(Ordering::SeqCst),
                ));
            }
            None
        }
    }

    /// Bounded poll: wait until `addr` refuses connections.
    ///
    /// At that point a stopped `MockBroker`'s listener has torn down, together
    /// with its per-connection handlers, which share the same cancelled token.
    /// This poll replaces a fixed settle after `stop()`. That sleep only
    /// waited for the socket teardown, so this helper polls the teardown
    /// directly and then runs the unchanged failover assertion. The timeout is
    /// a hang-guard.
    async fn wait_for_listener_closed(addr: std::net::SocketAddr) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while tokio::net::TcpStream::connect(addr).await.is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("broker listener must close after stop()");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_metadata_fails_over_when_bootstrap_broker_dies() {
        // Two bootstrap brokers. The client pins its bootstrap connection to the
        // first; when that broker is killed, `refresh_metadata` must evict the
        // dead bootstrap connection and reconnect to the second live broker
        // instead of failing forever on the dead socket — the client-side cause
        // of the on-cluster producer stall where a failover whose killed leader
        // was also the bootstrap-pinned broker left the producer stranded.
        let a = MockBroker::start(handler(0)).await;
        let b = MockBroker::start(handler(1)).await;
        let bootstrap = format!("{},{}", a.addr, b.addr);
        let client = Client::builder()
            .bootstrap(bootstrap)
            .request_timeout(millis(500))
            .build()
            .await
            .expect("client connects to bootstrap A");

        // First refresh pins the bootstrap connection to broker A.
        client
            .refresh_metadata()
            .await
            .expect("first refresh succeeds via A");

        // Broker A is killed. Poll until its listener is actually gone so the
        // pinned bootstrap connection is torn down before the failover refresh.
        let a_addr = a.addr;
        a.stop();
        wait_for_listener_closed(a_addr).await;

        // The next refresh must transparently fail over to the live broker B,
        // not hang/error on the dead A socket.
        let md = client
            .refresh_metadata()
            .await
            .expect("refresh must fail over to live bootstrap B after A dies");
        assert2::assert!(!md.brokers.is_empty());
    }

    /// `refresh_metadata_with` sends the given request, and the pool learns
    /// the brokers of the response as it does for `refresh_metadata`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_metadata_with_sends_the_request_and_learns_the_brokers() {
        let orders = MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some("orders".into()),
                ..Default::default()
            }]),
            allow_auto_topic_creation: true,
            ..Default::default()
        };
        for (name, request) in [
            ("all topics", MetadataRequest::default()),
            ("the topic orders", orders),
        ] {
            let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
            let handler_seen = Arc::clone(&seen);
            let broker = MockBroker::start(move |api_key, version, _corr, body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_v0());
                }
                if api_key == metadata_request::API_KEY {
                    let header_len =
                        2 + "krabka".len() + usize::from(version >= metadata_request::FLEXIBLE_MIN);
                    let mut request_body = &body[header_len..];
                    let decoded = MetadataRequest::decode(&mut request_body, version)
                        .expect("decode Metadata");
                    handler_seen.lock().expect("seen").push(decoded);
                    return Some(metadata_v(version, 7));
                }
                None
            })
            .await;
            let client = Client::builder()
                .bootstrap(broker.addr.to_string())
                .request_timeout(millis(500))
                .build()
                .await
                .expect("client connects");

            let refreshed = client.refresh_metadata_with(request.clone()).await.is_ok();
            let observed = (
                refreshed,
                seen.lock().expect("seen").clone(),
                client.knows_broker(7),
            );
            broker.stop();
            assert2::assert!(observed == (true, vec![request], true), "{name}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_metadata_uses_a_learned_broker_after_the_only_seed_retires() {
        let healthy_port = Arc::new(AtomicU16::new(0));
        let healthy_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let h_port = healthy_port.clone();
        let h_calls = healthy_calls.clone();
        let healthy = MockBroker::start(move |api_key, version, _corr, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_v0());
            }
            if api_key == metadata_request::API_KEY {
                h_calls.fetch_add(1, Ordering::SeqCst);
                return Some(metadata_v_with_port(
                    version,
                    7,
                    h_port.load(Ordering::SeqCst),
                ));
            }
            None
        })
        .await;
        healthy_port.store(healthy.addr.port(), Ordering::SeqCst);

        let seed_port = healthy_port.clone();
        let seed = MockBroker::start(dynamic_port_handler(7, seed_port)).await;
        let client = Client::builder()
            .bootstrap(seed.addr.to_string())
            .request_timeout(millis(500))
            .build()
            .await
            .expect("client resolves its only seed");
        client
            .refresh_metadata()
            .await
            .expect("seed supplies the current broker metadata");

        let seed_addr = seed.addr;
        seed.stop();
        wait_for_listener_closed(seed_addr).await;

        let metadata = client
            .refresh_metadata()
            .await
            .expect("learned broker remains usable after seed retirement");
        assert2::assert!(metadata.brokers[0].node_id == 7);
        assert2::assert!(healthy_calls.load(Ordering::SeqCst) == 1);
    }

    /// Kafka's `NetworkClient.leastLoadedNode`: after the first metadata
    /// response, a request with no fixed target goes to a known broker, not
    /// to the bootstrap address.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_goes_to_a_known_broker_after_the_first_metadata() {
        let counting_handler = |calls: Arc<std::sync::atomic::AtomicUsize>,
                                port: Arc<AtomicU16>| {
            move |api_key, version, _corr, _body: &[u8]| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_v0());
                }
                if api_key == metadata_request::API_KEY {
                    calls.fetch_add(1, Ordering::SeqCst);
                    return Some(metadata_v_with_port(
                        version,
                        7,
                        port.load(Ordering::SeqCst),
                    ));
                }
                None
            }
        };
        let port = Arc::new(AtomicU16::new(0));
        let seed_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let broker_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let broker = MockBroker::start(counting_handler(
            Arc::clone(&broker_calls),
            Arc::clone(&port),
        ))
        .await;
        port.store(broker.addr.port(), Ordering::SeqCst);
        let seed =
            MockBroker::start(counting_handler(Arc::clone(&seed_calls), Arc::clone(&port))).await;
        let client = Client::builder()
            .bootstrap(seed.addr.to_string())
            .request_timeout(millis(500))
            .build()
            .await
            .expect("client builds");

        client.send(MetadataRequest::default()).await.unwrap();
        client.refresh_metadata().await.unwrap();
        let before = (
            seed_calls.load(Ordering::SeqCst),
            broker_calls.load(Ordering::SeqCst),
        );
        for _ in 0..3 {
            client.send(MetadataRequest::default()).await.unwrap();
        }
        let after = (
            seed_calls.load(Ordering::SeqCst),
            broker_calls.load(Ordering::SeqCst),
        );
        seed.stop();
        broker.stop();
        assert2::assert!(before == (2, 0));
        assert2::assert!(after == (2, 3));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnect_bootstrap_forces_next_send_to_redial() {
        let a = MockBroker::start(handler(0)).await;
        let b = MockBroker::start(handler(1)).await;
        let bootstrap = format!("{},{}", a.addr, b.addr);
        let client = Client::builder()
            .bootstrap(bootstrap)
            .request_timeout(millis(500))
            .build()
            .await
            .expect("client builds");

        let _ = client
            .send(krabka_protocol::owned::metadata_request::MetadataRequest::default())
            .await
            .expect("first send succeeds via A");

        let a_addr = a.addr;
        a.stop();
        wait_for_listener_closed(a_addr).await;
        client.reconnect_bootstrap().await;

        let md = client
            .send(krabka_protocol::owned::metadata_request::MetadataRequest::default())
            .await
            .expect("send after reconnect_bootstrap reaches B");
        assert2::assert!(!md.brokers.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn broker_send_refreshes_metadata_when_learned_address_is_stale() {
        let a_port = Arc::new(AtomicU16::new(0));
        let b_port = Arc::new(AtomicU16::new(0));
        let a = MockBroker::start(dynamic_port_handler(1, a_port.clone())).await;
        a_port.store(a.addr.port(), Ordering::SeqCst);
        let b = MockBroker::start(dynamic_port_handler(1, b_port.clone())).await;
        b_port.store(b.addr.port(), Ordering::SeqCst);

        let bootstrap = format!("{},{}", a.addr, b.addr);
        let client = Client::builder()
            .bootstrap(bootstrap)
            .socket_connection_setup_timeout(millis(500))
            .request_timeout(millis(500))
            .build()
            .await
            .expect("client builds");

        client
            .refresh_metadata()
            .await
            .expect("first metadata refresh learns broker 1 at A");
        let a_addr = a.addr;
        a.stop();
        wait_for_listener_closed(a_addr).await;

        let md = client
            .broker(1)
            .send(krabka_protocol::owned::metadata_request::MetadataRequest::default())
            .await
            .expect("broker send should refresh metadata and redial broker 1 at B");
        assert2::assert!(!md.brokers.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_error_129_rebootstraps_immediately() {
        let handshakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let metadata_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let broker_port = Arc::new(AtomicU16::new(0));
        let h_handshakes = handshakes.clone();
        let h_metadata_calls = metadata_calls.clone();
        let h_broker_port = broker_port.clone();
        let broker = MockBroker::start(move |api_key, version, _corr, _body| {
            if api_key == api_versions_request::API_KEY {
                h_handshakes.fetch_add(1, Ordering::SeqCst);
                return Some(api_versions_v0());
            }
            if api_key == metadata_request::API_KEY {
                let call = h_metadata_calls.fetch_add(1, Ordering::SeqCst);
                return Some(if call == 0 {
                    empty_metadata_v(version, REBOOTSTRAP_REQUIRED)
                } else {
                    metadata_v_with_port(version, 7, h_broker_port.load(Ordering::SeqCst))
                });
            }
            None
        })
        .await;
        broker_port.store(broker.addr.port(), Ordering::SeqCst);
        let client = Client::builder()
            .bootstrap(broker.addr.to_string())
            .build()
            .await
            .expect("client builds");

        let metadata = client
            .refresh_metadata()
            .await
            .expect("error 129 triggers one rebootstrap and retry");

        assert2::assert!(metadata.brokers[0].node_id == 7);
        assert2::assert!(metadata_calls.load(Ordering::SeqCst) == 2);
        assert2::assert!(handshakes.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_metadata_timeout_rebootstraps_after_first_bad_attempt() {
        let handshakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let metadata_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let broker_port = Arc::new(AtomicU16::new(0));
        let h_handshakes = handshakes.clone();
        let h_metadata_calls = metadata_calls.clone();
        let h_broker_port = broker_port.clone();
        let broker = MockBroker::start(move |api_key, version, _corr, _body| {
            if api_key == api_versions_request::API_KEY {
                h_handshakes.fetch_add(1, Ordering::SeqCst);
                return Some(api_versions_v0());
            }
            if api_key == metadata_request::API_KEY {
                let call = h_metadata_calls.fetch_add(1, Ordering::SeqCst);
                return Some(if call < 2 {
                    empty_metadata_v(version, 0)
                } else {
                    metadata_v_with_port(version, 8, h_broker_port.load(Ordering::SeqCst))
                });
            }
            None
        })
        .await;
        broker_port.store(broker.addr.port(), Ordering::SeqCst);
        let client = Client::builder()
            .bootstrap(broker.addr.to_string())
            .metadata_recovery_rebootstrap_trigger(millis(20))
            .build()
            .await
            .expect("client builds");

        let first = client
            .refresh_metadata()
            .await
            .expect("first empty metadata response remains observable");
        assert2::assert!(first.brokers.is_empty());
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        let recovered = client
            .refresh_metadata()
            .await
            .expect("expired metadata attempt triggers rebootstrap");

        assert2::assert!(recovered.brokers[0].node_id == 8);
        assert2::assert!(metadata_calls.load(Ordering::SeqCst) == 3);
        assert2::assert!(handshakes.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_recovery_none_surfaces_error_129_without_retry() {
        let metadata_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let h_metadata_calls = metadata_calls.clone();
        let broker = MockBroker::start(move |api_key, version, _corr, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_v0());
            }
            if api_key == metadata_request::API_KEY {
                h_metadata_calls.fetch_add(1, Ordering::SeqCst);
                return Some(empty_metadata_v(version, REBOOTSTRAP_REQUIRED));
            }
            None
        })
        .await;
        let client = Client::builder()
            .bootstrap(broker.addr.to_string())
            .metadata_recovery_strategy(MetadataRecoveryStrategy::None)
            .build()
            .await
            .expect("client builds");

        let error = client
            .refresh_metadata()
            .await
            .expect_err("disabled recovery surfaces the proxy request");

        assert2::assert!(matches!(
            error,
            ClientError::Server {
                error_code: REBOOTSTRAP_REQUIRED
            }
        ));
        assert2::assert!(metadata_calls.load(Ordering::SeqCst) == 1);
    }
}

#[cfg(test)]
mod authentication_failure_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use bytes::BytesMut;
    use krabka_protocol::{
        Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            metadata_request::{self, MetadataRequest},
            metadata_response::{MetadataResponse, MetadataResponseBroker},
            sasl_handshake_request,
        },
    };
    use krabka_security::ListenerProtocol;

    use super::*;
    use crate::{
        OutboundSaslError, SaslAuthenticationError,
        error::AuthenticationError,
        mock::{MockBroker, MockReply, MockSaslAnswer},
        security::{ClientSecurity, SaslCredentials},
    };

    const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
    const ILLEGAL_SASL_STATE: i16 = 34;
    const UNSUPPORTED_VERSION: i16 = 35;
    const SASL_AUTHENTICATION_FAILED: i16 = 58;
    const BROKER_ID: i32 = 1;

    /// The request that the test sends after the SASL listener starts.
    #[derive(Clone, Copy, Debug)]
    enum Route {
        /// `Client::refresh_metadata` over the bootstrap connection.
        Bootstrap,
        /// `BrokerHandle::send` to a broker that a first metadata refresh
        /// registered.
        Broker,
    }

    /// How a call ended, as a caller classifies it.
    #[derive(Debug, PartialEq, Eq)]
    enum Outcome {
        Ok,
        Rejected(SaslAuthenticationError),
        SaslServerError(i16),
        Other(String),
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Observed {
        outcome: Outcome,
        handshakes: usize,
    }

    fn classify<T>(result: &Result<T, ClientError>) -> Outcome {
        match result {
            Ok(_) => Outcome::Ok,
            Err(ClientError::Authentication {
                source: AuthenticationError::Sasl(error),
                ..
            }) => Outcome::Rejected(error.clone()),
            Err(ClientError::Sasl {
                source: OutboundSaslError::Server { error_code, .. },
                ..
            }) => Outcome::SaslServerError(*error_code),
            Err(error) => Outcome::Other(error.to_string()),
        }
    }

    fn encode(response: &impl Encode, version: i16) -> Vec<u8> {
        let mut body = BytesMut::new();
        response.encode(&mut body, version).unwrap();
        body.to_vec()
    }

    /// A SASL listener that answers the exchange of connection `n` with
    /// `script[n]`, or with the last entry once the script runs out. It also
    /// answers `ApiVersions` and a `Metadata` that names itself as broker 1.
    async fn sasl_broker(script: Vec<MockSaslAnswer>, handshakes: Arc<AtomicUsize>) -> MockBroker {
        let port = Arc::new(std::sync::atomic::AtomicU16::new(0));
        let handler_port = Arc::clone(&port);
        let mut answer = *script.last().unwrap();
        let broker = MockBroker::start_with_replies(move |api_key, version, _corr, _body| {
            if api_key == sasl_handshake_request::API_KEY {
                let connection = handshakes.fetch_add(1, Ordering::SeqCst);
                answer = script
                    .get(connection)
                    .copied()
                    .unwrap_or_else(|| *script.last().unwrap());
            }
            if let Some(reply) = answer.reply(api_key, version) {
                return reply;
            }
            match api_key {
                api_versions_request::API_KEY => MockReply::Respond(encode(
                    &ApiVersionsResponse {
                        api_keys: vec![
                            ApiVersion {
                                api_key: api_versions_request::API_KEY,
                                min_version: 0,
                                max_version: 3,
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
                    },
                    0,
                )),
                metadata_request::API_KEY => MockReply::Respond(encode(
                    &MetadataResponse {
                        brokers: vec![MetadataResponseBroker {
                            node_id: BROKER_ID,
                            host: "127.0.0.1".into(),
                            port: i32::from(handler_port.load(Ordering::SeqCst)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    version,
                )),
                _ => MockReply::Silent,
            }
        })
        .await;
        port.store(broker.addr.port(), Ordering::SeqCst);
        broker
    }

    fn plain_security() -> ClientSecurity {
        ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain {
                username: "alice".into(),
                password: "secret".into(),
            }),
            sasl_host: None,
        }
    }

    fn authenticate_failed() -> SaslAuthenticationError {
        SaslAuthenticationError::Failed(
            "SaslAuthenticate(PLAIN) error_code=58 error_message=Some(\"rejected by mock broker\")"
                .into(),
        )
    }

    fn unsupported_mechanism() -> SaslAuthenticationError {
        SaslAuthenticationError::UnsupportedMechanism(
            "client SASL mechanism 'PLAIN' not enabled in the server, enabled mechanisms are \
             [\"PLAIN\"]"
                .into(),
        )
    }

    fn illegal_state() -> SaslAuthenticationError {
        SaslAuthenticationError::IllegalState(
            "unexpected handshake request with client mechanism PLAIN, enabled mechanisms are \
             [\"PLAIN\"]"
                .into(),
        )
    }

    /// Kafka's `NetworkClient.processDisconnection` stores an
    /// `AuthenticationException` for the node, and the next call raises it
    /// with no retry. A disconnect during authentication with no broker error
    /// stays retriable. The client must not evict and redial on a rejection.
    #[tokio::test]
    async fn sasl_rejection_is_raised_without_a_retry() {
        use MockSaslAnswer::{Accept, AuthenticateError, CloseAfterHandshake, HandshakeError};

        let cases = [
            (
                "bootstrap: SaslAuthenticate 58",
                Route::Bootstrap,
                vec![AuthenticateError(SASL_AUTHENTICATION_FAILED)],
                Observed {
                    outcome: Outcome::Rejected(authenticate_failed()),
                    handshakes: 1,
                },
            ),
            (
                "bootstrap: SaslHandshake 33",
                Route::Bootstrap,
                vec![HandshakeError(UNSUPPORTED_SASL_MECHANISM)],
                Observed {
                    outcome: Outcome::Rejected(unsupported_mechanism()),
                    handshakes: 1,
                },
            ),
            (
                "bootstrap: SaslHandshake 34",
                Route::Bootstrap,
                vec![HandshakeError(ILLEGAL_SASL_STATE)],
                Observed {
                    outcome: Outcome::Rejected(illegal_state()),
                    handshakes: 1,
                },
            ),
            (
                "bootstrap: SaslAuthenticate code that is not an authentication error",
                Route::Bootstrap,
                vec![AuthenticateError(UNSUPPORTED_VERSION)],
                Observed {
                    outcome: Outcome::SaslServerError(UNSUPPORTED_VERSION),
                    handshakes: 2,
                },
            ),
            (
                "bootstrap: close after SaslHandshake, then accept",
                Route::Bootstrap,
                vec![CloseAfterHandshake, Accept],
                Observed {
                    outcome: Outcome::Ok,
                    handshakes: 2,
                },
            ),
            (
                "broker: SaslAuthenticate 58",
                Route::Broker,
                vec![Accept, AuthenticateError(SASL_AUTHENTICATION_FAILED)],
                Observed {
                    outcome: Outcome::Rejected(authenticate_failed()),
                    handshakes: 2,
                },
            ),
            (
                "broker: SaslHandshake 33",
                Route::Broker,
                vec![Accept, HandshakeError(UNSUPPORTED_SASL_MECHANISM)],
                Observed {
                    outcome: Outcome::Rejected(unsupported_mechanism()),
                    handshakes: 2,
                },
            ),
            (
                "broker: close after SaslHandshake, then accept",
                Route::Broker,
                vec![Accept, CloseAfterHandshake, Accept],
                Observed {
                    outcome: Outcome::Ok,
                    handshakes: 3,
                },
            ),
        ];

        for (name, route, script, expected) in cases {
            let handshakes = Arc::new(AtomicUsize::new(0));
            let broker = sasl_broker(script, Arc::clone(&handshakes)).await;
            let client = Client::builder()
                .bootstrap(broker.addr.to_string())
                .security(plain_security())
                .build()
                .await
                .unwrap();

            let outcome = match route {
                Route::Bootstrap => classify(&client.refresh_metadata().await),
                Route::Broker => {
                    client.refresh_metadata().await.unwrap();
                    classify(
                        &client
                            .broker(BROKER_ID)
                            .send(MetadataRequest::default())
                            .await,
                    )
                }
            };
            let observed = Observed {
                outcome,
                handshakes: handshakes.load(Ordering::SeqCst),
            };

            broker.stop();
            assert2::check!(observed == expected, "case {name}");
        }
    }
}
