//! Client metrics push (KIP-714).
//!
//! When Kafka's `enable.metrics.push` is on (the default), a producer,
//! consumer or admin client asks a broker for its client metrics
//! subscription with `GetTelemetrySubscriptions`, and pushes the metrics that
//! the subscription names with `PushTelemetry` at the push interval. On close
//! it sends one last push with `terminating` set.
//!
//! [`ClientTelemetry`] is that reporter, as Kafka's `ClientTelemetryReporter`
//! and the telemetry sender of its `NetworkClient` are. A [`Client`] built
//! with [`ClientTelemetryConfig`] starts one. The metrics come from a
//! [`ClientMetrics`] registry; client-core registers the network metrics of
//! Kafka's `Selector` in it.
//!
//! Differences from Kafka:
//!
//! - The reporter sends on a connection that the client already has open. It
//!   does not open a connection of its own, so a client that has no open
//!   connection pushes nothing until it opens one.
//! - A subscription with the zero client instance id stops telemetry. Kafka's
//!   reporter throws from its response handler, after which it sends no
//!   further request.
//!
//! [`Client`]: crate::Client

mod metrics;
pub mod otlp;
mod sender;

use std::{sync::Arc, time::Duration};

use krabka_protocol::ProtocolRequest;
use krabka_units::{Time, convert::TimeExt as _};
use tokio::{sync::watch, time::Instant};
use tokio_util::sync::CancellationToken;

pub(crate) use self::metrics::ConnectionMetrics;
pub use self::metrics::{
    ClientMetrics, Counter, Gauge, MetricKey, NetworkMetrics, TELEMETRY_METRIC_DOMAIN,
};
use self::{
    metrics::{Collector, unix_nanos_now},
    otlp::{KeyValue, Resource},
    sender::{Failure, TelemetryRequest, TelemetrySender, TelemetryState},
};
use crate::{connection::Connection, error::ClientError};

/// The kind of client, which names its metric group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TelemetryClientType {
    Producer,
    Consumer,
    Admin,
}

impl TelemetryClientType {
    /// The group of the client's own metrics, such as the network metrics:
    /// `producer-metrics`, `consumer-metrics` or `admin-client-metrics`.
    #[must_use]
    pub const fn metric_group(self) -> &'static str {
        match self {
            Self::Producer => "producer-metrics",
            Self::Consumer => "consumer-metrics",
            Self::Admin => "admin-client-metrics",
        }
    }
}

/// What a client pushes: its type and the resource attributes of every
/// metric (`ClientTelemetryProvider.contextChange`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientTelemetryConfig {
    pub client_type: TelemetryClientType,
    /// The attributes of the OTLP resource, in order.
    pub resource_attributes: Vec<(String, String)>,
}

impl ClientTelemetryConfig {
    /// A producer. Kafka adds `transactional_id` when it is set.
    #[must_use]
    pub fn producer(transactional_id: Option<&str>) -> Self {
        Self::with_attributes(
            TelemetryClientType::Producer,
            [("transactional_id", transactional_id)],
        )
    }

    /// A consumer. Kafka adds `group_id`, `group_instance_id` and
    /// `client_rack` when they are set.
    #[must_use]
    pub fn consumer(
        group_id: Option<&str>,
        group_instance_id: Option<&str>,
        client_rack: Option<&str>,
    ) -> Self {
        Self::with_attributes(
            TelemetryClientType::Consumer,
            [
                ("group_id", group_id),
                ("group_instance_id", group_instance_id),
                ("client_rack", client_rack),
            ],
        )
    }

    /// An admin client.
    #[must_use]
    pub fn admin() -> Self {
        Self::with_attributes(TelemetryClientType::Admin, [])
    }

    fn with_attributes<'a>(
        client_type: TelemetryClientType,
        attributes: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
    ) -> Self {
        Self {
            client_type,
            resource_attributes: attributes
                .into_iter()
                .filter_map(|(key, value)| {
                    value
                        .filter(|value| !value.is_empty())
                        .map(|value| (key.to_owned(), value.to_owned()))
                })
                .collect(),
        }
    }

    fn resource(&self) -> Resource {
        Resource {
            attributes: self
                .resource_attributes
                .iter()
                .map(|(key, value)| KeyValue::new(key, value))
                .collect(),
        }
    }
}

/// The connections that a reporter sends on.
pub trait TelemetryConnections: Send + Sync + 'static {
    /// An open connection with its broker id, preferring the broker of
    /// `preferred` while it stays open. Per KIP-714 a client keeps pushing to
    /// the same broker. `None` when no connection is open.
    fn open_connection(&self, preferred: Option<i32>) -> Option<(i32, Arc<Connection>)>;
}

/// The timing of a reporter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TelemetryTiming {
    /// Kafka's `request.timeout.ms`: the wait for a response, and the bound
    /// on the terminating push after a close.
    pub request_timeout: Duration,
    /// Kafka's `reconnect.backoff.ms`: the wait for an open connection.
    pub reconnect_backoff: Duration,
}

/// A running KIP-714 reporter. Dropping it starts the close, as
/// [`close`](Self::close) does, without the wait.
#[derive(Debug)]
pub struct ClientTelemetry {
    close: CancellationToken,
    done: CancellationToken,
    /// The client instance id of the last subscription, `None` until the
    /// first one loads. The reporter task publishes it.
    instance_id: Arc<watch::Sender<Option<uuid::Uuid>>>,
}

impl ClientTelemetry {
    /// Start a reporter task that sends on `connections` and pushes the
    /// metrics of `metrics`.
    ///
    /// # Panics
    /// Panics outside a Tokio runtime.
    #[must_use]
    pub fn start(
        connections: impl TelemetryConnections,
        config: &ClientTelemetryConfig,
        metrics: ClientMetrics,
        timing: TelemetryTiming,
    ) -> Self {
        let close = CancellationToken::new();
        let done = CancellationToken::new();
        let instance_id = Arc::new(watch::Sender::new(None));
        let sender = TelemetrySender::new(Collector::new(metrics, config.resource()));
        tokio::spawn(run(
            connections,
            sender,
            timing,
            Arc::clone(&instance_id),
            (close.clone(), done.clone()),
        ));
        Self {
            close,
            done,
            instance_id,
        }
    }

    /// The client instance id that the broker assigned, waiting up to
    /// `timeout` for the first subscription to load. `None` when none has
    /// loaded by then, as Kafka's `ClientTelemetrySender.clientInstanceId`
    /// returns an empty `Optional`. A zero `timeout` does not wait.
    pub async fn client_instance_id(&self, timeout: Duration) -> Option<uuid::Uuid> {
        let mut loaded = self.instance_id.subscribe();
        tokio::time::timeout(timeout, loaded.wait_for(Option::is_some))
            .await
            .ok()
            .and_then(Result::ok)
            .and_then(|id| *id)
    }

    /// Send the terminating push, if the client has a subscription, and wait
    /// for the reporter to end. The wait is at most about two request
    /// timeouts: one for a request in flight and one for the terminating
    /// push.
    pub async fn close(&self) {
        self.close.cancel();
        self.done.cancelled().await;
    }
}

impl Drop for ClientTelemetry {
    fn drop(&mut self) {
        self.close.cancel();
    }
}

/// Kafka's `clientInstanceId` of a producer, consumer or admin client
/// (`ClientTelemetryUtils.fetchClientInstanceId`): the broker-assigned
/// client instance id, waiting up to `timeout` for the first subscription.
/// `Ok(None)` when none has loaded by then, as Kafka returns `null`.
///
/// # Errors
/// Returns [`ClientError::InvalidArgument`] for a negative or non-finite
/// `timeout`, and [`ClientError::TelemetryDisabled`] when the client does not
/// push metrics (`telemetry` is `None`), as Kafka throws an
/// `IllegalArgumentException` and an `IllegalStateException`.
pub async fn client_instance_id(
    telemetry: Option<&ClientTelemetry>,
    timeout: Time,
) -> Result<Option<uuid::Uuid>, ClientError> {
    if !timeout.secs_f64().is_finite() || timeout < Time::ZERO {
        return Err(ClientError::InvalidArgument(
            "The timeout cannot be negative.".to_owned(),
        ));
    }
    let telemetry = telemetry.ok_or(ClientError::TelemetryDisabled)?;
    Ok(telemetry.client_instance_id(timeout.to_std()).await)
}

/// The reporter task: Kafka's `NetworkClient.TelemetrySender.maybeUpdate`
/// in a loop.
async fn run(
    connections: impl TelemetryConnections,
    mut sender: TelemetrySender,
    timing: TelemetryTiming,
    instance_id: Arc<watch::Sender<Option<uuid::Uuid>>>,
    (close, done): (CancellationToken, CancellationToken),
) {
    let mut closing: Option<Instant> = None;
    let mut sticky: Option<i32> = None;
    loop {
        if closing.is_none() && close.is_cancelled() {
            closing = Some(Instant::now() + timing.request_timeout);
            if !sender.initiate_close() {
                break;
            }
        }
        let now = Instant::now();
        if closing.is_some_and(|deadline| now >= deadline) {
            break;
        }
        match sender.time_to_next_update(now, timing.request_timeout) {
            Some(wait) if wait.is_zero() => {}
            // During the close the only request is the terminating push,
            // which is due at once.
            _ if closing.is_some() => break,
            Some(wait) => {
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = close.cancelled() => {}
                }
                continue;
            }
            None => {
                close.cancelled().await;
                continue;
            }
        }
        let Some((broker_id, connection)) = connections.open_connection(sticky) else {
            sticky = None;
            if closing.is_some() {
                break;
            }
            tokio::select! {
                () = tokio::time::sleep(timing.reconnect_backoff) => {}
                () = close.cancelled() => {}
            }
            continue;
        };
        sticky = Some(broker_id);
        let Some(request) = sender.create_request(unix_nanos_now()) else {
            continue;
        };
        let failed = match request {
            TelemetryRequest::Subscriptions(request) => match send(&connection, request).await {
                Ok(response) => {
                    sender.handle_subscriptions_response(
                        &response,
                        Instant::now(),
                        crate::backoff::random_unit(),
                    );
                    // Kafka's `subscriptionLoaded.signalAll()`.
                    let loaded = sender
                        .client_instance_id()
                        .map(|id| uuid::Uuid::from_bytes(id.0));
                    instance_id.send_if_modified(|current| {
                        let changed = loaded.is_some() && *current != loaded;
                        if changed {
                            *current = loaded;
                        }
                        changed
                    });
                    None
                }
                Err(failure) => Some(failure),
            },
            TelemetryRequest::Push(request) => match send(&connection, request).await {
                Ok(response) => {
                    sender.handle_push_response(&response, Instant::now());
                    None
                }
                Err(failure) => Some(failure),
            },
        };
        if let Some(failure) = failed {
            // Per KIP-714, a failed broker is left for another one.
            sticky = None;
            sender.handle_failed_request(failure, Instant::now());
        }
        if sender.state() == TelemetryState::TerminatingPushInProgress {
            break;
        }
    }
    sender.close();
    done.cancel();
}

/// Send a telemetry request on `connection`. A broker that does not list
/// the API fails the request as Kafka's `UnsupportedVersionException` does.
async fn send<R: ProtocolRequest>(
    connection: &Connection,
    request: R,
) -> Result<R::Response, Failure> {
    if connection.advertised_api_range(R::API_KEY).is_none() {
        tracing::debug!(
            api_key = R::API_KEY,
            "broker does not support client telemetry"
        );
        return Err(Failure::Fatal);
    }
    connection.send(request).await.map_err(|error| {
        tracing::debug!(%error, api_key = R::API_KEY, "telemetry request failed");
        failure(&error)
    })
}

/// A disconnect or timeout is Kafka's `RetriableException`.
fn failure(error: &ClientError) -> Failure {
    match error {
        ClientError::Disconnected
        | ClientError::Timeout(_)
        | ClientError::Io(_)
        | ClientError::Connect { .. }
        | ClientError::Tls { .. }
        | ClientError::Sasl { .. } => Failure::Retriable,
        _ => Failure::Fatal,
    }
}

#[cfg(test)]
mod tests;
