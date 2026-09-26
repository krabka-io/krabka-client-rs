//! The state machine of Kafka's `ClientTelemetryReporter.DefaultClientTelemetrySender`.
//!
//! [`TelemetrySender`] decides when the next telemetry request is due, builds
//! it, and moves through [`TelemetryState`] on each response or failure. It
//! does no I/O: the reporter task sends the requests and passes the time in.

use std::time::Duration;

use bytes::Bytes;
use krabka_compression::CompressionType;
use krabka_protocol::{
    owned::{
        get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest,
        get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse,
        push_telemetry_request::PushTelemetryRequest,
        push_telemetry_response::PushTelemetryResponse,
    },
    primitives::uuid::Uuid,
};
use tokio::time::Instant;

use super::metrics::{Collector, MetricSelector};

/// `ClientTelemetryReporter.DEFAULT_PUSH_INTERVAL_MS`.
pub(crate) const DEFAULT_PUSH_INTERVAL: Duration = Duration::from_mins(5);
const DEFAULT_PUSH_INTERVAL_MS: i32 = 5 * 60 * 1000;

/// The bounds of the factor on the push interval before the first push of a
/// subscription.
const INITIAL_PUSH_JITTER_LOWER: f64 = 0.5;
const INITIAL_PUSH_JITTER_UPPER: f64 = 1.5;

const UNSUPPORTED_VERSION: i16 = 35;
const INVALID_REQUEST: i16 = 42;
const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;
const INVALID_RECORD: i16 = 87;
const THROTTLING_QUOTA_EXCEEDED: i16 = 89;
const UNKNOWN_SUBSCRIPTION_ID: i16 = 117;
const TELEMETRY_TOO_LARGE: i16 = 118;

/// Kafka's `ClientTelemetryState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TelemetryState {
    SubscriptionNeeded,
    SubscriptionInProgress,
    PushNeeded,
    PushInProgress,
    TerminatingPushNeeded,
    TerminatingPushInProgress,
    Terminated,
}

impl TelemetryState {
    /// `ClientTelemetryState.validateTransition`.
    const fn allows(self, next: Self) -> bool {
        use TelemetryState::{
            PushInProgress, PushNeeded, SubscriptionInProgress, SubscriptionNeeded, Terminated,
            TerminatingPushInProgress, TerminatingPushNeeded,
        };
        matches!(
            (self, next),
            (SubscriptionNeeded, SubscriptionInProgress | Terminated)
                | (
                    SubscriptionInProgress | PushNeeded | PushInProgress,
                    SubscriptionNeeded | TerminatingPushNeeded | Terminated,
                )
                | (SubscriptionInProgress | PushInProgress, PushNeeded)
                | (PushNeeded, PushInProgress)
                | (
                    TerminatingPushNeeded,
                    TerminatingPushInProgress | Terminated
                )
                | (TerminatingPushInProgress, Terminated)
        )
    }

    const fn is_terminating(self) -> bool {
        matches!(
            self,
            Self::TerminatingPushNeeded | Self::TerminatingPushInProgress | Self::Terminated
        )
    }
}

/// The subscription of the last `GetTelemetrySubscriptions` response
/// (`ClientTelemetrySubscription`).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Subscription {
    pub(crate) client_instance_id: Uuid,
    pub(crate) id: i32,
    pub(crate) push_interval_ms: i32,
    /// The known codecs of the broker's list, in its order of preference.
    pub(crate) accepted_compression_types: Vec<CompressionType>,
    pub(crate) delta_temporality: bool,
    pub(crate) selector: MetricSelector,
}

/// The wait after an error code (`ClientTelemetryUtils.maybeFetchErrorIntervalMs`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ErrorInterval {
    /// The error cannot be resolved by a retry: telemetry stops.
    Disable,
    /// Fetch the subscription again after this wait.
    Retry(Duration),
}

/// `maybeFetchErrorIntervalMs`: `None` for no error.
pub(crate) fn error_interval(
    error_code: i16,
    push_interval_ms: Option<i32>,
) -> Option<ErrorInterval> {
    match error_code {
        0 => None,
        INVALID_REQUEST | INVALID_RECORD | UNSUPPORTED_VERSION => Some(ErrorInterval::Disable),
        UNKNOWN_SUBSCRIPTION_ID | UNSUPPORTED_COMPRESSION_TYPE => {
            Some(ErrorInterval::Retry(Duration::ZERO))
        }
        TELEMETRY_TOO_LARGE | THROTTLING_QUOTA_EXCEEDED => Some(ErrorInterval::Retry(millis(
            push_interval_ms.unwrap_or(DEFAULT_PUSH_INTERVAL_MS),
        ))),
        _ => {
            tracing::error!(
                error_code,
                "unmapped telemetry error code, disabling telemetry"
            );
            Some(ErrorInterval::Disable)
        }
    }
}

fn millis(ms: i32) -> Duration {
    Duration::from_millis(u64::try_from(ms).unwrap_or(0))
}

/// How a telemetry request failed without a response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Failure {
    /// A disconnect or a timeout: Kafka's `RetriableException`.
    Retriable,
    /// The broker does not support the API, or another error that a retry
    /// does not fix.
    Fatal,
}

/// A telemetry request to send.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TelemetryRequest {
    Subscriptions(GetTelemetrySubscriptionsRequest),
    Push(PushTelemetryRequest),
}

/// Kafka's `DefaultClientTelemetrySender`.
#[derive(Debug)]
pub(crate) struct TelemetrySender {
    state: TelemetryState,
    subscription: Option<Subscription>,
    /// When the last request was made or its response handled. `None`
    /// before the first response: Kafka's `lastRequestMs == 0`.
    last_request: Option<Instant>,
    interval: Duration,
    /// `false` after a response or failure that a retry cannot fix.
    enabled: bool,
    /// Codecs that failed to compress a payload.
    unsupported_compression_types: Vec<CompressionType>,
    collector: Collector,
}

impl TelemetrySender {
    pub(crate) fn new(collector: Collector) -> Self {
        Self {
            state: TelemetryState::SubscriptionNeeded,
            subscription: None,
            last_request: None,
            interval: Duration::ZERO,
            enabled: true,
            unsupported_compression_types: Vec::new(),
            collector,
        }
    }

    pub(crate) fn state(&self) -> TelemetryState {
        self.state
    }

    /// The wait before [`create_request`](Self::create_request), or `None`
    /// when no request will follow (`timeToNextUpdate`).
    pub(crate) fn time_to_next_update(
        &self,
        now: Instant,
        request_timeout: Duration,
    ) -> Option<Duration> {
        if !self.enabled {
            return None;
        }
        match self.state {
            TelemetryState::SubscriptionInProgress | TelemetryState::PushInProgress => {
                Some(request_timeout)
            }
            TelemetryState::TerminatingPushInProgress | TelemetryState::Terminated => None,
            TelemetryState::TerminatingPushNeeded => Some(Duration::ZERO),
            TelemetryState::SubscriptionNeeded | TelemetryState::PushNeeded => {
                Some(self.last_request.map_or(Duration::ZERO, |last| {
                    (last + self.interval).saturating_duration_since(now)
                }))
            }
        }
    }

    /// Kafka's `maybeSetState`: a transition that the state machine does not
    /// allow disables telemetry.
    fn set_state(&mut self, next: TelemetryState) -> bool {
        if self.state.allows(next) {
            tracing::debug!(from = ?self.state, to = ?next, "telemetry state");
            self.state = next;
            true
        } else {
            tracing::warn!(from = ?self.state, to = ?next, "invalid telemetry state transition, disabling telemetry");
            self.enabled = false;
            false
        }
    }

    /// `updateErrorResult`.
    fn update_error_result(&mut self, interval: ErrorInterval, now: Instant) {
        match interval {
            ErrorInterval::Disable => self.enabled = false,
            ErrorInterval::Retry(interval) => self.interval = interval,
        }
        self.last_request = Some(now);
    }

    /// The request that is due, or `None` when the state has none
    /// (`createRequest`). `now_unix_nanos` stamps the metrics of a push.
    pub(crate) fn create_request(&mut self, now_unix_nanos: u64) -> Option<TelemetryRequest> {
        match self.state {
            TelemetryState::SubscriptionNeeded => {
                // Per KIP-714 the first request carries the zero UUID, which
                // asks the broker to assign a client instance id.
                let client_instance_id = self
                    .subscription
                    .as_ref()
                    .map_or(Uuid::ZERO, |subscription| subscription.client_instance_id);
                self.set_state(TelemetryState::SubscriptionInProgress)
                    .then(|| {
                        TelemetryRequest::Subscriptions(GetTelemetrySubscriptionsRequest {
                            client_instance_id,
                            ..Default::default()
                        })
                    })
            }
            TelemetryState::PushNeeded | TelemetryState::TerminatingPushNeeded => {
                self.create_push_request(now_unix_nanos)
            }
            state => {
                tracing::warn!(?state, "cannot make a telemetry request in this state");
                None
            }
        }
    }

    fn create_push_request(&mut self, now_unix_nanos: u64) -> Option<TelemetryRequest> {
        let Some(subscription) = self.subscription.clone() else {
            tracing::warn!(state = ?self.state, "telemetry push without a subscription");
            self.set_state(TelemetryState::SubscriptionNeeded);
            return None;
        };
        let terminating = self.state == TelemetryState::TerminatingPushNeeded;
        let next = if terminating {
            TelemetryState::TerminatingPushInProgress
        } else {
            TelemetryState::PushInProgress
        };
        if !self.set_state(next) {
            return None;
        }
        let payload = self
            .collector
            .collect(
                &subscription.selector,
                subscription.delta_temporality,
                now_unix_nanos,
            )
            .encode();
        let (compression_type, metrics) = self.compress(&subscription, payload);
        Some(TelemetryRequest::Push(PushTelemetryRequest {
            client_instance_id: subscription.client_instance_id,
            subscription_id: subscription.id,
            terminating,
            compression_type: i8::try_from(compression_type.as_attribute_bits()).unwrap_or(0),
            metrics,
            ..Default::default()
        }))
    }

    /// Compress the payload with the broker's first accepted codec that has
    /// not failed. A codec that fails is not used again, and the payload goes
    /// uncompressed (`preferredCompressionType` and `compress`).
    fn compress(
        &mut self,
        subscription: &Subscription,
        payload: Vec<u8>,
    ) -> (CompressionType, Bytes) {
        let preferred = subscription
            .accepted_compression_types
            .iter()
            .copied()
            .find(|codec| !self.unsupported_compression_types.contains(codec))
            .unwrap_or(CompressionType::None);
        match krabka_compression::compress(preferred, &payload) {
            Ok(compressed) => (preferred, compressed),
            Err(error) => {
                tracing::debug!(%error, codec = preferred.name(), "telemetry compression failed, sending uncompressed");
                self.unsupported_compression_types.push(preferred);
                (CompressionType::None, Bytes::from(payload))
            }
        }
    }

    /// Handle a `GetTelemetrySubscriptions` response. `jitter_unit` in
    /// `[0, 1)` spreads the first push of the subscription over 50% to 150%
    /// of its push interval.
    pub(crate) fn handle_subscriptions_response(
        &mut self,
        response: &GetTelemetrySubscriptionsResponse,
        now: Instant,
        jitter_unit: f64,
    ) {
        let old_push_interval = self
            .subscription
            .as_ref()
            .map(|subscription| subscription.push_interval_ms);
        if let Some(interval) = error_interval(response.error_code, old_push_interval) {
            self.set_state(TelemetryState::SubscriptionNeeded);
            self.update_error_result(interval, now);
            return;
        }
        if self.state.is_terminating() {
            // The close began after the request went out.
            return;
        }
        if response.client_instance_id == Uuid::ZERO {
            // Kafka's `validateClientInstanceId` throws here, and no later
            // request follows. Stop telemetry.
            tracing::warn!(
                "telemetry subscription without a client instance id, disabling telemetry"
            );
            self.set_state(TelemetryState::SubscriptionNeeded);
            self.update_error_result(ErrorInterval::Disable, now);
            return;
        }
        let push_interval_ms = if response.push_interval_ms <= 0 {
            DEFAULT_PUSH_INTERVAL_MS
        } else {
            response.push_interval_ms
        };
        let subscription = Subscription {
            client_instance_id: response.client_instance_id,
            id: response.subscription_id,
            push_interval_ms,
            accepted_compression_types: response
                .accepted_compression_types
                .iter()
                .filter_map(|&id| {
                    u8::try_from(id)
                        .ok()
                        .filter(|id| *id <= CompressionType::Zstd.as_attribute_bits())
                        .and_then(CompressionType::from_attribute_bits)
                })
                .collect(),
            delta_temporality: response.delta_temporality,
            selector: MetricSelector::from_requested(&response.requested_metrics),
        };
        if self
            .subscription
            .as_ref()
            .is_some_and(|old| old.delta_temporality != subscription.delta_temporality)
        {
            self.collector.reset();
        }
        // A subscription with no metrics waits one push interval and asks
        // again.
        let next = if subscription.selector == MetricSelector::NoMetrics {
            TelemetryState::SubscriptionNeeded
        } else {
            TelemetryState::PushNeeded
        };
        if !self.set_state(next) {
            return;
        }
        // `updateSubscriptionResult`.
        self.interval = if self.state == TelemetryState::PushNeeded {
            let factor = (INITIAL_PUSH_JITTER_UPPER - INITIAL_PUSH_JITTER_LOWER)
                .mul_add(jitter_unit, INITIAL_PUSH_JITTER_LOWER);
            Duration::from_secs_f64((factor * f64::from(push_interval_ms)).round() / 1000.0)
        } else {
            millis(push_interval_ms)
        };
        self.last_request = Some(now);
        self.subscription = Some(subscription);
    }

    /// Handle a `PushTelemetry` response.
    pub(crate) fn handle_push_response(&mut self, response: &PushTelemetryResponse, now: Instant) {
        if self.state.is_terminating() {
            return;
        }
        let push_interval_ms = self
            .subscription
            .as_ref()
            .map(|subscription| subscription.push_interval_ms);
        if let Some(interval) = error_interval(response.error_code, push_interval_ms) {
            self.set_state(TelemetryState::SubscriptionNeeded);
            self.update_error_result(interval, now);
            return;
        }
        self.last_request = Some(now);
        self.interval = millis(push_interval_ms.unwrap_or(DEFAULT_PUSH_INTERVAL_MS));
        self.set_state(TelemetryState::PushNeeded);
    }

    /// Handle a request that got no response (`handleFailedRequest`).
    pub(crate) fn handle_failed_request(&mut self, failure: Failure, now: Instant) {
        if self.state.is_terminating() {
            return;
        }
        if !matches!(
            self.state,
            TelemetryState::SubscriptionInProgress | TelemetryState::PushInProgress
        ) {
            tracing::warn!(state = ?self.state, "telemetry request failed in an unexpected state, disabling telemetry");
            self.update_error_result(ErrorInterval::Disable, now);
            return;
        }
        // The broker may not support telemetry. Wait before the next try; a
        // later request may go to another broker.
        let interval = match failure {
            Failure::Retriable => ErrorInterval::Retry(DEFAULT_PUSH_INTERVAL),
            Failure::Fatal => ErrorInterval::Disable,
        };
        self.update_error_result(interval, now);
        self.set_state(TelemetryState::SubscriptionNeeded);
    }

    /// Begin the close: the next request is the terminating push, if the
    /// client has a subscription (`initiateClose`). Returns whether a
    /// terminating push is due.
    pub(crate) fn initiate_close(&mut self) -> bool {
        if self.last_request.is_none() {
            tracing::debug!("telemetry subscription not loaded, no terminating push");
            return false;
        }
        if self.state == TelemetryState::SubscriptionNeeded {
            tracing::debug!("telemetry subscription needed, no terminating push");
            return false;
        }
        !self.state.is_terminating() && self.set_state(TelemetryState::TerminatingPushNeeded)
    }

    /// End telemetry (`close`).
    pub(crate) fn close(&mut self) {
        if self.state != TelemetryState::Terminated {
            self.set_state(TelemetryState::Terminated);
        }
    }
}

#[cfg(test)]
mod tests;
