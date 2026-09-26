//! Client metrics push (KIP-714) of the admin client: Kafka's
//! `enable.metrics.push` on `KafkaAdminClient`.
//!
//! The reporter of `krabka-client-core` sends on the admin client's current
//! bootstrap or controller connection, and every connection of the client
//! counts in the `admin-client-metrics` network metrics.

use std::sync::Arc;

use krabka_client_core::{Connection, telemetry::TelemetryConnections};
use krabka_units::Time;

use crate::{AdminClient, AdminError, RecoveringConnection};

/// The connection that the admin client's reporter sends on. It has no
/// broker id in the metadata, so the reporter sees it as `-1`, as a
/// bootstrap connection.
pub(crate) struct AdminTelemetryConnection(pub(crate) Arc<RecoveringConnection>);

impl TelemetryConnections for AdminTelemetryConnection {
    fn open_connection(&self, _preferred: Option<i32>) -> Option<(i32, Arc<Connection>)> {
        // A connection that is being replaced is skipped; the reporter tries
        // again after the reconnect backoff.
        let connection = self.0.inner.try_read().ok()?;
        (!connection.is_closed()).then(|| (-1, Arc::clone(&connection)))
    }
}

impl AdminClient {
    /// The client instance id that the broker assigned for metrics push
    /// (KIP-714), as Kafka's `KafkaAdminClient.clientInstanceId` returns it.
    ///
    /// The call waits up to `timeout` for the first
    /// `GetTelemetrySubscriptions` response. It returns `Ok(None)` when none
    /// has come by then, as Kafka returns `null`. A zero `timeout` does not
    /// wait.
    ///
    /// # Errors
    /// Returns [`AdminError::Transport`] with
    /// [`ClientError::InvalidArgument`] for a negative `timeout`, and with
    /// [`ClientError::TelemetryDisabled`] when
    /// [`AdminClientConfig::enable_metrics_push`] is `false`, as Kafka throws
    /// an `IllegalArgumentException` and an `IllegalStateException`.
    ///
    /// [`ClientError::InvalidArgument`]: krabka_client_core::ClientError::InvalidArgument
    /// [`ClientError::TelemetryDisabled`]: krabka_client_core::ClientError::TelemetryDisabled
    /// [`AdminClientConfig::enable_metrics_push`]: crate::AdminClientConfig::enable_metrics_push
    pub async fn client_instance_id(
        &self,
        timeout: Time,
    ) -> Result<Option<uuid::Uuid>, AdminError> {
        Ok(
            krabka_client_core::telemetry::client_instance_id(self.telemetry.as_ref(), timeout)
                .await?,
        )
    }

    /// Close the client: send the terminating telemetry push (KIP-714), if
    /// the client pushes metrics and has a subscription, and wait for it, as
    /// Kafka's `KafkaAdminClient.close` does. The wait is at most about two
    /// request timeouts. Dropping the client sends the push in the
    /// background instead.
    pub async fn close(self) {
        if let Some(telemetry) = &self.telemetry {
            telemetry.close().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;
    use bytes::BytesMut;
    use krabka_client_core::{
        MockBroker,
        telemetry::otlp::{MetricData, MetricsData, NumberValue},
    };
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            get_telemetry_subscriptions_request::{self, GetTelemetrySubscriptionsRequest},
            get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse,
            push_telemetry_request::{self, PushTelemetryRequest},
            push_telemetry_response::PushTelemetryResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    };
    use tokio::sync::mpsc;

    use crate::AdminClientConfig;

    const INSTANCE: WireUuid = WireUuid([7; 16]);
    const CONNECTION_COUNT: &str = "org.apache.kafka.admin.client.connection.count";

    /// A telemetry request that the broker received. A push holds each
    /// metric's name and value.
    #[derive(Clone, Debug, PartialEq)]
    enum Seen {
        Subscriptions(WireUuid),
        Push {
            client_instance_id: WireUuid,
            terminating: bool,
            metrics: Vec<(String, Option<NumberValue>)>,
        },
    }

    /// The body of a flexible request after the request header v2.
    fn request_body(frame: &[u8]) -> &[u8] {
        let client_id_len = usize::try_from(i16::from_be_bytes([frame[0], frame[1]])).unwrap_or(0);
        &frame[2 + client_id_len + 1..]
    }

    fn flexible_response(response: &impl Encode) -> Vec<u8> {
        let mut body = BytesMut::from(&[0u8][..]);
        response.encode(&mut body, 0).unwrap();
        body.to_vec()
    }

    fn seen_push(request: &PushTelemetryRequest) -> Seen {
        let data = MetricsData::decode(&request.metrics).unwrap();
        let metrics = data
            .resource_metrics
            .into_iter()
            .flat_map(|resource| resource.scope_metrics)
            .flat_map(|scope| scope.metrics)
            .map(|metric| {
                let value = match metric.data {
                    Some(
                        MetricData::Gauge { data_points } | MetricData::Sum { data_points, .. },
                    ) => data_points.first().and_then(|point| point.value),
                    None => None,
                };
                (metric.name, value)
            })
            .collect();
        Seen::Push {
            client_instance_id: request.client_instance_id,
            terminating: request.terminating,
            metrics,
        }
    }

    /// A broker that assigns [`INSTANCE`] and subscribes the admin client's
    /// connection count, pushed every 200 ms. With `advertise_telemetry`
    /// false it does not list the KIP-714 APIs.
    async fn start_broker(
        advertise_telemetry: bool,
    ) -> (MockBroker, mpsc::UnboundedReceiver<Seen>) {
        let (seen_tx, seen_rx) = mpsc::unbounded_channel();
        let broker = MockBroker::start(move |api_key, version, _corr, frame| match api_key {
            api_versions_request::API_KEY => {
                let mut keys = vec![(api_versions_request::API_KEY, 3)];
                if advertise_telemetry {
                    keys.push((get_telemetry_subscriptions_request::API_KEY, 0));
                    keys.push((push_telemetry_request::API_KEY, 0));
                }
                let response = ApiVersionsResponse {
                    api_keys: keys
                        .into_iter()
                        .map(|(api_key, max_version)| ApiVersion {
                            api_key,
                            min_version: 0,
                            max_version,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                };
                let mut body = BytesMut::new();
                response.encode(&mut body, version).unwrap();
                Some(body.to_vec())
            }
            get_telemetry_subscriptions_request::API_KEY => {
                let mut body = request_body(frame);
                let request = GetTelemetrySubscriptionsRequest::decode(&mut body, version).unwrap();
                seen_tx
                    .send(Seen::Subscriptions(request.client_instance_id))
                    .ok();
                Some(flexible_response(&GetTelemetrySubscriptionsResponse {
                    client_instance_id: INSTANCE,
                    subscription_id: 5,
                    push_interval_ms: 200,
                    telemetry_max_bytes: 1 << 20,
                    requested_metrics: vec![CONNECTION_COUNT.to_owned()],
                    ..Default::default()
                }))
            }
            push_telemetry_request::API_KEY => {
                let mut body = request_body(frame);
                let request = PushTelemetryRequest::decode(&mut body, version).unwrap();
                seen_tx.send(seen_push(&request)).ok();
                Some(flexible_response(&PushTelemetryResponse::default()))
            }
            _ => None,
        })
        .await;
        (broker, seen_rx)
    }

    async fn next(seen: &mut mpsc::UnboundedReceiver<Seen>) -> Seen {
        tokio::time::timeout(Duration::from_secs(10), seen.recv())
            .await
            .expect("a telemetry request within 10 s")
            .expect("the broker is running")
    }

    /// Kafka's admin client with `enable.metrics.push`: it asks for the
    /// subscription, pushes the subscribed metrics (its one open
    /// connection), returns the assigned id from `clientInstanceId`, and
    /// sends a terminating push on close. With the setting off, or with a
    /// broker without KIP-714, it sends no telemetry request.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admin_client_pushes_metrics_and_reports_its_instance_id() {
        let disabled =
            "client-core: Telemetry is not enabled. Set config `enable.metrics.push` to \
                        `true`."
                .to_owned();
        let push = |terminating| Seen::Push {
            client_instance_id: INSTANCE,
            terminating,
            metrics: vec![(CONNECTION_COUNT.to_owned(), Some(NumberValue::Double(1.0)))],
        };
        let cases = [
            (
                "enable.metrics.push=true",
                true,
                true,
                2,
                Ok(Some(uuid::Uuid::from_bytes(INSTANCE.0))),
                vec![Seen::Subscriptions(WireUuid::ZERO), push(false), push(true)],
            ),
            (
                "enable.metrics.push=false",
                false,
                true,
                0,
                Err(disabled),
                vec![],
            ),
            ("broker without KIP-714", true, false, 0, Ok(None), vec![]),
        ];
        for (name, enable_metrics_push, advertise_telemetry, count, expected_id, expected) in cases
        {
            let (broker, mut seen) = start_broker(advertise_telemetry).await;
            let admin = crate::AdminClient::connect_with_config(
                &[broker.addr.to_string()],
                AdminClientConfig {
                    enable_metrics_push,
                    ..AdminClientConfig::default()
                },
            )
            .await
            .expect("admin connects");
            let timeout = if count == 0 {
                krabka_units::millis(300)
            } else {
                krabka_units::secs(10)
            };
            let id = admin
                .client_instance_id(timeout)
                .await
                .map_err(|error| error.to_string());
            let mut requests = Vec::new();
            for _ in 0..count {
                requests.push(next(&mut seen).await);
            }
            tokio::time::timeout(Duration::from_secs(20), admin.close())
                .await
                .expect("the close ends");
            broker.stop();
            while let Ok(request) = seen.try_recv() {
                requests.push(request);
            }
            assert!((id, requests) == (expected_id, expected), "{name}");
        }
    }
}
