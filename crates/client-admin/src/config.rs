//! Admin client settings, as Kafka's `AdminClientConfig` defines them.

use std::sync::atomic::{AtomicU32, Ordering};

use krabka_client_core::{
    ClientDnsTimeout, ConnectionOptions, DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER,
    MetadataRecoveryStrategy, security::ClientSecurity,
};
use krabka_units::{Time, convert::TimeExt as _, millis, secs};

use crate::{AdminError, retry::RetryPolicy};

/// Kafka's `request.timeout.ms` default for the admin client.
pub const DEFAULT_ADMIN_REQUEST_TIMEOUT: Time = secs(30);
/// Kafka's `default.api.timeout.ms` default.
pub const DEFAULT_API_TIMEOUT: Time = secs(60);
/// Kafka's `retry.backoff.ms` default.
pub const DEFAULT_RETRY_BACKOFF: Time = millis(100);
/// Kafka's `retry.backoff.max.ms` default.
pub const DEFAULT_RETRY_BACKOFF_MAX: Time = secs(1);
/// Kafka's `socket.connection.setup.timeout.ms` default.
pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT: Time = secs(10);
/// Kafka's `socket.connection.setup.timeout.max.ms` default.
pub const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX: Time = secs(30);
/// Kafka's admin client `connections.max.idle.ms` default (5 minutes).
pub const DEFAULT_ADMIN_CONNECTIONS_MAX_IDLE: Time = krabka_units::minutes(5);

/// `CommonClientConfigs.RETRY_BACKOFF_JITTER`.
const RETRY_BACKOFF_JITTER: f64 = 0.2;

/// The sequence of generated client ids, as Kafka's
/// `KafkaAdminClient.ADMIN_CLIENT_ID_SEQUENCE`. The first id is
/// `adminclient-1`.
static ADMIN_CLIENT_ID_SEQUENCE: AtomicU32 = AtomicU32::new(1);

/// The settings of an [`AdminClient`](crate::AdminClient).
///
/// `Default` gives Kafka's `AdminClientConfig` defaults. A setting that is
/// `None` is not set, which matters where Kafka checks whether the user set
/// a value (`client.id`, `default.api.timeout.ms`).
#[derive(Clone, Debug)]
pub struct AdminClientConfig {
    /// `client.id`. `None` or an empty id gives a new `adminclient-<n>` for
    /// each client, as `KafkaAdminClient.generateClientId` does.
    pub client_id: Option<String>,
    /// `request.timeout.ms`: the deadline of one request.
    pub request_timeout: Time,
    /// `default.api.timeout.ms`: the deadline of a call and its retries when
    /// the call sets no timeout. `None` uses 60 s, raised to
    /// [`Self::request_timeout`] when that is larger.
    pub default_api_timeout: Option<Time>,
    /// `retries`: the number of retries of a call before its deadline.
    /// Kafka's default is `Integer.MAX_VALUE`.
    pub retries: u32,
    /// `retry.backoff.ms`.
    pub retry_backoff: Time,
    /// `retry.backoff.max.ms`.
    pub retry_backoff_max: Time,
    /// `socket.connection.setup.timeout.ms`: the connection setup deadline
    /// of the first attempt.
    pub socket_connection_setup_timeout: Time,
    /// `socket.connection.setup.timeout.max.ms`: the setup deadline grows
    /// per failed attempt up to this value.
    pub socket_connection_setup_timeout_max: Time,
    /// `connections.max.idle.ms`: a connection with no traffic closes after
    /// this time.
    pub connections_max_idle: Time,
    /// The DNS lookup deadline of one broker address.
    pub dns_timeout: ClientDnsTimeout,
    /// `security.protocol` and its TLS and SASL settings. `None` is
    /// plaintext.
    pub security: Option<ClientSecurity>,
    /// `metadata.recovery.strategy` (KIP-1102).
    pub metadata_recovery_strategy: MetadataRecoveryStrategy,
    /// `metadata.recovery.rebootstrap.trigger.ms` (KIP-1102).
    pub metadata_recovery_rebootstrap_trigger: Time,
}

impl Default for AdminClientConfig {
    fn default() -> Self {
        Self {
            client_id: None,
            request_timeout: DEFAULT_ADMIN_REQUEST_TIMEOUT,
            default_api_timeout: None,
            retries: u32::MAX,
            retry_backoff: DEFAULT_RETRY_BACKOFF,
            retry_backoff_max: DEFAULT_RETRY_BACKOFF_MAX,
            socket_connection_setup_timeout: DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT,
            socket_connection_setup_timeout_max: DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX,
            connections_max_idle: DEFAULT_ADMIN_CONNECTIONS_MAX_IDLE,
            dns_timeout: ClientDnsTimeout::default(),
            security: None,
            metadata_recovery_strategy: MetadataRecoveryStrategy::default(),
            metadata_recovery_rebootstrap_trigger: DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER,
        }
    }
}

/// The connection options and retry policy of one admin client.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedAdminConfig {
    pub(crate) connection: ConnectionOptions,
    pub(crate) retry: RetryPolicy,
    pub(crate) metadata_recovery_strategy: MetadataRecoveryStrategy,
    pub(crate) metadata_recovery_rebootstrap_trigger: Time,
}

impl AdminClientConfig {
    /// Check the settings and derive the connection options and the call
    /// retry policy.
    ///
    /// # Errors
    /// Returns [`AdminError::InvalidConfig`] when a timeout or backoff is not
    /// positive, or when `default_api_timeout` is set below
    /// `request_timeout`, as Kafka's `configureDefaultApiTimeoutMs` throws a
    /// `ConfigException`.
    pub(crate) fn resolve(self) -> Result<ResolvedAdminConfig, AdminError> {
        for (name, value) in [
            ("request_timeout", self.request_timeout),
            (
                "socket_connection_setup_timeout",
                self.socket_connection_setup_timeout,
            ),
            (
                "socket_connection_setup_timeout_max",
                self.socket_connection_setup_timeout_max,
            ),
            ("connections_max_idle", self.connections_max_idle),
        ] {
            if !value.secs_f64().is_finite() || value <= Time::ZERO {
                return Err(AdminError::InvalidConfig(format!(
                    "{name} must be positive and finite"
                )));
            }
        }
        if let Some(value) = self.default_api_timeout
            && (!value.secs_f64().is_finite() || value <= Time::ZERO)
        {
            return Err(AdminError::InvalidConfig(
                "default_api_timeout must be positive and finite".to_owned(),
            ));
        }
        for (name, value) in [
            ("retry_backoff", self.retry_backoff),
            ("retry_backoff_max", self.retry_backoff_max),
        ] {
            if !value.secs_f64().is_finite() || value < Time::ZERO {
                return Err(AdminError::InvalidConfig(format!(
                    "{name} must be finite and not negative"
                )));
            }
        }
        let api_timeout = default_api_timeout(self.request_timeout, self.default_api_timeout)?;
        let client_id = match self.client_id {
            Some(id) if !id.is_empty() => id,
            _ => generate_client_id(),
        };
        Ok(ResolvedAdminConfig {
            connection: ConnectionOptions {
                client_id,
                dns_timeout: self.dns_timeout,
                socket_connection_setup_timeout: self.socket_connection_setup_timeout,
                socket_connection_setup_timeout_max: self.socket_connection_setup_timeout_max,
                connections_max_idle: self.connections_max_idle,
                request_timeout: self.request_timeout,
                security: self.security.map(Box::new),
                ..ConnectionOptions::default()
            },
            retry: RetryPolicy {
                timeout: api_timeout.to_std(),
                initial_backoff: self.retry_backoff.to_std(),
                max_backoff: self.retry_backoff_max.to_std(),
                jitter: RETRY_BACKOFF_JITTER,
                max_retries: self.retries,
            },
            metadata_recovery_strategy: self.metadata_recovery_strategy,
            metadata_recovery_rebootstrap_trigger: self.metadata_recovery_rebootstrap_trigger,
        })
    }
}

/// A new `adminclient-<n>` id.
fn generate_client_id() -> String {
    format!(
        "adminclient-{}",
        ADMIN_CLIENT_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

/// The call deadline, as Kafka's `configureDefaultApiTimeoutMs` gives it: an
/// explicit value below the request timeout is an error, and the default
/// rises to the request timeout.
fn default_api_timeout(request_timeout: Time, explicit: Option<Time>) -> Result<Time, AdminError> {
    match explicit {
        Some(api_timeout) if api_timeout < request_timeout => Err(AdminError::InvalidConfig(
            "default_api_timeout must be no smaller than request_timeout".to_owned(),
        )),
        Some(api_timeout) => Ok(api_timeout),
        None if DEFAULT_API_TIMEOUT < request_timeout => {
            tracing::warn!(
                request_timeout_ms = request_timeout.millis_i64(),
                "overriding the default of default.api.timeout.ms (60000) with the configured \
                 request timeout"
            );
            Ok(request_timeout)
        }
        None => Ok(DEFAULT_API_TIMEOUT),
    }
}

/// The retry policy of a client built from bare [`ConnectionOptions`]:
/// Kafka's defaults, with the call deadline raised to the request timeout.
pub(crate) fn retry_for_connection_options(options: &ConnectionOptions) -> RetryPolicy {
    let timeout = if options.request_timeout > DEFAULT_API_TIMEOUT {
        options.request_timeout
    } else {
        DEFAULT_API_TIMEOUT
    };
    RetryPolicy {
        timeout: timeout.to_std(),
        initial_backoff: DEFAULT_RETRY_BACKOFF.to_std(),
        max_backoff: DEFAULT_RETRY_BACKOFF_MAX.to_std(),
        jitter: RETRY_BACKOFF_JITTER,
        max_retries: u32::MAX,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use assert2::{assert, check};
    use bytes::{Buf, BytesMut};
    use krabka_client_core::MockBroker;
    use krabka_protocol::{
        Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            unregister_broker_request,
            unregister_broker_response::UnregisterBrokerResponse,
        },
    };

    use super::*;
    use crate::AdminClient;

    /// The settings that a test compares: client id prefix or value, request
    /// timeout, connection setup timeouts, idle time, and the retry policy.
    #[derive(Debug, PartialEq)]
    struct Observed {
        client_id: String,
        request_timeout: Time,
        setup_timeouts: (Time, Time),
        connections_max_idle: Time,
        retry: RetryPolicy,
    }

    fn observe(config: AdminClientConfig) -> Result<Observed, String> {
        config
            .resolve()
            .map(|resolved| Observed {
                // A generated id ends with a process-wide sequence number.
                client_id: if resolved.connection.client_id.starts_with("adminclient-") {
                    "adminclient-<n>".to_owned()
                } else {
                    resolved.connection.client_id
                },
                request_timeout: resolved.connection.request_timeout,
                setup_timeouts: (
                    resolved.connection.socket_connection_setup_timeout,
                    resolved.connection.socket_connection_setup_timeout_max,
                ),
                connections_max_idle: resolved.connection.connections_max_idle,
                retry: resolved.retry,
            })
            .map_err(|error| error.to_string())
    }

    fn retry(timeout: Duration) -> RetryPolicy {
        RetryPolicy {
            timeout,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
            jitter: 0.2,
            max_retries: u32::MAX,
        }
    }

    /// Kafka's `AdminClientConfig` defaults, `generateClientId`, and
    /// `configureDefaultApiTimeoutMs`.
    #[test]
    fn settings_resolve_as_kafka_admin_client_config() {
        let defaults = || Observed {
            client_id: "adminclient-<n>".into(),
            request_timeout: secs(30),
            setup_timeouts: (secs(10), secs(30)),
            connections_max_idle: krabka_units::minutes(5),
            retry: retry(Duration::from_mins(1)),
        };
        for (name, config, expected) in [
            ("defaults", AdminClientConfig::default(), Ok(defaults())),
            (
                "request timeout 90 s raises the default api timeout",
                AdminClientConfig {
                    request_timeout: secs(90),
                    ..AdminClientConfig::default()
                },
                Ok(Observed {
                    request_timeout: secs(90),
                    retry: retry(Duration::from_secs(90)),
                    ..defaults()
                }),
            ),
            (
                "api timeout below the request timeout is an error",
                AdminClientConfig {
                    request_timeout: secs(90),
                    default_api_timeout: Some(secs(60)),
                    ..AdminClientConfig::default()
                },
                Err(
                    "invalid admin client configuration: default_api_timeout must be no smaller \
                     than request_timeout"
                        .to_owned(),
                ),
            ),
            (
                "api timeout equal to the request timeout",
                AdminClientConfig {
                    default_api_timeout: Some(secs(30)),
                    ..AdminClientConfig::default()
                },
                Ok(Observed {
                    retry: retry(Duration::from_secs(30)),
                    ..defaults()
                }),
            ),
            (
                "explicit client id",
                AdminClientConfig {
                    client_id: Some("tool-a".into()),
                    ..AdminClientConfig::default()
                },
                Ok(Observed {
                    client_id: "tool-a".into(),
                    ..defaults()
                }),
            ),
            (
                "empty client id is generated",
                AdminClientConfig {
                    client_id: Some(String::new()),
                    ..AdminClientConfig::default()
                },
                Ok(defaults()),
            ),
            (
                "retries, backoff and setup timeout",
                AdminClientConfig {
                    retries: 3,
                    retry_backoff: millis(50),
                    retry_backoff_max: secs(2),
                    socket_connection_setup_timeout: secs(5),
                    socket_connection_setup_timeout_max: secs(20),
                    connections_max_idle: secs(60),
                    ..AdminClientConfig::default()
                },
                Ok(Observed {
                    setup_timeouts: (secs(5), secs(20)),
                    connections_max_idle: secs(60),
                    retry: RetryPolicy {
                        initial_backoff: Duration::from_millis(50),
                        max_backoff: Duration::from_secs(2),
                        max_retries: 3,
                        ..retry(Duration::from_mins(1))
                    },
                    ..defaults()
                }),
            ),
            (
                "zero request timeout is an error",
                AdminClientConfig {
                    request_timeout: Time::ZERO,
                    ..AdminClientConfig::default()
                },
                Err(
                    "invalid admin client configuration: request_timeout must be positive and \
                     finite"
                        .to_owned(),
                ),
            ),
            (
                "infinite request timeout is an error",
                AdminClientConfig {
                    request_timeout: Time::from_secs_f64(f64::INFINITY),
                    ..AdminClientConfig::default()
                },
                Err(
                    "invalid admin client configuration: request_timeout must be positive and \
                     finite"
                        .to_owned(),
                ),
            ),
            (
                "NaN api timeout is an error",
                AdminClientConfig {
                    default_api_timeout: Some(Time::from_secs_f64(f64::NAN)),
                    ..AdminClientConfig::default()
                },
                Err(
                    "invalid admin client configuration: default_api_timeout must be positive \
                     and finite"
                        .to_owned(),
                ),
            ),
            (
                "NaN retry backoff is an error",
                AdminClientConfig {
                    retry_backoff: Time::from_secs_f64(f64::NAN),
                    ..AdminClientConfig::default()
                },
                Err(
                    "invalid admin client configuration: retry_backoff must be finite and not \
                     negative"
                        .to_owned(),
                ),
            ),
        ] {
            check!(observe(config) == expected, "{name}");
        }
    }

    #[test]
    fn generated_client_ids_follow_a_process_wide_sequence() {
        let id = || {
            AdminClientConfig::default()
                .resolve()
                .expect("defaults resolve")
                .connection
                .client_id
        };
        let number = |id: String| {
            id.strip_prefix("adminclient-")
                .and_then(|number| number.parse::<u32>().ok())
                .expect("adminclient-<n>")
        };
        let first = number(id());
        let second = number(id());
        assert!(second > first);
        assert!(first >= 1);
    }

    /// A broker that answers every `UnregisterBroker` with
    /// `REQUEST_TIMED_OUT`, and records the client id of each request.
    async fn timing_out_broker(
        requests: Arc<AtomicUsize>,
        client_ids: Arc<Mutex<Vec<String>>>,
    ) -> MockBroker {
        MockBroker::start(move |api_key, version, _, request| {
            let mut header = request;
            let client_id_len = usize::try_from(header.get_i16()).expect("client id length");
            client_ids
                .lock()
                .expect("client ids lock")
                .push(String::from_utf8_lossy(&header[..client_id_len]).into_owned());
            let mut body = BytesMut::new();
            match api_key {
                api_versions_request::API_KEY => ApiVersionsResponse {
                    api_keys: vec![
                        ApiVersion {
                            api_key: api_versions_request::API_KEY,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: unregister_broker_request::API_KEY,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }
                .encode(&mut body, 0)
                .unwrap(),
                unregister_broker_request::API_KEY => {
                    requests.fetch_add(1, Ordering::SeqCst);
                    body.extend_from_slice(&[0]);
                    UnregisterBrokerResponse {
                        error_code: 7,
                        ..Default::default()
                    }
                    .encode(&mut body, version)
                    .unwrap();
                }
                _ => return None,
            }
            Some(body.to_vec())
        })
        .await
    }

    /// A client built from `AdminClientConfig` sends its client id in each
    /// request header, stops a call at `default_api_timeout`, and stops after
    /// `retries` retries.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_client_uses_its_client_id_call_deadline_and_retries() {
        for (name, config, expected_requests) in [
            (
                "no retries",
                AdminClientConfig {
                    client_id: Some("tool-a".into()),
                    retries: 0,
                    ..AdminClientConfig::default()
                },
                1..=1,
            ),
            (
                "two retries",
                AdminClientConfig {
                    client_id: Some("tool-a".into()),
                    retries: 2,
                    retry_backoff: millis(1),
                    retry_backoff_max: millis(1),
                    ..AdminClientConfig::default()
                },
                3..=3,
            ),
            (
                "the call deadline stops the retries",
                AdminClientConfig {
                    client_id: Some("tool-a".into()),
                    request_timeout: millis(100),
                    default_api_timeout: Some(millis(400)),
                    retry_backoff: millis(100),
                    retry_backoff_max: millis(100),
                    ..AdminClientConfig::default()
                },
                2..=6,
            ),
        ] {
            let requests = Arc::new(AtomicUsize::new(0));
            let client_ids = Arc::new(Mutex::new(Vec::new()));
            let broker = timing_out_broker(Arc::clone(&requests), Arc::clone(&client_ids)).await;
            let mut admin = AdminClient::connect_with_config(&[broker.addr.to_string()], config)
                .await
                .expect("admin connects");

            let started = tokio::time::Instant::now();
            let result = admin.unregister_broker(1).await;
            let elapsed = started.elapsed();

            broker.stop();
            let code = match result {
                Err(crate::AdminError::Broker { code, .. }) => Some(code),
                _ => None,
            };
            let requests = requests.load(Ordering::SeqCst);
            let client_ids = client_ids.lock().expect("client ids lock").clone();
            check!(code == Some(7), "{name}");
            check!(
                expected_requests.contains(&requests),
                "{name}: {requests} requests"
            );
            check!(elapsed < Duration::from_secs(5), "{name}");
            check!(
                client_ids.iter().all(|id| id == "tool-a") && !client_ids.is_empty(),
                "{name}: {client_ids:?}"
            );
        }
    }
}
