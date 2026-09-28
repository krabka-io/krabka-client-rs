//! Broker and controller registration administration.

use krabka_protocol::owned::{
    unregister_broker_request::UnregisterBrokerRequest,
    unregister_controller_request::UnregisterControllerRequest,
};

use crate::{
    AdminClient, AdminError, kafka_error_name,
    retry::{ControllerRetry, REQUEST_TIMED_OUT, RetryPolicy},
};

impl AdminClient {
    /// Unregisters one broker from the active `KRaft` controller.
    ///
    /// Kafka's `KafkaAdminClient.unregisterBroker` retries
    /// `REQUEST_TIMED_OUT` (7) with the retry backoff until the call deadline
    /// (`default.api.timeout.ms`, 60 s), and fails at once on every other
    /// code, `NOT_CONTROLLER` (41) too. This call does the same.
    ///
    /// # Errors
    /// Returns a transport, protocol, or broker error. At the deadline it
    /// gives `REQUEST_TIMED_OUT` (7).
    pub async fn unregister_broker(&mut self, broker_id: i32) -> Result<(), AdminError> {
        self.unregister_broker_with_retry(broker_id, self.retry)
            .await
    }

    /// Unregisters one controller from the active `KRaft` controller, with
    /// an `UnregisterController` request (API key 94).
    ///
    /// This is Kafka's `Admin.unregisterController`. Like
    /// `KafkaAdminClient.unregisterBroker`, it goes to a broker of a broker
    /// connection or to the active controller of a controller connection. It
    /// retries `REQUEST_TIMED_OUT` (7) with the retry backoff until the call
    /// deadline, and fails at once on every other code: `NOT_CONTROLLER`
    /// (41), `INVALID_REQUEST` (42) for a controller in the voter set, and
    /// `CONTROLLER_ID_NOT_REGISTERED` (136).
    ///
    /// # Errors
    /// Returns a transport, protocol, or broker error. At the deadline it
    /// gives `REQUEST_TIMED_OUT` (7).
    pub async fn unregister_controller(&mut self, controller_id: i32) -> Result<(), AdminError> {
        self.unregister_controller_with_retry(controller_id, self.retry)
            .await
    }

    async fn unregister_broker_with_retry(
        &mut self,
        broker_id: i32,
        policy: RetryPolicy,
    ) -> Result<(), AdminError> {
        let request = UnregisterBrokerRequest {
            broker_id,
            ..Default::default()
        };
        let mut retry = ControllerRetry::new("UnregisterBroker", policy);
        loop {
            let response = retry.bounded(self.conn.send(request.clone())).await?;
            if response.error_code != REQUEST_TIMED_OUT {
                return unregister_error(
                    "UnregisterBroker",
                    response.error_code,
                    response.error_message,
                );
            }
            retry.after_retriable("REQUEST_TIMED_OUT").await?;
        }
    }

    async fn unregister_controller_with_retry(
        &mut self,
        controller_id: i32,
        policy: RetryPolicy,
    ) -> Result<(), AdminError> {
        let request = UnregisterControllerRequest {
            controller_id,
            ..Default::default()
        };
        let mut retry = ControllerRetry::new("UnregisterController", policy);
        loop {
            let response = retry.bounded(self.conn.send(request.clone())).await?;
            if response.error_code != REQUEST_TIMED_OUT {
                return unregister_error(
                    "UnregisterController",
                    response.error_code,
                    response.error_message,
                );
            }
            retry.after_retriable("REQUEST_TIMED_OUT").await?;
        }
    }
}

fn unregister_error(
    api: &'static str,
    code: i16,
    message: Option<String>,
) -> Result<(), AdminError> {
    if code == 0 {
        Ok(())
    } else {
        Err(AdminError::Broker {
            api,
            code,
            name: kafka_error_name(code),
            message,
        })
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

    use assert2::assert;
    use bytes::BytesMut;
    use krabka_client_core::MockBroker;
    use krabka_protocol::{
        Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            unregister_broker_request,
            unregister_broker_response::UnregisterBrokerResponse,
            unregister_controller_request,
            unregister_controller_response::UnregisterControllerResponse,
        },
    };

    use super::*;
    use crate::partition_leaders::test_support::decode_request;

    #[test]
    fn unregister_error_names_the_api_and_the_code() {
        for (api, code, message, expected) in [
            ("UnregisterBroker", 0, None, None),
            ("UnregisterController", 0, None, None),
            (
                "UnregisterBroker",
                42,
                Some("stale"),
                Some(("UnregisterBroker", 42, "INVALID_REQUEST", Some("stale"))),
            ),
            (
                "UnregisterController",
                136,
                None,
                Some((
                    "UnregisterController",
                    136,
                    "CONTROLLER_ID_NOT_REGISTERED",
                    None,
                )),
            ),
        ] {
            let actual = match unregister_error(api, code, message.map(str::to_owned)) {
                Ok(()) => None,
                Err(AdminError::Broker {
                    api,
                    code,
                    name,
                    message,
                }) => Some((api, code, name, message)),
                Err(other) => panic!("unexpected error {other:?}"),
            };
            let actual = actual
                .as_ref()
                .map(|(api, code, name, message)| (*api, *code, *name, message.as_deref()));
            assert!(actual == expected, "{api} {code}");
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Call {
        Broker,
        Controller,
    }

    /// One unregister request that the mock broker decoded.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Sent {
        Broker(UnregisterBrokerRequest),
        Controller(UnregisterControllerRequest),
    }

    /// A mock broker that advertises only `ApiVersions` and the API of
    /// `call`, answers each request of that API with the next code of
    /// `codes`, repeating the last, and records each request in `sent`.
    async fn unregister_mock(
        call: Call,
        codes: Vec<i16>,
        sent: Arc<Mutex<Vec<Sent>>>,
    ) -> MockBroker {
        let requests = AtomicUsize::new(0);
        let api_key = match call {
            Call::Broker => unregister_broker_request::API_KEY,
            Call::Controller => unregister_controller_request::API_KEY,
        };
        MockBroker::start(move |key, version, _, request_body| {
            let mut body = BytesMut::new();
            if key == api_versions_request::API_KEY {
                ApiVersionsResponse {
                    api_keys: vec![
                        ApiVersion {
                            api_key: api_versions_request::API_KEY,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }
                .encode(&mut body, 0)
                .unwrap();
                return Some(body.to_vec());
            }
            if key != api_key {
                return None;
            }
            let error_code = codes[requests.fetch_add(1, Ordering::SeqCst).min(codes.len() - 1)];
            // Both APIs are flexible from v0: a tagged-field byte follows the
            // correlation id of the response header.
            body.extend_from_slice(&[0]);
            match call {
                Call::Broker => {
                    sent.lock().unwrap().push(Sent::Broker(decode_request(
                        request_body,
                        version,
                        true,
                    )));
                    UnregisterBrokerResponse {
                        error_code,
                        ..Default::default()
                    }
                    .encode(&mut body, version)
                    .unwrap();
                }
                Call::Controller => {
                    sent.lock().unwrap().push(Sent::Controller(decode_request(
                        request_body,
                        version,
                        true,
                    )));
                    UnregisterControllerResponse {
                        error_code,
                        ..Default::default()
                    }
                    .encode(&mut body, version)
                    .unwrap();
                }
            }
            Some(body.to_vec())
        })
        .await
    }

    /// Kafka's `unregisterBroker` and `unregisterController` retry
    /// `REQUEST_TIMED_OUT` (7) until the deadline and fail at once on
    /// `NOT_CONTROLLER` (41) and every other code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unregister_retries_request_timed_out_as_kafka_does() {
        const LONG: Duration = Duration::from_secs(5);
        const SHORT: Duration = Duration::from_millis(300);
        for call in [Call::Broker, Call::Controller] {
            for (name, codes, timeout, expected_result, expected_requests) in [
                ("success", vec![0], LONG, Ok(()), 1),
                (
                    "request timed out, then success",
                    vec![7, 7, 0],
                    LONG,
                    Ok(()),
                    3,
                ),
                ("not controller is final", vec![41], LONG, Err(41), 1),
                ("invalid request is final", vec![42], LONG, Err(42), 1),
                (
                    "controller id not registered is final",
                    vec![136],
                    LONG,
                    Err(136),
                    1,
                ),
                (
                    "request timed out past the deadline",
                    vec![7],
                    SHORT,
                    Err(7),
                    1,
                ),
            ] {
                let sent = Arc::new(Mutex::new(Vec::new()));
                let broker = unregister_mock(call, codes, Arc::clone(&sent)).await;
                let mut admin = AdminClient::connect(&[broker.addr.to_string()])
                    .await
                    .expect("admin connects");
                let backoff = if timeout == LONG {
                    Duration::from_millis(1)
                } else {
                    timeout
                };
                let policy = RetryPolicy {
                    timeout,
                    initial_backoff: backoff,
                    max_backoff: backoff,
                    jitter: 0.0,
                    max_retries: u32::MAX,
                };

                let (result, expected_request) = match call {
                    Call::Broker => (
                        admin.unregister_broker_with_retry(9, policy).await,
                        Sent::Broker(UnregisterBrokerRequest {
                            broker_id: 9,
                            ..Default::default()
                        }),
                    ),
                    Call::Controller => (
                        admin.unregister_controller_with_retry(9, policy).await,
                        Sent::Controller(UnregisterControllerRequest {
                            controller_id: 9,
                            ..Default::default()
                        }),
                    ),
                };
                let result = result.map_err(|error| match error {
                    AdminError::Broker { code, .. } => code,
                    other => panic!("case {call:?} {name}: unexpected error {other:?}"),
                });

                broker.stop();
                let sent = sent.lock().unwrap().clone();
                assert!(
                    (result, sent) == (expected_result, vec![expected_request; expected_requests]),
                    "case {call:?} {name}"
                );
            }
        }
    }
}
