//! Broker registration administration.

use krabka_protocol::owned::unregister_broker_request::UnregisterBrokerRequest;

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
                return unregister_error(response.error_code, response.error_message);
            }
            retry.after_retriable("REQUEST_TIMED_OUT").await?;
        }
    }
}

fn unregister_error(code: i16, message: Option<String>) -> Result<(), AdminError> {
    if code == 0 {
        Ok(())
    } else {
        Err(AdminError::Broker {
            api: "UnregisterBroker",
            code,
            name: kafka_error_name(code),
            message,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn unregister_success_and_error_are_distinct() {
        assert!(unregister_error(0, None).is_ok());
        assert!(matches!(
            unregister_error(42, Some("stale".into())),
            Err(AdminError::Broker {
                api: "UnregisterBroker",
                code: 42,
                ..
            })
        ));
    }

    /// Kafka's `unregisterBroker` retries `REQUEST_TIMED_OUT` (7) until the
    /// deadline and fails at once on `NOT_CONTROLLER` (41) and every other
    /// code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unregister_broker_retries_request_timed_out_as_kafka_does() {
        use std::{
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
            time::Duration,
        };

        use bytes::BytesMut;
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

        const LONG: Duration = Duration::from_secs(5);
        const SHORT: Duration = Duration::from_millis(300);
        for (name, codes, timeout, expected) in [
            ("success", vec![0], LONG, (Ok(()), 1)),
            (
                "request timed out, then success",
                vec![7, 7, 0],
                LONG,
                (Ok(()), 3),
            ),
            ("not controller is final", vec![41], LONG, (Err(41), 1)),
            (
                "request timed out past the deadline",
                vec![7],
                SHORT,
                (Err(7), 1),
            ),
        ] {
            let requests = Arc::new(AtomicUsize::new(0));
            let handler_requests = Arc::clone(&requests);
            let broker = MockBroker::start(move |api_key, version, _, _| {
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
                        let request = handler_requests.fetch_add(1, Ordering::SeqCst);
                        body.extend_from_slice(&[0]);
                        UnregisterBrokerResponse {
                            error_code: codes[request.min(codes.len() - 1)],
                            ..Default::default()
                        }
                        .encode(&mut body, version)
                        .unwrap();
                    }
                    _ => return None,
                }
                Some(body.to_vec())
            })
            .await;
            let mut admin = AdminClient::connect(&[broker.addr.to_string()])
                .await
                .expect("admin connects");
            let backoff = if timeout == LONG {
                Duration::from_millis(1)
            } else {
                timeout
            };

            let result = admin
                .unregister_broker_with_retry(
                    1,
                    RetryPolicy {
                        timeout,
                        initial_backoff: backoff,
                        max_backoff: backoff,
                        jitter: 0.0,
                        max_retries: u32::MAX,
                    },
                )
                .await
                .map_err(|error| match error {
                    AdminError::Broker { code, .. } => code,
                    other => panic!("case {name}: unexpected error {other:?}"),
                });

            broker.stop();
            assert!(
                (result, requests.load(Ordering::SeqCst)) == expected,
                "case {name}"
            );
        }
    }
}
