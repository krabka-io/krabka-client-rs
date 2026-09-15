//! A rejected authentication fails the producer call, and a disconnect during
//! the SASL exchange does not.
//!
//! Kafka's `KafkaProducer.waitOnMetadata` raises the `AuthenticationException`
//! that `NetworkClient` stored in `Metadata.fatalError`, and `send` completes
//! the record with it. An idempotent producer fails `InitProducerId` with it
//! (`Sender.runOnce`, `TransactionManager.authenticationFailed`).

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use assert2::check;
use bytes::{Bytes, BytesMut};
use krabka_client_core::{
    AuthenticationError, ClientError, MockBroker, MockReply, MockSaslAnswer,
    SaslAuthenticationError,
    security::{ClientSecurity, SaslCredentials},
};
use krabka_client_producer::{Acks, Producer, ProducerError, ProducerRecord};
use krabka_protocol::{
    Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        metadata_request,
        metadata_response::{MetadataResponse, MetadataResponsePartition, MetadataResponseTopic},
        sasl_handshake_request,
    },
};
use krabka_security::ListenerProtocol;

const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
const SASL_AUTHENTICATION_FAILED: i16 = 58;

/// How a producer call ended, as an application classifies it.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// The record waits in the accumulator.
    Pending,
    Rejected(SaslAuthenticationError),
    Other(String),
}

#[derive(Debug, PartialEq, Eq)]
struct Observed {
    outcome: Outcome,
    handshakes: usize,
}

fn classify(error: &ProducerError) -> Outcome {
    match error {
        ProducerError::Client(ClientError::Authentication {
            source: AuthenticationError::Sasl(error),
            ..
        }) => Outcome::Rejected(error.clone()),
        error => Outcome::Other(error.to_string()),
    }
}

fn encode(response: &impl Encode, version: i16) -> Vec<u8> {
    let mut body = BytesMut::new();
    response.encode(&mut body, version).unwrap();
    body.to_vec()
}

/// A SASL listener that answers the exchange of connection `n` with
/// `script[n]`, or with the last entry once the script runs out. After a
/// successful exchange it answers `ApiVersions` and a `Metadata` with the
/// topic `orders`.
async fn sasl_broker(script: Vec<MockSaslAnswer>, handshakes: Arc<AtomicUsize>) -> MockBroker {
    let mut answer = *script.last().unwrap();
    MockBroker::start_with_replies(move |api_key, version, _corr, _body| {
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
            metadata_request::API_KEY => MockReply::Respond(encode(&orders_metadata(), version)),
            _ => MockReply::Silent,
        }
    })
    .await
}

/// Metadata with the topic `orders` and one partition, so `send` finds its
/// topic and does not wait for `max_block`.
fn orders_metadata() -> MetadataResponse {
    MetadataResponse {
        topics: vec![MetadataResponseTopic {
            name: Some("orders".into()),
            partitions: vec![MetadataResponsePartition::default()],
            ..Default::default()
        }],
        ..Default::default()
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_fails_the_record_with_a_sasl_rejection() {
    use MockSaslAnswer::{Accept, AuthenticateError, CloseAfterHandshake, HandshakeError};

    for (name, script, expected) in [
        (
            "SaslAuthenticate 58",
            vec![AuthenticateError(SASL_AUTHENTICATION_FAILED)],
            Observed {
                outcome: Outcome::Rejected(authenticate_failed()),
                handshakes: 1,
            },
        ),
        (
            "SaslHandshake 33",
            vec![HandshakeError(UNSUPPORTED_SASL_MECHANISM)],
            Observed {
                outcome: Outcome::Rejected(SaslAuthenticationError::UnsupportedMechanism(
                    "client SASL mechanism 'PLAIN' not enabled in the server, enabled mechanisms \
                     are [\"PLAIN\"]"
                        .into(),
                )),
                handshakes: 1,
            },
        ),
        (
            "close after SaslHandshake, then accept",
            vec![CloseAfterHandshake, Accept],
            Observed {
                outcome: Outcome::Pending,
                handshakes: 2,
            },
        ),
    ] {
        let handshakes = Arc::new(AtomicUsize::new(0));
        let broker = sasl_broker(script, Arc::clone(&handshakes)).await;
        // A long linger keeps the sender idle, so only `send` dials.
        let producer = Producer::builder()
            .bootstrap(broker.addr.to_string())
            .enable_idempotence(false)
            .acks(Acks::One)
            .linger(Duration::from_hours(1))
            .security(plain_security())
            .build()
            .await
            .unwrap();

        let mut receiver = producer
            .send(ProducerRecord {
                topic: "orders".into(),
                value: Some(Bytes::from_static(b"v")),
                ..Default::default()
            })
            .await;
        let outcome = match receiver.try_recv() {
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => Outcome::Pending,
            Ok(Err(error)) => classify(&error),
            other => Outcome::Other(format!("{other:?}")),
        };
        let observed = Observed {
            outcome,
            handshakes: handshakes.load(Ordering::SeqCst),
        };

        broker.stop();
        check!(observed == expected, "case {name}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_build_fails_with_a_sasl_rejection() {
    let handshakes = Arc::new(AtomicUsize::new(0));
    let broker = sasl_broker(
        vec![MockSaslAnswer::AuthenticateError(
            SASL_AUTHENTICATION_FAILED,
        )],
        Arc::clone(&handshakes),
    )
    .await;

    let result = Producer::builder()
        .bootstrap(broker.addr.to_string())
        .security(plain_security())
        .build()
        .await;
    let observed = Observed {
        outcome: result
            .err()
            .map_or_else(|| Outcome::Other("built".into()), |error| classify(&error)),
        handshakes: handshakes.load(Ordering::SeqCst),
    };

    broker.stop();
    check!(
        observed
            == Observed {
                outcome: Outcome::Rejected(authenticate_failed()),
                handshakes: 1,
            }
    );
}
