//! A rejected authentication fails the admin call, and a disconnect during the
//! SASL exchange is retried.
//!
//! Kafka's `KafkaAdminClient` fails a call with the `AuthenticationException`
//! that `NetworkClient` stored for the node, and `AdminMetadataManager` makes
//! it fatal. Kafka keeps a disconnect in the `AUTHENTICATE` state retriable.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use assert2::check;
use bytes::BytesMut;
use krabka_client_admin::{AdminClient, AdminError, CreateTopicSpec, TopicMutationOptions};
use krabka_client_core::{
    AuthenticationError, ClientError, MockBroker, MockReply, MockSaslAnswer,
    SaslAuthenticationError,
    security::{ClientSecurity, SaslCredentials},
};
use krabka_protocol::{
    Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        create_topics_request,
        create_topics_response::CreateTopicsResponse,
        sasl_handshake_request,
    },
};
use krabka_security::ListenerProtocol;
use krabka_units::secs;

const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
const SASL_AUTHENTICATION_FAILED: i16 = 58;

/// How an admin call ended, as an application classifies it.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Ok,
    Rejected(SaslAuthenticationError),
    Other(String),
}

#[derive(Debug, PartialEq, Eq)]
struct Observed {
    outcome: Outcome,
    handshakes: usize,
}

fn classify<T>(result: &Result<T, AdminError>) -> Outcome {
    match result {
        Ok(_) => Outcome::Ok,
        Err(AdminError::Transport(ClientError::Authentication {
            source: AuthenticationError::Sasl(error),
            ..
        })) => Outcome::Rejected(error.clone()),
        Err(error) => Outcome::Other(format!("{error:?}")),
    }
}

fn encode(response: &impl Encode, version: i16) -> Vec<u8> {
    let mut body = BytesMut::new();
    response.encode(&mut body, version).unwrap();
    body.to_vec()
}

/// A SASL listener that answers the exchange of connection `n` with
/// `script[n]`, or with the last entry once the script runs out. It closes the
/// first connection when that connection sends `CreateTopics`, as a broker
/// that restarts does, and it answers `CreateTopics` on a later connection.
async fn sasl_broker(script: Vec<MockSaslAnswer>, handshakes: Arc<AtomicUsize>) -> MockBroker {
    let mut answer = *script.last().unwrap();
    let mut connection = 0;
    MockBroker::start_with_replies(move |api_key, version, _corr, _body| {
        if api_key == sasl_handshake_request::API_KEY {
            connection = handshakes.fetch_add(1, Ordering::SeqCst);
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
                            api_key: create_topics_request::API_KEY,
                            min_version: 0,
                            max_version: 4,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                0,
            )),
            create_topics_request::API_KEY if connection == 0 => MockReply::Close,
            create_topics_request::API_KEY => {
                MockReply::Respond(encode(&CreateTopicsResponse::default(), version))
            }
            _ => MockReply::Silent,
        }
    })
    .await
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

fn topic() -> CreateTopicSpec {
    CreateTopicSpec {
        name: "orders".into(),
        partitions: 1,
        replicas: 1,
        configs: std::collections::BTreeMap::new(),
    }
}

#[tokio::test]
async fn create_topics_raises_a_sasl_rejection_on_reconnect() {
    use MockSaslAnswer::{Accept, AuthenticateError, CloseAfterHandshake, HandshakeError};

    for (name, script, expected) in [
        (
            "SaslHandshake 33",
            vec![Accept, HandshakeError(UNSUPPORTED_SASL_MECHANISM)],
            Observed {
                outcome: Outcome::Rejected(SaslAuthenticationError::UnsupportedMechanism(
                    "client SASL mechanism 'PLAIN' not enabled in the server, enabled mechanisms \
                     are [\"PLAIN\"]"
                        .into(),
                )),
                handshakes: 2,
            },
        ),
        (
            "SaslAuthenticate 58",
            vec![Accept, AuthenticateError(SASL_AUTHENTICATION_FAILED)],
            Observed {
                outcome: Outcome::Rejected(SaslAuthenticationError::Failed(
                    "SaslAuthenticate(PLAIN) error_code=58 error_message=Some(\"rejected by mock \
                     broker\")"
                        .into(),
                )),
                handshakes: 2,
            },
        ),
        (
            "close after SaslHandshake, then accept",
            vec![Accept, CloseAfterHandshake, Accept],
            Observed {
                outcome: Outcome::Ok,
                handshakes: 3,
            },
        ),
    ] {
        let handshakes = Arc::new(AtomicUsize::new(0));
        let broker = sasl_broker(script, Arc::clone(&handshakes)).await;
        let mut admin =
            AdminClient::connect_secured(&[broker.addr.to_string()], Some(plain_security()))
                .await
                .unwrap();

        let result = admin
            .create_topics(&[topic()], TopicMutationOptions::with_timeout(secs(5)))
            .await;
        let observed = Observed {
            outcome: classify(&result),
            handshakes: handshakes.load(Ordering::SeqCst),
        };

        broker.stop();
        check!(observed == expected, "case {name}");
    }
}

/// Kafka does not try another bootstrap node after an authentication failure.
#[tokio::test]
async fn bootstrap_stops_at_the_first_rejection() {
    let rejecting_handshakes = Arc::new(AtomicUsize::new(0));
    let rejecting = sasl_broker(
        vec![MockSaslAnswer::AuthenticateError(
            SASL_AUTHENTICATION_FAILED,
        )],
        Arc::clone(&rejecting_handshakes),
    )
    .await;
    let accepting_handshakes = Arc::new(AtomicUsize::new(0));
    let accepting = sasl_broker(
        vec![MockSaslAnswer::Accept],
        Arc::clone(&accepting_handshakes),
    )
    .await;

    let result = AdminClient::connect_secured(
        &[rejecting.addr.to_string(), accepting.addr.to_string()],
        Some(plain_security()),
    )
    .await;
    let observed = (
        classify(&result),
        rejecting_handshakes.load(Ordering::SeqCst),
        accepting_handshakes.load(Ordering::SeqCst),
    );

    rejecting.stop();
    accepting.stop();
    check!(
        observed
            == (
                Outcome::Rejected(SaslAuthenticationError::Failed(
                    "SaslAuthenticate(PLAIN) error_code=58 error_message=Some(\"rejected by mock \
                     broker\")"
                        .into(),
                )),
                1,
                0,
            )
    );
}
