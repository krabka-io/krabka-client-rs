//! A SASL rejection is fatal for the consumer, and a disconnect during the
//! SASL exchange is not.
//!
//! Kafka's `ConsumerNetworkClient.checkDisconnects` completes a request with
//! the `AuthenticationException` that `NetworkClient` stored for the node, and
//! `KafkaConsumer.poll` raises it. The heartbeat thread makes the exception its
//! failure cause (`AbstractCoordinator.HeartbeatThread.run`), and the next
//! `poll` raises it.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI32, AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::BytesMut;
use krabka_client_core::{
    AuthenticationError, Client, ClientError, MockBroker, MockReply, MockSaslAnswer,
    SaslAuthenticationError,
    security::{ClientSecurity, SaslCredentials},
};
use krabka_protocol::{
    Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        metadata_request,
        metadata_response::MetadataResponse,
        sasl_handshake_request,
    },
};
use krabka_security::ListenerProtocol;
use krabka_units::{millis, minutes, secs};
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::{
    Assignor, AutoOffsetReset, Consumer, IsolationLevel,
    consumer::{CommitIdentity, ConsumerRetryPolicy},
    coordinator::{
        CoordinatorRetryPolicy, CoordinatorState, HeartbeatOutcome, PollErrorSlot, heartbeat_once,
        take_poll_error,
    },
    error::ConsumerError,
    poll::{DEFAULT_FETCH_MAX, DEFAULT_FETCH_PARTITION_MAX},
};

const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
const SASL_AUTHENTICATION_FAILED: i16 = 58;

/// How a consumer call ended, as an application classifies it.
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

fn classify(error: Option<&ConsumerError>) -> Outcome {
    match error {
        None => Outcome::Ok,
        Some(ConsumerError::Client(ClientError::Authentication {
            source: AuthenticationError::Sasl(error),
            ..
        })) => Outcome::Rejected(error.clone()),
        Some(error) => Outcome::Other(error.to_string()),
    }
}

fn encode(response: &impl Encode, version: i16) -> Vec<u8> {
    let mut body = BytesMut::new();
    response.encode(&mut body, version).unwrap();
    body.to_vec()
}

/// A SASL listener that answers the exchange of connection `n` with
/// `script[n]`, or with the last entry once the script runs out. After a
/// successful exchange it answers `ApiVersions` and an empty `Metadata`, and
/// it gives no answer to other requests.
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
            metadata_request::API_KEY => {
                MockReply::Respond(encode(&MetadataResponse::default(), version))
            }
            _ => MockReply::Silent,
        }
    })
    .await
}

async fn sasl_client(broker: &MockBroker) -> Client {
    Client::builder()
        .bootstrap(broker.addr.to_string())
        .request_timeout(secs(5))
        .security(ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain {
                username: "alice".into(),
                password: "secret".into(),
            }),
            sasl_host: None,
        })
        .build()
        .await
        .unwrap()
}

/// A consumer with no assigned partition, so `poll` only refreshes metadata.
fn consumer(client: Client) -> Consumer {
    Consumer {
        client,
        group_id: "group-a".into(),
        coordinator_id: Arc::new(AtomicI32::new(0)),
        retry_policy: ConsumerRetryPolicy::default().into(),
        member_id: tokio::sync::watch::channel("member-a".to_owned()).1,
        commit_identity: Arc::new(Mutex::new(CommitIdentity {
            generation: 1,
            member_id: "member-a".into(),
            ownership_ids: HashMap::new(),
            rejoin_on_poll: false,
        })),
        commit_serialization: Arc::new(Mutex::new(())),
        commit_async_state: Arc::new(AtomicU8::new(0)),
        commit_async_callbacks: Arc::default(),
        group_instance_id: None,
        current_generation: Arc::new(AtomicI32::new(1)),
        subscribed_topics: vec!["orders".into()],
        assigned: Arc::new(Mutex::new(Vec::new())),
        assignment_changed: Arc::new(Notify::new()),
        next_offsets: Arc::new(Mutex::new(HashMap::new())),
        end_offsets: Arc::new(Mutex::new(HashMap::new())),
        positions: Arc::new(Mutex::new(HashMap::new())),
        topic_ids: Arc::new(Mutex::new(HashMap::new())),
        session_timeout: secs(45),
        heartbeat_interval: secs(3),
        rebalance_protocol: crate::assignor::RebalanceProtocol::Eager,
        coordinator_shutdown: CancellationToken::new(),
        coordinator_handle: None,
        isolation_level: IsolationLevel::ReadUncommitted,
        fetch_min: krabka_client_core::DEFAULT_FETCH_MIN,
        fetch_max: DEFAULT_FETCH_MAX,
        fetch_partition_max: DEFAULT_FETCH_PARTITION_MAX,
        fetch_max_wait: crate::consumer::DEFAULT_CONSUMER_FETCH_MAX_WAIT,
        fetches: crate::poll::Fetches::default(),
        client_rack: None,
        metadata_max_age: crate::consumer::DEFAULT_CONSUMER_METADATA_MAX_AGE,
        auto_offset_reset: AutoOffsetReset::Latest,
        poll_error: PollErrorSlot::default(),
        auto_commit: None,
        poll_signal: crate::coordinator::PollSignal::default(),
        rebalance_pending: tokio::sync::watch::channel(false).1,
        max_poll_records: crate::consumer::DEFAULT_CONSUMER_MAX_POLL_RECORDS,
        fetch_buffer: crate::fetch_buffer::FetchBuffer::default(),
        close_operation: tokio::sync::watch::Sender::new(crate::GroupMembershipOperation::Default),
        rebalance_listener: None,
        listener_calls: tokio::sync::mpsc::unbounded_channel().1,
        assigned_callback_pending: Arc::default(),
    }
}

fn coordinator_state(client: Client) -> CoordinatorState {
    CoordinatorState {
        client,
        group_id: "group-a".into(),
        coordinator_id: Arc::new(AtomicI32::new(0)),
        member_id: "member-a".into(),
        published_member_id: tokio::sync::watch::Sender::new("member-a".to_owned()),
        commit_identity: Arc::new(Mutex::new(CommitIdentity {
            generation: 1,
            member_id: "member-a".into(),
            ownership_ids: HashMap::new(),
            rejoin_on_poll: false,
        })),
        group_instance_id: None,
        generation_id: 1,
        current_generation: Arc::new(AtomicI32::new(1)),
        assignors: vec![Assignor::Range],
        rebalance_protocol: crate::assignor::RebalanceProtocol::Eager,
        subscribed_topics: vec!["orders".into()],
        assigned: Arc::new(Mutex::new(Vec::new())),
        assignment_changed: Arc::new(Notify::new()),
        next_ownership_id: 1,
        next_offsets: Arc::new(Mutex::new(HashMap::new())),
        end_offsets: Arc::new(Mutex::new(HashMap::new())),
        positions: Arc::new(Mutex::new(HashMap::new())),
        topic_ids: Arc::new(Mutex::new(HashMap::new())),
        session_timeout: secs(45),
        max_poll_interval: minutes(1),
        heartbeat_interval: secs(3),
        subscription_metadata_refresh_interval: minutes(5),
        leave_group_timeout: millis(100),
        auto_offset_reset: AutoOffsetReset::Latest,
        client_rack: None,
        initial_subscribed_counts: HashMap::new(),
        retry_policy: CoordinatorRetryPolicy {
            timeout: Duration::from_millis(200),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
        },
        poll_error: PollErrorSlot::default(),
        auto_commit: None,
        commit_serialization: Arc::new(Mutex::new(())),
        join_prepared: false,
        polls: crate::coordinator::PollSignal::default().subscribe(),
        rebalance_pending: tokio::sync::watch::Sender::new(false),
        close_operation: tokio::sync::watch::channel(crate::GroupMembershipOperation::Default).1,
        rejoin_reason: String::new(),
        listener_calls: None,
        assigned_callback_pending: Arc::default(),
        poll_timer: crate::coordinator::PollTimer::new(krabka_units::secs(300)),
        poll_timeout_in_callback: false,
        lost_partitions: Vec::new(),
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

#[tokio::test]
async fn poll_raises_a_sasl_rejection_and_retries_a_disconnect() {
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
                outcome: Outcome::Rejected(unsupported_mechanism()),
                handshakes: 1,
            },
        ),
        (
            "close after SaslHandshake, then accept",
            vec![CloseAfterHandshake, Accept],
            Observed {
                outcome: Outcome::Ok,
                handshakes: 2,
            },
        ),
    ] {
        let handshakes = Arc::new(AtomicUsize::new(0));
        let broker = sasl_broker(script, Arc::clone(&handshakes)).await;
        let mut consumer = consumer(sasl_client(&broker).await);

        let result = consumer.poll(millis(1)).await;
        let observed = Observed {
            outcome: classify(result.as_ref().err()),
            handshakes: handshakes.load(Ordering::SeqCst),
        };

        broker.stop();
        assert2::check!(observed == expected, "case {name}");
    }
}

#[tokio::test]
async fn heartbeat_sasl_rejection_waits_in_the_poll_error_slot() {
    let handshakes = Arc::new(AtomicUsize::new(0));
    let broker = sasl_broker(
        vec![MockSaslAnswer::AuthenticateError(
            SASL_AUTHENTICATION_FAILED,
        )],
        Arc::clone(&handshakes),
    )
    .await;
    let state = coordinator_state(sasl_client(&broker).await);

    let outcome = heartbeat_once(&state).await;
    let slot = take_poll_error(&state.poll_error);
    let observed = (
        outcome,
        classify(slot.as_ref()),
        handshakes.load(Ordering::SeqCst),
    );

    broker.stop();
    assert2::assert!(
        observed
            == (
                HeartbeatOutcome::Transient,
                Outcome::Rejected(authenticate_failed()),
                1
            )
    );
}
