//! [`BrokerTransport`] against an in-process mock cluster.
//!
//! The mock answers as the bootstrap broker, the partition leader and the
//! transaction coordinator at once. It has the coordination topic with one
//! partition, so every role maps to partition 0.

use std::sync::{
    Arc, Mutex as StdMutex, PoisonError,
    atomic::{AtomicU16, Ordering},
};

use assert2::{assert, check};
use bytes::BytesMut;
use krabka_client_core::{MockBroker, mock_test_support::recorded_levels};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        create_topics_request,
        create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
        find_coordinator_request,
        find_coordinator_response::FindCoordinatorResponse,
        init_producer_id_request::{self, InitProducerIdRequest},
        init_producer_id_response::InitProducerIdResponse,
        metadata_request,
        metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
        produce_request,
        produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
    },
};
use tracing::Level;

use super::*;

/// The producer id that the mock coordinator mints for every role.
const PRODUCER_ID: i64 = 7;

/// The first producer epoch that the mock coordinator mints.
const FIRST_EPOCH: i16 = 3;

/// What the mock cluster answers, and what it saw.
struct Cluster {
    /// The error code of a transactional `InitProducerId`.
    init_error: i16,
    /// The producer epoch the next transactional `InitProducerId` mints.
    next_epoch: i16,
    /// The error code of the coordination topic in `CreateTopics`.
    create_topic_error: i16,
    /// The number of `Produce` requests.
    produce_requests: usize,
}

impl Cluster {
    /// The error code and the producer epoch of the next transactional
    /// `InitProducerId`.
    fn mint(&mut self) -> (i16, i16) {
        let epoch = self.next_epoch;
        self.next_epoch += 1;
        (self.init_error, epoch)
    }

    fn new() -> Self {
        Self {
            init_error: 0,
            next_epoch: FIRST_EPOCH,
            create_topic_error: TOPIC_ALREADY_EXISTS,
            produce_requests: 0,
        }
    }
}

type SharedCluster = Arc<StdMutex<Cluster>>;

/// Encodes a response body at `version`, with the tagged-field byte of a
/// flexible response header.
fn body(response: &impl Encode, version: i16, flexible: bool) -> Vec<u8> {
    let mut buffer = BytesMut::new();
    if flexible {
        buffer.extend_from_slice(&[0]);
    }
    response
        .encode(&mut buffer, version)
        .expect("encode a response");
    buffer.to_vec()
}

/// Tells whether an `InitProducerId` request carries a `transactional.id`.
fn is_transactional_init(request: &[u8], version: i16) -> bool {
    let client_id_len = usize::try_from(i16::from_be_bytes([request[0], request[1]]).max(0))
        .expect("non-negative client id length");
    let flexible = usize::from(version >= init_producer_id_request::FLEXIBLE_MIN);
    let mut payload = &request[2 + client_id_len + flexible..];
    InitProducerIdRequest::decode(&mut payload, version)
        .expect("decode InitProducerId")
        .transactional_id
        .is_some()
}

fn api_versions() -> Vec<u8> {
    let range = |api_key, min_version, max_version| ApiVersion {
        api_key,
        min_version,
        max_version,
        ..Default::default()
    };
    body(
        &ApiVersionsResponse {
            api_keys: vec![
                range(api_versions_request::API_KEY, 0, 3),
                range(metadata_request::API_KEY, 0, 8),
                range(create_topics_request::API_KEY, 0, 4),
                range(find_coordinator_request::API_KEY, 0, 0),
                range(init_producer_id_request::API_KEY, 0, 0),
                range(produce_request::API_KEY, 3, 8),
            ],
            ..Default::default()
        },
        0,
        false,
    )
}

fn metadata(port: u16, version: i16) -> Vec<u8> {
    body(
        &MetadataResponse {
            controller_id: 1,
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: i32::from(port),
                ..Default::default()
            }],
            topics: vec![MetadataResponseTopic {
                name: Some(COORDINATION_STATE_TOPIC.to_owned()),
                partitions: vec![MetadataResponsePartition {
                    partition_index: 0,
                    leader_id: 1,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
        version,
        false,
    )
}

/// Answers one request of the mock cluster.
fn answer(
    cluster: &SharedCluster,
    port: u16,
    api_key: i16,
    version: i16,
    request: &[u8],
) -> Option<Vec<u8>> {
    let lock = || cluster.lock().unwrap_or_else(PoisonError::into_inner);
    match api_key {
        api_versions_request::API_KEY => Some(api_versions()),
        metadata_request::API_KEY => Some(metadata(port, version)),
        create_topics_request::API_KEY => Some(body(
            &CreateTopicsResponse {
                topics: vec![CreatableTopicResult {
                    name: COORDINATION_STATE_TOPIC.to_owned(),
                    error_code: lock().create_topic_error,
                    ..Default::default()
                }],
                ..Default::default()
            },
            version,
            false,
        )),
        find_coordinator_request::API_KEY => Some(body(
            &FindCoordinatorResponse {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: i32::from(port),
                ..Default::default()
            },
            version,
            false,
        )),
        init_producer_id_request::API_KEY => {
            let response = if is_transactional_init(request, version) {
                let (init_error, epoch) = lock().mint();
                InitProducerIdResponse {
                    error_code: init_error,
                    producer_id: PRODUCER_ID,
                    producer_epoch: epoch,
                    ..Default::default()
                }
            } else {
                InitProducerIdResponse {
                    producer_id: 1,
                    ..Default::default()
                }
            };
            Some(body(&response, version, false))
        }
        produce_request::API_KEY => {
            lock().produce_requests += 1;
            Some(body(
                &ProduceResponse {
                    responses: vec![TopicProduceResponse {
                        name: COORDINATION_STATE_TOPIC.to_owned(),
                        partition_responses: vec![PartitionProduceResponse {
                            index: 0,
                            base_offset: 0,
                            log_start_offset: -1,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                version,
                false,
            ))
        }
        _ => None,
    }
}

/// Starts the mock cluster and a transport against it.
async fn start(cluster: &SharedCluster) -> (MockBroker, BrokerTransport) {
    let port_cell = Arc::new(AtomicU16::new(0));
    let handler_port = Arc::clone(&port_cell);
    let handler_cluster = Arc::clone(cluster);
    let mock = MockBroker::start(move |api_key, version, _correlation_id, request| {
        answer(
            &handler_cluster,
            handler_port.load(Ordering::SeqCst),
            api_key,
            version,
            request,
        )
    })
    .await
    .expect("start the mock cluster");
    port_cell.store(mock.addr.port(), Ordering::SeqCst);
    let transport = BrokerTransport::builder()
        .bootstrap(mock.addr.to_string())
        .request_timeout(secs(2))
        .build()
        .await
        .expect("the transport connects to the mock cluster");
    (mock, transport)
}

fn shared_cluster(configure: impl FnOnce(&mut Cluster)) -> SharedCluster {
    let mut cluster = Cluster::new();
    configure(&mut cluster);
    Arc::new(StdMutex::new(cluster))
}

fn role(name: &str) -> Role {
    Role::new(name).expect("a valid role")
}

fn token(producer_epoch: i16) -> FencingToken {
    FencingToken::new(PRODUCER_ID, producer_epoch).expect("a valid token")
}

/// `acquire_epoch` creates the topic, reads its layout, and keeps the
/// producer bound to the minted token. A second call mints a later epoch,
/// replaces the holder, and closes the replaced producer.
#[tokio::test]
async fn acquire_epoch_holds_the_minted_token_and_replaces_the_previous_holder() {
    let cluster = shared_cluster(|_| {});
    let (_mock, transport) = start(&cluster).await;
    let name = role("orders-leader");

    let first = transport.acquire_epoch(&name).await.expect("first epoch");
    check!(first == token(FIRST_EPOCH));
    check!(transport.held_token(&name).await == Some(token(FIRST_EPOCH)));

    let (second, levels) = recorded_levels(transport.acquire_epoch(&name)).await;
    check!(second.expect("second epoch") == token(FIRST_EPOCH + 1));
    check!(transport.held_token(&name).await == Some(token(FIRST_EPOCH + 1)));
    // The replaced producer had no other owner, so it closed without a warning.
    check!(!levels.contains(&Level::WARN));
    check!(transport.bound_producer(&name).await.is_some());
    check!(transport.bound_producer(&role("other")).await.is_none());
    transport.close().await;
}

/// A replaced producer that a leadership handle still shares stays open, and
/// the transport says so in a warning.
#[tokio::test]
async fn a_replaced_producer_that_a_handle_shares_stays_open() {
    let cluster = shared_cluster(|_| {});
    let (_mock, transport) = start(&cluster).await;
    let name = role("orders-leader");
    transport.acquire_epoch(&name).await.expect("first epoch");
    let shared = transport
        .bound_producer(&name)
        .await
        .expect("the first producer");

    let (second, levels) = recorded_levels(transport.acquire_epoch(&name)).await;

    check!(second.expect("second epoch") == token(FIRST_EPOCH + 1));
    check!(levels.contains(&Level::WARN));
    let owners = Arc::strong_count(&shared);
    drop(shared);
    check!(owners == 1);
    transport.close().await;
}

/// A fencing `InitProducerId` answer is the loss of the role, and every other
/// error passes through as a producer error. Neither installs a holder.
#[tokio::test]
async fn hold_role_maps_init_transactions_errors() {
    for (name, init_error, fenced) in [
        ("producer fenced", PRODUCER_FENCED, true),
        ("invalid producer epoch", INVALID_PRODUCER_EPOCH, true),
        ("authorization failed", 53, false),
    ] {
        let cluster = shared_cluster(|cluster| cluster.init_error = init_error);
        let (_mock, transport) = start(&cluster).await;
        let role_name = role("orders-leader");
        let result = transport
            .hold_role(
                &role_name,
                transport
                    .build_role_producer(&role_name)
                    .await
                    .expect("the role producer"),
            )
            .await;

        if fenced {
            assert!(
                let Err(CoordinationError::Fenced { role: fenced_role }) = result,
                "{name}"
            );
            check!(fenced_role == role_name, "{name}");
        } else {
            assert!(let Err(CoordinationError::Producer(_)) = result, "{name}");
        }
        check!(transport.held_token(&role_name).await == None, "{name}");
        transport.close().await;
    }
}

/// `register` appends one registration record with the plain producer, on
/// the partition of the role.
#[tokio::test]
async fn register_appends_one_registration_record() {
    let cluster = shared_cluster(|_| {});
    let (_mock, transport) = start(&cluster).await;

    let registered = transport
        .register(
            &role("orders-leader"),
            &MemberId::new("node-1").expect("a valid member id"),
        )
        .await;

    check!(registered.is_ok());
    check!(
        cluster
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .produce_requests
            == 1
    );
    transport.close().await;
}

/// A `CreateTopics` error other than `TOPIC_ALREADY_EXISTS` fails the first
/// call that needs the topic.
#[tokio::test]
async fn a_topic_creation_error_fails_acquire_epoch() {
    let cluster = shared_cluster(|cluster| cluster.create_topic_error = 29);
    let (_mock, transport) = start(&cluster).await;

    let result = transport.acquire_epoch(&role("orders-leader")).await;

    assert!(
        let Err(CoordinationError::Admin(AdminError::Broker {
            api: "CreateTopics",
            code: 29,
            ..
        })) = result
    );
    transport.close().await;
}
