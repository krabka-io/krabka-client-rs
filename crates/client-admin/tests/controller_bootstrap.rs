//! KIP-919 controller-bootstrap rejections, driven through `MockBroker`.
//!
//! The in-crate tests in `src/lib.rs` cover discovery of the active controller
//! and the `DescribeCluster` version floor. This suite covers the rejection
//! paths:
//!
//! - A broker listener answers the controller `DescribeCluster` with
//!   `MISMATCHED_ENDPOINT_TYPE` (114), and `connect_controller` returns that
//!   error.
//! - A call that the controller listener does not advertise fails locally with
//!   `UNSUPPORTED_ENDPOINT_TYPE` (115). Replication-factor reconciliation has
//!   its own 115 preflight, because it needs a broker bootstrap.
//! - A 115 rejection sends nothing, so the controller connection stays usable.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU16, Ordering},
    },
};

use assert2::assert;
use bytes::BytesMut;
use krabka_client_admin::{
    AdminClient, AdminError, CreateTopicSpec, TopicConfigOverrides, TopicMutationOptions,
};
use krabka_client_core::MockBroker;
use krabka_protocol::{
    Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        create_topics_request, describe_cluster_request,
        describe_cluster_response::{self, DescribeClusterBroker, DescribeClusterResponse},
        describe_configs_request,
        describe_configs_response::{
            self, DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
        },
        metadata_request,
    },
};

/// Kafka `EndpointType.BROKER`.
const ENDPOINT_TYPE_BROKER: i8 = 1;
/// Kafka `EndpointType.CONTROLLER`.
const ENDPOINT_TYPE_CONTROLLER: i8 = 2;
/// Kafka `ConfigResource.Type.TOPIC`.
const RESOURCE_TYPE_TOPIC: i8 = 2;
/// Kafka `DescribeConfigsResponse.ConfigSource.DYNAMIC_TOPIC_CONFIG`.
const CONFIG_SOURCE_DYNAMIC_TOPIC: i8 = 1;
const TOPIC: &str = "controller-admin";

/// The message that Kafka's `AuthHelper` sets when a broker listener gets a
/// controller `DescribeCluster`.
const MISMATCH_MESSAGE: &str = "The request was sent to an endpoint of type BROKER, but we wanted an endpoint of type CONTROLLER";

/// Encodes a response body and adds the flexible response-header tag buffer
/// from `flexible_min` on.
fn body<M: Encode>(message: &M, version: i16, flexible_min: i16) -> Vec<u8> {
    let mut bytes = BytesMut::new();
    if version >= flexible_min {
        bytes.extend_from_slice(&[0]);
    }
    message.encode(&mut bytes, version).unwrap();
    bytes.to_vec()
}

/// Encodes an `ApiVersions` v0 body that advertises each `(api_key, max_version)`.
fn api_versions(api_keys: &[(i16, i16)]) -> Vec<u8> {
    let response = ApiVersionsResponse {
        api_keys: api_keys
            .iter()
            .map(|&(api_key, max_version)| ApiVersion {
                api_key,
                min_version: 0,
                max_version,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let mut bytes = BytesMut::new();
    response.encode(&mut bytes, 0).unwrap();
    bytes.to_vec()
}

/// A broker listener. It answers a controller `DescribeCluster` the way a
/// Kafka broker does.
async fn start_broker_listener() -> MockBroker {
    MockBroker::start(|api_key, version, _correlation_id, _body| match api_key {
        api_versions_request::API_KEY => Some(api_versions(&[
            (api_versions_request::API_KEY, 3),
            (
                describe_cluster_request::API_KEY,
                describe_cluster_request::MAX_VERSION,
            ),
            (metadata_request::API_KEY, metadata_request::MAX_VERSION),
        ])),
        describe_cluster_request::API_KEY => Some(body(
            &DescribeClusterResponse {
                error_code: 114,
                error_message: Some(MISMATCH_MESSAGE.into()),
                endpoint_type: ENDPOINT_TYPE_BROKER,
                ..Default::default()
            },
            version,
            describe_cluster_response::FLEXIBLE_MIN,
        )),
        _ => None,
    })
    .await
}

/// A controller listener that advertises `DescribeCluster` and
/// `DescribeConfigs`, and names itself as the active controller.
struct ControllerListener {
    mock: MockBroker,
    received: Arc<Mutex<Vec<i16>>>,
}

impl ControllerListener {
    async fn start() -> Self {
        let port = Arc::new(AtomicU16::new(0));
        let received = Arc::new(Mutex::new(Vec::new()));
        let handler_port = Arc::clone(&port);
        let handler_received = Arc::clone(&received);
        let mock = MockBroker::start(move |api_key, version, _correlation_id, _body| {
            handler_received.lock().unwrap().push(api_key);
            match api_key {
                api_versions_request::API_KEY => Some(api_versions(&[
                    (api_versions_request::API_KEY, 3),
                    (
                        describe_cluster_request::API_KEY,
                        describe_cluster_request::MAX_VERSION,
                    ),
                    (
                        describe_configs_request::API_KEY,
                        describe_configs_request::MAX_VERSION,
                    ),
                ])),
                describe_cluster_request::API_KEY => Some(body(
                    &DescribeClusterResponse {
                        endpoint_type: ENDPOINT_TYPE_CONTROLLER,
                        cluster_id: "controller-test".into(),
                        controller_id: 1,
                        brokers: vec![DescribeClusterBroker {
                            broker_id: 1,
                            host: "127.0.0.1".into(),
                            port: i32::from(handler_port.load(Ordering::SeqCst)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    version,
                    describe_cluster_response::FLEXIBLE_MIN,
                )),
                describe_configs_request::API_KEY => Some(body(
                    &DescribeConfigsResponse {
                        results: vec![DescribeConfigsResult {
                            resource_type: RESOURCE_TYPE_TOPIC,
                            resource_name: TOPIC.into(),
                            configs: vec![DescribeConfigsResourceResult {
                                name: "retention.ms".into(),
                                value: Some("60000".into()),
                                config_source: CONFIG_SOURCE_DYNAMIC_TOPIC,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    version,
                    describe_configs_response::FLEXIBLE_MIN,
                )),
                _ => None,
            }
        })
        .await;
        port.store(mock.addr.port(), Ordering::SeqCst);
        Self { mock, received }
    }

    fn received(&self) -> Vec<i16> {
        self.received.lock().unwrap().clone()
    }
}

fn expected_configs() -> Vec<TopicConfigOverrides> {
    vec![TopicConfigOverrides {
        topic: TOPIC.into(),
        overrides: BTreeMap::from([("retention.ms".into(), "60000".into())]),
    }]
}

/// Asserts a local 115 rejection and returns its message.
fn unsupported_endpoint_message<T: std::fmt::Debug>(result: Result<T, AdminError>) -> String {
    let Err(AdminError::Broker {
        api: "ControllerEndpoint",
        code: 115,
        name: "UNSUPPORTED_ENDPOINT_TYPE",
        message: Some(message),
    }) = result
    else {
        panic!("expected UNSUPPORTED_ENDPOINT_TYPE, got {result:?}");
    };
    message
}

#[tokio::test]
async fn controller_bootstrap_rejects_broker_endpoint() {
    let broker = start_broker_listener().await;

    let result = AdminClient::connect_controller(&[broker.addr.to_string()]).await;

    let Err(AdminError::Broker {
        api,
        code,
        name,
        message,
    }) = result
    else {
        panic!("expected MISMATCHED_ENDPOINT_TYPE, got {:?}", result.err());
    };
    assert!(
        (api, code, name, message.as_deref())
            == (
                "DescribeCluster",
                114,
                "MISMATCHED_ENDPOINT_TYPE",
                Some(MISMATCH_MESSAGE)
            )
    );
    broker.stop();
}

#[tokio::test]
async fn controller_bootstrap_rejects_unadvertised_api_locally() {
    let controller = ControllerListener::start().await;
    let mut admin = AdminClient::connect_controller(&[controller.mock.addr.to_string()])
        .await
        .unwrap();
    let before = controller.received();

    let result = admin
        .create_topics(
            &[CreateTopicSpec {
                name: "not-supported-through-controller".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            TopicMutationOptions::with_timeout(krabka_units::secs(5)),
        )
        .await;

    assert!(
        unsupported_endpoint_message(result)
            == format!(
                "api_key {} is not supported by the controller listener",
                create_topics_request::API_KEY
            )
    );
    assert!(controller.received() == before);
    controller.mock.stop();
}

#[tokio::test]
async fn controller_bootstrap_rejects_replication_factor_reconciliation_locally() {
    let controller = ControllerListener::start().await;
    let mut admin = AdminClient::connect_controller(&[controller.mock.addr.to_string()])
        .await
        .unwrap();
    let before = controller.received();

    let result = admin
        .reconcile_topic_replication_factor(TOPIC, 1, krabka_units::secs(5))
        .await;

    assert!(
        unsupported_endpoint_message(result)
            == "replication-factor reconciliation requires a broker bootstrap endpoint"
    );
    assert!(controller.received() == before);
    controller.mock.stop();
}

#[tokio::test]
async fn controller_connection_stays_usable_after_rejection() {
    let controller = ControllerListener::start().await;
    let mut admin = AdminClient::connect_controller(&[controller.mock.addr.to_string()])
        .await
        .unwrap();
    assert!(admin.describe_configs(&[TOPIC]).await.unwrap() == expected_configs());

    unsupported_endpoint_message(
        admin
            .reconcile_topic_replication_factor(TOPIC, 1, krabka_units::secs(5))
            .await,
    );
    unsupported_endpoint_message(
        admin
            .create_topics(
                &[CreateTopicSpec {
                    name: "not-supported-through-controller".into(),
                    partitions: 1,
                    replicas: 1,
                    configs: BTreeMap::new(),
                }],
                TopicMutationOptions::with_timeout(krabka_units::secs(5)),
            )
            .await,
    );
    let before = controller.received();

    assert!(admin.describe_configs(&[TOPIC]).await.unwrap() == expected_configs());
    // The same connection answers: no new ApiVersions or DescribeCluster.
    assert!(controller.received()[before.len()..] == [describe_configs_request::API_KEY]);
    controller.mock.stop();
}
