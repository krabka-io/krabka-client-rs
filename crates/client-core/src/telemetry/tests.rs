//! The request sequence of the reporter against a mock broker.

use std::time::Duration;

use assert2::assert;
use bytes::BytesMut;
use krabka_compression::CompressionType;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::{self, ApiVersionsRequest},
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        get_telemetry_subscriptions_request::{self, GetTelemetrySubscriptionsRequest},
        get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse,
        push_telemetry_request::{self, PushTelemetryRequest},
        push_telemetry_response::PushTelemetryResponse,
    },
    primitives::uuid::Uuid,
};
use tokio::sync::mpsc;

use super::{
    ClientTelemetryConfig,
    otlp::{KeyValue, MetricsData},
};
use crate::{Client, MockBroker};

const INSTANCE: Uuid = Uuid([9; 16]);
const PREFIX: &str = "org.apache.kafka.producer";
const PUSH_INTERVAL_MS: i32 = 200;
const UNKNOWN_SUBSCRIPTION_ID: i16 = 117;
const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;
const TELEMETRY_TOO_LARGE: i16 = 118;
const THROTTLING_QUOTA_EXCEEDED: i16 = 89;
const INVALID_RECORD: i16 = 87;
const INVALID_REQUEST: i16 = 42;

/// A telemetry request that the broker received. A push holds the names of
/// its metrics and the resource attributes of each metric.
#[derive(Clone, Debug, PartialEq)]
enum Seen {
    Subscriptions(GetTelemetrySubscriptionsRequest),
    Push {
        client_instance_id: Uuid,
        subscription_id: i32,
        terminating: bool,
        compression_type: i8,
        metric_names: Vec<String>,
        resources: Vec<Vec<KeyValue>>,
    },
}

fn get(client_instance_id: Uuid) -> Seen {
    Seen::Subscriptions(GetTelemetrySubscriptionsRequest {
        client_instance_id,
        ..Default::default()
    })
}

fn network_metric_names(group: &str) -> Vec<String> {
    [
        "connection.close.total",
        "connection.count",
        "connection.creation.total",
        "incoming.byte.total",
        "outgoing.byte.total",
        "request.total",
        "response.total",
    ]
    .map(|name| format!("org.apache.kafka.{group}.{name}"))
    .to_vec()
}

fn push(terminating: bool, compression: CompressionType, resource: &[(&str, &str)]) -> Seen {
    let metric_names = network_metric_names("producer");
    let resource = resource
        .iter()
        .map(|(key, value)| KeyValue::new(*key, *value))
        .collect::<Vec<_>>();
    Seen::Push {
        client_instance_id: INSTANCE,
        subscription_id: 11,
        terminating,
        compression_type: i8::try_from(compression.as_attribute_bits()).unwrap(),
        resources: vec![resource; metric_names.len()],
        metric_names,
    }
}

fn subscription(requested: &[&str], accepted: Vec<i8>) -> GetTelemetrySubscriptionsResponse {
    GetTelemetrySubscriptionsResponse {
        client_instance_id: INSTANCE,
        subscription_id: 11,
        accepted_compression_types: accepted,
        push_interval_ms: PUSH_INTERVAL_MS,
        telemetry_max_bytes: 1 << 20,
        requested_metrics: requested.iter().map(|name| (*name).to_owned()).collect(),
        ..Default::default()
    }
}

/// The body of a request after the request header v2: the client id and the
/// tagged fields.
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
    let codec =
        CompressionType::from_attribute_bits(u8::try_from(request.compression_type).unwrap())
            .unwrap();
    let payload =
        krabka_compression::decompress(codec, &request.metrics, krabka_units::mebibytes(1))
            .unwrap();
    let data = MetricsData::decode(&payload).unwrap();
    let (metric_names, resources) = data
        .resource_metrics
        .into_iter()
        .map(|resource_metrics| {
            let names = resource_metrics
                .scope_metrics
                .into_iter()
                .flat_map(|scope| scope.metrics)
                .map(|metric| metric.name)
                .collect::<Vec<_>>();
            (
                names.concat(),
                resource_metrics.resource.unwrap_or_default().attributes,
            )
        })
        .unzip();
    Seen::Push {
        client_instance_id: request.client_instance_id,
        subscription_id: request.subscription_id,
        terminating: request.terminating,
        compression_type: request.compression_type,
        metric_names,
        resources,
    }
}

/// How the mock broker answers.
struct BrokerScript {
    advertise_telemetry: bool,
    subscription: GetTelemetrySubscriptionsResponse,
    /// The error codes of the first pushes, in order. Later pushes succeed.
    push_errors: Vec<i16>,
}

async fn start_broker(mut script: BrokerScript) -> (MockBroker, mpsc::UnboundedReceiver<Seen>) {
    let (seen_tx, seen_rx) = mpsc::unbounded_channel();
    script.push_errors.reverse();
    let broker = MockBroker::start(move |api_key, version, _corr, frame| match api_key {
        api_versions_request::API_KEY => {
            let mut keys = vec![(api_versions_request::API_KEY, 3)];
            if script.advertise_telemetry {
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
            seen_tx.send(Seen::Subscriptions(request)).ok();
            Some(flexible_response(&script.subscription))
        }
        push_telemetry_request::API_KEY => {
            let mut body = request_body(frame);
            let request = PushTelemetryRequest::decode(&mut body, version).unwrap();
            seen_tx.send(seen_push(&request)).ok();
            Some(flexible_response(&PushTelemetryResponse {
                error_code: script.push_errors.pop().unwrap_or(0),
                ..Default::default()
            }))
        }
        _ => None,
    })
    .await;
    (broker, seen_rx)
}

/// A client with an open bootstrap connection.
async fn client(broker: &MockBroker, telemetry: Option<ClientTelemetryConfig>) -> Client {
    let client = Client::builder()
        .bootstrap(broker.addr.to_string())
        .client_id("telemetry-test")
        .request_timeout(krabka_units::secs(5))
        .maybe_telemetry(telemetry)
        .build()
        .await
        .unwrap();
    client.send(ApiVersionsRequest::default()).await.unwrap();
    client
}

async fn next(seen: &mut mpsc::UnboundedReceiver<Seen>) -> Seen {
    tokio::time::timeout(Duration::from_secs(10), seen.recv())
        .await
        .expect("a telemetry request within 10 s")
        .expect("the broker is running")
}

/// Collect `count` requests, close the telemetry, and collect the rest.
async fn run(
    script: BrokerScript,
    telemetry: Option<ClientTelemetryConfig>,
    count: usize,
) -> Vec<Seen> {
    let (broker, mut seen) = start_broker(script).await;
    let client = client(&broker, telemetry).await;
    let mut out = Vec::new();
    for _ in 0..count {
        out.push(next(&mut seen).await);
    }
    if count == 0 {
        // Nothing should arrive: give the reporter several push intervals.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    tokio::time::timeout(Duration::from_secs(20), client.close_telemetry())
        .await
        .expect("the reporter ends");
    client.close();
    broker.stop();
    while let Ok(request) = seen.try_recv() {
        out.push(request);
    }
    out
}

fn producer() -> ClientTelemetryConfig {
    ClientTelemetryConfig::producer(Some("tx"))
}

/// Kafka's request sequence: the subscription with the zero instance id,
/// pushes at the push interval, and a terminating push on close.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pushes_the_subscribed_metrics_and_a_terminating_push_on_close() {
    let cases = [
        (vec![], CompressionType::None),
        (vec![9, 1, 4], CompressionType::Gzip),
        (vec![4], CompressionType::Zstd),
        (vec![3], CompressionType::Lz4),
        (vec![2], CompressionType::Snappy),
    ];
    for (accepted, codec) in cases {
        let script = BrokerScript {
            advertise_telemetry: true,
            subscription: subscription(&[PREFIX], accepted.clone()),
            push_errors: vec![],
        };
        let seen = run(script, Some(producer()), 3).await;
        let resource = [("transactional_id", "tx")];
        assert!(
            seen == [
                get(Uuid::ZERO),
                push(false, codec, &resource),
                push(false, codec, &resource),
                push(true, codec, &resource),
            ],
            "{accepted:?}"
        );
    }
}

/// The subscription selects metrics by name prefix, and `*` selects all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_holds_the_metrics_that_match_a_requested_prefix() {
    let cases: [(&[&str], ClientTelemetryConfig, Vec<String>); 3] = [
        (
            &[
                "org.apache.kafka.producer.request",
                "org.apache.kafka.producer.response",
            ],
            ClientTelemetryConfig::producer(None),
            vec![
                "org.apache.kafka.producer.request.total".to_owned(),
                "org.apache.kafka.producer.response.total".to_owned(),
            ],
        ),
        (
            &["*"],
            ClientTelemetryConfig::consumer(Some("g"), None, Some("r1")),
            network_metric_names("consumer"),
        ),
        (
            &["org.apache.kafka.admin.client.connection"],
            ClientTelemetryConfig::admin(),
            network_metric_names("admin.client")[..3].to_vec(),
        ),
    ];
    for (requested, config, metric_names) in cases {
        let resource = config
            .resource_attributes
            .iter()
            .map(|(key, value)| KeyValue::new(key, value))
            .collect::<Vec<_>>();
        let script = BrokerScript {
            advertise_telemetry: true,
            subscription: subscription(requested, vec![]),
            push_errors: vec![],
        };
        let seen = run(script, Some(config), 2).await;
        assert!(
            seen[1]
                == Seen::Push {
                    client_instance_id: INSTANCE,
                    subscription_id: 11,
                    terminating: false,
                    compression_type: 0,
                    resources: vec![resource; metric_names.len()],
                    metric_names,
                },
            "{requested:?}"
        );
    }
}

/// With no subscription, or with the setting off, or with a broker that
/// does not support KIP-714, the client pushes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_push_without_a_subscription_the_setting_or_broker_support() {
    let no_subscription = GetTelemetrySubscriptionsResponse {
        subscription_id: 0,
        push_interval_ms: 60_000,
        ..subscription(&[], vec![])
    };
    let cases = [
        (
            "no subscription",
            true,
            no_subscription,
            Some(producer()),
            1,
            vec![get(Uuid::ZERO)],
        ),
        (
            "enable.metrics.push=false",
            true,
            subscription(&[PREFIX], vec![]),
            None,
            0,
            vec![],
        ),
        (
            "broker without KIP-714",
            false,
            subscription(&[PREFIX], vec![]),
            Some(producer()),
            0,
            vec![],
        ),
    ];
    for (name, advertise_telemetry, subscription, telemetry, count, expected) in cases {
        let script = BrokerScript {
            advertise_telemetry,
            subscription,
            push_errors: vec![],
        };
        let seen = run(script, telemetry, count).await;
        assert!(seen == expected, "{name}");
    }
}

/// A push error fetches the subscription again, with the assigned instance
/// id, or stops telemetry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_errors_refetch_the_subscription_or_stop() {
    let resource = [("transactional_id", "tx")];
    let refetched = vec![
        get(Uuid::ZERO),
        push(false, CompressionType::None, &resource),
        get(INSTANCE),
        push(false, CompressionType::None, &resource),
        push(true, CompressionType::None, &resource),
    ];
    let stopped = vec![
        get(Uuid::ZERO),
        push(false, CompressionType::None, &resource),
    ];
    let cases = [
        (UNKNOWN_SUBSCRIPTION_ID, 4, refetched.clone()),
        (UNSUPPORTED_COMPRESSION_TYPE, 4, refetched.clone()),
        (TELEMETRY_TOO_LARGE, 4, refetched.clone()),
        (THROTTLING_QUOTA_EXCEEDED, 4, refetched),
        (INVALID_RECORD, 2, stopped.clone()),
        (INVALID_REQUEST, 2, stopped),
    ];
    for (error_code, count, expected) in cases {
        let script = BrokerScript {
            advertise_telemetry: true,
            subscription: subscription(&[PREFIX], vec![]),
            push_errors: vec![error_code],
        };
        let seen = run(script, Some(producer()), count).await;
        assert!(seen == expected, "{error_code}");
    }
}
