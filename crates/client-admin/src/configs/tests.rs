use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use krabka_client_core::{MockBroker, MockReply};
use krabka_protocol::owned::{
    api_versions_request, describe_configs_request,
    describe_configs_response::DescribeConfigsSynonym,
    incremental_alter_configs_request,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    metadata_request,
    metadata_response::{MetadataResponse, MetadataResponseBroker},
};

use super::*;
use crate::partition_leaders::test_support::{
    api_versions, decode_request, encode_response, fast_admin, refused_address,
};

const LONG: krabka_units::Time = krabka_units::secs(5);
const SHORT: krabka_units::Time = krabka_units::millis(300);

fn every_type() -> Vec<ConfigResource> {
    vec![
        ConfigResource::topic("orders"),
        ConfigResource::broker(2),
        ConfigResource::broker_default(),
        ConfigResource::broker_logger(2),
        ConfigResource::client_metrics("all-metrics"),
        ConfigResource::group("billing"),
    ]
}

#[test]
fn config_source_ids_are_kafkas() {
    for (source, id) in [
        (ConfigSource::Unknown, 0),
        (ConfigSource::DynamicTopicConfig, 1),
        (ConfigSource::DynamicBrokerConfig, 2),
        (ConfigSource::DynamicDefaultBrokerConfig, 3),
        (ConfigSource::StaticBrokerConfig, 4),
        (ConfigSource::DefaultConfig, 5),
        (ConfigSource::DynamicBrokerLoggerConfig, 6),
        (ConfigSource::DynamicClientMetricsConfig, 7),
        (ConfigSource::DynamicGroupConfig, 8),
    ] {
        assert2::assert!((source.id(), ConfigSource::from_id(id)) == (id, Some(source)));
    }
    for (id, expected) in [
        (9, Some(ConfigSource::Unknown)),
        (i8::MAX, Some(ConfigSource::Unknown)),
        (-1, None),
        (i8::MIN, None),
    ] {
        assert2::assert!(ConfigSource::from_id(id) == expected, "id {id}");
    }
}

#[test]
fn config_type_ids_are_kafkas() {
    for (config_type, id) in [
        (ConfigType::Unknown, 0),
        (ConfigType::Boolean, 1),
        (ConfigType::String, 2),
        (ConfigType::Int, 3),
        (ConfigType::Short, 4),
        (ConfigType::Long, 5),
        (ConfigType::Double, 6),
        (ConfigType::List, 7),
        (ConfigType::Class, 8),
        (ConfigType::Password, 9),
    ] {
        assert2::assert!((config_type.id(), ConfigType::from_id(id)) == (id, Some(config_type)));
    }
    for (id, expected) in [(10, Some(ConfigType::Unknown)), (-1, None)] {
        assert2::assert!(ConfigType::from_id(id) == expected, "id {id}");
    }
}

#[test]
fn alter_config_ops_carry_kafkas_op_ids() {
    for (op, expected) in [
        (
            AlterConfigOp::set("retention.ms", "60000"),
            (Some("60000"), AlterConfigOpType::Set, 0),
        ),
        (
            AlterConfigOp::delete("retention.ms"),
            (None, AlterConfigOpType::Delete, 1),
        ),
        (
            AlterConfigOp::append("cleanup.policy", "compact"),
            (Some("compact"), AlterConfigOpType::Append, 2),
        ),
        (
            AlterConfigOp::subtract("cleanup.policy", "delete"),
            (Some("delete"), AlterConfigOpType::Subtract, 3),
        ),
    ] {
        assert2::assert!((op.value.as_deref(), op.op_type, op.op_type.id()) == expected);
    }
}

#[test]
fn node_for_routes_named_brokers_and_loggers_by_id() {
    for (resource, expected) in [
        (ConfigResource::topic("7"), Ok(None)),
        (ConfigResource::broker(7), Ok(Some(7))),
        (ConfigResource::broker_default(), Ok(None)),
        (ConfigResource::broker_logger(7), Ok(Some(7))),
        (ConfigResource::client_metrics("7"), Ok(None)),
        (ConfigResource::group("7"), Ok(None)),
        (
            ConfigResource::new(ConfigResourceType::Unknown, "7"),
            Ok(None),
        ),
        (
            ConfigResource::new(ConfigResourceType::Broker, "one"),
            Err(()),
        ),
        (
            ConfigResource::new(ConfigResourceType::BrokerLogger, ""),
            Err(()),
        ),
    ] {
        let routed = node_for(&resource).map_err(|error| {
            assert2::assert!(let AdminError::InvalidArgument(_) = error);
        });
        assert2::assert!(routed == expected, "resource {resource:?}");
    }
}

fn entry(name: &str, value: Option<&str>, source: ConfigSource) -> ConfigEntry {
    ConfigEntry {
        name: name.to_owned(),
        value: value.map(str::to_owned),
        source,
        is_sensitive: false,
        is_read_only: false,
        synonyms: Vec::new(),
        config_type: ConfigType::String,
        documentation: None,
    }
}

fn config(entries: Vec<ConfigEntry>) -> Config {
    Config {
        entries: entries
            .into_iter()
            .map(|entry| (entry.name.clone(), entry))
            .collect(),
    }
}

#[test]
fn dynamic_overrides_keep_the_source_of_the_resource() {
    let all = config(vec![
        entry("topic", Some("1"), ConfigSource::DynamicTopicConfig),
        entry("broker", Some("2"), ConfigSource::DynamicBrokerConfig),
        entry(
            "default",
            Some("3"),
            ConfigSource::DynamicDefaultBrokerConfig,
        ),
        entry("logger", Some("4"), ConfigSource::DynamicBrokerLoggerConfig),
        entry(
            "metrics",
            Some("5"),
            ConfigSource::DynamicClientMetricsConfig,
        ),
        entry("group", Some("6"), ConfigSource::DynamicGroupConfig),
        entry("static", Some("7"), ConfigSource::StaticBrokerConfig),
        entry("builtin", Some("8"), ConfigSource::DefaultConfig),
        entry("withheld", None, ConfigSource::DynamicTopicConfig),
    ]);
    for (resource, expected) in [
        (ConfigResource::topic("orders"), vec![("topic", "1")]),
        (ConfigResource::broker(1), vec![("broker", "2")]),
        (ConfigResource::broker_default(), vec![("default", "3")]),
        (ConfigResource::broker_logger(1), vec![("logger", "4")]),
        (ConfigResource::client_metrics("m"), vec![("metrics", "5")]),
        (ConfigResource::group("g"), vec![("group", "6")]),
        (
            ConfigResource::new(ConfigResourceType::Unknown, "x"),
            vec![],
        ),
    ] {
        let expected = expected
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect::<BTreeMap<_, _>>();
        assert2::assert!(
            all.dynamic_overrides(&resource) == expected,
            "resource {resource:?}"
        );
    }
}

#[test]
fn debug_redacts_a_sensitive_value() {
    let password = ConfigEntry {
        is_sensitive: true,
        ..entry(
            "ssl.key.password",
            Some("hunter2"),
            ConfigSource::DynamicBrokerConfig,
        )
    };
    let plain = entry(
        "retention.ms",
        Some("60000"),
        ConfigSource::DynamicTopicConfig,
    );
    for (entry, shown, hidden) in [
        (password, "value: \"Redacted\"", Some("hunter2")),
        (plain, "value: Some(\"60000\")", None),
    ] {
        let printed = format!("{entry:?}");
        assert2::assert!(printed.contains(shown), "{printed}");
        assert2::assert!(
            hidden.is_none_or(|hidden| !printed.contains(hidden)),
            "{printed}"
        );
    }
}

fn wire_result(resource: &ConfigResource) -> DescribeConfigsResult {
    DescribeConfigsResult {
        resource_type: resource.resource_type.id(),
        resource_name: resource.name.clone(),
        ..Default::default()
    }
}

fn wire_entry(name: &str, value: Option<&str>, source: i8) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult {
        name: name.to_owned(),
        value: value.map(str::to_owned),
        config_source: source,
        config_type: 2,
        documentation: None,
        ..Default::default()
    }
}

fn unknown_server_error_with(message: &str) -> KafkaError {
    KafkaError {
        code: UNKNOWN_SERVER_ERROR,
        name: "UNKNOWN_SERVER_ERROR",
        message: Some(message.to_owned()),
    }
}

/// Kafka's `describeConfigResult` maps each wire entry, including its
/// synonyms, sensitivity, type and documentation. A resource gets its own
/// error, `UNKNOWN_SERVER_ERROR` when the answer skips it, and the answer's
/// extra resources are dropped.
#[test]
fn describe_results_decode_each_resource() {
    let orders = ConfigResource::topic("orders");
    let broker = ConfigResource::broker(1);
    let missing = ConfigResource::topic("missing");
    let skipped = ConfigResource::group("billing");
    let asked = BTreeSet::from([
        orders.clone(),
        broker.clone(),
        missing.clone(),
        skipped.clone(),
    ]);
    let response = DescribeConfigsResponse {
        results: vec![
            DescribeConfigsResult {
                configs: vec![
                    DescribeConfigsResourceResult {
                        synonyms: vec![
                            DescribeConfigsSynonym {
                                name: "retention.ms".into(),
                                value: Some("60000".into()),
                                source: 1,
                                ..Default::default()
                            },
                            DescribeConfigsSynonym {
                                name: "log.retention.ms".into(),
                                value: None,
                                source: 5,
                                ..Default::default()
                            },
                        ],
                        config_type: 5,
                        documentation: Some("How long to keep a log.".into()),
                        ..wire_entry("retention.ms", Some("60000"), 1)
                    },
                    DescribeConfigsResourceResult {
                        config_type: 11,
                        ..wire_entry("future.type", Some("x"), 42)
                    },
                ],
                ..wire_result(&orders)
            },
            DescribeConfigsResult {
                configs: vec![DescribeConfigsResourceResult {
                    is_sensitive: true,
                    read_only: true,
                    config_type: 9,
                    ..wire_entry("ssl.key.password", None, 4)
                }],
                ..wire_result(&broker)
            },
            DescribeConfigsResult {
                error_code: 3,
                error_message: Some("no such topic".into()),
                ..wire_result(&missing)
            },
            wire_result(&ConfigResource::topic("not-asked")),
        ],
        ..Default::default()
    };
    let expected = BTreeMap::from([
        (
            orders,
            Ok(config(vec![
                ConfigEntry {
                    synonyms: vec![
                        ConfigSynonym {
                            name: "retention.ms".into(),
                            value: Some("60000".into()),
                            source: ConfigSource::DynamicTopicConfig,
                        },
                        ConfigSynonym {
                            name: "log.retention.ms".into(),
                            value: None,
                            source: ConfigSource::DefaultConfig,
                        },
                    ],
                    config_type: ConfigType::Long,
                    documentation: Some("How long to keep a log.".into()),
                    ..entry(
                        "retention.ms",
                        Some("60000"),
                        ConfigSource::DynamicTopicConfig,
                    )
                },
                ConfigEntry {
                    config_type: ConfigType::Unknown,
                    ..entry("future.type", Some("x"), ConfigSource::Unknown)
                },
            ])),
        ),
        (
            broker,
            Ok(config(vec![ConfigEntry {
                is_sensitive: true,
                is_read_only: true,
                config_type: ConfigType::Password,
                ..entry("ssl.key.password", None, ConfigSource::StaticBrokerConfig)
            }])),
        ),
        (
            missing,
            Err(KafkaError {
                code: 3,
                name: "UNKNOWN_TOPIC_OR_PARTITION",
                message: Some("no such topic".into()),
            }),
        ),
        (
            skipped,
            Err(unknown_server_error_with(
                "The node response did not contain a result for config resource \
                 ConfigResource { resource_type: Group, name: \"billing\" }",
            )),
        ),
    ]);
    assert2::assert!(describe_results(&asked, Ok(response)) == expected);
}

/// Kafka's `handleFailure` fails every resource of the request: when the
/// request fails, when the answer names a resource twice (`toMap` throws),
/// and when an entry has a negative source or type id (`forId` throws).
#[test]
fn describe_results_fail_every_resource_of_a_bad_answer() {
    let orders = ConfigResource::topic("orders");
    let broker = ConfigResource::broker(1);
    let asked = BTreeSet::from([orders.clone(), broker.clone()]);
    let with_entry = |entry| DescribeConfigsResponse {
        results: vec![
            DescribeConfigsResult {
                configs: vec![entry],
                ..wire_result(&orders)
            },
            wire_result(&broker),
        ],
        ..Default::default()
    };
    let timed_out = KafkaError {
        code: 7,
        name: "REQUEST_TIMED_OUT",
        message: None,
    };
    for (name, answer, expected) in [
        ("the request failed", Err(timed_out.clone()), timed_out),
        (
            "a resource twice",
            Ok(DescribeConfigsResponse {
                results: vec![
                    wire_result(&orders),
                    wire_result(&broker),
                    wire_result(&orders),
                ],
                ..Default::default()
            }),
            unknown_server_error_with(
                "Duplicate key for config resource \
                 ConfigResource { resource_type: Topic, name: \"orders\" }",
            ),
        ),
        (
            "a negative source",
            Ok(with_entry(wire_entry("retention.ms", None, -1))),
            unknown_server_error_with("id should be positive, id: -1"),
        ),
        (
            "a negative synonym source",
            Ok(with_entry(DescribeConfigsResourceResult {
                synonyms: vec![DescribeConfigsSynonym {
                    source: -2,
                    ..Default::default()
                }],
                ..wire_entry("retention.ms", None, 1)
            })),
            unknown_server_error_with("id should be positive, id: -2"),
        ),
        (
            "a negative type",
            Ok(with_entry(DescribeConfigsResourceResult {
                config_type: -3,
                ..wire_entry("retention.ms", None, 1)
            })),
            unknown_server_error_with("id should be positive, id: -3"),
        ),
    ] {
        let every = BTreeMap::from([
            (orders.clone(), Err(expected.clone())),
            (broker.clone(), Err(expected)),
        ]);
        assert2::assert!(describe_results(&asked, answer) == every, "case {name}");
    }
}

// ── mock cluster ─────────────────────────────────────────────────────────

/// What a broker of the mock cluster received.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Received {
    /// The broker id, the version and the request.
    Describe(i32, i16, DescribeConfigsRequest),
    /// The broker id and the request.
    Alter(i32, IncrementalAlterConfigsRequest),
}

type DescribeScript = fn(i32, &DescribeConfigsRequest) -> DescribeConfigsResponse;
type AlterScript =
    fn(i32, usize, &IncrementalAlterConfigsRequest) -> IncrementalAlterConfigsResponse;

/// How the brokers of the mock cluster answer. `alter` gets the broker id,
/// the number of earlier `IncrementalAlterConfigs` requests to the broker,
/// and the request.
#[derive(Clone, Copy)]
struct Script {
    describe: DescribeScript,
    alter: AlterScript,
}

/// The config that broker `broker_id` of the mock cluster describes for
/// every resource.
fn answered_by(broker_id: i32) -> Config {
    config(vec![entry(
        "answered.by",
        Some(&format!("broker-{broker_id}")),
        ConfigSource::DefaultConfig,
    )])
}

fn echo_describe(broker_id: i32, request: &DescribeConfigsRequest) -> DescribeConfigsResponse {
    DescribeConfigsResponse {
        results: request
            .resources
            .iter()
            .map(|resource| DescribeConfigsResult {
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.clone(),
                configs: vec![wire_entry(
                    "answered.by",
                    Some(&format!("broker-{broker_id}")),
                    ConfigSource::DefaultConfig.id(),
                )],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// Answers each resource with `code`, and a topic named `bad` with
/// `INVALID_CONFIG` (40).
fn alter_answer(
    request: &IncrementalAlterConfigsRequest,
    code: i16,
) -> IncrementalAlterConfigsResponse {
    IncrementalAlterConfigsResponse {
        responses: request
            .resources
            .iter()
            .map(|resource| {
                let bad = resource.resource_name == "bad";
                AlterConfigsResourceResponse {
                    error_code: if bad { 40 } else { code },
                    error_message: bad.then(|| "bad value".to_owned()),
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name.clone(),
                    ..Default::default()
                }
            })
            .collect(),
        ..Default::default()
    }
}

const ECHO: Script = Script {
    describe: echo_describe,
    alter: |_, _, request| alter_answer(request, 0),
};

/// A running cluster of brokers 1 and 2. Broker 1 is the bootstrap broker
/// and the controller.
struct Cluster {
    brokers: Vec<MockBroker>,
    received: Arc<Mutex<Vec<Received>>>,
    admin: AdminClient,
}

impl Cluster {
    /// Start the cluster. The metadata names broker 2 at an address that
    /// refuses connections when `broker_2_down`. Each broker advertises
    /// `DescribeConfigs` up to `describe_max`.
    async fn start(
        script: Script,
        broker_2_down: bool,
        describe_max: i16,
        timeout: krabka_units::Time,
    ) -> Self {
        let addresses = Arc::new(Mutex::new(Vec::new()));
        let received = Arc::new(Mutex::new(Vec::new()));
        let mut brokers = Vec::new();
        for broker_id in [1, 2] {
            brokers.push(
                cluster_broker(
                    broker_id,
                    script,
                    describe_max,
                    Arc::clone(&addresses),
                    Arc::clone(&received),
                )
                .await,
            );
        }
        let broker_2 = if broker_2_down {
            refused_address().await
        } else {
            brokers[1].addr
        };
        *addresses.lock().expect("addresses lock") = vec![(1, brokers[0].addr), (2, broker_2)];
        let admin = fast_admin(brokers[0].addr, timeout).await;
        Self {
            brokers,
            received,
            admin,
        }
    }

    /// Stop the brokers and return what they received, by broker id and then
    /// in the order each broker got it.
    fn stop(self) -> Vec<Received> {
        for broker in self.brokers {
            broker.stop();
        }
        let mut received = self.received.lock().expect("received lock").clone();
        received.sort_by_key(|entry| match entry {
            Received::Describe(id, ..) | Received::Alter(id, _) => *id,
        });
        received
    }
}

async fn cluster_broker(
    broker_id: i32,
    script: Script,
    describe_max: i16,
    addresses: Arc<Mutex<Vec<(i32, SocketAddr)>>>,
    received: Arc<Mutex<Vec<Received>>>,
) -> MockBroker {
    MockBroker::start_with_replies(move |api_key, version, _, body| {
        let reply = match api_key {
            api_versions_request::API_KEY => api_versions(&[
                (describe_configs_request::API_KEY, 1, describe_max),
                (incremental_alter_configs_request::API_KEY, 0, 1),
            ]),
            metadata_request::API_KEY => encode_response(
                &MetadataResponse {
                    controller_id: 1,
                    brokers: addresses
                        .lock()
                        .expect("addresses lock")
                        .iter()
                        .map(|(node_id, addr)| MetadataResponseBroker {
                            node_id: *node_id,
                            host: addr.ip().to_string(),
                            port: i32::from(addr.port()),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                version,
                true,
            ),
            describe_configs_request::API_KEY => {
                let flexible = version >= describe_configs_request::FLEXIBLE_MIN;
                let request: DescribeConfigsRequest = decode_request(body, version, flexible);
                let answer = (script.describe)(broker_id, &request);
                received
                    .lock()
                    .expect("received lock")
                    .push(Received::Describe(broker_id, version, request));
                encode_response(&answer, version, flexible)
            }
            incremental_alter_configs_request::API_KEY => {
                let flexible = version >= incremental_alter_configs_request::FLEXIBLE_MIN;
                let request: IncrementalAlterConfigsRequest =
                    decode_request(body, version, flexible);
                let mut received = received.lock().expect("received lock");
                let earlier = received
                    .iter()
                    .filter(|entry| matches!(entry, Received::Alter(id, _) if *id == broker_id))
                    .count();
                let answer = (script.alter)(broker_id, earlier, &request);
                received.push(Received::Alter(broker_id, request));
                encode_response(&answer, version, flexible)
            }
            _ => return MockReply::Silent,
        };
        MockReply::Respond(reply)
    })
    .await
}

fn describe_wire(
    resources: &[ConfigResource],
    include_synonyms: bool,
    include_documentation: bool,
) -> DescribeConfigsRequest {
    describe_request(
        &resources.iter().cloned().collect(),
        DescribeConfigsOptions {
            include_synonyms,
            include_documentation,
        },
    )
}

fn codes<T>(
    results: BTreeMap<ConfigResource, Result<T, KafkaError>>,
) -> BTreeMap<ConfigResource, Result<T, i16>> {
    results
        .into_iter()
        .map(|(resource, result)| (resource, result.map_err(|error| error.code)))
        .collect()
}

/// Kafka's `describeConfigs` sends one request to each broker that a named
/// `BROKER` or a `BROKER_LOGGER` resource names, and one with every other
/// resource to the least-loaded broker, with the options of the call. It
/// needs v3 for `include_documentation`, and a broker that it cannot reach
/// fails only its own resources.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_configs_routes_each_resource_as_kafka_does() {
    let unified = [
        ConfigResource::topic("orders"),
        ConfigResource::broker_default(),
        ConfigResource::client_metrics("all-metrics"),
        ConfigResource::group("billing"),
    ];
    let on_broker_2 = [ConfigResource::broker(2), ConfigResource::broker_logger(2)];
    let every_answered = every_type()
        .into_iter()
        .map(|resource| {
            let broker_id = if on_broker_2.contains(&resource) {
                2
            } else {
                1
            };
            (resource, Ok(answered_by(broker_id)))
        })
        .collect::<BTreeMap<_, _>>();
    let orders = ConfigResource::topic("orders");
    let broker_2 = ConfigResource::broker(2);
    let both = vec![orders.clone(), broker_2.clone()];
    let synonyms = DescribeConfigsOptions {
        include_synonyms: true,
        include_documentation: false,
    };
    let documentation = DescribeConfigsOptions {
        include_synonyms: false,
        include_documentation: true,
    };
    let both_options = DescribeConfigsOptions {
        include_synonyms: true,
        include_documentation: true,
    };
    for (name, broker_2_down, describe_max, resources, options, expected_requests, expected) in [
        (
            "every type",
            false,
            4,
            every_type(),
            DescribeConfigsOptions::default(),
            vec![
                Received::Describe(1, 4, describe_wire(&unified, false, false)),
                Received::Describe(2, 4, describe_wire(&on_broker_2, false, false)),
            ],
            every_answered,
        ),
        (
            "synonyms and documentation",
            false,
            4,
            both.clone(),
            both_options,
            vec![
                Received::Describe(
                    1,
                    4,
                    describe_wire(std::slice::from_ref(&orders), true, true),
                ),
                Received::Describe(
                    2,
                    4,
                    describe_wire(std::slice::from_ref(&broker_2), true, true),
                ),
            ],
            BTreeMap::from([
                (orders.clone(), Ok(answered_by(1))),
                (broker_2.clone(), Ok(answered_by(2))),
            ]),
        ),
        (
            "synonyms go at v1",
            false,
            1,
            vec![orders.clone()],
            synonyms,
            vec![Received::Describe(
                1,
                1,
                describe_wire(std::slice::from_ref(&orders), true, false),
            )],
            // v1 carries no type or documentation: each reads as its schema
            // default, `UNKNOWN` and "", as in Kafka.
            BTreeMap::from([(
                orders.clone(),
                Ok(config(vec![ConfigEntry {
                    config_type: ConfigType::Unknown,
                    documentation: Some(String::new()),
                    ..entry("answered.by", Some("broker-1"), ConfigSource::DefaultConfig)
                }])),
            )]),
        ),
        (
            "documentation needs v3",
            false,
            2,
            both.clone(),
            documentation,
            vec![],
            BTreeMap::from([(orders.clone(), Err(35)), (broker_2.clone(), Err(35))]),
        ),
        (
            "an unreachable broker fails its own resources",
            true,
            4,
            both.clone(),
            DescribeConfigsOptions::default(),
            vec![Received::Describe(
                1,
                4,
                describe_wire(std::slice::from_ref(&orders), false, false),
            )],
            BTreeMap::from([
                (orders.clone(), Ok(answered_by(1))),
                (broker_2.clone(), Err(7)),
            ]),
        ),
    ] {
        let timeout = if broker_2_down { SHORT } else { LONG };
        let cluster = Cluster::start(ECHO, broker_2_down, describe_max, timeout).await;
        let result = cluster
            .admin
            .describe_configs(&resources, options)
            .await
            .expect("describe_configs");
        let received = cluster.stop();
        assert2::assert!(
            (codes(result), received) == (expected, expected_requests),
            "case {name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_configs_refuses_a_broker_name_that_is_not_an_id() {
    let cluster = Cluster::start(ECHO, false, 4, LONG).await;
    let result = cluster
        .admin
        .describe_configs(
            &[
                ConfigResource::topic("orders"),
                ConfigResource::new(ConfigResourceType::Broker, "one"),
            ],
            DescribeConfigsOptions::default(),
        )
        .await;
    let received = cluster.stop();
    assert2::assert!(let Err(AdminError::InvalidArgument(_)) = result);
    assert2::assert!(received == vec![]);
}

fn alter_wire(
    resources: &[(&ConfigResource, &[AlterConfigOp])],
    validate_only: bool,
) -> IncrementalAlterConfigsRequest {
    alter_request(
        &resources
            .iter()
            .map(|(resource, ops)| ((*resource).clone(), *ops))
            .collect(),
        IncrementalAlterConfigsOptions { validate_only },
    )
}

/// Kafka's `incrementalAlterConfigs` sends a named `BROKER` resource and
/// each `BROKER_LOGGER` resource to its broker in a request of its own, and
/// the other resources in one request to the least-loaded broker, with each
/// operation and `validate_only`. Each resource gets its own error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_alter_configs_routes_each_resource_as_kafka_does() {
    let orders = ConfigResource::topic("orders");
    let bad = ConfigResource::topic("bad");
    let broker_2 = ConfigResource::broker(2);
    let default = ConfigResource::broker_default();
    let logger_2 = ConfigResource::broker_logger(2);
    let topic_ops = vec![
        AlterConfigOp::set("retention.ms", "60000"),
        AlterConfigOp::delete("segment.bytes"),
        AlterConfigOp::append("cleanup.policy", "compact"),
        AlterConfigOp::subtract("cleanup.policy", "delete"),
    ];
    let broker_ops = vec![AlterConfigOp::set("log.cleaner.threads", "2")];
    let logger_ops = vec![AlterConfigOp::set("kafka.server", "DEBUG")];
    let configs = BTreeMap::from([
        (orders.clone(), topic_ops.clone()),
        (bad.clone(), broker_ops.clone()),
        (broker_2.clone(), broker_ops.clone()),
        (default.clone(), broker_ops.clone()),
        (logger_2.clone(), logger_ops.clone()),
    ]);
    let expected = BTreeMap::from([
        (orders.clone(), Ok(())),
        (
            bad.clone(),
            Err(KafkaError {
                code: 40,
                name: "INVALID_CONFIG",
                message: Some("bad value".into()),
            }),
        ),
        (broker_2.clone(), Ok(())),
        (default.clone(), Ok(())),
        (logger_2.clone(), Ok(())),
    ]);
    for validate_only in [false, true] {
        let mut cluster = Cluster::start(ECHO, false, 4, LONG).await;
        let result = cluster
            .admin
            .incremental_alter_configs(&configs, IncrementalAlterConfigsOptions { validate_only })
            .await
            .expect("incremental_alter_configs");
        let mut received = cluster.stop();
        // Broker 2 gets its two requests at the same time, in either order.
        received[1..].sort_by_key(|entry| format!("{entry:?}"));
        let mut expected_requests = vec![
            Received::Alter(
                1,
                alter_wire(
                    &[
                        (&bad, &broker_ops),
                        (&orders, &topic_ops),
                        (&default, &broker_ops),
                    ],
                    validate_only,
                ),
            ),
            Received::Alter(2, alter_wire(&[(&broker_2, &broker_ops)], validate_only)),
            Received::Alter(2, alter_wire(&[(&logger_2, &logger_ops)], validate_only)),
        ];
        expected_requests[1..].sort_by_key(|entry| format!("{entry:?}"));
        assert2::assert!(
            (result, received) == (expected.clone(), expected_requests),
            "validate_only {validate_only}"
        );
    }
}

/// Kafka's `handleNotControllerError` retries a `NOT_CONTROLLER` answer on
/// the least-loaded broker and on a named broker until it is clean, a
/// missing result is `UNKNOWN_SERVER_ERROR`, and a broker that it cannot
/// reach fails only its own resource.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_alter_configs_retries_not_controller_and_isolates_failures() {
    let orders = ConfigResource::topic("orders");
    let broker_2 = ConfigResource::broker(2);
    let ops = vec![AlterConfigOp::set("retention.ms", "60000")];
    let configs = BTreeMap::from([
        (orders.clone(), ops.clone()),
        (broker_2.clone(), ops.clone()),
    ]);
    let to_1 = Received::Alter(1, alter_wire(&[(&orders, &ops)], false));
    let to_2 = Received::Alter(2, alter_wire(&[(&broker_2, &ops)], false));
    let not_controller_once = Script {
        describe: echo_describe,
        alter: |_, earlier, request| {
            alter_answer(request, if earlier == 0 { NOT_CONTROLLER } else { 0 })
        },
    };
    let no_results = Script {
        describe: echo_describe,
        alter: |_, _, _| IncrementalAlterConfigsResponse::default(),
    };
    for (name, script, broker_2_down, expected_requests, expected) in [
        (
            "not controller once",
            not_controller_once,
            false,
            vec![to_1.clone(), to_1.clone(), to_2.clone(), to_2.clone()],
            BTreeMap::from([(orders.clone(), Ok(())), (broker_2.clone(), Ok(()))]),
        ),
        (
            "no result",
            no_results,
            false,
            vec![to_1.clone(), to_2.clone()],
            BTreeMap::from([(orders.clone(), Err(-1)), (broker_2.clone(), Err(-1))]),
        ),
        (
            "an unreachable broker",
            ECHO,
            true,
            vec![to_1.clone()],
            BTreeMap::from([(orders.clone(), Ok(())), (broker_2.clone(), Err(7))]),
        ),
    ] {
        let timeout = if broker_2_down { SHORT } else { LONG };
        let mut cluster = Cluster::start(script, broker_2_down, 4, timeout).await;
        let result = cluster
            .admin
            .incremental_alter_configs(&configs, IncrementalAlterConfigsOptions::default())
            .await
            .expect("incremental_alter_configs");
        let received = cluster.stop();
        assert2::assert!(
            (codes(result), received) == (expected, expected_requests),
            "case {name}"
        );
    }
}

/// With a controller bootstrap (KIP-919), Kafka's `incrementalAlterConfigs`
/// sends a named `BROKER` resource with the others to the active controller,
/// and only a `BROKER_LOGGER` resource to its node, which it finds among the
/// controllers that `DescribeCluster` names. A `NOT_LEADER_OR_FOLLOWER`
/// answer then means a follower controller and is retried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_alter_configs_through_controllers_routes_as_kafka_does() {
    use krabka_protocol::owned::{
        describe_cluster_request,
        describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
    };

    let received = Arc::new(Mutex::new(Vec::new()));
    let handler_received = Arc::clone(&received);
    let port = Arc::new(std::sync::atomic::AtomicU16::new(0));
    let handler_port = Arc::clone(&port);
    let controller = MockBroker::start_with_replies(move |api_key, version, _, body| {
        let own_port = i32::from(handler_port.load(std::sync::atomic::Ordering::SeqCst));
        let reply = match api_key {
            api_versions_request::API_KEY => api_versions(&[
                (describe_cluster_request::API_KEY, 0, 2),
                (incremental_alter_configs_request::API_KEY, 0, 1),
            ]),
            describe_cluster_request::API_KEY => encode_response(
                &DescribeClusterResponse {
                    endpoint_type: 2,
                    controller_id: 3000,
                    brokers: vec![DescribeClusterBroker {
                        broker_id: 3000,
                        host: "127.0.0.1".into(),
                        port: own_port,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                version,
                true,
            ),
            incremental_alter_configs_request::API_KEY => {
                let flexible = version >= incremental_alter_configs_request::FLEXIBLE_MIN;
                let request: IncrementalAlterConfigsRequest =
                    decode_request(body, version, flexible);
                let mut received = handler_received.lock().expect("received lock");
                let code = if received.is_empty() {
                    NOT_LEADER_OR_FOLLOWER
                } else {
                    0
                };
                let answer = alter_answer(&request, code);
                received.push(request);
                encode_response(&answer, version, flexible)
            }
            _ => return MockReply::Silent,
        };
        MockReply::Respond(reply)
    })
    .await;
    port.store(controller.addr.port(), std::sync::atomic::Ordering::SeqCst);
    let mut admin = AdminClient::connect_controller_with_config(
        &[controller.addr.to_string()],
        crate::AdminClientConfig {
            request_timeout: LONG,
            retry_backoff: krabka_units::millis(1),
            retry_backoff_max: krabka_units::millis(1),
            default_api_timeout: Some(LONG),
            ..crate::AdminClientConfig::default()
        },
    )
    .await
    .expect("admin connects");

    let broker = ConfigResource::broker(1);
    let logger = ConfigResource::broker_logger(3000);
    let ops = vec![AlterConfigOp::set("log.cleaner.threads", "2")];
    let configs = BTreeMap::from([(broker.clone(), ops.clone()), (logger.clone(), ops.clone())]);
    let result = admin
        .incremental_alter_configs(&configs, IncrementalAlterConfigsOptions::default())
        .await
        .expect("incremental_alter_configs");
    controller.stop();

    let requests = received.lock().expect("received lock").clone();
    // The logger request goes first, gets NOT_LEADER_OR_FOLLOWER and goes
    // again; the broker request then goes to the controller.
    assert2::assert!(
        (result, requests)
            == (
                BTreeMap::from([(broker.clone(), Ok(())), (logger.clone(), Ok(()))]),
                vec![
                    alter_wire(&[(&logger, &ops)], false),
                    alter_wire(&[(&logger, &ops)], false),
                    alter_wire(&[(&broker, &ops)], false),
                ],
            )
    );
}
