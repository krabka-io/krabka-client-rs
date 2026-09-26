//! Cluster description (`DescribeCluster`, KIP-700), as Kafka's
//! `Admin.describeCluster`.

use std::collections::BTreeSet;

use krabka_protocol::owned::{
    describe_cluster_request::DescribeClusterRequest,
    describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
};

use crate::{
    AdminClient, AdminError, kafka_error_name,
    retry::ControllerRetry,
    users::{AclOperation, wire_to_operation},
};

/// `endpointType` of a `DescribeCluster` request for broker endpoints.
const ENDPOINT_TYPE_BROKERS: i8 = 1;
/// `endpointType` of a `DescribeCluster` request for controller endpoints
/// (KIP-919).
const ENDPOINT_TYPE_CONTROLLERS: i8 = 2;
/// The authorized-operations value of a response that did not compute them
/// (`Integer.MIN_VALUE`).
const AUTHORIZED_OPERATIONS_OMITTED: i32 = i32::MIN;

/// What [`AdminClient::describe_cluster`] asks for, as Kafka's
/// `DescribeClusterOptions`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DescribeClusterOptions {
    /// Ask for the operations that the caller may perform on the cluster.
    pub include_authorized_operations: bool,
    /// List fenced brokers too (KIP-1073). Needs `DescribeCluster` v2.
    pub include_fenced_brokers: bool,
}

/// One node of the cluster, as Kafka's `Node`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNode {
    /// The broker or controller ID.
    pub id: i32,
    /// The advertised host.
    pub host: String,
    /// The advertised port.
    pub port: i32,
    /// The rack, when the node has one.
    pub rack: Option<String>,
    /// Whether the controller has fenced this broker (KIP-1073).
    pub is_fenced: bool,
}

/// The cluster that [`AdminClient::describe_cluster`] found, as Kafka's
/// `DescribeClusterResult`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterDescription {
    /// The cluster ID.
    pub cluster_id: String,
    /// The node that the response names as controller, or `None` when it
    /// names no listed node. A broker names a random live broker, as `KRaft`
    /// clients cannot reach the active controller directly.
    pub controller: Option<ClusterNode>,
    /// Every node of the response.
    pub nodes: Vec<ClusterNode>,
    /// The operations that the caller may perform on the cluster, or `None`
    /// when the caller did not ask for them.
    pub authorized_operations: Option<BTreeSet<AclOperation>>,
}

fn cluster_node(broker: DescribeClusterBroker) -> ClusterNode {
    ClusterNode {
        id: broker.broker_id,
        host: broker.host,
        port: broker.port,
        rack: broker.rack,
        is_fenced: broker.is_fenced,
    }
}

/// The operations of an authorized-operations bit field, as Kafka's
/// `KafkaAdminClient.validAclOperations` reads it: `None` for
/// `Integer.MIN_VALUE`, and without `UNKNOWN`, `ANY` and `ALL`.
fn authorized_operations(bits: i32) -> Option<BTreeSet<AclOperation>> {
    if bits == AUTHORIZED_OPERATIONS_OMITTED {
        return None;
    }
    Some(
        (0_i8..32)
            .filter(|bit| bits & (1_i32 << bit) != 0)
            .filter_map(|bit| wire_to_operation(bit).ok())
            .filter(|operation| *operation != AclOperation::All)
            .collect(),
    )
}

/// Maps a `DescribeCluster` answer to the cluster description, as Kafka's
/// `describeCluster` `handleResponse` does.
fn cluster_description(
    response: DescribeClusterResponse,
) -> Result<ClusterDescription, AdminError> {
    if response.error_code != 0 {
        return Err(AdminError::Broker {
            api: "DescribeCluster",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: response.error_message,
        });
    }
    let nodes = response
        .brokers
        .into_iter()
        .map(cluster_node)
        .collect::<Vec<_>>();
    let controller = nodes
        .iter()
        .find(|node| node.id == response.controller_id)
        .cloned();
    Ok(ClusterDescription {
        cluster_id: response.cluster_id,
        controller,
        nodes,
        authorized_operations: authorized_operations(response.cluster_authorized_operations),
    })
}

impl AdminClient {
    /// Describes the cluster: its ID, its nodes, the node that serves as
    /// controller, and optionally the operations that the caller may perform
    /// on it, as Kafka's `describeCluster` operation does.
    ///
    /// The client sends `DescribeCluster` on its connection. Through broker
    /// bootstrap it asks for broker endpoints; through controller bootstrap
    /// (KIP-919) it asks for controller endpoints with v1 or higher, as
    /// Kafka's `KafkaAdminClient` does. `include_fenced_brokers` needs v2,
    /// as Kafka's builder refuses a lower version. The call stops at Kafka's
    /// default `default.api.timeout.ms` (60 s).
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Broker`] for a top-level error, such as
    /// `CLUSTER_AUTHORIZATION_FAILED` (31), and with `REQUEST_TIMED_OUT` (7)
    /// at the deadline. Returns [`AdminError::Transport`] when the
    /// connection fails, or when the peer does not support the
    /// `DescribeCluster` version that the options need.
    pub async fn describe_cluster(
        &self,
        options: DescribeClusterOptions,
    ) -> Result<ClusterDescription, AdminError> {
        let controllers = self.conn.uses_controller_bootstrap();
        let request = DescribeClusterRequest {
            include_cluster_authorized_operations: options.include_authorized_operations,
            endpoint_type: if controllers {
                ENDPOINT_TYPE_CONTROLLERS
            } else {
                ENDPOINT_TYPE_BROKERS
            },
            include_fenced_brokers: options.include_fenced_brokers,
            ..Default::default()
        };
        let min_version = if options.include_fenced_brokers {
            2
        } else {
            i16::from(controllers)
        };
        let retry = ControllerRetry::new("DescribeCluster", self.retry);
        let response = retry
            .bounded(self.conn.send_at_least(request, min_version))
            .await?;
        cluster_description(response)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use krabka_client_core::MockBroker;
    use krabka_protocol::owned::{api_versions_request, describe_cluster_request};

    use super::*;
    use crate::partition_leaders::test_support::{
        api_versions, decode_request, encode_response, fast_admin,
    };

    #[test]
    fn authorized_operations_skip_all_and_unknown_bits() {
        // Bits 0 (UNKNOWN), 1 (ANY), 2 (ALL), 3 (READ), 13 (CREATE_TOKENS)
        // and 20 (no such operation).
        let bits = 1 | 1 << 1 | 1 << 2 | 1 << 3 | 1 << 13 | 1 << 20;
        for (name, bits, expected) in [
            ("omitted", i32::MIN, None),
            ("none", 0, Some(BTreeSet::new())),
            (
                "some",
                bits,
                Some(BTreeSet::from([
                    AclOperation::Read,
                    AclOperation::CreateTokens,
                ])),
            ),
        ] {
            assert2::assert!(authorized_operations(bits) == expected, "case {name}");
        }
    }

    fn broker(broker_id: i32, rack: Option<&str>, is_fenced: bool) -> DescribeClusterBroker {
        DescribeClusterBroker {
            broker_id,
            host: format!("broker-{broker_id}"),
            port: 9092,
            rack: rack.map(str::to_owned),
            is_fenced,
            ..Default::default()
        }
    }

    fn node(id: i32, rack: Option<&str>, is_fenced: bool) -> ClusterNode {
        ClusterNode {
            id,
            host: format!("broker-{id}"),
            port: 9092,
            rack: rack.map(str::to_owned),
            is_fenced,
        }
    }

    /// Kafka's `describeCluster` asks for broker endpoints, names the
    /// listed node with the controller ID as controller, reads the
    /// authorized operations, and fails on a top-level code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_cluster_maps_the_answer_as_kafka_does() {
        let answer =
            |error_code, controller_id, cluster_authorized_operations| DescribeClusterResponse {
                error_code,
                error_message: (error_code != 0).then(|| "denied".to_owned()),
                cluster_id: "lkc-1".to_owned(),
                controller_id,
                brokers: vec![broker(1, Some("r1"), false), broker(2, None, true)],
                cluster_authorized_operations,
                ..Default::default()
            };
        let nodes = vec![node(1, Some("r1"), false), node(2, None, true)];
        for (name, options, response, expected) in [
            (
                "plain",
                DescribeClusterOptions::default(),
                answer(0, 2, i32::MIN),
                Ok(ClusterDescription {
                    cluster_id: "lkc-1".to_owned(),
                    controller: Some(node(2, None, true)),
                    nodes: nodes.clone(),
                    authorized_operations: None,
                }),
            ),
            (
                "fenced brokers and authorized operations",
                DescribeClusterOptions {
                    include_authorized_operations: true,
                    include_fenced_brokers: true,
                },
                answer(0, 7, 1 << 8),
                Ok(ClusterDescription {
                    cluster_id: "lkc-1".to_owned(),
                    controller: None,
                    nodes,
                    authorized_operations: Some(BTreeSet::from([AclOperation::Describe])),
                }),
            ),
            (
                "cluster authorization failed is final",
                DescribeClusterOptions::default(),
                answer(31, 1, i32::MIN),
                Err(("DescribeCluster", 31, Some("denied".to_owned()))),
            ),
        ] {
            let requests = Arc::new(Mutex::new(Vec::<DescribeClusterRequest>::new()));
            let handler_requests = Arc::clone(&requests);
            let broker = MockBroker::start(move |api_key, version, _, body| match api_key {
                api_versions_request::API_KEY => {
                    Some(api_versions(&[(describe_cluster_request::API_KEY, 0, 2)]))
                }
                describe_cluster_request::API_KEY => {
                    handler_requests
                        .lock()
                        .expect("requests lock")
                        .push(decode_request(body, version, true));
                    Some(encode_response(&response, version, true))
                }
                _ => None,
            })
            .await;
            let admin = fast_admin(broker.addr, krabka_units::secs(5)).await;

            let result = admin
                .describe_cluster(options)
                .await
                .map_err(|error| match error {
                    AdminError::Broker {
                        api, code, message, ..
                    } => (api, code, message),
                    other => panic!("case {name}: unexpected error {other:?}"),
                });

            broker.stop();
            assert2::assert!(
                (result, requests.lock().expect("requests lock").clone())
                    == (
                        expected,
                        vec![DescribeClusterRequest {
                            include_cluster_authorized_operations: options
                                .include_authorized_operations,
                            endpoint_type: ENDPOINT_TYPE_BROKERS,
                            include_fenced_brokers: options.include_fenced_brokers,
                            ..Default::default()
                        }]
                    ),
                "case {name}"
            );
        }
    }

    /// Kafka's `DescribeClusterRequest.Builder` refuses
    /// `includeFencedBrokers` below v2.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fenced_brokers_need_describe_cluster_v2() {
        let broker = MockBroker::start(move |api_key, _, _, _| match api_key {
            api_versions_request::API_KEY => {
                Some(api_versions(&[(describe_cluster_request::API_KEY, 0, 1)]))
            }
            _ => None,
        })
        .await;
        let admin = fast_admin(broker.addr, krabka_units::secs(5)).await;

        let result = admin
            .describe_cluster(DescribeClusterOptions {
                include_fenced_brokers: true,
                ..DescribeClusterOptions::default()
            })
            .await;

        broker.stop();
        assert2::assert!(matches!(
            result,
            Err(AdminError::Transport(
                krabka_client_core::ClientError::IncompatibleVersion { .. }
            ))
        ));
    }
}
