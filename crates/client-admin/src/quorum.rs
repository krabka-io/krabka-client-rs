//! KIP-853 metadata-quorum administration: `DescribeQuorum`, `AddRaftVoter`
//! and `RemoveRaftVoter`, as Kafka's `Admin.describeMetadataQuorum`,
//! `addRaftVoter` and `removeRaftVoter`.

use std::collections::BTreeSet;

use krabka_protocol::{
    owned::{
        add_raft_voter_request::{AddRaftVoterRequest, Listener},
        describe_quorum_request::{
            DescribeQuorumRequest, PartitionData as RequestPartition, TopicData as RequestTopic,
        },
        remove_raft_voter_request::RemoveRaftVoterRequest,
    },
    primitives::uuid::Uuid as ProtoUuid,
};

use crate::{AdminClient, AdminError, NOT_CONTROLLER, kafka_error_name, retry::ControllerRetry};

const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

/// One listener endpoint of a voter, as Kafka's `RaftVoterEndpoint`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RaftVoterEndpoint {
    listener: String,
    host: String,
    port: u16,
}

impl RaftVoterEndpoint {
    /// An endpoint of `listener` at `host:port`.
    ///
    /// # Errors
    /// Returns [`AdminError::InvalidArgument`] when `listener` is empty, has
    /// leading or trailing whitespace, or is not in upper case, as Kafka's
    /// `RaftVoterEndpoint` constructor refuses it.
    pub fn new(
        listener: impl Into<String>,
        host: impl Into<String>,
        port: u16,
    ) -> Result<Self, AdminError> {
        let listener = listener.into();
        if listener.trim() != listener {
            return Err(AdminError::InvalidArgument(format!(
                "listener {listener:?}: leading or trailing whitespace is not allowed"
            )));
        }
        if listener.is_empty() {
            return Err(AdminError::InvalidArgument(
                "listener: empty string is not allowed".to_owned(),
            ));
        }
        if listener.to_uppercase() != listener {
            return Err(AdminError::InvalidArgument(format!(
                "listener {listener:?}: string must be UPPERCASE"
            )));
        }
        Ok(Self {
            listener,
            host: host.into(),
            port,
        })
    }

    /// The listener name.
    #[must_use]
    pub fn listener(&self) -> &str {
        &self.listener
    }

    /// The advertised host.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The advertised port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }
}

/// The `AddRaftVoter` request of one voter, as Kafka's `addRaftVoter`
/// `createRequest` builds it. Each endpoint appears once, as Kafka takes a
/// `Set`. `ack_when_committed` keeps its schema default, `true`, which
/// Kafka's client does not change.
fn add_raft_voter_request(
    cluster_id: Option<&str>,
    voter_id: i32,
    voter_directory_id: uuid::Uuid,
    endpoints: &[RaftVoterEndpoint],
    timeout_ms: i32,
) -> AddRaftVoterRequest {
    let mut seen = BTreeSet::new();
    AddRaftVoterRequest {
        cluster_id: cluster_id.map(str::to_owned),
        timeout_ms,
        voter_id,
        voter_directory_id: ProtoUuid(*voter_directory_id.as_bytes()),
        listeners: endpoints
            .iter()
            .filter(|endpoint| seen.insert(*endpoint))
            .map(|endpoint| Listener {
                name: endpoint.listener.clone(),
                host: endpoint.host.clone(),
                port: endpoint.port,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `Ok` for code 0, and the broker error of `api` for any other code.
fn voter_result(api: &'static str, code: i16, message: Option<String>) -> Result<(), AdminError> {
    if code == 0 {
        Ok(())
    } else {
        Err(broker_error(api, code, message))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumReplica {
    pub node_id: i32,
    pub directory_id: uuid::Uuid,
    pub log_end_offset: i64,
    pub last_fetch_timestamp: i64,
    pub last_caught_up_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataQuorum {
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub high_watermark: i64,
    pub voters: Vec<QuorumReplica>,
    pub observers: Vec<QuorumReplica>,
}

fn broker_error(api: &'static str, code: i16, message: Option<String>) -> AdminError {
    AdminError::Broker {
        api,
        code,
        name: kafka_error_name(code),
        message,
    }
}

fn replica(
    value: &krabka_protocol::owned::common::describe_quorum_response::replica_state::ReplicaState,
) -> QuorumReplica {
    QuorumReplica {
        node_id: value.replica_id,
        directory_id: uuid::Uuid::from_bytes(value.replica_directory_id.0),
        log_end_offset: value.log_end_offset,
        last_fetch_timestamp: value.last_fetch_timestamp,
        last_caught_up_timestamp: value.last_caught_up_timestamp,
    }
}

impl AdminClient {
    /// Return the live `__cluster_metadata` partition-zero quorum view.
    ///
    /// # Errors
    /// Returns a transport, protocol, or Kafka error from `DescribeQuorum`.
    pub async fn describe_metadata_quorum(&mut self) -> Result<MetadataQuorum, AdminError> {
        let response = self
            .conn
            .send(DescribeQuorumRequest {
                topics: vec![RequestTopic {
                    topic_name: CLUSTER_METADATA_TOPIC.into(),
                    partitions: vec![RequestPartition {
                        partition_index: 0,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await?;
        if response.error_code != 0 {
            return Err(broker_error(
                "DescribeQuorum",
                response.error_code,
                response.error_message,
            ));
        }
        let partition = response
            .topics
            .into_iter()
            .find(|topic| topic.topic_name == CLUSTER_METADATA_TOPIC)
            .and_then(|topic| {
                topic
                    .partitions
                    .into_iter()
                    .find(|partition| partition.partition_index == 0)
            })
            .ok_or_else(|| {
                AdminError::Protocol("DescribeQuorum omitted __cluster_metadata partition 0".into())
            })?;
        if partition.error_code != 0 {
            return Err(broker_error(
                "DescribeQuorum",
                partition.error_code,
                partition.error_message,
            ));
        }
        Ok(MetadataQuorum {
            leader_id: partition.leader_id,
            leader_epoch: partition.leader_epoch,
            high_watermark: partition.high_watermark,
            voters: partition.current_voters.iter().map(replica).collect(),
            observers: partition.observers.iter().map(replica).collect(),
        })
    }

    /// Adds a voter to the metadata quorum (KIP-853), as Kafka's
    /// `addRaftVoter` operation does.
    ///
    /// The client sends `AddRaftVoter` on its connection: the active
    /// controller through controller bootstrap (KIP-919), or a broker, which
    /// forwards it, as Kafka's `LeastLoadedBrokerOrActiveKController` picks.
    /// `cluster_id` is Kafka's `AddRaftVoterOptions.clusterId`; `None` sends
    /// a null cluster ID, which the controller does not check. The request
    /// carries the time left before the call deadline as its `timeout_ms`.
    ///
    /// `NOT_CONTROLLER` (41) makes the client find the controller again and
    /// resend the request with the backoff until `default.api.timeout.ms`
    /// (60 s), as Kafka's `handleNotControllerError` does.
    ///
    /// # Errors
    /// Returns [`AdminError::Broker`] with the controller's error, such as
    /// `DUPLICATE_VOTER` (126), and with `REQUEST_TIMED_OUT` (7) at the
    /// deadline, or a transport error.
    pub async fn add_raft_voter(
        &mut self,
        cluster_id: Option<&str>,
        voter_id: i32,
        voter_directory_id: uuid::Uuid,
        endpoints: &[RaftVoterEndpoint],
    ) -> Result<(), AdminError> {
        let mut retry = ControllerRetry::new("AddRaftVoter", self.retry);
        loop {
            let request = add_raft_voter_request(
                cluster_id,
                voter_id,
                voter_directory_id,
                endpoints,
                retry.remaining_millis(),
            );
            let response = retry.bounded(self.conn.send(request)).await?;
            if response.error_code != NOT_CONTROLLER {
                return voter_result("AddRaftVoter", response.error_code, response.error_message);
            }
            retry.after_not_controller(self).await?;
        }
    }

    /// Remove one exact node and directory identity from the metadata quorum,
    /// as Kafka's `removeRaftVoter` operation does.
    ///
    /// `NOT_CONTROLLER` (41) makes the client find the controller again and
    /// resend the request with the backoff until `default.api.timeout.ms`
    /// (60 s), as Kafka's `handleNotControllerError` does.
    ///
    /// # Errors
    /// Returns a transport, protocol, or Kafka error from `RemoveRaftVoter`,
    /// and `REQUEST_TIMED_OUT` (7) at the deadline.
    pub async fn remove_raft_voter(
        &mut self,
        cluster_id: uuid::Uuid,
        node_id: i32,
        directory_id: uuid::Uuid,
    ) -> Result<(), AdminError> {
        let request = RemoveRaftVoterRequest {
            cluster_id: Some(cluster_id.to_string()),
            voter_id: node_id,
            voter_directory_id: ProtoUuid(*directory_id.as_bytes()),
            ..Default::default()
        };
        let mut retry = ControllerRetry::new("RemoveRaftVoter", self.retry);
        loop {
            let response = retry.bounded(self.conn.send(request.clone())).await?;
            if response.error_code != NOT_CONTROLLER {
                return voter_result(
                    "RemoveRaftVoter",
                    response.error_code,
                    response.error_message,
                );
            }
            retry.after_not_controller(self).await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use krabka_client_core::MockReply;
    use krabka_protocol::owned::{
        add_raft_voter_request, add_raft_voter_response::AddRaftVoterResponse,
        remove_raft_voter_request, remove_raft_voter_response::RemoveRaftVoterResponse,
    };

    use super::*;
    use crate::partition_leaders::test_support::{
        decode_request, encode_response, fast_admin, scripted_broker,
    };

    const DIRECTORY: uuid::Uuid = uuid::Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);

    fn endpoint(listener: &str, host: &str, port: u16) -> RaftVoterEndpoint {
        RaftVoterEndpoint::new(listener, host, port).expect("valid endpoint")
    }

    #[test]
    fn endpoint_listener_must_be_trimmed_non_empty_upper_case() {
        for (name, listener, valid) in [
            ("upper case", "CONTROLLER", true),
            ("upper case with digits and symbols", "CONTROLLER_2-X", true),
            ("empty", "", false),
            ("lower case", "controller", false),
            ("leading whitespace", " CONTROLLER", false),
            ("trailing whitespace", "CONTROLLER ", false),
        ] {
            let result = RaftVoterEndpoint::new(listener, "localhost", 9093);
            assert2::assert!(
                matches!(result, Err(AdminError::InvalidArgument(_))) != valid,
                "case {name}"
            );
        }
    }

    #[test]
    fn add_request_names_each_endpoint_once() {
        let endpoints = [
            endpoint("CONTROLLER", "c1", 9093),
            endpoint("CONTROLLER", "c1", 9093),
            endpoint("SSL", "c1", 9094),
        ];
        for (name, cluster_id, expected_cluster) in [
            (
                "with cluster id",
                Some("MkU3OEVBNTcwNTJENDM2Qk"),
                Some("MkU3OEVBNTcwNTJENDM2Qk"),
            ),
            ("without cluster id", None, None),
        ] {
            assert2::assert!(
                add_raft_voter_request(cluster_id, 3, DIRECTORY, &endpoints, 1_000)
                    == AddRaftVoterRequest {
                        cluster_id: expected_cluster.map(str::to_owned),
                        timeout_ms: 1_000,
                        voter_id: 3,
                        voter_directory_id: ProtoUuid(*DIRECTORY.as_bytes()),
                        listeners: vec![
                            Listener {
                                name: "CONTROLLER".to_owned(),
                                host: "c1".to_owned(),
                                port: 9093,
                                ..Default::default()
                            },
                            Listener {
                                name: "SSL".to_owned(),
                                host: "c1".to_owned(),
                                port: 9094,
                                ..Default::default()
                            },
                        ],
                        ack_when_committed: true,
                        ..Default::default()
                    },
                "case {name}"
            );
        }
    }

    /// Kafka's `addRaftVoter` and `removeRaftVoter` resend after
    /// `NOT_CONTROLLER` and fail at once on every other error code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn voter_calls_retry_not_controller_and_stop_on_other_codes() {
        for (name, add, codes, expected) in [
            ("add succeeds", true, vec![0], (Ok(()), 1)),
            (
                "add after not controller",
                true,
                vec![NOT_CONTROLLER, 0],
                (Ok(()), 2),
            ),
            (
                "add duplicate voter is final",
                true,
                vec![126],
                (Err(("AddRaftVoter", 126, Some("dup".to_owned()))), 1),
            ),
            ("remove succeeds", false, vec![0], (Ok(()), 1)),
            (
                "remove after not controller",
                false,
                vec![NOT_CONTROLLER, NOT_CONTROLLER, 0],
                (Ok(()), 3),
            ),
            (
                "remove voter not found is final",
                false,
                vec![127],
                (Err(("RemoveRaftVoter", 127, Some("dup".to_owned()))), 1),
            ),
        ] {
            let requests = Arc::new(Mutex::new(Vec::<(i16, i32)>::new()));
            let handler_requests = Arc::clone(&requests);
            let broker = scripted_broker(
                vec![
                    (add_raft_voter_request::API_KEY, 0, 1),
                    (remove_raft_voter_request::API_KEY, 0, 0),
                ],
                move |api_key, version, body, _| {
                    let mut requests = handler_requests.lock().expect("requests lock");
                    let code = codes[requests.len().min(codes.len() - 1)];
                    let message = (code != 0).then(|| "dup".to_owned());
                    let reply = match api_key {
                        add_raft_voter_request::API_KEY => {
                            let request: AddRaftVoterRequest = decode_request(body, version, true);
                            requests.push((version, request.voter_id));
                            encode_response(
                                &AddRaftVoterResponse {
                                    error_code: code,
                                    error_message: message,
                                    ..Default::default()
                                },
                                version,
                                true,
                            )
                        }
                        remove_raft_voter_request::API_KEY => {
                            let request: RemoveRaftVoterRequest =
                                decode_request(body, version, true);
                            requests.push((version, request.voter_id));
                            encode_response(
                                &RemoveRaftVoterResponse {
                                    error_code: code,
                                    error_message: message,
                                    ..Default::default()
                                },
                                version,
                                true,
                            )
                        }
                        _ => return MockReply::Silent,
                    };
                    MockReply::Respond(reply)
                },
            )
            .await;
            let mut admin = fast_admin(broker.addr, krabka_units::secs(5)).await;

            let result = if add {
                admin
                    .add_raft_voter(None, 3, DIRECTORY, &[endpoint("CONTROLLER", "c3", 9093)])
                    .await
            } else {
                admin
                    .remove_raft_voter(uuid::Uuid::nil(), 3, DIRECTORY)
                    .await
            }
            .map_err(|error| match error {
                AdminError::Broker {
                    api, code, message, ..
                } => (api, code, message),
                other => panic!("case {name}: unexpected error {other:?}"),
            });

            broker.stop();
            let version = i16::from(add);
            assert2::assert!(
                (result, requests.lock().expect("requests lock").clone())
                    == (expected.0, vec![(version, 3); expected.1]),
                "case {name}"
            );
        }
    }
}
