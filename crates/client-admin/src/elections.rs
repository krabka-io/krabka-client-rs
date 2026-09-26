//! Leader elections (`ElectLeaders`, KIP-183 and KIP-460), as Kafka's
//! `Admin.electLeaders`.

use std::collections::BTreeMap;

use krabka_protocol::owned::{
    elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
    elect_leaders_response::ElectLeadersResponse,
};

use crate::{
    AdminClient, AdminError, KafkaError, NOT_CONTROLLER, kafka_error_name, retry::ControllerRetry,
};

/// Which replica an election makes the leader, as Kafka's `ElectionType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionType {
    /// The preferred replica, the first of the assignment, when it is in
    /// sync (KIP-183).
    Preferred,
    /// Any live replica, even one that is out of sync, when no in-sync
    /// replica is live (KIP-460). Needs `ElectLeaders` v1.
    Unclean,
}

impl ElectionType {
    const fn wire(self) -> i8 {
        match self {
            Self::Preferred => 0,
            Self::Unclean => 1,
        }
    }
}

/// The election result of each `(topic, partition)`, as Kafka's
/// `ElectLeadersResult.partitions`: `Ok` for an elected leader, or the
/// partition's error.
pub type ElectionResults = BTreeMap<(String, i32), Result<(), KafkaError>>;

/// The `ElectLeaders` request of `partitions`, grouped by topic, as Kafka's
/// `ElectLeadersRequest.Builder` builds it. `None` asks for every partition
/// of the cluster (a null `topic_partitions`).
fn elect_leaders_request(
    election_type: ElectionType,
    partitions: Option<&[(String, i32)]>,
    timeout_ms: i32,
) -> ElectLeadersRequest {
    let topic_partitions = partitions.map(|partitions| {
        let mut by_topic = BTreeMap::<&str, Vec<i32>>::new();
        for (topic, partition) in partitions {
            let entry = by_topic.entry(topic.as_str()).or_default();
            if !entry.contains(partition) {
                entry.push(*partition);
            }
        }
        by_topic
            .into_iter()
            .map(|(topic, partitions)| TopicPartitions {
                topic: topic.to_owned(),
                partitions,
                ..Default::default()
            })
            .collect()
    });
    ElectLeadersRequest {
        election_type: election_type.wire(),
        topic_partitions,
        timeout_ms,
        ..Default::default()
    }
}

/// Whether Kafka's `handleNotControllerError` finds the controller again
/// after this answer: its top-level code or a partition code is
/// `NOT_CONTROLLER`.
fn names_not_controller(response: &ElectLeadersResponse) -> bool {
    response.error_code == NOT_CONTROLLER
        || response
            .replica_election_results
            .iter()
            .flat_map(|topic| &topic.partition_result)
            .any(|partition| partition.error_code == NOT_CONTROLLER)
}

/// Maps an `ElectLeaders` answer to the result of each partition, as Kafka's
/// `ElectLeadersResponse.electLeadersResult` does. A top-level error fails
/// the whole call.
fn elect_leaders_results(response: ElectLeadersResponse) -> Result<ElectionResults, AdminError> {
    if response.error_code != 0 {
        return Err(AdminError::Broker {
            api: "ElectLeaders",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: None,
        });
    }
    Ok(response
        .replica_election_results
        .into_iter()
        .flat_map(|topic| {
            let name = topic.topic;
            topic.partition_result.into_iter().map(move |partition| {
                let result = if partition.error_code == 0 {
                    Ok(())
                } else {
                    Err(KafkaError {
                        code: partition.error_code,
                        name: kafka_error_name(partition.error_code),
                        message: partition.error_message,
                    })
                };
                ((name.clone(), partition.partition_id), result)
            })
        })
        .collect())
}

impl AdminClient {
    /// Elects a leader for each partition of `partitions`, or of every
    /// partition of the cluster for `None`, as Kafka's `electLeaders`
    /// operation does.
    ///
    /// The client sends `ElectLeaders` to the controller connection, as
    /// Kafka's `KafkaAdminClient` sends it with a `ControllerNodeProvider`,
    /// with the time left before the call deadline as its `timeout_ms`.
    /// [`ElectionType::Unclean`] needs `ElectLeaders` v1, so the client
    /// negotiates v1 or higher for it, as Kafka's builder refuses v0.
    ///
    /// `NOT_CONTROLLER` (41), top-level or on any partition, makes the
    /// client find the controller again and resend the request with the
    /// backoff, until Kafka's default `default.api.timeout.ms` (60 s).
    ///
    /// The result has an entry for each partition that the controller
    /// answered for. A partition that needed no election gets
    /// `ELECTION_NOT_NEEDED` (84), as Kafka gives an
    /// `ElectionNotNeededException`.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Broker`] for a top-level error, such as
    /// `CLUSTER_AUTHORIZATION_FAILED` (31), and with `REQUEST_TIMED_OUT` (7)
    /// at the deadline. Returns [`AdminError::Transport`] when the
    /// connection fails, or when the controller does not support the
    /// `ElectLeaders` version that `election_type` needs.
    pub async fn elect_leaders(
        &mut self,
        election_type: ElectionType,
        partitions: Option<&[(String, i32)]>,
    ) -> Result<ElectionResults, AdminError> {
        let mut retry = ControllerRetry::new("ElectLeaders", self.retry);
        loop {
            let request =
                elect_leaders_request(election_type, partitions, retry.remaining_millis());
            let response = match election_type {
                ElectionType::Preferred => retry.bounded(self.conn.send(request)).await?,
                ElectionType::Unclean => retry.bounded(self.conn.send_at_least(request, 1)).await?,
            };
            if !names_not_controller(&response) {
                return elect_leaders_results(response);
            }
            retry.after_not_controller(self).await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU16, Ordering},
    };

    use krabka_client_core::MockBroker;
    use krabka_protocol::owned::{
        api_versions_request, elect_leaders_request,
        elect_leaders_response::{PartitionResult, ReplicaElectionResult},
        metadata_request,
    };

    use super::*;
    use crate::partition_leaders::test_support::{
        api_versions, decode_request, encode_response, fast_admin, orders_metadata,
    };

    fn orders(partitions: &[i32]) -> Vec<(String, i32)> {
        partitions
            .iter()
            .map(|partition| ("orders".to_owned(), *partition))
            .collect()
    }

    #[test]
    fn request_groups_partitions_by_topic_or_asks_for_all() {
        let keys = vec![
            ("orders".to_owned(), 1),
            ("audit".to_owned(), 0),
            ("orders".to_owned(), 0),
            ("orders".to_owned(), 1),
        ];
        for (name, election_type, partitions, expected) in [
            (
                "preferred for listed partitions",
                ElectionType::Preferred,
                Some(keys.as_slice()),
                ElectLeadersRequest {
                    election_type: 0,
                    topic_partitions: Some(vec![
                        TopicPartitions {
                            topic: "audit".to_owned(),
                            partitions: vec![0],
                            ..Default::default()
                        },
                        TopicPartitions {
                            topic: "orders".to_owned(),
                            partitions: vec![1, 0],
                            ..Default::default()
                        },
                    ]),
                    timeout_ms: 30_000,
                    ..Default::default()
                },
            ),
            (
                "unclean for every partition",
                ElectionType::Unclean,
                None,
                ElectLeadersRequest {
                    election_type: 1,
                    topic_partitions: None,
                    timeout_ms: 30_000,
                    ..Default::default()
                },
            ),
        ] {
            assert2::assert!(
                elect_leaders_request(election_type, partitions, 30_000) == expected,
                "case {name}"
            );
        }
    }

    /// An `ElectLeaders` answer with `top_code` and `code` on `orders`-0.
    fn election_answer(top_code: i16, code: i16) -> ElectLeadersResponse {
        ElectLeadersResponse {
            error_code: top_code,
            replica_election_results: vec![ReplicaElectionResult {
                topic: "orders".to_owned(),
                partition_result: vec![PartitionResult {
                    partition_id: 0,
                    error_code: code,
                    error_message: (code != 0).then(|| "no election".to_owned()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Kafka's `electLeaders` maps each partition code to its own result,
    /// fails the call on a top-level code, and finds the controller again
    /// after `NOT_CONTROLLER`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn elect_leaders_retries_not_controller_and_maps_each_partition() {
        let not_needed = Err(KafkaError {
            code: 84,
            name: "ELECTION_NOT_NEEDED",
            message: Some("no election".to_owned()),
        });
        for (name, election_type, answers, expected) in [
            (
                "success",
                ElectionType::Preferred,
                vec![(0, 0)],
                (Ok(BTreeMap::from([(("orders".to_owned(), 0), Ok(()))])), 1),
            ),
            (
                "election not needed is a partition result",
                ElectionType::Unclean,
                vec![(0, 84)],
                (
                    Ok(BTreeMap::from([(("orders".to_owned(), 0), not_needed)])),
                    1,
                ),
            ),
            (
                "not controller finds the controller again",
                ElectionType::Preferred,
                vec![(41, 0), (0, NOT_CONTROLLER), (0, 0)],
                (Ok(BTreeMap::from([(("orders".to_owned(), 0), Ok(()))])), 3),
            ),
            (
                "cluster authorization failed is final",
                ElectionType::Preferred,
                vec![(31, 0)],
                (Err(("ElectLeaders", 31)), 1),
            ),
        ] {
            let requests = Arc::new(Mutex::new(Vec::<(i16, ElectLeadersRequest)>::new()));
            let handler_requests = Arc::clone(&requests);
            let port = Arc::new(AtomicU16::new(0));
            let handler_port = Arc::clone(&port);
            let broker = MockBroker::start(move |api_key, version, _, body| {
                let own = std::net::SocketAddr::from((
                    [127, 0, 0, 1],
                    handler_port.load(Ordering::SeqCst),
                ));
                match api_key {
                    api_versions_request::API_KEY => {
                        Some(api_versions(&[(elect_leaders_request::API_KEY, 0, 2)]))
                    }
                    metadata_request::API_KEY => {
                        Some(encode_response(&orders_metadata(own), version, true))
                    }
                    elect_leaders_request::API_KEY => {
                        let flexible = version >= elect_leaders_request::FLEXIBLE_MIN;
                        let mut requests = handler_requests.lock().expect("requests lock");
                        let (top, code) = answers[requests.len().min(answers.len() - 1)];
                        requests.push((version, decode_request(body, version, flexible)));
                        Some(encode_response(
                            &election_answer(top, code),
                            version,
                            flexible,
                        ))
                    }
                    _ => None,
                }
            })
            .await;
            port.store(broker.addr.port(), Ordering::SeqCst);
            let mut admin = fast_admin(broker.addr, krabka_units::secs(5)).await;

            let result = admin
                .elect_leaders(election_type, Some(&orders(&[0])))
                .await
                .map_err(|error| match error {
                    AdminError::Broker { api, code, .. } => (api, code),
                    other => panic!("case {name}: unexpected error {other:?}"),
                });

            broker.stop();
            let requests = requests.lock().expect("requests lock");
            let shapes = requests
                .iter()
                .map(|(version, request)| {
                    (
                        *version,
                        ElectLeadersRequest {
                            timeout_ms: 0,
                            ..request.clone()
                        },
                    )
                })
                .collect::<Vec<_>>();
            let expected_shape = (
                2,
                elect_leaders_request(election_type, Some(&orders(&[0])), 0),
            );
            assert2::assert!(
                (result, shapes) == (expected.0, vec![expected_shape; expected.1]),
                "case {name}"
            );
            assert2::assert!(
                requests
                    .iter()
                    .all(|(_, request)| (1..=5_000).contains(&request.timeout_ms)),
                "case {name}: the timeout is the time left of the call"
            );
        }
    }

    /// Kafka's `ElectLeadersRequest.Builder` refuses an unclean election
    /// below v1, so a controller with v0 only fails the call before it sends
    /// anything.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unclean_election_needs_elect_leaders_v1() {
        let requests = Arc::new(Mutex::new(0_usize));
        let handler_requests = Arc::clone(&requests);
        let broker = MockBroker::start(move |api_key, version, _, _| match api_key {
            api_versions_request::API_KEY => {
                Some(api_versions(&[(elect_leaders_request::API_KEY, 0, 0)]))
            }
            elect_leaders_request::API_KEY => {
                *handler_requests.lock().expect("requests lock") += 1;
                Some(encode_response(&election_answer(0, 0), version, false))
            }
            _ => None,
        })
        .await;
        let mut admin = fast_admin(broker.addr, krabka_units::secs(5)).await;

        let result = admin
            .elect_leaders(ElectionType::Unclean, Some(&orders(&[0])))
            .await;

        broker.stop();
        assert2::assert!(matches!(
            result,
            Err(AdminError::Transport(
                krabka_client_core::ClientError::IncompatibleVersion { .. }
            ))
        ));
        assert2::assert!(*requests.lock().expect("requests lock") == 0);
    }
}
