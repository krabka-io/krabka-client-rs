//! Batched group deletion: [`AdminClient::delete_consumer_groups`],
//! [`AdminClient::delete_share_groups`] and
//! [`AdminClient::delete_streams_groups`].
//!
//! Each call finds the coordinator of every group with one batched
//! `FindCoordinator` request and sends one `DeleteGroups` request to each
//! coordinator, as Apache Kafka's `DeleteGroupsHandler` does with its
//! `CoordinatorStrategy`. A request to a coordinator that
//! fails or loses its connection finds the coordinator again.

use std::collections::BTreeMap;

use krabka_protocol::owned::delete_groups_request::DeleteGroupsRequest;

use crate::{
    AdminClient, KafkaError,
    group_coordinators::{GroupOutcome, group_error_outcome},
    group_descriptions::request_error_outcomes,
};

/// The per-group results of a delete call: one entry for each requested
/// group, `Ok(())` for a deleted group.
pub type GroupDeletions = BTreeMap<String, Result<(), KafkaError>>;

impl AdminClient {
    /// Deletes empty consumer groups with `DeleteGroups`, as Apache Kafka's
    /// `deleteConsumerGroups` does with its `DeleteConsumerGroupsHandler`.
    ///
    /// The call finds the coordinator of every group with one batched
    /// `FindCoordinator` request and sends one `DeleteGroups` request to each
    /// coordinator with the groups that it coordinates.
    /// `COORDINATOR_LOAD_IN_PROGRESS` (14) sends a group to the same
    /// coordinator again, and `COORDINATOR_NOT_AVAILABLE` (15) and
    /// `NOT_COORDINATOR` (16) find its coordinator again. A lost
    /// connection finds the coordinator again. The call waits with Kafka's
    /// backoff between rounds and stops at `default.api.timeout.ms` (60 s),
    /// where each pending group gets `REQUEST_TIMED_OUT` (7).
    ///
    /// # Errors
    /// The call itself does not fail. Each group gets its own [`KafkaError`],
    /// such as `NON_EMPTY_GROUP` (68), `GROUP_ID_NOT_FOUND` (69) or
    /// `REQUEST_TIMED_OUT` (7) at the deadline.
    pub async fn delete_consumer_groups(&self, groups: &[&str]) -> GroupDeletions {
        self.delete_groups("deleteConsumerGroups", groups).await
    }

    /// Deletes empty share groups. Apache Kafka's `deleteShareGroups` uses a
    /// `DeleteShareGroupsHandler` that builds the same `DeleteGroups` request
    /// as `deleteConsumerGroups`.
    ///
    /// # Errors
    /// See [`AdminClient::delete_consumer_groups`].
    pub async fn delete_share_groups(&self, groups: &[&str]) -> GroupDeletions {
        self.delete_groups("deleteShareGroups", groups).await
    }

    /// Deletes empty streams groups. Apache Kafka's `deleteStreamsGroups`
    /// delegates to `deleteConsumerGroups`.
    ///
    /// # Errors
    /// See [`AdminClient::delete_consumer_groups`].
    pub async fn delete_streams_groups(&self, groups: &[&str]) -> GroupDeletions {
        self.delete_consumer_groups(groups).await
    }

    /// The `DeleteGroups` call of Kafka's `DeleteGroupsHandler`.
    async fn delete_groups(&self, api: &'static str, groups: &[&str]) -> GroupDeletions {
        self.call_group_coordinators(api, groups, |coordinator, keys| async move {
            let connection = self.connect_coordinator(&coordinator).await?;
            let request = DeleteGroupsRequest {
                groups_names: keys.clone(),
                ..Default::default()
            };
            Ok(match connection.send(request).await {
                Ok(response) => response
                    .results
                    .into_iter()
                    .map(|result| {
                        let outcome = match result.error_code {
                            0 => GroupOutcome::Done(Ok(())),
                            code => group_error_outcome(code, None, &[]),
                        };
                        (result.group_id, outcome)
                    })
                    .collect(),
                Err(error) => request_error_outcomes(&keys, &error.into()),
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;
    use krabka_client_core::MockReply;
    use krabka_protocol::owned::{
        delete_groups_request,
        delete_groups_response::{DeletableGroupResult, DeleteGroupsResponse},
    };

    use super::*;
    use crate::{
        group_coordinators::{
            kafka_error,
            test_support::{Lookup, group_cluster},
        },
        partition_leaders::test_support::{decode_request, encode_response},
    };

    /// Kafka's `DeleteGroupsHandler` batches the groups of one coordinator,
    /// retries 14 on the same coordinator, finds the coordinator again on 15
    /// and 16, and fails every other code. A `FindCoordinator` error that
    /// Kafka does not retry fails the group without a `DeleteGroups` request.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_groups_batches_and_retries_as_kafka_does() {
        #[derive(Clone, Copy, Debug)]
        enum Call {
            Consumer,
            Share,
            Streams,
        }
        for call in [Call::Consumer, Call::Share, Call::Streams] {
            // "moved" is on broker 1 first, and then on broker 0.
            let codes = Arc::new(Mutex::new(BTreeMap::from([
                ("done", vec![0]),
                ("loading", vec![14, 0]),
                ("moved", vec![16, 0]),
                ("busy", vec![68]),
            ])));
            let cluster = group_cluster(
                2,
                vec![(delete_groups_request::API_KEY, 0, 2)],
                |group, lookups| match (group, lookups) {
                    ("denied", _) => Lookup::Error(30),
                    ("moved", 0) => Lookup::At(1),
                    _ => Lookup::At(0),
                },
                move |_, api_key, version, body, _| {
                    if api_key != delete_groups_request::API_KEY {
                        return MockReply::Silent;
                    }
                    let request: DeleteGroupsRequest = decode_request(body, version, version >= 2);
                    let mut codes = codes.lock().expect("codes lock");
                    let results = request
                        .groups_names
                        .iter()
                        .map(|group| {
                            let script = codes.get_mut(group.as_str()).expect("scripted group");
                            let error_code = if script.len() > 1 {
                                script.remove(0)
                            } else {
                                script[0]
                            };
                            DeletableGroupResult {
                                group_id: group.clone(),
                                error_code,
                                ..Default::default()
                            }
                        })
                        .collect();
                    MockReply::Respond(encode_response(
                        &DeleteGroupsResponse {
                            results,
                            ..Default::default()
                        },
                        version,
                        version >= 2,
                    ))
                },
            )
            .await;
            let admin = cluster.admin(krabka_units::secs(5)).await;
            let groups = ["done", "loading", "moved", "busy", "denied"];

            let result = match call {
                Call::Consumer => admin.delete_consumer_groups(&groups).await,
                Call::Share => admin.delete_share_groups(&groups).await,
                Call::Streams => admin.delete_streams_groups(&groups).await,
            };

            let seen = cluster.stop();
            let requests = seen
                .requests
                .iter()
                .map(|request| {
                    let body: DeleteGroupsRequest =
                        decode_request(&request.body, request.version, request.version >= 2);
                    (request.broker, request.version, body.groups_names)
                })
                .collect::<Vec<_>>();
            let names = |names: &[&str]| {
                names
                    .iter()
                    .map(|name| (*name).to_owned())
                    .collect::<Vec<_>>()
            };
            let lookups = seen
                .find_coordinator
                .into_iter()
                .map(|(_, keys)| keys)
                .collect::<Vec<_>>();
            assert!(
                (result, lookups)
                    == (
                        BTreeMap::from([
                            ("busy".to_owned(), Err(kafka_error(68, None))),
                            ("denied".to_owned(), Err(kafka_error(30, None))),
                            ("done".to_owned(), Ok(())),
                            ("loading".to_owned(), Ok(())),
                            ("moved".to_owned(), Ok(())),
                        ]),
                        vec![
                            names(&["busy", "denied", "done", "loading", "moved"]),
                            names(&["moved"]),
                        ],
                    ),
                "case {call:?}"
            );
            // The first round sends one request to each coordinator; the
            // second retries "loading" and the moved "moved" together.
            let mut first_round = requests[..2].to_vec();
            first_round.sort();
            assert!(
                (first_round, requests[2..].to_vec())
                    == (
                        vec![
                            (0, 2, names(&["busy", "done", "loading"])),
                            (1, 2, names(&["moved"])),
                        ],
                        vec![(0, 2, names(&["loading", "moved"]))],
                    ),
                "case {call:?}"
            );
        }
    }

    /// A broker below `FindCoordinator` v4 cannot look up many keys at once,
    /// so each group gets its own lookup, as Kafka's `CoordinatorStrategy`
    /// does after `disableBatch`. The groups of one coordinator still share
    /// one `DeleteGroups` request.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_groups_looks_up_one_group_at_a_time_below_find_coordinator_v4() {
        let cluster = group_cluster(
            1,
            vec![
                (delete_groups_request::API_KEY, 0, 2),
                (
                    krabka_protocol::owned::find_coordinator_request::API_KEY,
                    0,
                    3,
                ),
            ],
            |_, _| Lookup::At(0),
            |_, api_key, version, body, _| {
                if api_key != delete_groups_request::API_KEY {
                    return MockReply::Silent;
                }
                let request: DeleteGroupsRequest = decode_request(body, version, version >= 2);
                MockReply::Respond(encode_response(
                    &DeleteGroupsResponse {
                        results: request
                            .groups_names
                            .into_iter()
                            .map(|group_id| DeletableGroupResult {
                                group_id,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    },
                    version,
                    version >= 2,
                ))
            },
        )
        .await;
        let admin = cluster.admin(krabka_units::secs(5)).await;

        let result = admin.delete_consumer_groups(&["a", "b"]).await;

        let seen = cluster.stop();
        let names = |names: &[&str]| {
            names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>()
        };
        assert!(
            (result, seen.find_coordinator, seen.requests.len())
                == (
                    BTreeMap::from([("a".to_owned(), Ok(())), ("b".to_owned(), Ok(()))]),
                    vec![(3, names(&["a"])), (3, names(&["b"]))],
                    1,
                )
        );
    }
}
