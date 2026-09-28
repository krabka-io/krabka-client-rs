//! Routing of batched group admin calls to group coordinators.
//!
//! Apache Kafka's `CoordinatorStrategy` finds the coordinator of every group
//! of a call with one batched `FindCoordinator` request (v4 or later), and its
//! `AdminApiDriver` sends one batched request to each coordinator with the
//! groups that it coordinates. A group whose coordinator is loading retries
//! on the same coordinator, a group whose coordinator moved goes back to the
//! lookup, and the driver retries both with the backoff until the call
//! deadline. [`AdminClient::call_group_coordinators`] does the same for the
//! batched group calls of this crate.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
};

use krabka_client_core::{
    Connection, CoordinatorEndpoint, CoordinatorKeyType, build_find_coordinator,
};
use krabka_protocol::owned::{
    find_coordinator_request::{self, FindCoordinatorRequest},
    find_coordinator_response::FindCoordinatorResponse,
};

use crate::{
    AdminClient, AdminError, KafkaError, format_host_port,
    groups::list_groups_kafka_error,
    kafka_error_name,
    retry::{call_timeout_error, is_connection_failure},
};

/// `COORDINATOR_LOAD_IN_PROGRESS`: the coordinator is loading the group.
pub(crate) const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
/// `COORDINATOR_NOT_AVAILABLE`: no broker coordinates the group now.
pub(crate) const COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// `NOT_COORDINATOR`: the broker does not coordinate the group.
pub(crate) const NOT_COORDINATOR: i16 = 16;
/// The first `FindCoordinator` version that looks up many keys at once
/// (`FindCoordinatorRequest.MIN_BATCHED_VERSION`, KIP-699).
const MIN_BATCHED_FIND_COORDINATOR_VERSION: i16 = 4;
/// The message of a request that was still in flight at the call deadline.
const IN_FLIGHT_AT_DEADLINE: &str = "the request was in flight at the deadline";

/// What one coordinator answer means for one group, as the three maps of
/// Kafka's `AdminApiHandler.ApiResult` classify it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GroupOutcome<V> {
    /// The group is complete, with a value or a final error.
    Done(Result<V, KafkaError>),
    /// Send the group to the same coordinator again after the backoff. The
    /// string says why, for the timeout error.
    Retry(String),
    /// Find the coordinator of the group again (`unmappedKeys`). The string
    /// says why, for the timeout error.
    Unmap(String),
}

/// A [`KafkaError`] with the name of `code`.
pub(crate) fn kafka_error(code: i16, message: Option<String>) -> KafkaError {
    KafkaError {
        code,
        name: kafka_error_name(code),
        message,
    }
}

/// The outcome of a group error code, as the `handleError` of each Kafka
/// group handler classifies it: `COORDINATOR_LOAD_IN_PROGRESS` and each code
/// of `retry_codes` retry on the same coordinator, `COORDINATOR_NOT_AVAILABLE`
/// and `NOT_COORDINATOR` look the coordinator up again, and every other code
/// is final.
pub(crate) fn group_error_outcome<V>(
    code: i16,
    message: Option<String>,
    retry_codes: &[i16],
) -> GroupOutcome<V> {
    match code {
        COORDINATOR_LOAD_IN_PROGRESS => GroupOutcome::Retry(kafka_error_name(code).to_owned()),
        code if retry_codes.contains(&code) => {
            GroupOutcome::Retry(kafka_error_name(code).to_owned())
        }
        COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR => {
            GroupOutcome::Unmap(kafka_error_name(code).to_owned())
        }
        code => GroupOutcome::Done(Err(kafka_error(code, message))),
    }
}

/// Where the coordinator lookup puts one group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CoordinatorLookup {
    /// The coordinator of the group.
    Found(CoordinatorEndpoint),
    /// Look the group up again after the backoff. The string says why.
    Retry(String),
    /// The lookup failed for good.
    Failed(KafkaError),
}

/// Classifies one `FindCoordinator` entry, as Kafka's
/// `CoordinatorStrategy.handleError` does: `COORDINATOR_NOT_AVAILABLE` and
/// `COORDINATOR_LOAD_IN_PROGRESS` retry the lookup, and every other error is
/// final.
fn classify_coordinator(
    error_code: i16,
    error_message: Option<String>,
    endpoint: CoordinatorEndpoint,
) -> CoordinatorLookup {
    match error_code {
        0 => CoordinatorLookup::Found(endpoint),
        COORDINATOR_NOT_AVAILABLE | COORDINATOR_LOAD_IN_PROGRESS => {
            CoordinatorLookup::Retry(kafka_error_name(error_code).to_owned())
        }
        code => CoordinatorLookup::Failed(kafka_error(code, error_message)),
    }
}

/// The lookup result of each of `keys` in one `FindCoordinator` answer.
///
/// A v4 or later answer has one `coordinators` entry for each key. A key that
/// the answer omits stays in the lookup, as in Kafka's `AdminApiDriver`,
/// where only mapped and failed keys leave the lookup map. A v0 to v3 answer
/// holds the single key in its top-level fields.
pub(crate) fn coordinator_lookups(
    keys: &[String],
    response: FindCoordinatorResponse,
) -> Vec<(String, CoordinatorLookup)> {
    if response.coordinators.is_empty()
        && let [key] = keys
    {
        let endpoint = CoordinatorEndpoint {
            node_id: response.node_id,
            host: response.host,
            port: response.port,
        };
        return vec![(
            key.clone(),
            classify_coordinator(response.error_code, response.error_message, endpoint),
        )];
    }
    let mut entries = response
        .coordinators
        .into_iter()
        .map(|coordinator| {
            let lookup = classify_coordinator(
                coordinator.error_code,
                coordinator.error_message,
                CoordinatorEndpoint {
                    node_id: coordinator.node_id,
                    host: coordinator.host,
                    port: coordinator.port,
                },
            );
            (coordinator.key, lookup)
        })
        .collect::<BTreeMap<_, _>>();
    keys.iter()
        .map(|key| {
            let lookup = entries.remove(key).unwrap_or_else(|| {
                CoordinatorLookup::Retry(format!(
                    "FindCoordinator returned no entry for group {key:?}"
                ))
            });
            (key.clone(), lookup)
        })
        .collect()
}

/// The lookup result of each of `keys` after the `FindCoordinator` request
/// failed with `error`. A failed or lost connection retries the lookup, as
/// Kafka's `Call.fail` retries a disconnect, and every other failure is
/// final.
fn failed_lookups(keys: &[String], error: &AdminError) -> Vec<(String, CoordinatorLookup)> {
    let lookup = if is_connection_failure(error) {
        CoordinatorLookup::Retry(error.to_string())
    } else {
        CoordinatorLookup::Failed(list_groups_kafka_error(error))
    };
    keys.iter()
        .map(|key| (key.clone(), lookup.clone()))
        .collect()
}

impl AdminClient {
    /// Finds the coordinator of each group of `keys`.
    ///
    /// A broker that supports `FindCoordinator` v4 gets one request for all
    /// keys, as Kafka's `CoordinatorStrategy.buildRequest` batches them. An
    /// older broker gets one request for each key, as the strategy does after
    /// `disableBatch`.
    async fn find_group_coordinators(&self, keys: &[String]) -> Vec<(String, CoordinatorLookup)> {
        let batched = self
            .conn
            .advertised_api_range(find_coordinator_request::API_KEY)
            .await
            .is_some_and(|(_, max)| max >= MIN_BATCHED_FIND_COORDINATOR_VERSION);
        if batched {
            let request = FindCoordinatorRequest {
                key_type: CoordinatorKeyType::Group.as_wire(),
                coordinator_keys: keys.to_vec(),
                ..Default::default()
            };
            return match self.conn.send(request).await {
                Ok(response) => coordinator_lookups(keys, response),
                Err(error) => failed_lookups(keys, &error),
            };
        }
        let mut lookups = Vec::with_capacity(keys.len());
        for key in keys {
            let single = std::slice::from_ref(key);
            match self
                .conn
                .send(build_find_coordinator(key, CoordinatorKeyType::Group))
                .await
            {
                Ok(response) => lookups.extend(coordinator_lookups(single, response)),
                Err(error) => lookups.extend(failed_lookups(single, &error)),
            }
        }
        lookups
    }

    /// Opens a new connection to `coordinator`.
    pub(crate) async fn connect_coordinator(
        &self,
        coordinator: &CoordinatorEndpoint,
    ) -> Result<Connection, AdminError> {
        Self::connect_one(
            &format_host_port(&coordinator.host, coordinator.port),
            self.options.clone(),
        )
        .await
    }

    /// Runs one batched group call for `groups`, as Kafka's `AdminApiDriver`
    /// runs a handler with a `CoordinatorStrategy`.
    ///
    /// Each round finds the coordinator of every group that has none with
    /// [`AdminClient::find_group_coordinators`], and then runs `call` once for
    /// each coordinator with the groups that it coordinates, all coordinators
    /// at the same time. `call` gives the [`GroupOutcome`] of each of its
    /// groups, or an [`AdminError`] for the whole coordinator:
    ///
    /// - A failed or lost connection sends every group of that coordinator
    ///   back to the lookup, as `AdminApiDriver.onFailure` does after a
    ///   disconnect.
    /// - Another error is the final error of every group of that coordinator.
    /// - A group that the answer omits is sent again, as the driver keeps a
    ///   key that is neither complete, failed nor unmapped.
    ///
    /// The rounds wait with the backoff between them and stop at the call
    /// deadline, where each pending group gets `REQUEST_TIMED_OUT`, as Kafka
    /// gives a `TimeoutException`. The result has one entry for each group.
    pub(crate) async fn call_group_coordinators<V, F, Fut>(
        &self,
        api: &'static str,
        groups: &[&str],
        call: F,
    ) -> BTreeMap<String, Result<V, KafkaError>>
    where
        F: Fn(CoordinatorEndpoint, Vec<String>) -> Fut,
        Fut: Future<Output = Result<BTreeMap<String, GroupOutcome<V>>, AdminError>>,
    {
        let mut deadline = self.retry.start();
        let mut unmapped = groups
            .iter()
            .map(|group| (*group).to_owned())
            .collect::<BTreeSet<_>>();
        let mut mapped = BTreeMap::<String, CoordinatorEndpoint>::new();
        let mut out = BTreeMap::new();
        let mut attempts = 0_u32;
        let mut last_error = "none".to_owned();
        while !(unmapped.is_empty() && mapped.is_empty()) {
            attempts = attempts.saturating_add(1);
            if !unmapped.is_empty() {
                let keys = std::mem::take(&mut unmapped)
                    .into_iter()
                    .collect::<Vec<_>>();
                let Some(lookups) = deadline.bounded(self.find_group_coordinators(&keys)).await
                else {
                    IN_FLIGHT_AT_DEADLINE.clone_into(&mut last_error);
                    unmapped.extend(keys);
                    break;
                };
                for (group, lookup) in lookups {
                    match lookup {
                        CoordinatorLookup::Found(coordinator) => {
                            mapped.insert(group, coordinator);
                        }
                        CoordinatorLookup::Retry(reason) => {
                            last_error = reason;
                            unmapped.insert(group);
                        }
                        CoordinatorLookup::Failed(error) => {
                            out.insert(group, Err(error));
                        }
                    }
                }
            }

            let mut by_coordinator = BTreeMap::<String, (CoordinatorEndpoint, Vec<String>)>::new();
            for (group, coordinator) in &mapped {
                by_coordinator
                    .entry(format_host_port(&coordinator.host, coordinator.port))
                    .or_insert_with(|| (coordinator.clone(), Vec::new()))
                    .1
                    .push(group.clone());
            }
            let answers = futures_util::future::join_all(by_coordinator.into_values().map(
                |(coordinator, keys)| {
                    let answer = call(coordinator, keys.clone());
                    async move { (keys, deadline.bounded(answer).await) }
                },
            ))
            .await;
            for (keys, answer) in answers {
                match answer {
                    None => IN_FLIGHT_AT_DEADLINE.clone_into(&mut last_error),
                    Some(Err(error)) if is_connection_failure(&error) => {
                        last_error = error.to_string();
                        for key in keys {
                            mapped.remove(&key);
                            unmapped.insert(key);
                        }
                    }
                    Some(Err(error)) => {
                        let error = list_groups_kafka_error(&error);
                        for key in keys {
                            mapped.remove(&key);
                            out.insert(key, Err(error.clone()));
                        }
                    }
                    Some(Ok(mut outcomes)) => {
                        for key in keys {
                            match outcomes.remove(&key) {
                                Some(GroupOutcome::Done(result)) => {
                                    mapped.remove(&key);
                                    out.insert(key, result);
                                }
                                Some(GroupOutcome::Retry(reason)) => last_error = reason,
                                Some(GroupOutcome::Unmap(reason)) => {
                                    last_error = reason;
                                    mapped.remove(&key);
                                    unmapped.insert(key);
                                }
                                None => {
                                    last_error =
                                        format!("the {api} response did not contain group {key:?}");
                                }
                            }
                        }
                    }
                }
            }

            if (unmapped.is_empty() && mapped.is_empty()) || deadline.exhausted() {
                break;
            }
            tracing::debug!(
                api,
                groups = unmapped.len() + mapped.len(),
                last_error,
                "batched group admin call retries"
            );
            deadline.backoff().await;
            if deadline.expired() {
                break;
            }
        }
        let timeout = call_timeout_error(api, attempts, &last_error);
        for key in mapped.into_keys().chain(unmapped) {
            out.insert(key, Err(timeout.clone()));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::owned::find_coordinator_response::Coordinator;

    use super::*;

    fn endpoint(node_id: i32) -> CoordinatorEndpoint {
        CoordinatorEndpoint {
            node_id,
            host: format!("broker-{node_id}"),
            port: 9092,
        }
    }

    fn coordinator(key: &str, node_id: i32, error_code: i16) -> Coordinator {
        Coordinator {
            key: key.to_owned(),
            node_id,
            host: format!("broker-{node_id}"),
            port: 9092,
            error_code,
            error_message: None,
            ..Default::default()
        }
    }

    /// Kafka's `CoordinatorStrategy.handleResponse` maps each batched entry by
    /// its key, retries 14 and 15, fails every other code, and leaves a key
    /// that the answer omits in the lookup. A v0 to v3 answer holds its one
    /// key in the top-level fields.
    #[test]
    fn coordinator_lookups_classify_each_entry_as_kafka_does() {
        let keys = |keys: &[&str]| keys.iter().map(|key| (*key).to_owned()).collect::<Vec<_>>();
        for (name, keys, response, expected) in [
            (
                "batched answer",
                keys(&["a", "b", "c", "d", "e"]),
                FindCoordinatorResponse {
                    coordinators: vec![
                        coordinator("b", 2, 0),
                        coordinator("a", 1, 0),
                        coordinator("c", 3, 15),
                        coordinator("d", 3, 30),
                    ],
                    ..Default::default()
                },
                vec![
                    ("a", CoordinatorLookup::Found(endpoint(1))),
                    ("b", CoordinatorLookup::Found(endpoint(2))),
                    (
                        "c",
                        CoordinatorLookup::Retry("COORDINATOR_NOT_AVAILABLE".to_owned()),
                    ),
                    ("d", CoordinatorLookup::Failed(kafka_error(30, None))),
                    (
                        "e",
                        CoordinatorLookup::Retry(
                            "FindCoordinator returned no entry for group \"e\"".to_owned(),
                        ),
                    ),
                ],
            ),
            (
                "unbatched answer",
                keys(&["a"]),
                FindCoordinatorResponse {
                    node_id: 1,
                    host: "broker-1".to_owned(),
                    port: 9092,
                    ..Default::default()
                },
                vec![("a", CoordinatorLookup::Found(endpoint(1)))],
            ),
            (
                "unbatched load in progress",
                keys(&["a"]),
                FindCoordinatorResponse {
                    error_code: 14,
                    ..Default::default()
                },
                vec![(
                    "a",
                    CoordinatorLookup::Retry("COORDINATOR_LOAD_IN_PROGRESS".to_owned()),
                )],
            ),
        ] {
            let expected = expected
                .into_iter()
                .map(|(key, lookup)| (key.to_owned(), lookup))
                .collect::<Vec<_>>();
            assert!(
                coordinator_lookups(&keys, response) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn group_error_outcome_retries_as_the_kafka_handlers_do() {
        const REBALANCE_IN_PROGRESS: i16 = 27;
        for (name, code, retry_codes, expected) in [
            (
                "load in progress",
                14,
                &[][..],
                GroupOutcome::Retry("COORDINATOR_LOAD_IN_PROGRESS".to_owned()),
            ),
            (
                "not available",
                15,
                &[],
                GroupOutcome::Unmap("COORDINATOR_NOT_AVAILABLE".to_owned()),
            ),
            (
                "not coordinator",
                16,
                &[],
                GroupOutcome::Unmap("NOT_COORDINATOR".to_owned()),
            ),
            (
                "rebalance is final by default",
                REBALANCE_IN_PROGRESS,
                &[],
                GroupOutcome::Done(Err(kafka_error(REBALANCE_IN_PROGRESS, None))),
            ),
            (
                "rebalance retries when the handler retries it",
                REBALANCE_IN_PROGRESS,
                &[REBALANCE_IN_PROGRESS],
                GroupOutcome::Retry("REBALANCE_IN_PROGRESS".to_owned()),
            ),
            (
                "authorization is final",
                30,
                &[],
                GroupOutcome::Done(Err(kafka_error(30, None))),
            ),
        ] {
            assert!(
                group_error_outcome::<()>(code, None, retry_codes) == expected,
                "case {name}"
            );
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    //! A mock cluster for the batched group call tests.

    use std::{
        collections::BTreeMap,
        net::SocketAddr,
        sync::{Arc, Mutex},
    };

    use krabka_client_core::{MockBroker, MockReply};
    use krabka_protocol::owned::{
        find_coordinator_request::{self, FindCoordinatorRequest},
        find_coordinator_response::{Coordinator, FindCoordinatorResponse},
    };

    use crate::{
        AdminClient,
        partition_leaders::test_support::{
            api_versions, decode_request, encode_response, fast_admin,
        },
    };

    /// Where the bootstrap broker's `FindCoordinator` answer puts a group.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Lookup {
        /// The broker with this index coordinates the group.
        At(usize),
        /// The entry of the group carries this error code.
        Error(i16),
    }

    /// One request of a scripted API that a broker of the cluster received.
    #[derive(Debug, Clone)]
    pub(crate) struct SeenRequest {
        pub(crate) broker: usize,
        pub(crate) api_key: i16,
        pub(crate) version: i16,
        pub(crate) body: Vec<u8>,
    }

    /// What the brokers of a [`GroupCluster`] saw.
    #[derive(Debug, Clone, Default)]
    pub(crate) struct Seen {
        /// The version and the keys of each `FindCoordinator` request.
        pub(crate) find_coordinator: Vec<(i16, Vec<String>)>,
        /// Every other request except `ApiVersions`, in arrival order.
        pub(crate) requests: Vec<SeenRequest>,
    }

    /// A cluster of mock brokers. Broker 0 is the bootstrap broker.
    pub(crate) struct GroupCluster {
        pub(crate) addrs: Vec<SocketAddr>,
        brokers: Vec<MockBroker>,
        seen: Arc<Mutex<Seen>>,
    }

    impl GroupCluster {
        /// An admin client on broker 0 that retries after 1 ms and stops
        /// after `api_timeout`.
        pub(crate) async fn admin(&self, api_timeout: krabka_units::Time) -> AdminClient {
            fast_admin(self.addrs[0], api_timeout).await
        }

        /// Stops every broker and returns what they saw.
        pub(crate) fn stop(self) -> Seen {
            for broker in self.brokers {
                broker.stop();
            }
            self.seen.lock().expect("seen lock").clone()
        }
    }

    /// The `FindCoordinator` answer for `keys` at `version`. `lookup` gets
    /// each key and the number of earlier lookups of that key.
    fn find_coordinator_answer(
        keys: &[String],
        version: i16,
        addrs: &[SocketAddr],
        lookups: &mut BTreeMap<String, usize>,
        lookup: &dyn Fn(&str, usize) -> Lookup,
    ) -> Vec<u8> {
        let entries = keys
            .iter()
            .map(|key| {
                let count = lookups.entry(key.clone()).or_default();
                let answer = lookup(key, *count);
                *count += 1;
                match answer {
                    Lookup::At(index) => Coordinator {
                        key: key.clone(),
                        node_id: i32::try_from(index).expect("node id"),
                        host: addrs[index].ip().to_string(),
                        port: i32::from(addrs[index].port()),
                        ..Default::default()
                    },
                    Lookup::Error(error_code) => Coordinator {
                        key: key.clone(),
                        error_code,
                        error_message: None,
                        node_id: -1,
                        port: -1,
                        ..Default::default()
                    },
                }
            })
            .collect::<Vec<_>>();
        let response = if version >= 4 {
            FindCoordinatorResponse {
                coordinators: entries,
                ..Default::default()
            }
        } else {
            let entry = entries.into_iter().next().expect("one key");
            FindCoordinatorResponse {
                error_code: entry.error_code,
                node_id: entry.node_id,
                host: entry.host,
                port: entry.port,
                ..Default::default()
            }
        };
        encode_response(
            &response,
            version,
            version >= find_coordinator_request::FLEXIBLE_MIN,
        )
    }

    /// Starts `count` mock brokers that advertise `FindCoordinator` v0 to v6
    /// (unless `apis` names it) and each `(api_key, min, max)` of `apis`.
    ///
    /// Any broker answers `FindCoordinator` from `lookup`. Every other
    /// request goes to `answer(broker, api_key, version, body, own_address)`
    /// and is recorded in [`Seen::requests`].
    pub(crate) async fn group_cluster(
        count: usize,
        mut apis: Vec<(i16, i16, i16)>,
        lookup: impl Fn(&str, usize) -> Lookup + Send + Sync + 'static,
        answer: impl FnMut(usize, i16, i16, &[u8], SocketAddr) -> MockReply + Send + 'static,
    ) -> GroupCluster {
        if !apis
            .iter()
            .any(|(api_key, _, _)| *api_key == find_coordinator_request::API_KEY)
        {
            apis.push((find_coordinator_request::API_KEY, 0, 6));
        }
        let addrs = Arc::new(Mutex::new(Vec::<SocketAddr>::new()));
        let seen = Arc::new(Mutex::new(Seen::default()));
        let lookups = Arc::new(Mutex::new(BTreeMap::<String, usize>::new()));
        let lookup = Arc::new(lookup);
        let answer = Arc::new(Mutex::new(answer));
        let mut brokers = Vec::with_capacity(count);
        for broker in 0..count {
            let (apis, addrs, seen, lookups, lookup, answer) = (
                apis.clone(),
                Arc::clone(&addrs),
                Arc::clone(&seen),
                Arc::clone(&lookups),
                Arc::clone(&lookup),
                Arc::clone(&answer),
            );
            brokers.push(
                MockBroker::start_with_replies(move |api_key, version, _, body| {
                    let addrs = addrs.lock().expect("addrs lock").clone();
                    if api_key == krabka_protocol::owned::api_versions_request::API_KEY {
                        return MockReply::Respond(api_versions(&apis));
                    }
                    if api_key == find_coordinator_request::API_KEY {
                        let flexible = version >= find_coordinator_request::FLEXIBLE_MIN;
                        let request: FindCoordinatorRequest =
                            decode_request(body, version, flexible);
                        let keys = if version >= 4 {
                            request.coordinator_keys
                        } else {
                            vec![request.key]
                        };
                        seen.lock()
                            .expect("seen lock")
                            .find_coordinator
                            .push((version, keys.clone()));
                        let mut lookups = lookups.lock().expect("lookups lock");
                        return MockReply::Respond(find_coordinator_answer(
                            &keys,
                            version,
                            &addrs,
                            &mut lookups,
                            &*lookup,
                        ));
                    }
                    seen.lock().expect("seen lock").requests.push(SeenRequest {
                        broker,
                        api_key,
                        version,
                        body: body.to_vec(),
                    });
                    let mut answer = answer.lock().expect("answer lock");
                    (*answer)(broker, api_key, version, body, addrs[broker])
                })
                .await,
            );
        }
        let started = brokers.iter().map(|broker| broker.addr).collect::<Vec<_>>();
        addrs.lock().expect("addrs lock").clone_from(&started);
        GroupCluster {
            addrs: started,
            brokers,
            seen,
        }
    }
}
