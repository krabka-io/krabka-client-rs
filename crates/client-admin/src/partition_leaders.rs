//! Routing of admin calls to partition leaders.
//!
//! Apache Kafka's `PartitionLeaderStrategy` finds the leader of each
//! partition with a `Metadata` request, and its `AdminApiDriver` sends one
//! batched request to each leader. A partition whose leader is not known yet,
//! whose leader moved, or whose leader cannot be reached goes back to the
//! lookup, and the driver retries it with the backoff until the call deadline.
//! [`AdminClient::call_partition_leaders`] does the same for the
//! leader-routed calls of this crate.

use std::{collections::BTreeMap, future::Future};

use krabka_protocol::{
    owned::{
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::MetadataResponse,
    },
    primitives::uuid::Uuid as ProtoUuid,
};

use crate::{
    AdminClient, AdminError, KafkaError, format_host_port, kafka_error_name,
    retry::{call_timeout_error, is_connection_failure},
};

/// A topic partition, as Kafka's `TopicPartition`.
pub(crate) type PartitionKey = (String, i32);

/// The result of each partition of a leader-routed call.
pub(crate) type PartitionResults<V> = BTreeMap<PartitionKey, Result<V, KafkaError>>;

/// `UNKNOWN_SERVER_ERROR`: a call failed for a reason with no Kafka code.
const UNKNOWN_SERVER_ERROR: i16 = -1;
/// `UNKNOWN_TOPIC_OR_PARTITION`: the metadata has no such topic yet.
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// `LEADER_NOT_AVAILABLE`: the partition has no leader now.
const LEADER_NOT_AVAILABLE: i16 = 5;
/// `NOT_LEADER_OR_FOLLOWER`: the metadata is stale for the partition.
const NOT_LEADER_OR_FOLLOWER: i16 = 6;
/// `BROKER_NOT_AVAILABLE`: the leader broker is not available.
const BROKER_NOT_AVAILABLE: i16 = 8;
/// `REPLICA_NOT_AVAILABLE`: a replica of the partition is not available.
const REPLICA_NOT_AVAILABLE: i16 = 9;
/// `KAFKA_STORAGE_ERROR`: the leader's log directory is offline.
const KAFKA_STORAGE_ERROR: i16 = 56;

/// Where the leader lookup puts one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LeaderLookup {
    /// The `host:port` of the partition leader.
    Leader(String),
    /// No leader is known yet. The lookup runs again after the backoff. The
    /// string says why, for the timeout error.
    Retry(String),
    /// The lookup failed for good.
    Failed(KafkaError),
}

fn metadata_error(code: i16) -> KafkaError {
    KafkaError {
        code,
        name: kafka_error_name(code),
        message: None,
    }
}

/// The leader of `topic`-`partition` in `metadata`, as Kafka's
/// `PartitionLeaderStrategy.handleResponse` classifies it.
///
/// The topic errors `UNKNOWN_TOPIC_OR_PARTITION`, `LEADER_NOT_AVAILABLE` and
/// `BROKER_NOT_AVAILABLE` retry the lookup (`handleTopicError`), as do the
/// partition errors `NOT_LEADER_OR_FOLLOWER`, `REPLICA_NOT_AVAILABLE`,
/// `LEADER_NOT_AVAILABLE`, `BROKER_NOT_AVAILABLE` and `KAFKA_STORAGE_ERROR`
/// (`handlePartitionError`). A topic or partition that the answer omits, and
/// a partition with no leader, retry too. Every other error is final.
pub(crate) fn lookup_leader(
    topic: &str,
    partition: i32,
    metadata: &MetadataResponse,
) -> LeaderLookup {
    let Some(topic_metadata) = metadata
        .topics
        .iter()
        .find(|entry| entry.name.as_deref() == Some(topic))
    else {
        return LeaderLookup::Retry(format!("Metadata returned no entry for topic {topic:?}"));
    };
    match topic_metadata.error_code {
        0 => {}
        code @ (UNKNOWN_TOPIC_OR_PARTITION | LEADER_NOT_AVAILABLE | BROKER_NOT_AVAILABLE) => {
            return LeaderLookup::Retry(kafka_error_name(code).to_owned());
        }
        code => return LeaderLookup::Failed(metadata_error(code)),
    }
    let Some(partition_metadata) = topic_metadata
        .partitions
        .iter()
        .find(|entry| entry.partition_index == partition)
    else {
        return LeaderLookup::Retry(format!(
            "Metadata returned no entry for partition {topic}-{partition}"
        ));
    };
    match partition_metadata.error_code {
        0 => {}
        code @ (NOT_LEADER_OR_FOLLOWER
        | REPLICA_NOT_AVAILABLE
        | LEADER_NOT_AVAILABLE
        | BROKER_NOT_AVAILABLE
        | KAFKA_STORAGE_ERROR) => {
            return LeaderLookup::Retry(kafka_error_name(code).to_owned());
        }
        code => return LeaderLookup::Failed(metadata_error(code)),
    }
    metadata
        .brokers
        .iter()
        .find(|broker| broker.node_id == partition_metadata.leader_id)
        .map_or_else(
            || {
                LeaderLookup::Retry(format!(
                    "Metadata named leader {} for {topic}-{partition} but listed no such broker",
                    partition_metadata.leader_id
                ))
            },
            |broker| LeaderLookup::Leader(format_host_port(&broker.host, broker.port)),
        )
}

/// The `Metadata` request of the leader lookup of `topics`, as Kafka's
/// `PartitionLeaderStrategy.buildRequest` builds it.
pub(crate) fn leader_metadata_request(topics: &[&str]) -> MetadataRequest {
    MetadataRequest {
        topics: Some(
            topics
                .iter()
                .map(|name| MetadataRequestTopic {
                    topic_id: ProtoUuid::ZERO,
                    name: Some((*name).to_owned()),
                    ..Default::default()
                })
                .collect(),
        ),
        allow_auto_topic_creation: false,
        ..Default::default()
    }
}

/// The [`KafkaError`] that every partition of a failed call gets. A broker
/// error keeps its code, and any other failure becomes
/// `UNKNOWN_SERVER_ERROR` with the failure as its message.
pub(crate) fn call_error(error: &AdminError) -> KafkaError {
    match error {
        AdminError::Broker {
            code,
            name,
            message,
            ..
        } => KafkaError {
            code: *code,
            name,
            message: message.clone(),
        },
        other => KafkaError {
            code: UNKNOWN_SERVER_ERROR,
            name: kafka_error_name(UNKNOWN_SERVER_ERROR),
            message: Some(other.to_string()),
        },
    }
}

/// Gives every key that `results` omits `UNKNOWN_SERVER_ERROR`, as Kafka's
/// handlers do for a partition missing from a response, and drops the
/// entries that nobody asked for.
pub(crate) fn complete_results<V>(
    api: &str,
    keys: &[PartitionKey],
    mut results: PartitionResults<V>,
) -> PartitionResults<V> {
    results.retain(|key, _| {
        let asked = keys.contains(key);
        if !asked {
            tracing::warn!(
                api,
                topic = %key.0,
                partition = key.1,
                "the response names a partition that is not in the request"
            );
        }
        asked
    });
    for key in keys {
        results.entry(key.clone()).or_insert_with(|| {
            Err(KafkaError {
                code: UNKNOWN_SERVER_ERROR,
                name: kafka_error_name(UNKNOWN_SERVER_ERROR),
                message: Some(format!(
                    "the {api} response did not contain a result for partition {}-{}",
                    key.0, key.1
                )),
            })
        });
    }
    results
}

impl AdminClient {
    /// Runs one leader-routed call for `keys`, as Kafka's `AdminApiDriver`
    /// runs a handler with a `PartitionLeaderStrategy`.
    ///
    /// Each round looks up the leader of every pending partition with one
    /// `Metadata` request, and then runs `call` once for each leader with
    /// the partitions it leads, all leaders at the same time. `call` gives
    /// the result of each of its partitions, or an [`AdminError`] for the
    /// whole leader. A failed or lost connection to a leader, and a partition
    /// result whose code `retry_code` accepts, send the partition back to
    /// the lookup, so that the next round finds its leader again. The rounds
    /// wait with the backoff between them and stop at the call deadline,
    /// where each pending partition gets `REQUEST_TIMED_OUT`, as Kafka gives
    /// a `TimeoutException`.
    pub(crate) async fn call_partition_leaders<V, F, Fut>(
        &self,
        api: &'static str,
        keys: &[PartitionKey],
        retry_code: fn(i16) -> bool,
        call: F,
    ) -> PartitionResults<V>
    where
        F: Fn(String, Vec<PartitionKey>) -> Fut,
        Fut: Future<Output = Result<PartitionResults<V>, AdminError>>,
    {
        let mut deadline = self.retry.start();
        let mut pending = keys.to_vec();
        pending.sort();
        pending.dedup();
        let mut out = BTreeMap::new();
        let mut attempts = 0_u32;
        let mut last_error = "none".to_owned();
        while !pending.is_empty() {
            attempts = attempts.saturating_add(1);
            let mut topics = pending
                .iter()
                .map(|(topic, _)| topic.as_str())
                .collect::<Vec<_>>();
            topics.dedup();
            let Some(metadata) = deadline
                .bounded(self.conn.send(leader_metadata_request(&topics)))
                .await
            else {
                "the request was in flight at the deadline".clone_into(&mut last_error);
                break;
            };
            let mut retry = Vec::new();
            match metadata {
                // Kafka's `Call.fail` retries a disconnect with the backoff.
                Err(error) if is_connection_failure(&error) => {
                    last_error = error.to_string();
                    retry = std::mem::take(&mut pending);
                }
                Err(error) => {
                    let error = call_error(&error);
                    for key in std::mem::take(&mut pending) {
                        out.insert(key, Err(error.clone()));
                    }
                }
                Ok(metadata) => {
                    let mut groups = BTreeMap::<String, Vec<PartitionKey>>::new();
                    for key in std::mem::take(&mut pending) {
                        match lookup_leader(&key.0, key.1, &metadata) {
                            LeaderLookup::Leader(address) => {
                                groups.entry(address).or_default().push(key);
                            }
                            LeaderLookup::Retry(reason) => {
                                last_error = reason;
                                retry.push(key);
                            }
                            LeaderLookup::Failed(error) => {
                                out.insert(key, Err(error));
                            }
                        }
                    }
                    let answers = futures_util::future::join_all(groups.into_iter().map(
                        |(address, keys)| {
                            let answer = call(address, keys.clone());
                            async move { (keys, deadline.bounded(answer).await) }
                        },
                    ))
                    .await;
                    for (keys, answer) in answers {
                        match answer {
                            None => {
                                "the request was in flight at the deadline"
                                    .clone_into(&mut last_error);
                                retry.extend(keys);
                            }
                            Some(Err(error)) if is_connection_failure(&error) => {
                                last_error = error.to_string();
                                retry.extend(keys);
                            }
                            Some(Err(error)) => {
                                let error = call_error(&error);
                                for key in keys {
                                    out.insert(key, Err(error.clone()));
                                }
                            }
                            Some(Ok(results)) => {
                                for (key, result) in complete_results(api, &keys, results) {
                                    match result {
                                        Err(error) if retry_code(error.code) => {
                                            error.name.clone_into(&mut last_error);
                                            retry.push(key);
                                        }
                                        result => {
                                            out.insert(key, result);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            pending = retry;
            if pending.is_empty() || deadline.exhausted() {
                break;
            }
            tracing::debug!(
                api,
                partitions = pending.len(),
                last_error,
                "leader-routed admin call looks up leaders again"
            );
            deadline.backoff().await;
            if deadline.expired() {
                break;
            }
        }
        let timeout = call_timeout_error(api, attempts, &last_error);
        for key in pending {
            out.insert(key, Err(timeout.clone()));
        }
        out
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Mock-broker helpers of the leader-routed call tests.

    use std::{
        net::SocketAddr,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU16, Ordering},
        },
    };

    use bytes::{Buf, BytesMut};
    use krabka_client_core::{MockBroker, MockReply};
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            metadata_request,
            metadata_response::{
                MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
                MetadataResponseTopic,
            },
        },
    };

    use crate::AdminClient;

    /// The body of `response` at `version`, behind an empty tagged-field
    /// byte when `flexible` is set.
    pub(crate) fn encode_response(response: &impl Encode, version: i16, flexible: bool) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        if flexible {
            bytes.extend_from_slice(&[0]);
        }
        response
            .encode(&mut bytes, version)
            .expect("response encodes");
        bytes.to_vec()
    }

    /// Decodes the request body of a mock broker: the client id, the tagged
    /// field byte of a flexible header, and the request itself.
    pub(crate) fn decode_request<T: for<'a> Decode<'a>>(
        mut body: &[u8],
        version: i16,
        flexible: bool,
    ) -> T {
        let client_id_len = body.get_i16();
        body.advance(usize::try_from(client_id_len).expect("client id length"));
        if flexible {
            body.advance(1);
        }
        T::decode(&mut body, version).expect("request decodes")
    }

    /// An `ApiVersions` answer that advertises `ApiVersions` v0, `Metadata`
    /// v12 and each `(api_key, min, max)` of `apis`.
    pub(crate) fn api_versions(apis: &[(i16, i16, i16)]) -> Vec<u8> {
        let api_version = |api_key, min_version, max_version| ApiVersion {
            api_key,
            min_version,
            max_version,
            ..Default::default()
        };
        let mut api_keys = vec![
            api_version(api_versions_request::API_KEY, 0, 0),
            api_version(metadata_request::API_KEY, 12, 12),
        ];
        api_keys.extend(
            apis.iter()
                .map(|(api_key, min, max)| api_version(*api_key, *min, *max)),
        );
        encode_response(
            &ApiVersionsResponse {
                api_keys,
                ..Default::default()
            },
            0,
            false,
        )
    }

    /// A `Metadata` answer in which broker 1 at `leader` leads `orders`-0 and
    /// `orders`-1 and is the controller.
    pub(crate) fn orders_metadata(leader: SocketAddr) -> MetadataResponse {
        let partition = |partition_index| MetadataResponsePartition {
            partition_index,
            leader_id: 1,
            ..Default::default()
        };
        MetadataResponse {
            controller_id: 1,
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: leader.ip().to_string(),
                port: i32::from(leader.port()),
                ..Default::default()
            }],
            topics: vec![MetadataResponseTopic {
                name: Some("orders".to_owned()),
                partitions: vec![partition(0), partition(1)],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// An address on which no listener accepts connections.
    pub(crate) async fn refused_address() -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port");
        listener.local_addr().expect("local address")
    }

    /// What a [`leader_broker`] saw.
    #[derive(Debug, Default)]
    pub(crate) struct Seen {
        /// The number of `Metadata` requests.
        pub(crate) metadata_requests: usize,
        /// The version and body of each request of the scripted API.
        pub(crate) requests: Vec<(i16, Vec<u8>)>,
    }

    /// A mock broker that leads every `orders` partition.
    ///
    /// It advertises `apis`. Its `Metadata` answer names the address of
    /// entry `n` of `leaders` as the leader in the `n`th answer (the last
    /// entry repeats), or itself for `None`. It answers every request of
    /// `api_key` with `answer(n, version, body)` for the `n`th such request,
    /// and records it in `seen`.
    pub(crate) async fn leader_broker(
        apis: Vec<(i16, i16, i16)>,
        api_key: i16,
        leaders: Vec<Option<SocketAddr>>,
        seen: Arc<Mutex<Seen>>,
        mut answer: impl FnMut(usize, i16, &[u8]) -> Vec<u8> + Send + 'static,
    ) -> MockBroker {
        let port = Arc::new(AtomicU16::new(0));
        let handler_port = Arc::clone(&port);
        let broker = MockBroker::start_with_replies(move |key, version, _, body| {
            let mut seen = seen.lock().expect("seen lock");
            let own = SocketAddr::from(([127, 0, 0, 1], handler_port.load(Ordering::SeqCst)));
            let reply = match key {
                api_versions_request::API_KEY => api_versions(&apis),
                metadata_request::API_KEY => {
                    let leader = leaders[seen.metadata_requests.min(leaders.len() - 1)];
                    seen.metadata_requests += 1;
                    let mut metadata = orders_metadata(leader.unwrap_or(own));
                    if leader.is_some() {
                        metadata.brokers.push(MetadataResponseBroker {
                            node_id: 2,
                            host: own.ip().to_string(),
                            port: i32::from(own.port()),
                            ..Default::default()
                        });
                        metadata.controller_id = 2;
                    }
                    encode_response(&metadata, version, true)
                }
                key if key == api_key => {
                    let reply = answer(seen.requests.len(), version, body);
                    seen.requests.push((version, body.to_vec()));
                    reply
                }
                _ => return MockReply::Silent,
            };
            MockReply::Respond(reply)
        })
        .await;
        port.store(broker.addr.port(), Ordering::SeqCst);
        broker
    }

    /// An admin client on `bootstrap` that retries after 1 ms and gives up
    /// after `api_timeout`, which is also its request timeout.
    pub(crate) async fn fast_admin(
        bootstrap: SocketAddr,
        api_timeout: krabka_units::Time,
    ) -> AdminClient {
        AdminClient::connect_with_config(
            &[bootstrap.to_string()],
            crate::AdminClientConfig {
                request_timeout: api_timeout,
                retry_backoff: krabka_units::millis(1),
                retry_backoff_max: krabka_units::millis(1),
                default_api_timeout: Some(api_timeout),
                ..crate::AdminClientConfig::default()
            },
        )
        .await
        .expect("admin connects")
    }
}

#[cfg(test)]
mod tests {
    use krabka_protocol::owned::metadata_response::MetadataResponseTopic;

    use super::{test_support::orders_metadata, *};

    #[test]
    fn lookup_classifies_each_metadata_answer_as_kafka_does() {
        let addr: std::net::SocketAddr = "127.0.0.1:9092".parse().expect("address");
        let with_partition_error = |code| {
            let mut metadata = orders_metadata(addr);
            metadata.topics[0].partitions[1].error_code = code;
            metadata
        };
        let with_topic_error = |code| {
            let mut metadata = orders_metadata(addr);
            metadata.topics.push(MetadataResponseTopic {
                error_code: code,
                name: Some("payments".to_owned()),
                ..Default::default()
            });
            metadata
        };
        let mut unlisted_leader = orders_metadata(addr);
        unlisted_leader.topics[0].partitions[1].leader_id = 9;
        for (name, metadata, topic, partition, expected) in [
            (
                "leader",
                orders_metadata(addr),
                "orders",
                1,
                LeaderLookup::Leader("127.0.0.1:9092".to_owned()),
            ),
            (
                "not leader or follower retries",
                with_partition_error(NOT_LEADER_OR_FOLLOWER),
                "orders",
                1,
                LeaderLookup::Retry("NOT_LEADER_OR_FOLLOWER".to_owned()),
            ),
            (
                "a partition authorization error is final",
                with_partition_error(29),
                "orders",
                1,
                LeaderLookup::Failed(KafkaError {
                    code: 29,
                    name: "UNKNOWN",
                    message: None,
                }),
            ),
            (
                "unknown topic retries",
                with_topic_error(UNKNOWN_TOPIC_OR_PARTITION),
                "payments",
                0,
                LeaderLookup::Retry("UNKNOWN_TOPIC_OR_PARTITION".to_owned()),
            ),
            (
                "an invalid topic is final",
                with_topic_error(17),
                "payments",
                0,
                LeaderLookup::Failed(KafkaError {
                    code: 17,
                    name: "INVALID_TOPIC_EXCEPTION",
                    message: None,
                }),
            ),
            (
                "unlisted topic retries",
                orders_metadata(addr),
                "absent",
                0,
                LeaderLookup::Retry("Metadata returned no entry for topic \"absent\"".to_owned()),
            ),
            (
                "unlisted partition retries",
                orders_metadata(addr),
                "orders",
                5,
                LeaderLookup::Retry("Metadata returned no entry for partition orders-5".to_owned()),
            ),
            (
                "unlisted leader retries",
                unlisted_leader,
                "orders",
                1,
                LeaderLookup::Retry(
                    "Metadata named leader 9 for orders-1 but listed no such broker".to_owned(),
                ),
            ),
        ] {
            assert2::assert!(
                lookup_leader(topic, partition, &metadata) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn complete_results_fills_missing_partitions_and_drops_unasked_ones() {
        let keys = [("orders".to_owned(), 0), ("orders".to_owned(), 1)];
        let results = BTreeMap::from([
            (("orders".to_owned(), 0), Ok(7)),
            (("orders".to_owned(), 9), Ok(8)),
        ]);

        assert2::assert!(
            complete_results("ListOffsets", &keys, results)
                == BTreeMap::from([
                    (("orders".to_owned(), 0), Ok(7)),
                    (
                        ("orders".to_owned(), 1),
                        Err(KafkaError {
                            code: UNKNOWN_SERVER_ERROR,
                            name: "UNKNOWN_SERVER_ERROR",
                            message: Some(
                                "the ListOffsets response did not contain a result for \
                                 partition orders-1"
                                    .to_owned()
                            ),
                        })
                    ),
                ])
        );
    }
}
