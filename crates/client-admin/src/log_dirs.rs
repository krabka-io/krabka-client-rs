//! KIP-113 admin RPCs: `AlterReplicaLogDirs` (`api_key` 34) and
//! `DescribeLogDirs` (`api_key` 35).
//!
//! Both act only on the broker that receives the request. As Kafka's
//! `KafkaAdminClient.describeLogDirs` and `alterReplicaLogDirs` do, the admin
//! client finds each broker by id in the metadata and sends one request to
//! each broker (`ConstantNodeIdProvider`). A broker that fails gives an error
//! for that broker only.

use std::collections::{BTreeMap, BTreeSet};

use krabka_client_core::{Connection, ConnectionOptions};
use krabka_protocol::{
    ProtocolRequest,
    owned::{
        alter_replica_log_dirs_request::{
            AlterReplicaLogDir, AlterReplicaLogDirTopic, AlterReplicaLogDirsRequest,
        },
        alter_replica_log_dirs_response::AlterReplicaLogDirsResponse,
        describe_log_dirs_request::{DescribableLogDirTopic, DescribeLogDirsRequest},
        describe_log_dirs_response::DescribeLogDirsResponse,
        metadata_request::MetadataRequest,
    },
};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

use crate::{
    AdminClient, AdminError, KafkaError, format_host_port,
    groups::list_groups_kafka_error,
    kafka_error_if, kafka_error_name,
    retry::{
        CoordinatorRetry, KAFKA_ADMIN_RETRY, RetryAction, RetryDeadline, RetryPolicy,
        is_connection_failure,
    },
};

/// `UNKNOWN_SERVER_ERROR`.
const UNKNOWN_SERVER_ERROR: i16 = -1;
/// `REQUEST_TIMED_OUT`: Kafka's `TimeoutException`.
const REQUEST_TIMED_OUT: i16 = 7;
/// `CLUSTER_AUTHORIZATION_FAILED`.
const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;

/// One replica of a partition on one broker, as Kafka's
/// `TopicPartitionReplica`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicPartitionReplica {
    pub topic: String,
    pub partition: i32,
    pub broker_id: i32,
}

/// One log dir from a `DescribeLogDirs` response, as Kafka's
/// `LogDirDescription`.
#[derive(Debug, Clone, PartialEq)]
pub struct LogDirInfo {
    pub log_dir: String,
    pub error: Option<KafkaError>,
    pub topics: Vec<LogDirTopicInfo>,
    /// The size of the volume. `None` when the broker does not report it
    /// (`DescribeLogDirs` below v4, or `-1`).
    pub total: Option<ByteSize>,
    /// The free space of the volume that the log dir can use. `None` when the
    /// broker does not report it.
    pub usable: Option<ByteSize>,
    /// Whether the log dir takes no new partitions (KIP-1066).
    pub is_cordoned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirTopicInfo {
    pub name: String,
    pub partitions: Vec<LogDirPartitionInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirPartitionInfo {
    pub partition_index: i32,
    pub partition_size: i64,
    pub offset_lag: i64,
    pub is_future_key: bool,
}

/// The result of one broker: its value, or the Kafka error of the broker.
pub type BrokerResult<T> = Result<T, KafkaError>;

impl AdminClient {
    /// `AlterReplicaLogDirs` (KIP-113): moves replicas between the
    /// `log.dirs` of their brokers, as Kafka's
    /// `KafkaAdminClient.alterReplicaLogDirs` does.
    ///
    /// `assignments` maps each replica to the absolute path of its target log
    /// dir. The call groups the replicas by broker id and sends one request to
    /// each broker. The result has one entry for each replica:
    ///
    /// - The error code of the broker for that partition, `Ok(())` for 0.
    /// - `UNKNOWN_SERVER_ERROR` (-1) when the response of the broker has no
    ///   result for the replica (`completeUnrealizedFutures`).
    /// - The error of the broker call for every replica of a broker that
    ///   fails. A failed or lost connection is sent again with Kafka's backoff
    ///   until `default.api.timeout.ms` (60 s). A broker that is not in the
    ///   metadata at the deadline gives `REQUEST_TIMED_OUT` (7).
    ///
    /// # Errors
    /// Returns an error when the `Metadata` request fails.
    pub async fn alter_replica_log_dirs(
        &mut self,
        assignments: &BTreeMap<TopicPartitionReplica, String>,
    ) -> Result<BTreeMap<TopicPartitionReplica, BrokerResult<()>>, AdminError> {
        self.alter_replica_log_dirs_with_retry(assignments, KAFKA_ADMIN_RETRY)
            .await
    }

    async fn alter_replica_log_dirs_with_retry(
        &mut self,
        assignments: &BTreeMap<TopicPartitionReplica, String>,
        retry: RetryPolicy,
    ) -> Result<BTreeMap<TopicPartitionReplica, BrokerResult<()>>, AdminError> {
        let start = retry.start();
        let mut by_broker = BTreeMap::<i32, BTreeMap<TopicPartitionReplica, String>>::new();
        for (replica, path) in assignments {
            by_broker
                .entry(replica.broker_id)
                .or_default()
                .insert(replica.clone(), path.clone());
        }
        let broker_ids = by_broker.keys().copied().collect::<BTreeSet<_>>();
        let endpoints = self.broker_endpoints(&broker_ids, start).await?;
        let answers = futures_util::future::join_all(by_broker.iter().map(|(broker_id, moves)| {
            let endpoint = endpoints.get(broker_id).cloned();
            let options = self.options.clone();
            async move {
                let endpoint = endpoint.ok_or_else(|| unknown_broker(*broker_id))?;
                call_broker(
                    &endpoint,
                    &options,
                    alter_request(moves),
                    CoordinatorRetry::from_deadline(start),
                )
                .await
            }
        }))
        .await;

        let mut out = BTreeMap::new();
        for ((broker_id, moves), answer) in by_broker.into_iter().zip(answers) {
            match answer {
                Ok(response) => out.extend(alter_results(broker_id, &moves, response)),
                Err(error) => {
                    out.extend(
                        moves
                            .into_keys()
                            .map(|replica| (replica, Err(error.clone()))),
                    );
                }
            }
        }
        Ok(out)
    }

    /// `DescribeLogDirs` (KIP-113): lists every configured `log.dir` of each
    /// broker in `brokers`, with the partitions each one holds, as Kafka's
    /// `KafkaAdminClient.describeLogDirs` does.
    ///
    /// Pass `None` to fetch all partitions, as Kafka does. Pass `Some` with a
    /// topic to partitions filter to narrow the result. An empty inner vec
    /// means all partitions of that topic.
    ///
    /// The result has one entry for each broker id. A broker that answers
    /// with no log dir gives its top-level error code, or
    /// `CLUSTER_AUTHORIZATION_FAILED` (31) when it has none, as Kafka does.
    /// A failed or lost connection is sent again with Kafka's backoff until
    /// `default.api.timeout.ms` (60 s). A broker that is not in the metadata
    /// at the deadline gives `REQUEST_TIMED_OUT` (7).
    ///
    /// # Errors
    /// Returns an error when the `Metadata` request fails.
    pub async fn describe_log_dirs(
        &mut self,
        brokers: &[i32],
        filter: Option<&BTreeMap<String, Vec<i32>>>,
    ) -> Result<BTreeMap<i32, BrokerResult<Vec<LogDirInfo>>>, AdminError> {
        self.describe_log_dirs_with_retry(brokers, filter, KAFKA_ADMIN_RETRY)
            .await
    }

    async fn describe_log_dirs_with_retry(
        &mut self,
        brokers: &[i32],
        filter: Option<&BTreeMap<String, Vec<i32>>>,
        retry: RetryPolicy,
    ) -> Result<BTreeMap<i32, BrokerResult<Vec<LogDirInfo>>>, AdminError> {
        let start = retry.start();
        let broker_ids = brokers.iter().copied().collect::<BTreeSet<_>>();
        let endpoints = self.broker_endpoints(&broker_ids, start).await?;
        let request = describe_request(filter);
        let answers = futures_util::future::join_all(broker_ids.iter().map(|broker_id| {
            let endpoint = endpoints.get(broker_id).cloned();
            let options = self.options.clone();
            let request = request.clone();
            async move {
                let endpoint = endpoint.ok_or_else(|| unknown_broker(*broker_id))?;
                let response = call_broker(
                    &endpoint,
                    &options,
                    request,
                    CoordinatorRetry::from_deadline(start),
                )
                .await?;
                log_dir_infos(response)
            }
        }))
        .await;
        Ok(broker_ids.into_iter().zip(answers).collect())
    }

    /// The `host:port` of each broker id in `wanted` that the metadata names.
    /// While a wanted broker is missing, the call sends `Metadata` again with
    /// the retry backoff until the deadline, as Kafka's
    /// `ConstantNodeIdProvider` asks for a metadata update and waits.
    async fn broker_endpoints(
        &mut self,
        wanted: &BTreeSet<i32>,
        mut deadline: RetryDeadline,
    ) -> Result<BTreeMap<i32, String>, AdminError> {
        loop {
            let response = self
                .conn
                .send(MetadataRequest {
                    topics: Some(Vec::new()),
                    allow_auto_topic_creation: true,
                    ..Default::default()
                })
                .await?;
            let endpoints = response
                .brokers
                .into_iter()
                .filter(|broker| wanted.contains(&broker.node_id))
                .map(|broker| (broker.node_id, format_host_port(&broker.host, broker.port)))
                .collect::<BTreeMap<_, _>>();
            if endpoints.len() == wanted.len() || deadline.expired() {
                return Ok(endpoints);
            }
            deadline.backoff().await;
            if deadline.expired() {
                return Ok(endpoints);
            }
        }
    }
}

/// The error of a broker id that the metadata does not name at the deadline.
fn unknown_broker(broker_id: i32) -> KafkaError {
    KafkaError {
        code: REQUEST_TIMED_OUT,
        name: kafka_error_name(REQUEST_TIMED_OUT),
        message: Some(format!(
            "timed out waiting for broker {broker_id} to appear in the metadata"
        )),
    }
}

/// Send `request` to one broker until it answers, the error is final, or the
/// call deadline passes. A failed or lost connection, including a TLS or SASL
/// handshake with no verdict, connects again after the backoff, as Kafka's
/// `Call.fail` retries a `RetriableException`. The result of the last attempt
/// is the result at the deadline.
async fn call_broker<R>(
    host_port: &str,
    options: &ConnectionOptions,
    request: R,
    mut retry: CoordinatorRetry,
) -> BrokerResult<R::Response>
where
    R: ProtocolRequest + Clone,
{
    let mut connection: Option<Connection> = None;
    loop {
        let action = retry
            .run(broker_attempt(
                &mut connection,
                host_port,
                options,
                request.clone(),
            ))
            .await;
        if let Some(result) = retry.next(action).await {
            return result.map_err(|error| broker_error(host_port, &error));
        }
    }
}

/// The Kafka error of a failed broker call. A connection failure is the last
/// error of a call past its deadline, so it gives `REQUEST_TIMED_OUT` (7), as
/// Kafka's `Call.handleTimeoutFailure` gives a `TimeoutException`.
fn broker_error(host_port: &str, error: &AdminError) -> KafkaError {
    if is_connection_failure(error) {
        return KafkaError {
            code: REQUEST_TIMED_OUT,
            name: kafka_error_name(REQUEST_TIMED_OUT),
            message: Some(format!(
                "the call to broker {host_port} timed out; last error: {error}"
            )),
        };
    }
    list_groups_kafka_error(error)
}

/// One attempt of [`call_broker`]. A connection failure empties
/// `connection` and asks for another attempt.
async fn broker_attempt<R>(
    connection: &mut Option<Connection>,
    host_port: &str,
    options: &ConnectionOptions,
    request: R,
) -> RetryAction<R::Response>
where
    R: ProtocolRequest,
{
    let current = match connection {
        Some(current) => current,
        None => match AdminClient::connect_one(host_port, options.clone()).await {
            Ok(new) => connection.insert(new),
            Err(error) if is_connection_failure(&error) => {
                return RetryAction::SameCoordinator(Err(error));
            }
            Err(error) => return RetryAction::Done(Err(error)),
        },
    };
    match current.send(request).await {
        Ok(response) => RetryAction::Done(Ok(response)),
        Err(error) => {
            let error = AdminError::from(error);
            if is_connection_failure(&error) {
                *connection = None;
                RetryAction::SameCoordinator(Err(error))
            } else {
                RetryAction::Done(Err(error))
            }
        }
    }
}

/// The `AlterReplicaLogDirs` request of one broker. It lists each log dir
/// once, each topic once per log dir, and the partitions in order.
fn alter_request(moves: &BTreeMap<TopicPartitionReplica, String>) -> AlterReplicaLogDirsRequest {
    let mut dirs = BTreeMap::<&str, BTreeMap<&str, Vec<i32>>>::new();
    for (replica, path) in moves {
        dirs.entry(path.as_str())
            .or_default()
            .entry(replica.topic.as_str())
            .or_default()
            .push(replica.partition);
    }
    AlterReplicaLogDirsRequest {
        dirs: dirs
            .into_iter()
            .map(|(path, topics)| AlterReplicaLogDir {
                path: path.to_owned(),
                topics: topics
                    .into_iter()
                    .map(|(name, partitions)| AlterReplicaLogDirTopic {
                        name: name.to_owned(),
                        partitions,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// The result of each replica that `broker_id` moves. A result for a
/// partition that is not in the request is skipped, and a replica with no
/// result gets `UNKNOWN_SERVER_ERROR`, as Kafka does.
fn alter_results(
    broker_id: i32,
    moves: &BTreeMap<TopicPartitionReplica, String>,
    response: AlterReplicaLogDirsResponse,
) -> BTreeMap<TopicPartitionReplica, BrokerResult<()>> {
    let mut out = BTreeMap::new();
    for topic in response.results {
        for partition in topic.partitions {
            let replica = TopicPartitionReplica {
                topic: topic.topic_name.clone(),
                partition: partition.partition_index,
                broker_id,
            };
            if !moves.contains_key(&replica) {
                tracing::warn!(
                    broker_id,
                    topic = %replica.topic,
                    partition = replica.partition,
                    "the AlterReplicaLogDirs response names a partition that is not in the request"
                );
                continue;
            }
            let result = kafka_error_if(partition.error_code, None).map_or(Ok(()), Err);
            out.insert(replica, result);
        }
    }
    for replica in moves.keys() {
        out.entry(replica.clone()).or_insert_with(|| {
            Err(KafkaError {
                code: UNKNOWN_SERVER_ERROR,
                name: kafka_error_name(UNKNOWN_SERVER_ERROR),
                message: Some(format!(
                    "the response from broker {broker_id} did not contain a result for replica \
                     {}-{}",
                    replica.topic, replica.partition
                )),
            })
        });
    }
    out
}

fn describe_request(filter: Option<&BTreeMap<String, Vec<i32>>>) -> DescribeLogDirsRequest {
    DescribeLogDirsRequest {
        topics: filter.map(|filter| {
            filter
                .iter()
                .map(|(name, partitions)| DescribableLogDirTopic {
                    topic: name.clone(),
                    partitions: partitions.clone(),
                    ..Default::default()
                })
                .collect()
        }),
        ..Default::default()
    }
}

/// A volume size of the response, `None` for `-1`.
fn volume_bytes(bytes: i64) -> Option<ByteSize> {
    u64::try_from(bytes).ok().map(ByteSize::from_bytes)
}

/// The log dirs of one response. No log dir is an error of the broker, as
/// Kafka's `describeLogDirs` `handleResponse` makes it.
fn log_dir_infos(response: DescribeLogDirsResponse) -> BrokerResult<Vec<LogDirInfo>> {
    if response.results.is_empty() {
        let code = if response.error_code == 0 {
            CLUSTER_AUTHORIZATION_FAILED
        } else {
            response.error_code
        };
        return Err(KafkaError {
            code,
            name: kafka_error_name(code),
            message: None,
        });
    }
    Ok(response
        .results
        .into_iter()
        .map(|result| LogDirInfo {
            log_dir: result.log_dir,
            error: kafka_error_if(result.error_code, None),
            topics: result
                .topics
                .into_iter()
                .map(|topic| LogDirTopicInfo {
                    name: topic.name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|partition| LogDirPartitionInfo {
                            partition_index: partition.partition_index,
                            partition_size: partition.partition_size,
                            offset_lag: partition.offset_lag,
                            is_future_key: partition.is_future_key,
                        })
                        .collect(),
                })
                .collect(),
            total: volume_bytes(result.total_bytes),
            usable: volume_bytes(result.usable_bytes),
            is_cordoned: result.is_cordoned,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use assert2::assert;
    use bytes::{Buf, BytesMut};
    use krabka_client_core::MockBroker;
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            alter_replica_log_dirs_request,
            alter_replica_log_dirs_response::{
                AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
            },
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            describe_log_dirs_request,
            describe_log_dirs_response::{
                DescribeLogDirsPartition, DescribeLogDirsResult, DescribeLogDirsTopic,
            },
            metadata_request,
            metadata_response::{MetadataResponse, MetadataResponseBroker},
        },
    };

    use super::*;

    /// `KAFKA_STORAGE_ERROR`: a broker answers it for a replica it does not
    /// host.
    const KAFKA_STORAGE_ERROR: i16 = 56;
    const LONG: Duration = Duration::from_secs(5);
    const SHORT: Duration = Duration::from_millis(300);

    /// What each mock broker received: the broker id, and the request.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Received {
        Describe(i32, DescribeLogDirsRequest),
        Alter(i32, AlterReplicaLogDirsRequest),
    }

    /// How one mock broker of the cluster behaves.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Behavior {
        /// It answers for its own log dir and replicas.
        Normal,
        /// It answers `DescribeLogDirs` with no log dir and error code 0.
        NoLogDirs,
        /// The metadata names it at an address that refuses connections.
        Down,
    }

    fn encode(response: &impl Encode, version: i16, flexible: bool) -> Vec<u8> {
        let mut buf = BytesMut::new();
        if flexible {
            buf.extend_from_slice(&[0]);
        }
        response.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    fn decode<R: for<'de> Decode<'de>>(mut body: &[u8], version: i16, flexible: bool) -> R {
        let client_id_len = body.get_i16();
        body.advance(usize::try_from(client_id_len).expect("client id length"));
        if flexible {
            body.advance(1);
        }
        R::decode(&mut body, version).expect("request decodes")
    }

    fn api_versions() -> Vec<u8> {
        let api = |api_key, max_version| ApiVersion {
            api_key,
            min_version: 0,
            max_version,
            ..Default::default()
        };
        encode(
            &ApiVersionsResponse {
                api_keys: vec![
                    api(api_versions_request::API_KEY, 0),
                    api(metadata_request::API_KEY, 12),
                    api(describe_log_dirs_request::API_KEY, 4),
                    api(alter_replica_log_dirs_request::API_KEY, 2),
                ],
                ..Default::default()
            },
            0,
            false,
        )
    }

    fn log_dir(broker_id: i32) -> String {
        format!("/data/broker-{broker_id}")
    }

    /// The log dir that broker `broker_id` describes: partition
    /// `broker_id - 1` of `orders`, and a 1 GiB volume.
    fn described(broker_id: i32) -> Vec<LogDirInfo> {
        vec![LogDirInfo {
            log_dir: log_dir(broker_id),
            error: None,
            topics: vec![LogDirTopicInfo {
                name: "orders".into(),
                partitions: vec![LogDirPartitionInfo {
                    partition_index: broker_id - 1,
                    partition_size: 100,
                    offset_lag: 0,
                    is_future_key: false,
                }],
            }],
            total: Some(krabka_units::gibibytes(1)),
            usable: None,
            is_cordoned: false,
        }]
    }

    /// Start one broker of a two-broker cluster. Broker `broker_id` hosts
    /// partition `broker_id - 1` of `orders`, and `addresses` holds the
    /// address of each broker id that the metadata names.
    async fn cluster_broker(
        broker_id: i32,
        behavior: Behavior,
        addresses: Arc<Mutex<Vec<(i32, SocketAddr)>>>,
        received: Arc<Mutex<Vec<Received>>>,
    ) -> MockBroker {
        MockBroker::start(move |api_key, version, _, body| match api_key {
            api_versions_request::API_KEY => Some(api_versions()),
            metadata_request::API_KEY => Some(encode(
                &MetadataResponse {
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
                version >= metadata_request::FLEXIBLE_MIN,
            )),
            describe_log_dirs_request::API_KEY => {
                let flexible = version >= describe_log_dirs_request::FLEXIBLE_MIN;
                let request: DescribeLogDirsRequest = decode(body, version, flexible);
                received
                    .lock()
                    .expect("received lock")
                    .push(Received::Describe(broker_id, request));
                let results = if behavior == Behavior::NoLogDirs {
                    Vec::new()
                } else {
                    vec![DescribeLogDirsResult {
                        log_dir: log_dir(broker_id),
                        topics: vec![DescribeLogDirsTopic {
                            name: "orders".into(),
                            partitions: vec![DescribeLogDirsPartition {
                                partition_index: broker_id - 1,
                                partition_size: 100,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        total_bytes: 1 << 30,
                        usable_bytes: -1,
                        ..Default::default()
                    }]
                };
                Some(encode(
                    &DescribeLogDirsResponse {
                        results,
                        ..Default::default()
                    },
                    version,
                    flexible,
                ))
            }
            alter_replica_log_dirs_request::API_KEY => {
                let flexible = version >= alter_replica_log_dirs_request::FLEXIBLE_MIN;
                let request: AlterReplicaLogDirsRequest = decode(body, version, flexible);
                let results = request
                    .dirs
                    .iter()
                    .flat_map(|dir| dir.topics.iter())
                    .map(|topic| AlterReplicaLogDirTopicResult {
                        topic_name: topic.name.clone(),
                        partitions: topic
                            .partitions
                            .iter()
                            .map(|partition| AlterReplicaLogDirPartitionResult {
                                partition_index: *partition,
                                error_code: if *partition == broker_id - 1 {
                                    0
                                } else {
                                    KAFKA_STORAGE_ERROR
                                },
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    })
                    .collect();
                received
                    .lock()
                    .expect("received lock")
                    .push(Received::Alter(broker_id, request));
                Some(encode(
                    &AlterReplicaLogDirsResponse {
                        results,
                        ..Default::default()
                    },
                    version,
                    flexible,
                ))
            }
            _ => None,
        })
        .await
    }

    /// A running two-broker cluster.
    struct Cluster {
        brokers: Vec<MockBroker>,
        received: Arc<Mutex<Vec<Received>>>,
        admin: AdminClient,
    }

    impl Cluster {
        /// Start brokers 1 and 2. Broker 1 is the bootstrap broker.
        async fn start(broker_2: Behavior) -> Self {
            let addresses = Arc::new(Mutex::new(Vec::new()));
            let received = Arc::new(Mutex::new(Vec::new()));
            let one = cluster_broker(
                1,
                Behavior::Normal,
                Arc::clone(&addresses),
                Arc::clone(&received),
            )
            .await;
            let two =
                cluster_broker(2, broker_2, Arc::clone(&addresses), Arc::clone(&received)).await;
            let two_addr = if broker_2 == Behavior::Down {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind a port");
                listener.local_addr().expect("local address")
            } else {
                two.addr
            };
            *addresses.lock().expect("addresses lock") = vec![(1, one.addr), (2, two_addr)];
            let admin = AdminClient::connect(&[one.addr.to_string()])
                .await
                .expect("admin connects");
            Self {
                brokers: vec![one, two],
                received,
                admin,
            }
        }

        fn stop(self) -> Vec<Received> {
            for broker in self.brokers {
                broker.stop();
            }
            let mut received = self.received.lock().expect("received lock").clone();
            received.sort_by_key(|entry| match entry {
                Received::Describe(id, _) | Received::Alter(id, _) => *id,
            });
            received
        }
    }

    fn policy(timeout: Duration) -> RetryPolicy {
        let backoff = if timeout == LONG {
            Duration::from_millis(1)
        } else {
            timeout
        };
        RetryPolicy {
            timeout,
            initial_backoff: backoff,
            max_backoff: backoff,
            jitter: 0.0,
        }
    }

    /// The Kafka error code of each broker result.
    fn codes<T>(results: BTreeMap<i32, BrokerResult<T>>) -> BTreeMap<i32, Result<T, i16>> {
        results
            .into_iter()
            .map(|(broker_id, result)| (broker_id, result.map_err(|error| error.code)))
            .collect()
    }

    /// Kafka's `describeLogDirs` sends one `DescribeLogDirs` to each named
    /// broker (`ConstantNodeIdProvider`), waits for an unknown broker until
    /// the deadline, fails a broker that answers with no log dir with its
    /// error code or `CLUSTER_AUTHORIZATION_FAILED`, and times out a broker
    /// that it cannot reach.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_log_dirs_asks_each_broker_by_id() {
        let all = describe_request(None);
        for (name, broker_2, brokers, timeout, expected_requests, expected) in [
            (
                "both brokers",
                Behavior::Normal,
                vec![1, 2],
                LONG,
                vec![
                    Received::Describe(1, all.clone()),
                    Received::Describe(2, all.clone()),
                ],
                BTreeMap::from([(1, Ok(described(1))), (2, Ok(described(2)))]),
            ),
            (
                "one broker",
                Behavior::Normal,
                vec![2],
                LONG,
                vec![Received::Describe(2, all.clone())],
                BTreeMap::from([(2, Ok(described(2)))]),
            ),
            (
                "an unknown broker times out",
                Behavior::Normal,
                vec![3],
                SHORT,
                vec![],
                BTreeMap::from([(3, Err(REQUEST_TIMED_OUT))]),
            ),
            (
                "a broker with no log dir",
                Behavior::NoLogDirs,
                vec![1, 2],
                LONG,
                vec![
                    Received::Describe(1, all.clone()),
                    Received::Describe(2, all.clone()),
                ],
                BTreeMap::from([
                    (1, Ok(described(1))),
                    (2, Err(CLUSTER_AUTHORIZATION_FAILED)),
                ]),
            ),
            (
                "an unreachable broker times out",
                Behavior::Down,
                vec![1, 2],
                SHORT,
                vec![Received::Describe(1, all.clone())],
                BTreeMap::from([(1, Ok(described(1))), (2, Err(REQUEST_TIMED_OUT))]),
            ),
        ] {
            let mut cluster = Cluster::start(broker_2).await;
            let result = cluster
                .admin
                .describe_log_dirs_with_retry(&brokers, None, policy(timeout))
                .await
                .expect("metadata succeeds");
            let received = cluster.stop();
            assert!(
                (codes(result), received) == (expected, expected_requests),
                "case {name}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_log_dirs_sends_the_topic_filter() {
        let mut cluster = Cluster::start(Behavior::Normal).await;
        let filter = BTreeMap::from([("orders".to_string(), vec![0])]);
        cluster
            .admin
            .describe_log_dirs(&[1], Some(&filter))
            .await
            .expect("metadata succeeds");
        let received = cluster.stop();
        assert!(
            received
                == vec![Received::Describe(
                    1,
                    DescribeLogDirsRequest {
                        topics: Some(vec![DescribableLogDirTopic {
                            topic: "orders".into(),
                            partitions: vec![0],
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }
                )]
        );
    }

    fn replica(partition: i32, broker_id: i32) -> TopicPartitionReplica {
        TopicPartitionReplica {
            topic: "orders".into(),
            partition,
            broker_id,
        }
    }

    fn alter(dirs: &[(&str, &[i32])]) -> AlterReplicaLogDirsRequest {
        AlterReplicaLogDirsRequest {
            dirs: dirs
                .iter()
                .map(|(path, partitions)| AlterReplicaLogDir {
                    path: (*path).to_owned(),
                    topics: vec![AlterReplicaLogDirTopic {
                        name: "orders".into(),
                        partitions: partitions.to_vec(),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Kafka's `alterReplicaLogDirs` groups the moves by
    /// `TopicPartitionReplica.brokerId` and sends each group to its broker.
    /// A replica with no result in the response fails with
    /// `UNKNOWN_SERVER_ERROR`, and a broker that fails fails only its own
    /// replicas.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alter_replica_log_dirs_sends_each_move_to_its_broker() {
        for (name, broker_2, moves, timeout, expected_requests, expected) in [
            (
                "each move goes to the broker of its replica",
                Behavior::Normal,
                vec![(replica(0, 1), "/a"), (replica(1, 2), "/b")],
                LONG,
                vec![
                    Received::Alter(1, alter(&[("/a", &[0])])),
                    Received::Alter(2, alter(&[("/b", &[1])])),
                ],
                BTreeMap::from([(replica(0, 1), Ok(())), (replica(1, 2), Ok(()))]),
            ),
            (
                "a broker groups its moves by log dir",
                Behavior::Normal,
                vec![
                    (replica(0, 1), "/a"),
                    (replica(5, 1), "/a"),
                    (replica(6, 1), "/b"),
                ],
                LONG,
                vec![Received::Alter(1, alter(&[("/a", &[0, 5]), ("/b", &[6])]))],
                BTreeMap::from([
                    (replica(0, 1), Ok(())),
                    (replica(5, 1), Err(KAFKA_STORAGE_ERROR)),
                    (replica(6, 1), Err(KAFKA_STORAGE_ERROR)),
                ]),
            ),
            (
                "an unreachable broker fails only its replicas",
                Behavior::Down,
                vec![(replica(0, 1), "/a"), (replica(1, 2), "/b")],
                SHORT,
                vec![Received::Alter(1, alter(&[("/a", &[0])]))],
                BTreeMap::from([
                    (replica(0, 1), Ok(())),
                    (replica(1, 2), Err(REQUEST_TIMED_OUT)),
                ]),
            ),
            (
                "an unknown broker times out",
                Behavior::Normal,
                vec![(replica(0, 9), "/a")],
                SHORT,
                vec![],
                BTreeMap::from([(replica(0, 9), Err(REQUEST_TIMED_OUT))]),
            ),
        ] {
            let mut cluster = Cluster::start(broker_2).await;
            let assignments = moves
                .into_iter()
                .map(|(replica, path)| (replica, path.to_owned()))
                .collect();
            let result = cluster
                .admin
                .alter_replica_log_dirs_with_retry(&assignments, policy(timeout))
                .await
                .expect("metadata succeeds")
                .into_iter()
                .map(|(replica, result)| (replica, result.map_err(|error| error.code)))
                .collect::<BTreeMap<_, _>>();
            let received = cluster.stop();
            assert!(
                (result, received) == (expected, expected_requests),
                "case {name}"
            );
        }
    }

    #[test]
    fn a_replica_without_a_result_fails_with_unknown_server_error() {
        let moves = BTreeMap::from([
            (replica(0, 1), "/a".to_owned()),
            (replica(1, 1), "/a".to_owned()),
        ]);
        let response = AlterReplicaLogDirsResponse {
            results: vec![AlterReplicaLogDirTopicResult {
                topic_name: "orders".into(),
                partitions: vec![
                    AlterReplicaLogDirPartitionResult {
                        partition_index: 0,
                        ..Default::default()
                    },
                    AlterReplicaLogDirPartitionResult {
                        partition_index: 7,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(
            alter_results(1, &moves, response)
                == BTreeMap::from([
                    (replica(0, 1), Ok(())),
                    (
                        replica(1, 1),
                        Err(KafkaError {
                            code: UNKNOWN_SERVER_ERROR,
                            name: "UNKNOWN_SERVER_ERROR",
                            message: Some(
                                "the response from broker 1 did not contain a result for replica \
                                 orders-1"
                                    .into()
                            ),
                        })
                    ),
                ])
        );
    }

    #[test]
    fn a_log_dir_reports_its_volume_sizes() {
        let response = DescribeLogDirsResponse {
            results: vec![DescribeLogDirsResult {
                log_dir: "/d".into(),
                error_code: 57,
                total_bytes: 4096,
                usable_bytes: 1024,
                is_cordoned: true,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(
            log_dir_infos(response)
                == Ok(vec![LogDirInfo {
                    log_dir: "/d".into(),
                    error: kafka_error_if(57, None),
                    topics: vec![],
                    total: Some(krabka_units::kibibytes(4)),
                    usable: Some(krabka_units::kibibytes(1)),
                    is_cordoned: true,
                }])
        );
    }
}
