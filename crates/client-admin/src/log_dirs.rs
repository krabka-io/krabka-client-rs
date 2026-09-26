//! KIP-113 admin RPCs: `AlterReplicaLogDirs` (`api_key` 34) and
//! `DescribeLogDirs` (`api_key` 35).
//!
//! Both act only on the broker that receives the request. As Kafka's
//! `KafkaAdminClient.describeLogDirs` and `alterReplicaLogDirs` do, the admin
//! client finds each broker by id in the metadata and sends one request to
//! each broker (`ConstantNodeIdProvider`). A broker that fails gives an error
//! for that broker only.

use std::collections::{BTreeMap, BTreeSet};

use futures_util::{StreamExt as _, stream::FuturesUnordered};
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
    AdminClient, AdminError, KafkaError, RecoveringConnection, format_host_port,
    groups::list_groups_kafka_error,
    kafka_error_if, kafka_error_name,
    retry::{CoordinatorRetry, RetryAction, RetryPolicy, is_connection_failure},
};

/// `UNKNOWN_SERVER_ERROR`.
const UNKNOWN_SERVER_ERROR: i16 = -1;
/// `REQUEST_TIMED_OUT`: Kafka's `TimeoutException`.
const REQUEST_TIMED_OUT: i16 = 7;
/// `CLUSTER_AUTHORIZATION_FAILED`.
const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
/// `KAFKA_STORAGE_ERROR`: the log dir is offline, or the broker does not
/// host the replica.
const KAFKA_STORAGE_ERROR: i16 = 56;

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

/// Where one replica of a partition lives on its broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaLogDir {
    /// The absolute path of the log dir.
    pub path: String,
    /// How far the replica lags behind the log end offset of the partition
    /// on this broker (for the current replica) or behind the current
    /// replica (for a future replica).
    pub offset_lag: i64,
}

/// The log dirs of one replica, as Kafka's
/// `DescribeReplicaLogDirsResult.ReplicaLogDirInfo`. A replica that the
/// broker does not hold, or holds only in an offline log dir, has neither.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicaLogDirInfo {
    /// The log dir of the current replica.
    pub current: Option<ReplicaLogDir>,
    /// The log dir that an `AlterReplicaLogDirs` move is copying the replica
    /// to (KIP-113).
    pub future: Option<ReplicaLogDir>,
}

impl AdminClient {
    /// `AlterReplicaLogDirs` (KIP-113): moves replicas between the
    /// `log.dirs` of their brokers, as Kafka's
    /// `KafkaAdminClient.alterReplicaLogDirs` does.
    ///
    /// `assignments` maps each replica to the absolute path of its target log
    /// dir. The call groups the replicas by broker id and sends one request to
    /// each broker, all at the same time. The result has one entry for each
    /// replica:
    ///
    /// - The error code of the broker for that partition, `Ok(())` for 0.
    /// - `UNKNOWN_SERVER_ERROR` (-1) when the response of the broker has no
    ///   result for the replica (`completeUnrealizedFutures`).
    /// - The error of the broker call for every replica of a broker that
    ///   fails. The call finds each broker by id in fresh metadata for each
    ///   connection attempt, so a broker that is missing from the metadata or
    ///   that moved to a new address is found again. A missing broker and a
    ///   failed or lost connection are tried again with Kafka's backoff until
    ///   `default.api.timeout.ms` (60 s), and then give `REQUEST_TIMED_OUT`
    ///   (7). A slow or missing broker does not delay the other brokers.
    pub async fn alter_replica_log_dirs(
        &mut self,
        assignments: &BTreeMap<TopicPartitionReplica, String>,
    ) -> BTreeMap<TopicPartitionReplica, BrokerResult<()>> {
        self.alter_replica_log_dirs_with_retry(assignments, self.retry)
            .await
    }

    async fn alter_replica_log_dirs_with_retry(
        &mut self,
        assignments: &BTreeMap<TopicPartitionReplica, String>,
        retry: RetryPolicy,
    ) -> BTreeMap<TopicPartitionReplica, BrokerResult<()>> {
        let start = retry.start();
        let mut by_broker = BTreeMap::<i32, BTreeMap<TopicPartitionReplica, String>>::new();
        for (replica, path) in assignments {
            by_broker
                .entry(replica.broker_id)
                .or_default()
                .insert(replica.clone(), path.clone());
        }
        let (conn, options) = (&self.conn, &self.options);
        let answers = futures_util::future::join_all(by_broker.iter().map(|(broker_id, moves)| {
            call_broker(
                conn,
                *broker_id,
                options,
                alter_request(moves),
                CoordinatorRetry::from_deadline(start),
            )
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
        out
    }

    /// `DescribeLogDirs` (KIP-113): lists every configured `log.dir` of each
    /// broker in `brokers`, with the partitions each one holds, as Kafka's
    /// `KafkaAdminClient.describeLogDirs` does.
    ///
    /// Pass `None` to fetch all partitions, as Kafka does. Pass `Some` with a
    /// topic to partitions filter to narrow the result. An empty inner vec
    /// means all partitions of that topic.
    ///
    /// The result has one entry for each broker id, and the brokers are asked
    /// at the same time. A broker that answers with no log dir gives its
    /// top-level error code, or `CLUSTER_AUTHORIZATION_FAILED` (31) when it
    /// has none, as Kafka does. The call finds each broker by id in fresh
    /// metadata for each connection attempt. A missing broker and a failed or
    /// lost connection are tried again with Kafka's backoff until
    /// `default.api.timeout.ms` (60 s), and then give `REQUEST_TIMED_OUT` (7).
    pub async fn describe_log_dirs(
        &mut self,
        brokers: &[i32],
        filter: Option<&BTreeMap<String, Vec<i32>>>,
    ) -> BTreeMap<i32, BrokerResult<Vec<LogDirInfo>>> {
        self.describe_log_dirs_with_retry(brokers, filter, self.retry)
            .await
    }

    async fn describe_log_dirs_with_retry(
        &mut self,
        brokers: &[i32],
        filter: Option<&BTreeMap<String, Vec<i32>>>,
        retry: RetryPolicy,
    ) -> BTreeMap<i32, BrokerResult<Vec<LogDirInfo>>> {
        let start = retry.start();
        let broker_ids = brokers.iter().copied().collect::<BTreeSet<_>>();
        let request = describe_request(filter);
        let (conn, options) = (&self.conn, &self.options);
        let answers = futures_util::future::join_all(broker_ids.iter().map(|broker_id| {
            let request = request.clone();
            async move {
                let response = call_broker(
                    conn,
                    *broker_id,
                    options,
                    request,
                    CoordinatorRetry::from_deadline(start),
                )
                .await?;
                log_dir_infos(response)
            }
        }))
        .await;
        broker_ids.into_iter().zip(answers).collect()
    }

    /// `DescribeLogDirs` for single replicas: finds the log dir of each
    /// replica of `replicas`, and of its future replica when one is being
    /// moved, as Kafka's `KafkaAdminClient.describeReplicaLogDirs` does.
    ///
    /// The call groups the replicas by broker id and sends each broker one
    /// `DescribeLogDirs` request that names only its partitions, all at the
    /// same time. Each broker is found and retried as in
    /// [`Self::describe_log_dirs`]. The result has one entry for each
    /// replica, and the brokers complete their replicas in the order they
    /// answer, as Kafka completes the replica futures:
    ///
    /// - The log dirs that the broker reports for the partition. A log dir
    ///   with `KAFKA_STORAGE_ERROR` (56) is offline and skipped, as Kafka
    ///   does, so a replica that lives only there has neither log dir. A
    ///   replica that the answer does not name has neither log dir.
    /// - The top-level error code of the answer is not read, as Kafka's
    ///   `describeReplicaLogDirs` does not read it: a `DescribeLogDirs` v3+
    ///   answer with `CLUSTER_AUTHORIZATION_FAILED` (31) and no log dir gives
    ///   each replica of that broker neither log dir.
    /// - A log dir with any other error fails the call, as Kafka's
    ///   `handleFailure` with an `IllegalStateException` completes every
    ///   replica future of the call that is still pending: the replicas of
    ///   that broker and of every broker that has not answered yet get that
    ///   error. The replicas of brokers that answered before keep their log
    ///   dirs.
    /// - A broker call that fails, with `REQUEST_TIMED_OUT` (7) at
    ///   `default.api.timeout.ms` or another error, fails every pending
    ///   replica of the call in the same way.
    ///
    /// The call returns once no replica is pending; it does not wait for the
    /// brokers that have not answered after a failure.
    pub async fn describe_replica_log_dirs(
        &mut self,
        replicas: &[TopicPartitionReplica],
    ) -> BTreeMap<TopicPartitionReplica, BrokerResult<ReplicaLogDirInfo>> {
        self.describe_replica_log_dirs_with_retry(replicas, self.retry)
            .await
    }

    async fn describe_replica_log_dirs_with_retry(
        &mut self,
        replicas: &[TopicPartitionReplica],
        retry: RetryPolicy,
    ) -> BTreeMap<TopicPartitionReplica, BrokerResult<ReplicaLogDirInfo>> {
        let start = retry.start();
        let mut by_broker = BTreeMap::<i32, BTreeMap<String, BTreeSet<i32>>>::new();
        for replica in replicas {
            by_broker
                .entry(replica.broker_id)
                .or_default()
                .entry(replica.topic.clone())
                .or_default()
                .insert(replica.partition);
        }
        let (conn, options) = (&self.conn, &self.options);
        let mut results = ReplicaResults::new(&by_broker);
        let mut answers = by_broker
            .iter()
            .map(|(broker_id, topics)| async move {
                let answer = call_broker(
                    conn,
                    *broker_id,
                    options,
                    replica_request(topics),
                    CoordinatorRetry::from_deadline(start),
                )
                .await;
                (
                    *broker_id,
                    topics,
                    answer.and_then(|response| replica_log_dirs(*broker_id, topics, response)),
                )
            })
            .collect::<FuturesUnordered<_>>();
        while !results.is_complete()
            && let Some((broker_id, topics, answer)) = answers.next().await
        {
            results.complete(broker_id, topics, answer);
        }
        results.finish()
    }
}

/// The result of each replica of a `describe_replica_log_dirs` call, as
/// Kafka's replica futures: a broker's answer completes the replicas of that
/// broker that are still pending, and a failure completes every replica of
/// the call that is still pending (`completeAllExceptionally`).
#[derive(Debug)]
struct ReplicaResults {
    pending: BTreeSet<TopicPartitionReplica>,
    done: BTreeMap<TopicPartitionReplica, BrokerResult<ReplicaLogDirInfo>>,
}

impl ReplicaResults {
    fn new(by_broker: &BTreeMap<i32, BTreeMap<String, BTreeSet<i32>>>) -> Self {
        let pending = by_broker
            .iter()
            .flat_map(|(broker_id, topics)| {
                topics.iter().flat_map(move |(topic, partitions)| {
                    partitions
                        .iter()
                        .map(move |partition| TopicPartitionReplica {
                            topic: topic.clone(),
                            partition: *partition,
                            broker_id: *broker_id,
                        })
                })
            })
            .collect();
        Self {
            pending,
            done: BTreeMap::new(),
        }
    }

    /// Complete the replicas with the answer of broker `broker_id`, which the
    /// request asked about `topics`.
    fn complete(
        &mut self,
        broker_id: i32,
        topics: &BTreeMap<String, BTreeSet<i32>>,
        answer: BrokerResult<BTreeMap<(String, i32), ReplicaLogDirInfo>>,
    ) {
        match answer {
            Ok(mut infos) => {
                for (topic, partitions) in topics {
                    for partition in partitions {
                        let replica = TopicPartitionReplica {
                            topic: topic.clone(),
                            partition: *partition,
                            broker_id,
                        };
                        if self.pending.remove(&replica) {
                            let info = infos
                                .remove(&(topic.clone(), *partition))
                                .unwrap_or_default();
                            self.done.insert(replica, Ok(info));
                        }
                    }
                }
            }
            Err(error) => {
                for replica in std::mem::take(&mut self.pending) {
                    self.done.insert(replica, Err(error.clone()));
                }
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.pending.is_empty()
    }

    fn finish(self) -> BTreeMap<TopicPartitionReplica, BrokerResult<ReplicaLogDirInfo>> {
        self.done
    }
}

/// The `DescribeLogDirs` request of one broker for the partitions of
/// `topics`, as Kafka's `describeReplicaLogDirs` builds it.
fn replica_request(topics: &BTreeMap<String, BTreeSet<i32>>) -> DescribeLogDirsRequest {
    DescribeLogDirsRequest {
        topics: Some(
            topics
                .iter()
                .map(|(topic, partitions)| DescribableLogDirTopic {
                    topic: topic.clone(),
                    partitions: partitions.iter().copied().collect(),
                    ..Default::default()
                })
                .collect(),
        ),
        ..Default::default()
    }
}

/// The log dirs of each asked-for partition in one broker's answer, as
/// Kafka's `describeReplicaLogDirs` `handleResponse` reads them. The
/// top-level error code is not read, as Kafka does not read it. A partition
/// that the request did not name is skipped with a warning.
fn replica_log_dirs(
    broker_id: i32,
    topics: &BTreeMap<String, BTreeSet<i32>>,
    response: DescribeLogDirsResponse,
) -> BrokerResult<BTreeMap<(String, i32), ReplicaLogDirInfo>> {
    let mut out = BTreeMap::<(String, i32), ReplicaLogDirInfo>::new();
    for dir in response.results {
        if dir.error_code == KAFKA_STORAGE_ERROR {
            continue;
        }
        if dir.error_code != 0 {
            return Err(KafkaError {
                code: dir.error_code,
                name: kafka_error_name(dir.error_code),
                message: Some(format!(
                    "the error {} for log directory {} in the response from broker {broker_id} \
                     is illegal",
                    kafka_error_name(dir.error_code),
                    dir.log_dir
                )),
            });
        }
        for topic in dir.topics {
            for partition in topic.partitions {
                let asked = topics
                    .get(&topic.name)
                    .is_some_and(|partitions| partitions.contains(&partition.partition_index));
                if !asked {
                    tracing::warn!(
                        broker_id,
                        topic = %topic.name,
                        partition = partition.partition_index,
                        "the DescribeLogDirs response names a partition that is not in the request"
                    );
                    continue;
                }
                let info = out
                    .entry((topic.name.clone(), partition.partition_index))
                    .or_default();
                let location = Some(ReplicaLogDir {
                    path: dir.log_dir.clone(),
                    offset_lag: partition.offset_lag,
                });
                if partition.is_future_key {
                    info.future = location;
                } else {
                    info.current = location;
                }
            }
        }
    }
    Ok(out)
}

/// Send `request` to broker `broker_id` until it answers, the error is
/// final, or the call deadline passes, as one Kafka `Call` with a
/// `ConstantNodeIdProvider` does.
///
/// Each connection attempt looks the broker up in fresh metadata. A broker
/// that the metadata does not name, and a failed or lost connection
/// (including a TLS or SASL handshake with no verdict), are tried again after
/// the backoff, as Kafka's `Call.fail` retries a `RetriableException`.
async fn call_broker<R>(
    conn: &RecoveringConnection,
    broker_id: i32,
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
                conn,
                broker_id,
                options,
                request.clone(),
            ))
            .await;
        if let Some(result) = retry.next(action).await {
            return result.map_err(|error| broker_error(broker_id, &error));
        }
    }
}

/// The Kafka error of a failed broker call. A timeout is the error of a call
/// past its deadline, so it gives `REQUEST_TIMED_OUT` (7), as Kafka's
/// `Call.handleTimeoutFailure` gives a `TimeoutException`.
fn broker_error(broker_id: i32, error: &AdminError) -> KafkaError {
    if is_connection_failure(error) {
        return KafkaError {
            code: REQUEST_TIMED_OUT,
            name: kafka_error_name(REQUEST_TIMED_OUT),
            message: Some(format!("the call to broker {broker_id} timed out: {error}")),
        };
    }
    list_groups_kafka_error(error)
}

/// One attempt of [`call_broker`]. Without a connection, the attempt finds
/// the broker in the metadata and connects to it. A missing broker or a
/// connection failure empties `connection` and asks for another attempt.
async fn broker_attempt<R>(
    connection: &mut Option<Connection>,
    conn: &RecoveringConnection,
    broker_id: i32,
    options: &ConnectionOptions,
    request: R,
) -> RetryAction<R::Response>
where
    R: ProtocolRequest,
{
    if connection.is_none() {
        let endpoint = match broker_endpoint(conn, broker_id).await {
            Ok(Some(endpoint)) => endpoint,
            Ok(None) => {
                tracing::debug!(broker_id, "the broker is not in the metadata; retrying");
                return RetryAction::SameCoordinator(Err(AdminError::Broker {
                    api: "Metadata",
                    code: REQUEST_TIMED_OUT,
                    name: kafka_error_name(REQUEST_TIMED_OUT),
                    message: Some(format!("broker {broker_id} is not in the metadata")),
                }));
            }
            Err(error) if is_connection_failure(&error) => {
                return RetryAction::SameCoordinator(Err(error));
            }
            Err(error) => return RetryAction::Done(Err(error)),
        };
        match AdminClient::connect_one(&endpoint, options.clone()).await {
            Ok(new) => *connection = Some(new),
            Err(error) if is_connection_failure(&error) => {
                return RetryAction::SameCoordinator(Err(error));
            }
            Err(error) => return RetryAction::Done(Err(error)),
        }
    }
    let Some(current) = connection.as_ref() else {
        return RetryAction::SameCoordinator(Err(AdminError::Transport(
            krabka_client_core::ClientError::Disconnected,
        )));
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

/// The `host:port` of `broker_id` in fresh metadata, or `None` when the
/// metadata does not name it.
async fn broker_endpoint(
    conn: &RecoveringConnection,
    broker_id: i32,
) -> Result<Option<String>, AdminError> {
    let response = conn
        .send(MetadataRequest {
            topics: Some(Vec::new()),
            allow_auto_topic_creation: true,
            ..Default::default()
        })
        .await?;
    Ok(response
        .brokers
        .into_iter()
        .find(|broker| broker.node_id == broker_id)
        .map(|broker| format_host_port(&broker.host, broker.port)))
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
        /// The first metadata response names it at an address that refuses
        /// connections, and later responses name its real address, as after
        /// a restart with a new advertised address.
        Moved,
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
        addresses: Arc<Mutex<Addresses>>,
        received: Arc<Mutex<Vec<Received>>>,
    ) -> MockBroker {
        MockBroker::start(move |api_key, version, _, body| match api_key {
            api_versions_request::API_KEY => Some(api_versions()),
            metadata_request::API_KEY => Some(encode(
                &MetadataResponse {
                    brokers: addresses
                        .lock()
                        .expect("addresses lock")
                        .next()
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

    /// The broker addresses that the metadata names. `first` is used for the
    /// first metadata response only.
    #[derive(Default)]
    struct Addresses {
        first: Option<Vec<(i32, SocketAddr)>>,
        later: Vec<(i32, SocketAddr)>,
    }

    impl Addresses {
        fn next(&mut self) -> Vec<(i32, SocketAddr)> {
            self.first.take().unwrap_or_else(|| self.later.clone())
        }
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
            let addresses = Arc::new(Mutex::new(Addresses::default()));
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
            let refused = {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind a port");
                listener.local_addr().expect("local address")
            };
            *addresses.lock().expect("addresses lock") = match broker_2 {
                Behavior::Down => Addresses {
                    first: None,
                    later: vec![(1, one.addr), (2, refused)],
                },
                Behavior::Moved => Addresses {
                    first: Some(vec![(1, one.addr), (2, refused)]),
                    later: vec![(1, one.addr), (2, two.addr)],
                },
                Behavior::Normal | Behavior::NoLogDirs => Addresses {
                    first: None,
                    later: vec![(1, one.addr), (2, two.addr)],
                },
            };
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
            max_retries: u32::MAX,
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
                "a known broker is asked while an unknown broker waits",
                Behavior::Normal,
                vec![1, 3],
                SHORT,
                vec![Received::Describe(1, all.clone())],
                BTreeMap::from([(1, Ok(described(1))), (3, Err(REQUEST_TIMED_OUT))]),
            ),
            (
                "a broker at a new address is found again",
                Behavior::Moved,
                vec![2],
                LONG,
                vec![Received::Describe(2, all.clone())],
                BTreeMap::from([(2, Ok(described(2)))]),
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
                .await;
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
        cluster.admin.describe_log_dirs(&[1], Some(&filter)).await;
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

    fn replica_request_of(broker: &[(&str, &[i32])]) -> DescribeLogDirsRequest {
        DescribeLogDirsRequest {
            topics: Some(
                broker
                    .iter()
                    .map(|(topic, partitions)| DescribableLogDirTopic {
                        topic: (*topic).to_owned(),
                        partitions: partitions.to_vec(),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }

    /// The current log dir of the replica that broker `broker_id` hosts in
    /// the mock cluster.
    fn hosted(broker_id: i32) -> ReplicaLogDirInfo {
        ReplicaLogDirInfo {
            current: Some(ReplicaLogDir {
                path: log_dir(broker_id),
                offset_lag: 0,
            }),
            future: None,
        }
    }

    /// Kafka's `describeReplicaLogDirs` sends each broker one
    /// `DescribeLogDirs` that names only its replicas, finds a moved broker
    /// again, gives an empty `ReplicaLogDirInfo` to a replica the broker does
    /// not report, and times out a broker it cannot reach.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_replica_log_dirs_asks_the_broker_of_each_replica() {
        for (name, broker_2, replicas, timeout, expected_requests, expected) in [
            (
                "each replica is asked of its broker",
                Behavior::Normal,
                vec![replica(0, 1), replica(5, 1), replica(1, 2)],
                LONG,
                vec![
                    Received::Describe(1, replica_request_of(&[("orders", &[0, 5])])),
                    Received::Describe(2, replica_request_of(&[("orders", &[1])])),
                ],
                BTreeMap::from([
                    (replica(0, 1), Ok(hosted(1))),
                    (replica(5, 1), Ok(ReplicaLogDirInfo::default())),
                    (replica(1, 2), Ok(hosted(2))),
                ]),
            ),
            (
                "a broker at a new address is found again",
                Behavior::Moved,
                vec![replica(1, 2)],
                LONG,
                vec![Received::Describe(
                    2,
                    replica_request_of(&[("orders", &[1])]),
                )],
                BTreeMap::from([(replica(1, 2), Ok(hosted(2)))]),
            ),
            (
                "a broker with no log dir reports no replica",
                Behavior::NoLogDirs,
                vec![replica(1, 2)],
                LONG,
                vec![Received::Describe(
                    2,
                    replica_request_of(&[("orders", &[1])]),
                )],
                BTreeMap::from([(replica(1, 2), Ok(ReplicaLogDirInfo::default()))]),
            ),
            (
                "an unreachable broker fails the replicas still pending",
                Behavior::Down,
                vec![replica(0, 1), replica(1, 2)],
                SHORT,
                vec![Received::Describe(
                    1,
                    replica_request_of(&[("orders", &[0])]),
                )],
                BTreeMap::from([
                    (replica(0, 1), Ok(hosted(1))),
                    (replica(1, 2), Err(REQUEST_TIMED_OUT)),
                ]),
            ),
        ] {
            let mut cluster = Cluster::start(broker_2).await;
            let result = cluster
                .admin
                .describe_replica_log_dirs_with_retry(&replicas, policy(timeout))
                .await
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

    /// How Kafka's `describeReplicaLogDirs` `handleResponse` reads one
    /// answer: a future replica fills the future slot, an offline log dir is
    /// skipped, any other log-dir error fails the answer, and the top-level
    /// error code is not read.
    #[test]
    fn replica_log_dirs_reads_current_and_future_replicas() {
        let dir =
            |path: &str, error_code, partitions: Vec<(i32, i64, bool)>| DescribeLogDirsResult {
                error_code,
                log_dir: path.to_owned(),
                topics: vec![DescribeLogDirsTopic {
                    name: "orders".into(),
                    partitions: partitions
                        .into_iter()
                        .map(|(partition_index, offset_lag, is_future_key)| {
                            DescribeLogDirsPartition {
                                partition_index,
                                offset_lag,
                                is_future_key,
                                ..Default::default()
                            }
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            };
        let at = |path: &str, offset_lag| {
            Some(ReplicaLogDir {
                path: path.to_owned(),
                offset_lag,
            })
        };
        let asked = BTreeMap::from([("orders".to_owned(), BTreeSet::from([0, 1]))]);
        for (name, error_code, results, expected) in [
            (
                "a replica being moved has a current and a future log dir",
                0,
                vec![
                    dir("/a", 0, vec![(0, 0, false), (1, 3, false)]),
                    dir("/b", 0, vec![(0, 42, true), (9, 0, false)]),
                ],
                Ok(BTreeMap::from([
                    (
                        ("orders".to_owned(), 0),
                        ReplicaLogDirInfo {
                            current: at("/a", 0),
                            future: at("/b", 42),
                        },
                    ),
                    (
                        ("orders".to_owned(), 1),
                        ReplicaLogDirInfo {
                            current: at("/a", 3),
                            future: None,
                        },
                    ),
                ])),
            ),
            (
                "an offline log dir is skipped",
                0,
                vec![
                    dir("/offline", KAFKA_STORAGE_ERROR, vec![(0, 0, false)]),
                    dir("/a", 0, vec![(1, 0, false)]),
                ],
                Ok(BTreeMap::from([(
                    ("orders".to_owned(), 1),
                    ReplicaLogDirInfo {
                        current: at("/a", 0),
                        future: None,
                    },
                )])),
            ),
            (
                "another log dir error fails the broker",
                0,
                vec![dir("/a", 57, vec![])],
                Err(KafkaError {
                    code: 57,
                    name: "LOG_DIR_NOT_FOUND",
                    message: Some(
                        "the error LOG_DIR_NOT_FOUND for log directory /a in the response from \
                         broker 1 is illegal"
                            .to_owned(),
                    ),
                }),
            ),
            (
                "the top-level error is not read",
                CLUSTER_AUTHORIZATION_FAILED,
                vec![dir("/a", 0, vec![(1, 0, false)])],
                Ok(BTreeMap::from([(
                    ("orders".to_owned(), 1),
                    ReplicaLogDirInfo {
                        current: at("/a", 0),
                        future: None,
                    },
                )])),
            ),
        ] {
            let response = DescribeLogDirsResponse {
                error_code,
                results,
                ..Default::default()
            };
            assert!(
                replica_log_dirs(1, &asked, response) == expected,
                "case {name}"
            );
        }
    }

    /// Kafka's replica futures: an answer completes the pending replicas of
    /// its broker, and a failure (an illegal log-dir error or a failed call)
    /// completes every replica of the call that is still pending with that
    /// error. Replicas that are complete keep their result.
    #[test]
    fn a_failure_fails_every_replica_still_pending() {
        let asked = |partitions: &[i32]| {
            BTreeMap::from([(
                "orders".to_owned(),
                partitions.iter().copied().collect::<BTreeSet<_>>(),
            )])
        };
        let by_broker = BTreeMap::from([(1, asked(&[0, 5])), (2, asked(&[1]))]);
        let found = |partition: i32, broker_id: i32| {
            (
                ("orders".to_owned(), partition),
                ReplicaLogDirInfo {
                    current: Some(ReplicaLogDir {
                        path: log_dir(broker_id),
                        offset_lag: 0,
                    }),
                    future: None,
                },
            )
        };
        let answer = |broker_id: i32, partition: i32| {
            (broker_id, Ok(BTreeMap::from([found(partition, broker_id)])))
        };
        let illegal = KafkaError {
            code: 57,
            name: "LOG_DIR_NOT_FOUND",
            message: Some("illegal".to_owned()),
        };
        let timed_out = KafkaError {
            code: REQUEST_TIMED_OUT,
            name: "REQUEST_TIMED_OUT",
            message: None,
        };
        let hosted = |partition, broker_id| Ok(found(partition, broker_id).1);
        for (name, answers, expected) in [
            (
                "every broker answers",
                vec![answer(2, 1), answer(1, 0)],
                BTreeMap::from([
                    (replica(0, 1), hosted(0, 1)),
                    (replica(5, 1), Ok(ReplicaLogDirInfo::default())),
                    (replica(1, 2), hosted(1, 2)),
                ]),
            ),
            (
                "an illegal log dir after an answer",
                vec![answer(1, 0), (2, Err(illegal.clone()))],
                BTreeMap::from([
                    (replica(0, 1), hosted(0, 1)),
                    (replica(5, 1), Ok(ReplicaLogDirInfo::default())),
                    (replica(1, 2), Err(illegal.clone())),
                ]),
            ),
            (
                "an illegal log dir before an answer",
                vec![(2, Err(illegal.clone())), answer(1, 0)],
                BTreeMap::from([
                    (replica(0, 1), Err(illegal.clone())),
                    (replica(5, 1), Err(illegal.clone())),
                    (replica(1, 2), Err(illegal.clone())),
                ]),
            ),
            (
                "a failed call before an answer",
                vec![(1, Err(timed_out.clone())), answer(2, 1)],
                BTreeMap::from([
                    (replica(0, 1), Err(timed_out.clone())),
                    (replica(5, 1), Err(timed_out.clone())),
                    (replica(1, 2), Err(timed_out.clone())),
                ]),
            ),
        ] {
            let mut results = ReplicaResults::new(&by_broker);
            let mut completed_after = Vec::new();
            for (broker_id, answer) in answers {
                results.complete(broker_id, &by_broker[&broker_id], answer);
                completed_after.push(results.is_complete());
            }
            assert!(
                (completed_after.last().copied(), results.finish()) == (Some(true), expected),
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
