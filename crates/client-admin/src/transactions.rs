//! Transaction administration.

use std::collections::BTreeMap;

use bytes::BufMut;
use krabka_client_core::{
    Connection, ConnectionOptions, CoordinatorKeyType, build_find_coordinator,
};
use krabka_protocol::{
    Encode, ProtocolError, ProtocolRequest,
    owned::{
        describe_producers_request::{
            DescribeProducersRequest, TopicRequest as DescribeProducersTopicRequest,
        },
        describe_producers_response::{
            DescribeProducersResponse, ProducerState as WireProducerState,
        },
        describe_transactions_request::DescribeTransactionsRequest,
        describe_transactions_response::{DescribeTransactionsResponse, TransactionState},
        find_coordinator_response::FindCoordinatorResponse,
        init_producer_id_request::{self, InitProducerIdRequest},
        init_producer_id_response::InitProducerIdResponse,
        list_transactions_request::ListTransactionsRequest,
        list_transactions_response::{
            ListTransactionsResponse, TransactionState as ListedTransactionState,
        },
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::MetadataResponse,
        write_txn_markers_request::{
            WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequest,
        },
        write_txn_markers_response::WriteTxnMarkersResponse,
    },
    primitives::uuid::Uuid as ProtoUuid,
};
use krabka_units::{Time, convert::TimeExt as _};

use crate::{
    AdminClient, AdminError, KafkaError, format_host_port, kafka_error_name,
    partition_leaders::{PartitionKey, PartitionResults, complete_results},
    retry::{CoordinatorRetry, RetryAction, RetryDeadline, RetryPolicy, connection_failure_action},
};

/// `COORDINATOR_LOAD_IN_PROGRESS`: the coordinator is loading its state.
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
/// `COORDINATOR_NOT_AVAILABLE`: no broker coordinates the transactional ID now.
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// `NOT_COORDINATOR`: the broker does not coordinate the transactional ID.
const NOT_COORDINATOR: i16 = 16;
/// `CONCURRENT_TRANSACTIONS`: the coordinator is completing a transaction.
const CONCURRENT_TRANSACTIONS: i16 = 51;
/// `LEADER_NOT_AVAILABLE`: the metadata has no leader for a partition yet.
const LEADER_NOT_AVAILABLE: i16 = 5;
/// `NOT_LEADER_OR_FOLLOWER`: the broker is no longer (or not yet) the leader
/// of a partition.
const NOT_LEADER_OR_FOLLOWER: i16 = 6;

/// Map the result of one `DescribeTransactions` attempt to a retry action, as
/// Kafka's `DescribeTransactionsHandler.handleError` does. 14 retries on the
/// same coordinator. 15 and 16 unmap the transactional ID, so the next attempt
/// finds the coordinator again. Every other result is final.
fn describe_retry_action(
    result: Result<TransactionDescription, AdminError>,
) -> RetryAction<TransactionDescription> {
    match result {
        Err(AdminError::Broker {
            code: COORDINATOR_LOAD_IN_PROGRESS,
            ..
        }) => RetryAction::SameCoordinator(result),
        Err(AdminError::Broker {
            code: COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR,
            ..
        }) => RetryAction::FindCoordinator(result),
        result => RetryAction::Done(result),
    }
}

/// Map the error code of one `InitProducerId` answer to a retry action, as
/// Kafka's `FenceProducersHandler.handleError` does. 14 and 51 retry on the
/// same coordinator. 15 and 16 unmap the transactional ID, so the next attempt
/// finds the coordinator again. Every other code is final.
fn fence_retry_action(error_code: i16) -> RetryAction<()> {
    if error_code == 0 {
        return RetryAction::Done(Ok(()));
    }
    let result = Err(AdminError::Broker {
        api: "InitProducerId",
        code: error_code,
        name: kafka_error_name(error_code),
        message: None,
    });
    match error_code {
        COORDINATOR_LOAD_IN_PROGRESS | CONCURRENT_TRANSACTIONS => {
            RetryAction::SameCoordinator(result)
        }
        COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR => RetryAction::FindCoordinator(result),
        _ => RetryAction::Done(result),
    }
}

/// The transaction coordinator's view of one transactional ID.
///
/// A third party reads [`producer_id`](Self::producer_id) and
/// [`producer_epoch`](Self::producer_epoch) to verify the authority of a
/// writer. The coordinator raises the epoch each time it fences the previous
/// producer generation, so a writer that presents an older epoch no longer
/// holds authority over the transactional ID.
#[derive(Debug, Clone, PartialEq)]
pub struct TransactionDescription {
    /// The transactional ID that the caller asked about.
    pub transactional_id: String,
    /// The coordinator's state name, such as `Ongoing` or `CompleteCommit`.
    pub state: String,
    /// The transaction timeout that the producer registered.
    pub timeout: Time,
    /// The start of the current transaction, in Kafka epoch milliseconds. An
    /// instant is a coordinate, so it stays a raw integer.
    pub start_time_ms: i64,
    /// The producer ID that the coordinator holds for this transactional ID.
    pub producer_id: i64,
    /// The producer epoch that the coordinator holds for this transactional
    /// ID.
    pub producer_epoch: i16,
}

/// One transaction that `list_transactions` found on a broker, as Kafka's
/// `TransactionListing`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionListing {
    /// The transactional ID.
    pub transactional_id: String,
    /// The producer ID currently bound to the transactional ID.
    pub producer_id: i64,
    /// The coordinator's state name, such as `Ongoing` or `CompleteCommit`.
    pub state: String,
}

/// Narrows a [`AdminClient::list_transactions`] call, as Kafka's
/// `ListTransactionsOptions` does. The default matches every transaction, as
/// Kafka's default options do.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ListTransactionsFilter {
    /// Only transactions whose state name is in this list. Empty matches
    /// every state.
    pub state_filters: Vec<String>,
    /// Only transactions whose producer ID is in this list. Empty matches
    /// every producer ID.
    pub producer_id_filters: Vec<i64>,
    /// Only transactions that have run for at least this long. `None`
    /// matches every duration, as Kafka's `-1` filter does.
    pub min_duration: Option<Time>,
}

/// One producer's state on one partition, as Kafka's `ProducerState`
/// (`DescribeProducers`, KIP-664).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerStateInfo {
    /// The producer ID.
    pub producer_id: i64,
    /// The producer epoch.
    pub producer_epoch: i32,
    /// The last sequence number the producer wrote, or `-1` when it has
    /// written nothing yet.
    pub last_sequence: i32,
    /// The timestamp of the last write, in Kafka epoch milliseconds. An
    /// instant is a coordinate, so it stays a raw integer.
    pub last_timestamp_ms: i64,
    /// The epoch of the coordinator that last fenced this producer.
    pub coordinator_epoch: i32,
    /// The start offset of the producer's open transaction on this
    /// partition, or `None` when it has no open transaction.
    pub current_txn_start_offset: Option<i64>,
}

fn coordinator_address(
    transactional_id: &str,
    response: FindCoordinatorResponse,
) -> Result<String, AdminError> {
    if let Some(coordinator) = response
        .coordinators
        .into_iter()
        .find(|coordinator| coordinator.key == transactional_id)
    {
        if coordinator.error_code != 0 {
            return Err(AdminError::Broker {
                api: "FindCoordinator",
                code: coordinator.error_code,
                name: kafka_error_name(coordinator.error_code),
                message: coordinator.error_message,
            });
        }
        return Ok(format!("{}:{}", coordinator.host, coordinator.port));
    }

    if response.error_code != 0 {
        return Err(AdminError::Broker {
            api: "FindCoordinator",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: response.error_message,
        });
    }
    if response.host.is_empty() {
        return Err(AdminError::Protocol(format!(
            "FindCoordinator returned no entry for transactional id {transactional_id:?}"
        )));
    }
    Ok(format!("{}:{}", response.host, response.port))
}

/// An `InitProducerId` request limited to the released versions, v0 to v5.
///
/// `InitProducerIdRequest.json` marks v6 as its unstable latest version
/// (`"latestVersionUnstable": true`, KIP-939), and krabka-protocol's
/// generated `MAX_VERSION` includes v6. Apache Kafka's
/// `FenceProducersHandler.buildSingleRequest` uses
/// `new InitProducerIdRequest.Builder(data)`. That builder calls
/// `AbstractRequest.Builder(ApiKeys.INIT_PRODUCER_ID)`, which allows
/// `apiKey.latestVersion(false)`, the latest released version. This type
/// gives the same cap to version negotiation.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ReleasedInitProducerId(InitProducerIdRequest);

impl Encode for ReleasedInitProducerId {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for ReleasedInitProducerId {
    const API_KEY: i16 = init_producer_id_request::API_KEY;
    const MIN_VERSION: i16 = init_producer_id_request::MIN_VERSION;
    /// The latest released `InitProducerId` version.
    const MAX_VERSION: i16 = 5;
    /// The cap is a released version, so it is also the stable maximum.
    const LATEST_STABLE_VERSION: i16 = Self::MAX_VERSION;
    const FLEXIBLE_MIN: i16 = init_producer_id_request::FLEXIBLE_MIN;
    type Response = InitProducerIdResponse;
}

fn force_terminate_request(
    transactional_id: &str,
    transaction_timeout_ms: i32,
) -> ReleasedInitProducerId {
    ReleasedInitProducerId(InitProducerIdRequest {
        transactional_id: Some(transactional_id.to_owned()),
        transaction_timeout_ms,
        producer_id: -1,
        producer_epoch: -1,
        enable2_pc: false,
        keep_prepared_txn: false,
        ..Default::default()
    })
}

fn describe_transactions_request(transactional_id: &str) -> DescribeTransactionsRequest {
    DescribeTransactionsRequest {
        transactional_ids: vec![transactional_id.to_owned()],
        ..Default::default()
    }
}

/// Converts one coordinator row into the domain struct, or surfaces the error
/// code that the coordinator attached to that row.
fn described_transaction(state: TransactionState) -> Result<TransactionDescription, AdminError> {
    if state.error_code != 0 {
        return Err(AdminError::Broker {
            api: "DescribeTransactions",
            code: state.error_code,
            name: kafka_error_name(state.error_code),
            message: None,
        });
    }
    Ok(TransactionDescription {
        transactional_id: state.transactional_id,
        state: state.transaction_state,
        timeout: Time::from_millis(i64::from(state.transaction_timeout_ms)),
        start_time_ms: state.transaction_start_time_ms,
        producer_id: state.producer_id,
        producer_epoch: state.producer_epoch,
    })
}

/// Picks the row for `transactional_id` out of a `DescribeTransactions`
/// response and maps it to the domain struct.
fn transaction_description(
    transactional_id: &str,
    response: DescribeTransactionsResponse,
) -> Result<TransactionDescription, AdminError> {
    response
        .transaction_states
        .into_iter()
        .find(|state| state.transactional_id == transactional_id)
        .ok_or_else(|| {
            AdminError::Protocol(format!(
                "DescribeTransactions returned no state for transactional id {transactional_id:?}"
            ))
        })
        .and_then(described_transaction)
}

/// A `Metadata` request for `topics`, as Kafka's leader-routed handlers send
/// it. An empty slice asks for the broker list only, with no topics, as
/// `list_transactions` uses it.
fn build_topic_metadata(topics: &[&str]) -> MetadataRequest {
    MetadataRequest {
        topics: if topics.is_empty() {
            Some(Vec::new())
        } else {
            Some(
                topics
                    .iter()
                    .map(|name| MetadataRequestTopic {
                        topic_id: ProtoUuid::ZERO,
                        name: Some((*name).to_owned()),
                        ..Default::default()
                    })
                    .collect(),
            )
        },
        allow_auto_topic_creation: false,
        ..Default::default()
    }
}

/// The `host:port` of the leader of `topic`-`partition` in `metadata`, as
/// Kafka's `PartitionLeaderStrategy` resolves it.
fn partition_leader_address(
    topic: &str,
    partition: i32,
    metadata: &MetadataResponse,
) -> Result<String, AdminError> {
    let topic_metadata = metadata
        .topics
        .iter()
        .find(|entry| entry.name.as_deref() == Some(topic))
        .ok_or_else(|| {
            AdminError::Protocol(format!("Metadata returned no entry for topic {topic:?}"))
        })?;
    if topic_metadata.error_code != 0 {
        return Err(AdminError::Broker {
            api: "Metadata",
            code: topic_metadata.error_code,
            name: kafka_error_name(topic_metadata.error_code),
            message: None,
        });
    }
    let partition_metadata = topic_metadata
        .partitions
        .iter()
        .find(|entry| entry.partition_index == partition)
        .ok_or_else(|| {
            AdminError::Protocol(format!(
                "Metadata returned no entry for partition {topic}-{partition}"
            ))
        })?;
    if partition_metadata.error_code != 0 {
        return Err(AdminError::Broker {
            api: "Metadata",
            code: partition_metadata.error_code,
            name: kafka_error_name(partition_metadata.error_code),
            message: None,
        });
    }
    metadata
        .brokers
        .iter()
        .find(|broker| broker.node_id == partition_metadata.leader_id)
        .map(|broker| format_host_port(&broker.host, broker.port))
        .ok_or_else(|| {
            AdminError::Protocol(format!(
                "Metadata named leader {} for {topic}-{partition} but listed no such broker",
                partition_metadata.leader_id
            ))
        })
}

/// The identity of a transaction to abort, as Kafka's `AbortTransactionSpec`
/// bundles it: the partition it last wrote to, and the producer generation
/// that owns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortTransactionSpec {
    /// The topic that the transaction wrote to.
    pub topic: String,
    /// The partition that the transaction wrote to.
    pub partition: i32,
    /// The producer ID of the transaction to fence.
    pub producer_id: i64,
    /// The producer epoch of the transaction to fence.
    pub producer_epoch: i16,
    /// The epoch of the coordinator that assigned `producer_id`.
    pub coordinator_epoch: i32,
}

/// The `WriteTxnMarkers` request that aborts the transaction of `spec`.
fn abort_transaction_request(spec: &AbortTransactionSpec) -> WriteTxnMarkersRequest {
    WriteTxnMarkersRequest {
        markers: vec![WritableTxnMarker {
            producer_id: spec.producer_id,
            producer_epoch: spec.producer_epoch,
            transaction_result: false,
            topics: vec![WritableTxnMarkerTopic {
                name: spec.topic.clone(),
                partition_indexes: vec![spec.partition],
                ..Default::default()
            }],
            coordinator_epoch: spec.coordinator_epoch,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Picks the result of `spec`'s topic and partition out of a `WriteTxnMarkers`
/// response and maps it to an [`AdminError`].
fn abort_transaction_result(
    spec: &AbortTransactionSpec,
    response: WriteTxnMarkersResponse,
) -> Result<(), AdminError> {
    let marker = response
        .markers
        .into_iter()
        .find(|marker| marker.producer_id == spec.producer_id)
        .ok_or_else(|| {
            AdminError::Protocol(format!(
                "WriteTxnMarkers returned no result for producer id {}",
                spec.producer_id
            ))
        })?;
    let topic_result = marker
        .topics
        .into_iter()
        .find(|entry| entry.name == spec.topic)
        .ok_or_else(|| {
            AdminError::Protocol(format!(
                "WriteTxnMarkers returned no result for topic {:?}",
                spec.topic
            ))
        })?;
    let partition_result = topic_result
        .partitions
        .into_iter()
        .find(|entry| entry.partition_index == spec.partition)
        .ok_or_else(|| {
            AdminError::Protocol(format!(
                "WriteTxnMarkers returned no result for partition {}-{}",
                spec.topic, spec.partition
            ))
        })?;
    if partition_result.error_code != 0 {
        return Err(AdminError::Broker {
            api: "WriteTxnMarkers",
            code: partition_result.error_code,
            name: kafka_error_name(partition_result.error_code),
            message: None,
        });
    }
    Ok(())
}

/// Maps the outcome of one `WriteTxnMarkers` attempt to a retry action.
/// `NOT_LEADER_OR_FOLLOWER` and `LEADER_NOT_AVAILABLE` look the leader up
/// again, as Kafka's `AbortTransactionHandler.handleResponse` does. Every
/// other outcome is final.
fn abort_transaction_retry_action(result: Result<(), AdminError>) -> RetryAction<()> {
    match result {
        Err(AdminError::Broker {
            code: NOT_LEADER_OR_FOLLOWER | LEADER_NOT_AVAILABLE,
            ..
        }) => RetryAction::FindCoordinator(result),
        result => RetryAction::Done(result),
    }
}

/// The `DescribeProducers` request that asks for every partition in `keys`,
/// grouped by topic, as Kafka's `DescribeProducersHandler.buildBatchedRequest`
/// does.
fn describe_producers_request(keys: &[(String, i32)]) -> DescribeProducersRequest {
    let mut by_topic = BTreeMap::<&str, Vec<i32>>::new();
    for (topic, partition) in keys {
        by_topic.entry(topic.as_str()).or_default().push(*partition);
    }
    DescribeProducersRequest {
        topics: by_topic
            .into_iter()
            .map(|(name, partition_indexes)| DescribeProducersTopicRequest {
                name: name.to_owned(),
                partition_indexes,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// Converts one wire `ProducerState` row into the domain struct. `-1` marks
/// no open transaction, as Kafka's `ProducerState` does.
fn producer_state_info(state: &WireProducerState) -> ProducerStateInfo {
    ProducerStateInfo {
        producer_id: state.producer_id,
        producer_epoch: state.producer_epoch,
        last_sequence: state.last_sequence,
        last_timestamp_ms: state.last_timestamp,
        coordinator_epoch: state.coordinator_epoch,
        current_txn_start_offset: (state.current_txn_start_offset != -1)
            .then_some(state.current_txn_start_offset),
    }
}

/// Maps a `DescribeProducers` response to a result for each of `keys`. A
/// partition that the response omits gets `UNKNOWN_SERVER_ERROR`, as Kafka's
/// `DescribeProducersHandler.handleResponse` does for a missing result.
fn describe_producers_results(
    keys: &[PartitionKey],
    response: DescribeProducersResponse,
) -> PartitionResults<Vec<ProducerStateInfo>> {
    let mut out = BTreeMap::new();
    for topic in response.topics {
        for partition in topic.partitions {
            let result = if partition.error_code == 0 {
                Ok(partition
                    .active_producers
                    .iter()
                    .map(producer_state_info)
                    .collect())
            } else {
                Err(KafkaError {
                    code: partition.error_code,
                    name: kafka_error_name(partition.error_code),
                    message: partition.error_message,
                })
            };
            out.insert((topic.name.clone(), partition.partition_index), result);
        }
    }
    complete_results("DescribeProducers", keys, out)
}

/// Whether a `DescribeProducers` partition code sends the partition back to
/// the leader lookup. Kafka's `DescribeProducersHandler.handlePartitionError`
/// unmaps the partition on `NOT_LEADER_OR_FOLLOWER` only.
const fn describe_producers_retry_code(code: i16) -> bool {
    code == NOT_LEADER_OR_FOLLOWER
}

/// Sends one `DescribeProducers` request for `keys` to the leader at
/// `address`.
async fn describe_producers_on_leader(
    address: String,
    options: ConnectionOptions,
    keys: Vec<PartitionKey>,
) -> Result<PartitionResults<Vec<ProducerStateInfo>>, AdminError> {
    let connection = AdminClient::connect_one(&address, options).await?;
    let response = connection.send(describe_producers_request(&keys)).await?;
    Ok(describe_producers_results(&keys, response))
}

/// The `ListTransactions` request of `filter`, as Kafka's
/// `ListTransactionsHandler.buildRequest` builds it for every broker.
fn list_transactions_request(filter: &ListTransactionsFilter) -> ListTransactionsRequest {
    ListTransactionsRequest {
        state_filters: filter.state_filters.clone(),
        producer_id_filters: filter.producer_id_filters.clone(),
        duration_filter: filter
            .min_duration
            .map_or(-1, krabka_units::convert::TimeExt::millis_i64),
        ..Default::default()
    }
}

/// Converts one wire `TransactionState` row into the domain struct.
fn transaction_listing(state: ListedTransactionState) -> TransactionListing {
    TransactionListing {
        transactional_id: state.transactional_id,
        producer_id: state.producer_id,
        state: state.transaction_state,
    }
}

/// Sends `request` to one broker and returns its transaction listings, or the
/// broker's top-level error.
async fn list_transactions_on_broker(
    address: String,
    options: ConnectionOptions,
    request: ListTransactionsRequest,
) -> Result<Vec<TransactionListing>, AdminError> {
    let connection = AdminClient::connect_one(&address, options).await?;
    let response: ListTransactionsResponse = connection.send(request).await?;
    if response.error_code != 0 {
        return Err(AdminError::Broker {
            api: "ListTransactions",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: None,
        });
    }
    Ok(response
        .transaction_states
        .into_iter()
        .map(transaction_listing)
        .collect())
}

impl AdminClient {
    /// Fences the current producer generation and aborts any ongoing
    /// transaction for `transactional_id`.
    ///
    /// This is Kafka's `forceTerminateTransaction` operation: it discovers the
    /// transaction coordinator and sends `InitProducerId` with no producer
    /// identity and `keepPreparedTxn=false`. It is safe to call when no
    /// transaction is open; the coordinator still advances the producer
    /// generation so stale writers are fenced.
    ///
    /// The client negotiates `InitProducerId` v5 or lower, as Kafka's
    /// `FenceProducersHandler` does. v6 is an unstable version.
    ///
    /// The call retries as Apache Kafka's `FenceProducersHandler.handleError` does:
    ///
    /// - `COORDINATOR_LOAD_IN_PROGRESS` (14) and `CONCURRENT_TRANSACTIONS` (51): send the request again to the same coordinator.
    /// - `COORDINATOR_NOT_AVAILABLE` (15) and `NOT_COORDINATOR` (16): find the
    ///   coordinator again, then send the request again.
    ///
    /// A `FindCoordinator` answer of 14 or 15, and a failed or lost connection
    /// to the coordinator (including a TLS or SASL handshake that stops
    /// before the broker gives a verdict), also make the call find the
    /// coordinator again. The call waits between attempts with Kafka's
    /// backoff, and it stops when Kafka's default `default.api.timeout.ms`
    /// (60 s) elapses. A call whose retries are unresolved at the deadline, or
    /// whose attempt is still running, fails with a timeout error, as Kafka
    /// gives a `TimeoutException`.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Protocol`] for an empty transactional ID, or the
    /// coordinator lookup, connection, transport, and broker errors returned
    /// by Kafka. Returns [`ClientError::IncompatibleVersion`] when the
    /// coordinator does not support `InitProducerId` v5 or lower.
    ///
    /// [`ClientError::IncompatibleVersion`]: krabka_client_core::ClientError::IncompatibleVersion
    pub async fn force_terminate_transaction(
        &self,
        transactional_id: &str,
    ) -> Result<(), AdminError> {
        if transactional_id.is_empty() {
            return Err(AdminError::Protocol(
                "transactional id must not be empty".to_owned(),
            ));
        }

        self.force_terminate_transaction_with_retry(transactional_id, self.retry)
            .await
    }

    async fn force_terminate_transaction_with_retry(
        &self,
        transactional_id: &str,
        retry: RetryPolicy,
    ) -> Result<(), AdminError> {
        self.force_terminate_transaction_until(transactional_id, retry.start())
            .await
    }

    /// `force_terminate_transaction` with a call deadline that already runs,
    /// so that every ID of one `fence_producers` call shares one deadline.
    async fn force_terminate_transaction_until(
        &self,
        transactional_id: &str,
        deadline: RetryDeadline,
    ) -> Result<(), AdminError> {
        let mut coordinator = None;
        let mut retry = CoordinatorRetry::from_deadline(deadline);
        loop {
            let action = retry
                .run(self.force_terminate_attempt(
                    transactional_id,
                    &mut coordinator,
                    retry.find_coordinator(),
                ))
                .await;
            if let Some(result) = retry.next(action).await {
                return result;
            }
        }
    }

    /// Fences the current producer generation and aborts any ongoing
    /// transaction for each ID in `transactional_ids`, as Kafka's
    /// `fenceProducers` operation does.
    ///
    /// Two transactional IDs can live on two different coordinators, so the
    /// client fences them at the same time rather than one at a time. All IDs
    /// share one call deadline, as Kafka's `FenceProducersHandler` and
    /// `AdminApiDriver` run one `invokeDriver` for the whole call. The result
    /// has one entry for each ID in `transactional_ids`, in no particular
    /// order.
    ///
    /// See [`Self::force_terminate_transaction`] for the retry behavior and
    /// the `InitProducerId` version that the client negotiates.
    ///
    /// # Errors
    ///
    /// Each entry gets [`AdminError::Protocol`] for an empty transactional
    /// ID, or the coordinator lookup, connection, transport, and broker
    /// errors returned by Kafka for that ID.
    pub async fn fence_producers(
        &self,
        transactional_ids: &[&str],
    ) -> BTreeMap<String, Result<(), AdminError>> {
        let deadline = self.retry.start();
        futures_util::future::join_all(transactional_ids.iter().map(|transactional_id| {
            let transactional_id = (*transactional_id).to_owned();
            async move {
                let result = if transactional_id.is_empty() {
                    Err(AdminError::Protocol(
                        "transactional id must not be empty".to_owned(),
                    ))
                } else {
                    self.force_terminate_transaction_until(&transactional_id, deadline)
                        .await
                };
                (transactional_id, result)
            }
        }))
        .await
        .into_iter()
        .collect()
    }

    /// Fences the current producer generation and aborts the ongoing
    /// transaction that wrote to `topic`-`partition`, without contacting the
    /// transaction coordinator.
    ///
    /// This is Kafka's `abortTransaction` operation (KIP-664): it sends
    /// `WriteTxnMarkers` straight to the partition leader with the identity of
    /// the transaction to fence, as `AbortTransactionHandler` does. A
    /// consumer that reads `describe_producers` or the partition's records
    /// supplies `spec`.
    ///
    /// The call retries as Apache Kafka's
    /// `AbortTransactionHandler.handleResponse` does: `NOT_LEADER_OR_FOLLOWER`
    /// (6) and `LEADER_NOT_AVAILABLE` (5), from either the leader lookup or
    /// the `WriteTxnMarkers` answer, look the leader up again. A failed or
    /// lost connection to the leader does the same. Every other broker error
    /// is final. The call waits between attempts with Kafka's backoff, and it
    /// stops when Kafka's default `default.api.timeout.ms` (60 s) elapses.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Protocol`] for an empty topic name, or when the
    /// broker's answer names no result for the requested producer, topic, or
    /// partition. Returns [`AdminError::Broker`] when the leader lookup or the
    /// `WriteTxnMarkers` call fails, such as with
    /// `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` or `FENCED_INSTANCE_ID`.
    /// Returns [`AdminError::Transport`] when the connection to the leader
    /// fails.
    pub async fn abort_transaction(&self, spec: &AbortTransactionSpec) -> Result<(), AdminError> {
        if spec.topic.is_empty() {
            return Err(AdminError::Protocol("topic must not be empty".to_owned()));
        }

        let mut leader = None;
        let mut retry = CoordinatorRetry::new(self.retry);
        loop {
            let action = retry
                .run(self.abort_transaction_attempt(spec, &mut leader, retry.find_coordinator()))
                .await;
            if let Some(result) = retry.next(action).await {
                return result;
            }
        }
    }

    /// One attempt of `abort_transaction`.
    async fn abort_transaction_attempt(
        &self,
        spec: &AbortTransactionSpec,
        leader: &mut Option<Connection>,
        find_leader: bool,
    ) -> RetryAction<()> {
        let connection = match self
            .partition_leader_connection(&spec.topic, spec.partition, leader, find_leader)
            .await
        {
            Ok(connection) => connection,
            Err(action) => return action,
        };
        match connection.send(abort_transaction_request(spec)).await {
            Ok(response) => {
                abort_transaction_retry_action(abort_transaction_result(spec, response))
            }
            Err(error) => connection_failure_action(error.into()),
        }
    }

    /// Returns the connection to the leader of `topic`-`partition`. When
    /// `find_leader` is set, or when no connection is open, the attempt first
    /// fetches fresh metadata and connects to the leader that it names.
    async fn partition_leader_connection<'c, T>(
        &self,
        topic: &str,
        partition: i32,
        leader: &'c mut Option<Connection>,
        find_leader: bool,
    ) -> Result<&'c Connection, RetryAction<T>> {
        if find_leader {
            *leader = None;
        }
        if let Some(connection) = leader {
            return Ok(connection);
        }
        let metadata = self
            .conn
            .send(build_topic_metadata(&[topic]))
            .await
            .map_err(connection_failure_action)?;
        let address = match partition_leader_address(topic, partition, &metadata) {
            Ok(address) => address,
            Err(
                error @ AdminError::Broker {
                    code: NOT_LEADER_OR_FOLLOWER | LEADER_NOT_AVAILABLE,
                    ..
                },
            ) => return Err(RetryAction::FindCoordinator(Err(error))),
            Err(error) => return Err(RetryAction::Done(Err(error))),
        };
        match Self::connect_one(&address, self.options.clone()).await {
            Ok(connection) => Ok(leader.insert(connection)),
            Err(error) => Err(connection_failure_action(error)),
        }
    }

    /// Reads the producer state of each requested partition, as Kafka's
    /// `describeProducers` operation does (KIP-664).
    ///
    /// The client finds the leader of each partition with `Metadata` and
    /// sends one `DescribeProducers` request to each leader, all at the same
    /// time, batching every partition of that leader into it, as Kafka's
    /// `DescribeProducersHandler` with its `PartitionLeaderStrategy` does. The
    /// result has one entry for each requested `(topic, partition)` pair.
    ///
    /// A partition goes back to the leader lookup, and the call finds its
    /// leader again after the backoff, when:
    ///
    /// - the lookup names no leader yet, such as for an unknown topic or a
    ///   partition with `LEADER_NOT_AVAILABLE`;
    /// - the connection to its leader fails or is lost;
    /// - the leader answers `NOT_LEADER_OR_FOLLOWER` (6) for it.
    ///
    /// A slow or failed leader does not delay the other leaders. The call
    /// stops at Kafka's default `default.api.timeout.ms` (60 s), where each
    /// unresolved partition gets `REQUEST_TIMED_OUT` (7).
    ///
    /// # Errors
    ///
    /// The call itself does not fail. Each partition gets its own
    /// [`KafkaError`] in the result, such as the final lookup or
    /// `DescribeProducers` error of that partition.
    pub async fn describe_producers(
        &self,
        partitions: &[(String, i32)],
    ) -> BTreeMap<(String, i32), Result<Vec<ProducerStateInfo>, KafkaError>> {
        self.call_partition_leaders(
            "DescribeProducers",
            partitions,
            describe_producers_retry_code,
            |address, keys| describe_producers_on_leader(address, self.options.clone(), keys),
        )
        .await
    }

    /// Lists every transaction that the cluster's coordinators currently
    /// hold, as Kafka's `listTransactions` operation does (KIP-664).
    ///
    /// The client fetches the broker list from `Metadata` and sends
    /// `ListTransactions` to every broker at the same time, since each
    /// transaction coordinator answers only for the transactions it
    /// coordinates. `filter` narrows the state, producer ID, and minimum
    /// duration of the listed transactions, as Kafka's
    /// `ListTransactionsOptions` does; the default matches every transaction.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Broker`] when any broker gives a top-level
    /// error, and [`AdminError::Transport`] when the connection to any broker
    /// fails. A partial listing from the healthy brokers is not returned;
    /// callers that need per-broker results should read
    /// [`AdminError`](AdminError) for which broker failed and retry.
    pub async fn list_transactions(
        &self,
        filter: &ListTransactionsFilter,
    ) -> Result<Vec<TransactionListing>, AdminError> {
        let metadata = self.conn.send(build_topic_metadata(&[])).await?;
        let request = list_transactions_request(filter);
        let options = self.options.clone();
        let answers = futures_util::future::join_all(metadata.brokers.into_iter().map(|broker| {
            list_transactions_on_broker(
                format_host_port(&broker.host, broker.port),
                options.clone(),
                request.clone(),
            )
        }))
        .await;

        let mut listings = Vec::new();
        for answer in answers {
            listings.extend(answer?);
        }
        Ok(listings)
    }

    /// One attempt of `force_terminate_transaction`.
    async fn force_terminate_attempt(
        &self,
        transactional_id: &str,
        coordinator: &mut Option<Connection>,
        find_coordinator: bool,
    ) -> RetryAction<()> {
        let connection = match self
            .transaction_coordinator(transactional_id, coordinator, find_coordinator)
            .await
        {
            Ok(connection) => connection,
            Err(action) => return action,
        };
        let request =
            force_terminate_request(transactional_id, self.options.request_timeout.millis_i32());
        match connection.send(request).await {
            Ok(response) => fence_retry_action(response.error_code),
            Err(error) => connection_failure_action(error.into()),
        }
    }

    /// Reads the transaction coordinator's current state for one transactional
    /// ID.
    ///
    /// This is Kafka's `describeTransactions` operation (KIP-664). The client
    /// discovers the transaction coordinator for `transactional_id` and sends
    /// `DescribeTransactions` to that coordinator. An external system calls
    /// this to verify the authority of a writer. It joins no group and holds
    /// no producer state. The returned producer ID and producer epoch are the
    /// only generation from which the coordinator accepts writes.
    ///
    /// The call retries as Apache Kafka's `DescribeTransactionsHandler.handleError` does:
    ///
    /// - `COORDINATOR_LOAD_IN_PROGRESS` (14): send the request again to the same coordinator.
    /// - `COORDINATOR_NOT_AVAILABLE` (15) and `NOT_COORDINATOR` (16): find the
    ///   coordinator again, then send the request again.
    ///
    /// A `FindCoordinator` answer of 14 or 15, and a failed or lost connection
    /// to the coordinator (including a TLS or SASL handshake that stops
    /// before the broker gives a verdict), also make the call find the
    /// coordinator again. The call waits between attempts with Kafka's
    /// backoff, and it stops when Kafka's default `default.api.timeout.ms`
    /// (60 s) elapses. A call whose retries are unresolved at the deadline, or
    /// whose attempt is still running, fails with a timeout error, as Kafka
    /// gives a `TimeoutException`.
    ///
    /// # Errors
    ///
    /// Returns [`AdminError::Protocol`] for an empty transactional ID, and for
    /// a response that carries no row for `transactional_id`. Returns
    /// [`AdminError::Broker`] when the coordinator lookup fails, or when the
    /// coordinator attaches an error code to the row, such as
    /// `TRANSACTIONAL_ID_NOT_FOUND`,
    /// `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`, or `NOT_COORDINATOR`. Returns
    /// [`AdminError::Transport`] when the connection to the coordinator fails.
    pub async fn describe_transaction(
        &self,
        transactional_id: &str,
    ) -> Result<TransactionDescription, AdminError> {
        if transactional_id.is_empty() {
            return Err(AdminError::Protocol(
                "transactional id must not be empty".to_owned(),
            ));
        }

        self.describe_transaction_with_retry(transactional_id, self.retry)
            .await
    }

    async fn describe_transaction_with_retry(
        &self,
        transactional_id: &str,
        retry: RetryPolicy,
    ) -> Result<TransactionDescription, AdminError> {
        self.describe_transaction_until(transactional_id, retry.start())
            .await
    }

    /// `describe_transaction` with a call deadline that already runs, so that
    /// the IDs of one `describe_transactions` call share one deadline.
    async fn describe_transaction_until(
        &self,
        transactional_id: &str,
        deadline: RetryDeadline,
    ) -> Result<TransactionDescription, AdminError> {
        let mut coordinator = None;
        let mut retry = CoordinatorRetry::from_deadline(deadline);
        loop {
            let action = retry
                .run(self.describe_transaction_attempt(
                    transactional_id,
                    &mut coordinator,
                    retry.find_coordinator(),
                ))
                .await;
            if let Some(result) = retry.next(action).await {
                return result;
            }
        }
    }

    /// One attempt of `describe_transaction`.
    async fn describe_transaction_attempt(
        &self,
        transactional_id: &str,
        coordinator: &mut Option<Connection>,
        find_coordinator: bool,
    ) -> RetryAction<TransactionDescription> {
        let connection = match self
            .transaction_coordinator(transactional_id, coordinator, find_coordinator)
            .await
        {
            Ok(connection) => connection,
            Err(action) => return action,
        };
        match connection
            .send(describe_transactions_request(transactional_id))
            .await
        {
            Ok(response) => {
                describe_retry_action(transaction_description(transactional_id, response))
            }
            Err(error) => connection_failure_action(error.into()),
        }
    }

    /// Returns the connection to the transaction coordinator of
    /// `transactional_id`. When `find_coordinator` is set, or when no
    /// connection is open, the attempt first sends `FindCoordinator` and
    /// connects to the coordinator that it names.
    ///
    /// A `FindCoordinator` answer of `COORDINATOR_LOAD_IN_PROGRESS` (14) or
    /// `COORDINATOR_NOT_AVAILABLE` (15) and a failed connection to the
    /// coordinator give [`RetryAction::FindCoordinator`], as Kafka's
    /// `CoordinatorStrategy.handleError` and `AdminApiDriver.onFailure` do.
    async fn transaction_coordinator<'c, T>(
        &self,
        transactional_id: &str,
        coordinator: &'c mut Option<Connection>,
        find_coordinator: bool,
    ) -> Result<&'c Connection, RetryAction<T>> {
        if find_coordinator {
            *coordinator = None;
        }
        if let Some(connection) = coordinator {
            return Ok(connection);
        }
        let response = self
            .conn
            .send(build_find_coordinator(
                transactional_id,
                CoordinatorKeyType::Transaction,
            ))
            .await
            .map_err(connection_failure_action)?;
        let address = match coordinator_address(transactional_id, response) {
            Ok(address) => address,
            Err(
                error @ AdminError::Broker {
                    code: COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE,
                    ..
                },
            ) => return Err(RetryAction::FindCoordinator(Err(error))),
            Err(error) => return Err(RetryAction::Done(Err(error))),
        };
        match Self::connect_one(&address, self.options.clone()).await {
            Ok(connection) => Ok(coordinator.insert(connection)),
            Err(error) => Err(connection_failure_action(error)),
        }
    }

    /// Reads the transaction coordinator's current state for each ID in
    /// `transactional_ids`.
    ///
    /// Two transactional IDs can live on two different coordinators, so the
    /// client describes them one at a time. Each ID costs one
    /// `FindCoordinator` round trip, one new connection to the coordinator
    /// that the lookup names, and one `DescribeTransactions` round trip. The
    /// cost grows linearly with the number of IDs. Callers that describe a
    /// large set should expect that cost. The results keep the order of
    /// `transactional_ids`.
    ///
    /// # Errors
    ///
    /// Stops at the first ID that fails and returns its error. Returns
    /// [`AdminError::Protocol`] for an empty transactional ID, and for a
    /// response that carries no row for the ID. Returns
    /// [`AdminError::Broker`] when the coordinator lookup fails, or when the
    /// coordinator attaches an error code to the row. Returns
    /// [`AdminError::Transport`] when the connection to a coordinator fails.
    pub async fn describe_transactions(
        &self,
        transactional_ids: &[&str],
    ) -> Result<Vec<TransactionDescription>, AdminError> {
        // Kafka's `describeTransactions` runs all IDs under one deadline
        // (`invokeDriver` with one `deadlineMs`).
        let deadline = self.retry.start();
        let mut descriptions = Vec::with_capacity(transactional_ids.len());
        for transactional_id in transactional_ids {
            if transactional_id.is_empty() {
                return Err(AdminError::Protocol(
                    "transactional id must not be empty".to_owned(),
                ));
            }
            descriptions.push(
                self.describe_transaction_until(transactional_id, deadline)
                    .await?,
            );
        }
        Ok(descriptions)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use assert2::check;
    use bytes::{Buf, BytesMut};
    use krabka_client_core::{ClientError, MockBroker, MockReply};
    use krabka_protocol::{
        Decode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            describe_producers_request,
            describe_producers_response::{PartitionResponse, TopicResponse},
            describe_transactions_request, find_coordinator_request,
            find_coordinator_response::Coordinator,
            list_transactions_request, metadata_request,
            metadata_response::{
                MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
                MetadataResponseTopic,
            },
            write_txn_markers_request,
            write_txn_markers_response::{
                WritableTxnMarkerPartitionResult, WritableTxnMarkerResult,
                WritableTxnMarkerTopicResult,
            },
        },
    };

    use super::*;
    use crate::partition_leaders::test_support::{
        Seen, fast_admin as leader_fast_admin, leader_broker as partition_leader_broker,
    };

    fn state_row(transactional_id: &str, error_code: i16) -> TransactionState {
        TransactionState {
            error_code,
            transactional_id: transactional_id.to_owned(),
            ..Default::default()
        }
    }

    /// The `InitProducerId` request that fences `payments` with no producer
    /// identity and no kept prepared transaction.
    fn fence_payments() -> InitProducerIdRequest {
        InitProducerIdRequest {
            transactional_id: Some("payments".to_owned()),
            transaction_timeout_ms: 30_000,
            producer_id: -1,
            producer_epoch: -1,
            enable2_pc: false,
            keep_prepared_txn: false,
            ..Default::default()
        }
    }

    #[test]
    fn force_termination_request_fences_without_preserving_transaction() {
        let request = force_terminate_request("payments", 30_000);

        assert2::assert!(request == ReleasedInitProducerId(fence_payments()));
    }

    /// The body of `response` at `version`, behind an empty tagged-field
    /// byte when `flexible` is set.
    fn encode_response(response: &impl Encode, version: i16, flexible: bool) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        if flexible {
            bytes.extend_from_slice(&[0]);
        }
        response
            .encode(&mut bytes, version)
            .expect("response encodes");
        bytes.to_vec()
    }

    /// An `ApiVersions` response that advertises `InitProducerId` in
    /// `init_producer_id_range`.
    fn api_versions(init_producer_id_range: (i16, i16)) -> Vec<u8> {
        let api_version = |api_key, min_version, max_version| ApiVersion {
            api_key,
            min_version,
            max_version,
            ..Default::default()
        };
        encode_response(
            &ApiVersionsResponse {
                api_keys: vec![
                    api_version(api_versions_request::API_KEY, 0, 0),
                    api_version(find_coordinator_request::API_KEY, 0, 0),
                    api_version(metadata_request::API_KEY, 13, 13),
                    api_version(
                        init_producer_id_request::API_KEY,
                        init_producer_id_range.0,
                        init_producer_id_range.1,
                    ),
                ],
                ..Default::default()
            },
            0,
            false,
        )
    }

    /// The negotiated `InitProducerId` version and the result of
    /// `force_terminate_transaction`, with a version error as its ranges.
    type FenceResult = Result<(), (i16, i16, i16, i16, i16)>;

    /// Apache Kafka's `FenceProducersHandler` builds `InitProducerId` with
    /// `new InitProducerIdRequest.Builder(data)`, which allows only released
    /// versions. v6 is unstable, so the request stops at v5.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn force_terminate_transaction_sends_a_released_init_producer_id_version() {
        let sent_request = |version| vec![(version, fence_payments())];
        for (name, init_producer_id_range, expected_requests, expected_result) in [
            ("coordinator stops at v1", (0, 1), sent_request(1), Ok(())),
            ("coordinator stops at v4", (0, 4), sent_request(4), Ok(())),
            ("coordinator stops at v5", (0, 5), sent_request(5), Ok(())),
            (
                "coordinator supports unstable v6",
                (0, 6),
                sent_request(5),
                Ok(()),
            ),
            (
                "coordinator supports only unstable v6",
                (6, 6),
                Vec::new(),
                Err((init_producer_id_request::API_KEY, 6, 6, 0, 5)),
            ),
        ] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_in_mock = Arc::clone(&requests);
            let coordinator =
                MockBroker::start(move |api_key, version, _, mut body| match api_key {
                    api_versions_request::API_KEY => Some(api_versions(init_producer_id_range)),
                    init_producer_id_request::API_KEY => {
                        let client_id_len = body.get_i16();
                        body.advance(usize::try_from(client_id_len).expect("client id length"));
                        let flexible = version >= init_producer_id_request::FLEXIBLE_MIN;
                        if flexible {
                            body.advance(1);
                        }
                        let request = InitProducerIdRequest::decode(&mut body, version)
                            .expect("init producer id request decodes");
                        requests_in_mock
                            .lock()
                            .expect("requests lock")
                            .push((version, request));
                        Some(encode_response(
                            &InitProducerIdResponse {
                                producer_id: 4242,
                                producer_epoch: 1,
                                ..Default::default()
                            },
                            version,
                            flexible,
                        ))
                    }
                    _ => None,
                })
                .await;
            let coordinator_addr = coordinator.addr;
            let bootstrap = MockBroker::start(move |api_key, version, _, _| match api_key {
                api_versions_request::API_KEY => Some(api_versions(init_producer_id_range)),
                find_coordinator_request::API_KEY => Some(encode_response(
                    &FindCoordinatorResponse {
                        node_id: 2,
                        host: coordinator_addr.ip().to_string(),
                        port: i32::from(coordinator_addr.port()),
                        ..Default::default()
                    },
                    version,
                    false,
                )),
                metadata_request::API_KEY => {
                    Some(encode_response(&MetadataResponse::default(), version, true))
                }
                _ => None,
            })
            .await;
            let admin = AdminClient::connect(&[bootstrap.addr.to_string()])
                .await
                .expect("admin connects");

            let result: FenceResult =
                admin
                    .force_terminate_transaction("payments")
                    .await
                    .map_err(|error| match error {
                        AdminError::Transport(ClientError::IncompatibleVersion {
                            api_key,
                            broker_min,
                            broker_max,
                            client_min,
                            client_max,
                        }) => (api_key, broker_min, broker_max, client_min, client_max),
                        other => panic!("case {name}: unexpected error {other:?}"),
                    });

            bootstrap.stop();
            coordinator.stop();
            let requests = requests.lock().expect("requests lock").clone();
            assert2::assert!(
                (requests, result) == (expected_requests, expected_result),
                "case {name}"
            );
        }
    }

    #[test]
    fn coordinator_lookup_selects_the_matching_batched_entry() {
        let response = FindCoordinatorResponse {
            coordinators: vec![
                Coordinator {
                    key: "other".to_owned(),
                    host: "wrong".to_owned(),
                    port: 1,
                    ..Default::default()
                },
                Coordinator {
                    key: "payments".to_owned(),
                    host: "coordinator".to_owned(),
                    port: 9092,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let address = coordinator_address("payments", response).expect("matching coordinator");
        assert2::assert!(address == "coordinator:9092");
    }

    #[test]
    fn describe_request_asks_for_exactly_one_transactional_id() {
        let request = describe_transactions_request("payments");

        assert2::assert!(
            request
                == DescribeTransactionsRequest {
                    transactional_ids: vec!["payments".to_owned()],
                    ..Default::default()
                }
        );
    }

    #[test]
    fn description_maps_the_matching_row_to_the_domain_struct() {
        let response = DescribeTransactionsResponse {
            transaction_states: vec![
                TransactionState {
                    transactional_id: "other".to_owned(),
                    producer_id: 11,
                    producer_epoch: 1,
                    ..Default::default()
                },
                TransactionState {
                    transactional_id: "payments".to_owned(),
                    transaction_state: "Ongoing".to_owned(),
                    transaction_timeout_ms: 60_000,
                    transaction_start_time_ms: 1_700_000_000_123,
                    producer_id: 4242,
                    producer_epoch: 7,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let described = transaction_description("payments", response).expect("matching row");

        assert2::assert!(
            described
                == TransactionDescription {
                    transactional_id: "payments".to_owned(),
                    state: "Ongoing".to_owned(),
                    timeout: Time::from_millis(60_000),
                    start_time_ms: 1_700_000_000_123,
                    producer_id: 4242,
                    producer_epoch: 7,
                }
        );
    }

    #[test]
    fn description_surfaces_the_error_code_of_the_matching_row() {
        for (_case, code, want_name) in [
            ("unknown id", 105, "TRANSACTIONAL_ID_NOT_FOUND"),
            (
                "not authorized",
                53,
                "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
            ),
            ("moved coordinator", 16, "NOT_COORDINATOR"),
            ("loading", 14, "COORDINATOR_LOAD_IN_PROGRESS"),
        ] {
            let response = DescribeTransactionsResponse {
                transaction_states: vec![state_row("other", 0), state_row("payments", code)],
                ..Default::default()
            };

            let error = transaction_description("payments", response).expect_err("broker error");
            match error {
                AdminError::Broker {
                    api,
                    code: got,
                    name,
                    message,
                } => {
                    check!(
                        (api, got, name, message)
                            == ("DescribeTransactions", code, want_name, None)
                    );
                }
                other => panic!("expected AdminError::Broker, got {other:?}"),
            }
        }
    }

    #[test]
    fn description_rejects_a_response_that_omits_the_requested_id() {
        let response = DescribeTransactionsResponse {
            transaction_states: vec![state_row("other", 0)],
            ..Default::default()
        };

        let error = transaction_description("payments", response).expect_err("missing row");
        assert2::assert!(matches!(error, AdminError::Protocol(_)));
    }

    /// The transaction admin call of a retry case.
    #[derive(Clone, Copy, Debug)]
    enum TransactionCall {
        Describe,
        ForceTerminate,
    }

    /// The error codes that the mock brokers answer, one per request. The last
    /// code repeats.
    #[derive(Default)]
    struct CoordinatorScript {
        find_coordinator: Vec<i16>,
        coordinator: Vec<i16>,
        find_coordinator_requests: usize,
        coordinator_requests: usize,
        /// The first `unreachable_answers` `FindCoordinator` answers name
        /// `unreachable`, an address that refuses connections.
        unreachable_answers: usize,
        unreachable: Option<std::net::SocketAddr>,
        /// The coordinator does not answer the transaction request.
        silent: bool,
        /// The first `closed_find_coordinators` `FindCoordinator` requests
        /// close the connection with no answer.
        closed_find_coordinators: usize,
    }

    /// How the coordinator of a retry case behaves.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum CoordinatorBehavior {
        /// It answers from the script.
        Normal,
        /// The first `FindCoordinator` answer names an address that refuses
        /// connections.
        UnreachableOnce,
        /// It does not answer the transaction request.
        Silent,
        /// The bootstrap broker closes the connection on the first
        /// `FindCoordinator`.
        FindCoordinatorClosedOnce,
    }

    /// An address on which no listener accepts connections.
    async fn refused_address() -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port");
        listener.local_addr().expect("local address")
    }

    fn next_code(codes: &[i16], requests: &mut usize) -> i16 {
        let code = codes[(*requests).min(codes.len() - 1)];
        *requests += 1;
        code
    }

    /// An `ApiVersions` response for the retry mocks.
    fn retry_api_versions() -> Vec<u8> {
        let api_version = |api_key, min_version, max_version| ApiVersion {
            api_key,
            min_version,
            max_version,
            ..Default::default()
        };
        encode_response(
            &ApiVersionsResponse {
                api_keys: vec![
                    api_version(api_versions_request::API_KEY, 0, 0),
                    api_version(find_coordinator_request::API_KEY, 0, 0),
                    api_version(metadata_request::API_KEY, 13, 13),
                    api_version(init_producer_id_request::API_KEY, 0, 0),
                    api_version(describe_transactions_request::API_KEY, 0, 0),
                ],
                ..Default::default()
            },
            0,
            false,
        )
    }

    /// A mock broker that answers `FindCoordinator` with `coordinator`, and
    /// `DescribeTransactions` and `InitProducerId` with the codes of `script`.
    async fn scripted_transaction_broker(
        script: Arc<Mutex<CoordinatorScript>>,
        coordinator: Arc<Mutex<Option<std::net::SocketAddr>>>,
    ) -> MockBroker {
        MockBroker::start_with_replies(move |api_key, version, _, _| {
            let mut script = script.lock().expect("script lock");
            let script = &mut *script;
            if api_key == find_coordinator_request::API_KEY && script.closed_find_coordinators > 0 {
                script.closed_find_coordinators -= 1;
                script.find_coordinator_requests += 1;
                return MockReply::Close;
            }
            let reply = match api_key {
                api_versions_request::API_KEY => Some(retry_api_versions()),
                find_coordinator_request::API_KEY => {
                    let error_code = next_code(
                        &script.find_coordinator,
                        &mut script.find_coordinator_requests,
                    );
                    let addr = match script.unreachable {
                        Some(unreachable)
                            if script.find_coordinator_requests <= script.unreachable_answers =>
                        {
                            unreachable
                        }
                        _ => coordinator
                            .lock()
                            .expect("coordinator lock")
                            .expect("coordinator address"),
                    };
                    Some(encode_response(
                        &FindCoordinatorResponse {
                            error_code,
                            node_id: 2,
                            host: addr.ip().to_string(),
                            port: i32::from(addr.port()),
                            ..Default::default()
                        },
                        version,
                        false,
                    ))
                }
                describe_transactions_request::API_KEY => {
                    let error_code =
                        next_code(&script.coordinator, &mut script.coordinator_requests);
                    if script.silent {
                        return MockReply::Silent;
                    }
                    Some(encode_response(
                        &DescribeTransactionsResponse {
                            transaction_states: vec![TransactionState {
                                error_code,
                                transactional_id: "payments".to_owned(),
                                transaction_state: "Ongoing".to_owned(),
                                producer_id: 4242,
                                producer_epoch: 7,
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                        version,
                        true,
                    ))
                }
                init_producer_id_request::API_KEY => {
                    let error_code =
                        next_code(&script.coordinator, &mut script.coordinator_requests);
                    if script.silent {
                        return MockReply::Silent;
                    }
                    Some(encode_response(
                        &InitProducerIdResponse {
                            error_code,
                            producer_id: 4242,
                            producer_epoch: 8,
                            ..Default::default()
                        },
                        version,
                        false,
                    ))
                }
                _ => None,
            };
            reply.map_or(MockReply::Silent, MockReply::Respond)
        })
        .await
    }

    /// A call that stops at its deadline gives a timeout. The retry tests give
    /// it Kafka's `REQUEST_TIMED_OUT` code.
    const TIMED_OUT: i16 = 7;
    /// A deadline that several attempts fit in.
    const LONG: Duration = Duration::from_secs(5);
    /// One attempt fits in `NOW`, and the backoff after it reaches the
    /// deadline, so the call makes no second attempt.
    const NOW: Duration = Duration::from_millis(500);
    /// A deadline shorter than the request timeout.
    const SHORT: Duration = Duration::from_millis(300);

    type TransactionOutcome = Result<(), (&'static str, i16)>;

    type TransactionCase = (
        &'static str,
        TransactionCall,
        Vec<i16>,
        Vec<i16>,
        (Duration, CoordinatorBehavior),
        (TransactionOutcome, usize, usize),
    );

    const OK: TransactionOutcome = Ok(());

    fn failed(api: &'static str, code: i16) -> TransactionOutcome {
        Err((api, code))
    }

    /// Run one retry case against a scripted bootstrap broker and
    /// coordinator.
    async fn run_transaction_case(case: TransactionCase) {
        use TransactionCall::{Describe, ForceTerminate};

        let (name, call, find_coordinator, coordinator, (timeout, behavior), expected) = case;
        let backoff = if timeout == LONG {
            Duration::from_millis(1)
        } else {
            timeout
        };
        let script = Arc::new(Mutex::new(CoordinatorScript {
            find_coordinator,
            coordinator,
            unreachable_answers: usize::from(behavior == CoordinatorBehavior::UnreachableOnce),
            unreachable: Some(refused_address().await),
            silent: behavior == CoordinatorBehavior::Silent,
            closed_find_coordinators: usize::from(
                behavior == CoordinatorBehavior::FindCoordinatorClosedOnce,
            ),
            ..CoordinatorScript::default()
        }));
        let coordinator_addr = Arc::new(Mutex::new(None));
        let coordinator =
            scripted_transaction_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
        *coordinator_addr.lock().expect("coordinator lock") = Some(coordinator.addr);
        let bootstrap =
            scripted_transaction_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
        let admin = AdminClient::connect(&[bootstrap.addr.to_string()])
            .await
            .expect("admin connects");
        let retry = RetryPolicy {
            timeout,
            initial_backoff: backoff,
            max_backoff: backoff,
            jitter: 0.0,
            max_retries: u32::MAX,
        };

        let started = tokio::time::Instant::now();
        let result = match call {
            Describe => admin
                .describe_transaction_with_retry("payments", retry)
                .await
                .map(|_| ()),
            ForceTerminate => {
                admin
                    .force_terminate_transaction_with_retry("payments", retry)
                    .await
            }
        }
        .map_err(|error| match error {
            AdminError::Broker { api, code, .. } => (api, code),
            AdminError::Transport(ClientError::Timeout(_)) => ("timeout", TIMED_OUT),
            other => panic!("case {name}: unexpected error {other:?}"),
        });

        bootstrap.stop();
        coordinator.stop();
        let script = script.lock().expect("script lock");
        // A call never runs far past its deadline.
        let within_deadline = started.elapsed() < timeout + Duration::from_secs(2);
        assert2::assert!(
            (
                result,
                script.find_coordinator_requests,
                script.coordinator_requests,
                within_deadline
            ) == (expected.0, expected.1, expected.2, true),
            "case {name}"
        );
    }

    /// Apache Kafka's `DescribeTransactionsHandler.handleError` retries
    /// `COORDINATOR_LOAD_IN_PROGRESS` (14) on the same coordinator.
    /// `FenceProducersHandler.handleError` retries 14 and
    /// `CONCURRENT_TRANSACTIONS` (51) on the same coordinator. Both unmap the
    /// transactional ID on `COORDINATOR_NOT_AVAILABLE` (15) and
    /// `NOT_COORDINATOR` (16), so the driver finds the coordinator again, and
    /// fail on every other code. `CoordinatorStrategy.handleError` retries a
    /// `FindCoordinator` answer of 14 or 15. The driver stops at the call
    /// timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transaction_calls_retry_coordinator_errors_as_kafka_does() {
        use TransactionCall::{Describe, ForceTerminate};

        for case in [
            (
                "describe: no error",
                Describe,
                vec![0],
                vec![0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 1, 1),
            ),
            (
                "describe: coordinator load in progress retries on the same coordinator",
                Describe,
                vec![0],
                vec![14, 14, 0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 1, 3),
            ),
            (
                "describe: not coordinator finds the coordinator again",
                Describe,
                vec![0],
                vec![16, 0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 2, 2),
            ),
            (
                "describe: coordinator not available finds the coordinator again",
                Describe,
                vec![0],
                vec![15, 0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 2, 2),
            ),
            (
                "describe: concurrent transactions is final",
                Describe,
                vec![0],
                vec![51],
                (LONG, CoordinatorBehavior::Normal),
                (failed("DescribeTransactions", 51), 1, 1),
            ),
            (
                "describe: transactional id not found is final",
                Describe,
                vec![0],
                vec![105],
                (LONG, CoordinatorBehavior::Normal),
                (failed("DescribeTransactions", 105), 1, 1),
            ),
            (
                "describe: coordinator load in progress past the timeout times out",
                Describe,
                vec![0],
                vec![14],
                (NOW, CoordinatorBehavior::Normal),
                (failed("timeout", TIMED_OUT), 1, 1),
            ),
            (
                "describe: find coordinator not available, then the coordinator",
                Describe,
                vec![15, 0],
                vec![0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 2, 1),
            ),
            (
                "describe: find coordinator authorization failed is final",
                Describe,
                vec![53],
                vec![0],
                (LONG, CoordinatorBehavior::Normal),
                (failed("FindCoordinator", 53), 1, 0),
            ),
            (
                "fence: no error",
                ForceTerminate,
                vec![0],
                vec![0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 1, 1),
            ),
            (
                "fence: coordinator load in progress retries on the same coordinator",
                ForceTerminate,
                vec![0],
                vec![14, 0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 1, 2),
            ),
            (
                "fence: concurrent transactions retries on the same coordinator",
                ForceTerminate,
                vec![0],
                vec![51, 51, 0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 1, 3),
            ),
            (
                "fence: not coordinator finds the coordinator again",
                ForceTerminate,
                vec![0],
                vec![16, 0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 2, 2),
            ),
            (
                "fence: cluster authorization failed is final",
                ForceTerminate,
                vec![0],
                vec![31],
                (LONG, CoordinatorBehavior::Normal),
                (failed("InitProducerId", 31), 1, 1),
            ),
            (
                "fence: concurrent transactions past the timeout times out",
                ForceTerminate,
                vec![0],
                vec![51],
                (NOW, CoordinatorBehavior::Normal),
                (failed("timeout", TIMED_OUT), 1, 1),
            ),
            (
                "fence: find coordinator load in progress, then the coordinator",
                ForceTerminate,
                vec![14, 0],
                vec![0],
                (LONG, CoordinatorBehavior::Normal),
                (OK, 2, 1),
            ),
        ] {
            run_transaction_case(case).await;
        }
    }

    /// Kafka's `AdminApiDriver.onFailure` looks the coordinator up again
    /// after a disconnect, and `KafkaAdminClient` times out a call in flight
    /// at its deadline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transaction_calls_find_the_coordinator_again_after_connection_failures() {
        use TransactionCall::{Describe, ForceTerminate};

        for case in [
            (
                "describe: a closed connection during FindCoordinator recovers",
                Describe,
                vec![0],
                vec![0],
                (LONG, CoordinatorBehavior::FindCoordinatorClosedOnce),
                (OK, 2, 1),
            ),
            (
                "fence: a closed connection during FindCoordinator recovers",
                ForceTerminate,
                vec![0],
                vec![0],
                (LONG, CoordinatorBehavior::FindCoordinatorClosedOnce),
                (OK, 2, 1),
            ),
            (
                "describe: an unreachable coordinator finds the coordinator again",
                Describe,
                vec![0],
                vec![0],
                (LONG, CoordinatorBehavior::UnreachableOnce),
                (OK, 2, 1),
            ),
            (
                "describe: a silent coordinator stops the attempt at the deadline",
                Describe,
                vec![0],
                vec![0],
                (SHORT, CoordinatorBehavior::Silent),
                (failed("timeout", TIMED_OUT), 1, 1),
            ),
            (
                "fence: an unreachable coordinator finds the coordinator again",
                ForceTerminate,
                vec![0],
                vec![0],
                (LONG, CoordinatorBehavior::UnreachableOnce),
                (OK, 2, 1),
            ),
            (
                "fence: a silent coordinator stops the attempt at the deadline",
                ForceTerminate,
                vec![0],
                vec![0],
                (SHORT, CoordinatorBehavior::Silent),
                (failed("timeout", TIMED_OUT), 1, 1),
            ),
        ] {
            run_transaction_case(case).await;
        }
    }

    /// Kafka's `describeTransactions` runs every ID under one call deadline
    /// (`invokeDriver`), so a slow first ID leaves less time for the next.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_transactions_shares_one_deadline_across_ids() {
        let mut coordinator_codes = vec![14; 8];
        coordinator_codes.extend([0, 14]);
        let script = Arc::new(Mutex::new(CoordinatorScript {
            find_coordinator: vec![0],
            coordinator: coordinator_codes,
            ..CoordinatorScript::default()
        }));
        let coordinator_addr = Arc::new(Mutex::new(None));
        let coordinator =
            scripted_transaction_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
        *coordinator_addr.lock().expect("coordinator lock") = Some(coordinator.addr);
        let bootstrap =
            scripted_transaction_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
        let admin = AdminClient::connect_with_config(
            &[bootstrap.addr.to_string()],
            crate::AdminClientConfig {
                request_timeout: krabka_units::millis(200),
                default_api_timeout: Some(krabka_units::millis(1500)),
                retry_backoff: krabka_units::millis(100),
                retry_backoff_max: krabka_units::millis(100),
                ..crate::AdminClientConfig::default()
            },
        )
        .await
        .expect("admin connects");

        let started = tokio::time::Instant::now();
        let result = admin.describe_transactions(&["payments", "payments"]).await;
        let elapsed = started.elapsed();

        bootstrap.stop();
        coordinator.stop();
        // The first ID needs about 800 ms. The second ID gets the rest of the
        // 1.5 s deadline, not a new 1.5 s.
        assert2::assert!(matches!(
            result,
            Err(AdminError::Transport(ClientError::Timeout(_)))
        ));
        assert2::assert!(elapsed < Duration::from_millis(2200), "{elapsed:?}");
    }

    /// The transaction that the abort tests fence.
    fn orders_spec() -> AbortTransactionSpec {
        AbortTransactionSpec {
            topic: "orders".to_owned(),
            partition: 1,
            producer_id: 4242,
            producer_epoch: 7,
            coordinator_epoch: 3,
        }
    }

    /// A `Metadata` answer in which broker 1 at `addr` leads `orders`-0 and
    /// `orders`-1, with `partition_error` on each partition, and `missing`
    /// is an unknown topic.
    fn orders_metadata(addr: std::net::SocketAddr, partition_error: i16) -> MetadataResponse {
        let partition = |partition_index| MetadataResponsePartition {
            error_code: partition_error,
            partition_index,
            leader_id: 1,
            ..Default::default()
        };
        MetadataResponse {
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: addr.ip().to_string(),
                port: i32::from(addr.port()),
                ..Default::default()
            }],
            topics: vec![
                MetadataResponseTopic {
                    name: Some("orders".to_owned()),
                    partitions: vec![partition(0), partition(1)],
                    ..Default::default()
                },
                MetadataResponseTopic {
                    error_code: 3,
                    name: Some("missing".to_owned()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    /// The outcome of an admin call reduced to what the tests compare: the
    /// failing API and error code, or `("protocol", 0)` for a protocol error.
    fn outcome_of(result: Result<(), AdminError>) -> TransactionOutcome {
        result.map_err(|error| match error {
            AdminError::Broker { api, code, .. } => (api, code),
            AdminError::Protocol(_) => ("protocol", 0),
            other => panic!("unexpected error {other:?}"),
        })
    }

    #[test]
    fn partition_leader_address_resolves_the_leader_or_reports_the_metadata_error() {
        let addr: std::net::SocketAddr = "127.0.0.1:9092".parse().expect("address");
        let mut leaderless = orders_metadata(addr, 0);
        leaderless.topics[0].partitions[1].leader_id = 9;
        for (name, metadata, topic, partition, expected) in [
            (
                "leader",
                orders_metadata(addr, 0),
                "orders",
                1,
                Ok("127.0.0.1:9092".to_owned()),
            ),
            (
                "partition error",
                orders_metadata(addr, NOT_LEADER_OR_FOLLOWER),
                "orders",
                1,
                Err(("Metadata", NOT_LEADER_OR_FOLLOWER)),
            ),
            (
                "topic error",
                orders_metadata(addr, 0),
                "missing",
                0,
                Err(("Metadata", 3)),
            ),
            (
                "unlisted topic",
                orders_metadata(addr, 0),
                "absent",
                0,
                Err(("protocol", 0)),
            ),
            (
                "unlisted partition",
                orders_metadata(addr, 0),
                "orders",
                5,
                Err(("protocol", 0)),
            ),
            (
                "unlisted leader",
                leaderless,
                "orders",
                1,
                Err(("protocol", 0)),
            ),
        ] {
            let address = partition_leader_address(topic, partition, &metadata);
            let actual = match address {
                Ok(address) => Ok(address),
                Err(error) => Err(outcome_of(Err(error)).expect_err("an error")),
            };
            assert2::assert!(actual == expected, "case {name}");
        }
    }

    #[test]
    fn abort_request_writes_one_abort_marker_for_the_partition() {
        assert2::assert!(
            abort_transaction_request(&orders_spec())
                == WriteTxnMarkersRequest {
                    markers: vec![WritableTxnMarker {
                        producer_id: 4242,
                        producer_epoch: 7,
                        transaction_result: false,
                        topics: vec![WritableTxnMarkerTopic {
                            name: "orders".to_owned(),
                            partition_indexes: vec![1],
                            ..Default::default()
                        }],
                        coordinator_epoch: 3,
                        ..Default::default()
                    }],
                    ..Default::default()
                }
        );
    }

    /// A `WriteTxnMarkers` answer with `code` for `producer_id` on
    /// `topic`-`partition`.
    fn markers_response(
        producer_id: i64,
        topic: &str,
        partition: i32,
        code: i16,
    ) -> WriteTxnMarkersResponse {
        WriteTxnMarkersResponse {
            markers: vec![WritableTxnMarkerResult {
                producer_id,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: topic.to_owned(),
                    partitions: vec![WritableTxnMarkerPartitionResult {
                        partition_index: partition,
                        error_code: code,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn abort_result_maps_the_matching_partition_result() {
        for (name, response, expected) in [
            ("no error", markers_response(4242, "orders", 1, 0), OK),
            (
                "partition error",
                markers_response(4242, "orders", 1, 53),
                failed("WriteTxnMarkers", 53),
            ),
            (
                "other producer",
                markers_response(1, "orders", 1, 0),
                failed("protocol", 0),
            ),
            (
                "other topic",
                markers_response(4242, "payments", 1, 0),
                failed("protocol", 0),
            ),
            (
                "other partition",
                markers_response(4242, "orders", 0, 0),
                failed("protocol", 0),
            ),
        ] {
            let result = outcome_of(abort_transaction_result(&orders_spec(), response));
            assert2::assert!(result == expected, "case {name}");
        }
    }

    /// Apache Kafka's `AbortTransactionHandler.handleResponse` looks the
    /// leader up again after `NOT_LEADER_OR_FOLLOWER` and
    /// `LEADER_NOT_AVAILABLE`, and fails on every other code.
    #[test]
    fn abort_retries_only_leadership_errors() {
        #[derive(Debug, PartialEq, Eq)]
        enum Action {
            Done,
            FindLeader,
        }
        for (code, expected) in [
            (0, Action::Done),
            (LEADER_NOT_AVAILABLE, Action::FindLeader),
            (NOT_LEADER_OR_FOLLOWER, Action::FindLeader),
            (53, Action::Done),
            (47, Action::Done),
        ] {
            let result =
                abort_transaction_result(&orders_spec(), markers_response(4242, "orders", 1, code));
            let action = match abort_transaction_retry_action(result) {
                RetryAction::Done(_) => Action::Done,
                RetryAction::FindCoordinator(_) => Action::FindLeader,
                RetryAction::SameCoordinator(_) => panic!("code {code}: same leader"),
            };
            assert2::assert!(action == expected, "code {code}");
        }
    }

    #[test]
    fn describe_producers_request_groups_partitions_by_topic() {
        let keys = [
            ("orders".to_owned(), 1),
            ("audit".to_owned(), 0),
            ("orders".to_owned(), 0),
        ];

        assert2::assert!(
            describe_producers_request(&keys)
                == DescribeProducersRequest {
                    topics: vec![
                        DescribeProducersTopicRequest {
                            name: "audit".to_owned(),
                            partition_indexes: vec![0],
                            ..Default::default()
                        },
                        DescribeProducersTopicRequest {
                            name: "orders".to_owned(),
                            partition_indexes: vec![1, 0],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }
        );
    }

    /// The `DescribeProducers` answer of the tests: one producer with an open
    /// transaction and one without on `orders`-0, `NOT_LEADER_OR_FOLLOWER` on
    /// `orders`-1, and a partition that nobody asked about.
    fn orders_producers() -> DescribeProducersResponse {
        DescribeProducersResponse {
            topics: vec![TopicResponse {
                name: "orders".to_owned(),
                partitions: vec![
                    PartitionResponse {
                        partition_index: 0,
                        active_producers: vec![
                            WireProducerState {
                                producer_id: 4242,
                                producer_epoch: 7,
                                last_sequence: 12,
                                last_timestamp: 1_700_000_000_123,
                                coordinator_epoch: 3,
                                current_txn_start_offset: 88,
                                ..Default::default()
                            },
                            WireProducerState {
                                producer_id: 11,
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                    PartitionResponse {
                        partition_index: 1,
                        error_code: NOT_LEADER_OR_FOLLOWER,
                        error_message: Some("moved".to_owned()),
                        ..Default::default()
                    },
                    PartitionResponse {
                        partition_index: 9,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// The per-partition results of [`orders_producers`] for `orders`-0 and
    /// `orders`-1.
    fn orders_producer_results()
    -> BTreeMap<(String, i32), Result<Vec<ProducerStateInfo>, KafkaError>> {
        BTreeMap::from([
            (
                ("orders".to_owned(), 0),
                Ok(vec![
                    ProducerStateInfo {
                        producer_id: 4242,
                        producer_epoch: 7,
                        last_sequence: 12,
                        last_timestamp_ms: 1_700_000_000_123,
                        coordinator_epoch: 3,
                        current_txn_start_offset: Some(88),
                    },
                    ProducerStateInfo {
                        producer_id: 11,
                        producer_epoch: 0,
                        last_sequence: -1,
                        last_timestamp_ms: -1,
                        coordinator_epoch: 0,
                        current_txn_start_offset: None,
                    },
                ]),
            ),
            (
                ("orders".to_owned(), 1),
                Err(KafkaError {
                    code: NOT_LEADER_OR_FOLLOWER,
                    name: "NOT_LEADER_OR_FOLLOWER",
                    message: Some("moved".to_owned()),
                }),
            ),
        ])
    }

    #[test]
    fn describe_producers_results_map_each_requested_partition() {
        let keys = [
            ("orders".to_owned(), 0),
            ("orders".to_owned(), 1),
            ("orders".to_owned(), 2),
        ];

        let mut expected = orders_producer_results();
        expected.insert(
            ("orders".to_owned(), 2),
            Err(KafkaError {
                code: -1,
                name: "UNKNOWN_SERVER_ERROR",
                message: Some(
                    "the DescribeProducers response did not contain a result for partition \
                     orders-2"
                        .to_owned(),
                ),
            }),
        );
        assert2::assert!(describe_producers_results(&keys, orders_producers()) == expected);
    }

    #[test]
    fn list_transactions_request_carries_the_filter() {
        for (name, filter, expected) in [
            (
                "default matches everything",
                ListTransactionsFilter::default(),
                ListTransactionsRequest::default(),
            ),
            (
                "every filter",
                ListTransactionsFilter {
                    state_filters: vec!["Ongoing".to_owned()],
                    producer_id_filters: vec![4242],
                    min_duration: Some(krabka_units::secs(90)),
                },
                ListTransactionsRequest {
                    state_filters: vec!["Ongoing".to_owned()],
                    producer_id_filters: vec![4242],
                    duration_filter: 90_000,
                    ..Default::default()
                },
            ),
        ] {
            assert2::assert!(
                list_transactions_request(&filter) == expected,
                "case {name}"
            );
        }
    }

    /// Decodes the request body of a mock broker: the client id, the tagged
    /// field byte of a flexible header, and the request itself.
    fn decode_request<T: for<'a> Decode<'a>>(mut body: &[u8], version: i16, flexible: bool) -> T {
        let client_id_len = body.get_i16();
        body.advance(usize::try_from(client_id_len).expect("client id length"));
        if flexible {
            body.advance(1);
        }
        T::decode(&mut body, version).expect("request decodes")
    }

    /// An `ApiVersions` response for the leader-routed mocks.
    fn leader_api_versions() -> Vec<u8> {
        let api_version = |api_key, min_version, max_version| ApiVersion {
            api_key,
            min_version,
            max_version,
            ..Default::default()
        };
        encode_response(
            &ApiVersionsResponse {
                api_keys: vec![
                    api_version(api_versions_request::API_KEY, 0, 0),
                    api_version(metadata_request::API_KEY, 13, 13),
                    api_version(write_txn_markers_request::API_KEY, 1, 2),
                    api_version(list_transactions_request::API_KEY, 0, 2),
                ],
                ..Default::default()
            },
            0,
            false,
        )
    }

    /// What a leader-routed mock broker saw and answers.
    #[derive(Default)]
    struct LeaderScript {
        /// The `WriteTxnMarkers` error codes, one per request. The last
        /// repeats.
        write_codes: Vec<i16>,
        /// The `WriteTxnMarkers` requests that the broker received.
        write_requests: Vec<WriteTxnMarkersRequest>,
        /// The `Metadata` requests that the broker received.
        metadata_requests: usize,
        /// The `ListTransactions` requests that the broker received.
        list_requests: Vec<ListTransactionsRequest>,
        /// The `ListTransactions` answer of the broker.
        listing: ListTransactionsResponse,
    }

    /// A mock broker that leads every `orders` partition. `cluster` names the
    /// brokers that its `Metadata` answer lists, itself first.
    async fn leader_broker(
        script: Arc<Mutex<LeaderScript>>,
        cluster: Arc<Mutex<Vec<std::net::SocketAddr>>>,
    ) -> MockBroker {
        MockBroker::start(move |api_key, version, _, body| {
            let mut script = script.lock().expect("script lock");
            match api_key {
                api_versions_request::API_KEY => Some(leader_api_versions()),
                metadata_request::API_KEY => {
                    script.metadata_requests += 1;
                    let cluster = cluster.lock().expect("cluster lock").clone();
                    let mut metadata = orders_metadata(cluster[0], 0);
                    metadata.brokers = cluster
                        .iter()
                        .zip(1..)
                        .map(|(addr, node_id)| MetadataResponseBroker {
                            node_id,
                            host: addr.ip().to_string(),
                            port: i32::from(addr.port()),
                            ..Default::default()
                        })
                        .collect();
                    Some(encode_response(&metadata, version, true))
                }
                write_txn_markers_request::API_KEY => {
                    let request: WriteTxnMarkersRequest = decode_request(body, version, true);
                    let code = script.write_codes[script
                        .write_requests
                        .len()
                        .min(script.write_codes.len() - 1)];
                    script.write_requests.push(request);
                    Some(encode_response(
                        &markers_response(4242, "orders", 1, code),
                        version,
                        true,
                    ))
                }
                list_transactions_request::API_KEY => {
                    script
                        .list_requests
                        .push(decode_request(body, version, true));
                    Some(encode_response(&script.listing, version, true))
                }
                _ => None,
            }
        })
        .await
    }

    /// An admin client on `bootstrap` that retries after 1 ms.
    async fn fast_admin(bootstrap: std::net::SocketAddr) -> AdminClient {
        AdminClient::connect_with_config(
            &[bootstrap.to_string()],
            crate::AdminClientConfig {
                request_timeout: krabka_units::secs(2),
                retry_backoff: krabka_units::millis(1),
                retry_backoff_max: krabka_units::millis(1),
                default_api_timeout: Some(krabka_units::secs(5)),
                ..crate::AdminClientConfig::default()
            },
        )
        .await
        .expect("admin connects")
    }

    /// Apache Kafka's `AbortTransactionHandler` sends one `WriteTxnMarkers`
    /// to the partition leader, looks the leader up again after
    /// `NOT_LEADER_OR_FOLLOWER`, and fails on every other code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_transaction_writes_an_abort_marker_to_the_leader() {
        for (name, write_codes, expected) in [
            ("no error", vec![0], (OK, 1, 1)),
            (
                "not leader or follower finds the leader again",
                vec![NOT_LEADER_OR_FOLLOWER, 0],
                (OK, 2, 2),
            ),
            (
                "transactional id authorization failed is final",
                vec![53],
                (failed("WriteTxnMarkers", 53), 1, 1),
            ),
        ] {
            let script = Arc::new(Mutex::new(LeaderScript {
                write_codes,
                ..LeaderScript::default()
            }));
            let cluster = Arc::new(Mutex::new(Vec::new()));
            let broker = leader_broker(Arc::clone(&script), Arc::clone(&cluster)).await;
            cluster.lock().expect("cluster lock").push(broker.addr);
            let admin = fast_admin(broker.addr).await;

            let result = outcome_of(admin.abort_transaction(&orders_spec()).await);

            broker.stop();
            let script = script.lock().expect("script lock");
            let expected_requests = vec![abort_transaction_request(&orders_spec()); expected.2];
            assert2::assert!(
                (
                    result,
                    script.metadata_requests,
                    script.write_requests.clone()
                ) == (expected.0, expected.1, expected_requests),
                "case {name}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn abort_transaction_rejects_an_empty_topic() {
        let script = Arc::new(Mutex::new(LeaderScript::default()));
        let cluster = Arc::new(Mutex::new(Vec::new()));
        let broker = leader_broker(Arc::clone(&script), Arc::clone(&cluster)).await;
        cluster.lock().expect("cluster lock").push(broker.addr);
        let admin = fast_admin(broker.addr).await;

        let result = admin
            .abort_transaction(&AbortTransactionSpec {
                topic: String::new(),
                ..orders_spec()
            })
            .await;

        broker.stop();
        assert2::assert!(outcome_of(result) == failed("protocol", 0));
        assert2::assert!(script.lock().expect("script lock").metadata_requests == 0);
    }

    /// The `DescribeProducers` answer to `request`: the producers of
    /// [`orders_producers`] on `orders`-0, and `orders_1_code` on `orders`-1.
    fn producers_answer(
        request: &DescribeProducersRequest,
        orders_1_code: i16,
    ) -> DescribeProducersResponse {
        let orders_0 = orders_producers().topics[0].partitions[0].clone();
        DescribeProducersResponse {
            topics: request
                .topics
                .iter()
                .map(|topic| TopicResponse {
                    name: topic.name.clone(),
                    partitions: topic
                        .partition_indexes
                        .iter()
                        .map(|&partition_index| {
                            if partition_index == 0 {
                                orders_0.clone()
                            } else {
                                PartitionResponse {
                                    partition_index,
                                    error_code: orders_1_code,
                                    error_message: (orders_1_code != 0)
                                        .then(|| "refused".to_owned()),
                                    ..Default::default()
                                }
                            }
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Apache Kafka's `DescribeProducersHandler` batches the partitions of
    /// one leader into one request, gives each partition its own result, and
    /// sends a partition back to the `PartitionLeaderStrategy` lookup after
    /// `NOT_LEADER_OR_FOLLOWER` or a lost connection to its leader.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_producers_finds_the_leader_again_as_kafka_does() {
        let both = vec![("orders".to_owned(), 0), ("orders".to_owned(), 1)];
        let only_1 = vec![("orders".to_owned(), 1)];
        let refused = refused_address().await;
        let producers_0 = orders_producer_results()[&both[0]].clone();
        for (name, leaders, orders_1_codes, expected) in [
            (
                "success",
                vec![None],
                vec![0],
                (Ok(Vec::new()), 1, vec![both.clone()]),
            ),
            (
                "not leader or follower finds the leader again",
                vec![None],
                vec![NOT_LEADER_OR_FOLLOWER, 0],
                (Ok(Vec::new()), 2, vec![both.clone(), only_1.clone()]),
            ),
            (
                "a refused connection finds the leader again",
                vec![Some(refused), None],
                vec![0],
                (Ok(Vec::new()), 2, vec![both.clone()]),
            ),
            (
                "topic authorization failed is final",
                vec![None],
                vec![29],
                (
                    Err(KafkaError {
                        code: 29,
                        name: "TOPIC_AUTHORIZATION_FAILED",
                        message: Some("refused".to_owned()),
                    }),
                    1,
                    vec![both.clone()],
                ),
            ),
        ] {
            let seen = Arc::new(Mutex::new(Seen::default()));
            let broker = partition_leader_broker(
                vec![(describe_producers_request::API_KEY, 0, 0)],
                describe_producers_request::API_KEY,
                leaders,
                Arc::clone(&seen),
                move |n, version, body| {
                    let request: DescribeProducersRequest = decode_request(body, version, true);
                    let code = orders_1_codes[n.min(orders_1_codes.len() - 1)];
                    encode_response(&producers_answer(&request, code), version, true)
                },
            )
            .await;
            let admin = leader_fast_admin(broker.addr, krabka_units::secs(5)).await;

            let result = admin.describe_producers(&both).await;

            broker.stop();
            let seen = seen.lock().expect("seen lock");
            let requests = seen
                .requests
                .iter()
                .map(|(version, body)| {
                    decode_request::<DescribeProducersRequest>(body, *version, true)
                })
                .collect::<Vec<_>>();
            assert2::assert!(
                (result, seen.metadata_requests, requests)
                    == (
                        BTreeMap::from([
                            (both[0].clone(), producers_0.clone()),
                            (both[1].clone(), expected.0),
                        ]),
                        expected.1,
                        expected
                            .2
                            .iter()
                            .map(|keys| describe_producers_request(keys))
                            .collect::<Vec<_>>(),
                    ),
                "case {name}"
            );
        }
    }

    /// A `ListTransactions` answer with `error_code` and one transaction per
    /// ID in `transactional_ids`.
    fn listing(error_code: i16, transactional_ids: &[&str]) -> ListTransactionsResponse {
        ListTransactionsResponse {
            error_code,
            transaction_states: transactional_ids
                .iter()
                .zip(1..)
                .map(|(transactional_id, producer_id)| ListedTransactionState {
                    transactional_id: (*transactional_id).to_owned(),
                    producer_id,
                    transaction_state: "Ongoing".to_owned(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Apache Kafka's `ListTransactionsHandler` asks every broker
    /// (`AllBrokersStrategy`) and merges the listings. A broker error fails
    /// the call.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_transactions_merges_the_listing_of_every_broker() {
        let listed = |transactional_id: &str, producer_id| TransactionListing {
            transactional_id: transactional_id.to_owned(),
            producer_id,
            state: "Ongoing".to_owned(),
        };
        for (name, second_listing, expected) in [
            (
                "every broker answers",
                listing(0, &["audit"]),
                Ok(vec![
                    listed("payments", 1),
                    listed("orders", 2),
                    listed("audit", 1),
                ]),
            ),
            (
                "a broker is loading its coordinator",
                listing(COORDINATOR_LOAD_IN_PROGRESS, &[]),
                Err(("ListTransactions", COORDINATOR_LOAD_IN_PROGRESS)),
            ),
        ] {
            let first = Arc::new(Mutex::new(LeaderScript {
                listing: listing(0, &["payments", "orders"]),
                ..LeaderScript::default()
            }));
            let second = Arc::new(Mutex::new(LeaderScript {
                listing: second_listing,
                ..LeaderScript::default()
            }));
            let cluster = Arc::new(Mutex::new(Vec::new()));
            let first_broker = leader_broker(Arc::clone(&first), Arc::clone(&cluster)).await;
            let second_broker = leader_broker(Arc::clone(&second), Arc::clone(&cluster)).await;
            cluster
                .lock()
                .expect("cluster lock")
                .extend([first_broker.addr, second_broker.addr]);
            let admin = fast_admin(first_broker.addr).await;
            let filter = ListTransactionsFilter {
                state_filters: vec!["Ongoing".to_owned()],
                ..ListTransactionsFilter::default()
            };

            let result = admin
                .list_transactions(&filter)
                .await
                .map_err(|error| match error {
                    AdminError::Broker { api, code, .. } => (api, code),
                    other => panic!("case {name}: unexpected error {other:?}"),
                });

            first_broker.stop();
            second_broker.stop();
            let requests = [&first, &second]
                .map(|script| script.lock().expect("script lock").list_requests.clone());
            let expected_request = list_transactions_request(&filter);
            assert2::assert!(
                (result, requests)
                    == (
                        expected,
                        [vec![expected_request.clone()], vec![expected_request]]
                    ),
                "case {name}"
            );
        }
    }

    /// Apache Kafka's `fenceProducers` gives each transactional ID its own
    /// result, and an invalid ID does not stop the others.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fence_producers_gives_each_transactional_id_its_own_result() {
        let script = Arc::new(Mutex::new(CoordinatorScript {
            find_coordinator: vec![0],
            coordinator: vec![0],
            ..CoordinatorScript::default()
        }));
        let coordinator_addr = Arc::new(Mutex::new(None));
        let coordinator =
            scripted_transaction_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
        *coordinator_addr.lock().expect("coordinator lock") = Some(coordinator.addr);
        let bootstrap =
            scripted_transaction_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
        let admin = AdminClient::connect(&[bootstrap.addr.to_string()])
            .await
            .expect("admin connects");

        let results = admin.fence_producers(&["payments", "", "orders"]).await;

        bootstrap.stop();
        coordinator.stop();
        let results = results
            .into_iter()
            .map(|(transactional_id, result)| (transactional_id, outcome_of(result)))
            .collect::<BTreeMap<_, _>>();
        let script = script.lock().expect("script lock");
        assert2::assert!(
            (
                results,
                script.find_coordinator_requests,
                script.coordinator_requests
            ) == (
                BTreeMap::from([
                    (String::new(), failed("protocol", 0)),
                    ("orders".to_owned(), OK),
                    ("payments".to_owned(), OK),
                ]),
                2,
                2
            )
        );
    }

    fn assert_send<T: Send>(_: T) {}

    /// A caller can spawn every retrying admin call on a multi-thread
    /// runtime, so each future must be `Send`.
    #[test]
    fn retrying_admin_call_futures_are_send() {
        let _ = |admin: &mut AdminClient,
                 offsets: &std::collections::BTreeMap<(String, i32), i64>| {
            assert_send(admin.alter_consumer_group_offsets("workers", offsets));
            assert_send(admin.list_consumer_group_offsets("workers"));
            assert_send(admin.describe_transaction("payments"));
            assert_send(admin.force_terminate_transaction("payments"));
            assert_send(admin.fence_producers(&["payments"]));
            assert_send(admin.abort_transaction(&orders_spec()));
            assert_send(admin.describe_producers(&[("orders".to_owned(), 0)]));
            assert_send(admin.list_transactions(&ListTransactionsFilter::default()));
        };
    }
}
