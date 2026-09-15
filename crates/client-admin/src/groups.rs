//! Consumer-group admin APIs: [`AdminClient::list_groups`] and
//! [`AdminClient::list_consumer_group_offsets`].
//!
//! These wrap the `ListGroups` (`api_key`=16), `OffsetCommit` (`api_key`=8)
//! and `OffsetFetch` (`api_key`=9) RPCs.
//!
//! ## `ListGroups` fan-out
//!
//! A broker lists only the groups that it coordinates.
//! [`AdminClient::list_groups`] sends `ListGroups` to every broker in the
//! metadata and merges the answers, as Apache Kafka's
//! `KafkaAdminClient.listGroups` does.
//!
//! ## `OffsetCommit` version note
//!
//! [`AdminClient::alter_consumer_group_offsets`] sends `OffsetCommit` at v9 or
//! lower, where the request names each topic. Apache Kafka's
//! `AlterConsumerGroupOffsetsHandler` builds its request with
//! `OffsetCommitRequest.Builder.forTopicNames`, which caps the version at 9.
//!
//! ## `OffsetFetch` version note
//!
//! [`AdminClient::list_consumer_group_offsets`] sends `OffsetFetch` at v2 to
//! v9, where the response names each topic. Apache Kafka's
//! `ListConsumerGroupOffsetsHandler` builds its request with
//! `OffsetFetchRequest.Builder.forTopicNames`, which caps the version at 9.
//!
//! ## `OffsetFetch` retries
//!
//! [`AdminClient::list_consumer_group_offsets`] retries the coordinator error
//! codes as Apache Kafka's `ListConsumerGroupOffsetsHandler.handleGroupError`
//! does, until Kafka's default `default.api.timeout.ms` (60 s) elapses.

use std::collections::{BTreeMap, BTreeSet};

use bytes::BufMut;
use krabka_client_core::{
    AuthenticationError, ClientError, Connection, ConnectionOptions, CoordinatorKeyType,
    SaslAuthenticationError, build_find_coordinator, coordinator_endpoint,
};
use krabka_protocol::{
    Encode, ProtocolError, ProtocolRequest,
    owned::{
        list_groups_request::{self, ListGroupsRequest},
        list_groups_response::ListedGroup,
        metadata_request::MetadataRequest,
        metadata_response::MetadataResponseBroker,
        offset_commit_request::{
            self, OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_commit_response::OffsetCommitResponse,
        offset_fetch_request::{self, OffsetFetchRequest, OffsetFetchRequestGroup},
        offset_fetch_response::OffsetFetchResponse,
    },
};

use crate::{
    AdminClient, AdminError, KafkaError, format_host_port, kafka_error_if, kafka_error_name,
    retry::{KAFKA_ADMIN_RETRY, RetryAction, RetryPolicy, retry_coordinator_call},
    send_connection_at_least,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupOffsetOutcome {
    pub topic: String,
    pub partition: i32,
    pub error: Option<KafkaError>,
}

/// The state of a group, as Apache Kafka's `GroupState` names it.
///
/// `ListGroups` v4 (KIP-518) filters groups by these names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GroupState {
    /// A state name that the client does not know.
    Unknown,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
    Empty,
    Assigning,
    Reconciling,
    NotReady,
}

impl GroupState {
    const ALL: [Self; 9] = [
        Self::Unknown,
        Self::PreparingRebalance,
        Self::CompletingRebalance,
        Self::Stable,
        Self::Dead,
        Self::Empty,
        Self::Assigning,
        Self::Reconciling,
        Self::NotReady,
    ];

    /// The wire name of the state, for example `"Stable"`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "Unknown",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
            Self::Dead => "Dead",
            Self::Empty => "Empty",
            Self::Assigning => "Assigning",
            Self::Reconciling => "Reconciling",
            Self::NotReady => "NotReady",
        }
    }

    /// Reads a state name without regard to case. A name that the client
    /// does not know gives [`GroupState::Unknown`], as Kafka's
    /// `GroupState.parse` does.
    #[must_use]
    pub fn parse(name: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|state| state.as_str().eq_ignore_ascii_case(name))
            .unwrap_or(Self::Unknown)
    }
}

impl std::fmt::Display for GroupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The type of a group, as Apache Kafka's `GroupType` names it.
///
/// `ListGroups` v5 (KIP-848) filters groups by these names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GroupType {
    /// A type name that the client does not know.
    Unknown,
    Consumer,
    Classic,
    Share,
    Streams,
}

impl GroupType {
    const ALL: [Self; 5] = [
        Self::Unknown,
        Self::Consumer,
        Self::Classic,
        Self::Share,
        Self::Streams,
    ];

    /// The wire name of the type, for example `"Classic"`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "Unknown",
            Self::Consumer => "Consumer",
            Self::Classic => "Classic",
            Self::Share => "Share",
            Self::Streams => "Streams",
        }
    }

    /// Reads a type name without regard to case. A name that the client does
    /// not know gives [`GroupType::Unknown`], as Kafka's `GroupType.parse`
    /// does.
    #[must_use]
    pub fn parse(name: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|group_type| group_type.as_str().eq_ignore_ascii_case(name))
            .unwrap_or(Self::Unknown)
    }
}

impl std::fmt::Display for GroupType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The options of [`AdminClient::list_groups`], as Apache Kafka's
/// `ListGroupsOptions` holds them. An empty set does not filter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListGroupsOptions {
    /// The broker lists only groups in these states (`states_filter`).
    pub group_states: BTreeSet<GroupState>,
    /// The broker lists only groups of these types (`types_filter`).
    pub types: BTreeSet<GroupType>,
    /// The client keeps only groups with these protocol types.
    pub protocol_types: BTreeSet<String>,
}

impl ListGroupsOptions {
    /// Classic and consumer groups with the `consumer` protocol type or no
    /// protocol type, as Kafka's `ListGroupsOptions.forConsumerGroups` selects.
    #[must_use]
    pub fn for_consumer_groups() -> Self {
        Self {
            group_states: BTreeSet::new(),
            types: BTreeSet::from([GroupType::Classic, GroupType::Consumer]),
            protocol_types: BTreeSet::from([String::new(), "consumer".to_owned()]),
        }
    }

    /// Share groups, as Kafka's `ListGroupsOptions.forShareGroups` selects.
    #[must_use]
    pub fn for_share_groups() -> Self {
        Self {
            types: BTreeSet::from([GroupType::Share]),
            ..Self::default()
        }
    }

    /// Streams groups, as Kafka's `ListGroupsOptions.forStreamsGroups`
    /// selects.
    #[must_use]
    pub fn for_streams_groups() -> Self {
        Self {
            types: BTreeSet::from([GroupType::Streams]),
            ..Self::default()
        }
    }
}

/// One group of [`AdminClient::list_groups`], as Apache Kafka's
/// `GroupListing` holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupListing {
    pub group_id: String,
    /// `None` when the broker sends no type (`ListGroups` v4 or lower).
    pub group_type: Option<GroupType>,
    /// The protocol type, for example `"consumer"`. Empty for a simple
    /// consumer group.
    pub protocol_type: String,
    /// `None` when the broker sends no state (`ListGroups` v3 or lower).
    pub group_state: Option<GroupState>,
}

impl GroupListing {
    /// Whether the group is a classic group with no protocol type, as Kafka's
    /// `GroupListing.isSimpleConsumerGroup` decides.
    ///
    /// A group with no type (`None`, from `ListGroups` v4 or lower) gives
    /// `false`, as in Kafka, where `type.filter(gt -> gt == CLASSIC)` is empty
    /// for `Optional.empty()`. Such a broker lists only classic groups, so a
    /// caller that wants Kafka's older `listConsumerGroups` answer
    /// (`ConsumerGroupListing.isSimpleConsumerGroup`, which is
    /// `protocolType.isEmpty()`) reads [`GroupListing::protocol_type`] alone.
    #[must_use]
    pub fn is_simple_consumer_group(&self) -> bool {
        self.group_type == Some(GroupType::Classic) && self.protocol_type.is_empty()
    }
}

impl From<ListedGroup> for GroupListing {
    fn from(group: ListedGroup) -> Self {
        Self {
            group_type: (!group.group_type.is_empty()).then(|| GroupType::parse(&group.group_type)),
            group_state: (!group.group_state.is_empty())
                .then(|| GroupState::parse(&group.group_state)),
            group_id: group.group_id,
            protocol_type: group.protocol_type,
        }
    }
}

/// The failure of `ListGroups` on one broker.
///
/// Kafka's `listGroups` puts one exception for each failed broker in
/// `ListGroupsResult.errors`. `error` holds the Kafka error code of that
/// exception, as Kafka's `ApiError.fromThrowable` gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsError {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub error: KafkaError,
}

/// The result of [`AdminClient::list_groups`], as Apache Kafka's
/// `ListGroupsResult` holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsResult {
    /// The groups of every broker that answered, one entry for each group
    /// id, in group id order.
    pub valid: Vec<GroupListing>,
    /// One entry for each broker that failed, in metadata order.
    pub errors: Vec<ListGroupsError>,
}

impl ListGroupsResult {
    /// The groups when no broker failed, as Kafka's `ListGroupsResult.all`
    /// gives them.
    ///
    /// # Errors
    /// Returns the first broker failure.
    pub fn all(self) -> Result<Vec<GroupListing>, ListGroupsError> {
        match self.errors.into_iter().next() {
            Some(error) => Err(error),
            None => Ok(self.valid),
        }
    }
}

impl AdminClient {
    /// Commits explicit offsets for an inactive consumer group.
    ///
    /// The request names each topic, so the client negotiates `OffsetCommit`
    /// v9 or lower, as Apache Kafka's `AlterConsumerGroupOffsetsHandler` does.
    ///
    /// The call retries the partition error codes as Apache Kafka's
    /// `AlterConsumerGroupOffsetsHandler.handleError` does. One retriable code
    /// in the response makes the call send all the offsets again:
    ///
    /// - `COORDINATOR_LOAD_IN_PROGRESS` (14) and `REBALANCE_IN_PROGRESS` (27):
    ///   send the request again to the same coordinator.
    /// - `COORDINATOR_NOT_AVAILABLE` (15) and `NOT_COORDINATOR` (16): find the
    ///   coordinator again, then send the request again.
    ///
    /// A `FindCoordinator` answer of 14 or 15 also makes the call find the
    /// coordinator again. A lost connection to the coordinator makes the call
    /// find the coordinator again, as Kafka's `AdminApiDriver.onFailure` does.
    /// The call waits between attempts with Kafka's backoff, and it stops when
    /// Kafka's default `default.api.timeout.ms` (60 s) elapses. At that time
    /// it returns the outcomes of the last response.
    ///
    /// # Errors
    /// Returns an error when encoding, transport, or response handling fails.
    /// Returns [`ClientError::Server`] when `FindCoordinator` answers with an
    /// error code that Kafka does not retry, or with a retriable code after
    /// the timeout. Returns [`ClientError::IncompatibleVersion`] when the
    /// coordinator does not support `OffsetCommit` v9 or lower.
    ///
    /// [`ClientError::IncompatibleVersion`]: krabka_client_core::ClientError::IncompatibleVersion
    /// [`ClientError::Server`]: krabka_client_core::ClientError::Server
    pub async fn alter_consumer_group_offsets(
        &mut self,
        group: &str,
        offsets: &BTreeMap<(String, i32), i64>,
    ) -> Result<Vec<ConsumerGroupOffsetOutcome>, AdminError> {
        self.alter_consumer_group_offsets_with_retry(group, offsets, KAFKA_ADMIN_RETRY)
            .await
    }

    async fn alter_consumer_group_offsets_with_retry(
        &mut self,
        group: &str,
        offsets: &BTreeMap<(String, i32), i64>,
        retry: RetryPolicy,
    ) -> Result<Vec<ConsumerGroupOffsetOutcome>, AdminError> {
        retry_coordinator_call(retry, async |find_coordinator| {
            self.offset_commit_attempt(group, offsets, find_coordinator)
                .await
        })
        .await
    }

    /// One attempt of `alter_consumer_group_offsets`. When `find_coordinator`
    /// is set, the attempt first finds the group coordinator and connects to
    /// it.
    async fn offset_commit_attempt(
        &mut self,
        group: &str,
        offsets: &BTreeMap<(String, i32), i64>,
        find_coordinator: bool,
    ) -> RetryAction<Vec<ConsumerGroupOffsetOutcome>> {
        if find_coordinator && let Err(action) = self.find_group_coordinator_attempt(group).await {
            return action;
        }
        match self.conn.send(offset_commit_request(group, offsets)).await {
            Ok(response) => offset_commit_retry_action(response),
            Err(AdminError::Transport(error))
                if AdminClient::is_retriable_transport_error(&error) =>
            {
                RetryAction::FindCoordinator(Err(error.into()))
            }
            Err(error) => RetryAction::Done(Err(error)),
        }
    }

    /// Lists the groups of the whole cluster, as Apache Kafka's
    /// `KafkaAdminClient.listGroups` does.
    ///
    /// A broker lists only the groups that it coordinates. The call first
    /// sends `Metadata` to find every broker. It then sends `ListGroups` to
    /// each broker at the same time, on a new connection, and merges the
    /// answers:
    ///
    /// - The result has one [`GroupListing`] for each group id. A group that
    ///   two brokers list appears once.
    /// - A broker that fails gives one [`ListGroupsError`]. The groups of the
    ///   other brokers stay in [`ListGroupsResult::valid`].
    /// - `COORDINATOR_LOAD_IN_PROGRESS` (14) and `COORDINATOR_NOT_AVAILABLE`
    ///   (15) make the call send `ListGroups` to that broker again. A failed
    ///   connection or a lost connection also makes the call try again. A
    ///   rejected TLS or SASL authentication does not. It is an error for that
    ///   broker at once.
    /// - Kafka's default `default.api.timeout.ms` (60 s) is the deadline of
    ///   each broker. Each attempt on a broker (connection, TLS and SASL
    ///   handshakes, and request) gets the time that remains. A broker that
    ///   has not answered at the deadline gives `REQUEST_TIMED_OUT` (7), with
    ///   the last error in the message.
    /// - A filter that the broker version does not support gives
    ///   `UNSUPPORTED_VERSION` (35) for that broker. `states_filter` needs
    ///   `ListGroups` v4 (KIP-518) and `types_filter` needs v5 (KIP-848).
    ///   Below v5, the call omits a type filter that holds `Classic`, and
    ///   `Consumer` at most, as Kafka's `ListGroupsRequest.Builder.build`
    ///   does.
    /// - [`ListGroupsOptions::protocol_types`] filters on the client.
    ///
    /// # Errors
    /// Returns an error when the `Metadata` request fails, or when the
    /// metadata names no broker after the timeout. A failure on one broker is
    /// an entry in [`ListGroupsResult::errors`], not an error of the call.
    pub async fn list_groups(
        &self,
        options: &ListGroupsOptions,
    ) -> Result<ListGroupsResult, AdminError> {
        self.list_groups_with_retry(options, KAFKA_ADMIN_RETRY)
            .await
    }

    async fn list_groups_with_retry(
        &self,
        options: &ListGroupsOptions,
        retry: RetryPolicy,
    ) -> Result<ListGroupsResult, AdminError> {
        let start = tokio::time::Instant::now();
        let brokers = self.list_groups_brokers(start, retry).await?;
        let request = ListGroupsRequest {
            states_filter: options
                .group_states
                .iter()
                .map(|state| state.as_str().to_owned())
                .collect(),
            types_filter: options
                .types
                .iter()
                .map(|group_type| group_type.as_str().to_owned())
                .collect(),
            ..Default::default()
        };
        let min_version = list_groups_min_version(options);
        let answers = futures_util::future::join_all(brokers.iter().map(|broker| {
            list_groups_on_broker(
                broker,
                self.options.clone(),
                request.clone(),
                min_version,
                start,
                retry,
            )
        }))
        .await;

        let mut listings = BTreeMap::new();
        let mut errors = Vec::new();
        for (broker, answer) in brokers.into_iter().zip(answers) {
            match answer {
                Ok(groups) => {
                    for group in groups {
                        if options.protocol_types.is_empty()
                            || options.protocol_types.contains(&group.protocol_type)
                        {
                            let listing = GroupListing::from(group);
                            listings.insert(listing.group_id.clone(), listing);
                        }
                    }
                }
                Err(error) => errors.push(ListGroupsError {
                    node_id: broker.node_id,
                    host: broker.host,
                    port: broker.port,
                    error,
                }),
            }
        }
        Ok(ListGroupsResult {
            valid: listings.into_values().collect(),
            errors,
        })
    }

    /// Sends `Metadata` for no topics and returns each broker once. Kafka's
    /// `listGroups` retries a broker list that is empty
    /// (`StaleMetadataException`) until the timeout.
    async fn list_groups_brokers(
        &self,
        start: tokio::time::Instant,
        retry: RetryPolicy,
    ) -> Result<Vec<MetadataResponseBroker>, AdminError> {
        let mut backoff = retry.initial_backoff;
        loop {
            let response = self
                .conn
                .send(MetadataRequest {
                    topics: Some(Vec::new()),
                    allow_auto_topic_creation: true,
                    ..Default::default()
                })
                .await?;
            let mut brokers = response.brokers;
            let mut seen = BTreeSet::new();
            brokers.retain(|broker| seen.insert(broker.node_id));
            if !brokers.is_empty() {
                return Ok(brokers);
            }
            if start.elapsed() >= retry.timeout {
                return Err(AdminError::Protocol(
                    "failed to find brokers to send ListGroups: metadata has no brokers".to_owned(),
                ));
            }
            tokio::time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2).min(retry.max_backoff);
        }
    }

    /// Returns `(topic, partition) → committed_offset` for the named group.
    ///
    /// The call requests all topics and partitions (`topics: None`). It skips
    /// a partition that has an error code, and a partition with a committed
    /// offset below 0, which means no committed offset.
    ///
    /// The response names each topic, so the client negotiates `OffsetFetch`
    /// v2 to v9, as Apache Kafka's `ListConsumerGroupOffsetsHandler` does.
    ///
    /// The call retries the coordinator error codes as Apache Kafka's
    /// `ListConsumerGroupOffsetsHandler.handleGroupError` does:
    ///
    /// - `COORDINATOR_LOAD_IN_PROGRESS` (14): send the request again to the
    ///   same coordinator.
    /// - `COORDINATOR_NOT_AVAILABLE` (15) and `NOT_COORDINATOR` (16): find the
    ///   coordinator again, then send the request again.
    ///
    /// A `FindCoordinator` answer of 14 or 15 also makes the call find the
    /// coordinator again, as Kafka's `CoordinatorStrategy.handleError` does.
    /// The call waits between attempts, and it stops when Kafka's default
    /// `default.api.timeout.ms` (60 s) elapses.
    ///
    /// # Errors
    /// Returns an error when encoding, transport, or response handling fails.
    /// Returns [`AdminError::Broker`] when the coordinator answers with a
    /// group error code that Kafka does not retry, or with a retriable code
    /// after the timeout. Returns [`ClientError::Server`] when
    /// `FindCoordinator` answers with an error code that Kafka does not retry,
    /// or with a retriable code after the timeout. Returns
    /// [`ClientError::IncompatibleVersion`] when the coordinator does not
    /// support `OffsetFetch` v2 to v9.
    ///
    /// [`ClientError::IncompatibleVersion`]: krabka_client_core::ClientError::IncompatibleVersion
    /// [`ClientError::Server`]: krabka_client_core::ClientError::Server
    pub async fn list_consumer_group_offsets(
        &mut self,
        group: &str,
    ) -> Result<BTreeMap<(String, i32), i64>, AdminError> {
        self.list_consumer_group_offsets_with_retry(group, KAFKA_ADMIN_RETRY)
            .await
    }

    async fn list_consumer_group_offsets_with_retry(
        &mut self,
        group: &str,
        retry: RetryPolicy,
    ) -> Result<BTreeMap<(String, i32), i64>, AdminError> {
        retry_coordinator_call(retry, async |find_coordinator| {
            self.offset_fetch_attempt(group, find_coordinator).await
        })
        .await
    }

    /// One attempt of `list_consumer_group_offsets`. When `find_coordinator`
    /// is set, the attempt first finds the group coordinator and connects to
    /// it.
    async fn offset_fetch_attempt(
        &mut self,
        group: &str,
        find_coordinator: bool,
    ) -> RetryAction<BTreeMap<(String, i32), i64>> {
        if find_coordinator && let Err(action) = self.find_group_coordinator_attempt(group).await {
            return action;
        }
        match self.conn.send(offset_fetch_request(group)).await {
            Ok(response) => offset_fetch_retry_action(committed_offsets(group, response)),
            Err(error) => RetryAction::Done(Err(error)),
        }
    }

    /// Finds the group coordinator and connects to it. A `FindCoordinator`
    /// answer that Kafka's `CoordinatorStrategy.handleError` retries gives
    /// [`RetryAction::FindCoordinator`]. Another failure gives
    /// [`RetryAction::Done`].
    async fn find_group_coordinator_attempt<T>(
        &mut self,
        group: &str,
    ) -> Result<(), RetryAction<T>> {
        match self.reconnect_group_coordinator(group).await {
            Ok(()) => Ok(()),
            Err(AdminError::Transport(ClientError::Server { error_code }))
                if is_retriable_find_coordinator_error(error_code) =>
            {
                Err(RetryAction::FindCoordinator(Err(AdminError::Transport(
                    ClientError::Server { error_code },
                ))))
            }
            Err(error) => Err(RetryAction::Done(Err(error))),
        }
    }

    async fn reconnect_group_coordinator(&mut self, group: &str) -> Result<(), AdminError> {
        let response = self
            .conn
            .send(build_find_coordinator(group, CoordinatorKeyType::Group))
            .await?;
        let coordinator = coordinator_endpoint(group, response)?;
        self.reconnect(&format_host_port(&coordinator.host, coordinator.port))
            .await
    }
}

/// `COORDINATOR_LOAD_IN_PROGRESS`: the coordinator is loading the group.
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
/// `COORDINATOR_NOT_AVAILABLE`: no broker coordinates the group now.
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// `NOT_COORDINATOR`: the broker does not coordinate the group.
const NOT_COORDINATOR: i16 = 16;

/// Whether Kafka's `CoordinatorStrategy.handleError` retries this
/// `FindCoordinator` error code.
fn is_retriable_find_coordinator_error(code: i16) -> bool {
    matches!(
        code,
        COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE
    )
}

/// Map the result of one `OffsetFetch` attempt to a retry action, as Kafka's
/// `ListConsumerGroupOffsetsHandler.handleGroupError` does. 14 retries on the
/// same coordinator. 15 and 16 unmap the group, so the next attempt finds the
/// coordinator again. Every other result is final.
fn offset_fetch_retry_action<T>(result: Result<T, AdminError>) -> RetryAction<T> {
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

/// `REBALANCE_IN_PROGRESS`: the group is rebalancing.
const REBALANCE_IN_PROGRESS: i16 = 27;

/// Map one `OffsetCommit` response to a retry action, as Kafka's
/// `AlterConsumerGroupOffsetsHandler.handleResponse` does. A partition with 14
/// or 27 retries the request on the same coordinator. A partition with 15 or
/// 16 unmaps the group, so the next attempt finds the coordinator again. The
/// unmap wins when both occur, as the handler returns `ApiResult.unmapped`.
/// Every other code is a final per-partition outcome.
fn offset_commit_retry_action(
    response: OffsetCommitResponse,
) -> RetryAction<Vec<ConsumerGroupOffsetOutcome>> {
    let outcomes: Vec<ConsumerGroupOffsetOutcome> = response
        .topics
        .into_iter()
        .flat_map(|topic| {
            let name = topic.name;
            topic
                .partitions
                .into_iter()
                .map(move |partition| ConsumerGroupOffsetOutcome {
                    topic: name.clone(),
                    partition: partition.partition_index,
                    error: kafka_error_if(partition.error_code, None),
                })
        })
        .collect();
    let codes = || {
        outcomes
            .iter()
            .filter_map(|outcome| outcome.error.as_ref().map(|error| error.code))
    };
    if codes().any(|code| matches!(code, COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR)) {
        RetryAction::FindCoordinator(Ok(outcomes))
    } else if codes()
        .any(|code| matches!(code, COORDINATOR_LOAD_IN_PROGRESS | REBALANCE_IN_PROGRESS))
    {
        RetryAction::SameCoordinator(Ok(outcomes))
    } else {
        RetryAction::Done(Ok(outcomes))
    }
}

/// The lowest `ListGroups` version that carries the filters of `options`, as
/// Kafka's `ListGroupsRequest.Builder.build` requires it.
///
/// `states_filter` needs v4. `types_filter` needs v5, except when it holds
/// `Classic` and `Consumer` at most, with `Classic`. A broker below v5 lists
/// only classic groups, so the filter can be omitted. The request encoder
/// omits `types_filter` below v5.
fn list_groups_min_version(options: &ListGroupsOptions) -> i16 {
    let types_need_v5 = !options.types.is_empty()
        && (!options.types.contains(&GroupType::Classic)
            || options
                .types
                .iter()
                .any(|group_type| !matches!(group_type, GroupType::Classic | GroupType::Consumer)));
    if types_need_v5 {
        5
    } else if options.group_states.is_empty() {
        list_groups_request::MIN_VERSION
    } else {
        4
    }
}

/// Sends `ListGroups` to one broker until it answers, as one Kafka
/// `listGroups` node call does.
///
/// `COORDINATOR_LOAD_IN_PROGRESS`, `COORDINATOR_NOT_AVAILABLE`, a failed
/// connection and a lost connection make the call try again. A rejected
/// authentication does not, because Kafka's `Call.fail` does not retry an
/// `AuthenticationException`.
///
/// `retry.timeout` after `start` is the deadline of the call. Each attempt
/// (the TCP connection, the TLS and SASL handshakes, and the `ListGroups`
/// request) gets the time that remains. When the deadline passes, the call
/// fails with `REQUEST_TIMED_OUT` (7), as Kafka's `Call.fail` gives a
/// `TimeoutException` for a call past its deadline.
async fn list_groups_on_broker(
    broker: &MetadataResponseBroker,
    options: ConnectionOptions,
    request: ListGroupsRequest,
    min_version: i16,
    start: tokio::time::Instant,
    retry: RetryPolicy,
) -> Result<Vec<ListedGroup>, KafkaError> {
    let host_port = format_host_port(&broker.host, broker.port);
    let deadline = start + retry.timeout;
    let mut backoff = retry.initial_backoff;
    let mut connection = None;
    let mut attempts = 0_u32;
    let mut last = None;
    while tokio::time::Instant::now() < deadline {
        attempts += 1;
        let attempt = list_groups_attempt(
            &mut connection,
            &host_port,
            &options,
            request.clone(),
            min_version,
        );
        match tokio::time::timeout_at(deadline, attempt).await {
            Ok(RetryAction::Done(result)) => {
                return result.map_err(|error| list_groups_kafka_error(&error));
            }
            Ok(RetryAction::SameCoordinator(result) | RetryAction::FindCoordinator(result)) => {
                match result {
                    Ok(groups) => return Ok(groups),
                    Err(error) => last = Some(error),
                }
            }
            Err(_) => break,
        }
        tracing::debug!(
            node_id = broker.node_id,
            "ListGroups got a retriable error; retrying"
        );
        tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + backoff)).await;
        backoff = backoff.saturating_mul(2).min(retry.max_backoff);
    }
    Err(list_groups_timeout(attempts, last.as_ref()))
}

/// The `REQUEST_TIMED_OUT` error of a `ListGroups` node call past its
/// deadline. The message names the attempts and the last retriable error, as
/// the `TimeoutException` of Kafka's `Call.handleTimeoutFailure` holds the
/// attempt count and the cause.
fn list_groups_timeout(attempts: u32, last: Option<&AdminError>) -> KafkaError {
    const REQUEST_TIMED_OUT: i16 = 7;
    let cause = last
        .map(|error| format!("; last error: {error}"))
        .unwrap_or_default();
    KafkaError {
        code: REQUEST_TIMED_OUT,
        name: kafka_error_name(REQUEST_TIMED_OUT),
        message: Some(format!(
            "ListGroups timed out after {attempts} attempt(s){cause}"
        )),
    }
}

/// One `ListGroups` attempt on one broker. The attempt connects first when
/// `connection` is empty, and empties it after a connection error.
async fn list_groups_attempt(
    connection: &mut Option<Connection>,
    host_port: &str,
    options: &ConnectionOptions,
    request: ListGroupsRequest,
    min_version: i16,
) -> RetryAction<Vec<ListedGroup>> {
    let current = match connection {
        Some(current) => current,
        None => match AdminClient::connect_one(host_port, options.clone()).await {
            Ok(new) => connection.insert(new),
            Err(error) if error.is_authentication_failure() => {
                return RetryAction::Done(Err(error));
            }
            Err(error) => return RetryAction::SameCoordinator(Err(error)),
        },
    };
    match send_connection_at_least(current, request, min_version).await {
        Ok(response) => match response.error_code {
            0 => RetryAction::Done(Ok(response.groups)),
            code => {
                let error = Err(AdminError::Broker {
                    api: "ListGroups",
                    code,
                    name: kafka_error_name(code),
                    message: None,
                });
                if matches!(
                    code,
                    COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE
                ) {
                    RetryAction::SameCoordinator(error)
                } else {
                    RetryAction::Done(error)
                }
            }
        },
        Err(error) if AdminClient::is_retriable_transport_error(&error) => {
            *connection = None;
            RetryAction::SameCoordinator(Err(error.into()))
        }
        Err(error) => RetryAction::Done(Err(error.into())),
    }
}

/// The Kafka error code of a `ListGroups` failure on one broker, as Kafka's
/// `Errors.forException` maps the exception class.
///
/// A rejected authentication maps as its Kafka exception class does:
/// `UnsupportedSaslMechanismException` to 33, `IllegalSaslStateException` to
/// 34, `SaslAuthenticationException` to 58, and `SslAuthenticationException`
/// to 40, the code of its superclass `InvalidConfigurationException`.
fn list_groups_kafka_error(error: &AdminError) -> KafkaError {
    const UNKNOWN_SERVER_ERROR: i16 = -1;
    const REQUEST_TIMED_OUT: i16 = 7;
    const NETWORK_EXCEPTION: i16 = 13;
    const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
    const ILLEGAL_SASL_STATE: i16 = 34;
    const UNSUPPORTED_VERSION: i16 = 35;
    const INVALID_CONFIG: i16 = 40;
    const SASL_AUTHENTICATION_FAILED: i16 = 58;
    let (code, message) = match error {
        AdminError::Broker { code, message, .. } => (*code, message.clone()),
        AdminError::Transport(ClientError::IncompatibleVersion { .. }) => {
            (UNSUPPORTED_VERSION, Some(error.to_string()))
        }
        AdminError::Transport(ClientError::Timeout(_)) => {
            (REQUEST_TIMED_OUT, Some(error.to_string()))
        }
        AdminError::Transport(
            ClientError::Connect { .. } | ClientError::Disconnected | ClientError::Io(_),
        ) => (NETWORK_EXCEPTION, Some(error.to_string())),
        AdminError::Transport(ClientError::Authentication { source, .. }) => {
            let code = match source {
                AuthenticationError::Tls(_) => INVALID_CONFIG,
                AuthenticationError::Sasl(SaslAuthenticationError::UnsupportedMechanism(_)) => {
                    UNSUPPORTED_SASL_MECHANISM
                }
                AuthenticationError::Sasl(SaslAuthenticationError::IllegalState(_)) => {
                    ILLEGAL_SASL_STATE
                }
                AuthenticationError::Sasl(SaslAuthenticationError::Failed(_)) => {
                    SASL_AUTHENTICATION_FAILED
                }
            };
            (code, Some(error.to_string()))
        }
        _ => (UNKNOWN_SERVER_ERROR, Some(error.to_string())),
    };
    KafkaError {
        code,
        name: kafka_error_name(code),
        message,
    }
}

/// An `OffsetCommit` request that names its topics, capped at v9.
///
/// `OffsetCommit` v10 names each topic by id only. Apache Kafka's
/// `AlterConsumerGroupOffsetsHandler.buildBatchedRequest` uses
/// `OffsetCommitRequest.Builder.forTopicNames`, which allows v9 at most. This
/// type gives the same cap to version negotiation.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TopicNameOffsetCommit(OffsetCommitRequest);

impl Encode for TopicNameOffsetCommit {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for TopicNameOffsetCommit {
    const API_KEY: i16 = offset_commit_request::API_KEY;
    const MIN_VERSION: i16 = offset_commit_request::MIN_VERSION;
    /// The last `OffsetCommit` version that carries topic names.
    const MAX_VERSION: i16 = 9;
    const FLEXIBLE_MIN: i16 = offset_commit_request::FLEXIBLE_MIN;
    type Response = OffsetCommitResponse;
}

/// An `OffsetFetch` request that names its topics, limited to v2 to v9.
///
/// `OffsetFetch` v10 names each topic by id only. Apache Kafka's
/// `ListConsumerGroupOffsetsHandler.buildBatchedRequest` uses
/// `OffsetFetchRequest.Builder.forTopicNames`, which allows v9 at most. The
/// admin request asks for all topics, and `Builder.build` rejects that request
/// below v2 (`TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION`). This type gives
/// the same range to version negotiation.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TopicNameOffsetFetch(OffsetFetchRequest);

impl Encode for TopicNameOffsetFetch {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for TopicNameOffsetFetch {
    const API_KEY: i16 = offset_fetch_request::API_KEY;
    /// The first `OffsetFetch` version that can ask for all topics.
    const MIN_VERSION: i16 = 2;
    /// The last `OffsetFetch` version that carries topic names.
    const MAX_VERSION: i16 = 9;
    const FLEXIBLE_MIN: i16 = offset_fetch_request::FLEXIBLE_MIN;
    type Response = OffsetFetchResponse;
}

/// Builds the admin `OffsetFetch` request for all topics of `group`.
///
/// The request fills the v2 to v7 fields (`group_id`, `topics`) and the v8+
/// `groups` array, so it is valid at each negotiated version. Kafka's
/// `OffsetFetchRequest.Builder.maybeDowngrade` moves the group into the v2 to
/// v7 fields in the same way.
fn offset_fetch_request(group: &str) -> TopicNameOffsetFetch {
    TopicNameOffsetFetch(OffsetFetchRequest {
        group_id: group.into(),
        topics: None,
        groups: vec![OffsetFetchRequestGroup {
            group_id: group.into(),
            member_id: None,
            member_epoch: -1,
            topics: None,
            ..Default::default()
        }],
        require_stable: false,
        ..Default::default()
    })
}

/// Reads the committed offsets of `group` from an `OffsetFetch` response.
///
/// v8 and v9 put the group in `groups`. v2 to v7 put the group error code and
/// the topics at the top level, which Kafka's `OffsetFetchResponse.group`
/// reads in the same way. As in Kafka's
/// `ListConsumerGroupOffsetsHandler.handleResponse`, a partition with an error
/// code gives no row.
fn committed_offsets(
    group: &str,
    response: OffsetFetchResponse,
) -> Result<BTreeMap<(String, i32), i64>, AdminError> {
    let (error_code, rows): (i16, Vec<(String, i32, i64, i16)>) = if response.groups.is_empty() {
        (
            response.error_code,
            response
                .topics
                .into_iter()
                .flat_map(|topic| {
                    let name = topic.name;
                    topic.partitions.into_iter().map(move |partition| {
                        (
                            name.clone(),
                            partition.partition_index,
                            partition.committed_offset,
                            partition.error_code,
                        )
                    })
                })
                .collect(),
        )
    } else {
        let mut error_code = 0;
        let mut rows = Vec::new();
        for entry in response
            .groups
            .into_iter()
            .filter(|entry| entry.group_id == group)
        {
            error_code = entry.error_code;
            for topic in entry.topics {
                let name = topic.name;
                rows.extend(topic.partitions.into_iter().map(|partition| {
                    (
                        name.clone(),
                        partition.partition_index,
                        partition.committed_offset,
                        partition.error_code,
                    )
                }));
            }
        }
        (error_code, rows)
    };
    if error_code != 0 {
        return Err(AdminError::Broker {
            api: "OffsetFetch",
            code: error_code,
            name: kafka_error_name(error_code),
            message: Some(format!("group={group}")),
        });
    }
    Ok(rows
        .into_iter()
        .filter_map(|(topic, partition, offset, partition_error)| {
            if partition_error != 0 {
                tracing::warn!(
                    topic,
                    partition,
                    error_code = partition_error,
                    "skipping the committed offset of a partition with an error"
                );
                None
            } else if offset < 0 {
                None
            } else {
                Some(((topic, partition), offset))
            }
        })
        .collect())
}

fn offset_commit_request(
    group: &str,
    offsets: &BTreeMap<(String, i32), i64>,
) -> TopicNameOffsetCommit {
    let mut topics = BTreeMap::<String, Vec<OffsetCommitRequestPartition>>::new();
    for ((topic, partition), offset) in offsets {
        topics
            .entry(topic.clone())
            .or_default()
            .push(OffsetCommitRequestPartition {
                partition_index: *partition,
                committed_offset: *offset,
                committed_leader_epoch: -1,
                committed_metadata: None,
                ..Default::default()
            });
    }
    TopicNameOffsetCommit(OffsetCommitRequest {
        group_id: group.into(),
        generation_id_or_member_epoch: -1,
        member_id: String::new(),
        topics: topics
            .into_iter()
            .map(|(name, partitions)| OffsetCommitRequestTopic {
                name,
                partitions,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use assert2::assert;
    use bytes::{Buf, BytesMut};
    use krabka_client_core::{
        AuthenticationError, ClientError, MockBroker, MockReply, MockSaslAnswer,
        SaslAuthenticationError,
        security::{ClientSecurity, SaslCredentials},
    };
    use krabka_protocol::{
        Decode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            find_coordinator_request,
            find_coordinator_response::FindCoordinatorResponse,
            list_groups_response::ListGroupsResponse,
            metadata_request,
            metadata_response::MetadataResponse,
            offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
            offset_fetch_response::{
                OffsetFetchResponseGroup, OffsetFetchResponsePartition,
                OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
            },
            sasl_handshake_request,
        },
    };

    use super::*;

    #[test]
    fn offset_reset_builds_admin_commit() {
        let offsets = BTreeMap::from([(("orders".into(), 2), 41)]);
        let request = offset_commit_request("worker", &offsets);
        let expected = TopicNameOffsetCommit(OffsetCommitRequest {
            group_id: "worker".into(),
            topics: vec![OffsetCommitRequestTopic {
                name: "orders".into(),
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 2,
                    committed_offset: 41,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
        assert!(request == expected);
    }

    fn encode(response: &impl Encode, version: i16, flexible: bool) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        if flexible {
            bytes.extend_from_slice(&[0]);
        }
        response.encode(&mut bytes, version).unwrap();
        bytes.to_vec()
    }

    /// The `OffsetCommit` and `OffsetFetch` version ranges that a mock broker
    /// advertises.
    #[derive(Clone, Copy)]
    struct GroupRanges {
        offset_commit: (i16, i16),
        offset_fetch: (i16, i16),
    }

    /// An `ApiVersions` response that advertises `ranges`.
    fn api_versions(ranges: GroupRanges) -> Vec<u8> {
        encode(
            &ApiVersionsResponse {
                api_keys: vec![
                    ApiVersion {
                        api_key: api_versions_request::API_KEY,
                        min_version: 0,
                        max_version: 0,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: find_coordinator_request::API_KEY,
                        min_version: 0,
                        max_version: 0,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: offset_commit_request::API_KEY,
                        min_version: ranges.offset_commit.0,
                        max_version: ranges.offset_commit.1,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: offset_fetch_request::API_KEY,
                        min_version: ranges.offset_fetch.0,
                        max_version: ranges.offset_fetch.1,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: metadata_request::API_KEY,
                        min_version: 13,
                        max_version: 13,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            0,
            false,
        )
    }

    /// Decodes a request body of type `R` behind its request header.
    fn decode_request<R: ProtocolRequest + for<'de> Decode<'de>>(
        mut body: &[u8],
        version: i16,
    ) -> R {
        let client_id_len = body.get_i16();
        body.advance(usize::try_from(client_id_len).expect("client id length"));
        if version >= R::FLEXIBLE_MIN {
            body.advance(1);
        }
        R::decode(&mut body, version).expect("request decodes")
    }

    /// A bootstrap broker that sends every group RPC to `coordinator`.
    async fn bootstrap_for(
        coordinator: &MockBroker,
        ranges: GroupRanges,
        group_rpcs: Arc<AtomicUsize>,
    ) -> MockBroker {
        let coordinator_addr = coordinator.addr;
        MockBroker::start(move |api_key, version, _, _| match api_key {
            api_versions_request::API_KEY => Some(api_versions(ranges)),
            find_coordinator_request::API_KEY => Some(encode(
                &FindCoordinatorResponse {
                    node_id: 2,
                    host: coordinator_addr.ip().to_string(),
                    port: i32::from(coordinator_addr.port()),
                    ..Default::default()
                },
                version,
                false,
            )),
            offset_commit_request::API_KEY => {
                group_rpcs.fetch_add(1, Ordering::SeqCst);
                Some(encode(&OffsetCommitResponse::default(), version, true))
            }
            offset_fetch_request::API_KEY => {
                group_rpcs.fetch_add(1, Ordering::SeqCst);
                Some(encode(&OffsetFetchResponse::default(), version, true))
            }
            metadata_request::API_KEY => Some(encode(&MetadataResponse::default(), version, true)),
            _ => None,
        })
        .await
    }

    /// The negotiated `OffsetCommit` version and the result of
    /// `alter_consumer_group_offsets`, with a version error as its ranges.
    type CommitResult = Result<Vec<ConsumerGroupOffsetOutcome>, (i16, i16, i16, i16, i16)>;

    /// Apache Kafka's `AlterConsumerGroupOffsetsHandler` builds `OffsetCommit`
    /// with `OffsetCommitRequest.Builder.forTopicNames`, which caps the version
    /// at 9. The request names each topic at every negotiated version.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alter_consumer_group_offsets_sends_offset_commit_by_topic_name() {
        let sent_request = |version| {
            vec![(
                version,
                OffsetCommitRequest {
                    group_id: "workers".into(),
                    generation_id_or_member_epoch: -1,
                    topics: vec![OffsetCommitRequestTopic {
                        name: "orders".into(),
                        partitions: vec![OffsetCommitRequestPartition {
                            partition_index: 2,
                            committed_offset: 41,
                            committed_leader_epoch: -1,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )]
        };
        let committed = Ok(vec![ConsumerGroupOffsetOutcome {
            topic: "orders".into(),
            partition: 2,
            error: None,
        }]);
        for (name, offset_commit_range, expected_requests, expected_result) in [
            (
                "coordinator stops at v7",
                (2, 7),
                sent_request(7),
                committed.clone(),
            ),
            (
                "coordinator stops at v9",
                (2, 9),
                sent_request(9),
                committed.clone(),
            ),
            (
                "coordinator supports v10",
                (2, 10),
                sent_request(9),
                committed.clone(),
            ),
            (
                "coordinator supports only v10",
                (10, 10),
                Vec::new(),
                Err((offset_commit_request::API_KEY, 10, 10, 2, 9)),
            ),
        ] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_in_mock = Arc::clone(&requests);
            let ranges = GroupRanges {
                offset_commit: offset_commit_range,
                offset_fetch: (2, 10),
            };
            let coordinator = MockBroker::start(move |api_key, version, _, body| match api_key {
                api_versions_request::API_KEY => Some(api_versions(ranges)),
                offset_commit_request::API_KEY => {
                    let request: OffsetCommitRequest = decode_request(body, version);
                    let response = OffsetCommitResponse {
                        topics: request
                            .topics
                            .iter()
                            .map(|topic| OffsetCommitResponseTopic {
                                name: topic.name.clone(),
                                partitions: topic
                                    .partitions
                                    .iter()
                                    .map(|partition| OffsetCommitResponsePartition {
                                        partition_index: partition.partition_index,
                                        ..Default::default()
                                    })
                                    .collect(),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    };
                    requests_in_mock
                        .lock()
                        .expect("requests lock")
                        .push((version, request));
                    Some(encode(
                        &response,
                        version,
                        version >= offset_commit_request::FLEXIBLE_MIN,
                    ))
                }
                _ => None,
            })
            .await;
            let bootstrap = bootstrap_for(&coordinator, ranges, Arc::default()).await;
            let mut admin = AdminClient::connect(&[bootstrap.addr.to_string()])
                .await
                .expect("admin connects");

            let result: CommitResult = admin
                .alter_consumer_group_offsets(
                    "workers",
                    &BTreeMap::from([(("orders".into(), 2), 41)]),
                )
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
            assert!(
                (requests, result) == (expected_requests, expected_result),
                "case {name}"
            );
        }
    }

    /// The `OffsetFetch` answer of the mock coordinator at `version`: one
    /// committed offset, one partition with no committed offset, and one
    /// partition with an error code. v2 to v7 put the topics at the top level.
    fn offset_fetch_response(version: i16) -> OffsetFetchResponse {
        let rows = [
            ("orders", 2, 41, 0),
            ("orders", 3, -1, 0),
            ("payments", 0, 5, 3),
        ];
        if version < 8 {
            OffsetFetchResponse {
                topics: rows
                    .iter()
                    .map(|(name, partition_index, committed_offset, error_code)| {
                        OffsetFetchResponseTopic {
                            name: (*name).into(),
                            partitions: vec![OffsetFetchResponsePartition {
                                partition_index: *partition_index,
                                committed_offset: *committed_offset,
                                error_code: *error_code,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            }
        } else {
            OffsetFetchResponse {
                groups: vec![OffsetFetchResponseGroup {
                    group_id: "workers".into(),
                    topics: rows
                        .iter()
                        .map(|(name, partition_index, committed_offset, error_code)| {
                            OffsetFetchResponseTopics {
                                name: (*name).into(),
                                partitions: vec![OffsetFetchResponsePartitions {
                                    partition_index: *partition_index,
                                    committed_offset: *committed_offset,
                                    error_code: *error_code,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }
        }
    }

    /// The negotiated `OffsetFetch` version and the result of
    /// `list_consumer_group_offsets`, with a version error as its ranges.
    type FetchResult = Result<BTreeMap<(String, i32), i64>, (i16, i16, i16, i16, i16)>;

    /// Apache Kafka's `ListConsumerGroupOffsetsHandler` builds `OffsetFetch`
    /// with `OffsetFetchRequest.Builder.forTopicNames`, which caps the version
    /// at 9. A request for all topics also needs v2. The response names each
    /// topic, so the call sends no `Metadata` request.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_consumer_group_offsets_sends_offset_fetch_by_topic_name() {
        let legacy_request = |version| {
            vec![(
                version,
                OffsetFetchRequest {
                    group_id: "workers".into(),
                    topics: None,
                    ..Default::default()
                },
            )]
        };
        let grouped_request = |version| {
            vec![(
                version,
                OffsetFetchRequest {
                    groups: vec![OffsetFetchRequestGroup {
                        group_id: "workers".into(),
                        member_id: None,
                        member_epoch: -1,
                        topics: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )]
        };
        let fetched = Ok(BTreeMap::from([(("orders".into(), 2), 41)]));
        for (name, offset_fetch_range, expected_requests, expected_result) in [
            (
                "coordinator stops at v7",
                (2, 7),
                legacy_request(7),
                fetched.clone(),
            ),
            (
                "coordinator stops at v8",
                (2, 8),
                grouped_request(8),
                fetched.clone(),
            ),
            (
                "coordinator stops at v9",
                (2, 9),
                grouped_request(9),
                fetched.clone(),
            ),
            (
                "coordinator supports v10",
                (2, 10),
                grouped_request(9),
                fetched.clone(),
            ),
            (
                "coordinator supports only v10",
                (10, 10),
                Vec::new(),
                Err((offset_fetch_request::API_KEY, 10, 10, 2, 9)),
            ),
            (
                "coordinator supports only v1",
                (1, 1),
                Vec::new(),
                Err((offset_fetch_request::API_KEY, 1, 1, 2, 9)),
            ),
        ] {
            let ranges = GroupRanges {
                offset_commit: (2, 9),
                offset_fetch: offset_fetch_range,
            };
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_in_mock = Arc::clone(&requests);
            let metadata_requests = Arc::new(AtomicUsize::new(0));
            let metadata_requests_in_mock = Arc::clone(&metadata_requests);
            let coordinator = MockBroker::start(move |api_key, version, _, body| match api_key {
                api_versions_request::API_KEY => Some(api_versions(ranges)),
                offset_fetch_request::API_KEY => {
                    let request: OffsetFetchRequest = decode_request(body, version);
                    requests_in_mock
                        .lock()
                        .expect("requests lock")
                        .push((version, request));
                    Some(encode(
                        &offset_fetch_response(version),
                        version,
                        version >= offset_fetch_request::FLEXIBLE_MIN,
                    ))
                }
                metadata_request::API_KEY => {
                    metadata_requests_in_mock.fetch_add(1, Ordering::SeqCst);
                    Some(encode(&MetadataResponse::default(), version, true))
                }
                _ => None,
            })
            .await;
            let bootstrap = bootstrap_for(&coordinator, ranges, Arc::default()).await;
            let mut admin = AdminClient::connect(&[bootstrap.addr.to_string()])
                .await
                .expect("admin connects");

            let result: FetchResult =
                admin
                    .list_consumer_group_offsets("workers")
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
            assert!(
                (requests, metadata_requests.load(Ordering::SeqCst), result)
                    == (expected_requests, 0, expected_result),
                "case {name}"
            );
        }
    }

    /// The result of `committed_offsets`, with a broker error as its fields.
    type OffsetsResult =
        Result<BTreeMap<(String, i32), i64>, (&'static str, i16, &'static str, Option<String>)>;

    /// Kafka's `OffsetFetchResponse.group` reads the group error code from the
    /// top level below v8 and from the group entry from v8.
    #[test]
    fn committed_offsets_reads_both_response_shapes() {
        let offsets = BTreeMap::from([(("orders".into(), 2), 41)]);
        let group_error =
            |code, name| Err(("OffsetFetch", code, name, Some("group=workers".into())));
        for (name, response, expected) in [
            (
                "v2 to v7 shape",
                offset_fetch_response(7),
                Ok(offsets.clone()),
            ),
            ("v8 and v9 shape", offset_fetch_response(9), Ok(offsets)),
            (
                "v2 to v7 group error",
                OffsetFetchResponse {
                    error_code: 15,
                    ..Default::default()
                },
                group_error(15, "COORDINATOR_NOT_AVAILABLE"),
            ),
            (
                "v8 and v9 group error",
                OffsetFetchResponse {
                    groups: vec![OffsetFetchResponseGroup {
                        group_id: "workers".into(),
                        error_code: 16,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                group_error(16, "NOT_COORDINATOR"),
            ),
        ] {
            let result: OffsetsResult =
                committed_offsets("workers", response).map_err(|error| match error {
                    AdminError::Broker {
                        api,
                        code,
                        name,
                        message,
                    } => (api, code, name, message),
                    other => panic!("case {name}: unexpected error {other:?}"),
                });
            assert!(result == expected, "case {name}");
        }
    }

    /// The error of `list_consumer_group_offsets` in a form that tests can
    /// compare: an `OffsetFetch` group code or a `FindCoordinator` code.
    #[derive(Debug, PartialEq, Eq)]
    enum ListOffsetsError {
        OffsetFetch(i16),
        FindCoordinator(i16),
    }

    /// The error codes that the mock brokers answer, one per attempt. The
    /// last code repeats. A code of 0 answers with the coordinator or with the
    /// offsets of `offset_fetch_response`.
    #[derive(Default)]
    struct RetryScript {
        find_coordinator: Vec<i16>,
        offset_fetch: Vec<i16>,
        offset_commit: Vec<i16>,
        find_coordinator_requests: usize,
        offset_fetch_requests: usize,
        offset_commit_requests: usize,
    }

    impl RetryScript {
        fn next(codes: &[i16], requests: &mut usize) -> i16 {
            let code = codes[(*requests).min(codes.len() - 1)];
            *requests += 1;
            code
        }
    }

    /// A mock broker that answers `FindCoordinator` from `script` with
    /// `coordinator` as the coordinator, and `OffsetFetch` and `OffsetCommit`
    /// from `script`. An `OffsetCommit` answer puts its code on each
    /// partition of the request.
    async fn scripted_group_broker(
        script: Arc<Mutex<RetryScript>>,
        coordinator: Arc<Mutex<Option<std::net::SocketAddr>>>,
    ) -> MockBroker {
        let ranges = GroupRanges {
            offset_commit: (2, 9),
            offset_fetch: (2, 9),
        };
        MockBroker::start(move |api_key, version, _, body| match api_key {
            api_versions_request::API_KEY => Some(api_versions(ranges)),
            offset_commit_request::API_KEY => {
                let request: OffsetCommitRequest = decode_request(body, version);
                let mut script = script.lock().expect("script lock");
                let script = &mut *script;
                let error_code =
                    RetryScript::next(&script.offset_commit, &mut script.offset_commit_requests);
                let response = OffsetCommitResponse {
                    topics: request
                        .topics
                        .iter()
                        .map(|topic| OffsetCommitResponseTopic {
                            name: topic.name.clone(),
                            partitions: topic
                                .partitions
                                .iter()
                                .map(|partition| OffsetCommitResponsePartition {
                                    partition_index: partition.partition_index,
                                    error_code,
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                };
                Some(encode(
                    &response,
                    version,
                    version >= offset_commit_request::FLEXIBLE_MIN,
                ))
            }
            find_coordinator_request::API_KEY => {
                let mut script = script.lock().expect("script lock");
                let script = &mut *script;
                let error_code = RetryScript::next(
                    &script.find_coordinator,
                    &mut script.find_coordinator_requests,
                );
                let addr = coordinator
                    .lock()
                    .expect("coordinator lock")
                    .expect("coordinator address");
                Some(encode(
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
            offset_fetch_request::API_KEY => {
                let mut script = script.lock().expect("script lock");
                let script = &mut *script;
                let error_code =
                    RetryScript::next(&script.offset_fetch, &mut script.offset_fetch_requests);
                let response = if error_code == 0 {
                    offset_fetch_response(version)
                } else {
                    OffsetFetchResponse {
                        groups: vec![OffsetFetchResponseGroup {
                            group_id: "workers".into(),
                            error_code,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }
                };
                Some(encode(&response, version, true))
            }
            _ => None,
        })
        .await
    }

    /// Apache Kafka's `ListConsumerGroupOffsetsHandler.handleGroupError`
    /// retries `COORDINATOR_LOAD_IN_PROGRESS` (14) on the same coordinator,
    /// unmaps the group on `COORDINATOR_NOT_AVAILABLE` (15) and
    /// `NOT_COORDINATOR` (16) so the driver finds the coordinator again, and
    /// fails on every other group code. `CoordinatorStrategy.handleError`
    /// retries a `FindCoordinator` answer of 14 or 15. The driver stops at the
    /// call timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_consumer_group_offsets_retries_coordinator_errors_as_kafka_does() {
        const LONG: Duration = Duration::from_secs(5);
        const NOW: Duration = Duration::ZERO;
        let offsets = || Ok(BTreeMap::from([(("orders".into(), 2), 41)]));
        let fetch_error = |code| Err(ListOffsetsError::OffsetFetch(code));
        let find_error = |code| Err(ListOffsetsError::FindCoordinator(code));
        for (name, find_coordinator, offset_fetch, timeout, expected) in [
            ("no error", vec![0], vec![0], LONG, (offsets(), 1, 1)),
            (
                "coordinator load in progress retries on the same coordinator",
                vec![0],
                vec![14, 14, 0],
                LONG,
                (offsets(), 1, 3),
            ),
            (
                "coordinator not available finds the coordinator again",
                vec![0],
                vec![15, 0],
                LONG,
                (offsets(), 2, 2),
            ),
            (
                "not coordinator finds the coordinator again",
                vec![0],
                vec![16, 0],
                LONG,
                (offsets(), 2, 2),
            ),
            (
                "coordinator load in progress past the timeout fails",
                vec![0],
                vec![14],
                NOW,
                (fetch_error(14), 1, 1),
            ),
            (
                "not coordinator past the timeout fails",
                vec![0],
                vec![16],
                NOW,
                (fetch_error(16), 1, 1),
            ),
            (
                "group authorization failed is final",
                vec![0],
                vec![30],
                LONG,
                (fetch_error(30), 1, 1),
            ),
            (
                "another retriable group code is final",
                vec![0],
                vec![7],
                LONG,
                (fetch_error(7), 1, 1),
            ),
            (
                "find coordinator answers coordinator not available, then the coordinator",
                vec![15, 0],
                vec![0],
                LONG,
                (offsets(), 2, 1),
            ),
            (
                "find coordinator answers coordinator load in progress, then the coordinator",
                vec![14, 0],
                vec![0],
                LONG,
                (offsets(), 2, 1),
            ),
            (
                "find coordinator not available past the timeout fails",
                vec![15],
                vec![0],
                NOW,
                (find_error(15), 1, 0),
            ),
            (
                "find coordinator group authorization failed is final",
                vec![30],
                vec![0],
                LONG,
                (find_error(30), 1, 0),
            ),
        ] {
            let script = Arc::new(Mutex::new(RetryScript {
                find_coordinator,
                offset_fetch,
                ..RetryScript::default()
            }));
            let coordinator_addr = Arc::new(Mutex::new(None));
            let coordinator =
                scripted_group_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
            *coordinator_addr.lock().expect("coordinator lock") = Some(coordinator.addr);
            let bootstrap =
                scripted_group_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
            let mut admin = AdminClient::connect(&[bootstrap.addr.to_string()])
                .await
                .expect("admin connects");

            let result = admin
                .list_consumer_group_offsets_with_retry(
                    "workers",
                    RetryPolicy {
                        timeout,
                        initial_backoff: Duration::from_millis(1),
                        max_backoff: Duration::from_millis(1),
                        jitter: 0.0,
                    },
                )
                .await
                .map_err(|error| match error {
                    AdminError::Broker {
                        api: "OffsetFetch",
                        code,
                        ..
                    } => ListOffsetsError::OffsetFetch(code),
                    AdminError::Transport(ClientError::Server { error_code }) => {
                        ListOffsetsError::FindCoordinator(error_code)
                    }
                    other => panic!("case {name}: unexpected error {other:?}"),
                });

            bootstrap.stop();
            coordinator.stop();
            let script = script.lock().expect("script lock");
            assert!(
                (
                    result,
                    script.find_coordinator_requests,
                    script.offset_fetch_requests
                ) == expected,
                "case {name}"
            );
        }
    }

    /// Apache Kafka's `AlterConsumerGroupOffsetsHandler.handleError` retries
    /// `COORDINATOR_LOAD_IN_PROGRESS` (14) and `REBALANCE_IN_PROGRESS` (27) on
    /// the same coordinator, unmaps the group on `COORDINATOR_NOT_AVAILABLE`
    /// (15) and `NOT_COORDINATOR` (16) so the driver finds the coordinator
    /// again, and keeps every other partition code as the outcome.
    /// `CoordinatorStrategy.handleError` retries a `FindCoordinator` answer of
    /// 14 or 15. The driver stops at the call timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alter_consumer_group_offsets_retries_coordinator_errors_as_kafka_does() {
        const LONG: Duration = Duration::from_secs(5);
        const NOW: Duration = Duration::ZERO;
        let outcome = |code| {
            Ok(vec![ConsumerGroupOffsetOutcome {
                topic: "orders".into(),
                partition: 2,
                error: kafka_error_if(code, None),
            }])
        };
        let find_error = |code| Err(code);
        for (name, find_coordinator, offset_commit, timeout, expected) in [
            ("no error", vec![0], vec![0], LONG, (outcome(0), 1, 1)),
            (
                "coordinator load in progress retries on the same coordinator",
                vec![0],
                vec![14, 14, 0],
                LONG,
                (outcome(0), 1, 3),
            ),
            (
                "rebalance in progress retries on the same coordinator",
                vec![0],
                vec![27, 0],
                LONG,
                (outcome(0), 1, 2),
            ),
            (
                "coordinator not available finds the coordinator again",
                vec![0],
                vec![15, 0],
                LONG,
                (outcome(0), 2, 2),
            ),
            (
                "not coordinator finds the coordinator again",
                vec![0],
                vec![16, 0],
                LONG,
                (outcome(0), 2, 2),
            ),
            (
                "rebalance in progress past the timeout gives the last outcome",
                vec![0],
                vec![27],
                NOW,
                (outcome(27), 1, 1),
            ),
            (
                "not coordinator past the timeout gives the last outcome",
                vec![0],
                vec![16],
                NOW,
                (outcome(16), 1, 1),
            ),
            (
                "unknown member id is a final outcome",
                vec![0],
                vec![25],
                LONG,
                (outcome(25), 1, 1),
            ),
            (
                "find coordinator answers coordinator not available, then the coordinator",
                vec![15, 0],
                vec![0],
                LONG,
                (outcome(0), 2, 1),
            ),
            (
                "find coordinator group authorization failed is final",
                vec![30],
                vec![0],
                LONG,
                (find_error(30), 1, 0),
            ),
        ] {
            let script = Arc::new(Mutex::new(RetryScript {
                find_coordinator,
                offset_commit,
                ..RetryScript::default()
            }));
            let coordinator_addr = Arc::new(Mutex::new(None));
            let coordinator =
                scripted_group_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
            *coordinator_addr.lock().expect("coordinator lock") = Some(coordinator.addr);
            let bootstrap =
                scripted_group_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
            let mut admin = AdminClient::connect(&[bootstrap.addr.to_string()])
                .await
                .expect("admin connects");

            let result = admin
                .alter_consumer_group_offsets_with_retry(
                    "workers",
                    &BTreeMap::from([(("orders".into(), 2), 41)]),
                    RetryPolicy {
                        timeout,
                        initial_backoff: Duration::from_millis(1),
                        max_backoff: Duration::from_millis(1),
                        jitter: 0.0,
                    },
                )
                .await
                .map_err(|error| match error {
                    AdminError::Transport(ClientError::Server { error_code }) => error_code,
                    other => panic!("case {name}: unexpected error {other:?}"),
                });

            bootstrap.stop();
            coordinator.stop();
            let script = script.lock().expect("script lock");
            assert!(
                (
                    result,
                    script.find_coordinator_requests,
                    script.offset_commit_requests
                ) == expected,
                "case {name}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_offset_rpcs_use_the_group_coordinator() {
        let ranges = GroupRanges {
            offset_commit: (2, 10),
            offset_fetch: (2, 10),
        };
        let coordinator_group_rpcs = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&coordinator_group_rpcs);
        let coordinator = MockBroker::start(move |api_key, version, _, _| match api_key {
            api_versions_request::API_KEY => Some(api_versions(ranges)),
            offset_commit_request::API_KEY => {
                seen.fetch_add(1, Ordering::SeqCst);
                Some(encode(
                    &OffsetCommitResponse {
                        topics: vec![OffsetCommitResponseTopic {
                            name: "orders".into(),
                            partitions: vec![OffsetCommitResponsePartition {
                                partition_index: 2,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    version,
                    true,
                ))
            }
            offset_fetch_request::API_KEY => {
                seen.fetch_add(1, Ordering::SeqCst);
                Some(encode(&offset_fetch_response(version), version, true))
            }
            _ => None,
        })
        .await;

        let bootstrap_group_rpcs = Arc::new(AtomicUsize::new(0));
        let bootstrap =
            bootstrap_for(&coordinator, ranges, Arc::clone(&bootstrap_group_rpcs)).await;
        let mut admin = AdminClient::connect(&[bootstrap.addr.to_string()])
            .await
            .expect("admin connects");

        let committed = admin
            .alter_consumer_group_offsets("workers", &BTreeMap::from([(("orders".into(), 2), 41)]))
            .await
            .expect("offset commit succeeds");
        let mut admin = AdminClient::connect(&[bootstrap.addr.to_string()])
            .await
            .expect("second admin connects");
        let fetched = admin
            .list_consumer_group_offsets("workers")
            .await
            .expect("offset fetch succeeds");

        assert!(bootstrap_group_rpcs.load(Ordering::SeqCst) == 0);
        assert!(coordinator_group_rpcs.load(Ordering::SeqCst) == 2);
        assert!(
            committed
                == vec![ConsumerGroupOffsetOutcome {
                    topic: "orders".into(),
                    partition: 2,
                    error: None,
                }]
        );
        assert!(fetched == BTreeMap::from([(("orders".into(), 2), 41)]));
        bootstrap.stop();
        coordinator.stop();
    }

    /// The answer of a mock broker to one `ListGroups` request.
    #[derive(Clone)]
    enum ListGroupsAnswer {
        /// The groups that the broker coordinates. The broker applies the
        /// state and type filters of the request.
        Groups(Vec<ListedGroup>),
        /// A top-level error code.
        Error(i16),
    }

    /// One mock broker of a `ListGroups` case: its highest `ListGroups`
    /// version, and its answers, one per request. The last answer repeats.
    #[derive(Clone)]
    struct ListGroupsBroker {
        max_version: i16,
        answers: Vec<ListGroupsAnswer>,
    }

    fn listed(group_id: &str, protocol_type: &str, state: &str, group_type: &str) -> ListedGroup {
        ListedGroup {
            group_id: group_id.into(),
            protocol_type: protocol_type.into(),
            group_state: state.into(),
            group_type: group_type.into(),
            ..Default::default()
        }
    }

    fn consumer_group(group_id: &str) -> ListedGroup {
        listed(group_id, "consumer", "Stable", "Classic")
    }

    fn stable_listing(group_id: &str) -> GroupListing {
        GroupListing {
            group_id: group_id.into(),
            group_type: Some(GroupType::Classic),
            protocol_type: "consumer".into(),
            group_state: Some(GroupState::Stable),
        }
    }

    fn groups(ids: &[&str]) -> ListGroupsBroker {
        ListGroupsBroker {
            max_version: 5,
            answers: vec![ListGroupsAnswer::Groups(
                ids.iter().map(|id| consumer_group(id)).collect(),
            )],
        }
    }

    /// A mock broker that answers `Metadata` with every address in `cluster`
    /// (node ids from 1) and `ListGroups` from `script`. It records each
    /// `ListGroups` request with its version.
    async fn list_groups_broker(
        script: ListGroupsBroker,
        cluster: Arc<Mutex<Vec<std::net::SocketAddr>>>,
        requests: Arc<Mutex<Vec<(i16, ListGroupsRequest)>>>,
    ) -> MockBroker {
        MockBroker::start_with_replies(move |api_key, version, _, body| {
            MockSaslAnswer::Accept
                .reply(api_key, version)
                .unwrap_or_else(|| {
                    list_groups_reply(&script, &cluster, &requests, api_key, version, body)
                        .map_or(MockReply::Silent, MockReply::Respond)
                })
        })
        .await
    }

    /// The reply of a `list_groups_broker` to one request other than SASL.
    fn list_groups_reply(
        script: &ListGroupsBroker,
        cluster: &Mutex<Vec<std::net::SocketAddr>>,
        requests: &Mutex<Vec<(i16, ListGroupsRequest)>>,
        api_key: i16,
        version: i16,
        body: &[u8],
    ) -> Option<Vec<u8>> {
        match api_key {
            api_versions_request::API_KEY => Some(encode(
                &ApiVersionsResponse {
                    api_keys: vec![
                        ApiVersion {
                            api_key: api_versions_request::API_KEY,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: metadata_request::API_KEY,
                            min_version: 12,
                            max_version: 12,
                            ..Default::default()
                        },
                        ApiVersion {
                            api_key: list_groups_request::API_KEY,
                            max_version: script.max_version,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                0,
                false,
            )),
            metadata_request::API_KEY => Some(encode(
                &MetadataResponse {
                    brokers: cluster
                        .lock()
                        .expect("cluster lock")
                        .iter()
                        .zip(1..)
                        .map(|(addr, node_id)| MetadataResponseBroker {
                            node_id,
                            host: addr.ip().to_string(),
                            port: i32::from(addr.port()),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                version,
                true,
            )),
            list_groups_request::API_KEY => {
                let request: ListGroupsRequest = decode_request(body, version);
                let mut requests = requests.lock().expect("requests lock");
                let answer = &script.answers[requests.len().min(script.answers.len() - 1)];
                let response = match answer {
                    ListGroupsAnswer::Groups(groups) => ListGroupsResponse {
                        groups: groups
                            .iter()
                            .filter(|group| {
                                (request.states_filter.is_empty()
                                    || request.states_filter.contains(&group.group_state))
                                    && (request.types_filter.is_empty()
                                        || request.types_filter.contains(&group.group_type))
                            })
                            .cloned()
                            .collect(),
                        ..Default::default()
                    },
                    ListGroupsAnswer::Error(error_code) => ListGroupsResponse {
                        error_code: *error_code,
                        ..Default::default()
                    },
                };
                requests.push((version, request));
                Some(encode(
                    &response,
                    version,
                    version >= list_groups_request::FLEXIBLE_MIN,
                ))
            }
            _ => None,
        }
    }

    /// An expected broker failure: node id, error code and message.
    type ExpectedError = (i32, i16, Option<String>);

    /// One `ListGroups` fan-out case on a cluster of three mock brokers.
    struct ListGroupsCase {
        name: &'static str,
        brokers: [ListGroupsBroker; 3],
        options: ListGroupsOptions,
        timeout: Duration,
        valid: Vec<GroupListing>,
        errors: Vec<ExpectedError>,
        /// The `ListGroups` requests that each broker receives.
        requests: [Vec<(i16, ListGroupsRequest)>; 3],
    }

    /// The deadline of a case that ends before it.
    const LONG: Duration = Duration::from_secs(10);
    /// A deadline that ends before the first retry, which waits `BACKOFF`.
    const SHORT: Duration = Duration::from_millis(500);
    /// The wait between two `ListGroups` attempts on one broker.
    const BACKOFF: Duration = Duration::from_secs(1);

    fn plain(version: i16) -> (i16, ListGroupsRequest) {
        (version, ListGroupsRequest::default())
    }

    fn stable(version: i16) -> (i16, ListGroupsRequest) {
        (
            version,
            ListGroupsRequest {
                states_filter: vec!["Stable".into()],
                ..Default::default()
            },
        )
    }

    /// The request for the type filter `Classic` and `Consumer`. The encoder
    /// omits `types_filter` below v5.
    fn classic_and_consumer(version: i16) -> (i16, ListGroupsRequest) {
        (
            version,
            ListGroupsRequest {
                types_filter: if version >= 5 {
                    vec!["Consumer".into(), "Classic".into()]
                } else {
                    Vec::new()
                },
                ..Default::default()
            },
        )
    }

    fn consumer_type(version: i16) -> (i16, ListGroupsRequest) {
        (
            version,
            ListGroupsRequest {
                types_filter: vec!["Consumer".into()],
                ..Default::default()
            },
        )
    }

    /// The message of a `ListGroups` version error on a broker with
    /// `ListGroups` v0 to `broker_max`.
    fn unsupported(broker_max: i16, client_min: i16) -> String {
        AdminError::Transport(ClientError::IncompatibleVersion {
            api_key: list_groups_request::API_KEY,
            broker_min: 0,
            broker_max,
            client_min,
            client_max: 5,
        })
        .to_string()
    }

    fn states(states: &[GroupState]) -> ListGroupsOptions {
        ListGroupsOptions {
            group_states: states.iter().copied().collect(),
            ..ListGroupsOptions::default()
        }
    }

    fn types(types: &[GroupType]) -> ListGroupsOptions {
        ListGroupsOptions {
            types: types.iter().copied().collect(),
            ..ListGroupsOptions::default()
        }
    }

    fn broker_error(code: i16) -> ListGroupsBroker {
        ListGroupsBroker {
            max_version: 5,
            answers: vec![ListGroupsAnswer::Error(code)],
        }
    }

    fn below(max_version: i16, ids: &[&str]) -> ListGroupsBroker {
        ListGroupsBroker {
            max_version,
            ..groups(ids)
        }
    }

    /// Runs `case` on a new cluster of three mock brokers. Broker 1 is the
    /// bootstrap broker.
    async fn run_list_groups_case(case: ListGroupsCase) {
        let cluster = Arc::new(Mutex::new(Vec::new()));
        let mut brokers = Vec::new();
        let mut requests = Vec::new();
        for script in case.brokers {
            let sent = Arc::new(Mutex::new(Vec::new()));
            brokers.push(list_groups_broker(script, Arc::clone(&cluster), Arc::clone(&sent)).await);
            requests.push(sent);
        }
        *cluster.lock().expect("cluster lock") = brokers.iter().map(|broker| broker.addr).collect();
        let admin = AdminClient::connect(&[brokers[0].addr.to_string()])
            .await
            .expect("admin connects");

        let result = admin
            .list_groups_with_retry(
                &case.options,
                RetryPolicy {
                    timeout: case.timeout,
                    initial_backoff: BACKOFF,
                    max_backoff: BACKOFF,
                    jitter: 0.0,
                },
            )
            .await
            .expect("list_groups succeeds");

        let expected = ListGroupsResult {
            valid: case.valid,
            errors: case
                .errors
                .into_iter()
                .map(|(node_id, code, message)| {
                    let addr = brokers[usize::try_from(node_id - 1).expect("node id")].addr;
                    ListGroupsError {
                        node_id,
                        host: addr.ip().to_string(),
                        port: i32::from(addr.port()),
                        error: KafkaError {
                            code,
                            name: kafka_error_name(code),
                            message,
                        },
                    }
                })
                .collect(),
        };
        for broker in brokers {
            broker.stop();
        }
        let requests = requests
            .iter()
            .map(|sent| sent.lock().expect("requests lock").clone())
            .collect::<Vec<_>>();
        let name = case.name;
        assert!(
            (result, requests) == (expected, case.requests.to_vec()),
            "case {name}"
        );
    }

    /// Apache Kafka's `KafkaAdminClient.listGroups` sends `Metadata`, then
    /// `ListGroups` to every broker, and merges the answers: one listing per
    /// group id, and one error per failed broker. It retries 14 and 15 on the
    /// broker until the deadline, and then `Call.fail` gives a
    /// `TimeoutException` (7) with the last error as its cause.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_groups_asks_every_broker_and_merges() {
        for case in [
            ListGroupsCase {
                name: "one group on each broker",
                brokers: [groups(&["a"]), groups(&["b"]), groups(&["c"])],
                options: ListGroupsOptions::default(),
                timeout: LONG,
                valid: vec![
                    stable_listing("a"),
                    stable_listing("b"),
                    stable_listing("c"),
                ],
                errors: Vec::new(),
                requests: [vec![plain(5)], vec![plain(5)], vec![plain(5)]],
            },
            ListGroupsCase {
                name: "broker 2 has no groups",
                brokers: [groups(&["a"]), groups(&[]), groups(&["c"])],
                options: ListGroupsOptions::default(),
                timeout: LONG,
                valid: vec![stable_listing("a"), stable_listing("c")],
                errors: Vec::new(),
                requests: [vec![plain(5)], vec![plain(5)], vec![plain(5)]],
            },
            ListGroupsCase {
                name: "a group on two brokers is listed once",
                brokers: [groups(&["a"]), groups(&["a", "b"]), groups(&["c"])],
                options: ListGroupsOptions::default(),
                timeout: LONG,
                valid: vec![
                    stable_listing("a"),
                    stable_listing("b"),
                    stable_listing("c"),
                ],
                errors: Vec::new(),
                requests: [vec![plain(5)], vec![plain(5)], vec![plain(5)]],
            },
            ListGroupsCase {
                name: "broker 2 answers coordinator not available until the timeout",
                brokers: [groups(&["a"]), broker_error(15), groups(&["c"])],
                options: ListGroupsOptions::default(),
                timeout: SHORT,
                valid: vec![stable_listing("a"), stable_listing("c")],
                errors: vec![(
                    2,
                    7,
                    Some(format!(
                        "ListGroups timed out after 1 attempt(s); last error: {}",
                        AdminError::Broker {
                            api: "ListGroups",
                            code: 15,
                            name: "COORDINATOR_NOT_AVAILABLE",
                            message: None,
                        }
                    )),
                )],
                requests: [vec![plain(5)], vec![plain(5)], vec![plain(5)]],
            },
            ListGroupsCase {
                name: "broker 2 answers coordinator load in progress, then its groups",
                brokers: [
                    groups(&["a"]),
                    ListGroupsBroker {
                        max_version: 5,
                        answers: vec![
                            ListGroupsAnswer::Error(14),
                            ListGroupsAnswer::Error(15),
                            ListGroupsAnswer::Groups(vec![consumer_group("b")]),
                        ],
                    },
                    groups(&["c"]),
                ],
                options: ListGroupsOptions::default(),
                timeout: LONG,
                valid: vec![
                    stable_listing("a"),
                    stable_listing("b"),
                    stable_listing("c"),
                ],
                errors: Vec::new(),
                requests: [
                    vec![plain(5)],
                    vec![plain(5), plain(5), plain(5)],
                    vec![plain(5)],
                ],
            },
            ListGroupsCase {
                name: "broker 2 answers cluster authorization failed",
                brokers: [groups(&["a"]), broker_error(31), groups(&["c"])],
                options: ListGroupsOptions::default(),
                timeout: LONG,
                valid: vec![stable_listing("a"), stable_listing("c")],
                errors: vec![(2, 31, None)],
                requests: [vec![plain(5)], vec![plain(5)], vec![plain(5)]],
            },
        ] {
            run_list_groups_case(case).await;
        }
    }

    /// Kafka's `listGroups` sends the state and type filters to every broker,
    /// and filters protocol types on the client. `ListGroupsRequest.Builder.build`
    /// rejects a state filter below v4 and a type filter below v5, except a
    /// type filter of `Classic` and `Consumer` at most, with `Classic`, which
    /// it omits. The rejection is an error for that broker only.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_groups_sends_filters_as_kafka_does() {
        for case in [
            ListGroupsCase {
                name: "state filter Stable is sent to every broker",
                brokers: [
                    ListGroupsBroker {
                        max_version: 5,
                        answers: vec![ListGroupsAnswer::Groups(vec![
                            listed("a", "consumer", "Empty", "Classic"),
                            consumer_group("b"),
                        ])],
                    },
                    groups(&["c"]),
                    groups(&[]),
                ],
                options: states(&[GroupState::Stable]),
                timeout: LONG,
                valid: vec![stable_listing("b"), stable_listing("c")],
                errors: Vec::new(),
                requests: [vec![stable(5)], vec![stable(5)], vec![stable(5)]],
            },
            ListGroupsCase {
                name: "state filter on a broker below v4 is unsupported on that broker",
                brokers: [groups(&["a"]), below(3, &["b"]), groups(&["c"])],
                options: states(&[GroupState::Stable]),
                timeout: LONG,
                valid: vec![stable_listing("a"), stable_listing("c")],
                errors: vec![(2, 35, Some(unsupported(3, 4)))],
                requests: [vec![stable(5)], Vec::new(), vec![stable(5)]],
            },
            ListGroupsCase {
                name: "type filter Consumer on a broker below v5 is unsupported on that broker",
                brokers: [groups(&["a"]), below(4, &["b"]), groups(&["c"])],
                options: types(&[GroupType::Consumer]),
                timeout: LONG,
                valid: Vec::new(),
                errors: vec![(2, 35, Some(unsupported(4, 5)))],
                requests: [vec![consumer_type(5)], Vec::new(), vec![consumer_type(5)]],
            },
            ListGroupsCase {
                name: "type filter Classic and Consumer on a broker below v5 is omitted",
                brokers: [groups(&["a"]), below(4, &["b"]), groups(&["c"])],
                options: types(&[GroupType::Classic, GroupType::Consumer]),
                timeout: LONG,
                valid: vec![
                    stable_listing("a"),
                    GroupListing {
                        group_type: None,
                        ..stable_listing("b")
                    },
                    stable_listing("c"),
                ],
                errors: Vec::new(),
                requests: [
                    vec![classic_and_consumer(5)],
                    vec![classic_and_consumer(4)],
                    vec![classic_and_consumer(5)],
                ],
            },
            ListGroupsCase {
                name: "protocol type filter is applied by the client",
                brokers: [
                    ListGroupsBroker {
                        max_version: 5,
                        answers: vec![ListGroupsAnswer::Groups(vec![
                            consumer_group("a"),
                            listed("connect-a", "connect", "Stable", "Classic"),
                            listed("simple", "", "Empty", "Classic"),
                        ])],
                    },
                    groups(&[]),
                    ListGroupsBroker {
                        max_version: 5,
                        answers: vec![ListGroupsAnswer::Groups(vec![listed(
                            "share", "share", "Stable", "Share",
                        )])],
                    },
                ],
                options: ListGroupsOptions::for_consumer_groups(),
                timeout: LONG,
                valid: vec![
                    stable_listing("a"),
                    GroupListing {
                        group_id: "simple".into(),
                        group_type: Some(GroupType::Classic),
                        protocol_type: String::new(),
                        group_state: Some(GroupState::Empty),
                    },
                ],
                errors: Vec::new(),
                requests: [
                    vec![classic_and_consumer(5)],
                    vec![classic_and_consumer(5)],
                    vec![classic_and_consumer(5)],
                ],
            },
        ] {
            run_list_groups_case(case).await;
        }
    }

    /// Broker 2 of a SASL cluster in
    /// `list_groups_isolates_a_secured_broker_that_stalls_or_rejects`.
    #[derive(Clone, Copy, Debug)]
    enum SecuredBroker {
        /// Accepts TCP and never answers the SASL exchange.
        Stalls,
        /// Answers `SaslAuthenticate` with `SASL_AUTHENTICATION_FAILED` (58).
        RejectsAuthentication,
    }

    /// A started `SecuredBroker`. `connections` counts the TCP connections
    /// that it accepts, or the `SaslHandshake` requests that it answers.
    struct StartedSecuredBroker {
        addr: std::net::SocketAddr,
        connections: Arc<AtomicUsize>,
        mock: Option<MockBroker>,
        listener: Option<tokio::task::JoinHandle<()>>,
    }

    impl SecuredBroker {
        async fn start(self) -> StartedSecuredBroker {
            let connections = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&connections);
            match self {
                Self::Stalls => {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("listener binds");
                    let addr = listener.local_addr().expect("listener address");
                    let task = tokio::spawn(async move {
                        let mut open = Vec::new();
                        while let Ok((stream, _)) = listener.accept().await {
                            counter.fetch_add(1, Ordering::SeqCst);
                            open.push(stream);
                        }
                    });
                    StartedSecuredBroker {
                        addr,
                        connections,
                        mock: None,
                        listener: Some(task),
                    }
                }
                Self::RejectsAuthentication => {
                    let mock = MockBroker::start_with_replies(move |api_key, version, _, _| {
                        if api_key == sasl_handshake_request::API_KEY {
                            counter.fetch_add(1, Ordering::SeqCst);
                        }
                        MockSaslAnswer::AuthenticateError(SASL_AUTHENTICATION_FAILED)
                            .reply(api_key, version)
                            .unwrap_or(MockReply::Silent)
                    })
                    .await;
                    StartedSecuredBroker {
                        addr: mock.addr,
                        connections,
                        mock: Some(mock),
                        listener: None,
                    }
                }
            }
        }

        /// The error code and message that `list_groups` reports for the
        /// broker at `addr`.
        fn expected_error(self, addr: std::net::SocketAddr) -> (i16, Option<String>) {
            match self {
                Self::Stalls => (
                    7,
                    Some("ListGroups timed out after 1 attempt(s)".to_owned()),
                ),
                Self::RejectsAuthentication => (
                    SASL_AUTHENTICATION_FAILED,
                    Some(
                        AdminError::Transport(ClientError::Authentication {
                            addr,
                            source: AuthenticationError::Sasl(SaslAuthenticationError::Failed(
                                "SaslAuthenticate(PLAIN) error_code=58 \
                                 error_message=Some(\"rejected by mock broker\")"
                                    .to_owned(),
                            )),
                        })
                        .to_string(),
                    ),
                ),
            }
        }
    }

    impl StartedSecuredBroker {
        fn stop(self) {
            if let Some(mock) = self.mock {
                mock.stop();
            }
            if let Some(listener) = self.listener {
                listener.abort();
            }
        }
    }

    const SASL_AUTHENTICATION_FAILED: i16 = 58;

    /// A broker that stalls during the SASL exchange, or that rejects
    /// authentication, fails on its own. The groups of the other brokers stay
    /// in the result.
    ///
    /// Kafka bounds each node call of `listGroups` by the call deadline
    /// (`KafkaAdminClient` `timeoutCallsInFlight`, and `Call.fail` gives a
    /// `TimeoutException`). Kafka fails a node call with the
    /// `AuthenticationException` of the node and does not retry it
    /// (`handleResponses`, `Call.fail`: the exception is not a
    /// `RetriableException`). `ApiError.fromThrowable` maps
    /// `SaslAuthenticationException` to 58.
    ///
    /// The test runs in real time. With paused time, Tokio moves the clock
    /// forward while loopback I/O is in flight, and the healthy brokers time
    /// out too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_groups_isolates_a_secured_broker_that_stalls_or_rejects() {
        const RETRY: RetryPolicy = RetryPolicy {
            timeout: Duration::from_secs(2),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(100),
            jitter: 0.0,
        };
        for broker_2 in [SecuredBroker::Stalls, SecuredBroker::RejectsAuthentication] {
            let cluster = Arc::new(Mutex::new(Vec::new()));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let broker_1 =
                list_groups_broker(groups(&["a"]), Arc::clone(&cluster), Arc::clone(&requests))
                    .await;
            let broker_2_started = broker_2.start().await;
            let broker_3 =
                list_groups_broker(groups(&["c"]), Arc::clone(&cluster), Arc::clone(&requests))
                    .await;
            let broker_2_addr = broker_2_started.addr;
            *cluster.lock().expect("cluster lock") =
                vec![broker_1.addr, broker_2_addr, broker_3.addr];
            let admin = AdminClient::connect_secured(
                &[broker_1.addr.to_string()],
                Some(ClientSecurity {
                    protocol: krabka_security::ListenerProtocol::SaslPlaintext,
                    tls: None,
                    sasl: Some(SaslCredentials::Plain {
                        username: "alice".into(),
                        password: "secret".into(),
                    }),
                    sasl_host: None,
                }),
            )
            .await
            .expect("admin connects");

            let outcome = tokio::time::timeout(
                RETRY.timeout * 5,
                admin.list_groups_with_retry(&ListGroupsOptions::default(), RETRY),
            )
            .await
            .map(|result| result.expect("list_groups succeeds"));
            let observed = (outcome, broker_2_started.connections.load(Ordering::SeqCst));

            let (code, message) = broker_2.expected_error(broker_2_addr);
            let expected = (
                Ok(ListGroupsResult {
                    valid: vec![stable_listing("a"), stable_listing("c")],
                    errors: vec![ListGroupsError {
                        node_id: 2,
                        host: broker_2_addr.ip().to_string(),
                        port: i32::from(broker_2_addr.port()),
                        error: KafkaError {
                            code,
                            name: kafka_error_name(code),
                            message,
                        },
                    }],
                }),
                1,
            );
            broker_1.stop();
            broker_2_started.stop();
            broker_3.stop();
            assert!(observed == expected, "case {broker_2:?}");
        }
    }

    /// Kafka's `GroupListing.isSimpleConsumerGroup` is
    /// `type.filter(gt -> gt == GroupType.CLASSIC).isPresent() && protocol.isEmpty()`.
    /// A listing with no type is not a simple consumer group.
    #[test]
    fn is_simple_consumer_group_matches_kafka() {
        for (group_type, protocol_type, expected) in [
            (Some(GroupType::Classic), "", true),
            (Some(GroupType::Classic), "consumer", false),
            (Some(GroupType::Consumer), "", false),
            (Some(GroupType::Unknown), "", false),
            (None, "", false),
            (None, "consumer", false),
        ] {
            let listing = GroupListing {
                group_id: "g".into(),
                group_type,
                protocol_type: protocol_type.into(),
                group_state: None,
            };
            assert!(
                listing.is_simple_consumer_group() == expected,
                "type {group_type:?}, protocol type {protocol_type:?}"
            );
        }
    }

    /// Kafka's `GroupListing` has no type or state when the broker sends an
    /// empty name, and `GroupState.parse` and `GroupType.parse` ignore case
    /// and map an unknown name to `Unknown`.
    #[test]
    fn group_listing_reads_listed_group() {
        for (group, expected) in [
            (
                listed("g", "consumer", "Stable", "Consumer"),
                GroupListing {
                    group_id: "g".into(),
                    group_type: Some(GroupType::Consumer),
                    protocol_type: "consumer".into(),
                    group_state: Some(GroupState::Stable),
                },
            ),
            (
                listed("g", "", "", ""),
                GroupListing {
                    group_id: "g".into(),
                    group_type: None,
                    protocol_type: String::new(),
                    group_state: None,
                },
            ),
            (
                listed("g", "consumer", "preparingrebalance", "STREAMS"),
                GroupListing {
                    group_id: "g".into(),
                    group_type: Some(GroupType::Streams),
                    protocol_type: "consumer".into(),
                    group_state: Some(GroupState::PreparingRebalance),
                },
            ),
            (
                listed("g", "consumer", "Rebalancing", "Future"),
                GroupListing {
                    group_id: "g".into(),
                    group_type: Some(GroupType::Unknown),
                    protocol_type: "consumer".into(),
                    group_state: Some(GroupState::Unknown),
                },
            ),
        ] {
            assert!(GroupListing::from(group) == expected);
        }
    }
}
