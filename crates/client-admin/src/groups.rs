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

use krabka_client_core::{
    AllTopicsOffsetFetch, AuthenticationError, ClientError, Connection, ConnectionOptions,
    CoordinatorKeyType, MembersOnlyLeaveGroup, SaslAuthenticationError, TopicNameOffsetCommit,
    build_find_coordinator, coordinator_endpoint,
};
use krabka_protocol::owned::{
    alter_share_group_offsets_request::{
        AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsRequestPartition,
        AlterShareGroupOffsetsRequestTopic,
    },
    alter_share_group_offsets_response::AlterShareGroupOffsetsResponse,
    delete_share_group_offsets_request::{
        DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsRequestTopic,
    },
    delete_share_group_offsets_response::DeleteShareGroupOffsetsResponse,
    leave_group_request::{LeaveGroupRequest, MemberIdentity},
    leave_group_response::LeaveGroupResponse,
    list_groups_request::{self, ListGroupsRequest},
    list_groups_response::ListedGroup,
    metadata_request::MetadataRequest,
    metadata_response::MetadataResponseBroker,
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
    offset_commit_response::{
        OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    },
    offset_delete_request::{
        OffsetDeleteRequest, OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
    },
    offset_delete_response::{
        OffsetDeleteResponse, OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
    },
    offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestGroup},
    offset_fetch_response::{
        OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
        OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
};

use crate::{
    AdminClient, AdminError, KafkaError, format_host_port,
    group_descriptions::DescribeGroupsOptions,
    kafka_error_if, kafka_error_name,
    retry::{CoordinatorRetry, RetryAction, RetryPolicy, connection_failure_action},
    send_connection_at_least,
};

macro_rules! group_offset_attempt {
    ($this:ident, $group:expr, $find_coordinator:expr, $request:expr, $handle:expr) => {{
        if $find_coordinator && let Err(action) = $this.find_group_coordinator_attempt($group).await
        {
            return action;
        }
        match $this.conn.send($request).await {
            Ok(response) => $handle($group, response),
            Err(error) => connection_failure_action(error),
        }
    }};
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupOffsetOutcome {
    pub topic: String,
    pub partition: i32,
    pub error: Option<KafkaError>,
}

struct GroupOffsetResponseTopic<P> {
    name: String,
    partitions: Vec<P>,
}

trait IntoGroupOffsetResponseTopic {
    type Partition: GroupOffsetResponsePartition;

    fn into_group_offset_topic(self) -> GroupOffsetResponseTopic<Self::Partition>;
}

trait GroupOffsetResponsePartition {
    fn index(&self) -> i32;
    fn error_code(&self) -> i16;
}

macro_rules! impl_group_offset_response {
    ($topic:ty, $partition:ty) => {
        impl IntoGroupOffsetResponseTopic for $topic {
            type Partition = $partition;

            fn into_group_offset_topic(self) -> GroupOffsetResponseTopic<Self::Partition> {
                GroupOffsetResponseTopic {
                    name: self.name,
                    partitions: self.partitions,
                }
            }
        }

        impl GroupOffsetResponsePartition for $partition {
            fn index(&self) -> i32 {
                self.partition_index
            }
            fn error_code(&self) -> i16 {
                self.error_code
            }
        }
    };
}

impl_group_offset_response!(OffsetCommitResponseTopic, OffsetCommitResponsePartition);
impl_group_offset_response!(OffsetDeleteResponseTopic, OffsetDeleteResponsePartition);

fn group_offset_outcomes<T: IntoGroupOffsetResponseTopic>(
    topics: impl IntoIterator<Item = T>,
) -> Vec<ConsumerGroupOffsetOutcome> {
    topics
        .into_iter()
        .flat_map(|topic| {
            let GroupOffsetResponseTopic { name, partitions } = topic.into_group_offset_topic();
            partitions
                .into_iter()
                .map(move |partition| ConsumerGroupOffsetOutcome {
                    topic: name.clone(),
                    partition: partition.index(),
                    error: kafka_error_if(partition.error_code(), None),
                })
        })
        .collect()
}

/// Offsets keyed by topic name and partition.
pub type GroupOffsets = BTreeMap<(String, i32), i64>;

#[derive(Clone, Copy)]
enum GroupOffsetKind {
    Consumer,
    Share,
}

#[derive(Clone, Copy)]
struct GroupOffsetUpdate<'a> {
    kind: GroupOffsetKind,
    group: &'a str,
    offsets: &'a GroupOffsets,
}

impl<'a> GroupOffsetUpdate<'a> {
    fn consumer(group: &'a str, offsets: &'a GroupOffsets) -> Self {
        Self {
            kind: GroupOffsetKind::Consumer,
            group,
            offsets,
        }
    }

    fn share(group: &'a str, offsets: &'a GroupOffsets) -> Self {
        Self {
            kind: GroupOffsetKind::Share,
            group,
            offsets,
        }
    }
}

struct GroupOffsetAttempt<'a> {
    update: GroupOffsetUpdate<'a>,
    find_coordinator: bool,
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
}

macro_rules! impl_group_name_parse {
    ($type:ty, $($doc:literal),+ $(,)?) => {
        impl $type {
            $(#[doc = $doc])+
            #[must_use]
            pub fn parse(name: &str) -> Self {
                parse_group_name(Self::ALL, name, Self::as_str, Self::Unknown)
            }
        }
    };
}
impl_group_name_parse!(
    GroupState,
    "Reads a state name without regard to case. A name that the client does not know gives [`GroupState::Unknown`], as Kafka's `GroupState.parse` does."
);

macro_rules! impl_display_from_as_str {
    ($type:ty) => {
        impl std::fmt::Display for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}
impl_display_from_as_str!(GroupState);

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
}
impl_group_name_parse!(
    GroupType,
    "Reads a type name without regard to case. A name that the client does not know gives [`GroupType::Unknown`], as Kafka's `GroupType.parse` does."
);

fn parse_group_name<T: Copy>(
    values: impl IntoIterator<Item = T>,
    name: &str,
    as_str: fn(T) -> &'static str,
    unknown: T,
) -> T {
    values
        .into_iter()
        .find(|value| as_str(*value).eq_ignore_ascii_case(name))
        .unwrap_or(unknown)
}

impl_display_from_as_str!(GroupType);

/// The options of [`AdminClient::list_groups`], as Apache Kafka's
/// `ListGroupsOptions` holds them. An empty set does not filter.
#[derive(Debug, Clone, Default, PartialEq, Eq, bon::Builder)]
pub struct ListGroupsOptions {
    /// The broker lists only groups in these states (`states_filter`).
    #[builder(default)]
    pub group_states: BTreeSet<GroupState>,
    /// The broker lists only groups of these types (`types_filter`).
    #[builder(default)]
    pub types: BTreeSet<GroupType>,
    /// The client keeps only groups with these protocol types.
    #[builder(default)]
    pub protocol_types: BTreeSet<String>,
}

impl ListGroupsOptions {
    /// Classic and consumer groups with the `consumer` protocol type or no
    /// protocol type, as Kafka's `ListGroupsOptions.forConsumerGroups` selects.
    #[must_use]
    pub fn for_consumer_groups() -> Self {
        Self::builder()
            .types(maplit::btreeset! {
                GroupType::Classic,
                GroupType::Consumer,
            })
            .protocol_types(maplit::btreeset! {
                String::new(),
                "consumer".to_owned(),
            })
            .build()
    }

    /// Share groups, as Kafka's `ListGroupsOptions.forShareGroups` selects.
    #[must_use]
    pub fn for_share_groups() -> Self {
        Self::builder()
            .types(maplit::btreeset! {
                GroupType::Share,
            })
            .build()
    }

    /// Streams groups, as Kafka's `ListGroupsOptions.forStreamsGroups`
    /// selects.
    #[must_use]
    pub fn for_streams_groups() -> Self {
        Self::builder()
            .types(maplit::btreeset! {
                GroupType::Streams,
            })
            .build()
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

macro_rules! group_offsets_method_body {
    (delete_consumer, $admin:ident, $group:ident, $input:ident) => {
        $admin
            .delete_consumer_group_offsets_with_retry($group, $input, $admin.retry)
            .await
    };
    (delete_streams, $admin:ident, $group:ident, $input:ident) => {
        $admin.delete_consumer_group_offsets($group, $input).await
    };
    (alter_consumer, $admin:ident, $group:ident, $input:ident) => {
        $admin
            .alter_group_offsets(GroupOffsetUpdate::consumer($group, $input))
            .await
    };
    (alter_share, $admin:ident, $group:ident, $input:ident) => {
        $admin
            .alter_group_offsets(GroupOffsetUpdate::share($group, $input))
            .await
    };
}

macro_rules! group_offsets_method {
    ($(#[$meta:meta])* $name:ident($input:ident: $input_type:ty) => $kind:ident) => {
        $(#[$meta])*
        pub async fn $name(
            &mut self,
            group: &str,
            $input: &$input_type,
        ) -> Result<Vec<ConsumerGroupOffsetOutcome>, AdminError> {
            group_offsets_method_body!($kind, self, group, $input)
        }
    };
}

macro_rules! retry_group_call {
    ($retry_policy:expr, |$find:ident| $attempt:expr) => {{
        let mut retry = CoordinatorRetry::new($retry_policy);
        loop {
            let $find = retry.find_coordinator();
            let action = retry.run($attempt).await;
            if let Some(result) = retry.next(action).await {
                return result;
            }
        }
    }};
}

impl AdminClient {
    group_offsets_method! {
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
        /// coordinator again. A failed or lost connection to the coordinator,
        /// including a TLS or SASL handshake that stops before the broker gives a
        /// verdict, makes the call find the coordinator again, as Kafka's
        /// `AdminApiDriver.onFailure` does. The call waits between attempts with
        /// Kafka's backoff, and it stops when Kafka's default
        /// `default.api.timeout.ms` (60 s) elapses. A call whose retries are
        /// unresolved at the deadline, or whose attempt is still running, fails
        /// with [`ClientError::Timeout`], as Kafka gives a `TimeoutException`.
        ///
        /// # Errors
        /// Returns an error when encoding, transport, or response handling fails.
        /// Returns [`ClientError::Server`] when `FindCoordinator` answers with an
        /// error code that Kafka does not retry. Returns
        /// [`ClientError::Timeout`] at the deadline. Returns [`ClientError::IncompatibleVersion`] when the
        /// coordinator does not support `OffsetCommit` v9 or lower.
        ///
        /// [`ClientError::IncompatibleVersion`]: ClientError::IncompatibleVersion
        /// [`ClientError::Server`]: ClientError::Server
        /// [`ClientError::Timeout`]: ClientError::Timeout
        alter_consumer_group_offsets(offsets: GroupOffsets) => alter_consumer
    }

    async fn alter_group_offsets(
        &mut self,
        update: GroupOffsetUpdate<'_>,
    ) -> Result<Vec<ConsumerGroupOffsetOutcome>, AdminError> {
        self.alter_group_offsets_with_retry(update, self.retry)
            .await
    }

    async fn alter_group_offsets_with_retry(
        &mut self,
        update: GroupOffsetUpdate<'_>,
        retry: RetryPolicy,
    ) -> Result<Vec<ConsumerGroupOffsetOutcome>, AdminError> {
        retry_group_call!(retry, |find_coordinator| {
            self.alter_group_offsets_attempt(GroupOffsetAttempt {
                update,
                find_coordinator,
            })
        })
    }

    /// Sends one update attempt, finding the coordinator first when requested.
    async fn alter_group_offsets_attempt(
        &mut self,
        attempt: GroupOffsetAttempt<'_>,
    ) -> RetryAction<Vec<ConsumerGroupOffsetOutcome>> {
        let GroupOffsetAttempt {
            update,
            find_coordinator,
        } = attempt;
        if find_coordinator
            && let Err(action) = self.find_group_coordinator_attempt(update.group).await
        {
            return action;
        }
        match update.kind {
            GroupOffsetKind::Consumer => self
                .conn
                .send(offset_commit_request(update.group, update.offsets))
                .await
                .map_or_else(connection_failure_action, offset_commit_retry_action),
            GroupOffsetKind::Share => self
                .conn
                .send(alter_share_group_offsets_request(
                    update.group,
                    update.offsets,
                ))
                .await
                .map_or_else(connection_failure_action, |response| {
                    alter_share_group_offsets_retry_action(update.offsets, response)
                }),
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
        self.list_groups_with_retry(options, self.retry).await
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
    /// coordinator again, as Kafka's `CoordinatorStrategy.handleError` does. A
    /// failed or lost connection to the coordinator does the same, as
    /// `AdminApiDriver.onFailure` does. The call waits between attempts, and
    /// it stops when Kafka's default `default.api.timeout.ms` (60 s) elapses.
    /// A call whose retries are unresolved at the deadline, or whose attempt
    /// is still running, fails with [`ClientError::Timeout`].
    ///
    /// # Errors
    /// Returns an error when encoding, transport, or response handling fails.
    /// Returns [`AdminError::Broker`] when the coordinator answers with a
    /// group error code that Kafka does not retry. Returns
    /// [`ClientError::Server`] when `FindCoordinator` answers with an error
    /// code that Kafka does not retry. Returns [`ClientError::Timeout`] at
    /// the deadline. Returns
    /// [`ClientError::IncompatibleVersion`] when the coordinator does not
    /// support `OffsetFetch` v2 to v9.
    ///
    /// [`ClientError::IncompatibleVersion`]: ClientError::IncompatibleVersion
    /// [`ClientError::Server`]: ClientError::Server
    /// [`ClientError::Timeout`]: ClientError::Timeout
    pub async fn list_consumer_group_offsets(
        &mut self,
        group: &str,
    ) -> Result<GroupOffsets, AdminError> {
        self.list_consumer_group_offsets_with_retry(group, self.retry)
            .await
    }

    async fn list_consumer_group_offsets_with_retry(
        &mut self,
        group: &str,
        retry: RetryPolicy,
    ) -> Result<GroupOffsets, AdminError> {
        retry_group_call!(retry, |find| self.offset_fetch_attempt(group, find))
    }

    /// One attempt of `list_consumer_group_offsets`. When `find_coordinator`
    /// is set, the attempt first finds the group coordinator and connects to
    /// it.
    async fn offset_fetch_attempt(
        &mut self,
        group: &str,
        find_coordinator: bool,
    ) -> RetryAction<GroupOffsets> {
        let request = offset_fetch_request(group);
        group_offset_attempt!(self, group, find_coordinator, request, |group, response| {
            offset_fetch_retry_action(committed_offsets(group, response))
        })
    }

    /// Finds the group coordinator and connects to it. A `FindCoordinator`
    /// answer that Kafka's `CoordinatorStrategy.handleError` retries, and a
    /// connection failure to the coordinator that it names, give
    /// [`RetryAction::FindCoordinator`], as `AdminApiDriver.onFailure` looks
    /// the coordinator up again after a disconnect. Another failure gives
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
            Err(error) => Err(connection_failure_action(error)),
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

    group_offsets_method! {
        /// Deletes committed offsets of an inactive consumer group for the named
        /// partitions with `OffsetDelete`, as Apache Kafka's
        /// `deleteConsumerGroupOffsets` does. Routes to the group's coordinator
        /// with the same retry policy as
        /// [`AdminClient::alter_consumer_group_offsets`].
        ///
        /// # Errors
        /// Returns an error when encoding, transport, or response handling fails.
        /// Returns [`AdminError::Broker`] when the coordinator's top-level answer
        /// carries a group error code that Kafka does not retry, such as
        /// `GROUP_ID_NOT_FOUND` or `NON_EMPTY_GROUP`.
        delete_consumer_group_offsets(partitions: [(String, i32)]) => delete_consumer
    }

    group_offsets_method! {
        /// Deletes committed offsets of an inactive streams group. Apache Kafka's
        /// `KafkaAdminClient.deleteStreamsGroupOffsets` delegates to
        /// `deleteConsumerGroupOffsets` verbatim, so this does too.
        ///
        /// # Errors
        /// See [`AdminClient::delete_consumer_group_offsets`].
        delete_streams_group_offsets(partitions: [(String, i32)]) => delete_streams
    }

    async fn delete_consumer_group_offsets_with_retry(
        &mut self,
        group: &str,
        partitions: &[(String, i32)],
        retry: RetryPolicy,
    ) -> Result<Vec<ConsumerGroupOffsetOutcome>, AdminError> {
        retry_group_call!(retry, |find| self
            .offset_delete_attempt(group, partitions, find))
    }

    async fn offset_delete_attempt(
        &mut self,
        group: &str,
        partitions: &[(String, i32)],
        find_coordinator: bool,
    ) -> RetryAction<Vec<ConsumerGroupOffsetOutcome>> {
        let request = offset_delete_request(group, partitions);
        group_offset_attempt!(self, group, find_coordinator, request, |group, response| {
            offset_delete_retry_action(group, response)
        })
    }

    /// Removes members from a consumer group without waiting for their
    /// session timeout, with `LeaveGroup`, as Apache Kafka's
    /// `removeMembersFromConsumerGroup` does. Routes to the group's
    /// coordinator with the same retry policy as
    /// [`AdminClient::alter_consumer_group_offsets`].
    ///
    /// Each member is named by its `group.instance.id`, as Kafka's
    /// `MemberToRemove` names it, and is sent with the member id
    /// `UNKNOWN_MEMBER_ID` (empty). An empty `members` removes every member of
    /// the group, as Kafka's `removeAll` mode does: the call first describes
    /// the group with [`AdminClient::describe_consumer_groups`] and names each
    /// member by its instance id, or by its member id when it has none.
    ///
    /// Each member carries `options.reason`, cut to 255 characters, or
    /// Kafka's default `"member was removed by an admin"` when the reason is
    /// `None` or empty. `LeaveGroup`'s member list needs v3, so the client
    /// negotiates v3 or higher, as Kafka's `LeaveGroupRequest.Builder` does.
    ///
    /// The result maps each member of the `LeaveGroup` answer, by member id
    /// and instance id, to its error, as Kafka's
    /// `RemoveMembersFromConsumerGroupHandler.handleResponse` does.
    ///
    /// # Errors
    /// Returns an error when encoding, transport, or response handling fails.
    /// Returns [`AdminError::Broker`] when the coordinator's top-level answer
    /// carries a group error code that Kafka does not retry, or when the
    /// `removeAll` describe fails for the group. Returns
    /// [`ClientError::IncompatibleVersion`] when the coordinator does not
    /// support `LeaveGroup` v3 or higher.
    ///
    /// [`ClientError::IncompatibleVersion`]: ClientError::IncompatibleVersion
    pub async fn remove_members_from_consumer_group(
        &mut self,
        group: &str,
        members: &[MemberToRemove],
        options: &RemoveMembersOptions,
    ) -> Result<RemovedMembers, AdminError> {
        let reason = leave_group_reason(options.reason.as_deref());
        let identities = if members.is_empty() {
            self.all_members_of(group, &reason).await?
        } else {
            members
                .iter()
                .map(|member| MemberIdentity {
                    member_id: String::new(),
                    group_instance_id: Some(member.group_instance_id.clone()),
                    reason: Some(reason.clone()),
                    ..Default::default()
                })
                .collect()
        };
        self.remove_members_from_consumer_group_with_retry(group, &identities, self.retry)
            .await
    }

    /// The members of `group` for Kafka's `removeAll` mode, as
    /// `KafkaAdminClient.getMembersFromGroup` names them.
    async fn all_members_of(
        &self,
        group: &str,
        reason: &str,
    ) -> Result<Vec<MemberIdentity>, AdminError> {
        let description = self
            .describe_consumer_groups(&[group], DescribeGroupsOptions::builder().build())
            .await
            .remove(group)
            .unwrap_or_else(|| {
                Err(KafkaError {
                    code: UNKNOWN_SERVER_ERROR,
                    name: kafka_error_name(UNKNOWN_SERVER_ERROR),
                    message: Some(format!("no description for group {group}")),
                })
            })
            .map_err(|error| AdminError::Broker {
                api: "describeConsumerGroups",
                code: error.code,
                name: error.name,
                message: Some(format!(
                    "Encounter exception when trying to get members from group: {group}{}",
                    error
                        .message
                        .map(|message| format!(": {message}"))
                        .unwrap_or_default()
                )),
            })?;
        Ok(description
            .members
            .into_iter()
            .map(|member| {
                let (member_id, group_instance_id) = match member.group_instance_id {
                    Some(instance_id) => (String::new(), Some(instance_id)),
                    None => (member.member_id, None),
                };
                MemberIdentity {
                    member_id,
                    group_instance_id,
                    reason: Some(reason.to_owned()),
                    ..Default::default()
                }
            })
            .collect())
    }

    async fn remove_members_from_consumer_group_with_retry(
        &mut self,
        group: &str,
        members: &[MemberIdentity],
        retry: RetryPolicy,
    ) -> Result<RemovedMembers, AdminError> {
        retry_group_call!(retry, |find| self.leave_group_attempt(group, members, find))
    }

    async fn leave_group_attempt(
        &mut self,
        group: &str,
        members: &[MemberIdentity],
        find_coordinator: bool,
    ) -> RetryAction<RemovedMembers> {
        if find_coordinator && let Err(action) = self.find_group_coordinator_attempt(group).await {
            return action;
        }
        let request = MembersOnlyLeaveGroup::new(LeaveGroupRequest {
            group_id: group.into(),
            members: members.to_vec(),
            ..Default::default()
        });
        match self.conn.send(request).await {
            Ok(response) => leave_group_retry_action(response),
            Err(error) => connection_failure_action(error),
        }
    }

    group_offsets_method! {
        /// Commits explicit start offsets for a share group's partitions with
        /// `AlterShareGroupOffsets`, as Apache Kafka's `alterShareGroupOffsets`
        /// does. Routes to the group's coordinator with the same retry policy as
        /// [`AdminClient::alter_consumer_group_offsets`].
        ///
        /// A top-level `COORDINATOR_LOAD_IN_PROGRESS` (14) or
        /// `REBALANCE_IN_PROGRESS` (27) sends the request again to the same
        /// coordinator, and `COORDINATOR_NOT_AVAILABLE` (15) or `NOT_COORDINATOR`
        /// (16) finds the coordinator again. Every other top-level code, such as
        /// `NON_EMPTY_GROUP` (68), is the error of each requested partition, as
        /// Kafka's `AlterShareGroupOffsetsHandler.handleError` puts it in the
        /// partition results.
        ///
        /// # Errors
        /// Returns an error when the coordinator lookup, the connection, or the
        /// request fails, or at the deadline.
        alter_share_group_offsets(offsets: GroupOffsets) => alter_share
    }

    /// Commits explicit start offsets for a streams group's partitions.
    /// Apache Kafka's `KafkaAdminClient.alterStreamsGroupOffsets` delegates to
    /// `alterConsumerGroupOffsets` verbatim, so this does too.
    ///
    /// # Errors
    /// See [`AdminClient::alter_consumer_group_offsets`].
    pub async fn alter_streams_group_offsets(
        &mut self,
        group: &str,
        offsets: &GroupOffsets,
    ) -> Result<Vec<ConsumerGroupOffsetOutcome>, AdminError> {
        self.alter_consumer_group_offsets(group, offsets).await
    }

    /// Reads committed start offsets for a streams group's partitions.
    /// Apache Kafka's `KafkaAdminClient.listStreamsGroupOffsets` delegates to
    /// `listConsumerGroupOffsets` verbatim, so this does too.
    ///
    /// # Errors
    /// See [`AdminClient::list_consumer_group_offsets`].
    pub async fn list_streams_group_offsets(
        &mut self,
        group: &str,
    ) -> Result<GroupOffsets, AdminError> {
        self.list_consumer_group_offsets(group).await
    }

    /// Deletes a share group's committed offsets for the named topics with
    /// `DeleteShareGroupOffsets`, as Apache Kafka's
    /// `deleteShareGroupOffsets` does. Routes to the group's coordinator with
    /// the same retry policy as [`AdminClient::alter_consumer_group_offsets`]:
    /// a top-level `COORDINATOR_LOAD_IN_PROGRESS` (14) or
    /// `REBALANCE_IN_PROGRESS` (27) retries on the same coordinator, and
    /// `COORDINATOR_NOT_AVAILABLE` (15) or `NOT_COORDINATOR` (16) finds the
    /// coordinator again.
    ///
    /// # Errors
    /// Returns an error when encoding, transport, or response handling fails.
    /// Returns [`AdminError::Broker`] when the coordinator's top-level answer
    /// carries a group error code that Kafka does not retry.
    pub async fn delete_share_group_offsets(
        &mut self,
        group: &str,
        topics: &[String],
    ) -> Result<Vec<(String, Option<KafkaError>)>, AdminError> {
        self.delete_share_group_offsets_with_retry(group, topics, self.retry)
            .await
    }

    async fn delete_share_group_offsets_with_retry(
        &mut self,
        group: &str,
        topics: &[String],
        retry: RetryPolicy,
    ) -> Result<Vec<(String, Option<KafkaError>)>, AdminError> {
        retry_group_call!(retry, |find| {
            self.delete_share_group_offsets_attempt(group, topics, find)
        })
    }

    async fn delete_share_group_offsets_attempt(
        &mut self,
        group: &str,
        topics: &[String],
        find_coordinator: bool,
    ) -> RetryAction<Vec<(String, Option<KafkaError>)>> {
        if find_coordinator && let Err(action) = self.find_group_coordinator_attempt(group).await {
            return action;
        }
        let request = DeleteShareGroupOffsetsRequest {
            group_id: group.into(),
            topics: topics
                .iter()
                .map(|topic_name| delete_share_offset_topic(topic_name))
                .collect(),
            ..Default::default()
        };
        match self.conn.send(request).await {
            Ok(response) => delete_share_group_offsets_retry_action(response),
            Err(error) => connection_failure_action(error),
        }
    }
}

fn delete_share_offset_topic(topic_name: &str) -> DeleteShareGroupOffsetsRequestTopic {
    DeleteShareGroupOffsetsRequestTopic {
        topic_name: topic_name.into(),
        ..Default::default()
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
    let outcomes = group_offset_outcomes(response.topics);
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
        completed_group_offsets(outcomes)
    }
}

/// Builds an `OffsetDelete` request for the named partitions of `group`.
macro_rules! offset_request_topics {
    ($topics:expr, $topic_type:ident) => {
        $topics
            .into_iter()
            .map(|(name, partitions)| $topic_type {
                name,
                partitions,
                ..Default::default()
            })
            .collect()
    };
}

fn offset_delete_request(group: &str, partitions: &[(String, i32)]) -> OffsetDeleteRequest {
    let mut topics = BTreeMap::<String, Vec<OffsetDeleteRequestPartition>>::new();
    for (topic, partition) in partitions {
        topics
            .entry(topic.clone())
            .or_default()
            .push(OffsetDeleteRequestPartition {
                partition_index: *partition,
                ..Default::default()
            });
    }
    OffsetDeleteRequest {
        group_id: group.into(),
        topics: offset_request_topics!(topics, OffsetDeleteRequestTopic),
        ..Default::default()
    }
}

/// Maps one `OffsetDelete` answer to a retry action, as Kafka's
/// `DeleteConsumerGroupOffsetsHandler.handleResponse` does: the top-level
/// `error_code` names a group-level failure, and `14` retries on the same
/// coordinator while `15` and `16` unmap the group. A final answer becomes one
/// [`ConsumerGroupOffsetOutcome`] per requested partition.
fn offset_delete_retry_action(
    group: &str,
    response: OffsetDeleteResponse,
) -> RetryAction<Vec<ConsumerGroupOffsetOutcome>> {
    if response.error_code != 0 {
        let result = Err(AdminError::Broker {
            api: "OffsetDelete",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: Some(format!("group={group}")),
        });
        return group_coordinator_error_action(response.error_code, result);
    }
    let outcomes = group_offset_outcomes(response.topics);
    completed_group_offsets(outcomes)
}

/// A `LeaveGroup` request that always carries its member list, needing v3.
///
/// Apache Kafka's `RemoveMembersFromConsumerGroupHandler` builds `LeaveGroup`
/// with `LeaveGroupRequest.Builder`, which the client negotiates at v3 or
/// above so that the `members` array is not silently dropped by a downgrade.
/// Maps one `LeaveGroup` answer to a retry action, as Kafka's
/// `RemoveMembersFromConsumerGroupHandler.handleResponse` does. The top-level
/// `error_code` is a group-level failure: `14` retries on the same
/// coordinator, `15` and `16` unmap the group. A final answer maps each
/// answered member, by member id and instance id, to its error.
fn leave_group_retry_action(response: LeaveGroupResponse) -> RetryAction<RemovedMembers> {
    if response.error_code != 0 {
        let result = Err(AdminError::Broker {
            api: "LeaveGroup",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: None,
        });
        return group_coordinator_error_action(response.error_code, result);
    }
    let members = response
        .members
        .into_iter()
        .map(|member| {
            (
                RemovedMember {
                    member_id: member.member_id,
                    group_instance_id: member.group_instance_id,
                },
                kafka_error_if(member.error_code, None),
            )
        })
        .collect();
    RetryAction::Done(Ok(RemovedMembers { members }))
}

fn group_coordinator_error_action<T>(
    error_code: i16,
    result: Result<T, AdminError>,
) -> RetryAction<T> {
    match error_code {
        COORDINATOR_LOAD_IN_PROGRESS => RetryAction::SameCoordinator(result),
        COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR => RetryAction::FindCoordinator(result),
        _ => RetryAction::Done(result),
    }
}

/// `UNKNOWN_SERVER_ERROR`: the code that Kafka's `ApiError.fromThrowable`
/// gives an exception with no Kafka error code.
const UNKNOWN_SERVER_ERROR: i16 = -1;
/// Kafka's `KafkaAdminClient.DEFAULT_LEAVE_GROUP_REASON`.
const DEFAULT_LEAVE_GROUP_REASON: &str = "member was removed by an admin";
/// The longest reason that Kafka's `JoinGroupRequest.maybeTruncateReason`
/// keeps.
const MAX_REASON_CHARS: usize = 255;

/// The reason of each removed member: `reason` cut to 255 characters, or
/// Kafka's default when it is `None` or empty.
fn leave_group_reason(reason: Option<&str>) -> String {
    match reason {
        Some(reason) if !reason.is_empty() => reason.chars().take(MAX_REASON_CHARS).collect(),
        _ => DEFAULT_LEAVE_GROUP_REASON.to_owned(),
    }
}

/// One member to remove, as Apache Kafka's `MemberToRemove` names it: by its
/// `group.instance.id`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemberToRemove {
    pub group_instance_id: String,
}

/// The options of [`AdminClient::remove_members_from_consumer_group`], as
/// Apache Kafka's `RemoveMembersFromConsumerGroupOptions` holds them.
#[derive(Debug, Clone, Default, PartialEq, Eq, bon::Builder)]
pub struct RemoveMembersOptions {
    /// The reason that the coordinator logs for each member. `None` or empty
    /// sends Kafka's default reason.
    #[builder(into)]
    pub reason: Option<String>,
}

/// One member of a `LeaveGroup` answer, as the `MemberIdentity` keys of
/// Kafka's `RemoveMembersFromConsumerGroupHandler` result name it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RemovedMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

/// The result of [`AdminClient::remove_members_from_consumer_group`], as
/// Apache Kafka's `RemoveMembersFromConsumerGroupResult` holds it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemovedMembers {
    /// Each member of the `LeaveGroup` answer and its error.
    pub members: BTreeMap<RemovedMember, Option<KafkaError>>,
}

impl RemovedMembers {
    /// The result of one requested member, as Kafka's
    /// `RemoveMembersFromConsumerGroupResult.memberResult` gives it.
    ///
    /// # Errors
    /// Returns the member's error, or `UNKNOWN_SERVER_ERROR` when the answer
    /// does not name the member, as Kafka's `getSubLevelError` gives an
    /// `IllegalArgumentException`.
    pub fn member_result(&self, member: &MemberToRemove) -> Result<(), KafkaError> {
        let key = RemovedMember {
            member_id: String::new(),
            group_instance_id: Some(member.group_instance_id.clone()),
        };
        match self.members.get(&key) {
            Some(None) => Ok(()),
            Some(Some(error)) => Err(error.clone()),
            None => Err(KafkaError {
                code: UNKNOWN_SERVER_ERROR,
                name: kafka_error_name(UNKNOWN_SERVER_ERROR),
                message: Some(format!(
                    "Member \"{}\" was not included in the removal response",
                    member.group_instance_id
                )),
            }),
        }
    }
}

/// Builds an `AlterShareGroupOffsets` request for `group`'s partitions.
fn alter_share_group_offsets_request(
    group: &str,
    offsets: &GroupOffsets,
) -> AlterShareGroupOffsetsRequest {
    let mut topics = BTreeMap::<String, Vec<AlterShareGroupOffsetsRequestPartition>>::new();
    for ((topic, partition), start_offset) in offsets {
        topics
            .entry(topic.clone())
            .or_default()
            .push(AlterShareGroupOffsetsRequestPartition {
                partition_index: *partition,
                start_offset: *start_offset,
                ..Default::default()
            });
    }
    AlterShareGroupOffsetsRequest {
        group_id: group.into(),
        topics: topics
            .into_iter()
            .map(
                |(topic_name, partitions)| AlterShareGroupOffsetsRequestTopic {
                    topic_name,
                    partitions,
                    ..Default::default()
                },
            )
            .collect(),
        ..Default::default()
    }
}

fn share_group_coordinator_error_action<T>(
    code: i16,
    error: AdminError,
    otherwise: impl FnOnce(AdminError) -> Result<T, AdminError>,
) -> RetryAction<T> {
    match code {
        COORDINATOR_LOAD_IN_PROGRESS | REBALANCE_IN_PROGRESS => {
            RetryAction::SameCoordinator(Err(error))
        }
        COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR => RetryAction::FindCoordinator(Err(error)),
        _ => RetryAction::Done(otherwise(error)),
    }
}

fn completed_group_offsets<T>(outcomes: T) -> RetryAction<T> {
    RetryAction::Done(Ok(outcomes))
}

fn map_group_topic_rows<T, Rows, P, O>(
    topics: impl IntoIterator<Item = T>,
    mut rows: impl FnMut(T) -> (String, Rows),
    mut outcome: impl FnMut(String, P) -> O,
) -> Vec<O>
where
    Rows: IntoIterator<Item = P>,
{
    let mut outcomes = Vec::new();
    for topic in topics {
        let (name, partitions) = rows(topic);
        for partition in partitions {
            outcomes.push(outcome(name.clone(), partition));
        }
    }
    outcomes
}

/// Maps one `AlterShareGroupOffsets` answer to a retry action, as Kafka's
/// `AlterShareGroupOffsetsHandler.handleResponse` does. A top-level
/// `COORDINATOR_LOAD_IN_PROGRESS` or `REBALANCE_IN_PROGRESS` retries on the
/// same coordinator, and `COORDINATOR_NOT_AVAILABLE` or `NOT_COORDINATOR`
/// finds the coordinator again. Every other top-level code becomes the error
/// of each requested partition. Otherwise the answer gives one
/// [`ConsumerGroupOffsetOutcome`] for each answered partition.
fn alter_share_group_offsets_retry_action(
    offsets: &GroupOffsets,
    response: AlterShareGroupOffsetsResponse,
) -> RetryAction<Vec<ConsumerGroupOffsetOutcome>> {
    if response.error_code != 0 {
        let error = AdminError::Broker {
            api: "AlterShareGroupOffsets",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: response.error_message.clone(),
        };
        return share_group_coordinator_error_action(response.error_code, error, |_| {
            Ok(offsets
                .keys()
                .map(|(topic, partition)| ConsumerGroupOffsetOutcome {
                    topic: topic.clone(),
                    partition: *partition,
                    error: kafka_error_if(response.error_code, response.error_message.clone()),
                })
                .collect())
        });
    }
    let outcomes = map_group_topic_rows(
        response.responses,
        |topic| (topic.topic_name, topic.partitions),
        |topic, partition| ConsumerGroupOffsetOutcome {
            topic,
            partition: partition.partition_index,
            error: kafka_error_if(partition.error_code, partition.error_message),
        },
    );
    completed_group_offsets(outcomes)
}

/// Maps one `DeleteShareGroupOffsets` answer to a retry action, as Kafka's
/// `DeleteShareGroupOffsetsHandler.handleResponse` does. A top-level
/// `COORDINATOR_LOAD_IN_PROGRESS` or `REBALANCE_IN_PROGRESS` retries on the
/// same coordinator, `COORDINATOR_NOT_AVAILABLE` or `NOT_COORDINATOR` finds
/// the coordinator again, and every other top-level code fails the call. A
/// final answer gives one `(topic, error)` pair for each answered topic.
fn delete_share_group_offsets_retry_action(
    response: DeleteShareGroupOffsetsResponse,
) -> RetryAction<Vec<(String, Option<KafkaError>)>> {
    if response.error_code != 0 {
        let error = AdminError::Broker {
            api: "DeleteShareGroupOffsets",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: response.error_message,
        };
        return share_group_coordinator_error_action(response.error_code, error, Err);
    }
    let outcomes = map_group_topic_rows(
        response.responses,
        |topic| {
            (
                topic.topic_name,
                std::iter::once((topic.error_code, topic.error_message)),
            )
        },
        |topic, (code, message)| (topic, kafka_error_if(code, message)),
    );
    completed_group_offsets(outcomes)
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
        // Kafka's `Call.fail` stops after `retries` retries with a
        // `TimeoutException`.
        if attempts > retry.max_retries {
            break;
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
pub(crate) fn list_groups_kafka_error(error: &AdminError) -> KafkaError {
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

/// Builds the admin `OffsetFetch` request for all topics of `group`.
///
/// The request fills the v2 to v7 fields (`group_id`, `topics`) and the v8+
/// `groups` array, so it is valid at each negotiated version. Kafka's
/// `OffsetFetchRequest.Builder.maybeDowngrade` moves the group into the v2 to
/// v7 fields in the same way.
fn offset_fetch_group(group: &str) -> OffsetFetchRequestGroup {
    OffsetFetchRequestGroup {
        group_id: group.into(),
        member_id: None,
        member_epoch: -1,
        topics: None,
        ..Default::default()
    }
}

fn offset_fetch_request(group: &str) -> AllTopicsOffsetFetch {
    AllTopicsOffsetFetch::new(OffsetFetchRequest {
        group_id: group.into(),
        topics: None,
        groups: vec![offset_fetch_group(group)],
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
) -> Result<GroupOffsets, AdminError> {
    let (error_code, rows): (i16, Vec<(String, i32, i64, i16)>) = if response.groups.is_empty() {
        (
            response.error_code,
            response
                .topics
                .into_iter()
                .flat_map(offset_fetch_topic_rows)
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
                rows.extend(offset_fetch_topic_rows(topic));
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

trait IntoOffsetFetchRow {
    fn into_offset_fetch_row(self, topic: &str) -> (String, i32, i64, i16);
}

macro_rules! impl_offset_fetch_row {
    ($partition:ty) => {
        impl IntoOffsetFetchRow for $partition {
            fn into_offset_fetch_row(self, topic: &str) -> (String, i32, i64, i16) {
                (
                    topic.to_owned(),
                    self.partition_index,
                    self.committed_offset,
                    self.error_code,
                )
            }
        }
    };
}

impl_offset_fetch_row!(OffsetFetchResponsePartition);
impl_offset_fetch_row!(OffsetFetchResponsePartitions);

trait IntoOffsetFetchTopics {
    type Partition: IntoOffsetFetchRow;

    fn into_offset_fetch_parts(self) -> (String, Vec<Self::Partition>);
}

macro_rules! impl_offset_fetch_topics {
    ($topics:ty, $partition:ty) => {
        impl IntoOffsetFetchTopics for $topics {
            type Partition = $partition;

            fn into_offset_fetch_parts(self) -> (String, Vec<Self::Partition>) {
                (self.name, self.partitions)
            }
        }
    };
}

impl_offset_fetch_topics!(OffsetFetchResponseTopic, OffsetFetchResponsePartition);
impl_offset_fetch_topics!(OffsetFetchResponseTopics, OffsetFetchResponsePartitions);

fn offset_fetch_topic_rows<T: IntoOffsetFetchTopics>(topic: T) -> Vec<(String, i32, i64, i16)> {
    let (name, partitions) = topic.into_offset_fetch_parts();
    partitions
        .into_iter()
        .map(|partition| partition.into_offset_fetch_row(&name))
        .collect()
}

fn offset_commit_request(group: &str, offsets: &GroupOffsets) -> TopicNameOffsetCommit {
    let mut topics = BTreeMap::<String, Vec<OffsetCommitRequestPartition>>::new();
    for ((topic, partition), offset) in offsets {
        topics
            .entry(topic.clone())
            .or_default()
            .push(OffsetCommitRequestPartition {
                partition_index: *partition,
                committed_offset: *offset,
                committed_leader_epoch: -1,
                // Kafka's `OffsetAndMetadata` metadata defaults to "", which
                // is also the schema default this row takes.
                ..Default::default()
            });
    }
    TopicNameOffsetCommit::new(OffsetCommitRequest {
        group_id: group.into(),
        generation_id_or_member_epoch: -1,
        member_id: String::new(),
        topics: offset_request_topics!(topics, OffsetCommitRequestTopic),
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
        mock_test_support::offset_commit_response,
        security::{ClientSecurity, SaslCredentials},
    };
    use krabka_protocol::{
        Decode, Encode, ProtocolRequest,
        owned::{
            alter_share_group_offsets_response::{
                AlterShareGroupOffsetsResponsePartition, AlterShareGroupOffsetsResponseTopic,
            },
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            delete_share_group_offsets_response::DeleteShareGroupOffsetsResponseTopic,
            find_coordinator_request,
            find_coordinator_response::FindCoordinatorResponse,
            leave_group_response::MemberResponse,
            list_groups_response::ListGroupsResponse,
            metadata_request,
            metadata_response::MetadataResponse,
            offset_commit_request,
            offset_commit_response::{
                OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
            },
            offset_delete_response::{OffsetDeleteResponsePartition, OffsetDeleteResponseTopic},
            offset_fetch_request,
            offset_fetch_response::{
                OffsetFetchResponseGroup, OffsetFetchResponsePartition,
                OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
            },
            sasl_handshake_request,
        },
    };

    use super::*;

    fn test_group_offsets() -> GroupOffsets {
        maplit::btreemap! { ("orders".into(), 2) => 41 }
    }

    #[test]
    fn offset_reset_builds_admin_commit() {
        let offsets = test_group_offsets();
        let request = offset_commit_request("worker", &offsets);
        let expected = TopicNameOffsetCommit::new(commit_request_for_test("worker", -1, -1));
        assert!(request == expected);
    }

    fn commit_request_for_test(
        group_id: &str,
        generation_id_or_member_epoch: i32,
        committed_leader_epoch: i32,
    ) -> OffsetCommitRequest {
        OffsetCommitRequest {
            group_id: group_id.into(),
            generation_id_or_member_epoch,
            topics: vec![OffsetCommitRequestTopic {
                name: "orders".into(),
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 2,
                    committed_offset: 41,
                    committed_leader_epoch,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn encode(response: &impl Encode, version: i16, flexible: bool) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        if flexible {
            bytes.extend_from_slice(&[0]);
        }
        response.encode(&mut bytes, version).unwrap();
        bytes.to_vec()
    }

    fn encode_flexible(response: &impl Encode, version: i16) -> Vec<u8> {
        encode(response, version, true)
    }

    fn broker_port(addr: std::net::SocketAddr) -> i32 {
        i32::from(addr.port())
    }

    struct MockBrokerAddress {
        node_id: i32,
        host: String,
        port: i32,
    }

    fn mock_broker_address(addr: std::net::SocketAddr, node_id: i32) -> MockBrokerAddress {
        MockBrokerAddress {
            node_id,
            host: addr.ip().to_string(),
            port: broker_port(addr),
        }
    }

    macro_rules! broker_fields {
        ($type:ident, $broker:expr) => {
            $type {
                node_id: $broker.node_id,
                host: $broker.host,
                port: $broker.port,
                ..Default::default()
            }
        };
    }

    fn mock_metadata_broker(addr: std::net::SocketAddr, node_id: i32) -> MetadataResponseBroker {
        let broker = mock_broker_address(addr, node_id);
        broker_fields!(MetadataResponseBroker, broker)
    }

    async fn connect_admin(addr: std::net::SocketAddr, message: &'static str) -> AdminClient {
        AdminClient::connect(&[addr.to_string()])
            .await
            .expect(message)
    }

    fn mock_coordinator_response(
        addr: std::net::SocketAddr,
        error_code: i16,
    ) -> FindCoordinatorResponse {
        let broker = mock_broker_address(addr, 2);
        let mut response = broker_fields!(FindCoordinatorResponse, broker);
        response.error_code = error_code;
        response
    }

    fn incompatible_version(error: AdminError, case: &str) -> (i16, i16, i16, i16, i16) {
        match error {
            AdminError::Transport(ClientError::IncompatibleVersion {
                api_key,
                broker_min,
                broker_max,
                client_min,
                client_max,
            }) => (api_key, broker_min, broker_max, client_min, client_max),
            other => panic!("case {case}: unexpected error {other:?}"),
        }
    }

    async fn with_incompatible_version<T>(
        case: &'static str,
        result: impl std::future::Future<Output = Result<T, AdminError>>,
    ) -> Result<T, (i16, i16, i16, i16, i16)> {
        result
            .await
            .map_err(|error| incompatible_version(error, case))
    }

    /// The `OffsetCommit` and `OffsetFetch` version ranges that a mock broker
    /// advertises.
    #[derive(Clone, Copy)]
    struct GroupRanges {
        offset_commit: (i16, i16),
        offset_fetch: (i16, i16),
    }

    impl GroupRanges {
        fn offset_commit(offset_commit: (i16, i16)) -> Self {
            Self {
                offset_commit,
                offset_fetch: (2, 10),
            }
        }

        fn offset_fetch(offset_fetch: (i16, i16)) -> Self {
            Self {
                offset_commit: (2, 9),
                offset_fetch,
            }
        }
    }

    fn encode_api_versions(api_keys: Vec<ApiVersion>) -> Vec<u8> {
        encode(
            &ApiVersionsResponse {
                api_keys,
                ..Default::default()
            },
            0,
            false,
        )
    }

    macro_rules! api_version_entry {
        ($key:expr, $min:expr, $max:expr) => {
            ApiVersion {
                api_key: $key,
                min_version: $min,
                max_version: $max,
                ..Default::default()
            }
        };
    }

    /// An `ApiVersions` response that advertises `ranges`.
    fn api_versions(ranges: GroupRanges) -> Vec<u8> {
        encode_api_versions(vec![
            api_version_entry!(api_versions_request::API_KEY, 0, 0),
            api_version_entry!(find_coordinator_request::API_KEY, 0, 0),
            api_version_entry!(
                offset_commit_request::API_KEY,
                ranges.offset_commit.0,
                ranges.offset_commit.1
            ),
            api_version_entry!(
                offset_fetch_request::API_KEY,
                ranges.offset_fetch.0,
                ranges.offset_fetch.1
            ),
            api_version_entry!(metadata_request::API_KEY, 13, 13),
        ])
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
                &mock_coordinator_response(coordinator_addr, 0),
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
            metadata_request::API_KEY => Some(empty_metadata_reply(version)),
            _ => None,
        })
        .await
    }

    struct GroupAdminConnection {
        bootstrap: MockBroker,
        admin: AdminClient,
    }

    async fn connect_group_admin(
        coordinator: &MockBroker,
        ranges: GroupRanges,
    ) -> GroupAdminConnection {
        let bootstrap = bootstrap_for(coordinator, ranges, Arc::default()).await;
        let admin = connect_admin(bootstrap.addr, "admin connects").await;
        GroupAdminConnection { bootstrap, admin }
    }

    fn stop_group_brokers(bootstrap: MockBroker, coordinator: MockBroker) {
        bootstrap.stop();
        coordinator.stop();
    }

    fn record_group_request<Request>(
        requests: &Arc<Mutex<Vec<(i16, Request)>>>,
        version: i16,
        request: Request,
    ) {
        requests
            .lock()
            .expect("requests lock")
            .push((version, request));
    }

    struct GroupVersionAssertion<Request, Output> {
        name: &'static str,
        requests: Vec<(i16, Request)>,
        result: Output,
        expected_requests: Vec<(i16, Request)>,
        expected_result: Output,
    }

    fn assert_group_version_case<Request, Output>(case: GroupVersionAssertion<Request, Output>)
    where
        Request: std::fmt::Debug + PartialEq,
        Output: std::fmt::Debug + PartialEq,
    {
        assert!(
            (case.requests, case.result) == (case.expected_requests, case.expected_result),
            "case {}",
            case.name
        );
    }

    struct ExpectedGroupVersionCase<Request, Output> {
        name: &'static str,
        ranges: GroupRanges,
        requests: Vec<(i16, Request)>,
        result: Output,
    }

    fn expected_group_version_case<Request, Output>(
        name: &'static str,
        ranges: GroupRanges,
        requests: Vec<(i16, Request)>,
        result: Output,
    ) -> ExpectedGroupVersionCase<Request, Output> {
        ExpectedGroupVersionCase {
            name,
            ranges,
            requests,
            result,
        }
    }

    fn incompatible_group_version<T>(
        api_key: i16,
        broker_min: i16,
        broker_max: i16,
    ) -> Result<T, (i16, i16, i16, i16, i16)> {
        Err((api_key, broker_min, broker_max, 2, 9))
    }

    async fn run_group_version_case<Request, Output, Respond, Action, Fut>(
        case: ExpectedGroupVersionCase<Request, Output>,
        api_key: i16,
        respond: Respond,
        action: Action,
    ) where
        Request: ProtocolRequest
            + for<'de> Decode<'de>
            + Clone
            + PartialEq
            + std::fmt::Debug
            + Send
            + 'static,
        Output: PartialEq + std::fmt::Debug,
        Respond: Fn(&Request, i16) -> Vec<u8> + Copy + Send + Sync + 'static,
        Action: FnOnce(&'static str, AdminClient) -> Fut,
        Fut: std::future::Future<Output = Output>,
    {
        let ranges = case.ranges;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let metadata_requests = Arc::new(AtomicUsize::new(0));
        let metadata_count = Arc::clone(&metadata_requests);
        let coordinator =
            MockBroker::start(move |request_api, version, _, body| match request_api {
                api_versions_request::API_KEY => Some(api_versions(ranges)),
                request_api if request_api == api_key => {
                    let request = decode_request(body, version);
                    let response = respond(&request, version);
                    record_group_request(&recorded, version, request);
                    Some(response)
                }
                metadata_request::API_KEY => {
                    counted_reply(&metadata_count, Some(empty_metadata_reply(version)))
                }
                _ => None,
            })
            .await;
        let GroupAdminConnection { bootstrap, admin } =
            connect_group_admin(&coordinator, ranges).await;
        let result = action(case.name, admin).await;
        stop_group_brokers(bootstrap, coordinator);
        let requests = requests.lock().expect("requests lock").clone();
        assert!(
            metadata_requests.load(Ordering::SeqCst) == 0,
            "case {}",
            case.name
        );
        assert_group_version_case(GroupVersionAssertion {
            name: case.name,
            requests,
            result,
            expected_requests: case.requests,
            expected_result: case.result,
        });
    }

    /// The negotiated `OffsetCommit` version and the result of
    /// `alter_consumer_group_offsets`, with a version error as its ranges.
    type CommitResult = Result<Vec<ConsumerGroupOffsetOutcome>, (i16, i16, i16, i16, i16)>;

    /// Apache Kafka's `AlterConsumerGroupOffsetsHandler` builds `OffsetCommit`
    /// with `OffsetCommitRequest.Builder.forTopicNames`, which caps the version
    /// at 9. The request names each topic at every negotiated version.
    fn commit_group_version_cases()
    -> Vec<ExpectedGroupVersionCase<OffsetCommitRequest, CommitResult>> {
        let committed = Ok(successful_group_offset_outcome());
        let commit_case = |name, version_range, requests, result| {
            expected_group_version_case(
                name,
                GroupRanges::offset_commit(version_range),
                requests,
                result,
            )
        };
        let sent_request = |version| vec![(version, commit_request_for_test("workers", -1, -1))];
        vec![
            commit_case(
                "coordinator stops at v7",
                (2, 7),
                sent_request(7),
                committed.clone(),
            ),
            commit_case(
                "coordinator stops at v9",
                (2, 9),
                sent_request(9),
                committed.clone(),
            ),
            commit_case(
                "coordinator supports v10",
                (2, 10),
                sent_request(9),
                committed,
            ),
            commit_case(
                "coordinator supports only v10",
                (10, 10),
                Vec::new(),
                incompatible_group_version(offset_commit_request::API_KEY, 10, 10),
            ),
        ]
    }

    #[yare::parameterized(
        coordinator_stops_at_v7 = { 0 },
        coordinator_stops_at_v9 = { 1 },
        coordinator_supports_v10 = { 2 },
        coordinator_only_supports_v10 = { 3 },
    )]
    fn alter_consumer_group_offsets_sends_offset_commit_by_topic_name(index: usize) {
        let case = commit_group_version_cases().remove(index);
        block_on_test(run_group_version_case(
            case,
            offset_commit_request::API_KEY,
            |request, version| {
                encode(
                    &offset_commit_response(request, |_| 0),
                    version,
                    version >= offset_commit_request::FLEXIBLE_MIN,
                )
            },
            commit_group_offsets_by_topic,
        ));
    }

    async fn commit_group_offsets_by_topic(
        name: &'static str,
        mut admin: AdminClient,
    ) -> CommitResult {
        let offsets = test_group_offsets();
        Box::pin(with_incompatible_version(
            name,
            admin.alter_consumer_group_offsets("workers", &offsets),
        ))
        .await
    }

    macro_rules! offset_fetch_topics {
        ($topic:ident, $partition:ident, $rows:expr) => {
            ($rows)
                .iter()
                .map(
                    |(name, partition_index, committed_offset, error_code)| $topic {
                        name: (*name).into(),
                        partitions: vec![$partition {
                            partition_index: *partition_index,
                            committed_offset: *committed_offset,
                            error_code: *error_code,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )
                .collect()
        };
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
                topics: offset_fetch_topics!(
                    OffsetFetchResponseTopic,
                    OffsetFetchResponsePartition,
                    &rows
                ),
                ..Default::default()
            }
        } else {
            OffsetFetchResponse {
                groups: vec![OffsetFetchResponseGroup {
                    group_id: "workers".into(),
                    topics: offset_fetch_topics!(
                        OffsetFetchResponseTopics,
                        OffsetFetchResponsePartitions,
                        &rows
                    ),
                    ..Default::default()
                }],
                ..Default::default()
            }
        }
    }

    /// The negotiated `OffsetFetch` version and the result of
    /// `list_consumer_group_offsets`, with a version error as its ranges.
    type FetchResult = Result<GroupOffsets, (i16, i16, i16, i16, i16)>;

    /// Apache Kafka's `ListConsumerGroupOffsetsHandler` builds `OffsetFetch`
    /// with `OffsetFetchRequest.Builder.forTopicNames`, which caps the version
    /// at 9. A request for all topics also needs v2. The response names each
    /// topic, so the call sends no `Metadata` request.
    fn fetch_group_version_cases() -> Vec<ExpectedGroupVersionCase<OffsetFetchRequest, FetchResult>>
    {
        let fetched = Ok(test_group_offsets());
        let case = |name, ranges, requests, result| {
            expected_group_version_case(name, GroupRanges::offset_fetch(ranges), requests, result)
        };
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
                    groups: vec![offset_fetch_group("workers")],
                    ..Default::default()
                },
            )]
        };
        vec![
            case(
                "coordinator stops at v7",
                (2, 7),
                legacy_request(7),
                fetched.clone(),
            ),
            case(
                "coordinator stops at v8",
                (2, 8),
                grouped_request(8),
                fetched.clone(),
            ),
            case(
                "coordinator stops at v9",
                (2, 9),
                grouped_request(9),
                fetched.clone(),
            ),
            case(
                "coordinator supports v10",
                (2, 10),
                grouped_request(9),
                fetched,
            ),
            case(
                "coordinator supports only v10",
                (10, 10),
                Vec::new(),
                incompatible_group_version(offset_fetch_request::API_KEY, 10, 10),
            ),
            case(
                "coordinator supports only v1",
                (1, 1),
                Vec::new(),
                incompatible_group_version(offset_fetch_request::API_KEY, 1, 1),
            ),
        ]
    }

    async fn fetch_group_offsets_by_topic(
        name: &'static str,
        mut admin: AdminClient,
    ) -> FetchResult {
        let offsets = admin.list_consumer_group_offsets("workers");
        Box::pin(with_incompatible_version(name, offsets)).await
    }

    #[yare::parameterized(
        coordinator_stops_at_v7 = { 0 },
        coordinator_stops_at_v8 = { 1 },
        coordinator_stops_at_v9 = { 2 },
        coordinator_supports_v10 = { 3 },
        coordinator_only_supports_v10 = { 4 },
        coordinator_only_supports_v1 = { 5 },
    )]
    fn list_consumer_group_offsets_sends_offset_fetch_by_topic_name(index: usize) {
        let case = fetch_group_version_cases().remove(index);
        block_on_test(run_group_version_case(
            case,
            offset_fetch_request::API_KEY,
            |_, version| {
                encode(
                    &offset_fetch_response(version),
                    version,
                    version >= offset_fetch_request::FLEXIBLE_MIN,
                )
            },
            fetch_group_offsets_by_topic,
        ));
    }

    /// The result of `committed_offsets`, with a broker error as its fields.
    type OffsetsResult = Result<GroupOffsets, (&'static str, i16, &'static str, Option<String>)>;

    /// Kafka's `OffsetFetchResponse.group` reads the group error code from the
    /// top level below v8 and from the group entry from v8.
    #[yare::parameterized(
        v2_to_v7_shape = { offset_fetch_response(7), Ok(test_group_offsets()) },
        v8_and_v9_shape = { offset_fetch_response(9), Ok(test_group_offsets()) },
        v2_to_v7_group_error = { OffsetFetchResponse { error_code: 15, ..Default::default() }, Err(("OffsetFetch", 15, "COORDINATOR_NOT_AVAILABLE", Some("group=workers".into()))) },
        v8_and_v9_group_error = { OffsetFetchResponse { groups: vec![OffsetFetchResponseGroup { group_id: "workers".into(), error_code: 16, ..Default::default() }], ..Default::default() }, Err(("OffsetFetch", 16, "NOT_COORDINATOR", Some("group=workers".into()))) },
    )]
    fn committed_offsets_reads_both_response_shapes(
        response: OffsetFetchResponse,
        expected: OffsetsResult,
    ) {
        let result: OffsetsResult =
            committed_offsets("workers", response).map_err(|error| match error {
                AdminError::Broker {
                    api,
                    code,
                    name,
                    message,
                } => (api, code, name, message),
                other => panic!("unexpected error {other:?}"),
            });
        assert2::assert!(result == expected);
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum RetryExpected {
        Success,
        OffsetFetch(i16),
        CommitOutcome(i16),
        FindCoordinator(i16),
        Timeout,
    }

    /// The error codes that the mock brokers answer, one per attempt. The
    /// last code repeats. A code of 0 answers with the coordinator or with the
    /// offsets of `offset_fetch_response`.
    #[derive(Clone, Default)]
    struct RetryScript {
        find_coordinator: Vec<i16>,
        offset_fetch: Vec<i16>,
        offset_commit: Vec<i16>,
        find_coordinator_requests: usize,
        offset_fetch_requests: usize,
        offset_commit_requests: usize,
        /// The first `unreachable_answers` `FindCoordinator` answers name
        /// `unreachable`, an address that refuses connections.
        unreachable_answers: usize,
        unreachable: Option<std::net::SocketAddr>,
        /// The coordinator does not answer `OffsetCommit`.
        silent_offset_commit: bool,
        /// The first `closed_offset_commits` `OffsetCommit` requests close the
        /// connection with no answer.
        closed_offset_commits: usize,
    }

    /// An address on which no listener accepts connections.
    async fn refused_address() -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a port");
        listener.local_addr().expect("local address")
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
        let closing = Arc::clone(&script);
        let handler = move |api_key: i16, version: i16, body: &[u8]| match api_key {
            api_versions_request::API_KEY => Some(api_versions(ranges)),
            offset_commit_request::API_KEY => {
                let request: OffsetCommitRequest = decode_request(body, version);
                let mut script = script.lock().expect("script lock");
                let script = &mut *script;
                let error_code =
                    RetryScript::next(&script.offset_commit, &mut script.offset_commit_requests);
                if script.silent_offset_commit {
                    return None;
                }
                let response = offset_commit_response(&request, |_| error_code);
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
                Some(encode(
                    &mock_coordinator_response(addr, error_code),
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
        };
        MockBroker::start_with_replies(move |api_key, version, _, body| {
            if api_key == offset_commit_request::API_KEY {
                let mut script = closing.lock().expect("script lock");
                if script.closed_offset_commits > 0 {
                    script.closed_offset_commits -= 1;
                    script.offset_commit_requests += 1;
                    return MockReply::Close;
                }
            }
            handler(api_key, version, body).map_or(MockReply::Silent, MockReply::Respond)
        })
        .await
    }

    struct ScriptedGroupCluster {
        coordinator: MockBroker,
        bootstrap: MockBroker,
        admin: AdminClient,
    }

    impl ScriptedGroupCluster {
        async fn start(script: Arc<Mutex<RetryScript>>) -> Self {
            let coordinator_addr = Arc::new(Mutex::new(None));
            let coordinator =
                scripted_group_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
            *coordinator_addr.lock().expect("coordinator lock") = Some(coordinator.addr);
            let bootstrap =
                scripted_group_broker(Arc::clone(&script), Arc::clone(&coordinator_addr)).await;
            let admin = connect_admin(bootstrap.addr, "admin connects").await;
            Self {
                coordinator,
                bootstrap,
                admin,
            }
        }
    }

    async fn run_scripted_call<T, F, Fut>(script: RetryScript, call: F) -> (T, RetryScript)
    where
        F: FnOnce(AdminClient) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let shared_script = Arc::new(Mutex::new(script));
        let ScriptedGroupCluster {
            coordinator,
            bootstrap,
            admin,
        } = ScriptedGroupCluster::start(Arc::clone(&shared_script)).await;
        let result = call(admin).await;
        bootstrap.stop();
        coordinator.stop();
        let recorded = shared_script.lock().expect("script lock").clone();
        (result, recorded)
    }

    const GROUP_RETRY_LONG: Duration = Duration::from_secs(5);
    const GROUP_RETRY_NOW: Duration = Duration::from_millis(500);

    fn group_retry_backoff(timeout: Duration) -> Duration {
        if timeout == GROUP_RETRY_LONG {
            Duration::from_millis(1)
        } else {
            timeout
        }
    }

    fn test_retry_policy(timeout: Duration, initial_backoff: Duration) -> RetryPolicy {
        RetryPolicy {
            timeout,
            initial_backoff,
            max_backoff: initial_backoff,
            jitter: 0.0,
            max_retries: u32::MAX,
        }
    }

    fn list_groups_retry_policy(
        timeout: Duration,
        backoff: Duration,
        max_retries: u32,
    ) -> RetryPolicy {
        RetryPolicy {
            timeout,
            initial_backoff: backoff,
            max_backoff: backoff,
            jitter: 0.0,
            max_retries,
        }
    }

    #[derive(Clone, Copy)]
    struct GroupRetryScenario {
        name: &'static str,
        find_coordinator: &'static [i16],
        offset_fetch: &'static [i16],
        offset_commit: &'static [i16],
        timeout: Duration,
        fetch_expected: RetryExpected,
        fetch_calls: (usize, usize),
        commit_expected: RetryExpected,
        commit_calls: (usize, usize),
    }

    #[derive(Clone, Copy)]
    struct RetryCodes {
        find_coordinator: &'static [i16],
        offset_fetch: &'static [i16],
        offset_commit: &'static [i16],
    }

    #[derive(Clone, Copy)]
    struct RetryExpectations {
        fetch: RetryExpected,
        commit: RetryExpected,
    }

    fn successful_retry_case(
        name: &'static str,
        find_coordinator: &'static [i16],
        rpc_errors: &'static [i16],
        calls: (usize, usize),
    ) -> GroupRetryScenario {
        retry_scenario(
            name,
            RetryCodes {
                find_coordinator,
                offset_fetch: rpc_errors,
                offset_commit: rpc_errors,
            },
            GROUP_RETRY_LONG,
            RetryExpectations {
                fetch: RetryExpected::Success,
                commit: RetryExpected::CommitOutcome(0),
            },
            calls,
        )
    }

    fn retry_scenario(
        name: &'static str,
        codes: RetryCodes,
        timeout: Duration,
        expected: RetryExpectations,
        calls: (usize, usize),
    ) -> GroupRetryScenario {
        GroupRetryScenario {
            name,
            find_coordinator: codes.find_coordinator,
            offset_fetch: codes.offset_fetch,
            offset_commit: codes.offset_commit,
            timeout,
            fetch_expected: expected.fetch,
            fetch_calls: calls,
            commit_expected: expected.commit,
            commit_calls: calls,
        }
    }

    fn coordinator_recovery_case(name: &'static str, codes: &'static [i16]) -> GroupRetryScenario {
        successful_retry_case(name, codes, &[0], (2, 1))
    }

    fn timed_out_retry_case(name: &'static str, codes: RetryCodes) -> GroupRetryScenario {
        retry_scenario(
            name,
            codes,
            GROUP_RETRY_NOW,
            RetryExpectations {
                fetch: RetryExpected::Timeout,
                commit: RetryExpected::Timeout,
            },
            (1, 1),
        )
    }

    fn group_retry_scenarios() -> Vec<GroupRetryScenario> {
        use RetryExpected::{FindCoordinator as FetchFindError, Timeout as FetchTimeout};

        vec![
            successful_retry_case("no error", &[0], &[0], (1, 1)),
            successful_retry_case("coordinator load in progress", &[0], &[14, 14, 0], (1, 3)),
            successful_retry_case("coordinator not available", &[0], &[15, 0], (2, 2)),
            successful_retry_case("not coordinator", &[0], &[16, 0], (2, 2)),
            timed_out_retry_case(
                "load in progress timeout",
                RetryCodes {
                    find_coordinator: &[0],
                    offset_fetch: &[14],
                    offset_commit: &[27],
                },
            ),
            timed_out_retry_case(
                "not coordinator timeout",
                RetryCodes {
                    find_coordinator: &[0],
                    offset_fetch: &[16],
                    offset_commit: &[16],
                },
            ),
            retry_scenario(
                "group error is final",
                RetryCodes {
                    find_coordinator: &[0],
                    offset_fetch: &[30],
                    offset_commit: &[25],
                },
                GROUP_RETRY_LONG,
                RetryExpectations {
                    fetch: RetryExpected::OffsetFetch(30),
                    commit: RetryExpected::CommitOutcome(25),
                },
                (1, 1),
            ),
            retry_scenario(
                "another group error is final",
                RetryCodes {
                    find_coordinator: &[0],
                    offset_fetch: &[7],
                    offset_commit: &[7],
                },
                GROUP_RETRY_LONG,
                RetryExpectations {
                    fetch: RetryExpected::OffsetFetch(7),
                    commit: RetryExpected::CommitOutcome(7),
                },
                (1, 1),
            ),
            coordinator_recovery_case("find coordinator not available then succeeds", &[15, 0]),
            coordinator_recovery_case("find coordinator load in progress then succeeds", &[14, 0]),
            retry_scenario(
                "find coordinator timeout",
                RetryCodes {
                    find_coordinator: &[15],
                    offset_fetch: &[0],
                    offset_commit: &[0],
                },
                GROUP_RETRY_NOW,
                RetryExpectations {
                    fetch: FetchTimeout,
                    commit: FetchTimeout,
                },
                (1, 0),
            ),
            retry_scenario(
                "find coordinator authorization error",
                RetryCodes {
                    find_coordinator: &[30],
                    offset_fetch: &[0],
                    offset_commit: &[0],
                },
                GROUP_RETRY_LONG,
                RetryExpectations {
                    fetch: FetchFindError(30),
                    commit: FetchFindError(30),
                },
                (1, 0),
            ),
        ]
    }

    const TIMED_OUT: i16 = 7;

    #[derive(Clone, Copy)]
    enum RetryApi {
        Fetch,
        Commit,
    }

    #[derive(Debug, PartialEq)]
    enum RetryObservation {
        Fetch(Result<GroupOffsets, RetryExpected>),
        Commit(Result<Vec<ConsumerGroupOffsetOutcome>, i16>),
    }

    async fn alter_consumer_offsets_with_retry(
        mut admin: AdminClient,
        timeout: Duration,
    ) -> Result<Vec<ConsumerGroupOffsetOutcome>, AdminError> {
        let offsets = test_group_offsets();
        admin
            .alter_group_offsets_with_retry(
                GroupOffsetUpdate {
                    kind: GroupOffsetKind::Consumer,
                    group: "workers",
                    offsets: &offsets,
                },
                test_retry_policy(timeout, group_retry_backoff(timeout)),
            )
            .await
    }

    async fn assert_retry_scenario(case: GroupRetryScenario, api: RetryApi) {
        let expected = match (api, case.fetch_expected, case.commit_expected) {
            (RetryApi::Fetch, RetryExpected::Success, _) => {
                RetryObservation::Fetch(Ok(test_group_offsets()))
            }
            (RetryApi::Fetch, RetryExpected::OffsetFetch(code), _) => {
                RetryObservation::Fetch(Err(RetryExpected::OffsetFetch(code)))
            }
            (RetryApi::Fetch, RetryExpected::FindCoordinator(code), _) => {
                RetryObservation::Fetch(Err(RetryExpected::FindCoordinator(code)))
            }
            (RetryApi::Fetch, RetryExpected::Timeout, _) => {
                RetryObservation::Fetch(Err(RetryExpected::Timeout))
            }
            (RetryApi::Commit, _, RetryExpected::CommitOutcome(code)) => {
                RetryObservation::Commit(Ok(vec![ConsumerGroupOffsetOutcome {
                    topic: "orders".into(),
                    partition: 2,
                    error: kafka_error_if(code, None),
                }]))
            }
            (RetryApi::Commit, _, RetryExpected::FindCoordinator(code)) => {
                RetryObservation::Commit(Err(code))
            }
            (RetryApi::Commit, _, RetryExpected::Timeout) => {
                RetryObservation::Commit(Err(TIMED_OUT))
            }
            _ => unreachable!("retry expectation belongs to another API"),
        };
        let expected_calls = match api {
            RetryApi::Fetch => case.fetch_calls,
            RetryApi::Commit => case.commit_calls,
        };
        let mut script = RetryScript {
            find_coordinator: case.find_coordinator.to_vec(),
            ..RetryScript::default()
        };
        match api {
            RetryApi::Fetch => script.offset_fetch = case.offset_fetch.to_vec(),
            RetryApi::Commit => script.offset_commit = case.offset_commit.to_vec(),
        }

        let ((result, within_deadline), script) =
            run_scripted_call(script, |mut admin| async move {
                let started = tokio::time::Instant::now();
                let result = match api {
                    RetryApi::Fetch => {
                        let result = admin
                            .list_consumer_group_offsets_with_retry(
                                "workers",
                                test_retry_policy(case.timeout, group_retry_backoff(case.timeout)),
                            )
                            .await
                            .map_err(|error| match error {
                                AdminError::Broker {
                                    api: "OffsetFetch",
                                    code,
                                    ..
                                } => RetryExpected::OffsetFetch(code),
                                AdminError::Transport(ClientError::Server { error_code }) => {
                                    RetryExpected::FindCoordinator(error_code)
                                }
                                AdminError::Transport(ClientError::Timeout(_)) => {
                                    RetryExpected::Timeout
                                }
                                other => panic!("case {}: unexpected error {other:?}", case.name),
                            });
                        RetryObservation::Fetch(result)
                    }
                    RetryApi::Commit => {
                        let result = alter_consumer_offsets_with_retry(admin, case.timeout)
                            .await
                            .map_err(|error| match error {
                                AdminError::Transport(ClientError::Server { error_code }) => {
                                    error_code
                                }
                                AdminError::Transport(ClientError::Timeout(_)) => TIMED_OUT,
                                other => panic!("case {}: unexpected error {other:?}", case.name),
                            });
                        RetryObservation::Commit(result)
                    }
                };
                let within_deadline = match api {
                    RetryApi::Fetch => true,
                    RetryApi::Commit => started.elapsed() < case.timeout + Duration::from_secs(2),
                };
                (result, within_deadline)
            })
            .await;
        let rpc_calls = match api {
            RetryApi::Fetch => script.offset_fetch_requests,
            RetryApi::Commit => script.offset_commit_requests,
        };
        assert!(
            (
                result,
                script.find_coordinator_requests,
                rpc_calls,
                within_deadline
            ) == (expected, expected_calls.0, expected_calls.1, true),
            "case {}",
            case.name
        );
    }

    /// These named cases exercise the shared retry behavior of `OffsetFetch` and
    /// `OffsetCommit`, while preserving each API's result and call-count checks.
    #[yare::parameterized(
        no_error = { 0 },
        coordinator_load_in_progress = { 1 },
        coordinator_not_available = { 2 },
        not_coordinator = { 3 },
        load_timeout = { 4 },
        coordinator_timeout = { 5 },
        group_error = { 6 },
        other_group_error = { 7 },
        find_coordinator_not_available = { 8 },
        find_coordinator_load = { 9 },
        find_coordinator_timeout = { 10 },
        find_coordinator_authorization = { 11 },
    )]
    fn group_offset_retry_scenarios_match_kafka(index: usize) {
        block_on_test(async move {
            let case = group_retry_scenarios()[index];
            assert_retry_scenario(case, RetryApi::Fetch).await;
            assert_retry_scenario(case, RetryApi::Commit).await;
        });
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Coordinator {
        UnreachableOnce,
        Silent,
        ClosedOnce,
    }

    /// The coordinator retry driver recovers from a connection close, resolves
    /// an unreachable coordinator, and times out when the coordinator is silent.
    #[yare::parameterized(
        closed_connection = { "closed connection", GROUP_RETRY_LONG, Coordinator::ClosedOnce, 1, 2, Ok(0) },
        unreachable_coordinator = { "unreachable coordinator", GROUP_RETRY_LONG, Coordinator::UnreachableOnce, 2, 1, Ok(0) },
        silent_coordinator = { "silent coordinator", Duration::from_millis(300), Coordinator::Silent, 1, 1, Err(TIMED_OUT) },
    )]
    #[test_macro(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn alter_consumer_group_offsets_recovers_from_connection_failures(
        name: &'static str,
        timeout: Duration,
        coordinator: Coordinator,
        find_calls: usize,
        commit_calls: usize,
        expected: Result<i16, i16>,
    ) {
        let script = RetryScript {
            find_coordinator: vec![0],
            offset_commit: vec![0],
            unreachable_answers: usize::from(coordinator == Coordinator::UnreachableOnce),
            unreachable: Some(refused_address().await),
            silent_offset_commit: coordinator == Coordinator::Silent,
            closed_offset_commits: usize::from(coordinator == Coordinator::ClosedOnce),
            ..RetryScript::default()
        };
        let ((result, within_deadline), script) = run_scripted_call(script, |admin| async move {
            let started = tokio::time::Instant::now();
            let result = alter_consumer_offsets_with_retry(admin, timeout)
                .await
                .map(|_| 0_i16)
                .map_err(|error| match error {
                    AdminError::Transport(ClientError::Server { error_code }) => error_code,
                    AdminError::Transport(ClientError::Timeout(_)) => TIMED_OUT,
                    other => panic!("case {name}: unexpected error {other:?}"),
                });
            (result, started.elapsed() < timeout + Duration::from_secs(2))
        })
        .await;
        assert2::assert!(
            (
                result,
                script.find_coordinator_requests,
                script.offset_commit_requests,
                within_deadline
            ) == (expected, find_calls, commit_calls, true),
            "case {name}"
        );
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
                Some(crate::encode_test_struct_field!(
                    encode,
                    OffsetCommitResponse,
                    topics,
                    vec![successful_offset_commit_topic()],
                    version,
                    true,
                ))
            }
            offset_fetch_request::API_KEY => {
                counted_reply(&seen, Some(offset_fetch_reply(version)))
            }
            _ => None,
        })
        .await;

        let bootstrap_group_rpcs = Arc::new(AtomicUsize::new(0));
        let bootstrap =
            bootstrap_for(&coordinator, ranges, Arc::clone(&bootstrap_group_rpcs)).await;
        let mut admin = connect_admin(bootstrap.addr, "admin connects").await;

        let committed = admin
            .alter_consumer_group_offsets("workers", &test_group_offsets())
            .await
            .expect("offset commit succeeds");
        let mut admin = connect_admin(bootstrap.addr, "second admin connects").await;
        let fetched = admin
            .list_consumer_group_offsets("workers")
            .await
            .expect("offset fetch succeeds");

        assert!(bootstrap_group_rpcs.load(Ordering::SeqCst) == 0);
        assert!(coordinator_group_rpcs.load(Ordering::SeqCst) == 2);
        assert!(committed == successful_group_offset_outcome());
        assert!(fetched == test_group_offsets());
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

    fn stable_listings(group_ids: &[&str]) -> Vec<GroupListing> {
        group_ids
            .iter()
            .map(|group_id| stable_listing(group_id))
            .collect()
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
            api_versions_request::API_KEY => Some(encode_api_versions(vec![
                api_version_entry!(api_versions_request::API_KEY, 0, 0),
                api_version_entry!(metadata_request::API_KEY, 12, 12),
                api_version_entry!(list_groups_request::API_KEY, 0, script.max_version),
            ])),
            metadata_request::API_KEY => Some(crate::encode_test_struct_field!(
                encode,
                MetadataResponse,
                brokers,
                cluster
                    .lock()
                    .expect("cluster lock")
                    .iter()
                    .zip(1..)
                    .map(|(addr, node_id)| mock_metadata_broker(*addr, node_id))
                    .collect(),
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

    #[derive(Default)]
    struct ListGroupsCaseBuilder {
        name: &'static str,
        brokers: Option<[ListGroupsBroker; 3]>,
        options: ListGroupsOptions,
        timeout: Option<Duration>,
        valid: Vec<GroupListing>,
        errors: Vec<ExpectedError>,
        requests: Option<[Vec<(i16, ListGroupsRequest)>; 3]>,
    }

    impl ListGroupsCase {
        fn builder() -> ListGroupsCaseBuilder {
            ListGroupsCaseBuilder::default()
        }
    }

    impl ListGroupsCaseBuilder {
        fn name(mut self, name: &'static str) -> Self {
            self.name = name;
            self
        }

        fn brokers(mut self, brokers: [ListGroupsBroker; 3]) -> Self {
            self.brokers = Some(brokers);
            self
        }

        fn options(mut self, options: ListGroupsOptions) -> Self {
            self.options = options;
            self
        }

        fn timeout(mut self, timeout: Duration) -> Self {
            self.timeout = Some(timeout);
            self
        }

        fn valid(mut self, valid: Vec<GroupListing>) -> Self {
            self.valid = valid;
            self
        }

        fn errors(mut self, errors: Vec<ExpectedError>) -> Self {
            self.errors = errors;
            self
        }

        fn requests(mut self, requests: [Vec<(i16, ListGroupsRequest)>; 3]) -> Self {
            self.requests = Some(requests);
            self
        }

        fn build(self) -> ListGroupsCase {
            ListGroupsCase {
                name: self.name,
                brokers: self.brokers.expect("brokers must be set"),
                options: self.options,
                timeout: self.timeout.unwrap_or(LONG),
                valid: self.valid,
                errors: self.errors,
                requests: self
                    .requests
                    .unwrap_or_else(|| [vec![plain(5)], vec![plain(5)], vec![plain(5)]]),
            }
        }
    }

    fn plain(version: i16) -> (i16, ListGroupsRequest) {
        (version, ListGroupsRequest::default())
    }

    fn stable(version: i16) -> (i16, ListGroupsRequest) {
        (
            version,
            ListGroupsRequest {
                states_filter: stable_state_filter(),
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
        ListGroupsOptions::builder()
            .group_states(states.iter().copied().collect())
            .build()
    }

    fn stable_state_filter() -> Vec<String> {
        vec![GroupState::Stable.to_string()]
    }

    fn single_broker_error() -> Vec<ExpectedError> {
        vec![(2, 31, None)]
    }

    fn expected_kafka_error(code: i16, message: Option<String>) -> KafkaError {
        kafka_error_if(code, message).expect("error code is nonzero")
    }

    fn types(types: &[GroupType]) -> ListGroupsOptions {
        ListGroupsOptions::builder()
            .types(types.iter().copied().collect())
            .build()
    }

    fn broker_error(code: i16) -> ListGroupsBroker {
        ListGroupsBroker {
            max_version: 5,
            answers: vec![ListGroupsAnswer::Error(code)],
        }
    }

    /// Kafka's `Call.fail` stops a node call after `retries` retries with a
    /// `TimeoutException`, before the deadline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_groups_stops_a_broker_after_the_retry_limit() {
        let cluster = Arc::new(Mutex::new(Vec::new()));
        let sent = Arc::new(Mutex::new(Vec::new()));
        let broker =
            list_groups_broker(broker_error(14), Arc::clone(&cluster), Arc::clone(&sent)).await;
        *cluster.lock().expect("cluster lock") = vec![broker.addr];
        let admin = connect_admin(broker.addr, "admin connects").await;

        let options = ListGroupsOptions::builder().build();
        let result = run_list_groups_with_retry(
            &admin,
            &options,
            list_groups_retry_policy(Duration::from_secs(10), Duration::from_millis(1), 2),
        )
        .await;

        broker.stop();
        let codes = result
            .errors
            .iter()
            .map(|error| error.error.code)
            .collect::<Vec<_>>();
        assert!((codes, sent.lock().expect("sent lock").len()) == (vec![7], 3));
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
        let admin = connect_admin(brokers[0].addr, "admin connects").await;

        let result = run_list_groups_with_retry(
            &admin,
            &case.options,
            list_groups_retry_policy(case.timeout, BACKOFF, u32::MAX),
        )
        .await;

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
                        port: broker_port(addr),
                        error: expected_kafka_error(code, message),
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

    async fn run_list_groups_with_retry(
        admin: &AdminClient,
        options: &ListGroupsOptions,
        policy: RetryPolicy,
    ) -> ListGroupsResult {
        admin
            .list_groups_with_retry(options, policy)
            .await
            .expect("list_groups succeeds")
    }

    fn block_on_test(future: impl std::future::Future<Output = ()> + Send + 'static) {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(future);
    }

    fn run_list_groups_case_sync(case: ListGroupsCase) {
        block_on_test(run_list_groups_case(case));
    }

    /// Apache Kafka's `KafkaAdminClient.listGroups` sends `Metadata`, then
    /// `ListGroups` to every broker, and merges the answers: one listing per
    /// group id, and one error per failed broker. It retries 14 and 15 on the
    /// broker until the deadline, and then `Call.fail` gives a
    /// `TimeoutException` (7) with the last error as its cause.
    #[yare::parameterized(
        one_group_each_broker = { 0 },
        empty_broker = { 1 },
        duplicate_group = { 2 },
        broker_timeout = { 3 },
        broker_retries = { 4 },
        authorization_error = { 5 },
    )]
    fn list_groups_asks_every_broker_and_merges(index: usize) {
        run_list_groups_case_sync(list_groups_merge_cases().remove(index));
    }

    /// Kafka's `listGroups` sends the state and type filters to every broker,
    /// and filters protocol types on the client. `ListGroupsRequest.Builder.build`
    /// rejects a state filter below v4 and a type filter below v5, except a
    /// type filter of `Classic` and `Consumer` at most, with `Classic`, which
    /// it omits. The rejection is an error for that broker only.
    #[yare::parameterized(
        state_filter = { 0 },
        old_state_filter = { 1 },
        consumer_type_filter = { 2 },
        old_consumer_type_pair = { 3 },
        protocol_filter = { 4 },
    )]
    fn list_groups_sends_filters_as_kafka_does(index: usize) {
        run_list_groups_case_sync(list_groups_filter_cases().remove(index));
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
    #[yare::parameterized(
        stalls = { SecuredBroker::Stalls },
        rejects_authentication = { SecuredBroker::RejectsAuthentication },
    )]
    #[test_macro(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn list_groups_isolates_a_secured_broker_that_stalls_or_rejects(broker_2: SecuredBroker) {
        const RETRY: RetryPolicy = RetryPolicy {
            timeout: Duration::from_secs(2),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(100),
            jitter: 0.0,
            max_retries: u32::MAX,
        };
        let cluster = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let broker_1 =
            list_groups_broker(groups(&["a"]), Arc::clone(&cluster), Arc::clone(&requests)).await;
        let broker_2_started = broker_2.start().await;
        let broker_3 =
            list_groups_broker(groups(&["c"]), Arc::clone(&cluster), Arc::clone(&requests)).await;
        let broker_2_addr = broker_2_started.addr;
        *cluster.lock().expect("cluster lock") = vec![broker_1.addr, broker_2_addr, broker_3.addr];
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
            admin.list_groups_with_retry(&ListGroupsOptions::builder().build(), RETRY),
        )
        .await
        .map(|result| result.expect("list_groups succeeds"));
        let observed = (outcome, broker_2_started.connections.load(Ordering::SeqCst));

        let (code, message) = broker_2.expected_error(broker_2_addr);
        let expected = (
            Ok(ListGroupsResult {
                valid: stable_listings(&["a", "c"]),
                errors: vec![ListGroupsError {
                    node_id: 2,
                    host: broker_2_addr.ip().to_string(),
                    port: broker_port(broker_2_addr),
                    error: expected_kafka_error(code, message),
                }],
            }),
            1,
        );
        broker_1.stop();
        broker_2_started.stop();
        broker_3.stop();
        assert2::assert!(observed == expected, "case {broker_2:?}");
    }

    fn list_groups_merge_cases() -> Vec<ListGroupsCase> {
        vec![
            ListGroupsCase::builder()
                .name("one group on each broker")
                .brokers([groups(&["a"]), groups(&["b"]), groups(&["c"])])
                .valid(stable_listings(&["a", "b", "c"]))
                .build(),
            ListGroupsCase::builder()
                .name("broker 2 has no groups")
                .brokers([groups(&["a"]), groups(&[]), groups(&["c"])])
                .valid(stable_listings(&["a", "c"]))
                .build(),
            ListGroupsCase::builder()
                .name("a group on two brokers is listed once")
                .brokers([groups(&["a"]), groups(&["a", "b"]), groups(&["c"])])
                .valid(stable_listings(&["a", "b", "c"]))
                .build(),
            ListGroupsCase::builder()
                .name("broker 2 answers coordinator not available until the timeout")
                .brokers([groups(&["a"]), broker_error(15), groups(&["c"])])
                .timeout(SHORT)
                .valid(stable_listings(&["a", "c"]))
                .errors(vec![(
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
                )])
                .build(),
            ListGroupsCase::builder()
                .name("broker 2 answers coordinator load in progress, then its groups")
                .brokers([
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
                ])
                .valid(vec![
                    stable_listing("a"),
                    stable_listing("b"),
                    stable_listing("c"),
                ])
                .requests([
                    vec![plain(5)],
                    vec![plain(5), plain(5), plain(5)],
                    vec![plain(5)],
                ])
                .build(),
            ListGroupsCase::builder()
                .name("broker 2 answers cluster authorization failed")
                .brokers([groups(&["a"]), broker_error(31), groups(&["c"])])
                .valid(stable_listings(&["a", "c"]))
                .errors(single_broker_error())
                .build(),
        ]
    }

    fn list_groups_filter_cases() -> Vec<ListGroupsCase> {
        vec![
            ListGroupsCase::builder()
                .name("state filter Stable is sent to every broker")
                .brokers([
                    ListGroupsBroker {
                        max_version: 5,
                        answers: vec![ListGroupsAnswer::Groups(vec![
                            listed("a", "consumer", "Empty", "Classic"),
                            consumer_group("b"),
                        ])],
                    },
                    groups(&["c"]),
                    groups(&[]),
                ])
                .options(states(&[GroupState::Stable]))
                .valid(stable_listings(&["b", "c"]))
                .requests([vec![stable(5)], vec![stable(5)], vec![stable(5)]])
                .build(),
            ListGroupsCase::builder()
                .name("state filter on a broker below v4 is unsupported on that broker")
                .brokers([groups(&["a"]), below(3, &["b"]), groups(&["c"])])
                .options(states(&[GroupState::Stable]))
                .valid(stable_listings(&["a", "c"]))
                .errors(vec![(2, 35, Some(unsupported(3, 4)))])
                .requests([vec![stable(5)], Vec::new(), vec![stable(5)]])
                .build(),
            ListGroupsCase::builder()
                .name("type filter Consumer on a broker below v5 is unsupported on that broker")
                .brokers([groups(&["a"]), below(4, &["b"]), groups(&["c"])])
                .options(types(&[GroupType::Consumer]))
                .errors(vec![(2, 35, Some(unsupported(4, 5)))])
                .requests([vec![consumer_type(5)], Vec::new(), vec![consumer_type(5)]])
                .build(),
            ListGroupsCase::builder()
                .name("type filter Classic and Consumer on a broker below v5 is omitted")
                .brokers([groups(&["a"]), below(4, &["b"]), groups(&["c"])])
                .options(types(&[GroupType::Classic, GroupType::Consumer]))
                .valid(vec![
                    stable_listing("a"),
                    GroupListing {
                        group_type: None,
                        ..stable_listing("b")
                    },
                    stable_listing("c"),
                ])
                .requests([
                    vec![classic_and_consumer(5)],
                    vec![classic_and_consumer(4)],
                    vec![classic_and_consumer(5)],
                ])
                .build(),
            ListGroupsCase::builder()
                .name("protocol type filter is applied by the client")
                .brokers([
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
                ])
                .options(ListGroupsOptions::for_consumer_groups())
                .valid(vec![
                    stable_listing("a"),
                    GroupListing {
                        group_id: "simple".into(),
                        group_type: Some(GroupType::Classic),
                        protocol_type: String::new(),
                        group_state: Some(GroupState::Empty),
                    },
                ])
                .requests([
                    vec![classic_and_consumer(5)],
                    vec![classic_and_consumer(5)],
                    vec![classic_and_consumer(5)],
                ])
                .build(),
        ]
    }

    /// A listing with no type is not a simple consumer group.
    #[yare::parameterized(
        classic_without_protocol = {
            Some(GroupType::Classic),
            "",
            true,
        },
        classic_with_protocol = {
            Some(GroupType::Classic),
            "consumer",
            false,
        },
        consumer = { Some(GroupType::Consumer), "", false },
        unknown = { Some(GroupType::Unknown), "", false },
        no_type = { None, "", false },
        no_type_with_protocol = { None, "consumer", false },
    )]
    fn is_simple_consumer_group_matches_kafka(
        group_type: Option<GroupType>,
        protocol_type: &str,
        expected: bool,
    ) {
        let listing = GroupListing {
            group_id: "g".into(),
            group_type,
            protocol_type: protocol_type.into(),
            group_state: None,
        };
        assert2::assert!(listing.is_simple_consumer_group() == expected);
    }

    /// Kafka's `GroupListing` has no type or state when the broker sends an
    /// empty name, and `GroupState.parse` and `GroupType.parse` ignore case
    /// and map an unknown name to `Unknown`.
    #[yare::parameterized(
        consumer = {
            listed("g", "consumer", "Stable", "Consumer"),
            GroupListing {
                group_id: "g".into(),
                group_type: Some(GroupType::Consumer),
                protocol_type: "consumer".into(),
                group_state: Some(GroupState::Stable),
            },
        },
        empty = {
            listed("g", "", "", ""),
            GroupListing {
                group_id: "g".into(),
                group_type: None,
                protocol_type: String::new(),
                group_state: None,
            },
        },
        streams = {
            listed("g", "consumer", "preparingrebalance", "STREAMS"),
            GroupListing {
                group_id: "g".into(),
                group_type: Some(GroupType::Streams),
                protocol_type: "consumer".into(),
                group_state: Some(GroupState::PreparingRebalance),
            },
        },
        unknown = {
            listed("g", "consumer", "Rebalancing", "Future"),
            GroupListing {
                group_id: "g".into(),
                group_type: Some(GroupType::Unknown),
                protocol_type: "consumer".into(),
                group_state: Some(GroupState::Unknown),
            },
        },
    )]
    fn group_listing_reads_listed_group(group: ListedGroup, expected: GroupListing) {
        assert2::assert!(GroupListing::from(group) == expected);
    }

    /// An `ApiVersions` answer that advertises every group RPC this module's
    /// new-call tests use, each at its highest defined version, so a test
    /// picks that call's `LATEST_STABLE_VERSION` and exercises the real wire
    /// shape rather than a downgrade.
    fn api_versions_all() -> Vec<u8> {
        let api = |api_key, max_version| ApiVersion {
            api_key,
            min_version: 0,
            max_version,
            ..Default::default()
        };
        encode_api_versions(vec![
            api(api_versions_request::API_KEY, 0),
            api(find_coordinator_request::API_KEY, 0),
            api(metadata_request::API_KEY, 13),
            api(offset_commit_request::API_KEY, 9),
            api(offset_fetch_request::API_KEY, 9),
            api(OffsetDeleteRequest::API_KEY, 0),
            api(LeaveGroupRequest::API_KEY, 5),
            api(AlterShareGroupOffsetsRequest::API_KEY, 1),
            api(DeleteShareGroupOffsetsRequest::API_KEY, 0),
        ])
    }

    fn empty_metadata_response(version: i16) -> Vec<u8> {
        encode(&MetadataResponse::default(), version, true)
    }

    fn offset_fetch_response_bytes(version: i16) -> Vec<u8> {
        encode(&offset_fetch_response(version), version, true)
    }

    fn empty_metadata_reply(version: i16) -> Vec<u8> {
        empty_metadata_response(version)
    }

    fn offset_fetch_reply(version: i16) -> Vec<u8> {
        offset_fetch_response_bytes(version)
    }

    fn counted_reply(counter: &AtomicUsize, response: Option<Vec<u8>>) -> Option<Vec<u8>> {
        counter.fetch_add(1, Ordering::SeqCst);
        response
    }

    /// Connects an `AdminClient` to a single mock broker that is its own
    /// group coordinator: it answers `ApiVersions` with
    /// [`api_versions_all`], `FindCoordinator` by naming itself, and
    /// `target_api_key` by calling `respond` with the next code out of
    /// `codes` (the last code repeats, as [`RetryScript::next`] gives it) and
    /// the negotiated version. `Metadata` answers empty.
    ///
    /// Returns the connected client, the broker (the caller stops it), and
    /// the number of `target_api_key` requests the broker answered.
    async fn connect_self_coordinating(
        target_api_key: i16,
        codes: Vec<i16>,
        respond: impl Fn(i16, i16) -> Vec<u8> + Send + Sync + 'static,
    ) -> (AdminClient, MockBroker, Arc<AtomicUsize>) {
        let coordinator_addr = Arc::new(Mutex::new(None::<std::net::SocketAddr>));
        let addr_in_mock = Arc::clone(&coordinator_addr);
        let script = Arc::new(Mutex::new((codes, 0_usize)));
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_mock = Arc::clone(&calls);
        let broker = MockBroker::start(move |api_key, version, _, _| {
            if api_key == target_api_key {
                calls_in_mock.fetch_add(1, Ordering::SeqCst);
                let mut guard = script.lock().expect("script lock");
                let (codes, requests) = &mut *guard;
                let code = RetryScript::next(codes, requests);
                Some(respond(code, version))
            } else {
                match api_key {
                    api_versions_request::API_KEY => Some(api_versions_all()),
                    find_coordinator_request::API_KEY => {
                        let addr = addr_in_mock
                            .lock()
                            .expect("coordinator lock")
                            .expect("coordinator address set before use");
                        Some(encode(&mock_coordinator_response(addr, 0), version, false))
                    }
                    metadata_request::API_KEY => Some(empty_metadata_reply(version)),
                    _ => None,
                }
            }
        })
        .await;
        *coordinator_addr.lock().expect("coordinator lock") = Some(broker.addr);
        let admin = connect_admin(broker.addr, "admin connects").await;
        (admin, broker, calls)
    }

    fn successful_group_offset_outcome() -> Vec<ConsumerGroupOffsetOutcome> {
        vec![ConsumerGroupOffsetOutcome {
            topic: "orders".into(),
            partition: 2,
            error: None,
        }]
    }

    macro_rules! successful_offset_topic {
        ($topic:ident, $partition:ident, $name_field:ident) => {
            $topic {
                $name_field: "orders".into(),
                partitions: vec![$partition {
                    partition_index: 2,
                    error_code: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }
        };
    }

    fn successful_offset_delete_topic() -> OffsetDeleteResponseTopic {
        successful_offset_topic!(
            OffsetDeleteResponseTopic,
            OffsetDeleteResponsePartition,
            name
        )
    }

    fn offset_delete_response(error_code: i16, version: i16) -> Vec<u8> {
        let topics =
            successful_response_rows(error_code, || vec![successful_offset_delete_topic()]);
        encode(
            &OffsetDeleteResponse {
                error_code,
                topics,
                ..Default::default()
            },
            version,
            false,
        )
    }

    fn offset_commit_success_response(version: i16) -> Vec<u8> {
        encode(
            &OffsetCommitResponse {
                topics: vec![successful_offset_commit_topic()],
                ..Default::default()
            },
            version,
            true,
        )
    }

    fn delete_share_offsets_response(error_code: i16, version: i16) -> Vec<u8> {
        let responses = successful_response_rows(error_code, || {
            vec![DeleteShareGroupOffsetsResponseTopic {
                topic_name: "orders".into(),
                error_code: 0,
                ..Default::default()
            }]
        });
        crate::encode_test_struct_fields!(
            encode,
            DeleteShareGroupOffsetsResponse,
            { error_code: error_code, responses: responses },
            version,
            true,
        )
    }

    fn successful_offset_commit_topic() -> OffsetCommitResponseTopic {
        successful_offset_topic!(
            OffsetCommitResponseTopic,
            OffsetCommitResponsePartition,
            name
        )
    }

    fn successful_share_offset_topic() -> AlterShareGroupOffsetsResponseTopic {
        successful_offset_topic!(
            AlterShareGroupOffsetsResponseTopic,
            AlterShareGroupOffsetsResponsePartition,
            topic_name
        )
    }

    fn successful_response_rows<T>(error_code: i16, rows: impl FnOnce() -> Vec<T>) -> Vec<T> {
        if error_code == 0 { rows() } else { Vec::new() }
    }

    fn assert_single_group_offset_success(
        result: Result<Vec<ConsumerGroupOffsetOutcome>, String>,
        calls: usize,
    ) {
        assert!((result, calls) == (Ok(successful_group_offset_outcome()), 1));
    }

    struct CoordinatorRetryCase<T> {
        name: &'static str,
        codes: Vec<i16>,
        expected_calls: usize,
        expected: Result<T, i16>,
    }

    #[bon::bon]
    impl<T> CoordinatorRetryCase<T> {
        #[builder]
        fn success(name: &'static str, codes: Vec<i16>, expected_calls: usize, outcome: T) -> Self {
            Self {
                name,
                codes,
                expected_calls,
                expected: Ok(outcome),
            }
        }

        fn error(name: &'static str, code: i16) -> Self {
            Self {
                name,
                codes: vec![code],
                expected_calls: 1,
                expected: Err(code),
            }
        }
    }

    impl<T: Clone> CoordinatorRetryCase<T> {
        fn standard(outcome: &T) -> Vec<Self> {
            vec![
                Self::success()
                    .name("no error")
                    .codes(vec![0])
                    .expected_calls(1)
                    .outcome(outcome.clone())
                    .call(),
                Self::success()
                    .name("coordinator load in progress retries on the same coordinator")
                    .codes(vec![14, 0])
                    .expected_calls(2)
                    .outcome(outcome.clone())
                    .call(),
                Self::success()
                    .name("not coordinator finds the coordinator again")
                    .codes(vec![16, 0])
                    .expected_calls(2)
                    .outcome(outcome.clone())
                    .call(),
            ]
        }

        fn with_rebalance_in_progress(outcome: &T) -> Vec<Self> {
            let mut cases = Self::standard(outcome);
            cases.push(
                Self::success()
                    .name("rebalance in progress retries on the same coordinator")
                    .codes(vec![27, 0])
                    .expected_calls(2)
                    .outcome(outcome.clone())
                    .call(),
            );
            cases
        }

        fn with_authorization_failure(mut cases: Vec<Self>) -> Vec<Self> {
            cases.push(Self::error("group authorization failed is final", 30));
            cases
        }
    }

    #[bon::builder]
    async fn run_coordinator_retry_cases<T, F, Fut, Resp>(
        api_key: i16,
        respond: Resp,
        cases: Vec<CoordinatorRetryCase<T>>,
        action: F,
    ) where
        T: PartialEq + std::fmt::Debug,
        Resp: Fn(i16, i16) -> Vec<u8> + Copy + Send + Sync + 'static,
        F: Fn(AdminClient) -> Fut,
        Fut: std::future::Future<Output = Result<T, AdminError>>,
    {
        for case in cases {
            let (admin, broker, calls) =
                connect_self_coordinating(api_key, case.codes, respond).await;
            let result = action(admin).await.map_err(|error| match error {
                AdminError::Broker { code, .. } => code,
                other => panic!("case {}: unexpected error {other:?}", case.name),
            });
            broker.stop();
            assert!(
                (result, calls.load(Ordering::SeqCst)) == (case.expected, case.expected_calls),
                "case {}",
                case.name
            );
        }
    }

    macro_rules! run_retry_cases {
        ($api_key:expr, $respond:expr, $cases:expr, $action:expr) => {
            run_coordinator_retry_cases()
                .api_key($api_key)
                .respond($respond)
                .cases($cases)
                .action($action)
                .call()
                .await
        };
    }

    async fn delete_consumer_offsets(
        mut admin: AdminClient,
    ) -> Result<Vec<ConsumerGroupOffsetOutcome>, AdminError> {
        admin
            .delete_consumer_group_offsets("workers", &[("orders".into(), 2)])
            .await
    }

    async fn alter_share_offsets(
        mut admin: AdminClient,
    ) -> Result<Vec<ConsumerGroupOffsetOutcome>, AdminError> {
        admin
            .alter_share_group_offsets("workers", &test_group_offsets())
            .await
    }

    async fn delete_share_offsets(
        mut admin: AdminClient,
    ) -> Result<Vec<(String, Option<KafkaError>)>, AdminError> {
        admin
            .delete_share_group_offsets("workers", &["orders".to_string()])
            .await
    }

    /// Apache Kafka's `DeleteConsumerGroupOffsetsHandler.handleResponse`
    /// reads the top-level `error_code` as a group-level failure, retried the
    /// same way as `DeleteGroups`, and otherwise returns one outcome per
    /// requested partition.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_consumer_group_offsets_retries_coordinator_errors_as_kafka_does() {
        let respond = offset_delete_response;
        let outcome = successful_group_offset_outcome();
        let mut cases = CoordinatorRetryCase::standard(&outcome);
        cases.push(CoordinatorRetryCase::error("non-empty group is final", 68));
        run_retry_cases!(
            OffsetDeleteRequest::API_KEY,
            respond,
            cases,
            delete_consumer_offsets
        );
    }

    #[derive(Clone, Copy)]
    enum StreamsGroupOffsetCall {
        Delete,
        Alter,
    }

    impl StreamsGroupOffsetCall {
        fn api_key(self) -> i16 {
            match self {
                Self::Delete => OffsetDeleteRequest::API_KEY,
                Self::Alter => offset_commit_request::API_KEY,
            }
        }

        fn response(self, version: i16) -> Vec<u8> {
            match self {
                Self::Delete => offset_delete_response(0, version),
                Self::Alter => offset_commit_success_response(version),
            }
        }

        async fn call(
            self,
            admin: &mut AdminClient,
        ) -> Result<Vec<ConsumerGroupOffsetOutcome>, AdminError> {
            match self {
                Self::Delete => {
                    admin
                        .delete_streams_group_offsets("workers", &[("orders".into(), 2)])
                        .await
                }
                Self::Alter => {
                    admin
                        .alter_streams_group_offsets("workers", &test_group_offsets())
                        .await
                }
            }
        }
    }

    /// Kafka's Streams group offset APIs delegate to their Consumer group
    /// counterparts.
    #[yare::parameterized(
        delete = { StreamsGroupOffsetCall::Delete },
        alter = { StreamsGroupOffsetCall::Alter },
    )]
    #[test_macro(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn streams_group_offset_calls_delegate_to_consumer_calls(call: StreamsGroupOffsetCall) {
        let respond = move |_code: i16, version: i16| call.response(version);
        let (mut admin, broker, calls) =
            connect_self_coordinating(call.api_key(), vec![0], respond).await;
        let result = call
            .call(&mut admin)
            .await
            .map_err(|error| error.to_string());
        broker.stop();
        assert_single_group_offset_success(result, calls.load(Ordering::SeqCst));
    }

    /// Apache Kafka's `RemoveMembersFromConsumerGroupHandler.handleResponse`
    /// reads the top-level `error_code` as a group-level failure, retried the
    /// same way as `DeleteGroups`, and otherwise maps each answered member to
    /// its error. `removeMembersFromConsumerGroup` names each member by its
    /// instance id with `UNKNOWN_MEMBER_ID` and sends the default reason.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remove_members_from_consumer_group_retries_coordinator_errors_as_kafka_does() {
        let answered = |instance_id: &str, error_code| MemberResponse {
            member_id: String::new(),
            group_instance_id: Some(instance_id.into()),
            error_code,
            ..Default::default()
        };
        let respond = move |code: i16, version: i16| {
            let members = successful_response_rows(code, || {
                // The coordinator answers in its own order.
                vec![answered("b", 25), answered("a", 0)]
            });
            let response = LeaveGroupResponse {
                error_code: code,
                members,
                ..Default::default()
            };
            encode_flexible(&response, version)
        };
        let removed = |instance_id: &str| RemovedMember {
            member_id: String::new(),
            group_instance_id: Some(instance_id.into()),
        };
        let outcome = RemovedMembers {
            members: maplit::btreemap! {
                removed("a") => None,
                removed("b") => Some(KafkaError {
                code: 25,
                name: "UNKNOWN_MEMBER_ID",
                message: None,
            }),
            },
        };
        let cases = CoordinatorRetryCase::with_authorization_failure(
            CoordinatorRetryCase::standard(&outcome),
        );
        run_retry_cases!(
            LeaveGroupRequest::API_KEY,
            respond,
            cases,
            |mut admin| async move {
                admin
                    .remove_members_from_consumer_group(
                        "workers",
                        &[
                            MemberToRemove {
                                group_instance_id: "a".into(),
                            },
                            MemberToRemove {
                                group_instance_id: "b".into(),
                            },
                        ],
                        &RemoveMembersOptions::builder().build(),
                    )
                    .await
            }
        );
    }

    /// Kafka's `removeMembersFromConsumerGroup` sends each member with
    /// `UNKNOWN_MEMBER_ID`, its instance id and the reason, which is the
    /// default for `None` or empty and is cut to 255 characters.
    #[yare::parameterized(
        none = { None, DEFAULT_LEAVE_GROUP_REASON.to_owned() },
        empty = { Some(String::new()), DEFAULT_LEAVE_GROUP_REASON.to_owned() },
        short = { Some("rolling restart".to_owned()), "rolling restart".to_owned() },
        long = { Some("r".repeat(300)), "r".repeat(255) },
    )]
    fn leave_group_reason_matches_kafka(reason: Option<String>, expected: String) {
        assert2::assert!(leave_group_reason(reason.as_deref()) == expected);
    }

    /// `RemoveMembersFromConsumerGroupResult.memberResult` looks a member up
    /// by `UNKNOWN_MEMBER_ID` and its instance id, and gives an error for a
    /// member that the answer does not name.
    #[test]
    fn removed_members_member_result_matches_kafka() {
        let member = |instance_id: &str| MemberToRemove {
            group_instance_id: instance_id.into(),
        };
        let fenced = KafkaError {
            code: 82,
            name: kafka_error_name(82),
            message: None,
        };
        let result = RemovedMembers {
            members: maplit::btreemap! {
                RemovedMember {
                member_id: String::new(),
                group_instance_id: Some("a".into()),
            } => None,
                RemovedMember {
                member_id: String::new(),
                group_instance_id: Some("b".into()),
            } => Some(fenced.clone()),
            },
        };
        assert!(
            [member("a"), member("b"), member("c")].map(|member| result.member_result(&member))
                == [
                    Ok(()),
                    Err(fenced),
                    Err(KafkaError {
                        code: -1,
                        name: "UNKNOWN_SERVER_ERROR",
                        message: Some(
                            "Member \"c\" was not included in the removal response".into()
                        ),
                    }),
                ]
        );
    }

    /// Apache Kafka's `AlterShareGroupOffsetsHandler.handleResponse` retries
    /// a top-level 14 or 27 on the same coordinator, finds the coordinator
    /// again on 15 or 16, and puts every other top-level code on each
    /// requested partition.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alter_share_group_offsets_retries_coordinator_errors_as_kafka_does() {
        let respond = |code: i16, version: i16| {
            let responses =
                successful_response_rows(code, || vec![successful_share_offset_topic()]);
            let response = AlterShareGroupOffsetsResponse {
                error_code: code,
                responses,
                ..Default::default()
            };
            encode_flexible(&response, version)
        };
        let outcome = successful_group_offset_outcome();
        let mut cases = CoordinatorRetryCase::with_rebalance_in_progress(&outcome);
        cases.push(
            CoordinatorRetryCase::success()
                .name("a final group error is the error of each partition")
                .codes(vec![68])
                .expected_calls(1)
                .outcome(vec![ConsumerGroupOffsetOutcome {
                    topic: "orders".into(),
                    partition: 2,
                    error: Some(KafkaError {
                        code: 68,
                        name: "NON_EMPTY_GROUP",
                        message: None,
                    }),
                }])
                .call(),
        );
        run_retry_cases!(
            AlterShareGroupOffsetsRequest::API_KEY,
            respond,
            cases,
            alter_share_offsets
        );
    }

    /// Apache Kafka's `KafkaAdminClient.listStreamsGroupOffsets` delegates to
    /// `listConsumerGroupOffsets` verbatim.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_streams_group_offsets_delegates_to_offset_fetch() {
        let respond =
            |_code: i16, version: i16| encode(&offset_fetch_response(version), version, true);
        let (mut admin, broker, calls) =
            connect_self_coordinating(offset_fetch_request::API_KEY, vec![0], respond).await;
        let result = admin
            .list_streams_group_offsets("workers")
            .await
            .map_err(|error| error.to_string());
        broker.stop();
        assert!((result, calls.load(Ordering::SeqCst)) == (Ok(test_group_offsets()), 1));
    }

    /// Apache Kafka's `DeleteShareGroupOffsetsHandler.handleResponse` reads
    /// the top-level `error_code` as a group-level failure, retried the same
    /// way as `DeleteGroups`, and otherwise returns one `(topic, error)` pair
    /// per requested topic.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_share_group_offsets_retries_coordinator_errors_as_kafka_does() {
        let respond = delete_share_offsets_response;
        let outcome = vec![("orders".to_string(), None)];
        let cases = CoordinatorRetryCase::with_authorization_failure(
            CoordinatorRetryCase::with_rebalance_in_progress(&outcome),
        );
        run_retry_cases!(
            DeleteShareGroupOffsetsRequest::API_KEY,
            respond,
            cases,
            delete_share_offsets
        );
    }
}
