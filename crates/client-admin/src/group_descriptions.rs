//! Batched group descriptions: [`AdminClient::describe_consumer_groups`],
//! [`AdminClient::describe_classic_groups`],
//! [`AdminClient::describe_share_groups`] and
//! [`AdminClient::describe_streams_groups`].
//!
//! Each call finds the coordinator of every group with one batched
//! `FindCoordinator` request and sends one describe request to each
//! coordinator, as Apache Kafka's `Describe*GroupsHandler` classes do with
//! their `CoordinatorStrategy`. A request to a coordinator that
//! fails or loses its connection finds the coordinator again.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

use bytes::{Buf, BufMut};
use krabka_client_core::{ClientError, CoordinatorEndpoint};
use krabka_protocol::{
    Decode, Encode, ProtocolError, ProtocolRequest,
    owned::{
        consumer_group_describe_request::{self, ConsumerGroupDescribeRequest},
        consumer_group_describe_response::{
            DescribedGroup as ConsumerDescribedGroup, Member as ConsumerMember,
        },
        consumer_protocol_assignment::{self, ConsumerProtocolAssignment},
        describe_groups_request::DescribeGroupsRequest,
        describe_groups_response::{DescribedGroup as ClassicDescribedGroup, DescribedGroupMember},
        share_group_describe_request::ShareGroupDescribeRequest,
        share_group_describe_response::DescribedGroup as ShareDescribedGroup,
        streams_group_describe_request::{self, StreamsGroupDescribeRequest},
        streams_group_describe_response::{
            DescribedGroup as StreamsDescribedGroup, StreamsGroupDescribeResponse,
        },
    },
};

use crate::{
    AdminClient, AdminError, ClusterNode, KafkaError,
    cluster::authorized_operations,
    group_coordinators::{GroupOutcome, group_error_outcome, kafka_error},
    groups::{GroupState, GroupType, list_groups_kafka_error},
    kafka_error_name,
    retry::is_connection_failure,
    users::AclOperation,
};

/// `UNKNOWN_SERVER_ERROR`: the code that Kafka's `ApiError.fromThrowable`
/// gives an exception with no Kafka error code.
const UNKNOWN_SERVER_ERROR: i16 = -1;
/// `UNSUPPORTED_VERSION`.
const UNSUPPORTED_VERSION: i16 = 35;
/// `GROUP_ID_NOT_FOUND`.
const GROUP_ID_NOT_FOUND: i16 = 69;
/// The protocol type of a classic consumer group
/// (`ConsumerProtocol.PROTOCOL_TYPE`).
const CONSUMER_PROTOCOL_TYPE: &str = "consumer";

/// A classic group's description from `DescribeGroups`, as Apache Kafka's
/// `ClassicGroupDescription` holds it. This is the broker's wire response
/// entry for the group.
pub type ClassicGroupDescription = ClassicDescribedGroup;

/// A share group's description from `ShareGroupDescribe`, as Apache Kafka's
/// `ShareGroupDescription` holds it. This is the broker's wire response entry
/// for the group.
pub type ShareGroupDescription = ShareDescribedGroup;

/// A streams group's description from `StreamsGroupDescribe`, as Apache
/// Kafka's `StreamsGroupDescription` holds it. This is the broker's wire
/// response entry for the group.
pub type StreamsGroupDescription = StreamsDescribedGroup;

/// The options of the describe calls, as Apache Kafka's
/// `Describe*GroupsOptions` hold them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DescribeGroupsOptions {
    /// Ask for the operations that the caller may perform on each group.
    /// Kafka's default is `false`.
    pub include_authorized_operations: bool,
}

/// One member of a consumer group, as Apache Kafka's `MemberDescription`
/// holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDescription {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    /// `None` for a member of a classic group.
    pub rack_id: Option<String>,
    pub client_id: String,
    pub host: String,
    /// The partitions that the member owns now.
    pub assignment: BTreeSet<(String, i32)>,
    /// The partitions that the coordinator wants the member to own. `None`
    /// for a member of a classic group.
    pub target_assignment: Option<BTreeSet<(String, i32)>>,
    /// `None` for a member of a classic group.
    pub member_epoch: Option<i32>,
    /// Whether the member uses the consumer protocol (KIP-1099). `None` when
    /// the coordinator does not say (`ConsumerGroupDescribe` v0) or for a
    /// member of a classic group.
    pub upgraded: Option<bool>,
}

/// A consumer group, classic or KIP-848, as Apache Kafka's
/// `ConsumerGroupDescription` holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDescription {
    pub group_id: String,
    /// A classic group with no protocol type.
    pub is_simple_consumer_group: bool,
    pub members: Vec<MemberDescription>,
    /// The server assignor of a consumer group, or the protocol of a classic
    /// group.
    pub partition_assignor: String,
    /// [`GroupType::Consumer`] or [`GroupType::Classic`].
    pub group_type: GroupType,
    pub group_state: GroupState,
    /// The coordinator that `FindCoordinator` named.
    pub coordinator: ClusterNode,
    /// `None` when the caller did not ask for them.
    pub authorized_operations: Option<BTreeSet<AclOperation>>,
    /// `None` for a classic group.
    pub group_epoch: Option<i32>,
    /// `None` for a classic group.
    pub target_assignment_epoch: Option<i32>,
}

/// The per-group results of a describe call: one entry for each requested
/// group.
pub type GroupDescriptions<T> = BTreeMap<String, Result<T, KafkaError>>;

/// A `StreamsGroupDescribe` request limited to v0.
///
/// Apache Kafka 4.3.1 defines `StreamsGroupDescribe` v0 only. v1
/// (`IncludeTopologyDescription`, KIP-1331) is newer than that release, so
/// this type keeps version negotiation at v0.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ReleasedStreamsGroupDescribe(StreamsGroupDescribeRequest);

impl Encode for ReleasedStreamsGroupDescribe {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for ReleasedStreamsGroupDescribe {
    const API_KEY: i16 = streams_group_describe_request::API_KEY;
    const MIN_VERSION: i16 = streams_group_describe_request::MIN_VERSION;
    /// The last `StreamsGroupDescribe` version of Kafka 4.3.1.
    const MAX_VERSION: i16 = 0;
    const LATEST_STABLE_VERSION: i16 = Self::MAX_VERSION;
    const FLEXIBLE_MIN: i16 = streams_group_describe_request::FLEXIBLE_MIN;
    type Response = StreamsGroupDescribeResponse;
}

/// The node that `FindCoordinator` named, as Kafka's `Node` holds it.
fn coordinator_node(coordinator: &CoordinatorEndpoint) -> ClusterNode {
    ClusterNode {
        id: coordinator.node_id,
        host: coordinator.host.clone(),
        port: coordinator.port,
        rack: None,
        is_fenced: false,
    }
}

/// The outcome of every group of `keys` after a request to their coordinator
/// failed with `error`: a failed or lost connection finds the coordinator
/// again, as Kafka's `AdminApiDriver.onFailure` does after a disconnect, and
/// every other failure is final.
pub(crate) fn request_error_outcomes<V>(
    keys: &[String],
    error: &AdminError,
) -> BTreeMap<String, GroupOutcome<V>> {
    keys.iter()
        .map(|key| {
            let outcome = if is_connection_failure(error) {
                GroupOutcome::Unmap(error.to_string())
            } else {
                GroupOutcome::Done(Err(list_groups_kafka_error(error)))
            };
            (key.clone(), outcome)
        })
        .collect()
}

/// The partitions of a `ConsumerGroupDescribe` assignment.
fn consumer_assignment(
    assignment: krabka_protocol::owned::common::consumer_group_describe_response::assignment::Assignment,
) -> BTreeSet<(String, i32)> {
    assignment
        .topic_partitions
        .into_iter()
        .flat_map(|topic| {
            let name = topic.topic_name;
            topic
                .partitions
                .into_iter()
                .map(move |partition| (name.clone(), partition))
        })
        .collect()
}

/// A member of a `ConsumerGroupDescribe` answer, as Kafka's
/// `DescribeConsumerGroupsHandler.handledConsumerGroupResponse` reads it.
fn consumer_member(member: ConsumerMember) -> MemberDescription {
    MemberDescription {
        member_id: member.member_id,
        group_instance_id: member.instance_id,
        rack_id: member.rack_id,
        client_id: member.client_id,
        host: member.client_host,
        assignment: consumer_assignment(member.assignment),
        target_assignment: Some(consumer_assignment(member.target_assignment)),
        member_epoch: Some(member.member_epoch),
        upgraded: (member.member_type != -1).then_some(member.member_type == 1),
    }
}

/// Reads a version-prefixed `ConsumerProtocolAssignment`, as Kafka's
/// `ConsumerProtocol.deserializeAssignment` does: a version above the
/// highest known version reads as the highest known version, and a negative
/// version is an error.
fn classic_assignment(mut bytes: &[u8]) -> Result<BTreeSet<(String, i32)>, String> {
    if bytes.is_empty() {
        return Ok(BTreeSet::new());
    }
    if bytes.len() < 2 {
        return Err("Buffer underflow while parsing consumer protocol's header".to_owned());
    }
    let version = bytes.get_i16();
    if version < consumer_protocol_assignment::MIN_VERSION {
        return Err(format!("Unsupported assignment version: {version}"));
    }
    let version = version.min(consumer_protocol_assignment::MAX_VERSION);
    let assignment = ConsumerProtocolAssignment::decode(&mut bytes, version)
        .map_err(|error| format!("Error reading consumer assignment: {error}"))?;
    Ok(assignment
        .assigned_partitions
        .into_iter()
        .flat_map(|topic| {
            let name = topic.topic;
            topic
                .partitions
                .into_iter()
                .map(move |partition| (name.clone(), partition))
        })
        .collect())
}

/// A member of a classic `DescribeGroups` answer, as Kafka's
/// `DescribeConsumerGroupsHandler.handledClassicGroupResponse` reads it.
fn classic_member(member: DescribedGroupMember) -> Result<MemberDescription, String> {
    Ok(MemberDescription {
        assignment: classic_assignment(&member.member_assignment)?,
        member_id: member.member_id,
        group_instance_id: member.group_instance_id,
        rack_id: None,
        client_id: member.client_id,
        host: member.client_host,
        target_assignment: None,
        member_epoch: None,
        upgraded: None,
    })
}

/// What the answer of `ConsumerGroupDescribe` means for one group.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ConsumerAnswer {
    Outcome(GroupOutcome<ConsumerGroupDescription>),
    /// Describe the group with `DescribeGroups`, as Kafka's
    /// `useClassicGroupApi` set records it. The message of a
    /// `GROUP_ID_NOT_FOUND` answer, if any, is kept for the classic answer.
    Classic {
        not_found_message: Option<String>,
    },
}

/// Classifies one `ConsumerGroupDescribe` entry, as Kafka's
/// `DescribeConsumerGroupsHandler.handleError` does with
/// `isConsumerGroupResponse` set: `UNSUPPORTED_VERSION` and
/// `GROUP_ID_NOT_FOUND` move the group to `DescribeGroups`.
fn consumer_answer(group: ConsumerDescribedGroup, coordinator: &ClusterNode) -> ConsumerAnswer {
    match group.error_code {
        0 => ConsumerAnswer::Outcome(GroupOutcome::Done(Ok(ConsumerGroupDescription {
            group_id: group.group_id,
            is_simple_consumer_group: false,
            members: group.members.into_iter().map(consumer_member).collect(),
            partition_assignor: group.assignor_name,
            group_type: GroupType::Consumer,
            group_state: GroupState::parse(&group.group_state),
            coordinator: coordinator.clone(),
            authorized_operations: authorized_operations(group.authorized_operations),
            group_epoch: Some(group.group_epoch),
            target_assignment_epoch: Some(group.assignment_epoch),
        }))),
        UNSUPPORTED_VERSION => ConsumerAnswer::Classic {
            not_found_message: None,
        },
        GROUP_ID_NOT_FOUND => ConsumerAnswer::Classic {
            not_found_message: group.error_message,
        },
        code => ConsumerAnswer::Outcome(group_error_outcome(code, group.error_message, &[])),
    }
}

/// Classifies one `DescribeGroups` entry of `describe_consumer_groups`, as
/// Kafka's `DescribeConsumerGroupsHandler` does for a group in
/// `useClassicGroupApi`. A group whose protocol type is neither `consumer`
/// nor empty is not a consumer group. `GROUP_ID_NOT_FOUND` carries the
/// message of the `ConsumerGroupDescribe` answer when there was one.
fn classic_consumer_outcome(
    group: ClassicDescribedGroup,
    coordinator: &ClusterNode,
    not_found_message: Option<String>,
) -> GroupOutcome<ConsumerGroupDescription> {
    match group.error_code {
        0 => {}
        GROUP_ID_NOT_FOUND => {
            return GroupOutcome::Done(Err(kafka_error(
                GROUP_ID_NOT_FOUND,
                not_found_message.or(group.error_message),
            )));
        }
        code => return group_error_outcome(code, group.error_message, &[]),
    }
    if group.protocol_type != CONSUMER_PROTOCOL_TYPE && !group.protocol_type.is_empty() {
        return GroupOutcome::Done(Err(kafka_error(
            UNKNOWN_SERVER_ERROR,
            Some(format!(
                "GroupId {} is not a consumer group ({}).",
                group.group_id, group.protocol_type
            )),
        )));
    }
    let members = match group
        .members
        .into_iter()
        .map(classic_member)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(members) => members,
        Err(message) => {
            return GroupOutcome::Done(Err(kafka_error(UNKNOWN_SERVER_ERROR, Some(message))));
        }
    };
    GroupOutcome::Done(Ok(ConsumerGroupDescription {
        is_simple_consumer_group: group.protocol_type.is_empty(),
        group_id: group.group_id,
        members,
        partition_assignor: group.protocol_data,
        group_type: GroupType::Classic,
        group_state: GroupState::parse(&group.group_state),
        coordinator: coordinator.clone(),
        authorized_operations: authorized_operations(group.authorized_operations),
        group_epoch: None,
        target_assignment_epoch: None,
    }))
}

impl AdminClient {
    /// Describes consumer groups, classic and KIP-848, as Apache Kafka's
    /// `describeConsumerGroups` does with its `DescribeConsumerGroupsHandler`.
    ///
    /// The call finds the coordinator of every group with one batched
    /// `FindCoordinator` request, and sends one `ConsumerGroupDescribe`
    /// request to each coordinator with the groups that it coordinates. A
    /// group that the answer gives `GROUP_ID_NOT_FOUND` (69) or
    /// `UNSUPPORTED_VERSION` (35), and every group of a coordinator that does
    /// not support `ConsumerGroupDescribe`, is described with `DescribeGroups`
    /// instead, as Kafka's handler falls back for a classic group. Kafka
    /// sends that `DescribeGroups` request in the next round of its driver.
    /// This call sends it in the same round, to the same coordinator.
    ///
    /// `COORDINATOR_LOAD_IN_PROGRESS` (14) sends a group to the same
    /// coordinator again, and `COORDINATOR_NOT_AVAILABLE` (15) and
    /// `NOT_COORDINATOR` (16) find its coordinator again. A lost
    /// connection finds the coordinator again. The call waits with Kafka's
    /// backoff between rounds and stops at `default.api.timeout.ms` (60 s),
    /// where each pending group gets `REQUEST_TIMED_OUT` (7).
    ///
    /// # Errors
    /// The call itself does not fail. Each group gets its own [`KafkaError`]:
    /// the error code of the answer, `GROUP_ID_NOT_FOUND` with the message of
    /// the `ConsumerGroupDescribe` answer for a group that does not exist,
    /// `UNKNOWN_SERVER_ERROR` (-1) for a classic group that is not a consumer
    /// group, or `REQUEST_TIMED_OUT` (7) at the deadline.
    pub async fn describe_consumer_groups(
        &self,
        groups: &[&str],
        options: DescribeGroupsOptions,
    ) -> GroupDescriptions<ConsumerGroupDescription> {
        let classic = Mutex::new(BTreeMap::<String, Option<String>>::new());
        self.call_group_coordinators("describeConsumerGroups", groups, |coordinator, keys| {
            self.describe_consumer_groups_on(coordinator, keys, options, &classic)
        })
        .await
    }

    /// One `describe_consumer_groups` request to one coordinator. `classic`
    /// holds the groups that use `DescribeGroups`, with the message of their
    /// `GROUP_ID_NOT_FOUND` answer.
    async fn describe_consumer_groups_on(
        &self,
        coordinator: CoordinatorEndpoint,
        keys: Vec<String>,
        options: DescribeGroupsOptions,
        classic: &Mutex<BTreeMap<String, Option<String>>>,
    ) -> Result<BTreeMap<String, GroupOutcome<ConsumerGroupDescription>>, AdminError> {
        let connection = self.connect_coordinator(&coordinator).await?;
        let node = coordinator_node(&coordinator);
        let lock = || {
            classic
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        };
        let (classic_keys, consumer_keys): (Vec<_>, Vec<_>) = {
            let classic = lock();
            keys.into_iter().partition(|key| classic.contains_key(key))
        };
        let mut outcomes = BTreeMap::new();
        let mut classic_keys = classic_keys;
        // Kafka's `NodeApiVersions` fails a request for an API that the
        // coordinator does not advertise with an `UnsupportedVersionException`,
        // and `handleUnsupportedVersionException` moves each group to the
        // classic API.
        let consumer_describe_supported = connection
            .advertised_api_range(consumer_group_describe_request::API_KEY)
            .is_some();
        if !consumer_describe_supported {
            let mut classic = lock();
            for key in &consumer_keys {
                classic.entry(key.clone()).or_insert(None);
            }
            classic_keys.extend(consumer_keys.iter().cloned());
        } else if !consumer_keys.is_empty() {
            let request = ConsumerGroupDescribeRequest {
                group_ids: consumer_keys.clone(),
                include_authorized_operations: options.include_authorized_operations,
                ..Default::default()
            };
            match connection.send(request).await {
                Ok(response) => {
                    for group in response.groups {
                        let group_id = group.group_id.clone();
                        match consumer_answer(group, &node) {
                            ConsumerAnswer::Outcome(outcome) => {
                                outcomes.insert(group_id, outcome);
                            }
                            ConsumerAnswer::Classic { not_found_message } => {
                                lock().insert(group_id.clone(), not_found_message);
                                classic_keys.push(group_id);
                            }
                        }
                    }
                }
                // Kafka's `handleUnsupportedVersionException` moves each group
                // to the classic API.
                Err(ClientError::IncompatibleVersion { .. }) => {
                    let mut classic = lock();
                    for key in consumer_keys {
                        classic.entry(key.clone()).or_insert(None);
                        classic_keys.push(key);
                    }
                }
                Err(error) => {
                    let error = AdminError::from(error);
                    if is_connection_failure(&error) {
                        return Err(error);
                    }
                    outcomes.extend(request_error_outcomes(&consumer_keys, &error));
                }
            }
        }
        if classic_keys.is_empty() {
            return Ok(outcomes);
        }
        let request = DescribeGroupsRequest {
            groups: classic_keys.clone(),
            include_authorized_operations: options.include_authorized_operations,
            ..Default::default()
        };
        match connection.send(request).await {
            Ok(response) => {
                for group in response.groups {
                    let group_id = group.group_id.clone();
                    let not_found_message = lock().get(&group_id).cloned().flatten();
                    outcomes.insert(
                        group_id,
                        classic_consumer_outcome(group, &node, not_found_message),
                    );
                }
            }
            Err(error) => {
                outcomes.extend(request_error_outcomes(&classic_keys, &error.into()));
            }
        }
        Ok(outcomes)
    }

    /// Describes classic groups with `DescribeGroups`, as Apache Kafka's
    /// `describeClassicGroups` does with its `DescribeClassicGroupsHandler`.
    ///
    /// The call finds the coordinator of every group with one batched
    /// `FindCoordinator` request and sends one `DescribeGroups` request to
    /// each coordinator. The coordinator codes retry as
    /// [`AdminClient::describe_consumer_groups`] describes. Every other code
    /// is the final error of its group, with no message, as the handler uses
    /// `error.message()`.
    ///
    /// # Errors
    /// The call itself does not fail. Each group gets its own [`KafkaError`].
    pub async fn describe_classic_groups(
        &self,
        groups: &[&str],
        options: DescribeGroupsOptions,
    ) -> GroupDescriptions<ClassicGroupDescription> {
        self.call_group_coordinators(
            "describeClassicGroups",
            groups,
            |coordinator, keys| async move {
                let connection = self.connect_coordinator(&coordinator).await?;
                let request = DescribeGroupsRequest {
                    groups: keys.clone(),
                    include_authorized_operations: options.include_authorized_operations,
                    ..Default::default()
                };
                Ok(match connection.send(request).await {
                    Ok(response) => response
                        .groups
                        .into_iter()
                        .map(|group| {
                            let group_id = group.group_id.clone();
                            let outcome = match group.error_code {
                                0 => GroupOutcome::Done(Ok(group)),
                                code => group_error_outcome(code, None, &[]),
                            };
                            (group_id, outcome)
                        })
                        .collect(),
                    Err(error) => request_error_outcomes(&keys, &error.into()),
                })
            },
        )
        .await
    }

    /// Describes share groups with `ShareGroupDescribe`, as Apache Kafka's
    /// `describeShareGroups` does with its `DescribeShareGroupsHandler`.
    ///
    /// The call finds the coordinator of every group with one batched
    /// `FindCoordinator` request and sends one `ShareGroupDescribe` request
    /// to each coordinator. The coordinator codes retry as
    /// [`AdminClient::describe_consumer_groups`] describes. Every other code,
    /// such as `GROUP_ID_NOT_FOUND`, is the final error of its group, with
    /// the message of the answer.
    ///
    /// # Errors
    /// The call itself does not fail. Each group gets its own [`KafkaError`].
    pub async fn describe_share_groups(
        &self,
        groups: &[&str],
        options: DescribeGroupsOptions,
    ) -> GroupDescriptions<ShareGroupDescription> {
        self.call_group_coordinators(
            "describeShareGroups",
            groups,
            |coordinator, keys| async move {
                let connection = self.connect_coordinator(&coordinator).await?;
                let request = ShareGroupDescribeRequest {
                    group_ids: keys.clone(),
                    include_authorized_operations: options.include_authorized_operations,
                    ..Default::default()
                };
                Ok(match connection.send(request).await {
                    Ok(response) => response
                        .groups
                        .into_iter()
                        .map(|group| {
                            let group_id = group.group_id.clone();
                            let outcome = match group.error_code {
                                0 => GroupOutcome::Done(Ok(group)),
                                code => group_error_outcome(code, group.error_message, &[]),
                            };
                            (group_id, outcome)
                        })
                        .collect(),
                    Err(error) => request_error_outcomes(&keys, &error.into()),
                })
            },
        )
        .await
    }

    /// Describes streams groups with `StreamsGroupDescribe` v0, as Apache
    /// Kafka's `describeStreamsGroups` does with its
    /// `DescribeStreamsGroupsHandler`.
    ///
    /// The call finds the coordinator of every group with one batched
    /// `FindCoordinator` request and sends one `StreamsGroupDescribe` request
    /// to each coordinator. The coordinator codes retry as
    /// [`AdminClient::describe_consumer_groups`] describes. Every other code
    /// is the final error of its group, with the message of the answer. A
    /// group whose answer has no topology gets `UNKNOWN_SERVER_ERROR` (-1),
    /// as the handler fails it with an `IllegalStateException`.
    ///
    /// # Errors
    /// The call itself does not fail. Each group gets its own [`KafkaError`].
    pub async fn describe_streams_groups(
        &self,
        groups: &[&str],
        options: DescribeGroupsOptions,
    ) -> GroupDescriptions<StreamsGroupDescription> {
        self.call_group_coordinators(
            "describeStreamsGroups",
            groups,
            |coordinator, keys| async move {
                let connection = self.connect_coordinator(&coordinator).await?;
                let request = ReleasedStreamsGroupDescribe(StreamsGroupDescribeRequest {
                    group_ids: keys.clone(),
                    include_authorized_operations: options.include_authorized_operations,
                    ..Default::default()
                });
                Ok(match connection.send(request).await {
                    Ok(response) => response
                        .groups
                        .into_iter()
                        .map(|group| (group.group_id.clone(), streams_outcome(group)))
                        .collect(),
                    Err(error) => request_error_outcomes(&keys, &error.into()),
                })
            },
        )
        .await
    }
}

/// Classifies one `StreamsGroupDescribe` entry, as Kafka's
/// `DescribeStreamsGroupsHandler.handleResponse` does.
fn streams_outcome(group: StreamsDescribedGroup) -> GroupOutcome<StreamsGroupDescription> {
    match group.error_code {
        0 if group.topology.is_none() => GroupOutcome::Done(Err(KafkaError {
            code: UNKNOWN_SERVER_ERROR,
            name: kafka_error_name(UNKNOWN_SERVER_ERROR),
            message: Some("Topology information is missing".to_owned()),
        })),
        0 => GroupOutcome::Done(Ok(group)),
        code => group_error_outcome(code, group.error_message, &[]),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;
    use bytes::BytesMut;
    use krabka_client_core::MockReply;
    use krabka_protocol::owned::{
        common::consumer_group_describe_response::{
            assignment::Assignment, topic_partitions::TopicPartitions,
        },
        consumer_group_describe_response::ConsumerGroupDescribeResponse,
        consumer_protocol_assignment::TopicPartition as AssignedTopic,
        describe_groups_request,
        describe_groups_response::DescribeGroupsResponse,
        share_group_describe_request,
        share_group_describe_response::ShareGroupDescribeResponse,
        streams_group_describe_response::Topology,
    };

    use super::*;
    use crate::{
        group_coordinators::test_support::{GroupCluster, Lookup, Seen, group_cluster},
        partition_leaders::test_support::{decode_request, encode_response},
    };

    const OMITTED: i32 = i32::MIN;

    fn assignment_bytes(version: i16, partitions: &[(&str, &[i32])]) -> bytes::Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_i16(version);
        ConsumerProtocolAssignment {
            assigned_partitions: partitions
                .iter()
                .map(|(topic, partitions)| AssignedTopic {
                    topic: (*topic).to_owned(),
                    partitions: partitions.to_vec(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
        .encode(&mut bytes, version.min(3))
        .expect("assignment encodes");
        bytes.freeze()
    }

    #[test]
    fn classic_assignment_reads_as_kafka_does() {
        let set = |pairs: &[(&str, i32)]| {
            pairs
                .iter()
                .map(|(topic, partition)| ((*topic).to_owned(), *partition))
                .collect::<BTreeSet<_>>()
        };
        for (name, bytes, expected) in [
            ("empty", bytes::Bytes::new(), Ok(BTreeSet::new())),
            (
                "v0",
                assignment_bytes(0, &[("orders", &[0, 2])]),
                Ok(set(&[("orders", 0), ("orders", 2)])),
            ),
            (
                "a newer version reads as v3",
                assignment_bytes(9, &[("orders", &[1])]),
                Ok(set(&[("orders", 1)])),
            ),
            (
                "negative version",
                bytes::Bytes::from_static(&[0xff, 0xff]),
                Err("Unsupported assignment version: -1".to_owned()),
            ),
            (
                "truncated header",
                bytes::Bytes::from_static(&[0]),
                Err("Buffer underflow while parsing consumer protocol's header".to_owned()),
            ),
        ] {
            assert!(classic_assignment(&bytes) == expected, "case {name}");
        }
    }

    fn node(addr: std::net::SocketAddr, id: i32) -> ClusterNode {
        ClusterNode {
            id,
            host: addr.ip().to_string(),
            port: i32::from(addr.port()),
            rack: None,
            is_fenced: false,
        }
    }

    fn consumer_entry(
        group_id: &str,
        error_code: i16,
        message: Option<&str>,
    ) -> ConsumerDescribedGroup {
        ConsumerDescribedGroup {
            error_code,
            error_message: message.map(str::to_owned),
            group_id: group_id.to_owned(),
            group_state: "Stable".to_owned(),
            group_epoch: 4,
            assignment_epoch: 3,
            assignor_name: "uniform".to_owned(),
            members: vec![ConsumerMember {
                member_id: "m1".to_owned(),
                client_id: "c1".to_owned(),
                client_host: "/10.0.0.1".to_owned(),
                member_epoch: 4,
                assignment: Assignment {
                    topic_partitions: vec![TopicPartitions {
                        topic_name: "orders".to_owned(),
                        partitions: vec![0],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                member_type: 1,
                ..Default::default()
            }],
            authorized_operations: OMITTED,
            ..Default::default()
        }
    }

    fn classic_entry(
        group_id: &str,
        error_code: i16,
        protocol_type: &str,
    ) -> ClassicDescribedGroup {
        ClassicDescribedGroup {
            error_code,
            group_id: group_id.to_owned(),
            group_state: "Stable".to_owned(),
            protocol_type: protocol_type.to_owned(),
            protocol_data: "range".to_owned(),
            members: vec![DescribedGroupMember {
                member_id: "m2".to_owned(),
                group_instance_id: Some("static-1".to_owned()),
                client_id: "c2".to_owned(),
                client_host: "/10.0.0.2".to_owned(),
                member_assignment: assignment_bytes(1, &[("orders", &[1])]),
                ..Default::default()
            }],
            authorized_operations: 1 << 3,
            ..Default::default()
        }
    }

    fn consumer_description(group_id: &str, coordinator: ClusterNode) -> ConsumerGroupDescription {
        ConsumerGroupDescription {
            group_id: group_id.to_owned(),
            is_simple_consumer_group: false,
            members: vec![MemberDescription {
                member_id: "m1".to_owned(),
                group_instance_id: None,
                rack_id: None,
                client_id: "c1".to_owned(),
                host: "/10.0.0.1".to_owned(),
                assignment: BTreeSet::from([("orders".to_owned(), 0)]),
                target_assignment: Some(BTreeSet::new()),
                member_epoch: Some(4),
                upgraded: Some(true),
            }],
            partition_assignor: "uniform".to_owned(),
            group_type: GroupType::Consumer,
            group_state: GroupState::Stable,
            coordinator,
            authorized_operations: None,
            group_epoch: Some(4),
            target_assignment_epoch: Some(3),
        }
    }

    fn classic_description(group_id: &str, coordinator: ClusterNode) -> ConsumerGroupDescription {
        ConsumerGroupDescription {
            group_id: group_id.to_owned(),
            is_simple_consumer_group: false,
            members: vec![MemberDescription {
                member_id: "m2".to_owned(),
                group_instance_id: Some("static-1".to_owned()),
                rack_id: None,
                client_id: "c2".to_owned(),
                host: "/10.0.0.2".to_owned(),
                assignment: BTreeSet::from([("orders".to_owned(), 1)]),
                target_assignment: None,
                member_epoch: None,
                upgraded: None,
            }],
            partition_assignor: "range".to_owned(),
            group_type: GroupType::Classic,
            group_state: GroupState::Stable,
            coordinator,
            authorized_operations: Some(BTreeSet::from([AclOperation::Read])),
            group_epoch: None,
            target_assignment_epoch: None,
        }
    }

    /// The group ids of each request of `api_key` that `seen` recorded, with
    /// the broker index and the version, in broker order.
    fn requests_of<T>(
        seen: &Seen,
        api_key: i16,
        read: impl Fn(&[u8], i16) -> T,
    ) -> Vec<(usize, i16, T)> {
        let mut requests = seen
            .requests
            .iter()
            .filter(|request| request.api_key == api_key)
            .map(|request| {
                (
                    request.broker,
                    request.version,
                    read(&request.body, request.version),
                )
            })
            .collect::<Vec<_>>();
        // The coordinators answer at the same time, so order by broker.
        requests.sort_by_key(|(broker, _, _)| *broker);
        requests
    }

    fn consumer_describe_ids(body: &[u8], version: i16) -> Vec<String> {
        decode_request::<ConsumerGroupDescribeRequest>(body, version, true).group_ids
    }

    fn describe_groups_ids(body: &[u8], version: i16) -> Vec<String> {
        decode_request::<DescribeGroupsRequest>(body, version, version >= 5).groups
    }

    /// Kafka's `DescribeConsumerGroupsHandler` sends one batched
    /// `ConsumerGroupDescribe` to each coordinator, falls back to
    /// `DescribeGroups` for a group that the coordinator gives
    /// `GROUP_ID_NOT_FOUND` or `UNSUPPORTED_VERSION`, keeps the message of
    /// the first `GROUP_ID_NOT_FOUND`, and fails a classic group that is not a
    /// consumer group.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_consumer_groups_batches_and_falls_back_to_describe_groups() {
        // "kip848" and "classic" live on broker 0, "other" and "missing" and
        // "connect" on broker 1.
        let cluster: GroupCluster = group_cluster(
            2,
            vec![
                (consumer_group_describe_request::API_KEY, 0, 1),
                (describe_groups_request::API_KEY, 0, 5),
            ],
            |group, _| Lookup::At(usize::from(!matches!(group, "kip848" | "classic"))),
            |_, api_key, version, body, _| {
                let reply = match api_key {
                    consumer_group_describe_request::API_KEY => {
                        let ids = consumer_describe_ids(body, version);
                        let groups = ids
                            .iter()
                            .map(|id| match id.as_str() {
                                "kip848" | "other" => consumer_entry(id, 0, None),
                                "classic" | "connect" => {
                                    consumer_entry(id, 69, Some("Group is not a consumer group."))
                                }
                                "missing" => {
                                    consumer_entry(id, 69, Some("Group missing does not exist."))
                                }
                                _ => consumer_entry(id, 35, None),
                            })
                            .collect();
                        encode_response(
                            &ConsumerGroupDescribeResponse {
                                groups,
                                ..Default::default()
                            },
                            version,
                            true,
                        )
                    }
                    describe_groups_request::API_KEY => {
                        let ids = describe_groups_ids(body, version);
                        let groups = ids
                            .iter()
                            .map(|id| match id.as_str() {
                                "classic" => classic_entry(id, 0, "consumer"),
                                "connect" => classic_entry(id, 0, "connect"),
                                _ => classic_entry(id, 69, ""),
                            })
                            .collect();
                        encode_response(
                            &DescribeGroupsResponse {
                                groups,
                                ..Default::default()
                            },
                            version,
                            version >= 5,
                        )
                    }
                    _ => return MockReply::Silent,
                };
                MockReply::Respond(reply)
            },
        )
        .await;
        let admin = cluster.admin(krabka_units::secs(5)).await;

        let result = admin
            .describe_consumer_groups(
                &["kip848", "classic", "other", "missing", "connect"],
                DescribeGroupsOptions::default(),
            )
            .await;

        let (b0, b1) = (node(cluster.addrs[0], 0), node(cluster.addrs[1], 1));
        let seen = cluster.stop();
        assert!(
            result
                == BTreeMap::from([
                    (
                        "kip848".to_owned(),
                        Ok(consumer_description("kip848", b0.clone()))
                    ),
                    ("classic".to_owned(), Ok(classic_description("classic", b0))),
                    ("other".to_owned(), Ok(consumer_description("other", b1))),
                    (
                        "missing".to_owned(),
                        Err(kafka_error(
                            69,
                            Some("Group missing does not exist.".to_owned())
                        ))
                    ),
                    (
                        "connect".to_owned(),
                        Err(kafka_error(
                            -1,
                            Some("GroupId connect is not a consumer group (connect).".to_owned())
                        ))
                    ),
                ])
        );
        let ids = |ids: &[&str]| ids.iter().map(|id| (*id).to_owned()).collect::<Vec<_>>();
        assert!(
            (
                seen.find_coordinator.clone(),
                requests_of(
                    &seen,
                    consumer_group_describe_request::API_KEY,
                    consumer_describe_ids
                ),
                requests_of(&seen, describe_groups_request::API_KEY, describe_groups_ids),
            ) == (
                vec![(
                    6,
                    ids(&["classic", "connect", "kip848", "missing", "other"])
                )],
                vec![
                    (0, 1, ids(&["classic", "kip848"])),
                    (1, 1, ids(&["connect", "missing", "other"])),
                ],
                vec![
                    (0, 5, ids(&["classic"])),
                    (1, 5, ids(&["connect", "missing"]))
                ],
            )
        );
    }

    /// A coordinator that does not support `ConsumerGroupDescribe` gets
    /// `DescribeGroups` for every group, as Kafka's
    /// `handleUnsupportedVersionException` moves them to the classic API.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_consumer_groups_uses_describe_groups_on_an_old_coordinator() {
        let cluster = group_cluster(
            1,
            vec![(describe_groups_request::API_KEY, 0, 5)],
            |_, _| Lookup::At(0),
            |_, api_key, version, body, _| {
                if api_key != describe_groups_request::API_KEY {
                    return MockReply::Silent;
                }
                let groups = describe_groups_ids(body, version)
                    .iter()
                    .map(|id| classic_entry(id, 0, "consumer"))
                    .collect();
                MockReply::Respond(encode_response(
                    &DescribeGroupsResponse {
                        groups,
                        ..Default::default()
                    },
                    version,
                    true,
                ))
            },
        )
        .await;
        let admin = cluster.admin(krabka_units::secs(5)).await;

        let result = admin
            .describe_consumer_groups(&["classic"], DescribeGroupsOptions::default())
            .await;

        let b0 = node(cluster.addrs[0], 0);
        cluster.stop();
        assert!(
            result
                == BTreeMap::from([("classic".to_owned(), Ok(classic_description("classic", b0)))])
        );
    }

    /// Every describe handler retries `COORDINATOR_LOAD_IN_PROGRESS` on the
    /// same coordinator, finds the coordinator again on
    /// `COORDINATOR_NOT_AVAILABLE` and `NOT_COORDINATOR`, and fails every
    /// other code with the message of the answer (none for
    /// `describeClassicGroups`). The call sends the options' flag.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_calls_retry_coordinator_errors_as_kafka_does() {
        #[derive(Clone, Copy, Debug)]
        enum Call {
            Classic,
            Share,
            Streams,
        }
        let message = Some("from the broker".to_owned());
        for call in [Call::Classic, Call::Share, Call::Streams] {
            for (name, codes, expected_lookups, expected_requests, expected) in [
                ("no error", vec![0], 1, 1, Ok(())),
                ("load in progress", vec![14, 0], 1, 2, Ok(())),
                ("not coordinator", vec![16, 0], 2, 2, Ok(())),
                ("not available", vec![15, 0], 2, 2, Ok(())),
                ("group authorization failed", vec![30], 1, 1, Err(30)),
                ("group id not found", vec![69], 1, 1, Err(69)),
            ] {
                let api_key = match call {
                    Call::Classic => describe_groups_request::API_KEY,
                    Call::Share => share_group_describe_request::API_KEY,
                    Call::Streams => streams_group_describe_request::API_KEY,
                };
                let script = Arc::new(Mutex::new(codes));
                let reply_message = message.clone();
                let cluster = group_cluster(
                    1,
                    vec![(api_key, 0, 6)],
                    |_, _| Lookup::At(0),
                    move |_, key, version, _, _| {
                        if key != api_key {
                            return MockReply::Silent;
                        }
                        let code = {
                            let mut script = script.lock().expect("script lock");
                            if script.len() > 1 {
                                script.remove(0)
                            } else {
                                script[0]
                            }
                        };
                        let body = match call {
                            Call::Classic => encode_response(
                                &DescribeGroupsResponse {
                                    groups: vec![ClassicDescribedGroup {
                                        error_code: code,
                                        error_message: reply_message.clone(),
                                        group_id: "workers".to_owned(),
                                        ..Default::default()
                                    }],
                                    ..Default::default()
                                },
                                version,
                                version >= 5,
                            ),
                            Call::Share => encode_response(
                                &ShareGroupDescribeResponse {
                                    groups: vec![ShareDescribedGroup {
                                        error_code: code,
                                        error_message: reply_message.clone(),
                                        group_id: "workers".to_owned(),
                                        ..Default::default()
                                    }],
                                    ..Default::default()
                                },
                                version,
                                true,
                            ),
                            Call::Streams => encode_response(
                                &StreamsGroupDescribeResponse {
                                    groups: vec![StreamsDescribedGroup {
                                        error_code: code,
                                        error_message: reply_message.clone(),
                                        group_id: "workers".to_owned(),
                                        topology: Some(Topology::default()),
                                        ..Default::default()
                                    }],
                                    ..Default::default()
                                },
                                version,
                                true,
                            ),
                        };
                        MockReply::Respond(body)
                    },
                )
                .await;
                let admin = cluster.admin(krabka_units::secs(5)).await;
                let options = DescribeGroupsOptions {
                    include_authorized_operations: true,
                };
                let result = match call {
                    Call::Classic => admin
                        .describe_classic_groups(&["workers"], options)
                        .await
                        .remove("workers")
                        .map(|result| result.map(drop)),
                    Call::Share => admin
                        .describe_share_groups(&["workers"], options)
                        .await
                        .remove("workers")
                        .map(|result| result.map(drop)),
                    Call::Streams => admin
                        .describe_streams_groups(&["workers"], options)
                        .await
                        .remove("workers")
                        .map(|result| result.map(drop)),
                };
                let seen = cluster.stop();
                let expected_message = match call {
                    Call::Classic => None,
                    Call::Share | Call::Streams => message.clone(),
                };
                let flags = seen
                    .requests
                    .iter()
                    .map(|request| {
                        let (version, body) = (request.version, &request.body[..]);
                        match call {
                            Call::Classic => {
                                decode_request::<DescribeGroupsRequest>(body, version, version >= 5)
                                    .include_authorized_operations
                            }
                            Call::Share => {
                                decode_request::<ShareGroupDescribeRequest>(body, version, true)
                                    .include_authorized_operations
                            }
                            Call::Streams => {
                                decode_request::<StreamsGroupDescribeRequest>(body, version, true)
                                    .include_authorized_operations
                            }
                        }
                    })
                    .collect::<Vec<_>>();
                assert!(
                    (result, seen.find_coordinator.len(), flags)
                        == (
                            Some(expected.map_err(|code| kafka_error(code, expected_message))),
                            expected_lookups,
                            vec![true; expected_requests],
                        ),
                    "case {call:?} {name}"
                );
            }
        }
    }

    /// Kafka 4.3.1 defines `StreamsGroupDescribe` v0 only, and its handler
    /// fails a group whose answer has no topology.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_streams_groups_sends_v0_and_needs_a_topology() {
        let cluster = group_cluster(
            1,
            vec![(streams_group_describe_request::API_KEY, 0, 1)],
            |_, _| Lookup::At(0),
            |_, api_key, version, _, _| {
                if api_key != streams_group_describe_request::API_KEY {
                    return MockReply::Silent;
                }
                MockReply::Respond(encode_response(
                    &StreamsGroupDescribeResponse {
                        groups: vec![
                            StreamsDescribedGroup {
                                group_id: "with".to_owned(),
                                topology: Some(Topology::default()),
                                ..Default::default()
                            },
                            StreamsDescribedGroup {
                                group_id: "without".to_owned(),
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                    version,
                    true,
                ))
            },
        )
        .await;
        let admin = cluster.admin(krabka_units::secs(5)).await;

        let result = admin
            .describe_streams_groups(&["with", "without"], DescribeGroupsOptions::default())
            .await;

        let seen = cluster.stop();
        let versions = seen
            .requests
            .iter()
            .map(|request| request.version)
            .collect::<Vec<_>>();
        assert!(
            (result, versions)
                == (
                    BTreeMap::from([
                        (
                            "with".to_owned(),
                            Ok(StreamsDescribedGroup {
                                group_id: "with".to_owned(),
                                topology: Some(Topology::default()),
                                ..Default::default()
                            })
                        ),
                        (
                            "without".to_owned(),
                            Err(kafka_error(
                                -1,
                                Some("Topology information is missing".to_owned())
                            ))
                        ),
                    ]),
                    vec![0],
                )
        );
    }
}
