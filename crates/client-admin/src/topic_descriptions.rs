//! Topic listing and description, as Kafka's `Admin.listTopics` and
//! `Admin.describeTopics`.
//!
//! `listTopics` and `describeTopics` by topic ID read `Metadata`.
//! `describeTopics` by name first reads the cluster's nodes with
//! `DescribeCluster`, then pages through `DescribeTopicPartitions` (KIP-966),
//! and falls back to `Metadata` when the peer does not support it, as Kafka's
//! `KafkaAdminClient` does.

use std::collections::{BTreeMap, BTreeSet};

use krabka_client_core::ClientError;
use krabka_protocol::{
    ProtocolRequest as _,
    owned::{
        describe_topic_partitions_request::{
            Cursor as RequestCursor, DescribeTopicPartitionsRequest, TopicRequest,
        },
        describe_topic_partitions_response::{
            DescribeTopicPartitionsResponse, DescribeTopicPartitionsResponsePartition,
            DescribeTopicPartitionsResponseTopic,
        },
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::{MetadataResponse, MetadataResponsePartition, MetadataResponseTopic},
    },
    primitives::uuid::Uuid as ProtoUuid,
};
use uuid::Uuid;

use crate::{
    AdminClient, AdminError, ClusterNode, DescribeClusterOptions, KafkaError,
    cluster::authorized_operations, groups::list_groups_kafka_error, kafka_error_name,
    retry::ControllerRetry, users::AclOperation,
};

/// `UNKNOWN_TOPIC_OR_PARTITION`.
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
/// `INVALID_TOPIC_EXCEPTION`.
const INVALID_TOPIC_EXCEPTION: i16 = 17;
/// `UNKNOWN_TOPIC_ID`.
const UNKNOWN_TOPIC_ID: i16 = 100;

/// What [`AdminClient::list_topics`] lists, as Kafka's `ListTopicsOptions`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListTopicsOptions {
    /// List internal topics, such as `__consumer_offsets`, too.
    pub list_internal: bool,
}

/// One topic of [`AdminClient::list_topics`], as Kafka's `TopicListing`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicListing {
    /// The topic name.
    pub name: String,
    /// The topic ID, or `None` when the response carries the zero ID.
    pub topic_id: Option<Uuid>,
    /// Whether the topic is internal.
    pub is_internal: bool,
}

/// What [`AdminClient::describe_topics`] asks for, as Kafka's
/// `DescribeTopicsOptions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DescribeTopicsOptions {
    /// Ask for the operations that the caller may perform on each topic.
    pub include_authorized_operations: bool,
    /// The most partitions that one `DescribeTopicPartitions` answer
    /// carries. Kafka's default is 2000.
    pub partition_size_limit_per_response: i32,
}

impl Default for DescribeTopicsOptions {
    fn default() -> Self {
        Self {
            include_authorized_operations: false,
            partition_size_limit_per_response: 2000,
        }
    }
}

/// One partition of a [`TopicDescription`], as Kafka's
/// `TopicPartitionInfo`.
///
/// A replica that the cluster's node list does not name appears with an
/// empty host and port -1, as Kafka's `new Node(id, "", -1)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionInfo {
    /// The partition index.
    pub partition: i32,
    /// The leader, or `None` when the partition has no leader or the leader
    /// is not a known node.
    pub leader: Option<ClusterNode>,
    /// The replicas, preferred replica first.
    pub replicas: Vec<ClusterNode>,
    /// The in-sync replicas.
    pub isr: Vec<ClusterNode>,
    /// The eligible leader replicas (KIP-966). Always empty through
    /// `Metadata`.
    pub elr: Vec<ClusterNode>,
    /// The last known eligible leader replicas (KIP-966). Always empty
    /// through `Metadata`.
    pub last_known_elr: Vec<ClusterNode>,
}

/// One topic of [`AdminClient::describe_topics`], as Kafka's
/// `TopicDescription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDescription {
    /// The topic name.
    pub name: String,
    /// The topic ID, or `None` when the response carries the zero ID.
    pub topic_id: Option<Uuid>,
    /// Whether the topic is internal.
    pub is_internal: bool,
    /// The partitions, in partition order.
    pub partitions: Vec<TopicPartitionInfo>,
    /// The operations that the caller may perform on the topic, or `None`
    /// when the caller did not ask for them.
    pub authorized_operations: Option<BTreeSet<AclOperation>>,
}

/// The description of each topic of a describe call, or its error.
pub type TopicDescriptions<K> = BTreeMap<K, Result<TopicDescription, KafkaError>>;

fn topic_id(id: ProtoUuid) -> Option<Uuid> {
    (id != ProtoUuid::ZERO).then(|| Uuid::from_bytes(id.0))
}

fn topic_error(code: i16, message: String) -> KafkaError {
    KafkaError {
        code,
        name: kafka_error_name(code),
        message: Some(message),
    }
}

/// The node of `id`, or Kafka's placeholder `Node(id, "", -1)`.
fn node(nodes: &BTreeMap<i32, ClusterNode>, id: i32) -> ClusterNode {
    nodes.get(&id).cloned().unwrap_or_else(|| ClusterNode {
        id,
        host: String::new(),
        port: -1,
        rack: None,
        is_fenced: false,
    })
}

fn node_list(nodes: &BTreeMap<i32, ClusterNode>, ids: &[i32]) -> Vec<ClusterNode> {
    ids.iter().map(|id| node(nodes, *id)).collect()
}

/// The request of Kafka's `MetadataRequest.Builder.allTopics()`.
fn all_topics_request() -> MetadataRequest {
    MetadataRequest {
        topics: None,
        allow_auto_topic_creation: true,
        ..Default::default()
    }
}

/// The listings of a `Metadata` answer, as Kafka's `listTopics`
/// `handleResponse` reads it.
fn topic_listings(
    response: MetadataResponse,
    options: ListTopicsOptions,
) -> BTreeMap<String, TopicListing> {
    response
        .topics
        .into_iter()
        .filter(|topic| !topic.is_internal || options.list_internal)
        .filter_map(|topic| {
            let name = topic.name?;
            Some((
                name.clone(),
                TopicListing {
                    name,
                    topic_id: topic_id(topic.topic_id),
                    is_internal: topic.is_internal,
                },
            ))
        })
        .collect()
}

/// One `DescribeTopicPartitions` request, as Kafka's
/// `generateDescribeTopicsCallWithDescribeTopicPartitionsApi`
/// `createRequest` builds it. The cursor points past the partitions already
/// read of the topic that the previous answer cut.
fn describe_partitions_request(
    pending: &BTreeSet<String>,
    partial: Option<&TopicDescription>,
    options: DescribeTopicsOptions,
) -> DescribeTopicPartitionsRequest {
    DescribeTopicPartitionsRequest {
        topics: pending
            .iter()
            .map(|name| TopicRequest {
                name: name.clone(),
                ..Default::default()
            })
            .collect(),
        response_partition_limit: options.partition_size_limit_per_response,
        cursor: partial.map(|partial| RequestCursor {
            topic_name: partial.name.clone(),
            partition_index: i32::try_from(partial.partitions.len()).unwrap_or(i32::MAX),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn partition_from_describe(
    partition: &DescribeTopicPartitionsResponsePartition,
    nodes: &BTreeMap<i32, ClusterNode>,
) -> TopicPartitionInfo {
    TopicPartitionInfo {
        partition: partition.partition_index,
        leader: nodes.get(&partition.leader_id).cloned(),
        replicas: node_list(nodes, &partition.replica_nodes),
        isr: node_list(nodes, &partition.isr_nodes),
        elr: node_list(
            nodes,
            partition
                .eligible_leader_replicas
                .as_deref()
                .unwrap_or_default(),
        ),
        last_known_elr: node_list(
            nodes,
            partition.last_known_elr.as_deref().unwrap_or_default(),
        ),
    }
}

/// The description of one `DescribeTopicPartitions` topic, as Kafka's
/// `getTopicDescriptionFromDescribeTopicsResponseTopic`.
fn description_from_describe(
    topic: &DescribeTopicPartitionsResponseTopic,
    name: String,
    nodes: &BTreeMap<i32, ClusterNode>,
    include_authorized_operations: bool,
) -> TopicDescription {
    TopicDescription {
        name,
        topic_id: topic_id(topic.topic_id),
        is_internal: topic.is_internal,
        partitions: topic
            .partitions
            .iter()
            .map(|partition| partition_from_describe(partition, nodes))
            .collect(),
        authorized_operations: if include_authorized_operations {
            authorized_operations(topic.topic_authorized_operations)
        } else {
            None
        },
    }
}

/// The paging state of one `describeTopics` by name.
#[derive(Debug, Default)]
struct DescribePages {
    /// The topics that no answer has finished.
    pending: BTreeSet<String>,
    /// The topic that the last answer cut, with the partitions read so far.
    partial: Option<TopicDescription>,
    /// The finished topics.
    out: TopicDescriptions<String>,
}

impl DescribePages {
    /// Reads one answer, as Kafka's `handleResponse` does: a topic error
    /// finishes the topic, the cursor topic waits for the next page, and the
    /// partial topic of the previous page gets the new partitions.
    fn read(
        &mut self,
        response: DescribeTopicPartitionsResponse,
        nodes: &BTreeMap<i32, ClusterNode>,
        include_authorized_operations: bool,
    ) {
        let mut cursor = response.next_cursor.map(|cursor| cursor.topic_name);
        let mut next = None;
        for topic in response.topics {
            let Some(name) = topic.name.clone() else {
                tracing::warn!("the DescribeTopicPartitions response names a topic without a name");
                continue;
            };
            let asked = self.pending.contains(&name)
                || self.partial.as_ref().is_some_and(|p| p.name == name);
            if !asked {
                tracing::warn!(
                    topic = %name,
                    "the DescribeTopicPartitions response names a topic that is not in the request"
                );
                continue;
            }
            if topic.error_code != 0 {
                self.out.entry(name.clone()).or_insert_with(|| {
                    Err(KafkaError {
                        code: topic.error_code,
                        name: kafka_error_name(topic.error_code),
                        message: None,
                    })
                });
                self.pending.remove(&name);
                if cursor.as_deref() == Some(name.as_str()) {
                    cursor = None;
                }
                continue;
            }
            let current = description_from_describe(
                &topic,
                name.clone(),
                nodes,
                include_authorized_operations,
            );
            if let Some(partial) = self.partial.as_mut()
                && partial.name == name
            {
                partial.partitions.extend(current.partitions);
                continue;
            }
            if cursor.as_deref() == Some(name.as_str()) {
                next = Some(current);
                continue;
            }
            self.pending.remove(&name);
            self.out.entry(name).or_insert(Ok(current));
        }
        if let Some(partial) = self
            .partial
            .take_if(|partial| cursor.as_deref() != Some(partial.name.as_str()))
        {
            self.pending.remove(&partial.name);
            self.out.entry(partial.name.clone()).or_insert(Ok(partial));
        }
        if next.is_some() {
            self.partial = next;
        }
    }

    /// Whether another page is needed.
    fn unfinished(&self) -> bool {
        !self.pending.is_empty() || self.partial.is_some()
    }

    /// Fails every unfinished topic with `error`.
    fn fail(mut self, error: &KafkaError) -> TopicDescriptions<String> {
        let unfinished = std::mem::take(&mut self.pending)
            .into_iter()
            .chain(self.partial.take().map(|partial| partial.name));
        for name in unfinished {
            self.out.entry(name).or_insert_with(|| Err(error.clone()));
        }
        self.out
    }
}

/// The `Metadata` request of `describeTopics` by name, as Kafka's
/// `generateDescribeTopicsCallWithMetadataApi` builds it.
fn metadata_by_name_request(
    names: &BTreeSet<String>,
    options: DescribeTopicsOptions,
) -> MetadataRequest {
    MetadataRequest {
        topics: Some(
            names
                .iter()
                .map(|name| MetadataRequestTopic {
                    topic_id: ProtoUuid::ZERO,
                    name: Some(name.clone()),
                    ..Default::default()
                })
                .collect(),
        ),
        allow_auto_topic_creation: false,
        include_topic_authorized_operations: options.include_authorized_operations,
        ..Default::default()
    }
}

/// The `Metadata` request of `describeTopics` by ID, as Kafka's
/// `handleDescribeTopicsByIds` builds it.
fn metadata_by_id_request(ids: &BTreeSet<Uuid>, options: DescribeTopicsOptions) -> MetadataRequest {
    MetadataRequest {
        topics: Some(
            ids.iter()
                .map(|id| MetadataRequestTopic {
                    topic_id: ProtoUuid(*id.as_bytes()),
                    name: None,
                    ..Default::default()
                })
                .collect(),
        ),
        allow_auto_topic_creation: false,
        include_topic_authorized_operations: options.include_authorized_operations,
        ..Default::default()
    }
}

fn metadata_nodes(response: &MetadataResponse) -> BTreeMap<i32, ClusterNode> {
    response
        .brokers
        .iter()
        .map(|broker| {
            (
                broker.node_id,
                ClusterNode {
                    id: broker.node_id,
                    host: broker.host.clone(),
                    port: broker.port,
                    rack: broker.rack.clone(),
                    is_fenced: false,
                },
            )
        })
        .collect()
}

fn partition_from_metadata(
    partition: &MetadataResponsePartition,
    nodes: &BTreeMap<i32, ClusterNode>,
) -> TopicPartitionInfo {
    TopicPartitionInfo {
        partition: partition.partition_index,
        leader: (partition.leader_id >= 0)
            .then(|| nodes.get(&partition.leader_id).cloned())
            .flatten(),
        replicas: node_list(nodes, &partition.replica_nodes),
        isr: node_list(nodes, &partition.isr_nodes),
        elr: Vec::new(),
        last_known_elr: Vec::new(),
    }
}

/// The description of one `Metadata` topic, as Kafka's
/// `getTopicDescriptionFromCluster`: partitions in partition order, and the
/// authorized operations whenever the answer carries them.
fn description_from_metadata(
    topic: &MetadataResponseTopic,
    name: String,
    nodes: &BTreeMap<i32, ClusterNode>,
) -> TopicDescription {
    let mut partitions = topic
        .partitions
        .iter()
        .map(|partition| partition_from_metadata(partition, nodes))
        .collect::<Vec<_>>();
    partitions.sort_by_key(|partition| partition.partition);
    TopicDescription {
        name,
        topic_id: topic_id(topic.topic_id),
        is_internal: topic.is_internal,
        partitions,
        authorized_operations: authorized_operations(topic.topic_authorized_operations),
    }
}

/// The description of each topic of `names` in a `Metadata` answer, as
/// Kafka's `generateDescribeTopicsCallWithMetadataApi` `handleResponse`
/// reads it: a topic error first, then `UNKNOWN_TOPIC_OR_PARTITION` for a
/// topic the answer does not name.
fn descriptions_by_name(
    names: &BTreeSet<String>,
    response: MetadataResponse,
) -> TopicDescriptions<String> {
    let nodes = metadata_nodes(&response);
    let mut topics = response
        .topics
        .into_iter()
        .filter_map(|topic| Some((topic.name.clone()?, topic)))
        .collect::<BTreeMap<_, _>>();
    names
        .iter()
        .map(|name| {
            let result = match topics.remove(name) {
                Some(topic) if topic.error_code != 0 => Err(KafkaError {
                    code: topic.error_code,
                    name: kafka_error_name(topic.error_code),
                    message: None,
                }),
                Some(topic) => Ok(description_from_metadata(&topic, name.clone(), &nodes)),
                None => Err(topic_error(
                    UNKNOWN_TOPIC_OR_PARTITION,
                    format!("Topic {name} not found."),
                )),
            };
            (name.clone(), result)
        })
        .collect()
}

/// The description of each topic of `ids` in a `Metadata` answer, as
/// Kafka's `handleDescribeTopicsByIds` `handleResponse` reads it. Kafka's
/// `Cluster` holds only topics without an error, so an ID that the answer
/// does not name without an error gives `UNKNOWN_TOPIC_ID`.
fn descriptions_by_id(ids: &BTreeSet<Uuid>, response: MetadataResponse) -> TopicDescriptions<Uuid> {
    let nodes = metadata_nodes(&response);
    let mut topics = response
        .topics
        .into_iter()
        .filter(|topic| topic.error_code == 0)
        .filter_map(|topic| Some((topic_id(topic.topic_id)?, topic)))
        .collect::<BTreeMap<_, _>>();
    ids.iter()
        .map(|id| {
            let result = match topics.remove(id) {
                Some(mut topic) => match topic.name.take() {
                    Some(name) => Ok(description_from_metadata(&topic, name, &nodes)),
                    None => Err(topic_error(
                        UNKNOWN_TOPIC_ID,
                        format!("TopicId {id} not found."),
                    )),
                },
                None => Err(topic_error(
                    UNKNOWN_TOPIC_ID,
                    format!("TopicId {id} not found."),
                )),
            };
            (*id, result)
        })
        .collect()
}

/// Whether `error` means that the peer does not support the request's
/// version, as Kafka's `UnsupportedVersionException`.
const fn is_unsupported_version(error: &AdminError) -> bool {
    matches!(
        error,
        AdminError::Transport(ClientError::IncompatibleVersion { .. })
    )
}

impl AdminClient {
    /// Lists the topics of the cluster, as Kafka's `listTopics` operation
    /// does: one `Metadata` request for all topics, keeping internal topics
    /// only with [`ListTopicsOptions::list_internal`].
    ///
    /// # Errors
    /// Returns a transport error, and [`AdminError::Broker`] with
    /// `REQUEST_TIMED_OUT` (7) at `default.api.timeout.ms` (60 s).
    pub async fn list_topics(
        &self,
        options: ListTopicsOptions,
    ) -> Result<BTreeMap<String, TopicListing>, AdminError> {
        let retry = ControllerRetry::new("Metadata", self.retry);
        let response = retry
            .bounded(self.conn.send_at_least(all_topics_request(), 1))
            .await?;
        Ok(topic_listings(response, options))
    }

    /// Describes each topic of `names`, as Kafka's `describeTopics`
    /// operation does for a `TopicNameCollection`.
    ///
    /// The client reads the nodes of the cluster with `DescribeCluster`,
    /// then sends `DescribeTopicPartitions` (KIP-966) and follows its cursor
    /// until every topic is complete, at most
    /// [`DescribeTopicsOptions::partition_size_limit_per_response`]
    /// partitions per answer. When the peer does not support
    /// `DescribeTopicPartitions`, such as a controller reached through
    /// controller bootstrap, it sends `Metadata` instead.
    ///
    /// The result has one entry for each name:
    ///
    /// - The description of the topic.
    /// - `INVALID_TOPIC_EXCEPTION` (17) for an empty name, which Kafka
    ///   cannot send.
    /// - The topic error of the answer, such as `UNKNOWN_TOPIC_OR_PARTITION`
    ///   (3) or `TOPIC_AUTHORIZATION_FAILED` (29).
    /// - The error of the call for every topic it did not finish, such as a
    ///   failed `DescribeCluster`, a transport error, or `REQUEST_TIMED_OUT`
    ///   (7) at `default.api.timeout.ms` (60 s).
    pub async fn describe_topics(
        &self,
        names: &[&str],
        options: DescribeTopicsOptions,
    ) -> TopicDescriptions<String> {
        let mut pages = DescribePages::default();
        for name in names {
            if name.is_empty() {
                pages.out.insert(
                    String::new(),
                    Err(topic_error(
                        INVALID_TOPIC_EXCEPTION,
                        "The given topic name '' cannot be represented in a request.".to_owned(),
                    )),
                );
            } else {
                pages.pending.insert((*name).to_owned());
            }
        }
        if pages.pending.is_empty() {
            return pages.out;
        }
        let nodes = match self
            .describe_cluster(DescribeClusterOptions::default())
            .await
        {
            Ok(cluster) => cluster
                .nodes
                .into_iter()
                .map(|node| (node.id, node))
                .collect::<BTreeMap<_, _>>(),
            Err(error) => return pages.fail(&list_groups_kafka_error(&error)),
        };
        if self
            .conn
            .advertised_api_range(DescribeTopicPartitionsRequest::API_KEY)
            .await
            .is_none()
        {
            return self.describe_topics_with_metadata(pages, options).await;
        }
        let retry = ControllerRetry::new("DescribeTopicPartitions", self.retry);
        while pages.unfinished() {
            let request =
                describe_partitions_request(&pages.pending, pages.partial.as_ref(), options);
            match retry.bounded(self.conn.send(request)).await {
                Ok(response) => pages.read(response, &nodes, options.include_authorized_operations),
                Err(error) if is_unsupported_version(&error) => {
                    return self.describe_topics_with_metadata(pages, options).await;
                }
                Err(error) => return pages.fail(&list_groups_kafka_error(&error)),
            }
        }
        pages.out
    }

    /// The `Metadata` fallback of [`Self::describe_topics`] for the topics
    /// that `pages` has not finished. A peer below `Metadata` v4 cannot
    /// refuse auto-creation, so it gets a request for all topics, as
    /// Kafka's `supportsDisablingTopicCreation` fallback sends.
    async fn describe_topics_with_metadata(
        &self,
        mut pages: DescribePages,
        options: DescribeTopicsOptions,
    ) -> TopicDescriptions<String> {
        if let Some(partial) = pages.partial.take() {
            pages.pending.insert(partial.name);
        }
        let retry = ControllerRetry::new("Metadata", self.retry);
        let request = metadata_by_name_request(&pages.pending, options);
        let response = match retry.bounded(self.conn.send_at_least(request, 4)).await {
            Err(error) if is_unsupported_version(&error) => {
                retry
                    .bounded(self.conn.send_at_least(all_topics_request(), 1))
                    .await
            }
            other => other,
        };
        match response {
            Ok(response) => {
                let pending = std::mem::take(&mut pages.pending);
                for (name, result) in descriptions_by_name(&pending, response) {
                    pages.out.entry(name).or_insert(result);
                }
                pages.out
            }
            Err(error) => pages.fail(&list_groups_kafka_error(&error)),
        }
    }

    /// Describes each topic of `ids`, as Kafka's `describeTopics` operation
    /// does for a `TopicIdCollection`: one `Metadata` request (v12 or
    /// higher, which Kafka needs for topic IDs).
    ///
    /// The result has one entry for each ID: the description of the topic,
    /// `INVALID_TOPIC_EXCEPTION` (17) for the nil ID, `UNKNOWN_TOPIC_ID`
    /// (100) for an ID that the answer does not describe, or the error of
    /// the call, such as `UNSUPPORTED_VERSION` (35) for a peer below
    /// `Metadata` v12.
    pub async fn describe_topics_by_id(
        &self,
        ids: &[Uuid],
        options: DescribeTopicsOptions,
    ) -> TopicDescriptions<Uuid> {
        let mut out = TopicDescriptions::new();
        let mut pending = BTreeSet::new();
        for id in ids {
            if id.is_nil() {
                out.insert(
                    *id,
                    Err(topic_error(
                        INVALID_TOPIC_EXCEPTION,
                        format!("The given topic id '{id}' cannot be represented in a request."),
                    )),
                );
            } else {
                pending.insert(*id);
            }
        }
        if pending.is_empty() {
            return out;
        }
        let retry = ControllerRetry::new("Metadata", self.retry);
        let request = metadata_by_id_request(&pending, options);
        match retry.bounded(self.conn.send_at_least(request, 12)).await {
            Ok(response) => out.extend(descriptions_by_id(&pending, response)),
            Err(error) => {
                let error = list_groups_kafka_error(&error);
                out.extend(pending.into_iter().map(|id| (id, Err(error.clone()))));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{Arc, Mutex},
    };

    use krabka_client_core::MockReply;
    use krabka_protocol::owned::{
        describe_cluster_request,
        describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
        describe_topic_partitions_request,
        describe_topic_partitions_response::Cursor as ResponseCursor,
        metadata_request,
        metadata_response::MetadataResponseBroker,
    };

    use super::*;
    use crate::partition_leaders::test_support::{
        decode_request, encode_response, fast_admin, scripted_broker,
    };

    const ALPHA: Uuid = Uuid::from_u128(0xa);
    const BETA: Uuid = Uuid::from_u128(0xb);

    fn id_of(name: &str) -> Uuid {
        if name == "alpha" { ALPHA } else { BETA }
    }

    /// Broker 1 at `own`, the only node that the cluster names.
    fn broker_one(own: SocketAddr) -> ClusterNode {
        ClusterNode {
            id: 1,
            host: own.ip().to_string(),
            port: i32::from(own.port()),
            rack: None,
            is_fenced: false,
        }
    }

    /// Broker 2, which the cluster does not name: Kafka's placeholder node.
    fn unknown_two() -> ClusterNode {
        ClusterNode {
            id: 2,
            host: String::new(),
            port: -1,
            rack: None,
            is_fenced: false,
        }
    }

    /// Partition `index` as `DescribeTopicPartitions` gives it: leader 1,
    /// replicas 1 and 2, ISR 1, ELR 2.
    fn page_partition(index: i32) -> DescribeTopicPartitionsResponsePartition {
        DescribeTopicPartitionsResponsePartition {
            partition_index: index,
            leader_id: 1,
            replica_nodes: vec![1, 2],
            isr_nodes: vec![1],
            eligible_leader_replicas: Some(vec![2]),
            last_known_elr: Some(Vec::new()),
            ..Default::default()
        }
    }

    /// A `DescribeTopicPartitions` topic whose authorized operations are
    /// `READ` (bit 3).
    fn page_topic(
        name: &str,
        error_code: i16,
        partitions: &[i32],
    ) -> DescribeTopicPartitionsResponseTopic {
        DescribeTopicPartitionsResponseTopic {
            error_code,
            name: Some(name.to_owned()),
            topic_id: ProtoUuid(*id_of(name).as_bytes()),
            partitions: partitions.iter().copied().map(page_partition).collect(),
            topic_authorized_operations: 1 << 3,
            ..Default::default()
        }
    }

    fn page(
        topics: Vec<DescribeTopicPartitionsResponseTopic>,
        cursor: Option<(&str, i32)>,
    ) -> DescribeTopicPartitionsResponse {
        DescribeTopicPartitionsResponse {
            topics,
            next_cursor: cursor.map(|(topic_name, partition_index)| ResponseCursor {
                topic_name: topic_name.to_owned(),
                partition_index,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// The description that `DescribeTopicPartitions` pages give `name`.
    fn paged(name: &str, partitions: &[i32], own: SocketAddr) -> TopicDescription {
        TopicDescription {
            name: name.to_owned(),
            topic_id: Some(id_of(name)),
            is_internal: false,
            partitions: partitions
                .iter()
                .map(|partition| TopicPartitionInfo {
                    partition: *partition,
                    leader: Some(broker_one(own)),
                    replicas: vec![broker_one(own), unknown_two()],
                    isr: vec![broker_one(own)],
                    elr: vec![unknown_two()],
                    last_known_elr: Vec::new(),
                })
                .collect(),
            authorized_operations: None,
        }
    }

    fn dtp_request(topics: &[&str], cursor: Option<(&str, i32)>) -> DescribeTopicPartitionsRequest {
        DescribeTopicPartitionsRequest {
            topics: topics
                .iter()
                .map(|name| TopicRequest {
                    name: (*name).to_owned(),
                    ..Default::default()
                })
                .collect(),
            response_partition_limit: 2000,
            cursor: cursor.map(|(topic_name, partition_index)| RequestCursor {
                topic_name: topic_name.to_owned(),
                partition_index,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// A `Metadata` topic with partitions `indexes` in that order: leader
    /// `leader`, replicas 1 and 2, ISR 1.
    fn metadata_topic(
        name: &str,
        error_code: i16,
        indexes: &[i32],
        leader: i32,
    ) -> MetadataResponseTopic {
        MetadataResponseTopic {
            error_code,
            name: Some(name.to_owned()),
            topic_id: ProtoUuid(*id_of(name).as_bytes()),
            is_internal: name.starts_with("__"),
            partitions: indexes
                .iter()
                .map(|index| MetadataResponsePartition {
                    partition_index: *index,
                    leader_id: leader,
                    replica_nodes: vec![1, 2],
                    isr_nodes: vec![1],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn metadata(own: SocketAddr, topics: Vec<MetadataResponseTopic>) -> MetadataResponse {
        MetadataResponse {
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: own.ip().to_string(),
                port: i32::from(own.port()),
                ..Default::default()
            }],
            controller_id: 1,
            topics,
            ..Default::default()
        }
    }

    /// The description that `Metadata` gives `name`, partitions sorted.
    fn from_metadata(
        name: &str,
        partitions: &[i32],
        own: SocketAddr,
        leader: bool,
    ) -> TopicDescription {
        TopicDescription {
            name: name.to_owned(),
            topic_id: Some(id_of(name)),
            is_internal: false,
            partitions: partitions
                .iter()
                .map(|partition| TopicPartitionInfo {
                    partition: *partition,
                    leader: leader.then(|| broker_one(own)),
                    replicas: vec![broker_one(own), unknown_two()],
                    isr: vec![broker_one(own)],
                    elr: Vec::new(),
                    last_known_elr: Vec::new(),
                })
                .collect(),
            authorized_operations: None,
        }
    }

    fn topic_err(code: i16, message: Option<String>) -> Result<TopicDescription, KafkaError> {
        Err(KafkaError {
            code,
            name: kafka_error_name(code),
            message,
        })
    }

    /// What the mock broker of one case answers to `DescribeTopicPartitions`.
    #[derive(Clone)]
    enum Answer {
        Page(DescribeTopicPartitionsResponse),
        Close,
    }

    /// What one mock broker received.
    #[derive(Debug, Clone, PartialEq)]
    enum Received {
        Pages(DescribeTopicPartitionsRequest),
        Metadata(i16, MetadataRequest),
    }

    /// One case of [`describe_topics_pages_and_falls_back_to_metadata`].
    struct DescribeCase {
        name: &'static str,
        names: Vec<&'static str>,
        cluster_error: i16,
        dtp: bool,
        answers: Vec<Answer>,
        metadata_topics: Vec<MetadataResponseTopic>,
        expected: fn(SocketAddr) -> (TopicDescriptions<String>, Vec<Received>),
    }

    /// Runs `describe_topics` against a broker that answers
    /// `DescribeCluster` with the case's error and broker 1, each
    /// `DescribeTopicPartitions` request with the next answer (when the case
    /// advertises the API), and `Metadata` with the case's topics.
    async fn describe_case(
        case: &DescribeCase,
    ) -> (TopicDescriptions<String>, Vec<Received>, SocketAddr) {
        let received = Arc::new(Mutex::new(Vec::new()));
        let handler_received = Arc::clone(&received);
        let mut apis = vec![(describe_cluster_request::API_KEY, 0, 2)];
        if case.dtp {
            apis.push((describe_topic_partitions_request::API_KEY, 0, 0));
        }
        let cluster_error = case.cluster_error;
        let metadata_topics = case.metadata_topics.clone();
        let pages = Arc::new(Mutex::new(case.answers.clone().into_iter()));
        let broker = scripted_broker(apis, move |api_key, version, body, own| {
            let mut received = handler_received.lock().expect("received lock");
            let reply = match api_key {
                describe_cluster_request::API_KEY => encode_response(
                    &DescribeClusterResponse {
                        error_code: cluster_error,
                        controller_id: 1,
                        brokers: vec![DescribeClusterBroker {
                            broker_id: 1,
                            host: own.ip().to_string(),
                            port: i32::from(own.port()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    version,
                    true,
                ),
                describe_topic_partitions_request::API_KEY => {
                    received.push(Received::Pages(decode_request(body, version, true)));
                    match pages.lock().expect("pages lock").next() {
                        Some(Answer::Page(page)) => encode_response(&page, version, true),
                        Some(Answer::Close) => return MockReply::Close,
                        None => return MockReply::Silent,
                    }
                }
                metadata_request::API_KEY => {
                    let flexible = version >= metadata_request::FLEXIBLE_MIN;
                    received.push(Received::Metadata(
                        version,
                        decode_request(body, version, flexible),
                    ));
                    encode_response(&metadata(own, metadata_topics.clone()), version, flexible)
                }
                _ => return MockReply::Silent,
            };
            MockReply::Respond(reply)
        })
        .await;
        let admin = fast_admin(broker.addr, krabka_units::secs(5)).await;
        let result = admin
            .describe_topics(&case.names, DescribeTopicsOptions::default())
            .await;
        let own = broker.addr;
        broker.stop();
        let received = received.lock().expect("received lock").clone();
        (result, received, own)
    }

    /// Kafka's `describeTopics` by name pages through
    /// `DescribeTopicPartitions` with its cursor, maps each replica to its
    /// node, fails a topic with its error, retries a lost connection, falls
    /// back to `Metadata` when the peer lacks the API, and fails every topic
    /// when `DescribeCluster` fails.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_topics_pages_and_falls_back_to_metadata() {
        let cases = [
            DescribeCase {
                name: "one page",
                names: vec!["beta", "alpha"],
                cluster_error: 0,
                dtp: true,
                answers: vec![Answer::Page(page(
                    vec![page_topic("alpha", 0, &[0]), page_topic("beta", 0, &[0, 1])],
                    None,
                ))],
                metadata_topics: Vec::new(),
                expected: |own| {
                    (
                        BTreeMap::from([
                            ("alpha".to_owned(), Ok(paged("alpha", &[0], own))),
                            ("beta".to_owned(), Ok(paged("beta", &[0, 1], own))),
                        ]),
                        vec![Received::Pages(dtp_request(&["alpha", "beta"], None))],
                    )
                },
            },
            DescribeCase {
                name: "the cursor splits a topic across pages",
                names: vec!["alpha", "beta"],
                cluster_error: 0,
                dtp: true,
                answers: vec![
                    Answer::Page(page(
                        vec![page_topic("alpha", 0, &[0]), page_topic("beta", 0, &[0])],
                        Some(("beta", 1)),
                    )),
                    Answer::Page(page(vec![page_topic("beta", 0, &[1, 2])], None)),
                ],
                metadata_topics: Vec::new(),
                expected: |own| {
                    (
                        BTreeMap::from([
                            ("alpha".to_owned(), Ok(paged("alpha", &[0], own))),
                            ("beta".to_owned(), Ok(paged("beta", &[0, 1, 2], own))),
                        ]),
                        vec![
                            Received::Pages(dtp_request(&["alpha", "beta"], None)),
                            Received::Pages(dtp_request(&["beta"], Some(("beta", 1)))),
                        ],
                    )
                },
            },
            DescribeCase {
                name: "a topic error fails that topic only",
                names: vec!["alpha", "beta"],
                cluster_error: 0,
                dtp: true,
                answers: vec![Answer::Page(page(
                    vec![page_topic("alpha", 29, &[]), page_topic("beta", 0, &[0])],
                    None,
                ))],
                metadata_topics: Vec::new(),
                expected: |own| {
                    (
                        BTreeMap::from([
                            ("alpha".to_owned(), topic_err(29, None)),
                            ("beta".to_owned(), Ok(paged("beta", &[0], own))),
                        ]),
                        vec![Received::Pages(dtp_request(&["alpha", "beta"], None))],
                    )
                },
            },
            DescribeCase {
                name: "a lost connection is retried",
                names: vec!["alpha"],
                cluster_error: 0,
                dtp: true,
                answers: vec![
                    Answer::Close,
                    Answer::Page(page(vec![page_topic("alpha", 0, &[0])], None)),
                ],
                metadata_topics: Vec::new(),
                expected: |own| {
                    (
                        BTreeMap::from([("alpha".to_owned(), Ok(paged("alpha", &[0], own)))]),
                        vec![
                            Received::Pages(dtp_request(&["alpha"], None)),
                            Received::Pages(dtp_request(&["alpha"], None)),
                        ],
                    )
                },
            },
            DescribeCase {
                name: "a peer without DescribeTopicPartitions is asked with Metadata",
                names: vec!["alpha", "beta", ""],
                cluster_error: 0,
                dtp: false,
                answers: Vec::new(),
                metadata_topics: vec![metadata_topic("alpha", 0, &[1, 0], -1)],
                expected: |own| {
                    (
                        BTreeMap::from([
                            (
                                String::new(),
                                topic_err(
                                    17,
                                    Some(
                                        "The given topic name '' cannot be represented in a \
                                         request."
                                            .to_owned(),
                                    ),
                                ),
                            ),
                            (
                                "alpha".to_owned(),
                                Ok(from_metadata("alpha", &[0, 1], own, false)),
                            ),
                            (
                                "beta".to_owned(),
                                topic_err(3, Some("Topic beta not found.".to_owned())),
                            ),
                        ]),
                        vec![Received::Metadata(
                            12,
                            metadata_by_name_request(
                                &BTreeSet::from(["alpha".to_owned(), "beta".to_owned()]),
                                DescribeTopicsOptions::default(),
                            ),
                        )],
                    )
                },
            },
            DescribeCase {
                name: "a failed DescribeCluster fails every topic",
                names: vec!["alpha"],
                cluster_error: 31,
                dtp: true,
                answers: Vec::new(),
                metadata_topics: Vec::new(),
                expected: |_| {
                    (
                        BTreeMap::from([("alpha".to_owned(), topic_err(31, None))]),
                        Vec::new(),
                    )
                },
            },
        ];
        for case in &cases {
            let (result, received, own) = describe_case(case).await;
            assert2::assert!(
                (result, received) == (case.expected)(own),
                "case {}",
                case.name
            );
        }
    }

    /// Kafka's `listTopics` asks `Metadata` for all topics, keeps internal
    /// topics only with `listInternal`, and retries a lost connection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_topics_keeps_internal_topics_on_request() {
        let listing = |name: &str, is_internal| {
            (
                name.to_owned(),
                TopicListing {
                    name: name.to_owned(),
                    topic_id: Some(id_of(name)),
                    is_internal,
                },
            )
        };
        for (name, list_internal, close_first, expected) in [
            (
                "without internal topics",
                false,
                false,
                (BTreeMap::from([listing("alpha", false)]), 1),
            ),
            (
                "with internal topics",
                true,
                false,
                (
                    BTreeMap::from([listing("__consumer_offsets", true), listing("alpha", false)]),
                    1,
                ),
            ),
            (
                "a lost connection is retried",
                false,
                true,
                (BTreeMap::from([listing("alpha", false)]), 2),
            ),
        ] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let handler_requests = Arc::clone(&requests);
            let broker = scripted_broker(Vec::new(), move |api_key, version, body, own| {
                if api_key != metadata_request::API_KEY {
                    return MockReply::Silent;
                }
                let mut requests = handler_requests.lock().expect("requests lock");
                let request: MetadataRequest = decode_request(body, version, true);
                requests.push(request);
                if close_first && requests.len() == 1 {
                    return MockReply::Close;
                }
                MockReply::Respond(encode_response(
                    &metadata(
                        own,
                        vec![
                            metadata_topic("alpha", 0, &[0], 1),
                            metadata_topic("__consumer_offsets", 0, &[0], 1),
                        ],
                    ),
                    version,
                    true,
                ))
            })
            .await;
            let admin = fast_admin(broker.addr, krabka_units::secs(5)).await;
            let result = admin
                .list_topics(ListTopicsOptions { list_internal })
                .await
                .expect("list topics");
            broker.stop();
            let requests = requests.lock().expect("requests lock").clone();
            assert2::assert!(
                (result, requests) == (expected.0, vec![all_topics_request(); expected.1]),
                "case {name}"
            );
        }
    }

    /// Kafka's `describeTopics` by ID sends `Metadata` v12 or higher with
    /// the IDs, gives `UNKNOWN_TOPIC_ID` to an ID it does not find or that
    /// has an error, refuses the nil ID, and fails every ID on a peer below
    /// v12.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_topics_by_id_uses_metadata_v12() {
        let nil = topic_err(
            17,
            Some(format!(
                "The given topic id '{}' cannot be represented in a request.",
                Uuid::nil()
            )),
        );
        let unknown_beta = topic_err(100, Some(format!("TopicId {BETA} not found.")));
        let unsupported = topic_err(35, None);
        let options = DescribeTopicsOptions {
            include_authorized_operations: true,
            ..DescribeTopicsOptions::default()
        };
        let asked = BTreeSet::from([ALPHA, BETA]);
        for (name, max_metadata, topics, expected_requests) in [
            (
                "found and missing",
                12,
                vec![metadata_topic("alpha", 0, &[0], 1)],
                vec![(12, metadata_by_id_request(&asked, options))],
            ),
            (
                "a topic with an error is unknown",
                13,
                vec![
                    metadata_topic("alpha", 0, &[0], 1),
                    metadata_topic("beta", 29, &[], -1),
                ],
                vec![(13, metadata_by_id_request(&asked, options))],
            ),
            ("a peer below v12", 11, Vec::new(), Vec::new()),
        ] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let handler_requests = Arc::clone(&requests);
            let broker = scripted_broker(
                vec![(metadata_request::API_KEY, 1, max_metadata)],
                move |api_key, version, body, own| {
                    if api_key != metadata_request::API_KEY {
                        return MockReply::Silent;
                    }
                    let request: MetadataRequest = decode_request(body, version, true);
                    handler_requests
                        .lock()
                        .expect("requests lock")
                        .push((version, request));
                    MockReply::Respond(encode_response(
                        &metadata(own, topics.clone()),
                        version,
                        true,
                    ))
                },
            )
            .await;
            let admin = fast_admin(broker.addr, krabka_units::secs(5)).await;
            let result = admin
                .describe_topics_by_id(&[BETA, Uuid::nil(), ALPHA], options)
                .await
                .into_iter()
                .map(|(id, result)| {
                    // A version error names the version ranges in its
                    // message; the code is what the case checks.
                    let result = result.map_err(|error| KafkaError {
                        message: error.message.filter(|_| error.code != 35),
                        ..error
                    });
                    (id, result)
                })
                .collect::<BTreeMap<_, _>>();
            let own = broker.addr;
            broker.stop();
            let expected = if max_metadata < 12 {
                BTreeMap::from([
                    (Uuid::nil(), nil.clone()),
                    (ALPHA, unsupported.clone()),
                    (BETA, unsupported.clone()),
                ])
            } else {
                BTreeMap::from([
                    (Uuid::nil(), nil.clone()),
                    (ALPHA, Ok(from_metadata("alpha", &[0], own, true))),
                    (BETA, unknown_beta.clone()),
                ])
            };
            assert2::assert!(
                (result, requests.lock().expect("requests lock").clone())
                    == (expected, expected_requests),
                "case {name}"
            );
        }
    }

    /// Kafka's `getTopicDescriptionFromDescribeTopicsResponseTopic` reads
    /// the authorized operations only when the caller asked for them.
    #[test]
    fn describe_page_reads_authorized_operations_on_request() {
        for (include, expected) in [
            (false, None),
            (true, Some(BTreeSet::from([AclOperation::Read]))),
        ] {
            let description = description_from_describe(
                &page_topic("alpha", 0, &[]),
                "alpha".to_owned(),
                &BTreeMap::new(),
                include,
            );
            assert2::assert!(
                description.authorized_operations == expected,
                "include {include}"
            );
        }
    }
}
