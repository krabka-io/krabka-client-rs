//! Topic CRUD wrappers.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};

use krabka_protocol::{
    owned::{
        alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
        },
        alter_partition_reassignments_response::AlterPartitionReassignmentsResponse,
        create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        delete_records_request::{
            DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
        },
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        describe_cluster_request::DescribeClusterRequest,
        list_partition_reassignments_request::{
            ListPartitionReassignmentsRequest, ListPartitionReassignmentsTopics,
        },
        list_partition_reassignments_response::ListPartitionReassignmentsResponse,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::MetadataResponse,
    },
    primitives::uuid::Uuid as ProtoUuid,
};
use krabka_units::{Time, convert::TimeExt as _};
use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    AdminClient, AdminError, KafkaError, NOT_CONTROLLER, kafka_error_if,
    retry::{ControllerRetry, KAFKA_ADMIN_RETRY, RetryPolicy, call_timeout_error},
};

#[derive(Debug, Clone)]
pub struct CreateTopicSpec {
    pub name: String,
    pub partitions: i32,
    pub replicas: i32,
    pub configs: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTopicOutcome {
    pub name: String,
    pub topic_id: Option<Uuid>,
    pub error: Option<KafkaError>,
    /// The throttle time of a `THROTTLING_QUOTA_EXCEEDED` (89) error, as
    /// Kafka's `ThrottlingQuotaExceededException.throttleTimeMs` gives it.
    /// `None` for every other outcome.
    pub throttle_time: Option<Time>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteTopicOutcome {
    pub name: String,
    pub error: Option<KafkaError>,
    /// The throttle time of a `THROTTLING_QUOTA_EXCEEDED` (89) error.
    /// `None` for every other outcome.
    pub throttle_time: Option<Time>,
}

/// Options of [`AdminClient::create_topics`], [`AdminClient::delete_topics`]
/// and [`AdminClient::create_partitions`], as Kafka's `CreateTopicsOptions`,
/// `DeleteTopicsOptions` and `CreatePartitionsOptions`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopicMutationOptions {
    /// The deadline of the call, including retries. Each request carries the
    /// time that remains as its `timeout_ms`. `None` uses Kafka's
    /// `default.api.timeout.ms` (60 s).
    pub timeout: Option<Time>,
    /// Retry the topics that fail with `THROTTLING_QUOTA_EXCEEDED` (89) until
    /// the deadline. Kafka's default is `true`.
    pub retry_on_quota_violation: bool,
}

impl Default for TopicMutationOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            retry_on_quota_violation: true,
        }
    }
}

impl TopicMutationOptions {
    /// The default options with a call deadline of `timeout`.
    #[must_use]
    pub fn with_timeout(timeout: Time) -> Self {
        Self {
            timeout: Some(timeout),
            ..Self::default()
        }
    }

    fn retry_policy(self) -> RetryPolicy {
        match self.timeout {
            Some(timeout) => RetryPolicy {
                timeout: Duration::from_millis(u64::try_from(timeout.millis_i64()).unwrap_or(0)),
                ..KAFKA_ADMIN_RETRY
            },
            None => KAFKA_ADMIN_RETRY,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsOp {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsOutcome {
    pub topic: String,
    pub partition: i32,
    pub error_code: i16,
    pub low_watermark: i64,
}

#[derive(Debug, Clone)]
pub struct CreatePartitionsOp {
    pub name: String,
    pub new_total_count: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreatePartitionsOutcome {
    pub name: String,
    pub error: Option<KafkaError>,
    /// The throttle time of a `THROTTLING_QUOTA_EXCEEDED` (89) error.
    /// `None` for every other outcome.
    pub throttle_time: Option<Time>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicMetadata {
    pub controller_id: i32,
    pub topics: Vec<TopicMetadataEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadataEntry {
    pub name: String,
    pub topic_id: Option<Uuid>,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub error: Option<KafkaError>,
}

/// Result of converging one topic's partition assignments to a replication
/// factor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicReplicationStatus {
    /// Every partition already has the requested number of replicas.
    InSync,
    /// Kafka is still completing an earlier reassignment for the topic.
    ReassignmentInProgress,
    /// A reassignment for every out-of-sync partition was accepted.
    ReassignmentSubmitted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAssignment {
    pub topic: String,
    pub partition: i32,
    pub replicas: Vec<i32>,
    pub adding_replicas: Vec<i32>,
    pub removing_replicas: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAssignmentOutcome {
    pub topic: String,
    pub partition: i32,
    pub error: Option<KafkaError>,
}

impl AdminClient {
    /// Returns current replica membership for every selected partition.
    ///
    /// Pass an empty topic slice to inspect the whole cluster before broker
    /// evacuation.
    ///
    /// # Errors
    /// Returns a transport or protocol error.
    pub async fn describe_partition_assignments(
        &mut self,
        topics: &[&str],
    ) -> Result<Vec<PartitionAssignment>, AdminError> {
        let response: MetadataResponse = self.conn.send(build_metadata(topics)).await?;
        Ok(response
            .topics
            .into_iter()
            .flat_map(|topic| {
                let name = topic.name.unwrap_or_default();
                topic
                    .partitions
                    .into_iter()
                    .map(move |partition| PartitionAssignment {
                        topic: name.clone(),
                        partition: partition.partition_index,
                        replicas: partition.replica_nodes,
                        adding_replicas: Vec::new(),
                        removing_replicas: Vec::new(),
                    })
            })
            .collect())
    }

    /// Lists active partition reassignments and their replica membership.
    ///
    /// An empty filter lists every active reassignment.
    ///
    /// # Errors
    /// Returns a transport, protocol, or top-level broker error.
    pub async fn list_partition_reassignments(
        &mut self,
        partitions: &BTreeMap<String, Vec<i32>>,
        timeout: Time,
    ) -> Result<Vec<PartitionAssignment>, AdminError> {
        let response = self
            .conn
            .send(ListPartitionReassignmentsRequest {
                timeout_ms: timeout.millis_i32(),
                topics: (!partitions.is_empty()).then(|| {
                    partitions
                        .iter()
                        .map(|(name, indexes)| ListPartitionReassignmentsTopics {
                            name: name.clone(),
                            partition_indexes: indexes.clone(),
                            ..Default::default()
                        })
                        .collect()
                }),
                ..Default::default()
            })
            .await?;
        broker_error(
            "ListPartitionReassignments",
            response.error_code,
            response.error_message,
        )?;
        Ok(response
            .topics
            .into_iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .into_iter()
                    .map(move |partition| PartitionAssignment {
                        topic: topic.name.clone(),
                        partition: partition.partition_index,
                        replicas: partition.replicas,
                        adding_replicas: partition.adding_replicas,
                        removing_replicas: partition.removing_replicas,
                    })
            })
            .collect())
    }

    /// Submits exact replica lists; `None` cancels an active reassignment.
    ///
    /// # Errors
    /// Returns a transport, protocol, or top-level broker error.
    pub async fn alter_partition_assignments(
        &mut self,
        assignments: &BTreeMap<(String, i32), Option<Vec<i32>>>,
        timeout: Time,
    ) -> Result<Vec<PartitionAssignmentOutcome>, AdminError> {
        let request = build_partition_assignment_request(assignments, timeout);
        let mut retry = ControllerRetry::new("AlterPartitionReassignments", KAFKA_ADMIN_RETRY);
        loop {
            let response = retry.bounded(self.conn.send(request.clone())).await?;
            if response.error_code != NOT_CONTROLLER {
                return parse_partition_assignment_response(response);
            }
            retry.after_not_controller(self).await?;
        }
    }

    /// Metadata for the named topics. Pass an empty slice to fetch all
    /// topics, per Kafka semantics.
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn metadata(&mut self, topics: &[&str]) -> Result<TopicMetadata, AdminError> {
        let req = build_metadata(topics);
        let resp = self.conn.send(req).await?;
        Ok(parse_metadata(resp))
    }

    /// Converges every partition of `topic` to `replication_factor` through
    /// `AlterPartitionReassignments`.
    ///
    /// Existing replicas are retained where possible. Added replicas rotate
    /// across the broker list, and decreases preserve the leading replica
    /// order. An existing reassignment is never overwritten.
    ///
    /// # Errors
    /// Returns an error when the factor is invalid for the live broker set,
    /// metadata cannot be read, Kafka rejects a reassignment, or transport I/O
    /// fails.
    pub async fn reconcile_topic_replication_factor(
        &mut self,
        topic: &str,
        replication_factor: i32,
        timeout: Time,
    ) -> Result<TopicReplicationStatus, AdminError> {
        if self.conn.uses_controller_bootstrap() {
            return Err(AdminError::Broker {
                api: "ControllerEndpoint",
                code: 115,
                name: "UNSUPPORTED_ENDPOINT_TYPE",
                message: Some(
                    "replication-factor reconciliation requires a broker bootstrap endpoint".into(),
                ),
            });
        }
        // Replica selection depends on the controller's authoritative
        // heartbeat registry. Connect to the active controller before reading
        // Metadata so dead-but-still-registered brokers are not candidates.
        self.refresh_controller_connection().await?;
        let mut retry = ControllerRetry::new("AlterPartitionReassignments", KAFKA_ADMIN_RETRY);
        loop {
            match retry
                .bounded(self.reconcile_topic_replication_factor_once(
                    topic,
                    replication_factor,
                    timeout,
                ))
                .await
            {
                Err(AdminError::Broker {
                    code: NOT_CONTROLLER,
                    ..
                }) => retry.after_not_controller(self).await?,
                result => return result,
            }
        }
    }

    async fn reconcile_topic_replication_factor_once(
        &mut self,
        topic: &str,
        replication_factor: i32,
        timeout: Time,
    ) -> Result<TopicReplicationStatus, AdminError> {
        let ongoing: ListPartitionReassignmentsResponse = self
            .conn
            .send(ListPartitionReassignmentsRequest::default())
            .await?;
        broker_error(
            "ListPartitionReassignments",
            ongoing.error_code,
            ongoing.error_message,
        )?;
        if ongoing.topics.iter().any(|entry| entry.name == topic) {
            return Ok(TopicReplicationStatus::ReassignmentInProgress);
        }

        // The public entrypoint reconnects to the active controller before
        // calling this method, so DescribeCluster carries that controller's
        // authoritative heartbeat/fencing state.
        let cluster = self
            .conn
            .send_at_least(
                DescribeClusterRequest {
                    include_fenced_brokers: true,
                    ..Default::default()
                },
                2,
            )
            .await?;
        broker_error("DescribeCluster", cluster.error_code, cluster.error_message)?;
        let eligible_brokers = cluster
            .brokers
            .iter()
            .filter(|broker| !broker.is_fenced)
            .map(|broker| broker.broker_id)
            .collect::<Vec<_>>();

        let metadata: MetadataResponse = self.conn.send(build_metadata(&[topic])).await?;
        let Some(request) = build_replication_factor_reassignment(
            &metadata,
            &eligible_brokers,
            topic,
            replication_factor,
            timeout,
        )?
        else {
            return Ok(TopicReplicationStatus::InSync);
        };
        let response: AlterPartitionReassignmentsResponse = self.conn.send(request).await?;
        broker_error(
            "AlterPartitionReassignments",
            response.error_code,
            response.error_message,
        )?;
        if let Some(error) = response.responses.into_iter().find_map(|topic_response| {
            topic_response
                .partitions
                .into_iter()
                .find(|partition| partition.error_code != 0)
        }) {
            return Err(AdminError::Broker {
                api: "AlterPartitionReassignments",
                code: error.error_code,
                name: crate::kafka_error_name(error.error_code),
                message: error.error_message,
            });
        }
        Ok(TopicReplicationStatus::ReassignmentSubmitted)
    }

    /// Creates topics through the active controller, as Kafka's
    /// `KafkaAdminClient.createTopics` does.
    ///
    /// The call retries until the deadline of `options`:
    ///
    /// - `NOT_CONTROLLER` (41) for any topic: find the controller again, wait
    ///   for the backoff, and send every topic that has no outcome again
    ///   (`handleNotControllerError`). With controller bootstrap,
    ///   `NOT_LEADER_OR_FOLLOWER` (6) does the same.
    /// - `THROTTLING_QUOTA_EXCEEDED` (89): when
    ///   [`TopicMutationOptions::retry_on_quota_violation`] is set, wait for the
    ///   throttle time of the response and send only the throttled topics
    ///   again. A topic that has an outcome is not sent again.
    ///
    /// At the deadline a throttled topic keeps code 89, with the throttle time
    /// that remains. Another topic without an outcome gets
    /// `REQUEST_TIMED_OUT` (7). The outcomes keep the order of `specs`.
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, or transport I/O fails.
    pub async fn create_topics(
        &mut self,
        specs: &[CreateTopicSpec],
        options: TopicMutationOptions,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError> {
        self.create_topics_with_retry(specs, options, options.retry_policy())
            .await
    }

    async fn create_topics_with_retry(
        &mut self,
        specs: &[CreateTopicSpec],
        options: TopicMutationOptions,
        policy: RetryPolicy,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError> {
        let names = specs.iter().map(|spec| spec.name.clone()).collect();
        self.mutate_topics(
            "CreateTopics",
            names,
            options.retry_on_quota_violation,
            policy,
            |pending, timeout_ms| {
                let specs = specs
                    .iter()
                    .filter(|spec| pending.contains(&spec.name))
                    .cloned()
                    .collect::<Vec<_>>();
                build_create_topics(&specs, timeout_ms)
            },
            parse_create_topics,
        )
        .await
    }

    /// Deletes topics through the active controller, as Kafka's
    /// `KafkaAdminClient.deleteTopics` does. The call retries
    /// `NOT_CONTROLLER` (41) and `THROTTLING_QUOTA_EXCEEDED` (89) as
    /// [`AdminClient::create_topics`] does.
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, or transport I/O fails.
    pub async fn delete_topics(
        &mut self,
        names: &[&str],
        options: TopicMutationOptions,
    ) -> Result<Vec<DeleteTopicOutcome>, AdminError> {
        self.delete_topics_with_retry(names, options, options.retry_policy())
            .await
    }

    async fn delete_topics_with_retry(
        &mut self,
        names: &[&str],
        options: TopicMutationOptions,
        policy: RetryPolicy,
    ) -> Result<Vec<DeleteTopicOutcome>, AdminError> {
        let names = names.iter().map(|name| (*name).to_owned()).collect();
        self.mutate_topics(
            "DeleteTopics",
            names,
            options.retry_on_quota_violation,
            policy,
            build_delete_topics,
            parse_delete_topics,
        )
        .await
    }

    /// Adds partitions through the active controller, as Kafka's
    /// `KafkaAdminClient.createPartitions` does. The call retries
    /// `NOT_CONTROLLER` (41) and `THROTTLING_QUOTA_EXCEEDED` (89) as
    /// [`AdminClient::create_topics`] does.
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, or transport I/O fails.
    pub async fn create_partitions(
        &mut self,
        ops: &[CreatePartitionsOp],
        options: TopicMutationOptions,
    ) -> Result<Vec<CreatePartitionsOutcome>, AdminError> {
        self.create_partitions_with_retry(ops, options, options.retry_policy())
            .await
    }

    async fn create_partitions_with_retry(
        &mut self,
        ops: &[CreatePartitionsOp],
        options: TopicMutationOptions,
        policy: RetryPolicy,
    ) -> Result<Vec<CreatePartitionsOutcome>, AdminError> {
        let names = ops.iter().map(|op| op.name.clone()).collect();
        self.mutate_topics(
            "CreatePartitions",
            names,
            options.retry_on_quota_violation,
            policy,
            |pending, timeout_ms| {
                let ops = ops
                    .iter()
                    .filter(|op| pending.contains(&op.name))
                    .cloned()
                    .collect::<Vec<_>>();
                build_create_partitions(&ops, timeout_ms)
            },
            parse_create_partitions,
        )
        .await
    }

    /// Sends one topic mutation until every topic has an outcome or the
    /// deadline passes. See [`AdminClient::create_topics`] for the rules.
    async fn mutate_topics<R, T>(
        &mut self,
        api: &'static str,
        topics: Vec<String>,
        retry_on_quota_violation: bool,
        policy: RetryPolicy,
        build: impl Fn(&[String], i32) -> R,
        parse: impl Fn(R::Response) -> Vec<T>,
    ) -> Result<Vec<T>, AdminError>
    where
        R: krabka_protocol::ProtocolRequest + Clone,
        R::Response: 'static,
        T: TopicMutationOutcome,
    {
        let mut order = Vec::new();
        for topic in topics {
            if !order.contains(&topic) {
                order.push(topic);
            }
        }
        let mut pending = order.clone();
        let mut done = HashMap::<String, T>::new();
        let mut throttled = HashMap::<String, (T, Instant)>::new();
        let mut deadline = policy.start();
        let mut attempts = 0_u32;
        let mut last_error = "none";
        while !pending.is_empty() {
            attempts = attempts.saturating_add(1);
            let request = build(&pending, deadline.remaining_millis());
            // A request still in flight at the deadline stops the call, as
            // Kafka's `KafkaAdminClient` times out a call in flight.
            let Some(response) = deadline.bounded(self.conn.send(request)).await else {
                last_error = "the request was in flight at the deadline";
                break;
            };
            let outcomes = parse(response?);
            let controller_bootstrap = self.conn.uses_controller_bootstrap();
            if outcomes
                .iter()
                .any(|outcome| is_not_controller(outcome.error_code(), controller_bootstrap))
            {
                last_error = "NOT_CONTROLLER";
                if deadline
                    .bounded(self.refresh_controller_after_not_controller())
                    .await
                    .transpose()?
                    .is_none()
                {
                    break;
                }
                if !deadline.expired() {
                    deadline.backoff().await;
                }
                if deadline.expired() {
                    break;
                }
                continue;
            }

            let received = Instant::now();
            let mut retry = Vec::new();
            let mut throttle = Duration::ZERO;
            for outcome in outcomes {
                let topic = outcome.topic().to_owned();
                if !pending.contains(&topic) || done.contains_key(&topic) {
                    tracing::warn!(api, topic, "server response mentioned an unknown topic");
                    continue;
                }
                if retry_on_quota_violation && outcome.error_code() == THROTTLING_QUOTA_EXCEEDED {
                    throttle = throttle.max(outcome.throttle_duration());
                    retry.push(topic.clone());
                    throttled.insert(topic, (outcome, received));
                } else {
                    throttled.remove(&topic);
                    done.insert(topic, outcome);
                }
            }
            for topic in &pending {
                if !done.contains_key(topic) && !retry.contains(topic) {
                    let error = KafkaError {
                        code: UNKNOWN_SERVER_ERROR,
                        name: crate::kafka_error_name(UNKNOWN_SERVER_ERROR),
                        message: Some(format!(
                            "The controller response did not contain a result for topic {topic}"
                        )),
                    };
                    done.insert(topic.clone(), T::failed(topic, error));
                }
            }
            pending = retry;
            if pending.is_empty() {
                break;
            }
            last_error = "THROTTLING_QUOTA_EXCEEDED";
            // Kafka's `NetworkClient` mutes the connection for the throttle
            // time of the response (KIP-219) before the retry goes out. A
            // zero throttle time falls back to the retry backoff, so a broken
            // broker cannot make the call spin.
            if throttle.is_zero() {
                deadline.backoff().await;
            } else {
                deadline.sleep(throttle).await;
            }
            if deadline.expired() {
                break;
            }
        }
        for topic in pending {
            let outcome = match throttled.remove(&topic) {
                Some((outcome, received)) => {
                    let remaining = outcome
                        .throttle_duration()
                        .saturating_sub(received.elapsed());
                    outcome.with_throttle_duration(remaining)
                }
                None => T::failed(&topic, call_timeout_error(api, attempts, last_error)),
            };
            done.insert(topic, outcome);
        }
        Ok(order
            .into_iter()
            .filter_map(|topic| done.remove(&topic))
            .collect())
    }

    /// Deletes records below each requested partition offset.
    ///
    /// `offset == -1` follows Kafka `DeleteRecords` semantics and truncates to
    /// the partition high watermark. The returned `low_watermark` is the
    /// broker's resulting log-start offset for that partition.
    ///
    /// # Errors
    ///
    /// Returns an [`AdminError`] when metadata lookup, leader routing, transport,
    /// or protocol handling fails. Kafka partition-level failures remain in the
    /// returned [`DeleteRecordsOutcome::error_code`].
    pub async fn delete_records(
        &mut self,
        ops: &[DeleteRecordsOp],
        timeout: Time,
    ) -> Result<Vec<DeleteRecordsOutcome>, AdminError> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }

        let mut outcomes = Vec::new();
        let groups = self
            .delete_records_leader_groups(ops, &mut outcomes)
            .await?;
        for (endpoint, leader_ops) in groups {
            self.reconnect(&endpoint).await?;
            let req = build_delete_records(&leader_ops, timeout);
            let resp = self.conn.send(req).await?;
            outcomes.extend(parse_delete_records(resp));
        }
        Ok(outcomes)
    }

    /// Fetches Metadata, finds the controller's `host:port`, and replaces
    /// `self.conn` with a connection to it. The per-method `NOT_CONTROLLER`
    /// retry paths above use it.
    pub(crate) async fn refresh_controller_connection(&mut self) -> Result<(), AdminError> {
        if self.conn.uses_controller_bootstrap() {
            return self.conn.rebootstrap().await;
        }
        let md_resp = self.conn.send(build_metadata(&[])).await?;
        let Some(controller_addr) = controller_endpoint(&md_resp) else {
            // In-process/test brokers can advertise port 0 while the
            // bootstrap address still contains the actual bound port. Reuse
            // that known-good address instead of attempting `host:0`.
            if controller_requires_bootstrap_fallback(&md_resp) {
                return self.conn.rebootstrap().await;
            }
            return Err(AdminError::Protocol(format!(
                "metadata names no endpoint for controller {}",
                md_resp.controller_id
            )));
        };
        self.reconnect(&controller_addr).await
    }

    async fn delete_records_leader_groups(
        &mut self,
        ops: &[DeleteRecordsOp],
        outcomes: &mut Vec<DeleteRecordsOutcome>,
    ) -> Result<BTreeMap<String, Vec<DeleteRecordsOp>>, AdminError> {
        let mut topic_names = ops.iter().map(|op| op.topic.as_str()).collect::<Vec<_>>();
        topic_names.sort_unstable();
        topic_names.dedup();

        let metadata = self.conn.send(build_metadata(&topic_names)).await?;
        let broker_endpoints = metadata
            .brokers
            .iter()
            .map(|broker| {
                let endpoint = if broker.port > 0 {
                    format!("{}:{}", broker.host, broker.port)
                } else {
                    self.bootstrap_addrs
                        .first()
                        .cloned()
                        .unwrap_or_else(|| format!("{}:{}", broker.host, broker.port))
                };
                (broker.node_id, endpoint)
            })
            .collect::<BTreeMap<_, _>>();
        let mut groups = BTreeMap::<String, Vec<DeleteRecordsOp>>::new();

        for op in ops {
            let Some(topic) = metadata
                .topics
                .iter()
                .find(|topic| topic.name.as_deref() == Some(op.topic.as_str()))
            else {
                outcomes.push(delete_records_error_outcome(op, 3));
                continue;
            };

            if topic.error_code != 0 {
                outcomes.push(delete_records_error_outcome(op, topic.error_code));
                continue;
            }

            let Some(partition) = topic
                .partitions
                .iter()
                .find(|partition| partition.partition_index == op.partition)
            else {
                outcomes.push(delete_records_error_outcome(op, 3));
                continue;
            };

            if partition.error_code != 0 {
                outcomes.push(delete_records_error_outcome(op, partition.error_code));
                continue;
            }

            let Some(endpoint) = broker_endpoints.get(&partition.leader_id) else {
                return Err(AdminError::Protocol(format!(
                    "no broker endpoint for DeleteRecords leader {}",
                    partition.leader_id
                )));
            };
            groups.entry(endpoint.clone()).or_default().push(op.clone());
        }

        Ok(groups)
    }
}

/// `UNKNOWN_SERVER_ERROR`.
const UNKNOWN_SERVER_ERROR: i16 = -1;
/// `NOT_LEADER_OR_FOLLOWER`: a controller that is not the active controller
/// answers with it through controller bootstrap.
const NOT_LEADER_OR_FOLLOWER: i16 = 6;
/// `THROTTLING_QUOTA_EXCEEDED`: the controller mutation quota (KIP-599).
const THROTTLING_QUOTA_EXCEEDED: i16 = 89;

/// Whether a topic code makes Kafka's `handleNotControllerError` find the
/// controller again. `NOT_LEADER_OR_FOLLOWER` counts only with controller
/// bootstrap.
fn is_not_controller(code: i16, controller_bootstrap: bool) -> bool {
    code == NOT_CONTROLLER || (controller_bootstrap && code == NOT_LEADER_OR_FOLLOWER)
}

/// One per-topic outcome of a topic mutation.
trait TopicMutationOutcome {
    /// The topic name.
    fn topic(&self) -> &str;
    /// The error code, 0 for success.
    fn error_code(&self) -> i16;
    /// The throttle time of the outcome, zero when it has none.
    fn throttle_duration(&self) -> Duration;
    /// This outcome with the throttle time `remaining`.
    #[must_use]
    fn with_throttle_duration(self, remaining: Duration) -> Self;
    /// A failed outcome for `topic`.
    fn failed(topic: &str, error: KafkaError) -> Self;
}

fn throttle_duration(throttle_time: Option<Time>) -> Duration {
    throttle_time.map_or(Duration::ZERO, |time| {
        Duration::from_millis(u64::try_from(time.millis_i64()).unwrap_or(0))
    })
}

fn throttle_time(remaining: Duration) -> Time {
    Time::from_millis(i64::try_from(remaining.as_millis()).unwrap_or(i64::MAX))
}

/// The throttle time of an outcome with `code`: the response throttle time
/// for `THROTTLING_QUOTA_EXCEEDED`, `None` otherwise.
fn quota_throttle_time(code: i16, throttle_time_ms: i32) -> Option<Time> {
    (code == THROTTLING_QUOTA_EXCEEDED).then(|| Time::from_millis(i64::from(throttle_time_ms)))
}

impl TopicMutationOutcome for CreateTopicOutcome {
    fn topic(&self) -> &str {
        &self.name
    }
    fn error_code(&self) -> i16 {
        self.error.as_ref().map_or(0, |error| error.code)
    }
    fn throttle_duration(&self) -> Duration {
        throttle_duration(self.throttle_time)
    }
    fn with_throttle_duration(self, remaining: Duration) -> Self {
        Self {
            throttle_time: Some(throttle_time(remaining)),
            ..self
        }
    }
    fn failed(topic: &str, error: KafkaError) -> Self {
        Self {
            name: topic.to_owned(),
            topic_id: None,
            error: Some(error),
            throttle_time: None,
        }
    }
}

impl TopicMutationOutcome for DeleteTopicOutcome {
    fn topic(&self) -> &str {
        &self.name
    }
    fn error_code(&self) -> i16 {
        self.error.as_ref().map_or(0, |error| error.code)
    }
    fn throttle_duration(&self) -> Duration {
        throttle_duration(self.throttle_time)
    }
    fn with_throttle_duration(self, remaining: Duration) -> Self {
        Self {
            throttle_time: Some(throttle_time(remaining)),
            ..self
        }
    }
    fn failed(topic: &str, error: KafkaError) -> Self {
        Self {
            name: topic.to_owned(),
            error: Some(error),
            throttle_time: None,
        }
    }
}

impl TopicMutationOutcome for CreatePartitionsOutcome {
    fn topic(&self) -> &str {
        &self.name
    }
    fn error_code(&self) -> i16 {
        self.error.as_ref().map_or(0, |error| error.code)
    }
    fn throttle_duration(&self) -> Duration {
        throttle_duration(self.throttle_time)
    }
    fn with_throttle_duration(self, remaining: Duration) -> Self {
        Self {
            throttle_time: Some(throttle_time(remaining)),
            ..self
        }
    }
    fn failed(topic: &str, error: KafkaError) -> Self {
        Self {
            name: topic.to_owned(),
            error: Some(error),
            throttle_time: None,
        }
    }
}

fn build_partition_assignment_request(
    assignments: &BTreeMap<(String, i32), Option<Vec<i32>>>,
    timeout: Time,
) -> AlterPartitionReassignmentsRequest {
    let mut topics = BTreeMap::<String, Vec<ReassignablePartition>>::new();
    for ((topic, partition), replicas) in assignments {
        topics
            .entry(topic.clone())
            .or_default()
            .push(ReassignablePartition {
                partition_index: *partition,
                replicas: replicas.clone(),
                ..Default::default()
            });
    }
    AlterPartitionReassignmentsRequest {
        timeout_ms: timeout.millis_i32(),
        allow_replication_factor_change: true,
        topics: topics
            .into_iter()
            .map(|(name, partitions)| ReassignableTopic {
                name,
                partitions,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn parse_partition_assignment_response(
    response: AlterPartitionReassignmentsResponse,
) -> Result<Vec<PartitionAssignmentOutcome>, AdminError> {
    broker_error(
        "AlterPartitionReassignments",
        response.error_code,
        response.error_message,
    )?;
    Ok(response
        .responses
        .into_iter()
        .flat_map(|topic| {
            topic
                .partitions
                .into_iter()
                .map(move |partition| PartitionAssignmentOutcome {
                    topic: topic.name.clone(),
                    partition: partition.partition_index,
                    error: kafka_error_if(partition.error_code, partition.error_message),
                })
        })
        .collect())
}

fn build_metadata(topics: &[&str]) -> MetadataRequest {
    MetadataRequest {
        topics: if topics.is_empty() {
            None
        } else {
            Some(
                topics
                    .iter()
                    .map(|n| MetadataRequestTopic {
                        topic_id: ProtoUuid::ZERO,
                        name: Some((*n).to_string()),
                        ..Default::default()
                    })
                    .collect(),
            )
        },
        allow_auto_topic_creation: false,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
        ..Default::default()
    }
}

fn broker_error(api: &'static str, code: i16, message: Option<String>) -> Result<(), AdminError> {
    if code == 0 {
        return Ok(());
    }
    Err(AdminError::Broker {
        api,
        code,
        name: crate::kafka_error_name(code),
        message,
    })
}

fn build_replication_factor_reassignment(
    metadata: &MetadataResponse,
    eligible_brokers: &[i32],
    topic_name: &str,
    replication_factor: i32,
    timeout: Time,
) -> Result<Option<AlterPartitionReassignmentsRequest>, AdminError> {
    broker_error("Metadata", metadata.error_code, None)?;
    let desired = usize::try_from(replication_factor).map_err(|_| {
        AdminError::Protocol("replication factor must be a positive integer".into())
    })?;
    if desired == 0 {
        return Err(AdminError::Protocol(
            "replication factor must be a positive integer".into(),
        ));
    }

    let mut brokers = eligible_brokers.to_vec();
    brokers.sort_unstable();
    brokers.dedup();
    if desired > brokers.len() {
        return Err(AdminError::Broker {
            api: "AlterPartitionReassignments",
            code: 38,
            name: crate::kafka_error_name(38),
            message: Some(format!(
                "replication factor {replication_factor} exceeds live broker count {}",
                brokers.len()
            )),
        });
    }

    let topic = metadata
        .topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some(topic_name))
        .ok_or_else(|| AdminError::Broker {
            api: "Metadata",
            code: 3,
            name: crate::kafka_error_name(3),
            message: Some(format!("topic {topic_name:?} is absent from metadata")),
        })?;
    broker_error("Metadata", topic.error_code, None)?;

    let available = brokers.iter().copied().collect::<HashSet<_>>();
    let mut partitions = Vec::new();
    for (rotation, partition) in topic.partitions.iter().enumerate() {
        broker_error("Metadata", partition.error_code, None)?;
        let mut selected = HashSet::with_capacity(desired);
        let mut target = partition
            .replica_nodes
            .iter()
            .copied()
            .filter(|broker| available.contains(broker) && selected.insert(*broker))
            .take(desired)
            .collect::<Vec<_>>();
        for offset in 0..brokers.len() {
            if target.len() == desired {
                break;
            }
            let broker = brokers[(rotation + offset) % brokers.len()];
            if selected.insert(broker) {
                target.push(broker);
            }
        }
        if target != partition.replica_nodes {
            partitions.push(ReassignablePartition {
                partition_index: partition.partition_index,
                replicas: Some(target),
                ..Default::default()
            });
        }
    }
    if partitions.is_empty() {
        return Ok(None);
    }

    Ok(Some(AlterPartitionReassignmentsRequest {
        timeout_ms: timeout.millis_i32(),
        allow_replication_factor_change: true,
        topics: vec![ReassignableTopic {
            name: topic_name.to_string(),
            partitions,
            ..Default::default()
        }],
        ..Default::default()
    }))
}

fn build_create_topics(specs: &[CreateTopicSpec], timeout_ms: i32) -> CreateTopicsRequest {
    CreateTopicsRequest {
        topics: specs
            .iter()
            .map(|s| CreatableTopic {
                name: s.name.clone(),
                num_partitions: s.partitions,
                replication_factor: i16::try_from(s.replicas).unwrap_or(i16::MAX),
                assignments: Vec::new(),
                configs: s
                    .configs
                    .iter()
                    .map(|(k, v)| CreatableTopicConfig {
                        name: k.clone(),
                        value: Some(v.clone()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        timeout_ms,
        validate_only: false,
        ..Default::default()
    }
}

/// A `DeleteTopicsRequest` that names `names`. It fills both the v0-v5
/// `topic_names` field and the v6+ `topics` field, and the encoder uses the
/// field of the negotiated version.
fn build_delete_topics(names: &[String], timeout_ms: i32) -> DeleteTopicsRequest {
    DeleteTopicsRequest {
        topic_names: names.to_vec(),
        topics: names
            .iter()
            .map(|name| DeleteTopicState {
                name: Some(name.clone()),
                topic_id: ProtoUuid::ZERO,
                ..Default::default()
            })
            .collect(),
        timeout_ms,
        ..Default::default()
    }
}

fn build_create_partitions(ops: &[CreatePartitionsOp], timeout_ms: i32) -> CreatePartitionsRequest {
    CreatePartitionsRequest {
        topics: ops
            .iter()
            .map(|op| CreatePartitionsTopic {
                name: op.name.clone(),
                count: op.new_total_count,
                assignments: None,
                ..Default::default()
            })
            .collect(),
        timeout_ms,
        validate_only: false,
        ..Default::default()
    }
}

fn build_delete_records(ops: &[DeleteRecordsOp], timeout: Time) -> DeleteRecordsRequest {
    let mut topics = BTreeMap::<String, Vec<DeleteRecordsPartition>>::new();
    for op in ops {
        topics
            .entry(op.topic.clone())
            .or_default()
            .push(DeleteRecordsPartition {
                partition_index: op.partition,
                offset: op.offset,
                ..Default::default()
            });
    }

    DeleteRecordsRequest {
        topics: topics
            .into_iter()
            .map(|(name, partitions)| DeleteRecordsTopic {
                name,
                partitions,
                ..Default::default()
            })
            .collect(),
        timeout_ms: timeout.millis_i32(),
        ..Default::default()
    }
}

fn parse_create_topics(
    resp: <CreateTopicsRequest as krabka_protocol::ProtocolRequest>::Response,
) -> Vec<CreateTopicOutcome> {
    let throttle_time_ms = resp.throttle_time_ms;
    resp.topics
        .into_iter()
        .map(|t| CreateTopicOutcome {
            name: t.name,
            topic_id: proto_uuid_to_opt(t.topic_id),
            throttle_time: quota_throttle_time(t.error_code, throttle_time_ms),
            error: kafka_error_if(t.error_code, t.error_message),
        })
        .collect()
}

fn parse_delete_topics(
    resp: <DeleteTopicsRequest as krabka_protocol::ProtocolRequest>::Response,
) -> Vec<DeleteTopicOutcome> {
    let throttle_time_ms = resp.throttle_time_ms;
    resp.responses
        .into_iter()
        .map(|t| DeleteTopicOutcome {
            name: t.name.unwrap_or_default(),
            throttle_time: quota_throttle_time(t.error_code, throttle_time_ms),
            error: kafka_error_if(t.error_code, t.error_message),
        })
        .collect()
}

fn parse_create_partitions(
    resp: <CreatePartitionsRequest as krabka_protocol::ProtocolRequest>::Response,
) -> Vec<CreatePartitionsOutcome> {
    let throttle_time_ms = resp.throttle_time_ms;
    resp.results
        .into_iter()
        .map(|t| CreatePartitionsOutcome {
            name: t.name,
            throttle_time: quota_throttle_time(t.error_code, throttle_time_ms),
            error: kafka_error_if(t.error_code, t.error_message),
        })
        .collect()
}

fn parse_delete_records(
    resp: <DeleteRecordsRequest as krabka_protocol::ProtocolRequest>::Response,
) -> Vec<DeleteRecordsOutcome> {
    resp.topics
        .into_iter()
        .flat_map(|topic| {
            let topic_name = topic.name;
            topic
                .partitions
                .into_iter()
                .map(move |partition| DeleteRecordsOutcome {
                    topic: topic_name.clone(),
                    partition: partition.partition_index,
                    error_code: partition.error_code,
                    low_watermark: partition.low_watermark,
                })
        })
        .collect()
}

fn delete_records_error_outcome(op: &DeleteRecordsOp, error_code: i16) -> DeleteRecordsOutcome {
    DeleteRecordsOutcome {
        topic: op.topic.clone(),
        partition: op.partition,
        error_code,
        low_watermark: -1,
    }
}

fn parse_metadata(
    resp: <MetadataRequest as krabka_protocol::ProtocolRequest>::Response,
) -> TopicMetadata {
    let topics = resp
        .topics
        .into_iter()
        .map(|t| {
            let partition_count = i32::try_from(t.partitions.len()).unwrap_or(i32::MAX);
            let replication_factor = i32::from(t.partitions.first().map_or(0, |p| {
                i16::try_from(p.replica_nodes.len()).unwrap_or(i16::MAX)
            }));
            TopicMetadataEntry {
                name: t.name.unwrap_or_default(),
                topic_id: proto_uuid_to_opt(t.topic_id),
                partition_count,
                replication_factor,
                error: kafka_error_if(t.error_code, None),
            }
        })
        .collect();
    TopicMetadata {
        controller_id: resp.controller_id,
        topics,
    }
}

fn controller_endpoint(
    resp: &<MetadataRequest as krabka_protocol::ProtocolRequest>::Response,
) -> Option<String> {
    let id = resp.controller_id;
    resp.brokers
        .iter()
        .find(|b| b.node_id == id && !b.host.is_empty() && b.port > 0)
        .map(|b| format!("{}:{}", b.host, b.port))
}

fn controller_requires_bootstrap_fallback(
    resp: &<MetadataRequest as krabka_protocol::ProtocolRequest>::Response,
) -> bool {
    resp.brokers
        .iter()
        .any(|broker| broker.node_id == resp.controller_id && broker.port <= 0)
}

fn proto_uuid_to_opt(u: ProtoUuid) -> Option<Uuid> {
    if u == ProtoUuid::ZERO {
        None
    } else {
        Some(Uuid::from_bytes(u.0))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use krabka_protocol::{
        UnknownTaggedFields,
        owned::metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
    };

    use super::*;

    #[test]
    fn exact_assignment_request_preserves_replica_order() {
        let assignments = BTreeMap::from([(("orders".into(), 3), Some(vec![4, 2, 1]))]);
        let request = build_partition_assignment_request(&assignments, krabka_units::secs(30));
        assert2::assert!(
            request
                == AlterPartitionReassignmentsRequest {
                    timeout_ms: 30_000,
                    allow_replication_factor_change: true,
                    topics: vec![ReassignableTopic {
                        name: "orders".into(),
                        partitions: vec![ReassignablePartition {
                            partition_index: 3,
                            replicas: Some(vec![4, 2, 1]),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }
        );
    }

    fn reassignment_metadata(assignments: &[&[i32]]) -> MetadataResponse {
        MetadataResponse {
            brokers: (1..=3)
                .map(|node_id| MetadataResponseBroker {
                    node_id,
                    ..Default::default()
                })
                .collect(),
            topics: vec![MetadataResponseTopic {
                name: Some("orders".into()),
                partitions: assignments
                    .iter()
                    .enumerate()
                    .map(|(partition_index, replicas)| MetadataResponsePartition {
                        partition_index: i32::try_from(partition_index)
                            .expect("test partition index fits i32"),
                        replica_nodes: replicas.to_vec(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn replication_factor_reassignment_preserves_and_rotates_replicas() {
        let cases = [
            (
                "increase",
                reassignment_metadata(&[&[1], &[2]]),
                2,
                vec![(0, vec![1, 2]), (1, vec![2, 3])],
            ),
            (
                "decrease",
                reassignment_metadata(&[&[1, 2, 3], &[2, 3, 1]]),
                2,
                vec![(0, vec![1, 2]), (1, vec![2, 3])],
            ),
        ];

        for (case, metadata, replication_factor, expected_partitions) in cases {
            let actual = build_replication_factor_reassignment(
                &metadata,
                &[1, 2, 3],
                "orders",
                replication_factor,
                krabka_units::secs(5),
            )
            .unwrap();
            let expected = Some(AlterPartitionReassignmentsRequest {
                timeout_ms: 5_000,
                allow_replication_factor_change: true,
                topics: vec![ReassignableTopic {
                    name: "orders".into(),
                    partitions: expected_partitions
                        .into_iter()
                        .map(|(partition_index, replicas)| ReassignablePartition {
                            partition_index,
                            replicas: Some(replicas),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            });
            assert2::assert!(actual == expected, "case {case}");
        }
    }

    #[test]
    fn replication_factor_reassignment_rejects_factor_above_broker_count() {
        let error = build_replication_factor_reassignment(
            &reassignment_metadata(&[&[1]]),
            &[1, 2, 3],
            "orders",
            4,
            krabka_units::secs(5),
        )
        .unwrap_err();

        assert2::assert!(matches!(
            error,
            AdminError::Broker {
                api: "AlterPartitionReassignments",
                code: 38,
                name: "INVALID_REPLICATION_FACTOR",
                ..
            }
        ));
    }

    #[test]
    fn replication_factor_reassignment_replaces_fenced_replica() {
        let actual = build_replication_factor_reassignment(
            &reassignment_metadata(&[&[1, 2]]),
            &[1, 3],
            "orders",
            2,
            krabka_units::secs(5),
        )
        .unwrap()
        .expect("fenced replica requires reassignment");

        assert2::assert!(actual.topics[0].partitions[0].replicas == Some(vec![1, 3]));
    }

    #[test]
    fn build_create_topics_one_spec() {
        let req = build_create_topics(
            &[CreateTopicSpec {
                name: "foo".into(),
                partitions: 3,
                replicas: 1,
                configs: BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]),
            }],
            5_000,
        );
        assert2::assert!(
            req == CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: "foo".to_string(),
                    num_partitions: 3,
                    replication_factor: 1,
                    assignments: vec![],
                    configs: vec![CreatableTopicConfig {
                        name: "retention.ms".to_string(),
                        value: Some("60000".to_string()),
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                timeout_ms: 5_000,
                validate_only: false,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn build_delete_records_groups_partitions_by_topic() {
        let req = build_delete_records(
            &[
                DeleteRecordsOp {
                    topic: "beta".to_string(),
                    partition: 1,
                    offset: -1,
                },
                DeleteRecordsOp {
                    topic: "alpha".to_string(),
                    partition: 0,
                    offset: 50,
                },
                DeleteRecordsOp {
                    topic: "alpha".to_string(),
                    partition: 2,
                    offset: 75,
                },
            ],
            krabka_units::secs(5),
        );

        assert2::assert!(
            req == DeleteRecordsRequest {
                topics: vec![
                    DeleteRecordsTopic {
                        name: "alpha".to_string(),
                        partitions: vec![
                            DeleteRecordsPartition {
                                partition_index: 0,
                                offset: 50,
                                unknown_tagged_fields: UnknownTaggedFields(vec![]),
                            },
                            DeleteRecordsPartition {
                                partition_index: 2,
                                offset: 75,
                                unknown_tagged_fields: UnknownTaggedFields(vec![]),
                            },
                        ],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                    DeleteRecordsTopic {
                        name: "beta".to_string(),
                        partitions: vec![DeleteRecordsPartition {
                            partition_index: 1,
                            offset: -1,
                            unknown_tagged_fields: UnknownTaggedFields(vec![]),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                ],
                timeout_ms: 5_000,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn parse_delete_records_flattens_partition_errors() {
        use krabka_protocol::owned::delete_records_response::{
            DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
        };

        let resp = DeleteRecordsResponse {
            topics: vec![DeleteRecordsTopicResult {
                name: "wal".to_string(),
                partitions: vec![
                    DeleteRecordsPartitionResult {
                        partition_index: 0,
                        low_watermark: 50,
                        error_code: 0,
                        ..Default::default()
                    },
                    DeleteRecordsPartitionResult {
                        partition_index: 1,
                        low_watermark: -1,
                        error_code: 1,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        let outcomes = parse_delete_records(resp);
        assert_eq!(
            outcomes,
            vec![
                DeleteRecordsOutcome {
                    topic: "wal".to_string(),
                    partition: 0,
                    error_code: 0,
                    low_watermark: 50,
                },
                DeleteRecordsOutcome {
                    topic: "wal".to_string(),
                    partition: 1,
                    error_code: 1,
                    low_watermark: -1,
                },
            ]
        );
    }

    // ── NOT_CONTROLLER retry predicate ─────────────────────────────
    //
    // The full retry pipeline (first response carries NOT_CONTROLLER →
    // refresh controller endpoint → reconnect → second response succeeds)
    // is exercised against a real broker in `tests/round_trip.rs`. The
    // unit tests below lock the two pure pieces — the predicate that
    // decides whether to retry, and the metadata-response → host:port
    // resolver — so a refactor can't silently flip either one.

    /// Kafka's `handleNotControllerError` retries `NOT_CONTROLLER` (41),
    /// and `NOT_LEADER_OR_FOLLOWER` (6) only with bootstrap controllers.
    #[test]
    fn not_controller_codes_match_kafka() {
        let cases = [
            ((NOT_CONTROLLER, false), true),
            ((NOT_CONTROLLER, true), true),
            ((NOT_LEADER_OR_FOLLOWER, false), false),
            ((NOT_LEADER_OR_FOLLOWER, true), true),
            ((36, true), false),
            ((THROTTLING_QUOTA_EXCEEDED, false), false),
            ((0, false), false),
        ];
        let actual = cases
            .map(|((code, bootstrap), _)| ((code, bootstrap), is_not_controller(code, bootstrap)));
        assert2::assert!(actual == cases);
    }

    // ── controller_endpoint resolver ───────────────────────────────

    /// Spec test name: `connect_walks_bootstrap_list`, the resolver half. The
    /// bootstrap-walking integration coverage itself lives in
    /// `tests/connect.rs`. `controller_endpoint` extracts the `host:port` of
    /// the broker whose `node_id` matches the metadata response's
    /// `controller_id`. This is the address the `NOT_CONTROLLER` retry path
    /// reconnects to.
    #[test]
    fn controller_endpoint_picks_broker_with_matching_node_id() {
        use krabka_protocol::owned::metadata_response::{MetadataResponse, MetadataResponseBroker};
        let resp = MetadataResponse {
            controller_id: 2,
            brokers: vec![
                MetadataResponseBroker {
                    node_id: 1,
                    host: "h1".into(),
                    port: 9092,
                    rack: None,
                    ..Default::default()
                },
                MetadataResponseBroker {
                    node_id: 2,
                    host: "h2".into(),
                    port: 9093,
                    rack: None,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let addr = controller_endpoint(&resp);
        assert2::assert!(addr.as_deref() == Some("h2:9093"));
    }

    /// When the controller id does not appear in the broker list, for example
    /// when the cluster is mid-failover, `controller_endpoint` returns `None`.
    /// The retry path maps that to `AdminError::NotControllerExhausted`.
    #[test]
    fn controller_endpoint_returns_none_when_no_match() {
        use krabka_protocol::owned::metadata_response::{MetadataResponse, MetadataResponseBroker};
        let resp = MetadataResponse {
            controller_id: 99,
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "h1".into(),
                port: 9092,
                rack: None,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert2::assert!(controller_endpoint(&resp).is_none());
    }

    #[test]
    fn controller_endpoint_rejects_non_dialable_ephemeral_port() {
        use krabka_protocol::owned::metadata_response::{MetadataResponse, MetadataResponseBroker};
        let resp = MetadataResponse {
            controller_id: 1,
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: 0,
                ..Default::default()
            }],
            ..Default::default()
        };

        assert2::assert!(controller_endpoint(&resp).is_none());
        assert2::assert!(controller_requires_bootstrap_fallback(&resp));
    }

    // ── parse_metadata ─────────────────────────────────────────────────
    //
    // `parse_metadata` is the pure response→`TopicMetadata` transformer
    // the live `metadata` RPC delegates to. The tests below feed it
    // synthetic responses and assert the per-topic fields are projected
    // correctly. Covers the error-mapping, uuid-zeroing, and
    // partition/replication-factor count paths.

    #[test]
    fn parse_metadata_carries_through_per_topic_errors() {
        use krabka_protocol::owned::metadata_response::{MetadataResponse, MetadataResponseTopic};
        let resp = MetadataResponse {
            topics: vec![
                MetadataResponseTopic {
                    name: Some("ok-topic".into()),
                    error_code: 0,
                    ..Default::default()
                },
                MetadataResponseTopic {
                    name: Some("missing".into()),
                    error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let md = parse_metadata(resp);
        assert2::assert!(
            md == TopicMetadata {
                controller_id: -1,
                topics: vec![
                    TopicMetadataEntry {
                        name: "ok-topic".to_string(),
                        topic_id: None,
                        partition_count: 0,
                        replication_factor: 0,
                        error: None,
                    },
                    TopicMetadataEntry {
                        name: "missing".to_string(),
                        topic_id: None,
                        partition_count: 0,
                        replication_factor: 0,
                        error: Some(KafkaError {
                            code: 3,
                            name: "UNKNOWN_TOPIC_OR_PARTITION",
                            message: None,
                        }),
                    },
                ],
            }
        );
    }

    #[test]
    fn parse_metadata_zero_uuid_becomes_none() {
        use krabka_protocol::owned::metadata_response::{MetadataResponse, MetadataResponseTopic};
        let resp = MetadataResponse {
            topics: vec![MetadataResponseTopic {
                name: Some("foo".into()),
                topic_id: ProtoUuid::ZERO,
                ..Default::default()
            }],
            ..Default::default()
        };
        let md = parse_metadata(resp);
        assert2::assert!(md.topics[0].topic_id.is_none());
    }

    #[test]
    fn parse_metadata_computes_partition_count_and_replication_factor() {
        use krabka_protocol::owned::metadata_response::{
            MetadataResponse, MetadataResponsePartition, MetadataResponseTopic,
        };
        let part = MetadataResponsePartition {
            replica_nodes: vec![1, 2],
            ..Default::default()
        };
        let resp = MetadataResponse {
            topics: vec![MetadataResponseTopic {
                name: Some("foo".into()),
                partitions: vec![part.clone(), part.clone(), part],
                ..Default::default()
            }],
            ..Default::default()
        };
        let md = parse_metadata(resp);
        assert2::assert!(
            (
                md.topics[0].partition_count,
                md.topics[0].replication_factor
            ) == (3, 2)
        );
    }

    // ── parse_create_topics ────────────────────────────────────────────

    #[test]
    fn parse_create_topics_per_topic_error() {
        use krabka_protocol::owned::create_topics_response::{
            CreatableTopicResult, CreateTopicsResponse,
        };
        let resp = CreateTopicsResponse {
            topics: vec![
                CreatableTopicResult {
                    name: "ok".into(),
                    topic_id: ProtoUuid([7; 16]),
                    error_code: 0,
                    error_message: None,
                    ..Default::default()
                },
                CreatableTopicResult {
                    name: "dup".into(),
                    error_code: 36, // TOPIC_ALREADY_EXISTS
                    error_message: Some("already there".into()),
                    ..Default::default()
                },
                CreatableTopicResult {
                    name: "throttled".into(),
                    error_code: THROTTLING_QUOTA_EXCEEDED,
                    ..Default::default()
                },
            ],
            throttle_time_ms: 250,
            ..Default::default()
        };
        let outcomes = parse_create_topics(resp);
        assert2::assert!(
            outcomes
                == vec![
                    CreateTopicOutcome {
                        name: "ok".to_string(),
                        // Non-zero uuid maps to Some.
                        topic_id: Some(Uuid::from_bytes([7; 16])),
                        error: None,
                        throttle_time: None,
                    },
                    CreateTopicOutcome {
                        name: "dup".to_string(),
                        topic_id: None,
                        error: Some(KafkaError {
                            code: 36,
                            name: "TOPIC_ALREADY_EXISTS",
                            message: Some("already there".to_string()),
                        }),
                        throttle_time: None,
                    },
                    CreateTopicOutcome {
                        name: "throttled".to_string(),
                        topic_id: None,
                        error: Some(KafkaError {
                            code: THROTTLING_QUOTA_EXCEEDED,
                            name: "THROTTLING_QUOTA_EXCEEDED",
                            message: None,
                        }),
                        throttle_time: Some(Time::from_millis(250)),
                    },
                ]
        );
    }

    // ── parse_delete_topics ────────────────────────────────────────────

    #[test]
    fn parse_delete_topics_handles_missing_name() {
        use krabka_protocol::owned::delete_topics_response::{
            DeletableTopicResult, DeleteTopicsResponse,
        };
        let resp = DeleteTopicsResponse {
            responses: vec![
                DeletableTopicResult {
                    name: None,
                    error_code: 0,
                    ..Default::default()
                },
                DeletableTopicResult {
                    name: Some("named".into()),
                    error_code: 3,
                    error_message: Some("nope".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let outs = parse_delete_topics(resp);
        assert2::assert!(
            outs == vec![
                DeleteTopicOutcome {
                    // `name: None` falls through to `unwrap_or_default()`
                    // → empty string.
                    name: String::new(),
                    error: None,
                    throttle_time: None,
                },
                DeleteTopicOutcome {
                    name: "named".to_string(),
                    error: Some(KafkaError {
                        code: 3,
                        name: "UNKNOWN_TOPIC_OR_PARTITION",
                        message: Some("nope".to_string()),
                    }),
                    throttle_time: None,
                },
            ]
        );
    }

    // ── parse_create_partitions ────────────────────────────────────────

    #[test]
    fn parse_create_partitions_per_topic_error() {
        use krabka_protocol::owned::create_partitions_response::{
            CreatePartitionsResponse, CreatePartitionsTopicResult,
        };
        let resp = CreatePartitionsResponse {
            results: vec![
                CreatePartitionsTopicResult {
                    name: "ok".into(),
                    error_code: 0,
                    error_message: None,
                    ..Default::default()
                },
                CreatePartitionsTopicResult {
                    name: "bad".into(),
                    error_code: 37,
                    error_message: Some("bad count".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let outs = parse_create_partitions(resp);
        assert2::assert!(
            outs == vec![
                CreatePartitionsOutcome {
                    name: "ok".to_string(),
                    error: None,
                    throttle_time: None,
                },
                CreatePartitionsOutcome {
                    name: "bad".to_string(),
                    error: Some(KafkaError {
                        code: 37,
                        name: "INVALID_PARTITIONS",
                        message: Some("bad count".to_string()),
                    }),
                    throttle_time: None,
                },
            ]
        );
    }

    // ── topic mutation retries against a scripted controller ──────────

    use std::sync::{Arc, Mutex};

    use bytes::{Buf, BytesMut};
    use krabka_client_core::MockBroker;
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            create_partitions_request,
            create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
            create_topics_request,
            create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
            delete_topics_request,
            delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
            metadata_request,
        },
    };

    /// The topic mutation call of a retry case.
    #[derive(Clone, Copy, Debug)]
    enum Mutation {
        CreateTopics,
        DeleteTopics,
        CreatePartitions,
    }

    /// One scripted controller answer: the code of each topic that it names,
    /// and the response throttle time. A topic that is not in the list gets
    /// no result.
    #[derive(Clone, Debug)]
    struct Answer {
        codes: Vec<(&'static str, i16)>,
        throttle_time_ms: i32,
        /// The controller does not answer the request.
        silent: bool,
    }

    fn answer(codes: &[(&'static str, i16)]) -> Answer {
        Answer {
            codes: codes.to_vec(),
            throttle_time_ms: 0,
            silent: false,
        }
    }

    fn throttled(codes: &[(&'static str, i16)], throttle_time_ms: i32) -> Answer {
        Answer {
            codes: codes.to_vec(),
            throttle_time_ms,
            silent: false,
        }
    }

    fn silent() -> Answer {
        Answer {
            codes: Vec::new(),
            throttle_time_ms: 0,
            silent: true,
        }
    }

    /// The topic names and the `timeout_ms` of one request.
    type SentRequest = (Vec<String>, i32);

    /// One topic mutation retry case: name, controller script, deadline,
    /// options, expected requests, expected outcomes.
    type MutationCase = (
        &'static str,
        Vec<Answer>,
        Duration,
        TopicMutationOptions,
        Vec<Vec<String>>,
        Vec<Observed>,
    );

    /// An outcome in a form that tests compare: the topic, the code, and
    /// whether the outcome has a throttle time.
    type Observed = (String, i16, bool);

    fn body(response: &impl Encode, version: i16, flexible: bool) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        if flexible {
            bytes.extend_from_slice(&[0]);
        }
        response
            .encode(&mut bytes, version)
            .expect("response encodes");
        bytes.to_vec()
    }

    fn decode_body<R: for<'de> Decode<'de>>(mut body: &[u8], version: i16, flexible: bool) -> R {
        let client_id_len = body.get_i16();
        body.advance(usize::try_from(client_id_len).expect("client id length"));
        if flexible {
            body.advance(1);
        }
        R::decode(&mut body, version).expect("request decodes")
    }

    /// A broker that is its own controller. It answers each topic mutation
    /// with the next entry of `script` (the last entry repeats), and records
    /// the topic names and the `timeout_ms` of each request.
    async fn scripted_controller(
        script: Vec<Answer>,
        requests: Arc<Mutex<Vec<SentRequest>>>,
    ) -> MockBroker {
        let port = Arc::new(std::sync::atomic::AtomicU16::new(0));
        let handler_port = Arc::clone(&port);
        let mut next = 0_usize;
        let broker = MockBroker::start(move |api_key, version, _, request| {
            let api_version = |api_key, max_version| ApiVersion {
                api_key,
                min_version: 0,
                max_version,
                ..Default::default()
            };
            let mut take = |names: Vec<String>, timeout_ms: i32| {
                requests
                    .lock()
                    .expect("requests lock")
                    .push((names, timeout_ms));
                let entry = script[next.min(script.len() - 1)].clone();
                next += 1;
                entry
            };
            match api_key {
                api_versions_request::API_KEY => Some(body(
                    &ApiVersionsResponse {
                        api_keys: vec![
                            api_version(api_versions_request::API_KEY, 0),
                            api_version(metadata_request::API_KEY, 12),
                            api_version(create_topics_request::API_KEY, 7),
                            api_version(delete_topics_request::API_KEY, 6),
                            api_version(create_partitions_request::API_KEY, 3),
                        ],
                        ..Default::default()
                    },
                    0,
                    false,
                )),
                metadata_request::API_KEY => Some(body(
                    &MetadataResponse {
                        controller_id: 1,
                        brokers: vec![MetadataResponseBroker {
                            node_id: 1,
                            host: "127.0.0.1".into(),
                            port: i32::from(handler_port.load(std::sync::atomic::Ordering::SeqCst)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    version,
                    version >= metadata_request::FLEXIBLE_MIN,
                )),
                create_topics_request::API_KEY => {
                    let flexible = version >= create_topics_request::FLEXIBLE_MIN;
                    let request: CreateTopicsRequest = decode_body(request, version, flexible);
                    let entry = take(
                        request
                            .topics
                            .iter()
                            .map(|topic| topic.name.clone())
                            .collect(),
                        request.timeout_ms,
                    );
                    if entry.silent {
                        return None;
                    }
                    Some(body(
                        &CreateTopicsResponse {
                            throttle_time_ms: entry.throttle_time_ms,
                            topics: entry
                                .codes
                                .iter()
                                .map(|(name, code)| CreatableTopicResult {
                                    name: (*name).to_owned(),
                                    error_code: *code,
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        },
                        version,
                        flexible,
                    ))
                }
                delete_topics_request::API_KEY => {
                    let flexible = version >= delete_topics_request::FLEXIBLE_MIN;
                    let request: DeleteTopicsRequest = decode_body(request, version, flexible);
                    let entry = take(
                        request
                            .topics
                            .iter()
                            .map(|topic| topic.name.clone().unwrap_or_default())
                            .collect(),
                        request.timeout_ms,
                    );
                    if entry.silent {
                        return None;
                    }
                    Some(body(
                        &DeleteTopicsResponse {
                            throttle_time_ms: entry.throttle_time_ms,
                            responses: entry
                                .codes
                                .iter()
                                .map(|(name, code)| DeletableTopicResult {
                                    name: Some((*name).to_owned()),
                                    error_code: *code,
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        },
                        version,
                        flexible,
                    ))
                }
                create_partitions_request::API_KEY => {
                    let flexible = version >= create_partitions_request::FLEXIBLE_MIN;
                    let request: CreatePartitionsRequest = decode_body(request, version, flexible);
                    let entry = take(
                        request
                            .topics
                            .iter()
                            .map(|topic| topic.name.clone())
                            .collect(),
                        request.timeout_ms,
                    );
                    if entry.silent {
                        return None;
                    }
                    Some(body(
                        &CreatePartitionsResponse {
                            throttle_time_ms: entry.throttle_time_ms,
                            results: entry
                                .codes
                                .iter()
                                .map(|(name, code)| CreatePartitionsTopicResult {
                                    name: (*name).to_owned(),
                                    error_code: *code,
                                    ..Default::default()
                                })
                                .collect(),
                            ..Default::default()
                        },
                        version,
                        flexible,
                    ))
                }
                _ => None,
            }
        })
        .await;
        port.store(broker.addr.port(), std::sync::atomic::Ordering::SeqCst);
        broker
    }

    fn observe<T: TopicMutationOutcome>(outcomes: Vec<T>) -> Vec<Observed> {
        outcomes
            .into_iter()
            .map(|outcome| {
                (
                    outcome.topic().to_owned(),
                    outcome.error_code(),
                    !outcome.throttle_duration().is_zero(),
                )
            })
            .collect()
    }

    async fn run_mutation(
        admin: &mut AdminClient,
        mutation: Mutation,
        topics: &[&str],
        options: TopicMutationOptions,
        policy: RetryPolicy,
    ) -> Vec<Observed> {
        match mutation {
            Mutation::CreateTopics => {
                let specs = topics
                    .iter()
                    .map(|name| CreateTopicSpec {
                        name: (*name).to_owned(),
                        partitions: 1,
                        replicas: 1,
                        configs: BTreeMap::new(),
                    })
                    .collect::<Vec<_>>();
                observe(
                    admin
                        .create_topics_with_retry(&specs, options, policy)
                        .await
                        .expect("create_topics"),
                )
            }
            Mutation::DeleteTopics => observe(
                admin
                    .delete_topics_with_retry(topics, options, policy)
                    .await
                    .expect("delete_topics"),
            ),
            Mutation::CreatePartitions => {
                let ops = topics
                    .iter()
                    .map(|name| CreatePartitionsOp {
                        name: (*name).to_owned(),
                        new_total_count: 2,
                    })
                    .collect::<Vec<_>>();
                observe(
                    admin
                        .create_partitions_with_retry(&ops, options, policy)
                        .await
                        .expect("create_partitions"),
                )
            }
        }
    }

    /// Kafka's `getCreateTopicsCall`, `getDeleteTopicsCall` and
    /// `getCreatePartitionsCall` retry `NOT_CONTROLLER` (41) for the whole
    /// call through `handleNotControllerError`, and retry
    /// `THROTTLING_QUOTA_EXCEEDED` (89) for the throttled topics only when
    /// `retryOnQuotaViolation` is set. At the deadline a throttled topic keeps
    /// 89 with its throttle time, and another topic gets a `TimeoutException`
    /// (7).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn topic_mutations_retry_not_controller_and_quota_as_kafka_does() {
        const LONG: Duration = Duration::from_secs(5);
        // One request fits in `NOW`, and the backoff after it reaches the
        // deadline, so the call sends no second request.
        const NOW: Duration = Duration::from_millis(500);
        let ok = |name: &str| (name.to_owned(), 0, false);
        let failed = |name: &str, code| (name.to_owned(), code, false);
        let quota = |name: &str| (name.to_owned(), THROTTLING_QUOTA_EXCEEDED, true);
        let names = |lists: &[&[&str]]| {
            lists
                .iter()
                .map(|list| {
                    list.iter()
                        .map(|name| (*name).to_owned())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        let retry_quota = TopicMutationOptions::default();
        let no_quota_retry = TopicMutationOptions {
            retry_on_quota_violation: false,
            ..TopicMutationOptions::default()
        };
        let cases: Vec<MutationCase> = vec![
            (
                "no error",
                vec![answer(&[("a", 0), ("b", 0)])],
                LONG,
                retry_quota,
                names(&[&["a", "b"]]),
                vec![ok("a"), ok("b")],
            ),
            (
                "not controller twice, then success",
                vec![
                    answer(&[("a", 41), ("b", 41)]),
                    answer(&[("a", 41), ("b", 41)]),
                    answer(&[("a", 0), ("b", 0)]),
                ],
                LONG,
                retry_quota,
                names(&[&["a", "b"], &["a", "b"], &["a", "b"]]),
                vec![ok("a"), ok("b")],
            ),
            (
                "not controller for one topic sends every topic again",
                vec![
                    answer(&[("a", 0), ("b", 41)]),
                    answer(&[("a", 0), ("b", 0)]),
                ],
                LONG,
                retry_quota,
                names(&[&["a", "b"], &["a", "b"]]),
                vec![ok("a"), ok("b")],
            ),
            (
                "quota for one topic sends only that topic again",
                vec![answer(&[("a", 0), ("b", 89)]), answer(&[("b", 0)])],
                LONG,
                retry_quota,
                names(&[&["a", "b"], &["b"]]),
                vec![ok("a"), ok("b")],
            ),
            (
                "quota with retry off is final",
                vec![throttled(&[("a", 89), ("b", 0)], 250)],
                LONG,
                no_quota_retry,
                names(&[&["a", "b"]]),
                vec![quota("a"), ok("b")],
            ),
            (
                "not controller past the deadline times out",
                vec![answer(&[("a", 41), ("b", 41)])],
                NOW,
                retry_quota,
                names(&[&["a", "b"]]),
                vec![failed("a", 7), failed("b", 7)],
            ),
            (
                "quota past the deadline keeps the quota error",
                vec![throttled(&[("a", 89), ("b", 36)], 60_000)],
                Duration::from_millis(100),
                retry_quota,
                names(&[&["a", "b"]]),
                vec![quota("a"), failed("b", 36)],
            ),
            (
                "a silent controller stops the call at the deadline",
                vec![silent()],
                Duration::from_millis(300),
                retry_quota,
                names(&[&["a", "b"]]),
                vec![failed("a", 7), failed("b", 7)],
            ),
            (
                "a topic with no result fails",
                vec![answer(&[("a", 0)])],
                LONG,
                retry_quota,
                names(&[&["a", "b"]]),
                vec![ok("a"), failed("b", -1)],
            ),
        ];
        for mutation in [
            Mutation::CreateTopics,
            Mutation::DeleteTopics,
            Mutation::CreatePartitions,
        ] {
            for (name, script, timeout, options, expected_requests, expected) in cases.clone() {
                let requests = Arc::new(Mutex::new(Vec::new()));
                let controller = scripted_controller(script, Arc::clone(&requests)).await;
                let mut admin = AdminClient::connect(&[controller.addr.to_string()])
                    .await
                    .expect("admin connects");
                let backoff = if timeout == LONG {
                    Duration::from_millis(1)
                } else {
                    timeout
                };
                let policy = RetryPolicy {
                    timeout,
                    initial_backoff: backoff,
                    max_backoff: backoff,
                    jitter: 0.0,
                };

                let started = tokio::time::Instant::now();
                let observed =
                    run_mutation(&mut admin, mutation, &["a", "b"], options, policy).await;
                let within_deadline = started.elapsed() < timeout + Duration::from_secs(2);

                controller.stop();
                let requests = requests.lock().expect("requests lock").clone();
                let timeouts_within_deadline = requests.iter().all(|(_, timeout_ms)| {
                    u128::try_from(*timeout_ms).is_ok_and(|ms| ms <= timeout.as_millis())
                });
                let sent = requests
                    .into_iter()
                    .map(|(names, _)| names)
                    .collect::<Vec<_>>();
                assert2::assert!(
                    (sent, observed, timeouts_within_deadline, within_deadline)
                        == (expected_requests, expected, true, true),
                    "{mutation:?}: {name}"
                );
            }
        }
    }

    /// Kafka completes a throttled topic at the deadline with the throttle
    /// time that remains (`maybeCompleteQuotaExceededException`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quota_error_at_the_deadline_carries_the_remaining_throttle_time() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let controller =
            scripted_controller(vec![throttled(&[("a", 89)], 60_000)], Arc::clone(&requests)).await;
        let mut admin = AdminClient::connect(&[controller.addr.to_string()])
            .await
            .expect("admin connects");
        let policy = RetryPolicy {
            timeout: Duration::from_millis(100),
            ..KAFKA_ADMIN_RETRY
        };

        let outcomes = admin
            .delete_topics_with_retry(&["a"], TopicMutationOptions::default(), policy)
            .await
            .expect("delete_topics");

        controller.stop();
        let [outcome] = outcomes.as_slice() else {
            panic!("one outcome expected, got {outcomes:?}");
        };
        let remaining = outcome.throttle_duration();
        assert2::assert!(
            (
                outcome.error.as_ref().map(|error| error.code),
                remaining > Duration::from_secs(59),
                remaining < Duration::from_mins(1),
            ) == (Some(THROTTLING_QUOTA_EXCEEDED), true, true)
        );
    }

    fn assert_send<T: Send>(_: T) {}

    /// A caller can spawn every controller call on a multi-thread runtime.
    #[test]
    fn controller_call_futures_are_send() {
        let _ = |admin: &mut AdminClient, specs: &[CreateTopicSpec], names: &[&str]| {
            assert_send(admin.create_topics(specs, TopicMutationOptions::default()));
            assert_send(admin.delete_topics(names, TopicMutationOptions::default()));
            assert_send(admin.create_partitions(&[], TopicMutationOptions::default()));
            assert_send(admin.unregister_broker(1));
            assert_send(admin.update_features(&[], krabka_units::secs(1)));
            assert_send(admin.alter_partition_assignments(&BTreeMap::new(), krabka_units::secs(1)));
            assert_send(admin.reconcile_topic_replication_factor("t", 1, krabka_units::secs(1)));
        };
    }
}
