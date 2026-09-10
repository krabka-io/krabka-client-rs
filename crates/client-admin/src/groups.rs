//! Consumer-group admin APIs: [`AdminClient::list_groups`] and
//! [`AdminClient::list_consumer_group_offsets`].
//!
//! These are thin wrappers over the `ListGroups` (`api_key`=16) and
//! `OffsetFetch` (`api_key`=9, v8+ grouped form) RPCs.
//!
//! ## `OffsetFetch` version note
//!
//! The `Connection` negotiates the highest mutually supported version, which
//! is v10 at the time of writing. At v10 the response encodes topics by
//! `topic_id` only, and the wire omits the `name` field. To return the
//! human-readable `(topic, partition) → offset` map, the client calls
//! `Metadata` with no filter immediately after, which fetches all topics, and
//! builds an id→name lookup table.

use std::collections::{BTreeMap, HashMap};

use krabka_client_core::{CoordinatorKeyType, build_find_coordinator, coordinator_endpoint};
use krabka_protocol::{
    owned::{
        list_groups_request::ListGroupsRequest,
        metadata_request::MetadataRequest,
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestGroup},
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    AdminClient, AdminError, KafkaError, format_host_port, kafka_error_if, kafka_error_name,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupOffsetOutcome {
    pub topic: String,
    pub partition: i32,
    pub error: Option<KafkaError>,
}

/// One committed-offset row collected from an `OffsetFetch` response. It keeps
/// the `topic_id`, so `Metadata` can resolve name-less v10 topics.
struct Entry {
    topic_name: String,
    topic_id: WireUuid,
    partition: i32,
    offset: i64,
}

impl AdminClient {
    /// Commits explicit offsets for an inactive consumer group.
    ///
    /// # Errors
    /// Returns an error when encoding, transport, or response handling fails.
    pub async fn alter_consumer_group_offsets(
        &mut self,
        group: &str,
        offsets: &BTreeMap<(String, i32), i64>,
    ) -> Result<Vec<ConsumerGroupOffsetOutcome>, AdminError> {
        self.reconnect_group_coordinator(group).await?;
        let topic_ids = self.topic_ids().await?;
        let topic_names = topic_ids
            .iter()
            .map(|(name, id)| (*id, name.clone()))
            .collect::<HashMap<_, _>>();
        let response = self
            .conn
            .send(offset_commit_request(group, offsets, &topic_ids))
            .await?;
        Ok(response
            .topics
            .into_iter()
            .flat_map(|topic| {
                let name = if topic.name.is_empty() {
                    topic_names
                        .get(&topic.topic_id)
                        .cloned()
                        .unwrap_or_default()
                } else {
                    topic.name
                };
                topic
                    .partitions
                    .into_iter()
                    .map(move |partition| ConsumerGroupOffsetOutcome {
                        topic: name.clone(),
                        partition: partition.partition_index,
                        error: kafka_error_if(partition.error_code, None),
                    })
            })
            .collect())
    }

    /// Returns the group-id of every consumer group known to the broker.
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn list_groups(&mut self) -> Result<Vec<String>, AdminError> {
        // Default request lists every group (empty state/type filters).
        let req = ListGroupsRequest::default();
        let resp = self.conn.send(req).await?;
        if resp.error_code != 0 {
            return Err(AdminError::Broker {
                api: "ListGroups",
                code: resp.error_code,
                name: kafka_error_name(resp.error_code),
                message: None,
            });
        }
        Ok(resp.groups.into_iter().map(|g| g.group_id).collect())
    }

    /// Returns `(topic, partition) → committed_offset` for the named group.
    ///
    /// The call requests all topics and partitions (`topics: None`). It skips
    /// entries with a committed offset < 0, which means no committed offset.
    ///
    /// At `OffsetFetch` v10 the response carries `topic_id` instead of
    /// `name`, so a `Metadata` round-trip resolves the topic ids to names.
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn list_consumer_group_offsets(
        &mut self,
        group: &str,
    ) -> Result<BTreeMap<(String, i32), i64>, AdminError> {
        self.reconnect_group_coordinator(group).await?;
        let req = OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: group.to_string(),
                member_id: None,
                member_epoch: -1,
                topics: None,
                ..Default::default()
            }],
            ..Default::default()
        };
        let resp = self.conn.send(req).await?;

        // Collect committed offsets, keeping each topic's id for name resolution.
        let mut raw: Vec<Entry> = Vec::new();
        for g in resp.groups {
            if g.error_code != 0 {
                return Err(AdminError::Broker {
                    api: "OffsetFetch",
                    code: g.error_code,
                    name: kafka_error_name(g.error_code),
                    message: Some(format!("group={}", g.group_id)),
                });
            }
            for t in g.topics {
                for p in t.partitions {
                    if p.committed_offset >= 0 {
                        raw.push(Entry {
                            topic_name: t.name.clone(),
                            topic_id: t.topic_id,
                            partition: p.partition_index,
                            offset: p.committed_offset,
                        });
                    }
                }
            }
        }

        // Build an id→name map from a `Metadata` round-trip (default request =
        // all topics). At OffsetFetch v10 the response omits names, so this is
        // how empty-named entries below recover their topic; at v8/v9 names are
        // already present and the per-entry resolution simply ignores this map.
        let id_to_name = self
            .topic_ids()
            .await?
            .into_iter()
            .map(|(name, id)| (id, name))
            .collect::<HashMap<_, _>>();

        let mut out = BTreeMap::new();
        for e in raw {
            let name = if e.topic_name.is_empty() {
                match id_to_name.get(&e.topic_id) {
                    Some(n) => n.clone(),
                    None => continue, // unknown id — skip
                }
            } else {
                e.topic_name
            };
            out.insert((name, e.partition), e.offset);
        }
        Ok(out)
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

    async fn topic_ids(&self) -> Result<HashMap<String, WireUuid>, AdminError> {
        Ok(self
            .conn
            .send(MetadataRequest::default())
            .await?
            .topics
            .into_iter()
            .filter_map(|topic| {
                if topic.topic_id == WireUuid::ZERO {
                    None
                } else {
                    topic.name.map(|name| (name, topic.topic_id))
                }
            })
            .collect())
    }
}

fn offset_commit_request(
    group: &str,
    offsets: &BTreeMap<(String, i32), i64>,
    topic_ids: &HashMap<String, WireUuid>,
) -> OffsetCommitRequest {
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
    OffsetCommitRequest {
        group_id: group.into(),
        generation_id_or_member_epoch: -1,
        member_id: String::new(),
        topics: topics
            .into_iter()
            .map(|(name, partitions)| OffsetCommitRequestTopic {
                topic_id: topic_ids.get(&name).copied().unwrap_or_default(),
                name,
                partitions,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use assert2::assert;
    use bytes::{Buf, BytesMut};
    use krabka_client_core::MockBroker;
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            find_coordinator_request,
            find_coordinator_response::FindCoordinatorResponse,
            metadata_request,
            metadata_response::{MetadataResponse, MetadataResponseTopic},
            offset_commit_request,
            offset_commit_response::{
                OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
            },
            offset_fetch_request,
            offset_fetch_response::{
                OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartitions,
                OffsetFetchResponseTopics,
            },
        },
    };

    use super::*;

    #[test]
    fn offset_reset_builds_admin_commit() {
        let offsets = BTreeMap::from([(("orders".into(), 2), 41)]);
        let topic_id = WireUuid([7; 16]);
        let request = offset_commit_request(
            "worker",
            &offsets,
            &HashMap::from([("orders".into(), topic_id)]),
        );
        let expected = OffsetCommitRequest {
            group_id: "worker".into(),
            topics: vec![OffsetCommitRequestTopic {
                name: "orders".into(),
                topic_id,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 2,
                    committed_offset: 41,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
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

    fn api_versions() -> Vec<u8> {
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
                        min_version: 10,
                        max_version: 10,
                        ..Default::default()
                    },
                    ApiVersion {
                        api_key: offset_fetch_request::API_KEY,
                        min_version: 10,
                        max_version: 10,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_offset_rpcs_use_the_group_coordinator() {
        let topic_id = WireUuid([7; 16]);
        let commit_used_topic_id = Arc::new(AtomicUsize::new(0));
        let used_topic_id = Arc::clone(&commit_used_topic_id);
        let coordinator_group_rpcs = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&coordinator_group_rpcs);
        let coordinator = MockBroker::start(move |api_key, version, _, mut body| match api_key {
            api_versions_request::API_KEY => Some(api_versions()),
            offset_commit_request::API_KEY => {
                seen.fetch_add(1, Ordering::SeqCst);
                let client_id_len = body.get_i16();
                body.advance(usize::try_from(client_id_len).expect("client id length"));
                body.advance(1);
                let request = OffsetCommitRequest::decode(&mut body, version)
                    .expect("offset commit request decodes");
                if request.topics[0].topic_id == topic_id {
                    used_topic_id.fetch_add(1, Ordering::SeqCst);
                }
                Some(encode(
                    &OffsetCommitResponse {
                        topics: vec![OffsetCommitResponseTopic {
                            topic_id,
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
                Some(encode(
                    &OffsetFetchResponse {
                        groups: vec![OffsetFetchResponseGroup {
                            group_id: "workers".into(),
                            topics: vec![OffsetFetchResponseTopics {
                                topic_id,
                                partitions: vec![OffsetFetchResponsePartitions {
                                    partition_index: 2,
                                    committed_offset: 41,
                                    ..Default::default()
                                }],
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
            metadata_request::API_KEY => Some(encode(
                &MetadataResponse {
                    topics: vec![MetadataResponseTopic {
                        name: Some("orders".into()),
                        topic_id,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                version,
                true,
            )),
            _ => None,
        })
        .await;

        let bootstrap_group_rpcs = Arc::new(AtomicUsize::new(0));
        let wrong_broker = Arc::clone(&bootstrap_group_rpcs);
        let coordinator_addr = coordinator.addr;
        let bootstrap = MockBroker::start(move |api_key, version, _, _| match api_key {
            api_versions_request::API_KEY => Some(api_versions()),
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
                wrong_broker.fetch_add(1, Ordering::SeqCst);
                Some(encode(&OffsetCommitResponse::default(), version, true))
            }
            offset_fetch_request::API_KEY => {
                wrong_broker.fetch_add(1, Ordering::SeqCst);
                Some(encode(&OffsetFetchResponse::default(), version, true))
            }
            metadata_request::API_KEY => Some(encode(&MetadataResponse::default(), version, true)),
            _ => None,
        })
        .await;
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
        assert!(commit_used_topic_id.load(Ordering::SeqCst) == 1);
        assert!(committed[0].topic == "orders");
        assert!(fetched == BTreeMap::from([(("orders".into(), 2), 41)]));
        bootstrap.stop();
        coordinator.stop();
    }
}
