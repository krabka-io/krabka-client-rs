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

use crate::{AdminClient, AdminError, KafkaError, kafka_error_if, kafka_error_name};

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
        let response = self
            .conn
            .send(offset_commit_request(group, offsets))
            .await?;
        Ok(response
            .topics
            .into_iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .into_iter()
                    .map(move |partition| ConsumerGroupOffsetOutcome {
                        topic: topic.name.clone(),
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
        let id_to_name: HashMap<WireUuid, String> = {
            let meta = self.conn.send(MetadataRequest::default()).await?;
            meta.topics
                .into_iter()
                .filter_map(|t| {
                    if t.topic_id == WireUuid::ZERO {
                        None
                    } else {
                        t.name.map(|n| (t.topic_id, n))
                    }
                })
                .collect()
        };

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
}

fn offset_commit_request(
    group: &str,
    offsets: &BTreeMap<(String, i32), i64>,
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
    use assert2::assert;

    use super::*;

    #[test]
    fn offset_reset_builds_admin_commit() {
        let offsets = BTreeMap::from([(("orders".into(), 2), 41)]);
        let request = offset_commit_request("worker", &offsets);
        let expected = OffsetCommitRequest {
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
        };
        assert!(request == expected);
    }
}
