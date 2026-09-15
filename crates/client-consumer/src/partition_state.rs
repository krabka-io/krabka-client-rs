//! Per-partition state that the application reads and changes: pause and
//! resume, the fetch position and the committed offset.
//!
//! These are Kafka's `KafkaConsumer.pause`, `resume`, `paused`, `position` and
//! `committed`.

use std::collections::{HashMap, HashSet};

use krabka_units::convert::TimeExt as _;

use crate::{
    commit::OffsetAndMetadata, consumer::Consumer, coordinator::CoordinatorRetryPolicy,
    error::ConsumerError, poll::is_reset_sentinel,
};

/// Paused partitions, with the ownership of each partition at the pause.
///
/// Kafka keeps the pause in the `TopicPartitionState` of the partition. A
/// rebalance that keeps the partition keeps its state, and an assignment that
/// adds the partition again creates a new state that is not paused
/// (`SubscriptionState.assignFromSubscribed`). The ownership id changes in the
/// same cases, so a pause is in effect only while the ownership id is the one
/// of the pause.
pub(crate) type PausedPartitions = std::sync::Mutex<HashMap<(String, i32), u64>>;

impl Consumer {
    /// Stop fetching from `partitions`. Kafka's `KafkaConsumer.pause`.
    ///
    /// `poll` does not return records of a paused partition and does not fetch
    /// it. The consumer stays in the group. Records that `poll` fetched before
    /// the pause come after [`resume`](Self::resume). A rebalance that keeps
    /// the partition keeps the pause.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::NoCurrentAssignment`] if the consumer does not
    /// own a partition. The call then pauses no partition.
    pub async fn pause(&self, partitions: &[(String, i32)]) -> Result<(), ConsumerError> {
        let ownership_ids = self.commit_identity.lock().await.ownership_ids.clone();
        let mut paused = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let Some(ownership_id) = ownership_ids.get(partition) else {
                return Err(no_current_assignment(partition));
            };
            paused.push((partition.clone(), *ownership_id));
        }
        self.paused
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(paused);
        Ok(())
    }

    /// Fetch from `partitions` again. Kafka's `KafkaConsumer.resume`.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::NoCurrentAssignment`] if the consumer does not
    /// own a partition. The call then resumes no partition.
    pub async fn resume(&self, partitions: &[(String, i32)]) -> Result<(), ConsumerError> {
        let ownership_ids = self.commit_identity.lock().await.ownership_ids.clone();
        if let Some(partition) = partitions
            .iter()
            .find(|partition| !ownership_ids.contains_key(*partition))
        {
            return Err(no_current_assignment(partition));
        }
        let mut paused = self
            .paused
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for partition in partitions {
            paused.remove(partition);
        }
        Ok(())
    }

    /// The paused partitions that the consumer owns. Kafka's
    /// `KafkaConsumer.paused`.
    pub async fn paused(&self) -> HashSet<(String, i32)> {
        let ownership_ids = self.commit_identity.lock().await.ownership_ids.clone();
        self.paused_of(&ownership_ids)
    }

    /// The partitions whose pause is in effect for `ownership_ids`.
    pub(crate) fn paused_of(
        &self,
        ownership_ids: &HashMap<(String, i32), u64>,
    ) -> HashSet<(String, i32)> {
        let mut paused = self
            .paused
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        paused.retain(|partition, ownership_id| ownership_ids.get(partition) == Some(ownership_id));
        paused.keys().cloned().collect()
    }

    /// The offset of the next record that `poll` fetches for `(topic,
    /// partition)`. Kafka's `KafkaConsumer.position`.
    ///
    /// When the partition waits for an offset reset or for a leader epoch
    /// validation, the call sends `ListOffsets` or `OffsetForLeaderEpoch` and
    /// waits for the position, for at most `default_api_timeout`.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::NoCurrentAssignment`] if the consumer does not
    /// own the partition, [`ConsumerError::Timeout`] if the position is not
    /// known before `default_api_timeout`, or the error of a request.
    pub async fn position(
        &self,
        topic: impl Into<String>,
        partition: i32,
    ) -> Result<i64, ConsumerError> {
        let key = (topic.into(), partition);
        let deadline = tokio::time::Instant::now() + self.default_api_timeout.to_std();
        let wait = async {
            loop {
                if !self.assigned.lock().await.contains(&key) {
                    return Err(no_current_assignment(&key));
                }
                if let Some(offset) = self.valid_position(&key).await {
                    return Ok(offset);
                }
                self.update_fetch_positions(None).await?;
                if self.valid_position(&key).await.is_none() {
                    tokio::time::sleep(self.retry_policy.initial_backoff).await;
                }
            }
        };
        // The timeout bounds the requests too, as Kafka's `position` passes
        // its timer to `updateFetchPositions` and `client.poll`.
        tokio::time::timeout_at(deadline, wait)
            .await
            .unwrap_or_else(|_| {
                Err(ConsumerError::Timeout(format!(
                    "the position for partition {}-{} could not be determined",
                    key.0, key.1
                )))
            })
    }

    /// The position of `key` when it is valid: Kafka's
    /// `SubscriptionState.validPosition`. A position that waits for a reset or
    /// for validation is not valid.
    async fn valid_position(&self, key: &(String, i32)) -> Option<i64> {
        let offsets = self.next_offsets.lock().await;
        let positions = self.positions.lock().await;
        let offset = *offsets.get(key)?;
        let awaiting_validation = positions
            .get(key)
            .is_some_and(|position| position.awaiting_validation);
        (!is_reset_sentinel(offset) && !awaiting_validation).then_some(offset)
    }

    /// The committed offsets of `partitions` in the group. Kafka's
    /// `KafkaConsumer.committed`.
    ///
    /// A partition without a committed offset maps to `None`. The partitions
    /// do not need to be assigned to this consumer.
    ///
    /// # Errors
    ///
    /// Returns the error of the `OffsetFetch` request after the retries that
    /// Kafka's consumer does, for at most `default_api_timeout`.
    pub async fn committed(
        &self,
        partitions: &[(String, i32)],
    ) -> Result<HashMap<(String, i32), Option<OffsetAndMetadata>>, ConsumerError> {
        self.require_group_id()?;
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }
        let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for (topic, partition) in partitions {
            by_topic.entry(topic.clone()).or_default().push(*partition);
        }
        let request = crate::offset_wire::build_offset_fetch(&self.group_id, &by_topic);
        let fetch = crate::coordinator::send_offset_fetch(
            &self.client,
            &self.group_id,
            &self.coordinator_id,
            &request,
            CoordinatorRetryPolicy {
                timeout: self.default_api_timeout.to_std(),
                ..self.retry_policy
            },
        );
        // The timeout bounds each request too. Kafka's
        // `fetchCommittedOffsets` gives up when its timer expires.
        let response = tokio::time::timeout(self.default_api_timeout.to_std(), fetch)
            .await
            .map_err(|_| {
                ConsumerError::Timeout(
                    "the last committed offsets could not be determined".to_owned(),
                )
            })??;
        let mut committed = crate::offset_wire::parse_committed_offsets(&response);
        committed.retain(|partition, _| partitions.contains(partition));
        for partition in partitions {
            committed.entry(partition.clone()).or_insert(None);
        }
        Ok(committed)
    }
}

fn no_current_assignment((topic, partition): &(String, i32)) -> ConsumerError {
    ConsumerError::NoCurrentAssignment {
        topic: topic.clone(),
        partition: *partition,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use krabka_client_core::{Client, MockBroker};
    use krabka_protocol::{
        Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            offset_fetch_request,
            offset_fetch_response::{
                OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
            },
        },
    };
    use krabka_units::secs;

    use crate::{commit::OffsetAndMetadata, poll::partition_error_tests::consumer_with_client};

    fn encode(response: &impl Encode, version: i16) -> Vec<u8> {
        let mut buf = bytes::BytesMut::new();
        response.encode(&mut buf, version).expect("encode");
        buf.to_vec()
    }

    /// A coordinator that answers `OffsetFetch` v5 with `rows`:
    /// `(partition, committed offset, leader epoch, metadata, error code)` of
    /// `orders`.
    async fn offset_fetch_coordinator(rows: Vec<(i32, i64, i32, &'static str, i16)>) -> MockBroker {
        MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                let versions = ApiVersionsResponse {
                    api_keys: [
                        (api_versions_request::API_KEY, 0, 3),
                        (offset_fetch_request::API_KEY, 5, 5),
                    ]
                    .into_iter()
                    .map(|(api_key, min_version, max_version)| ApiVersion {
                        api_key,
                        min_version,
                        max_version,
                        ..Default::default()
                    })
                    .collect(),
                    ..Default::default()
                };
                return Some(encode(&versions, 0));
            }
            if api_key != offset_fetch_request::API_KEY {
                return None;
            }
            let response = OffsetFetchResponse {
                topics: vec![OffsetFetchResponseTopic {
                    name: "orders".into(),
                    partitions: rows
                        .iter()
                        .map(
                            |(
                                partition_index,
                                committed_offset,
                                committed_leader_epoch,
                                metadata,
                                error_code,
                            )| {
                                OffsetFetchResponsePartition {
                                    partition_index: *partition_index,
                                    committed_offset: *committed_offset,
                                    committed_leader_epoch: *committed_leader_epoch,
                                    metadata: Some((*metadata).into()),
                                    error_code: *error_code,
                                    ..Default::default()
                                }
                            },
                        )
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            Some(encode(&response, version))
        })
        .await
    }

    /// Kafka's `committed` maps a partition without a committed offset to
    /// `null`, and keeps the leader epoch and metadata of a committed one
    /// (`ConsumerCoordinator.OffsetFetchResponseHandler`).
    #[tokio::test]
    async fn committed_returns_kafkas_offsets_and_metadata() {
        let mock = offset_fetch_coordinator(vec![
            (0, 10, 3, "note", 0),
            (1, -1, -1, "", 0),
            (3, 7, -1, "", 0),
        ])
        .await;
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(secs(30))
            .build()
            .await
            .expect("client");
        let consumer = consumer_with_client(client);
        let partitions = [
            ("orders".to_string(), 0),
            ("orders".to_string(), 1),
            ("orders".to_string(), 2),
        ];
        let committed = consumer
            .committed(&partitions)
            .await
            .map_err(|error| error.to_string());
        mock.stop();
        assert2::assert!(
            committed
                == Ok(HashMap::from([
                    (
                        ("orders".to_string(), 0),
                        Some(OffsetAndMetadata {
                            offset: 10,
                            leader_epoch: Some(3),
                            metadata: "note".into(),
                        })
                    ),
                    (("orders".to_string(), 1), None),
                    (("orders".to_string(), 2), None),
                ]))
        );
    }

    /// Kafka's `pause`, `resume` and `paused` act on assigned partitions only,
    /// and a partition that the consumer owns again after a rebalance is not
    /// paused (`SubscriptionState.assignFromSubscribed` gives it a new state).
    #[tokio::test]
    async fn pause_follows_the_ownership_of_the_partition() {
        let key = ("orders".to_string(), 0);
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, pause, resume, new_ownership, expected_error, expected_paused) in [
            (
                "pause",
                vec![key.clone()],
                vec![],
                false,
                None,
                vec![key.clone()],
            ),
            (
                "pause and resume",
                vec![key.clone()],
                vec![key.clone()],
                false,
                None,
                vec![],
            ),
            ("owned again", vec![key.clone()], vec![], true, None, vec![]),
            (
                "unowned partition",
                vec![("orders".to_string(), 9)],
                vec![],
                false,
                Some("no current assignment for partition orders-9"),
                vec![],
            ),
        ] {
            let mock = offset_fetch_coordinator(vec![]).await;
            let client = Client::builder()
                .bootstrap(mock.addr.to_string())
                .build()
                .await
                .expect("client");
            let consumer = consumer_with_client(client);
            let mut error = consumer
                .pause(&pause)
                .await
                .err()
                .map(|error| error.to_string());
            if let Err(resume_error) = consumer.resume(&resume).await {
                error.get_or_insert(resume_error.to_string());
            }
            if new_ownership {
                consumer
                    .commit_identity
                    .lock()
                    .await
                    .ownership_ids
                    .insert(key.clone(), 2);
            }
            let paused = consumer.paused().await;
            mock.stop();
            actual.push((name, error, paused));
            wanted.push((
                name,
                expected_error.map(str::to_owned),
                expected_paused.into_iter().collect::<HashSet<_>>(),
            ));
        }
        assert2::assert!(actual == wanted);
    }

    /// Kafka's `committed(partitions, timeout)` gives up when the timeout
    /// expires, also while a request waits for its response.
    #[tokio::test]
    async fn committed_returns_by_the_api_timeout_when_the_coordinator_is_silent() {
        let mock = MockBroker::start(|api_key, _version, _corr_id, _body| {
            (api_key == api_versions_request::API_KEY).then(|| {
                encode(
                    &ApiVersionsResponse {
                        api_keys: [
                            (api_versions_request::API_KEY, 0, 3),
                            (offset_fetch_request::API_KEY, 5, 5),
                        ]
                        .into_iter()
                        .map(|(api_key, min_version, max_version)| ApiVersion {
                            api_key,
                            min_version,
                            max_version,
                            ..Default::default()
                        })
                        .collect(),
                        ..Default::default()
                    },
                    0,
                )
            })
        })
        .await;
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(secs(30))
            .build()
            .await
            .expect("client");
        let mut consumer = consumer_with_client(client);
        consumer.default_api_timeout = krabka_units::millis(200);
        let started = tokio::time::Instant::now();
        let committed = consumer
            .committed(&[("orders".to_string(), 0)])
            .await
            .map_err(|error| error.to_string());
        let in_time = started.elapsed() < std::time::Duration::from_secs(5);
        mock.stop();
        assert2::assert!(
            (committed, in_time)
                == (
                    Err("timeout: the last committed offsets could not be determined".to_owned()),
                    true
                )
        );
    }

    /// Kafka's `seekToEnd` requests a reset and keeps the high watermark of
    /// the last fetch. A partition that waits for the reset is not at the end.
    #[tokio::test]
    async fn seek_to_end_keeps_the_end_offset_and_leaves_the_log_end() {
        let mock = offset_fetch_coordinator(vec![]).await;
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .build()
            .await
            .expect("client");
        let consumer = consumer_with_client(client);
        let key = ("orders".to_string(), 0);
        consumer.next_offsets.lock().await.insert(key.clone(), 12);
        consumer.end_offsets.lock().await.insert(key.clone(), 12);
        let before = consumer.at_log_end().await;
        consumer.seek_to_end(&[]).await.expect("seek to end");
        let after = consumer.at_log_end().await;
        let end = consumer.end_offsets.lock().await.get(&key).copied();
        mock.stop();
        assert2::assert!((before, after, end) == (true, false, Some(12)));
    }

    /// Kafka's `assign` keeps the position of a partition that stays
    /// assigned, and a new partition starts at the committed offset of the
    /// group, or by `auto.offset.reset` without one
    /// (`refreshCommittedOffsetsIfNeeded`, `resetInitializingPositions`).
    #[tokio::test]
    async fn assign_starts_new_partitions_at_the_committed_offset_or_the_reset() {
        let mock = offset_fetch_coordinator(vec![(1, 10, 2, "", 0), (2, -1, -1, "", 0)]).await;
        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(secs(30))
            .build()
            .await
            .expect("client");
        let mut consumer = consumer_with_client(client);
        consumer.subscription = crate::subscription::shared(Vec::new(), None, true);
        consumer.auto_offset_reset = crate::AutoOffsetReset::Earliest;
        let key = |partition: i32| ("orders".to_string(), partition);
        consumer
            .assign(&[key(0), key(1), key(2)])
            .await
            .expect("assign");
        consumer
            .resolve_committed_sentinels()
            .await
            .expect("resolve");
        let mut offsets: Vec<_> = consumer
            .next_offsets
            .lock()
            .await
            .clone()
            .into_iter()
            .collect();
        offsets.sort();
        let identity = consumer.commit_identity.lock().await.clone();
        let rejected = consumer
            .subscribe(["orders"])
            .await
            .map_err(|error| error.to_string());
        mock.stop();
        assert2::assert!(
            (
                offsets,
                identity.generation,
                identity.member_id,
                consumer.assignment().await,
                rejected
            ) == (
                vec![(key(0), 5), (key(1), 10), (key(2), 0)],
                -1,
                String::new(),
                vec![key(0), key(1), key(2)],
                Err("illegal state: the consumer was built without a subscription, so it has no group membership; build it with subscribe or subscribe_pattern to join a group".to_owned())
            )
        );
    }
}
