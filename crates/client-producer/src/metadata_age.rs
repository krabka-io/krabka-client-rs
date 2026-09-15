//! The periodic metadata refresh of the producer.
//!
//! Kafka's producer refreshes its metadata when the last successful refresh
//! is older than `metadata.max.age.ms` (`Metadata.timeToNextUpdate`), and it
//! stops asking for a topic that had no send for `metadata.max.idle.ms`
//! (`ProducerMetadata.retainTopic`). The refresh gives the producer a larger
//! partition count and moved leaders without a send error.

use std::{collections::HashMap, sync::Arc, time::Duration};

use dashmap::DashMap;
use krabka_client_core::Client;
use tokio::{sync::Mutex, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    builder::ProducerRetryPolicy, error::ProducerError, metadata_wait::MetadataRefresh,
    producer::TopicMetadata, sender::adopt_metadata,
};

/// Kafka's producer `metadata.max.age.ms` default (5 minutes).
pub const DEFAULT_PRODUCER_METADATA_MAX_AGE: Duration = Duration::from_mins(5);

/// Kafka's producer `metadata.max.idle.ms` default (5 minutes).
pub const DEFAULT_PRODUCER_METADATA_MAX_IDLE: Duration = Duration::from_mins(5);

/// Kafka's lower bound of `metadata.max.idle.ms` (`ProducerConfig`
/// `atLeast(5000)`).
pub const MIN_PRODUCER_METADATA_MAX_IDLE: Duration = Duration::from_secs(5);

/// The state that the periodic refresh reads and updates.
pub(crate) struct MetadataAge {
    client: Client,
    max_age: Duration,
    max_idle: Duration,
    retry_backoff: Duration,
    retry_backoff_max: Duration,
    metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>>,
    partition_leaders: Arc<DashMap<(String, i32), i32>>,
    metadata_refresh: Arc<MetadataRefresh>,
}

impl MetadataAge {
    /// A refresh for `client` with `(max_age, max_idle)` and the retry backoff
    /// of `retry_policy`.
    ///
    /// # Errors
    /// Returns [`ProducerError::InvalidConfig`] for a `max_idle` below 5 s, as
    /// Kafka's `ProducerConfig` defines `metadata.max.idle.ms` with
    /// `atLeast(5000)`.
    pub(crate) fn new(
        client: &Client,
        (max_age, max_idle): (Duration, Duration),
        retry_policy: &ProducerRetryPolicy,
    ) -> Result<Self, ProducerError> {
        if max_idle < MIN_PRODUCER_METADATA_MAX_IDLE {
            return Err(ProducerError::InvalidConfig(format!(
                "metadata_max_idle must be at least 5 s, got {max_idle:?}"
            )));
        }
        Ok(Self::unchecked(client, (max_age, max_idle), retry_policy))
    }

    fn unchecked(
        client: &Client,
        (max_age, max_idle): (Duration, Duration),
        retry_policy: &ProducerRetryPolicy,
    ) -> Self {
        Self {
            client: client.clone(),
            max_age,
            max_idle,
            retry_backoff: retry_policy.retry_backoff(),
            retry_backoff_max: retry_policy.retry_backoff_max(),
            metadata_cache: Arc::default(),
            partition_leaders: Arc::default(),
            metadata_refresh: Arc::default(),
        }
    }

    /// Use the caches of the producer.
    pub(crate) fn with_caches(
        mut self,
        metadata_cache: &Arc<Mutex<HashMap<String, TopicMetadata>>>,
        partition_leaders: &Arc<DashMap<(String, i32), i32>>,
        metadata_refresh: &Arc<MetadataRefresh>,
    ) -> Self {
        self.metadata_cache = Arc::clone(metadata_cache);
        self.partition_leaders = Arc::clone(partition_leaders);
        self.metadata_refresh = Arc::clone(metadata_refresh);
        self
    }

    /// Run [`Self::run`] on a task.
    pub(crate) fn spawn(self, shutdown: CancellationToken) {
        tokio::spawn(self.run(shutdown));
    }

    /// Refresh the metadata each time it becomes older than `max_age`, until
    /// `shutdown`.
    async fn run(self, shutdown: CancellationToken) {
        let started = Instant::now();
        let mut backoff = self.retry_backoff;
        loop {
            let topics = self.client.metadata_topics();
            let last = topics.last_refreshed().unwrap_or(started);
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep_until(last + self.max_age) => {}
            }
            if topics
                .last_refreshed()
                .is_some_and(|refreshed| refreshed > last)
            {
                // Another refresh came while this task slept.
                continue;
            }
            self.expire_idle_topics().await;
            match self.client.refresh_metadata().await {
                Ok(response) => {
                    adopt_metadata(&response, &self.metadata_cache, &self.partition_leaders).await;
                    backoff = self.retry_backoff;
                }
                Err(error) => {
                    tracing::debug!(error = %error, "periodic producer metadata refresh failed");
                    tokio::select! {
                        () = shutdown.cancelled() => return,
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = backoff.saturating_mul(2).min(self.retry_backoff_max);
                }
            }
        }
    }

    /// Drop the topics with no send for `max_idle` from the requests and the
    /// caches. The next send to such a topic waits for its metadata again.
    async fn expire_idle_topics(&self) {
        let expired = self.client.metadata_topics().remove_idle(self.max_idle);
        if expired.is_empty() {
            return;
        }
        tracing::debug!(topics = ?expired, "producer metadata topics expired");
        self.metadata_refresh.forget(&expired);
        let mut cache = self.metadata_cache.lock().await;
        for topic in &expired {
            cache.remove(topic);
        }
        drop(cache);
        self.partition_leaders
            .retain(|(topic, _), _| !expired.contains(topic));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex as StdMutex,
        atomic::{AtomicI32, AtomicU16, Ordering},
    };

    use assert2::check;
    use bytes::BytesMut;
    use krabka_client_core::MockBroker;
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            metadata_request::{self, MetadataRequest},
            metadata_response::{
                self, MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
                MetadataResponseTopic,
            },
        },
    };

    use super::*;
    use crate::Producer;

    const TOPIC: &str = "orders";
    const CLIENT_ID: &str = "metadata-age-test";

    /// A broker whose partition count and partition 0 leader a test changes.
    /// It records the topics of each Metadata request.
    struct ChangingBroker {
        mock: MockBroker,
        partitions: Arc<AtomicI32>,
        leader: Arc<AtomicI32>,
        requests: Arc<StdMutex<Vec<MetadataRequest>>>,
    }

    async fn changing_broker() -> ChangingBroker {
        let port = Arc::new(AtomicU16::new(0));
        let partitions = Arc::new(AtomicI32::new(4));
        let leader = Arc::new(AtomicI32::new(1));
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let (h_port, h_partitions, h_leader, h_requests) = (
            Arc::clone(&port),
            Arc::clone(&partitions),
            Arc::clone(&leader),
            Arc::clone(&requests),
        );
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                let mut buf = BytesMut::new();
                ApiVersionsResponse {
                    api_keys: vec![ApiVersion {
                        api_key: metadata_request::API_KEY,
                        min_version: 0,
                        max_version: 12,
                        ..Default::default()
                    }],
                    ..Default::default()
                }
                .encode(&mut buf, 0)
                .unwrap();
                return Some(buf.to_vec());
            }
            if api_key != metadata_request::API_KEY {
                return None;
            }
            let header_len =
                2 + CLIENT_ID.len() + usize::from(version >= metadata_request::FLEXIBLE_MIN);
            let mut request_body = &body[header_len..];
            let request = MetadataRequest::decode(&mut request_body, version).unwrap();
            let named = request.topics.as_ref().is_some_and(|topics| {
                topics
                    .iter()
                    .any(|topic| topic.name.as_deref() == Some(TOPIC))
            });
            h_requests.lock().unwrap().push(request);
            let port = i32::from(h_port.load(Ordering::SeqCst));
            let response = MetadataResponse {
                brokers: [1, 2]
                    .into_iter()
                    .map(|node_id| MetadataResponseBroker {
                        node_id,
                        host: "127.0.0.1".into(),
                        port,
                        ..Default::default()
                    })
                    .collect(),
                topics: named
                    .then(|| MetadataResponseTopic {
                        name: Some(TOPIC.into()),
                        partitions: (0..h_partitions.load(Ordering::SeqCst))
                            .map(|partition_index| MetadataResponsePartition {
                                partition_index,
                                leader_id: if partition_index == 0 {
                                    h_leader.load(Ordering::SeqCst)
                                } else {
                                    1
                                },
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    })
                    .into_iter()
                    .collect(),
                ..Default::default()
            };
            let mut buf = BytesMut::new();
            if version >= metadata_response::FLEXIBLE_MIN {
                buf.extend_from_slice(&[0]);
            }
            response.encode(&mut buf, version).unwrap();
            Some(buf.to_vec())
        })
        .await;
        port.store(mock.addr.port(), Ordering::SeqCst);
        ChangingBroker {
            mock,
            partitions,
            leader,
            requests,
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum Change {
        Partitions(i32),
        Leader(i32),
    }

    /// What the producer holds for the topic after the wait.
    #[derive(Debug, PartialEq, Eq)]
    struct Held {
        partitions: Option<i32>,
        leader_of_partition_0: Option<i32>,
    }

    /// Kafka's `Metadata.timeToNextUpdate`: the producer refreshes when the
    /// last successful refresh is older than `metadata.max.age.ms`. The test
    /// uses a 400 ms age in place of Kafka's 5 minutes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_producer_adopts_broker_changes_after_metadata_max_age() {
        let max_age = Duration::from_millis(400);
        let before_age = Duration::from_millis(150);
        let after_age = Duration::from_millis(700);
        for (name, change, wait, expected) in [
            (
                "partitions 4 to 8, after the age",
                Change::Partitions(8),
                after_age,
                Held {
                    partitions: Some(8),
                    leader_of_partition_0: Some(1),
                },
            ),
            (
                "partitions 4 to 8, before the age",
                Change::Partitions(8),
                before_age,
                Held {
                    partitions: Some(4),
                    leader_of_partition_0: Some(1),
                },
            ),
            (
                "leader of partition 0 moves to broker 2, after the age",
                Change::Leader(2),
                after_age,
                Held {
                    partitions: Some(4),
                    leader_of_partition_0: Some(2),
                },
            ),
        ] {
            let broker = changing_broker().await;
            let producer = Producer::builder()
                .bootstrap(broker.mock.addr.to_string())
                .client_id(CLIENT_ID)
                .enable_idempotence(false)
                .metadata_max_age(max_age)
                .build()
                .await
                .expect("producer connects");
            producer
                .partition_count(TOPIC, None)
                .await
                .expect("topic metadata");
            // Start the age at a refresh of the full topic list.
            producer.client.refresh_metadata().await.unwrap();
            match change {
                Change::Partitions(count) => broker.partitions.store(count, Ordering::SeqCst),
                Change::Leader(leader) => broker.leader.store(leader, Ordering::SeqCst),
            }
            tokio::time::sleep(wait).await;
            let held = Held {
                partitions: producer
                    .metadata_cache
                    .lock()
                    .await
                    .get(TOPIC)
                    .map(|topic| topic.num_partitions),
                leader_of_partition_0: producer
                    .partition_leaders
                    .get(&(TOPIC.to_owned(), 0))
                    .map(|leader| *leader),
            };
            broker.mock.stop();
            drop(producer);
            check!(held == expected, "{name}");
        }
    }

    /// Kafka's `ProducerMetadata.retainTopic`: a topic with no send for
    /// `metadata.max.idle.ms` leaves the requests and the caches.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_idle_topic_leaves_the_requests_and_the_caches() {
        let broker = changing_broker().await;
        let client = Client::builder()
            .bootstrap(broker.mock.addr.to_string())
            .client_id(CLIENT_ID)
            .metadata_scope(krabka_client_core::MetadataScope::Topics {
                allow_auto_topic_creation: true,
            })
            .build()
            .await
            .unwrap();
        client.metadata_topics().add(TOPIC);
        let metadata_cache = Arc::new(Mutex::new(HashMap::from([(
            TOPIC.to_owned(),
            TopicMetadata {
                num_partitions: 4,
                topic_id: krabka_protocol::primitives::uuid::Uuid::ZERO,
            },
        )])));
        let partition_leaders = Arc::new(DashMap::from_iter([((TOPIC.to_owned(), 0), 1)]));
        let shutdown = CancellationToken::new();
        let retry_policy = ProducerRetryPolicy::default();
        let metadata_refresh = Arc::new(MetadataRefresh::default());
        let task = tokio::spawn(
            MetadataAge::unchecked(
                &client,
                (Duration::from_millis(300), Duration::from_millis(200)),
                &retry_policy,
            )
            .with_caches(&metadata_cache, &partition_leaders, &metadata_refresh)
            .run(shutdown.clone()),
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
        shutdown.cancel();
        task.await.unwrap();
        let last = broker.requests.lock().unwrap().last().cloned();
        let observed = (
            last,
            metadata_cache.lock().await.contains_key(TOPIC),
            partition_leaders.len(),
        );
        broker.mock.stop();
        check!(
            observed
                == (
                    Some(krabka_client_core::topics_request(Vec::new(), true)),
                    false,
                    0
                )
        );
    }

    #[tokio::test]
    async fn metadata_max_idle_below_five_seconds_is_rejected() {
        let result = Producer::builder()
            .bootstrap("127.0.0.1:9")
            .enable_idempotence(false)
            .metadata_max_idle(Duration::from_millis(4999))
            .build()
            .await;
        check!(matches!(
            result,
            Err(crate::ProducerError::InvalidConfig(message))
                if message == "metadata_max_idle must be at least 5 s, got 4.999s"
        ));
    }
}
