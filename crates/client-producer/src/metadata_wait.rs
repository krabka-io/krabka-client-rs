//! The wait of `Producer::send` for the metadata of its topic.
//!
//! Kafka's `KafkaProducer.waitOnMetadata` blocks `send` until the metadata
//! holds the topic, and holds the requested partition when the record names
//! one. It waits for at most `max.block.ms`, and then throws
//! `TimeoutException`. It never picks a partition from a guessed partition
//! count. [`MetadataWait`] gives the same rules to this producer.

use std::{
    collections::{BTreeSet, HashMap},
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use dashmap::DashMap;
use krabka_client_core::ClientError;
use krabka_protocol::owned::{
    metadata_request::{MetadataRequest, MetadataRequestTopic},
    metadata_response::MetadataResponse,
};
use tokio::{sync::Mutex, time::Instant};

use crate::{error::ProducerError, producer::TopicMetadata};

/// The broker error code for no error.
const NONE: i16 = 0;
/// `INVALID_TOPIC_EXCEPTION`. Kafka's `Metadata.maybeThrowExceptionForTopic`
/// throws `InvalidTopicException` for it, so the wait stops at once.
const INVALID_TOPIC_EXCEPTION: i16 = 17;
/// `TOPIC_AUTHORIZATION_FAILED`. Kafka's `Metadata.maybeThrowExceptionForTopic`
/// throws `TopicAuthorizationException` for it, so the wait stops at once.
const TOPIC_AUTHORIZATION_FAILED: i16 = 29;

/// The metadata refreshes that the waits of one producer share.
///
/// Many sends can wait for the same topic at the same time. Kafka's producer
/// then sends one metadata request, and every waiter reads its result. Here a
/// waiter that finds a response newer than its last look uses that response
/// when the request named its topic, and does not send a request of its own.
#[derive(Debug, Default)]
pub(crate) struct MetadataRefresh {
    /// The number of responses stored in `last`. A waiter reads it before it
    /// looks at the cache, so it can see a response that came after that look.
    generation: AtomicU64,
    /// The last response. A failed refresh clears it. The lock also makes the
    /// waiters send their refreshes one at a time.
    last: Mutex<Option<Refreshed>>,
    /// The topics that the requests name. The lock is never held across an
    /// `.await`.
    topics: std::sync::Mutex<ProducerTopics>,
}

/// A response and the topics that its request named.
#[derive(Debug)]
struct Refreshed {
    topics: Vec<String>,
    response: Arc<MetadataResponse>,
}

/// The topics of Kafka's `ProducerMetadata`.
#[derive(Debug, Default)]
struct ProducerTopics {
    /// Every topic that a send waited for (`ProducerMetadata.topics`).
    known: BTreeSet<String>,
    /// The known topics that no response has listed yet
    /// (`ProducerMetadata.newTopics`).
    new: BTreeSet<String>,
}

impl ProducerTopics {
    /// The topics of the next request. Kafka's `Metadata.newMetadataRequestAndVersion`
    /// asks only for the new topics while there are some
    /// (`newMetadataRequestBuilderForNewTopics`), and otherwise for all known
    /// topics (`newMetadataRequestBuilder`).
    fn next_request(&self) -> Vec<String> {
        let topics = if self.new.is_empty() {
            &self.known
        } else {
            &self.new
        };
        topics.iter().cloned().collect()
    }
}

/// The `Metadata` request for `topics`. Kafka's `ProducerMetadata` builds
/// `new MetadataRequest.Builder(topics, true)`, so the broker may create a
/// topic that does not exist.
pub(crate) fn metadata_request(topics: Vec<String>) -> MetadataRequest {
    MetadataRequest {
        topics: Some(
            topics
                .into_iter()
                .map(|name| MetadataRequestTopic {
                    name: Some(name),
                    ..Default::default()
                })
                .collect(),
        ),
        allow_auto_topic_creation: true,
        ..Default::default()
    }
}

impl MetadataRefresh {
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn topics(&self) -> std::sync::MutexGuard<'_, ProducerTopics> {
        self.topics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Remove `topics` from the topics that the requests name, as Kafka's
    /// `ProducerMetadata.retainTopic` drops an idle topic.
    pub(crate) fn forget(&self, topics: &[String]) {
        let mut known = self.topics();
        for topic in topics {
            known.known.remove(topic);
            known.new.remove(topic);
        }
    }

    /// Add `topic` to the topics that the requests name. Kafka's
    /// `KafkaProducer.waitOnMetadata` calls `ProducerMetadata.add` first.
    fn add(&self, topic: &str) {
        let mut topics = self.topics();
        if topics.known.insert(topic.to_owned()) {
            topics.new.insert(topic.to_owned());
        }
    }

    /// Return a response newer than generation `seen` whose request named
    /// `topic`, or send a refresh.
    async fn newer_than<F, Fut>(
        &self,
        seen: &mut u64,
        topic: &str,
        refresh: &mut F,
    ) -> Result<Arc<MetadataResponse>, ClientError>
    where
        F: FnMut(Vec<String>) -> Fut,
        Fut: Future<Output = Result<MetadataResponse, ClientError>>,
    {
        let mut last = self.last.lock().await;
        let current = self.generation();
        if current != *seen
            && let Some(refreshed) = last.as_ref()
            && refreshed.topics.iter().any(|named| named == topic)
        {
            *seen = current;
            return Ok(Arc::clone(&refreshed.response));
        }
        let topics = self.topics().next_request();
        match refresh(topics.clone()).await {
            Ok(response) => {
                {
                    // Kafka's `ProducerMetadata.update` removes each topic of
                    // the response from the new topics.
                    let mut known = self.topics();
                    for entry in &response.topics {
                        if let Some(name) = entry.name.as_deref() {
                            known.new.remove(name);
                        }
                    }
                }
                let response = Arc::new(response);
                *last = Some(Refreshed {
                    topics,
                    response: Arc::clone(&response),
                });
                *seen = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
                Ok(response)
            }
            Err(error) => {
                *last = None;
                Err(error)
            }
        }
    }
}

/// What a metadata response says about one topic.
#[derive(Debug, PartialEq, Eq)]
enum TopicLookup {
    /// The topic has partitions.
    Known(TopicMetadata),
    /// The broker gave an error that stops the wait.
    Fatal(i16),
    /// The topic is absent, has no partitions, or has a retriable error.
    Unknown,
}

fn lookup(response: &MetadataResponse, topic: &str) -> TopicLookup {
    let Some(entry) = response
        .topics
        .iter()
        .find(|entry| entry.name.as_deref() == Some(topic))
    else {
        return TopicLookup::Unknown;
    };
    match entry.error_code {
        // Kafka's `Cluster.partitionCountForTopic` gives no count for a topic
        // without partitions, so the wait goes on.
        NONE => match i32::try_from(entry.partitions.len()) {
            Ok(num_partitions) if num_partitions > 0 => TopicLookup::Known(TopicMetadata {
                num_partitions,
                topic_id: entry.topic_id,
            }),
            _ => TopicLookup::Unknown,
        },
        code @ (INVALID_TOPIC_EXCEPTION | TOPIC_AUTHORIZATION_FAILED) => TopicLookup::Fatal(code),
        _ => TopicLookup::Unknown,
    }
}

/// Whether a failed refresh stops the wait.
///
/// Kafka's `NetworkClient` gives `Metadata.fatalError` an authentication
/// failure and an unsupported version, and `KafkaProducer.waitOnMetadata`
/// throws them at once. A disconnect or a timeout only counts as a failed
/// update, and the wait goes on.
///
/// Client-core decides which handshake failures are rejections. A TLS engine
/// error and a SASL rejection are [`ClientError::Authentication`]. A
/// [`ClientError::Tls`] or [`ClientError::Sasl`] failed with no verdict from
/// the peer, for example on EOF or a reset. Kafka's `NetworkClient` keeps such
/// a disconnect in the `AUTHENTICATE` state retriable, so the wait goes on.
fn stops_the_wait(error: &ClientError) -> bool {
    error.is_authentication_failure()
        || matches!(
            error,
            ClientError::IncompatibleVersion { .. } | ClientError::InvalidConfig(_)
        )
}

/// Run `future` until `deadline`. No deadline means no limit.
async fn before<F: Future>(deadline: Option<Instant>, future: F) -> Option<F::Output> {
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, future).await.ok(),
        None => Some(future.await),
    }
}

/// The state and limits of one producer's metadata waits.
pub(crate) struct MetadataWait<'a> {
    pub cache: &'a Mutex<HashMap<String, TopicMetadata>>,
    pub partition_leaders: &'a DashMap<(String, i32), i32>,
    pub refresh: &'a MetadataRefresh,
    /// Kafka's `max.block.ms`.
    pub max_block: Duration,
    /// The first pause between two refreshes. Kafka's `retry.backoff.ms`.
    pub retry_backoff: Duration,
    /// The longest pause between two refreshes. Kafka's
    /// `retry.backoff.max.ms`.
    pub max_backoff: Duration,
}

impl MetadataWait<'_> {
    /// Return the partition count of `topic`, and wait for metadata that
    /// holds the topic, and `partition` when it is `Some`.
    ///
    /// `refresh` sends one metadata request for the given topics. The request
    /// names `topic` and the other topics of the producer, as Kafka's
    /// `ProducerMetadata` does. The wait sends it again after a
    /// pause that starts at `retry_backoff` and doubles up to `max_backoff`,
    /// as Kafka's `Metadata.timeToAllowUpdate` does (without jitter).
    ///
    /// # Errors
    ///
    /// [`ProducerError::MetadataTimeout`] when `max_block` ends first.
    /// [`ProducerError::Server`] for `INVALID_TOPIC_EXCEPTION` or
    /// `TOPIC_AUTHORIZATION_FAILED` on the topic. [`ProducerError::Client`]
    /// for a refresh failure that Kafka treats as fatal.
    pub async fn partition_count<F, Fut>(
        &self,
        topic: &str,
        partition: Option<i32>,
        mut refresh: F,
    ) -> Result<i32, ProducerError>
    where
        F: FnMut(Vec<String>) -> Fut,
        Fut: Future<Output = Result<MetadataResponse, ClientError>>,
    {
        let mut seen = self.refresh.generation();
        let mut known = self
            .cache
            .lock()
            .await
            .get(topic)
            .map(|meta| meta.num_partitions);
        let covers = |count: i32| partition.is_none_or(|partition| partition < count);
        if let Some(count) = known
            && covers(count)
        {
            return Ok(count);
        }

        self.refresh.add(topic);
        let deadline = Instant::now().checked_add(self.max_block);
        let timeout = |partition_count: Option<i32>| ProducerError::MetadataTimeout {
            topic: topic.to_owned(),
            partition,
            partition_count,
            waited: self.max_block,
        };
        let mut backoff = self.retry_backoff;
        loop {
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(timeout(known));
            }
            let Some(attempt) = before(
                deadline,
                self.refresh.newer_than(&mut seen, topic, &mut refresh),
            )
            .await
            else {
                return Err(timeout(known));
            };
            match attempt {
                Ok(response) => match lookup(&response, topic) {
                    TopicLookup::Known(meta) => {
                        let count = meta.num_partitions;
                        self.store(topic, meta, &response).await;
                        if covers(count) {
                            return Ok(count);
                        }
                        known = Some(count);
                    }
                    TopicLookup::Fatal(code) => return Err(ProducerError::Server(code)),
                    TopicLookup::Unknown => {}
                },
                Err(error) if stops_the_wait(&error) => return Err(ProducerError::Client(error)),
                Err(_) => {}
            }
            if before(deadline, tokio::time::sleep(backoff))
                .await
                .is_none()
            {
                return Err(timeout(known));
            }
            backoff = backoff.saturating_mul(2).min(self.max_backoff);
        }
    }

    /// Cache the count and id of `topic`, and the leader of each partition,
    /// so the sender can route each Produce to the partition leader.
    async fn store(&self, topic: &str, meta: TopicMetadata, response: &MetadataResponse) {
        for entry in response
            .topics
            .iter()
            .filter(|entry| entry.name.as_deref() == Some(topic))
        {
            for partition in &entry.partitions {
                self.partition_leaders.insert(
                    (topic.to_owned(), partition.partition_index),
                    partition.leader_id,
                );
            }
        }
        self.cache.lock().await.insert(topic.to_owned(), meta);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use krabka_client_core::{AuthenticationError, OutboundSaslError, SaslAuthenticationError};
    use krabka_protocol::{
        owned::metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
        primitives::uuid::Uuid,
    };
    use krabka_units::secs;

    use super::*;

    const TOPIC: &str = "orders";
    const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    const LEADER_NOT_AVAILABLE: i16 = 5;
    const UNKNOWN_SERVER_ERROR: i16 = -1;
    const ADDR: &str = "127.0.0.1:9093";
    const RETRY_BACKOFF: Duration = Duration::from_millis(100);
    const MAX_BACKOFF: Duration = Duration::from_secs(1);
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

    /// One scripted answer to a refresh.
    #[derive(Clone, Copy)]
    enum Answer {
        Topic {
            error_code: i16,
            partitions: i32,
        },
        /// The request times out after `REQUEST_TIMEOUT`.
        Silent,
        /// The refresh fails at once with the error that the function makes.
        Fails(fn() -> ClientError),
    }

    fn topic(error_code: i16, partitions: i32) -> Answer {
        Answer::Topic {
            error_code,
            partitions,
        }
    }

    fn response(error_code: i16, partitions: i32) -> MetadataResponse {
        MetadataResponse {
            topics: vec![MetadataResponseTopic {
                error_code,
                name: Some(TOPIC.into()),
                topic_id: Uuid([7; 16]),
                partitions: (0..partitions)
                    .map(|partition_index| MetadataResponsePartition {
                        partition_index,
                        leader_id: 1,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// A refresh that gives the scripted answers in order, and repeats the
    /// last one.
    struct Script {
        answers: std::sync::Mutex<VecDeque<Answer>>,
        refreshes: AtomicUsize,
    }

    impl Script {
        fn new(answers: Vec<Answer>) -> Self {
            Self {
                answers: std::sync::Mutex::new(answers.into()),
                refreshes: AtomicUsize::new(0),
            }
        }

        async fn refresh(&self) -> Result<MetadataResponse, ClientError> {
            self.refreshes.fetch_add(1, Ordering::SeqCst);
            let next = {
                let mut answers = self.answers.lock().expect("script");
                if answers.len() > 1 {
                    answers.pop_front().expect("answer")
                } else {
                    *answers.front().expect("answer")
                }
            };
            match next {
                Answer::Topic {
                    error_code,
                    partitions,
                } => Ok(response(error_code, partitions)),
                Answer::Silent => {
                    tokio::time::sleep(REQUEST_TIMEOUT).await;
                    Err(ClientError::Timeout(secs(1)))
                }
                Answer::Fails(error) => Err(error()),
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct WaitOutcome {
        result: Result<i32, String>,
        waited: Duration,
        refreshes: usize,
        cached: Option<TopicMetadata>,
    }

    struct Producerless {
        cache: Mutex<HashMap<String, TopicMetadata>>,
        partition_leaders: DashMap<(String, i32), i32>,
        refresh: MetadataRefresh,
    }

    impl Producerless {
        fn new() -> Self {
            Self {
                cache: Mutex::new(HashMap::new()),
                partition_leaders: DashMap::new(),
                refresh: MetadataRefresh::default(),
            }
        }

        fn wait(&self, max_block: Duration) -> MetadataWait<'_> {
            MetadataWait {
                cache: &self.cache,
                partition_leaders: &self.partition_leaders,
                refresh: &self.refresh,
                max_block,
                retry_backoff: RETRY_BACKOFF,
                max_backoff: MAX_BACKOFF,
            }
        }
    }

    fn known(num_partitions: i32) -> TopicMetadata {
        TopicMetadata {
            num_partitions,
            topic_id: Uuid([7; 16]),
        }
    }

    /// Run one wait against the scripted answers.
    async fn run_wait(
        answers: Vec<Answer>,
        max_block: Duration,
        partition: Option<i32>,
    ) -> WaitOutcome {
        let state = Producerless::new();
        let script = Script::new(answers);
        let started = Instant::now();
        let result = state
            .wait(max_block)
            .partition_count(TOPIC, partition, |_| script.refresh())
            .await
            .map_err(|error| error.to_string());
        WaitOutcome {
            result,
            waited: started.elapsed(),
            refreshes: script.refreshes.load(Ordering::SeqCst),
            cached: state.cache.lock().await.get(TOPIC).cloned(),
        }
    }

    /// A retriable answer makes the wait ask again after the backoff, until
    /// the metadata holds the topic and partition or `max_block` ends. Each
    /// row checks the result, the time the wait took, the refreshes it sent,
    /// and what it cached. Paused time makes the backoff and the limit exact.
    #[tokio::test(start_paused = true)]
    async fn retriable_answers_wait_for_the_topic_until_max_block() {
        let minute = Duration::from_mins(1);
        let short = Duration::from_millis(100);
        let ms = Duration::from_millis;
        for (name, answers, max_block, partition, expected) in [
            (
                "the topic appears on the second refresh",
                vec![topic(UNKNOWN_TOPIC_OR_PARTITION, 0), topic(0, 12)],
                minute,
                None,
                WaitOutcome {
                    result: Ok(12),
                    waited: ms(100),
                    refreshes: 2,
                    cached: Some(known(12)),
                },
            ),
            (
                "the topic never appears",
                vec![topic(UNKNOWN_TOPIC_OR_PARTITION, 0)],
                short,
                None,
                WaitOutcome {
                    result: Err("Topic orders not present in metadata after 100 ms.".into()),
                    waited: ms(100),
                    refreshes: 1,
                    cached: None,
                },
            ),
            (
                "the backoff doubles up to its cap, then max_block ends",
                vec![topic(UNKNOWN_TOPIC_OR_PARTITION, 0)],
                Duration::from_millis(2_500),
                None,
                WaitOutcome {
                    result: Err("Topic orders not present in metadata after 2500 ms.".into()),
                    // Refreshes at 0, 100, 300, 700 and 1500 ms. The pause
                    // after 1500 ms is 1 s, capped, and ends at the limit.
                    waited: ms(2_500),
                    refreshes: 5,
                    cached: None,
                },
            ),
            (
                "a request timeout, then the topic",
                vec![Answer::Silent, topic(0, 12)],
                minute,
                None,
                WaitOutcome {
                    result: Ok(12),
                    waited: ms(1_100),
                    refreshes: 2,
                    cached: Some(known(12)),
                },
            ),
            (
                "max_block ends during a silent request",
                vec![Answer::Silent],
                ms(300),
                None,
                WaitOutcome {
                    result: Err("Topic orders not present in metadata after 300 ms.".into()),
                    waited: ms(300),
                    refreshes: 1,
                    cached: None,
                },
            ),
            (
                "a disconnect, then the topic",
                vec![Answer::Fails(|| ClientError::Disconnected), topic(0, 3)],
                minute,
                None,
                WaitOutcome {
                    result: Ok(3),
                    waited: ms(100),
                    refreshes: 2,
                    cached: Some(known(3)),
                },
            ),
            (
                "a SASL stream failure, then the topic",
                vec![
                    Answer::Fails(|| ClientError::Sasl {
                        addr: ADDR.parse().expect("address"),
                        source: OutboundSaslError::Io(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "eof",
                        )),
                    }),
                    topic(0, 3),
                ],
                minute,
                None,
                WaitOutcome {
                    result: Ok(3),
                    waited: ms(100),
                    refreshes: 2,
                    cached: Some(known(3)),
                },
            ),
            (
                "a SaslAuthenticate code that is not a rejection, then the topic",
                vec![
                    Answer::Fails(|| ClientError::Sasl {
                        addr: ADDR.parse().expect("address"),
                        source: OutboundSaslError::Server {
                            error_code: UNKNOWN_SERVER_ERROR,
                            error_message: None,
                        },
                    }),
                    topic(0, 3),
                ],
                minute,
                None,
                WaitOutcome {
                    result: Ok(3),
                    waited: ms(100),
                    refreshes: 2,
                    cached: Some(known(3)),
                },
            ),
            (
                "leader not available, then the topic",
                vec![topic(LEADER_NOT_AVAILABLE, 0), topic(0, 12)],
                minute,
                None,
                WaitOutcome {
                    result: Ok(12),
                    waited: ms(100),
                    refreshes: 2,
                    cached: Some(known(12)),
                },
            ),
            (
                "a topic without partitions is not known",
                vec![topic(0, 0), topic(0, 2)],
                minute,
                None,
                WaitOutcome {
                    result: Ok(2),
                    waited: ms(100),
                    refreshes: 2,
                    cached: Some(known(2)),
                },
            ),
            (
                "an explicit partition beyond the count",
                vec![topic(0, 4)],
                short,
                Some(7),
                WaitOutcome {
                    result: Err(
                        "Partition 7 of topic orders with partition count 4 is not present in metadata after 100 ms."
                            .into(),
                    ),
                    waited: ms(100),
                    refreshes: 1,
                    cached: Some(known(4)),
                },
            ),
            (
                "an explicit partition after the count grows",
                vec![topic(0, 4), topic(0, 12)],
                minute,
                Some(7),
                WaitOutcome {
                    result: Ok(12),
                    waited: ms(100),
                    refreshes: 2,
                    cached: Some(known(12)),
                },
            ),
        ] {
            assert2::assert!(run_wait(answers, max_block, partition).await == expected, "{name}");
        }
    }

    /// An answer that Kafka treats as fatal stops the wait at once, and a
    /// zero `max_block` times out before the first refresh.
    #[tokio::test(start_paused = true)]
    async fn fatal_answers_stop_the_wait_at_once() {
        let minute = Duration::from_mins(1);
        let ms = Duration::from_millis;
        for (name, answers, max_block, partition, expected) in [
            (
                "topic authorization failed stops the wait",
                vec![topic(TOPIC_AUTHORIZATION_FAILED, 0)],
                minute,
                None,
                WaitOutcome {
                    result: Err("broker error_code 29".into()),
                    waited: Duration::ZERO,
                    refreshes: 1,
                    cached: None,
                },
            ),
            (
                "an invalid topic stops the wait",
                vec![topic(INVALID_TOPIC_EXCEPTION, 0)],
                minute,
                None,
                WaitOutcome {
                    result: Err("broker error_code 17".into()),
                    waited: Duration::ZERO,
                    refreshes: 1,
                    cached: None,
                },
            ),
            (
                "an unsupported version stops the wait",
                vec![Answer::Fails(|| ClientError::IncompatibleVersion {
                    api_key: 3,
                    broker_min: 0,
                    broker_max: 0,
                    client_min: 1,
                    client_max: 12,
                })],
                minute,
                None,
                WaitOutcome {
                    result: Err("client: incompatible version: broker supports 0..=0, client wants 1..=12 for api_key 3".into()),
                    waited: Duration::ZERO,
                    refreshes: 1,
                    cached: None,
                },
            ),
            (
                "a TLS rejection stops the wait",
                vec![Answer::Fails(|| ClientError::Authentication {
                    addr: ADDR.parse().expect("address"),
                    source: AuthenticationError::Tls(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "bad certificate",
                    )),
                })],
                minute,
                None,
                WaitOutcome {
                    result: Err("client: authentication with 127.0.0.1:9093 failed: TLS handshake failed: bad certificate".into()),
                    waited: Duration::ZERO,
                    refreshes: 1,
                    cached: None,
                },
            ),
            (
                "a SASL rejection stops the wait",
                vec![Answer::Fails(|| ClientError::Authentication {
                    addr: ADDR.parse().expect("address"),
                    source: AuthenticationError::Sasl(SaslAuthenticationError::Failed(
                        "bad password".into(),
                    )),
                })],
                minute,
                None,
                WaitOutcome {
                    result: Err("client: authentication with 127.0.0.1:9093 failed: SASL authentication failed: bad password".into()),
                    waited: Duration::ZERO,
                    refreshes: 1,
                    cached: None,
                },
            ),
            (
                "a reset TLS handshake is retried",
                vec![
                    Answer::Fails(|| ClientError::Tls {
                        addr: ADDR.parse().expect("address"),
                        source: std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset"),
                    }),
                    topic(0, 1),
                ],
                minute,
                None,
                WaitOutcome {
                    result: Ok(1),
                    waited: ms(100),
                    refreshes: 2,
                    cached: Some(known(1)),
                },
            ),
            (
                "a zero max_block times out before a refresh",
                vec![topic(0, 1)],
                Duration::ZERO,
                None,
                WaitOutcome {
                    result: Err("Topic orders not present in metadata after 0 ms.".into()),
                    waited: Duration::ZERO,
                    refreshes: 0,
                    cached: None,
                },
            ),
        ] {
            assert2::assert!(run_wait(answers, max_block, partition).await == expected, "{name}");
        }
    }

    /// A cached count that covers the record answers at once, with no
    /// refresh. The leaders of a found topic go to the leader map.
    #[tokio::test(start_paused = true)]
    async fn a_cached_count_answers_without_a_refresh() {
        let state = Producerless::new();
        let script = Script::new(vec![topic(0, 2)]);
        let wait = state.wait(Duration::from_mins(1));
        let first = wait
            .partition_count(TOPIC, None, |_| script.refresh())
            .await
            .expect("topic found");
        let second = wait
            .partition_count(TOPIC, Some(1), |_| script.refresh())
            .await
            .expect("cached");
        let mut leaders: Vec<_> = state
            .partition_leaders
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();
        leaders.sort();
        assert2::assert!((first, second) == (2, 2));
        assert2::assert!(script.refreshes.load(Ordering::SeqCst) == 1);
        assert2::assert!(leaders == vec![((TOPIC.to_owned(), 0), 1), ((TOPIC.to_owned(), 1), 1)]);
    }

    /// The refresh names the awaited topic. While a topic is new, the request
    /// names only the new topics. After a response lists the topic, the
    /// request names every topic of the producer. Kafka's `ProducerMetadata`
    /// builds its requests in the same way.
    #[tokio::test(start_paused = true)]
    async fn the_refresh_names_the_topics_of_the_producer() {
        let state = Producerless::new();
        let requests = std::sync::Mutex::new(Vec::new());
        let refresh = |topics: Vec<String>| {
            let answer = {
                let mut requests = requests.lock().expect("requests");
                requests.push(topics.clone());
                MetadataResponse {
                    topics: topics
                        .iter()
                        .map(|name| {
                            // `payments` has no leader on its first listing.
                            let pending = name == "payments" && requests.len() == 2;
                            MetadataResponseTopic {
                                error_code: if pending { LEADER_NOT_AVAILABLE } else { 0 },
                                name: Some(name.clone()),
                                partitions: if pending {
                                    Vec::new()
                                } else {
                                    vec![MetadataResponsePartition::default()]
                                },
                                ..Default::default()
                            }
                        })
                        .collect(),
                    ..Default::default()
                }
            };
            async move { Ok(answer) }
        };
        let wait = state.wait(Duration::from_mins(1));
        let orders = wait.partition_count("orders", None, refresh).await;
        let payments = wait.partition_count("payments", None, refresh).await;
        // A partition beyond the count makes a known topic ask again, and
        // the request names every topic. A short limit ends the wait after
        // that one request.
        let beyond = state
            .wait(Duration::from_millis(50))
            .partition_count("orders", Some(1), refresh)
            .await
            .map_err(|error| error.to_string());
        let named = |topics: &[&str]| topics.iter().map(ToString::to_string).collect::<Vec<_>>();
        assert2::assert!(
            (
                orders.map_err(|error| error.to_string()),
                payments.map_err(|error| error.to_string()),
                beyond,
                requests.into_inner().expect("requests"),
            ) == (
                Ok(1),
                Ok(1),
                Err(
                    "Partition 1 of topic orders with partition count 1 is not present in metadata after 50 ms."
                        .into()
                ),
                vec![
                    named(&["orders"]),
                    named(&["payments"]),
                    named(&["orders", "payments"]),
                    named(&["orders", "payments"]),
                ],
            )
        );
    }

    /// `metadata_request` names the topics and allows auto topic creation,
    /// as Kafka's `ProducerMetadata.newMetadataRequestBuilder` does.
    #[test]
    fn the_metadata_request_names_the_topics_and_allows_auto_creation() {
        let topic = |name: &str| MetadataRequestTopic {
            name: Some(name.into()),
            ..Default::default()
        };
        assert2::assert!(
            metadata_request(vec!["orders".into(), "payments".into()])
                == MetadataRequest {
                    topics: Some(vec![topic("orders"), topic("payments")]),
                    allow_auto_topic_creation: true,
                    ..Default::default()
                }
        );
    }

    /// Waits that start together share one refresh, as the waiters of Kafka's
    /// producer share one metadata update.
    #[tokio::test(start_paused = true)]
    async fn concurrent_waits_share_a_refresh() {
        let state = Producerless::new();
        let script = Script::new(vec![topic(UNKNOWN_TOPIC_OR_PARTITION, 0), topic(0, 6)]);
        let wait = state.wait(Duration::from_mins(1));
        let results = futures::future::join_all(
            (0..8).map(|_| wait.partition_count(TOPIC, None, |_| script.refresh())),
        )
        .await
        .into_iter()
        .map(|result| result.map_err(|error| error.to_string()))
        .collect::<Vec<_>>();
        assert2::assert!(results == vec![Ok(6); 8]);
        assert2::assert!(script.refreshes.load(Ordering::SeqCst) == 2);
    }
}
