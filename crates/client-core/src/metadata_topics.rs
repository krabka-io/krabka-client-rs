//! The topics that a client names in its `Metadata` requests.

use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use krabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use tokio::time::Instant;

/// Which topics [`crate::Client::refresh_metadata`] asks for.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MetadataScope {
    /// Every topic of the cluster (`MetadataRequest.Builder.allTopics`). A
    /// full-cluster request never makes the broker create a topic. Kafka's
    /// consumer uses it only for a client-side pattern subscription.
    #[default]
    AllTopics,
    /// The topics in [`MetadataTopics`], as Kafka's `ProducerMetadata` and
    /// `ConsumerMetadata` name them. With `allow_auto_topic_creation`, a broker
    /// with `auto.create.topics.enable=true` creates a named topic that does
    /// not exist.
    Topics { allow_auto_topic_creation: bool },
}

/// The topics of a client's metadata requests, with the last time that each
/// topic was used.
///
/// The lock is never held across an `.await`.
#[derive(Debug, Default)]
pub struct MetadataTopics {
    scope: MetadataScope,
    topics: Mutex<BTreeMap<String, Instant>>,
    /// When a request of this scope last succeeded.
    refreshed: Mutex<Option<Instant>>,
}

impl MetadataTopics {
    /// An empty topic set with `scope`.
    #[must_use]
    pub fn new(scope: MetadataScope) -> Self {
        Self {
            scope,
            topics: Mutex::default(),
            refreshed: Mutex::default(),
        }
    }

    /// Record a successful response to [`Self::request`], as Kafka's
    /// `Metadata.update` sets `lastSuccessfulRefreshMs` for a full update.
    pub fn mark_refreshed(&self) {
        *self
            .refreshed
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
    }

    /// When a response to [`Self::request`] last succeeded, or `None` before
    /// the first one.
    #[must_use]
    pub fn last_refreshed(&self) -> Option<Instant> {
        *self
            .refreshed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn topics(&self) -> MutexGuard<'_, BTreeMap<String, Instant>> {
        self.topics.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The scope of the requests.
    #[must_use]
    pub const fn scope(&self) -> MetadataScope {
        self.scope
    }

    /// Add `topic`, or mark it as used now. Kafka's producer calls
    /// `ProducerMetadata.add` for each send. Returns whether the topic is new.
    pub fn add(&self, topic: &str) -> bool {
        self.topics()
            .insert(topic.to_owned(), Instant::now())
            .is_none()
    }

    /// Replace the topics with `topics`, as a consumer subscription change
    /// does (`SubscriptionState.metadataTopics`).
    pub fn set<I, S>(&self, topics: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let now = Instant::now();
        *self.topics() = topics
            .into_iter()
            .map(|topic| (topic.into(), now))
            .collect();
    }

    /// Remove the topics that were not used for `idle` and return their
    /// names, as Kafka's `ProducerMetadata.retainTopic` drops a topic after
    /// `metadata.max.idle.ms`.
    pub fn remove_idle(&self, idle: Duration) -> Vec<String> {
        let now = Instant::now();
        let mut removed = Vec::new();
        self.topics().retain(|topic, used| {
            let keep = now.saturating_duration_since(*used) < idle;
            if !keep {
                removed.push(topic.clone());
            }
            keep
        });
        removed
    }

    /// The topic names, in order.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.topics().keys().cloned().collect()
    }

    /// The `Metadata` request of the scope and the current topics.
    #[must_use]
    pub fn request(&self) -> MetadataRequest {
        match self.scope {
            MetadataScope::AllTopics => MetadataRequest::default(),
            MetadataScope::Topics {
                allow_auto_topic_creation,
            } => topics_request(self.names(), allow_auto_topic_creation),
        }
    }
}

/// A `Metadata` request that names `topics` (`MetadataRequest.Builder`
/// `forTopicNames`). An empty list asks for no topic.
#[must_use]
pub fn topics_request(
    topics: impl IntoIterator<Item = String>,
    allow_auto_topic_creation: bool,
) -> MetadataRequest {
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
        allow_auto_topic_creation,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn requests_follow_the_scope_and_the_topics() {
        let named =
            |names: &[&str], allow| topics_request(names.iter().map(|&n| n.to_owned()), allow);
        let producer = MetadataScope::Topics {
            allow_auto_topic_creation: true,
        };
        let no_auto_create = MetadataScope::Topics {
            allow_auto_topic_creation: false,
        };
        for (name, scope, added, expected) in [
            (
                "all topics",
                MetadataScope::AllTopics,
                vec!["t1"],
                MetadataRequest::default(),
            ),
            ("no topic yet", producer, vec![], named(&[], true)),
            (
                "two sends",
                producer,
                vec!["t1", "t2", "t1"],
                named(&["t1", "t2"], true),
            ),
            (
                "auto creation off",
                no_auto_create,
                vec!["a"],
                named(&["a"], false),
            ),
        ] {
            let topics = MetadataTopics::new(scope);
            for topic in added {
                topics.add(topic);
            }
            check!(topics.request() == expected, "{name}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn set_replaces_and_remove_idle_drops_unused_topics() {
        let topics = MetadataTopics::new(MetadataScope::Topics {
            allow_auto_topic_creation: true,
        });
        topics.set(["a", "b"]);
        check!(topics.names() == vec!["a".to_owned(), "b".to_owned()]);
        tokio::time::advance(Duration::from_secs(200)).await;
        check!(!topics.add("b"));
        check!(topics.add("c"));
        tokio::time::advance(Duration::from_secs(101)).await;
        check!(topics.remove_idle(Duration::from_mins(5)) == vec!["a".to_owned()]);
        check!(topics.names() == vec!["b".to_owned(), "c".to_owned()]);
    }
}
