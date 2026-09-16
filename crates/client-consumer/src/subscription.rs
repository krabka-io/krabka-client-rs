//! The subscription of a consumer: Kafka's `subscribe(Collection)`,
//! `subscribe(Pattern)`, `unsubscribe` and `subscription`.
//!
//! The consumer and its coordinator task share the subscription. The consumer
//! changes it when the application calls `subscribe` or `unsubscribe`. The
//! coordinator task changes the topics of a pattern subscription when the
//! cluster metadata changes. Each change that the task sees makes the next
//! `poll` join the group again, as Kafka's
//! `ConsumerCoordinator.rejoinNeededOrPending` does when the subscription is
//! not the joined one.

use std::{collections::BTreeSet, fmt, sync::Arc};

use krabka_protocol::owned::metadata_response::MetadataResponse;

use crate::{consumer::Consumer, error::ConsumerError};

/// A topic pattern for [`Consumer::subscribe_pattern`]. Kafka's
/// `subscribe(java.util.regex.Pattern)`.
///
/// The consumer calls the matcher for each topic in the cluster metadata and
/// subscribes to the topics that match. Pass for example
/// `move |topic| regex.is_match(topic)`.
#[derive(Clone)]
pub struct TopicPattern {
    matcher: Arc<dyn Fn(&str) -> bool + Send + Sync>,
}

impl TopicPattern {
    /// A pattern that matches the topics for which `matcher` returns `true`.
    pub fn new(matcher: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        Self {
            matcher: Arc::new(matcher),
        }
    }

    /// Whether `topic` matches.
    #[must_use]
    pub fn matches(&self, topic: &str) -> bool {
        (self.matcher)(topic)
    }
}

impl fmt::Debug for TopicPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TopicPattern")
    }
}

/// What a consumer subscribes to.
#[derive(Clone, Debug, Default)]
pub(crate) struct Subscription {
    /// The subscribed topics, sorted. For a pattern, the topics that matched
    /// the last metadata.
    pub topics: Vec<String>,
    /// The pattern of a pattern subscription.
    pub pattern: Option<TopicPattern>,
    /// The regular expression of a broker-side pattern subscription of the
    /// consumer group protocol (KIP-848). Kafka's
    /// `SubscriptionState.subscribedRe2JPattern`: the group coordinator
    /// matches it with RE2J, so the consumer does no matching.
    pub regex: Option<String>,
    /// Kafka's `exclude.internal.topics`: a pattern does not match an
    /// internal topic.
    pub exclude_internal_topics: bool,
    /// The number of `unsubscribe` calls. The coordinator task leaves the
    /// group for each one that it did not see, also when a new subscription
    /// came before it saw the change.
    pub unsubscribes: u64,
    /// The number of changes of the application: `subscribe`,
    /// `subscribe_pattern` and `unsubscribe`. The coordinator task stores the
    /// topics of a pattern only while this number is the one of its metadata
    /// request.
    pub version: u64,
    /// The consumer has a manual assignment. Kafka's
    /// `SubscriptionType.USER_ASSIGNED`.
    pub manual_assignment: bool,
}

impl Subscription {
    /// A subscription to `topics`.
    pub(crate) fn topics(topics: impl IntoIterator<Item = String>, exclude_internal: bool) -> Self {
        Self {
            topics: sorted(topics),
            pattern: None,
            regex: None,
            exclude_internal_topics: exclude_internal,
            unsubscribes: 0,
            version: 0,
            manual_assignment: false,
        }
    }

    /// Whether the consumer subscribes to nothing. Kafka's
    /// `SubscriptionState.subscriptionType == NONE`.
    pub(crate) fn is_none(&self) -> bool {
        self.topics.is_empty() && self.pattern.is_none() && self.regex.is_none()
    }

    /// Whether `topic` is subscribed.
    pub(crate) fn contains(&self, topic: &str) -> bool {
        self.topics
            .binary_search_by(|t| t.as_str().cmp(topic))
            .is_ok()
    }

    /// Store the topics of an assignment of a regular expression
    /// subscription. Return whether they changed.
    ///
    /// The group coordinator matches the regular expression, so the topics of
    /// the subscription come from the assignment. They give `poll` the topics
    /// of its metadata and its fetch filter.
    pub(crate) fn store_assigned_topics(&mut self, assigned: &[String]) -> bool {
        if self.regex.is_none() {
            return false;
        }
        let topics = sorted(assigned.iter().cloned());
        if self.topics == topics {
            return false;
        }
        self.topics = topics;
        true
    }

    /// Store `matched` as the topics of the pattern of `version`. Return
    /// whether the topics changed. A newer change of the application keeps
    /// its subscription.
    pub(crate) fn store_pattern_topics(&mut self, version: u64, matched: &[String]) -> bool {
        if self.version != version || self.pattern.is_none() || self.topics == matched {
            return false;
        }
        self.topics = matched.to_vec();
        true
    }

    /// The topics of `metadata` that the pattern matches, sorted. Kafka's
    /// `ConsumerCoordinator.updatePatternSubscription`: an internal topic
    /// matches only without `exclude.internal.topics`.
    pub(crate) fn matching_topics(&self, metadata: &MetadataResponse) -> Option<Vec<String>> {
        let pattern = self.pattern.as_ref()?;
        Some(sorted(
            metadata
                .topics
                .iter()
                .filter(|topic| !(self.exclude_internal_topics && topic.is_internal))
                .filter_map(|topic| topic.name.clone())
                .filter(|name| pattern.matches(name)),
        ))
    }
}

fn sorted(topics: impl IntoIterator<Item = String>) -> Vec<String> {
    topics
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// The subscription that the consumer and its coordinator task share.
pub(crate) type SharedSubscription = Arc<tokio::sync::watch::Sender<Subscription>>;

/// The shared subscription of a new consumer.
pub(crate) fn shared(
    topics: Vec<String>,
    pattern: Option<TopicPattern>,
    exclude_internal_topics: bool,
) -> SharedSubscription {
    Arc::new(tokio::sync::watch::Sender::new(Subscription {
        pattern,
        ..Subscription::topics(topics, exclude_internal_topics)
    }))
}

/// The `JoinGroup` reason after a subscription change. Kafka's
/// `ConsumerCoordinator.rejoinNeededOrPending`.
pub(crate) fn subscription_changed_reason(joined: &[String], now: &[String]) -> String {
    format!(
        "the subscription has changed from [{}] to [{}] since the last group join",
        joined.join(", "),
        now.join(", ")
    )
}

impl Consumer {
    /// Subscribe to `topics`, in place of the current subscription. Kafka's
    /// `KafkaConsumer.subscribe(Collection)`.
    ///
    /// The next `poll` joins the group again with the new topics. An empty
    /// list is [`unsubscribe`](Self::unsubscribe).
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::InvalidArgument`] for an empty topic name, and
    /// the error of the listener call of `unsubscribe`.
    pub async fn subscribe<I, S>(&mut self, topics: I) -> Result<(), ConsumerError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let topics: Vec<String> = topics.into_iter().map(Into::into).collect();
        if topics.iter().any(String::is_empty) {
            return Err(ConsumerError::InvalidArgument(
                "Topic collection to subscribe to cannot contain null or empty topic".to_owned(),
            ));
        }
        if topics.is_empty() {
            return self.unsubscribe().await;
        }
        self.require_group_membership()?;
        if self.subscription.borrow().manual_assignment {
            return Err(ConsumerError::IllegalState(
                "Subscription to topics, partitions and pattern are mutually exclusive".to_owned(),
            ));
        }
        self.client.metadata_topics().set(topics.iter().cloned());
        // Kafka's `subscribe(topics)` calls
        // `fetcher.clearBufferedDataForUnassignedTopics(topics)`.
        self.fetch_buffer
            .retain_topics(&topics.iter().cloned().collect());
        self.subscription.send_modify(|subscription| {
            subscription.topics = sorted(topics);
            subscription.pattern = None;
            subscription.regex = None;
            subscription.version += 1;
        });
        Ok(())
    }

    /// Subscribe to the topics that `pattern` matches, in place of the current
    /// subscription. Kafka's `KafkaConsumer.subscribe(Pattern)`.
    ///
    /// The coordinator task matches the pattern against all topics of the
    /// cluster at once and each `subscription_metadata_refresh_interval`, and
    /// the next `poll` joins the group again when the matched topics change.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::InvalidGroupId`] without a group id, and
    /// [`ConsumerError::IllegalState`] for a consumer without group
    /// membership.
    pub fn subscribe_pattern(&mut self, pattern: TopicPattern) -> Result<(), ConsumerError> {
        self.require_group_membership()?;
        if self.subscription.borrow().manual_assignment {
            return Err(ConsumerError::IllegalState(
                "Subscription to topics, partitions and pattern are mutually exclusive".to_owned(),
            ));
        }
        // Kafka's `subscribe(pattern)` calls
        // `fetcher.clearBufferedDataForUnassignedPartitions(emptySet())`.
        self.fetch_buffer = crate::fetch_buffer::FetchBuffer::default();
        self.subscription.send_modify(|subscription| {
            subscription.topics.clear();
            subscription.pattern = Some(pattern);
            subscription.regex = None;
            subscription.version += 1;
        });
        Ok(())
    }

    /// Subscribe to the topics that the regular expression `regex` matches,
    /// in place of the current subscription. Kafka's
    /// `KafkaConsumer.subscribe(SubscriptionPattern)` (KIP-848).
    ///
    /// The group coordinator matches the expression with RE2J and assigns the
    /// partitions of the topics that match, so the consumer sends no topic
    /// names and matches nothing itself. The expression must follow the RE2
    /// syntax; a broker that cannot compile it answers with
    /// `INVALID_REGULAR_EXPRESSION`, which `poll` returns.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::InvalidArgument`] for an empty expression,
    /// [`ConsumerError::InvalidGroupId`] without a group id, and
    /// [`ConsumerError::IllegalState`] for a consumer with a manual
    /// assignment or with the classic group protocol.
    pub fn subscribe_regex(&mut self, regex: &str) -> Result<(), ConsumerError> {
        if regex.is_empty() {
            return Err(ConsumerError::InvalidArgument(
                "Topic pattern to subscribe to cannot be empty".to_owned(),
            ));
        }
        self.require_group_membership()?;
        if self.group_protocol != crate::GroupProtocol::Consumer {
            return Err(ConsumerError::IllegalState(
                crate::consumer_group::RE2J_NEEDS_CONSUMER_PROTOCOL.to_owned(),
            ));
        }
        if self.subscription.borrow().manual_assignment {
            return Err(ConsumerError::IllegalState(
                "Subscription to topics, partitions and pattern are mutually exclusive".to_owned(),
            ));
        }
        self.fetch_buffer = crate::fetch_buffer::FetchBuffer::default();
        self.subscription.send_modify(|subscription| {
            // The topics of a regular expression subscription are the topics
            // of its assignment, not a list of the application. They stay
            // until the next assignment replaces them, so `poll` keeps the
            // metadata of the partitions that the member still owns.
            subscription.pattern = None;
            subscription.regex = Some(regex.to_owned());
            subscription.version += 1;
        });
        Ok(())
    }

    /// Give up the subscription and the assigned partitions, and leave the
    /// group. Kafka's `KafkaConsumer.unsubscribe`.
    ///
    /// The rebalance listener gets the owned partitions first, as in `close`.
    /// Then the coordinator task sends `LeaveGroup` with the reason `the
    /// consumer unsubscribed from all topics`. A `poll` without a subscription
    /// returns [`ConsumerError::NotSubscribed`].
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::RebalanceListenerFailed`] when the listener
    /// callback fails. The consumer unsubscribes also then.
    pub async fn unsubscribe(&mut self) -> Result<(), ConsumerError> {
        // Kafka's `unsubscribe` runs `onLeavePrepare` and `maybeLeaveGroup`
        // only with a coordinator, that is with a group.
        let listener_result = if self.coordinator_handle.is_some() {
            self.leave_prepare().await
        } else {
            Ok(())
        };
        self.fetch_buffer = crate::fetch_buffer::FetchBuffer::default();
        // Kafka's `SubscriptionState.unsubscribe` clears the assignment before
        // `unsubscribe` returns. The coordinator task also clears it when it
        // handles the change.
        {
            let mut assigned = self.assigned.lock().await;
            let mut identity = self.commit_identity.lock().await;
            assigned.clear();
            identity.ownership_ids.clear();
        }
        self.next_offsets.lock().await.clear();
        self.positions.lock().await.clear();
        self.end_offsets.lock().await.clear();
        self.subscription.send_modify(|subscription| {
            subscription.topics.clear();
            subscription.pattern = None;
            subscription.regex = None;
            subscription.manual_assignment = false;
            subscription.unsubscribes += 1;
            subscription.version += 1;
        });
        listener_result
    }

    /// The subscribed topics. For a pattern subscription, the topics that
    /// matched the last metadata. Kafka's `KafkaConsumer.subscription`.
    #[must_use]
    pub fn subscription(&self) -> Vec<String> {
        self.subscription.borrow().topics.clone()
    }
}

#[cfg(test)]
mod tests {
    /// The coordinator task stores the topics of a pattern only for the
    /// subscription that its metadata request matched.
    #[test]
    fn pattern_topics_are_stored_only_for_the_matched_version() {
        let matched = vec!["orders-eu".to_owned()];
        let pattern = || Subscription {
            pattern: Some(TopicPattern::new(|topic| topic.starts_with("orders"))),
            version: 3,
            ..Subscription::topics(Vec::new(), true)
        };
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, mut subscription, version, expected) in [
            ("same version", pattern(), 3, (true, matched.clone())),
            ("a newer change", pattern(), 2, (false, vec![])),
            (
                "a topic subscription now",
                Subscription {
                    version: 3,
                    ..Subscription::topics(vec!["payments".to_owned()], true)
                },
                3,
                (false, vec!["payments".to_owned()]),
            ),
        ] {
            let stored = subscription.store_pattern_topics(version, &matched);
            actual.push((name, (stored, subscription.topics)));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    use krabka_protocol::owned::metadata_response::MetadataResponseTopic;

    use super::*;

    #[test]
    fn a_pattern_matches_the_topics_of_the_metadata() {
        let metadata = MetadataResponse {
            topics: [
                ("orders-eu", false),
                ("orders-us", false),
                ("payments", false),
                ("orders-internal", true),
            ]
            .into_iter()
            .map(|(name, is_internal)| MetadataResponseTopic {
                name: Some(name.into()),
                is_internal,
                ..Default::default()
            })
            .collect(),
            ..Default::default()
        };
        let orders = TopicPattern::new(|topic| topic.starts_with("orders"));
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, exclude_internal, expected) in [
            ("exclude internal", true, vec!["orders-eu", "orders-us"]),
            (
                "include internal",
                false,
                vec!["orders-eu", "orders-internal", "orders-us"],
            ),
        ] {
            let subscription = Subscription {
                pattern: Some(orders.clone()),
                ..Subscription::topics(Vec::new(), exclude_internal)
            };
            actual.push((name, subscription.matching_topics(&metadata)));
            wanted.push((
                name,
                Some(expected.into_iter().map(str::to_owned).collect::<Vec<_>>()),
            ));
        }
        assert2::assert!(actual == wanted);
    }
}
