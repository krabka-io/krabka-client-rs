//! Error type for `krabka-client-consumer`.

use std::collections::BTreeSet;

use thiserror::Error;

/// Errors returned by [`Consumer`](crate::Consumer).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConsumerError {
    #[error("client: {0}")]
    Client(#[from] krabka_client_core::ClientError),

    #[error("protocol: {0}")]
    Protocol(#[from] krabka_protocol::ProtocolError),

    #[error("rebalance failed: {0}")]
    RebalanceFailed(String),

    /// A rebalance listener callback failed. Kafka's `poll` throws a
    /// `KafkaException` with the cause.
    #[error("rebalance listener failed: {0}")]
    RebalanceListenerFailed(String),

    /// A builder setting is not valid. Kafka's `ConfigException`.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("startup failed after joining group: {0}")]
    StartupAfterJoin(Box<ConsumerError>),

    #[error("not subscribed to any topic")]
    NotSubscribed,

    #[error("illegal state: {0}")]
    IllegalState(String),

    /// An argument of a call is not valid. Kafka's `IllegalArgumentException`.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The broker answered `INVALID_TOPIC_EXCEPTION` for the topic. Kafka's
    /// `InvalidTopicException`.
    #[error("topic '{0}' is invalid")]
    InvalidTopic(String),

    /// [`WakeupHandle::wakeup`](crate::WakeupHandle::wakeup) woke the call.
    /// Kafka's `WakeupException`.
    #[error("the consumer was woken up")]
    Wakeup,

    /// The call needs a group, and the consumer has no group id. Kafka's
    /// `InvalidGroupIdException`.
    #[error(
        "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer configuration."
    )]
    InvalidGroupId,

    /// A call did not complete before its timeout. Kafka's
    /// `TimeoutException`.
    ///
    /// `cause` is the last retriable error when a retry loop ran out of time,
    /// as Kafka's `CommitRequestManager.maybeWrapAsTimeoutException` wraps it.
    /// It is `None` when the call ran out of time while it waited.
    #[error("timeout: {message}")]
    Timeout {
        message: String,
        #[source]
        cause: Option<Box<ConsumerError>>,
    },

    #[error("invalid seek offset {0}: must be non-negative")]
    InvalidOffset(i64),

    /// The call names a partition that the consumer does not own. Kafka's
    /// `SubscriptionState.assignedState` raises `IllegalStateException`.
    #[error("no current assignment for partition {topic}-{partition}")]
    NoCurrentAssignment { topic: String, partition: i32 },

    /// An asynchronous commit failed with a retriable error. A later commit
    /// can succeed. Kafka's consumer gives `RetriableCommitFailedException` to
    /// the `OffsetCommitCallback`.
    #[error("offset commit failed with a retriable exception: {0}")]
    RetriableCommitFailed(Box<ConsumerError>),

    #[error("commit conflict: rejoined since this poll")]
    CommitInvalid,

    #[error("coordinator unavailable")]
    CoordinatorUnavailable,

    #[error(
        "log truncation detected on {topic}-{partition}: fetch offset {fetch_offset} is past the leader's log; safe offset {safe_offset}"
    )]
    LogTruncation {
        topic: String,
        partition: i32,
        fetch_offset: i64,
        safe_offset: i64,
    },

    #[error("broker error_code {0}")]
    Server(i16),

    /// The coordinator answered `GROUP_AUTHORIZATION_FAILED` (30) for the
    /// group. Kafka's consumer raises `GroupAuthorizationException`.
    #[error("not authorized to access group: {0}")]
    GroupAuthorizationFailed(String),

    /// The coordinator answered `TOPIC_AUTHORIZATION_FAILED` (29) for
    /// partitions of these topics. Kafka's consumer raises
    /// `TopicAuthorizationException`.
    #[error(
        "not authorized to access topics: [{}]",
        .0.iter().map(String::as_str).collect::<Vec<_>>().join(", ")
    )]
    TopicAuthorizationFailed(BTreeSet<String>),

    /// An `OffsetFetch` response carried an error code that Kafka's consumer
    /// does not retry.
    #[error("unexpected error in offset fetch response: error_code {0}")]
    OffsetFetchFailed(i16),

    /// The coordinator answered `FENCED_INSTANCE_ID` (82) for this
    /// `group.instance.id`, because another consumer joined with the same
    /// id. `poll` returns this error once. The next `poll` joins the group
    /// again. Kafka's consumer raises `FencedInstanceIdException`.
    #[error(
        "fenced group.instance.id {0}: another consumer with the same group.instance.id joined the group"
    )]
    FencedInstanceId(String),

    /// A commit found that the consumer is not part of an active group:
    /// a fatal coordinator error removed the member and no `poll` joined the
    /// group again yet, or the coordinator task stopped. Kafka's consumer raises
    /// `CommitFailedException`.
    #[error(
        "offset commit failed: the consumer is not part of an active group; it is likely that the consumer was kicked out of the group"
    )]
    CommitFailed,

    /// The coordinator answered `ILLEGAL_GENERATION` (22), `UNKNOWN_MEMBER_ID`
    /// (25) or `FENCED_INSTANCE_ID` (82) while the group was still
    /// `PREPARING_REBALANCE`, or `REBALANCE_IN_PROGRESS` (27). Kafka's
    /// `ConsumerCoordinator.OffsetCommitResponseHandler` raises
    /// `RebalanceInProgressException` for these, and `commitOffsetsSync` does
    /// not retry: the offsets are not committed, and the caller must call
    /// `poll()` to finish the rebalance before it commits again.
    #[error(
        "offset commit cannot be completed since the consumer is undergoing a rebalance for group {0}: call poll() and retry"
    )]
    RebalanceInProgress(String),

    /// A partition has no committed offset and `auto.offset.reset = none`.
    /// Kafka never auto-resets under `none`
    /// (`SubscriptionState.resetInitializingPositions`); it raises
    /// `NoOffsetForPartitionException` from `poll()` and `position()` instead.
    #[error(
        "undefined offset with no reset policy for partitions: [{}]",
        .0.iter().map(|(topic, partition)| format!("{topic}-{partition}")).collect::<Vec<_>>().join(", ")
    )]
    NoOffsetForPartition(BTreeSet<(String, i32)>),
}

impl ConsumerError {
    /// A [`ConsumerError::Timeout`] with no cause.
    pub(crate) fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout {
            message: message.into(),
            cause: None,
        }
    }

    /// Whether this is an `OffsetFetch` error that Kafka's consumer does not
    /// retry, so the application must see it.
    pub(crate) fn is_fatal_offset_fetch_error(&self) -> bool {
        matches!(
            self,
            Self::GroupAuthorizationFailed(_)
                | Self::TopicAuthorizationFailed(_)
                | Self::OffsetFetchFailed(_)
        )
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn display_messages() {
        for (_name, error, expected) in [
            (
                "not subscribed",
                ConsumerError::NotSubscribed,
                "not subscribed to any topic",
            ),
            ("server", ConsumerError::Server(25), "broker error_code 25"),
            (
                "invalid config",
                ConsumerError::InvalidConfig("group_id required".into()),
                "invalid configuration: group_id required",
            ),
            (
                "group authorization failed",
                ConsumerError::GroupAuthorizationFailed("workers".into()),
                "not authorized to access group: workers",
            ),
            (
                "topic authorization failed",
                ConsumerError::TopicAuthorizationFailed(BTreeSet::from([
                    "orders".to_string(),
                    "payments".to_string(),
                ])),
                "not authorized to access topics: [orders, payments]",
            ),
            (
                "offset fetch failed",
                ConsumerError::OffsetFetchFailed(6),
                "unexpected error in offset fetch response: error_code 6",
            ),
            (
                "fenced instance id",
                ConsumerError::FencedInstanceId("instance-a".into()),
                "fenced group.instance.id instance-a: another consumer with the same group.instance.id joined the group",
            ),
            (
                "commit failed",
                ConsumerError::CommitFailed,
                "offset commit failed: the consumer is not part of an active group; it is likely that the consumer was kicked out of the group",
            ),
            (
                "rebalance in progress",
                ConsumerError::RebalanceInProgress("group-a".into()),
                "offset commit cannot be completed since the consumer is undergoing a rebalance for group group-a: call poll() and retry",
            ),
            (
                "no current assignment",
                ConsumerError::NoCurrentAssignment {
                    topic: "t".into(),
                    partition: 9,
                },
                "no current assignment for partition t-9",
            ),
            (
                "retriable commit failed",
                ConsumerError::RetriableCommitFailed(Box::new(ConsumerError::Server(15))),
                "offset commit failed with a retriable exception: broker error_code 15",
            ),
            (
                "log truncation",
                ConsumerError::LogTruncation {
                    topic: "t".into(),
                    partition: 3,
                    fetch_offset: 100,
                    safe_offset: 42,
                },
                "log truncation detected on t-3: fetch offset 100 is past the leader's log; safe offset 42",
            ),
            (
                "no offset for partition",
                ConsumerError::NoOffsetForPartition(BTreeSet::from([
                    ("orders".to_string(), 0),
                    ("orders".to_string(), 1),
                ])),
                "undefined offset with no reset policy for partitions: [orders-0, orders-1]",
            ),
        ] {
            assert2::assert!(error.to_string() == expected);
        }
    }
}
