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

    #[error("startup failed after joining group: {0}")]
    StartupAfterJoin(Box<ConsumerError>),

    #[error("not subscribed to any topic")]
    NotSubscribed,

    #[error("illegal state: {0}")]
    IllegalState(String),

    #[error("invalid seek offset {0}: must be non-negative")]
    InvalidOffset(i64),

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
    /// id. The consumer stops. Kafka's consumer raises
    /// `FencedInstanceIdException`.
    #[error(
        "fenced group.instance.id {0}: another consumer with the same group.instance.id joined the group"
    )]
    FencedInstanceId(String),

    /// A commit found that the consumer is not part of an active group,
    /// because its coordinator task stopped. Kafka's consumer raises
    /// `CommitFailedException`.
    #[error(
        "offset commit failed: the consumer is not part of an active group; it is likely that the consumer was kicked out of the group"
    )]
    CommitFailed,
}

impl ConsumerError {
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
                "log truncation",
                ConsumerError::LogTruncation {
                    topic: "t".into(),
                    partition: 3,
                    fetch_offset: 100,
                    safe_offset: 42,
                },
                "log truncation detected on t-3: fetch offset 100 is past the leader's log; safe offset 42",
            ),
        ] {
            assert2::assert!(error.to_string() == expected);
        }
    }
}
