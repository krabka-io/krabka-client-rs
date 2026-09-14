//! Retry classes of Kafka error codes.
//!
//! Apache Kafka's clients do not keep a list of retriable codes. They map each
//! code to an exception class in `org.apache.kafka.common.protocol.Errors`, and
//! then test the class: `RetriableException` means "send again", and
//! `InvalidMetadataException` (a subclass of `RetriableException`) means "send
//! again after a metadata refresh". [`class`] gives the same answer for a code
//! without the exception objects.

/// The retry class of one Kafka error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorClass {
    /// `NONE` (0).
    None,
    /// The exception of the code extends `InvalidMetadataException`. The
    /// client refreshes metadata, and it can send the request again.
    InvalidMetadata,
    /// The exception of the code extends `RetriableException`, and does not
    /// extend `InvalidMetadataException`. The client can send the request
    /// again.
    Retriable,
    /// Any other code, and a code that Kafka does not define. Kafka's
    /// `Errors.forCode` maps an unknown code to `UNKNOWN_SERVER_ERROR`, which
    /// is not retriable.
    NotRetriable,
}

impl ErrorClass {
    /// Tell if Kafka's clients can send a request again after this code.
    pub(crate) const fn is_retriable(self) -> bool {
        matches!(self, Self::InvalidMetadata | Self::Retriable)
    }
}

/// Give the retry class of `code`, from Kafka's `Errors` table (trunk
/// `f87be33`).
pub(crate) const fn class(code: i16) -> ErrorClass {
    match code {
        0 => ErrorClass::None,
        // UNKNOWN_TOPIC_OR_PARTITION, LEADER_NOT_AVAILABLE,
        // NOT_LEADER_OR_FOLLOWER, REPLICA_NOT_AVAILABLE, NETWORK_EXCEPTION,
        // KAFKA_STORAGE_ERROR, LISTENER_NOT_FOUND, FENCED_LEADER_EPOCH,
        // PREFERRED_LEADER_NOT_AVAILABLE, ELIGIBLE_LEADERS_NOT_AVAILABLE,
        // ELECTION_NOT_NEEDED, UNKNOWN_TOPIC_ID, INCONSISTENT_TOPIC_ID.
        3 | 5 | 6 | 9 | 13 | 56 | 72 | 74 | 80 | 83 | 84 | 100 | 103 => ErrorClass::InvalidMetadata,
        // CORRUPT_MESSAGE, REQUEST_TIMED_OUT, COORDINATOR_LOAD_IN_PROGRESS,
        // COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR, NOT_ENOUGH_REPLICAS,
        // NOT_ENOUGH_REPLICAS_AFTER_APPEND, NOT_CONTROLLER,
        // CONCURRENT_TRANSACTIONS, FETCH_SESSION_ID_NOT_FOUND,
        // INVALID_FETCH_SESSION_EPOCH, UNKNOWN_LEADER_EPOCH, OFFSET_NOT_AVAILABLE,
        // UNSTABLE_OFFSET_COMMIT, THROTTLING_QUOTA_EXCEEDED,
        // FETCH_SESSION_TOPIC_ID_ERROR, SHARE_SESSION_NOT_FOUND,
        // INVALID_SHARE_SESSION_EPOCH, SHARE_SESSION_LIMIT_REACHED.
        2 | 7 | 14 | 15 | 16 | 19 | 20 | 41 | 51 | 70 | 71 | 75 | 78 | 88 | 89 | 106 | 122
        | 123 | 133 => ErrorClass::Retriable,
        _ => ErrorClass::NotRetriable,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{ErrorClass, class};

    /// Each row of Kafka's `Errors` table, with the class that the exception
    /// hierarchy gives.
    ///
    /// The rows come from `clients/src/main/java/org/apache/kafka/common/`
    /// `protocol/Errors.java` and the `extends` clause of each exception in
    /// `common/errors/`, at trunk `f87be33`.
    const KAFKA_ERRORS: [(i16, &str, ErrorClass); 138] = {
        use ErrorClass::{InvalidMetadata as M, None as Ok, NotRetriable as N, Retriable as R};
        [
            (-1, "UNKNOWN_SERVER_ERROR", N),
            (0, "NONE", Ok),
            (1, "OFFSET_OUT_OF_RANGE", N),
            (2, "CORRUPT_MESSAGE", R),
            (3, "UNKNOWN_TOPIC_OR_PARTITION", M),
            (4, "INVALID_FETCH_SIZE", N),
            (5, "LEADER_NOT_AVAILABLE", M),
            (6, "NOT_LEADER_OR_FOLLOWER", M),
            (7, "REQUEST_TIMED_OUT", R),
            (8, "BROKER_NOT_AVAILABLE", N),
            (9, "REPLICA_NOT_AVAILABLE", M),
            (10, "MESSAGE_TOO_LARGE", N),
            (11, "STALE_CONTROLLER_EPOCH", N),
            (12, "OFFSET_METADATA_TOO_LARGE", N),
            (13, "NETWORK_EXCEPTION", M),
            (14, "COORDINATOR_LOAD_IN_PROGRESS", R),
            (15, "COORDINATOR_NOT_AVAILABLE", R),
            (16, "NOT_COORDINATOR", R),
            (17, "INVALID_TOPIC_EXCEPTION", N),
            (18, "RECORD_LIST_TOO_LARGE", N),
            (19, "NOT_ENOUGH_REPLICAS", R),
            (20, "NOT_ENOUGH_REPLICAS_AFTER_APPEND", R),
            (21, "INVALID_REQUIRED_ACKS", N),
            (22, "ILLEGAL_GENERATION", N),
            (23, "INCONSISTENT_GROUP_PROTOCOL", N),
            (24, "INVALID_GROUP_ID", N),
            (25, "UNKNOWN_MEMBER_ID", N),
            (26, "INVALID_SESSION_TIMEOUT", N),
            (27, "REBALANCE_IN_PROGRESS", N),
            (28, "INVALID_COMMIT_OFFSET_SIZE", N),
            (29, "TOPIC_AUTHORIZATION_FAILED", N),
            (30, "GROUP_AUTHORIZATION_FAILED", N),
            (31, "CLUSTER_AUTHORIZATION_FAILED", N),
            (32, "INVALID_TIMESTAMP", N),
            (33, "UNSUPPORTED_SASL_MECHANISM", N),
            (34, "ILLEGAL_SASL_STATE", N),
            (35, "UNSUPPORTED_VERSION", N),
            (36, "TOPIC_ALREADY_EXISTS", N),
            (37, "INVALID_PARTITIONS", N),
            (38, "INVALID_REPLICATION_FACTOR", N),
            (39, "INVALID_REPLICA_ASSIGNMENT", N),
            (40, "INVALID_CONFIG", N),
            (41, "NOT_CONTROLLER", R),
            (42, "INVALID_REQUEST", N),
            (43, "UNSUPPORTED_FOR_MESSAGE_FORMAT", N),
            (44, "POLICY_VIOLATION", N),
            (45, "OUT_OF_ORDER_SEQUENCE_NUMBER", N),
            (46, "DUPLICATE_SEQUENCE_NUMBER", N),
            (47, "INVALID_PRODUCER_EPOCH", N),
            (48, "INVALID_TXN_STATE", N),
            (49, "INVALID_PRODUCER_ID_MAPPING", N),
            (50, "INVALID_TRANSACTION_TIMEOUT", N),
            (51, "CONCURRENT_TRANSACTIONS", R),
            (52, "TRANSACTION_COORDINATOR_FENCED", N),
            (53, "TRANSACTIONAL_ID_AUTHORIZATION_FAILED", N),
            (54, "SECURITY_DISABLED", N),
            (55, "OPERATION_NOT_ATTEMPTED", N),
            (56, "KAFKA_STORAGE_ERROR", M),
            (57, "LOG_DIR_NOT_FOUND", N),
            (58, "SASL_AUTHENTICATION_FAILED", N),
            (59, "UNKNOWN_PRODUCER_ID", N),
            (60, "REASSIGNMENT_IN_PROGRESS", N),
            (61, "DELEGATION_TOKEN_AUTH_DISABLED", N),
            (62, "DELEGATION_TOKEN_NOT_FOUND", N),
            (63, "DELEGATION_TOKEN_OWNER_MISMATCH", N),
            (64, "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED", N),
            (65, "DELEGATION_TOKEN_AUTHORIZATION_FAILED", N),
            (66, "DELEGATION_TOKEN_EXPIRED", N),
            (67, "INVALID_PRINCIPAL_TYPE", N),
            (68, "NON_EMPTY_GROUP", N),
            (69, "GROUP_ID_NOT_FOUND", N),
            (70, "FETCH_SESSION_ID_NOT_FOUND", R),
            (71, "INVALID_FETCH_SESSION_EPOCH", R),
            (72, "LISTENER_NOT_FOUND", M),
            (73, "TOPIC_DELETION_DISABLED", N),
            (74, "FENCED_LEADER_EPOCH", M),
            (75, "UNKNOWN_LEADER_EPOCH", R),
            (76, "UNSUPPORTED_COMPRESSION_TYPE", N),
            (77, "STALE_BROKER_EPOCH", N),
            (78, "OFFSET_NOT_AVAILABLE", R),
            (79, "MEMBER_ID_REQUIRED", N),
            (80, "PREFERRED_LEADER_NOT_AVAILABLE", M),
            (81, "GROUP_MAX_SIZE_REACHED", N),
            (82, "FENCED_INSTANCE_ID", N),
            (83, "ELIGIBLE_LEADERS_NOT_AVAILABLE", M),
            (84, "ELECTION_NOT_NEEDED", M),
            (85, "NO_REASSIGNMENT_IN_PROGRESS", N),
            (86, "GROUP_SUBSCRIBED_TO_TOPIC", N),
            (87, "INVALID_RECORD", N),
            (88, "UNSTABLE_OFFSET_COMMIT", R),
            (89, "THROTTLING_QUOTA_EXCEEDED", R),
            (90, "PRODUCER_FENCED", N),
            (91, "RESOURCE_NOT_FOUND", N),
            (92, "DUPLICATE_RESOURCE", N),
            (93, "UNACCEPTABLE_CREDENTIAL", N),
            (94, "INCONSISTENT_VOTER_SET", N),
            (95, "INVALID_UPDATE_VERSION", N),
            (96, "FEATURE_UPDATE_FAILED", N),
            (97, "PRINCIPAL_DESERIALIZATION_FAILURE", N),
            (98, "SNAPSHOT_NOT_FOUND", N),
            (99, "POSITION_OUT_OF_RANGE", N),
            (100, "UNKNOWN_TOPIC_ID", M),
            (101, "DUPLICATE_BROKER_REGISTRATION", N),
            (102, "BROKER_ID_NOT_REGISTERED", N),
            (103, "INCONSISTENT_TOPIC_ID", M),
            (104, "INCONSISTENT_CLUSTER_ID", N),
            (105, "TRANSACTIONAL_ID_NOT_FOUND", N),
            (106, "FETCH_SESSION_TOPIC_ID_ERROR", R),
            (107, "INELIGIBLE_REPLICA", N),
            (108, "NEW_LEADER_ELECTED", N),
            (109, "OFFSET_MOVED_TO_TIERED_STORAGE", N),
            (110, "FENCED_MEMBER_EPOCH", N),
            (111, "UNRELEASED_INSTANCE_ID", N),
            (112, "UNSUPPORTED_ASSIGNOR", N),
            (113, "STALE_MEMBER_EPOCH", N),
            (114, "MISMATCHED_ENDPOINT_TYPE", N),
            (115, "UNSUPPORTED_ENDPOINT_TYPE", N),
            (116, "UNKNOWN_CONTROLLER_ID", N),
            (117, "UNKNOWN_SUBSCRIPTION_ID", N),
            (118, "TELEMETRY_TOO_LARGE", N),
            (119, "INVALID_REGISTRATION", N),
            (120, "TRANSACTION_ABORTABLE", N),
            (121, "INVALID_RECORD_STATE", N),
            (122, "SHARE_SESSION_NOT_FOUND", R),
            (123, "INVALID_SHARE_SESSION_EPOCH", R),
            (124, "FENCED_STATE_EPOCH", N),
            (125, "INVALID_VOTER_KEY", N),
            (126, "DUPLICATE_VOTER", N),
            (127, "VOTER_NOT_FOUND", N),
            (128, "INVALID_REGULAR_EXPRESSION", N),
            (129, "REBOOTSTRAP_REQUIRED", N),
            (130, "STREAMS_INVALID_TOPOLOGY", N),
            (131, "STREAMS_INVALID_TOPOLOGY_EPOCH", N),
            (132, "STREAMS_TOPOLOGY_FENCED", N),
            (133, "SHARE_SESSION_LIMIT_REACHED", R),
            (134, "GROUP_DELETION_FAILED", N),
            (135, "STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED", N),
            (136, "CONTROLLER_ID_NOT_REGISTERED", N),
        ]
    };

    #[test]
    fn each_kafka_error_code_has_the_class_of_its_exception() {
        for (code, name, expected) in KAFKA_ERRORS {
            assert!(class(code) == expected, "{code} {name}");
        }
    }

    #[test]
    fn a_code_that_kafka_does_not_define_is_not_retriable() {
        for code in [-2, 137, 500, i16::MIN, i16::MAX] {
            assert!(class(code) == ErrorClass::NotRetriable, "{code}");
        }
    }

    #[test]
    fn only_retriable_and_invalid_metadata_classes_are_retriable() {
        let cases = [
            (ErrorClass::None, false),
            (ErrorClass::InvalidMetadata, true),
            (ErrorClass::Retriable, true),
            (ErrorClass::NotRetriable, false),
        ];
        for (error_class, expected) in cases {
            assert!(error_class.is_retriable() == expected, "{error_class:?}");
        }
    }
}
