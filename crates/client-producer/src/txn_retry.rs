//! Retry decisions for the requests to the transaction coordinator.
//!
//! A transport failure can hide the result of an `EndTxn` request. The
//! coordinator can have written `PrepareCommit` before the connection closed.
//! Apache Kafka's `TransactionManager` does not guess. Its `EndTxnHandler`
//! sends the same request again, with the same producer id and epoch, until
//! the coordinator gives an answer. The coordinator keeps the transaction state
//! in `__transaction_state`, so a retry after a coordinator restart learns the
//! real outcome:
//!
//! - `Ongoing`: the retry completes the transaction.
//! - `PrepareCommit`: the retry gets `CONCURRENT_TRANSACTIONS`, and a later
//!   retry gets `NONE`.
//! - `CompleteCommit`: the retry gets `NONE`.
//!
//! [`decide_end_txn`] and [`decide_add_partitions`] are the pure parts of the
//! retry loops. The producer sends the request, and these functions tell it
//! what the answer means. The rules come from `TransactionManager` at Kafka
//! trunk `f87be33`:
//!
//! - `TxnRequestHandler.onComplete`: a disconnect finds the coordinator again
//!   and sends the request again.
//! - `EndTxnHandler.handleResponse` and `AddPartitionsToTxnHandler.handleResponse`:
//!   `COORDINATOR_NOT_AVAILABLE` and `NOT_COORDINATOR` find the coordinator
//!   again and send the request again. Every other `RetriableException` sends
//!   the request again. `INVALID_PRODUCER_EPOCH` and `PRODUCER_FENCED` fence
//!   the producer. A code that the handler passes to `fatalError` gives a
//!   `Fatal` decision, and the producer then moves to its fatal error state.

use crate::error_class;

/// `NONE`.
const NONE: i16 = 0;
/// `COORDINATOR_NOT_AVAILABLE`.
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// `NOT_COORDINATOR`.
const NOT_COORDINATOR: i16 = 16;
/// `INVALID_PRODUCER_EPOCH`.
const INVALID_PRODUCER_EPOCH: i16 = 47;
/// `CONCURRENT_TRANSACTIONS`.
const CONCURRENT_TRANSACTIONS: i16 = 51;
/// `PRODUCER_FENCED`.
const PRODUCER_FENCED: i16 = 90;
/// `ILLEGAL_GENERATION`.
const ILLEGAL_GENERATION: i16 = 22;
/// `UNKNOWN_MEMBER_ID`.
const UNKNOWN_MEMBER_ID: i16 = 25;
/// `GROUP_AUTHORIZATION_FAILED`.
const GROUP_AUTHORIZATION_FAILED: i16 = 30;
/// `UNKNOWN_PRODUCER_ID`.
const UNKNOWN_PRODUCER_ID: i16 = 59;
/// `GROUP_ID_NOT_FOUND`.
const GROUP_ID_NOT_FOUND: i16 = 69;
/// `FENCED_INSTANCE_ID`.
const FENCED_INSTANCE_ID: i16 = 82;
/// `STALE_MEMBER_EPOCH`.
const STALE_MEMBER_EPOCH: i16 = 113;
/// `TRANSACTION_ABORTABLE`.
const TRANSACTION_ABORTABLE: i16 = 120;
/// `REQUEST_TIMED_OUT`.
const REQUEST_TIMED_OUT: i16 = 7;
/// `CLUSTER_AUTHORIZATION_FAILED`.
const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
/// `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`.
const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
/// `INVALID_TXN_STATE`.
const INVALID_TXN_STATE: i16 = 48;
/// `INVALID_PRODUCER_ID_MAPPING`.
const INVALID_PRODUCER_ID_MAPPING: i16 = 49;

/// The result of one send to the transaction coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoordinatorAttempt {
    /// The coordinator answered with this error code.
    Answered(i16),
    /// The request failed in transport. The coordinator may have applied it.
    Lost,
}

/// What the producer does after one `EndTxn` attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndTxnDecision {
    /// The coordinator applied the requested result.
    Complete,
    /// Send the same request again after a backoff.
    Retry {
        /// Find the coordinator and open a new connection first.
        rediscover: bool,
    },
    /// The coordinator refused the request because a newer epoch owns the
    /// transactional id. No earlier attempt was lost, so this request changed
    /// nothing.
    Fenced,
    /// The retry deadline ended while the coordinator answered
    /// `CONCURRENT_TRANSACTIONS`. No attempt was lost, so the caller can retry
    /// or abort.
    ConcurrentTransactions,
    /// The coordinator refused the request with this code, and the
    /// application must abort the transaction. No earlier attempt was lost, so
    /// this request changed nothing.
    Abortable(i16),
    /// The coordinator refused the request with this fatal code. No earlier
    /// attempt was lost, so this request changed nothing. The producer can do
    /// no more transactional work.
    Fatal(i16),
    /// The retry deadline ended while the coordinator answered this retriable
    /// code. No attempt was lost, so the caller can retry.
    TimedOut(i16),
    /// An earlier attempt was lost, and this answer does not show whether
    /// that attempt took effect.
    OutcomeUnknown,
}

/// Decide what one `EndTxn` attempt means.
///
/// `earlier_attempt_lost` is `true` when an earlier send of this same request
/// failed in transport. After that, only `NONE` is proof of the outcome. A
/// fence or a refusal can come from a newer epoch that completed or aborted the
/// transaction after the lost attempt, so the outcome stays unknown.
///
/// `committed` is the result that the request asks for. Kafka's
/// `EndTxnHandler` makes `TRANSACTION_ABORTABLE` fatal for an abort, because
/// an abort that is abortable again would loop.
pub(crate) const fn decide_end_txn(
    attempt: CoordinatorAttempt,
    earlier_attempt_lost: bool,
    committed: bool,
) -> EndTxnDecision {
    match attempt {
        CoordinatorAttempt::Answered(NONE) => EndTxnDecision::Complete,
        CoordinatorAttempt::Answered(COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR)
        | CoordinatorAttempt::Lost => EndTxnDecision::Retry { rediscover: true },
        CoordinatorAttempt::Answered(code) if error_class::class(code).is_retriable() => {
            EndTxnDecision::Retry { rediscover: false }
        }
        CoordinatorAttempt::Answered(_) if earlier_attempt_lost => EndTxnDecision::OutcomeUnknown,
        CoordinatorAttempt::Answered(INVALID_PRODUCER_EPOCH | PRODUCER_FENCED) => {
            EndTxnDecision::Fenced
        }
        CoordinatorAttempt::Answered(TRANSACTION_ABORTABLE) if !committed => {
            EndTxnDecision::Fatal(TRANSACTION_ABORTABLE)
        }
        CoordinatorAttempt::Answered(code @ (UNKNOWN_PRODUCER_ID | TRANSACTION_ABORTABLE)) => {
            EndTxnDecision::Abortable(code)
        }
        CoordinatorAttempt::Answered(code) => EndTxnDecision::Fatal(code),
    }
}

/// Decide what to report when the retry deadline ends before an answer.
///
/// `last` is the final attempt, whose decision was a retry.
pub(crate) const fn decide_end_txn_at_deadline(
    last: CoordinatorAttempt,
    earlier_attempt_lost: bool,
) -> EndTxnDecision {
    match last {
        CoordinatorAttempt::Answered(CONCURRENT_TRANSACTIONS) if !earlier_attempt_lost => {
            EndTxnDecision::ConcurrentTransactions
        }
        CoordinatorAttempt::Answered(code) if !earlier_attempt_lost => {
            EndTxnDecision::TimedOut(code)
        }
        CoordinatorAttempt::Answered(_) | CoordinatorAttempt::Lost => {
            EndTxnDecision::OutcomeUnknown
        }
    }
}

/// What the producer does after one `AddPartitionsToTxn` attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AddPartitionsDecision {
    /// The coordinator added the partition to the transaction.
    Added,
    /// Send the same request again after a backoff. The coordinator ignores a
    /// partition that the transaction already holds, so a retry is safe.
    Retry {
        /// Find the coordinator and open a new connection first.
        rediscover: bool,
    },
    /// A newer epoch owns the transactional id.
    Fenced,
    /// The coordinator refused the request with this code, and the application
    /// must abort the transaction.
    Abortable(i16),
    /// The coordinator refused the request with this fatal code.
    Fatal(i16),
}

/// Decide what one `AddPartitionsToTxn` attempt means.
///
/// Kafka's `AddPartitionsToTxnHandler.handleResponse`: 15 and 16 find the
/// coordinator again, every other `RetriableException` sends the request
/// again, `INVALID_PRODUCER_EPOCH` and `PRODUCER_FENCED` fence the producer,
/// and `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`, `INVALID_TXN_STATE` and
/// `INVALID_PRODUCER_ID_MAPPING` are fatal. Every other code, which includes
/// `TOPIC_AUTHORIZATION_FAILED`, `OPERATION_NOT_ATTEMPTED` and an unexpected
/// code, gives an abortable error.
pub(crate) const fn decide_add_partitions(attempt: CoordinatorAttempt) -> AddPartitionsDecision {
    match attempt {
        CoordinatorAttempt::Answered(NONE) => AddPartitionsDecision::Added,
        CoordinatorAttempt::Answered(COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR)
        | CoordinatorAttempt::Lost => AddPartitionsDecision::Retry { rediscover: true },
        CoordinatorAttempt::Answered(code) if error_class::class(code).is_retriable() => {
            AddPartitionsDecision::Retry { rediscover: false }
        }
        CoordinatorAttempt::Answered(INVALID_PRODUCER_EPOCH | PRODUCER_FENCED) => {
            AddPartitionsDecision::Fenced
        }
        CoordinatorAttempt::Answered(
            code @ (TRANSACTIONAL_ID_AUTHORIZATION_FAILED
            | INVALID_TXN_STATE
            | INVALID_PRODUCER_ID_MAPPING),
        ) => AddPartitionsDecision::Fatal(code),
        CoordinatorAttempt::Answered(code) => AddPartitionsDecision::Abortable(code),
    }
}

/// What the producer does after one `AddOffsetsToTxn`, `TxnOffsetCommit` or
/// `InitProducerId` attempt.
///
/// The rules follow the handler of each request in Kafka's
/// `TransactionManager`. `fatalError` in those handlers becomes
/// [`TxnRequestDecision::Fatal`], except for `INVALID_PRODUCER_EPOCH` and
/// `PRODUCER_FENCED`, which give [`TxnRequestDecision::Fenced`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxnRequestDecision {
    /// The coordinator applied the request.
    Done,
    /// Send the same request again after a backoff.
    Retry {
        /// Find the coordinator and open a new connection first.
        rediscover: bool,
    },
    /// A newer epoch owns the transactional id.
    Fenced,
    /// The coordinator refused the request with this code, and the application
    /// must abort the transaction.
    Abortable(i16),
    /// The coordinator refused the request with this fatal code.
    Fatal(i16),
}

/// Decide what one `AddOffsetsToTxn` attempt means.
///
/// Kafka's `AddOffsetsToTxnHandler.handleResponse`: 15 and 16 find the
/// coordinator again, every other `RetriableException` sends the request
/// again, `UNKNOWN_PRODUCER_ID`, `GROUP_AUTHORIZATION_FAILED` and
/// `TRANSACTION_ABORTABLE` give an abortable error, `INVALID_PRODUCER_EPOCH`
/// and `PRODUCER_FENCED` fence the producer, and every other code is fatal.
pub(crate) const fn decide_add_offsets_to_txn(attempt: CoordinatorAttempt) -> TxnRequestDecision {
    match attempt {
        CoordinatorAttempt::Answered(NONE) => TxnRequestDecision::Done,
        CoordinatorAttempt::Answered(COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR)
        | CoordinatorAttempt::Lost => TxnRequestDecision::Retry { rediscover: true },
        CoordinatorAttempt::Answered(code) if error_class::class(code).is_retriable() => {
            TxnRequestDecision::Retry { rediscover: false }
        }
        CoordinatorAttempt::Answered(INVALID_PRODUCER_EPOCH | PRODUCER_FENCED) => {
            TxnRequestDecision::Fenced
        }
        CoordinatorAttempt::Answered(
            code @ (UNKNOWN_PRODUCER_ID | GROUP_AUTHORIZATION_FAILED | TRANSACTION_ABORTABLE),
        ) => TxnRequestDecision::Abortable(code),
        CoordinatorAttempt::Answered(code) => TxnRequestDecision::Fatal(code),
    }
}

/// Decide what one partition row of a `TxnOffsetCommit` answer means.
///
/// Kafka's `TxnOffsetCommitHandler.handleResponse`: 15, 16 and
/// `REQUEST_TIMED_OUT` find the group coordinator again, every other
/// `RetriableException` sends the request again,
/// `GROUP_AUTHORIZATION_FAILED`, `FENCED_INSTANCE_ID`,
/// `TRANSACTION_ABORTABLE`, and the four group metadata mismatch codes
/// (`UNKNOWN_MEMBER_ID`, `ILLEGAL_GENERATION`, `GROUP_ID_NOT_FOUND`,
/// `STALE_MEMBER_EPOCH`) give an abortable error, `INVALID_PRODUCER_EPOCH` and
/// `PRODUCER_FENCED` fence the producer, and every other code is fatal.
pub(crate) const fn decide_txn_offset_commit(attempt: CoordinatorAttempt) -> TxnRequestDecision {
    match attempt {
        CoordinatorAttempt::Answered(NONE) => TxnRequestDecision::Done,
        CoordinatorAttempt::Answered(
            COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR | REQUEST_TIMED_OUT,
        )
        | CoordinatorAttempt::Lost => TxnRequestDecision::Retry { rediscover: true },
        CoordinatorAttempt::Answered(code) if error_class::class(code).is_retriable() => {
            TxnRequestDecision::Retry { rediscover: false }
        }
        CoordinatorAttempt::Answered(INVALID_PRODUCER_EPOCH | PRODUCER_FENCED) => {
            TxnRequestDecision::Fenced
        }
        CoordinatorAttempt::Answered(
            code @ (GROUP_AUTHORIZATION_FAILED
            | FENCED_INSTANCE_ID
            | TRANSACTION_ABORTABLE
            | UNKNOWN_MEMBER_ID
            | ILLEGAL_GENERATION
            | GROUP_ID_NOT_FOUND
            | STALE_MEMBER_EPOCH),
        ) => TxnRequestDecision::Abortable(code),
        CoordinatorAttempt::Answered(code) => TxnRequestDecision::Fatal(code),
    }
}

/// Decide what one `InitProducerId` attempt means.
///
/// Kafka's `InitProducerIdHandler.handleResponse`: 15 and 16 find the
/// coordinator again, every other `RetriableException` sends the request
/// again, `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`,
/// `CLUSTER_AUTHORIZATION_FAILED` and `TRANSACTION_ABORTABLE` give an
/// abortable error, `INVALID_PRODUCER_EPOCH` and `PRODUCER_FENCED` fence the
/// producer, and every other code is fatal.
pub(crate) const fn decide_init_producer_id(attempt: CoordinatorAttempt) -> TxnRequestDecision {
    match attempt {
        CoordinatorAttempt::Answered(NONE) => TxnRequestDecision::Done,
        CoordinatorAttempt::Answered(COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR)
        | CoordinatorAttempt::Lost => TxnRequestDecision::Retry { rediscover: true },
        CoordinatorAttempt::Answered(code) if error_class::class(code).is_retriable() => {
            TxnRequestDecision::Retry { rediscover: false }
        }
        CoordinatorAttempt::Answered(INVALID_PRODUCER_EPOCH | PRODUCER_FENCED) => {
            TxnRequestDecision::Fenced
        }
        CoordinatorAttempt::Answered(
            code @ (TRANSACTIONAL_ID_AUTHORIZATION_FAILED
            | CLUSTER_AUTHORIZATION_FAILED
            | TRANSACTION_ABORTABLE),
        ) => TxnRequestDecision::Abortable(code),
        CoordinatorAttempt::Answered(code) => TxnRequestDecision::Fatal(code),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{
        AddPartitionsDecision, CoordinatorAttempt, EndTxnDecision, TxnRequestDecision,
        decide_add_offsets_to_txn, decide_add_partitions, decide_end_txn,
        decide_end_txn_at_deadline, decide_init_producer_id, decide_txn_offset_commit,
    };

    #[test]
    fn each_end_txn_answer_maps_to_one_decision() {
        use CoordinatorAttempt::{Answered, Lost};
        use EndTxnDecision::{Abortable, Complete, Fatal, Fenced, OutcomeUnknown, Retry};
        let resend = Retry { rediscover: false };
        let rediscover = Retry { rediscover: true };
        let cases = [
            ("first none", Answered(0), false, Complete),
            ("retried none", Answered(0), true, Complete),
            ("first loading", Answered(14), false, resend),
            ("retried loading", Answered(14), true, resend),
            ("first unavailable", Answered(15), false, rediscover),
            ("retried unavailable", Answered(15), true, rediscover),
            ("first moved", Answered(16), false, rediscover),
            ("retried moved", Answered(16), true, rediscover),
            ("first transport loss", Lost, false, rediscover),
            ("retried transport loss", Lost, true, rediscover),
            ("first concurrent", Answered(51), false, resend),
            ("retried concurrent", Answered(51), true, resend),
            ("first request timed out", Answered(7), false, resend),
            ("first not enough replicas", Answered(19), false, resend),
            ("first network exception", Answered(13), false, resend),
            ("first invalid epoch", Answered(47), false, Fenced),
            ("retried invalid epoch", Answered(47), true, OutcomeUnknown),
            ("first producer fenced", Answered(90), false, Fenced),
            (
                "retried producer fenced",
                Answered(90),
                true,
                OutcomeUnknown,
            ),
            ("first invalid state", Answered(48), false, Fatal(48)),
            ("retried invalid state", Answered(48), true, OutcomeUnknown),
            ("first id mapping", Answered(49), false, Fatal(49)),
            (
                "first transactional id authorization",
                Answered(53),
                false,
                Fatal(53),
            ),
            ("first unexpected code", Answered(42), false, Fatal(42)),
            ("retried id mapping", Answered(49), true, OutcomeUnknown),
            (
                "first unknown producer id",
                Answered(59),
                false,
                Abortable(59),
            ),
            ("first abortable", Answered(120), false, Abortable(120)),
            ("retried abortable", Answered(120), true, OutcomeUnknown),
        ];
        for (name, attempt, earlier_attempt_lost, expected) in cases {
            let actual = decide_end_txn(attempt, earlier_attempt_lost, true);
            assert!(actual == expected, "{name}");
        }
    }

    /// Kafka's `EndTxnHandler` makes `TRANSACTION_ABORTABLE` fatal for an
    /// abort, and abortable for a commit. The other codes do not depend on the
    /// result that the request asks for.
    #[test]
    fn end_txn_abort_makes_transaction_abortable_fatal() {
        use CoordinatorAttempt::Answered;
        use EndTxnDecision::{Abortable, Complete, Fatal, Fenced};
        let cases = [
            ("commit abortable", true, Answered(120), Abortable(120)),
            ("abort abortable", false, Answered(120), Fatal(120)),
            (
                "commit unknown producer id",
                true,
                Answered(59),
                Abortable(59),
            ),
            (
                "abort unknown producer id",
                false,
                Answered(59),
                Abortable(59),
            ),
            ("abort invalid state", false, Answered(48), Fatal(48)),
            ("abort fenced", false, Answered(90), Fenced),
            ("abort none", false, Answered(0), Complete),
        ];
        for (name, committed, attempt, expected) in cases {
            assert!(
                decide_end_txn(attempt, false, committed) == expected,
                "{name}"
            );
        }
    }

    #[test]
    fn end_txn_deadline_reports_unknown_only_after_a_lost_attempt() {
        use CoordinatorAttempt::{Answered, Lost};
        use EndTxnDecision::{ConcurrentTransactions, OutcomeUnknown, TimedOut};
        let cases = [
            ("loading, nothing lost", Answered(14), false, TimedOut(14)),
            ("moved, nothing lost", Answered(16), false, TimedOut(16)),
            (
                "concurrent, nothing lost",
                Answered(51),
                false,
                ConcurrentTransactions,
            ),
            ("loading after a loss", Answered(14), true, OutcomeUnknown),
            (
                "concurrent after a loss",
                Answered(51),
                true,
                OutcomeUnknown,
            ),
            ("still lost", Lost, true, OutcomeUnknown),
        ];
        for (name, last, earlier_attempt_lost, expected) in cases {
            let actual = decide_end_txn_at_deadline(last, earlier_attempt_lost);
            assert!(actual == expected, "{name}");
        }
    }

    #[test]
    fn each_add_partitions_answer_maps_to_one_decision() {
        use AddPartitionsDecision::{Abortable, Added, Fatal, Fenced, Retry};
        use CoordinatorAttempt::{Answered, Lost};
        let resend = Retry { rediscover: false };
        let rediscover = Retry { rediscover: true };
        let cases = [
            ("none", Answered(0), Added),
            ("loading", Answered(14), resend),
            ("unavailable", Answered(15), rediscover),
            ("moved", Answered(16), rediscover),
            ("transport loss", Lost, rediscover),
            ("concurrent", Answered(51), resend),
            ("request timed out", Answered(7), resend),
            ("invalid epoch", Answered(47), Fenced),
            ("producer fenced", Answered(90), Fenced),
            ("topic authorization", Answered(29), Abortable(29)),
            ("invalid state", Answered(48), Fatal(48)),
            ("id mapping", Answered(49), Fatal(49)),
            ("transactional id authorization", Answered(53), Fatal(53)),
            ("operation not attempted", Answered(55), Abortable(55)),
            ("unexpected code", Answered(42), Abortable(42)),
            ("unknown producer id", Answered(59), Abortable(59)),
            ("abortable", Answered(120), Abortable(120)),
        ];
        for (name, attempt, expected) in cases {
            assert!(decide_add_partitions(attempt) == expected, "{name}");
        }
    }

    /// One row of a decision table: the name of the case, the attempt, and the
    /// decision for each of the three request kinds.
    type RequestDecisionRow = (&'static str, CoordinatorAttempt, TxnRequestDecision);

    #[test]
    fn each_add_offsets_to_txn_answer_maps_to_one_decision() {
        use CoordinatorAttempt::{Answered, Lost};
        use TxnRequestDecision::{Abortable, Done, Fatal, Fenced, Retry};
        let resend = Retry { rediscover: false };
        let rediscover = Retry { rediscover: true };
        let cases: [RequestDecisionRow; 14] = [
            ("none", Answered(0), Done),
            ("loading", Answered(14), resend),
            ("unavailable", Answered(15), rediscover),
            ("moved", Answered(16), rediscover),
            ("transport loss", Lost, rediscover),
            ("request timed out", Answered(7), resend),
            ("concurrent", Answered(51), resend),
            ("invalid epoch", Answered(47), Fenced),
            ("producer fenced", Answered(90), Fenced),
            ("unknown producer id", Answered(59), Abortable(59)),
            ("group authorization", Answered(30), Abortable(30)),
            ("transaction abortable", Answered(120), Abortable(120)),
            ("invalid state", Answered(48), Fatal(48)),
            ("id mapping", Answered(49), Fatal(49)),
        ];
        for (name, attempt, expected) in cases {
            assert!(decide_add_offsets_to_txn(attempt) == expected, "{name}");
        }
    }

    #[test]
    fn each_txn_offset_commit_answer_maps_to_one_decision() {
        use CoordinatorAttempt::{Answered, Lost};
        use TxnRequestDecision::{Abortable, Done, Fatal, Fenced, Retry};
        let resend = Retry { rediscover: false };
        let rediscover = Retry { rediscover: true };
        let cases: [RequestDecisionRow; 17] = [
            ("none", Answered(0), Done),
            ("loading", Answered(14), resend),
            ("unavailable", Answered(15), rediscover),
            ("moved", Answered(16), rediscover),
            ("request timed out", Answered(7), rediscover),
            ("transport loss", Lost, rediscover),
            ("unknown topic or partition", Answered(3), resend),
            ("invalid epoch", Answered(47), Fenced),
            ("producer fenced", Answered(90), Fenced),
            ("group authorization", Answered(30), Abortable(30)),
            ("fenced instance id", Answered(82), Abortable(82)),
            ("transaction abortable", Answered(120), Abortable(120)),
            ("unknown member id", Answered(25), Abortable(25)),
            ("illegal generation", Answered(22), Abortable(22)),
            ("group id not found", Answered(69), Abortable(69)),
            ("stale member epoch", Answered(113), Abortable(113)),
            ("unsupported message format", Answered(43), Fatal(43)),
        ];
        for (name, attempt, expected) in cases {
            assert!(decide_txn_offset_commit(attempt) == expected, "{name}");
        }
    }

    #[test]
    fn each_init_producer_id_answer_maps_to_one_decision() {
        use CoordinatorAttempt::{Answered, Lost};
        use TxnRequestDecision::{Abortable, Done, Fatal, Fenced, Retry};
        let resend = Retry { rediscover: false };
        let rediscover = Retry { rediscover: true };
        let cases: [RequestDecisionRow; 12] = [
            ("none", Answered(0), Done),
            ("loading", Answered(14), resend),
            ("unavailable", Answered(15), rediscover),
            ("moved", Answered(16), rediscover),
            ("transport loss", Lost, rediscover),
            ("request timed out", Answered(7), resend),
            ("concurrent", Answered(51), resend),
            ("invalid epoch", Answered(47), Fenced),
            ("producer fenced", Answered(90), Fenced),
            (
                "transactional id authorization",
                Answered(53),
                Abortable(53),
            ),
            ("cluster authorization", Answered(31), Abortable(31)),
            ("invalid state", Answered(48), Fatal(48)),
        ];
        for (name, attempt, expected) in cases {
            assert!(decide_init_producer_id(attempt) == expected, "{name}");
        }
    }
}
