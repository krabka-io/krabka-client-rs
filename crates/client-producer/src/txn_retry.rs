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
//!   the producer.

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
    /// The coordinator refused the request with this code. No earlier attempt
    /// was lost, so this request changed nothing.
    Refused(i16),
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
pub(crate) const fn decide_end_txn(
    attempt: CoordinatorAttempt,
    earlier_attempt_lost: bool,
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
        CoordinatorAttempt::Answered(code) => EndTxnDecision::Refused(code),
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
            EndTxnDecision::Refused(code)
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
    /// The coordinator refused the request with this code.
    Refused(i16),
}

/// Decide what one `AddPartitionsToTxn` attempt means.
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
        CoordinatorAttempt::Answered(code) => AddPartitionsDecision::Refused(code),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{
        AddPartitionsDecision, CoordinatorAttempt, EndTxnDecision, decide_add_partitions,
        decide_end_txn, decide_end_txn_at_deadline,
    };

    #[test]
    fn each_end_txn_answer_maps_to_one_decision() {
        use CoordinatorAttempt::{Answered, Lost};
        use EndTxnDecision::{Complete, Fenced, OutcomeUnknown, Refused, Retry};
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
            ("first invalid state", Answered(48), false, Refused(48)),
            ("retried invalid state", Answered(48), true, OutcomeUnknown),
            ("first id mapping", Answered(49), false, Refused(49)),
            ("retried id mapping", Answered(49), true, OutcomeUnknown),
            (
                "first unknown producer id",
                Answered(59),
                false,
                Refused(59),
            ),
            ("first abortable", Answered(120), false, Refused(120)),
            ("retried abortable", Answered(120), true, OutcomeUnknown),
        ];
        for (name, attempt, earlier_attempt_lost, expected) in cases {
            let actual = decide_end_txn(attempt, earlier_attempt_lost);
            assert!(actual == expected, "{name}");
        }
    }

    #[test]
    fn end_txn_deadline_reports_unknown_only_after_a_lost_attempt() {
        use CoordinatorAttempt::{Answered, Lost};
        use EndTxnDecision::{ConcurrentTransactions, OutcomeUnknown, Refused};
        let cases = [
            ("loading, nothing lost", Answered(14), false, Refused(14)),
            ("moved, nothing lost", Answered(16), false, Refused(16)),
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
        use AddPartitionsDecision::{Added, Fenced, Refused, Retry};
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
            ("topic authorization", Answered(29), Refused(29)),
            ("invalid state", Answered(48), Refused(48)),
            ("id mapping", Answered(49), Refused(49)),
            ("transactional id authorization", Answered(53), Refused(53)),
            ("operation not attempted", Answered(55), Refused(55)),
            ("unknown producer id", Answered(59), Refused(59)),
            ("abortable", Answered(120), Refused(120)),
        ];
        for (name, attempt, expected) in cases {
            assert!(decide_add_partitions(attempt) == expected, "{name}");
        }
    }
}
