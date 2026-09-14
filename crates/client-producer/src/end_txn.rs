//! Outcome decisions for one `EndTxn` attempt.
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
//! [`decide`] is the pure part of that loop. The producer sends the request,
//! and this function tells it what the answer means.

/// `NONE`.
const NONE: i16 = 0;
/// `COORDINATOR_LOAD_IN_PROGRESS`.
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
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

/// The result of one `EndTxn` send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndTxnAttempt {
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
    /// The coordinator refused the request because a state transition is in
    /// progress. No earlier attempt was lost, so the caller can retry or abort.
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
pub(crate) const fn decide(attempt: EndTxnAttempt, earlier_attempt_lost: bool) -> EndTxnDecision {
    match attempt {
        EndTxnAttempt::Answered(NONE) => EndTxnDecision::Complete,
        EndTxnAttempt::Answered(COORDINATOR_LOAD_IN_PROGRESS) => {
            EndTxnDecision::Retry { rediscover: false }
        }
        EndTxnAttempt::Answered(COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR)
        | EndTxnAttempt::Lost => EndTxnDecision::Retry { rediscover: true },
        EndTxnAttempt::Answered(CONCURRENT_TRANSACTIONS) if earlier_attempt_lost => {
            EndTxnDecision::Retry { rediscover: false }
        }
        EndTxnAttempt::Answered(_) if earlier_attempt_lost => EndTxnDecision::OutcomeUnknown,
        EndTxnAttempt::Answered(CONCURRENT_TRANSACTIONS) => EndTxnDecision::ConcurrentTransactions,
        EndTxnAttempt::Answered(INVALID_PRODUCER_EPOCH | PRODUCER_FENCED) => EndTxnDecision::Fenced,
        EndTxnAttempt::Answered(code) => EndTxnDecision::Refused(code),
    }
}

/// Decide what to report when the retry deadline ends before an answer.
///
/// `last` is the decision for the final attempt, which was a retry.
pub(crate) const fn decide_at_deadline(
    last: EndTxnAttempt,
    earlier_attempt_lost: bool,
) -> EndTxnDecision {
    match last {
        EndTxnAttempt::Answered(code) if !earlier_attempt_lost => EndTxnDecision::Refused(code),
        EndTxnAttempt::Answered(_) | EndTxnAttempt::Lost => EndTxnDecision::OutcomeUnknown,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{EndTxnAttempt, EndTxnDecision, decide, decide_at_deadline};

    #[test]
    fn each_end_txn_answer_maps_to_one_decision() {
        use EndTxnAttempt::{Answered, Lost};
        use EndTxnDecision::{
            Complete, ConcurrentTransactions, Fenced, OutcomeUnknown, Refused, Retry,
        };
        let cases = [
            ("first none", Answered(0), false, Complete),
            ("retried none", Answered(0), true, Complete),
            (
                "first loading",
                Answered(14),
                false,
                Retry { rediscover: false },
            ),
            (
                "retried loading",
                Answered(14),
                true,
                Retry { rediscover: false },
            ),
            (
                "first unavailable",
                Answered(15),
                false,
                Retry { rediscover: true },
            ),
            (
                "retried unavailable",
                Answered(15),
                true,
                Retry { rediscover: true },
            ),
            (
                "first moved",
                Answered(16),
                false,
                Retry { rediscover: true },
            ),
            (
                "retried moved",
                Answered(16),
                true,
                Retry { rediscover: true },
            ),
            (
                "first transport loss",
                Lost,
                false,
                Retry { rediscover: true },
            ),
            (
                "retried transport loss",
                Lost,
                true,
                Retry { rediscover: true },
            ),
            (
                "first concurrent",
                Answered(51),
                false,
                ConcurrentTransactions,
            ),
            (
                "retried concurrent",
                Answered(51),
                true,
                Retry { rediscover: false },
            ),
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
            ("first abortable", Answered(120), false, Refused(120)),
            ("retried abortable", Answered(120), true, OutcomeUnknown),
        ];
        for (name, attempt, earlier_attempt_lost, expected) in cases {
            let actual = decide(attempt, earlier_attempt_lost);
            assert!(actual == expected, "{name}");
        }
    }

    #[test]
    fn deadline_reports_unknown_only_after_a_lost_attempt() {
        use EndTxnAttempt::{Answered, Lost};
        use EndTxnDecision::{OutcomeUnknown, Refused};
        let cases = [
            ("loading, nothing lost", Answered(14), false, Refused(14)),
            ("moved, nothing lost", Answered(16), false, Refused(16)),
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
            let actual = decide_at_deadline(last, earlier_attempt_lost);
            assert!(actual == expected, "{name}");
        }
    }
}
