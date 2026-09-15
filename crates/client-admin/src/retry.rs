//! Retry limits, deadlines and backoff of admin calls.
//!
//! Apache Kafka's `KafkaAdminClient` gives each call a deadline of
//! `default.api.timeout.ms` (60000) unless the call sets its own timeout. It
//! waits between attempts with an `ExponentialBackoff` that starts at
//! `retry.backoff.ms` (100), doubles up to `retry.backoff.max.ms` (1000) and
//! adds a random jitter of 20 percent (`CommonClientConfigs.RETRY_BACKOFF_EXP_BASE`
//! and `RETRY_BACKOFF_JITTER`).

use std::time::Duration;

use tokio::time::Instant;

use crate::AdminError;

/// The retry limits of one admin call.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct RetryPolicy {
    /// The call does not start another attempt after this time elapses.
    pub(crate) timeout: Duration,
    /// The wait before the first retry (`retry.backoff.ms`).
    pub(crate) initial_backoff: Duration,
    /// The wait between retries doubles up to this limit
    /// (`retry.backoff.max.ms`).
    pub(crate) max_backoff: Duration,
    /// The random jitter factor of each wait, from 0 to 1.
    pub(crate) jitter: f64,
}

/// Apache Kafka's admin client defaults: `default.api.timeout.ms` (60000),
/// `retry.backoff.ms` (100), `retry.backoff.max.ms` (1000) and a jitter of
/// 0.2, from `AdminClientConfig` and `CommonClientConfigs`.
pub(crate) const KAFKA_ADMIN_RETRY: RetryPolicy = RetryPolicy {
    timeout: Duration::from_mins(1),
    initial_backoff: Duration::from_millis(100),
    max_backoff: Duration::from_secs(1),
    jitter: 0.2,
};

/// `CommonClientConfigs.RETRY_BACKOFF_EXP_BASE`.
const RETRY_BACKOFF_EXP_BASE: f64 = 2.0;

impl RetryPolicy {
    /// The wait after `attempts` failed retries, as Kafka's
    /// `ExponentialBackoff.backoff` computes it. `random` is a value in
    /// `[0, 1)`.
    pub(crate) fn backoff_with(&self, attempts: u32, random: f64) -> Duration {
        let initial = self.initial_backoff.min(self.max_backoff).as_secs_f64() * 1000.0;
        let max = self.max_backoff.as_secs_f64() * 1000.0;
        if max <= initial {
            return Duration::from_secs_f64(initial / 1000.0);
        }
        let exp_max = (max / initial.max(1.0)).ln() / RETRY_BACKOFF_EXP_BASE.ln();
        // `initial * 2^exp_max` is `max`. Use `max` there, so that a rounding
        // error of `powf` does not give a wait below the maximum.
        let term = if f64::from(attempts) >= exp_max {
            max
        } else {
            initial * RETRY_BACKOFF_EXP_BASE.powf(f64::from(attempts))
        };
        let factor = if self.jitter <= 0.0 {
            1.0
        } else {
            (1.0 - self.jitter) + random * 2.0 * self.jitter
        };
        // Kafka truncates the wait to whole milliseconds.
        Duration::from_secs_f64((factor * term).floor().min(max) / 1000.0)
    }

    /// The deadline state of a call that starts now.
    pub(crate) fn start(self) -> RetryDeadline {
        RetryDeadline {
            policy: self,
            deadline: Instant::now() + self.timeout,
            retries: 0,
        }
    }
}

/// A random value in `[0, 1)` for the backoff jitter.
fn jitter_random() -> f64 {
    use ring::rand::SecureRandom as _;

    let mut bytes = [0_u8; 4];
    if ring::rand::SystemRandom::new().fill(&mut bytes).is_err() {
        return 0.5;
    }
    f64::from(u32::from_be_bytes(bytes)) / (f64::from(u32::MAX) + 1.0)
}

/// The deadline and retry count of one running admin call.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryDeadline {
    policy: RetryPolicy,
    deadline: Instant,
    retries: u32,
}

impl RetryDeadline {
    /// Whether the call deadline has passed.
    pub(crate) fn expired(&self) -> bool {
        Instant::now() >= self.deadline
    }

    /// The number of retries that the call has started.
    pub(crate) const fn retries(&self) -> u32 {
        self.retries
    }

    /// Wait for the next backoff, but not past the deadline.
    pub(crate) async fn backoff(&mut self) {
        let wait = self.policy.backoff_with(self.retries, jitter_random());
        self.retries = self.retries.saturating_add(1);
        tokio::time::sleep_until(self.deadline.min(Instant::now() + wait)).await;
    }
}

/// What a coordinator call does after one attempt.
#[derive(Debug)]
pub(crate) enum RetryAction<T> {
    /// Return this result.
    Done(Result<T, AdminError>),
    /// Send the request again to the same coordinator. Return this result
    /// when the call deadline has passed.
    SameCoordinator(Result<T, AdminError>),
    /// Find the coordinator again, then send the request again. Return this
    /// result when the call deadline has passed.
    FindCoordinator(Result<T, AdminError>),
}

/// Run a coordinator call until an attempt is final or the deadline passes.
///
/// `attempt` gets `true` when it must find the coordinator first. The first
/// attempt always finds the coordinator. After a retriable result the call
/// waits for the backoff, as Kafka's `AdminApiDriver` does, and returns the
/// last result when the deadline has passed.
pub(crate) async fn retry_coordinator_call<T>(
    policy: RetryPolicy,
    mut attempt: impl AsyncFnMut(bool) -> RetryAction<T>,
) -> Result<T, AdminError> {
    let mut deadline = policy.start();
    let mut find_coordinator = true;
    loop {
        let (last, find_next) = match attempt(find_coordinator).await {
            RetryAction::Done(result) => return result,
            RetryAction::SameCoordinator(last) => (last, false),
            RetryAction::FindCoordinator(last) => (last, true),
        };
        if deadline.expired() {
            return last;
        }
        tracing::debug!(
            find_coordinator = find_next,
            retries = deadline.retries(),
            "admin coordinator call got a retriable error; retrying"
        );
        find_coordinator = find_next;
        deadline.backoff().await;
        if deadline.expired() {
            return last;
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// Kafka's `ExponentialBackoff(100, 2, 1000, jitter)`: 100, 200, 400,
    /// 800, then 1000 ms, with the random factor in `[1 - jitter, 1 + jitter)`
    /// and never above the maximum.
    #[test]
    fn backoff_matches_kafka_exponential_backoff() {
        let millis =
            |attempts, random| KAFKA_ADMIN_RETRY.backoff_with(attempts, random).as_millis();
        let cases = [
            ((0, 0.5), 100),
            ((1, 0.5), 200),
            ((2, 0.5), 400),
            ((3, 0.5), 800),
            ((4, 0.5), 1000),
            ((30, 0.5), 1000),
            ((0, 0.0), 80),
            ((1, 0.999_999), 239),
            ((4, 0.0), 800),
            ((4, 0.999_999), 1000),
        ];
        let actual =
            cases.map(|((attempts, random), _)| ((attempts, random), millis(attempts, random)));
        assert!(actual == cases);
    }

    #[test]
    fn backoff_is_constant_when_the_maximum_is_not_above_the_initial_wait() {
        let policy = RetryPolicy {
            timeout: Duration::from_secs(1),
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_millis(200),
            jitter: 0.2,
        };
        assert!(
            [0, 3, 9].map(|attempts| policy.backoff_with(attempts, 0.9))
                == [Duration::from_millis(200); 3]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_does_not_wait_past_the_deadline() {
        let mut deadline = RetryPolicy {
            timeout: Duration::from_millis(50),
            ..KAFKA_ADMIN_RETRY
        }
        .start();
        let started = Instant::now();
        deadline.backoff().await;
        assert!(
            (started.elapsed(), deadline.expired(), deadline.retries())
                == (Duration::from_millis(50), true, 1)
        );
    }
}
