//! Retry limits, deadlines and backoff of admin calls.
//!
//! Apache Kafka's `KafkaAdminClient` gives each call a deadline of
//! `default.api.timeout.ms` (60000) unless the call sets its own timeout. It
//! waits between attempts with an `ExponentialBackoff` that starts at
//! `retry.backoff.ms` (100), doubles up to `retry.backoff.max.ms` (1000) and
//! adds a random jitter of 20 percent (`CommonClientConfigs.RETRY_BACKOFF_EXP_BASE`
//! and `RETRY_BACKOFF_JITTER`).

use std::{future::Future, time::Duration};

use krabka_client_core::ClientError;
use krabka_units::{Time, convert::TimeExt as _};
use tokio::time::Instant;

use crate::{AdminClient, AdminError, KafkaError, kafka_error_name};

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
    /// The call stops after this many retries (`retries`), as Kafka's
    /// `Call.fail` does when `tries > maxRetries`.
    pub(crate) max_retries: u32,
}

/// Apache Kafka's admin client defaults: `default.api.timeout.ms` (60000),
/// `retry.backoff.ms` (100), `retry.backoff.max.ms` (1000) and a jitter of
/// 0.2, from `AdminClientConfig` and `CommonClientConfigs`.
#[cfg(test)]
pub(crate) const KAFKA_ADMIN_RETRY: RetryPolicy = RetryPolicy {
    timeout: Duration::from_mins(1),
    initial_backoff: Duration::from_millis(100),
    max_backoff: Duration::from_secs(1),
    jitter: 0.2,
    max_retries: u32::MAX,
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

    /// The error of a call whose retries are unresolved at the deadline. Kafka
    /// fails such a call with a `TimeoutException`, whatever the last
    /// retriable answer was (`Call.fail`, `AdminApiDriver.onFailure`).
    pub(crate) fn timeout<T>(&self, last: &Result<T, AdminError>) -> AdminError {
        tracing::debug!(
            retries = self.retries,
            last_error = ?last.as_ref().err(),
            "admin call deadline passed with an unresolved retriable result"
        );
        AdminError::Transport(ClientError::Timeout(Time::from_std(self.policy.timeout)))
    }

    /// Whether the call must stop instead of retrying: the deadline has
    /// passed, or the call has used all its retries.
    pub(crate) fn exhausted(&self) -> bool {
        self.expired() || self.retries >= self.policy.max_retries
    }

    /// Run one attempt, but not past the call deadline. An attempt that is
    /// still running at the deadline stops and gives
    /// [`ClientError::Timeout`], as Kafka's `KafkaAdminClient` times out a
    /// call in flight at its deadline (`TimeoutProcessor`).
    pub(crate) async fn run<T>(
        &self,
        attempt: impl Future<Output = RetryAction<T>>,
    ) -> RetryAction<T> {
        tokio::time::timeout_at(self.deadline, attempt)
            .await
            .unwrap_or_else(|_| {
                RetryAction::Done(Err(AdminError::Transport(ClientError::Timeout(
                    Time::from_std(self.policy.timeout),
                ))))
            })
    }

    /// Run `attempt`, but not past the call deadline. Returns `None` when the
    /// deadline passes first.
    pub(crate) async fn bounded<F: Future>(&self, attempt: F) -> Option<F::Output> {
        tokio::time::timeout_at(self.deadline, attempt).await.ok()
    }

    /// The time that remains until the deadline, in whole milliseconds, as
    /// Kafka's `Call.calcTimeoutMsRemainingAsInt` gives it to a request.
    pub(crate) fn remaining_millis(&self) -> i32 {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        i32::try_from(remaining.as_millis()).unwrap_or(i32::MAX)
    }

    /// Wait until `wait` elapses, but not past the deadline.
    pub(crate) async fn sleep(&self, wait: Duration) {
        tokio::time::sleep_until(self.deadline.min(Instant::now() + wait)).await;
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

/// Whether `error` is a failed or lost connection with no verdict from the
/// peer: a TCP connection failure, a timeout, a disconnect, an I/O error, or a
/// TLS or SASL handshake that stopped before the peer answered. Kafka's
/// `NetworkClient` reports each as a disconnect, and `AdminApiDriver.onFailure`
/// finds the coordinator again after a disconnect. A rejected authentication
/// is not such an error.
pub(crate) fn is_connection_failure(error: &AdminError) -> bool {
    matches!(
        error,
        AdminError::Transport(
            ClientError::Connect { .. }
                | ClientError::Tls { .. }
                | ClientError::Sasl { .. }
                | ClientError::Timeout(_)
                | ClientError::Disconnected
                | ClientError::Io(_)
        )
    )
}

/// The retry action after `error` on a connection to, or a request on, a
/// coordinator: find the coordinator again after a connection failure, and
/// stop after every other error.
pub(crate) fn connection_failure_action<T>(error: AdminError) -> RetryAction<T> {
    if is_connection_failure(&error) {
        RetryAction::FindCoordinator(Err(error))
    } else {
        RetryAction::Done(Err(error))
    }
}

/// `REQUEST_TIMED_OUT`: Kafka's `TimeoutException`.
pub(crate) const REQUEST_TIMED_OUT: i16 = 7;

/// The error of a call past its deadline, as Kafka's
/// `Call.handleTimeoutFailure` gives a `TimeoutException` that names the
/// attempts and the last retriable error.
pub(crate) fn call_timeout_error(api: &str, attempts: u32, last: &str) -> KafkaError {
    KafkaError {
        code: REQUEST_TIMED_OUT,
        name: kafka_error_name(REQUEST_TIMED_OUT),
        message: Some(format!(
            "{api} timed out after {attempts} attempt(s); last error: {last}"
        )),
    }
}

/// The retry state of one controller call.
///
/// On `NOT_CONTROLLER` Kafka's `KafkaAdminClient.handleNotControllerError`
/// clears the controller and throws a retriable `NotControllerException`. The
/// call then waits for the backoff and sends the request again to the
/// refreshed controller until its deadline. Past the deadline the call fails
/// with `REQUEST_TIMED_OUT` (7).
#[derive(Debug)]
pub(crate) struct ControllerRetry {
    api: &'static str,
    deadline: RetryDeadline,
    attempts: u32,
}

impl ControllerRetry {
    /// The retry state of a call that starts now.
    pub(crate) fn new(api: &'static str, policy: RetryPolicy) -> Self {
        Self {
            api,
            deadline: policy.start(),
            attempts: 1,
        }
    }

    /// Run one request of the call, but not past the call deadline. A request
    /// still in flight at the deadline gives `REQUEST_TIMED_OUT` (7), as
    /// Kafka's `KafkaAdminClient` times out a call in flight.
    pub(crate) async fn bounded<T>(
        &self,
        attempt: impl Future<Output = Result<T, AdminError>>,
    ) -> Result<T, AdminError> {
        match self.deadline.bounded(attempt).await {
            Some(result) => result,
            None => Err(self.timeout_error("the request was in flight at the deadline")),
        }
    }

    /// Handle a `NOT_CONTROLLER` answer: find the controller again and wait
    /// for the backoff. Returns the `REQUEST_TIMED_OUT` error when the
    /// deadline has passed, and a rejected authentication of the refresh.
    pub(crate) async fn after_not_controller(
        &mut self,
        admin: &mut AdminClient,
    ) -> Result<(), AdminError> {
        self.bounded(admin.refresh_controller_after_not_controller())
            .await?;
        if self.deadline.exhausted() {
            return Err(self.timeout_error("NOT_CONTROLLER"));
        }
        self.deadline.backoff().await;
        if self.deadline.expired() {
            return Err(self.timeout_error("NOT_CONTROLLER"));
        }
        self.attempts = self.attempts.saturating_add(1);
        Ok(())
    }

    /// Handle a retriable answer that needs no controller refresh: wait for
    /// the backoff. Returns the `REQUEST_TIMED_OUT` error when the deadline
    /// has passed.
    pub(crate) async fn after_retriable(&mut self, last: &str) -> Result<(), AdminError> {
        if self.deadline.exhausted() {
            return Err(self.timeout_error(last));
        }
        self.deadline.backoff().await;
        if self.deadline.expired() {
            return Err(self.timeout_error(last));
        }
        self.attempts = self.attempts.saturating_add(1);
        Ok(())
    }

    fn timeout_error(&self, last: &str) -> AdminError {
        let error = call_timeout_error(self.api, self.attempts, last);
        AdminError::Broker {
            api: self.api,
            code: error.code,
            name: error.name,
            message: error.message,
        }
    }
}

impl AdminClient {
    /// Find the active controller after a `NOT_CONTROLLER` answer. Only a
    /// rejected authentication is an error. Kafka's `ControllerNodeProvider`
    /// keeps the call pending while the metadata names no controller.
    pub(crate) async fn refresh_controller_after_not_controller(
        &mut self,
    ) -> Result<(), AdminError> {
        match self.refresh_controller_connection().await {
            Err(error) if error.is_authentication_failure() => Err(error),
            Err(error) => {
                tracing::debug!(error = %error, "controller refresh failed; retrying");
                Ok(())
            }
            Ok(()) => Ok(()),
        }
    }
}

/// What a coordinator call does after one attempt.
#[derive(Debug)]
pub(crate) enum RetryAction<T> {
    /// Return this result.
    Done(Result<T, AdminError>),
    /// Send the request again to the same coordinator. When the call deadline
    /// has passed, the call fails with a timeout.
    SameCoordinator(Result<T, AdminError>),
    /// Find the coordinator again, then send the request again. When the call
    /// deadline has passed, the call fails with a timeout.
    FindCoordinator(Result<T, AdminError>),
}

/// The retry state of one coordinator call.
///
/// The caller runs one attempt at a time and hands its [`RetryAction`] to
/// [`CoordinatorRetry::next`]. The first attempt always finds the coordinator.
/// After a retriable result the call waits for the backoff, as Kafka's
/// `AdminApiDriver` does, and returns the last result when the deadline has
/// passed.
///
/// The loop lives in the caller, not in a closure, so that the future of the
/// call stays `Send`.
#[derive(Debug)]
pub(crate) struct CoordinatorRetry {
    deadline: RetryDeadline,
    find_coordinator: bool,
}

impl CoordinatorRetry {
    /// The retry state of a call that starts now.
    pub(crate) fn new(policy: RetryPolicy) -> Self {
        Self {
            deadline: policy.start(),
            find_coordinator: true,
        }
    }

    /// The retry state of one part of a call whose deadline `deadline`
    /// already runs, such as the request to one broker of a call to many.
    pub(crate) const fn from_deadline(deadline: RetryDeadline) -> Self {
        Self {
            deadline,
            find_coordinator: true,
        }
    }

    /// Whether the next attempt must find the coordinator first.
    pub(crate) const fn find_coordinator(&self) -> bool {
        self.find_coordinator
    }

    /// Run one attempt, but not past the call deadline. See
    /// [`RetryDeadline::run`].
    pub(crate) async fn run<T>(
        &self,
        attempt: impl Future<Output = RetryAction<T>>,
    ) -> RetryAction<T> {
        self.deadline.run(attempt).await
    }

    /// Handle the result of one attempt. Returns the result of the call when
    /// the attempt is final or the deadline has passed, and `None` after the
    /// backoff when the caller must run another attempt.
    pub(crate) async fn next<T>(
        &mut self,
        action: RetryAction<T>,
    ) -> Option<Result<T, AdminError>> {
        let (last, find_next) = match action {
            RetryAction::Done(result) => return Some(result),
            RetryAction::SameCoordinator(last) => (last, false),
            RetryAction::FindCoordinator(last) => (last, true),
        };
        if self.deadline.exhausted() {
            return Some(Err(self.deadline.timeout(&last)));
        }
        tracing::debug!(
            find_coordinator = find_next,
            retries = self.deadline.retries(),
            "admin coordinator call got a retriable error; retrying"
        );
        self.find_coordinator = find_next;
        self.deadline.backoff().await;
        if self.deadline.expired() {
            return Some(Err(self.deadline.timeout(&last)));
        }
        None
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
            max_retries: u32::MAX,
        };
        assert!(
            [0, 3, 9].map(|attempts| policy.backoff_with(attempts, 0.9))
                == [Duration::from_millis(200); 3]
        );
    }

    /// Kafka's `NetworkClient` reports a failed connection, a request
    /// timeout, and a TLS or SASL handshake that stops without a verdict as a
    /// disconnect, and `AdminApiDriver.onFailure` looks the coordinator up
    /// again. An `AuthenticationException` and a broker answer are final.
    #[test]
    fn connection_failures_find_the_coordinator_again() {
        #[derive(Debug, PartialEq, Eq)]
        enum Action {
            Done,
            SameCoordinator,
            FindCoordinator,
        }
        let addr: std::net::SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let io = || std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        let cases = [
            (
                "connect",
                AdminError::Transport(ClientError::Connect { addr, source: io() }),
                Action::FindCoordinator,
            ),
            (
                "TLS handshake without a verdict",
                AdminError::Transport(ClientError::Tls { addr, source: io() }),
                Action::FindCoordinator,
            ),
            (
                "SASL exchange without a verdict",
                AdminError::Transport(ClientError::Sasl {
                    addr,
                    source: krabka_client_core::OutboundSaslError::Io(io()),
                }),
                Action::FindCoordinator,
            ),
            (
                "timeout",
                AdminError::Transport(ClientError::Timeout(krabka_units::secs(1))),
                Action::FindCoordinator,
            ),
            (
                "disconnect",
                AdminError::Transport(ClientError::Disconnected),
                Action::FindCoordinator,
            ),
            (
                "I/O",
                AdminError::Transport(ClientError::Io(io())),
                Action::FindCoordinator,
            ),
            (
                "rejected authentication",
                AdminError::Transport(ClientError::Authentication {
                    addr,
                    source: krabka_client_core::AuthenticationError::Tls(io()),
                }),
                Action::Done,
            ),
            (
                "server error",
                AdminError::Transport(ClientError::Server { error_code: 30 }),
                Action::Done,
            ),
            (
                "protocol",
                AdminError::Protocol("bad".to_owned()),
                Action::Done,
            ),
        ];
        for (name, error, expected) in cases {
            let action = match connection_failure_action::<()>(error) {
                RetryAction::Done(_) => Action::Done,
                RetryAction::SameCoordinator(_) => Action::SameCoordinator,
                RetryAction::FindCoordinator(_) => Action::FindCoordinator,
            };
            assert!(action == expected, "{name}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_attempt_stops_at_the_call_deadline() {
        let retry = CoordinatorRetry::new(RetryPolicy {
            timeout: Duration::from_millis(50),
            ..KAFKA_ADMIN_RETRY
        });
        let started = Instant::now();
        let action = retry.run(std::future::pending::<RetryAction<()>>()).await;
        assert!(started.elapsed() == Duration::from_millis(50));
        assert!(matches!(
            action,
            RetryAction::Done(Err(AdminError::Transport(ClientError::Timeout(timeout))))
                if timeout == krabka_units::millis(50)
        ));
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
