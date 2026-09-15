//! Consumer control calls: Kafka's `KafkaConsumer.wakeup`, `enforceRebalance`
//! and `close(CloseOptions)`.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use krabka_units::{Time, convert::TimeExt as _, secs};

use crate::{
    consumer::{Consumer, GroupMembershipOperation},
    error::ConsumerError,
};

/// Kafka's `ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS`.
pub const DEFAULT_CONSUMER_CLOSE_TIMEOUT: Time = secs(30);

/// The `JoinGroup` reason of [`Consumer::enforce_rebalance`] without a reason.
/// Kafka's `ClassicKafkaConsumer.DEFAULT_REASON`.
pub(crate) const ENFORCED_REBALANCE_REASON: &str = "rebalance enforced by user";

/// How [`Consumer::close_with`](crate::Consumer::close_with) closes the
/// consumer. Kafka's `CloseOptions` (KIP-1092).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct CloseOptions {
    /// What the consumer does with its group membership.
    pub group_membership_operation: GroupMembershipOperation,
    /// The time that the close can take. `None` is
    /// [`DEFAULT_CONSUMER_CLOSE_TIMEOUT`].
    pub timeout: Option<Time>,
}

impl CloseOptions {
    /// The options with `timeout`. Kafka's `CloseOptions.timeout`.
    #[must_use]
    pub fn timeout(timeout: Time) -> Self {
        Self {
            timeout: Some(timeout),
            ..Self::default()
        }
    }

    /// The options with `operation`. Kafka's
    /// `CloseOptions.groupMembershipOperation`.
    #[must_use]
    pub fn group_membership_operation(operation: GroupMembershipOperation) -> Self {
        Self {
            group_membership_operation: operation,
            ..Self::default()
        }
    }
}

/// What the coordinator task needs when it stops.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CloseRequest {
    pub operation: GroupMembershipOperation,
}

/// The close timeout of `options`, or an error for a negative timeout.
pub(crate) fn close_timeout(options: CloseOptions) -> Result<Time, ConsumerError> {
    let timeout = options.timeout.unwrap_or(DEFAULT_CONSUMER_CLOSE_TIMEOUT);
    if timeout.secs_f64() < 0.0 || !timeout.secs_f64().is_finite() {
        return Err(ConsumerError::InvalidArgument(
            "The timeout cannot be negative.".to_owned(),
        ));
    }
    Ok(timeout)
}

/// A handle that wakes a [`Consumer::poll`](crate::Consumer::poll) from
/// another task. Kafka's `KafkaConsumer.wakeup`.
///
/// Get it with [`Consumer::wakeup_handle`](crate::Consumer::wakeup_handle).
/// The poll that runs, or the next poll, returns [`ConsumerError::Wakeup`]
/// once.
#[derive(Clone, Debug, Default)]
pub struct WakeupHandle {
    inner: Arc<WakeupState>,
}

#[derive(Debug, Default)]
struct WakeupState {
    pending: AtomicBool,
    notify: tokio::sync::Notify,
}

impl WakeupHandle {
    /// Wake the poll that runs, or the next poll.
    pub fn wakeup(&self) {
        self.inner.pending.store(true, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    /// Take a pending wakeup. Kafka's `ConsumerNetworkClient.maybeTriggerWakeup`.
    pub(crate) fn take(&self) -> bool {
        self.inner.pending.swap(false, Ordering::SeqCst)
    }

    /// Wait until a wakeup comes, and take it.
    pub(crate) async fn woken(&self) {
        loop {
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.take() {
                return;
            }
            notified.await;
        }
    }
}

impl Consumer {
    /// A handle that wakes the `poll` of this consumer from another task.
    /// Kafka's `KafkaConsumer.wakeup` is thread safe; `poll` takes `&mut self`
    /// here, so another task needs its own handle.
    #[must_use]
    pub fn wakeup_handle(&self) -> WakeupHandle {
        self.wakeup.clone()
    }

    /// Make the next `poll` return [`ConsumerError::Wakeup`]. Kafka's
    /// `KafkaConsumer.wakeup`.
    pub fn wakeup(&self) {
        self.wakeup.wakeup();
    }

    /// Ask the group to rebalance. The next `poll` sends `JoinGroup` with
    /// `reason`, or with Kafka's default reason `rebalance enforced by user`
    /// when `reason` is `None` or empty. Kafka's
    /// `KafkaConsumer.enforceRebalance`.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::InvalidGroupId`] without a group id, and
    /// [`ConsumerError::IllegalState`] for a consumer without group
    /// membership (Kafka: `Tried to force a rebalance but consumer does not
    /// have a group.`).
    pub fn enforce_rebalance(&self, reason: Option<&str>) -> Result<(), ConsumerError> {
        self.require_group_membership()?;
        if self.subscription.borrow().manual_assignment {
            return Err(ConsumerError::IllegalState(
                "Tried to force a rebalance but the consumer has a manual assignment.".to_owned(),
            ));
        }
        let reason = reason
            .filter(|reason| !reason.is_empty())
            .unwrap_or(ENFORCED_REBALANCE_REASON);
        // A closed channel means that the coordinator task stopped, and no
        // rebalance can come.
        // The `poll` count now: the rebalance starts with the first `poll`
        // after this call, also when the task sees the request after that
        // `poll`.
        let polls = *self.poll_signal.borrow();
        let _ = self.enforced_rebalances.send((reason.to_owned(), polls));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use krabka_units::millis;

    use super::*;

    #[test]
    fn close_timeout_follows_kafkas_close() {
        let actual = [
            CloseOptions::default(),
            CloseOptions::timeout(millis(250)),
            CloseOptions::timeout(Time::from_secs_f64(-1.0)),
        ]
        .map(|options| close_timeout(options).map_err(|error| error.to_string()));
        assert2::assert!(
            actual
                == [
                    Ok(DEFAULT_CONSUMER_CLOSE_TIMEOUT),
                    Ok(millis(250)),
                    Err("invalid argument: The timeout cannot be negative.".to_owned()),
                ]
        );
    }

    #[tokio::test]
    async fn wakeup_is_taken_once() {
        let handle = WakeupHandle::default();
        handle.wakeup();
        let first =
            tokio::time::timeout(std::time::Duration::from_millis(50), handle.woken()).await;
        let second =
            tokio::time::timeout(std::time::Duration::from_millis(50), handle.woken()).await;
        assert2::assert!((first.is_ok(), second.is_ok()) == (true, false));
    }
}
