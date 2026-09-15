//! The rebalance listener: Kafka's `ConsumerRebalanceListener`.
//!
//! The coordinator task runs the joins, but the listener runs inside `poll`
//! and `close`, on the task of the application, as in Kafka. The coordinator
//! task sends each call to the `Consumer` and waits until `poll` ran it. It
//! sends heartbeats while it waits.

use async_trait::async_trait;

use crate::{consumer::Consumer, error::ConsumerError};

/// The error that a listener callback returns.
pub type RebalanceListenerError = Box<dyn std::error::Error + Send + Sync>;

/// Callbacks for the partitions that a rebalance takes from or gives to the
/// consumer. Kafka's `ConsumerRebalanceListener`.
///
/// The consumer calls them inside [`Consumer::poll`] and
/// [`Consumer::close`](Consumer::close), in this order:
///
/// - Eager protocol: `on_partitions_revoked` with all owned partitions before
///   the `JoinGroup`, then `on_partitions_assigned` with the new assignment.
/// - Cooperative protocol: after the `SyncGroup`, `on_partitions_revoked` with
///   the partitions that left the assignment, only when there are some, then
///   `on_partitions_assigned` with the added partitions, also when there are
///   none.
/// - `on_partitions_lost` in place of `on_partitions_revoked` when the member
///   lost its generation: `UNKNOWN_MEMBER_ID`, `FENCED_INSTANCE_ID` or
///   `max_poll_interval`.
///
/// Each list is sorted by topic and partition. A callback can use the
/// consumer, for example to commit with [`Consumer::commit_sync`] from
/// `on_partitions_revoked`. An error from a callback makes that `poll` return
/// [`ConsumerError::RebalanceListenerFailed`] after the rebalance step.
#[async_trait]
pub trait ConsumerRebalanceListener: Send + Sync {
    /// Kafka's `onPartitionsRevoked`.
    ///
    /// # Errors
    ///
    /// An error that `poll` returns.
    async fn on_partitions_revoked(
        &mut self,
        consumer: &Consumer,
        partitions: &[(String, i32)],
    ) -> Result<(), RebalanceListenerError>;

    /// Kafka's `onPartitionsAssigned`.
    ///
    /// # Errors
    ///
    /// An error that `poll` returns.
    async fn on_partitions_assigned(
        &mut self,
        consumer: &Consumer,
        partitions: &[(String, i32)],
    ) -> Result<(), RebalanceListenerError>;

    /// Kafka's `onPartitionsLost`. The default calls
    /// [`on_partitions_revoked`](Self::on_partitions_revoked), as in Kafka.
    ///
    /// # Errors
    ///
    /// An error that `poll` returns.
    async fn on_partitions_lost(
        &mut self,
        consumer: &Consumer,
        partitions: &[(String, i32)],
    ) -> Result<(), RebalanceListenerError> {
        self.on_partitions_revoked(consumer, partitions).await
    }
}

/// Which callback a [`ListenerCall`] runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListenerCallKind {
    Revoked,
    Assigned,
    Lost,
}

/// One callback that the coordinator task asks `poll` to run.
#[derive(Debug)]
pub(crate) struct ListenerCall {
    pub kind: ListenerCallKind,
    pub partitions: Vec<(String, i32)>,
    /// Completed when the callback ran.
    pub done: tokio::sync::oneshot::Sender<()>,
}

/// The sending side of the listener calls, in the coordinator task.
pub(crate) type ListenerCalls = tokio::sync::mpsc::UnboundedSender<ListenerCall>;

impl Consumer {
    /// Run one listener callback on `partitions`.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::RebalanceListenerFailed`] when the callback
    /// fails.
    pub(crate) async fn run_listener(
        &mut self,
        kind: ListenerCallKind,
        partitions: &[(String, i32)],
    ) -> Result<(), ConsumerError> {
        let Some(mut listener) = self.rebalance_listener.take() else {
            return Ok(());
        };
        let mut sorted = partitions.to_vec();
        sorted.sort();
        let result = match kind {
            ListenerCallKind::Revoked => listener.on_partitions_revoked(self, &sorted).await,
            ListenerCallKind::Assigned => listener.on_partitions_assigned(self, &sorted).await,
            ListenerCallKind::Lost => listener.on_partitions_lost(self, &sorted).await,
        };
        self.rebalance_listener = Some(listener);
        result.map_err(|error| ConsumerError::RebalanceListenerFailed(error.to_string()))
    }

    /// Run a listener call and tell the coordinator task that it ran.
    pub(crate) async fn complete_listener_call(
        &mut self,
        call: ListenerCall,
    ) -> Result<(), ConsumerError> {
        let result = self.run_listener(call.kind, &call.partitions).await;
        let _ = call.done.send(());
        result
    }

    /// Run every listener call that waits now. The first error comes back
    /// after all calls ran, as Kafka's `ConsumerCoordinator` completes the
    /// rebalance step before it throws.
    pub(crate) async fn run_pending_listener_calls(&mut self) -> Result<(), ConsumerError> {
        let mut first_error = None;
        while let Ok(call) = self.listener_calls.try_recv() {
            if let Err(error) = self.complete_listener_call(call).await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
