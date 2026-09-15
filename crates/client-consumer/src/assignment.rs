//! Manual partition assignment: Kafka's `KafkaConsumer.assign`.
//!
//! A consumer without a subscription fetches the partitions that the
//! application assigns. It joins no group. With a group id it can commit the
//! offsets, with generation `-1` and an empty member id, as Kafka's
//! consumer does for a manual assignment. A new partition gets its position
//! at the next position update: the committed offset when the consumer has a
//! group id and the group committed one, otherwise `auto_offset_reset`
//! (Kafka's `ClassicKafkaConsumer.updateFetchPositions`,
//! `refreshCommittedOffsetsIfNeeded` and `resetInitializingPositions`).

use std::{
    collections::{BTreeSet, HashSet},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{consumer::Consumer, error::ConsumerError, poll::COMMITTED_SENTINEL};

/// Ownership ids of manual assignments. They start far above the ids of
/// group assignments, and they never repeat, so a commit that waits across
/// an `assign` never takes a new ownership for the old one.
static MANUAL_OWNERSHIP_IDS: AtomicU64 = AtomicU64::new(1 << 62);

impl Consumer {
    /// Fetch `partitions`, in place of the current manual assignment. Kafka's
    /// `KafkaConsumer.assign`.
    ///
    /// A partition that stays assigned keeps its position. A new partition
    /// starts at the committed offset of the group, or by `auto_offset_reset`.
    /// An empty slice gives up the assignment, as Kafka's `assign` of an empty
    /// collection calls `unsubscribe`.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::InvalidArgument`] for an empty topic name, and
    /// [`ConsumerError::IllegalState`] while the consumer has a subscription.
    pub async fn assign(&self, partitions: &[(String, i32)]) -> Result<(), ConsumerError> {
        if partitions.iter().any(|(topic, _)| topic.is_empty()) {
            return Err(ConsumerError::InvalidArgument(
                "Topic partitions to assign to cannot have null or empty topic".to_owned(),
            ));
        }
        if !self.subscription.borrow().is_none() {
            return Err(ConsumerError::IllegalState(
                "Subscription to topics, partitions and pattern are mutually exclusive".to_owned(),
            ));
        }
        let wanted: Vec<(String, i32)> = partitions
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let wanted_set: HashSet<(String, i32)> = wanted.iter().cloned().collect();
        let added: Vec<(String, i32)> = {
            // Lock order: `assigned`, then `commit_identity`, as the
            // coordinator task publishes an assignment.
            let mut assigned = self.assigned.lock().await;
            let mut identity = self.commit_identity.lock().await;
            let added = wanted
                .iter()
                .filter(|partition| !assigned.contains(*partition))
                .cloned()
                .collect();
            identity
                .ownership_ids
                .retain(|partition, _| wanted_set.contains(partition));
            for partition in &wanted {
                identity
                    .ownership_ids
                    .entry(partition.clone())
                    .or_insert_with(|| MANUAL_OWNERSHIP_IDS.fetch_add(1, Ordering::Relaxed));
            }
            identity.generation = -1;
            identity.member_id.clear();
            identity.rejoin_on_poll = false;
            assigned.clone_from(&wanted);
            added
        };
        {
            let mut offsets = self.next_offsets.lock().await;
            let mut positions = self.positions.lock().await;
            offsets.retain(|partition, _| wanted_set.contains(partition));
            positions.retain(|partition, _| wanted_set.contains(partition));
            for partition in added {
                offsets.insert(partition.clone(), COMMITTED_SENTINEL);
                positions.entry(partition).or_default();
            }
        }
        self.end_offsets
            .lock()
            .await
            .retain(|partition, _| wanted_set.contains(partition));
        self.current_generation.store(-1, Ordering::Release);
        // Kafka's `assignFromUser` changes the metadata topics to the topics of
        // the assignment.
        self.client.metadata_topics().set(
            wanted
                .iter()
                .map(|(topic, _)| topic.clone())
                .collect::<BTreeSet<_>>(),
        );
        self.subscription.send_if_modified(|subscription| {
            let manual = !wanted.is_empty();
            let changed = subscription.manual_assignment != manual;
            subscription.manual_assignment = manual;
            changed
        });
        self.assignment_changed.notify_waiters();
        Ok(())
    }

    /// Fail a call that needs a group id. Kafka's
    /// `throwIfGroupIdNotDefined`.
    pub(crate) fn require_group_id(&self) -> Result<(), ConsumerError> {
        if self.group_id.is_empty() {
            return Err(ConsumerError::InvalidGroupId);
        }
        Ok(())
    }

    /// Fail a call that needs the group membership of a coordinator task.
    pub(crate) fn require_group_membership(&self) -> Result<(), ConsumerError> {
        self.require_group_id()?;
        if self.coordinator_handle.is_none() {
            return Err(ConsumerError::IllegalState(
                "the consumer was built without a subscription, so it has no group membership; build it with subscribe or subscribe_pattern to join a group".to_owned(),
            ));
        }
        Ok(())
    }
}
