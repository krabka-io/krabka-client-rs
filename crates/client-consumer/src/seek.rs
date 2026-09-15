//! `Consumer::seek` sets the fetch position of an assigned partition.
//!
//! This is Kafka's `KafkaConsumer.seek` and `SubscriptionState.seekUnvalidated`.
//! The call changes the position at once. The next `poll` fetches from it.
//! A seek on a partition that the consumer does not own fails, as Kafka's
//! `SubscriptionState.assignedState` does.
//!
//! The coordinator task sets the position of an added partition before it
//! publishes the partition in `assigned`. A seek that finds its partition in
//! `assigned` therefore comes after that position, and the position does not
//! overwrite the seek. The seek holds the `assigned` lock while it writes the
//! position, so a rebalance cannot remove the partition during the write.
//!
//! Records that `poll` fetched from the earlier position stay in the fetch
//! buffer, but `poll` drops them because their position is not the current
//! one. Kafka's `FetchCollector` does the same check.

use krabka_ids::LeaderEpoch;

use crate::{consumer::Consumer, error::ConsumerError};

impl Consumer {
    /// Set the next offset that `poll` fetches for `(topic, partition)`.
    ///
    /// `offset` is the next offset to read, that is the last consumed offset
    /// plus 1, the same value as a committed group offset. Pass `0` to read a
    /// partition from the beginning. The consumer does not validate the
    /// position with `OffsetForLeaderEpoch`, as Kafka's `seek(TopicPartition,
    /// long)` does not.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::InvalidOffset`] if `offset` is negative, and
    /// [`ConsumerError::NoCurrentAssignment`] if the consumer does not own the
    /// partition.
    #[tracing::instrument(
        name = "consumer.seek",
        level = "debug",
        skip_all,
        fields(group_id = %self.group_id, topic = tracing::field::Empty, partition, offset),
        err
    )]
    pub async fn seek(
        &self,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
    ) -> Result<(), ConsumerError> {
        self.seek_to_position(topic.into(), partition, offset, None)
            .await
    }

    /// Set the next offset that `poll` fetches for `(topic, partition)`, with
    /// the leader epoch of the record before `offset`.
    ///
    /// This is Kafka's `seek(TopicPartition, OffsetAndMetadata)` with a leader
    /// epoch. When the consumer knows the current leader epoch of the
    /// partition, it validates the position with `OffsetForLeaderEpoch` before
    /// the next fetch, as Kafka's `SubscriptionState.seekUnvalidated` does. A
    /// truncated log then resets the position by `auto.offset.reset`.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::InvalidOffset`] if `offset` is negative, and
    /// [`ConsumerError::NoCurrentAssignment`] if the consumer does not own the
    /// partition.
    #[tracing::instrument(
        name = "consumer.seek_with_leader_epoch",
        level = "debug",
        skip_all,
        fields(group_id = %self.group_id, topic = tracing::field::Empty, partition, offset, leader_epoch),
        err
    )]
    pub async fn seek_with_leader_epoch(
        &self,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
        leader_epoch: i32,
    ) -> Result<(), ConsumerError> {
        self.seek_to_position(topic.into(), partition, offset, Some(leader_epoch))
            .await
    }

    async fn seek_to_position(
        &self,
        topic: String,
        partition: i32,
        offset: i64,
        leader_epoch: Option<i32>,
    ) -> Result<(), ConsumerError> {
        if offset < 0 {
            return Err(ConsumerError::InvalidOffset(offset));
        }
        tracing::Span::current().record("topic", tracing::field::display(&topic));
        let key = (topic, partition);
        // Lock order: `assigned`, then `next_offsets`, then `positions`, as
        // `drain_fetch_buffer` in poll.rs.
        let assigned = self.assigned.lock().await;
        if !assigned.contains(&key) {
            let (topic, partition) = key;
            return Err(ConsumerError::NoCurrentAssignment { topic, partition });
        }
        let mut offsets = self.next_offsets.lock().await;
        let mut positions = self.positions.lock().await;
        offsets.insert(key.clone(), offset);
        seek_position(positions.entry(key.clone()).or_default(), leader_epoch);
        drop(positions);
        drop(offsets);
        drop(assigned);
        Ok(())
    }
}

/// Set the leader epoch state of a sought position.
///
/// Kafka's `SubscriptionState.TopicPartitionState.validatePosition` waits for
/// validation when the position has an offset epoch and the current leader
/// epoch is known. Without an offset epoch there is nothing to validate.
fn seek_position(position: &mut crate::position::PartitionPosition, leader_epoch: Option<i32>) {
    position.offset_epoch = LeaderEpoch(leader_epoch.unwrap_or(-1));
    position.awaiting_validation =
        position.offset_epoch.is_known() && position.leader_epoch.is_known();
}

#[cfg(test)]
mod tests {
    use krabka_ids::LeaderEpoch;

    use super::seek_position;
    use crate::position::PartitionPosition;

    /// Kafka's `validatePosition` waits for validation only when both the
    /// offset epoch and the current leader epoch are known.
    #[test]
    fn seek_position_follows_kafkas_validate_position() {
        for (name, current_leader_epoch, awaiting, seek_epoch, expected) in [
            ("no epoch", 5, true, None, (-1, false)),
            ("epoch, leader known", 5, false, Some(4), (4, true)),
            ("epoch, leader unknown", -1, false, Some(4), (4, false)),
        ] {
            let mut position = PartitionPosition {
                offset_epoch: LeaderEpoch(3),
                leader_id: 1,
                leader_epoch: LeaderEpoch(current_leader_epoch),
                awaiting_validation: awaiting,
            };
            seek_position(&mut position, seek_epoch);
            assert2::assert!(
                position
                    == PartitionPosition {
                        offset_epoch: LeaderEpoch(expected.0),
                        leader_id: 1,
                        leader_epoch: LeaderEpoch(current_leader_epoch),
                        awaiting_validation: expected.1,
                    },
                "{name}"
            );
        }
    }
}
