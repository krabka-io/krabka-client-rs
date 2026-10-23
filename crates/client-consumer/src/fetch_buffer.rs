//! Fetched records that `poll` did not return yet, for `max_poll_records`.
//!
//! Kafka's `FetchBuffer` keeps each completed fetch until `poll` drains it, and
//! `FetchCollector.collectFetch` returns at most `max.poll.records` records.
//! The consumed position of a partition moves only past the records that
//! `poll` returned. A commit therefore never covers a record that the
//! application did not get.

use std::collections::{HashMap, HashSet, VecDeque};

use krabka_ids::LeaderEpoch;

use crate::{consumer::ConsumerRecord, position::PartitionPosition};

/// The fetched data of one partition that `poll` did not return yet.
#[derive(Debug, Clone)]
pub(crate) struct BufferedPartition {
    pub key: (String, i32),
    /// The consumed position that the buffered data starts at. The data is
    /// stale when `next_offsets` holds another value, for example after a seek
    /// or a reset.
    pub position: i64,
    /// The records that `poll` did not return yet, in offset order.
    pub records: VecDeque<ConsumerRecord>,
    /// The position after all fetched batches. It can be past the last record,
    /// because control batches and aborted transactions have no records.
    pub next_offset: i64,
    /// The leader epoch of the last fetched batch.
    pub last_epoch: Option<LeaderEpoch>,
}

/// Fetched records, by partition in fetch order.
#[derive(Debug, Default)]
pub(crate) struct FetchBuffer {
    partitions: VecDeque<BufferedPartition>,
}

impl FetchBuffer {
    /// The number of buffered records.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.partitions
            .iter()
            .map(|partition| partition.records.len())
            .sum()
    }

    /// Add the fetched data of a partition.
    pub(crate) fn push(&mut self, partition: BufferedPartition) {
        self.partitions.push_back(partition);
    }

    /// Return up to `max_records` records, and move the consumed position of
    /// each partition past the records that this call returns.
    ///
    /// The call drops the data of a partition that is no longer in `assigned`,
    /// and of a partition whose position changed since the fetch. Kafka's
    /// `SubscriptionState` gives no position to an unassigned partition, and a
    /// seek clears the buffered data of its partition. The call keeps the data
    /// of a `paused` partition and moves it to the end of the buffer, as
    /// Kafka's `FetchCollector.collectFetch` does.
    pub(crate) fn drain(
        &mut self,
        max_records: usize,
        assigned: &HashSet<(String, i32)>,
        paused: &HashSet<(String, i32)>,
        offsets: &mut HashMap<(String, i32), i64>,
        positions: &mut HashMap<(String, i32), PartitionPosition>,
    ) -> Vec<ConsumerRecord> {
        let mut out = Vec::new();
        let mut paused_data = VecDeque::new();
        while let Some(partition) = self.partitions.front_mut() {
            if !assigned.contains(&partition.key)
                || offsets.get(&partition.key) != Some(&partition.position)
            {
                self.partitions.pop_front();
                continue;
            }
            if paused.contains(&partition.key) {
                paused_data.extend(self.partitions.pop_front());
                continue;
            }
            let mut consumed_epoch = None;
            while out.len() < max_records {
                let Some(record) = partition.records.pop_front() else {
                    break;
                };
                partition.position = record.offset + 1;
                consumed_epoch = Some(LeaderEpoch(record.leader_epoch));
                out.push(record);
            }
            // Both maps are exclusively borrowed. Publish the final position
            // once per buffer, before checking the next buffer for this key.
            let complete = partition.records.is_empty();
            if complete {
                partition.position = partition.next_offset;
                consumed_epoch = partition.last_epoch.or(consumed_epoch);
            }
            if complete || consumed_epoch.is_some() {
                offsets.insert(partition.key.clone(), partition.position);
            }
            if let Some(epoch) = consumed_epoch {
                positions
                    .entry(partition.key.clone())
                    .or_default()
                    .offset_epoch = epoch;
            }
            if !complete {
                break;
            }
            self.partitions.pop_front();
            if out.len() >= max_records {
                break;
            }
        }
        self.partitions.extend(paused_data);
        out
    }

    /// Drop the data of the partitions whose topic is not in `topics`.
    /// Kafka's `FetchBuffer.retainAll` after a subscription change.
    pub(crate) fn retain_topics(&mut self, topics: &HashSet<String>) {
        self.partitions
            .retain(|partition| topics.contains(&partition.key.0));
    }

    /// The partitions that have buffered data.
    pub(crate) fn buffered_partitions(&self) -> HashSet<(String, i32)> {
        self.partitions
            .iter()
            .map(|partition| partition.key.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn record(partition: i32, offset: i64, leader_epoch: i32) -> ConsumerRecord {
        ConsumerRecord {
            topic: "orders".into(),
            partition,
            offset,
            leader_epoch,
            timestamp: 0,
            timestamp_type: crate::TimestampType::CreateTime,
            key: None,
            value: Some(Bytes::from(offset.to_string())),
            headers: Vec::new(),
        }
    }

    fn buffered(
        partition: i32,
        offsets: std::ops::Range<i64>,
        next_offset: i64,
    ) -> BufferedPartition {
        BufferedPartition {
            key: ("orders".into(), partition),
            position: offsets.start,
            records: offsets.map(|offset| record(partition, offset, 3)).collect(),
            next_offset,
            last_epoch: Some(LeaderEpoch(4)),
        }
    }

    /// What one `drain` call returned and left.
    #[derive(Debug, PartialEq)]
    struct Drained {
        offsets: Vec<i64>,
        next_offsets: Vec<((String, i32), i64)>,
        epochs: Vec<((String, i32), LeaderEpoch)>,
    }

    #[test]
    fn drain_returns_at_most_max_records_and_moves_the_position_past_them() {
        let key = |partition: i32| ("orders".to_string(), partition);
        let all = maplit::hashset! {key(0), key(1)};
        let only_1 = maplit::hashset! {key(1)};
        let none = HashSet::new();
        let paused_0 = maplit::hashset! {key(0)};
        for (name, max_records, assigned, paused, seek, expected, remaining) in [
            (
                "partial drain of the first partition",
                3,
                &all,
                &none,
                None,
                vec![Drained {
                    offsets: vec![10, 11, 12],
                    next_offsets: vec![(key(0), 13), (key(1), 20)],
                    epochs: vec![(key(0), LeaderEpoch(3))],
                }],
                vec![key(0), key(1)],
            ),
            (
                "two calls cross the partition border and apply the batch end",
                4,
                &all,
                &none,
                None,
                vec![
                    Drained {
                        offsets: vec![10, 11, 12, 13],
                        next_offsets: vec![(key(0), 16), (key(1), 20)],
                        epochs: vec![(key(0), LeaderEpoch(4))],
                    },
                    Drained {
                        offsets: vec![20, 21],
                        next_offsets: vec![(key(0), 16), (key(1), 22)],
                        epochs: vec![(key(0), LeaderEpoch(4)), (key(1), LeaderEpoch(4))],
                    },
                ],
                vec![],
            ),
            (
                "an unassigned partition is dropped",
                10,
                &only_1,
                &none,
                None,
                vec![Drained {
                    offsets: vec![20, 21],
                    next_offsets: vec![(key(0), 10), (key(1), 22)],
                    epochs: vec![(key(1), LeaderEpoch(4))],
                }],
                vec![],
            ),
            (
                "a seek drops the data of its partition",
                10,
                &all,
                &none,
                Some(5),
                vec![Drained {
                    offsets: vec![20, 21],
                    next_offsets: vec![(key(0), 5), (key(1), 22)],
                    epochs: vec![(key(1), LeaderEpoch(4))],
                }],
                vec![],
            ),
            (
                "a paused partition keeps its data at the end of the buffer",
                10,
                &all,
                &paused_0,
                None,
                vec![
                    Drained {
                        offsets: vec![20, 21],
                        next_offsets: vec![(key(0), 10), (key(1), 22)],
                        epochs: vec![(key(1), LeaderEpoch(4))],
                    },
                    Drained {
                        offsets: vec![],
                        next_offsets: vec![(key(0), 10), (key(1), 22)],
                        epochs: vec![(key(1), LeaderEpoch(4))],
                    },
                ],
                vec![key(0)],
            ),
        ] {
            let mut buffer = FetchBuffer::default();
            // Offsets 13..16 of partition 0 are a control batch.
            buffer.push(buffered(0, 10..14, 16));
            buffer.push(buffered(1, 20..22, 22));
            let mut offsets = maplit::hashmap! {key(0) => 10, key(1) => 20};
            if let Some(offset) = seek {
                offsets.insert(key(0), offset);
            }
            let mut positions = HashMap::new();
            let mut actual = Vec::new();
            for _ in 0..expected.len() {
                let records =
                    buffer.drain(max_records, assigned, paused, &mut offsets, &mut positions);
                let mut next_offsets: Vec<_> = offsets.clone().into_iter().collect();
                next_offsets.sort();
                let mut epochs: Vec<_> = positions
                    .iter()
                    .map(|(key, position)| (key.clone(), position.offset_epoch))
                    .collect();
                epochs.sort();
                actual.push(Drained {
                    offsets: records.iter().map(|record| record.offset).collect(),
                    next_offsets,
                    epochs,
                });
            }
            let mut left: Vec<_> = buffer.buffered_partitions().into_iter().collect();
            left.sort();
            assert2::check!((actual, left) == (expected, remaining), "case {name}");
        }
    }

    #[test]
    fn drain_publishes_each_buffer_before_the_next_buffer_for_the_same_partition() {
        let key = ("orders".to_string(), 0);
        let assigned = HashSet::from([key.clone()]);
        for last_epoch in [None, Some(LeaderEpoch(9))] {
            let mut buffer = FetchBuffer::default();
            buffer.push(BufferedPartition {
                records: VecDeque::from([record(0, 10, 2), record(0, 12, 5)]),
                last_epoch,
                ..buffered(0, 10..10, 15)
            });
            buffer.push(BufferedPartition {
                last_epoch: None,
                ..buffered(0, 15..17, 20)
            });
            // This empty fetch represents control or aborted-only batches.
            buffer.push(BufferedPartition {
                last_epoch: Some(LeaderEpoch(11)),
                ..buffered(0, 20..20, 25)
            });
            let mut offsets = HashMap::from([(key.clone(), 10)]);
            let initial = PartitionPosition {
                leader_id: 7,
                leader_epoch: LeaderEpoch(13),
                awaiting_validation: true,
                ..PartitionPosition::default()
            };
            let mut positions = HashMap::from([(key.clone(), initial)]);
            for (limit, expected_records, expected_offset, expected_epoch) in [
                (0, vec![], 10, LeaderEpoch(-1)),
                (1, vec![10], 11, LeaderEpoch(2)),
                (2, vec![12, 15], 16, LeaderEpoch(3)),
                (1, vec![16], 20, LeaderEpoch(3)),
                (0, vec![], 25, LeaderEpoch(11)),
            ] {
                let records = buffer.drain(
                    limit,
                    &assigned,
                    &HashSet::new(),
                    &mut offsets,
                    &mut positions,
                );
                assert2::assert!(
                    records
                        .iter()
                        .map(|record| record.offset)
                        .collect::<Vec<_>>()
                        == expected_records
                );
                assert2::assert!(offsets == HashMap::from([(key.clone(), expected_offset)]));
                assert2::assert!(
                    positions
                        == HashMap::from([(
                            key.clone(),
                            PartitionPosition {
                                offset_epoch: expected_epoch,
                                ..initial
                            }
                        )])
                );
            }
            assert2::assert!(buffer.partitions.is_empty());
        }
    }

    #[test]
    fn completed_buffer_uses_batch_epoch_or_last_consumed_epoch() {
        let key = ("orders".to_string(), 0);
        for (last_epoch, expected_epoch) in [
            (None, LeaderEpoch(5)),
            (Some(LeaderEpoch(9)), LeaderEpoch(9)),
        ] {
            let mut buffer = FetchBuffer::default();
            buffer.push(BufferedPartition {
                records: VecDeque::from([record(0, 10, 2), record(0, 12, 5)]),
                last_epoch,
                ..buffered(0, 10..10, 20)
            });
            let mut offsets = HashMap::from([(key.clone(), 10)]);
            let mut positions = HashMap::new();
            let records = buffer.drain(
                10,
                &HashSet::from([key.clone()]),
                &HashSet::new(),
                &mut offsets,
                &mut positions,
            );
            assert2::assert!(
                records
                    .iter()
                    .map(|record| record.offset)
                    .collect::<Vec<_>>()
                    == vec![10, 12]
            );
            assert2::assert!(offsets == HashMap::from([(key.clone(), 20)]));
            assert2::assert!(
                positions
                    == HashMap::from([(
                        key.clone(),
                        PartitionPosition {
                            offset_epoch: expected_epoch,
                            ..PartitionPosition::default()
                        }
                    )])
            );
        }
    }

    /// Kafka's `subscribe(topics)` clears the buffered data of the topics that
    /// the consumer no longer subscribes to.
    #[test]
    fn retain_topics_drops_the_data_of_other_topics() {
        let mut buffer = FetchBuffer::default();
        buffer.push(buffered(0, 10..12, 12));
        buffer.push(BufferedPartition {
            key: ("payments".into(), 0),
            ..buffered(0, 20..22, 22)
        });
        buffer.retain_topics(&maplit::hashset! {"payments".to_string()});
        let mut left: Vec<_> = buffer.buffered_partitions().into_iter().collect();
        left.sort();
        assert2::assert!(left == vec![("payments".to_string(), 0)]);
    }
}
