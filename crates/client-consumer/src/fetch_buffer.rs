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
    /// seek clears the buffered data of its partition.
    pub(crate) fn drain(
        &mut self,
        max_records: usize,
        assigned: &HashSet<(String, i32)>,
        offsets: &mut HashMap<(String, i32), i64>,
        positions: &mut HashMap<(String, i32), PartitionPosition>,
    ) -> Vec<ConsumerRecord> {
        let mut out = Vec::new();
        while let Some(partition) = self.partitions.front_mut() {
            if !assigned.contains(&partition.key)
                || offsets.get(&partition.key) != Some(&partition.position)
            {
                self.partitions.pop_front();
                continue;
            }
            while out.len() < max_records {
                let Some(record) = partition.records.pop_front() else {
                    break;
                };
                partition.position = record.offset + 1;
                offsets.insert(partition.key.clone(), partition.position);
                positions
                    .entry(partition.key.clone())
                    .or_default()
                    .offset_epoch = LeaderEpoch(record.leader_epoch);
                out.push(record);
            }
            if !partition.records.is_empty() {
                break;
            }
            offsets.insert(partition.key.clone(), partition.next_offset);
            if let Some(epoch) = partition.last_epoch {
                positions
                    .entry(partition.key.clone())
                    .or_default()
                    .offset_epoch = epoch;
            }
            self.partitions.pop_front();
            if out.len() >= max_records {
                break;
            }
        }
        out
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
        let all = HashSet::from([key(0), key(1)]);
        let only_1 = HashSet::from([key(1)]);
        for (name, max_records, assigned, seek, expected) in [
            (
                "partial drain of the first partition",
                3,
                &all,
                None,
                vec![Drained {
                    offsets: vec![10, 11, 12],
                    next_offsets: vec![(key(0), 13), (key(1), 20)],
                    epochs: vec![(key(0), LeaderEpoch(3))],
                }],
            ),
            (
                "two calls cross the partition border and apply the batch end",
                4,
                &all,
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
            ),
            (
                "an unassigned partition is dropped",
                10,
                &only_1,
                None,
                vec![Drained {
                    offsets: vec![20, 21],
                    next_offsets: vec![(key(0), 10), (key(1), 22)],
                    epochs: vec![(key(1), LeaderEpoch(4))],
                }],
            ),
            (
                "a seek drops the data of its partition",
                10,
                &all,
                Some(5),
                vec![Drained {
                    offsets: vec![20, 21],
                    next_offsets: vec![(key(0), 5), (key(1), 22)],
                    epochs: vec![(key(1), LeaderEpoch(4))],
                }],
            ),
        ] {
            let mut buffer = FetchBuffer::default();
            // Offsets 13..16 of partition 0 are a control batch.
            buffer.push(buffered(0, 10..14, 16));
            buffer.push(buffered(1, 20..22, 22));
            let mut offsets = HashMap::from([(key(0), 10), (key(1), 20)]);
            if let Some(offset) = seek {
                offsets.insert(key(0), offset);
            }
            let mut positions = HashMap::new();
            let mut actual = Vec::new();
            for _ in 0..expected.len() {
                let records = buffer.drain(max_records, assigned, &mut offsets, &mut positions);
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
            assert2::check!(actual == expected, "case {name}");
        }
    }
}
