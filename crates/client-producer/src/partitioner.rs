//! Kafka's built-in partitioner (KIP-480 and KIP-794).
//!
//! A keyed record goes to `toPositive(murmur2(key)) % partitions`, unless
//! `partitioner.ignore.keys` is set. A keyless record goes to the sticky
//! partition of its topic. The producer keeps a sticky partition until about
//! `batch.size` bytes went to it, and then picks a new one at random. The pick
//! uses only partitions with a leader. With adaptive partitioning, the sender
//! weights the pick by the queue size of each partition, so a partition with a
//! long queue gets fewer new records. This is Apache Kafka's
//! `BuiltInPartitioner`.

use std::{
    collections::HashMap,
    hash::{BuildHasher as _, RandomState},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

/// The settings of the built-in partitioner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PartitionerConfig {
    /// The bytes that go to a sticky partition before the producer picks a
    /// new one. Kafka uses `batch.size`.
    pub sticky_batch_size: usize,
    /// Kafka's `partitioner.ignore.keys`.
    pub ignore_keys: bool,
    /// Kafka's `partitioner.adaptive.partitioning.enable`.
    pub adaptive_partitioning: bool,
    /// Kafka's `partitioner.availability.timeout.ms`. Zero turns the check
    /// off.
    pub availability_timeout: Duration,
}

impl Default for PartitionerConfig {
    /// Kafka's defaults.
    fn default() -> Self {
        Self {
            sticky_batch_size: crate::DEFAULT_PRODUCER_BATCH_BYTES,
            ignore_keys: crate::DEFAULT_PRODUCER_PARTITIONER_IGNORE_KEYS,
            adaptive_partitioning: crate::DEFAULT_PRODUCER_PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE,
            availability_timeout: crate::DEFAULT_PRODUCER_PARTITIONER_AVAILABILITY_TIMEOUT,
        }
    }
}

/// A source of random 32-bit values. Tests give a scripted source.
pub(crate) type RandomSource = Arc<dyn Fn() -> u32 + Send + Sync>;

/// A random 32-bit value.
///
/// Each `RandomState` has new random keys, so its hash of a fixed value is a
/// random number. This needs no extra dependency.
fn random_u32() -> u32 {
    let bits = RandomState::new().hash_one(0_u8) >> 32;
    u32::try_from(bits).unwrap_or(u32::MAX)
}

/// The partitions of a topic, as the metadata of the producer shows them.
pub(crate) struct TopicPartitions<'a> {
    /// The partition count. It is greater than zero.
    pub count: i32,
    /// Tell if a partition has a leader.
    pub has_leader: &'a (dyn Fn(i32) -> bool + Sync),
}

/// The sticky partition of a topic at one time.
///
/// Two values are equal only when they come from the same pick. Kafka compares
/// the identity of its `StickyPartitionInfo` object in the same way, so a pick
/// of the same partition number still counts as a change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StickyPartition {
    partition: i32,
    pick: u64,
}

impl StickyPartition {
    pub(crate) const fn partition(self) -> i32 {
        self.partition
    }
}

/// The queue sizes of the partitions of a topic, for adaptive partitioning.
///
/// Kafka's `RecordAccumulator.partitionReady` builds them. `sizes` and
/// `partition_ids` hold only the partitions that the pick can use: they have a
/// leader, and the availability timeout did not exclude them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueueSizes {
    pub sizes: Vec<u32>,
    pub partition_ids: Vec<i32>,
    /// The number of partitions that have a queue, with or without a leader.
    pub all: usize,
}

/// The cumulative frequency table of a weighted pick.
#[derive(Clone, Debug, PartialEq, Eq)]
struct LoadStats {
    cumulative_frequency: Vec<u32>,
    partition_ids: Vec<i32>,
}

impl LoadStats {
    /// Kafka's `BuiltInPartitioner.updatePartitionLoadStats`.
    ///
    /// It gives `None` when a uniform pick is correct: no partition can take
    /// records, the topic has fewer than two partitions, or every partition
    /// can take records and all queues have the same size.
    fn from_queue_sizes(queues: QueueSizes) -> Option<Self> {
        let QueueSizes {
            sizes,
            partition_ids,
            all,
        } = queues;
        if sizes.is_empty() || all < 2 || sizes.len() != partition_ids.len() {
            return None;
        }
        let largest = sizes.iter().copied().max()?;
        if sizes.iter().all(|&size| size == largest) && sizes.len() == all {
            return None;
        }
        // Invert each size, so a shorter queue gets a larger weight, and sum
        // the weights. A value from 0 to the last sum then maps to the first
        // partition whose sum is greater. Sizes 0, 3, 1 give weights 4, 1, 3
        // and the table 4, 5, 8.
        let weight_base = largest.saturating_add(1);
        let mut running = 0_u32;
        let cumulative_frequency = sizes
            .iter()
            .map(|&size| {
                running = running.saturating_add(weight_base - size);
                running
            })
            .collect();
        Some(Self {
            cumulative_frequency,
            partition_ids,
        })
    }
}

/// The partitioner state of one topic.
#[derive(Debug, Default)]
struct TopicState {
    /// The sticky partition and the bytes that went to it.
    sticky: Option<(StickyPartition, usize)>,
    load_stats: Option<LoadStats>,
}

/// Kafka's `BuiltInPartitioner`, for every topic of a producer.
pub(crate) struct BuiltInPartitioner {
    config: PartitionerConfig,
    random: RandomSource,
    picks: AtomicU64,
    /// No code holds this lock across an `.await`.
    topics: Mutex<HashMap<String, TopicState>>,
}

impl std::fmt::Debug for BuiltInPartitioner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BuiltInPartitioner")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl BuiltInPartitioner {
    pub(crate) fn new(config: PartitionerConfig) -> Self {
        Self::with_random(config, Arc::new(random_u32))
    }

    /// A partitioner that takes its random values from `random`.
    pub(crate) fn with_random(config: PartitionerConfig, random: RandomSource) -> Self {
        Self {
            config,
            random,
            picks: AtomicU64::new(0),
            topics: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) const fn config(&self) -> PartitionerConfig {
        self.config
    }

    /// The partition of a keyed record, or `None` when the sticky partition
    /// takes the record. Kafka's `KafkaProducer.partition`.
    pub(crate) fn keyed_partition(&self, key: Option<&[u8]>, count: i32) -> Option<i32> {
        key.filter(|_| !self.config.ignore_keys)
            .map(|key| partition_for_key(key, count))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, TopicState>> {
        self.topics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Kafka's `BuiltInPartitioner.peekCurrentPartitionInfo`: the sticky
    /// partition of `topic`, picked now when the topic has none.
    pub(crate) fn peek(&self, topic: &str, partitions: &TopicPartitions<'_>) -> StickyPartition {
        let mut topics = self.lock();
        let state = topics.entry(topic.to_owned()).or_default();
        if let Some((sticky, _)) = state.sticky {
            return sticky;
        }
        let sticky = self.pick(state.load_stats.as_ref(), partitions);
        state.sticky = Some((sticky, 0));
        sticky
    }

    /// Kafka's `BuiltInPartitioner.isPartitionChanged`: tell if another send
    /// picked a new sticky partition after `sticky`.
    pub(crate) fn is_changed(&self, topic: &str, sticky: StickyPartition) -> bool {
        self.lock()
            .get(topic)
            .and_then(|state| state.sticky)
            .is_none_or(|(current, _)| current != sticky)
    }

    /// Kafka's `BuiltInPartitioner.updatePartitionInfo`: count `appended`
    /// bytes on `sticky`, and pick a new sticky partition when enough bytes
    /// went to it.
    ///
    /// The switch needs `sticky_batch_size` bytes and `enable_switch`, or
    /// twice `sticky_batch_size` bytes. The caller turns the switch off while
    /// the partition has a batch that is not full, so a switch does not leave
    /// a small batch behind.
    pub(crate) fn update(
        &self,
        topic: &str,
        sticky: StickyPartition,
        appended: usize,
        partitions: &TopicPartitions<'_>,
        enable_switch: bool,
    ) {
        let mut topics = self.lock();
        let Some(state) = topics.get_mut(topic) else {
            return;
        };
        let Some((current, produced)) = state.sticky.as_mut() else {
            return;
        };
        if *current != sticky {
            return;
        }
        *produced = produced.saturating_add(appended);
        let size = self.config.sticky_batch_size.max(1);
        if (*produced >= size && enable_switch) || *produced >= size.saturating_mul(2) {
            let next = self.pick(state.load_stats.as_ref(), partitions);
            state.sticky = Some((next, 0));
        }
    }

    /// Store the queue sizes of `topic` for the next pick, or `None` for a
    /// uniform pick. Kafka's `BuiltInPartitioner.updatePartitionLoadStats`.
    pub(crate) fn update_load_stats(&self, topic: &str, queues: Option<QueueSizes>) {
        let load_stats = queues.and_then(LoadStats::from_queue_sizes);
        let mut topics = self.lock();
        match topics.get_mut(topic) {
            Some(state) => state.load_stats = load_stats,
            None if load_stats.is_some() => {
                topics.insert(
                    topic.to_owned(),
                    TopicState {
                        sticky: None,
                        load_stats,
                    },
                );
            }
            None => {}
        }
    }

    /// Kafka's `BuiltInPartitioner.nextPartition`.
    fn pick(
        &self,
        load_stats: Option<&LoadStats>,
        partitions: &TopicPartitions<'_>,
    ) -> StickyPartition {
        // Kafka's `Utils.toPositive` of a random `int`.
        let random = (self.random)() & 0x7fff_ffff;
        let partition = if let Some(stats) = load_stats {
            let total = stats
                .cumulative_frequency
                .last()
                .copied()
                .unwrap_or(1)
                .max(1);
            let weighted = random % total;
            // The table only grows, so the first sum greater than the value
            // names the partition.
            let index = stats
                .cumulative_frequency
                .partition_point(|&sum| sum <= weighted);
            stats.partition_ids.get(index).copied().unwrap_or(0)
        } else {
            let available: Vec<i32> = (0..partitions.count)
                .filter(|&partition| (partitions.has_leader)(partition))
                .collect();
            let random = usize::try_from(random).unwrap_or(0);
            if available.is_empty() {
                // No partition has a leader, so every partition can take the
                // record.
                let count = usize::try_from(partitions.count.max(1)).unwrap_or(1);
                i32::try_from(random % count).unwrap_or(0)
            } else {
                available[random % available.len()]
            }
        };
        StickyPartition {
            partition,
            pick: self.picks.fetch_add(1, Ordering::AcqRel),
        }
    }
}

/// Pick the partition that Kafka picks for `key`.
///
/// Kafka computes `toPositive(murmur2(key)) % num_partitions`, and
/// `Utils.toPositive` masks the sign bit with `& 0x7fffffff`. It does not take
/// the absolute value. The two agree for a non-negative hash and disagree for
/// every negative one, so the mask is what keeps a Krabka producer and a JVM
/// producer on the same partition for the same key.
///
/// # Panics
///
/// Panics when `num_partitions` is not greater than zero.
#[must_use]
pub fn partition_for_key(key: &[u8], num_partitions: i32) -> i32 {
    assert2::assert!(num_partitions > 0);
    // The mask clears the sign bit, so the value is in [0, i32::MAX] and the
    // remainder is in [0, num_partitions).
    (murmur2(key) & 0x7fff_ffff) % num_partitions
}

/// `MurmurHash2`, the key hash of Kafka's `DefaultPartitioner`.
///
/// This is the reference implementation. The length cast to u32 matches the
/// canonical spec.
fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let length = data.len();
    // Reference MurmurHash2 impl truncates length to u32 as part of the spec.
    let usize_u32_max = usize::try_from(u32::MAX).unwrap_or(usize::MAX);
    let length_low =
        u32::try_from(length & usize_u32_max).expect("masked Murmur2 input length must fit in u32");
    let mut h: u32 = SEED ^ length_low;

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();
    for chunk in chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    match remainder.len() {
        3 => {
            h ^= u32::from(remainder[2]) << 16;
            h ^= u32::from(remainder[1]) << 8;
            h ^= u32::from(remainder[0]);
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= u32::from(remainder[1]) << 8;
            h ^= u32::from(remainder[0]);
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= u32::from(remainder[0]);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;

    // Reinterpret bits as i32 — intentional per MurmurHash2 reference spec.
    h.cast_signed()
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use assert2::check;

    use super::*;

    const TOPIC: &str = "t";

    fn config(sticky_batch_size: usize) -> PartitionerConfig {
        PartitionerConfig {
            sticky_batch_size,
            ignore_keys: false,
            adaptive_partitioning: true,
            availability_timeout: Duration::ZERO,
        }
    }

    /// A partitioner whose random values come from `values` in order.
    fn scripted(config: PartitionerConfig, values: &[u32]) -> BuiltInPartitioner {
        let values = Arc::new(Mutex::new(values.iter().copied().collect::<VecDeque<_>>()));
        BuiltInPartitioner::with_random(
            config,
            Arc::new(move || {
                values
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("the test scripts enough random values")
            }),
        )
    }

    fn all_leaders(_: i32) -> bool {
        true
    }

    #[test]
    fn a_new_sticky_partition_is_random_among_partitions_with_a_leader() {
        // Each row: partition count, the partitions with a leader, the random
        // value, and the pick. Kafka's `nextPartition` masks the sign bit and
        // takes the value modulo the partitions with a leader, or modulo all
        // partitions when none has a leader.
        let cases: [(&str, i32, &[i32], u32, i32); 7] = [
            ("zero", 6, &[0, 1, 2, 3, 4, 5], 0, 0),
            ("value 7 of 6", 6, &[0, 1, 2, 3, 4, 5], 7, 1),
            ("sign bit masked", 6, &[0, 1, 2, 3, 4, 5], 0x8000_0005, 5),
            ("p1 and p2 have no leader, value 0", 4, &[0, 3], 0, 0),
            ("p1 and p2 have no leader, value 1", 4, &[0, 3], 1, 3),
            ("p1 and p2 have no leader, value 5", 4, &[0, 3], 5, 3),
            ("no partition has a leader", 4, &[], 6, 2),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (name, count, with_leader, random, want) in cases {
            let partitioner = scripted(config(100), &[random]);
            let has_leader = |partition: i32| with_leader.contains(&partition);
            let partitions = TopicPartitions {
                count,
                has_leader: &has_leader,
            };
            let first = partitioner.peek(TOPIC, &partitions).partition();
            // A second peek keeps the partition and takes no random value.
            let second = partitioner.peek(TOPIC, &partitions).partition();
            actual.push((name, first, second));
            expected.push((name, want, want));
        }
        assert2::assert!(actual == expected);
    }

    #[test]
    fn the_sticky_partition_switches_after_batch_size_bytes() {
        // Each row: the appends as (bytes, enable_switch), and the partition
        // after each append. The random values pick 1, then 2, then 3.
        /// A row: the name, the appends as `(bytes, enable_switch)`, and the
        /// partition after each append.
        type SwitchRow = (&'static str, &'static [(usize, bool)], &'static [i32]);
        let cases: [SwitchRow; 5] = [
            (
                "no switch before batch.size",
                &[(30, true), (30, true), (30, true)],
                &[1, 1, 1],
            ),
            (
                "switch once batch.size bytes went to the partition",
                &[(30, true), (30, true), (30, true), (30, true), (30, true)],
                &[1, 1, 1, 2, 2],
            ),
            (
                "a batch that is not full defers the switch",
                &[(60, false), (60, false), (0, true)],
                &[1, 1, 2],
            ),
            (
                "twice batch.size switches with the switch off",
                &[(60, false), (60, false), (60, false), (60, false)],
                &[1, 1, 1, 2],
            ),
            (
                "a new partition counts from zero",
                &[(100, true), (99, true), (1, true)],
                &[2, 2, 3],
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (name, appends, want) in cases {
            let partitioner = scripted(config(100), &[1, 2, 3]);
            let partitions = TopicPartitions {
                count: 4,
                has_leader: &all_leaders,
            };
            let mut after = Vec::new();
            for &(bytes, enable_switch) in appends {
                let sticky = partitioner.peek(TOPIC, &partitions);
                partitioner.update(TOPIC, sticky, bytes, &partitions, enable_switch);
                after.push(partitioner.peek(TOPIC, &partitions).partition());
            }
            actual.push((name, after));
            expected.push((name, want.to_vec()));
        }
        assert2::assert!(actual == expected);
    }

    #[test]
    fn a_stale_sticky_partition_is_a_change_and_does_not_count_bytes() {
        let partitioner = scripted(config(100), &[2, 2]);
        let partitions = TopicPartitions {
            count: 4,
            has_leader: &all_leaders,
        };
        let old = partitioner.peek(TOPIC, &partitions);
        partitioner.update(TOPIC, old, 100, &partitions, true);
        let new = partitioner.peek(TOPIC, &partitions);
        // The new pick is the same partition number, and it is still a new
        // pick. Bytes for the old pick do not count on the new one.
        partitioner.update(TOPIC, old, 100, &partitions, true);
        check!(
            (
                old.partition(),
                new.partition(),
                partitioner.is_changed(TOPIC, old),
                partitioner.is_changed(TOPIC, new),
                partitioner.peek(TOPIC, &partitions),
            ) == (2, 2, true, false, new)
        );
    }

    #[test]
    fn keys_pick_the_partition_unless_ignore_keys_is_set() {
        let cases = [
            ("keyed", false, Some(b"kafka".as_slice()), Some(0)),
            ("keyless", false, None, None),
            (
                "keyed with ignore keys",
                true,
                Some(b"kafka".as_slice()),
                None,
            ),
        ];
        for (name, ignore_keys, key, want) in cases {
            let partitioner = BuiltInPartitioner::new(PartitionerConfig {
                ignore_keys,
                ..config(100)
            });
            check!(partitioner.keyed_partition(key, 4) == want, "{name}");
        }
    }

    #[test]
    fn distinct_topics_have_distinct_sticky_partitions() {
        let partitioner = scripted(config(100), &[1, 3]);
        let partitions = TopicPartitions {
            count: 4,
            has_leader: &all_leaders,
        };
        let a = partitioner.peek("a", &partitions).partition();
        let b = partitioner.peek("b", &partitions).partition();
        check!((a, b) == (1, 3));
    }

    #[test]
    fn load_stats_follow_kafka() {
        let queues = |sizes: &[u32], partition_ids: &[i32], all: usize| QueueSizes {
            sizes: sizes.to_vec(),
            partition_ids: partition_ids.to_vec(),
            all,
        };
        let stats = |cumulative_frequency: &[u32], partition_ids: &[i32]| {
            Some(LoadStats {
                cumulative_frequency: cumulative_frequency.to_vec(),
                partition_ids: partition_ids.to_vec(),
            })
        };
        let cases = [
            (
                "Kafka's example",
                queues(&[0, 3, 1], &[0, 1, 2], 3),
                stats(&[4, 5, 8], &[0, 1, 2]),
            ),
            ("equal queues", queues(&[2, 2, 2], &[0, 1, 2], 3), None),
            (
                "equal queues, one partition excluded",
                queues(&[2, 2], &[0, 2], 3),
                stats(&[1, 2], &[0, 2]),
            ),
            ("one partition", queues(&[5], &[0], 1), None),
            (
                "one partition left of two",
                queues(&[5], &[1], 2),
                stats(&[1], &[1]),
            ),
            ("no partition left", queues(&[], &[], 3), None),
        ];
        for (name, queues, want) in cases {
            check!(LoadStats::from_queue_sizes(queues) == want, "{name}");
        }
    }

    #[test]
    fn adaptive_pick_weights_by_queue_size() {
        // Queue sizes 0, 3, 1 on partitions 4, 5, 6 give the table 4, 5, 8.
        // Values 0 to 3 pick partition 4, value 4 picks 5, and values 5 to 7
        // pick 6. Value 8 wraps to 0. Each update switches, so each loop takes
        // one value, and the last switch takes one more.
        let partitioner = scripted(config(1), &[0, 1, 2, 3, 4, 5, 6, 7, 8, 0]);
        let partitions = TopicPartitions {
            count: 7,
            has_leader: &all_leaders,
        };
        partitioner.update_load_stats(
            TOPIC,
            Some(QueueSizes {
                sizes: vec![0, 3, 1],
                partition_ids: vec![4, 5, 6],
                all: 3,
            }),
        );
        let picks: Vec<i32> = (0..9)
            .map(|_| {
                let sticky = partitioner.peek(TOPIC, &partitions);
                partitioner.update(TOPIC, sticky, 1, &partitions, true);
                sticky.partition()
            })
            .collect();
        check!(picks == vec![4, 4, 4, 4, 5, 6, 6, 6, 4]);
    }

    #[test]
    fn keyed_partitioning_matches_kafka_to_positive_not_absolute_value() {
        // Kafka computes toPositive(murmur2(key)) % n, and toPositive masks the
        // sign bit. An absolute value instead of the mask sends every key whose
        // hash is negative to a different partition than a JVM producer picks,
        // which breaks key ordering and co-partitioning across the two clients.
        // Each row below is (key, num_partitions, partition), computed with
        // Kafka's own expression.
        for (key, partitions, want) in [
            (b"".as_slice(), 10, 1),
            (b"a".as_slice(), 10, 4),
            (b"ab".as_slice(), 10, 4),
            (b"abc".as_slice(), 10, 7),
            (b"abcd".as_slice(), 10, 0),
            (b"abcde".as_slice(), 10, 1),
            (b"kafka".as_slice(), 10, 0),
            (b"my-key".as_slice(), 10, 9),
            (b"abcd".as_slice(), 16, 4),
            (b"kafka".as_slice(), 16, 4),
            (b"abcd".as_slice(), 3, 2),
            (b"kafka".as_slice(), 3, 1),
        ] {
            let got = partition_for_key(key, partitions);
            assert2::assert!(got == want, "key {key:?} over {partitions} partitions");
            assert2::assert!(got >= 0 && got < partitions);
        }
    }

    #[test]
    fn a_negative_hash_masks_its_sign_bit_rather_than_negating() {
        // "kafka" hashes to -798503068. The mask gives 1348980580, and the
        // absolute value would give 798503068. The two land on different
        // partitions, so this pins the one Kafka uses.
        let hash = murmur2(b"kafka");
        assert2::assert!(hash == -798_503_068);
        assert2::assert!((hash & 0x7fff_ffff) == 1_348_980_580);
        assert2::assert!(partition_for_key(b"kafka", 10) == 1_348_980_580 % 10);
    }

    #[test]
    fn murmur2_matches_kafka_golden_vectors() {
        for (_name, input, want) in [
            ("empty", b"".as_slice(), 275_646_681),
            ("one byte", b"a".as_slice(), -1_563_381_124),
            ("two bytes", b"ab".as_slice(), 316_155_434),
            ("three bytes", b"abc".as_slice(), 479_470_107),
            ("four bytes", b"abcd".as_slice(), -1_323_649_548),
            ("five bytes", b"abcde".as_slice(), 461_995_741),
            ("kafka", b"kafka".as_slice(), -798_503_068),
            ("my key", b"my-key".as_slice(), 1_748_425_209),
        ] {
            assert2::assert!(murmur2(input) == want);
        }
    }
}
