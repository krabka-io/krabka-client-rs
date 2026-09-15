//! Partition assignors: the eager `range`, `roundrobin` and `sticky`, and the
//! incremental `cooperative-sticky` from KIP-429.

pub(crate) mod cooperative_sticky;
pub(crate) mod range;
pub(crate) mod round_robin;

use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::builder::DecodedSubscription;

/// Kafka's `ConsumerPartitionAssignor.RebalanceProtocol`. The order is the
/// order of preference: a later variant is newer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RebalanceProtocol {
    Eager,
    Cooperative,
}

/// A built-in partition assignor, Kafka's `partition.assignment.strategy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Assignor {
    /// `range`, Kafka's `RangeAssignor`.
    Range,
    /// `roundrobin`, Kafka's `RoundRobinAssignor`.
    RoundRobin,
    /// `sticky`, Kafka's eager `StickyAssignor`.
    Sticky,
    /// `cooperative-sticky`, Kafka's `CooperativeStickyAssignor` (KIP-429).
    CooperativeSticky,
}

impl std::str::FromStr for Assignor {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "range" => Ok(Self::Range),
            "roundrobin" => Ok(Self::RoundRobin),
            "sticky" => Ok(Self::Sticky),
            "cooperative-sticky" => Ok(Self::CooperativeSticky),
            _ => Err(format!("invalid assignor: {value}")),
        }
    }
}

impl Assignor {
    /// Kafka's default `partition.assignment.strategy`.
    pub const DEFAULT_LIST: [Self; 2] = [Self::Range, Self::CooperativeSticky];

    /// The protocol name in `JoinGroup` and `SyncGroup`.
    pub(crate) fn protocol_name(self) -> &'static str {
        match self {
            Assignor::Range => "range",
            Assignor::RoundRobin => "roundrobin",
            Assignor::Sticky => "sticky",
            Assignor::CooperativeSticky => "cooperative-sticky",
        }
    }

    /// Kafka's `ConsumerPartitionAssignor.supportedProtocols`.
    fn supported_protocols(self) -> &'static [RebalanceProtocol] {
        match self {
            Assignor::Range | Assignor::RoundRobin | Assignor::Sticky => {
                &[RebalanceProtocol::Eager]
            }
            Assignor::CooperativeSticky => {
                &[RebalanceProtocol::Cooperative, RebalanceProtocol::Eager]
            }
        }
    }

    /// The assignor with `name` in `assignors`.
    pub(crate) fn by_protocol_name(assignors: &[Self], name: &str) -> Option<Self> {
        assignors
            .iter()
            .copied()
            .find(|assignor| assignor.protocol_name() == name)
    }
}

/// Validate an assignor list and return its rebalance protocol.
///
/// Kafka's `ConsumerConfig` rejects a duplicate strategy, and
/// `ConsumerCoordinator` uses the newest rebalance protocol that every assignor
/// supports. A subscribing consumer needs at least one assignor
/// (`ConsumerCoordinator.poll`).
pub(crate) fn rebalance_protocol_of(assignors: &[Assignor]) -> Result<RebalanceProtocol, String> {
    for (index, assignor) in assignors.iter().enumerate() {
        if assignors[..index].contains(assignor) {
            return Err(format!(
                "consumer assignors: duplicate assignor {}",
                assignor.protocol_name()
            ));
        }
    }
    let first = assignors
        .first()
        .ok_or_else(|| "consumer assignors must not be empty".to_owned())?;
    first
        .supported_protocols()
        .iter()
        .copied()
        .filter(|protocol| {
            assignors
                .iter()
                .all(|assignor| assignor.supported_protocols().contains(protocol))
        })
        .max()
        .ok_or_else(|| "consumer assignors have no common rebalance protocol".to_owned())
}

/// The identity by which Kafka's `AbstractPartitionAssignor.MemberInfo` sorts
/// members: static members first by `group.instance.id`, then dynamic members
/// by member id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberKey {
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

impl Ord for MemberKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (&self.group_instance_id, &other.group_instance_id) {
            (Some(a), Some(b)) => a.cmp(b),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => self.member_id.cmp(&other.member_id),
        }
    }
}

impl PartialOrd for MemberKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// One member of a `JoinGroup` response, with its decoded subscription.
pub(crate) struct GroupMember {
    pub key: MemberKey,
    pub subscription: DecodedSubscription,
}

/// Kafka's `AbstractStickyAssignor.DEFAULT_GENERATION`.
const DEFAULT_GENERATION: i32 = -1;

/// The `userData` of the subscription for `assignor`.
///
/// `sticky` sends its last assignment and generation (`StickyAssignor`
/// user data v1), and nothing before its first assignment.
/// `cooperative-sticky` sends its generation. The other assignors send
/// nothing.
pub(crate) fn subscription_user_data(
    assignor: Assignor,
    last_assignment: Option<&[(String, i32)]>,
    generation: i32,
) -> Option<Bytes> {
    match assignor {
        Assignor::Range | Assignor::RoundRobin => None,
        Assignor::Sticky => last_assignment.map(|partitions| {
            let mut by_topic: std::collections::BTreeMap<&str, Vec<i32>> =
                std::collections::BTreeMap::new();
            for (topic, partition) in partitions {
                by_topic.entry(topic).or_default().push(*partition);
            }
            let mut buf = BytesMut::new();
            put_array_len(&mut buf, by_topic.len());
            for (topic, mut partitions) in by_topic {
                partitions.sort_unstable();
                buf.put_i16(i16::try_from(topic.len()).unwrap_or(i16::MAX));
                buf.put_slice(topic.as_bytes());
                put_array_len(&mut buf, partitions.len());
                for partition in partitions {
                    buf.put_i32(partition);
                }
            }
            buf.put_i32(generation);
            buf.freeze()
        }),
        Assignor::CooperativeSticky => {
            let mut buf = BytesMut::with_capacity(4);
            buf.put_i32(generation);
            Some(buf.freeze())
        }
    }
}

fn put_array_len(buf: &mut BytesMut, len: usize) {
    buf.put_i32(i32::try_from(len).unwrap_or(i32::MAX));
}

/// The owned partitions and generation in `sticky` user data.
///
/// Kafka's `StickyAssignor.deserializeTopicPartitionAssignment` reads v1, then
/// v0 without a generation, and ignores user data that neither parses.
fn decode_sticky_user_data(user_data: Option<&Bytes>) -> (Vec<(String, i32)>, i32) {
    let Some(user_data) = user_data.filter(|data| !data.is_empty()) else {
        return (Vec::new(), DEFAULT_GENERATION);
    };
    let mut buf = user_data.clone();
    let Some(partitions) = read_topic_partitions(&mut buf) else {
        return (Vec::new(), DEFAULT_GENERATION);
    };
    let generation = if buf.remaining() >= 4 {
        buf.get_i32()
    } else {
        DEFAULT_GENERATION
    };
    (partitions, generation)
}

fn read_topic_partitions(buf: &mut Bytes) -> Option<Vec<(String, i32)>> {
    let mut partitions = Vec::new();
    if buf.remaining() < 4 {
        return None;
    }
    let topics = usize::try_from(buf.get_i32()).ok()?;
    for _ in 0..topics {
        if buf.remaining() < 2 {
            return None;
        }
        let len = usize::try_from(buf.get_i16()).ok()?;
        if buf.remaining() < len + 4 {
            return None;
        }
        let topic = String::from_utf8(buf.split_to(len).to_vec()).ok()?;
        let count = usize::try_from(buf.get_i32()).ok()?;
        if buf.remaining() < count.checked_mul(4)? {
            return None;
        }
        for _ in 0..count {
            partitions.push((topic.clone(), buf.get_i32()));
        }
    }
    Some(partitions)
}

/// Run `assignor` over the members of a `JoinGroup` response, as the group
/// leader does in Kafka's `ConsumerCoordinator.onLeaderElected`.
pub(crate) fn assign(
    assignor: Assignor,
    members: &[GroupMember],
    topic_partitions: &HashMap<String, i32>,
) -> HashMap<String, Vec<(String, i32)>> {
    match assignor {
        Assignor::Range => range::assign_members(
            members
                .iter()
                .map(|member| (member.key.clone(), member.subscription.topics.clone()))
                .collect(),
            topic_partitions,
        ),
        Assignor::RoundRobin => round_robin::assign(
            members
                .iter()
                .map(|member| (member.key.clone(), member.subscription.topics.clone()))
                .collect(),
            topic_partitions,
        ),
        Assignor::Sticky => {
            // Kafka's `StickyAssignor.memberData` reads the owned partitions
            // and the generation from the user data only.
            let inputs: Vec<cooperative_sticky::MemberInput> = members
                .iter()
                .map(|member| {
                    let (owned, generation) =
                        decode_sticky_user_data(member.subscription.user_data.as_ref());
                    (
                        member.key.member_id.clone(),
                        member.subscription.topics.clone(),
                        owned,
                        generation,
                    )
                })
                .collect();
            cooperative_sticky::assign_eager(&inputs, topic_partitions)
        }
        Assignor::CooperativeSticky => {
            let inputs: Vec<cooperative_sticky::MemberInput> = members
                .iter()
                .map(|member| {
                    (
                        member.key.member_id.clone(),
                        member.subscription.topics.clone(),
                        member.subscription.owned.clone(),
                        member.subscription.generation_id,
                    )
                })
                .collect();
            cooperative_sticky::assign(&inputs, topic_partitions)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn assignor_values_parse_kafka_names() {
        let actual = [
            "range",
            "roundrobin",
            "sticky",
            "cooperative-sticky",
            "cooperative_sticky",
            "round-robin",
            "unknown",
        ]
        .map(|name| name.parse::<Assignor>().ok());
        assert2::assert!(
            actual
                == [
                    Some(Assignor::Range),
                    Some(Assignor::RoundRobin),
                    Some(Assignor::Sticky),
                    Some(Assignor::CooperativeSticky),
                    None,
                    None,
                    None,
                ]
        );
    }

    #[test]
    fn assignor_protocol_names_match_kafka_protocols() {
        let actual = [
            Assignor::Range,
            Assignor::RoundRobin,
            Assignor::Sticky,
            Assignor::CooperativeSticky,
        ]
        .map(Assignor::protocol_name);
        assert2::assert!(actual == ["range", "roundrobin", "sticky", "cooperative-sticky"]);
    }

    /// Kafka's `ConsumerCoordinator` constructor keeps the rebalance protocols
    /// that every assignor supports and uses the newest one.
    #[test]
    fn rebalance_protocol_is_the_newest_common_protocol() {
        use Assignor::{CooperativeSticky, Range, RoundRobin, Sticky};
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, assignors, expected) in [
            (
                "default list",
                &Assignor::DEFAULT_LIST[..],
                Ok(RebalanceProtocol::Eager),
            ),
            (
                "cooperative sticky only",
                &[CooperativeSticky][..],
                Ok(RebalanceProtocol::Cooperative),
            ),
            (
                "round robin",
                &[RoundRobin][..],
                Ok(RebalanceProtocol::Eager),
            ),
            (
                "sticky and cooperative sticky",
                &[CooperativeSticky, Sticky][..],
                Ok(RebalanceProtocol::Eager),
            ),
            (
                "empty",
                &[][..],
                Err("consumer assignors must not be empty".to_owned()),
            ),
            (
                "duplicate",
                &[Range, Range][..],
                Err("consumer assignors: duplicate assignor range".to_owned()),
            ),
        ] {
            actual.push((name, rebalance_protocol_of(assignors)));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }

    /// The byte layout of Kafka's `StickyAssignor` user data v1 and
    /// `CooperativeStickyAssignor` user data v0.
    #[test]
    fn subscription_user_data_matches_kafka_layouts() {
        let owned = vec![
            ("b".to_owned(), 1),
            ("a".to_owned(), 2),
            ("b".to_owned(), 0),
        ];
        let actual = [
            subscription_user_data(Assignor::Range, Some(&owned), 7),
            subscription_user_data(Assignor::RoundRobin, Some(&owned), 7),
            subscription_user_data(Assignor::Sticky, None, 7),
            subscription_user_data(Assignor::Sticky, Some(&owned), 7),
            subscription_user_data(Assignor::CooperativeSticky, Some(&owned), 7),
        ];
        let sticky = Bytes::from_static(&[
            0, 0, 0, 2, // two topics
            0, 1, b'a', 0, 0, 0, 1, 0, 0, 0, 2, // a: [2]
            0, 1, b'b', 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, // b: [0, 1]
            0, 0, 0, 7, // generation
        ]);
        assert2::assert!(
            actual
                == [
                    None,
                    None,
                    None,
                    Some(sticky.clone()),
                    Some(Bytes::from_static(&[0, 0, 0, 7])),
                ]
        );
        let v0 = sticky.slice(..sticky.len() - 4);
        let decoded = [
            decode_sticky_user_data(Some(&sticky)),
            decode_sticky_user_data(Some(&v0)),
            decode_sticky_user_data(Some(&Bytes::from_static(&[0, 0]))),
            decode_sticky_user_data(None),
        ];
        let sorted = vec![
            ("a".to_owned(), 2),
            ("b".to_owned(), 0),
            ("b".to_owned(), 1),
        ];
        assert2::assert!(
            decoded
                == [
                    (sorted.clone(), 7),
                    (sorted, -1),
                    (Vec::new(), -1),
                    (Vec::new(), -1),
                ]
        );
    }

    /// The eager `sticky` assignor moves partitions to a new member at once. The
    /// `cooperative-sticky` assignor holds a moving partition back for the
    /// next round.
    #[test]
    fn sticky_moves_partitions_at_once_and_cooperative_sticky_holds_them_back() {
        let counts = HashMap::from([("t".to_owned(), 4)]);
        let owned = vec![
            ("t".to_owned(), 0),
            ("t".to_owned(), 1),
            ("t".to_owned(), 2),
            ("t".to_owned(), 3),
        ];
        let member = |id: &str, user_data: Option<Bytes>, owned: Vec<(String, i32)>, generation| {
            GroupMember {
                key: MemberKey {
                    member_id: id.to_owned(),
                    group_instance_id: None,
                },
                subscription: DecodedSubscription {
                    topics: vec!["t".to_owned()],
                    owned,
                    generation_id: generation,
                    rack_id: None,
                    user_data,
                },
            }
        };
        let sticky_members = [
            member(
                "a",
                subscription_user_data(Assignor::Sticky, Some(&owned), 3),
                Vec::new(),
                3,
            ),
            member("b", None, Vec::new(), -1),
        ];
        let cooperative_members = [
            member(
                "a",
                subscription_user_data(Assignor::CooperativeSticky, Some(&owned), 3),
                owned.clone(),
                3,
            ),
            member(
                "b",
                subscription_user_data(Assignor::CooperativeSticky, None, -1),
                Vec::new(),
                -1,
            ),
        ];
        let actual = (
            assign(Assignor::Sticky, &sticky_members, &counts),
            assign(Assignor::CooperativeSticky, &cooperative_members, &counts),
        );
        assert2::assert!(
            actual
                == (
                    HashMap::from([
                        (
                            "a".to_owned(),
                            vec![("t".to_owned(), 0), ("t".to_owned(), 1)]
                        ),
                        (
                            "b".to_owned(),
                            vec![("t".to_owned(), 2), ("t".to_owned(), 3)]
                        ),
                    ]),
                    HashMap::from([
                        (
                            "a".to_owned(),
                            vec![("t".to_owned(), 0), ("t".to_owned(), 1)]
                        ),
                        ("b".to_owned(), Vec::new()),
                    ]),
                )
        );
    }

    /// Kafka's `MemberInfo.compareTo`: static members first, by instance id.
    #[test]
    fn range_orders_static_members_by_group_instance_id() {
        let counts = HashMap::from([("t".to_owned(), 3)]);
        let member = |id: &str, instance: Option<&str>| GroupMember {
            key: MemberKey {
                member_id: id.to_owned(),
                group_instance_id: instance.map(str::to_owned),
            },
            subscription: DecodedSubscription {
                topics: vec!["t".to_owned()],
                owned: Vec::new(),
                generation_id: -1,
                rack_id: None,
                user_data: None,
            },
        };
        let members = [member("a", None), member("z", Some("i-1"))];
        assert2::assert!(
            assign(Assignor::Range, &members, &counts)
                == HashMap::from([
                    (
                        "z".to_owned(),
                        vec![("t".to_owned(), 0), ("t".to_owned(), 1)]
                    ),
                    ("a".to_owned(), vec![("t".to_owned(), 2)]),
                ])
        );
    }
}
