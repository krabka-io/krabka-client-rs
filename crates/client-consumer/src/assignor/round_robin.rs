//! Round robin assignor, Kafka's `RoundRobinAssignor`.
//!
//! It sorts every partition of every subscribed topic by topic and partition,
//! and hands them out one by one to the members in member order, skipping a
//! member that does not subscribe to the topic of the partition.

use std::collections::{BTreeSet, HashMap};

use super::MemberKey;

/// Returns `member_id → Vec<(topic, partition)>` assignments.
#[must_use]
#[tracing::instrument(
    name = "consumer.assignor.round_robin",
    level = "info",
    skip_all,
    fields(members = members.len(), topics = topic_partitions.len())
)]
pub fn assign(
    mut members: Vec<(MemberKey, Vec<String>)>,
    topic_partitions: &HashMap<String, i32>,
) -> HashMap<String, Vec<(String, i32)>> {
    let mut out: HashMap<String, Vec<(String, i32)>> = members
        .iter()
        .map(|(member, _)| (member.member_id.clone(), Vec::new()))
        .collect();
    if members.is_empty() {
        return out;
    }
    members.sort_by(|(a, _), (b, _)| a.cmp(b));
    let topics: BTreeSet<&String> = members.iter().flat_map(|(_, topics)| topics).collect();
    let mut next = 0;
    for topic in topics {
        let Some(&count) = topic_partitions.get(topic) else {
            continue;
        };
        for partition in 0..count.max(0) {
            // Kafka's `CircularIterator`: advance to the next member that
            // subscribes to the topic. One member subscribes to it at least.
            while !members[next % members.len()].1.contains(topic) {
                next += 1;
            }
            let member = &members[next % members.len()].0.member_id;
            next += 1;
            if let Some(slot) = out.get_mut(member) {
                slot.push((topic.clone(), partition));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, instance: Option<&str>, topics: &[&str]) -> (MemberKey, Vec<String>) {
        (
            MemberKey {
                member_id: id.into(),
                group_instance_id: instance.map(str::to_owned),
            },
            topics.iter().map(|topic| (*topic).to_owned()).collect(),
        )
    }

    fn partitions(items: &[(&str, i32)]) -> Vec<(String, i32)> {
        items
            .iter()
            .map(|(topic, p)| ((*topic).to_owned(), *p))
            .collect()
    }

    /// The cases of Kafka's `RoundRobinAssignorTest`.
    #[test]
    fn round_robin_assigns_as_kafka_does() {
        let counts = HashMap::from([("t1".to_owned(), 3), ("t2".to_owned(), 3)]);
        let mut actual = Vec::new();
        let mut wanted = Vec::new();
        for (name, members, expected) in [
            (
                "two members, two topics",
                vec![
                    member("c1", None, &["t1", "t2"]),
                    member("c2", None, &["t1", "t2"]),
                ],
                HashMap::from([
                    (
                        "c1".to_owned(),
                        partitions(&[("t1", 0), ("t1", 2), ("t2", 1)]),
                    ),
                    (
                        "c2".to_owned(),
                        partitions(&[("t1", 1), ("t2", 0), ("t2", 2)]),
                    ),
                ]),
            ),
            (
                "members with different subscriptions",
                vec![
                    member("c0", None, &["t1"]),
                    member("c1", None, &["t1", "t2"]),
                    member("c2", None, &["t1"]),
                ],
                HashMap::from([
                    ("c0".to_owned(), partitions(&[("t1", 0)])),
                    (
                        "c1".to_owned(),
                        partitions(&[("t1", 1), ("t2", 0), ("t2", 1), ("t2", 2)]),
                    ),
                    ("c2".to_owned(), partitions(&[("t1", 2)])),
                ]),
            ),
            (
                "static members sort by group instance id",
                vec![
                    member("b", Some("i2"), &["t1"]),
                    member("a", Some("i1"), &["t1"]),
                ],
                HashMap::from([
                    ("a".to_owned(), partitions(&[("t1", 0), ("t1", 2)])),
                    ("b".to_owned(), partitions(&[("t1", 1)])),
                ]),
            ),
            (
                "a topic without metadata",
                vec![member("c1", None, &["t3"])],
                HashMap::from([("c1".to_owned(), Vec::new())]),
            ),
        ] {
            actual.push((name, assign(members, &counts)));
            wanted.push((name, expected));
        }
        assert2::assert!(actual == wanted);
    }
}
