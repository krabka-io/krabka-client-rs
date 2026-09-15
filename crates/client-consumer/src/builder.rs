//! Codec helpers for the `ConsumerProtocol` subscription and assignment
//! payloads, plus the [`AutoOffsetReset`] and [`IsolationLevel`] enums that
//! [`Consumer::builder`] uses.

use bytes::{Bytes, BytesMut};

/// What to do when a partition has no committed offset. Kafka's
/// `auto.offset.reset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoOffsetReset {
    /// Start from offset 0.
    Earliest,
    /// Start from the log-end offset. `Consumer::poll` resolves it lazily with
    /// `ListOffsets(timestamp=-1)`.
    Latest,
    /// Do not reset automatically. On a missing offset or a detected truncation,
    /// `poll` returns `ConsumerError::LogTruncation` and surfaces the error.
    None,
    /// Start from the first offset whose timestamp is at or after now minus
    /// this duration (KIP-1106, `by_duration:<ISO-8601 duration>`).
    /// `Consumer::poll` resolves it with `ListOffsets(timestamp)`.
    ByDuration(std::time::Duration),
}

impl std::str::FromStr for AutoOffsetReset {
    type Err = String;

    /// Parse Kafka's `auto.offset.reset` values: `earliest`, `latest`, `none`
    /// and `by_duration:<ISO-8601 duration>`, as
    /// `AutoOffsetResetStrategy.fromString` does.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "earliest" => Ok(Self::Earliest),
            "latest" => Ok(Self::Latest),
            "none" => Ok(Self::None),
            "by_duration" => {
                Err("<:duration> part is missing in by_duration auto offset reset strategy.".into())
            }
            _ => match value.strip_prefix("by_duration:") {
                Some(duration) => parse_iso_duration(duration).map(Self::ByDuration),
                None => Err(format!("Unknown auto offset reset strategy: {value}")),
            },
        }
    }
}

/// Split the text before `unit` from the front of `part`.
fn duration_component(part: &str, unit: char) -> (Option<&str>, &str) {
    match part.find(unit) {
        Some(end) => (Some(&part[..end]), &part[end + 1..]),
        None => (None, part),
    }
}

/// A decimal integer with an optional sign that fits `i64`, as Java's
/// `Long.parseLong` in `Duration.parse` requires.
fn duration_integer(number: &str) -> Option<i128> {
    let digits = number.strip_prefix(['-', '+']).unwrap_or(number);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    number.parse::<i64>().ok().map(i128::from)
}

/// `value * unit_seconds` seconds in nanoseconds, added to `nanos`, or `None`
/// on overflow.
fn add_duration_part(nanos: i128, value: i128, unit_seconds: i128) -> Option<i128> {
    const NANOS_PER_SECOND: i128 = 1_000_000_000;
    value
        .checked_mul(unit_seconds)?
        .checked_mul(NANOS_PER_SECOND)
        .and_then(|part| nanos.checked_add(part))
}

/// Parse an ISO-8601 duration as Java's `Duration.parse` does:
/// `[-+]P[nD][T[nH][nM][n[.fffffffff]S]]`, case-insensitive, each number with
/// an optional sign. `by_duration` rejects a negative result.
fn parse_iso_duration(text: &str) -> Result<std::time::Duration, String> {
    const NANOS_PER_SECOND: i128 = 1_000_000_000;
    let invalid =
        || "Unable to parse duration string in by_duration offset reset strategy.".to_owned();
    let upper = text.to_ascii_uppercase();
    let (negative, rest) = match upper.as_bytes().first() {
        Some(b'-') => (true, &upper[1..]),
        Some(b'+') => (false, &upper[1..]),
        _ => (false, upper.as_str()),
    };
    let rest = rest.strip_prefix('P').ok_or_else(invalid)?;
    let (date, time) = match rest.split_once('T') {
        Some((date, time)) => (date, Some(time)),
        None => (rest, None),
    };
    let mut nanos: i128 = 0;
    let mut components = 0;
    let (days, date_rest) = duration_component(date, 'D');
    if !date_rest.is_empty() {
        return Err(invalid());
    }
    if let Some(days) = days {
        nanos = add_duration_part(nanos, duration_integer(days).ok_or_else(invalid)?, 86_400)
            .ok_or_else(invalid)?;
        components += 1;
    }
    if let Some(time) = time {
        let (hours, time) = duration_component(time, 'H');
        let (minutes, time) = duration_component(time, 'M');
        let (seconds, time) = duration_component(time, 'S');
        if !time.is_empty() || (hours.is_none() && minutes.is_none() && seconds.is_none()) {
            return Err(invalid());
        }
        if let Some(hours) = hours {
            nanos = add_duration_part(nanos, duration_integer(hours).ok_or_else(invalid)?, 3_600)
                .ok_or_else(invalid)?;
        }
        if let Some(minutes) = minutes {
            nanos = add_duration_part(nanos, duration_integer(minutes).ok_or_else(invalid)?, 60)
                .ok_or_else(invalid)?;
        }
        if let Some(seconds) = seconds {
            let (whole, fraction) = match seconds.split_once(['.', ',']) {
                Some((whole, fraction)) => (whole, fraction),
                None => (seconds, ""),
            };
            if fraction.len() > 9 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid());
            }
            let whole_value = duration_integer(whole).ok_or_else(invalid)?;
            let fraction_value = if fraction.is_empty() {
                0
            } else {
                format!("{fraction:0<9}")
                    .parse::<i128>()
                    .map_err(|_| invalid())?
            };
            let sign = if whole.starts_with('-') { -1 } else { 1 };
            nanos = add_duration_part(nanos, whole_value, 1)
                .and_then(|nanos| nanos.checked_add(sign * fraction_value))
                .ok_or_else(invalid)?;
        }
        components += 1;
    }
    if components == 0 {
        return Err(invalid());
    }
    if negative {
        nanos = -nanos;
    }
    if nanos < 0 {
        return Err(
            "Negative duration is not supported in by_duration offset reset strategy.".into(),
        );
    }
    // Java's `Duration` holds the seconds in a `long`.
    let seconds = i64::try_from(nanos / NANOS_PER_SECOND).map_err(|_| invalid())?;
    let seconds = u64::try_from(seconds).map_err(|_| invalid())?;
    let subsec = u32::try_from(nanos % NANOS_PER_SECOND).map_err(|_| invalid())?;
    Ok(std::time::Duration::new(seconds, subsec))
}

/// Controls which records are visible to this consumer.
///
/// This maps to Kafka's `isolation.level` configuration and to the
/// `isolation_level` field in the `Fetch` request, whose wire value is an `i8`.
///
/// The default is [`ReadUncommitted`](IsolationLevel::ReadUncommitted) for
/// backward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// All records are visible, including those from open or aborted
    /// transactions. Equivalent to `isolation.level=read_uncommitted`.
    ReadUncommitted,
    /// Only records from committed transactions, plus non-transactional
    /// records, are visible. Equivalent to `isolation.level=read_committed`.
    ReadCommitted,
}

impl std::str::FromStr for IsolationLevel {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // Kafka's `ConsumerConfig` accepts exactly these `isolation.level`
        // values.
        match value {
            "read_uncommitted" => Ok(Self::ReadUncommitted),
            "read_committed" => Ok(Self::ReadCommitted),
            _ => Err(format!("invalid isolation level: {value}")),
        }
    }
}

impl IsolationLevel {
    /// Returns the `i8` wire encoding that the `Fetch` request uses.
    pub(crate) fn wire(self) -> i8 {
        match self {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
        }
    }
}

// ── subscription / assignment codec (ConsumerProtocol v3) ─────────────────

use krabka_protocol::{
    Decode, Encode, UnknownTaggedFields,
    owned::{
        consumer_protocol_assignment::{
            ConsumerProtocolAssignment, TopicPartition as AssignTopicPartition,
        },
        consumer_protocol_subscription::{
            ConsumerProtocolSubscription, TopicPartition as SubTopicPartition,
        },
    },
};

const SUBSCRIPTION_WIRE_VERSION: i16 = 3;
const ASSIGNMENT_WIRE_VERSION: i16 = 3;

#[derive(Debug, PartialEq)]
pub(crate) struct DecodedSubscription {
    pub topics: Vec<String>,
    pub owned: Vec<(String, i32)>,
    pub generation_id: i32,
    /// Part of the `ConsumerProtocolSubscription` v3 wire surface. No built-in
    /// assignor reads it yet: rack-aware assignment needs replica racks.
    pub rack_id: Option<String>,
    /// The assignor-specific `userData`.
    pub user_data: Option<Bytes>,
}

fn group_by_topic(pairs: &[(String, i32)]) -> std::collections::BTreeMap<&str, Vec<i32>> {
    let mut by_topic: std::collections::BTreeMap<&str, Vec<i32>> =
        std::collections::BTreeMap::new();
    for (t, p) in pairs {
        by_topic.entry(t.as_str()).or_default().push(*p);
    }
    by_topic
}

fn peek_version(bytes: &[u8]) -> i16 {
    if bytes.len() < 2 {
        return 0;
    }
    i16::from_be_bytes([bytes[0], bytes[1]])
}

fn empty_subscription() -> DecodedSubscription {
    DecodedSubscription {
        topics: Vec::new(),
        owned: Vec::new(),
        generation_id: -1,
        rack_id: None,
        user_data: None,
    }
}

/// Encode a `ConsumerProtocolSubscription` v3 as Kafka's
/// `ConsumerProtocol.serializeSubscription` does: sorted topics, and owned
/// partitions sorted by topic and partition.
pub(crate) fn encode_subscription(
    topics: &[String],
    owned: &[(String, i32)],
    generation_id: i32,
    rack_id: Option<&str>,
    user_data: Option<Bytes>,
) -> Bytes {
    use bytes::BufMut;
    let owned_partitions: Vec<SubTopicPartition> = group_by_topic(owned)
        .into_iter()
        .map(|(topic, mut partitions)| {
            partitions.sort_unstable();
            SubTopicPartition {
                topic: topic.to_string(),
                partitions,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
        })
        .collect();
    let mut sorted_topics = topics.to_vec();
    sorted_topics.sort();
    let msg = ConsumerProtocolSubscription {
        topics: sorted_topics,
        user_data,
        owned_partitions,
        generation_id,
        rack_id: rack_id.map(str::to_string),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::with_capacity(2 + msg.encoded_len(SUBSCRIPTION_WIRE_VERSION));
    buf.put_i16(SUBSCRIPTION_WIRE_VERSION);
    msg.encode(&mut buf, SUBSCRIPTION_WIRE_VERSION)
        .expect("ConsumerProtocolSubscription encode");
    buf.freeze()
}

pub(crate) fn decode_subscription(bytes: &[u8]) -> DecodedSubscription {
    let Some(payload) = bytes.get(2..) else {
        return empty_subscription();
    };
    let version = peek_version(bytes).clamp(0, SUBSCRIPTION_WIRE_VERSION);
    let mut cur = payload;
    let Ok(msg) = ConsumerProtocolSubscription::decode(&mut cur, version) else {
        return empty_subscription();
    };
    let mut owned = Vec::new();
    for tp in msg.owned_partitions {
        for p in tp.partitions {
            owned.push((tp.topic.clone(), p));
        }
    }
    DecodedSubscription {
        topics: msg.topics,
        owned,
        generation_id: msg.generation_id,
        rack_id: msg.rack_id,
        user_data: msg.user_data,
    }
}

pub(crate) fn encode_assignment(partitions: &[(String, i32)]) -> Bytes {
    use bytes::BufMut;
    let assigned_partitions: Vec<AssignTopicPartition> = group_by_topic(partitions)
        .into_iter()
        .map(|(topic, partitions)| AssignTopicPartition {
            topic: topic.to_string(),
            partitions,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
        .collect();
    let msg = ConsumerProtocolAssignment {
        assigned_partitions,
        user_data: None,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::with_capacity(2 + msg.encoded_len(ASSIGNMENT_WIRE_VERSION));
    buf.put_i16(ASSIGNMENT_WIRE_VERSION);
    msg.encode(&mut buf, ASSIGNMENT_WIRE_VERSION)
        .expect("ConsumerProtocolAssignment encode");
    buf.freeze()
}

pub(crate) fn decode_assignment(bytes: &[u8]) -> Vec<(String, i32)> {
    let Some(payload) = bytes.get(2..) else {
        return Vec::new();
    };
    let version = peek_version(bytes).clamp(0, ASSIGNMENT_WIRE_VERSION);
    let mut cur = payload;
    let Ok(msg) = ConsumerProtocolAssignment::decode(&mut cur, version) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for tp in msg.assigned_partitions {
        for p in tp.partitions {
            out.push((tp.topic.clone(), p));
        }
    }
    out
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn consumer_behavior_values_parse_kafka_spellings() {
        use std::time::Duration;
        let reset = |value: &str| value.parse::<AutoOffsetReset>();
        let isolation = |value: &str| value.parse::<IsolationLevel>().map_err(|_| ());
        let actual = (
            [
                reset("earliest"),
                reset("latest"),
                reset("none"),
                reset("by_duration:PT1H"),
                reset("by_duration:P1DT2H3M4.5S"),
                reset("by_duration:pt0.000000001s"),
                reset("by_duration:PT1H-30M"),
                reset("by_duration:+P2D"),
                reset("by_duration"),
                reset("by_duration:-PT1H"),
                reset("by_duration:PT"),
                reset("by_duration:P"),
                reset("by_duration:1H"),
                reset("by_duration:PT1.0000000001S"),
                reset("by_duration:P9223372036854775807D"),
                reset("by_duration:PT9223372036854775808H"),
                reset("by_duration:P106751991167300DT24H"),
                reset("EARLIEST"),
                reset("unknown"),
            ],
            [
                isolation("read_committed"),
                isolation("read_uncommitted"),
                isolation("read-committed"),
                isolation("READ_COMMITTED"),
            ],
        );
        let unparsable =
            Err("Unable to parse duration string in by_duration offset reset strategy.".to_owned());
        let expected = (
            [
                Ok(AutoOffsetReset::Earliest),
                Ok(AutoOffsetReset::Latest),
                Ok(AutoOffsetReset::None),
                Ok(AutoOffsetReset::ByDuration(Duration::from_hours(1))),
                Ok(AutoOffsetReset::ByDuration(Duration::from_millis(
                    ((24 + 2) * 3600 + 3 * 60 + 4) * 1000 + 500,
                ))),
                Ok(AutoOffsetReset::ByDuration(Duration::from_nanos(1))),
                Ok(AutoOffsetReset::ByDuration(Duration::from_mins(30))),
                Ok(AutoOffsetReset::ByDuration(Duration::from_hours(48))),
                Err(
                    "<:duration> part is missing in by_duration auto offset reset strategy."
                        .to_owned(),
                ),
                Err(
                    "Negative duration is not supported in by_duration offset reset strategy."
                        .to_owned(),
                ),
                unparsable.clone(),
                unparsable.clone(),
                unparsable.clone(),
                unparsable.clone(),
                unparsable.clone(),
                unparsable.clone(),
                unparsable,
                Err("Unknown auto offset reset strategy: EARLIEST".to_owned()),
                Err("Unknown auto offset reset strategy: unknown".to_owned()),
            ],
            [
                Ok(IsolationLevel::ReadCommitted),
                Ok(IsolationLevel::ReadUncommitted),
                Err(()),
                Err(()),
            ],
        );
        assert2::assert!(actual == expected);
    }

    #[test]
    fn isolation_level_wire_values_match_fetch_request_encoding() {
        for (_name, isolation, expected) in [
            ("read uncommitted", IsolationLevel::ReadUncommitted, 0),
            ("read committed", IsolationLevel::ReadCommitted, 1),
        ] {
            assert2::assert!(isolation.wire() == expected);
        }
    }

    #[test]
    fn peek_version_requires_two_bytes() {
        for (_name, bytes, want) in [
            ("empty", &[][..], 0),
            ("truncated", &[0x7f][..], 0),
            ("two byte version", &[0, 3][..], 3),
        ] {
            assert2::assert!(peek_version(bytes) == want);
        }
    }

    #[test]
    fn decode_subscription_short_or_malformed_payload_uses_empty_fallback() {
        for (_name, payload) in [
            ("empty", &[][..]),
            ("truncated version", &[0x7f][..]),
            ("version only", &[0, 3][..]),
        ] {
            let decoded = decode_subscription(payload);
            assert2::assert!(
                decoded
                    == DecodedSubscription {
                        topics: Vec::new(),
                        owned: Vec::new(),
                        generation_id: -1,
                        rack_id: None,
                        user_data: None,
                    }
            );
        }
    }

    #[test]
    fn decode_assignment_short_or_malformed_payload_uses_empty_fallback() {
        for (_name, payload) in [
            ("empty", &[][..]),
            ("truncated version", &[0x7f][..]),
            ("version only", &[0, 3][..]),
        ] {
            assert2::assert!(decode_assignment(payload).is_empty());
        }
    }

    /// Kafka's `ConsumerProtocol.serializeSubscription` sorts the topics and
    /// the owned partitions, and carries the user data.
    #[test]
    fn subscription_sorts_topics_and_owned_partitions_and_keeps_user_data() {
        let s = encode_subscription(
            &["t2".into(), "t1".into()],
            &[("t2".into(), 1), ("t1".into(), 3), ("t2".into(), 0)],
            5,
            None,
            Some(Bytes::from_static(&[0, 0, 0, 5])),
        );
        assert2::assert!(
            decode_subscription(&s)
                == DecodedSubscription {
                    topics: vec!["t1".into(), "t2".into()],
                    owned: vec![("t1".into(), 3), ("t2".into(), 0), ("t2".into(), 1)],
                    generation_id: 5,
                    rack_id: None,
                    user_data: Some(Bytes::from_static(&[0, 0, 0, 5])),
                }
        );
    }

    #[test]
    fn subscription_round_trip() {
        let s = encode_subscription(&["t1".into(), "t2".into()], &[], -1, None, None);
        let decoded = decode_subscription(&s);
        assert2::assert!(decoded.topics == vec!["t1", "t2"]);
    }

    #[test]
    fn subscription_empty_round_trip() {
        let s = encode_subscription(&[], &[], -1, None, None);
        let decoded = decode_subscription(&s);
        assert2::assert!(
            decoded
                == DecodedSubscription {
                    topics: Vec::new(),
                    owned: Vec::new(),
                    generation_id: -1,
                    rack_id: None,
                    user_data: None,
                }
        );
    }

    #[test]
    fn subscription_v3_owned_partitions_round_trip() {
        let owned = vec![("t".into(), 0), ("t".into(), 1), ("u".into(), 0)];
        let s = encode_subscription(&["t".into(), "u".into()], &owned, -1, None, None);
        let decoded = decode_subscription(&s);
        let mut got = decoded.owned.clone();
        got.sort();
        let mut want = owned.clone();
        want.sort();
        assert2::assert!(got == want);
    }

    #[test]
    fn subscription_v3_generation_and_rack_round_trip() {
        let s = encode_subscription(&["t".into()], &[], 42, Some("rack-a"), None);
        let decoded = decode_subscription(&s);
        assert2::assert!(
            decoded
                == DecodedSubscription {
                    topics: vec!["t".into()],
                    owned: Vec::new(),
                    generation_id: 42,
                    rack_id: Some("rack-a".into()),
                    user_data: None,
                }
        );
    }

    #[test]
    fn subscription_decodes_v1_payload() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();
        buf.put_i16(1);
        buf.put_i32(1);
        let t = "t1";
        buf.put_i16(i16::try_from(t.len()).unwrap());
        buf.put_slice(t.as_bytes());
        buf.put_i32(-1); // user_data null
        buf.put_i32(0); // owned_partitions empty (v1)
        let payload = buf.freeze();
        let decoded = decode_subscription(&payload);
        assert2::assert!(
            decoded
                == DecodedSubscription {
                    topics: vec!["t1".to_string()],
                    owned: Vec::new(),
                    generation_id: -1,
                    rack_id: None,
                    user_data: None,
                }
        );
    }

    #[test]
    fn assignment_round_trip() {
        let s = encode_assignment(&[("t".into(), 0), ("t".into(), 1), ("u".into(), 0)]);
        let mut decoded = decode_assignment(&s);
        decoded.sort();
        assert2::assert!(
            decoded
                == vec![
                    ("t".to_string(), 0),
                    ("t".to_string(), 1),
                    ("u".to_string(), 0)
                ]
        );
    }

    #[test]
    fn assignment_empty_round_trip() {
        let s = encode_assignment(&[]);
        let decoded = decode_assignment(&s);
        assert2::assert!(decoded.is_empty());
    }
}
