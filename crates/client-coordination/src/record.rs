//! The frozen `__coordination_state` record layouts and their codec.
//!
//! A member of a coordination role writes its registration and its lease to the
//! compacted internal topic [`COORDINATION_STATE_TOPIC`]. The key carries the
//! record kind, so one topic holds both kinds. The key is also the compaction
//! key, so the topic keeps the last record of every role and member. A record
//! with a null value is a tombstone. A tombstone of kind 0 deregisters one
//! member, and a tombstone of kind 1 clears the lease of one role.
//!
//! The layouts below are frozen. `krabka-streams-java` and `krabka-streams-go`
//! re-implement them byte for byte, and all three projects assert the same
//! golden bytes. Do not change a field, a field order, or a version number
//! without the same change in the two ports.
//!
//! ```text
//! key:
//!   version  i16 = 0
//!   kind     i16          0 registration, 1 lease
//!   role     string
//!   member   string       the member id for kind 0, the empty string for kind 1
//!
//! registration value:
//!   version        i16 = 0
//!   member         string
//!   registered_at  i64    epoch milliseconds
//!
//! lease value:
//!   version         i16 = 0
//!   member          string
//!   producer_id     i64
//!   producer_epoch  i16
//!   granted_at      i64   epoch milliseconds
//!   deadline        i64   epoch milliseconds
//! ```
//!
//! Every integer is big-endian and signed. A string is an `i16` byte length and
//! then plain UTF-8 bytes. This is Kafka's own native string layout. It is not
//! Java's modified UTF-8, which `DataOutput::writeUTF` produces. The length is
//! never negative, because the format has no null string. An absent member is
//! the empty string, and it encodes as the two bytes `00 00`.

use std::{fmt, str::FromStr};

use bytes::{BufMut, Bytes, BytesMut};

/// The compacted internal topic that holds the coordination state of a cluster.
pub const COORDINATION_STATE_TOPIC: &str = "__coordination_state";

/// The maximum length of a [`Role`], in bytes.
///
/// A role becomes a Kafka `transactional.id`. The bound is the same bound Kafka
/// puts on a topic name, so a role stays short enough to log and to compare.
pub const MAX_ROLE_LEN: usize = 249;

/// The maximum length of a [`MemberId`], in bytes.
pub const MAX_MEMBER_ID_LEN: usize = 249;

/// The version that every key and every value of the topic carries.
const RECORD_VERSION: i16 = 0;

/// The record part a decode failed on. Each names the [`RecordError::Format`]
/// `part` field.
const KEY_PART: &str = "key";
const REGISTRATION_VALUE_PART: &str = "registration value";
const LEASE_VALUE_PART: &str = "lease value";

/// The field names that [`RecordError::Empty`] and [`RecordError::TooLong`]
/// report.
const ROLE_FIELD: &str = "role";
const MEMBER_FIELD: &str = "member id";

/// A failure to build or to decode a coordination state record.
///
/// [`RecordError::Format`] reports malformed bytes, and its `part` field names
/// the record part that failed. The other variants report a value that a
/// caller or a record gave, and each names the field it rejects.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RecordError {
    /// The bytes do not match the frozen layout of the record part.
    #[error("malformed coordination state {part}: {message}")]
    Format {
        /// The record part the decoder failed on. It is `key`,
        /// `registration value`, or `lease value`.
        part: &'static str,
        /// What the decoder found.
        message: String,
    },

    /// A role or a member id is the empty string.
    #[error("a coordination {field} must not be empty")]
    Empty {
        /// The field that is empty.
        field: &'static str,
    },

    /// A role or a member id is longer than its bound.
    #[error("a coordination {field} of {length} bytes is longer than the {bound}-byte maximum")]
    TooLong {
        /// The field that is too long.
        field: &'static str,
        /// The length the caller gave, in bytes.
        length: usize,
        /// The maximum length of the field, in bytes.
        bound: usize,
    },

    /// A fencing token carries a negative producer id or a negative producer
    /// epoch.
    #[error("a fencing token must not be negative, got {producer_id}:{producer_epoch}")]
    NegativeFencingToken {
        /// The producer id the caller gave.
        producer_id: i64,
        /// The producer epoch the caller gave.
        producer_epoch: i16,
    },

    /// A string does not have the `producer_id:producer_epoch` form of a
    /// fencing token.
    #[error("expected a fencing token of the form producer_id:producer_epoch, got {text:?}")]
    FencingTokenForm {
        /// The string the caller gave.
        text: String,
    },
}

/// The name of a coordination role, such as the name of an active controller.
///
/// A role becomes a Kafka `transactional.id`. The transaction coordinator mints
/// one producer epoch for that id, so the role names the fenced group of
/// members that compete for the same leadership.
///
/// [`Role::new`] is the only way to build one, and it rejects an empty name and
/// a name longer than [`MAX_ROLE_LEN`]. A `Role` value is proof that the name
/// is well formed, so this type does not implement `From<String>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Role(String);

impl Role {
    /// Parses a role name.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError::Empty`] when `role` is the empty string. Returns
    /// [`RecordError::TooLong`] when `role` holds more than [`MAX_ROLE_LEN`]
    /// bytes.
    pub fn new(role: &str) -> Result<Self, RecordError> {
        check_name(role, ROLE_FIELD, MAX_ROLE_LEN)?;
        Ok(Self(role.to_owned()))
    }

    /// The role name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Role {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The identity of one member that competes for a role.
///
/// A member picks its own id and keeps it across a reconnect, so the succession
/// order survives a short network failure.
///
/// [`MemberId::new`] is the only way to build one, and it rejects an empty id
/// and an id longer than [`MAX_MEMBER_ID_LEN`]. A `MemberId` value is proof
/// that the id is well formed, so this type does not implement `From<String>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MemberId(String);

impl MemberId {
    /// Parses a member id.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError::Empty`] when `member` is the empty string.
    /// Returns [`RecordError::TooLong`] when `member` holds more than
    /// [`MAX_MEMBER_ID_LEN`] bytes.
    pub fn new(member: &str) -> Result<Self, RecordError> {
        check_name(member, MEMBER_FIELD, MAX_MEMBER_ID_LEN)?;
        Ok(Self(member.to_owned()))
    }

    /// The member id.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MemberId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Rejects an empty name and a name past `bound`.
fn check_name(name: &str, field: &'static str, bound: usize) -> Result<(), RecordError> {
    if name.is_empty() {
        return Err(RecordError::Empty { field });
    }
    if name.len() > bound {
        return Err(RecordError::TooLong {
            field,
            length: name.len(),
            bound,
        });
    }
    Ok(())
}

/// The quorum-minted proof that one member holds a role.
///
/// The transaction coordinator mints the pair when a member calls
/// `InitProducerId` for the `transactional.id` of the role. The quorum picks
/// the values, the broker enforces them, and a write from an older pair fails
/// with a fenced error. A holder passes the token to every guarded write.
///
/// The field order is load-bearing. The derived [`Ord`] compares `producer_id`
/// first and `producer_epoch` second, so it is lexicographic. A producer epoch
/// is an `i16` and it wraps. Kafka then allocates a new producer id and resets
/// the epoch to zero. A comparison on the epoch alone would rank that fresh
/// token below the stale one, and the old leader would keep the role. Never
/// reorder these two fields, and never compare the epoch on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FencingToken {
    producer_id: i64,
    producer_epoch: i16,
}

impl FencingToken {
    /// Builds a fencing token from a minted producer id and producer epoch.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError::NegativeFencingToken`] when either value is
    /// negative. Kafka writes `-1` for "no producer", and that pair never
    /// proves leadership.
    pub fn new(producer_id: i64, producer_epoch: i16) -> Result<Self, RecordError> {
        if producer_id < 0 || producer_epoch < 0 {
            return Err(RecordError::NegativeFencingToken {
                producer_id,
                producer_epoch,
            });
        }
        Ok(Self {
            producer_id,
            producer_epoch,
        })
    }

    /// The producer id the transaction coordinator minted.
    #[must_use]
    pub const fn producer_id(self) -> i64 {
        self.producer_id
    }

    /// The producer epoch the transaction coordinator minted.
    #[must_use]
    pub const fn producer_epoch(self) -> i16 {
        self.producer_epoch
    }
}

impl fmt::Display for FencingToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.producer_id, self.producer_epoch)
    }
}

impl FromStr for FencingToken {
    type Err = RecordError;

    fn from_str(serialized: &str) -> Result<Self, Self::Err> {
        let form = || RecordError::FencingTokenForm {
            text: serialized.to_owned(),
        };
        let (producer_id, producer_epoch) = serialized.split_once(':').ok_or_else(form)?;
        if producer_epoch.contains(':') {
            return Err(form());
        }
        Self::new(
            producer_id.parse().map_err(|_error| form())?,
            producer_epoch.parse().map_err(|_error| form())?,
        )
    }
}

/// Which record of [`COORDINATION_STATE_TOPIC`] a key names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RecordKind {
    /// One member of a role announces that it is available.
    Registration,
    /// One role names the member that holds it now.
    Lease,
}

impl RecordKind {
    /// The wire code a writer puts in the key.
    #[must_use]
    pub const fn code(self) -> i16 {
        match self {
            Self::Registration => 0,
            Self::Lease => 1,
        }
    }

    /// Maps a wire code onto a record kind.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError::Format`] when no record kind carries `code`.
    pub fn from_code(code: i16) -> Result<Self, RecordError> {
        match code {
            0 => Ok(Self::Registration),
            1 => Ok(Self::Lease),
            other => Err(RecordError::Format {
                part: KEY_PART,
                message: format!("unknown coordination state record kind {other}"),
            }),
        }
    }
}

/// The decoded key of a coordination state record.
///
/// `member` names one member for [`RecordKind::Registration`], and it is `None`
/// for [`RecordKind::Lease`]. A lease belongs to the role and not to one
/// member, so the lease key holds the empty string in that position.
/// [`CoordinationKey::registration`] and [`CoordinationKey::lease`] build a key
/// that keeps this rule.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CoordinationKey {
    /// The record the key names.
    pub kind: RecordKind,
    /// The role the record belongs to.
    pub role: Role,
    /// The member for a registration key, and `None` for a lease key.
    pub member: Option<MemberId>,
}

impl CoordinationKey {
    /// The key of the registration of one member of a role.
    #[must_use]
    pub fn registration(role: Role, member: MemberId) -> Self {
        Self {
            kind: RecordKind::Registration,
            role,
            member: Some(member),
        }
    }

    /// The key of the lease of a role.
    #[must_use]
    pub fn lease(role: Role) -> Self {
        Self {
            kind: RecordKind::Lease,
            role,
            member: None,
        }
    }
}

/// One member of a role announces that it is available.
///
/// A member writes a registration when it joins, and it writes a tombstone on
/// the same key when it leaves.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Registration {
    /// The member that registered.
    pub member: MemberId,
    /// The time the member registered, in milliseconds since the Unix epoch.
    pub registered_at: i64,
}

/// The member that holds a role now, and the time its claim ends.
///
/// The `token` is the authority. The broker fences a write that carries an
/// older token, and no clock takes part in that check. The `deadline` is an
/// anti-flap device. A standby waits for the deadline to pass before it
/// challenges the holder, so a short pause does not move the role.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Lease {
    /// The member that holds the role.
    pub member: MemberId,
    /// The quorum-minted proof of the claim of the holder.
    pub token: FencingToken,
    /// The time the holder took the lease, in milliseconds since the Unix
    /// epoch.
    pub granted_at: i64,
    /// The time the lease expires, in milliseconds since the Unix epoch.
    pub deadline: i64,
}

/// One decoded value of [`COORDINATION_STATE_TOPIC`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CoordinationRecord {
    /// The value of a registration record.
    Registration(Registration),
    /// The value of a lease record.
    Lease(Lease),
    /// A null value. It deregisters the member of a registration key, and it
    /// clears the lease of a lease key.
    Tombstone,
}

/// Encodes a key of [`COORDINATION_STATE_TOPIC`].
///
/// The function writes the empty member string for a lease key, whatever
/// `key.member` holds, because the frozen layout gives a lease no member.
#[must_use]
pub fn encode_key(key: &CoordinationKey) -> Bytes {
    let member = match key.kind {
        RecordKind::Registration => key.member.as_ref().map_or("", MemberId::as_str),
        RecordKind::Lease => "",
    };
    let role = key.role.as_str();
    let mut buffer = BytesMut::with_capacity(8 + role.len() + member.len());
    buffer.put_i16(RECORD_VERSION);
    buffer.put_i16(key.kind.code());
    put_string(&mut buffer, role);
    put_string(&mut buffer, member);
    buffer.freeze()
}

/// Encodes a value of [`COORDINATION_STATE_TOPIC`].
///
/// The result is `None` for [`CoordinationRecord::Tombstone`], because a
/// tombstone is a record with a null value.
#[must_use]
pub fn encode_value(record: &CoordinationRecord) -> Option<Bytes> {
    match record {
        CoordinationRecord::Registration(registration) => {
            let member = registration.member.as_str();
            let mut buffer = BytesMut::with_capacity(12 + member.len());
            buffer.put_i16(RECORD_VERSION);
            put_string(&mut buffer, member);
            buffer.put_i64(registration.registered_at);
            Some(buffer.freeze())
        }
        CoordinationRecord::Lease(lease) => {
            let member = lease.member.as_str();
            let mut buffer = BytesMut::with_capacity(30 + member.len());
            buffer.put_i16(RECORD_VERSION);
            put_string(&mut buffer, member);
            buffer.put_i64(lease.token.producer_id());
            buffer.put_i16(lease.token.producer_epoch());
            buffer.put_i64(lease.granted_at);
            buffer.put_i64(lease.deadline);
            Some(buffer.freeze())
        }
        CoordinationRecord::Tombstone => None,
    }
}

/// Decodes a key of [`COORDINATION_STATE_TOPIC`].
///
/// # Errors
///
/// Returns [`RecordError::Format`] when the buffer is truncated, when it holds
/// trailing bytes, when the version is not `0`, when the kind is neither `0`
/// nor `1`, when a string length is negative, when a string is not UTF-8, or
/// when a lease key names a member. Returns [`RecordError::Empty`] or
/// [`RecordError::TooLong`] when the role, or the member of a registration key,
/// is empty or too long.
pub fn decode_key(bytes: &[u8]) -> Result<CoordinationKey, RecordError> {
    let mut reader = Reader::new(bytes, KEY_PART);
    reader.version()?;
    let kind = RecordKind::from_code(reader.i16()?)?;
    let role = reader.string()?;
    let member = reader.string()?;
    reader.finish()?;
    let member = match kind {
        RecordKind::Registration => Some(MemberId::new(member)?),
        RecordKind::Lease => {
            if !member.is_empty() {
                return Err(reader.malformed(format!(
                    "a lease key carries the empty member string, got {member:?}"
                )));
            }
            None
        }
    };
    Ok(CoordinationKey {
        kind,
        role: Role::new(role)?,
        member,
    })
}

/// Decodes a value of [`COORDINATION_STATE_TOPIC`] under the kind of its key.
///
/// `bytes` is `None` for a record with a null value, and the result is then
/// [`CoordinationRecord::Tombstone`] for either kind.
///
/// # Errors
///
/// Returns [`RecordError::Format`] when the buffer is truncated, when it holds
/// trailing bytes, when the version is not `0`, when a string length is
/// negative, or when a string is not UTF-8. Returns [`RecordError::Empty`] or
/// [`RecordError::TooLong`] when the member is empty or too long. Returns
/// [`RecordError::NegativeFencingToken`] when a lease value carries a negative
/// producer id or producer epoch.
pub fn decode_value(
    kind: RecordKind,
    bytes: Option<&[u8]>,
) -> Result<CoordinationRecord, RecordError> {
    let Some(bytes) = bytes else {
        return Ok(CoordinationRecord::Tombstone);
    };
    match kind {
        RecordKind::Registration => {
            decode_registration(bytes).map(CoordinationRecord::Registration)
        }
        RecordKind::Lease => decode_lease(bytes).map(CoordinationRecord::Lease),
    }
}

fn decode_registration(bytes: &[u8]) -> Result<Registration, RecordError> {
    let mut reader = Reader::new(bytes, REGISTRATION_VALUE_PART);
    reader.version()?;
    let member = reader.string()?;
    let registered_at = reader.i64()?;
    reader.finish()?;
    Ok(Registration {
        member: MemberId::new(member)?,
        registered_at,
    })
}

fn decode_lease(bytes: &[u8]) -> Result<Lease, RecordError> {
    let mut reader = Reader::new(bytes, LEASE_VALUE_PART);
    reader.version()?;
    let member = reader.string()?;
    let producer_id = reader.i64()?;
    let producer_epoch = reader.i16()?;
    let granted_at = reader.i64()?;
    let deadline = reader.i64()?;
    reader.finish()?;
    Ok(Lease {
        member: MemberId::new(member)?,
        token: FencingToken::new(producer_id, producer_epoch)?,
        granted_at,
        deadline,
    })
}

/// Writes one string as an `i16` byte length and then UTF-8 bytes.
fn put_string(buffer: &mut BytesMut, value: &str) {
    let length = i16::try_from(value.len())
        .expect("Role::new and MemberId::new reject a name longer than 249 bytes");
    buffer.put_i16(length);
    buffer.put_slice(value.as_bytes());
}

/// Reads the big-endian layout of the coordination state topic.
///
/// Every integer is signed, and a string is an `i16` byte length and then UTF-8
/// bytes. The reader never sizes an allocation from a length it read, and it
/// never panics. It takes a subslice of the input and returns a
/// [`RecordError`] for every malformed input.
struct Reader<'a> {
    data: &'a [u8],
    part: &'static str,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8], part: &'static str) -> Self {
        Self { data, part }
    }

    fn malformed(&self, message: String) -> RecordError {
        RecordError::Format {
            part: self.part,
            message,
        }
    }

    fn truncated(&self) -> RecordError {
        self.malformed(format!("truncated coordination state {}", self.part))
    }

    /// Fails when bytes remain after the last field.
    fn finish(&self) -> Result<(), RecordError> {
        if self.data.is_empty() {
            return Ok(());
        }
        Err(self.malformed(format!(
            "trailing bytes in coordination state {}",
            self.part
        )))
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], RecordError> {
        let Some((head, rest)) = self.data.split_at_checked(count) else {
            return Err(self.truncated());
        };
        self.data = rest;
        Ok(head)
    }

    fn i16(&mut self) -> Result<i16, RecordError> {
        let bytes = self.take(2)?;
        Ok(i16::from_be_bytes(bytes.try_into().expect("two bytes")))
    }

    fn i64(&mut self) -> Result<i64, RecordError> {
        let bytes = self.take(8)?;
        Ok(i64::from_be_bytes(bytes.try_into().expect("eight bytes")))
    }

    /// Reads one string. A negative length is malformed, and the `i16` length
    /// bounds the read at 32767 bytes, so no wire value sizes an allocation.
    fn string(&mut self) -> Result<&'a str, RecordError> {
        let length = self.i16()?;
        let length = usize::try_from(length).map_err(|_error| {
            self.malformed(format!(
                "negative string length {length} in coordination state {}",
                self.part
            ))
        })?;
        let bytes = self.take(length)?;
        std::str::from_utf8(bytes).map_err(|_error| {
            self.malformed(format!(
                "non-UTF-8 string in coordination state {}",
                self.part
            ))
        })
    }

    /// Reads the version and rejects every version but [`RECORD_VERSION`].
    fn version(&mut self) -> Result<(), RecordError> {
        let version = self.i16()?;
        if version == RECORD_VERSION {
            return Ok(());
        }
        Err(self.malformed(format!(
            "unsupported coordination state {} version {version}",
            self.part
        )))
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use proptest::prelude::*;

    use super::*;

    /// The pattern of a valid role name and a valid member id in the property
    /// tests.
    const NAME: &str = "[a-zA-Z0-9._-]{1,64}";

    fn role(name: &str) -> Role {
        Role::new(name).unwrap()
    }

    fn member(name: &str) -> MemberId {
        MemberId::new(name).unwrap()
    }

    /// A name renders as itself. The codec writes the name from `as_str`, so
    /// a `Display` that dropped the text would leave a log line and an error
    /// message naming nothing while the wire stayed correct.
    #[test]
    fn a_role_and_a_member_render_as_their_own_names() {
        check!(role("controller").to_string() == "controller");
        check!(member("node-1").to_string() == "node-1");
        check!(
            format!("{} holds {}", member("node-1"), role("controller"))
                == "node-1 holds controller"
        );
    }

    /// The frozen golden bytes of every record part.
    ///
    /// `krabka-streams-java` and `krabka-streams-go` assert the same arrays.
    /// Copy an array into a port without a change. A change here is a change of
    /// the wire format, and it needs the same change in the two ports.
    mod golden {
        use super::*;

        const ROLE: &str = "controller";
        const MEMBER: &str = "node-1";
        const REGISTERED_AT: i64 = 1_700_000_000_000;
        const GRANTED_AT: i64 = 1_700_000_000_000;
        const DEADLINE: i64 = 1_700_000_030_000;
        const PRODUCER_ID: i64 = 4242;
        const PRODUCER_EPOCH: i16 = 7;

        /// The key of the registration of member `node-1` for role
        /// `controller`.
        pub(super) const REGISTRATION_KEY: [u8; 24] = [
            0x00, 0x00, // version i16 = 0
            0x00, 0x00, // kind i16 = 0 (registration)
            0x00, 0x0A, // role length i16 = 10
            0x63, 0x6F, 0x6E, 0x74, 0x72, 0x6F, 0x6C, 0x6C, 0x65, 0x72, // "controller"
            0x00, 0x06, // member length i16 = 6
            0x6E, 0x6F, 0x64, 0x65, 0x2D, 0x31, // "node-1"
        ];

        /// The key of the lease of role `controller`. It carries no member.
        pub(super) const LEASE_KEY: [u8; 18] = [
            0x00, 0x00, // version i16 = 0
            0x00, 0x01, // kind i16 = 1 (lease)
            0x00, 0x0A, // role length i16 = 10
            0x63, 0x6F, 0x6E, 0x74, 0x72, 0x6F, 0x6C, 0x6C, 0x65, 0x72, // "controller"
            0x00, 0x00, // member length i16 = 0, the empty string
        ];

        /// The registration value of member `node-1` at 1700000000000.
        pub(super) const REGISTRATION_VALUE: [u8; 18] = [
            0x00, 0x00, // version i16 = 0
            0x00, 0x06, // member length i16 = 6
            0x6E, 0x6F, 0x64, 0x65, 0x2D, 0x31, // "node-1"
            0x00, 0x00, 0x01, 0x8B, 0xCF, 0xE5, 0x68, 0x00, // registered_at = 1700000000000
        ];

        /// The lease value of member `node-1`, token `4242:7`, granted at
        /// 1700000000000 and expiring at 1700000030000.
        pub(super) const LEASE_VALUE: [u8; 36] = [
            0x00, 0x00, // version i16 = 0
            0x00, 0x06, // member length i16 = 6
            0x6E, 0x6F, 0x64, 0x65, 0x2D, 0x31, // "node-1"
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x92, // producer_id i64 = 4242
            0x00, 0x07, // producer_epoch i16 = 7
            0x00, 0x00, 0x01, 0x8B, 0xCF, 0xE5, 0x68, 0x00, // granted_at i64 = 1700000000000
            0x00, 0x00, 0x01, 0x8B, 0xCF, 0xE5, 0xDD, 0x30, // deadline i64 = 1700000030000
        ];

        fn registration_key() -> CoordinationKey {
            CoordinationKey::registration(role(ROLE), member(MEMBER))
        }

        fn lease_key() -> CoordinationKey {
            CoordinationKey::lease(role(ROLE))
        }

        fn registration_value() -> CoordinationRecord {
            CoordinationRecord::Registration(Registration {
                member: member(MEMBER),
                registered_at: REGISTERED_AT,
            })
        }

        fn lease_value() -> CoordinationRecord {
            CoordinationRecord::Lease(Lease {
                member: member(MEMBER),
                token: FencingToken::new(PRODUCER_ID, PRODUCER_EPOCH).unwrap(),
                granted_at: GRANTED_AT,
                deadline: DEADLINE,
            })
        }

        #[test]
        fn a_registration_key_encodes_to_the_frozen_bytes() {
            check!(encode_key(&registration_key()).as_ref() == REGISTRATION_KEY.as_slice());
            check!(decode_key(&REGISTRATION_KEY).unwrap() == registration_key());
        }

        #[test]
        fn a_lease_key_encodes_to_the_frozen_bytes() {
            check!(encode_key(&lease_key()).as_ref() == LEASE_KEY.as_slice());
            check!(decode_key(&LEASE_KEY).unwrap() == lease_key());
        }

        #[test]
        fn a_registration_value_encodes_to_the_frozen_bytes() {
            let encoded = encode_value(&registration_value()).unwrap();
            check!(encoded.as_ref() == REGISTRATION_VALUE.as_slice());
            let decoded =
                decode_value(RecordKind::Registration, Some(&REGISTRATION_VALUE)).unwrap();
            check!(decoded == registration_value());
        }

        #[test]
        fn a_lease_value_encodes_to_the_frozen_bytes() {
            let encoded = encode_value(&lease_value()).unwrap();
            check!(encoded.as_ref() == LEASE_VALUE.as_slice());
            let decoded = decode_value(RecordKind::Lease, Some(&LEASE_VALUE)).unwrap();
            check!(decoded == lease_value());
        }
    }

    #[test]
    fn the_topic_name_is_frozen() {
        check!(COORDINATION_STATE_TOPIC == "__coordination_state");
    }

    #[test]
    fn a_record_kind_maps_onto_its_wire_code() {
        for kind in [RecordKind::Registration, RecordKind::Lease] {
            check!(RecordKind::from_code(kind.code()).unwrap() == kind);
        }
        check!(RecordKind::Registration.code() == 0);
        check!(RecordKind::Lease.code() == 1);
        for code in [-1_i16, 2, 9, i16::MAX, i16::MIN] {
            check!(RecordKind::from_code(code).is_err(), "kind {code}");
        }
    }

    #[test]
    fn a_tombstone_encodes_to_a_null_value() {
        check!(encode_value(&CoordinationRecord::Tombstone).is_none());
        for kind in [RecordKind::Registration, RecordKind::Lease] {
            check!(decode_value(kind, None).unwrap() == CoordinationRecord::Tombstone);
        }
    }

    #[test]
    fn a_lease_key_ignores_a_member_the_caller_set() {
        let key = CoordinationKey {
            kind: RecordKind::Lease,
            role: role("controller"),
            member: Some(member("node-1")),
        };
        check!(
            decode_key(&encode_key(&key)).unwrap() == CoordinationKey::lease(role("controller"))
        );
    }

    #[test]
    fn a_role_rejects_an_empty_name_and_a_long_name() {
        assert!(let Err(RecordError::Empty { field }) = Role::new(""));
        check!(field == "role");
        let long = "r".repeat(MAX_ROLE_LEN + 1);
        assert!(let Err(RecordError::TooLong { length, bound, .. }) = Role::new(&long));
        check!(length == MAX_ROLE_LEN + 1);
        check!(bound == MAX_ROLE_LEN);
        check!(Role::new(&"r".repeat(MAX_ROLE_LEN)).is_ok());
    }

    #[test]
    fn a_member_id_rejects_an_empty_name_and_a_long_name() {
        assert!(let Err(RecordError::Empty { field }) = MemberId::new(""));
        check!(field == "member id");
        let long = "m".repeat(MAX_MEMBER_ID_LEN + 1);
        check!(MemberId::new(&long).is_err());
        check!(MemberId::new(&"m".repeat(MAX_MEMBER_ID_LEN)).is_ok());
    }

    #[test]
    fn a_fencing_token_orders_on_the_producer_id_first() {
        let fresh = FencingToken::new(5, 0).unwrap();
        let wrapped = FencingToken::new(4, i16::MAX).unwrap();
        check!(fresh > wrapped);
        check!(FencingToken::new(4, 1).unwrap() > FencingToken::new(4, 0).unwrap());
        check!(FencingToken::new(4, 0).unwrap() == FencingToken::new(4, 0).unwrap());
    }

    #[test]
    fn a_fencing_token_rejects_a_negative_component() {
        check!(FencingToken::new(-1, 0).is_err());
        check!(FencingToken::new(0, -1).is_err());
        check!(FencingToken::new(0, 0).is_ok());
    }

    #[test]
    fn a_fencing_token_round_trips_through_its_string_form() {
        let token = FencingToken::new(4242, 7).unwrap();
        check!(token.to_string() == "4242:7");
        check!("4242:7".parse::<FencingToken>().unwrap() == token);
        for text in ["", "4242", "4242:7:1", "a:7", "4242:b", "-1:7", "4242:-1"] {
            check!(text.parse::<FencingToken>().is_err(), "token {text:?}");
        }
    }

    /// Malformed keys. Each returns an error, and none panics.
    #[test]
    fn a_malformed_key_returns_an_error() {
        let mut trailing = golden::REGISTRATION_KEY.to_vec();
        trailing.push(0x00);
        let cases: [(&str, Vec<u8>); 10] = [
            ("empty buffer", vec![]),
            ("truncated version", vec![0x00]),
            ("truncated kind", vec![0x00, 0x00, 0x00]),
            (
                "unknown version",
                vec![0x00, 0x01, 0x00, 0x00, 0x00, 0x01, b'a', 0x00, 0x01, b'm'],
            ),
            (
                "unknown kind",
                vec![0x00, 0x00, 0x00, 0x09, 0x00, 0x01, b'a', 0x00, 0x00],
            ),
            (
                "negative role length",
                vec![0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x00, 0x00],
            ),
            (
                "role length past the buffer",
                vec![0x00, 0x00, 0x00, 0x00, 0x7F, 0xFF, b'a'],
            ),
            (
                "non-UTF-8 role",
                vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFF, 0x00, 0x01, b'm'],
            ),
            (
                "registration key with an empty member",
                vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x01, b'a', 0x00, 0x00],
            ),
            ("trailing bytes", trailing),
        ];
        for (name, bytes) in cases {
            check!(decode_key(&bytes).is_err(), "key case {name}");
        }
    }

    #[test]
    fn a_lease_key_that_names_a_member_returns_an_error() {
        let bytes = vec![
            0x00, 0x00, // version
            0x00, 0x01, // kind = lease
            0x00, 0x01, b'a', // role "a"
            0x00, 0x01, b'm', // member "m", which a lease key must not carry
        ];
        assert!(let Err(RecordError::Format { part, .. }) = decode_key(&bytes));
        check!(part == "key");
    }

    /// Malformed values of both kinds. Each returns an error, and none panics.
    #[test]
    fn a_malformed_value_returns_an_error() {
        let mut trailing_registration = golden::REGISTRATION_VALUE.to_vec();
        trailing_registration.push(0x00);
        let mut trailing_lease = golden::LEASE_VALUE.to_vec();
        trailing_lease.push(0x00);
        let cases: [(&str, RecordKind, Vec<u8>); 9] = [
            ("empty registration", RecordKind::Registration, vec![]),
            (
                "unknown registration version",
                RecordKind::Registration,
                vec![0x00, 0x01, 0x00, 0x01, b'm', 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            (
                "negative registration member length",
                RecordKind::Registration,
                vec![0x00, 0x00, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            (
                "truncated registration timestamp",
                RecordKind::Registration,
                vec![0x00, 0x00, 0x00, 0x01, b'm', 0x00],
            ),
            (
                "trailing bytes after a registration",
                RecordKind::Registration,
                trailing_registration,
            ),
            ("empty lease", RecordKind::Lease, vec![]),
            (
                "truncated lease deadline",
                RecordKind::Lease,
                vec![
                    0x00, 0x00, 0x00, 0x01, b'm', 0, 0, 0, 0, 0, 0, 0, 1, 0x00, 0x00, 0, 0, 0, 0,
                    0, 0, 0, 1,
                ],
            ),
            (
                "negative lease producer id",
                RecordKind::Lease,
                vec![
                    0x00, 0x00, 0x00, 0x01, b'm', 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                    0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2,
                ],
            ),
            (
                "trailing bytes after a lease",
                RecordKind::Lease,
                trailing_lease,
            ),
        ];
        for (name, kind, bytes) in cases {
            check!(
                decode_value(kind, Some(&bytes)).is_err(),
                "value case {name}"
            );
        }
    }

    proptest! {
        #[test]
        fn a_registration_key_survives_a_round_trip(name in NAME, id in NAME) {
            let key = CoordinationKey::registration(role(&name), member(&id));
            let decoded = decode_key(&encode_key(&key)).unwrap();
            check!(decoded == key);
        }

        #[test]
        fn a_lease_key_survives_a_round_trip(name in NAME) {
            let key = CoordinationKey::lease(role(&name));
            let decoded = decode_key(&encode_key(&key)).unwrap();
            check!(decoded == key);
        }

        #[test]
        fn a_registration_value_survives_a_round_trip(
            id in NAME,
            registered_at in any::<i64>(),
        ) {
            let record = CoordinationRecord::Registration(Registration {
                member: member(&id),
                registered_at,
            });
            let encoded = encode_value(&record).unwrap();
            let decoded = decode_value(RecordKind::Registration, Some(&encoded)).unwrap();
            check!(decoded == record);
        }

        #[test]
        fn a_lease_value_survives_a_round_trip(
            id in NAME,
            producer_id in 0_i64..=i64::MAX,
            producer_epoch in 0_i16..=i16::MAX,
            granted_at in any::<i64>(),
            deadline in any::<i64>(),
        ) {
            let record = CoordinationRecord::Lease(Lease {
                member: member(&id),
                token: FencingToken::new(producer_id, producer_epoch).unwrap(),
                granted_at,
                deadline,
            });
            let encoded = encode_value(&record).unwrap();
            let decoded = decode_value(RecordKind::Lease, Some(&encoded)).unwrap();
            check!(decoded == record);
        }

        /// The decoders read bytes off the network, so no input may panic.
        #[test]
        fn a_decoder_never_panics_on_arbitrary_bytes(
            bytes in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let _ = decode_key(&bytes);
            let _ = decode_value(RecordKind::Registration, Some(&bytes));
            let _ = decode_value(RecordKind::Lease, Some(&bytes));
        }
    }
}
