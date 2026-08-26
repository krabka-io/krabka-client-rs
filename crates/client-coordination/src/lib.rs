//! Leader election, leases, and fencing tokens for Apache Kafka clients.
//!
//! This crate gives an active or standby process a way to elect one leader per
//! role and to prove that leadership on every guarded write. It builds the
//! primitives on Kafka itself, so a cluster needs no second consensus service.
//! Per-role state lives in the compacted internal topic
//! [`COORDINATION_STATE_TOPIC`].
//!
//! The leadership epoch is the producer epoch. Kafka's transaction coordinator
//! mints that epoch when a member calls `InitProducerId` for the
//! `transactional.id` of the role. The quorum picks the value, so the epoch is
//! monotonic, and every broker enforces it. A write that carries an older epoch
//! fails with a fenced error. The lease adds no safety of its own. It is an
//! anti-flap device: a standby waits for the deadline of the holder to pass
//! before it challenges, so a short pause does not move the role.
//!
//! # Key Types
//!
//! - [`Role`] and [`MemberId`] — the validated names of a role and of one
//!   member that competes for it.
//! - [`FencingToken`] — the quorum-minted proof that a member holds a role.
//! - [`CoordinationKey`] and [`CoordinationRecord`] — the decoded key and value
//!   of one record of [`COORDINATION_STATE_TOPIC`].
//! - [`encode_key`], [`decode_key`], [`encode_value`], and [`decode_value`] —
//!   the codec for the frozen record layouts.
//!
//! ## Quick start
//!
//! The record codec is the part of the crate that exists now. It encodes and
//! decodes the frozen layouts that `krabka-streams-java` and
//! `krabka-streams-go` also implement.
//!
//! ```
//! use crabka_client_coordination::{
//!     CoordinationKey, CoordinationRecord, FencingToken, Lease, MemberId, Role, decode_key,
//!     decode_value, encode_key, encode_value,
//! };
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let key = CoordinationKey::lease(Role::new("controller")?);
//! let record = CoordinationRecord::Lease(Lease {
//!     member: MemberId::new("node-1")?,
//!     token: FencingToken::new(4242, 7)?,
//!     granted_at: 1_700_000_000_000,
//!     deadline: 1_700_000_030_000,
//! });
//!
//! // Produce these two buffers to `__coordination_state`. A `None` value is a
//! // tombstone, and it clears the lease.
//! let key_bytes = encode_key(&key);
//! let value_bytes = encode_value(&record);
//!
//! // Read them back on the consumer side.
//! let decoded_key = decode_key(&key_bytes)?;
//! let decoded = decode_value(decoded_key.kind, value_bytes.as_deref())?;
//! let CoordinationRecord::Lease(lease) = decoded else {
//!     unreachable!("the record is a lease")
//! };
//! println!(
//!     "{} holds {} with token {} until {}",
//!     lease.member, decoded_key.role, lease.token, lease.deadline
//! );
//! # Ok(())
//! # }
//! ```
//!
//! ## Scope and boundaries
//!
//! This crate owns the coordination record format and the client API built on
//! it. It does not change the broker, and it needs no broker-side feature. A
//! caller supplies its own producer and consumer, and this crate supplies the
//! succession rules, the lease clock, and the codec below.
#![doc(html_root_url = "https://docs.rs/crabka-client-coordination/0.4.0")]

mod record;

pub use record::{
    COORDINATION_STATE_TOPIC, CoordinationKey, CoordinationRecord, FencingToken, Lease,
    MAX_MEMBER_ID_LEN, MAX_ROLE_LEN, MemberId, RecordError, RecordKind, Registration, Role,
    decode_key, decode_value, encode_key, encode_value,
};
