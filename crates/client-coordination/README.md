# crabka-client-coordination

[![Crates.io](https://img.shields.io/crates/v/crabka-client-coordination.svg)](https://crates.io/crates/crabka-client-coordination)
[![Docs.rs](https://docs.rs/crabka-client-coordination/badge.svg)](https://docs.rs/crabka-client-coordination)

Leader election, leases, and fencing tokens for Apache Kafka clients.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

An active or standby process uses `crabka-client-coordination` to elect one
leader for each role and to prove that leadership on every guarded write. The
crate builds the primitives on Kafka itself, so a cluster needs no second
consensus service, and no broker-side feature.

The leadership epoch is the producer epoch. Kafka's transaction coordinator
mints that epoch when a member calls `InitProducerId` for the
`transactional.id` of the role. The quorum picks the value, so the epoch is
monotonic, and every broker enforces it. A write that carries an older epoch
fails with a fenced error. The lease adds no safety of its own. It is an
anti-flap device: a standby waits for the deadline of the holder to pass before
it challenges, so a short pause does not move the role.

## Capabilities

- Parse and hold a validated `Role` and `MemberId`.
- Compare a `FencingToken` in the order that survives a producer-epoch wrap.
- Encode and decode the records of the compacted internal topic
  `__coordination_state`.

## Record Format

The key and the value layouts of `__coordination_state` are frozen.
`krabka-streams-java` and `krabka-streams-go` re-implement them byte for byte,
and all three projects assert the same golden bytes. Every integer is
big-endian and signed. A string is an `i16` byte length and then plain UTF-8
bytes, which is Kafka's own native string layout. A record with a null value is
a tombstone. See the `record` module documentation for the field lists.

## Documentation

- [API documentation](https://docs.rs/crabka-client-coordination)
- [Design](docs/design.md) - why the epoch comes from the transaction
  coordinator, why the lease carries no safety, and the succession recipe.
- [Crate style guides](../../docs/style_guides/README.md)
