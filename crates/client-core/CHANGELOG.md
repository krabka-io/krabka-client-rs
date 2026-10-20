# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0] — 2026-10-20

**Breaking Rust API.** Uses the krabka-protocol 0.7 crate types in its public
API, including the OAuth and SCRAM fixes in krabka-security 0.7.

- The pool, the connection, the fetch helpers and the `Client` calls record
  an error they return at the level of their own tracing span, debug or info,
  rather than at ERROR. Every such error reaches the caller, which may retry it, so a
  run that recovered no longer prints `ERROR ... error=connection closed`
  (krabka-io/krabka-broker#1515). The `mock` feature's
  `mock_test_support::recorded_levels` captures the level of every event a
  call records, for tests.
- `Client::refresh_metadata` tries the broker with the least-loaded open
  connection first, as Kafka's `NetworkClient.leastLoadedNode` does, rather
  than the lowest broker id, so a metadata request does not queue behind a
  parked long-poll `Fetch`. `MetadataTopics` gains `request_update` and
  `update_requested` (Kafka's `Metadata.requestUpdate`): a change of the topic
  set requests an update, and a successful refresh clears it.
- Add `ConnectionOptions::validate` and validate transport policy before socket I/O,
  stream negotiation, or client bootstrap. Invalid or unrepresentable durations,
  inverted setup/reconnect bounds, and invalid socket buffers return configuration errors.
- Make `connections_max_idle = 0` disable idle eviction. Positive idle budgets retain
  their existing behavior; socket buffers set to `None` retain OS defaults.

## [0.6.0] — 2026-10-08

- First release on crates.io from krabka-io/krabka-client-rs, under the
  `krabka-*` name. Earlier versions were published as `crabka-*`; those names
  are being retired.
- Moves to the krabka-protocol 0.6 crates. The client crates expose their
  types in the public API, so this is a breaking release.

## [0.4.0] — 2026-08-12


### <!-- 0 -->🚀 Features


- Add Gres PostgreSQL runtime ([#795](https://github.com/robot-head/crabka/pull/795))

- Expose runtime configuration policy ([#904](https://github.com/robot-head/crabka/pull/904)) (**breaking**)


### <!-- 1 -->🐛 Bug Fixes


- Start removing clippy suppressions ([#784](https://github.com/robot-head/crabka/pull/784))


### <!-- 3 -->📚 Documentation


- Rewrite all prose to ASD-STE100 Simplified Technical English ([#982](https://github.com/robot-head/crabka/pull/982))

## [0.3.9] — 2026-07-07


### <!-- 0 -->🚀 Features


- Cross-service demo traces + codebase-wide instrumentation ([#706](https://github.com/robot-head/crabka/pull/706))


### <!-- 2 -->🚜 Refactor


- Simplify shared helper logic ([#749](https://github.com/robot-head/crabka/pull/749))


### <!-- 6 -->🧪 Testing


- Harden broker mutant coverage ([#713](https://github.com/robot-head/crabka/pull/713))

- Adopt rs-clock and make sleep-based time/progress tests deterministic ([#725](https://github.com/robot-head/crabka/pull/725))

## [0.3.8] — 2026-06-23


### <!-- 1 -->🐛 Bug Fixes


- Seamless KRaft failover — voter hostname re-resolution, submit_change forwarding, SIGTERM controlled shutdown ([#591](https://github.com/robot-head/crabka/pull/591))


### <!-- 2 -->🚜 Refactor


- Migrate whole-function cargo-mutants exclusions to #[mutants::skip] ([#615](https://github.com/robot-head/crabka/pull/615))


### <!-- 6 -->🧪 Testing


- Kill engine-IO mutants with mockall seams, shrink mutants.toml ([#607](https://github.com/robot-head/crabka/pull/607))

## [0.3.7] — 2026-06-17


### <!-- 0 -->🚀 Features


- Scaffold rules_rust client build + lean client_minimal facade ([#570](https://github.com/robot-head/crabka/pull/570))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- De-hardcode release versions + sign/attest published Helm charts ([#530](https://github.com/robot-head/crabka/pull/530))

## [0.3.6] — 2026-06-13

## [0.3.5] — 2026-06-12


### <!-- 0 -->🚀 Features


- Versioned KTables slice 1 (KIP-889/914) ([#481](https://github.com/robot-head/crabka/pull/481))

## [0.1.1] — 2026-05-29


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#49](https://github.com/robot-head/crabka/pull/49))

- Release v0.1.0 ([#52](https://github.com/robot-head/crabka/pull/52))

## [0.1.0] — 2026-05-12


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#49](https://github.com/robot-head/crabka/pull/49))

