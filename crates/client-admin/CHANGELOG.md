# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- Add the `gssapi` feature, off by default, which forwards
  `krabka-client-core/gssapi` for SASL/GSSAPI
  ([#196](https://github.com/krabka-io/krabka-client-rs/issues/196)).

## [0.7.1] — 2026-10-20

- Require Rustls 0.23.46 and update the locked certificate verifier to 0.103.16.
- Release with krabka-client-producer 0.7.1 and its producer recovery fixes.

## [0.7.0] — 2026-10-20

**Breaking Rust API.** Uses the krabka-protocol 0.7 crate types in its public
API, including the OAuth and SCRAM fixes in krabka-security 0.7.

- Expose DNS selection, reconnect backoff/max, socket buffers, dispatch capacity,
  and frame limit through `AdminClientConfig`, alongside existing API retry settings.
  Resolved options reach administrative connections without losing retry or recovery policy.
- Validate transport budgets and accept zero idle lifetime to disable idle eviction.
  The default administrative idle lifetime remains five minutes.

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


- Add Dioxus broker admin UI ([#741](https://github.com/robot-head/crabka/pull/741))


### <!-- 2 -->🚜 Refactor


- Simplify shared helper logic ([#749](https://github.com/robot-head/crabka/pull/749))


### <!-- 6 -->🧪 Testing


- Harden broker mutant coverage ([#713](https://github.com/robot-head/crabka/pull/713))

## [0.3.8] — 2026-06-23


### <!-- 0 -->🚀 Features


- Geo-replication engine (Slice 1) — MirrorMaker-2 equivalent + data residency ([#593](https://github.com/robot-head/crabka/pull/593))

## [0.3.7] — 2026-06-17


### <!-- 0 -->🚀 Features


- KIP-939 2PC participation — coordinator semantics + stateright model ([#560](https://github.com/robot-head/crabka/pull/560))

- Scaffold rules_rust client build + lean client_minimal facade ([#570](https://github.com/robot-head/crabka/pull/570))


### <!-- 1 -->🐛 Bug Fixes


- Resolve GKE ext4 lost+found format failure and add deployment docs ([#504](https://github.com/robot-head/crabka/pull/504))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- De-hardcode release versions + sign/attest published Helm charts ([#530](https://github.com/robot-head/crabka/pull/530))

## [0.3.6] — 2026-06-13

## [0.3.5] — 2026-06-12

## [0.1.1] — 2026-05-29


### <!-- 0 -->🚀 Features


- KafkaUser SCRAM-SHA-256 authentication (slice 36b) ([#269](https://github.com/robot-head/crabka/pull/269))

- KIP-853 dynamic KRaft quorum reconfiguration ([#290](https://github.com/robot-head/crabka/pull/290))

