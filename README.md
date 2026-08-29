# krabka-client-rs

The Rust client for Apache Kafka and [krabka](https://github.com/krabka-io):
producer, consumer, admin, and the connection layer they share.

It speaks the Kafka wire protocol through
[`krabka-protocol`](https://github.com/krabka-io/krabka-protocol) and depends on
nothing else in the krabka stack — in particular, not on the broker.

## Crates

| Crate | What it is |
| --- | --- |
| `krabka-client-core` | Connection management, request dispatch, retries, and the SASL/TLS handshake. |
| `krabka-client-producer` | Producer, including the idempotent and transactional paths. |
| `krabka-client-consumer` | Consumer, consumer groups, and share groups. |
| `krabka-client-admin` | Admin API: topics, configs, ACLs, groups, and offsets. |

## Build

Bazel is the build and test path. Cargo stays the dependency source of truth:
[`rules_rs`](https://github.com/hermeticbuild/rules_rs) reads the same
`Cargo.toml` and `Cargo.lock` that Cargo does, so there is no second dependency
set to keep in sync.

```
bazel test //...
```

`cargo` works the same way it always did:

```
cargo nextest run --workspace
```

Both run the same 449 tests and the same 4 rustdoc examples; nothing here is
tagged `manual`.

## Depending on the wire layer

`krabka-protocol` is pinned by revision in exactly one place, the
`[patch.crates-io]` block at the bottom of the root `Cargo.toml`. Member
manifests declare those crates as ordinary `krabka-x = "0.4.0"` requirements and
the patch redirects them at the git checkout, so a manifest still reads as a
normal Cargo manifest. To move to a newer wire layer, change the revision there
and re-run `cargo generate-lockfile`.

### Everything CI does, locally

The [Aspect CLI](https://github.com/aspect-build/aspect-cli) narrows each task to
what a change actually touched. Every one has a plain-Bazel equivalent, so the
CLI is a convenience rather than a requirement:

| | Aspect CLI | Plain Bazel |
| --- | --- | --- |
| Build | `aspect build //...` | `bazel build //...` |
| Test | `aspect test //...` | `bazel test //...` |
| Lint | `aspect lint` | `bazel build --config=lint //...` |
| Format | `aspect format` | `bazel run //tools/format` |
| Coverage | `aspect test --coverage` | `bazel coverage //crates/...` |
| Docs | — | `bazel build //crates/client-core:client-core_doc` |

Formatting and linting are Bazel targets rather than a separate `cargo fmt` /
`cargo clippy` pass, so they see exactly the files and crates the build sees. A
file in no target cannot drift unnoticed, and clippy resolves the same features
the build resolves.

Two details worth knowing:

* **rustfmt runs on a pinned nightly.** `rustfmt.toml` uses
  `format_code_in_doc_comments`, `group_imports` and `imports_granularity`, all
  still nightly-gated; stable rustfmt warns and silently skips them. The nightly
  is pinned in `MODULE.bazel`, so formatting is reproducible rather than a
  function of whichever nightly is installed.
* **`rustfmt.toml` states its edition.** `cargo fmt` passes `--edition` from
  `Cargo.toml`; rustfmt invoked directly defaults to 2015 and sorts `use` lists
  differently. Stating it makes formatting a property of the repository rather
  than of how rustfmt was launched.

## Mutation testing

Mutation sweeps run through
[`rules_rs_mutants`](https://github.com/robot-head/rules_rs_mutants). Each crate
has a `<crate>_mutants` target:

```
bazel test //crates/client-producer:client-producer_mutants
```

They are tagged `manual`, so `bazel test //...` skips them and a nightly job
runs the full sweep. Only `#[cfg(test)]` unit tests take part, so mutants that
the `tests/*.rs` suites would kill are reported as survivors and scores read
lower than the monorepo's `cargo mutants` numbers for the same code.

## Broker-backed tests

Five suites that booted an in-process broker to exercise the admin and consumer
clients moved to
[`krabka-broker`](https://github.com/krabka-io/krabka-broker), where the broker
lives. Keeping them here would have made this repository depend on the thing it
sits below.

## Publishing

These crates are published to crates.io from
[`robot-head/crabka`](https://github.com/robot-head/crabka), which is still the
release home for the `krabka-*` names. This repository has no release
automation; consumers pin it by git revision.
