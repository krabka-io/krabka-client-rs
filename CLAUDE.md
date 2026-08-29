# krabka-client-rs — project-specific guidance

## Compatibility

**krabka is greenfield and undeployed.** There are no production users, no
persisted state to migrate, and no clients pinned to a specific build. Do not
write backwards-compatibility shims:

- No `#[serde(default)]` on metadata fields "to keep old raft logs readable"
- No `V2` enum variants that stay alongside `V1` to support replay
- No feature flags that gate new behavior behind a default-off switch
- No migration code or one-shot upgraders for on-disk format changes
- No deprecated-but-kept API surfaces

When a schema, enum, wire format, or interface changes, change it. Delete local
raft logs and data directories during development if necessary.

**Kafka compatibility is the constraint that matters.** Always keep:

- Apache Kafka wire-protocol byte exactness for request and response shapes,
  field order, error codes, and version negotiation
- KIP semantics for the feature that you implement
- Behavior that the JVM admin tools rely on, such as `kafka-topics`,
  `kafka-acls`, `kafka-leader-election`, and `kafka-reassign-partitions`

When in doubt, match Kafka. If Kafka's behavior is undocumented or
version-dependent, check the behavior of the latest released cp-kafka image. Do
not rely on the wiki.

## Build

Bazel is the build and test path; Cargo is the dependency source of truth.
`rules_rs` reads the same `Cargo.toml` / `Cargo.lock` Cargo does.

```
bazel test //...          # everything CI gates on
cargo nextest run --workspace
```

Per-crate BUILD files stay small on purpose: `//bazel:defs.bzl` reads crate
name, edition, feature set and dependency labels out of the `@crates` repo that
`crate.from_cargo` generates, so a manifest change does not need a matching
BUILD edit. Add a new workspace member by writing its `Cargo.toml` and a
four-line `BUILD.bazel` that calls `crate_library` and `crate_tests`.

Suites that cannot run hermetically are tagged `manual` at their `crate_tests`
call, with a comment saying why. Add to that list rather than deleting a test.

The wire layer lives in a sibling repository,
[`krabka-protocol`](https://github.com/krabka-io/krabka-protocol). It is pinned
by revision in one place -- the `[patch.crates-io]` block at the bottom of the
root `Cargo.toml`. Member manifests declare those crates as ordinary
`krabka-x = "0.4.0"` requirements; the patch is what redirects them at the git
checkout. To move to a newer wire layer, change the revision there, re-run
`cargo generate-lockfile`, and commit both files.

`MODULE.bazel` additionally names each sibling crate's directory. rules_rs finds
a git crate's path by matching the crate name against the workspace `members`
list, and krabka-protocol's members is the glob `crates/*`, which it skips.

## Code & Documentation Style

Follow the style guides in [`docs/style_guides/`](docs/style_guides/README.md):
[code](docs/style_guides/code_style_guide.md),
[rustdoc](docs/style_guides/rustdoc_style_guide.md),
[README](docs/style_guides/readme_style_guide.md),
[design docs](docs/style_guides/design_doc_style_guide.md), and
[coverage reports](docs/style_guides/coverage_report_style_guide.md). Examples
are the pinned stable toolchain, `cargo +nightly fmt`, forbidden `unsafe`, and
`clippy::pedantic`.

Do not make style-only sweeps across untouched files. Bring a file into line
with the guides only when you already edit it. Keep the tidy-up proportionate to
the change.

### Assertions and Clippy

- Never add `#[allow(clippy::...)]` or any equivalent Clippy suppression. Fix
  every Clippy warning in the code, regardless of the effort required.
- Never use Rust's plain `assert!`, `assert_eq!`, or `assert_ne!` macros. Use
  the `assert2` crate's `assert!` macro instead. Use it also for equality and
  inequality comparisons.

Clippy is a Cargo-side gate. `bazel build` applies `-Funsafe_code` (the one
`[workspace.lints]` entry whose guarantee must not lapse under a second build
system) but does not run Clippy, so run `cargo clippy --workspace --all-targets
-- -D warnings` before you push.

## Execution

When you execute an implementation plan, always use **subagent-driven
development in parallel batches** where the per-task file sets do not overlap.
Dispatch all tasks in a batch concurrently, in one message with multiple Agent
calls. Then wait for the batch to complete, review it, and move to the next.

A "conflict" between parallel implementers occurs only when both edit the same
file. When in doubt, list the file set that each task touches before you decide.

**Never discard working-tree state while parallel implementers run.**
`git checkout -- <path>`, `git restore`, `git stash`, and `git clean` all
destroy *every* uncommitted change in the files they touch, not only yours. To
undo your own edit, reverse it directly.

Tests must exercise behavior, not source text. Do not read source files in tests
and assert against their contents. `include_str!` and `fs::read_to_string` are
examples of such reads. If a behavior is hard to test, add a narrow helper or
seam. Then test that behavior directly.

When you check generated protocol records or other structured values in tests,
compare the whole expected struct. This is better than long chains of
field-by-field assertions. Use table-driven or parameterized tests for repeated
scenarios that differ only by inputs, protocol version, or expected request
shape.

## Releases

This repository has no release automation. The `krabka-*` crates.io names are
still published from [`robot-head/crabka`](https://github.com/robot-head/crabka);
consumers here pin by git revision.
