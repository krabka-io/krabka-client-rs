"""Linter aspects.

Clippy runs over the same `rust_library` / `rust_test` targets the build already
has, so it sees exactly the crates, features and dependencies the build sees --
rather than a second `cargo clippy` resolution that can disagree with it.

The aspect comes from the `@rules_rust` that rules_rs vends, not from
aspect_rules_lint_rust: that module registers a Rust toolchain of its own, pinned
to 1.92.0 / edition 2021, and rules_rs refuses two configurations under one repo
name. One toolchain, one resolution.

    bazel build --config=lint //...

`clippy.toml` at the repository root is picked up by the aspect through
`--@rules_rust//:clippy.toml`, set in //.bazelrc.
"""

load("@rules_rust//rust:defs.bzl", "rust_clippy_aspect")

clippy = rust_clippy_aspect
