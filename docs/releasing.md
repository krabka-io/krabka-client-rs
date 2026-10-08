# Releasing

A release of this repository is an annotated tag `vX.Y.Z` on `main`. The push
of the tag starts [`publish.yml`](../.github/workflows/publish.yml), which
publishes the client crates to crates.io. No other workflow publishes them.

The workspace releases as one unit. Every published crate inherits
`[workspace.package] version`, so one tag names one version of all of them.

## The published crates

| Crate | Depends on |
| :--- | :--- |
| `krabka-client-core` | |
| `krabka-client-admin` | `krabka-client-core` |
| `krabka-client-consumer` | `krabka-client-core` |
| `krabka-client-producer` | `krabka-client-core`, `krabka-client-consumer` |

The table lists only the workspace crates. Each crate also depends on crates
of [krabka-protocol](https://github.com/krabka-io/krabka-protocol), such as
`krabka-protocol`, `krabka-ids`, `krabka-units`, `krabka-compression` and
`krabka-security`.

`krabka-client-coordination` sets `publish = false`. It is not a library that
other repositories build on. A new member crate is published unless its
manifest sets `publish = false`.

Each published crate takes `repository`, `license`, `authors`, `edition` and
`rust-version` from the workspace. Its `include` list ships the library source,
its README and its CHANGELOG, but not its tests, examples or fixtures. Cargo
prints an "ignoring test" or "ignoring example" warning for each target that
the package leaves out. The warnings are expected.

Every `krabka-*` normal dependency carries a `version` as well as its `path`.
`cargo publish` writes that version into the published manifest. A path or git
dependency that is not on crates.io may appear only as a dev-dependency, and
without a `version`, so that cargo drops it from the published manifest.

The krabka repositories publish in this order, because each one depends on the
ones before it:

1. krabka-protocol
2. krabka-client-rs
3. krabka-schema-registry
4. krabka-broker

## Prerequisite: the krabka-protocol crates on crates.io

`cargo publish` ignores `[patch.crates-io]`. The published crates, and the
`--dry-run` that verifies them, resolve every krabka-protocol crate from
crates.io at the `version` that the manifests name. So a release of this
repository needs a krabka-protocol release with the same version on crates.io
first.

crates.io must have `krabka-security`, which waits on `krabka-sspi`.
`krabka-security` uses `sspi` from a git fork, which a crates.io package cannot
name. The fork is to be published to crates.io as `krabka-sspi` 0.23.0, which
keeps the library name `sspi`. krabka-protocol then switches `krabka-security`
to `sspi = { package = "krabka-sspi", version = "0.23.0" }` and publishes it.
See the "The sspi dependency" section of krabka-protocol's
[`docs/releasing.md`](https://github.com/krabka-io/krabka-protocol/blob/main/docs/releasing.md).

`krabka-sspi` is temporary. The fork's fixes are being upstreamed, and
`krabka-sspi` will be deprecated and yanked once an upstream `sspi` release has
them.

Every crate of this repository reaches `krabka-security` through
`krabka-client-core`. Until crates.io has `krabka-security`, `cargo publish
--dry-run` fails for every crate here, and the `plan` job stops before any
upload. The `crates` input does not help, because no crate here is free of
`krabka-security`.

## 1. Prepare the version

Set the new version in these places:

- `[workspace.package] version` in the root `Cargo.toml`.
- The first-party `version` requirements in `[workspace.dependencies]`, and
  the same requirements in each member `Cargo.toml`. A path dependency also
  carries a version, and `cargo publish` uses that version in the published
  manifest.
- `version` in `MODULE.bazel`.
- Each `#![doc(html_root_url = "https://docs.rs/krabka-<name>/<version>")]`.

Then run `cargo update --workspace` to update `Cargo.lock`. Move the
`Unreleased` entries of each crate's `CHANGELOG.md` under the new version.

Before you merge, run a dry run from the branch: start `publish.yml` from the
Actions tab with `dry_run` on. It packages each crate and builds it from the
packaged sources, as crates.io users get them.

## 2. Tag the release

Tag the merge commit on `main`, then push the tag:

```sh
git tag -a v0.5.1 -m "krabka-client-rs 0.5.1"
git push origin v0.5.1
```

## 3. What the workflow does

The `plan` job holds no credential. It:

1. stops unless the tagged commit is an ancestor of `origin/main`.
2. stops unless a `push` run of `ci.yml` passed on that commit.
3. stops unless every published crate has the version that the tag names.
4. asks the crates.io API which crate versions exist, and keeps the others.
5. runs `cargo publish --dry-run` over the crates that it kept. Cargo resolves
   a pending sibling from the packages it has just made, and every other
   dependency from crates.io.

The `publish` job runs in the `crates-io` environment. It uploads the pending
crates one at a time, in dependency order. Cargo waits until each crate is in
the index before it uploads the next one.

A rerun is safe. The `plan` job skips each version that crates.io already
has, so a rerun after a failure uploads only the rest.

A manual run takes two inputs:

- `dry_run`, on by default. Off, the run uploads, and it must start from a
  `v*` tag.
- `crates`, a space-separated list of crate names. A run with a list publishes
  only those crates. Use it when one crate cannot publish and the others must
  not wait for it.

## Credentials: bootstrap, then trusted publishing

The `publish` job authenticates with one of two credentials:

- **A token.** When the `CARGO_REGISTRY_TOKEN` secret of the `crates-io`
  environment is set, the job uses it.
- **Trusted publishing.** When that secret is not set, the job runs
  [`rust-lang/crates-io-auth-action`](https://github.com/rust-lang/crates-io-auth-action).
  The action exchanges the job's GitHub OIDC token for a crates.io token. That
  token expires after 30 minutes, and the action revokes it when the job ends.
  No long-lived secret exists.

crates.io allows trusted publishing only for a crate that already exists. So
the first release of each crate name needs the token, and every later release
uses trusted publishing.

### Once: the GitHub environment

In the repository settings, open **Environments** and create `crates-io`. Under
**Deployment branches and tags**, allow only tags that match `v*`. Add required
reviewers if a person should approve each publish.

### First publish of a crate name

1. Sign in to crates.io with the account that will own the crates. Under
   **Account Settings**, open **API Tokens** and create a token with the
   `publish-new` and `publish-update` scopes. Limit it to the crate pattern
   `krabka-client-*` and give it a short expiry.
2. Add the token to the `crates-io` environment as the secret
   `CARGO_REGISTRY_TOKEN`.
3. Push the release tag, or rerun `publish.yml` on it.

crates.io limits new crate names to a burst of five, then one every ten
minutes. The four crates fit in the burst. On a `429` answer the job waits ten
minutes and tries again.

### Then: trusted publishing for each crate

For each published crate:

1. On crates.io, open the crate, then **Settings**, then **Trusted
   Publishing**.
2. Add a GitHub publisher with these values:
   - Repository owner: `krabka-io`
   - Repository name: `krabka-client-rs`
   - Workflow filename: `publish.yml`
   - Environment: `crates-io`

When all four crates have a publisher, delete the `CARGO_REGISTRY_TOKEN`
secret, and revoke the token on crates.io. The next release uses trusted
publishing. Its log says "publishing through trusted publishing".

A crate that joins the published set later needs the token once, for its
first release. Add the secret again for that release, configure the new
crate's publisher, and delete the secret again.
