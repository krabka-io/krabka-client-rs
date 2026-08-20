#!/usr/bin/env bash
# Loads the Kafka images a container-driven suite needs, then runs it.
#
# The suites start containers through testcontainers, which by default pulls a
# missing image from the network mid-test. Loading Bazel's digest-pinned
# tarballs first means the tag the suite asks for is already present, so nothing
# is fetched while the test runs and the bytes are the ones the build pinned.
#
# Argument 1 is the test binary; KRABKA_IMAGE_TARS is a colon-separated list of
# tarballs, both passed as runfiles by //bazel:defs.bzl.
set -euo pipefail

if ! docker info >/dev/null 2>&1; then
    echo "docker_test.sh: no reachable Docker daemon." >&2
    echo "  These suites drive real Kafka containers. Start Docker, or run" >&2
    echo "  the default \`bazel test\`, which filters them out." >&2
    exit 1
fi

# `$(rootpath)` yields a path relative to the runfiles root, and a test does not
# run from there. Everything handed to this script is resolved against it.
runfile() {
    if [[ -e "$1" ]]; then
        printf '%s' "$1"
    elif [[ "$1" == external/* ]]; then
        # A path in another repository: those sit directly under the runfiles
        # root, keyed by repository name, not under this workspace's directory.
        printf '%s' "${TEST_SRCDIR}/${1#external/}"
    else
        printf '%s' "${TEST_SRCDIR}/${TEST_WORKSPACE}/$1"
    fi
}

binary="$(runfile "$1")"
shift

# Bazel hands us a hermetic JDK; the suites that need one shell out to `java`
# rather than going through a toolchain, so put it on PATH.
if [[ -n "${JAVA_HOME:-}" ]]; then
    JAVA_HOME="$(cd "$(runfile "${JAVA_HOME}")" && pwd)"
    export JAVA_HOME
    export PATH="${JAVA_HOME}/bin:${PATH}"
fi

if [[ -n "${KRABKA_IMAGE_TARS:-}" ]]; then
    while IFS= read -r -d ':' tar || [[ -n "${tar}" ]]; do
        [[ -n "${tar}" ]] || continue
        # `docker load` is a no-op when the image is already present, so this
        # costs nothing on a warm daemon and is the whole fetch on a cold one.
        docker load --quiet --input "$(runfile "${tar}")" >/dev/null
    done <<<"${KRABKA_IMAGE_TARS}"
fi

# `--ignored`: under Cargo these cases are `#[ignore]`d, because they need the
# daemon this script just checked for. This is the target that runs them.
#
# `--test-threads=1`: several of these bind a fixed host port, so the JVM tools
# in the container have a known bootstrap target. Run in parallel they collide
# with "Address already in use". The monorepo serialises them the same way, with
# nextest test-groups capped at one thread; libtest has no group concept, so the
# whole binary goes single-threaded.
exec "${binary}" --ignored --test-threads=1 "$@"
