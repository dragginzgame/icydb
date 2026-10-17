#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$root"
export PATH="$root/.tools/host/bin:$PATH"
export CARGO_HOME="${CARGO_HOME:-$(make --no-print-directory -s print-cargo-home)}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(make --no-print-directory -s print-cargo-target-dir)}"
previous="$(bash scripts/ci/read-cargo-workspace-version.sh --stable Cargo.toml)"
next="$(bash scripts/ci/next-release-version.sh "$previous" "${1:-patch}")"
[[ -z "${RELEASE_VERSION:-}" || "$RELEASE_VERSION" == "$next" ]] || exit 1
scratch="$(mktemp -d "${TMPDIR:-/tmp}/icydb-version-projection.XXXXXX")"
echo "Version projection retained: $scratch" >&2
# Prepare every candidate before publishing any file. The enclosing release
# transaction owns originals, rollback and recovery evidence.
cargo metadata --locked --offline --no-deps --format-version 1 > "$scratch/metadata.json"
for path in Cargo.toml Cargo.lock ci/dependency-pinning-exceptions.json; do
    mkdir -p "$scratch/$(dirname "$path")"
    bash scripts/release/render-candidate.sh "$path" "$path" "$previous" "$next" "$scratch/metadata.json" > "$scratch/$path"
done
for path in Cargo.lock ci/dependency-pinning-exceptions.json Cargo.toml; do
    cat "$scratch/$path" > "$path"
done
cargo metadata --locked --offline --no-deps --format-version 1 > "$scratch/prepared.json"
