#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)" || exit 1
cd "$root"
export PATH="$root/.tools/host/bin:$PATH"
export CARGO_HOME="${CARGO_HOME:-$(make --no-print-directory -s print-cargo-home)}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(make --no-print-directory -s print-cargo-target-dir)}"
previous="$(bash scripts/ci/read-cargo-workspace-version.sh --stable Cargo.toml)" || exit 1
next="$(bash scripts/ci/next-release-version.sh "$previous" "${1:-patch}")" || exit 1
[[ -z "${RELEASE_VERSION:-}" || "$RELEASE_VERSION" == "$next" ]] || exit 1
if git rev-parse "v$next" >/dev/null 2>&1; then
    echo "Tag v$next already exists; refusing preparation" >&2
    exit 1
fi
scratch="$(mktemp -d "${TMPDIR:-/tmp}/icydb-version-projection.XXXXXX")" || exit 1
complete=false
finish_projection() {
    local status=$?
    [[ "$complete" == true || "$status" != 0 ]] || status=1
    if [[ "$status" == 0 ]]; then rm -rf "$scratch"
    else printf 'Failed version projection retained: %s\n' "$scratch" >&2; fi
    exit "$status"
}
trap finish_projection EXIT
# Prepare every candidate before publishing any file. The enclosing release
# transaction owns originals, rollback and recovery evidence.
cargo metadata --locked --offline --no-deps --format-version 1 > "$scratch/metadata.json" || exit 1
paths=(Cargo.toml Cargo.lock ci/dependency-pinning-exceptions.json)
# Private qualification owns its lock and refreshes it during adapter adoption.
# A product release never resolves or rewrites that independent graph.
for path in "${paths[@]}"; do
    mkdir -p "$scratch/$(dirname "$path")"
    bash scripts/release/render-candidate.sh "$path" "$path" "$previous" "$next" "$scratch/metadata.json" > "$scratch/$path" || exit 1
done
for path in "${paths[@]}"; do
    cat "$scratch/$path" > "$path" || exit 1
done
cargo metadata --locked --offline --no-deps --format-version 1 > "$scratch/prepared.json" || exit 1
complete=true
