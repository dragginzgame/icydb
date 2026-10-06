#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
bash "$root/scripts/ci/check-release-commands.sh" "$root" scripts/ci/actionlint-checksums.tsv
fixture="$(mktemp -d "${TMPDIR:-/tmp}/standard-release-entry.XXXXXX")"
# Surface inner Make diagnostics before removing disposable fixture inputs.
trap '
    status=$?
    if [[ "$status" != 0 && -f "$fixture/output" ]]; then
        cat "$fixture/output" >&2 || true
    fi
    rm -rf "$fixture"
    exit "$status"
' EXIT
mkdir -p "$fixture/bin"
real_bash="$(command -v bash)"
printf '#!%s\n' "$real_bash" > "$fixture/bin/bash"
cat >> "$fixture/bin/bash" <<'STUB'
set -euo pipefail
printf '%s\n' "$*" >> "$EVENTS"
[[ "${FAIL_RUNNER:-0}" == 0 ]]
STUB
chmod +x "$fixture/bin/bash"
if [[ -f "$root/tool-versions.env" ]]; then cp "$root/tool-versions.env" "$fixture/"; fi
export EVENTS="$fixture/events"
cd "$fixture"
real_make="$(command -v make)"
# Real preflight must fill its selected cache before the offline validation gate.
# Simulate a cold cache without registry requests or dependency changes.
mkdir -p scripts/ci
cp "$root/scripts/ci/finalize-release-changelog.awk" scripts/ci/
printf '# Changelog\n\n## [0.1.1]\n\nFixture notes.\n' > CHANGELOG.md
export CACHE_READY="$fixture/cache-ready"
export EXPECTED_CARGO_HOME="$root/.cache/cargo/icydb"
printf '#!%s\n' "$real_bash" > "$fixture/bin/cargo"
cat >> "$fixture/bin/cargo" <<'CARGO'
set -euo pipefail
[[ "$CARGO_HOME" == "$EXPECTED_CARGO_HOME" ]]
printf 'cargo %s\n' "$*" >> "$EVENTS"
case "$*" in
    'fetch --locked')
        [[ "${CARGO_NET_OFFLINE:-false}" != true ]]
        [[ "${FAIL_FETCH:-0}" == 0 ]] || exit 1
        touch "$CACHE_READY"
        ;;
    'metadata --locked --offline --no-deps --format-version 1')
        [[ "${CARGO_NET_OFFLINE:-false}" == true && -f "$CACHE_READY" ]]
        ;;
    *) exit 99 ;;
esac
CARGO
printf '#!%s\n' "$real_bash" > "$fixture/bin/make"
cat >> "$fixture/bin/make" <<'MAKE'
set -euo pipefail
if [[ "$*" == '--no-print-directory validate' ]]; then
    CARGO_HOME="$EXPECTED_CARGO_HOME" cargo metadata --locked --offline --no-deps --format-version 1
else
    exec "$REAL_MAKE" -f "$ROOT_MAKEFILE" "$@"
fi
MAKE
chmod +x "$fixture/bin/cargo" "$fixture/bin/make"
export REAL_MAKE="$real_make"
export ROOT_MAKEFILE="$root/Makefile"
for fail in 0 1; do
    : > "$EVENTS"
    rm -f "$CACHE_READY"
    status=0
    # Only command stubs run here; the simulated preflight precedes offline validation.
    CARGO_NET_OFFLINE=false PATH="$fixture/bin:$PATH" FAIL_FETCH="$fail" "$real_make" --no-print-directory -f "$root/Makefile" \
        release-preflight release-verify "MAKE=$fixture/bin/make" \
        RELEASE_SOURCE=1111111111111111111111111111111111111111 \
        RELEASE_VERSION=0.1.1 RELEASE_DATE=2026-10-05 \
        "RELEASE_TMP_DIR=$fixture/release-tmp" > "$fixture/output" 2>&1 || status=$?
    printf '%s\n' 'scripts/ci/release-candidate-receipt.sh verify-tested-tree 1111111111111111111111111111111111111111' \
        'cargo fetch --locked' > "$fixture/expected"
    if [[ "$fail" == 0 ]]; then
        [[ "$status" == 0 ]]
        printf '%s\n' 'cargo metadata --locked --offline --no-deps --format-version 1' >> "$fixture/expected"
    else
        [[ "$status" != 0 && ! -e "$CACHE_READY" ]]
    fi
    cmp "$fixture/expected" "$EVENTS"
done
echo 'standard release Make adapters passed (command stubs; no Git effects)'
