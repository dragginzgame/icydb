#!/usr/bin/env bash

set -euo pipefail

SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SELF_DIR/../.." && pwd)"
cd "$ROOT_DIR"
export PATH="$ROOT_DIR/.tools/host/bin:$PATH"

PUBLISH_DRY_RUN="${PUBLISH_DRY_RUN:-0}"
PUBLISH_FROM="${PUBLISH_FROM:-}"
PUBLISH_POLL_SECS="${PUBLISH_POLL_SECS:-10}"
PUBLISH_TIMEOUT_SECS="${PUBLISH_TIMEOUT_SECS:-300}"
PUBLISH_VALIDATE_ONLY="${PUBLISH_VALIDATE_ONLY:-0}"
PUBLISH_VERIFY="${PUBLISH_VERIFY:-auto}"
RELEASE_RECEIPT_DIR="${RELEASE_RECEIPT_DIR:-$ROOT_DIR/.cache/release-receipts}"

PUBLISH_ORDER=(
    icydb-diagnostic-code
    icydb-schema
    icydb-model-macros
    icydb-model
    icydb-core
    icydb
    icydb-cli
)

# Returns the package name for a manifest, ignoring unpublished helper crates.
publishable_manifest_name() {
    local manifest="$1"

    if grep -Eq '^[[:space:]]*publish[[:space:]]*=[[:space:]]*false' "$manifest"; then
        return 0
    fi

    awk '
        /^\[package\]/ { in_package = 1; next }
        /^\[/ && in_package { exit }
        in_package && $1 == "name" {
            gsub(/"/, "", $3);
            print $3;
            exit;
        }
    ' "$manifest"
}

# Fails before any publish attempt if the explicit order omits a publishable
# crate under crates/.  The order stays checked in so cargo errors identify the
# exact crate that failed instead of hiding behind runtime topological sorting.
validate_publish_order() {
    local expected
    local actual

    expected="$(printf '%s\n' "${PUBLISH_ORDER[@]}" | sort)"
    actual="$(
        find crates -mindepth 2 -maxdepth 2 -name Cargo.toml -print0 |
            while IFS= read -r -d '' manifest; do
                publishable_manifest_name "$manifest"
            done |
            sort
    )"

    if [ "$actual" != "$expected" ]; then
        echo "Publish order does not match publishable crates under crates/." >&2
        echo "" >&2
        echo "Expected from PUBLISH_ORDER:" >&2
        printf '%s\n' "$expected" >&2
        echo "" >&2
        echo "Actual publishable crates:" >&2
        printf '%s\n' "$actual" >&2
        exit 1
    fi
}

# Waits until crates.io exposes the freshly published version before publishing
# dependent crates that resolve the dependency from the registry.
wait_for_registry_version() {
    local crate="$1"
    local version="$2"
    local deadline=$((SECONDS + PUBLISH_TIMEOUT_SECS)) status

    while [ "$SECONDS" -lt "$deadline" ]; do
        if bash "$ROOT_DIR/scripts/ci/check-crates-io-version.sh" "$crate" "$version"; then
            echo "Observed $crate $version on crates.io"
            return 0
        else
            status=$?
            [[ "$status" == 1 ]] || return "$status"
        fi

        echo "Waiting for crates.io to expose $crate $version..."
        sleep "$PUBLISH_POLL_SECS"
    done

    echo "Timed out waiting for $crate $version to appear on crates.io" >&2
    return 1
}

# Returns success only when the shared release workflow recorded this exact annotated
# version tag after verifying its tested candidate transition.
release_receipt_matches() {
    local head_commit="$1"
    RELEASE_RECEIPT_DIR="$RELEASE_RECEIPT_DIR" \
        scripts/ci/verify-release-gate-receipt.sh "$head_commit"
}

version="$(bash "$SELF_DIR/read-cargo-workspace-version.sh" --stable "$ROOT_DIR/Cargo.toml")" || exit 1

validate_publish_order

for flag in PUBLISH_DRY_RUN PUBLISH_VALIDATE_ONLY; do
    [[ "${!flag}" == 0 || "${!flag}" == 1 ]] || {
        echo "$flag must be 0 or 1" >&2; exit 1;
    }
done
for interval in PUBLISH_POLL_SECS PUBLISH_TIMEOUT_SECS; do
    [[ "${!interval}" =~ ^[1-9][0-9]{0,8}$ ]] || {
        echo "$interval must be a positive integer of at most nine digits" >&2; exit 1;
    }
done

# Resolve the existing resume selection once, before any registry or publish call.
start=0
if [[ -n "$PUBLISH_FROM" ]]; then
    for ((start=0; start<${#PUBLISH_ORDER[@]}; start++)); do
        [[ "${PUBLISH_ORDER[start]}" != "$PUBLISH_FROM" ]] || break
    done
    [[ "$start" -lt "${#PUBLISH_ORDER[@]}" ]] || {
        echo "PUBLISH_FROM=$PUBLISH_FROM is not in the publish order" >&2; exit 1;
    }
fi

# Publication owns source admission, including direct script invocations.
git diff --quiet --ignore-submodules HEAD -- || {
    echo 'Publishing requires a clean tracked worktree and index' >&2; exit 1;
}
untracked="$(git ls-files --others --exclude-standard)"
[[ -z "$untracked" ]] || {
    echo 'Publishing requires committing or removing untracked source files' >&2; exit 1;
}
head_commit="$(git rev-parse --verify HEAD)"

case "$PUBLISH_VERIFY" in
    always)
        verify_packages=1
        echo "Cargo package verification forced for v$version"
        ;;
    auto)
        if release_receipt_matches "$head_commit"; then
            verify_packages=0
            echo "Reusing release-gate receipt for v$version; Cargo package rebuilds will be skipped"
        else
            verify_packages=1
            echo "No matching release-gate receipt for v$version; Cargo will verify every package"
        fi
        ;;
    *)
        echo "PUBLISH_VERIFY must be 'auto' or 'always'" >&2
        exit 1
        ;;
esac

if [ "$PUBLISH_VALIDATE_ONLY" = "1" ]; then
    echo "Publish order validated for IcyDB workspace version $version"
    exit 0
fi

for crate in "${PUBLISH_ORDER[@]:start}"; do
    if bash "$ROOT_DIR/scripts/ci/check-crates-io-version.sh" "$crate" "$version"; then
        echo "Skipping $crate $version (already on crates.io)"
        continue
    else
        status=$?
        [[ "$status" == 1 ]] || exit "$status"
    fi

    echo "Publishing $crate $version"
    publish_args=(publish -p "$crate" --locked --registry crates-io)
    if [ "$PUBLISH_DRY_RUN" = "1" ]; then
        publish_args+=(--dry-run)
    fi
    if [ "$verify_packages" -eq 0 ]; then
        publish_args+=(--no-verify)
    fi

    printf '+ cargo'
    printf ' %q' "${publish_args[@]}"
    printf '\n'
    cargo "${publish_args[@]}"

    if [ "$PUBLISH_DRY_RUN" != "1" ]; then
        wait_for_registry_version "$crate" "$version"
    fi
done
