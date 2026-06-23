#!/bin/bash

set -euo pipefail

SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SELF_DIR/../.." && pwd)"
cd "$ROOT_DIR"

PUBLISH_DRY_RUN="${PUBLISH_DRY_RUN:-0}"
PUBLISH_FROM="${PUBLISH_FROM:-}"
PUBLISH_POLL_SECS="${PUBLISH_POLL_SECS:-10}"
PUBLISH_TIMEOUT_SECS="${PUBLISH_TIMEOUT_SECS:-300}"
PUBLISH_VALIDATE_ONLY="${PUBLISH_VALIDATE_ONLY:-0}"

PUBLISH_ORDER=(
    icydb-diagnostic-code
    icydb-primitives
    icydb-utils
    icydb-schema
    icydb-build
    icydb-derive
    icydb-core
    icydb-schema-derive
    icydb-config
    icydb
    icydb-cli
)

# Extracts the current workspace version from the root manifest.
workspace_version() {
    awk '
        /^\[workspace.package\]/ { in_section = 1; next }
        /^\[/ && in_section { exit }
        in_section && $1 == "version" {
            gsub(/"/, "", $3);
            print $3;
            exit;
        }
    ' Cargo.toml
}

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

# Returns success once crates.io reports the expected version for a crate.
registry_has_version() {
    local crate="$1"
    local version="$2"

    cargo search "$crate" --limit 20 2>/dev/null |
        awk -v crate="$crate" -v version="$version" '
            $1 == crate {
                gsub(/"/, "", $3);
                if ($3 == version) {
                    found = 1;
                    exit 0;
                }
            }
            END { exit(found ? 0 : 1) }
        '
}

# Waits until crates.io exposes the freshly published version before publishing
# dependent crates that resolve the dependency from the registry.
wait_for_registry_version() {
    local crate="$1"
    local version="$2"
    local deadline=$((SECONDS + PUBLISH_TIMEOUT_SECS))

    while [ "$SECONDS" -lt "$deadline" ]; do
        if registry_has_version "$crate" "$version"; then
            echo "Observed $crate $version on crates.io"
            return 0
        fi

        echo "Waiting for crates.io to expose $crate $version..."
        sleep "$PUBLISH_POLL_SECS"
    done

    echo "Timed out waiting for $crate $version to appear on crates.io" >&2
    return 1
}

version="$(workspace_version)"
if [ -z "$version" ]; then
    echo "Failed to determine workspace version from Cargo.toml" >&2
    exit 1
fi

validate_publish_order
if [ "$PUBLISH_VALIDATE_ONLY" = "1" ]; then
    echo "Publish order validated for IcyDB workspace version $version"
    exit 0
fi

started=0
matched_from=0
if [ -z "$PUBLISH_FROM" ]; then
    started=1
fi

for crate in "${PUBLISH_ORDER[@]}"; do
    if [ "$started" -eq 0 ]; then
        if [ "$crate" != "$PUBLISH_FROM" ]; then
            continue
        fi
        started=1
        matched_from=1
    fi

    if registry_has_version "$crate" "$version"; then
        echo "Skipping $crate $version (already on crates.io)"
        continue
    fi

    echo "Publishing $crate $version"
    publish_args=(publish -p "$crate" --locked)
    if [ "$PUBLISH_DRY_RUN" = "1" ]; then
        publish_args+=(--dry-run)
    fi

    printf '+ cargo'
    printf ' %q' "${publish_args[@]}"
    printf '\n'
    cargo "${publish_args[@]}"

    if [ "$PUBLISH_DRY_RUN" != "1" ]; then
        wait_for_registry_version "$crate" "$version"
    fi
done

if [ -n "$PUBLISH_FROM" ] && [ "$matched_from" -eq 0 ]; then
    echo "PUBLISH_FROM=$PUBLISH_FROM is not in the publish order" >&2
    exit 1
fi
