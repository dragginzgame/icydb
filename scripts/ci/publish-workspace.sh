#!/bin/bash

set -euo pipefail

SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SELF_DIR/../.." && pwd)"
cd "$ROOT_DIR"

PUBLISH_DRY_RUN="${PUBLISH_DRY_RUN:-0}"
PUBLISH_FROM="${PUBLISH_FROM:-}"
PUBLISH_POLL_SECS="${PUBLISH_POLL_SECS:-10}"
PUBLISH_TIMEOUT_SECS="${PUBLISH_TIMEOUT_SECS:-300}"

PUBLISH_ORDER=(
    icydb-primitives
    icydb-utils
    icydb-derive
    icydb-core
    icydb-schema
    icydb-schema-derive
    icydb-build
    icydb
)

# Extract the shared workspace version from the root manifest so every publish
# step uses the same source of truth as the release bump flow.
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

# Treat crates.io visibility as the publish completion signal. This keeps the
# script restartable and allows `PUBLISH_FROM` resumes after partial publishes.
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

# Poll crates.io after a successful publish so dependent crates do not race
# against registry propagation.
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

    cargo "${publish_args[@]}"

    if [ "$PUBLISH_DRY_RUN" != "1" ]; then
        wait_for_registry_version "$crate" "$version"
    fi
done

if [ -n "$PUBLISH_FROM" ] && [ "$matched_from" -eq 0 ]; then
    echo "PUBLISH_FROM=$PUBLISH_FROM is not in the publish order" >&2
    exit 1
fi
