#!/usr/bin/env bash
set -euo pipefail
export RELEASE_GROUP=database

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-numbered-notes.XXXXXX")"
fixture="$(cd -P "$fixture" && pwd)"
fixture_complete=false
# shellcheck source=vendor/toko-tooling/scripts/ci/fixture-lifecycle.sh
source "$root/vendor/toko-tooling/scripts/ci/fixture-lifecycle.sh"
trap 'fixture_finish "$?" "$fixture" "Release-note fixture retained"' EXIT
mkdir -p "$fixture/scripts/ci" "$fixture/scripts/release" "$fixture/releases/database"
mkdir -p "$fixture/vendor/toko-tooling/scripts/ci/"
cp "$root/vendor/toko-tooling/scripts/ci/finalize-release-changelog.awk" "$fixture/vendor/toko-tooling/scripts/ci/"
cp "$root/scripts/release/finalize-notes.sh" "$fixture/scripts/release/"
cd "$fixture"
mkdir -p ci scripts/ci scripts/release releases/database
cp "$root/ci/cargo-release-groups.json" ci/
cp "$root/scripts/release/group.sh" scripts/release/
mkdir -p "vendor/toko-tooling/scripts/ci/"
cp "$root/vendor/toko-tooling/scripts/ci/cargo-release-groups.jq" "vendor/toko-tooling/scripts/ci/"
# This fixture selects its own numbered draft independently of repository notes.
sed 's/0.295/0.265/g' ci/cargo-release-groups.json > ci/selected.json
mv ci/selected.json ci/cargo-release-groups.json


for candidate in 0.264.11 0.265.0; do
    line="${candidate%.*}"
    jq --arg path "releases/database/$line.md" '(.groups[] | select(.name == "database").metadata_files) = [$path]' ci/cargo-release-groups.json > ci/next.json
    mv ci/next.json ci/cargo-release-groups.json
    printf '# Changelog\n\n## [%s] - 2026-10-05\n\nPending root notes.\n\n## [0.264.x] - 2026-10-04\n\nPublished root history.\n' "$candidate" > releases/database/CHANGELOG.md
    printf '# 0.264\n\n## 0.264.10 — 2026-10-04\n\nPublished minor history.\n' > releases/database/0.264.md
    cp releases/database/0.264.md "$fixture/published"
    printf '# %s\n\n## [%s]\n\nPending detailed notes.\n' "$line" "$candidate" > "$fixture/pending"
    if [[ "$line" == 0.264 ]]; then
        cat "$fixture/pending" releases/database/0.264.md > "$fixture/combined"
        cp "$fixture/combined" releases/database/0.264.md
    else
        cp "$fixture/pending" "releases/database/$line.md"
    fi
    RELEASE_VERSION="$candidate" RELEASE_DATE=2026-10-05 bash scripts/release/finalize-notes.sh
    grep -Fx "## [$candidate] - 2026-10-05" releases/database/CHANGELOG.md
    grep -Fx "## [$candidate] - 2026-10-05" "releases/database/$line.md"
    grep -Fx 'Pending root notes.' releases/database/CHANGELOG.md
    grep -Fx 'Pending detailed notes.' "releases/database/$line.md"
    if [[ "$line" == 0.264 ]]; then
        awk '/^# 0.264$/ { count++ } count == 2 { print }' releases/database/0.264.md > "$fixture/history"
        cmp "$fixture/published" "$fixture/history"
    else
        cmp "$fixture/published" releases/database/0.264.md
    fi
    cp releases/database/CHANGELOG.md "$fixture/root-before"
    cp "releases/database/$line.md" "$fixture/detail-before"
    if RELEASE_VERSION="$candidate" RELEASE_DATE=2026-10-05 bash scripts/release/finalize-notes.sh > "$fixture/output" 2>&1; then
        echo 'accepted repeated finalization' >&2; exit 1
    fi
    cmp "$fixture/root-before" releases/database/CHANGELOG.md
    cmp "$fixture/detail-before" "releases/database/$line.md"
done

# The shared hard cut requires numbered candidates at the top of both views.
for invalid in draft misplaced; do
    for view in root detail; do
        printf '# Changelog\n\n## [0.265.0] - 2026-10-05\n\nRoot notes.\n' > releases/database/CHANGELOG.md
        printf '# 0.265\n\n## [0.265.0]\n\nDetailed notes.\n' > releases/database/0.265.md
        target=releases/database/CHANGELOG.md
        [[ "$view" != detail ]] || target=releases/database/0.265.md
        if [[ "$invalid" == draft ]]; then
            printf '# Notes\n\n## [Draft]\n\nUnnumbered notes.\n' > "$target"
        else
            printf '# Notes\n\n## [0.264.0] - 2026-10-04\n\nHistory first.\n\n## [0.265.0]\n\nMisplaced candidate.\n' > "$target"
        fi
        cp releases/database/CHANGELOG.md "$fixture/root-before"
        cp releases/database/0.265.md "$fixture/detail-before"
        if RELEASE_VERSION=0.265.0 RELEASE_DATE=2026-10-05 bash scripts/release/finalize-notes.sh > "$fixture/output" 2>&1; then
            echo 'accepted unnumbered or misplaced notes' >&2; exit 1
        fi
        cmp "$fixture/root-before" releases/database/CHANGELOG.md
        cmp "$fixture/detail-before" releases/database/0.265.md
    done
done

# Either view conflicting with the saved release selection preserves both files.
for conflict in root detail; do
    root_version=0.265.0
    detail_version=0.265.0
    if [[ "$conflict" == root ]]; then root_version=0.265.1; else detail_version=0.265.1; fi
    printf '# Changelog\n\n## [%s] - 2026-10-05\n\nRoot notes.\n' "$root_version" > releases/database/CHANGELOG.md
    printf '# 0.265\n\n## [%s]\n\nDetailed notes.\n' "$detail_version" > releases/database/0.265.md
    cp releases/database/CHANGELOG.md "$fixture/root-before"
    cp releases/database/0.265.md "$fixture/detail-before"
    if RELEASE_VERSION=0.265.0 RELEASE_DATE=2026-10-05 bash scripts/release/finalize-notes.sh > "$fixture/output" 2>&1; then
        echo 'accepted conflicting numbered notes' >&2; exit 1
    fi
    cmp "$fixture/root-before" releases/database/CHANGELOG.md
    cmp "$fixture/detail-before" releases/database/0.265.md
done
echo 'numbered release-note finalization and preservation passed (isolated files)'
fixture_complete=true
