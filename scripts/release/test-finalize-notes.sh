#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-numbered-notes.XXXXXX")"
fixture_complete=false
finish() {
    local status=$?
    [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
    if [[ "$status" == 0 ]]; then rm -rf "$fixture"
    else echo "Release-note fixture retained: $fixture" >&2; fi
    exit "$status"
}
trap finish EXIT
mkdir -p "$fixture/scripts/ci" "$fixture/scripts/release" "$fixture/docs/changelog"
cp "$root/scripts/ci/finalize-release-changelog.awk" "$fixture/scripts/ci/"
cp "$root/scripts/release/finalize-notes.sh" "$fixture/scripts/release/"
cd "$fixture"

for candidate in 0.264.11 0.265.0; do
    line="${candidate%.*}"
    printf '# Changelog\n\n## [%s]\n\nPending root notes.\n\n## [0.264.x] - 2026-10-04\n\nPublished root history.\n' "$candidate" > CHANGELOG.md
    printf '# 0.264\n\n## 0.264.10 — 2026-10-04\n\nPublished minor history.\n' > docs/changelog/0.264.md
    cp docs/changelog/0.264.md "$fixture/published"
    printf '# %s\n\n## [%s]\n\nPending detailed notes.\n' "$line" "$candidate" > "$fixture/pending"
    if [[ "$line" == 0.264 ]]; then
        cat "$fixture/pending" docs/changelog/0.264.md > "$fixture/combined"
        cp "$fixture/combined" docs/changelog/0.264.md
    else
        cp "$fixture/pending" "docs/changelog/$line.md"
    fi
    RELEASE_VERSION="$candidate" RELEASE_DATE=2026-10-05 bash scripts/release/finalize-notes.sh
    grep -Fx "## [$candidate] - 2026-10-05" CHANGELOG.md
    grep -Fx "## [$candidate] - 2026-10-05" "docs/changelog/$line.md"
    grep -Fx 'Pending root notes.' CHANGELOG.md
    grep -Fx 'Pending detailed notes.' "docs/changelog/$line.md"
    if [[ "$line" == 0.264 ]]; then
        awk '/^# 0.264$/ { count++ } count == 2 { print }' docs/changelog/0.264.md > "$fixture/history"
        cmp "$fixture/published" "$fixture/history"
    else
        cmp "$fixture/published" docs/changelog/0.264.md
    fi
    cp CHANGELOG.md "$fixture/root-before"
    cp "docs/changelog/$line.md" "$fixture/detail-before"
    if RELEASE_VERSION="$candidate" RELEASE_DATE=2026-10-05 bash scripts/release/finalize-notes.sh > "$fixture/output" 2>&1; then
        echo 'accepted repeated finalization' >&2; exit 1
    fi
    cmp "$fixture/root-before" CHANGELOG.md
    cmp "$fixture/detail-before" "docs/changelog/$line.md"
done

# Either view conflicting with the saved release selection preserves both files.
for conflict in root detail; do
    root_version=0.265.0
    detail_version=0.265.0
    if [[ "$conflict" == root ]]; then root_version=0.265.1; else detail_version=0.265.1; fi
    printf '# Changelog\n\n## [%s]\n\nRoot notes.\n' "$root_version" > CHANGELOG.md
    printf '# 0.265\n\n## [%s]\n\nDetailed notes.\n' "$detail_version" > docs/changelog/0.265.md
    cp CHANGELOG.md "$fixture/root-before"
    cp docs/changelog/0.265.md "$fixture/detail-before"
    if RELEASE_VERSION=0.265.0 RELEASE_DATE=2026-10-05 bash scripts/release/finalize-notes.sh > "$fixture/output" 2>&1; then
        echo 'accepted conflicting numbered notes' >&2; exit 1
    fi
    cmp "$fixture/root-before" CHANGELOG.md
    cmp "$fixture/detail-before" docs/changelog/0.265.md
done
echo 'numbered release-note finalization and preservation passed (isolated files)'
fixture_complete=true
