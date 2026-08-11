#!/usr/bin/env bash
set -euo pipefail

fake_cargo() {
    case "${1:-} ${2:-}" in
        "set-version --help")
            exit 0
            ;;
        "get workspace.package.version")
            awk '
                /^\[workspace.package\]/ { in_section = 1; next }
                /^\[/ && in_section { exit }
                in_section && $1 == "version" {
                    gsub(/"/, "", $3);
                    print $3;
                    exit;
                }
            ' "$ICYDB_RELEASE_ROOT/Cargo.toml"
            ;;
        "set-version --workspace")
            sed -i "0,/version = \"$BUMP_FIXTURE_OLD\"/s//version = \"$BUMP_FIXTURE_NEW\"/" \
                "$ICYDB_RELEASE_ROOT/Cargo.toml"
            if [[ "$BUMP_FIXTURE_FAILURE" == "set" ]]; then
                exit 1
            fi
            ;;
        "generate-lockfile ")
            sed -i "0,/version = \"$BUMP_FIXTURE_OLD\"/s//version = \"$BUMP_FIXTURE_NEW\"/" \
                "$ICYDB_RELEASE_ROOT/Cargo.lock"
            if [[ "$BUMP_FIXTURE_FAILURE" == "lock" ]]; then
                printf 'partial lock mutation\n' >> "$ICYDB_RELEASE_ROOT/Cargo.lock"
                exit 1
            fi
            ;;
        *)
            echo "Unexpected fake cargo invocation: $*" >&2
            exit 2
            ;;
    esac
}

fake_sync() {
    sed -i "0,/$BUMP_FIXTURE_OLD/s//$1/" "$ICYDB_RELEASE_ROOT/README.md"
    case "$BUMP_FIXTURE_FAILURE" in
        sync)
            printf 'partial sync mutation\n' >> "$ICYDB_RELEASE_ROOT/README.md"
            exit 1
            ;;
        receipt)
            printf 'non-version receipt mutation\n' >> "$ICYDB_RELEASE_ROOT/README.md"
            ;;
    esac
}

case "$(basename "$0")" in
    cargo)
        fake_cargo "$@"
        exit 0
        ;;
    sync-release-surface-version.sh)
        fake_sync "$@"
        exit 0
        ;;
esac

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SUBJECT="$ROOT_DIR/scripts/ci/bump-version.sh"
# shellcheck source=scripts/ci/release-version-transition.sh
source "$ROOT_DIR/scripts/ci/release-version-transition.sh"
TEST_ROOT="$(mktemp -d)"
FIXTURE="$TEST_ROOT/repository"
BIN_DIR="$TEST_ROOT/bin"
RECEIPTS="$TEST_ROOT/receipts"
OLD_VERSION="0.223.7"
NEW_VERSION="0.223.8"

[[ "$(release_next_version patch 0.223.7)" == "0.223.8" ]]
[[ "$(release_next_version minor 0.223.7)" == "0.224.0" ]]
[[ "$(release_next_version major 0.223.7)" == "1.0.0" ]]
release_transition_is_valid minor 0.223.7 0.224.0
if release_transition_is_valid minor 0.223.7 0.223.8 >/dev/null 2>&1; then
    echo "Invalid release transition was accepted" >&2
    exit 1
fi

cleanup() {
    find "$TEST_ROOT" -depth -delete
}
trap cleanup EXIT

run_bump() {
    local failure="${1:-}"

    PATH="$BIN_DIR:$PATH" \
    CARGO_HOME="$TEST_ROOT/cargo-home" \
    CARGO_TARGET_DIR="$TEST_ROOT/cargo-target" \
    ICYDB_RELEASE_ROOT="$FIXTURE" \
    RELEASE_RECEIPT_DIR="$RECEIPTS" \
    BUMP_FIXTURE_OLD="$OLD_VERSION" \
    BUMP_FIXTURE_NEW="$NEW_VERSION" \
    BUMP_FIXTURE_FAILURE="$failure" \
        "$SUBJECT" patch "$candidate_commit"
}

expect_failure() {
    if "$@" >/dev/null 2>&1; then
        echo "Expected command to fail: $*" >&2
        exit 1
    fi
}

assert_rolled_back() {
    git -C "$FIXTURE" diff --quiet --ignore-submodules HEAD --
    grep -Fxq "version = \"$OLD_VERSION\"" "$FIXTURE/Cargo.toml"
    test ! -e "$RECEIPTS/v$NEW_VERSION.candidate"
}

mkdir -p "$FIXTURE/scripts/ci" "$BIN_DIR" "$RECEIPTS"
ln -s "$ROOT_DIR/scripts/ci/test-release-bump-transaction.sh" "$BIN_DIR/cargo"
ln -s "$ROOT_DIR/scripts/ci/test-release-bump-transaction.sh" \
    "$FIXTURE/scripts/ci/sync-release-surface-version.sh"
ln -s "$ROOT_DIR/scripts/ci/release-candidate-receipt.sh" \
    "$FIXTURE/scripts/ci/release-candidate-receipt.sh"
ln -s "$ROOT_DIR/scripts/ci/release-version-transition.sh" \
    "$FIXTURE/scripts/ci/release-version-transition.sh"

git -C "$FIXTURE" init -q
git -C "$FIXTURE" config user.name "IcyDB release fixture"
git -C "$FIXTURE" config user.email "release-fixture@invalid.example"
printf '[workspace.package]\nversion = "%s"\n' "$OLD_VERSION" > "$FIXTURE/Cargo.toml"
printf 'version = 3\n\n[[package]]\nname = "fixture"\nversion = "%s"\n\n[[package]]\nname = "external"\nversion = "%s"\nsource = "registry+https://example.invalid/index"\n' \
    "$OLD_VERSION" "$OLD_VERSION" > "$FIXTURE/Cargo.lock"
printf 'Current workspace version: %s%s%s\n' '`' "$OLD_VERSION" '`' > "$FIXTURE/README.md"
git -C "$FIXTURE" add Cargo.toml Cargo.lock README.md
git -C "$FIXTURE" commit -q --no-verify -m "candidate"
candidate_commit="$(git -C "$FIXTURE" rev-parse HEAD)"

expect_failure env \
    ICYDB_RELEASE_ROOT="$FIXTURE" CARGO_HOME="$TEST_ROOT/cargo-home" \
    CARGO_TARGET_DIR="$TEST_ROOT/cargo-target" \
    "$SUBJECT" patch 0000000000000000000000000000000000000000
assert_rolled_back

git -C "$FIXTURE" tag "v$NEW_VERSION"
expect_failure run_bump
assert_rolled_back
git -C "$FIXTURE" tag -d "v$NEW_VERSION" >/dev/null

expect_failure run_bump set
assert_rolled_back
expect_failure run_bump lock
assert_rolled_back
expect_failure run_bump sync
assert_rolled_back
expect_failure run_bump receipt
assert_rolled_back

run_bump >/dev/null
test -s "$RECEIPTS/v$NEW_VERSION.candidate"
grep -Fxq "version = \"$NEW_VERSION\"" "$FIXTURE/Cargo.toml"
grep -Fq "name = \"external\"" "$FIXTURE/Cargo.lock"
grep -Fq "version = \"$OLD_VERSION\"" "$FIXTURE/Cargo.lock"

echo "release bump transaction behavior passed"
