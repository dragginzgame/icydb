#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SUBJECT="$ROOT_DIR/scripts/ci/release-candidate-receipt.sh"
TEST_ROOT="$(mktemp -d)"
FIXTURE="$TEST_ROOT/repository"
RECEIPTS="$TEST_ROOT/receipts"

cleanup() {
    find "$TEST_ROOT" -depth -delete
}
trap cleanup EXIT

run_subject() {
    ICYDB_RELEASE_ROOT="$FIXTURE" RELEASE_RECEIPT_DIR="$RECEIPTS" "$SUBJECT" "$@"
}

expect_failure() {
    if "$@" >/dev/null 2>&1; then
        echo "Expected command to fail: $*" >&2
        exit 1
    fi
}

mkdir -p "$FIXTURE"
git -C "$FIXTURE" init -q
git -C "$FIXTURE" config user.name "IcyDB release fixture"
git -C "$FIXTURE" config user.email "release-fixture@invalid.example"
mkdir -p "$FIXTURE/docs/changelog"
printf '[workspace.package]\nversion = "0.223.6"\n' > "$FIXTURE/Cargo.toml"
printf 'version = 3\n\n[[package]]\nname = "fixture"\nversion = "0.223.6"\n' > "$FIXTURE/Cargo.lock"
printf 'IcyDB 0.223.6\n' > "$FIXTURE/README.md"
printf 'root release notes\n' > "$FIXTURE/CHANGELOG.md"
printf 'detailed release notes\n' > "$FIXTURE/docs/changelog/0.223.md"
printf 'candidate source\n' > "$FIXTURE/code.txt"
git -C "$FIXTURE" add Cargo.toml Cargo.lock README.md CHANGELOG.md docs/changelog/0.223.md code.txt
git -C "$FIXTURE" commit -q --no-verify -m "candidate"
candidate_commit="$(git -C "$FIXTURE" rev-parse HEAD)"

run_subject verify-tested-tree "$candidate_commit"
printf 'candidate source changed during test\n' > "$FIXTURE/code.txt"
expect_failure run_subject verify-tested-tree "$candidate_commit"
git -C "$FIXTURE" add code.txt
expect_failure run_subject verify-tested-tree "$candidate_commit"
git -C "$FIXTURE" restore --staged code.txt
git -C "$FIXTURE" restore code.txt
printf 'root release notes updated during test\n' > "$FIXTURE/CHANGELOG.md"
printf 'detailed release notes updated during test\n' > "$FIXTURE/docs/changelog/0.223.md"
run_subject verify-tested-tree "$candidate_commit"
git -C "$FIXTURE" add CHANGELOG.md docs/changelog/0.223.md
run_subject verify-tested-tree "$candidate_commit"

printf '[workspace.package]\nversion = "0.223.7"\n' > "$FIXTURE/Cargo.toml"
printf 'version = 3\n\n[[package]]\nname = "fixture"\nversion = "0.223.7"\n' > "$FIXTURE/Cargo.lock"
printf 'IcyDB 0.223.7\n' > "$FIXTURE/README.md"
expect_failure run_subject record patch 0000000000000000000000000000000000000000
run_subject record patch "$candidate_commit" >/dev/null

receipt="$RECEIPTS/v0.223.7.candidate"
test -s "$receipt"
grep -Fxq "candidate_commit=$candidate_commit" "$receipt"
grep -Fxq "candidate_version=0.223.6" "$receipt"
grep -Fxq "release_version=0.223.7" "$receipt"

git -C "$FIXTURE" add Cargo.toml Cargo.lock README.md
run_subject verify-staged

printf 'IcyDB 0.223.7 tampered\n' > "$FIXTURE/README.md"
git -C "$FIXTURE" add README.md
expect_failure run_subject verify-staged
git -C "$FIXTURE" commit -q --no-verify -m "tampered release"
expect_failure run_subject verify-commit
git -C "$FIXTURE" switch -q --detach "$candidate_commit"
printf '[workspace.package]\nversion = "0.223.7"\n' > "$FIXTURE/Cargo.toml"
printf 'version = 3\n\n[[package]]\nname = "fixture"\nversion = "0.223.7"\n' > "$FIXTURE/Cargo.lock"
printf 'IcyDB 0.223.7\n' > "$FIXTURE/README.md"
printf 'root release notes updated during test\n' > "$FIXTURE/CHANGELOG.md"
printf 'detailed release notes updated during test\n' > "$FIXTURE/docs/changelog/0.223.md"
git -C "$FIXTURE" add Cargo.toml Cargo.lock README.md CHANGELOG.md docs/changelog/0.223.md
run_subject verify-staged

git -C "$FIXTURE" commit -q --no-verify -m "Release 0.223.7"
run_subject verify-commit

printf 'dirty source\n' >> "$FIXTURE/code.txt"
expect_failure run_subject verify-commit
git -C "$FIXTURE" restore code.txt

printf '[workspace.package]\nversion = "0.223.8"\n' > "$FIXTURE/Cargo.toml"
printf 'version = 3\n\n[[package]]\nname = "fixture"\nversion = "0.223.8"\n' > "$FIXTURE/Cargo.lock"
printf 'IcyDB 0.223.8\n' > "$FIXTURE/README.md"
printf 'candidate source changed during bump\n' > "$FIXTURE/code.txt"
expect_failure run_subject record patch "$(git -C "$FIXTURE" rev-parse HEAD)"
grep -Fq 'version = "0.223.8"' "$FIXTURE/Cargo.toml"
git -C "$FIXTURE" restore code.txt
printf 'non-version lockfile change\n' >> "$FIXTURE/Cargo.lock"
expect_failure run_subject record patch "$(git -C "$FIXTURE" rev-parse HEAD)"
grep -Fq 'version = "0.223.8"' "$FIXTURE/Cargo.toml"

echo "release candidate receipt behavior passed"
