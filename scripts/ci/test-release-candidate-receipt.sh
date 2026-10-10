#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SUBJECT="$ROOT_DIR/scripts/ci/release-candidate-receipt.sh"
TEST_ROOT="$(mktemp -d)"
FIXTURE="$TEST_ROOT/repository"
RECEIPTS="$TEST_ROOT/receipts"
fixture_complete=false

cleanup() {
    local status=$?
    [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
    if [[ "$status" == 0 ]]; then find "$TEST_ROOT" -depth -delete
    else echo "Release candidate receipt fixture retained: $TEST_ROOT" >&2; fi
    exit "$status"
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

write_lockfile() {
    local version="$1"

    # The checksum deliberately matches an unescaped SemVer regex but not the
    # literal version. Release validation must leave it untouched.
    printf 'version = 3\nchecksum = "prefix0a223b6suffix"\n\n[[package]]\nname = "fixture"\nversion = "%s"\n' \
        "$version" > "$FIXTURE/Cargo.lock"
}

mkdir -p "$FIXTURE"
git -C "$FIXTURE" init -q
git -C "$FIXTURE" config user.name "IcyDB release fixture"
git -C "$FIXTURE" config user.email "release-fixture@invalid.example"
mkdir -p "$FIXTURE/docs/changelog" "$FIXTURE/ci"
printf '.ignored/\n' > "$FIXTURE/.gitignore"
printf '[workspace]\n[workspace.package]\nversion = "0.223.6"\n[package]\nname = "fixture"\nversion.workspace = true\nedition = "2024"\n[lib]\npath = "code.txt"\n' > "$FIXTURE/Cargo.toml"
write_lockfile 0.223.6
printf 'IcyDB 0.223.6\n' > "$FIXTURE/README.md"
printf 'root release notes\n' > "$FIXTURE/CHANGELOG.md"
printf 'detailed release notes\n' > "$FIXTURE/docs/changelog/0.223.md"
printf '[{"rule":"cargo-exact","file":"Cargo.toml","subject":"fixture","value":"=0.223.6","reason":"Coupled generated API","evidence":"README.md"}]\n' | jq . > "$FIXTURE/ci/dependency-pinning-exceptions.json"
printf 'candidate source\n' > "$FIXTURE/code.txt"
git -C "$FIXTURE" add .gitignore Cargo.toml Cargo.lock README.md CHANGELOG.md docs/changelog/0.223.md code.txt ci/dependency-pinning-exceptions.json
git -C "$FIXTURE" commit -q --no-verify -m "candidate"
candidate_commit="$(git -C "$FIXTURE" rev-parse HEAD)"

run_subject verify-tested-tree "$candidate_commit"
printf 'untracked source used only by the working tree\n' > "$FIXTURE/build.rs"
expect_failure run_subject verify-tested-tree "$candidate_commit"
find "$FIXTURE" -maxdepth 1 -type f -name build.rs -delete
mkdir -p "$FIXTURE/.ignored"
printf 'ignored local artifact\n' > "$FIXTURE/.ignored/artifact.txt"
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

printf '[workspace]\n[workspace.package]\nversion = "0.223.7"\n[package]\nname = "fixture"\nversion.workspace = true\nedition = "2024"\n[lib]\npath = "code.txt"\n' > "$FIXTURE/Cargo.toml"
write_lockfile 0.223.7
jq --arg previous 0.223.6 --arg release 0.223.7 --argjson packages '["fixture"]' \
    -f "$ROOT_DIR/scripts/release/pin-exceptions.jq" "$FIXTURE/ci/dependency-pinning-exceptions.json" > "$TEST_ROOT/projected.json"
cp "$TEST_ROOT/projected.json" "$FIXTURE/ci/dependency-pinning-exceptions.json"
printf 'IcyDB 0.223.7\n' > "$FIXTURE/README.md"
expect_failure run_subject record patch 0000000000000000000000000000000000000000
run_subject record patch "$candidate_commit" >/dev/null

receipt="$RECEIPTS/v0.223.7.candidate"
test -s "$receipt"
grep -Fxq "candidate_commit=$candidate_commit" "$receipt"
grep -Fxq "candidate_version=0.223.6" "$receipt"
grep -Fxq "release_version=0.223.7" "$receipt"

git -C "$FIXTURE" add Cargo.toml Cargo.lock README.md ci/dependency-pinning-exceptions.json
run_subject verify-staged

# An exception reason is source policy, not version-only release metadata.
jq '.[0].reason = "changed during release"' "$FIXTURE/ci/dependency-pinning-exceptions.json" > "$TEST_ROOT/tampered.json"
cp "$TEST_ROOT/tampered.json" "$FIXTURE/ci/dependency-pinning-exceptions.json"
git -C "$FIXTURE" add ci/dependency-pinning-exceptions.json
expect_failure run_subject verify-staged
cp "$TEST_ROOT/projected.json" "$FIXTURE/ci/dependency-pinning-exceptions.json"
git -C "$FIXTURE" add ci/dependency-pinning-exceptions.json
run_subject verify-staged

printf 'IcyDB 0.223.7 tampered\n' > "$FIXTURE/README.md"
git -C "$FIXTURE" add README.md
expect_failure run_subject verify-staged
git -C "$FIXTURE" commit -q --no-verify -m "tampered release"
expect_failure run_subject verify-commit
git -C "$FIXTURE" switch -q --detach "$candidate_commit"
printf '[workspace]\n[workspace.package]\nversion = "0.223.7"\n[package]\nname = "fixture"\nversion.workspace = true\nedition = "2024"\n[lib]\npath = "code.txt"\n' > "$FIXTURE/Cargo.toml"
write_lockfile 0.223.7
jq --arg previous 0.223.6 --arg release 0.223.7 --argjson packages '["fixture"]' \
    -f "$ROOT_DIR/scripts/release/pin-exceptions.jq" "$FIXTURE/ci/dependency-pinning-exceptions.json" > "$TEST_ROOT/projected.json"
cp "$TEST_ROOT/projected.json" "$FIXTURE/ci/dependency-pinning-exceptions.json"
printf 'IcyDB 0.223.7\n' > "$FIXTURE/README.md"
printf 'root release notes updated during test\n' > "$FIXTURE/CHANGELOG.md"
printf 'detailed release notes updated during test\n' > "$FIXTURE/docs/changelog/0.223.md"
git -C "$FIXTURE" add Cargo.toml Cargo.lock README.md ci/dependency-pinning-exceptions.json CHANGELOG.md docs/changelog/0.223.md
run_subject verify-staged

git -C "$FIXTURE" commit -q --no-verify -m "Release 0.223.7"
run_subject verify-commit

# A newer committed fix does not replace the selected release's tested proof.
release_commit="$(git -C "$FIXTURE" rev-parse HEAD)"
printf 'newer committed fix\n' >> "$FIXTURE/code.txt"
git -C "$FIXTURE" add code.txt
git -C "$FIXTURE" commit -q --no-verify -m "later fix"
expect_failure run_subject verify-commit
RELEASE_COMMIT="$release_commit" run_subject verify-commit
RELEASE_COMMIT="$candidate_commit" expect_failure run_subject verify-commit
RELEASE_COMMIT=invalid expect_failure run_subject verify-commit

printf 'dirty source\n' >> "$FIXTURE/code.txt"
RELEASE_COMMIT="$release_commit" expect_failure run_subject verify-commit
git -C "$FIXTURE" restore code.txt
RELEASE_COMMIT="$release_commit" run_subject verify-commit

printf '[workspace]\n[workspace.package]\nversion = "0.223.8"\n[package]\nname = "fixture"\nversion.workspace = true\nedition = "2024"\n[lib]\npath = "code.txt"\n' > "$FIXTURE/Cargo.toml"
write_lockfile 0.223.8
printf 'IcyDB 0.223.8\n' > "$FIXTURE/README.md"
printf 'candidate source changed during bump\n' > "$FIXTURE/code.txt"
expect_failure run_subject record patch "$(git -C "$FIXTURE" rev-parse HEAD)"
grep -Fq 'version = "0.223.8"' "$FIXTURE/Cargo.toml"
git -C "$FIXTURE" restore code.txt
printf 'non-version lockfile change\n' >> "$FIXTURE/Cargo.lock"
expect_failure run_subject record patch "$(git -C "$FIXTURE" rev-parse HEAD)"
grep -Fq 'version = "0.223.8"' "$FIXTURE/Cargo.toml"

echo "release candidate receipt behavior passed"
fixture_complete=true
