#!/usr/bin/env bash
set -euo pipefail

ROOT="$0"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
FIXTURE="$(mktemp -d "${TMPDIR:-/tmp}/dependency-pins-test.XXXXXX")"
trap 'if [[ $? == 0 ]]; then rm -rf "$FIXTURE"; else printf "Failed dependency-pins fixture retained: %s\n" "$FIXTURE" >&2; fi' EXIT
FIXTURE="$(cd "$FIXTURE" && pwd -P)"
consumer="$FIXTURE/consumer"
mkdir -p "$consumer/.github/workflows" "$consumer/src" "$consumer/ci" "$FIXTURE/sibling"
git init -q "$consumer"
cat > "$consumer/Cargo.toml" <<'TOML'
[package]
name = "pin-fixture"
version = "0.1.0"
edition = "2021"
[workspace]
[workspace.dependencies]
serde = "1.0"
TOML
printf 'pub fn fixture() {}\n' > "$consumer/src/lib.rs"
printf 'version = 3\n' > "$consumer/Cargo.lock"
printf '# Approved pinning exceptions\n\nFixture policy evidence.\n' > "$consumer/AGENTS.md"
git -C "$consumer" add -- Cargo.toml Cargo.lock src/lib.rs AGENTS.md
cp "$consumer/Cargo.toml" "$FIXTURE/base.toml"
cat > "$consumer/.github/workflows/ci.yml" <<'YAML'
name: fixture
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: "actions/checkout@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      - uses: >-
          owner/action@bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
      - uses: ./local-action
      - run: 'echo "uses: an/example@main"'
  reuse:
    uses: owner/repo/.github/workflows/reuse.yml@cccccccccccccccccccccccccccccccccccccccc
YAML
cp "$consumer/.github/workflows/ci.yml" "$FIXTURE/base.yml"
check() { bash "$ROOT/scripts/ci/check-dependency-pins.sh" --consumer "$consumer" > "$FIXTURE/output" 2>&1; }
reject() {
    local expected="$1"
    if check; then echo "pin fixture unexpectedly passed: $expected" >&2; exit 1; fi
    if ! rg -F "$expected" "$FIXTURE/output" >/dev/null; then cat "$FIXTURE/output" >&2; exit 1; fi
}
check || { cat "$FIXTURE/output" >&2; exit 1; }

# Full Git revs work in inline and ordinary TOML tables; floating choices fail.
cat >> "$consumer/Cargo.toml" <<'TOML'
example = { git = "https://example.invalid/repository", rev = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }
[target.'cfg(unix)'.build-dependencies.other]
git = "https://example.invalid/other"
rev = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
TOML
check || { cat "$FIXTURE/output" >&2; exit 1; }
for selection in 'branch = "main"' 'tag = "v1.0.0"' 'rev = "abcdef0"'; do
    cp "$FIXTURE/base.toml" "$consumer/Cargo.toml"
    printf 'example = { git = "https://example.invalid/repository", %s }\n' "$selection" >> "$consumer/Cargo.toml"
    reject cargo-git
done
cp "$FIXTURE/base.toml" "$consumer/Cargo.toml"
printf 'example = "*"\n' >> "$consumer/Cargo.toml"
reject cargo-range

cp "$FIXTURE/base.toml" "$consumer/Cargo.toml"
printf 'example = "=1.2.3"\n' >> "$consumer/Cargo.toml"
reject cargo-exact
cat > "$consumer/ci/dependency-pinning-exceptions.json" <<'JSON'
[{"rule":"cargo-exact","file":"Cargo.toml","subject":"example","value":"=1.2.3","reason":"Fixture ABI must match its paired component.","evidence":"AGENTS.md"}]
JSON
check || { cat "$FIXTURE/output" >&2; exit 1; }
cp "$consumer/ci/dependency-pinning-exceptions.json" "$FIXTURE/valid-exceptions.json"
cp "$consumer/Cargo.toml" "$FIXTURE/selected-manifest"
cp "$consumer/Cargo.lock" "$FIXTURE/selected-lock"
cp "$consumer/.git/index" "$FIXTURE/selected-index"
# Admission and suppression must use the same single exception document.
for mutation in 'map(del(.reason))' '. + .' '.'; do
    jq "$mutation" "$FIXTURE/valid-exceptions.json" > "$FIXTURE/exception-document"
    for order in first last; do
        if [[ "$order" == first ]]; then
            cat "$FIXTURE/exception-document" > "$consumer/ci/dependency-pinning-exceptions.json"
            printf '[]\n' >> "$consumer/ci/dependency-pinning-exceptions.json"
        else
            printf '[]\n' > "$consumer/ci/dependency-pinning-exceptions.json"
            cat "$FIXTURE/exception-document" >> "$consumer/ci/dependency-pinning-exceptions.json"
        fi
        cp "$consumer/ci/dependency-pinning-exceptions.json" "$FIXTURE/selected-exceptions"
        reject 'malformed or duplicate'
        cmp "$FIXTURE/selected-exceptions" "$consumer/ci/dependency-pinning-exceptions.json"
        cmp "$FIXTURE/selected-manifest" "$consumer/Cargo.toml"
        cmp "$FIXTURE/selected-lock" "$consumer/Cargo.lock"
        cmp "$FIXTURE/selected-index" "$consumer/.git/index"
    done
done
for invalid in '' 'null' '['; do
    printf '%s\n' "$invalid" > "$consumer/ci/dependency-pinning-exceptions.json"
    reject 'malformed or duplicate'
done
cp "$FIXTURE/valid-exceptions.json" "$consumer/ci/dependency-pinning-exceptions.json"
check || { cat "$FIXTURE/output" >&2; exit 1; }
# A reason for one version cannot silently authorize another version.
sed 's/=1.2.3/=1.2.4/' "$consumer/Cargo.toml" > "$FIXTURE/changed.toml"
cp "$FIXTURE/changed.toml" "$consumer/Cargo.toml"
reject cargo-exact
printf '{}\n' > "$consumer/ci/dependency-pinning-exceptions.json"
reject 'malformed or duplicate'
rm "$consumer/ci/dependency-pinning-exceptions.json"

cp "$FIXTURE/base.toml" "$consumer/Cargo.toml"
printf 'example = ">=1.0, =1.2.3"\n' >> "$consumer/Cargo.toml"
reject cargo-exact

cp "$FIXTURE/base.toml" "$consumer/Cargo.toml"
printf 'example = { path = "../sibling" }\n' >> "$consumer/Cargo.toml"
reject cargo-external-path
cat > "$consumer/ci/dependency-pinning-exceptions.json" <<'JSON'
[{"rule":"cargo-external-path","file":"Cargo.toml","subject":"example","value":"../sibling","reason":"Development checkout; release qualification records and verifies its commit and clean state.","evidence":"AGENTS.md"}]
JSON
check || { cat "$FIXTURE/output" >&2; exit 1; }
rm "$consumer/ci/dependency-pinning-exceptions.json"
cp "$FIXTURE/base.toml" "$consumer/Cargo.toml"
git -C "$consumer" rm --cached -q -- Cargo.lock
reject 'lockfile is not tracked'
git -C "$consumer" add -- Cargo.lock

# Independent nested workspaces need their own tracked lockfile.
mkdir -p "$consumer/testing/src"
cp "$FIXTURE/base.toml" "$consumer/testing/Cargo.toml"
cp "$consumer/src/lib.rs" "$consumer/testing/src/lib.rs"
reject 'requires a tracked lockfile'
cp "$consumer/Cargo.lock" "$consumer/testing/Cargo.lock"
git -C "$consumer" add -- testing/Cargo.lock
check || { cat "$FIXTURE/output" >&2; exit 1; }

for reference in 'actions/checkout@v6' 'owner/action@main' 'owner/action@abcdef0' 'docker://alpine:3'; do
    printf 'jobs: {test: {steps: [{uses: "%s"}]}}\n' "$reference" > "$consumer/.github/workflows/ci.yml"
    reject action-ref
done
cat > "$consumer/.github/workflows/ci.yml" <<'YAML'
jobs:
  assets:
    steps:
      - uses: actions/checkout@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
        with:
          repository: owner/assets
          ref: main
YAML
reject checkout-ref
cat > "$consumer/ci/dependency-pinning-exceptions.json" <<'JSON'
[{"rule":"checkout-ref","file":".github/workflows/ci.yml","subject":"owner/assets","value":"main","reason":"Follow published assets during development; freeze and verify the resolved commit in release qualification.","evidence":"AGENTS.md"}]
JSON
check || { cat "$FIXTURE/output" >&2; exit 1; }
rm "$consumer/ci/dependency-pinning-exceptions.json"
cp "$FIXTURE/base.yml" "$consumer/.github/workflows/ci.yml"

# Composite actions and reusable workflows are checked structurally too.
mkdir -p "$consumer/local-action"
printf 'runs: {using: composite, steps: [{uses: "owner/action@main"}]}\n' > "$consumer/local-action/action.yaml"
reject action-ref
rm "$consumer/local-action/action.yaml"
printf 'jobs: {test: {uses: "owner/repo/.github/workflows/test.yml@main"}}\n' > "$consumer/.github/workflows/ci.yml"
reject action-ref
printf 'jobs: {test: {container: "alpine:3"}}\n' > "$consumer/.github/workflows/ci.yml"
reject container-image
printf 'jobs: {test: {container: "alpine@sha256:%064d"}}\n' 0 > "$consumer/.github/workflows/ci.yml"
check || { cat "$FIXTURE/output" >&2; exit 1; }
printf 'jobs: [\n' > "$consumer/.github/workflows/ci.yml"
reject 'cannot parse'
cp "$FIXTURE/base.yml" "$consumer/.github/workflows/ci.yml"
check || { cat "$FIXTURE/output" >&2; exit 1; }
echo 'dependency pinning tests passed'
