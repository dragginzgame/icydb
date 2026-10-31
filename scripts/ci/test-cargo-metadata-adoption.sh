#!/usr/bin/env bash
set -euo pipefail

# Exercise IcyDB's real Make gate in a disposable current-manifest inventory.
# Git effects are fixture-only; Cargo consumes prepared locked offline inputs.
unset MAKEFLAGS MAKEOVERRIDES MFLAGS GNUMAKEFLAGS
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
export PATH="$ROOT/.tools/host/bin:$ROOT/.tools/rust/common/bin:$PATH"
YQ="${YQ:-$ROOT/.tools/host/bin/yq}"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-cargo-adoption.XXXXXX")"
fixture_complete=false
finish() {
  local status=$?
  [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
  if [[ "$status" == 0 ]]; then rm -rf "$fixture"
  else echo "Cargo adoption fixture retained: $fixture" >&2; fi
  exit "$status"
}
trap finish EXIT
mkdir "$fixture/repo"
git -C "$ROOT" archive --format=tar HEAD > "$fixture/source.tar"
tar -xf "$fixture/source.tar" -C "$fixture/repo"
git -C "$ROOT" ls-files --cached --others --exclude-standard -z -- '*Cargo.toml' > "$fixture/manifests"
while IFS= read -r -d '' path; do
  if [[ -f "$ROOT/$path" ]]; then
    mkdir -p "$fixture/repo/$(dirname "$path")"
    cp "$ROOT/$path" "$fixture/repo/$path"
  fi
done < "$fixture/manifests"
# The pending qualification workspace is not in the committed archive yet.
# Copy its actual manifest, lock and targets together before Cargo admission.
cp -R "$ROOT/testing/canic-adapter" "$fixture/repo/testing/"
for path in Cargo.lock Makefile rust-toolchain.toml ci/dependency-pinning-exceptions.json \
  scripts/ci/read-cargo-workspace-version.sh scripts/ci/check-dependency-graph-invariants.sh \
  scripts/ci/check-cargo-dependency-boundaries.sh ci/external-ic-dependencies.txt \
  ci/cargo-dependency-boundaries.json; do
  cp "$ROOT/$path" "$fixture/repo/$path"
done
bash "$ROOT/scripts/ci/prepare-script-fixture.sh" "$ROOT" "$fixture/repo" \
  scripts/ci/check-dependency-pins.sh
cd "$fixture/repo"
git init --quiet
git add -- .
check() {
  make --no-print-directory check-dependency-pins "YQ=$YQ" > "$fixture/check.log" 2>&1
}
check || { cat "$fixture/check.log" >&2; exit 1; }
bash scripts/ci/check-dependency-graph-invariants.sh > "$fixture/graph.log" 2>&1
version="$(make --no-print-directory -s version "YQ=$YQ")"
cp crates/icydb-diagnostic-code/Cargo.toml "$fixture/member.toml"
# Renamed dependencies inherit their identity from the root catalog.
requirement="$("$YQ" -p toml -o json '.workspace.dependencies.remain' Cargo.toml |
  jq -r 'if type == "string" then . else .version end')"
printf '\n[workspace.dependencies.adoption_alias]\npackage = "remain"\nversion = "%s"\n' "$requirement" >> Cargo.toml
for section in dependencies dev-dependencies build-dependencies 'target."cfg(unix)".build-dependencies'; do
  cp "$fixture/member.toml" crates/icydb-diagnostic-code/Cargo.toml
  printf '\n[%s.adoption_alias]\nworkspace = true\n' "$section" \
    >> crates/icydb-diagnostic-code/Cargo.toml
  check || { cat "$fixture/check.log" >&2; exit 1; }
  printf 'version = "1"\n' >> crates/icydb-diagnostic-code/Cargo.toml
  if check; then echo 'member dependency source override admitted' >&2; exit 1; fi
done
cp "$fixture/member.toml" crates/icydb-diagnostic-code/Cargo.toml
printf '\n[build-dependencies.missing_alias]\nworkspace = true\n' >> crates/icydb-diagnostic-code/Cargo.toml
if check; then echo 'missing root dependency alias admitted' >&2; exit 1; fi
sed "s/version = { workspace = true }/version = \"$version\"/" "$fixture/member.toml" \
  > crates/icydb-diagnostic-code/Cargo.toml
if check; then echo 'independent member version admitted' >&2; exit 1; fi
cp "$fixture/member.toml" crates/icydb-diagnostic-code/Cargo.toml
check || { cat "$fixture/check.log" >&2; exit 1; }
# Exercise the actual consumer allowlist before injecting a forbidden canonical
# SDK/substrate identity through aliases, disabled optional features and inactive targets.
CARGO_HOME="$ROOT/.cache/cargo/icydb" \
  bash scripts/ci/check-cargo-dependency-boundaries.sh --consumer "$PWD" --external-ic \
  > "$fixture/boundaries.log" 2>&1
cp Cargo.toml "$fixture/catalog.toml"
for dependency in canic-icydb ic-cdk ic-stable-structures pocket-ic ic-representation-independent-hash ic-cdk-management-canister blake3; do
  cp "$fixture/catalog.toml" Cargo.toml
  case "$dependency" in
    canic-icydb) requirement=0.113.0 ;;
    blake3) requirement=1.8.7 ;;
    ic-cdk) requirement=0.20.3 ;;
    ic-stable-structures) requirement=0.7.2 ;;
    pocket-ic) requirement=16.1.0 ;;
    ic-representation-independent-hash) requirement=3.1.0 ;;
    ic-cdk-management-canister) requirement=0.1.1 ;;
  esac
  printf '\n[workspace.dependencies.boundary_alias]\npackage = "%s"\nversion = "%s"\n' \
    "$dependency" "$requirement" >> Cargo.toml
  for section in dependencies 'target."cfg(target_os = \"none\")".build-dependencies' \
      'target."cfg(target_os = \"none\")".dev-dependencies'; do
    cp "$fixture/member.toml" crates/icydb-diagnostic-code/Cargo.toml
    printf '\n[%s.boundary_alias]\nworkspace = true\n' "$section" \
      >> crates/icydb-diagnostic-code/Cargo.toml
    if [[ "$section" == dependencies ]]; then
      printf 'optional = true\n\n[features]\ndisabled-boundary = ["dep:boundary_alias"]\n' \
        >> crates/icydb-diagnostic-code/Cargo.toml
    fi
    # Successful metadata admission proves rejection is from the boundary policy,
    # rather than a malformed target table or an unavailable dependency.
    CARGO_HOME="$ROOT/.cache/cargo/icydb" cargo metadata --locked --offline \
      --no-deps --format-version 1 > "$fixture/injected-metadata.json"
    jq -e --arg dependency "$dependency" '.packages[] | select(.name == "icydb-diagnostic-code") | .dependencies |
      any(.name == $dependency and .rename == "boundary_alias")' \
      "$fixture/injected-metadata.json" > /dev/null
    if CARGO_HOME="$ROOT/.cache/cargo/icydb" \
      bash scripts/ci/check-cargo-dependency-boundaries.sh --consumer "$PWD" --external-ic \
        > "$fixture/boundary-rejection.log" 2>&1; then
      echo "consumer boundary admitted excluded dependency alias: $dependency" >&2; exit 1
    fi
  done
done
cp "$fixture/member.toml" crates/icydb-diagnostic-code/Cargo.toml
cp "$fixture/catalog.toml" Cargo.toml

# Core resolution must not even parse the composition workspace. Break only its
# manifest and inspect the real locked root graph; package selection alone would
# not provide this boundary if actors were still root workspace members.
cp testing/canic-adapter/Cargo.toml "$fixture/adapter.toml"
printf 'not valid TOML\n' > testing/canic-adapter/Cargo.toml
CARGO_HOME="$ROOT/.cache/cargo/icydb" cargo metadata --locked --offline \
  --format-version 1 > "$fixture/core-only-metadata.json"
jq -e 'all(.packages[]; .name != "canic-icydb" and .name != "canic" and
  .name != "canic-macros")' "$fixture/core-only-metadata.json" > /dev/null
cp "$fixture/adapter.toml" testing/canic-adapter/Cargo.toml

# Qualify the selected-source release checker with real Cargo and isolated Git
# commits. Unused catalog entries expose global-vs-grouped ordering without
# introducing a registry dependency into this fixture.
mkdir -p "$fixture/metadata/crates/primary/src"
cd "$fixture/metadata"
cat > Cargo.toml <<'MANIFEST'
[workspace]
members = ["crates/primary"]
resolver = "3"

[workspace.package]
version = "0.1.0"

[workspace.dependencies]
# Production dependencies
zeta = "1"
zulu = "1"

# Tests and tooling only
alpha = "1"
beta = "1"
MANIFEST
cat > crates/primary/Cargo.toml <<'MANIFEST'
[package]
name = "primary"
version.workspace = true
edition = "2024"
MANIFEST
printf 'pub struct Primary;\n' > crates/primary/src/lib.rs
printf '## [0.1.0] - 2026-10-18\n' > CHANGELOG.md
cargo generate-lockfile --offline > "$fixture/metadata-lock.log" 2>&1
cp Cargo.toml "$fixture/grouped.toml"
RELEASE_VERSION=0.1.0 RELEASE_DATE=2026-10-18 \
  bash "$ROOT/scripts/ci/cargo-release-metadata.sh" check primary --manifest-order-grouped \
  > "$fixture/current-grouped.log" 2>&1
# Formatting is stable and retains both role groups, despite global disorder.
cargo sort --workspace --grouped > "$fixture/grouped-write.log" 2>&1
cmp "$fixture/grouped.toml" Cargo.toml
git init --quiet
git add -- Cargo.toml Cargo.lock CHANGELOG.md crates/primary
git -c user.name=Fixture -c user.email=fixture@example.invalid -c core.hooksPath=/dev/null -c commit.gpgsign=false \
  commit --quiet -m 'Grouped release fixture'
selected="$(git rev-parse HEAD)"
perl -0777 -pe 's/(zeta = "1"\n)(zulu = "1"\n)/$2$1/ or die "missing group inputs\n"' \
  "$fixture/grouped.toml" > Cargo.toml
if RELEASE_VERSION=0.1.0 RELEASE_DATE=2026-10-18 \
  bash "$ROOT/scripts/ci/cargo-release-metadata.sh" check primary --manifest-order-grouped \
  > "$fixture/current-unsorted.log" 2>&1; then
  echo 'release ordering admitted an unsorted role group' >&2; exit 1
fi
# The selected commit passes even though the working manifest does not.
RELEASE_VERSION=0.1.0 RELEASE_DATE=2026-10-18 RELEASE_COMMIT="$selected" \
  bash "$ROOT/scripts/ci/cargo-release-metadata.sh" check primary --manifest-order-grouped \
  > "$fixture/committed-grouped.log" 2>&1
git add -- Cargo.toml
git -c user.name=Fixture -c user.email=fixture@example.invalid -c core.hooksPath=/dev/null -c commit.gpgsign=false \
  commit --quiet -m 'Unsorted selected fixture'
selected="$(git rev-parse HEAD)"
cp "$fixture/grouped.toml" Cargo.toml
if RELEASE_VERSION=0.1.0 RELEASE_DATE=2026-10-18 RELEASE_COMMIT="$selected" \
  bash "$ROOT/scripts/ci/cargo-release-metadata.sh" check primary --manifest-order-grouped \
  > "$fixture/committed-unsorted.log" 2>&1; then
  echo 'release ordering read the newer worktree instead of the selected commit' >&2; exit 1
fi
echo 'IcyDB Cargo inheritance, grouped ordering and selected-source release policies passed'
fixture_complete=true
