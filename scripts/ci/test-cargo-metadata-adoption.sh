#!/usr/bin/env bash
set -euo pipefail

ordering_only=false
if [[ $# != 0 ]]; then
  [[ $# == 2 && "$1" == --case && "$2" == ordering ]] || {
    echo 'usage: test-cargo-metadata-adoption.sh [--case ordering]' >&2
    exit 2
  }
  ordering_only=true
fi

# Exercise IcyDB's real Make gate in a disposable current-manifest inventory.
# Git effects are fixture-only; Cargo consumes prepared locked offline inputs.
unset MAKEFLAGS MAKEOVERRIDES MFLAGS GNUMAKEFLAGS
# Each disposable repository selects its own release identity, including the
# coordinated workspace below; do not inherit the caller's product group.
unset RELEASE_GROUP RELEASE_COMMIT RELEASE_SOURCE
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
export PATH="$ROOT/.tools/host/bin:$ROOT/.tools/rust/common/bin:$PATH"
YQ="${YQ:-$ROOT/.tools/host/bin/yq}"
mkdir -p "$ROOT/target"
# shellcheck source=vendor/toko-tooling/scripts/ci/fixture-lifecycle.sh
source "$ROOT/vendor/toko-tooling/scripts/ci/fixture-lifecycle.sh"
fixture="$(fixture_create "$ROOT/target/cargo-adoption.XXXXXX")"
fixture_complete=false
trap 'fixture_finish "$?" "$fixture" "Cargo adoption fixture retained"' EXIT
mkdir -p "$fixture/templates"
# Synthetic repositories must not inherit the caller's Git/index context,
# signing policy or template hooks. Keep overrides confined to this process.
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TEMPLATE_DIR="$fixture/templates"
git_context="$(git rev-parse --local-env-vars)"
while IFS= read -r variable; do
  unset "$variable"
done <<< "$git_context"

# Keep the broader dependency matrix out of focused ordering qualification.
if [[ "$ordering_only" == false ]]; then
  mkdir "$fixture/repo"
  # Copy each current Git-visible input once. Deleted paths stay absent, and
  # pending source/lock/tooling changes share the same inventory as manifests.
  git -C "$ROOT" ls-files --cached --others --exclude-standard -z > "$fixture/source-paths"
  while IFS= read -r -d '' path; do
    [[ -e "$ROOT/$path" || -L "$ROOT/$path" ]] || continue
    [[ -f "$ROOT/$path" && ! -L "$ROOT/$path" ]] || {
      printf 'Cargo fixture requires a regular source file: %s\n' "$path" >&2
      exit 1
    }
    mkdir -p "$fixture/repo/$(dirname "$path")"
    cp -p "$ROOT/$path" "$fixture/repo/$path"
  done < "$fixture/source-paths"
  cd "$fixture/repo"
  git init --quiet
  git add -- .
  check() {
    make --no-print-directory check-dependency-pins "YQ=$YQ" \
      "CARGO_WORK_HOME=$ROOT/.cache/cargo/icydb" > "$fixture/check.log" 2>&1
  }
  check || { cat "$fixture/check.log" >&2; exit 1; }
  bash scripts/ci/check-dependency-graph-invariants.sh > "$fixture/graph.log" 2>&1
  cp Cargo.lock "$fixture/qualified.lock"
  # Derive distinct same-line versions so a supplier patch never makes a
  # negative example identical to the already qualified selection.
  duplicate_versions="$("$YQ" -p toml -o json '.package' "$fixture/qualified.lock" |
    jq -er '[.[] | select(.name == "ic-platform-types" or .name == "ic-platform-runtime" or
      .name == "sha2" or .name == "digest") |
      .name as $name | .version | split(".") |
      "\($name) \(.[0]).\(.[1]).\((.[2] | tonumber) + 1)"] |
      if length >= 4 then .[] else error("expected Types, Runtime and selected hash families") end')"
  duplicate_patches=()
  while IFS= read -r duplicate; do
    duplicate_patches+=("$duplicate")
  done <<< "$duplicate_versions"
  # Coexisting hash families do not admit another line or duplicate versions
  # within a family, and do not relax other sensitive dependency checks.
  for addition in 'sha2 0.12.0' 'digest 0.12.0' 'ic-storage 0.1.0' \
      'ic-testkit 0.100.2' 'pocket-ic 16.1.0' "${duplicate_patches[@]}"; do
    cp "$fixture/qualified.lock" Cargo.lock
    read -r package unexpected_version <<< "$addition"
    printf '\n[[package]]\nname = "%s"\nversion = "%s"\n' "$package" "$unexpected_version" >> Cargo.lock
    if bash scripts/ci/check-dependency-graph-invariants.sh > "$fixture/graph-negative.log" 2>&1; then
      echo "unexpected dependency admitted: $addition" >&2
      exit 1
    fi
  done
  cp "$fixture/qualified.lock" Cargo.lock
  printf '\n[[package]]\nname = "ic-platform-runtime"\nversion = "0.20.3"\ndependencies = ["ic-platform-runtime-guard"]\n' >> Cargo.lock
  if bash scripts/ci/check-dependency-graph-invariants.sh > "$fixture/runtime-negative.log" 2>&1; then
    echo 'old Storage runtime executor closure admitted' >&2
    exit 1
  fi
  cp "$fixture/qualified.lock" Cargo.lock
  version="$(RELEASE_GROUP=database make --no-print-directory -s release-version "YQ=$YQ" \
    "CARGO_WORK_HOME=$ROOT/.cache/cargo/icydb")"
  cp crates/icydb-diagnostic-code/Cargo.toml "$fixture/member.toml"
  # Renamed dependencies inherit their identity from the root catalog.
  requirement="$("$YQ" -p toml -o json '.workspace.dependencies.remain' Cargo.toml |
    jq -r 'if type == "string" then . else .version end')"
  printf '\n[workspace.dependencies.adoption_alias]\npackage = "remain"\nversion = "%s"\n' "$requirement" >> Cargo.toml
  for section in dependencies dev-dependencies build-dependencies 'target."cfg(unix)".build-dependencies'; do
    # Replace the original edge: Cargo rejects two names for the same package.
    sed '/^remain = /d' "$fixture/member.toml" > crates/icydb-diagnostic-code/Cargo.toml
    printf '\n[%s.adoption_alias]\nworkspace = true\n' "$section" \
      >> crates/icydb-diagnostic-code/Cargo.toml
    # Product dependency kinds alter the qualification lock's nonmember closure.
    CARGO_HOME="$ROOT/.cache/cargo/icydb" cargo metadata --offline --format-version 1 \
      --manifest-path qualification/Cargo.toml > "$fixture/renamed-qualification.json"
    check || { cat "$fixture/check.log" >&2; exit 1; }
    printf 'version = "1"\n' >> crates/icydb-diagnostic-code/Cargo.toml
    if check; then echo 'member dependency source override admitted' >&2; exit 1; fi
  done
  cp "$fixture/member.toml" crates/icydb-diagnostic-code/Cargo.toml
  printf '\n[build-dependencies.missing_alias]\nworkspace = true\n' >> crates/icydb-diagnostic-code/Cargo.toml
  if check; then echo 'missing root dependency alias admitted' >&2; exit 1; fi
  sed "s/^version = \"$version\"/version = \"0.0.0\"/" "$fixture/member.toml" \
    > crates/icydb-diagnostic-code/Cargo.toml
  if check; then echo 'mismatched group member version admitted' >&2; exit 1; fi
  cp "$fixture/member.toml" crates/icydb-diagnostic-code/Cargo.toml
  CARGO_HOME="$ROOT/.cache/cargo/icydb" cargo metadata --offline --format-version 1 \
    --manifest-path qualification/Cargo.toml > "$fixture/restored-qualification.json"
  check || { cat "$fixture/check.log" >&2; exit 1; }
  # Exercise the actual consumer allowlist before injecting a forbidden canonical
  # SDK/substrate identity through aliases, disabled optional features and inactive targets.
  CARGO_HOME="$ROOT/.cache/cargo/icydb" \
    bash vendor/toko-tooling/scripts/ci/check-cargo-dependency-boundaries.sh --consumer "$PWD" --external-ic \
    > "$fixture/boundaries.log" 2>&1
  cp Cargo.toml "$fixture/catalog.toml"
  for dependency in canic-icydb ic-cdk ic-stable-structures pocket-ic ic-representation-independent-hash ic-cdk-management-canister blake3 ic-platform ic-platform-runtime ic-platform-types; do
    cp "$fixture/catalog.toml" Cargo.toml
    case "$dependency" in
      canic-icydb) requirement=0.113.0 ;;
      blake3) requirement=1.8.7 ;;
      ic-cdk) requirement=0.20.3 ;;
      ic-stable-structures) requirement=0.7.2 ;;
      pocket-ic) requirement=16.1.0 ;;
      ic-representation-independent-hash) requirement=3.1.0 ;;
      ic-cdk-management-canister) requirement=0.1.1 ;;
      ic-platform) requirement=0.17.0 ;;
      ic-platform-runtime) requirement=0.17.1 ;;
      ic-platform-types) requirement=0.16.0 ;;
    esac
    printf '\n[workspace.dependencies.boundary_alias]\npackage = "%s"\nversion = "%s"\n' \
      "$dependency" "$requirement" >> Cargo.toml
    for section in dependencies build-dependencies dev-dependencies \
        'target."cfg(target_os = \"none\")".build-dependencies' \
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
        bash vendor/toko-tooling/scripts/ci/check-cargo-dependency-boundaries.sh --consumer "$PWD" --external-ic \
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
  jq -e 'all(.packages[].targets[]; .name != "canister_artifact_contract")' \
    "$fixture/core-only-metadata.json" > /dev/null
  cp "$fixture/adapter.toml" testing/canic-adapter/Cargo.toml
  # Product acceptance checks the declared composition owner without resolving its
  # SDK graph: an incompatible adapter lock must not block independent core work.
  "$YQ" -p toml -o json '.' testing/integration/Cargo.toml |
    jq -e '.package.workspace == "../canic-adapter" and
      any(.test[]; .name == "canister_artifact_contract" and
        .path == "tests/canister_artifact_contract.rs")' > /dev/null

fi

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
  bash "$ROOT/vendor/toko-tooling/scripts/ci/cargo-release-metadata.sh" check primary --manifest-order-grouped \
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
  bash "$ROOT/vendor/toko-tooling/scripts/ci/cargo-release-metadata.sh" check primary --manifest-order-grouped \
  > "$fixture/current-unsorted.log" 2>&1; then
  echo 'release ordering admitted an unsorted role group' >&2; exit 1
fi
# The selected commit passes even though the working manifest does not.
RELEASE_VERSION=0.1.0 RELEASE_DATE=2026-10-18 RELEASE_COMMIT="$selected" \
  bash "$ROOT/vendor/toko-tooling/scripts/ci/cargo-release-metadata.sh" check primary --manifest-order-grouped \
  > "$fixture/committed-grouped.log" 2>&1
git add -- Cargo.toml
git -c user.name=Fixture -c user.email=fixture@example.invalid -c core.hooksPath=/dev/null -c commit.gpgsign=false \
  commit --quiet -m 'Unsorted selected fixture'
selected="$(git rev-parse HEAD)"
cp "$fixture/grouped.toml" Cargo.toml
if RELEASE_VERSION=0.1.0 RELEASE_DATE=2026-10-18 RELEASE_COMMIT="$selected" \
  bash "$ROOT/vendor/toko-tooling/scripts/ci/cargo-release-metadata.sh" check primary --manifest-order-grouped \
  > "$fixture/committed-unsorted.log" 2>&1; then
  echo 'release ordering read the newer worktree instead of the selected commit' >&2; exit 1
fi
if [[ "$ordering_only" == true ]]; then
  echo 'IcyDB grouped ordering and selected-source release policies passed'
else
  echo 'IcyDB Cargo inheritance, grouped ordering and selected-source release policies passed'
fi
fixture_complete=true
