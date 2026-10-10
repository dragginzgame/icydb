#!/usr/bin/env bash
set -euo pipefail

# Exercise real release preparation with only Cargo/Git effects substituted.
# No commits, registry calls or repository version changes occur.
unset MAKEFLAGS MAKEOVERRIDES MFLAGS
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export PATH="$ROOT/.tools/host/bin:$PATH"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-release-pins.XXXXXX")"
fixture_complete=false
finish() {
  local status=$?
  [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
  if [[ "$status" == 0 ]]; then
    rm -rf "$fixture"
  else
    echo "Release pin fixture retained: $fixture" >&2
  fi
  exit "$status"
}
trap finish EXIT
mkdir -p "$fixture/scripts/ci" "$fixture/scripts/release" "$fixture/ci" "$fixture/bin"
export PIN_FIXTURE_MKTEMP PIN_FIXTURE_SYSTEM_TMP
PIN_FIXTURE_MKTEMP="$(command -v mktemp)"
PIN_FIXTURE_SYSTEM_TMP="$fixture/native-temp"
mkdir "$PIN_FIXTURE_SYSTEM_TMP"
cp "$ROOT/scripts/ci/"{bump-version.sh,next-release-version.sh,sync-release-surface-version.sh} "$fixture/scripts/ci/"
cp "$ROOT/scripts/ci/rewrite-local-lock-versions.pl" "$fixture/scripts/ci/"
cp "$ROOT/scripts/ci/read-cargo-workspace-version.sh" "$fixture/scripts/ci/"
cp "$ROOT/scripts/release/pin-exceptions.jq" "$fixture/scripts/release/"
cp "$ROOT/scripts/release/check-metadata.sh" "$fixture/scripts/release/"
cp "$ROOT/ci/dependency-pinning-exceptions.json" "$fixture/ci/"
cp "$ROOT/Cargo.toml" "$fixture/"
previous="$(bash "$ROOT/scripts/ci/read-cargo-workspace-version.sh" --stable "$fixture/Cargo.toml")"
release="$(bash "$ROOT/scripts/ci/next-release-version.sh" "$previous" patch)"
packages="$(jq '[.[] | select(.subject | startswith("icydb")) | .subject]' "$fixture/ci/dependency-pinning-exceptions.json")"
jq -n --arg version "$previous" --argjson names "$packages" \
  '{workspace_members:$names, packages:[$names[] | {id:.,name:.,version:$version}]}' > "$fixture/metadata.json"
printf 'version = 3\n' > "$fixture/Cargo.lock"
jq -r '.packages[] | "\n[[package]]\nname = \"\(.name)\"\nversion = \"\(.version)\""' "$fixture/metadata.json" >> "$fixture/Cargo.lock"
printf '\n[[package]]\nname = "external"\nversion = "%s"\nsource = "registry+fixture"\nchecksum = "fixed"\n' "$previous" >> "$fixture/Cargo.lock"
# Backticks are literal Markdown delimiters in the release fixture.
# shellcheck disable=SC2016
printf 'Current workspace version: `%s`\ntag = "v%s"\n' "$previous" "$previous" > "$fixture/README.md"
cp "$fixture/ci/dependency-pinning-exceptions.json" "$fixture/before.json"
cp "$fixture/Cargo.toml" "$fixture/original.toml"
cp "$fixture/Cargo.lock" "$fixture/original.lock"
cp "$fixture/README.md" "$fixture/original-readme"

cat > "$fixture/bin/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  locate-project)
    [[ "${FAIL_VERSION_READ:-0}" == 0 ]] || exit 7
    printf '%s\n' "$PWD/Cargo.toml"
    ;;
  metadata)
    if [[ -n "${METADATA_TRACE:-}" ]]; then printf 'metadata\n' >> "$METADATA_TRACE"; fi
    cat metadata.json
    ;;
  set-version)
    if [[ "$2" == --help ]]; then exit 0; fi
    [[ "$2" == --workspace && "$4" == --offline ]]
    awk -v version="$3" '/^\[workspace.package\]/ { section=1 } /^\[/ && $0!="[workspace.package]" { section=0 } section && $1=="version" {$0="version = \"" version "\""} {print}' Cargo.toml > updated.toml
    cat updated.toml > Cargo.toml
    ;;
  *) exit 99 ;;
esac
CARGO
printf '#!/usr/bin/env bash\nexit 1\n' > "$fixture/bin/git"
# Darwin's template-free mode selects its native user temp directory before
# TMPDIR. Preserve that behavior in the substitute; explicit paths still use
# the real mktemp, qualifying caller-selected evidence retention on every host.
cat > "$fixture/bin/mktemp" <<'MKTEMP'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$#" == 1 && "$1" == -d ]]; then
  exec "$PIN_FIXTURE_MKTEMP" -d "$PIN_FIXTURE_SYSTEM_TMP/native.XXXXXX"
fi
exec "$PIN_FIXTURE_MKTEMP" "$@"
MKTEMP
chmod +x "$fixture/bin/"{cargo,git,mktemp}
# Version discovery and candidate disagreement must stop before manifest writes,
# including on the system Bash used by the native macOS qualification lanes.
mkdir "$fixture/guards"
mkdir -p "$fixture/docs/changelog"
# All later metadata checks could succeed. Refusal must come from the version
# boundary itself, rather than a missing date, notes file or checker fixture.
printf '## [%s] - 2026-10-06\n## [] - 2026-10-06\n' "$previous" > "$fixture/CHANGELOG.md"
cp "$fixture/CHANGELOG.md" "$fixture/docs/changelog/${previous%.*}.md"
cp "$fixture/CHANGELOG.md" "$fixture/docs/changelog/.md"
printf '#!/usr/bin/env bash\nexit 0\n' > "$fixture/scripts/ci/check-dependency-pins.sh"
for rejection in read mismatch; do
  fail_read=0
  candidate="$release"
  if [[ "$rejection" == read ]]; then fail_read=1; else candidate=0.0.0; fi
  if (
    cd "$fixture"
    TMPDIR="$fixture/guards" CARGO_HOME="$fixture/cargo" CARGO_TARGET_DIR="$fixture/target" \
      FAIL_VERSION_READ="$fail_read" RELEASE_VERSION="$candidate" PATH="$fixture/bin:$PATH" \
      bash scripts/ci/bump-version.sh patch
  ) > "$fixture/guard-$rejection" 2>&1; then
    echo 'version preparation admitted a failed discovery or conflicting candidate' >&2
    exit 1
  fi
  if (
    cd "$fixture"
    FAIL_VERSION_READ="$fail_read" RELEASE_VERSION="$candidate" RELEASE_DATE=2026-10-06 \
      METADATA_TRACE="$fixture/metadata-trace" PATH="$fixture/bin:$PATH" \
      bash scripts/release/check-metadata.sh
  ) > "$fixture/metadata-$rejection" 2>&1; then
    echo 'metadata admission accepted a failed discovery or conflicting version' >&2
    exit 1
  fi
  [[ ! -e "$fixture/metadata-trace" ]]
  cmp "$fixture/original.toml" "$fixture/Cargo.toml"
  cmp "$fixture/original.lock" "$fixture/Cargo.lock"
  cmp "$fixture/before.json" "$fixture/ci/dependency-pinning-exceptions.json"
done
(
  cd "$fixture"
  RELEASE_VERSION="$previous" RELEASE_DATE=2026-10-06 METADATA_TRACE="$fixture/metadata-trace" \
    PATH="$fixture/bin:$PATH" bash scripts/release/check-metadata.sh
) > "$fixture/metadata-admitted" 2>&1
[[ -s "$fixture/metadata-trace" ]]
(
  cd "$fixture"
  CARGO_HOME="$fixture/cargo" CARGO_TARGET_DIR="$fixture/target" \
    RELEASE_VERSION="$release" PATH="$fixture/bin:$PATH" bash scripts/ci/bump-version.sh patch
) > "$fixture/output" 2>&1 || { cat "$fixture/output" >&2; exit 1; }

# The candidate admission projection must predict exactly the prepared bytes.
jq --arg previous "$previous" --arg release "$release" --argjson packages "$packages" \
  -f "$ROOT/scripts/release/pin-exceptions.jq" "$fixture/before.json" > "$fixture/expected.json"
cmp "$fixture/expected.json" "$fixture/ci/dependency-pinning-exceptions.json"
jq -e --arg version "=$release" 'all(.[] | select(.subject | startswith("icydb")); .value == $version)' "$fixture/expected.json" >/dev/null
jq 'map(del(.value))' "$fixture/before.json" > "$fixture/reasons-before.json"
jq 'map(del(.value))' "$fixture/expected.json" > "$fixture/reasons-after.json"
cmp "$fixture/reasons-before.json" "$fixture/reasons-after.json"
jq '[.[] | select(.subject=="rusqlite")]' "$fixture/before.json" > "$fixture/oracle-before.json"
jq '[.[] | select(.subject=="rusqlite")]' "$fixture/expected.json" > "$fixture/oracle-after.json"
cmp "$fixture/oracle-before.json" "$fixture/oracle-after.json"
rg -U -F "$(printf 'name = "external"\nversion = "%s"\nsource = "registry+fixture"\nchecksum = "fixed"' "$previous")" "$fixture/Cargo.lock" >/dev/null

# A roster/lock mismatch must refuse replacement and retain the selected input
# plus failed candidate. The enclosing release adapter owns manifest rollback.
cp "$fixture/original.toml" "$fixture/Cargo.toml"
cp "$fixture/original.lock" "$fixture/Cargo.lock"
cp "$fixture/original-readme" "$fixture/README.md"
cp "$fixture/before.json" "$fixture/ci/dependency-pinning-exceptions.json"
jq --arg previous "$previous" \
  '.workspace_members += ["additional-member"] | .packages += [{id:"additional-member",name:"additional-member",version:$previous}]' \
  "$fixture/metadata.json" > "$fixture/inconsistent-metadata"
mv "$fixture/inconsistent-metadata" "$fixture/metadata.json"
mkdir "$fixture/retained"
if (
  cd "$fixture"
  TMPDIR="$fixture/retained" CARGO_HOME="$fixture/cargo" CARGO_TARGET_DIR="$fixture/target" \
    RELEASE_VERSION="$release" PATH="$fixture/bin:$PATH" bash scripts/ci/bump-version.sh patch
) > "$fixture/rejected" 2>&1; then
  echo 'version preparation admitted inconsistent local lock identities' >&2
  exit 1
fi
cmp "$fixture/original.lock" "$fixture/Cargo.lock"
retained=0
for input in "$fixture/retained"/*; do
  [[ -d "$input" && -f "$input/new-lock" && ! -s "$input/new-lock" ]]
  cmp "$fixture/original.lock" "$input/Cargo.lock"
  retained=$((retained + 1))
done
[[ "$retained" == 1 ]]
echo 'release pin exceptions follow preparation and admission; oracle, reasons and external lock selections preserved'
fixture_complete=true
