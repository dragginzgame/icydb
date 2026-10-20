#!/usr/bin/env bash
set -euo pipefail

# Exercise real release preparation with only Cargo/Git effects substituted.
# No commits, registry calls or repository version changes occur.
unset MAKEFLAGS MAKEOVERRIDES MFLAGS
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export PATH="$ROOT/.tools/host/bin:$PATH"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-release-pins.XXXXXX")"
fixture="$(cd -P "$fixture" && pwd)"
fixture_complete=false
# shellcheck source=scripts/ci/fixture-lifecycle.sh
source "$ROOT/scripts/ci/fixture-lifecycle.sh"
trap 'fixture_finish "$?" "$fixture" "Release pin fixture retained"' EXIT
mkdir -p "$fixture/scripts/ci" "$fixture/scripts/release" "$fixture/ci" "$fixture/bin"
export PIN_FIXTURE_MKTEMP PIN_FIXTURE_SYSTEM_TMP
PIN_FIXTURE_MKTEMP="$(command -v mktemp)"
PIN_FIXTURE_SYSTEM_TMP="$fixture/native-temp"
mkdir "$PIN_FIXTURE_SYSTEM_TMP"
cp "$ROOT/scripts/ci/"{bump-version.sh,next-release-version.sh} "$fixture/scripts/ci/"
cp "$ROOT/scripts/release/"{render-candidate.sh,prepare.sh,files.sh} "$fixture/scripts/release/"
# Shared version admission retains its declared companions.
cp "$ROOT/scripts/ci/"{validate-release-inputs.pl,StableVersion.pm} "$fixture/scripts/ci/"
cp "$ROOT/scripts/ci/read-cargo-workspace-version.sh" "$fixture/scripts/ci/"
cp "$ROOT/scripts/release/pin-exceptions.jq" "$fixture/scripts/release/"
cp "$ROOT/scripts/release/check-metadata.sh" "$fixture/scripts/release/"
bash "$ROOT/scripts/ci/prepare-script-fixture.sh" "$ROOT" "$fixture" \
  scripts/ci/cargo-release-metadata.sh
# A producer may print a complete version before reporting failure. Qualify
# caller admission separately from parsing the version itself.
cat >> "$fixture/scripts/ci/read-cargo-workspace-version.sh" <<'READER'
[[ "${FAIL_AFTER_VERSION:-0}" == 0 ]] || exit 17
READER
cp "$ROOT/ci/dependency-pinning-exceptions.json" "$fixture/ci/"
cp "$ROOT/Cargo.toml" "$fixture/"
previous="$(bash "$ROOT/scripts/ci/read-cargo-workspace-version.sh" --stable "$fixture/Cargo.toml")"
release="$(bash "$ROOT/scripts/ci/next-release-version.sh" "$previous" patch)"
packages="$(jq '[.[] | select(.subject | startswith("icydb")) | .subject]' "$fixture/ci/dependency-pinning-exceptions.json")"
jq -n --arg version "$previous" --argjson names "$packages" --arg root "$fixture" \
  '{workspace_members:$names, packages:[$names[] | {id:.,name:.,version:$version,manifest_path:($root+"/crates/"+.+"/Cargo.toml")}]}' > "$fixture/metadata.json"
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
  sort) [[ "$*" == "sort --workspace --check --grouped" ]] ;;
  metadata)
    if [[ -n "${METADATA_TRACE:-}" ]]; then printf 'metadata\n' >> "$METADATA_TRACE"; fi
    cat metadata.json
    ;;
  *) exit 99 ;;
esac
CARGO
cat > "$fixture/bin/git" <<'GIT'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == ls-files ]]; then
  [[ "${FAIL_FILE_INVENTORY:-0}" == 0 ]] || exit 23
  exit 0
fi
exit 1
GIT
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
# files.sh emits the known metadata paths before asking Git for child manifests.
# A failed Git inventory must stop before the first metadata/edit attempt, even
# when enough valid paths were emitted to take snapshots and continue.
printf '## [%s]\n' "$release" > "$fixture/CHANGELOG.md"
cp "$fixture/CHANGELOG.md" "$fixture/docs/changelog/${release%.*}.md"
if (
  cd "$fixture"
  TMPDIR="$fixture/guards" CARGO_HOME="$fixture/cargo" CARGO_TARGET_DIR="$fixture/target" \
    FAIL_FILE_INVENTORY=1 METADATA_TRACE="$fixture/metadata-trace" \
    RELEASE_PREVIOUS="$previous" RELEASE_VERSION="$release" RELEASE_KIND=patch \
    RELEASE_DATE=2026-10-06 RELEASE_SOURCE=1111111111111111111111111111111111111111 \
    PATH="$fixture/bin:$PATH" bash scripts/release/prepare.sh
) > "$fixture/inventory-rejected" 2>&1; then
  echo 'preparation admitted a failed release inventory' >&2
  exit 1
fi
[[ ! -e "$fixture/metadata-trace" ]] || {
  echo 'preparation attempted edits after a failed release inventory' >&2
  exit 1
}
[[ -n "$(find "$fixture/guards" -type f -name file-inventory -print)" ]]
cmp "$fixture/original.toml" "$fixture/Cargo.toml"
cmp "$fixture/original.lock" "$fixture/Cargo.lock"
cmp "$fixture/before.json" "$fixture/ci/dependency-pinning-exceptions.json"
# All later metadata checks could succeed. Refusal must come from the version
# boundary itself, rather than a missing date, notes file or checker fixture.
printf '## [%s] - 2026-10-06\n## [] - 2026-10-06\n' "$previous" > "$fixture/CHANGELOG.md"
cp "$fixture/CHANGELOG.md" "$fixture/docs/changelog/${previous%.*}.md"
cp "$fixture/CHANGELOG.md" "$fixture/docs/changelog/.md"
printf '#!/usr/bin/env bash\nexit 0\n' > "$fixture/scripts/ci/check-dependency-pins.sh"
for rejection in read complete-read mismatch; do
  fail_read=0 fail_after=0
  candidate="$release"
  if [[ "$rejection" == read ]]; then fail_read=1;
  elif [[ "$rejection" == complete-read ]]; then fail_after=1;
  else candidate=0.0.0; fi
  if (
    cd "$fixture"
    TMPDIR="$fixture/guards" CARGO_HOME="$fixture/cargo" CARGO_TARGET_DIR="$fixture/target" \
      FAIL_VERSION_READ="$fail_read" FAIL_AFTER_VERSION="$fail_after" RELEASE_VERSION="$candidate" PATH="$fixture/bin:$PATH" \
      bash scripts/ci/bump-version.sh patch
  ) > "$fixture/guard-$rejection" 2>&1; then
    echo 'version preparation admitted a failed discovery or conflicting candidate' >&2
    exit 1
  fi
  if (
    cd "$fixture"
    FAIL_VERSION_READ="$fail_read" FAIL_AFTER_VERSION="$fail_after" RELEASE_VERSION="$candidate" RELEASE_DATE=2026-10-06 \
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
# Complete-looking output with a nonzero editor status never publishes metadata.
export REAL_METADATA_EDITOR="$CARGO_RELEASE_METADATA_EDITOR"
cat > "$fixture/bin/failed-editor" <<'EDITOR'
#!/usr/bin/env bash
set -euo pipefail
"$REAL_METADATA_EDITOR" "$@"
if [[ "$3" == "$FAILED_OPERATION" ]]; then exit 23; fi
EDITOR
chmod +x "$fixture/bin/failed-editor"
for operation in manifest lock; do
  if (
    cd "$fixture"
    CARGO_HOME="$fixture/cargo" CARGO_TARGET_DIR="$fixture/target" \
      RELEASE_VERSION="$release" PATH="$fixture/bin:$PATH" FAILED_OPERATION="$operation" \
      CARGO_RELEASE_METADATA_EDITOR="$fixture/bin/failed-editor" bash scripts/ci/bump-version.sh patch
  ) > "$fixture/failed-$operation" 2>&1; then
    echo 'failed renderer published a candidate' >&2; exit 1
  fi
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

# Prose stays outside automatic version editing.
cmp "$fixture/original-readme" "$fixture/README.md"

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
cmp "$fixture/original.toml" "$fixture/Cargo.toml"
cmp "$fixture/before.json" "$fixture/ci/dependency-pinning-exceptions.json"
cmp "$fixture/original-readme" "$fixture/README.md"
[[ -n "$(find "$fixture/retained" -type f -name metadata.json -print)" ]]
echo 'release pin exceptions follow preparation and admission; oracle, reasons and external lock selections preserved'
fixture_complete=true
