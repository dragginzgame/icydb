#!/usr/bin/env bash
set -euo pipefail

# Exercise real release preparation with only Cargo/Git effects substituted.
# No commits, registry calls or repository version changes occur.
unset MAKEFLAGS MAKEOVERRIDES MFLAGS
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-release-pins.XXXXXX")"
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/scripts/ci" "$fixture/scripts/release" "$fixture/ci" "$fixture/bin"
cp "$ROOT/scripts/ci/"{bump-version.sh,next-release-version.sh,sync-release-surface-version.sh} "$fixture/scripts/ci/"
cp "$ROOT/scripts/release/pin-exceptions.jq" "$fixture/scripts/release/"
cp "$ROOT/ci/dependency-pinning-exceptions.json" "$fixture/ci/"
cp "$ROOT/Cargo.toml" "$fixture/"
previous="$(awk '/^\[workspace.package\]/ { section=1; next } /^\[/ { section=0 } section && $1=="version" { gsub(/"/,"",$3); print $3 }' "$fixture/Cargo.toml")"
release="$(bash "$ROOT/scripts/ci/next-release-version.sh" "$previous" patch)"
packages="$(jq '[.[] | select(.subject | startswith("icydb")) | .subject]' "$fixture/ci/dependency-pinning-exceptions.json")"
jq -n --arg version "$previous" --argjson names "$packages" \
  '{workspace_members:$names, packages:[$names[] | {id:.,name:.,version:$version}]}' > "$fixture/metadata.json"
printf 'version = 3\n' > "$fixture/Cargo.lock"
jq -r '.packages[] | "\n[[package]]\nname = \"\(.name)\"\nversion = \"\(.version)\""' "$fixture/metadata.json" >> "$fixture/Cargo.lock"
printf '\n[[package]]\nname = "external"\nversion = "%s"\nsource = "registry+fixture"\nchecksum = "fixed"\n' "$previous" >> "$fixture/Cargo.lock"
printf 'Current workspace version: `%s`\ntag = "v%s"\n' "$previous" "$previous" > "$fixture/README.md"
cp "$fixture/ci/dependency-pinning-exceptions.json" "$fixture/before.json"

cat > "$fixture/bin/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  get)
    awk '/^\[workspace.package\]/ { section=1; next } /^\[/ { section=0 } section && $1=="version" { gsub(/"/,"",$3); print $3 }' Cargo.toml
    ;;
  metadata) cat metadata.json ;;
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
chmod +x "$fixture/bin/"{cargo,git}
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
echo 'release pin exceptions follow preparation and admission; oracle, reasons and external lock selections preserved'
