#!/usr/bin/env bash
set -euo pipefail

# Read only the selected TOML input. Callers own Git identity and mutation.
stable=false
if [[ "${1:-}" == --stable ]]; then stable=true; shift; fi
[[ $# == 1 && "$1" != -* ]] || {
    echo 'usage: read-cargo-workspace-version.sh [--stable] <manifest>' >&2; exit 2;
}
[[ -f "$1" && ! -L "$1" ]] || { echo 'manifest must be a regular file' >&2; exit 1; }
# yq projects TOML but does not reject duplicate keys. Cargo owns manifest
# validity; locate-project parses without resolving dependencies or building.
CARGO_NET_OFFLINE=true RUSTUP_AUTO_INSTALL=0 cargo locate-project --workspace \
    --message-format plain --manifest-path "$1" >/dev/null || exit 1
temporary="$(mktemp -d "${TMPDIR:-/tmp}/workspace-version.XXXXXX")"
trap 'rm -rf "$temporary"' EXIT
# Capture before projection: a failed parser must never emit an accepted version.
"${YQ:-yq}" -p toml -o json -I 0 '.' "$1" > "$temporary/manifest.json" || exit 1
jq -ers --argjson stable "$stable" '
  def number: "(0|[1-9][0-9]*)";
  def prerelease: "(0|[1-9][0-9]*|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*)";
  if length != 1 then error("expected one TOML document") else .[0] end
  | .workspace.package.version
  | if type != "string" then error("workspace.package.version must be a string") else . end
  | if test("^" + number + "\\." + number + "\\." + number +
      (if $stable then "" else "(-" + prerelease + "(\\." + prerelease + ")*)?(\\+[0-9A-Za-z-]+(\\.[0-9A-Za-z-]+)*)?" end) + "$")
      and (contains("\n") | not)
    then . else error("workspace.package.version must be canonical SemVer of the selected kind") end
' "$temporary/manifest.json"
