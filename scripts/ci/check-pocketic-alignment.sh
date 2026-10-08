#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd -P)"
usage() {
    echo 'usage: check-pocketic-alignment.sh --manifest Cargo.toml --pins ic-tools.tsv [--bin EXECUTABLE --sha256 DIGEST]' >&2
}
manifest='' pins='' executable='' digest=''
while [[ $# -gt 0 ]]; do
    case "$1" in
        --manifest|--pins|--bin|--sha256)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            case "$1" in
                --manifest) manifest="$2" ;; --pins) pins="$2" ;;
                --bin) executable="$2" ;; --sha256) digest="$2" ;;
            esac
            shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done
[[ -n "$manifest" && -n "$pins" ]] || { usage; exit 2; }
if [[ -n "$executable" || -n "$digest" ]]; then
    [[ -n "$executable" && "$digest" =~ ^[0-9a-f]{64}$ ]] || { usage; exit 2; }
fi
[[ -f "$manifest" && ! -L "$manifest" && -f "$pins" ]] || {
    echo 'selected manifest and pin matrix must be files' >&2; exit 1;
}
server="$(awk -v tool=pocket-ic -f "$ROOT/scripts/ci/ic-tool-pins.awk" "$pins")" || {
    echo 'invalid or incomplete IC tool pin matrix' >&2; exit 1;
}
manifest="$(cd "$(dirname "$manifest")" && pwd -P)/$(basename "$manifest")"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/pocketic-alignment.XXXXXX")"
trap 'if [[ $? == 0 ]]; then rm -rf "$scratch"; else printf "PocketIC alignment evidence retained: %s\n" "$scratch" >&2; fi' EXIT
# Cargo owns TOML/lock validity, dependency selection and duplicate-key refusal.
# The selected workspace must already have its locked offline cache prepared.
(
    cd "$(dirname "$manifest")"
    CARGO_NET_OFFLINE=true RUSTUP_AUTO_INSTALL=0 cargo metadata \
        --locked --offline --format-version 1 --manifest-path "$manifest"
) > "$scratch/metadata.json" || exit 1
client="$(jq -ers '
    if length != 1 or .[0].version != 1 or (.[0].packages | type) != "array"
    then error("expected one Cargo metadata document") else .[0] end
    | [.packages[] | select(.name == "pocket-ic")]
    | if length != 1 then error("expected exactly one PocketIC client package") else .[0].version end
    | if type != "string" then error("missing PocketIC version")
      elif test("^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$") and (contains("\n") | not)
      then . else error("PocketIC client must select exact stable SemVer") end
' "$scratch/metadata.json")" || exit 1
[[ "$client" == "$server" ]] || {
    printf 'PocketIC exact alignment failed: locked client %s; server pin %s\n' "$client" "$server" >&2
    exit 1
}
if [[ -n "$executable" ]]; then
    bash "$ROOT/scripts/ci/check-pocketic-binary.sh" "$server" "$digest" "$executable"
fi
printf '%s\n' "$server"
