#!/usr/bin/env bash
set -euo pipefail
# Read a consumer-owned data file: one supported delivery per line, no shell code.
# An empty path retains the common direct/pr selection. This check has no effects.
[[ $# == 2 ]] || { echo 'usage: check-release-delivery.sh POLICY-FILE-OR-EMPTY DELIVERY' >&2; exit 2; }
fail() { printf 'release delivery refused: %s\n' "$1" >&2; exit 2; }
direct=false pr=false
if [[ -z "$1" ]]; then
    direct=true pr=true
else
    [[ -f "$1" && -r "$1" && ! -L "$1" ]] || fail 'policy must be a readable regular file'
    while IFS= read -r mode || [[ -n "$mode" ]]; do
        case "$mode" in
            direct) [[ "$direct" == false ]] || fail 'duplicate direct policy'; direct=true ;;
            pr) [[ "$pr" == false ]] || fail 'duplicate pr policy'; pr=true ;;
            *) fail 'policy lines must be direct or pr' ;;
        esac
    done < "$1"
    [[ "$direct" == true || "$pr" == true ]] || fail 'policy is empty'
fi
case "$2" in
    direct) [[ "$direct" == true ]] || fail 'consumer does not support direct delivery' ;;
    pr) [[ "$pr" == true ]] || fail 'consumer does not support PR delivery' ;;
    *) fail 'delivery must be direct or pr' ;;
esac
