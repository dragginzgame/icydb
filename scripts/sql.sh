#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  scripts/sql.sh [--canister NAME] [--method NAME] "SELECT ..."

Examples:
  scripts/sql.sh "select name, charisma from character order by charisma desc"
  scripts/sql.sh --canister sql_test "select count(*) from character"

Environment:
  SQLQ_CANISTER  Default canister name (default: sql_test)
  SQLQ_METHOD    Default method name (default: query)
USAGE
}

canister="${SQLQ_CANISTER:-sql_test}"
method="${SQLQ_METHOD:-query}"

# Parse flags first, then treat remaining args as a single SQL string.
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        -c|--canister)
            if [[ $# -lt 2 ]]; then
                echo "error: --canister requires a value" >&2
                exit 2
            fi
            canister="$2"
            shift 2
            ;;
        -m|--method)
            if [[ $# -lt 2 ]]; then
                echo "error: --method requires a value" >&2
                exit 2
            fi
            method="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        -*)
            echo "error: unknown option: $1" >&2
            usage
            exit 2
            ;;
        *)
            break
            ;;
    esac
done

if [[ $# -eq 0 ]]; then
    usage
    exit 2
fi

sql="$*"

# Escape characters that would break the Candid string argument.
sql=${sql//\\/\\\\}
sql=${sql//\"/\\\"}

raw_json="$(dfx canister call "$canister" "$method" "(\"$sql\")" --output json)"

# Print rows for successful results, or a readable error for failed ones.
printf '%s\n' "$raw_json" | jq -r '
  first(.. | objects | select(has("Ok") or has("Err"))) as $r
  | if $r == null then
      "ERROR: unexpected response shape"
    elif ($r | has("Ok")) then
      $r.Ok[]
    else
      "ERROR: " + ($r.Err | tostring)
    end
'
