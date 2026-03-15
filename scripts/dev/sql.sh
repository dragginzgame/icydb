#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  sql.sh [--canister NAME] [--method NAME] [--deploy] [--reset] [--init] "SELECT ..."
  sql.sh [--canister NAME] [--deploy] [--reset] [--init]

Examples:
  sql.sh "select name, charisma from character order by charisma desc"
  sql.sh --canister sql_test "select count(*) from character"
  sql.sh --deploy
  sql.sh --reset
  sql.sh --init
  sql.sh --init "select count(*) from character"  # deploy + reset + load

Environment:
  SQLQ_CANISTER  Default canister name (default: sql_test)
  SQLQ_METHOD    Default method name (default: query)

Flags:
  --deploy  Deploy canister only.
  --reset   Destructive: erase all fixtures, then load default fixtures.
  --init    Convenience: equivalent to --deploy --reset.
USAGE
}

canister="${SQLQ_CANISTER:-sql_test}"
method="${SQLQ_METHOD:-query}"
deploy_requested=false
reset_requested=false
init_requested=false

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
        --init)
            init_requested=true
            shift
            ;;
        --deploy)
            deploy_requested=true
            shift
            ;;
        --reset)
            reset_requested=true
            shift
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

if [[ "$init_requested" == true ]]; then
    deploy_requested=true
    reset_requested=true
fi

if [[ "$deploy_requested" == true ]]; then
    echo "[sql.sh] deploying canister '$canister'" >&2
    dfx deploy "$canister"
fi

if [[ "$reset_requested" == true ]]; then
    echo "[sql.sh] resetting fixtures on '$canister' (erase + load default)" >&2
    dfx canister call "$canister" fixtures_reset "()"
    dfx canister call "$canister" fixtures_load_default "()"
fi

if [[ $# -eq 0 ]]; then
    if [[ "$deploy_requested" == true || "$reset_requested" == true ]]; then
        exit 0
    fi

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
