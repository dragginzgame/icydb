#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  sql.sh [--canister NAME] [--deploy] [--reset] [--init] "SELECT ..."
  sql.sh [--canister NAME] [--deploy] [--reset] [--init]

Examples:
  sql.sh
  sql.sh "select name, charisma from character order by charisma desc"
  sql.sh "select species, count(*) from character group by species order by species asc"
  sql.sh "explain select count(*) from character"
  sql.sh "describe character"
  sql.sh "show entities"
  sql.sh "show indexes character"
  sql.sh "show columns character"
  sql.sh --canister demo_rpg "select count(*) from character"
  sql.sh --deploy
  sql.sh --reset
  sql.sh --init
  sql.sh --init "select count(*) from character"  # deploy + reset + load

Environment:
  SQLQ_CANISTER  Default canister name (default: demo_rpg)
  SQLQ_HISTORY_FILE  Interactive history path (default: .cache/sql_history)

Flags:
  --deploy  Deploy canister only.
  --reset   Destructive: erase all fixtures, then load default fixtures.
  --init    Convenience: equivalent to --deploy --reset.

With no SQL argument, `sql.sh` starts an interactive readline shell with
history and arrow-key navigation through the Rust SQL shell binary.
USAGE
}

canister="${SQLQ_CANISTER:-demo_rpg}"
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
    exec cargo run --quiet -p icydb --bin sql_shell --features sql-shell -- --canister "$canister"
fi

exec cargo run --quiet -p icydb --bin sql_shell --features sql-shell -- --canister "$canister" --sql "$*"
