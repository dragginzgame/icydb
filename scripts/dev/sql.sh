#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  sql.sh [--canister NAME] [--deploy] [--reinstall] [--upgrade] [--reset] [--init] "SELECT ..."
  sql.sh [--canister NAME] [--deploy] [--reinstall] [--upgrade] [--reset] [--init]

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
  sql.sh --reinstall
  sql.sh --upgrade
  sql.sh --reset
  sql.sh --init
  sql.sh --init "select count(*) from character"  # deploy + reset + load

Environment:
  SQLQ_CANISTER  Default canister name (default: demo_rpg)
  SQLQ_HISTORY_FILE  Interactive history path (default: .cache/sql_history)

Flags:
  --deploy   Deploy canister with dfx deploy, preserving stable memory on existing installs.
  --reinstall
             Destructive: deploy with reinstall mode when the canister already exists.
  --upgrade  Build, then upgrade the existing canister without resetting data.
  --reset    Destructive: erase all fixtures, then load default fixtures.
  --init     Convenience: equivalent to --reinstall --reset.

Schema-change rejection test:
  1. Run: sql.sh --init "describe character"
  2. Edit the generated Character schema.
  3. Run: sql.sh --upgrade
  4. Run: sql.sh "describe character"

The upgrade path preserves stable memory, so schema reconciliation can reject
the changed generated schema against the previously accepted schema snapshot.

With no SQL argument, `sql.sh` starts an interactive readline shell with
history and arrow-key navigation through the Rust SQL shell binary.
USAGE
}

canister="${SQLQ_CANISTER:-demo_rpg}"
deploy_requested=false
reinstall_requested=false
upgrade_requested=false
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
        --reinstall)
            reinstall_requested=true
            shift
            ;;
        --upgrade)
            upgrade_requested=true
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
    reinstall_requested=true
    reset_requested=true
fi

selected_install_modes=0
if [[ "$deploy_requested" == true ]]; then
    selected_install_modes=$((selected_install_modes + 1))
fi
if [[ "$reinstall_requested" == true ]]; then
    selected_install_modes=$((selected_install_modes + 1))
fi
if [[ "$upgrade_requested" == true ]]; then
    selected_install_modes=$((selected_install_modes + 1))
fi

if [[ "$selected_install_modes" -gt 1 ]]; then
    echo "error: --deploy, --reinstall, and --upgrade are mutually exclusive" >&2
    exit 2
fi

if [[ "$deploy_requested" == true ]]; then
    echo "[sql.sh] deploying canister '$canister'" >&2
    dfx deploy "$canister"
fi

if [[ "$reinstall_requested" == true ]]; then
    echo "[sql.sh] reinstalling canister '$canister' when already installed" >&2
    if dfx canister status "$canister" >/dev/null 2>&1; then
        dfx deploy "$canister" --mode reinstall --yes
    else
        dfx deploy "$canister"
    fi
fi

if [[ "$upgrade_requested" == true ]]; then
    wasm_path=".dfx/local/canisters/$canister/$canister.wasm"
    echo "[sql.sh] building canister '$canister' for stable-memory-preserving upgrade" >&2
    dfx build "$canister"

    if [[ ! -f "$wasm_path" ]]; then
        echo "error: expected wasm not found after build: $wasm_path" >&2
        exit 1
    fi

    echo "[sql.sh] upgrading canister '$canister' without fixture reset" >&2
    dfx canister install "$canister" --mode upgrade --wasm "$wasm_path"
fi

if [[ "$reset_requested" == true ]]; then
    echo "[sql.sh] resetting fixtures on '$canister' (erase + load default)" >&2
    dfx canister call "$canister" fixtures_reset "()"
    dfx canister call "$canister" fixtures_load_default "()"
fi

if [[ $# -eq 0 ]]; then
    exec cargo run --quiet -p icydb-cli -- --canister "$canister"
fi

exec cargo run --quiet -p icydb-cli -- --canister "$canister" --sql "$*"
