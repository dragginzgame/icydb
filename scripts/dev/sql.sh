#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  sql.sh [--canister NAME] [--deploy] [--reset] [--init] "SELECT ..."
  sql.sh [--canister NAME] [--deploy] [--reset] [--init]

Examples:
  sql.sh "select name, charisma from character order by charisma desc"
  sql.sh "describe character"
  sql.sh "show entities"
  sql.sh "show indexes character"
  sql.sh --canister sql_test "select count(*) from character"
  sql.sh --deploy
  sql.sh --reset
  sql.sh --init
  sql.sh --init "select count(*) from character"  # deploy + reset + load

Environment:
  SQLQ_CANISTER  Default canister name (default: sql_test)

Flags:
  --deploy  Deploy canister only.
  --reset   Destructive: erase all fixtures, then load default fixtures.
  --init    Convenience: equivalent to --deploy --reset.
USAGE
}

canister="${SQLQ_CANISTER:-sql_test}"
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

raw_json="$(dfx canister call "$canister" query "(\"$sql\")" --output json)"

# Print one readable text surface for successful SQL results, or one readable error.
printf '%s\n' "$raw_json" | jq -r '
  def projection_lines($p):
    [
      "surface=projection entity=\($p.entity) row_count=\($p.row_count)",
      ("columns: " + ($p.columns | join(", ")))
    ] + ($p.rows | map("row: " + join(" | ")));

  def describe_lines($d):
    [
      "entity: \($d.entity_name)",
      "path: \($d.entity_path)",
      "primary_key: \($d.primary_key)",
      "fields:"
    ]
    + ($d.fields | map(
        "  - \(.name): \(.kind) (primary_key=\(.primary_key), queryable=\(.queryable))"
      ))
    + (if ($d.indexes | length) == 0 then
         ["indexes: []"]
       else
         ["indexes:"]
         + ($d.indexes | map(
             "  - \(.name)(\(.fields | join(", ")))"
             + (if .unique then ", unique" else "" end)
           ))
       end)
    + (if ($d.relations | length) == 0 then
         ["relations: []"]
       else
         ["relations:"]
         + ($d.relations | map(
             "  - \(.field) -> \(.target_entity_name) (\(.strength), \(.cardinality))"
           ))
       end);

  def query_result_lines($ok):
    if ($ok | has("Projection")) then
      projection_lines($ok.Projection)
    elif ($ok | has("Explain")) then
      ["surface=explain"] + ($ok.Explain.explain | split("\n"))
    elif ($ok | has("Describe")) then
      describe_lines($ok.Describe)
    elif ($ok | has("ShowIndexes")) then
      ["surface=indexes entity=\($ok.ShowIndexes.entity) index_count=\($ok.ShowIndexes.indexes | length)"]
      + $ok.ShowIndexes.indexes
    elif ($ok | has("ShowEntities")) then
      ["surface=entities"] + ($ok.ShowEntities.entities | map("entity=\(.)"))
    else
      ["ERROR: unexpected query payload: " + ($ok | tostring)]
    end;

  first(.. | objects | select(has("Ok") or has("Err"))) as $r
  | if $r == null then
      "ERROR: unexpected response shape"
    elif ($r | has("Ok")) then
      query_result_lines($r.Ok)[]
    else
      "ERROR: " + ($r.Err | tostring)
    end
'
