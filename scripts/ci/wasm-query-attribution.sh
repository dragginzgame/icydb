#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
out_dir="$ROOT/artifacts/wasm-attribution"
export CARGO_HOME="${CARGO_HOME:-$(make --no-print-directory -s -C "$ROOT" print-cargo-home)}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(make --no-print-directory -s -C "$ROOT" print-cargo-target-dir)}"
canister_names=(
    one_entity_reachable_operations
    one_entity_typed_query
    one_entity_dynamic_query
    one_entity_sql_query
    ten_entity_reachable_operations
    ten_entity_typed_query
)

usage() {
    cat <<'EOF'
usage: wasm-query-attribution.sh [--canister name]

Builds symbol-bearing, non-deployable wasm for query attribution. Repeat
--canister to override the default typed/dynamic/SQL comparison set.
EOF
}

requested=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --canister)
            requested+=("${2:-}")
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "[wasm-attribution] unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [[ "${#requested[@]}" -gt 0 ]]; then
    canister_names=("${requested[@]}")
fi
for canister_name in "${canister_names[@]}"; do
    if [[ -z "$canister_name" ]]; then
        echo "[wasm-attribution] --canister requires a value" >&2
        exit 1
    fi
done

for required_tool in cargo ic-wasm twiggy; do
    if ! command -v "$required_tool" >/dev/null 2>&1; then
        echo "[wasm-attribution] missing required tool: $required_tool" >&2
        exit 1
    fi
done

mkdir -p "$out_dir"

for canister_name in "${canister_names[@]}"; do
    echo "[wasm-attribution] Building '$canister_name'"
    (
        cd "$ROOT"
        cargo run -p icydb-testing-integration --bin build_fixture_canister --locked -- \
            "$canister_name" \
            --build-profile production \
            --profile wasm-attribution \
            --sql-mode on \
            --candid-export off
    )

    staged_wasm="$ROOT/.icp/local/canisters/$canister_name/$canister_name.wasm"
    if [[ ! -f "$staged_wasm" ]]; then
        echo "[wasm-attribution] expected wasm missing: $staged_wasm" >&2
        exit 1
    fi

    raw_wasm="$out_dir/$canister_name.wasm-attribution.wasm"
    named_wasm="$out_dir/$canister_name.wasm-attribution.named-shrunk.wasm"
    cp "$staged_wasm" "$raw_wasm"
    ic-wasm "$raw_wasm" -o "$named_wasm" shrink --keep-name-section

    twiggy top -n 60 "$named_wasm" > "$out_dir/$canister_name.twiggy-top.txt"
    twiggy dominators -r 200 "$named_wasm" > "$out_dir/$canister_name.twiggy-dominators.txt"
    twiggy top --retained -n 60 -f csv "$named_wasm" \
        > "$out_dir/$canister_name.twiggy-retained.csv"
    twiggy monos "$named_wasm" > "$out_dir/$canister_name.twiggy-monos.txt"

    raw_bytes="$(wc -c < "$raw_wasm")"
    named_bytes="$(wc -c < "$named_wasm")"
    printf '%s\t%s\t%s\n' "$canister_name" "$raw_bytes" "$named_bytes"
done
