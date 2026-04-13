#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

OUTPUT_PATH="${1:-}"

COMMON_GLOBS=(
  --glob '!**/tests/**'
  --glob '!**/tests.rs'
  --glob '!**/*_tests.rs'
  --glob '!**/test_*.rs'
)

PARSER_ROOT="crates/icydb-core/src/db/sql/parser"
LOWERING_ROOT="crates/icydb-core/src/db/sql/lowering"
EXECUTION_ROOT="crates/icydb-core/src/db/session/sql"

run_rg() {
  local pattern=$1
  shift
  rg -n --no-heading --color=never "$pattern" "$@" "${COMMON_GLOBS[@]}" || true
}

strip_comment_only() {
  awk -F: '{
    code=$0
    sub(/^[^:]+:[0-9]+:/, "", code)
    if (code ~ /^[[:space:]]*\/\//) {
      next
    }
    print $0
  }'
}

layer_hit_count() {
  local pattern=$1
  local root=$2
  local hits
  hits="$(run_rg "$pattern" "$root" | strip_comment_only)"
  if [[ -n "$hits" ]]; then
    printf '%s\n' "$hits" | wc -l | tr -d ' '
  else
    echo 0
  fi
}

if [[ -n "$OUTPUT_PATH" ]]; then
  mkdir -p "$(dirname "$OUTPUT_PATH")"
  printf 'decision\tkind\tparser_hits\tlowering_hits\texecution_hits\tlayer_count\tstatus\n' >"$OUTPUT_PATH"
fi

recomputed_count=0
propagated_count=0

DECISIONS="$(cat <<'EOF'
grouped_surface_recomputation	recomputed	group_by\.|having\.|SqlSelectItem::Aggregate
insert_select_source_shape	recomputed	SQL INSERT SELECT requires scalar SELECT source|SQL INSERT SELECT does not support aggregate source projection
grouped_having_shape_validation	recomputed	UnsupportedSelectGroupBy|UnsupportedSelectHaving|grouped_projection_aggregates|projection_aggregates
projection_shape_selection	recomputed	ProjectionSelection::(All|Fields|Exprs)
grouped_surface_shape_propagation	propagated	LoweredSelectQueryShape::|select_shape\(|\.shape\(
EOF
)"

while IFS=$'\t' read -r decision kind pattern; do
  [[ -z "$decision" ]] && continue

  parser_hits="$(layer_hit_count "$pattern" "$PARSER_ROOT")"
  lowering_hits="$(layer_hit_count "$pattern" "$LOWERING_ROOT")"
  execution_hits="$(layer_hit_count "$pattern" "$EXECUTION_ROOT")"

  layer_count=0
  [[ "$parser_hits" != 0 ]] && layer_count=$((layer_count + 1))
  [[ "$lowering_hits" != 0 ]] && layer_count=$((layer_count + 1))
  [[ "$execution_hits" != 0 ]] && layer_count=$((layer_count + 1))

  semantic_layer_count=0
  [[ "$lowering_hits" != 0 ]] && semantic_layer_count=$((semantic_layer_count + 1))
  [[ "$execution_hits" != 0 ]] && semantic_layer_count=$((semantic_layer_count + 1))

  status="owned"
  if [[ "$kind" == "recomputed" && $semantic_layer_count -gt 1 ]]; then
    status="duplicate"
    recomputed_count=$((recomputed_count + 1))
  elif [[ "$kind" == "propagated" && $layer_count -gt 1 ]]; then
    status="propagated"
    propagated_count=$((propagated_count + 1))
  fi

  if [[ -n "$OUTPUT_PATH" ]]; then
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$decision" "$kind" "$parser_hits" "$lowering_hits" "$execution_hits" "$layer_count" "$status" \
      >>"$OUTPUT_PATH"
  fi
done <<<"$DECISIONS"

echo "recomputed_decision_count=$recomputed_count"
echo "propagated_decision_count=$propagated_count"
echo "tracked_decisions=5"
