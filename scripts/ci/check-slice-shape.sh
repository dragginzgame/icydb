#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

SOFT_MAX_FILES=15
HARD_MAX_FILES=25
MAX_PRIMARY_DOMAINS=2
ROOT_MODULE_GROWTH_LIMIT=200

GUARDED_ROOTS=(
  "crates/icydb-core/src/db/sql/parser/mod.rs"
  "crates/icydb-core/src/db/session/sql/mod.rs"
)

resolve_pr_body() {
  if [[ -n "${GITHUB_EVENT_PATH:-}" ]] && command -v python3 >/dev/null 2>&1; then
    python3 - "$GITHUB_EVENT_PATH" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    payload = json.load(fh)

body = payload.get("pull_request", {}).get("body") or ""
sys.stdout.write(body)
PY
    return
  fi

  git log -1 --pretty=%B
}

resolve_base_and_head() {
  if [[ -n "${SLICE_BASE_REF:-}" && -n "${SLICE_HEAD_REF:-}" ]]; then
    printf '%s\n%s\n' "$SLICE_BASE_REF" "$SLICE_HEAD_REF"
    return
  fi

  if [[ -n "${GITHUB_EVENT_PATH:-}" ]] && command -v python3 >/dev/null 2>&1; then
    python3 - "$GITHUB_EVENT_PATH" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    payload = json.load(fh)

pr = payload.get("pull_request") or {}
base = (pr.get("base") or {}).get("sha")
head = (pr.get("head") or {}).get("sha")
if base and head:
    sys.stdout.write(f"{base}\n{head}\n")
PY
    return
  fi

  if git rev-parse --verify HEAD~1 >/dev/null 2>&1; then
    printf '%s\n%s\n' "HEAD~1" "HEAD"
    return
  fi

  echo "[ERROR] Unable to resolve a diff range for slice-shape analysis." >&2
  exit 1
}

classify_primary_domain() {
  local path=$1

  case "$path" in
    crates/icydb-core/src/db/sql/parser/*)
      printf '%s\n' "parser"
      ;;
    crates/icydb-core/src/db/sql/lowering/*|crates/icydb-core/src/db/session/sql/*|crates/icydb/src/db/session/*)
      printf '%s\n' "lowering-session"
      ;;
    crates/icydb-core/src/db/executor/*|crates/icydb-core/src/db/query/*|crates/icydb-core/src/db/access/*)
      printf '%s\n' "executor-planner"
      ;;
    crates/icydb-build/*|canisters/*|schema/*|crates/icydb-schema/*|crates/icydb/*)
      printf '%s\n' "build-canister"
      ;;
    testing/*|crates/*/tests/*)
      printf '%s\n' "integration-tests"
      ;;
    docs/*|*.md|Cargo.toml|Cargo.lock|.github/*)
      ;;
    crates/icydb-core/src/*)
      printf '%s\n' "other-core"
      ;;
    *)
      ;;
  esac
}

diff_refs="$(resolve_base_and_head)"
base_ref="$(printf '%s\n' "$diff_refs" | sed -n '1p')"
head_ref="$(printf '%s\n' "$diff_refs" | sed -n '2p')"

changed_files="$(
  git diff --name-only "$base_ref" "$head_ref" \
    | sed '/^$/d'
)"

if [[ -z "$changed_files" ]]; then
  echo "[OK] Slice shape gate skipped: no changed files in resolved diff range."
  exit 0
fi

file_count="$(printf '%s\n' "$changed_files" | awk 'NF { count += 1 } END { print count + 0 }')"

primary_domains="$(
  while IFS= read -r path; do
    classify_primary_domain "$path"
  done <<< "$changed_files" | sed '/^$/d' | sort -u
)"
primary_domain_count="$(printf '%s\n' "$primary_domains" | awk 'NF { count += 1 } END { print count + 0 }')"

pr_body="$(resolve_pr_body | tr -d '\r')"
slice_override="$(
  printf '%s\n' "$pr_body" \
    | awk -F': ' '/^Slice-Override:/ { print $2; exit }'
)"
slice_justification="$(
  printf '%s\n' "$pr_body" \
    | awk -F': ' '/^Slice-Justification:/ { print substr($0, index($0, ":") + 2); exit }'
)"

override_active=0
if [[ "$slice_override" == "yes" ]]; then
  override_active=1
fi

echo "Slice shape"
echo "  Base: $base_ref"
echo "  Head: $head_ref"
echo "  Files changed: $file_count"
echo "  Primary domains touched: $primary_domain_count"
printf '%s\n' "$primary_domains" | sed '/^$/d' | sed 's/^/    - /'

if (( override_active == 1 )); then
  if [[ -z "$slice_justification" ]]; then
    echo "[ERROR] Slice override is present but Slice-Justification is empty." >&2
    exit 1
  fi

  echo "  Slice override: yes"
  echo "  Slice justification: $slice_justification"
fi

growth_violation=0
for guarded_root in "${GUARDED_ROOTS[@]}"; do
  added_lines="$(
    git diff --numstat "$base_ref" "$head_ref" -- "$guarded_root" \
      | awk 'NF { if ($1 ~ /^[0-9]+$/) total += $1 } END { print total + 0 }'
  )"

  if (( added_lines > 0 )); then
    echo "  Guarded root growth: $guarded_root -> +$added_lines lines"
  fi

  if (( added_lines > ROOT_MODULE_GROWTH_LIMIT )); then
    growth_violation=1
    echo "[ERROR] Guarded root exceeded the growth limit: $guarded_root (+$added_lines)." >&2
  fi
done

if (( file_count > SOFT_MAX_FILES )); then
  echo "[WARN] Slice file count exceeded the soft target ($SOFT_MAX_FILES)." >&2
fi

needs_override=0
if (( file_count > HARD_MAX_FILES )); then
  needs_override=1
  echo "[WARN] Slice file count exceeded the hard limit ($HARD_MAX_FILES)." >&2
fi

if (( primary_domain_count > MAX_PRIMARY_DOMAINS )); then
  needs_override=1
  echo "[WARN] Slice touched more than $MAX_PRIMARY_DOMAINS primary domains." >&2
fi

if (( growth_violation == 1 )); then
  needs_override=1
fi

if (( needs_override == 1 && override_active == 0 )); then
  echo "[ERROR] Slice shape exceeded guarded limits without a PR override." >&2
  echo "[ERROR] Add these lines to the PR body:" >&2
  echo "[ERROR]   Slice-Override: yes" >&2
  echo "[ERROR]   Slice-Justification: <why the cross-layer change is unavoidable>" >&2
  exit 1
fi

echo "[OK] Slice shape gate passed."
