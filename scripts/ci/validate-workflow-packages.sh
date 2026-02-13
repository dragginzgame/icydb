#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
WORKFLOW_FILE="${1:-$ROOT/.github/workflows/ci.yml}"

if [[ ! -f "$WORKFLOW_FILE" ]]; then
  echo "[ERROR] Workflow file not found: $WORKFLOW_FILE" >&2
  exit 1
fi

mapfile -t WORKSPACE_PACKAGES < <(
  cargo metadata --no-deps --format-version=1 \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); ids={p["id"]:p["name"] for p in d["packages"]}; print("\n".join(sorted(ids[i] for i in d["workspace_members"])))'
)

if [[ ${#WORKSPACE_PACKAGES[@]} -eq 0 ]]; then
  echo "[ERROR] No workspace packages found via cargo metadata." >&2
  exit 1
fi

declare -A WORKSPACE_LOOKUP=()
for pkg in "${WORKSPACE_PACKAGES[@]}"; do
  WORKSPACE_LOOKUP["$pkg"]=1
done

mapfile -t REFERENCED_PACKAGES < <(
  perl -ne 'if (/cargo\s/) { while (/(?:^|\s)(?:-p|--package(?:=|\s+))([A-Za-z0-9_.-]+)/g) { print "$1\n" } }' \
    "$WORKFLOW_FILE" \
    | sort -u
)

if [[ ${#REFERENCED_PACKAGES[@]} -eq 0 ]]; then
  echo "[OK] No explicit cargo package flags found in $WORKFLOW_FILE."
  exit 0
fi

missing=0
for pkg in "${REFERENCED_PACKAGES[@]}"; do
  if [[ -z "${WORKSPACE_LOOKUP[$pkg]+x}" ]]; then
    if [[ $missing -eq 0 ]]; then
      echo "[ERROR] Workflow references package(s) not found in workspace:"
    fi
    echo "  - $pkg"
    missing=1
  fi
done

if [[ $missing -eq 1 ]]; then
  echo
  echo "Workspace packages:"
  for pkg in "${WORKSPACE_PACKAGES[@]}"; do
    echo "  - $pkg"
  done
  exit 1
fi

echo "[OK] Workflow package references are valid: ${REFERENCED_PACKAGES[*]}"
