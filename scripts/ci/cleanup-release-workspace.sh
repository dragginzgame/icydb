#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RELEASE_TMP_DIR="$ROOT/.cache/release-tmp"
SQLITE_TMP_DIR="$ROOT/.cache/icydb-sqlite-comparison"
CACHE_DIR="$ROOT/.cache"

# Automatic and explicit release cleanup own only transient paths rooted in
# this workspace. Cargo build-cache deletion is always a separate manual action.
# Shared /tmp may contain live files from unrelated processes and must never be
# swept here.
mkdir -p "$RELEASE_TMP_DIR"

if [[ -d "$RELEASE_TMP_DIR" ]]; then
  find "$RELEASE_TMP_DIR" -depth -mindepth 1 -delete
fi

if [[ -d "$SQLITE_TMP_DIR" ]]; then
  find "$SQLITE_TMP_DIR" -depth -mindepth 1 -delete
fi

if [[ -d "$CACHE_DIR" ]]; then
  find "$CACHE_DIR" -maxdepth 1 -type f -name 'pocket_ic_*.port' -delete
fi
