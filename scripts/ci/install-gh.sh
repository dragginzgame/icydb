#!/usr/bin/env bash
set -euo pipefail

if command -v gh >/dev/null 2>&1; then
  gh --version
  exit 0
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  if ! command -v brew >/dev/null 2>&1; then
    echo "GitHub CLI is missing. Install Homebrew or gh manually, then retry." >&2
    exit 1
  fi
  brew install gh
  gh --version
  exit 0
fi

if ! command -v apt-get >/dev/null 2>&1; then
  echo "GitHub CLI is missing and apt-get is unavailable." >&2
  echo "Install gh with the platform package manager, then retry." >&2
  exit 1
fi

privilege_cmd="env"
if [[ "$(id -u)" -ne 0 ]]; then
  if ! command -v sudo >/dev/null 2>&1; then
    echo "GitHub CLI is missing and sudo is unavailable." >&2
    echo "Install gh with the platform package manager, then retry." >&2
    exit 1
  fi
  privilege_cmd=sudo
fi

"$privilege_cmd" apt-get update
"$privilege_cmd" apt-get install -y gh

if ! command -v gh >/dev/null 2>&1; then
  echo "GitHub CLI is still missing after apt installation." >&2
  exit 1
fi

gh --version
