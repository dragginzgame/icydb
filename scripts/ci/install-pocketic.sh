#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ "$#" -ne 0 ]]; then
  echo "usage: install-pocketic.sh" >&2
  exit 2
fi

if [[ "$(uname -s):$(uname -m)" != "Linux:x86_64" ]] &&
   [[ "$(uname -s):$(uname -m)" != "Linux:amd64" ]]; then
  echo "unsupported PocketIC platform: $(uname -s) $(uname -m)" >&2
  exit 1
fi

pocket_ic_version="$(
  awk '
    /^\[\[package\]\]$/ { package_name = "" }
    /^name = "pocket-ic"$/ { package_name = "pocket-ic" }
    package_name == "pocket-ic" && /^version = / {
      gsub(/"/, "", $3)
      print $3
      exit
    }
  ' "$ROOT/Cargo.lock"
)"
if [[ -z "$pocket_ic_version" ]]; then
  echo "Cargo.lock does not contain a PocketIC package version" >&2
  exit 1
fi

cache_root="${RUNNER_TEMP:-${TMPDIR:-$ROOT/.cache}}"
install_path="${POCKET_IC_BIN:-$cache_root/pocket-ic-server-$pocket_ic_version/pocket-ic}"
expected_version="pocket-ic-server $pocket_ic_version"

if [[ -f "$install_path" ]]; then
  chmod +x "$install_path"
  actual_version="$($install_path --version 2>/dev/null || true)"
  if [[ "$actual_version" != "$expected_version" ]]; then
    echo "PocketIC binary at $install_path reports '$actual_version', expected '$expected_version'" >&2
    exit 1
  fi
  printf '%s\n' "$install_path"
  exit 0
fi

mkdir -p "$cache_root" "$(dirname "$install_path")"
scratch="$(mktemp -d "$cache_root/icydb-pocketic-install.XXXXXX")"
archive="$scratch/pocket-ic.gz"
candidate="$scratch/pocket-ic"
cleanup() {
  rm -f "$archive" "$candidate"
  rmdir "$scratch" 2>/dev/null || true
}
trap cleanup EXIT

curl \
  --fail \
  --location \
  --show-error \
  --silent \
  --retry 5 \
  --retry-all-errors \
  --retry-delay 2 \
  --connect-timeout 15 \
  --max-time 180 \
  --output "$archive" \
  "https://github.com/dfinity/pocketic/releases/download/$pocket_ic_version/pocket-ic-x86_64-linux.gz"
gzip --decompress --stdout "$archive" >"$candidate"
chmod +x "$candidate"

actual_version="$($candidate --version)"
if [[ "$actual_version" != "$expected_version" ]]; then
  echo "downloaded PocketIC reports '$actual_version', expected '$expected_version'" >&2
  exit 1
fi

mv "$candidate" "$install_path"
printf '%s\n' "$install_path"
