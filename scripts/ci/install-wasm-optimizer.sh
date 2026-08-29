#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BINARYEN_VERSION="version_132"
ARCHIVE_NAME="binaryen-$BINARYEN_VERSION-x86_64-linux.tar.gz"
ARCHIVE_SHA256="195ddc94f9bc89f45abdabb0b9eea86023d727ba90eac8b35b80f2544fc30572"
WASM_OPT_SHA256="1014958e6f20d412f1542320b43970214b0fb1ed780595e8f7c0d8761ed53725"
check_latest=0

case "$#" in
  0) ;;
  1)
    if [[ "$1" != "--check-latest" ]]; then
      echo "usage: install-wasm-optimizer.sh [--check-latest]" >&2
      exit 2
    fi
    check_latest=1
    ;;
  *)
    echo "usage: install-wasm-optimizer.sh [--check-latest]" >&2
    exit 2
    ;;
esac

report_latest_binaryen_release() {
  local latest_url
  local latest_version

  if ! latest_url="$(
    curl \
      --fail \
      --location \
      --show-error \
      --silent \
      --retry 3 \
      --retry-all-errors \
      --retry-delay 2 \
      --connect-timeout 15 \
      --max-time 60 \
      --output /dev/null \
      --write-out '%{url_effective}' \
      https://github.com/WebAssembly/binaryen/releases/latest
  )"; then
    echo "[WARN] unable to check the latest official Binaryen release" >&2
    return
  fi

  latest_version="${latest_url##*/}"
  if [[ ! "$latest_version" =~ ^version_[0-9]+$ ]]; then
    echo "[WARN] unexpected latest Binaryen release URL: $latest_url" >&2
    return
  fi
  if [[ "$latest_version" != "$BINARYEN_VERSION" ]]; then
    echo "[WARN] Binaryen pin $BINARYEN_VERSION differs from latest official release $latest_version" >&2
    return
  fi

  echo "[OK] Binaryen pin matches the latest official release: $BINARYEN_VERSION"
}

if [[ "$(uname -s):$(uname -m)" != "Linux:x86_64" ]] &&
   [[ "$(uname -s):$(uname -m)" != "Linux:amd64" ]]; then
  echo "unsupported Binaryen platform: $(uname -s) $(uname -m)" >&2
  exit 1
fi

install_dir="${WASM_OPT_INSTALL_DIR:-$HOME/.local/bin}"
wasm_opt_bin="$install_dir/wasm-opt"

if [[ ! -x "$wasm_opt_bin" ]] ||
   [[ "$(sha256sum "$wasm_opt_bin" | cut -d ' ' -f 1)" != "$WASM_OPT_SHA256" ]]; then
  echo "[binaryen] installing official $BINARYEN_VERSION into $install_dir"
  mkdir -p "$install_dir"
  scratch="$(mktemp -d "${TMPDIR:-/tmp}/icydb-binaryen-install.XXXXXX")"
  archive="$scratch/$ARCHIVE_NAME"
  candidate="$scratch/wasm-opt"
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
    --max-time 300 \
    --output "$archive" \
    "https://github.com/WebAssembly/binaryen/releases/download/$BINARYEN_VERSION/$ARCHIVE_NAME"

  observed_archive_sha256="$(sha256sum "$archive" | cut -d ' ' -f 1)"
  if [[ "$observed_archive_sha256" != "$ARCHIVE_SHA256" ]]; then
    echo "unexpected Binaryen archive SHA-256 '$observed_archive_sha256'; expected '$ARCHIVE_SHA256'" >&2
    exit 1
  fi

  tar \
    --extract \
    --gzip \
    --file "$archive" \
    --directory "$scratch" \
    --strip-components 2 \
    "binaryen-$BINARYEN_VERSION/bin/wasm-opt"

  observed_wasm_opt_sha256="$(sha256sum "$candidate" | cut -d ' ' -f 1)"
  if [[ "$observed_wasm_opt_sha256" != "$WASM_OPT_SHA256" ]]; then
    echo "unexpected wasm optimizer SHA-256 '$observed_wasm_opt_sha256'; expected '$WASM_OPT_SHA256'" >&2
    exit 1
  fi

  chmod +x "$candidate"
  mv "$candidate" "$wasm_opt_bin"
else
  echo "[binaryen] reusing verified $BINARYEN_VERSION at $wasm_opt_bin"
fi

export PATH="$install_dir:$PATH"
if [[ -n "${GITHUB_PATH:-}" ]]; then
  printf '%s\n' "$install_dir" >> "$GITHUB_PATH"
fi
bash "$ROOT/scripts/ci/verify-wasm-optimizer.sh"
if [[ "$check_latest" -eq 1 ]]; then
  report_latest_binaryen_release
fi
