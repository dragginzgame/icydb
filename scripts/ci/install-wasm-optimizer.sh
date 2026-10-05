#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/ci/wasm-optimizer-pin.sh
source "$ROOT/scripts/ci/wasm-optimizer-pin.sh"
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
      --proto '=https' --proto-redir '=https' --tlsv1.2 \
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

install_dir="${WASM_OPT_INSTALL_DIR:-$HOME/.local/bin}"
wasm_opt_bin="$install_dir/wasm-opt"

if [[ ! -x "$wasm_opt_bin" ]] ||
   ! bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$WASM_OPT_SHA256" "$wasm_opt_bin" >/dev/null 2>&1; then
  echo "[binaryen] installing official $BINARYEN_VERSION into $install_dir"
  mkdir -p "$install_dir"
  if [[ -n "${TMPDIR:-}" ]]; then
    mkdir -p "$TMPDIR"
  fi
  scratch="$(mktemp -d "${TMPDIR:-/tmp}/icydb-binaryen-install.XXXXXX")"
  archive="$scratch/$ARCHIVE_NAME"
  candidate="$scratch/wasm-opt"
  cleanup() {
    rm -f "$archive" "$candidate"
    rmdir "$scratch" 2>/dev/null || true
  }
  trap cleanup EXIT

  curl \
    --proto '=https' --proto-redir '=https' --tlsv1.2 \
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

  bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$ARCHIVE_SHA256" "$archive"

  tar \
    --extract \
    --gzip \
    --file "$archive" \
    --directory "$scratch" \
    --strip-components 2 \
    "binaryen-$BINARYEN_VERSION/bin/wasm-opt"

  bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$WASM_OPT_SHA256" "$candidate"

  chmod +x "$candidate"
  if [[ "$("$candidate" --version)" != "$WASM_OPT_VERSION" ]]; then
    echo "Binaryen candidate does not report the pinned version" >&2
    exit 1
  fi
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
