#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: scripts/dev/workstation-setup.sh install|update" >&2
}

MODE="${1:-}"
if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi
case "$MODE" in
  install|update) ;;
  *)
    usage
    exit 2
    ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ACTIONLINT_INSTALL_DIR="${ACTIONLINT_INSTALL_DIR:-$HOME/.local/bin}"
# npm owns ic-wasm; prefer its user-local binary over any older Cargo install.
export PATH="$HOME/.local/bin:$ACTIONLINT_INSTALL_DIR:${CARGO_HOME:-$HOME/.cargo}/bin:$HOME/.cargo/bin:$PATH"
cd "$ROOT"

DEV_SYSTEM_PACKAGES=(
  build-essential
  cmake
  curl
  wget
  gzip
  libssl-dev
  pkg-config
  perl
  ripgrep
  shellcheck
  nodejs
  npm
  bubblewrap
  wabt
  jq
  cloc
)

CARGO_WORKSTATION_TOOLS=(
  candid-extractor
  twiggy
  cargo-edit
  cargo-get
  cargo-sort
  cargo-sort-derives
  cargo-watch
)

NPM_WORKSTATION_TOOLS=(
  @icp-sdk/icp-cli
  @icp-sdk/ic-wasm
)

install_system_packages() {
  if [[ "$(uname -s)" == "Darwin" ]]; then
    if ! xcode-select -p >/dev/null 2>&1 || ! command -v brew >/dev/null 2>&1; then
      echo "Install Xcode Command Line Tools and Homebrew, then re-run this target." >&2
      exit 1
    fi
    brew install cmake curl openssl@3 pkg-config perl ripgrep shellcheck node wabt jq cloc make
    return
  fi

  if ! command -v apt-get >/dev/null 2>&1; then
    echo "apt-get not found. Install these packages manually, then re-run this target:" >&2
    echo "  ${DEV_SYSTEM_PACKAGES[*]}" >&2
    exit 1
  fi

  local privilege_cmd="env"
  if [[ "$(id -u)" -ne 0 ]]; then
    if ! command -v sudo >/dev/null 2>&1; then
      echo "Missing sudo. Install these packages manually, then re-run this target:" >&2
      echo "  ${DEV_SYSTEM_PACKAGES[*]}" >&2
      exit 1
    fi
    privilege_cmd=sudo
  fi

  "$privilege_cmd" apt-get update
  "$privilege_cmd" apt-get install -y "${DEV_SYSTEM_PACKAGES[@]}"
}

install_actionlint() {
  local bin

  bin="$(ACTIONLINT_INSTALL_DIR="$ACTIONLINT_INSTALL_DIR" bash "$ROOT/scripts/ci/install-icydb-actionlint.sh")"
  "$bin" -version
}

ensure_rustup() {
  if ! command -v rustup >/dev/null 2>&1; then
    echo "Missing rustup. Install it using https://rustup.rs, then re-run this target." >&2
    exit 1
  fi
}

install_tooling() {
  bash "$ROOT/scripts/ci/install-gh.sh"

  rustup toolchain install --target wasm32-unknown-unknown

  install_actionlint

  if [[ "$MODE" == "update" ]]; then
    cargo install --quiet "${CARGO_WORKSTATION_TOOLS[@]}" --locked
  else
    cargo install "${CARGO_WORKSTATION_TOOLS[@]}" --locked
  fi

  npm install -g --prefix "$HOME/.local" "${NPM_WORKSTATION_TOOLS[@]}"
  if [[ "$MODE" == "update" ]]; then
    bash "$ROOT/scripts/ci/install-wasm-optimizer.sh" --check-latest
  else
    bash "$ROOT/scripts/ci/install-wasm-optimizer.sh"
  fi
  icp --version
  ic-wasm --version
}

install_repository_hook() {
  make --no-print-directory -C "$ROOT" install-hooks
}

if [[ "$MODE" == "install" ]]; then
  install_system_packages
fi
ensure_rustup
install_tooling
install_repository_hook

if [[ "$MODE" == "install" ]]; then
  echo "Local developer dependencies and formatting hook installed"
else
  echo "Local developer tooling and formatting hook updated; repository dependencies are unchanged"
fi
