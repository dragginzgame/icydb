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
# shellcheck source=/dev/null
source "$ROOT/ci/tool-versions.env"
# shellcheck source=/dev/null
source "$ROOT/ci/icydb-tools.env"
ACTIONLINT_INSTALL_DIR="${ACTIONLINT_INSTALL_DIR:-$HOME/.local/bin}"
export PATH="$ROOT/.tools/host/bin:$ROOT/.tools/ic/bin:${CARGO_HOME:-$HOME/.cargo}/bin:$HOME/.cargo/bin:$HOME/.local/bin:$ACTIONLINT_INSTALL_DIR:$PATH"
cd "$ROOT"

DEV_SYSTEM_PACKAGES=(
  build-essential
  cmake
  curl
  git
  tar
  xz-utils
  wget
  gzip
  libssl-dev
  pkg-config
  perl
  shellcheck
  bubblewrap
  wabt
  cloc
)

CARGO_WORKSTATION_TOOLS=(
  "candid-extractor@$ICYDB_CANDID_EXTRACTOR_VERSION"
  "twiggy@$ICYDB_TWIGGY_VERSION"
  "cargo-edit@$ICYDB_CARGO_EDIT_VERSION"
  "cargo-watch@$ICYDB_CARGO_WATCH_VERSION"
)

install_system_packages() {
  if [[ "$(uname -s)" == "Darwin" ]]; then
    if ! xcode-select -p >/dev/null 2>&1 || ! command -v brew >/dev/null 2>&1; then
      echo "Install Xcode Command Line Tools and Homebrew, then re-run this target." >&2
      exit 1
    fi
    brew install cmake curl git xz openssl@3 pkg-config perl shellcheck wabt cloc make
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
  local tool name version
  bash "$ROOT/scripts/ci/install-gh.sh"

  rustup toolchain install --target wasm32-unknown-unknown

  install_actionlint
  make --no-print-directory -C "$ROOT" install-tools

  cargo install cargo-sort --version "$SHARED_TOOLING_CARGO_SORT_VERSION" --locked
  cargo install cargo-sort-derives --version "$ICYDB_CARGO_SORT_DERIVES_VERSION" --locked

  for tool in "${CARGO_WORKSTATION_TOOLS[@]}"; do
    name="${tool%@*}"
    version="${tool##*@}"
    if [[ "$MODE" == "update" ]]; then
      cargo install --quiet "$name" --version "$version" --locked
    else
      cargo install "$name" --version "$version" --locked
    fi
  done

  [[ "$(candid-extractor --version)" == "candid-extractor $ICYDB_CANDID_EXTRACTOR_VERSION" ]] || {
    echo "candid-extractor does not report the reviewed version $ICYDB_CANDID_EXTRACTOR_VERSION" >&2
    exit 1
  }
  make --no-print-directory -C "$ROOT" tools-check
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
