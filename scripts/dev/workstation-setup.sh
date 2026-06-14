#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: scripts/dev/workstation-setup.sh install|update" >&2
}

MODE="${1:-}"
case "$MODE" in
  install|update) ;;
  *)
    usage
    exit 2
    ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ACTIONLINT_VERSION="${ACTIONLINT_VERSION:-1.7.12}"
ACTIONLINT_INSTALL_DIR="${ACTIONLINT_INSTALL_DIR:-$HOME/.local/bin}"

DEV_SYSTEM_PACKAGES=(
  build-essential
  cmake
  curl
  wget
  gzip
  libssl-dev
  pkg-config
  ripgrep
  nodejs
  npm
  bubblewrap
  binaryen
  wabt
  jq
)

CARGO_WORKSTATION_TOOLS=(
  candid-extractor
  ic-wasm
  twiggy
  cargo-audit
  cargo-bloat
  cargo-deny
  cargo-edit
  cargo-expand
  cargo-get
  cargo-machete
  cargo-llvm-lines
  cargo-sort
  cargo-tarpaulin
  cargo-sort-derives
)

NPM_WORKSTATION_TOOLS=(
  @icp-sdk/icp-cli
  @icp-sdk/ic-wasm
)

install_system_packages() {
  if ! command -v apt-get >/dev/null 2>&1; then
    echo "apt-get not found. Install these packages manually, then re-run this target:" >&2
    echo "  ${DEV_SYSTEM_PACKAGES[*]}" >&2
    exit 1
  fi

  local sudo_cmd=()
  if [[ "$(id -u)" -ne 0 ]]; then
    if ! command -v sudo >/dev/null 2>&1; then
      echo "Missing sudo. Install these packages manually, then re-run this target:" >&2
      echo "  ${DEV_SYSTEM_PACKAGES[*]}" >&2
      exit 1
    fi
    sudo_cmd=(sudo)
  fi

  "${sudo_cmd[@]}" apt-get update
  "${sudo_cmd[@]}" apt-get install -y "${DEV_SYSTEM_PACKAGES[@]}"
}

install_actionlint() {
  local bin

  bin="$(ACTIONLINT_INSTALL_DIR="$ACTIONLINT_INSTALL_DIR" bash "$ROOT/scripts/ci/install-actionlint.sh" "$ACTIONLINT_VERSION")"
  "$bin" -version
}

ensure_rustup() {
  if command -v rustup >/dev/null 2>&1 || [[ -x "$HOME/.cargo/bin/rustup" ]]; then
    return
  fi

  local rustup_installer
  local tmp_dir

  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' EXIT
  rustup_installer="$tmp_dir/rustup-init.sh"
  curl \
    --proto '=https' \
    --tlsv1.2 \
    --fail \
    --location \
    --show-error \
    --silent \
    --retry 5 \
    --retry-all-errors \
    --retry-delay 2 \
    --connect-timeout 15 \
    --max-time 120 \
    --output "$rustup_installer" \
    https://sh.rustup.rs
  sh "$rustup_installer" -y
  trap - EXIT
  rm -rf "$tmp_dir"
}

install_tooling() {
  export PATH="$ACTIONLINT_INSTALL_DIR:$HOME/.cargo/bin:$HOME/.local/bin:$PATH"

  if [[ "$MODE" == "update" ]]; then
    command -v rustup >/dev/null 2>&1 || {
      echo "Missing rustup after workstation setup." >&2
      exit 1
    }
    command -v cargo >/dev/null 2>&1 || {
      echo "Missing cargo after workstation setup." >&2
      exit 1
    }
  fi

  (
    cd "$ROOT"
    rustup toolchain install --target wasm32-unknown-unknown
  )

  install_actionlint

  if [[ "$MODE" == "update" ]]; then
    cargo install --quiet "${CARGO_WORKSTATION_TOOLS[@]}" --locked
  else
    cargo install "${CARGO_WORKSTATION_TOOLS[@]}" --locked
  fi

  npm install -g --prefix "$HOME/.local" "${NPM_WORKSTATION_TOOLS[@]}"
  icp --version
  ic-wasm --version
}

install_hooks() {
  if [[ -d "$ROOT/.git" ]]; then
    git -C "$ROOT" config --local core.hooksPath .githooks || true
    chmod +x "$ROOT"/.githooks/* 2>/dev/null || true
    echo "Git hooks configured (core.hooksPath -> .githooks)"
  else
    echo "Not a git repo, skipping hooks setup"
  fi
}

run_update_checks() {
  export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
  export CARGO_HOME="${CARGO_HOME:-$(make --no-print-directory -s -C "$ROOT" print-cargo-home)}"
  export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(make --no-print-directory -s -C "$ROOT" print-cargo-target-dir)}"

  cargo audit
  cargo update --quiet
}

if [[ "$MODE" == "install" ]]; then
  install_system_packages
fi
ensure_rustup
install_tooling

if [[ "$MODE" == "install" ]]; then
  install_hooks
  echo "Local developer dependencies and git hooks installed"
else
  run_update_checks
fi
