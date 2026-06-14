#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:-${ACTIONLINT_VERSION:-1.7.12}}"
INSTALL_DIR="${ACTIONLINT_INSTALL_DIR:-$HOME/.local/bin}"

platform() {
  local os
  local arch

  case "$(uname -s)" in
    Linux) os="linux" ;;
    Darwin) os="darwin" ;;
    *)
      echo "unsupported actionlint platform: $(uname -s)" >&2
      exit 1
      ;;
  esac

  case "$(uname -m)" in
    x86_64 | amd64) arch="amd64" ;;
    arm64 | aarch64) arch="arm64" ;;
    *)
      echo "unsupported actionlint architecture: $(uname -m)" >&2
      exit 1
      ;;
  esac

  printf '%s_%s\n' "$os" "$arch"
}

main() {
  local version_no_v="${VERSION#v}"
  local archive="actionlint_${version_no_v}_$(platform).tar.gz"
  local url="https://github.com/rhysd/actionlint/releases/download/v${version_no_v}/${archive}"
  local archive_path
  local install_path="$INSTALL_DIR/actionlint"
  local installed_version
  local tmp_dir

  if [[ -x "$install_path" ]]; then
    installed_version="$("$install_path" -version 2>&1 | sed -n '1{s/[[:space:]].*//;p;}')"
    if [[ "$installed_version" == "$version_no_v" ]]; then
      printf '%s\n' "$install_path"
      return
    fi
  fi

  if [[ -n "${TMPDIR:-}" ]]; then
    mkdir -p "$TMPDIR"
  fi

  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' EXIT
  archive_path="$tmp_dir/$archive"
  mkdir -p "$INSTALL_DIR"
  curl \
    --fail \
    --location \
    --show-error \
    --silent \
    --retry 5 \
    --retry-all-errors \
    --retry-delay 2 \
    --connect-timeout 15 \
    --max-time 120 \
    --output "$archive_path" \
    "$url"

  tar -tzf "$archive_path" actionlint >/dev/null
  tar -xzf "$archive_path" -C "$tmp_dir" actionlint
  mv "$tmp_dir/actionlint" "$install_path"
  chmod +x "$install_path"
  trap - EXIT
  rm -rf "$tmp_dir"

  printf '%s\n' "$install_path"
}

main "$@"
