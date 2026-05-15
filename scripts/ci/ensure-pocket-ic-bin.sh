#!/usr/bin/env bash
set -euo pipefail

# Resolve the PocketIC server version from Cargo.lock unless explicitly set.
resolve_version_from_lock() {
  awk '
    $0 == "[[package]]" {
      in_pkg = 1
      is_target = 0
      next
    }
    in_pkg && $0 == "name = \"pocket-ic\"" {
      is_target = 1
      next
    }
    in_pkg && is_target && $1 == "version" && $2 == "=" {
      gsub(/"/, "", $3)
      print $3
      exit
    }
  ' Cargo.lock
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
    return 0
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
    return 0
  fi

  return 1
}

verify_sha256() {
  local path="$1"
  local actual

  if [[ -z "${POCKET_IC_SERVER_SHA256:-}" ]]; then
    return 0
  fi

  if ! actual="$(sha256_file "${path}")"; then
    echo "POCKET_IC_SERVER_SHA256 is set, but no SHA-256 tool was found." >&2
    exit 1
  fi

  if [[ "${actual}" != "${POCKET_IC_SERVER_SHA256}" ]]; then
    echo "PocketIC binary checksum mismatch for ${path}" >&2
    echo "expected: ${POCKET_IC_SERVER_SHA256}" >&2
    echo "actual:   ${actual}" >&2
    exit 1
  fi
}

if [[ -n "${POCKET_IC_BIN:-}" ]]; then
  if [[ ! -x "${POCKET_IC_BIN}" ]]; then
    echo "POCKET_IC_BIN is set to '${POCKET_IC_BIN}', but it is not executable." >&2
    exit 1
  fi
  verify_sha256 "${POCKET_IC_BIN}"
  printf '%s\n' "${POCKET_IC_BIN}"
  exit 0
fi

server_version="${POCKET_IC_SERVER_VERSION:-}"
if [[ -z "${server_version}" ]]; then
  server_version="$(resolve_version_from_lock)"
fi
if [[ -z "${server_version}" ]]; then
  echo "failed to resolve pocket-ic version; set POCKET_IC_SERVER_VERSION explicitly" >&2
  exit 1
fi

default_tmp_bin="${TMPDIR:-/tmp}/pocket-ic-server-${server_version}/pocket-ic"
if [[ -x "${default_tmp_bin}" ]]; then
  verify_sha256 "${default_tmp_bin}"
  printf '%s\n' "${default_tmp_bin}"
  exit 0
fi

os="$(uname -s)"
arch="$(uname -m)"

case "${os}" in
  Linux) os="linux" ;;
  Darwin) os="darwin" ;;
  *)
    echo "unsupported OS '${os}' for PocketIC binary download" >&2
    exit 1
    ;;
esac

case "${arch}" in
  x86_64|amd64) arch="x86_64" ;;
  arm64|aarch64) arch="arm64" ;;
  *)
    echo "unsupported architecture '${arch}' for PocketIC binary download" >&2
    exit 1
    ;;
esac

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
bin_dir="${root_dir}/.cache/pocket-ic-server-${server_version}"
bin_path="${bin_dir}/pocket-ic"
url="https://github.com/dfinity/pocketic/releases/download/${server_version}/pocket-ic-${arch}-${os}.gz"

if [[ ! -x "${bin_path}" ]]; then
  if [[ "${ICYDB_ALLOW_POCKET_IC_DOWNLOAD:-0}" != "1" ]]; then
    echo "PocketIC server ${server_version} is not cached at ${bin_path}." >&2
    echo "Set POCKET_IC_BIN to a trusted executable, or set ICYDB_ALLOW_POCKET_IC_DOWNLOAD=1 to download from:" >&2
    echo "${url}" >&2
    echo "Set POCKET_IC_SERVER_SHA256 to verify the downloaded binary when you have a trusted digest." >&2
    exit 1
  fi
  if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required to download PocketIC server binaries." >&2
    exit 1
  fi
  if ! command -v gzip >/dev/null 2>&1; then
    echo "gzip is required to unpack PocketIC server binaries." >&2
    exit 1
  fi
  mkdir -p "${bin_dir}"
  tmp_gz="${bin_path}.download.gz"
  trap 'rm -f "${tmp_gz}"' EXIT
  echo "downloading PocketIC server ${server_version} from ${url}" >&2
  curl -fL --retry 3 --retry-delay 1 --retry-connrefused "${url}" -o "${tmp_gz}"
  gzip -dc "${tmp_gz}" > "${bin_path}"
  verify_sha256 "${bin_path}"
  chmod +x "${bin_path}"
  rm -f "${tmp_gz}"
  trap - EXIT
fi

if [[ ! -x "${bin_path}" ]]; then
  echo "expected executable PocketIC binary at ${bin_path}" >&2
  exit 1
fi

verify_sha256 "${bin_path}"
printf '%s\n' "${bin_path}"
