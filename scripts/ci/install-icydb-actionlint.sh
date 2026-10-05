#!/usr/bin/env bash
set -euo pipefail

# This file owns consumer pin selection; the immutable shared installer owns
# download, digest verification, version verification and installation.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PINS="$ROOT/scripts/ci/actionlint-checksums.tsv"
INSTALL_DIR="${ACTIONLINT_INSTALL_DIR:-$HOME/.local/bin}"

if [[ $# -ne 0 ]]; then
  echo "usage: install-icydb-actionlint.sh" >&2
  exit 2
fi

case "$(uname -s):$(uname -m)" in
  Linux:x86_64 | Linux:amd64) platform=linux_amd64 ;;
  Linux:arm64 | Linux:aarch64) platform=linux_arm64 ;;
  Darwin:x86_64 | Darwin:amd64) platform=darwin_amd64 ;;
  Darwin:arm64 | Darwin:aarch64) platform=darwin_arm64 ;;
  *)
    echo "unsupported actionlint platform" >&2
    exit 1
    ;;
esac

version="$(awk '$1 == "version" {print $2}' "$PINS")"
asset="actionlint_${version}_${platform}.tar.gz"
digest="$(awk -v asset="$asset" '$2 == asset {print $1}' "$PINS")"
if [[ -z "$digest" ]]; then
  echo "actionlint artifact has no admitted digest: $asset" >&2
  exit 1
fi

exec bash "$ROOT/scripts/ci/install-actionlint.sh" \
  --version "$version" --sha256 "$digest" --install-dir "$INSTALL_DIR"
