#!/usr/bin/env bash
set -euo pipefail

# Consumer selections adapt the canonical installer without changing its bytes.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/ci/tool-versions.env"
case "$(uname -s):$(uname -m)" in
  Linux:x86_64|Linux:amd64) digest="$ICYDB_YQ_SHA256_LINUX_AMD64" ;;
  Linux:aarch64|Linux:arm64) digest="$ICYDB_YQ_SHA256_LINUX_ARM64" ;;
  Darwin:x86_64|Darwin:amd64) digest="$ICYDB_YQ_SHA256_DARWIN_AMD64" ;;
  Darwin:arm64|Darwin:aarch64) digest="$ICYDB_YQ_SHA256_DARWIN_ARM64" ;;
  *) echo 'unsupported yq host' >&2; exit 1 ;;
esac
exec bash "$ROOT/scripts/ci/install-yq.sh" \
  --version "$ICYDB_YQ_VERSION" --sha256 "$digest" \
  --install-dir "$ROOT/.cache/tools"
