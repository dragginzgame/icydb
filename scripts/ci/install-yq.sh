#!/usr/bin/env bash
set -euo pipefail

# Install Mike Farah's YAML/TOML parser using a consumer-selected version/digest.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
VERSION=""
CHECKSUM=""
INSTALL_DIR="${TOOL_INSTALL_DIR:-$HOME/.local/bin}"
usage() { echo 'usage: install-yq.sh --version <version> --sha256 <digest> [--install-dir <directory>]' >&2; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --version|--sha256|--install-dir)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            case "$1" in
                --version) VERSION="${2#v}" ;;
                --sha256) CHECKSUM="$2" ;;
                --install-dir) INSTALL_DIR="$2" ;;
            esac
            shift 2
            ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done
[[ "$VERSION" =~ ^4\.[0-9]+\.[0-9]+$ && "$CHECKSUM" =~ ^[0-9a-f]{64}$ && -n "$INSTALL_DIR" ]] || { usage; exit 2; }
case "$(uname -s):$(uname -m)" in
    Linux:x86_64|Linux:amd64) platform=linux_amd64 ;;
    Linux:aarch64|Linux:arm64) platform=linux_arm64 ;;
    Darwin:x86_64|Darwin:amd64) platform=darwin_amd64 ;;
    Darwin:arm64|Darwin:aarch64) platform=darwin_arm64 ;;
    *) echo 'unsupported yq host' >&2; exit 1 ;;
esac
temporary="$(mktemp -d "${TMPDIR:-/tmp}/install-yq.XXXXXX")"
trap 'rm -rf "$temporary"' EXIT
candidate="$temporary/yq"
curl --proto '=https' --proto-redir '=https' --tlsv1.2 -fsSL \
    --retry 5 --retry-all-errors --retry-delay 2 --connect-timeout 15 --max-time 120 \
    -o "$candidate" "https://github.com/mikefarah/yq/releases/download/v$VERSION/yq_$platform"
bash "$SCRIPT_DIR/verify-file-checksum.sh" sha256 "$CHECKSUM" "$candidate"
chmod +x "$candidate"
[[ "$("$candidate" --version)" == "yq (https://github.com/mikefarah/yq/) version v$VERSION" ]] || {
    echo 'installed yq does not report the pinned version' >&2; exit 1;
}
mkdir -p "$INSTALL_DIR"
mv "$candidate" "$INSTALL_DIR/yq"
printf '%s\n' "$INSTALL_DIR/yq"
