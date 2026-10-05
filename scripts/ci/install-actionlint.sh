#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION=""
CHECKSUM=""
INSTALL_DIR="${TOOL_INSTALL_DIR:-$HOME/.local/bin}"
TMP_DIR=""

usage() {
    cat >&2 <<'USAGE'
usage: install-actionlint.sh --version <version> --sha256 <digest> [--install-dir <directory>]
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
    --version)
        [ "$#" -ge 2 ] || { usage; exit 2; }
        VERSION="$2"
        shift 2
        ;;
    --sha256)
        [ "$#" -ge 2 ] || { usage; exit 2; }
        CHECKSUM="$2"
        shift 2
        ;;
    --install-dir)
        [ "$#" -ge 2 ] || { usage; exit 2; }
        INSTALL_DIR="$2"
        shift 2
        ;;
    -h | --help)
        usage
        exit 0
        ;;
    *)
        usage
        exit 2
        ;;
    esac
done

if [ -z "$VERSION" ] || [ -z "$CHECKSUM" ]; then
    usage
    exit 2
fi

VERSION="${VERSION#v}"
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?$ ]]; then
    echo "invalid actionlint version: $VERSION" >&2
    exit 1
fi
if [[ ! "$CHECKSUM" =~ ^[0-9a-f]{64}$ ]]; then
    echo "invalid SHA-256 digest" >&2
    exit 1
fi

resolve_platform() {
    case "$(uname -s):$(uname -m)" in
    Darwin:x86_64 | Darwin:amd64)
        platform="darwin_amd64"
        ;;
    Darwin:arm64 | Darwin:aarch64)
        platform="darwin_arm64"
        ;;
    Linux:x86_64 | Linux:amd64)
        platform="linux_amd64"
        ;;
    Linux:arm64 | Linux:aarch64)
        platform="linux_arm64"
        ;;
    *)
        echo "unsupported actionlint platform: $(uname -s) $(uname -m)" >&2
        exit 1
        ;;
    esac
}

main() {
    local version_no_v="$VERSION"
    local archive="actionlint_${version_no_v}_${platform}.tar.gz"
    local url="https://github.com/rhysd/actionlint/releases/download/v${version_no_v}/${archive}"
    local installed
    local candidate
    local reported_version
    local version_output

    TMP_DIR="$(mktemp -d)"
    trap 'rm -rf "$TMP_DIR"' EXIT
    mkdir -p "$INSTALL_DIR"
    curl --proto '=https' --proto-redir '=https' --tlsv1.2 -fsSL \
        --retry 5 --retry-all-errors --retry-delay 2 \
        --connect-timeout 15 --max-time 120 \
        -o "$TMP_DIR/$archive" "$url"
    bash "$SCRIPT_DIR/verify-file-checksum.sh" sha256 "$CHECKSUM" "$TMP_DIR/$archive"
    tar -xzf "$TMP_DIR/$archive" -C "$TMP_DIR" actionlint
    candidate="$TMP_DIR/actionlint"
    chmod +x "$candidate"
    version_output="$("$candidate" -version 2>&1)"
    reported_version="${version_output%%$'\n'*}"
    if [ "$reported_version" != "$VERSION" ]; then
        echo "installed actionlint does not report the pinned version" >&2
        echo "expected: $VERSION" >&2
        echo "actual:   $version_output" >&2
        exit 1
    fi

    installed="$INSTALL_DIR/actionlint"
    mv "$candidate" "$installed"
    printf '%s\n' "$installed"
}

resolve_platform
main
