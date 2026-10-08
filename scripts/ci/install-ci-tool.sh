#!/usr/bin/env bash
# Shared companions: scripts/ci/verify-file-checksum.sh
set -euo pipefail

# Internal implementation of the reviewed CI-tool entry points.
SCRIPT_DIR="${BASH_SOURCE[0]}"
[[ "$SCRIPT_DIR" == /* ]] || SCRIPT_DIR="$PWD/$SCRIPT_DIR"
SCRIPT_DIR="$(cd -P "${SCRIPT_DIR%/*}" && printf '%s/.' "$PWD")"
SCRIPT_DIR="${SCRIPT_DIR%/.}"
tool="${1:-}"
case "$tool" in actionlint|gitleaks|shellcheck|sccache|yq) shift ;; *) echo 'unknown CI tool' >&2; exit 2 ;; esac
version=""
checksum=""
install_dir="${TOOL_INSTALL_DIR:-$HOME/.local/bin}"
usage() { echo "usage: install-$tool.sh --version <version> --sha256 <digest> [--install-dir <directory>]" >&2; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --version|--sha256|--install-dir)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            case "$1" in
                --version) version="${2#v}" ;;
                --sha256) checksum="$2" ;;
                --install-dir) install_dir="$2" ;;
            esac
            shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?$ ]] || { usage; exit 2; }
if [[ "$tool" == yq ]]; then
    [[ "$version" =~ ^4\.[0-9]+\.[0-9]+$ ]] || { usage; exit 2; }
fi
[[ "$checksum" =~ ^[0-9a-f]{64}$ ]] || { echo 'invalid SHA-256 digest' >&2; exit 2; }
case "$(uname -s):$(uname -m)" in
    Linux:x86_64|Linux:amd64) os=linux; arch=amd64 ;;
    Linux:arm64|Linux:aarch64) os=linux; arch=arm64 ;;
    Darwin:x86_64|Darwin:amd64) os=darwin; arch=amd64 ;;
    Darwin:arm64|Darwin:aarch64) os=darwin; arch=arm64 ;;
    *) echo "unsupported $tool host" >&2; exit 1 ;;
esac
case "$tool" in
    yq)
        repo=mikefarah/yq
        archive="yq_${os}_${arch}"
        member="$archive"; format=raw; version_argument=--version ;;
    actionlint)
        repo=rhysd/actionlint
        archive="actionlint_${version}_${os}_${arch}.tar.gz"
        member=actionlint; format=gzip; version_argument=-version ;;
    gitleaks)
        repo=gitleaks/gitleaks
        [[ "$arch" != amd64 ]] || arch=x64
        archive="gitleaks_${version}_${os}_${arch}.tar.gz"
        member=gitleaks; format=gzip; version_argument=version ;;
    shellcheck)
        repo=koalaman/shellcheck
        case "$arch" in amd64) arch=x86_64 ;; arm64) arch=aarch64 ;; esac
        archive="shellcheck-v${version}.${os}.${arch}.tar.xz"
        member="shellcheck-v$version/shellcheck"; format=xz; version_argument=--version ;;
    sccache)
        # Preserve Canic's reviewed CI asset scope. Other hosts prepare their
        # consumer-selected sccache separately; no unqualified asset fallback.
        [[ "$os:$arch" == linux:amd64 ]] || {
            echo 'sccache CI installer requires Linux x86_64' >&2; exit 1;
        }
        repo=mozilla/sccache
        package="sccache-v$version-x86_64-unknown-linux-musl"
        archive="$package.tar.gz"
        member="$package/sccache"; format=gzip; version_argument=--version ;;
esac
mkdir -p "$install_dir"
[[ ! -d "$install_dir/$tool" ]] || { echo 'tool destination is a directory' >&2; exit 1; }
# Stage on the destination filesystem so successful publication is one rename.
stage="$(mktemp -d "$install_dir/.$tool-install.XXXXXX")"
finish() {
    local status=$?
    if [[ "$status" == 0 ]]; then rm -rf "$stage"
    else echo "Failed $tool installation retained: $stage" >&2; fi
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
curl --proto '=https' --proto-redir '=https' --tlsv1.2 -fsSL \
    --retry 5 --retry-all-errors --retry-delay 2 --connect-timeout 15 --max-time 120 \
    -o "$stage/$archive" "https://github.com/$repo/releases/download/v$version/$archive"
bash "$SCRIPT_DIR/verify-file-checksum.sh" sha256 "$checksum" "$stage/$archive"
case "$format" in
    raw) ;;
    gzip) tar -xzf "$stage/$archive" -C "$stage" "$member" ;;
    xz) tar -xJf "$stage/$archive" -C "$stage" "$member" ;;
esac
candidate="$stage/$member"
[[ -f "$candidate" && ! -L "$candidate" ]] || { echo 'tool payload must be a regular file' >&2; exit 1; }
chmod +x "$candidate"
version_output="$("$candidate" "$version_argument" 2>&1)" || { printf '%s\n' "$version_output" >&2; exit 1; }
reported_version="$version_output"
case "$tool" in
    yq)
        reported_version="${version_output#yq (https://github.com/mikefarah/yq/) version v}"
        [[ "$version_output" == "yq (https://github.com/mikefarah/yq/) version v$reported_version" ]] || exit 1 ;;
    actionlint) reported_version="${version_output%%$'\n'*}" ;;
    sccache) reported_version="${version_output#sccache }"
        [[ "$version_output" == "sccache $reported_version" ]] || exit 1 ;;
    shellcheck)
        reported_version=""
        while IFS= read -r line; do
            case "$line" in 'version: '*) reported_version="${line#version: }"; break ;; esac
        done <<< "$version_output" ;;
esac
[[ "$reported_version" == "$version" ]] || {
    printf 'installed %s version mismatch: expected %s, got %s\n' "$tool" "$version" "$version_output" >&2; exit 1;
}
mv "$candidate" "$install_dir/$tool"
printf '%s\n' "$install_dir/$tool"
