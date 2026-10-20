#!/usr/bin/env bash
# Shared companions: scripts/ci/verify-file-checksum.sh scripts/ci/tool-catalog-records.pl scripts/dev/tool-store.sh scripts/dev/host-platform.sh ci/tool-versions.env
set -euo pipefail

# Public entry point for reviewed CI binary installation.
SCRIPT_DIR="${BASH_SOURCE[0]}"
[[ "$SCRIPT_DIR" == /* ]] || SCRIPT_DIR="$PWD/$SCRIPT_DIR"
SCRIPT_DIR="$(cd -P "${SCRIPT_DIR%/*}" && printf '%s/.' "$PWD")"
SCRIPT_DIR="${SCRIPT_DIR%/.}"
tool="${1:-}"
case "$tool" in actionlint|gitleaks|shellcheck|sccache) shift ;; *) echo 'unknown CI tool' >&2; exit 2 ;; esac
version=""
checksum=""
install_dir=""
catalog="$SCRIPT_DIR/../../ci/tool-versions.env"
catalog_selected=false
usage() { echo "usage: install-ci-tool.sh $tool --install-dir <directory> [--catalog <file> | --version <version> --sha256 <digest>]" >&2; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --version|--sha256|--install-dir|--catalog)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            case "$1" in
                --version) version="${2#v}" ;;
                --sha256) checksum="$2" ;;
                --install-dir) install_dir="$2" ;;
                --catalog) catalog="$2"; catalog_selected=true ;;
            esac
            shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done
[[ -n "$install_dir" ]] || { usage; exit 2; }
explicit_pins=false
if [[ -n "$version" || -n "$checksum" ]]; then
    [[ "$catalog_selected" == false ]] || { usage; exit 2; }
    [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?$ ]] || { usage; exit 2; }
    [[ "$checksum" =~ ^[0-9a-f]{64}$ ]] || { echo 'invalid SHA-256 digest' >&2; exit 2; }
    explicit_pins=true
elif [[ "$tool" == sccache ]]; then
    echo 'sccache requires explicit version and SHA-256 pins' >&2; exit 2
fi
platform="$(bash "$SCRIPT_DIR/../dev/host-platform.sh")"
case "$platform" in
    Linux:x86_64) os=linux; arch=amd64 ;;
    Linux:arm64) os=linux; arch=arm64 ;;
    Darwin:x86_64) os=darwin; arch=amd64 ;;
    Darwin:arm64) os=darwin; arch=arm64 ;;
esac
if [[ "$explicit_pins" == false ]]; then
    case "$tool" in
        actionlint) catalog_tool=ACTIONLINT ;;
        gitleaks) catalog_tool=GITLEAKS ;;
        shellcheck) catalog_tool=SHELLCHECK ;;
    esac
    record="$(perl "$SCRIPT_DIR/tool-catalog-records.pl" ci "$catalog" "$catalog_tool")"
    IFS=$'\t' read -r _ version selected_host checksum <<< "$record"
    [[ "$os:$arch:$selected_host" == linux:amd64:LINUX_AMD64 ]] || {
        echo "no catalog pin for $tool on $os:$arch; select qualified explicit pins" >&2; exit 1;
    }
fi
case "$tool" in
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
command -v perl >/dev/null || { echo 'Perl is required for atomic CI tool publication' >&2; exit 1; }
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
# shellcheck source=scripts/dev/tool-store.sh
source "$SCRIPT_DIR/../dev/tool-store.sh"
tool_store_download "https://github.com/$repo/releases/download/v$version/$archive" "$stage/$archive"
bash "$SCRIPT_DIR/verify-file-checksum.sh" sha256 "$checksum" "$stage/$archive"
case "$format" in
    gzip) tar -xzf "$stage/$archive" -C "$stage" "$member" ;;
    xz) tar -xJf "$stage/$archive" -C "$stage" "$member" ;;
esac
candidate="$stage/$member"
[[ -f "$candidate" && ! -L "$candidate" ]] || { echo 'tool payload must be a regular file' >&2; exit 1; }
chmod +x "$candidate"
version_output="$("$candidate" "$version_argument" 2>&1)" || { printf '%s\n' "$version_output" >&2; exit 1; }
reported_version="$version_output"
case "$tool" in
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
# rename(2) targets this exact entry: a late directory must never become a
# container, and a late symlink must not redirect publication into its target.
perl -e 'rename($ARGV[0], $ARGV[1]) or die "publish CI tool: $!\n"' "$candidate" "$install_dir/$tool"
printf '%s\n' "$install_dir/$tool"
