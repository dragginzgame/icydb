#!/usr/bin/env bash
# Shared companions: scripts/ci/verify-file-checksum.sh
set -euo pipefail

# Explicit local provisioning of the parsers and optional source-analysis tools.
ROOT="${BASH_SOURCE[0]}"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
consumer="$ROOT"
versions=""
check=false
with_ripgrep=false
with_cloc=false
usage() { echo 'usage: install-host-tools.sh [--consumer <checkout>] [--versions <env-file>] [--with-ripgrep] [--with-cloc] [--check]' >&2; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --consumer|--versions)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            if [[ "$1" == --consumer ]]; then consumer="$2"; else versions="$2"; fi
            shift 2 ;;
        --check) check=true; shift ;;
        --with-ripgrep) with_ripgrep=true; shift ;;
        --with-cloc) with_cloc=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done
[[ "$consumer" == /* ]] || consumer="$PWD/$consumer"
consumer="$(cd -P "$consumer" && printf '%s/.' "$PWD")"
consumer="${consumer%/.}"
versions="${versions:-$consumer/ci/tool-versions.env}"
# This reviewed shell file is code, just like the consumer's Makefile.
# shellcheck disable=SC1090
source "$versions"
case "$(uname -s):$(uname -m)" in
    Linux:x86_64|Linux:amd64) host=LINUX_AMD64; yq_platform=linux_amd64; jq_platform=linux-amd64 ;;
    Linux:aarch64|Linux:arm64) host=LINUX_ARM64; yq_platform=linux_arm64; jq_platform=linux-arm64 ;;
    Darwin:x86_64|Darwin:amd64) host=DARWIN_AMD64; yq_platform=darwin_amd64; jq_platform=macos-amd64 ;;
    Darwin:arm64|Darwin:aarch64) host=DARWIN_ARM64; yq_platform=darwin_arm64; jq_platform=macos-arm64 ;;
    *) echo 'unsupported host-tool platform' >&2; exit 1 ;;
esac
tools=(JQ YQ)
if [[ "$with_ripgrep" == true ]]; then tools[2]=RIPGREP; fi
for tool in "${tools[@]}"; do
    version_key="SHARED_TOOLING_${tool}_VERSION"
    digest_key="SHARED_TOOLING_${tool}_SHA256_$host"
    [[ "${!version_key}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ && "${!digest_key}" =~ ^[0-9a-f]{64}$ ]] || {
        echo "invalid $tool version or checksum" >&2; exit 1;
    }
done
if [[ "$with_cloc" == true ]]; then
    [[ "${SHARED_TOOLING_CLOC_VERSION:-}" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ &&
       "${SHARED_TOOLING_CLOC_SHA256:-}" =~ ^[0-9a-f]{64}$ ]] || {
        echo 'invalid CLOC version or checksum' >&2; exit 1;
    }
fi
if [[ "$with_ripgrep" == true ]]; then
    case "$host" in
        LINUX_AMD64) rg_target=x86_64-unknown-linux-musl ;;
        LINUX_ARM64) rg_target=aarch64-unknown-linux-musl ;;
        DARWIN_AMD64) rg_target=x86_64-apple-darwin ;;
        DARWIN_ARM64) rg_target=aarch64-apple-darwin ;;
    esac
    rg_member="ripgrep-$SHARED_TOOLING_RIPGREP_VERSION-$rg_target/rg"
    rg_digest_key="SHARED_TOOLING_RIPGREP_SHA256_$host"
fi
jq_digest_key="SHARED_TOOLING_JQ_SHA256_$host"
yq_digest_key="SHARED_TOOLING_YQ_SHA256_$host"
verify() {
    local directory="$1" output
    [[ -d "$directory" && ! -L "$directory" ]] || return 1
    for tool in jq yq; do
        [[ -f "$directory/bin/$tool" && ! -L "$directory/bin/$tool" && -x "$directory/bin/$tool" ]] || return 1
    done
    # Authenticate every selected payload before executing any of the tools.
    bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "${!jq_digest_key}" "$directory/bin/jq" >&2 || return 1
    bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "${!yq_digest_key}" "$directory/bin/yq" >&2 || return 1
    if [[ "$with_cloc" == true ]]; then
        [[ -f "$directory/bin/cloc" && ! -L "$directory/bin/cloc" && -x "$directory/bin/cloc" ]] || return 1
        bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$SHARED_TOOLING_CLOC_SHA256" "$directory/bin/cloc" >&2 || return 1
    fi
    if [[ "$with_ripgrep" == true ]]; then
        [[ -f "$directory/ripgrep.tar.gz" && ! -L "$directory/ripgrep.tar.gz" &&
           -f "$directory/bin/rg" && ! -L "$directory/bin/rg" && -x "$directory/bin/rg" ]] || return 1
        bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "${!rg_digest_key}" "$directory/ripgrep.tar.gz" >&2 || return 1
        # Compare with the authenticated archive before executing installed bytes.
        tar -xOzf "$directory/ripgrep.tar.gz" "$rg_member" | cmp -s - "$directory/bin/rg" || return 1
    fi
    output="$("$directory/bin/jq" --version)" || return 1
    [[ "$output" == "jq-$SHARED_TOOLING_JQ_VERSION" ]] || return 1
    output="$("$directory/bin/yq" --version)" || return 1
    [[ "$output" == "yq (https://github.com/mikefarah/yq/) version v$SHARED_TOOLING_YQ_VERSION" ]] || return 1
    if [[ "$with_cloc" == true ]]; then
        output="$("$directory/bin/cloc" --version)" || return 1
        [[ "$output" == "$SHARED_TOOLING_CLOC_VERSION" ]] || return 1
    fi
    if [[ "$with_ripgrep" == true ]]; then
        output="$("$directory/bin/rg" --version)" || return 1
        case "${output%%$'\n'*}" in
            "ripgrep $SHARED_TOOLING_RIPGREP_VERSION"|"ripgrep $SHARED_TOOLING_RIPGREP_VERSION (rev "*")") ;;
            *) echo 'ripgrep version mismatch' >&2; return 1 ;;
        esac
        "$directory/bin/rg" --pcre2-version >&2 || return 1
    fi
}
tool_root="$consumer/.tools"
active="$tool_root/host"
[[ ! -L "$tool_root" ]] || { echo 'repository .tools may not be a symlink' >&2; exit 1; }
if [[ -e "$active" && ! -L "$active" ]]; then
    echo 'refusing to replace unmanaged .tools/host' >&2; exit 1
fi
if [[ -L "$active" ]]; then
    # Preserve trailing newlines so admission checks the literal link target.
    selection="$(perl -e 'my $s=readlink($ARGV[0]); defined($s) or exit 1; print $s,"/."' "$active")"
    selection="${selection%/.}"
    [[ "$selection" =~ ^host-set\.[[:alnum:]]+$ ]] || { echo 'unmanaged host-tool selection' >&2; exit 1; }
    if verify "$tool_root/$selection"; then printf '%s\n' "$active/bin"; exit 0; fi
fi
[[ "$check" == false ]] || { echo 'host tools missing or changed; run make install-host-tools' >&2; exit 1; }
for command in curl perl; do command -v "$command" >/dev/null; done
mkdir -p "$tool_root"
lock="$tool_root/.host-tools.lock"
mkdir "$lock" 2>/dev/null || { echo "host tool installation already locked: $lock" >&2; exit 1; }
stage=""
finish() {
    local status=$?
    rm -f "$lock/selected" "$lock/owner"
    rmdir "$lock"
    if [[ "$status" != 0 && -n "$stage" ]]; then echo "Failed host tool installation retained: $stage" >&2; fi
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'pid=%s\nconsumer=%s\n' "$$" "$consumer" > "$lock/owner"
stage="$(mktemp -d "$tool_root/host-set.XXXXXX")"
mkdir "$stage/bin"
for tool in jq yq; do
    case "$tool" in
        jq) url="https://github.com/jqlang/jq/releases/download/jq-$SHARED_TOOLING_JQ_VERSION/jq-$jq_platform" ;;
        yq) url="https://github.com/mikefarah/yq/releases/download/v$SHARED_TOOLING_YQ_VERSION/yq_$yq_platform" ;;
    esac
    curl --proto '=https' --proto-redir '=https' --tlsv1.2 -fsSL \
        --connect-timeout 15 --max-time 300 -o "$stage/bin/$tool" "$url"
    chmod 0755 "$stage/bin/$tool"
done
if [[ "$with_cloc" == true ]]; then
    curl --proto '=https' --proto-redir '=https' --tlsv1.2 -fsSL \
        --connect-timeout 15 --max-time 300 -o "$stage/bin/cloc" \
        "https://github.com/AlDanial/cloc/releases/download/v$SHARED_TOOLING_CLOC_VERSION/cloc-$SHARED_TOOLING_CLOC_VERSION.pl"
    chmod 0755 "$stage/bin/cloc"
fi
if [[ "$with_ripgrep" == true ]]; then
    command -v tar >/dev/null
    curl --proto '=https' --proto-redir '=https' --tlsv1.2 -fsSL \
        --connect-timeout 15 --max-time 300 -o "$stage/ripgrep.tar.gz" \
        "https://github.com/BurntSushi/ripgrep/releases/download/$SHARED_TOOLING_RIPGREP_VERSION/ripgrep-$SHARED_TOOLING_RIPGREP_VERSION-$rg_target.tar.gz"
    bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "${!rg_digest_key}" "$stage/ripgrep.tar.gz"
    tar -xOzf "$stage/ripgrep.tar.gz" "$rg_member" > "$stage/bin/rg"
    chmod 0755 "$stage/bin/rg"
fi
verify "$stage"
ln -s "${stage##*/}" "$lock/selected"
perl -e 'rename($ARGV[0], $ARGV[1]) or die "activate host tools: $!\n"' "$lock/selected" "$active"
printf '%s\n' "$active/bin"
