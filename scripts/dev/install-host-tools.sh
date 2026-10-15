#!/usr/bin/env bash
# Shared companions: scripts/ci/verify-file-checksum.sh scripts/dev/tool-store.sh ci/tool-versions.env
set -euo pipefail

# Explicit local provisioning of the complete common host-tool set.
ROOT="${BASH_SOURCE[0]}"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
consumer="$ROOT"
versions=""
check=false
usage() { echo 'usage: install-host-tools.sh [--consumer <checkout>] [--versions <env-file>] [--check]' >&2; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --consumer|--versions)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            if [[ "$1" == --consumer ]]; then consumer="$2"; else versions="$2"; fi
            shift 2 ;;
        --check) check=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done
[[ "$consumer" == /* ]] || consumer="$PWD/$consumer"
case "$consumer" in *$'\n'*|*$'\r'*) echo 'consumer path contains CR/LF' >&2; exit 1 ;; esac
consumer="$(cd -P "$consumer" && printf '%s/.' "$PWD")"
consumer="${consumer%/.}"
case "$consumer" in *$'\n'*|*$'\r'*) echo 'resolved consumer path contains CR/LF' >&2; exit 1 ;; esac
versions="${versions:-$ROOT/ci/tool-versions.env}"
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
for tool in JQ YQ RIPGREP; do
    version_key="SHARED_TOOLING_${tool}_VERSION"
    digest_key="SHARED_TOOLING_${tool}_SHA256_$host"
    [[ "${!version_key}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ && "${!digest_key}" =~ ^[0-9a-f]{64}$ ]] || {
        echo "invalid $tool version or checksum" >&2; exit 1;
    }
done
[[ "${SHARED_TOOLING_CLOC_VERSION:-}" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ &&
   "${SHARED_TOOLING_CLOC_SHA256:-}" =~ ^[0-9a-f]{64}$ ]] || {
    echo 'invalid CLOC version or checksum' >&2; exit 1;
}
case "$host" in
    LINUX_AMD64) rg_target=x86_64-unknown-linux-musl ;;
    LINUX_ARM64) rg_target=aarch64-unknown-linux-musl ;;
    DARWIN_AMD64) rg_target=x86_64-apple-darwin ;;
    DARWIN_ARM64) rg_target=aarch64-apple-darwin ;;
esac
rg_member="ripgrep-$SHARED_TOOLING_RIPGREP_VERSION-$rg_target/rg"
rg_digest_key="SHARED_TOOLING_RIPGREP_SHA256_$host"
jq_digest_key="SHARED_TOOLING_JQ_SHA256_$host"
yq_digest_key="SHARED_TOOLING_YQ_SHA256_$host"
# shellcheck source=scripts/dev/tool-store.sh
source "$ROOT/scripts/dev/tool-store.sh"
tool_store_init host "$host $SHARED_TOOLING_JQ_VERSION ${!jq_digest_key} $SHARED_TOOLING_YQ_VERSION ${!yq_digest_key} $SHARED_TOOLING_RIPGREP_VERSION ${!rg_digest_key} $SHARED_TOOLING_CLOC_VERSION $SHARED_TOOLING_CLOC_SHA256"
tool_failure() {
    local tool="$1" path="$2" reason="$3" actual="${4-not-probed}" version_key
    case "$tool" in
        jq) version_key=SHARED_TOOLING_JQ_VERSION ;;
        yq) version_key=SHARED_TOOLING_YQ_VERSION ;;
        rg) version_key=SHARED_TOOLING_RIPGREP_VERSION ;;
        cloc) version_key=SHARED_TOOLING_CLOC_VERSION ;;
    esac
    printf 'host tool check failed: tool=%s expected=%s path=%s reason=%s actual=%q\nRun make install-host-tools in %s, then make host-tools-check\n' \
        "$tool" "${!version_key}" "$path" "$reason" "$actual" "$consumer" >&2
}
verify() {
    local directory="$1" output tool digest_key expected status
    [[ -d "$directory" && ! -L "$directory" && ! -L "$directory/bin" ]] || {
        tool_failure jq "$directory/bin/jq" missing-or-invalid-bundle; return 1;
    }
    for tool in jq yq rg cloc; do
        [[ -f "$directory/bin/$tool" && ! -L "$directory/bin/$tool" && -x "$directory/bin/$tool" ]] || {
            tool_failure "$tool" "$directory/bin/$tool" missing-or-invalid-executable; return 1;
        }
    done
    # Authenticate every selected payload before executing any of the tools.
    for tool in jq yq cloc; do
        case "$tool" in
            jq) digest_key="$jq_digest_key" ;;
            yq) digest_key="$yq_digest_key" ;;
            cloc) digest_key=SHARED_TOOLING_CLOC_SHA256 ;;
        esac
        bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "${!digest_key}" "$directory/bin/$tool" >&2 || {
            tool_failure "$tool" "$directory/bin/$tool" authentication-failed; return 1;
        }
    done
    [[ -f "$directory/ripgrep.tar.gz" && ! -L "$directory/ripgrep.tar.gz" ]] || {
        tool_failure rg "$directory/bin/rg" missing-or-invalid-archive; return 1;
    }
    bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "${!rg_digest_key}" "$directory/ripgrep.tar.gz" >&2 || {
        tool_failure rg "$directory/bin/rg" archive-authentication-failed; return 1;
    }
    # Compare with the authenticated archive before executing installed bytes.
    tar -xOzf "$directory/ripgrep.tar.gz" "$rg_member" | cmp -s - "$directory/bin/rg" || {
        tool_failure rg "$directory/bin/rg" payload-mismatch; return 1;
    }
    for tool in jq yq cloc rg; do
        case "$tool" in
            jq) expected="jq-$SHARED_TOOLING_JQ_VERSION" ;;
            yq) expected="yq (https://github.com/mikefarah/yq/) version v$SHARED_TOOLING_YQ_VERSION" ;;
            cloc) expected="$SHARED_TOOLING_CLOC_VERSION" ;;
            rg) expected="ripgrep $SHARED_TOOLING_RIPGREP_VERSION" ;;
        esac
        if output="$("$directory/bin/$tool" --version)"; then :; else
            status=$?
            tool_failure "$tool" "$directory/bin/$tool" "version-probe-failed(exit=$status)" "$output"; return 1;
        fi
        if [[ "$tool" == rg ]]; then
            case "${output%%$'\n'*}" in
                "$expected"|"$expected (rev "*")") continue ;;
            esac
        elif [[ "$output" == "$expected" ]]; then continue; fi
        tool_failure "$tool" "$directory/bin/$tool" version-mismatch "$output"; return 1
    done
    if output="$("$directory/bin/rg" --pcre2-version)"; then
        :
    else
        status=$?
        tool_failure rg "$directory/bin/rg" "PCRE2-probe-failed(exit=$status)" "$output"; return 1
    fi
}
tool_root="$consumer/.tools"
active="$tool_root/host"
bundle="$(tool_store_verified_selection verify)"
if [[ -n "$bundle" ]]; then
    printf '%s\n' "$active/bin"
    exit 0
fi
if [[ "$check" == true && ! -L "$active" ]]; then
    tool_failure jq "$active/bin/jq" missing-bundle
fi
[[ "$check" == false ]] || exit 1
for command in curl perl tar; do command -v "$command" >/dev/null; done
tool_store_prepare_install verify
if [[ -z "$stage" ]]; then
    printf '%s\n' "$active/bin"
    exit 0
fi
mkdir "$stage/bin"
for tool in jq yq; do
    case "$tool" in
        jq) url="https://github.com/jqlang/jq/releases/download/jq-$SHARED_TOOLING_JQ_VERSION/jq-$jq_platform" ;;
        yq) url="https://github.com/mikefarah/yq/releases/download/v$SHARED_TOOLING_YQ_VERSION/yq_$yq_platform" ;;
    esac
    tool_store_download "$url" "$stage/bin/$tool"
    chmod 0755 "$stage/bin/$tool"
done
tool_store_download \
    "https://github.com/AlDanial/cloc/releases/download/v$SHARED_TOOLING_CLOC_VERSION/cloc-$SHARED_TOOLING_CLOC_VERSION.pl" "$stage/bin/cloc"
chmod 0755 "$stage/bin/cloc"
tool_store_download \
    "https://github.com/BurntSushi/ripgrep/releases/download/$SHARED_TOOLING_RIPGREP_VERSION/ripgrep-$SHARED_TOOLING_RIPGREP_VERSION-$rg_target.tar.gz" "$stage/ripgrep.tar.gz"
bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "${!rg_digest_key}" "$stage/ripgrep.tar.gz"
tar -xOzf "$stage/ripgrep.tar.gz" "$rg_member" > "$stage/bin/rg"
chmod 0755 "$stage/bin/rg"
verify "$stage"
tool_store_activate "$stage"
printf '%s\n' "$active/bin"
