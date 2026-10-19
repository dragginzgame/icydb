#!/usr/bin/env bash
# Shared companions: scripts/ci/verify-file-checksum.sh scripts/ci/tool-catalog-records.pl scripts/dev/tool-store.sh scripts/dev/host-platform.sh ci/tool-versions.env
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
records="$(perl "$ROOT/scripts/ci/tool-catalog-records.pl" host "$versions")"
platform="$(bash "$ROOT/scripts/dev/host-platform.sh")"
case "$platform" in
    Linux:x86_64) host=LINUX_AMD64; yq_platform=linux_amd64; jq_platform=linux-amd64 ;;
    Linux:arm64) host=LINUX_ARM64; yq_platform=linux_arm64; jq_platform=linux-arm64 ;;
    Darwin:x86_64) host=DARWIN_AMD64; yq_platform=darwin_amd64; jq_platform=macos-amd64 ;;
    Darwin:arm64) host=DARWIN_ARM64; yq_platform=darwin_arm64; jq_platform=macos-arm64 ;;
esac
while IFS=$'\t' read -r tool version selected_host digest; do
    [[ "$selected_host" == "$host" || "$selected_host" == all ]] || continue
    case "$tool" in
        JQ) jq_version="$version"; jq_digest="$digest" ;;
        YQ) yq_version="$version"; yq_digest="$digest" ;;
        RIPGREP) rg_version="$version"; rg_digest="$digest" ;;
        CLOC) cloc_version="$version"; cloc_digest="$digest" ;;
    esac
done <<< "$records"
case "$host" in
    LINUX_AMD64) rg_target=x86_64-unknown-linux-musl ;;
    LINUX_ARM64) rg_target=aarch64-unknown-linux-musl ;;
    DARWIN_AMD64) rg_target=x86_64-apple-darwin ;;
    DARWIN_ARM64) rg_target=aarch64-apple-darwin ;;
esac
rg_member="ripgrep-$rg_version-$rg_target/rg"
# shellcheck source=scripts/dev/tool-store.sh
source "$ROOT/scripts/dev/tool-store.sh"
tool_store_init host "$host $jq_version $jq_digest $yq_version $yq_digest $rg_version $rg_digest $cloc_version $cloc_digest"
tool_failure() {
    local tool="$1" path="$2" reason="$3" actual="${4-not-probed}" expected_version
    case "$tool" in
        jq) expected_version="$jq_version" ;;
        yq) expected_version="$yq_version" ;;
        rg) expected_version="$rg_version" ;;
        cloc) expected_version="$cloc_version" ;;
    esac
    printf 'host tool check failed: tool=%s expected=%s path=%s reason=%s actual=%q\nRun make install-host-tools in %s, then make host-tools-check\n' \
        "$tool" "$expected_version" "$path" "$reason" "$actual" "$consumer" >&2
}
verify() {
    local directory="$1" output tool expected_digest expected status
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
            jq) expected_digest="$jq_digest" ;;
            yq) expected_digest="$yq_digest" ;;
            cloc) expected_digest="$cloc_digest" ;;
        esac
        bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$expected_digest" "$directory/bin/$tool" >&2 || {
            tool_failure "$tool" "$directory/bin/$tool" authentication-failed; return 1;
        }
    done
    [[ -f "$directory/ripgrep.tar.gz" && ! -L "$directory/ripgrep.tar.gz" ]] || {
        tool_failure rg "$directory/bin/rg" missing-or-invalid-archive; return 1;
    }
    bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$rg_digest" "$directory/ripgrep.tar.gz" >&2 || {
        tool_failure rg "$directory/bin/rg" archive-authentication-failed; return 1;
    }
    # Compare with the authenticated archive before executing installed bytes.
    tar -xOzf "$directory/ripgrep.tar.gz" "$rg_member" | cmp -s - "$directory/bin/rg" || {
        tool_failure rg "$directory/bin/rg" payload-mismatch; return 1;
    }
    for tool in jq yq cloc rg; do
        case "$tool" in
            jq) expected="jq-$jq_version" ;;
            yq) expected="yq (https://github.com/mikefarah/yq/) version v$yq_version" ;;
            cloc) expected="$cloc_version" ;;
            rg) expected="ripgrep $rg_version" ;;
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
        jq) url="https://github.com/jqlang/jq/releases/download/jq-$jq_version/jq-$jq_platform" ;;
        yq) url="https://github.com/mikefarah/yq/releases/download/v$yq_version/yq_$yq_platform" ;;
    esac
    tool_store_download "$url" "$stage/bin/$tool"
    chmod 0755 "$stage/bin/$tool"
done
tool_store_download \
    "https://github.com/AlDanial/cloc/releases/download/v$cloc_version/cloc-$cloc_version.pl" "$stage/bin/cloc"
chmod 0755 "$stage/bin/cloc"
tool_store_download \
    "https://github.com/BurntSushi/ripgrep/releases/download/$rg_version/ripgrep-$rg_version-$rg_target.tar.gz" "$stage/ripgrep.tar.gz"
bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$rg_digest" "$stage/ripgrep.tar.gz"
tar -xOzf "$stage/ripgrep.tar.gz" "$rg_member" > "$stage/bin/rg"
chmod 0755 "$stage/bin/rg"
verify "$stage"
tool_store_activate "$stage"
printf '%s\n' "$active/bin"
