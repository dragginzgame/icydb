#!/usr/bin/env bash
# Shared companions: scripts/ci/verify-file-checksum.sh scripts/ci/verify-evidence-checksums.sh scripts/ci/ic-tool-pins.awk scripts/dev/tool-store.sh ci/ic-tools.tsv
set -euo pipefail

ROOT="${BASH_SOURCE[0]}"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
consumer="$ROOT"
pins=""
check=false
preflight=false
usage() {
    echo 'usage: install-ic-tools.sh [--consumer <checkout>] [--pins <tsv>] [--check | --preflight]' >&2
}
while [[ $# -gt 0 ]]; do
    case "$1" in
        --consumer|--pins)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            if [[ "$1" == --consumer ]]; then consumer="$2"; else pins="$2"; fi
            shift 2 ;;
        --check) check=true; shift ;;
        --preflight) preflight=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done
[[ "$check" == false || "$preflight" == false ]] || { usage; exit 2; }
[[ "$consumer" == /* ]] || consumer="$PWD/$consumer"
case "$consumer" in *$'\n'*|*$'\r'*) echo 'consumer path contains CR/LF' >&2; exit 1 ;; esac
consumer="$(cd -P "$consumer" && printf '%s/.' "$PWD")"
consumer="${consumer%/.}"
case "$consumer" in *$'\n'*|*$'\r'*) echo 'resolved consumer path contains CR/LF' >&2; exit 1 ;; esac
pins="${pins:-$ROOT/ci/ic-tools.tsv}"
[[ -f "$pins" ]] || { echo "missing IC tool pins: $pins" >&2; exit 1; }
[[ "$pins" == /* ]] || pins="$PWD/$pins"
pins_name="${pins##*/}"
pins="$(cd -P "${pins%/*}" && printf '%s/.' "$PWD")"
pins="${pins%/.}/$pins_name"
platform="$(uname -s):$(uname -m)"
case "$platform" in
    Linux:x86_64|Linux:amd64) host=linux-x86_64; target=x86_64-unknown-linux-gnu; os=linux; arch=x86_64 ;;
    Darwin:x86_64|Darwin:amd64) host=darwin-x86_64; target=x86_64-apple-darwin; os=macos; arch=x86_64 ;;
    Darwin:arm64|Darwin:aarch64) host=darwin-arm64; target=aarch64-apple-darwin; os=macos; arch=arm64 ;;
    *) echo "no complete pinned IC toolset for this host: $platform; use a supported Linux x86-64 or macOS x86-64/ARM64 host" >&2; exit 1 ;;
esac

validate_pins() {
    awk -v records=1 -f "$ROOT/scripts/ci/ic-tool-pins.awk" "$1" | LC_ALL=C sort || {
        echo 'invalid or incomplete IC tool pin matrix' >&2; return 1;
    }
}
selected_records="$(validate_pins "$pins")"
# Platform and catalog admission only: no installed tools, downloads or writes.
[[ "$preflight" == false ]] || exit 0
# shellcheck source=scripts/dev/tool-store.sh
source "$ROOT/scripts/dev/tool-store.sh"
tool_store_init ic "$host
$selected_records"

ic_asset() {
    case "$tool" in
        quill)
            repo=dfinity/quill; tag="v$version"; asset="quill-$os-$arch"; format=raw; member='' ;;
        wasm-opt)
            repo=WebAssembly/binaryen; tag="version_$version"; asset="binaryen-version_$version-$arch-$os.tar.gz"
            format=binaryen; member="binaryen-version_$version" ;;
        icp|didc|ic-wasm)
            case "$tool" in
                icp) repo=dfinity/icp-cli; tag="v$version"; package=icp-cli ;;
                didc) repo=dfinity/candid; tag="didc-v$version"; package=didc ;;
                ic-wasm) repo=dfinity/ic-wasm; tag="$version"; package=ic-wasm ;;
            esac
            asset="$package-$target.tar.xz"; format=xz; member="$package-$target/$tool" ;;
    esac
}

version_check() {
    local executable="$1" tool="$2" version="$3" expected output
    [[ -f "$executable" && ! -L "$executable" && -x "$executable" ]] || return 1
    case "$tool" in
        wasm-opt) expected="wasm-opt version $version" ;;
        *) expected="$tool $version" ;;
    esac
    output="$("$executable" --version)" || return 1
    case "$output" in
        "$expected"|"$expected "*) ;;
        *) echo "$tool version mismatch: expected '$expected', got '$output'" >&2; return 1 ;;
    esac
}
verify_bundle() (
    set -e
    [[ -d "$1" && ! -L "$1" && ! -L "$1/bin" && ! -L "$1/lib" ]] || exit 1
    cd "$1" || exit 1
    for receipt in pins.tsv host files.sha256; do
        [[ -f "$receipt" && ! -L "$receipt" ]] || exit 1
    done
    installed_records="$(validate_pins pins.tsv)" || exit 1
    [[ "$selected_records" == "$installed_records" ]] || exit 1
    [[ "$(cat host)" == "$host" ]] || exit 1
    # Receipts may name only physical payloads under this bundle, never external
    # paths or linked runtime libraries. Require every runtime file in the receipt.
    perl -MFile::Find -e '
        my %files;
        find({no_chdir=>1,wanted=>sub {
            -l $_ and die "linked IC payload\n";
            $files{$_}=1 if -f $_;
        }}, "bin", "lib");
        open my $f,"<","files.sha256" or die $!;
        while (<$f>) {
            s/\n\z//;
            /^[0-9a-f]{64}  ((?:bin|lib)\/[^\r\n]+)$/ or die "invalid IC receipt path\n";
            delete $files{$1} or die "unexpected or duplicate IC payload\n";
        }
        keys(%files)==0 or die "unrecorded IC payload\n";
    ' || exit 1
    bash "$ROOT/scripts/ci/verify-evidence-checksums.sh" files.sha256 >&2 || exit 1
    # Authenticate against publisher-pinned archives too: a changed binary plus
    # an edited local receipt must not become reusable across repositories.
    while IFS=$'\t' read -r tool version selected_host digest || [[ -n "$tool" ]]; do
        [[ "$tool" != \#* && "$selected_host" == "$host" ]] || continue
        ic_asset
        archive="$PWD/downloads/$tool/$asset"
        [[ ! -L downloads && ! -L "downloads/$tool" && -f "$archive" && ! -L "$archive" ]] || exit 1
        bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$digest" "$archive" >&2 || exit 1
        case "$format" in
            raw) cmp -s "$archive" "bin/$tool" || exit 1 ;;
            xz) tar -xOJf "$archive" "$member" | cmp -s - "bin/$tool" || exit 1 ;;
            binaryen)
                tar -xOzf "$archive" "$member/bin/wasm-opt" | cmp -s - bin/wasm-opt || exit 1
                while IFS= read -r record; do
                    file="${record#*  }"
                    [[ "$file" == lib/* ]] || continue
                    tar -xOzf "$archive" "$member/$file" | cmp -s - "$file" || exit 1
                done < files.sha256 ;;
        esac
    done < pins.tsv
    while IFS=$'\t' read -r tool version selected_host digest || [[ -n "$tool" ]]; do
        [[ "$tool" != \#* && "$selected_host" == "$host" ]] || continue
        # Each executable must have a checksum before any version execution.
        awk -v file="bin/$tool" '$2 == file { n++ } END { if (n != 1) exit 1 }' files.sha256 || exit 1
        version_check "$PWD/bin/$tool" "$tool" "$version" || exit 1
    done < pins.tsv
)

tool_root="$consumer/.tools"
active="$tool_root/ic"
selection="$(tool_store_active_reference)"
if [[ -n "$selection" ]]; then
    if bundle="$(tool_store_resolve "$selection" "$store_slot")" &&
        tool_store_unlocked && verify_bundle "$bundle"; then
        printf '%s\n' "$active/bin"
        exit 0
    fi
fi
[[ "$check" == false ]] || { echo 'IC tools missing, changed or selected with different pins; run make install-ic-tools' >&2; exit 1; }
for command in curl tar gzip perl; do command -v "$command" >/dev/null; done
tool_store_prepare_install verify_bundle
if [[ -z "$stage" ]]; then
    printf '%s\n' "$active/bin"
    exit 0
fi
mkdir "$stage/bin" "$stage/lib" "$stage/downloads"
cp "$pins" "$stage/pins.tsv"
validate_pins "$stage/pins.tsv" >/dev/null
printf '%s\n' "$host" > "$stage/host"
while IFS=$'\t' read -r tool version selected_host digest || [[ -n "$tool" ]]; do
    [[ "$tool" != \#* && "$selected_host" == "$host" ]] || continue
    scratch="$stage/downloads/$tool"
    mkdir "$scratch"
    ic_asset
    archive="$scratch/$asset"
    echo "Installing $tool $version ($host)" >&2
    tool_store_download "https://github.com/$repo/releases/download/$tag/$asset" "$archive"
    bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$digest" "$archive"
    case "$format" in
        raw) cp "$archive" "$stage/bin/$tool" ;;
        xz)
            tar -xJf "$archive" -C "$scratch" "$member"
            [[ -f "$scratch/$member" && ! -L "$scratch/$member" ]] || exit 1
            cp "$scratch/$member" "$stage/bin/$tool" ;;
        binaryen)
            tar -xzf "$archive" -C "$scratch" "$member/bin/wasm-opt" "$member/lib"
            [[ -f "$scratch/$member/bin/wasm-opt" && ! -L "$scratch/$member/bin/wasm-opt" ]] || exit 1
            cp "$scratch/$member/bin/wasm-opt" "$stage/bin/wasm-opt"
            cp -R "$scratch/$member/lib/." "$stage/lib/" ;;
    esac
    chmod 0755 "$stage/bin/$tool"
    version_check "$stage/bin/$tool" "$tool" "$version"
done < "$stage/pins.tsv"

# Keep runtime libraries beside bin (required by native macOS Binaryen).
# Record installed bytes for offline verification before future execution.
(
    cd "$stage"
    # The receipt format uses unescaped line records. Reject names it cannot
    # represent, and observe traversal failure before writing any receipt.
    find bin lib -type f -print0 > receipt-files.nul
    while IFS= read -r -d '' file; do
        case "$file" in
            *$'\n'*|*\\*) echo 'installed filename cannot be represented in checksum receipt' >&2; exit 1 ;;
        esac
        printf '%s\n' "$file"
    done < receipt-files.nul > receipt-files.txt
    LC_ALL=C sort receipt-files.txt > receipt-files.sorted
    while IFS= read -r file; do
        digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$file")"
        printf '%s  %s\n' "$digest" "$file"
    done < receipt-files.sorted
    rm receipt-files.nul receipt-files.txt receipt-files.sorted
) > "$stage/files.sha256"
verify_bundle "$stage"
cmp -s "$pins" "$stage/pins.tsv" || { echo 'IC tool pins changed during installation' >&2; exit 1; }
tool_store_activate "$stage"
printf '%s\n' "$active/bin"
