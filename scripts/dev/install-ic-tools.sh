#!/usr/bin/env bash
# Shared companions: scripts/ci/verify-file-checksum.sh scripts/ci/verify-evidence-checksums.sh scripts/ci/ic-tool-pins.awk
set -euo pipefail

ROOT="${BASH_SOURCE[0]}"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
consumer="$ROOT"
pins=""
check=false
usage() {
    echo 'usage: install-ic-tools.sh [--consumer <checkout>] [--pins <tsv>] [--check]' >&2
}
while [[ $# -gt 0 ]]; do
    case "$1" in
        --consumer|--pins)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            if [[ "$1" == --consumer ]]; then consumer="$2"; else pins="$2"; fi
            shift 2 ;;
        --check) check=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done
[[ "$consumer" == /* ]] || consumer="$PWD/$consumer"
consumer="$(cd -P "$consumer" && printf '%s/.' "$PWD")"
consumer="${consumer%/.}"
pins="${pins:-$consumer/ci/ic-tools.tsv}"
[[ -f "$pins" ]] || { echo "missing IC tool pins: $pins" >&2; exit 1; }
[[ "$pins" == /* ]] || pins="$PWD/$pins"
pins_name="${pins##*/}"
pins="$(cd -P "${pins%/*}" && printf '%s/.' "$PWD")"
pins="${pins%/.}/$pins_name"
case "$(uname -s):$(uname -m)" in
    Linux:x86_64|Linux:amd64) host=linux-x86_64; target=x86_64-unknown-linux-gnu; os=linux; arch=x86_64 ;;
    Darwin:x86_64|Darwin:amd64) host=darwin-x86_64; target=x86_64-apple-darwin; os=macos; arch=x86_64 ;;
    Darwin:arm64|Darwin:aarch64) host=darwin-arm64; target=aarch64-apple-darwin; os=macos; arch=arm64 ;;
    *) echo 'no complete pinned IC toolset for this host' >&2; exit 1 ;;
esac

validate_pins() {
    awk -f "$ROOT/scripts/ci/ic-tool-pins.awk" "$1" || {
        echo 'invalid or incomplete IC tool pin matrix' >&2; return 1;
    }
}
validate_pins "$pins"

version_check() {
    local executable="$1" tool="$2" version="$3" expected output
    [[ -f "$executable" && ! -L "$executable" && -x "$executable" ]] || return 1
    case "$tool" in
        pocket-ic) expected="pocket-ic-server $version" ;;
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
    cd "$1" || exit 1
    cmp -s "$pins" pins.tsv || exit 1
    [[ "$(cat host)" == "$host" ]] || exit 1
    bash "$ROOT/scripts/ci/verify-evidence-checksums.sh" files.sha256 >&2 || exit 1
    while IFS=$'\t' read -r tool version selected_host digest; do
        [[ "$tool" != \#* && "$selected_host" == "$host" ]] || continue
        # Each executable must have a checksum before any version execution.
        awk -v file="bin/$tool" '$2 == file { n++ } END { if (n != 1) exit 1 }' files.sha256 || exit 1
        version_check "$PWD/bin/$tool" "$tool" "$version" || exit 1
    done < pins.tsv
)

tool_root="$consumer/.tools"
active="$tool_root/ic"
[[ ! -L "$tool_root" ]] || { echo 'repository .tools may not be a symlink' >&2; exit 1; }
if [[ -e "$active" && ! -L "$active" ]]; then
    echo 'refusing to replace an unmanaged .tools/ic path' >&2; exit 1
fi
if [[ -L "$active" ]]; then
    # Preserve trailing newlines so admission checks the literal link target.
    selection="$(perl -e 'my $s=readlink($ARGV[0]); defined($s) or exit 1; print $s,"/."' "$active")"
    selection="${selection%/.}"
    [[ "$selection" =~ ^ic-set\.[[:alnum:]]+$ ]] || {
        echo 'refusing an unmanaged .tools/ic link' >&2; exit 1;
    }
    [[ ! -L "$tool_root/$selection" ]] || { echo 'IC tool bundle may not be a symlink' >&2; exit 1; }
    if verify_bundle "$tool_root/$selection"; then
        printf '%s\n' "$active/bin"
        exit 0
    fi
fi
[[ "$check" == false ]] || { echo 'IC tools missing, changed or selected with different pins; run make install-ic-tools' >&2; exit 1; }
for command in curl tar gzip perl; do command -v "$command" >/dev/null; done
mkdir -p "$tool_root"
lock="$tool_root/.ic-tools.lock"
mkdir "$lock" 2>/dev/null || { echo "IC tool installation already locked: $lock" >&2; exit 1; }
stage=""
link="$lock/selected"
finish() {
    local status=$?
    rm -f "$link" "$lock/owner"
    rmdir "$lock"
    if [[ "$status" != 0 && -n "$stage" ]]; then echo "Failed tool installation retained: $stage" >&2; fi
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'pid=%s\nconsumer=%s\n' "$$" "$consumer" > "$lock/owner"
stage="$(mktemp -d "$tool_root/ic-set.XXXXXX")"
mkdir "$stage/bin" "$stage/lib" "$stage/downloads"
cp "$pins" "$stage/pins.tsv"
validate_pins "$stage/pins.tsv"
printf '%s\n' "$host" > "$stage/host"
while IFS=$'\t' read -r tool version selected_host digest; do
    [[ "$tool" != \#* && "$selected_host" == "$host" ]] || continue
    scratch="$stage/downloads/$tool"
    mkdir "$scratch"
    case "$tool" in
        quill)
            repo=dfinity/quill; tag="v$version"; asset="quill-$os-$arch"; format=raw; member='' ;;
        pocket-ic)
            repo=dfinity/pocketic; tag="$version"; asset="pocket-ic-$arch-${host%-*}.gz"; format=gzip; member='' ;;
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
    archive="$scratch/$asset"
    echo "Installing $tool $version ($host)" >&2
    curl --proto '=https' --proto-redir '=https' --tlsv1.2 -fsSL \
        --connect-timeout 15 --max-time 300 -o "$archive" \
        "https://github.com/$repo/releases/download/$tag/$asset"
    bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$digest" "$archive"
    case "$format" in
        raw) cp "$archive" "$stage/bin/$tool" ;;
        gzip) gzip -dc "$archive" > "$stage/bin/$tool" ;;
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
ln -s "${stage##*/}" "$link"
# rename(2) replaces the symlink itself on Linux and macOS, never its directory.
perl -e 'rename($ARGV[0], $ARGV[1]) or die "activate IC tools: $!\n"' "$link" "$active"
printf '%s\n' "$active/bin"
