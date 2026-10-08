#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd -P)"
consumer="$ROOT"
versions_file="$ROOT/ci/tool-versions.env"
check_only=false
usage() {
    echo 'usage: install-rust-tools.sh [--consumer DIR] [--versions FILE] [--check]' >&2
}
while [[ $# -gt 0 ]]; do
    case "$1" in
        --consumer|--versions)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            if [[ "$1" == --consumer ]]; then consumer="$2"; else versions_file="$2"; fi
            shift 2 ;;
        --check) check_only=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done
consumer="$(cd "$consumer" && pwd -P)"
# Reviewed executable configuration, just like the host-tool versions file.
# shellcheck source=/dev/null
source "$versions_file"
tool_names=(cargo-sort cargo-sort-derives candid-extractor)
tool_versions=("${SHARED_TOOLING_CARGO_SORT_VERSION:-}" "${SHARED_TOOLING_CARGO_SORT_DERIVES_VERSION:-}" "${SHARED_TOOLING_CANDID_EXTRACTOR_VERSION:-}")
for version in "${tool_versions[@]}"; do
    [[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || {
        echo 'Rust tools require exact stable versions for all three tools' >&2; exit 2;
    }
done
install_root="$consumer/.tools/rust"
export RUSTUP_AUTO_INSTALL=0

check_install_paths() {
    local path
    local files=("$install_root/.crates.toml" "$install_root/.crates2.json")
    # Admit the whole fixed route before any tool probe or Cargo dispatch. Do
    # not follow even dangling links, or inspect unrelated host/IC bundle links.
    for path in "$consumer/.tools" "$install_root" "$install_root/bin" "$install_root/build"; do
        if [[ -L "$path" || ( -e "$path" && ! -d "$path" ) ]]; then
            printf 'Rust tool directory must be a physical directory: %s\n' "$path" >&2
            return 1
        fi
    done
    # Cargo writes its receipts here too; executable/receipt symlinks must not
    # redirect a later install after apparently valid version probes.
    for path in "${tool_names[@]}"; do files+=("$install_root/bin/$path"); done
    for path in "${files[@]}"; do
        if [[ -L "$path" || ( -e "$path" && ! -f "$path" ) ]]; then
            printf 'Rust tool executable or receipt must be a regular file: %s\n' "$path" >&2
            return 1
        fi
    done
}

check_tool() {
    local tool="$1" version="$2" actual
    local arguments=(--version)
    [[ "$tool" != cargo-sort-derives ]] || arguments=(sort-derives --version)
    [[ -x "$install_root/bin/$tool" ]] &&
        actual="$("$install_root/bin/$tool" "${arguments[@]}")" &&
        [[ "$actual" == "$tool $version" ]]
}

check_install_paths
for index in "${!tool_names[@]}"; do
    tool="${tool_names[$index]}"
    version="${tool_versions[$index]}"
    if check_tool "$tool" "$version"; then continue; fi
    if [[ "$check_only" == true ]]; then
        echo "missing or mismatched $tool $version; run make install-rust-tools" >&2
        exit 1
    fi
    # Cargo owns registry integrity, install locking and receipts. Keep build
    # output in the selected checkout, including after a failed installation.
    cargo install "$tool" --version "=$version" --locked \
        --root "$install_root" --target-dir "$install_root/build"
    check_install_paths
    check_tool "$tool" "$version" || {
        echo "installed $tool failed its version check" >&2; exit 1;
    }
done
printf '%s\n' "$install_root/bin"
