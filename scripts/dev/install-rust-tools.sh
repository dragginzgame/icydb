#!/usr/bin/env bash
# Shared companions: scripts/ci/verify-file-checksum.sh
set -euo pipefail

ROOT="$0"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
consumer="$ROOT"
versions_file="$ROOT/ci/tool-versions.env"
check_only=false
package='' selected_version='' kind='' target='' profile=''
usage() {
    echo 'usage: install-rust-tools.sh [--consumer DIR] [--versions FILE] [--check]' >&2
    echo '   or: install-rust-tools.sh [--consumer DIR] --package NAME --version X.Y.Z (--bin NAME | --example NAME) --profile (debug | release) [--check]' >&2
}
while [[ $# -gt 0 ]]; do
    case "$1" in
        --consumer|--versions)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            if [[ "$1" == --consumer ]]; then consumer="$2"; else versions_file="$2"; fi
            shift 2 ;;
        --check) check_only=true; shift ;;
        --package|--version|--bin|--example|--profile)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            case "$1" in
                --package) package="$2" ;;
                --version) selected_version="$2" ;;
                --bin|--example)
                    [[ -z "$kind" ]] || { usage; exit 2; }
                    kind="${1#--}"; target="$2" ;;
                --profile) profile="$2" ;;
            esac
            shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done
[[ "$consumer" == /* ]] || consumer="$PWD/$consumer"
consumer="$(cd -P "$consumer" && printf '%s/.' "$PWD")"
consumer="${consumer%/.}"
selected=false
if [[ -n "$package$selected_version$kind$target$profile" ]]; then
    selected=true
    [[ "$package" =~ ^[a-zA-Z0-9][a-zA-Z0-9_-]*$ &&
       "$target" =~ ^[a-zA-Z0-9][a-zA-Z0-9_-]*$ && -n "$kind" &&
       ( "$profile" == debug || "$profile" == release ) ]] || { usage; exit 2; }
    tool_names=("$target")
    tool_versions=("$selected_version")
else
    # Reviewed executable configuration, just like the host-tool versions file.
    # shellcheck source=/dev/null
    source "$versions_file"
    tool_names=(cargo-sort cargo-sort-derives candid-extractor)
    tool_versions=("${SHARED_TOOLING_CARGO_SORT_VERSION:-}" "${SHARED_TOOLING_CARGO_SORT_DERIVES_VERSION:-}" "${SHARED_TOOLING_CANDID_EXTRACTOR_VERSION:-}")
fi
for version in "${tool_versions[@]}"; do
    [[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || {
        echo 'Rust tools require exact stable versions' >&2; exit 2;
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

# Both entry points use Cargo's locked registry installer. Selected targets add
# explicit target/profile arguments; the established formatter bundle is intact.
install_cargo_tool() {
    local package_name="$1" version="$2" destination="$3"; shift 3
    cargo install "$package_name" --version "=$version" --locked \
        --root "$destination" --target-dir "$destination/build" "$@"
}

selected_paths() {
    local directory="$1" path
    for path in "$directory" "$directory/bin" "$directory/build"; do
        [[ ! -L "$path" && ( ! -e "$path" || -d "$path" ) ]] || {
            printf 'Cargo tool directory must be physical: %s\n' "$path" >&2; return 1;
        }
    done
    for path in "$directory/bin/$target" "$directory/.crates.toml" \
        "$directory/.crates2.json" "$directory/selection.json"; do
        [[ ! -L "$path" && ( ! -e "$path" || -f "$path" ) ]] || {
            printf 'Cargo tool file must be regular: %s\n' "$path" >&2; return 1;
        }
    done
}

selected_receipt() {
    jq -se --arg identity "$package $selected_version (registry+https://github.com/rust-lang/crates.io-index)" \
        --arg version "=$selected_version" --arg target "$target" --arg profile "$profile" --arg host "$host" '
        length == 1 and (.[0] | (.installs | keys) == [$identity] and
        (.installs[$identity] | .version_req == $version and .bins == [$target] and
          (if $profile == "debug" then (.profile == "dev" or .profile == "debug")
           else .profile == "release" end) and .target == $host and
          (.rustc | type == "string" and length > 0)))
    ' "$1/.crates2.json" > /dev/null
}

selected_identity() {
    local directory="$1" binary_digest receipt_digest
    binary_digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$directory/bin/$target")" || return 1
    receipt_digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$directory/.crates2.json")" || return 1
    jq -n --arg package "$package" --arg version "$selected_version" --arg kind "$kind" \
        --arg target "$target" --arg profile "$profile" --arg host "$host" \
        --arg binary_sha256 "$binary_digest" --arg receipt_sha256 "$receipt_digest" \
        '{package:$package,version:$version,kind:$kind,target:$target,profile:$profile,
          host:$host,binary_sha256:$binary_sha256,receipt_sha256:$receipt_sha256}'
}

check_selected() {
    selected_paths "$destination" || return 1
    [[ -x "$destination/bin/$target" ]] || return 1
    selected_receipt "$destination" || return 1
    local expected
    expected="$(selected_identity "$destination")" || return 1
    jq -se --argjson expected "$expected" 'length == 1 and .[0] == $expected' "$destination/selection.json" > /dev/null
}

install_selected() (
    stage=''
    # Versioned selections preserve earlier usable installations. No candidate
    # is visible at the admitted destination before receipt and byte admission.
    slot="$install_root/$package-$selected_version-$kind-$target-$profile"
    destination="$slot/installed"
    lock="$slot/install.lock"
    selected_paths "$slot"
    selected_paths "$destination"
    [[ ! -e "$lock" && ! -L "$lock" ]] || { echo "Cargo tool installation locked: $lock" >&2; return 1; }
    host="$(rustc -vV | sed -n 's/^host: //p')"
    [[ -n "$host" ]]
    if [[ -e "$destination" ]]; then
        check_selected || {
            printf 'invalid selected Cargo tool: package=%s version=%s target=%s:%s profile=%s destination=%s\n' \
                "$package" "$selected_version" "$kind" "$target" "$profile" "$destination" >&2
            return 1
        }
    else
        [[ "$check_only" == false ]] || {
            printf 'missing selected Cargo tool: package=%s version=%s target=%s:%s profile=%s destination=%s\n' \
                "$package" "$selected_version" "$kind" "$target" "$profile" "$destination" >&2
            return 1
        }
        mkdir -p "$slot"
        mkdir "$lock" 2>/dev/null || { echo "Cargo tool installation locked: $lock" >&2; return 1; }
        # An interrupted owner leaves the lock for inspection; never reclaim it
        # from a PID guess. Ordinary failure removes only this empty owned lock.
        trap 'rmdir "$lock"; if [[ -n "$stage" ]]; then printf "Cargo tool attempt retained: %s\n" "$stage" >&2; fi' EXIT
        trap 'exit 130' INT
        trap 'exit 143' TERM
        selected_paths "$slot"
        selected_paths "$destination"
        if [[ -e "$destination" ]]; then
            check_selected
        else
            # Keep failed candidates under the established CI evidence route.
            mkdir -p "$install_root/build"
            stage="$(mktemp -d "$install_root/build/cargo-attempt.XXXXXX")"
            selection=("--$kind" "$target" --registry crates-io)
            [[ "$profile" != debug ]] || selection+=(--debug)
            install_cargo_tool "$package" "$selected_version" "$stage" "${selection[@]}" > "$stage/install.log" 2>&1 || {
                status=$?
                cat "$stage/install.log" >&2 || :
                return "$status"
            }
            # Cargo is an external effect boundary. Admit the whole route again
            # before reading candidate receipts or activating its executable.
            check_install_paths
            selected_paths "$slot"
            selected_paths "$stage"
            [[ -x "$stage/bin/$target" ]]
            selected_receipt "$stage"
            [[ ! -e "$stage/selection.json" ]] || { echo 'unexpected candidate selection receipt' >&2; return 1; }
            selected_identity "$stage" > "$stage/selection.json"
            selected_paths "$destination"
            [[ ! -e "$destination" ]] || { echo 'Cargo tool destination appeared during install' >&2; return 1; }
            perl -e 'rename($ARGV[0], $ARGV[1]) or die "activate Cargo tool: $!\n"' "$stage" "$destination"
            stage=''
            check_selected
        fi
        rmdir "$lock"
        trap - EXIT INT TERM
    fi
    printf '%s\n' "$destination/bin/$target"
)

check_install_paths
if [[ "$selected" == true ]]; then
    install_selected
    exit 0
fi
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
    install_cargo_tool "$tool" "$version" "$install_root"
    check_install_paths
    check_tool "$tool" "$version" || {
        echo "installed $tool failed its version check" >&2; exit 1;
    }
done
printf '%s\n' "$install_root/bin"
