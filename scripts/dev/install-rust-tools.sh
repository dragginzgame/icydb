#!/usr/bin/env bash
# Shared companions: scripts/ci/verify-file-checksum.sh scripts/dev/tool-store.sh scripts/dev/cargo-tool-build.sh ci/tool-versions.env
set -euo pipefail

ROOT="$0"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
consumer="$ROOT"
versions_file="$ROOT/ci/tool-versions.env"
check_only=false
preflight=false
package='' selected_version='' lockfile='' kind='' target='' profile=''
usage() {
    echo 'usage: install-rust-tools.sh [--consumer DIR] [--versions FILE] [--check | --preflight]' >&2
    echo '   or: install-rust-tools.sh [--consumer DIR] --package NAME (--version X.Y.Z | --lockfile FILE) (--bin NAME | --example NAME) --profile (debug | release) [--check]' >&2
}
while [[ $# -gt 0 ]]; do
    case "$1" in
        --consumer|--versions)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            if [[ "$1" == --consumer ]]; then consumer="$2"; else versions_file="$2"; fi
            shift 2 ;;
        --check) check_only=true; shift ;;
        --preflight) preflight=true; shift ;;
        --package|--version|--lockfile|--bin|--example|--profile)
            [[ $# -ge 2 && -n "$2" ]] || { usage; exit 2; }
            case "$1" in
                --package) package="$2" ;;
                --version) selected_version="$2" ;;
                --lockfile) lockfile="$2" ;;
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
[[ "$check_only" == false || "$preflight" == false ]] || { usage; exit 2; }
[[ "$consumer" == /* ]] || consumer="$PWD/$consumer"
case "$consumer" in *$'\n'*|*$'\r'*) echo 'consumer path contains CR/LF' >&2; exit 1 ;; esac
consumer="$(cd -P "$consumer" && printf '%s/.' "$PWD")"
consumer="${consumer%/.}"
case "$consumer" in *$'\n'*|*$'\r'*) echo 'resolved consumer path contains CR/LF' >&2; exit 1 ;; esac
selection_consumer="$consumer"
if [[ "$consumer" == "${SHARED_TOOLING_FORMAT_WORKTREE:-}" ]]; then
    [[ "$check_only" == true && -n "${SHARED_TOOLING_FORMAT_TOOLS_FROM:-}" ]] || {
        echo 'format exports only admit prepared Rust tools with --check; run explicit setup in the checkout' >&2
        exit 2
    }
    selection_consumer="$(cd -P "$SHARED_TOOLING_FORMAT_TOOLS_FROM" && printf '%s/.' "$PWD")"
    selection_consumer="${selection_consumer%/.}"
fi
export PATH="$consumer/.tools/host/bin:$PATH"

read_locked_version() (
    [[ -f "$lockfile" && ! -L "$lockfile" ]] || {
        printf 'Cargo tool selection requires a regular lockfile: %s\n' "$lockfile" >&2
        return 2
    }
    # Reuse explicitly prepared host tools; never resolve/build a Cargo graph.
    yq -p toml -o json -I 0 '.' "$lockfile" | jq -ers --arg package "$package" '
        if length != 1 then error("expected one lock document") else .[0] end
        | [.package[] | select(.name == $package)]
        | if length != 1 then error("expected exactly one locked package: " + $package) else .[0] end
        | if .source != "registry+https://github.com/rust-lang/crates.io-index"
          then error("Cargo tool requires a published crates.io package") else . end
        | .version
        | if type != "string" then error("locked version must be a string") else . end
        | if test("^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$") and (contains("\n") | not)
          then . else error("Cargo tool requires an exact stable version") end
    ' || return $?
)

check_locked_selection() {
    [[ -n "$lockfile" ]] || return 0
    local observed
    observed="$(read_locked_version)" || return $?
    [[ "$observed" == "$selected_version" ]] || {
        printf 'Cargo tool lock selection changed: package=%s expected=%s observed=%s; retry with the current lockfile\n' \
            "$package" "$selected_version" "$observed" >&2
        return 1
    }
}

selected=false
if [[ -n "$package$selected_version$lockfile$kind$target$profile" ]]; then
    [[ "$preflight" == false ]] || { usage; exit 2; }
    selected=true
    [[ "$package" =~ ^[a-zA-Z0-9][a-zA-Z0-9_-]*$ &&
       "$target" =~ ^[a-zA-Z0-9][a-zA-Z0-9_-]*$ && -n "$kind" &&
       ( "$profile" == debug || "$profile" == release ) ]] || { usage; exit 2; }
    if [[ -n "$lockfile" ]]; then
        [[ -z "$selected_version" ]] || { usage; exit 2; }
        [[ "$lockfile" == /* ]] || lockfile="$consumer/$lockfile"
        selected_version="$(read_locked_version)" || exit $?
    fi
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
install_root="$selection_consumer/.tools/rust"
export RUSTUP_AUTO_INSTALL=0
# shellcheck source=scripts/dev/tool-store.sh
source "$ROOT/scripts/dev/tool-store.sh"
# shellcheck source=scripts/dev/cargo-tool-build.sh
source "$ROOT/scripts/dev/cargo-tool-build.sh"

check_install_paths() {
    local path
    # Admit the whole fixed route before any tool probe or Cargo dispatch. Do
    # not follow even dangling links, or inspect unrelated host/IC bundle links.
    for path in "$selection_consumer/.tools" "$install_root" "$install_root/build"; do
        if [[ -L "$path" || ( -e "$path" && ! -d "$path" ) ]]; then
            printf 'Rust tool directory must be a physical directory: %s\n' "$path" >&2
            return 1
        fi
    done
}

check_tool() {
    local executable="$1" tool="$2" version="$3" actual
    local arguments=(--version)
    [[ "$tool" != cargo-sort-derives ]] || arguments=(sort-derives --version)
    [[ -x "$executable" ]] &&
        actual="$("$executable" "${arguments[@]}")" &&
        [[ "$actual" == "$tool $version" ]]
}

# Shared-store entries carry exact source, compiler and standard build inputs.
selected_paths() {
    local directory="$1" path
    for path in "$directory" "$directory/bin" "$directory/build"; do
        [[ ! -L "$path" && ( ! -e "$path" || -d "$path" ) ]] || {
            printf 'Cargo tool directory must be physical: %s\n' "$path" >&2; return 1;
        }
    done
    for path in "$directory/bin/$target" "$directory/.crates.toml" \
        "$directory/.crates2.json" "$directory/selection.json" "$directory/inputs.json" \
        "$directory/source.crate" "$directory/Cargo.lock" "$directory/native-environment.txt" \
        "$directory/build-identity.observed.json" "$directory/native-environment.observed.txt"; do
        [[ ! -L "$path" && ( ! -e "$path" || -f "$path" ) ]] || {
            printf 'Cargo tool file must be regular: %s\n' "$path" >&2; return 1;
        }
    done
}

selected_receipt() {
    jq -se --arg identity "$package $selected_version (registry+https://github.com/rust-lang/crates.io-index)" \
        --arg version "=$selected_version" --arg target "$target" --arg profile "$profile" --arg host "$host" \
        --arg compiler "$compiler_version" '
        length == 1 and (.[0] | (.installs | keys) == [$identity] and
        (.installs[$identity] | .version_req == $version and .bins == [$target] and
          (if $profile == "debug" then (.profile == "dev" or .profile == "debug")
           else .profile == "release" end) and .target == $host and
          (.rustc | rtrimstr("\n")) == $compiler))
    ' "$1/.crates2.json" > /dev/null
}

selected_identity() {
    local directory="$1" binary_digest receipt_digest
    binary_digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$directory/bin/$target")" || return 1
    receipt_digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$directory/.crates2.json")" || return 1
    jq -cnS --argjson inputs "$inputs" --arg binary_sha256 "$binary_digest" --arg receipt_sha256 "$receipt_digest" \
        '{inputs:$inputs,binary_sha256:$binary_sha256,receipt_sha256:$receipt_sha256}'
}

check_selected() {
    selected_paths "$destination" || return 1
    [[ -x "$destination/bin/$target" ]] || return 1
    jq -se --argjson expected "$inputs" 'length == 1 and .[0] == $expected' "$destination/inputs.json" > /dev/null || return 1
    bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$(jq -r .source_sha256 <<< "$inputs")" "$destination/source.crate" >&2 || return 1
    bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$(jq -r .lock_sha256 <<< "$inputs")" "$destination/Cargo.lock" >&2 || return 1
    bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$(jq -r .build.native <<< "$inputs")" "$destination/native-environment.txt" >&2 || return 1
    selected_receipt "$destination" || return 1
    local expected
    expected="$(selected_identity "$destination")" || return 1
    jq -se --argjson expected "$expected" 'length == 1 and .[0] == $expected' "$destination/selection.json" > /dev/null || return 1
    [[ "$selected" == true ]] || check_tool "$destination/bin/$target" "$target" "$selected_version"
}

install_selected() (
    stage=''
    store_lock=''
    lock_owned=false
    # Common setup captures this function in a conditional substitution, where
    # Bash disables errexit. Propagate every fallible step explicitly, including
    # preparation before the checkout lock is acquired.
    # shellcheck disable=SC2329 # Invoked by the EXIT trap below.
    finish_selected() {
        local status=$? cleanup_status=0
        trap - EXIT
        if [[ "$lock_owned" == true ]]; then
            rm -f "$lock/selected" || cleanup_status=$?
            rmdir "$lock" || cleanup_status=$?
        fi
        tool_store_release || cleanup_status=$?
        [[ "$status" != 0 ]] || status=$cleanup_status
        if [[ "$status" != 0 && -n "$stage" ]]; then printf 'Cargo tool attempt retained: %s\n' "$stage" >&2; fi
        exit "$status"
    }
    trap finish_selected EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    cargo_source || exit $?
    tool_store_init rust "$inputs" || exit $?
    tool_root="$install_root"
    active="$install_root/selected-$store_id"
    lock="$install_root/.selected-$store_id.lock"
    [[ ! -e "$lock" && ! -L "$lock" ]] || { echo "Cargo tool installation locked: $lock" >&2; exit 1; }
    if [[ -L "$active" ]]; then
        reference="$(perl -e 'my $p=readlink(shift); defined $p or exit 1; print "$p/."' "$active")" || exit $?
        reference="${reference%/.}"
        [[ "$reference" =~ ^rust-set\.[[:alnum:]]+$ ]] || { echo 'invalid Cargo tool reference' >&2; exit 1; }
        if destination="$(tool_store_resolve "$install_root/$reference" "$store_slot")" &&
            tool_store_unlocked && check_selected; then
            check_locked_selection || exit $?
            printf '%s/bin/%s\n' "$active" "$target" || exit $?
            exit 0
        fi
    elif [[ -e "$active" ]]; then
        echo "unmanaged Cargo tool selection: $active" >&2; exit 1
    fi
    [[ "$check_only" == false ]] || { echo "missing or invalid shared Cargo tool: $package $selected_version ($kind:$target, $profile); run explicit setup" >&2; exit 1; }
    mkdir -p "$install_root" || exit $?
    mkdir "$lock" || exit $?
    lock_owned=true
    tool_store_acquire || exit $?
    if destination="$(tool_store_cached)" && check_selected; then
        tool_store_reference "$destination" || exit $?
    else
        tool_store_new || exit $?
        selected_paths "$stage" || exit $?
        printf '%s\n' "$inputs" > "$stage/inputs.json" || exit $?
        printf '%s\n' "$packaged_lock" > "$stage/Cargo.lock" || exit $?
        printf '%s\n' "$native_identity" > "$stage/native-environment.txt" || exit $?
        cp "$source_archive" "$stage/source.crate" || exit $?
        options=(); [[ "$profile" != debug ]] || options=(--debug)
        cargo_build "$stage" ${options[@]+"${options[@]}"} > "$stage/install.log" 2>&1 || {
            status=$?; cat "$stage/install.log" >&2 || :; exit "$status";
        }
        check_install_paths || exit $?
        selected_paths "$stage" || exit $?
        selected_receipt "$stage" || exit $?
        # Refuse a changed source/compiler/native environment before publication.
        expected_build="$build_identity"
        cargo_build_init || exit $?
        if [[ "$expected_build" != "$build_identity" ]]; then
            # Retain the exact observation used to refuse activation. Do not
            # sample again: compiler/native state may already have changed.
            printf '%s\n' "$build_identity" > "$stage/build-identity.observed.json" || exit 1
            printf '%s\n' "$native_identity" > "$stage/native-environment.observed.txt" || exit 1
            changed_fields="$(jq -nr --argjson expected "$expected_build" --argjson observed "$build_identity" \
                '[$expected | keys[] as $key | select(.[$key] != $observed[$key]) | $key] | join(", ")')" || exit 1
            printf 'Cargo tool build environment changed: %s\n' "$changed_fields" >&2
            printf 'Compare inputs.json (.build) and native-environment.txt with build-identity.observed.json and native-environment.observed.txt in %s\n' "$stage" >&2
            exit 1
        fi
        cargo_cache_check || exit $?
        bash "$ROOT/scripts/ci/verify-file-checksum.sh" sha256 "$(jq -r .source_sha256 <<< "$inputs")" "$source_archive" || exit $?
        selected_identity "$stage" > "$stage/selection.json" || exit $?
        destination="$stage"
        check_selected || exit $?
    fi
    check_locked_selection || exit $?
    tool_store_activate "$destination" || exit $?
    printf '%s/bin/%s\n' "$active" "$target"
)

common_paths() {
    check_install_paths || return 1
    local active="$install_root/common" name path
    if [[ -L "$active" ]]; then
        name="$(readlink "$active" && printf '.')" || return 1
        name="${name%.}"; name="${name%$'\n'}"
        [[ "$name" =~ ^common-set\.[[:alnum:]]+$ ]] || return 1
        for path in "$install_root/$name" "$install_root/$name/bin"; do
            [[ -d "$path" && ! -L "$path" ]] || return 1
        done
    else
        [[ ! -e "$active" ]] || return 1
    fi
}

check_common_links() {
    common_paths || return 1
    [[ -L "$install_root/common" ]] || return 1
    local index link expected
    for index in "${!tool_names[@]}"; do
        link="$install_root/common/bin/${tool_names[$index]}"
        expected="../../${common_selections[$index]#"$install_root/"}"
        [[ -L "$link" && "$(readlink "$link" && printf '.')" == "$expected"$'\n.' ]] || return 1
    done
}

install_common() (
    common_paths || { echo 'invalid common Rust tool PATH selection' >&2; exit 1; }
    common_lock="$install_root/common.lock"
    [[ ! -e "$common_lock" && ! -L "$common_lock" ]] || { echo 'Common Rust tool installation locked' >&2; exit 1; }
    if [[ "$check_only" == false ]]; then
        mkdir -p "$install_root" || exit $?
        mkdir "$common_lock" || exit $?
        trap 'rm -f "$common_lock/selected"; rmdir "$common_lock"' EXIT
        trap 'exit 130' INT
        trap 'exit 143' TERM
    fi
    common_selections=()
    for index in "${!tool_names[@]}"; do
        package="${tool_names[$index]}"; target="$package"
        selected_version="${tool_versions[$index]}"; kind=bin; profile=release
        installed="$(install_selected)" || exit $?
        common_selections+=("$installed")
    done
    if ! check_common_links; then
        [[ "$check_only" == false ]] || { echo 'missing or mismatched common Rust tool set; run make install-rust-tools' >&2; exit 1; }
        common_paths || exit $?
        bundle="$(mktemp -d "$install_root/common-set.XXXXXX")" || exit $?
        mkdir "$bundle/bin" || exit $?
        for index in "${!tool_names[@]}"; do
            ln -s "../../${common_selections[$index]#"$install_root/"}" "$bundle/bin/${tool_names[$index]}" || exit $?
        done
        ln -s "${bundle##*/}" "$common_lock/selected" || exit $?
        common_paths || exit $?
        perl -e 'rename($ARGV[0], $ARGV[1]) or die "activate common Rust tools: $!\n"' "$common_lock/selected" "$install_root/common" || exit $?
        check_common_links || exit $?
    fi
    printf '%s\n' "$install_root/common/bin"
)


check_install_paths
# Catalogs and lockfile paths above retain their documented caller/consumer
# resolution. Select the consumer's prepared compiler before entering the
# standard tool-build environment; application Cargo configuration stays local.
cd "$consumer"
if [[ "$preflight" == true ]]; then
    # Rustup selects by working directory. Probe the consumer's toolchain without
    # auto-installing one or touching existing tools, receipts or build output.
    for prerequisite in rustc cargo; do
        executable="$(command -v "$prerequisite")" || {
            printf 'missing Rust setup prerequisite: tool=%s; prepare the declared Rust/Cargo toolchain and select it on PATH, then rerun make install-tools\n' "$prerequisite" >&2
            exit 1
        }
        if output="$("$executable" --version 2>&1)"; then
            continue
        else
            status=$?
            printf 'unavailable Rust setup prerequisite: tool=%s path=%s status=%s\n%s\nPrepare the declared Rust/Cargo toolchain, then rerun make install-tools\n' \
                "$prerequisite" "$executable" "$status" "$output" >&2
            exit "$status"
        fi
    done
    exit 0
fi
cargo_build_init
if [[ "$selected" == true ]]; then
    install_selected
    exit 0
fi
install_common
