#!/usr/bin/env bash
# Shared companions: scripts/dev/tool-store.sh scripts/ci/verify-file-checksum.sh
# shellcheck disable=SC2154 # Sourced by the Cargo installer; caller owns selections.
# shellcheck disable=SC2034 # The installer consumes the prepared inputs and lock state.

cargo_native_identity() (
    export LC_ALL=C
    uname -srm || exit $?
    for native in /usr/bin/cc /usr/bin/c++ /usr/bin/ar /usr/bin/ld; do
        [[ -x "$native" ]] || { echo "missing native tool: $native" >&2; exit 1; }
        bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$native" || exit $?
    done
    /usr/bin/cc --version || exit $?
    case "$(uname -s)" in
        Linux)
            if [[ -x /usr/bin/dpkg-query ]]; then /usr/bin/dpkg-query -W -f='${Package}=${Version}\n'
            elif [[ -x /usr/bin/rpm ]]; then /usr/bin/rpm -qa | sort
            else echo 'Cargo sharing requires a qualified Linux native package inventory' >&2; exit 1; fi ;;
        Darwin)
            /usr/bin/sw_vers || exit $?
            /usr/bin/xcode-select -p || exit $?
            /usr/bin/xcrun --show-sdk-path || exit $?
            /usr/bin/xcrun --show-sdk-version ;;
        *) echo 'unsupported Cargo tool build host' >&2; exit 1 ;;
    esac
)

# Tool builds deliberately exclude project flags, wrappers, credentials and
# arbitrary environment variables. Resolve the consumer's prepared compiler
# first, then invoke its real binaries from a neutral working directory.
cargo_build_init() {
    compiler_root="$(rustc --print sysroot && printf '.')" || return 1
    compiler_root="${compiler_root%.}"; compiler_root="${compiler_root%$'\n'}"
    [[ "$compiler_root" == /* && "$compiler_root" != *$'\n'* && "$compiler_root" != *$'\r'* ]] || {
        echo 'Cargo tool compiler root must be an absolute path without CR/LF' >&2; return 1;
    }
    compiler_root="$(cd -P "$compiler_root" && printf '%s/.' "$PWD")" || return 1
    compiler_root="${compiler_root%/.}"
    [[ "$compiler_root" != *$'\n'* && "$compiler_root" != *$'\r'* ]] || {
        echo 'resolved Cargo tool compiler root contains CR/LF' >&2; return 1;
    }
    compiler="$compiler_root/bin/rustc"
    cargo_bin="$compiler_root/bin/cargo"
    [[ -x "$compiler" && -x "$cargo_bin" ]] || { echo 'prepared compiler must include rustc and cargo' >&2; return 1; }
    compiler_version="$("$compiler" -vV)" || return 1
    cargo_version="$("$cargo_bin" -vV)" || return 1
    host="$(printf '%s\n' "$compiler_version" | sed -n 's/^host: //p')"
    [[ "$host" =~ ^[A-Za-z0-9_-]+$ ]] || { echo 'invalid Cargo tool host' >&2; return 1; }
    store_root="$(tool_store_root)" || return 1
    store_root="${store_root%/.}"
    cargo_cache="$store_root/cargo-cache"
    build_path="$compiler_root/bin:/usr/bin:/bin:/usr/sbin:/sbin"
    build_environment=(env -i "PATH=$build_path" LANG=C LC_ALL=C TZ=UTC
        "CARGO_HOME=$cargo_cache" "RUSTC=$compiler" "RUSTDOC=$compiler_root/bin/rustdoc"
        RUSTUP_AUTO_INSTALL=0)
    # Network policy is an explicit Cargo flag, never a compiler environment input.
    offline_args=()
    local offline="${CARGO_NET_OFFLINE:-}" user_config user_home="${CARGO_HOME:-$HOME/.cargo}"
    if [[ -z "$offline" ]]; then
        # Preserve an explicitly configured offline policy without importing
        # unrelated user Cargo build settings into the standard environment.
        user_config="$user_home/config.toml"
        [[ ! -f "$user_home/config" ]] || user_config="$user_home/config"
        if [[ -f "$user_config" ]]; then
            offline="$(yq -p toml -o json '.' "$user_config" | jq -ers '
                # yq represents an empty TOML document as null.
                if length == 1 and (.[0] == null or (.[0] | type) == "object")
                then (.[0] // {}) else error("expected one Cargo configuration object") end
                | if has("net") then .net else {} end
                | if type != "object" then error("Cargo net configuration must be a table") else . end
                | if has("offline") then .offline else false end
                | if type == "boolean" then tostring
                  else error("Cargo net.offline must be a boolean") end
            ')" || return 1
        fi
    fi
    case "${offline:-false}" in
        true|1) offline_args=(--offline) ;;
        false|0) ;;
        *) echo 'CARGO_NET_OFFLINE must be true/false or 1/0' >&2; return 1 ;;
    esac
    # System tools/SDKs are the standard native build environment. Fingerprint
    # native package state too: upgrading libraries must select another build.
    native_identity="$(cargo_native_identity)" || return 1
    native_digest="$(printf '%s\n' "$native_identity" | perl -MDigest::SHA=sha256_hex -0777 -ne 'print sha256_hex($_)')" || return 1
    local compiler_digest cargo_digest policy_digest
    compiler_digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$compiler")" || return 1
    cargo_digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$cargo_bin")" || return 1
    policy_digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$ROOT/scripts/dev/cargo-tool-build.sh")" || return 1
    build_identity="$(jq -cnS --arg compiler "$compiler_version" --arg cargo "$cargo_version" \
        --arg compiler_path "$compiler_root" --arg native "$native_digest" --arg host "$host" \
        --arg compiler_sha256 "$compiler_digest" --arg cargo_sha256 "$cargo_digest" --arg policy "$policy_digest" \
        '{compiler:$compiler,cargo:$cargo,compiler_path:$compiler_path,native:$native,host:$host,
          compiler_sha256:$compiler_sha256,cargo_sha256:$cargo_sha256,policy_sha256:$policy,environment:"standard",jobs:2}')" || return 1
}

cargo_cache_check() {
    local path
    for path in "$store_root" "$cargo_cache"; do tool_store_directory "$path" || return 1; done
    # Cargo configuration is not part of this standard environment. Never read
    # a config injected into the shared cache, or reinterpret it as reusable.
    for path in "$cargo_cache/config" "$cargo_cache/config.toml"; do
        [[ ! -e "$path" && ! -L "$path" ]] || { echo "unexpected Cargo tool configuration: $path" >&2; return 1; }
    done
    # Cargo may populate these routes during info/install. Reject redirected
    # ancestors before dispatch, not only when reading the resulting archive.
    shopt -s nullglob
    for path in "$cargo_cache/registry" "$cargo_cache/registry/cache" "$cargo_cache/registry/src" \
        "$cargo_cache/registry/index" "$cargo_cache"/registry/cache/* \
        "$cargo_cache"/registry/src/* "$cargo_cache"/registry/src/*/* "$cargo_cache"/registry/index/*; do
        if [[ -e "$path" || -L "$path" ]]; then tool_store_directory "$path" || return 1; fi
    done
}

cargo_source() {
    local archives=() checksum lock_digest path
    if [[ "$check_only" == false ]]; then
        # Bootstrap is idempotent; Cargo owns serialization of source downloads.
        # Admit existing routes before creation and recheck them below before use.
        for path in "$store_root" "$cargo_cache"; do
            if [[ -e "$path" || -L "$path" ]]; then tool_store_directory "$path" || return $?; fi
        done
        (umask 077; mkdir -p "$cargo_cache") || return $?
    fi
    cargo_cache_check || return 1
    shopt -s nullglob
    archives=("$cargo_cache"/registry/cache/index.crates.io-*/"$package-$selected_version.crate")
    if [[ ${#archives[@]} == 0 && "$check_only" == false ]]; then
        # Cargo owns registry download/checksum admission; no second registry client.
        (umask 077; cd "$cargo_cache" && "${build_environment[@]}" "HOME=$cargo_cache" "$cargo_bin" info \
            "$package@$selected_version" --registry crates-io ${offline_args[@]+"${offline_args[@]}"}) >&2 || return $?
        archives=("$cargo_cache"/registry/cache/index.crates.io-*/"$package-$selected_version.crate")
    fi
    if [[ ${#archives[@]} == 0 ]]; then
        printf 'Cargo tool source is missing from %s: %s %s\n' "$cargo_cache/registry/cache" "$package" "$selected_version" >&2
        return 1
    elif [[ ${#archives[@]} != 1 ]]; then
        printf 'Cargo tool source is ambiguous for %s %s; cached archives:\n' "$package" "$selected_version" >&2
        printf '  %q\n' "${archives[@]}" >&2
        return 1
    fi
    [[ -f "${archives[0]}" && ! -L "${archives[0]}" ]] || {
        printf 'Cargo tool source must be a regular non-symlink archive: %s\n' "${archives[0]}" >&2
        return 1
    }
    source_archive="${archives[0]}"
    local parent="${source_archive%/*}"
    while [[ "$parent" != "$store_root" ]]; do
        tool_store_directory "$parent" || return 1
        parent="${parent%/*}"
    done
    checksum="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$source_archive")" || return 1
    packaged_lock="$(tar -xOzf "$source_archive" "$package-$selected_version/Cargo.lock")" || {
        echo 'shared Cargo tools require a published Cargo.lock' >&2; return 1;
    }
    # Refuse unresolved/non-registry dependency identities instead of inventing
    # cross-checkout equivalence for mutable Git/path overrides.
    printf '%s\n' "$packaged_lock" | yq -p toml -o json '.' | jq -e '
        (.package | type == "array" and length > 0) and
        all(.package[]; (.source == null) or
            (.source == "registry+https://github.com/rust-lang/crates.io-index" and
             (.checksum | type == "string" and test("^[0-9a-f]{64}$"))))' > /dev/null || return 1
    lock_digest="$(printf '%s\n' "$packaged_lock" | perl -MDigest::SHA=sha256_hex -0777 -ne 'print sha256_hex($_)')" || return 1
    inputs="$(jq -cnS --argjson build "$build_identity" --arg package "$package" --arg version "$selected_version" \
        --arg kind "$kind" --arg target "$target" --arg profile "$profile" --arg source "$checksum" --arg lock "$lock_digest" \
        --argjson features "${features_json:-[]}" \
        '{build:$build,registry:"crates-io",package:$package,version:$version,kind:$kind,target:$target,
          profile:$profile,source_sha256:$source,lock_sha256:$lock} +
          (if $features == [] then {} else {features:$features} end)')" || return 1
}

cargo_build() {
    local candidate="$1"; shift
    local feature_options=()
    [[ -z "${features_csv:-}" ]] || feature_options=(--features "$features_csv")
    cargo_cache_check || return 1
    mkdir "$candidate/home" || return 1
    # No caller PATH, flags, wrappers, Cargo config or arbitrary environment is
    # passed to the build. The exact packaged lockfile is required above.
    (umask 077; cd "$candidate/home" && "${build_environment[@]}" "HOME=$candidate/home" "$cargo_bin" install \
        "$package" --version "=$selected_version" --locked --registry crates-io \
        "--$kind" "$target" --root "$candidate" --target-dir "$candidate/build" \
        --jobs 2 "$@" ${feature_options[@]+"${feature_options[@]}"} ${offline_args[@]+"${offline_args[@]}"})
}
