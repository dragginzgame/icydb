#!/usr/bin/env bash
set -euo pipefail
ROOT="$0"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/rust-tools-test.XXXXXX")"
fixture="$(cd "$fixture" && pwd -P)"
trap 'if [[ $? == 0 ]]; then rm -rf "$fixture"; else printf "Failed Rust tool fixture retained: %s\n" "$fixture" >&2; fi' EXIT
mkdir -p "$fixture/bin" "$fixture/consumer with spaces & [glob]"
consumer="$fixture/consumer with spaces & [glob]"
cat > "$fixture/versions.env" <<'ENV'
SHARED_TOOLING_CARGO_SORT_VERSION=99.1.0
SHARED_TOOLING_CARGO_SORT_DERIVES_VERSION=99.2.0
SHARED_TOOLING_CANDID_EXTRACTOR_VERSION=99.3.0
ENV
export RUST_TOOL_FIXTURE_LOG="$fixture/install.log"
export RUST_TOOL_FIXTURE_PROBES="$fixture/probes.log"
cat > "$fixture/bin/cargo" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
[[ $# == 9 && "$1" == install && "$3" == --version && "$4" == =99.* && "$5" == --locked && "$6" == --root && "$8" == --target-dir && "$9" == "$7/build" ]]
[[ "$RUSTUP_AUTO_INSTALL" == 0 ]]
printf '%s\n' "$2 $4" >> "$RUST_TOOL_FIXTURE_LOG"
mkdir -p "$7/bin" "$9"
printf 'retained build evidence\n' > "$9/retained"
[[ "${RUST_TOOL_INSTALL_FAIL:-}" != "$2" ]] || exit 23
cat > "$7/bin/$2" <<TOOL
#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' '$2' >> "\$RUST_TOOL_FIXTURE_PROBES"
if [[ "$2" == cargo-sort-derives ]]; then
    [[ \$# == 2 && \$1 == sort-derives && \$2 == --version ]]
else
    [[ \$# == 1 && \$1 == --version ]]
fi
printf '%s %s\\n' '$2' '${4#=}'
exit "\${RUST_TOOL_VERSION_STATUS:-0}"
TOOL
chmod +x "$7/bin/$2"
if [[ "${RUST_TOOL_INSTALL_LINK:-}" == "$2" ]]; then
    mv "$7/bin/$2" "$7/linked-$2"
    ln -s "../linked-$2" "$7/bin/$2"
fi
SCRIPT
chmod +x "$fixture/bin/cargo"
export PATH="$fixture/bin:$PATH"
installer="$ROOT/scripts/dev/install-rust-tools.sh"
args=(--consumer "$consumer" --versions "$fixture/versions.env")
if bash "$installer" "${args[@]}" --check > "$fixture/absent.log" 2>&1; then exit 1; fi
[[ ! -e "$RUST_TOOL_FIXTURE_LOG" && ! -e "$consumer/.tools" ]]
bash "$installer" "${args[@]}" > "$fixture/install-path"
[[ "$(cat "$fixture/install-path")" == "$consumer/.tools/rust/bin" ]]
[[ "$(wc -l < "$RUST_TOOL_FIXTURE_LOG" | tr -d ' ')" == 3 ]]
cp "$RUST_TOOL_FIXTURE_LOG" "$fixture/expected-log"
bash "$installer" "${args[@]}" --check > "$fixture/check-path"
bash "$installer" "${args[@]}" > "$fixture/repeated-path"
cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/expected-log"
cmp "$fixture/install-path" "$fixture/check-path"
# Matching version text from a failed executable is never accepted.
if RUST_TOOL_VERSION_STATUS=23 bash "$installer" "${args[@]}" --check > "$fixture/failed-probe.log" 2>&1; then exit 1; fi
cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/expected-log"
printf 'SHARED_TOOLING_CANDID_EXTRACTOR_VERSION=latest\n' >> "$fixture/versions.env"
if bash "$installer" "${args[@]}" > "$fixture/invalid.log" 2>&1; then exit 1; fi
cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/expected-log"
printf 'SHARED_TOOLING_CANDID_EXTRACTOR_VERSION=99.3.0\n' >> "$fixture/versions.env"
mkdir "$fixture/failed-consumer"
status=0
RUST_TOOL_INSTALL_FAIL=cargo-sort-derives bash "$installer" \
    --consumer "$fixture/failed-consumer" --versions "$fixture/versions.env" \
    > "$fixture/failed-install.log" 2>&1 || status=$?
[[ "$status" == 23 && -f "$fixture/failed-consumer/.tools/rust/build/retained" ]]
[[ -x "$fixture/failed-consumer/.tools/rust/bin/cargo-sort" && ! -e "$fixture/failed-consumer/.tools/rust/bin/candid-extractor" ]]

# Retry keeps the earlier tool and build evidence, then completes the set.
cp "$fixture/failed-consumer/.tools/rust/build/retained" "$fixture/expected-retained"
bash "$installer" --consumer "$fixture/failed-consumer" --versions "$fixture/versions.env" > "$fixture/retry.log"
bash "$installer" --consumer "$fixture/failed-consumer" --versions "$fixture/versions.env" --check > "$fixture/retry-check.log"
cmp "$fixture/failed-consumer/.tools/rust/build/retained" "$fixture/expected-retained"

# Check the complete route before probing even the first tool. Existing and
# dangling links both redirect installation; regular files cannot be parents.
for selected in .tools .tools/rust .tools/rust/bin .tools/rust/build \
    .tools/rust/bin/cargo-sort .tools/rust/bin/cargo-sort-derives .tools/rust/bin/candid-extractor \
    .tools/rust/.crates.toml .tools/rust/.crates2.json; do
    for shape in symlink dangling wrong-type; do
        probe="$fixture/boundary-${selected//\//-}-$shape"
        mkdir -p "$probe/consumer" "$probe/outside"
        cp -R "$consumer/.tools" "$probe/consumer/"
        case "$selected" in
            */.crates*) printf 'retained Cargo receipt\n' > "$probe/consumer/$selected" ;;
        esac
        mv "$probe/consumer/$selected" "$probe/outside/original"
        case "$shape" in
            symlink) ln -s "$probe/outside/original" "$probe/consumer/$selected" ;;
            dangling) ln -s "$probe/outside/missing" "$probe/consumer/$selected" ;;
            wrong-type)
                if [[ -d "$probe/outside/original" ]]; then
                    printf 'not a directory\n' > "$probe/consumer/$selected"
                else
                    mkdir "$probe/consumer/$selected"
                fi ;;
        esac
        cp -R "$probe/outside" "$probe/expected-outside"
        for mode in install check; do
            mode_args=()
            [[ "$mode" != check ]] || mode_args=(--check)
            status=0
            RUST_TOOL_FIXTURE_LOG="$probe/$mode-install.log" \
                RUST_TOOL_FIXTURE_PROBES="$probe/$mode-probes.log" \
                bash "$installer" --consumer "$probe/consumer" --versions "$fixture/versions.env" \
                ${mode_args[@]+"${mode_args[@]}"} > "$probe/$mode.log" 2>&1 || status=$?
            [[ "$status" == 1 && ! -e "$probe/$mode-install.log" && ! -e "$probe/$mode-probes.log" ]] || {
                printf 'Rust path boundary accepted/dispatched %s (%s, %s)\n' "$selected" "$shape" "$mode" >&2
                exit 1
            }
            diff -r "$probe/expected-outside" "$probe/outside"
        done
    done
done

# A newly produced link must be refused before its version probe, preserving
# the install candidate and build evidence for diagnosis.
mkdir "$fixture/linked-result"
status=0
RUST_TOOL_INSTALL_LINK=cargo-sort RUST_TOOL_FIXTURE_PROBES="$fixture/linked-result-probes.log" \
    bash "$installer" --consumer "$fixture/linked-result" --versions "$fixture/versions.env" \
    > "$fixture/linked-result.log" 2>&1 || status=$?
[[ "$status" == 1 && ! -e "$fixture/linked-result-probes.log" ]]
[[ -L "$fixture/linked-result/.tools/rust/bin/cargo-sort" && -f "$fixture/linked-result/.tools/rust/build/retained" ]]

# The selected checkout may be addressed through an alias. The Rust route below
# its physical root must stay local, without rejecting the other managed sets.
mkdir -p "$consumer/.tools/host-set.fixture" "$consumer/.tools/ic-set.fixture"
ln -s host-set.fixture "$consumer/.tools/host"
ln -s ic-set.fixture "$consumer/.tools/ic"
ln -s "$consumer" "$fixture/consumer-alias"
cp "$RUST_TOOL_FIXTURE_LOG" "$fixture/before-alias-install.log"
bash "$installer" --consumer "$fixture/consumer-alias" --versions "$fixture/versions.env" > "$fixture/alias-path"
bash "$installer" --consumer "$fixture/consumer-alias" --versions "$fixture/versions.env" --check > "$fixture/alias-check-path"
cmp "$fixture/install-path" "$fixture/alias-path"
cmp "$fixture/alias-path" "$fixture/alias-check-path"
cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/before-alias-install.log"
[[ "$(readlink "$consumer/.tools/host")" == host-set.fixture && "$(readlink "$consumer/.tools/ic")" == ic-set.fixture ]]
echo 'Pinned Rust tool setup, offline checks and failure retention passed (substitute Cargo)'
