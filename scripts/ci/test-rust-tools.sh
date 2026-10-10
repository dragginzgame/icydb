#!/usr/bin/env bash
# Shared companions: scripts/dev/install-rust-tools.sh scripts/ci/verify-file-checksum.sh
set -euo pipefail
ROOT="$0"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/rust-tools-test.XXXXXX")"
fixture="$(cd "$fixture" && pwd -P)"
# Bash 3.2 can enter EXIT with status zero after nounset; require completion too.
fixture_complete=false
finish() {
    local status=$?
    [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
    if [[ "$status" == 0 ]]; then rm -rf "$fixture"
    else printf "Failed Rust tool fixture retained: %s\n" "$fixture" >&2; fi
    exit "$status"
}
trap finish EXIT
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
[[ $# == 9 && "$1" == install && "$3" == --version && "$4" == =99.* && "$5" == --locked && "$6" == --root && "$8" == --target-dir && "$9" == "$7/build" ]] || exit 1
[[ "$RUSTUP_AUTO_INSTALL" == 0 ]] || exit 1
printf '%s\n' "$2 $4" >> "$RUST_TOOL_FIXTURE_LOG"
mkdir -p "$7/bin" "$9"
printf 'retained build evidence\n' > "$9/retained"
[[ "${RUST_TOOL_INSTALL_FAIL:-}" != "$2" ]] || exit 23
cat > "$7/bin/$2" <<TOOL
#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' '$2' >> "\$RUST_TOOL_FIXTURE_PROBES"
if [[ "$2" == cargo-sort-derives ]]; then
    [[ \$# == 2 && \$1 == sort-derives && \$2 == --version ]] || exit 1
else
    [[ \$# == 1 && \$1 == --version ]] || exit 1
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
[[ ! -e "$RUST_TOOL_FIXTURE_LOG" && ! -e "$consumer/.tools" ]] || exit 1
bash "$installer" "${args[@]}" > "$fixture/install-path"
[[ "$(cat "$fixture/install-path")" == "$consumer/.tools/rust/bin" ]] || exit 1
[[ "$(wc -l < "$RUST_TOOL_FIXTURE_LOG" | tr -d ' ')" == 3 ]] || exit 1
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
[[ "$status" == 23 && -f "$fixture/failed-consumer/.tools/rust/build/retained" ]] || exit 1
[[ -x "$fixture/failed-consumer/.tools/rust/bin/cargo-sort" && ! -e "$fixture/failed-consumer/.tools/rust/bin/candid-extractor" ]] || exit 1

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
[[ "$status" == 1 && ! -e "$fixture/linked-result-probes.log" ]] || exit 1
[[ -L "$fixture/linked-result/.tools/rust/bin/cargo-sort" && -f "$fixture/linked-result/.tools/rust/build/retained" ]] || exit 1

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
[[ "$(readlink "$consumer/.tools/host")" == host-set.fixture && "$(readlink "$consumer/.tools/ic")" == ic-set.fixture ]] || exit 1
echo 'Pinned Rust tool setup, offline checks and failure retention passed (substitute Cargo)'

# Selected targets need Cargo provenance and local byte identity, not --version.
cat > "$fixture/bin/rustc" <<'RUSTC'
#!/usr/bin/env bash
[[ "${SELECTED_HOST_EMPTY:-}" != yes ]] || exit 0
printf 'host: fixture-host\n'
RUSTC
cat > "$fixture/bin/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
[[ $# == 13 || $# == 14 ]] || exit 1
[[ "$1" == install && "$3" == --version && "$5" == --locked && "$6" == --root && "$8" == --target-dir ]] || exit 1
[[ "$9" == "$7/build" && "${12}" == --registry && "${13}" == crates-io ]] || exit 1
[[ "${10}" == --bin || "${10}" == --example ]] || exit 1
printf 'install\n' >> "$RUST_TOOL_FIXTURE_LOG"
mkdir -p "$7/bin" "$9"
printf 'retained build\n' > "$9/evidence"
if [[ -n "${SELECTED_PAUSE:-}" ]]; then
    touch "$SELECTED_PAUSE/started"
    while [[ ! -f "$SELECTED_PAUSE/continue" ]]; do sleep 0.1; done
fi
[[ "${SELECTED_FAIL:-}" != build ]] || exit 23
# Executing this target during admission would be a test failure.
printf '#!/usr/bin/env bash\nexit 97\n' > "$7/bin/${11}"
chmod +x "$7/bin/${11}"
profile=release
[[ "${14:-}" != --debug ]] || profile="${SELECTED_DEBUG_PROFILE:-debug}"
jq -n --arg identity "$2 ${4#=} (registry+https://github.com/rust-lang/crates.io-index)" \
    --arg version "$4" --arg target "${11}" --arg profile "$profile" '
    {installs:{($identity):{version_req:$version,bins:[$target],profile:$profile,
     target:"fixture-host",rustc:"fixture rustc"}}}' > "$7/.crates2.json"
printf 'Cargo receipt\n' > "$7/.crates.toml"
case "${SELECTED_FAIL:-}" in
    receipt) printf '{}\n' > "$7/.crates2.json" ;;
    receipt-stream)
        cp "$7/.crates2.json" "$7/original.json"
        printf '{}\n' > "$7/.crates2.json"
        cat "$7/original.json" >> "$7/.crates2.json" ;;
    binary-link) mv "$7/bin/${11}" "$7/original"; ln -s ../original "$7/bin/${11}" ;;
    binary-nonexec) chmod -x "$7/bin/${11}" ;;
    receipt-link) mv "$7/.crates2.json" "$7/original"; ln -s original "$7/.crates2.json" ;;
esac
if [[ -n "${SELECTED_REDIRECT:-}" ]]; then
    mv "$SELECTED_REDIRECT" "$SELECTED_REDIRECT.original"
    ln -s "${SELECTED_REDIRECT##*/}.original" "$SELECTED_REDIRECT"
fi
CARGO
chmod +x "$fixture/bin/rustc" "$fixture/bin/cargo"
mkdir "$fixture/selected"
selected_args=(--consumer "$fixture/selected" --package sample --version 1.2.3 --example prepare --profile debug)
slot="$fixture/selected/.tools/rust/sample-1.2.3-example-prepare-debug"
cp "$RUST_TOOL_FIXTURE_LOG" "$fixture/before-selected"
status=0
bash "$installer" "${selected_args[@]}" --check > "$fixture/selected-absent.out" 2> "$fixture/selected-absent.log" || status=$?
[[ "$status" == 1 && ! -s "$fixture/selected-absent.out" && ! -e "$fixture/selected/.tools" ]] || exit 1
for field in package=sample version=1.2.3 target=example:prepare profile=debug "destination=$slot/installed"; do
    grep -F -- "$field" "$fixture/selected-absent.log" > /dev/null
done
cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/before-selected"
# An absent host identity must stop before Cargo or destination preparation.
status=0
SELECTED_HOST_EMPTY=yes bash "$installer" "${selected_args[@]}" > "$fixture/empty-host.log" 2>&1 || status=$?
[[ "$status" != 0 && ! -e "$fixture/selected/.tools" ]] || exit 1
cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/before-selected"
SELECTED_DEBUG_PROFILE=dev bash "$installer" "${selected_args[@]}" > "$fixture/selected-path"
[[ "$(cat "$fixture/selected-path")" == "$slot/installed/bin/prepare" ]] || exit 1
cp "$RUST_TOOL_FIXTURE_LOG" "$fixture/selected-installs"
bash "$installer" "${selected_args[@]}" --check > "$fixture/selected-check" 2> "$fixture/selected-check.err"
[[ ! -s "$fixture/selected-check.err" ]] || exit 1
bash "$installer" "${selected_args[@]}" > "$fixture/selected-repeat"
cmp "$fixture/selected-path" "$fixture/selected-check"
cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/selected-installs"

# A checksum backend failure is not an empty digest, even in conditional checks.
cp "$slot/installed/selection.json" "$fixture/good-selection"
jq '.binary_sha256 = "" | .receipt_sha256 = ""' "$fixture/good-selection" > "$slot/installed/selection.json"
printf '#!/usr/bin/env bash\nexit 23\n' > "$fixture/bin/sha256sum"
chmod +x "$fixture/bin/sha256sum"
if bash "$installer" "${selected_args[@]}" --check > "$fixture/failed-checksum.log" 2>&1; then exit 1; fi
rm "$fixture/bin/sha256sum"
cp "$fixture/good-selection" "$slot/installed/selection.json"

# A valid final JSON document must not conceal an earlier conflicting receipt.
for file in selection.json .crates2.json; do
    cp "$slot/installed/$file" "$fixture/single-receipt"
    for prefix in '{}' 'null'; do
        printf '%s\n' "$prefix" > "$slot/installed/$file"
        cat "$fixture/single-receipt" >> "$slot/installed/$file"
        if [[ "$file" == .crates2.json ]]; then
            digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$slot/installed/$file")"
            jq --arg digest "$digest" '.receipt_sha256 = $digest' "$fixture/good-selection" > "$slot/installed/selection.json"
        fi
        for mode in install check; do
            extra=()
            [[ "$mode" != check ]] || extra=(--check)
            if bash "$installer" "${selected_args[@]}" ${extra[@]+"${extra[@]}"} > "$fixture/receipt-stream.log" 2>&1; then
                echo "multiple JSON documents accepted in $file ($mode)" >&2; exit 1
            fi
            cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/selected-installs"
        done
    done
    cp "$fixture/single-receipt" "$slot/installed/$file"
    cp "$fixture/good-selection" "$slot/installed/selection.json"
done

# Corruption is refused without executing Cargo or replacing the damaged bytes.
for file in bin/prepare .crates2.json selection.json; do
    cp "$slot/installed/$file" "$fixture/before"
    printf '\nchanged\n' >> "$slot/installed/$file"
    status=0
    bash "$installer" "${selected_args[@]}" > "$fixture/changed.out" 2> "$fixture/changed.log" || status=$?
    [[ "$status" == 1 && ! -s "$fixture/changed.out" ]] || exit 1
    for field in package=sample version=1.2.3 target=example:prepare profile=debug "destination=$slot/installed"; do
        grep -F -- "$field" "$fixture/changed.log" > /dev/null
    done
    cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/selected-installs"
    cp "$fixture/before" "$slot/installed/$file"
done
# Mismatched Cargo provenance is refused independently of the byte comparison.
cp "$slot/installed/.crates2.json" "$fixture/original-cargo-receipt"
for field in version_req bins profile target; do
    jq --arg field "$field" '.installs[][$field] = "wrong"' "$fixture/original-cargo-receipt" > "$slot/installed/.crates2.json"
    digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$slot/installed/.crates2.json")"
    jq --arg digest "$digest" '.receipt_sha256 = $digest' "$slot/installed/selection.json" > "$fixture/updated-selection"
    cp "$fixture/updated-selection" "$slot/installed/selection.json"
    if bash "$installer" "${selected_args[@]}" --check > "$fixture/wrong-$field.log" 2>&1; then exit 1; fi
done
cp "$fixture/original-cargo-receipt" "$slot/installed/.crates2.json"
digest="$(bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$slot/installed/.crates2.json")"
jq --arg digest "$digest" '.receipt_sha256 = $digest' "$slot/installed/selection.json" > "$fixture/updated-selection"
cp "$fixture/updated-selection" "$slot/installed/selection.json"

# Refuse redirected ancestors, output, receipts and lock before Cargo dispatch.
for relative in .tools .tools/rust sample-1.2.3-example-prepare-debug installed \
    installed/bin installed/build installed/bin/prepare installed/.crates.toml \
    installed/.crates2.json installed/selection.json install.lock; do
    case "$relative" in
        .tools*) path="$fixture/selected/$relative" ;;
        sample-*) path="$fixture/selected/.tools/rust/$relative" ;;
        *) path="$slot/$relative" ;;
    esac
    for shape in link dangling wrong-type; do
        [[ ! -e "$path" ]] || mv "$path" "$fixture/saved-path"
        case "$shape" in
            link) ln -s "$fixture/saved-path" "$path" ;;
            dangling) ln -s "$fixture/missing" "$path" ;;
            wrong-type)
                if [[ -f "$fixture/saved-path" ]]; then mkdir "$path"; else printf 'wrong\n' > "$path"; fi ;;
        esac
        for mode in install check; do
            extra=()
            [[ "$mode" != check ]] || extra=(--check)
            if bash "$installer" "${selected_args[@]}" ${extra[@]+"${extra[@]}"} > "$fixture/selected-path-refusal.log" 2>&1; then exit 1; fi
        done
        cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/selected-installs"
        if [[ -d "$path" && ! -L "$path" ]]; then rmdir "$path"; else rm "$path"; fi
        [[ ! -e "$fixture/saved-path" ]] || mv "$fixture/saved-path" "$path"
    done
done

# Failed builds/admission of a new selection preserve the previous usable one.
next_args=(--consumer "$fixture/selected" --package sample --version 1.2.4 --bin prepare --profile release)
next_slot="$fixture/selected/.tools/rust/sample-1.2.4-bin-prepare-release"
status=0
bash "$installer" "${next_args[@]}" --check > "$fixture/next-absent.out" 2> "$fixture/next-absent.log" || status=$?
[[ "$status" == 1 && ! -s "$fixture/next-absent.out" && ! -e "$next_slot" ]] || exit 1
for field in package=sample version=1.2.4 target=bin:prepare profile=release "destination=$next_slot/installed"; do
    grep -F -- "$field" "$fixture/next-absent.log" > /dev/null
done
cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/selected-installs"
failures=(build receipt receipt-stream binary-link binary-nonexec receipt-link)
for failure in "${failures[@]}"; do
    status=0
    SELECTED_FAIL="$failure" bash "$installer" "${next_args[@]}" > "$fixture/failure-$failure.log" 2>&1 || status=$?
    [[ "$status" != 0 ]] || exit 1
    [[ "$failure" != build || "$status" == 23 ]] || exit 1
    [[ ! -e "$next_slot/installed" && ! -e "$next_slot/install.lock" ]] || exit 1
    bash "$installer" "${selected_args[@]}" --check > /dev/null
done
[[ "$(find "$fixture/selected/.tools/rust/build" -name evidence | wc -l | tr -d ' ')" == "${#failures[@]}" ]] || exit 1

# The returned candidate is not admissible through a redirected ancestor, even
# when its own directory, executable and receipts are all regular files.
for relative in .tools .tools/rust .tools/rust/build sample-1.2.4-bin-prepare-release; do
    redirected="$fixture/redirected-${relative//\//-}"
    cp -R "$fixture/selected" "$redirected"
    redirect_path="$redirected/$relative"
    [[ "$relative" != sample-* ]] || redirect_path="$redirected/.tools/rust/$relative"
    status=0
    SELECTED_REDIRECT="$redirect_path" bash "$installer" --consumer "$redirected" \
        --package sample --version 1.2.4 --bin prepare --profile release \
        > "$redirected/refusal.log" 2>&1 || status=$?
    [[ "$status" != 0 && -L "$redirect_path" &&
       ! -e "$redirected/.tools/rust/sample-1.2.4-bin-prepare-release/installed" ]] || exit 1
    # Follow the deliberately moved fixture route only to compare preserved
    # original bytes and the failed build, never to execute the binary.
    cmp "$slot/installed/bin/prepare" "$redirected/.tools/rust/sample-1.2.3-example-prepare-debug/installed/bin/prepare"
    cmp "$slot/installed/selection.json" "$redirected/.tools/rust/sample-1.2.3-example-prepare-debug/installed/selection.json"
    [[ "$(find -L "$redirected/.tools/rust/build" -name evidence | wc -l | tr -d ' ')" == "$((${#failures[@]} + 1))" ]] || exit 1
done

# A competing installer cannot build or activate while the owner holds its lock.
mkdir "$fixture/pause"
SELECTED_PAUSE="$fixture/pause" bash "$installer" "${next_args[@]}" > "$fixture/concurrent-first.log" 2>&1 &
first=$!
for ((attempt=0; attempt<200; attempt++)); do
    [[ ! -e "$fixture/pause/started" ]] || break
    sleep 0.1
done
[[ -e "$fixture/pause/started" ]] || exit 1
cp "$RUST_TOOL_FIXTURE_LOG" "$fixture/before-contention"
if bash "$installer" "${next_args[@]}" > "$fixture/concurrent-second.log" 2>&1; then exit 1; fi
cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/before-contention"
touch "$fixture/pause/continue"
wait "$first"
bash "$installer" "${next_args[@]}" > "$fixture/concurrent-retry"
cmp "$fixture/concurrent-first.log" "$fixture/concurrent-retry"
cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/before-contention"

# Select from a real TOML lock with prepared yq/jq, without Cargo resolution.
locked_consumer="$fixture/locked consumer"
mkdir -p "$locked_consumer/graph"
locked_file="$locked_consumer/graph/Cargo.lock"
cat > "$locked_file" <<'LOCK'
version = 4

[[package]]
name = "sample"
version = "2.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "unrelated"
version = "1.0.0"
LOCK
cp "$locked_file" "$fixture/good.lock"
locked_args=(--consumer "$locked_consumer" --package sample --lockfile graph/Cargo.lock --bin prepare --profile release)
cp "$RUST_TOOL_FIXTURE_LOG" "$fixture/locked-before"
if bash "$installer" "${locked_args[@]}" --check > "$fixture/locked-missing.log" 2>&1; then exit 1; fi
cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/locked-before"
[[ ! -e "$locked_consumer/.tools" ]] || exit 1
bash "$installer" "${locked_args[@]}" > "$fixture/locked-path"
locked_slot="$locked_consumer/.tools/rust/sample-2.0.0-bin-prepare-release/installed"
[[ "$(cat "$fixture/locked-path")" == "$locked_slot/bin/prepare" ]] || exit 1
cp "$locked_slot/selection.json" "$fixture/locked-receipt"
cp "$RUST_TOOL_FIXTURE_LOG" "$fixture/locked-installs"
bash "$installer" "${locked_args[@]}" --check > "$fixture/locked-check"
bash "$installer" "${locked_args[@]}" > "$fixture/locked-reuse"
cmp "$fixture/locked-path" "$fixture/locked-check"
cmp "$fixture/locked-path" "$fixture/locked-reuse"
cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/locked-installs"
cmp "$locked_file" "$fixture/good.lock"

# Failed parsers cannot supply an apparently valid selection to setup/check.
for parser in yq jq; do
    real_parser="$(command -v "$parser")"
    cat > "$fixture/bin/$parser" <<'PARSER'
#!/usr/bin/env bash
"$LOCK_TEST_PARSER" "$@" || exit $?
echo 'injected lock parser failure' >&2
exit 23
PARSER
    chmod +x "$fixture/bin/$parser"
    for mode in setup check; do
        mode_args=()
        [[ "$mode" != check ]] || mode_args=(--check)
        status=0
        LOCK_TEST_PARSER="$real_parser" bash "$installer" "${locked_args[@]}" ${mode_args[@]+"${mode_args[@]}"} \
            > "$fixture/parser-$parser-$mode.out" 2> "$fixture/parser-$parser-$mode.log" || status=$?
        [[ "$status" == 23 && ! -s "$fixture/parser-$parser-$mode.out" ]] || exit 1
        cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/locked-installs"
    done
    rm "$fixture/bin/$parser"
done

# Absolute lock paths and example targets use the same selection contract.
sed 's/2.0.0/1.2.3/' "$fixture/good.lock" > "$fixture/example.lock"
bash "$installer" --consumer "$fixture/selected" --package sample --lockfile "$fixture/example.lock" \
    --example prepare --profile debug --check > "$fixture/locked-example"
cmp "$fixture/selected-path" "$fixture/locked-example"

for invalid in missing symlink directory malformed absent duplicate git path registry prerelease build number newline conflicting; do
    cp "$fixture/good.lock" "$locked_file"
    extra=()
    case "$invalid" in
        missing) rm "$locked_file" ;;
        symlink) rm "$locked_file"; ln -s "$fixture/good.lock" "$locked_file" ;;
        directory) rm "$locked_file"; mkdir "$locked_file" ;;
        malformed) printf '[[package\n' > "$locked_file" ;;
        absent) sed 's/name = "sample"/name = "absent"/' "$fixture/good.lock" > "$locked_file" ;;
        duplicate) sed -n '/^\[\[package\]\]/,$p' "$fixture/good.lock" >> "$locked_file" ;;
        git) sed 's,registry+https://github.com/rust-lang/crates.io-index,git+https://example.invalid/repo#123,' "$fixture/good.lock" > "$locked_file" ;;
        path) sed '/^source =/d' "$fixture/good.lock" > "$locked_file" ;;
        registry) sed 's,crates.io-index,other-index,' "$fixture/good.lock" > "$locked_file" ;;
        prerelease) sed 's/2.0.0/2.0.0-rc.1/' "$fixture/good.lock" > "$locked_file" ;;
        build) sed 's/2.0.0/2.0.0+build/' "$fixture/good.lock" > "$locked_file" ;;
        number) sed 's/"2.0.0"/2/' "$fixture/good.lock" > "$locked_file" ;;
        newline) sed 's/2.0.0/2.0.0\\n/' "$fixture/good.lock" > "$locked_file" ;;
        conflicting) extra=(--version 2.0.0) ;;
    esac
    for mode in setup check; do
        mode_args=()
        [[ "$mode" != check ]] || mode_args=(--check)
        if bash "$installer" "${locked_args[@]}" ${extra[@]+"${extra[@]}"} ${mode_args[@]+"${mode_args[@]}"} \
            > "$fixture/lock-$invalid-$mode.out" 2> "$fixture/lock-$invalid-$mode.log"; then exit 1; fi
        [[ ! -s "$fixture/lock-$invalid-$mode.out" ]] || exit 1
        cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/locked-installs"
        cmp "$locked_slot/selection.json" "$fixture/locked-receipt"
    done
    if [[ "$invalid" == directory ]]; then rmdir "$locked_file"
    else rm -f "$locked_file"; fi
done

# A lock change chooses a new slot; offline admission never installs it.
sed 's/2.0.0/2.0.1/' "$fixture/good.lock" > "$locked_file"
if bash "$installer" "${locked_args[@]}" --check > "$fixture/lock-updated.log" 2>&1; then exit 1; fi
cmp "$RUST_TOOL_FIXTURE_LOG" "$fixture/locked-installs"
grep -F 'version=2.0.1' "$fixture/lock-updated.log" > /dev/null

# Selection changes during Cargo installation retain the attempt, not activation.
mkdir "$fixture/lock-pause"
SELECTED_PAUSE="$fixture/lock-pause" bash "$installer" "${locked_args[@]}" > "$fixture/lock-changed.out" 2> "$fixture/lock-changed.log" &
first=$!
for ((attempt=0; attempt<200; attempt++)); do
    [[ ! -e "$fixture/lock-pause/started" ]] || break
    sleep 0.1
done
[[ -e "$fixture/lock-pause/started" ]] || exit 1
cp "$fixture/good.lock" "$locked_file"
touch "$fixture/lock-pause/continue"
if wait "$first"; then exit 1; fi
[[ ! -s "$fixture/lock-changed.out" && ! -e "$locked_consumer/.tools/rust/sample-2.0.1-bin-prepare-release/installed" ]] || exit 1
grep -F 'lock selection changed' "$fixture/lock-changed.log" > /dev/null
[[ "$(find "$locked_consumer/.tools/rust/build" -name evidence | wc -l | tr -d ' ')" == 1 ]] || exit 1
cmp "$locked_slot/selection.json" "$fixture/locked-receipt"
bash "$installer" "${locked_args[@]}" --check > "$fixture/locked-after-change"
cmp "$fixture/locked-path" "$fixture/locked-after-change"
echo 'Selected Cargo binary/example admission, offline reuse, failure retention and locking passed (substitute Cargo)'
fixture_complete=true
