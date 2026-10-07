#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd -P)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/rust-tools-test.XXXXXX")"
trap 'if [[ $? == 0 ]]; then rm -rf "$fixture"; else printf "Failed Rust tool fixture retained: %s\n" "$fixture" >&2; fi' EXIT
mkdir -p "$fixture/bin" "$fixture/consumer with spaces"
consumer="$fixture/consumer with spaces"
cat > "$fixture/versions.env" <<'ENV'
SHARED_TOOLING_CARGO_SORT_VERSION=99.1.0
SHARED_TOOLING_CARGO_SORT_DERIVES_VERSION=99.2.0
SHARED_TOOLING_CANDID_EXTRACTOR_VERSION=99.3.0
ENV
export RUST_TOOL_FIXTURE_LOG="$fixture/install.log"
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
if [[ "$2" == cargo-sort-derives ]]; then
    [[ \$# == 2 && \$1 == sort-derives && \$2 == --version ]]
else
    [[ \$# == 1 && \$1 == --version ]]
fi
printf '%s %s\\n' '$2' '${4#=}'
exit "\${RUST_TOOL_VERSION_STATUS:-0}"
TOOL
chmod +x "$7/bin/$2"
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
echo 'Pinned Rust tool setup, offline checks and failure retention passed (substitute Cargo)'
