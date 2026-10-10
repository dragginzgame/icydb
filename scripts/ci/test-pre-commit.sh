#!/usr/bin/env bash
set -euo pipefail

# Shared Tooling qualifies the real consumer fmt/fmt-check commands. Keep only
# IcyDB's additional derive sorter and unusual selected-filename cases here.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-formatting.XXXXXX")"
fixture="$(cd "$fixture" && pwd -P)"
fixture_complete=false
finish() {
  local status=$?
  [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
  if [[ "$status" == 0 ]]; then rm -rf "$fixture"
  else echo "IcyDB formatting evidence retained: $fixture" >&2; fi
  exit "$status"
}
trap finish EXIT
export PATH="$ROOT/.tools/host/bin:$ROOT/.tools/rust/bin:$PATH"
export CARGO_NET_OFFLINE=true RUSTUP_AUTO_INSTALL=0
cd "$ROOT"

# Perturb only dependency ordering; the actual manifest sorter must restore it.
perl -0777 -pe 's/(\[workspace.dependencies\]\n)([A-Za-z0-9_-]+ = [^\n]*\n)([A-Za-z0-9_-]+ = [^\n]*\n)/$1$3$2/ or die "expected adjacent formatter inputs\n"' \
  Cargo.toml > "$fixture/unsorted.toml"
overlays=(Cargo.lock rust-toolchain.toml ci/tool-versions.env ci/icydb-tools.env scripts/ci/check-format-tools.sh scripts/ci/check-make-execution.sh scripts/ci/run-formatting.sh)
git ls-files --modified --others --exclude-standard -z > "$fixture/working-inputs"
while IFS= read -r -d '' path; do
  case "$path" in
    Cargo.toml|Cargo.lock|rust-toolchain.toml|ci/tool-versions.env|ci/icydb-tools.env|crates/icydb-diagnostic-code/src/lib.rs) continue ;;
    *.rs|*/Cargo.toml|*/Cargo.lock|.cargo/*)
      [[ ! -f "$path" ]] || overlays[${#overlays[@]}]="$path" ;;
  esac
done < "$fixture/working-inputs"
bash scripts/ci/check-formatting-hooks.sh "$ROOT" crates/icydb-diagnostic-code/src/lib.rs \
  Cargo.toml "$fixture/unsorted.toml" make/tools.mk make/release.mk make/rust-format.mk make/execution.mk "${overlays[@]}"

# A dependency-free disposable workspace runs IcyDB's same Makefile formatters
# against derive ordering and filenames Cargo can select as explicit targets.
mkdir -p "$fixture/product/src" "$fixture/product/scripts/ci" "$fixture/product/.githooks" "$fixture/templates"
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TEMPLATE_DIR="$fixture/templates"
cp "$ROOT/Makefile" "$ROOT/rust-toolchain.toml" "$fixture/product/"
mkdir -p "$fixture/product/make"
cp "$ROOT/make/tools.mk" "$ROOT/make/release.mk" "$ROOT/make/rust-format.mk" "$ROOT/make/execution.mk" "$fixture/product/make/"
cp "$ROOT/scripts/ci/actionlint-checksums.tsv" "$fixture/product/scripts/ci/"
cp "$ROOT/scripts/ci/check-format-tools.sh" "$fixture/product/scripts/ci/"
cp "$ROOT/scripts/ci/check-make-execution.sh" "$fixture/product/scripts/ci/"
cp "$ROOT/scripts/ci/run-formatting.sh" "$fixture/product/scripts/ci/"
mkdir -p "$fixture/product/ci"
cp "$ROOT/ci/tool-versions.env" "$fixture/product/ci/"
cp "$ROOT/.githooks/pre-commit" "$fixture/product/.githooks/"
cd "$fixture/product"
git init --quiet
# Qualify the local derive prerequisite and Cargo environment through the actual
# Makefile, including parallel dispatch and refusal before shared formatters.
cat > "$fixture/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
[[ "$CARGO_HOME" == "$PWD/.cache/cargo/icydb" && "$CARGO_TARGET_DIR" == "$PWD/target/icydb" ]]
[[ "$CARGO_NET_OFFLINE" == true && "$RUSTUP_AUTO_INSTALL" == 0 ]]
if [[ "${FORMAT_REQUIRE_JOBSERVER:-0}" == 1 ]]; then
  [[ "${MAKEFLAGS:-}" =~ --jobserver-(auth|fds)=([0-9]+),([0-9]+) ]]
  reader="${BASH_REMATCH[2]}"; writer="${BASH_REMATCH[3]}"
  : <&"$reader"
  : >&"$writer"
fi
case "$*" in
  'sort --version') echo "cargo-sort $SHARED_TOOLING_CARGO_SORT_VERSION"; exit 0 ;;
  'fmt --version') exit 0 ;;
esac
printf '%s\n' "$*" >> "$FORMAT_FIXTURE_EVENTS"
printf 'formatter stdout: %s\n' "$*"
printf 'formatter stderr: %s\n' "$*" >&2
[[ "$*" != "${FORMAT_FIXTURE_FAIL:-}" ]] || exit 43
CARGO
chmod +x "$fixture/cargo"
# The prepared fixture copy exports the reviewed tool identities.
# shellcheck source=/dev/null
source ci/tool-versions.env
export FORMAT_FIXTURE_EVENTS="$fixture/format-events"
export RUNNER_TEMP="$fixture"
make --no-print-directory > "$fixture/default-goal.log" 2>&1
[[ ! -e "$FORMAT_FIXTURE_EVENTS" ]]
for target in fmt fmt-check; do
  if [[ "$target" == fmt ]]; then
    derive='sort-derives'; sort='sort --workspace'; format='fmt --all'
    label='Formatting'
  else
    derive='sort-derives --check'; sort='sort --workspace --check'; format='fmt --all -- --check'
    label='Checking formatting'
  fi
  : > "$FORMAT_FIXTURE_EVENTS"
  FORMAT_REQUIRE_JOBSERVER=1 make --no-print-directory -j2 "$target" "FORMAT_CARGO=$fixture/cargo" > "$fixture/$target-dispatch.log" 2>&1
  printf '%s\n' "$derive" "$sort" "$format" > "$fixture/expected-format"
  cmp "$fixture/expected-format" "$FORMAT_FIXTURE_EVENTS"
  printf '%s... ok\n' "$label" > "$fixture/expected-output"
  cmp "$fixture/expected-output" "$fixture/$target-dispatch.log"
  for failure in "$derive" "$sort" "$format"; do
    : > "$FORMAT_FIXTURE_EVENTS"
    if FORMAT_FIXTURE_FAIL="$failure" make --no-print-directory -j2 "$target" "FORMAT_CARGO=$fixture/cargo" \
      > "$fixture/$target-refusal.log" 2>&1; then
      echo 'formatting accepted a failed formatter' >&2; exit 1
    fi
    printf '%s\n' "$derive" > "$fixture/expected-format"
    [[ "$failure" == "$derive" ]] || printf '%s\n' "$sort" >> "$fixture/expected-format"
    [[ "$failure" != "$format" ]] || printf '%s\n' "$format" >> "$fixture/expected-format"
    cmp "$fixture/expected-format" "$FORMAT_FIXTURE_EVENTS"
    grep -Fx "$label... FAILED (exit 43)" "$fixture/$target-refusal.log" >/dev/null
    logs=("$fixture"/formatting.*)
    [[ ${#logs[@]} == 1 && -f "${logs[0]}" ]]
    printf 'Details: %q\n' "${logs[0]}" > "$fixture/expected-details"
    grep '^Details: ' "$fixture/$target-refusal.log" > "$fixture/actual-details"
    cmp "$fixture/expected-details" "$fixture/actual-details"
    : > "$fixture/expected-log"
    while IFS= read -r command; do
      printf 'formatter stdout: %s\nformatter stderr: %s\n' "$command" "$command" >> "$fixture/expected-log"
    done < "$fixture/expected-format"
    cmp "$fixture/expected-log" "${logs[0]}"
    rm "${logs[0]}"
  done
done
# Release TMPDIR can be inside IcyDB; keep this package its own workspace.
cat > Cargo.toml <<'MANIFEST'
[workspace]

[package]
name = "icydb-formatting-fixture"
version = "0.1.0"
edition = "2024"

[[bin]]
name = "brackets"
path = "src/staged [1].rs"

[[bin]]
name = "newline"
path = "src/staged\nnewline.rs"
MANIFEST
newline_path=$'src/staged\nnewline.rs'
printf 'fn main() {}\n' > 'src/staged [1].rs'
printf 'fn main() {}\n' > "$newline_path"
printf '#[derive(Clone, Copy, Eq, PartialEq)]\nstruct Flag;\n' > src/lib.rs
make --no-print-directory fmt > "$fixture/product-baseline.log" 2>&1
printf '#[derive(PartialEq, Clone, Eq, Copy)]\nstruct Flag;\n' > src/lib.rs
printf 'fn main( ) { }\n' > 'src/staged [1].rs'
printf 'fn main( ) { }\n' > "$newline_path"
git add -- Makefile make/tools.mk make/release.mk make/rust-format.mk make/execution.mk rust-toolchain.toml Cargo.toml src/lib.rs 'src/staged [1].rs' "$newline_path" .githooks/pre-commit scripts/ci/actionlint-checksums.tsv scripts/ci/check-format-tools.sh scripts/ci/check-make-execution.sh scripts/ci/run-formatting.sh ci/tool-versions.env
printf 'fn unrelated( ) { }\n' > unselected.rs
cp unselected.rs "$fixture/unselected-before"
# Refusal precedes formatter dispatch and preserves selected bytes and index.
cp .git/index "$fixture/index-before"
cp src/lib.rs "$fixture/lib-before"
cp 'src/staged [1].rs' "$fixture/brackets-before"
cp "$newline_path" "$fixture/newline-before"
for flags in i n q t v --ignore-errors --just-print --question --touch --version; do
  status=0
  MAKEFLAGS="$flags" bash .githooks/pre-commit \
    > "$fixture/refused-${flags#--}.log" 2>&1 || status=$?
  [[ "$status" -ne 0 ]]
  cmp .git/index "$fixture/index-before"
  cmp src/lib.rs "$fixture/lib-before"
  cmp 'src/staged [1].rs' "$fixture/brackets-before"
  cmp "$newline_path" "$fixture/newline-before"
done
bash .githooks/pre-commit > "$fixture/product-hook.log" 2>&1
git diff --exit-code
rg -Fx '#[derive(Clone, Copy, Eq, PartialEq)]' src/lib.rs >/dev/null
rg -Fx 'fn main() {}' 'src/staged [1].rs' "$newline_path" >/dev/null
cmp "$fixture/unselected-before" unselected.rs
echo 'IcyDB real formatting, derive sorting and unusual selected paths passed'
fixture_complete=true
