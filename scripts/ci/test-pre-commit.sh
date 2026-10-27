#!/usr/bin/env bash
set -euo pipefail

# Qualify IcyDB's actual Make/hook selection, derive coverage and staged-file
# preservation. Generic formatter/input-reuse permutations belong upstream.
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
export PATH="$ROOT/.tools/host/bin:$ROOT/.tools/rust/common/bin:$PATH"
export CARGO_NET_OFFLINE=true RUSTUP_AUTO_INSTALL=0
cd "$ROOT"

# Perturb adjacent complete entries below the role heading. The actual sorter
# must restore ordering without moving comments between dependency identities.
# Skip comment-prefixed entries rather than detaching their TOML decoration.
perl -0777 -pe 's/(\[workspace.dependencies\]\n.*?^(?![ \t]*#)[^\n]*\n)([A-Za-z0-9_-]+ = (?:"[^\n]*"|\{[^\n]*\})\n)([A-Za-z0-9_-]+ = (?:"[^\n]*"|\{[^\n]*\})\n)/$1$3$2/sm or die "expected adjacent formatter inputs\n"' \
  Cargo.toml > "$fixture/unsorted.toml"
overlays=(Cargo.lock .shared-tooling.snapshot rust-toolchain.toml ci/icydb-tools.env)
# Keep the exported Shared layer coherent with its selected Makefiles, including
# newly declared companions absent from the committed consumer baseline.
bash scripts/ci/verify-shared-tooling-snapshot.sh --read-manifest .shared-tooling.snapshot > "$fixture/shared-manifest"
while IFS=$'\t' read -r kind _digest _mode path; do
  [[ "$kind" != file ]] || overlays[${#overlays[@]}]="$path"
done < "$fixture/shared-manifest"
git ls-files --modified --others --exclude-standard -z > "$fixture/working-inputs"
while IFS= read -r -d '' path; do
  case "$path" in
    Cargo.toml|Cargo.lock|rust-toolchain.toml|ci/tool-versions.env|ci/icydb-tools.env|crates/icydb-diagnostic-code/src/lib.rs) continue ;;
    *.rs|*/Cargo.toml|*/Cargo.lock|.cargo/*)
      [[ ! -f "$path" ]] || overlays[${#overlays[@]}]="$path" ;;
  esac
done < "$fixture/working-inputs"
bash scripts/ci/check-formatting-hooks.sh "$ROOT" crates/icydb-diagnostic-code/src/lib.rs \
  Cargo.toml "$fixture/unsorted.toml" make/consumer.mk make/tools.mk make/release.mk make/rust-format.mk make/execution.mk "${overlays[@]}"

# A dependency-free disposable workspace runs IcyDB's same Makefile formatters
# against derive ordering and filenames Cargo can select as explicit targets.
mkdir -p "$fixture/product/src" "$fixture/product/scripts/ci" "$fixture/product/.githooks" "$fixture/templates"
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TEMPLATE_DIR="$fixture/templates"
cp "$ROOT/Makefile" "$ROOT/rust-toolchain.toml" "$fixture/product/"
mkdir -p "$fixture/product/make"
cp "$ROOT/make/consumer.mk" "$ROOT/make/tools.mk" "$ROOT/make/release.mk" "$ROOT/make/rust-format.mk" "$ROOT/make/execution.mk" "$fixture/product/make/"
cp "$ROOT/scripts/ci/actionlint-checksums.tsv" "$fixture/product/scripts/ci/"
bash "$ROOT/scripts/ci/prepare-script-fixture.sh" "$ROOT" "$fixture/product" make/rust-format.mk
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
# Ordinary write mode keeps successful earlier passes when a later pass fails;
# the hook separately isolates those partial writes from the real index.
if [[ "$*" == 'sort --workspace --grouped' ]]; then
  printf '# manifest formatted\n' >> "$FORMAT_FIXTURE_MANIFEST"
fi
CARGO
chmod +x "$fixture/cargo"
# The prepared fixture copy exports the reviewed tool identities.
# shellcheck source=/dev/null
source ci/tool-versions.env
export FORMAT_FIXTURE_EVENTS="$fixture/format-events"
export FORMAT_FIXTURE_MANIFEST="$fixture/pass-manifest"
export RUNNER_TEMP="$fixture"
make --no-print-directory > "$fixture/default-goal.log" 2>&1
[[ ! -e "$FORMAT_FIXTURE_EVENTS" ]]
for target in fmt fmt-check; do
  if [[ "$target" == fmt ]]; then
    derive='sort-derives'; sort='sort --workspace --grouped'; format='fmt --all'
  else
    derive='sort-derives --check'; sort='sort --workspace --grouped --check'; format='fmt --all -- --check'
  fi
  : > "$FORMAT_FIXTURE_EVENTS"
  FORMAT_REQUIRE_JOBSERVER=1 make --no-print-directory -j2 "$target" "FORMAT_CARGO=$fixture/cargo" \
    'FORMAT_RUST_ARGS=--sort-derives --no-cache' > "$fixture/$target-dispatch.log" 2>&1
  printf '%s\n' 'sort-derives --version' "$sort" "$derive" "$format" > "$fixture/expected-format"
  cmp "$fixture/expected-format" "$FORMAT_FIXTURE_EVENTS"
  for label in 'Checking derive formatter' 'Manifest sorting [.]' 'Derive sorting [.]' 'Rust formatting [.]'; do
    grep -F "$label... ok (" "$fixture/$target-dispatch.log" >/dev/null
  done
  for failure in 'sort-derives --version' "$sort" "$derive" "$format"; do
    : > "$FORMAT_FIXTURE_EVENTS"
    : > "$FORMAT_FIXTURE_MANIFEST"
    if FORMAT_FIXTURE_FAIL="$failure" make --no-print-directory -j2 "$target" "FORMAT_CARGO=$fixture/cargo" \
      'FORMAT_RUST_ARGS=--sort-derives --no-cache' \
      > "$fixture/$target-refusal.log" 2>&1; then
      echo 'formatting accepted a failed formatter' >&2; exit 1
    fi
    : > "$fixture/expected-format"
    for command in 'sort-derives --version' "$sort" "$derive" "$format"; do
      printf '%s\n' "$command" >> "$fixture/expected-format"
      [[ "$command" != "$failure" ]] || break
    done
    cmp "$fixture/expected-format" "$FORMAT_FIXTURE_EVENTS"
    grep -F '... FAILED (exit 43,' "$fixture/$target-refusal.log" >/dev/null
    if [[ "$target" == fmt && ( "$failure" == "$derive" || "$failure" == "$format" ) ]]; then
      grep -Fx '# manifest formatted' "$FORMAT_FIXTURE_MANIFEST" >/dev/null
    else
      [[ ! -s "$FORMAT_FIXTURE_MANIFEST" ]]
    fi
    logs=("$fixture"/formatting.*)
    [[ ${#logs[@]} == 1 && -f "${logs[0]}" ]]
    printf 'Details: %q\n' "${logs[0]}" > "$fixture/expected-details"
    grep '^Details: ' "$fixture/$target-refusal.log" > "$fixture/actual-details"
    cmp "$fixture/expected-details" "$fixture/actual-details"
    printf 'formatter stdout: %s\nformatter stderr: %s\n' "$failure" "$failure" > "$fixture/expected-log"
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
cargo generate-lockfile --offline > "$fixture/lock-preparation.log" 2>&1
cp Cargo.lock "$fixture/product-lock-before"
make --no-print-directory fmt > "$fixture/product-baseline.log" 2>&1
# Real prepared tools and the consumer's declarative selection must converge,
# reuse only unchanged inputs, and still support a deliberately forced run.
make --no-print-directory fmt-check > "$fixture/product-checked.log" 2>&1
[[ -f target/shared-tooling/formatting.receipt ]]
make --no-print-directory fmt-check > "$fixture/product-reused.log" 2>&1
grep -F 'Checking formatting inputs... unchanged (' "$fixture/product-reused.log" >/dev/null
printf '// Changed consumer source.\n' >> src/lib.rs
make --no-print-directory fmt-check > "$fixture/product-invalidated.log" 2>&1
grep -F 'Derive sorting [.]... ok (' "$fixture/product-invalidated.log" >/dev/null
make --no-print-directory fmt-check 'FORMAT_RUST_ARGS=--sort-derives --no-cache' > "$fixture/product-forced.log" 2>&1
grep -F 'Derive sorting [.]... ok (' "$fixture/product-forced.log" >/dev/null
cmp Cargo.lock "$fixture/product-lock-before"
printf '#[derive(PartialEq, Clone, Eq, Copy)]\nstruct Flag;\n' > src/lib.rs
printf 'fn main( ) { }\n' > 'src/staged [1].rs'
printf 'fn main( ) { }\n' > "$newline_path"
git add -- Makefile make rust-toolchain.toml Cargo.toml src/lib.rs 'src/staged [1].rs' "$newline_path" .githooks/pre-commit scripts/ci ci
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
grep -F 'Derive sorting [.]... ok (' "$fixture/product-hook.log" >/dev/null
git diff --exit-code
rg -Fx '#[derive(Clone, Copy, Eq, PartialEq)]' src/lib.rs >/dev/null
rg -Fx 'fn main() {}' 'src/staged [1].rs' "$newline_path" >/dev/null
cmp "$fixture/unselected-before" unselected.rs
cmp Cargo.lock "$fixture/product-lock-before"
echo 'IcyDB real formatting, derive sorting and unusual selected paths passed'
fixture_complete=true
