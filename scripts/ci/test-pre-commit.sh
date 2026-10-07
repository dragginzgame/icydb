#!/usr/bin/env bash
set -euo pipefail

# Shared Tooling qualifies the real consumer fmt/fmt-check commands. Keep only
# IcyDB's additional derive sorter and unusual selected-filename cases here.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-formatting.XXXXXX")"
fixture="$(cd "$fixture" && pwd -P)"
finish() {
  local status=$?
  if [[ "$status" == 0 ]]; then rm -rf "$fixture"
  else echo "IcyDB formatting evidence retained: $fixture" >&2; fi
}
trap finish EXIT
export PATH="$ROOT/.tools/host/bin:$ROOT/.tools/rust/bin:$PATH"
export CARGO_NET_OFFLINE=true RUSTUP_AUTO_INSTALL=0
cd "$ROOT"

# Perturb only dependency ordering; the actual manifest sorter must restore it.
perl -0777 -pe 's/(\[workspace.dependencies\]\n)([A-Za-z0-9_-]+ = [^\n]*\n)([A-Za-z0-9_-]+ = [^\n]*\n)/$1$3$2/ or die "expected adjacent formatter inputs\n"' \
  Cargo.toml > "$fixture/unsorted.toml"
overlays=(Cargo.lock rust-toolchain.toml ci/tool-versions.env ci/icydb-tools.env scripts/ci/check-format-tools.sh scripts/ci/check-make-execution.sh)
git ls-files --modified --others --exclude-standard -z > "$fixture/working-inputs"
while IFS= read -r -d '' path; do
  case "$path" in
    Cargo.toml|Cargo.lock|rust-toolchain.toml|ci/tool-versions.env|ci/icydb-tools.env|crates/icydb-diagnostic-code/src/lib.rs) continue ;;
    *.rs|*/Cargo.toml|*/Cargo.lock|.cargo/*)
      [[ ! -f "$path" ]] || overlays[${#overlays[@]}]="$path" ;;
  esac
done < "$fixture/working-inputs"
bash scripts/ci/check-formatting-hooks.sh "$ROOT" crates/icydb-diagnostic-code/src/lib.rs \
  Cargo.toml "$fixture/unsorted.toml" make/tools.mk "${overlays[@]}"

# A dependency-free disposable workspace runs IcyDB's same Makefile formatters
# against derive ordering and filenames Cargo can select as explicit targets.
mkdir -p "$fixture/product/src" "$fixture/product/scripts/ci" "$fixture/product/.githooks" "$fixture/templates"
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TEMPLATE_DIR="$fixture/templates"
cp "$ROOT/Makefile" "$ROOT/rust-toolchain.toml" "$fixture/product/"
mkdir -p "$fixture/product/make"
cp "$ROOT/make/tools.mk" "$fixture/product/make/"
cp "$ROOT/scripts/ci/actionlint-checksums.tsv" "$fixture/product/scripts/ci/"
cp "$ROOT/scripts/ci/check-format-tools.sh" "$fixture/product/scripts/ci/"
cp "$ROOT/scripts/ci/check-make-execution.sh" "$fixture/product/scripts/ci/"
mkdir -p "$fixture/product/ci"
cp "$ROOT/ci/tool-versions.env" "$fixture/product/ci/"
cp "$ROOT/.githooks/pre-commit" "$fixture/product/.githooks/"
cd "$fixture/product"
git init --quiet
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
git add -- Makefile make/tools.mk rust-toolchain.toml Cargo.toml src/lib.rs 'src/staged [1].rs' "$newline_path" .githooks/pre-commit scripts/ci/actionlint-checksums.tsv scripts/ci/check-format-tools.sh scripts/ci/check-make-execution.sh ci/tool-versions.env
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
