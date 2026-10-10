#!/usr/bin/env bash
# Shared companions: make/tools.mk
set -euo pipefail

ROOT="$0"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/shared-tool-commands.XXXXXX")"
fixture="$(cd "$fixture" && pwd -P)"
trap 'if [[ $? == 0 ]]; then rm -rf "$fixture"; else printf "Failed tool command fixture retained: %s\n" "$fixture" >&2; fi' EXIT
unset MAKEFLAGS MFLAGS MAKEOVERRIDES GNUMAKEFLAGS MAKEFILES
consumer="$fixture/consumer with spaces"
snapshot="$consumer/vendor/shared-tooling"
mkdir -p "$snapshot/make" "$snapshot/scripts/dev" "$consumer/.tools/host/bin" "$consumer/.tools/ic/bin"
cp "$ROOT/make/tools.mk" "$snapshot/make/"
export TOOL_COMMAND_LOG="$fixture/commands"
export TOOL_COMMAND_CONSUMER="$consumer"
cat > "$fixture/tool-stub" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
name="${0##*/}"
printf '%s' "$name" >> "$TOOL_COMMAND_LOG"
printf ' <%s>' "$@" >> "$TOOL_COMMAND_LOG"
printf '\n' >> "$TOOL_COMMAND_LOG"
[[ "$PATH" == "$TOOL_COMMAND_CONSUMER/.tools/host/bin:$TOOL_COMMAND_CONSUMER/.tools/ic/bin:"* ]]
if [[ "${TOOL_COMMAND_FAIL:-}" == "$name" ]]; then exit 23; fi
SCRIPT
for tool in install-host-tools.sh install-ic-tools.sh install-rust-tools.sh cloc.sh; do
    cp "$fixture/tool-stub" "$snapshot/scripts/dev/$tool"
done
cat > "$consumer/Makefile" <<'MAKE'
SHARED_TOOLING_ROOT := $(CURDIR)/vendor/shared-tooling
include vendor/shared-tooling/make/tools.mk
.PHONY: help
help:
	@echo consumer help
MAKE
# Including common commands must not make bare `make` install anything.
[[ "$(make --no-print-directory -C "$consumer")" == 'consumer help' ]]
[[ ! -e "$TOOL_COMMAND_LOG" ]]

make --no-print-directory -j4 -C "$consumer" install-tools > "$fixture/install.log" 2>&1
cat > "$fixture/expected" <<EOF
install-host-tools.sh <--consumer> <$consumer> <--versions> <$consumer/ci/tool-versions.env>
install-ic-tools.sh <--consumer> <$consumer> <--pins> <$consumer/ci/ic-tools.tsv>
install-rust-tools.sh <--consumer> <$consumer> <--versions> <$consumer/ci/tool-versions.env>
EOF
cmp "$fixture/expected" "$TOOL_COMMAND_LOG"

: > "$TOOL_COMMAND_LOG"
make --no-print-directory -j4 -C "$consumer" tools-check \
    HOST_TOOL_VERSIONS="$consumer/reviewed host pins.env" IC_TOOL_PINS="$consumer/reviewed IC pins.tsv" \
    > "$fixture/check.log" 2>&1
cat > "$fixture/expected" <<EOF
install-host-tools.sh <--consumer> <$consumer> <--versions> <$consumer/reviewed host pins.env> <--check>
install-ic-tools.sh <--consumer> <$consumer> <--pins> <$consumer/reviewed IC pins.tsv> <--check>
install-rust-tools.sh <--consumer> <$consumer> <--versions> <$consumer/reviewed host pins.env> <--check>
EOF
cmp "$fixture/expected" "$TOOL_COMMAND_LOG"

# Every common failure stops the ordered aggregate, including under parallel Make.
for target in install-tools tools-check; do
  count=0
  for failed in install-host-tools.sh install-ic-tools.sh install-rust-tools.sh; do
    count=$((count + 1))
    : > "$TOOL_COMMAND_LOG"
    if TOOL_COMMAND_FAIL="$failed" make --no-print-directory -j4 -C "$consumer" "$target" \
        > "$fixture/$target-failed.log" 2>&1; then
        echo "accepted failed common step: $target ($failed)" >&2; exit 1
    fi
    [[ "$(wc -l < "$TOOL_COMMAND_LOG" | tr -d ' ')" == "$count" ]]
  done
done

: > "$TOOL_COMMAND_LOG"
make --no-print-directory -C "$consumer" cloc > "$fixture/cloc.log" 2>&1
printf 'cloc.sh <%s>\n' "$consumer" > "$fixture/expected"
cmp "$fixture/expected" "$TOOL_COMMAND_LOG"
if TOOL_COMMAND_FAIL=cloc.sh make --no-print-directory -C "$consumer" cloc \
    > "$fixture/cloc-failed.log" 2>&1; then
    echo 'accepted failed LOC report' >&2; exit 1
fi

# A caller can select the sibling reporter without copying the common recipe.
cp "$fixture/tool-stub" "$snapshot/scripts/dev/cloc-siblings.sh"
: > "$TOOL_COMMAND_LOG"
make --no-print-directory -C "$consumer" cloc \
    CLOC_REPORT="$snapshot/scripts/dev/cloc-siblings.sh" CLOC_ROOT="$fixture" \
    > "$fixture/siblings.log" 2>&1
printf 'cloc-siblings.sh <%s>\n' "$fixture" > "$fixture/expected"
cmp "$fixture/expected" "$TOOL_COMMAND_LOG"

: > "$TOOL_COMMAND_LOG"
make --no-print-directory -C "$consumer" cloc CLOC_MANIFEST=testing/Cargo.toml \
    > "$fixture/manifest.log" 2>&1
printf 'cloc.sh <--manifest> <testing/Cargo.toml> <%s>\n' "$consumer" > "$fixture/expected"
cmp "$fixture/expected" "$TOOL_COMMAND_LOG"

# Ordinary setup/check/LOC commands above work without any fleet reporter.
# An omitted optional report must explain its owner, without invoking a sibling.
: > "$TOOL_COMMAND_LOG"
if make --no-print-directory -C "$consumer" cloc-tooling > "$fixture/no-fleet.log" 2>&1; then
    echo 'accepted unselected fleet report' >&2; exit 1
fi
grep -F 'Fleet tooling reports are optional: run make cloc-tooling in Shared Tooling' "$fixture/no-fleet.log"
[[ ! -s "$TOOL_COMMAND_LOG" ]]

# Explicitly selected fleet tooling remains available through the same include.
cat > "$snapshot/scripts/dev/cloc-tooling.pl" <<'PERL'
use strict;
use warnings;
open my $log, '>>', $ENV{TOOL_COMMAND_LOG} or die $!;
print {$log} "cloc-tooling.pl <$ARGV[0]>\n";
PERL
: > "$TOOL_COMMAND_LOG"
make --no-print-directory -C "$consumer" cloc-tooling CLOC_PARENT="$fixture" > "$fixture/tooling.log" 2>&1
printf 'cloc-tooling.pl <%s>\n' "$fixture" > "$fixture/expected"
cmp "$fixture/expected" "$TOOL_COMMAND_LOG"

# Product tools extend the ordered recipe rather than racing as prerequisites.
cat >> "$consumer/Makefile" <<'MAKE'
LOCAL_TOOL_INSTALL_TARGETS += install-product install-second
LOCAL_TOOL_CHECK_TARGETS += check-product check-second
.PHONY: install-product install-second check-product check-second
install-product install-second check-product check-second:
	@printf '%s\n' '$@' >> "$(TOOL_COMMAND_LOG)"
	@test "$(TOOL_COMMAND_FAIL)" != '$@'
MAKE
: > "$TOOL_COMMAND_LOG"
make --no-print-directory -j4 -C "$consumer" install-tools > "$fixture/rust-install.log" 2>&1
[[ "$(sed -n '3p' "$TOOL_COMMAND_LOG")" == "install-rust-tools.sh <--consumer> <$consumer> <--versions> <$consumer/ci/tool-versions.env>" ]]
printf 'install-product\ninstall-second\n' > "$fixture/product-expected"
tail -2 "$TOOL_COMMAND_LOG" > "$fixture/product-actual"
cmp "$fixture/product-expected" "$fixture/product-actual"
: > "$TOOL_COMMAND_LOG"
make --no-print-directory -j4 -C "$consumer" tools-check RUST_TOOL_VERSIONS="$consumer/rust pins.env" > "$fixture/rust-check.log" 2>&1
[[ "$(sed -n '3p' "$TOOL_COMMAND_LOG")" == "install-rust-tools.sh <--consumer> <$consumer> <--versions> <$consumer/rust pins.env> <--check>" ]]
printf 'check-product\ncheck-second\n' > "$fixture/product-expected"
tail -2 "$TOOL_COMMAND_LOG" > "$fixture/product-actual"
cmp "$fixture/product-expected" "$fixture/product-actual"
for target in install-tools tools-check; do
    phase=install; [[ "$target" != tools-check ]] || phase=check
    for failed in install-rust-tools.sh "$phase-product" "$phase-second"; do
        : > "$TOOL_COMMAND_LOG"
        if TOOL_COMMAND_FAIL="$failed" make --no-print-directory -j4 -C "$consumer" "$target" \
            > "$fixture/product-failure.log" 2>&1; then exit 1; fi
        case "$failed" in
            install-rust-tools.sh) expected_count=3 ;;
            *-product) expected_count=4 ;;
            *-second) expected_count=5 ;;
        esac
        [[ "$(wc -l < "$TOOL_COMMAND_LOG" | tr -d ' ')" == "$expected_count" ]]
    done
done
# Keep later default-goal comparison on its current command record.
cp "$TOOL_COMMAND_LOG" "$fixture/expected"

# An explicitly selected default goal remains unchanged too.
printf '.DEFAULT_GOAL := help\n' > "$fixture/prefixed.mk"
cat "$consumer/Makefile" >> "$fixture/prefixed.mk"
cp "$fixture/prefixed.mk" "$consumer/Makefile"
[[ "$(make --no-print-directory -C "$consumer")" == 'consumer help' ]]
cmp "$fixture/expected" "$TOOL_COMMAND_LOG"
echo 'Shared tool Make commands passed (substitute installers and reports)'
