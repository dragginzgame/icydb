#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd -P)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/shared-tool-commands.XXXXXX")"
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
for tool in install-host-tools.sh install-ic-tools.sh cloc.sh; do
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

make --no-print-directory -C "$consumer" install-tools > "$fixture/install.log" 2>&1
cat > "$fixture/expected" <<EOF
install-host-tools.sh <--consumer> <$consumer> <--versions> <$consumer/ci/tool-versions.env> <--with-ripgrep> <--with-cloc>
install-ic-tools.sh <--consumer> <$consumer> <--pins> <$consumer/ci/ic-tools.tsv>
EOF
cmp "$fixture/expected" "$TOOL_COMMAND_LOG"

: > "$TOOL_COMMAND_LOG"
make --no-print-directory -C "$consumer" tools-check \
    HOST_TOOL_VERSIONS="$consumer/reviewed host pins.env" IC_TOOL_PINS="$consumer/reviewed IC pins.tsv" \
    > "$fixture/check.log" 2>&1
cat > "$fixture/expected" <<EOF
install-host-tools.sh <--consumer> <$consumer> <--versions> <$consumer/reviewed host pins.env> <--with-ripgrep> <--with-cloc> <--check>
install-ic-tools.sh <--consumer> <$consumer> <--pins> <$consumer/reviewed IC pins.tsv> <--check>
EOF
cmp "$fixture/expected" "$TOOL_COMMAND_LOG"

# The aggregate stops before IC setup/checks when the host step fails.
for target in install-tools tools-check; do
    : > "$TOOL_COMMAND_LOG"
    if TOOL_COMMAND_FAIL=install-host-tools.sh make --no-print-directory -C "$consumer" "$target" \
        > "$fixture/$target-failed.log" 2>&1; then
        echo "accepted failed host step: $target" >&2; exit 1
    fi
    [[ "$(wc -l < "$TOOL_COMMAND_LOG" | tr -d ' ')" == 1 ]]
    grep '^install-host-tools.sh ' "$TOOL_COMMAND_LOG" >/dev/null
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

# The sibling tooling inventory is available through the same shared include.
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

# An explicitly selected default goal remains unchanged too.
printf '.DEFAULT_GOAL := help\n' > "$fixture/prefixed.mk"
cat "$consumer/Makefile" >> "$fixture/prefixed.mk"
cp "$fixture/prefixed.mk" "$consumer/Makefile"
[[ "$(make --no-print-directory -C "$consumer")" == 'consumer help' ]]
cmp "$fixture/expected" "$TOOL_COMMAND_LOG"
echo 'Shared tool Make commands passed (substitute installers and reports)'
