#!/usr/bin/env bash
set -euo pipefail

unset MAKEFLAGS MAKEOVERRIDES MFLAGS GNUMAKEFLAGS MAKEFILES

# Qualify IcyDB's caller ordering; Shared owns the generic Cargo admission matrix.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-adapter-tooling.XXXXXX")"
fixture="$(cd "$fixture" && pwd -P)"
fixture_complete=false
# shellcheck source=scripts/ci/fixture-lifecycle.sh
source "$ROOT/scripts/ci/fixture-lifecycle.sh"
trap 'fixture_finish "$?" "$fixture" "Adapter tooling fixture retained"' EXIT
mkdir -p "$fixture/scripts/ci" "$fixture/scripts/dev" "$fixture/testing/canic-adapter" "$fixture/.tools/host/bin"
cp "$ROOT/scripts/ci/check-adapter-tooling.sh" "$fixture/scripts/ci/"
cat > "$fixture/scripts/dev/tooling-lock.pl" <<'PERL'
use strict;
use warnings;
use JSON::PP;
my $adapter = $ARGV[0] =~ m{/testing/canic-adapter\z};
my $owner = $adapter ? 'adapter' : 'root';
open my $log, '>>', $ENV{ADAPTER_TOOLING_LOG} or die $!;
print {$log} "$owner\n";
close $log or die $!;
die "selected admission refused\n" if $ENV{ADAPTER_TOOLING_CASE} eq "$owner-failure";
print JSON::PP->new->canonical->encode({
    version => $adapter && $ENV{ADAPTER_TOOLING_CASE} eq 'mismatch' ? '0.37.0' : '0.36.0',
    checksum => 'a' x 64,
}), "\n";
PERL
cat > "$fixture/.tools/host/bin/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
[[ "$*" == "fetch --manifest-path $PWD/testing/canic-adapter/Cargo.toml --locked" ]]
printf 'fetch\n' >> "$ADAPTER_TOOLING_LOG"
CARGO
chmod +x "$fixture/.tools/host/bin/cargo"
# Copy the actual target recipes and dependency edge, without the unrelated
# product/release targets or effectful fetch command. Only Cargo is substituted.
{
    printf 'ROOT_DIR := %s\n' "$fixture"
    cat <<'MAKE'
ADAPTER_MANIFEST_FLAG := --manifest-path "$(ROOT_DIR)/testing/canic-adapter/Cargo.toml"
export PATH := $(ROOT_DIR)/.tools/host/bin:$(PATH)
.PHONY: shared-tooling-check check-adapter-tooling fetch-adapter
shared-tooling-check:
	@test "$$ADAPTER_TOOLING_CASE" != snapshot-failure
MAKE
    awk '/^fetch-adapter .*: check-adapter-tooling$/ { print }
         /^fetch-adapter:$/ || /^check-adapter-tooling: / { selected = 1; print; next }
         selected && /^\t/ { print; next }
         selected && /^[^[:space:]#]/ { selected = 0 }' "$ROOT/Makefile"
} > "$fixture/Makefile"
export ADAPTER_TOOLING_LOG="$fixture/calls"
for control in snapshot-failure root-failure adapter-failure mismatch pass; do
    : > "$ADAPTER_TOOLING_LOG"
    status=0
    ADAPTER_TOOLING_CASE="$control" make --no-print-directory -C "$fixture" fetch-adapter \
        > "$fixture/$control.log" 2>&1 || status=$?
    if [[ "$control" == pass ]]; then
        [[ "$status" == 0 ]]
        printf 'root\nadapter\nfetch\n' > "$fixture/expected"
    else
        [[ "$status" != 0 ]]
        case "$control" in
            snapshot-failure) : > "$fixture/expected" ;;
            root-failure) printf 'root\n' > "$fixture/expected" ;;
            *) printf 'root\nadapter\n' > "$fixture/expected" ;;
        esac
    fi
    cmp "$fixture/expected" "$ADAPTER_TOOLING_LOG"
done
fixture_complete=true
echo 'Adapter tooling caller ordering passed: 5 cases (substitute selection/Cargo; no fetching).'
