#!/usr/bin/env bash
# Shared companions: scripts/dev/cargo-tool-build.sh scripts/dev/tool-store.sh scripts/dev/install-rust-tools.sh scripts/ci/verify-file-checksum.sh
set -euo pipefail

# Repository CI only: prepared host tools and compiler are required. This reads
# the installer's identity; it neither installs tools nor admits cached bytes.
[[ $# == 1 ]] || { echo 'usage: cargo-tool-cache-key.sh TOOL-VERSIONS' >&2; exit 2; }
ROOT="${BASH_SOURCE[0]}"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
# shellcheck source=scripts/dev/tool-store.sh
source "$ROOT/scripts/dev/tool-store.sh"
# shellcheck source=scripts/dev/cargo-tool-build.sh
source "$ROOT/scripts/dev/cargo-tool-build.sh"
cargo_build_init
key="$(
    {
        printf '%s\n' "$build_identity" "${ImageOS:-}" "${ImageVersion:-}"
        # Never execute a catalog during inspection. Only normalize the plain
        # assignments we can prove literal; executable/ambiguous catalogs retain
        # their complete bytes so a hidden selection change cannot share a key.
        perl -0777 -e '
            use strict; use warnings;
            open my $file, "<", $ARGV[0] or die "cannot read tool catalog: $!\n";
            my $bytes = <$file> // "";
            close $file or die "cannot close tool catalog: $!\n";
            my %wanted = map { $_ => 1 } qw(CARGO_SORT CARGO_SORT_DERIVES CANDID_EXTRACTOR);
            my (%seen, %pins);
            my $literal = 1;
            for my $line (split /\n/, $bytes) {
                next if $line =~ /\A[ \t]*(?:#.*)?\z/;
                unless ($line =~ /\A[ \t]*(?:export[ \t]+)?(SHARED_TOOLING_[A-Z0-9_]+_(?:VERSION|SHA256(?:_[A-Z0-9_]+)?))=([A-Za-z0-9_.+-]+)(?:[ \t]+#.*)?[ \t]*\z/
                    && !$seen{$1}++) { $literal = 0; last; }
                my ($name, $value) = ($1, $2);
                if ($name =~ /\ASHARED_TOOLING_(.+)_VERSION\z/ && $wanted{$1}) {
                    $pins{$1} = $value;
                }
            }
            if ($literal && keys(%pins) == keys(%wanted)
                && !grep { !/\A(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\z/ } values %pins) {
                print "literal-cargo-pins\n", map { "$_=$pins{$_}\n" } sort keys %pins;
            } else { print "catalog-bytes\n", $bytes; }
        ' -- "$1" || exit $?
        for file in scripts/ci/cargo-tool-cache-key.sh \
            scripts/dev/install-rust-tools.sh scripts/dev/tool-store.sh \
            scripts/ci/verify-file-checksum.sh; do
            bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$ROOT/$file" || exit $?
        done
    } | perl -MDigest::SHA=sha256_hex -0777 -ne 'print sha256_hex($_)'
)"
# tool_store_root already rejects CR/LF before these GitHub output records.
printf 'key=%s\nroot=%s\n' "$key" "$store_root"
