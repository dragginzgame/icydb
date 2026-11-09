#!/usr/bin/env bash
# Shared companions: ci/external-ic-dependencies.txt ci/cbor-dependencies.json ci/hash-dependencies.json
set -euo pipefail
[[ ( $# -eq 2 || ( $# -eq 3 && "$3" == --external-ic ) ) && "$1" == --consumer ]] || {
    echo 'usage: check-cargo-dependency-boundaries.sh --consumer ROOT [--external-ic]' >&2
    exit 2
}
consumer="$2"
common=''
cbor=''
hashes=''
if [[ $# == 3 ]]; then
    common="${BASH_SOURCE[0]}"
    [[ "$common" == /* ]] || common="$PWD/$common"
    cbor="${common%/*}/../../ci/cbor-dependencies.json"
    hashes="${common%/*}/../../ci/hash-dependencies.json"
    common="${common%/*}/../../ci/external-ic-dependencies.txt"
fi
case "$consumer$PWD" in *$'\n'*|*$'\r'*) echo 'consumer directory contains CR/LF' >&2; exit 2 ;; esac
[[ "$consumer" == /* ]] || consumer="$PWD/$consumer"
cd -P -- "$consumer"
case "$PWD" in *$'\n'*|*$'\r'*) echo 'resolved consumer directory contains CR/LF' >&2; exit 2 ;; esac
[[ -f Cargo.toml && ! -L Cargo.toml ]] || { echo 'consumer requires a regular Cargo.toml' >&2; exit 2; }
policy=ci/cargo-dependency-boundaries.json
CARGO_NET_OFFLINE=true RUSTUP_AUTO_INSTALL=0 perl -MJSON::PP -e '
        use strict;
        use warnings;
        use Errno qw(ENOENT);
        local $/;
        my $policy = [];
        if (lstat($ARGV[0])) {
            -f _ && !-l _ or die "boundary policy must be a regular file\n";
            open my $file, "<", $ARGV[0] or die "cannot read boundary policy: $!\n";
            $policy = decode_json(<$file>);
            close $file or die "cannot close boundary policy: $!\n";
            ref($policy) eq "ARRAY" && @$policy or die "expected a nonempty policy array\n";
        } else {
            $! == ENOENT && length($ARGV[2])
                or die "missing or unreadable boundary policy: $!\n";
        }
        my %common;
        if (length $ARGV[2]) {
            open my $file, "<", $ARGV[2] or die "cannot read common IC policy: $!\n";
            for my $name (split /\n/, <$file>) {
                next if $name =~ /\A(?:#.*)?\z/;
                $name =~ /\A[A-Za-z0-9_-]+\z/ && !$common{$name}++
                    or die "invalid or duplicate common IC dependency\n";
            }
            close $file or die "cannot close common IC policy: $!\n";
            keys(%common) or die "empty common IC policy\n";
        }
        my %cbor;
        if (length $ARGV[3]) {
            open my $file, "<", $ARGV[3] or die "cannot read CBOR inventory: $!\n";
            my $inventory = decode_json(<$file>);
            close $file or die $!;
            ref($inventory->{rust}) eq "ARRAY" && @{$inventory->{rust}}
                or die "invalid CBOR inventory\n";
            for my $name (@{$inventory->{rust}}) {
                $name =~ /\A[A-Za-z0-9_-]+\z/ && !$cbor{$name}++
                    or die "invalid or duplicate CBOR package\n";
            }
        }
        my %hash_review;
        if (length $ARGV[4]) {
            open my $file, "<", $ARGV[4] or die "cannot read hash inventory: $!\n";
            my $inventory = decode_json(<$file>);
            close $file or die $!;
            ref($inventory) eq "HASH" && keys(%$inventory) == 2
                && ref($inventory->{approved}) eq "HASH" && keys(%{$inventory->{approved}})
                && ref($inventory->{review_required}) eq "ARRAY" && @{$inventory->{review_required}}
                or die "invalid hash inventory\n";
            my %seen;
            for my $name (keys %{$inventory->{approved}}, @{$inventory->{review_required}}) {
                defined($name) && !ref($name) && $name =~ /\A[A-Za-z0-9_-]+\z/ && !$seen{$name}++
                    or die "invalid or duplicate hash package\n";
            }
            for my $purpose (values %{$inventory->{approved}}) {
                defined($purpose) && !ref($purpose) && $purpose =~ /\S/
                    or die "missing approved hash purpose\n";
            }
            %hash_review = map {$_ => 1} @{$inventory->{review_required}};
        }
        my %rules;
        for my $rule (@$policy) {
            ref($rule) eq "HASH" && keys(%$rule) == 2
                && exists($rule->{dependency}) && exists($rule->{allowed_packages})
                or die "expected dependency and allowed_packages\n";
            my $name = $rule->{dependency};
            defined($name) && !ref($name) && $name =~ /\A[A-Za-z0-9_-]+\z/
                or die "invalid dependency name\n";
            exists($rules{$name}) and die "duplicate dependency rule: $name\n";
            ref($rule->{allowed_packages}) eq "ARRAY" or die "expected package array\n";
            my %allowed;
            for my $package (@{$rule->{allowed_packages}}) {
                defined($package) && !ref($package) && $package =~ /\A[A-Za-z0-9_-]+\z/
                    or die "invalid allowed package\n";
                $allowed{$package}++ and die "duplicate allowed package: $package\n";
            }
            $rules{$name} = \%allowed;
        }
        # Admit a complete successful observation before interpreting its bytes.
        # A child may emit valid JSON and then fail or die from a signal.
        open my $cargo, "-|", "cargo", "metadata", "--no-deps", "--locked", "--format-version", "1"
            or die "cannot run Cargo metadata: $!\n";
        my $bytes = <$cargo> // "";
        close $cargo or die "Cargo metadata failed\n";
        my $metadata = decode_json($bytes);
        ref($metadata) eq "HASH" && !ref($metadata->{version}) && ($metadata->{version} // 0) eq "1"
            && defined($metadata->{workspace_root}) && !ref($metadata->{workspace_root})
            && $metadata->{workspace_root} eq $ARGV[1]
            && ref($metadata->{workspace_members}) eq "ARRAY" && @{$metadata->{workspace_members}}
            && ref($metadata->{packages}) eq "ARRAY"
            or die "invalid Cargo workspace metadata\n";
        my (%members, %packages, %violations);
        for my $member (@{$metadata->{workspace_members}}) {
            defined($member) && !ref($member) && length($member) && !$members{$member}++
                or die "invalid or duplicate workspace member\n";
        }
        for my $package (@{$metadata->{packages}}) {
            ref($package) eq "HASH" && defined($package->{id}) && !ref($package->{id})
                && length($package->{id}) && !$packages{$package->{id}}++
                && defined($package->{name}) && !ref($package->{name}) && length($package->{name})
                && ref($package->{dependencies}) eq "ARRAY"
                or die "invalid or duplicate Cargo package\n";
            for my $dependency (@{$package->{dependencies}}) {
                ref($dependency) eq "HASH" && defined($dependency->{name})
                    && !ref($dependency->{name}) && length($dependency->{name})
                    or die "invalid Cargo dependency\n";
                my $name = $dependency->{name};
                $violations{"$package->{name} -> $name (hash/crypto dependency requires shared review: rules/hashing.md)"} = 1
                    if $members{$package->{id}} && $hash_review{$name};
                $violations{"$package->{name} -> $name"} = 1
                    if $members{$package->{id}} && (
                        (exists($rules{$name}) && !$rules{$name}{$package->{name}})
                        || ($cbor{$name} && $package->{name} ne "ic-platform"
                            && !($package->{name} eq "ic-platform-types"
                                && ($dependency->{kind} // "") eq "dev"))
                        || ($common{$name} && $package->{name} ne "ic-platform"
                            && !($package->{name} eq "ic-platform-types" && $name eq "ic_principal")
                            && !($package->{name} eq "ic-wire-benchmarks" && $name eq "candid"
                                && ($dependency->{kind} // "") eq "dev" && !defined($dependency->{target})
                                && ref($package->{publish}) eq "ARRAY" && !@{$package->{publish}})
                            && !($package->{name} eq "ic-testkit"
                                && ($name eq "candid" || $name eq "ic-certification" || $name eq "ic-management-canister-types" || $name eq "ic-transport-types")
                                && !defined($dependency->{kind})
                                && ($dependency->{target} // "") eq q{cfg(not(target_family = "wasm"))})));
            }
        }
        $packages{$_} or die "missing workspace member package\n" for keys %members;
        die "Forbidden direct Cargo dependencies:\n" . join("\n", sort keys %violations) . "\n"
            if keys %violations;
        print "Cargo dependency boundaries passed.\n";
    ' "$policy" "$PWD" "$common" "$cbor" "$hashes"
