#!/usr/bin/env bash
# Consumer-owned policy; canonical Cargo package names include renamed dependencies.
set -euo pipefail
[[ $# -eq 2 && "$1" == --consumer ]] || {
    echo 'usage: check-cargo-dependency-boundaries.sh --consumer ROOT' >&2
    exit 2
}
consumer="$2"
case "$consumer$PWD" in *$'\n'*|*$'\r'*) echo 'consumer directory contains CR/LF' >&2; exit 2 ;; esac
[[ "$consumer" == /* ]] || consumer="$PWD/$consumer"
cd -P -- "$consumer"
case "$PWD" in *$'\n'*|*$'\r'*) echo 'resolved consumer directory contains CR/LF' >&2; exit 2 ;; esac
[[ -f Cargo.toml && ! -L Cargo.toml ]] || { echo 'consumer requires a regular Cargo.toml' >&2; exit 2; }
policy=ci/cargo-dependency-boundaries.json
[[ -f "$policy" && ! -L "$policy" ]] || {
    echo "missing or non-regular dependency boundary policy: $policy" >&2
    exit 1
}
CARGO_NET_OFFLINE=true RUSTUP_AUTO_INSTALL=0 perl -MJSON::PP -e '
        use strict;
        use warnings;
        local $/;
        open my $file, "<", $ARGV[0] or die "cannot read boundary policy: $!\n";
        my $policy = decode_json(<$file>);
        close $file or die "cannot close boundary policy: $!\n";
        ref($policy) eq "ARRAY" && @$policy or die "expected a nonempty policy array\n";
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
                $violations{"$package->{name} -> $name"} = 1
                    if $members{$package->{id}} && exists($rules{$name}) && !$rules{$name}{$package->{name}};
            }
        }
        $packages{$_} or die "missing workspace member package\n" for keys %members;
        die "Forbidden direct Cargo dependencies:\n" . join("\n", sort keys %violations) . "\n"
            if keys %violations;
        print "Cargo dependency boundaries passed.\n";
    ' "$policy" "$PWD"
