#!/usr/bin/env perl
# Cargo owns selection; this adapter admits the source-bundle dependency contract.
use strict;
use warnings;
use JSON::PP;
use Cwd qw(abs_path);
my $json=JSON::PP->new->canonical;
@ARGV == 1 or die "usage: tooling-lock.pl CONSUMER\n";
my $root=abs_path($ARGV[0]) // die "missing consumer\n";
chdir $root or die "cannot enter consumer: $!\n";
sub command_json {
    open my $fh, '-|', @_ or die "cannot run $_[0]: $!\n";
    local $/; my $text=<$fh>;
    close $fh or die "tooling admission command failed: $_[0]\n";
    return $json->decode($text);
}
sub toml {
    my $path=shift;
    -f $path && !-l $path or die "expected regular file: $path\n";
    return command_json('yq','-p','toml','-o','json','.',$path);
}
sub tooling_declaration {
    my ($manifest) = @_;
    my $direct = $manifest->{'dev-dependencies'}{'toko-tooling'};
    my $marker = $manifest->{target}{'cfg(any())'}{'dev-dependencies'}{'toko-tooling'};
    !(defined($direct) && defined($marker))
        or die "Declare toko-tooling once, directly or under target.'cfg(any())'.dev-dependencies\n";
    # A source-bundle pin remains in Cargo metadata/lock without being linked.
    return defined($marker) ? $marker : $direct;
}
my $manifest=toml("$root/Cargo.toml");
my $workspace=exists($manifest->{workspace});
!exists($manifest->{workspace}{metadata}{'toko-tooling'})
    && !grep({ $_ ne 'loc' } keys %{$manifest->{package}{metadata}{'toko-tooling'} // {}})
    && !-e "$root/toko-tooling.lock" && !-l "$root/toko-tooling.lock"
    or die "Remove the old toko-tooling metadata tables and toko-tooling.lock; select tooling through Cargo dependencies and Cargo.lock.\n";
my $selection=$workspace ? $manifest->{workspace}{dependencies}{'toko-tooling'}
    : tooling_declaration($manifest);
my $requirement;
if (!ref($selection)) {$requirement=$selection;}
elsif (ref($selection) eq 'HASH') {
    !grep {$_ ne 'version' && $_ ne 'default-features'} keys %$selection
        or die "toko-tooling must select crates.io directly, without aliases, paths, features or alternate registries\n";
    $requirement=$selection->{version};
}
defined($requirement) && $requirement =~ /\A=((?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*))\z/
    or die "Declare toko-tooling = \"=X.Y.Z\" in root workspace.dependencies (or standalone dev-dependencies).\n";
my $version=$1;
my ($major,$minor)=split /\./,$version;
($major>0 || $minor>=36) or die "Cargo-visible per-package tooling selection requires toko-tooling 0.36.0 or later\n";
for my $registry (values %{$manifest->{patch} // {}}) {
    for my $name (keys %$registry) {
        my $entry=$registry->{$name};
        $name ne 'toko-tooling' && !(ref($entry) eq 'HASH' && ($entry->{package}//'') eq 'toko-tooling')
            or die "toko-tooling source overrides are not allowed\n";
    }
}
my $metadata=command_json('cargo','metadata','--format-version','1','--no-deps','--locked','--offline','--manifest-path',"$root/Cargo.toml");
$metadata->{workspace_root} eq $root or die "Run tooling admission at its Cargo workspace root\n";
my %members=map {$_=>1} @{$metadata->{workspace_members}};
my @packages=grep {$members{$_->{id}}} @{$metadata->{packages}};
@packages or die "Tooling selection requires real Cargo packages; do not create selection-only helper crates\n";
my $registry='registry+https://github.com/rust-lang/crates.io-index';
for my $package (@packages) {
    my $path=$package->{manifest_path};
    my $member=toml($path);
    if ($workspace) {
        my $dep=tooling_declaration($member);
        ref($dep) eq 'HASH' && keys(%$dep)==1 && JSON::PP::is_bool($dep->{workspace}) && $dep->{workspace}
            or die "$path: every workspace package must inherit toko-tooling in dev-dependencies (directly or under target.'cfg(any())')\n";
    } else {
        @packages==1 && $path eq "$root/Cargo.toml"
            or die "A workspace must select toko-tooling in root workspace.dependencies\n";
    }
    my @deps=grep {$_->{name} eq 'toko-tooling'} @{$package->{dependencies}};
    @deps==1 && ($deps[0]{kind}//'') eq 'dev' && (!defined($deps[0]{target}) || $deps[0]{target} eq 'cfg(any())')
        && !defined($deps[0]{rename}) && !defined($deps[0]{path})
        && ($deps[0]{source}//'') eq $registry && $deps[0]{req} eq "=$version" && !$deps[0]{optional}
        or die "$path: require exactly one crates.io toko-tooling dev-dependency at =$version, directly or under target.'cfg(any())'\n";
}
my $lock=toml("$root/Cargo.lock");
my @locked=grep {$_->{name} eq 'toko-tooling'} @{$lock->{package}//[]};
@locked==1 && $locked[0]{version} eq $version && ($locked[0]{source}//'') eq $registry
    && ($locked[0]{checksum}//'') =~ /\A[0-9a-f]{64}\z/
    or die "Cargo.lock must select exactly one crates.io toko-tooling $version with its checksum; run cargo update -p toko-tooling\n";
print $json->encode({version=>$version,checksum=>$locked[0]{checksum}}),"\n";
