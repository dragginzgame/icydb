#!/usr/bin/env perl
# Shared companions: scripts/ci/ic-tool-pins.awk
use strict;
use warnings;
use FindBin qw($RealBin);

# Parse data, never source a catalog. The cache mode preserves its historical
# byte fallback for catalogs whose literal selections cannot be established.
my ($kind, $path) = @ARGV;
@ARGV == 2 && $kind =~ /\A(?:host|rust|ic|cargo-cache)\z/
    or die "usage: tool-catalog-records.pl host|rust|ic|cargo-cache CATALOG\n";
if ($kind eq 'ic') {
    open my $rows, '-|', 'awk', '-v', 'records=1', '-f', "$RealBin/ic-tool-pins.awk", '--', $path
        or die "cannot run IC pin parser: $!\n";
    my @records = <$rows>;
    close $rows or die "invalid IC catalog: $path\n";
    print sort @records;
    exit 0;
}
open my $input, '<', $path or die "cannot read catalog $path: $!\n";
my $bytes = do { local $/; <$input> // '' };
close $input or die "cannot close catalog: $!\n";
my (%pins, $invalid);
for my $line (split /\n/, $bytes) {
    next if $line =~ /\A[ \t]*(?:#.*)?\z/;
    unless ($line =~ /\A[ \t]*(?:export[ \t]+)?(SHARED_TOOLING_[A-Z0-9_]+_(?:VERSION|SHA256(?:_[A-Z0-9_]+)?))=([A-Za-z0-9_.+-]+)(?:[ \t]+#.*)?[ \t]*\z/
        && !exists $pins{$1}) { $invalid = 1; last; }
    $pins{$1} = $2;
}
my @rust = qw(CARGO_SORT CARGO_SORT_DERIVES CANDID_EXTRACTOR);
my $stable = qr/(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)/;
if ($kind eq 'cargo-cache') {
    if (!$invalid && !grep { !defined($pins{"SHARED_TOOLING_${_}_VERSION"}) || $pins{"SHARED_TOOLING_${_}_VERSION"} !~ /\A$stable\z/ } @rust) {
        print "literal-cargo-pins\n", map { "$_=$pins{\"SHARED_TOOLING_${_}_VERSION\"}\n" } sort @rust;
    } else { print "catalog-bytes\n", $bytes; }
    exit 0;
}
die "unresolved nonliteral/duplicate catalog: $path\n" if $invalid;
my @records;
for my $tool ($kind eq 'rust' ? @rust : qw(JQ YQ RIPGREP CLOC)) {
    my $version = $pins{"SHARED_TOOLING_${tool}_VERSION"};
    defined($version) && ($version =~ /\A$stable\z/ || ($tool eq 'CLOC' && $version =~ /\A[0-9]+\.[0-9]+\z/))
        or die "missing/invalid $tool version in $path\n";
    if ($kind eq 'rust') { push @records, "$tool\t$version\tall\t-\n"; next; }
    for my $host ($tool eq 'CLOC' ? ('all') : qw(LINUX_AMD64 LINUX_ARM64 DARWIN_AMD64 DARWIN_ARM64)) {
        my $key = "SHARED_TOOLING_${tool}_SHA256" . ($host eq 'all' ? '' : "_$host");
        defined($pins{$key}) && $pins{$key} =~ /\A[0-9a-f]{64}\z/ or die "missing/invalid $key in $path\n";
        push @records, "$tool\t$version\t$host\t$pins{$key}\n";
    }
}
print sort @records;
