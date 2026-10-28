#!/usr/bin/env perl
# Shared companions: scripts/ci/ic-tool-pins.awk scripts/ci/StableVersion.pm
use strict;
use warnings;
use FindBin qw($RealBin);
use lib $RealBin;
use StableVersion qw(stable);

# One literal data contract for installation, inspection and cache identity.
my ($kind, $path, $selected) = @ARGV;
(@ARGV == 2 || @ARGV == 3) && $kind =~ /\A(?:host|rust|ic|ci)\z/
    && (!defined($selected) || $selected =~ /\A[A-Za-z0-9_-]+\z/)
    or die "usage: tool-catalog-records.pl host|rust|ic|ci CATALOG [TOOL]\n";
if ($kind eq 'ic') {
    open my $rows, '-|', 'awk', '-f', "$RealBin/ic-tool-pins.awk", '--', $path
        or die "cannot run IC pin parser: $!\n";
    my @records = <$rows>;
    close $rows or die "invalid IC catalog: $path\n";
    @records = grep { (split /\t/)[0] eq $selected } @records if defined $selected;
    @records or die "tool not present in IC catalog: $selected\n";
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
my @rust = qw(CARGO_SORT CARGO_SORT_DERIVES);
die "unresolved nonliteral/duplicate catalog: $path\n" if $invalid;
my %ci_hosts = (ACTIONLINT => 'LINUX_AMD64', SHELLCHECK => 'LINUX_X86_64', GITLEAKS => 'LINUX_X64');
# Optional tools do not enlarge the common installation or admission roster.
$ci_hosts{SCCACHE} = 'LINUX_AMD64' if defined($selected) && $selected eq 'SCCACHE';
my @records;
for my $tool ($kind eq 'rust' ? @rust : $kind eq 'ci' ? sort keys %ci_hosts : qw(JQ YQ RIPGREP CLOC)) {
    my $version = $pins{"SHARED_TOOLING_${tool}_VERSION"};
    defined($version) && (stable($version) || ($tool eq 'CLOC' && $version =~ /\A[0-9]+\.[0-9]+\z/))
        or die "missing/invalid $tool version in $path\n";
    if ($kind eq 'rust') { push @records, "$tool\t$version\tall\t-\n"; next; }
    if ($kind eq 'ci') {
        my $key = "SHARED_TOOLING_${tool}_SHA256_$ci_hosts{$tool}";
        defined($pins{$key}) && $pins{$key} =~ /\A[0-9a-f]{64}\z/ or die "missing/invalid $key in $path\n";
        push @records, "$tool\t$version\tLINUX_AMD64\t$pins{$key}\n";
        next;
    }
    for my $host ($tool eq 'CLOC' ? ('all') : qw(LINUX_AMD64 LINUX_ARM64 DARWIN_AMD64 DARWIN_ARM64)) {
        my $key = "SHARED_TOOLING_${tool}_SHA256" . ($host eq 'all' ? '' : "_$host");
        defined($pins{$key}) && $pins{$key} =~ /\A[0-9a-f]{64}\z/ or die "missing/invalid $key in $path\n";
        push @records, "$tool\t$version\t$host\t$pins{$key}\n";
    }
}
@records = grep { (split /\t/)[0] eq $selected } @records if defined $selected;
@records or die "tool not present in $kind catalog: " . ($selected // '') . "\n";
print sort @records;
