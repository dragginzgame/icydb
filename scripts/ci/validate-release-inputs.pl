#!/usr/bin/env perl
# Shared companions: scripts/ci/StableVersion.pm
use strict;
use warnings;
use FindBin qw($RealBin);
use lib $RealBin;
use StableVersion qw(stable);

# Pure admission: stable u64 version components, or Gregorian UTC date spelling.
# Increment headroom and release/receipt identity remain caller-owned.
@ARGV == 2 or die "usage: validate-release-inputs.pl version|date VALUE\n";
my ($kind, $value) = @ARGV;
if ($kind eq 'version') {
    stable($value) or die "expected canonical stable major.minor.patch version\n";
    my @parts = split /\./, $value;
    for my $part (@parts) {
        length($part) < 20 || (length($part) == 20 && $part le '18446744073709551615')
            or die "release version component exceeds u64\n";
    }
    # Keep large integers as decimal strings; do not round through Perl numbers.
    print join("\t", @parts), "\n";
} elsif ($kind eq 'date') {
    my ($year, $month, $day) = $value =~ /\A([0-9]{4})-([0-9]{2})-([0-9]{2})\z/;
    defined($day) && $year != 0 && $month >= 1 && $month <= 12
        or die "expected Gregorian UTC release date YYYY-MM-DD\n";
    my @days = (0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);
    $days[2] = 29 if $year % 400 == 0 || ($year % 4 == 0 && $year % 100 != 0);
    $day >= 1 && $day <= $days[$month]
        or die "invalid Gregorian UTC release date\n";
    print "$value\n";
} else {
    die "unknown release input kind\n";
}
