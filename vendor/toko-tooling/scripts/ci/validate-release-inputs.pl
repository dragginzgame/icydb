#!/usr/bin/env perl
# Shared companions: scripts/ci/StableVersion.pm
use strict;
use warnings;
use FindBin qw($RealBin);
use lib $RealBin;
use StableVersion qw(stable);
use Math::BigInt;

# Pure version/date admission and exact u64 release increment calculation.
(@ARGV == 2 && ($ARGV[0] eq 'version' || $ARGV[0] eq 'date')) ||
    (@ARGV == 3 && $ARGV[0] eq 'next-version')
    or die "usage: validate-release-inputs.pl version|date VALUE | next-version X.Y.Z patch|minor|major\n";
my ($kind, $value, $increment) = @ARGV;
if ($kind eq 'version' || $kind eq 'next-version') {
    stable($value) or die "expected canonical stable major.minor.patch version\n";
    my @parts = split /\./, $value;
    for my $part (@parts) {
        length($part) < 20 || (length($part) == 20 && $part le '18446744073709551615')
            or die "release version component exceeds u64\n";
    }
    if ($kind eq 'next-version') {
        my %position = (major => 0, minor => 1, patch => 2);
        exists $position{$increment} or die "release kind must be patch, minor or major\n";
        my $index = $position{$increment};
        $parts[$index] ne '18446744073709551615' or die "release increment exceeds u64\n";
        $parts[$index] = Math::BigInt->new($parts[$index])->binc()->bstr();
        $parts[$_] = '0' for $index + 1 .. 2;
        print join('.', @parts), "\n";
    } else {
        # Keep large integers as decimal strings; do not round through Perl numbers.
        print join("\t", @parts), "\n";
    }
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
