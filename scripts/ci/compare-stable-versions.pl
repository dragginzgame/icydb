#!/usr/bin/env perl
# Shared companions: scripts/ci/StableVersion.pm
use strict;
use warnings;
use FindBin qw($RealBin);
use lib $RealBin;
use StableVersion qw(version_cmp);

# Pure exact ordering. No shell arithmetic, numeric coercion or metadata writes.
@ARGV == 2 or die "usage: compare-stable-versions.pl VERSION BASELINE\n";
my $order = version_cmp(@ARGV);
print "$order\n" or die "version comparison output: $!\n";
close STDOUT or die "version comparison output: $!\n";
