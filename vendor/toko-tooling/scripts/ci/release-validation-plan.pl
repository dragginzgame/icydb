#!/usr/bin/env perl
# Shared companions: scripts/ci/ValidationPlan.pm
# Private companion of run-validation-targets.sh; dispatch remains with that runner.
use strict;
use warnings;
use JSON::PP;
use FindBin qw($RealBin);
use lib $RealBin;
use ValidationPlan qw(plan);
my ($root, $path, $source, $group, $baseline, $output) = @ARGV;
@ARGV == 6 or die "usage: release-validation-plan.pl ROOT PLAN SOURCE GROUP CHECKPOINT OUTPUT\n";
my $json = JSON::PP->new->canonical->utf8;
my ($plan, $policy, $inputs, $rows, $parsed) = plan($root, $path, $source, $group);
open my $in, '<', $baseline or die "read initial checkpoint: $!\n";
my $expected = do { local $/; $json->decode(<$in>) };
close $in or die "close initial checkpoint: $!\n";
for my $name (keys %$parsed) {
    !exists($expected->{$name}) || $expected->{$name} eq $parsed->{$name}
        or die "plan input changed during selection: $name\n";
    $expected->{$name} = $parsed->{$name};
}
open my $out, '>', $output or die "write plan checkpoint: $!\n";
print {$out} $json->encode($expected), "\n" or die "write plan checkpoint: $!\n";
close $out or die "close plan checkpoint: $!\n";
for my $item (['plan', $plan], ['policy', $policy]) {
    open my $snapshot, '>', "$output.$item->[0].json" or die "retain plan/policy: $!\n";
    print {$snapshot} $json->encode($item->[1]), "\n" or die "retain plan/policy: $!\n";
    close $snapshot or die "close retained plan/policy: $!\n";
}
# Emit only after every decision is admitted; no partial plan can dispatch.
# This is a UTF-8 text stream, independent of each reason's codepoint range.
binmode STDOUT, ':encoding(UTF-8)' or die "encode plan selection: $!\n";
print "input\t$_\n" for @$inputs;
print "$_\n" for @$rows;
