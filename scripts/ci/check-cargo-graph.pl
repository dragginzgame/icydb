#!/usr/bin/env perl
# Shared companions: docs/cargo-graph-checks.md
use strict;
use warnings;
use Cwd qw(abs_path);
use Getopt::Long qw(GetOptions);

# Observe one explicitly selected locked graph. Policy and build qualification
# remain consumer-owned; this is not artifact/deployment admission.
Getopt::Long::Configure(qw(no_auto_abbrev no_ignore_case));
my ($consumer, $target, $features, $all_features, $no_defaults, $depth);
my (@packages, @forbidden, @required, @patterns, @maximums, @forbidden_features);
GetOptions(
    'consumer=s' => \$consumer, 'target=s' => \$target,
    'package=s@' => \@packages, 'features=s' => \$features,
    'all-features' => \$all_features, 'no-default-features' => \$no_defaults,
    'forbid=s@' => \@forbidden, 'require=s@' => \@required,
    'forbid-pattern=s@' => \@patterns, 'max-count=s@' => \@maximums, 'depth=s' => \$depth,
    'forbid-feature=s@' => \@forbidden_features,
) && !@ARGV && defined($consumer) && defined($target) && @packages
    && (@forbidden || @required || @patterns || @maximums || @forbidden_features)
    or die "usage: check-cargo-graph.pl --consumer ROOT --package NAME [--package NAME] --target TARGET [--features LIST|--all-features] [--no-default-features] [--forbid-feature PACKAGE/FEATURE] [--depth COUNT] [--forbid NAME] [--forbid-pattern REGEX] [--require NAME=COUNT] [--max-count NAME=COUNT]\n";
defined($features) && $all_features and die "select features or all-features, not both\n";
$consumer !~ /[\r\n]/ or die "consumer directory contains CR/LF\n";
my $root = abs_path($consumer);
defined($root) && -d $root && $root !~ /[\r\n]/
    or die "invalid resolved consumer directory\n";
chdir $root or die "cannot enter consumer: $!\n";
-f 'Cargo.toml' && !-l 'Cargo.toml' or die "consumer requires a regular Cargo.toml\n";
$target =~ /\A[A-Za-z0-9_.-]+\z/ && $target !~ /^-/
    or die "expected an explicit target triple or all\n";
!defined($features) || (length($features) && $features !~ /[\r\n]/)
    or die "invalid feature selection\n";
!defined($depth) || $depth =~ /\A(?:0|[1-9][0-9]*)\z/
    or die "invalid graph depth\n";
my @compiled_patterns;
for my $pattern (@patterns) {
    length($pattern) && $pattern !~ /[\r\n]/ or die "invalid forbidden pattern\n";
    my $compiled = eval { qr/$pattern/ };
    defined($compiled) or die "invalid forbidden pattern: $@";
    push @compiled_patterns, $compiled;
}
for my $selection (@forbidden_features) {
    $selection =~ /\A[A-Za-z0-9_-]+\/[^\s\/"\r\n]+\z/
        or die "expected PACKAGE/FEATURE\n";
}
my (%roots, %denied, %counts, %caps);
for my $name (@packages) {
    $name =~ /\A[A-Za-z0-9_-]+\z/ && !$roots{$name}++ or die "invalid or duplicate root package\n";
}
for my $name (@forbidden) {
    $name =~ /\A[A-Za-z0-9_-]+\z/ or die "invalid forbidden package\n";
    $denied{$name} = 1;
}
for my $requirement (@required) {
    $requirement =~ /\A([A-Za-z0-9_-]+)=(0|[1-9][0-9]*)\z/
        or die "expected NAME=COUNT\n";
    my ($name, $count) = ($1, $2);
    !exists($counts{$name}) && !exists($denied{$name})
        or die "duplicate or conflicting identity count\n";
    $counts{$name} = $count;
}
for my $requirement (@maximums) {
    $requirement =~ /\A([A-Za-z0-9_-]+)=(0|[1-9][0-9]*)\z/
        or die "expected NAME=COUNT\n";
    my ($name, $count) = ($1, $2);
    !exists($caps{$name}) && !exists($counts{$name}) && !exists($denied{$name})
        or die "duplicate or conflicting identity count\n";
    $caps{$name} = $count;
}
my @command = ('cargo', 'tree', '--locked', '--offline', '--color', 'never',
    '--target', $target, '--edges', @forbidden_features ? 'normal,build,features' : 'normal,build',
    '--prefix', 'none', '--format', '{p}');
push @command, ('--package', $_) for @packages;
push @command, ('--depth', $depth) if defined $depth;
push @command, '--all-features' if $all_features;
push @command, '--no-default-features' if $no_defaults;
push @command, ('--features', $features) if defined $features;
$ENV{CARGO_NET_OFFLINE} = 'true';
$ENV{RUSTUP_AUTO_INSTALL} = '0';
open my $cargo, '-|', @command or die "cannot observe Cargo graph: $!\n";
my $graph;
{ local $/; $graph = <$cargo> // ''; }
unless (close $cargo) {
    my $status = $?;
    print STDERR $graph;
    print STDERR "Cargo graph observation failed; no graph was admitted\n";
    exit(($status & 127) ? 128 + ($status & 127) : ($status >> 8) || 1);
}
my (%identities, %observed_features);
for my $line (split /\n/, $graph) {
    next if $line eq ''; # Cargo separates multiple selected roots with a blank.
    $line =~ s/ \(\*\)\z//;
    if (@forbidden_features && $line =~ /\A([A-Za-z0-9_-]+) feature "([^"\r\n]+)"\z/) {
        $observed_features{"$1/$2"} = 1;
        next;
    }
    $line =~ /\A([A-Za-z0-9_-]+) v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.+-]+)?(?: \([^\r\n]+\))?\z/
        or die "invalid flat Cargo package observation: $line\n";
    $identities{$1}{$line} = 1;
}
keys(%identities) or die "empty Cargo graph\n";
exists($identities{$_}) or die "selected root absent from Cargo graph: $_\n" for @packages;
my @violations;
for my $selection (@forbidden_features) {
    push @violations, "forbidden dependency feature edge: $selection" if $observed_features{$selection};
}
for my $name (sort keys %denied) {
    push @violations, "forbidden $name: " . join('; ', sort keys %{$identities{$name}})
        if exists $identities{$name};
}
for my $name (sort keys %identities) {
    push @violations, "forbidden pattern matches $name"
        if grep { $name =~ $_ } @compiled_patterns;
}
for my $name (sort keys %caps) {
    my $observed = exists($identities{$name}) ? scalar(keys %{$identities{$name}}) : 0;
    # Compare nonnegative decimal strings without machine-integer overflow.
    my $maximum = $caps{$name};
    push @violations, "$name: maximum $maximum rendered package identities, observed $observed"
        if length("$observed") > length($maximum)
            || (length("$observed") == length($maximum) && "$observed" gt $maximum);
}
for my $name (sort keys %counts) {
    my $observed = exists($identities{$name}) ? scalar(keys %{$identities{$name}}) : 0;
    push @violations, "$name: expected $counts{$name} rendered package identities, observed $observed"
        if "$observed" ne $counts{$name};
}
if (@violations) {
    print STDERR $graph;
    die "Cargo graph policy failed:\n" . join("\n", @violations) . "\n";
}
print "Cargo graph passed: ", join(', ', @packages), " on $target (normal/build edges).\n";
