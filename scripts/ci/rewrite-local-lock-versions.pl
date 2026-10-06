#!/usr/bin/env perl
use strict;
use warnings;

# Cargo owns TOML and dependency resolution. This transforms Cargo-generated
# lockfiles without reserializing or resolving their external selections.
@ARGV >= 4 or die "usage: rewrite-local-lock-versions.pl LOCKFILE PREVIOUS CANDIDATE PACKAGE...\n";
my ($path, $previous, $candidate, @packages) = @ARGV;
for ($previous, $candidate) {
    /\A(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\z/
        or die "expected canonical stable version\n";
}
$previous ne $candidate or die "previous and candidate must differ\n";
my %selected;
for (@packages) {
    /\A[A-Za-z0-9][A-Za-z0-9_-]*\z/ or die "invalid package name\n";
    !$selected{$_}++ or die "duplicate selected package: $_\n";
}
-f $path && !-l $path or die "expected regular lockfile\n";
open my $input, '<', $path or die "$path: $!\n";
my $text = do { local $/; <$input> };
close $input or die "$path: $!\n";
defined($text) && $text =~ /^version = [34]$/m && $text !~ /\r/
    or die "expected Cargo-generated LF lockfile (format 3 or 4)\n";
my %changed;
my @sections = split /(?=^\[)/m, $text;
for my $section (@sections) {
    next unless $section =~ /\A\[\[package\]\]\n/;
    my @names = $section =~ /^name = "([^"]+)"$/mg;
    @names == 1 or die "malformed package identity\n";
    my $name = $names[0];
    if ($selected{$name} && $section !~ /^source\s*=/m) {
        !$changed{$name}++ or die "duplicate local package: $name\n";
        my @versions = $section =~ /^version = "([^"]+)"$/mg;
        @versions == 1 && $versions[0] eq $previous
            or die "local package version mismatch: $name\n";
        $section =~ s/^version = "\Q$previous\E"$/version = "$candidate"/m;
    }
    # Only unqualified exact dependency entries select a local identity. Leave
    # source-qualified registry/Git references and all other bytes untouched.
    $section =~ s{(^dependencies = \[\n)(.*?)(^\])}{
        my ($start, $entries, $end) = ($1, $2, $3);
        for my $package (@packages) {
            $entries =~ s/^([ \t]*"\Q$package\E )\Q$previous\E("[,]?[ \t]*)$/$1$candidate$2/gm;
        }
        $start . $entries . $end
    }gmse;
}
for (@packages) {
    ($changed{$_} // 0) == 1 or die "missing local package: $_\n";
}
print join('', @sections) or die "lockfile output: $!\n";
