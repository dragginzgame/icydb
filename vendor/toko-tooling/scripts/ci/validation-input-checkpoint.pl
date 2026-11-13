#!/usr/bin/env perl
# Shared companions: scripts/ci/verify-file-checksum.sh
# Private companion of the source-snapshotted validation runner.
use strict;
use warnings;
use JSON::PP;
my ($root, $baseline, $output, $checksum, @paths) = @ARGV;
my $json = JSON::PP->new->canonical;
my $expected = {};
if (length $baseline) {
    open my $in, '<', $baseline or die "error: read checkpoint $baseline: $!\n";
    local $/;
    $expected = $json->decode(<$in>);
    close $in or die "error: close checkpoint: $!\n";
}
my %observed;
my $failed = 0;
for my $path (sort keys %{{map { $_ => 1 } (keys %$expected, @paths)}}) {
    die "error: checkpoint inputs must be relative file paths: $path\n"
        if $path =~ m{^/|[\x00\r\n]|(?:^|/)\.{1,2}(?:/|$)|//|/$};
    my $hash;
    eval {
        my $route = $root;
        for my $part (split m{/}, $path) {
            $route .= "/$part";
            die "symlink in input route" if -l $route;
        }
        die "not a regular readable file" unless -f $route && -r $route;
        open my $in, '-|', 'bash', $checksum, '--print', 'sha256', $route or die "checksum: $!";
        local $/;
        $hash = <$in>;
        close $in or die "checksum observation failed";
        $hash =~ s/\n\z//;
        die "invalid checksum" unless $hash =~ /\A[0-9a-f]{64}\z/;
    };
    if ($@) {
        $observed{$path} = {error => "$@"};
        print STDERR "error: cannot observe validation input $path: $@\n";
        $failed = 1;
    } else {
        $observed{$path} = $hash;
        if (exists $expected->{$path} && $expected->{$path} ne $hash) {
            print STDERR "error: validation input changed: $path ($expected->{$path} -> $hash)\n";
            $failed = 1;
        }
    }
}
open my $out, '>', $output or die "error: write input evidence $output: $!\n";
print {$out} $json->encode(\%observed), "\n" or die "error: write input evidence: $!\n";
close $out or die "error: close input evidence: $!\n";
if ($failed) {
    print STDERR "Restart preparation and the full gate against the current inputs; prior validation cannot be reused.\n";
    exit 1;
}
