#!/usr/bin/env perl
# Product stage classification; Shared retains execution and evidence admission.
use strict;
use warnings;
use JSON::PP;
use Digest::SHA qw(sha256_hex);

my ($selection) = @ARGV;
@ARGV == 1 && $selection =~ /\A(?:check|fast|collect|full)\z/
    or die "usage: validation-stages.pl check|fast|collect|full\n";
sub bytes {
    my ($path) = @_;
    -f $path && !-l $path or die "missing regular stage declaration $path\n";
    open my $in, '<:raw', $path or die "read $path: $!\n";
    defined(read($in, my $raw, 1048577)) or die "read $path: $!\n";
    length($raw) <= 1048576 && eof($in) or die "stage declaration exceeds 1 MiB\n";
    return $raw;
}
my $policy = decode_json(bytes('ci/release-validation.json'));
my $mapping_raw = bytes('ci/release-validation-contract.json');
my $mapping = decode_json($mapping_raw);
my (%stages, %supplemental, %collect);
for (@{$policy->{stages}}) {
    /\A[A-Za-z0-9][A-Za-z0-9_-]*\z/ && !$stages{$_}++
        or die "invalid or duplicate stage\n";
}
for (@{$mapping->{supplemental_stages}}) {
    exists($stages{$_}) && !$supplemental{$_}++ or die "invalid supplemental classification\n";
}
for (@{$mapping->{collect_stages}}) {
    exists($stages{$_}) && !exists($supplemental{$_}) && !$collect{$_}++
        or die "invalid collecting classification\n";
}
my @full = grep { !exists $supplemental{$_} } @{$policy->{stages}};
my @fast = grep { !exists $collect{$_} } @full;
@fast >= 2 && $fast[0] eq 'tools-check' && $fast[1] eq 'check-rust-compiler'
    && keys(%collect) && grep($_ eq 'clippy', @fast)
    or die "full gate must retain lint before collecting stages\n";
if ($selection eq 'check') {
    # Production validate must admit the reviewed roster before deriving its
    # commands; the pilot selector already admits these same declarations.
    $mapping->{schema} == 1
        && $mapping_raw eq JSON::PP->new->canonical->utf8->encode($mapping)."\n"
        or die "invalid consumer mapping contract\n";
    for my $path ('Makefile', 'ci/cargo-release-groups.json', 'ci/release-validation.json') {
        sha256_hex(bytes($path)) eq ($mapping->{files}{$path}//'')
            or die "release recipes/group policy changed; requalify mapping\n";
    }
    exit 0;
}
my @selected = $selection eq 'full' ? @full
    : $selection eq 'fast' ? @fast : grep { exists $collect{$_} } @full;
print "$_\n" for @selected;
