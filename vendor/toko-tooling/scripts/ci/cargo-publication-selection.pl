#!/usr/bin/env perl
use strict;
use warnings;
use JSON::PP qw(decode_json);

# Private metadata projection for the opt-in publication Make adapter.
@ARGV == 4 or die "usage: cargo-publication-selection.pl METADATA ELIGIBLE SELECTED REGISTRY\n";
my ($path, $eligible_text, $selected_text, $registry) = @ARGV;
sub names {
    my ($text) = @_;
    $text !~ /[\r\n]/ or die "publication package list contains CR/LF\n";
    $text =~ s/\A[ \t]+|[ \t]+\z//g;
    length $text or die "select at least one publication package\n";
    my @names = split /[ \t]+/, $text;
    my %seen;
    for my $name (@names) {
        $name =~ /\A[A-Za-z][A-Za-z0-9_-]*\z/ && !$seen{$name}++
            or die "invalid or duplicate publication package: $name\n";
    }
    return @names;
}
my @eligible = names($eligible_text);
my @selected = names($selected_text);
my %eligible = map { $_ => 1 } @eligible;
for my $name (@selected) {
    $eligible{$name} or die "package is outside the publication roster: $name\n";
}
open my $in, '<', $path or die "cannot read publication metadata: $!\n";
local $/;
my $metadata = decode_json(<$in>);
close $in or die "cannot close publication metadata: $!\n";
ref($metadata) eq 'HASH' && ($metadata->{version} // 0) == 1
    && ref($metadata->{packages}) eq 'ARRAY' && ref($metadata->{workspace_members}) eq 'ARRAY'
    or die "invalid publication workspace metadata\n";
my %members = map { $_ => 1 } @{$metadata->{workspace_members}};
my %packages;
for my $package (@{$metadata->{packages}}) {
    ref($package) eq 'HASH' or die "invalid publication package metadata\n";
    next unless $members{$package->{id}};
    my $name = $package->{name};
    defined($name) && !exists($packages{$name})
        or die "invalid or duplicate workspace package\n";
    $packages{$name} = $package;
}
for my $name (@eligible) {
    my $package = $packages{$name} or die "publication package is not a workspace member: $name\n";
    !defined($package->{source}) && ($package->{version} // '') =~ /\A(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\z/
        or die "publication requires a local package with a stable version: $name\n";
    if (defined $package->{publish}) {
        ref($package->{publish}) eq 'ARRAY' && grep { $_ eq $registry } @{$package->{publish}}
            or die "package does not admit the selected registry: $name\n";
    }
}
# Publish no partial selection if admission of any roster member failed.
for my $name (@selected) { print "$name\@$packages{$name}{version}\0"; }
