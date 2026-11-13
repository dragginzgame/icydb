#!/usr/bin/env perl
# Internal projection of already admitted installer selections; no tool execution.
use strict;
use warnings;
use Cwd qw(abs_path);
use Digest::SHA qw(sha256_hex);
use JSON::PP;

my ($kind, $consumer, $source, $catalog, @recipe) = @ARGV;
defined($catalog) && $kind =~ /\A(?:host|rust|ic)\z/
    or die "expected kind, consumer, source and catalog\n";
for ($consumer, $source) {
    /[\r\n]/ and die "invalid selection path\n";
    $_ = abs_path($_) // die "missing selection path\n";
    /[\r\n]/ and die "invalid physical selection path\n";
}
open my $file, '<', $source or die "cannot read installer source: $!\n";
my $bytes = do { local $/; <$file> };
close $file or die "cannot close installer source: $!\n";
my $result = { kind => $kind, consumer => $consumer, source => $source,
    source_sha256 => sha256_hex($bytes), admission => 'selection only; not installed-tool qualification' };
if (length $catalog) {
    !@recipe && $catalog !~ /[\r\n]/ or die "invalid catalog projection\n";
    $catalog = abs_path($catalog) // die "missing catalog\n";
    $catalog !~ /[\r\n]/ or die "invalid physical catalog path\n";
    $result->{catalog} = $catalog;
    my @records;
    while (<STDIN>) {
        chomp;
        my @fields = split /\t/, $_, -1;
        @fields == 4 or die "invalid admitted catalog row\n";
        push @records, { tool => $fields[0], version => $fields[1], host => $fields[2], digest => $fields[3] };
    }
    @records or die "empty admitted selection\n";
    $result->{records} = \@records;
} else {
    $kind eq 'rust' && @recipe == 7 or die "invalid selected Cargo recipe\n";
    @{$result}{qw(package version target_kind target profile features lockfile)} = @recipe;
}
print JSON::PP->new->canonical->pretty->encode($result);
