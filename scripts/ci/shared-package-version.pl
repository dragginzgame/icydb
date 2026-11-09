#!/usr/bin/env perl
# Read/edit Toko Tooling's canonical literal [package].version without tool setup.
use strict;
use warnings;
my $set;
if (($ARGV[0] // '') eq '--set') { shift; $set=shift; }
@ARGV==1 or die "usage: shared-package-version.pl [--set X.Y.Z] FILE|-\n";
my $path=shift;
my $f;
if ($path eq '-') { $f=*STDIN; }
else { -f $path && !-l $path or die "expected regular manifest: $path\n"; open $f,'<',$path or die "$path: $!\n"; }
my $text=do {local $/; <$f>};
my ($in_package,$packages,$versions,$version)=(0,0,0,undef);
# This repo deliberately keeps one canonical package table and a literal version.
# This is not a general Cargo/TOML parser or an inheritance implementation.
my @lines=split /(?<=\n)/,$text;
for my $line (@lines) {
    if ($line =~ /^\s*\[/) {
        $in_package=($line =~ /^\[package\][ \t]*(?:#.*)?\n?\z/) ? 1 : 0;
        $packages++ if $in_package;
    }
    next unless $in_package && $line =~ /^\s*version\s*=/;
    $versions++;
    $line =~ /^version[ \t]*=[ \t]*"((?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*))"[ \t]*(?:#.*)?\n?\z/
        or die "package.version must be a canonical literal stable version\n";
    $version=$1;
    if (defined $set) {
        $set =~ /\A(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\z/ or die "invalid replacement version\n";
        $line =~ s/"\Q$version\E"/"$set"/;
    }
}
$packages==1 && $versions==1 or die "expected one literal [package].version\n";
print defined($set) ? join('',@lines) : "$version\n";
