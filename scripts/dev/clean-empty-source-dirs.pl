#!/usr/bin/env perl
use strict;
use warnings;
use Cwd qw(abs_path);
use Errno qw(ENOTEMPTY EEXIST ENOENT);

# Default to existing conventional source roots, never the whole checkout.
open my $git, '-|', 'git', 'rev-parse', '--show-toplevel' or die "git: $!\n";
chomp(my $root = <$git> // '');
close $git or die "not a Git checkout\n";
abs_path('.') eq $root or die "run from the repository root\n";
my %protected = map { $_ => 1 } qw(.git .tools .cache .tmp target build dist coverage node_modules vendor recovery-evidence);
if (!@ARGV) {
    @ARGV = grep { -d $_ && !-l $_ } qw(
        src crates apps backend fleets canisters packages tests testing schema
        profiler scripts frontend/src frontend/tests
    );
}
for my $path (@ARGV) {
    $path !~ m{^/|[\r\n]} or die "expected repository-relative source path: $path\n";
    my @parts = split m{/}, $path, -1;
    my $prefix = '';
    for my $part (@parts) {
        length($part) && $part ne '.' && $part ne '..' && !$protected{$part}
            or die "unsafe source path: $path\n";
        $prefix .= (length($prefix) ? '/' : '') . $part;
        !-l $prefix or die "symlink source path: $prefix\n";
    }
    -d $path or die "source directory missing: $path\n";
}
# Protect indexed paths even when their tracked files are currently deleted.
open my $index, '-|', 'git', 'ls-files', '-z' or die "git: $!\n";
my %tracked;
{
    local $/ = "\0";
    while (my $path = <$index>) {
        chomp $path;
        $tracked{$path} = 1;
        while ($path =~ s{/[^/]+$}{}) { $tracked{$path} = 1; }
    }
}
close $index or die "cannot read Git index\n";
my $deleted = 0;
sub visit {
    my ($path, $is_root) = @_;
    return if -l $path || !-d $path;
    opendir my $dir, $path or die "read $path: $!\n";
    my @children = sort grep { $_ ne '.' && $_ ne '..' } readdir $dir;
    closedir $dir or die "close $path: $!\n";
    for my $child (@children) {
        next if $protected{$child};
        visit("$path/$child", 0);
    }
    return if $is_root || $tracked{$path} || -l $path;
    opendir $dir, $path or die "read $path: $!\n";
    my @remaining = grep { $_ ne '.' && $_ ne '..' } readdir $dir;
    closedir $dir;
    return if @remaining;
    my $status = system 'git', 'check-ignore', '-q', '--', "$path/";
    return if $status == 256;
    $status == 0 or die "Git ignore check failed for $path\n";
    if (rmdir $path) { ++$deleted; }
    elsif (!$!{ENOTEMPTY} && !$!{EEXIST} && !$!{ENOENT}) { die "remove $path: $!\n"; }
}
visit($_, 1) for @ARGV;
print "Deleted $deleted empty ignored source directories.\n";
