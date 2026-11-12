#!/usr/bin/env perl
use strict;
use warnings;
use Cwd qw(abs_path getcwd);
use File::Basename qw(dirname);
use IO::Handle;

sub usage {
    return "Usage: cargo-dependencies.pl update [--consumer DIR] [--manifest RELATIVE/Cargo.toml] [--package NAME] [--precise VERSION]\n";
}
my @attempted;
eval {
    (shift(@ARGV) // '') eq 'update' or die usage();
    my ($consumer, $package, $precise) = (getcwd(), undef, undef);
    my @extra;
    while (@ARGV) {
        my $arg = shift @ARGV;
        @ARGV or die usage();
        my $value = shift @ARGV;
        if ($arg eq '--consumer') { $consumer = $value; }
        elsif ($arg eq '--manifest') { push @extra, $value; }
        elsif ($arg eq '--package') { defined $package and die "select one package\n"; $package = $value; }
        elsif ($arg eq '--precise') { $precise = $value; }
        else { die usage(); }
    }
    !defined($package) || $package =~ /\A[a-zA-Z0-9][a-zA-Z0-9_-]*\z/ or die "invalid package\n";
    !defined($precise) || (defined($package) && $precise =~ /\A[0-9][0-9A-Za-z.+-]*\z/)
        or die "--precise requires --package and a version\n";
    $consumer !~ /[\r\n]/ && -d $consumer or die "invalid consumer directory\n";
    $consumer = abs_path($consumer);
    $consumer !~ /[\r\n]/ or die "invalid resolved consumer directory\n";
    my @manifests = ('Cargo.toml');
    push @manifests, 'qualification/Cargo.toml' if -e "$consumer/qualification/Cargo.toml";
    push @manifests, @extra;
    my (%seen, @selected);
    # Check every explicit path before allowing Cargo to change any lockfile.
    for my $relative (@manifests) {
        $relative =~ m{\A(?:[a-zA-Z0-9_. -]+/)*Cargo\.toml\z} &&
            !grep { $_ eq '.' || $_ eq '..' } split m{/}, $relative
            or die "select a canonical repository-relative Cargo.toml: $relative\n";
        next if $seen{$relative}++;
        my $path = "$consumer/$relative";
        -f $path && !-l $path && abs_path($path) eq $path or die "missing or aliased manifest: $path\n";
        push @selected, $relative;
    }
    for my $relative (@selected) {
        my $manifest = "$consumer/$relative";
        chdir dirname($manifest) or die $!;
        my $heading = "==> $relative";
        $heading = "\e[36m$heading\e[0m" if -t STDOUT && !exists($ENV{NO_COLOR}) && ($ENV{TERM} // '') ne 'dumb';
        print "$heading\n";
        STDOUT->flush();
        push @attempted, $relative;
        my @command = ('cargo', 'update', '-v', '--manifest-path', $manifest);
        push @command, '--package', $package if defined $package;
        push @command, '--precise', $precise if defined $precise;
        system { $command[0] } @command;
        if ($? != 0) {
            my $status = $? == -1 ? 2 : ($? & 127) ? 128 + ($? & 127) : $? >> 8;
            warn 'Update attempts (locks may have changed; no rollback): ' . join(', ', @attempted) . "\n";
            exit $status;
        }
    }
};
if ($@) { warn $@; exit 2; }
