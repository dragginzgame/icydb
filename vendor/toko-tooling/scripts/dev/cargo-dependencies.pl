#!/usr/bin/env perl
use strict;
use warnings;
use Cwd qw(abs_path getcwd);
use File::Basename qw(dirname);
use IO::Handle;
use JSON::PP qw(decode_json);

sub usage {
    return "Usage: cargo-dependencies.pl update|list [--consumer DIR] [--manifest RELATIVE/Cargo.toml] [--package NAME] [--precise VERSION]\n"
        . "list: Local package versions; publication status is not checked. '-' means no release group.\n"
        . "update: Runs cargo update -v; --package and --precise apply only to update.\n";
}
my @attempted;
eval {
    my $mode = shift(@ARGV) // '';
    if ($mode eq '--help' || (@ARGV == 1 && $ARGV[0] eq '--help')) { print usage(); exit 0; }
    $mode eq 'update' || $mode eq 'list' or die usage();
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
    $mode ne 'list' || (!defined($package) && !defined($precise))
        or die "list does not accept update selectors\n";
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
    if ($mode eq 'list') {
        my @rows;
        for my $relative (@selected) {
            chdir dirname("$consumer/$relative") or die $!;
            open my $cargo, '-|', 'cargo', 'metadata', '--no-deps', '--format-version', '1',
                '--offline', '--locked', '--manifest-path', "$consumer/$relative" or die $!;
            my $json = do { local $/; <$cargo> };
            close $cargo or die "Cargo metadata failed for $relative\n";
            my $metadata = decode_json($json);
            my %members = map { $_ => 1 } @{$metadata->{workspace_members}};
            my @packages = grep { $members{$_->{id}} } @{$metadata->{packages}};
            my %names = map { $_->{name} => 1 } @packages;
            my %groups;
            my $policy = dirname("$consumer/$relative") . '/ci/cargo-release-groups.json';
            if (-e $policy) {
                open my $fh, '<', $policy or die $!;
                my $data = decode_json(do { local $/; <$fh> }); close $fh;
                ref($data->{groups}) eq 'ARRAY' or die "invalid group policy: $policy\n";
                for my $group (@{$data->{groups}}) {
                    defined($group->{name}) && ref($group->{members}) eq 'ARRAY'
                        or die "invalid group in $policy\n";
                    for my $name (@{$group->{members}}) {
                        $names{$name} && !exists($groups{$name})
                            or die "unknown or multiply grouped member: $name\n";
                        $groups{$name} = $group->{name};
                    }
                }
            }
            for my $p (sort { ($groups{$a->{name}} // '') cmp ($groups{$b->{name}} // '')
                    || $a->{name} cmp $b->{name} } @packages) {
                my $workspace = $relative eq 'Cargo.toml' ? '(root)' : $relative =~ s{Cargo\.toml\z}{}r;
                push @rows, [$p->{name}, $groups{$p->{name}} // '-', $workspace, $p->{version}];
            }
        }
        my @width = map { length } qw(PACKAGE GROUP WORKSPACE VERSION);
        for my $row (@rows) {
            for my $i (0..3) {
                $row->[$i] !~ /[\x00-\x1f\x7f]/ or die "invalid display value\n";
                $width[$i] = length($row->[$i]) if length($row->[$i]) > $width[$i];
            }
        }
        my $format = join('  ', map { "%-${_}s" } @width) . "\n";
        printf $format, qw(PACKAGE GROUP WORKSPACE VERSION);
        printf $format, map { '-' x $_ } @width;
        printf $format, @$_ for @rows;
        exit 0;
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
