#!/usr/bin/env perl
use strict;
use warnings;

# Package versions stand for managed native files. Application data, executables
# and private application libraries do not form the standard build environment.
my ($format, $info) = @ARGV;
defined($format) && (($format eq 'dpkg' && @ARGV == 2) || ($format eq 'rpm' && @ARGV == 1))
    or die "usage: native-package-inputs.pl dpkg INFO-DIRECTORY | rpm\n";
sub relevant {
    my ($path) = @_;
    return $path =~ m{\A/usr/include/}
        || $path =~ m{\A/(?:usr/)?lib(?:32|64)?/(?:[^/]+-linux-[^/]+/)?[^/]+\.(?:so(?:\.[^/]+)*|a|o)\z}
        || $path =~ m{\A/usr/(?:lib(?:64)?/(?:[^/]+-linux-[^/]+/)?|share/)pkgconfig/}
        || $path =~ m{\A/usr/(?:lib|libexec)/(?:gcc|clang|llvm-[^/]+)/}
        || $path =~ m{\A/usr/share/(?:cmake(?:-[^/]+)?|aclocal|automake[^/]*|autoconf|libtool)/}
        || $path =~ m{\A/(?:usr/)?bin/(?:cc|c\+\+|gcc|g\+\+|clang(?:\+\+)?|ld(?:\.bfd|\.gold|\.lld)?|as|ar|ranlib|nm|strip|objcopy|objdump|pkg-config|pkgconf|make|cmake|ninja|autoconf|automake|libtool|bison|flex|nasm|yasm|protoc|env|sh|bash|perl|python3(?:\.[0-9]+)?)(?:-[0-9.]+)?\z};
}
my %selected;
my ($name,$version,$keep);
sub admit {
    defined($name) or return;
    $selected{$name} = $version if $keep;
}
while (my $line = <STDIN>) {
    chomp $line;
    if ($format eq 'dpkg') {
        my @fields = split /\t/, $line, -1;
        my ($status,$package,$release) = @fields;
        @fields==3 && $status =~ /\A[a-zA-Z][a-zA-Z][ R]\z/
            && $package =~ /\A[a-zA-Z0-9][a-zA-Z0-9.+:-]*\z/ && $release =~ /\A[^\s]+\z/
            or die "malformed native package record\n";
        # The second status byte is the installed state. Held packages (hi)
        # and packages selected for removal but still installed remain inputs.
        next unless substr($status,1,1) eq 'i';
        my $list = "$info/$package.list";
        # Some single-architecture packages omit the architecture in info paths.
        if (!-e $list) { (my $plain=$package) =~ s/:[^:]+\z//; $list="$info/$plain.list"; }
        -f $list && !-l $list or die "missing managed native file inventory: $list\n";
        open my $files, '<', $list or die "read $list: $!\n";
        while (my $path=<$files>) { chomp $path; $selected{$package}=$release if relevant($path); }
        close $files or die "close $list: $!\n";
    } elsif ($line =~ /\APACKAGE\t([^\t]+)\t([^\t]+)\z/) {
        admit(); ($name,$version,$keep)=($1,$2,0);
        $name =~ /\A[a-zA-Z0-9][a-zA-Z0-9.+_-]*\z/ && $version =~ /\A[^\s]+\z/
            or die "malformed RPM identity\n";
    } elsif ($line =~ m{\A/}) {
        defined($name) or die "RPM path without package\n";
        $keep=1 if relevant($line);
    } else { die "malformed RPM file inventory\n"; }
}
admit() if $format eq 'rpm';
keys(%selected) or die "empty managed native package inventory\n";
print "$_=$selected{$_}\n" for sort keys %selected;
