#!/usr/bin/env perl
use strict;
use warnings;
use Fcntl qw(F_GETFD F_GETFL O_ACCMODE O_RDONLY O_WRONLY);

# Test-only observation: never consume or return a jobserver token.
@ARGV == 0 or die "usage: check-pipe-jobserver.pl\n";
($ENV{MAKEFLAGS} // '') =~ /(?:^|\s)--jobserver-(?:auth|fds)=(\d+),(\d+)(?:\s|$)/
    or die "missing pipe jobserver identity\n";
my @fds = ($1, $2);
$fds[0] != $fds[1] or die "jobserver descriptors must differ\n";
for my $index (0, 1) {
    my $fd = $fds[$index];
    open my $handle, ($index ? '>&=' : '<&='), $fd
        or die "closed jobserver descriptor $fd: $!\n";
    defined fcntl($handle, F_GETFD, 0) or die "invalid jobserver descriptor $fd\n";
    -p $handle or die "jobserver descriptor $fd is not a pipe\n";
    my $flags = fcntl($handle, F_GETFL, 0);
    defined($flags) && ($flags & O_ACCMODE) == ($index ? O_WRONLY : O_RDONLY)
        or die "wrong jobserver descriptor direction: $fd\n";
}
