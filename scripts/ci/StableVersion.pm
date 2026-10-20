package StableVersion;
use strict;
use warnings;
use Exporter 'import';
our @EXPORT_OK = qw(stable version_cmp);

sub stable {
    defined $_[0] && !ref($_[0]) &&
        $_[0] =~ /\A(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\z/;
}

# Compare decimal components exactly, without native integer/float limits.
sub version_cmp {
    my ($candidate, $baseline) = @_;
    stable($candidate) && stable($baseline) or die "expected canonical stable version\n";
    my @candidate = split /\./, $candidate;
    my @baseline = split /\./, $baseline;
    for my $index (0 .. 2) {
        next if $candidate[$index] eq $baseline[$index];
        my $order = length($candidate[$index]) <=> length($baseline[$index]);
        $order ||= $candidate[$index] cmp $baseline[$index];
        return $order < 0 ? -1 : 1;
    }
    return 0;
}
1;
