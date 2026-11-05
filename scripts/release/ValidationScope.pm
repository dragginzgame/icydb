package ValidationScope;
use strict;
use warnings;
use Exporter 'import';
our @EXPORT_OK = qw(cli_private);
sub cli_private {
    my ($group,$changes)=@_;
    return 0 unless $group eq 'cli' && @$changes;
    for my $path (@$changes) {
        next if $path =~ m{\Acrates/icydb-cli/(?:src|tests)/[^\x00-\x1f]+\.rs\z}
            && $path ne 'crates/icydb-cli/src/lib.rs';
        next if $path eq 'releases/cli/CHANGELOG.md' || $path =~ m{\Areleases/cli/\d+\.\d+\.md\z};
        return 0;
    }
    return 1;
}
1;
