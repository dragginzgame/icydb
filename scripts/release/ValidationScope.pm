package ValidationScope;
use strict;
use warnings;
use Exporter 'import';
our @EXPORT_OK = qw(cli_private database_product exclusions);
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
# All product lint, feature and workspace tests remain required. Only fixtures
# for unchanged automation are omitted; unknown inputs cannot acquire this scope.
sub database_product {
    my ($group,$changes,$metadata_equal,$existing)=@_;
    return 0 unless $group eq 'database' && @$changes;
    my $product=0;
    for my $path (@$changes) {
        if ($path =~ m{\Acrates/icydb(?:-core|-schema|-model|-model-macros|-diagnostic-code)?/(?:src|tests)/[A-Za-z0-9_/-]+\.rs\z}) {
            return 0 if $existing && !$existing->($path);
            $product=1; next;
        }
        if (cli_private('cli',[$path])) {
            return 0 if $path =~ /\.rs\z/ && $existing && !$existing->($path);
            next;
        }
        next if $path eq 'releases/database/CHANGELOG.md' || $path =~ m{\Areleases/database/\d+\.\d+\.md\z};
        next if $metadata_equal && $metadata_equal->($path);
        return 0;
    }
    return $product;
}
sub exclusions {
    my ($group)=@_;
    return $group eq 'cli' ? [qw(clippy check check-feature-matrix test check-automation)]
        : $group eq 'database' ? [qw(clippy-cli check-cli test-cli package-cli check-automation)]
        : die "unknown validation group\n";
}
1;
