#!/usr/bin/env perl
# Bounded feedback only. Release admission remains owned by scripts/release.
use strict;
use warnings;
use JSON::PP;
my $json = JSON::PP->new->canonical->utf8;
@ARGV == 2 && $ARGV[0] =~ /\Adatabase-v\d+\.\d+\.\d+\z/
    && $ARGV[1] =~ /\A(?:auto|workspace)\z/
    or die "usage: database-validation-plan.pl database-vVERSION auto|workspace\n";
my ($baseline, $scope) = @ARGV;
sub capture {
    open my $fh, '-|', @_ or die "start observation: $!\n";
    binmode $fh;
    my $raw = '';
    while (1) {
        my $n = read($fh, my $chunk, 65536);
        defined $n or die "read observation\n";
        last unless $n;
        $raw .= $chunk;
        length($raw) <= 4194304 or die "observation exceeds 4 MiB\n";
    }
    close $fh or die "observation failed\n";
    return $raw;
}
my $source = capture('git', 'rev-parse', 'HEAD');
chomp $source;
my $plan = {schema=>1, source=>$source, baseline_tag=>$baseline,
    scope=>'workspace', target=>'validate', changed=>[], subsystems=>[],
    release_omission_admitted=>JSON::PP::false};
sub finish {
    $plan->{reason} = shift;
    print $json->encode($plan), "\n";
    exit 0;
}
finish('Explicit workspace selection.') if $scope eq 'workspace';
finish('Uncommitted or untracked inputs require full validation.')
    if capture('git', 'status', '--porcelain', '--untracked-files=all') ne '';
my $observed = $json->decode(capture('perl', 'vendor/toko-tooling/scripts/ci/validation-evidence.pl',
    'baseline', '.', $source, "refs/tags/$baseline"));
finish('Missing, lightweight or non-ancestor database baseline requires full validation.')
    unless $observed->{available};
$plan->{baseline} = $observed->{baseline};
$plan->{baseline_object} = $observed->{object};
$plan->{changed} = $observed->{changes};
my %selected;
for my $path (@{$plan->{changed}}) {
    if ($path =~ m{\Acrates/icydb-core/src/db/sql/parser/[A-Za-z0-9_/]+\.rs\z}) {
        $selected{parser} = 1;
    } elsif ($path =~ m{\Acrates/icydb-core/src/db/query/plan/[A-Za-z0-9_/]+\.rs\z}) {
        $selected{planner} = 1;
    } elsif ($path eq 'releases/database/CHANGELOG.md' || $path =~ m{\Areleases/database/\d+\.\d+\.md\z}) {
        next;
    } else {
        $plan->{fallback_path} = $path;
        finish('Shared, unknown or cross-subsystem input requires full validation.');
    }
}
finish('No bounded database source changes; full validation required.') unless %selected;
$plan->{subsystems} = [sort keys %selected];
$plan->{scope} = 'subsystem';
$plan->{target} = keys(%selected) == 2 ? 'ci-database-query' : 'ci-database-'.(sort keys %selected)[0];
finish('Bounded feedback with mandatory lowering, session pagination/order, SQLite and mutation references, and public SQL facade acceptance. Not qualified for release omissions.');
