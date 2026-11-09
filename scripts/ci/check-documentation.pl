#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib $FindBin::Bin;
use StableVersion qw(stable version_cmp);

# Validate references, not sentences. Current behavior is exercised by the
# compiled example and codec tests linked from the maintained contracts.
chdir "$FindBin::Bin/../.." or die "cannot enter repository: $!\n";
my @documents = @ARGV ? @ARGV : (
    'CHANGELOG.md', 'releases/history.md', glob('releases/*/*.md'),
    'README.md', 'INSTALLING.md', 'SECURITY.md', 'docs/governance/documentation.md',
    glob('docs/*.md'), glob('docs/contracts/*.md'),
    glob('docs/guides/*.md'), glob('docs/operations/*.md'),
    glob('crates/*/README.md'),
    'docs/governance/toko-tooling.md', 'docs/audits/README.md',
    'docs/audits/recurring/crosscutting/crosscutting-flow-convergence-and-duplication.md',
    'docs/audits/recurring/crosscutting/crosscutting-complexity-and-technical-debt.md',
    'docs/audits/targeted/modules/module-surface-hardening.md',
    'docs/audits/targeted/modules/module-cleanup-runner.md',
    'docs/audits/archive/shared-adoption/README.md',
);
my $failures = 0;

sub read_file {
    my ($path) = @_;
    open my $file, '<', $path or die "cannot read $path: $!\n";
    local $/;
    return <$file>;
}

unless (@ARGV) {
    # The snapshot owns its Markdown file set; keep navigation qualification
    # aligned with it instead of maintaining a second shared-document roster.
    my $snapshot = read_file('.toko-tooling.snapshot');
    push @documents, ($snapshot =~ /^file\t[^\t]+\t[^\t]+\t([^\n]+\.md)$/mg);
}
my %seen;
@documents = grep { !$seen{$_}++ } @documents;

# Shared Tooling owns Markdown navigation mechanics. IcyDB owns this roster
# and the structured product facts checked below.
system($^X, "$FindBin::Bin/check-documentation-links.pl", '--root', '.', @documents) == 0
    or die "shared documentation link check failed\n";

for my $document (@documents) {
    my $text = read_file($document);
    # The durable-surface inventory's source-owner cells use repository paths.
    # Check those references without prescribing table labels or prose.
    if ($document eq 'docs/contracts/PERSISTED_FORMAT_INVENTORY.md') {
        while ($text =~ /`((?:crates|testing)\/[^`]+)`/g) {
            unless (-e $1) {
                warn "$document: missing source owner $1\n";
                ++$failures;
            }
        }
    }
}

# Released examples agree on a supported tag independently of metadata
# preparation. The pending hard cut instead documents the exact development
# pair: runtime without generation and the same source with explicit build.
unless (@ARGV) {
    my $manifest = read_file('crates/icydb/Cargo.toml');
    $manifest =~ /\[package\]([^\[]+)/s or die "missing database package\n";
    my $package = $1;
    $package =~ /^version\s*=\s*"([^"]+)"/m or die "missing database version\n";
    my $version = $1;
    stable($version) or die "invalid database version\n";
    my $readme = read_file('README.md');
    my @declarations = ($readme =~ /^icydb\s*=\s*(\{[^}\n]*\})\s*$/mg);
    my @tags = ($readme =~ /^icydb\s*=\s*\{[^}\n]*\btag\s*=\s*"([^"]*)"/mg);
    my $development = @declarations == 2
        && $declarations[0] =~ /\A\{\s*git\s*=\s*"https:\/\/github\.com\/dragginzgame\/icydb\.git"\s*\}\z/
        && $declarations[1] =~ /\A\{\s*git\s*=\s*"https:\/\/github\.com\/dragginzgame\/icydb\.git"\s*,\s*features\s*=\s*\[\s*"build"\s*\]\s*\}\z/;
    if (@declarations != 2 || (@tags != 2 && !$development)) {
        warn "README.md: expected matching released or explicit-build development dependencies\n";
        ++$failures;
    }
    for my $tag (@tags) {
        if ($tag !~ /^(?:database-)?v(.+)$/ || !stable($1)) {
            warn "README.md: invalid release tag $tag\n";
            ++$failures;
        } elsif (version_cmp($1, $version) > 0) {
            warn "README.md: dependency tag $tag is newer than workspace $version\n";
            ++$failures;
        }
    }
    if (@tags == 2 && $tags[0] ne $tags[1]) {
        warn "README.md: runtime and build dependency tags differ\n";
        ++$failures;
    }
}

die "documentation references failed ($failures)\n" if $failures;
print "[OK] IcyDB documentation inventory and version facts verified.\n";
