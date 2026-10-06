#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;

# Validate references, not sentences. Current behavior is exercised by the
# compiled example and codec tests linked from the maintained contracts.
chdir "$FindBin::Bin/../.." or die "cannot enter repository: $!\n";
my @documents = @ARGV ? @ARGV : (
    'README.md', 'INSTALLING.md', 'SECURITY.md', 'docs/governance/documentation.md',
    glob('docs/*.md'), glob('docs/contracts/*.md'),
    glob('docs/guides/*.md'), glob('docs/operations/*.md'),
    glob('crates/*/README.md'),
    'docs/governance/shared-tooling.md', 'docs/audits/README.md',
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
    my $snapshot = read_file('.shared-tooling.snapshot');
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

# Version numbers are release data, unlike narrative wording. Keep the only
# dependency example pinned to the workspace; other guides link to README.
unless (@ARGV) {
    my $manifest = read_file('Cargo.toml');
    $manifest =~ /\[workspace\.package\]([^\[]+)/s or die "missing workspace package\n";
    my $package = $1;
    $package =~ /^version\s*=\s*"([^"]+)"/m or die "missing workspace version\n";
    my $version = $1;
    my $readme = read_file('README.md');
    while ($readme =~ /tag\s*=\s*"v([^"]+)"/g) {
        if ($1 ne $version) {
            warn "README.md: dependency tag v$1 differs from workspace $version\n";
            ++$failures;
        }
    }
}

die "documentation references failed ($failures)\n" if $failures;
print "[OK] IcyDB documentation inventory and version facts verified.\n";
