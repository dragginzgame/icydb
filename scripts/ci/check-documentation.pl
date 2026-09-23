#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use File::Basename qw(dirname);
use File::Spec;

# Validate references, not sentences. Current behavior is exercised by the
# compiled example and codec tests linked from the maintained contracts.
chdir "$FindBin::Bin/../.." or die "cannot enter repository: $!\n";
my @documents = @ARGV ? @ARGV : (
    'README.md', 'INSTALLING.md', 'SECURITY.md', 'docs/governance/documentation.md',
    glob('docs/*.md'), glob('docs/contracts/*.md'),
    glob('docs/guides/*.md'), glob('docs/operations/*.md'),
    glob('crates/*/README.md'),
);
my ($failures, $references) = (0, 0);

sub read_file {
    my ($path) = @_;
    open my $file, '<', $path or die "cannot read $path: $!\n";
    local $/;
    return <$file>;
}

sub check_target {
    my ($document, $target, $from_root) = @_;
    return if $target =~ m{^(?:[a-z][a-z0-9+.-]*:|\#|//)}i;
    $target =~ s/[#?].*\z//;
    $target =~ s/%([0-9a-f]{2})/chr(hex($1))/egi;
    return unless length $target;
    my $path = ($from_root || File::Spec->file_name_is_absolute($target))
        ? $target : File::Spec->catfile(dirname($document), $target);
    ++$references;
    unless (-e $path) {
        warn "$document: missing local target $target\n";
        ++$failures;
    }
}

for my $document (@documents) {
    my $text = read_file($document);
    # Fenced examples are code, not document navigation. Anchor validation and
    # external network availability are deliberately outside this local gate.
    $text =~ s/^(`{3,}|~{3,})[^\n]*\n.*?^\1\s*$//msg;
    while ($text =~ /\]\(\s*(?:<([^>]+)>|([^\s)]+))(?:\s+"[^"]*")?\s*\)/g) {
        check_target($document, defined($1) ? $1 : $2, 0);
    }
    while ($text =~ /^\s*\[[^\]]+\]:\s*<?([^\s>]+)>?/mg) {
        check_target($document, $1, 0);
    }
    # The durable-surface inventory's source-owner cells use repository paths.
    # Check those references without prescribing table labels or prose.
    if ($document eq 'docs/contracts/PERSISTED_FORMAT_INVENTORY.md') {
        while ($text =~ /`((?:crates|testing)\/[^`]+)`/g) {
            check_target($document, $1, 1);
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
print "[OK] $references local references across ", scalar(@documents), " documents.\n";
