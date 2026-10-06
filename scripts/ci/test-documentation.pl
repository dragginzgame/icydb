#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use File::Copy qw(copy);
use File::Path qw(make_path);
use File::Basename qw(dirname);
use File::Temp qw(tempdir);
use Test::More;

# Qualify consumer projection; generic Markdown grammar belongs upstream.
my $directory = tempdir('icydb-documentation-XXXXXX', TMPDIR => 1, CLEANUP => 1);
make_path("$directory/scripts/ci");
for my $script ('check-documentation.pl', 'check-documentation-links.pl') {
    copy("$FindBin::Bin/$script", "$directory/scripts/ci/$script") or die "copy $script: $!";
}
my $checker = "$directory/scripts/ci/check-documentation.pl";

sub fixture {
    my ($name, $contents) = @_;
    my $path = "$directory/$name";
    make_path(dirname($path));
    open my $file, '>', $path or die "cannot create $path: $!";
    print {$file} $contents;
    close $file or die "cannot close $path: $!";
    return $path;
}

sub check_document {
    # List-form execution keeps caller-selected paths out of the shell.
    open my $result, '-|', $^X, $checker, @_ or die "cannot run checker: $!";
    local $/;
    my $output = <$result>;
    close $result;
    return ($? >> 8, $output);
}

fixture('selected target.md', "# Target\n");
my $selected = fixture('selected document.md', '[Selected](<selected target.md>)');
my ($status) = check_document($selected);
is($status, 0, 'explicit document selection reaches the shared checker');
fixture('selected document.md', '[Selected](missing.md)');
($status) = check_document($selected);
isnt($status, 0, 'shared failure stops the consumer adapter');

for my $document (
    'README.md', 'INSTALLING.md', 'SECURITY.md', 'docs/governance/documentation.md',
    'docs/governance/shared-tooling.md', 'docs/audits/README.md',
    'docs/audits/recurring/crosscutting/crosscutting-flow-convergence-and-duplication.md',
    'docs/audits/recurring/crosscutting/crosscutting-complexity-and-technical-debt.md',
    'docs/audits/targeted/modules/module-surface-hardening.md',
    'docs/audits/targeted/modules/module-cleanup-runner.md',
    'docs/audits/archive/shared-adoption/README.md',
) { fixture($document, "# Fixture\n"); }
fixture('Cargo.toml', "[workspace.package]\nversion = \"0.1.1\"\n");
fixture('README.md', "tag = \"v0.1.1\"\n");
fixture('crates/owner.rs', "// Source owner\n");
fixture('docs/contracts/PERSISTED_FORMAT_INVENTORY.md', '`crates/owner.rs`');
fixture('shared-guide.md', '[Owner](crates/owner.rs)');
fixture('.shared-tooling.snapshot', "file\tunused\t-\tshared-guide.md\n");
($status) = check_document();
is($status, 0, 'default roster admits current version and persisted source owner');
fixture('shared-guide.md', '[Owner](missing.md)');
($status) = check_document();
isnt($status, 0, 'snapshot Markdown is part of the default roster');
fixture('shared-guide.md', "# Guide\n");
fixture('README.md', "tag = \"v0.1.0\"\n");
($status) = check_document();
isnt($status, 0, 'product dependency example must match the workspace version');
fixture('README.md', "tag = \"v0.1.1\"\n");
fixture('docs/contracts/PERSISTED_FORMAT_INVENTORY.md', '`crates/missing.rs`');
($status) = check_document();
isnt($status, 0, 'product persisted inventory requires an existing source owner');
done_testing();
