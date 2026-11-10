#!/usr/bin/env perl
# Shared companions: LICENSE.md
use strict;
use warnings;
use FindBin;
use Getopt::Long qw(GetOptions);

my ($file, $archive, $member);
GetOptions('file=s' => \$file, 'archive=s' => \$archive, 'member=s' => \$member)
    && !@ARGV && defined($file) && (defined($archive) == defined($member))
    or die "usage: check-license.pl --file LICENSE.md [--archive PACKAGE.crate --member PACKAGE-VERSION/LICENSE.md]\n";

sub read_file {
    my ($path) = @_;
    -f $path && !-l $path or die "not a regular non-symlink file: $path\n";
    open my $fh, '<', $path or die "cannot read $path: $!\n";
    binmode $fh;
    local $/;
    my $text = <$fh>;
    close $fh or die "cannot close $path: $!\n";
    return $text;
}

my $canonical_path = "$FindBin::Bin/../../LICENSE.md";
my $expected = read_file($canonical_path);
$expected =~ /\A# TOKO STUDIOS NETWORK LICENCE\n## Version 1\.0\n/
    or die "canonical licence has an unexpected title or version\n";
$expected =~ /\*\*Licence identifier:\*\* TSNL-1\.0\n/
    or die "canonical licence has an unexpected identifier\n";
$expected =~ /governed by the laws of the Principality of Monaco/
    or die "canonical licence lacks the selected governing law\n";
$expected =~ /competent courts of the Principality of Monaco have exclusive jurisdiction/
    or die "canonical licence lacks the selected forum\n";
$expected =~ /\*\*Adopted by Toko Studios Limited on 10 November 2026\.\*\*/
    or die "canonical licence lacks its adoption record\n";
$expected !~ /\{\{|\}\}|Draft for Legal Review|not approved for publication or adoption/
    or die "canonical licence retains draft text or a placeholder\n";

my $actual = read_file($file);
$actual eq $expected
    or die "licence bytes differ from canonical TSNL-1.0: $file\n";

if (defined $archive) {
    -f $archive && !-l $archive or die "archive must be a regular non-symlink file\n";
    length($member) && $member =~ m{/}
        && $member !~ m{\A/|(?:\A|/)\.{1,2}(?:/|\z)|//|/\z|[\r\n\0]}
        or die "select a canonical package-relative licence member\n";
    require File::Spec;
    my $input = File::Spec->rel2abs($archive);
    system('gzip', '-t', '--', $input) == 0 or die "archive gzip integrity failed\n";
    require Archive::Tar;
    my $tar = Archive::Tar->new;
    local $SIG{__WARN__} = sub { die "archive parsing failed: @_" };
    my $read = $tar->read($input, 1, {filter_cb => sub {
        my ($entry) = @_;
        return 0 if $entry->is_longlink || $entry->is_label || $entry->type =~ /\A[xg]\z/;
        my $name = $entry->full_path;
        $name =~ s{/$}{} if $entry->is_dir;
        length($name) && $name !~ m{\A/|(?:\A|/)\.{1,2}(?:/|\z)|//|/\z}
            or die "noncanonical archive member\n";
        $entry->is_file || $entry->is_dir or die "nonregular archive member: $name\n";
        return 0 unless $name eq $member;
        $entry->is_file && $entry->size == length($expected)
            or die "archive licence type or length differs: $member\n";
        return 1;
    }});
    defined($read) && $read == 1 && !$tar->error
        or die "expected exactly one readable licence member: $member\n";
    my @entries = $tar->get_files;
    @entries == 1 && $entries[0]->get_content eq $expected
        or die "archive licence bytes differ from $file\n";
    print "[OK] $archive:$member matches TSNL-1.0 byte-for-byte\n";
}
print "[OK] $file matches canonical TSNL-1.0\n";
