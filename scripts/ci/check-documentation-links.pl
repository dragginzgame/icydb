#!/usr/bin/env perl
use strict;
use warnings;
use File::Basename qw(dirname);
use File::Spec;

# A local-path check, not a Markdown renderer or an anchor/network checker.
@ARGV >= 3 && shift(@ARGV) eq '--root'
    or die "usage: check-documentation-links.pl --root ROOT DOCUMENT [...]\n";
my $root = shift @ARGV;
chdir $root or die "cannot enter $root: $!\n";
my ($failures, $references) = (0, 0);

sub check_target {
    my ($document, $target) = @_;
    return if $target =~ m{^(?:[a-z][a-z0-9+.-]*:|\#|//)}i;
    $target =~ s/[#?].*\z//;
    $target =~ s/%([0-9a-f]{2})/chr(hex($1))/egi;
    return unless length $target;
    # Absolute links keep filesystem semantics, as in the IcyDB source checker.
    my $path = File::Spec->file_name_is_absolute($target)
        ? $target : File::Spec->catfile(dirname($document), $target);
    ++$references;
    unless (-e $path) {
        warn "$document: missing local target $target\n";
        ++$failures;
    }
}

for my $document (@ARGV) {
    -f $document or die "not a document file: $document\n";
    open my $file, '<', $document or die "cannot read $document: $!\n";
    my ($text, $fence, $length) = ('', '', 0);
    local $! = 0;
    while (my $line = <$file>) {
        if (length $fence) {
            if ($line =~ /^ {0,3}(\Q$fence\E{$length,})[ \t]*\r?\n?\z/) {
                $fence = '';
            }
            next;
        }
        if ($line =~ /^ {0,3}(`{3,}|~{3,})/) {
            $fence = substr($1, 0, 1);
            $length = length $1;
            next;
        }
        $text .= $line;
    }
    die "cannot read $document: $!\n" if $!;
    close $file or die "cannot close $document: $!\n";
    # Omit inline code examples too. Full CommonMark parsing is out of scope.
    $text =~ s/(`+)(?!`)(.*?)\1(?!`)/ /sg;
    my $title = qr/(?:"[^"\n]*"|'[^'\n]*'|\([^()\n]*\))/;
    while ($text =~ /\]\(\s*(?:<([^>\n]+)>|([^\s)]+))(?:\s+$title)?\s*\)/g) {
        check_target($document, defined($1) ? $1 : $2);
    }
    while ($text =~ /^ {0,3}\[[^\]\n]+\]:[ \t]*(?:<([^>\n]+)>|([^\s]+))/mg) {
        check_target($document, defined($1) ? $1 : $2);
    }
}
die "documentation references failed ($failures)\n" if $failures;
print "[OK] $references local references across ", scalar(@ARGV), " documents.\n";
