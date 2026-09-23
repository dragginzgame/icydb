#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use File::Spec;
use File::Temp qw(tempdir);
use Test::More;

my $directory = tempdir('icydb-documentation-XXXXXX', TMPDIR => 1, CLEANUP => 1);
my $checker = "$FindBin::Bin/check-documentation.pl";

sub fixture {
    my ($name, $contents) = @_;
    my $path = File::Spec->catfile($directory, $name);
    open my $file, '>', $path or die "cannot create $path: $!";
    print {$file} $contents;
    close $file or die "cannot close $path: $!";
    return $path;
}

sub check_document {
    my ($path) = @_;
    # List-form execution preserves spaces and keeps Markdown out of the shell.
    open my $result, '-|', $^X, $checker, $path or die "cannot run checker: $!";
    local $/;
    my $output = <$result>;
    close $result;
    return ($? >> 8, $output);
}

my $target = fixture('target file.md', "# Destination\n");
my $valid = fixture('valid.md', <<"MARKDOWN");
# Any heading

[Relative](target%20file.md#section)
[Absolute](<$target>)
[Reference][destination]

[destination]: <target%20file.md> "A title"
[Remote](https://example.invalid/document)
[Fragment](#local-heading)

~~~markdown
[Example only](missing-example.md)
~~~
MARKDOWN
my ($status, $output) = check_document($valid);
is($status, 0, 'local targets, encoded spaces, references and fences are handled');
like($output, qr/3 local references/, 'only navigational local targets are checked');

my $rewritten = fixture('rewritten.md', "# Different prose\n\n[Another label](<$target>)\n");
($status, $output) = check_document($rewritten);
is($status, 0, 'wording and labels do not determine navigation validity');

my $broken = fixture('broken.md', '[Missing](missing-target.md)');
($status, $output) = check_document($broken);
isnt($status, 0, 'missing local target rejects');

done_testing();
