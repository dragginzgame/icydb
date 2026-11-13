#!/usr/bin/env perl
# Shared companions: scripts/ci/shared-package-version.pl
use strict;
use warnings;
use Digest::SHA qw(sha256_hex);
use JSON::PP;
use File::Path qw(make_path);
use File::Basename qw(dirname);
use FindBin;

sub regular {
    my ($path) = @_;
    -f $path && !-l $path or die "expected regular package file: $path\n";
    open my $f, '<:raw', $path or die "read $path: $!\n";
    local $/; my $bytes = <$f>;
    close $f or die "close $path: $!\n";
    return $bytes;
}
sub relative {
    my ($path) = @_;
    length($path) && $path !~ m{\A/|(?:\A|/)\.{1,2}(?:/|\z)|//|/\z|[\x00-\x1f\x7f]}
        && $path !~ m{(?:\A|/)\.git(?:/|\z)}
        or die "invalid package member: $path\n";
}
my $op = shift // '';
if ($op eq 'verify') {
    @ARGV == 2 && $ARGV[1] =~ /\A[0-9a-f]{64}\z/ or die "expected archive and SHA-256\n";
    sha256_hex(regular($ARGV[0])) eq $ARGV[1] or die "registry package checksum mismatch\n";
} elsif ($op eq 'extract') {
    @ARGV == 4 or die "usage: registry-package.pl extract ARCHIVE CHECKSUM VERSION NEW-DIRECTORY\n";
    my ($archive, $checksum, $version, $out) = @ARGV;
    $checksum =~ /\A[0-9a-f]{64}\z/ && $version =~ /\A(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\z/
        or die "invalid registry package identity\n";
    !-e $out && !-l $out or die "package output already exists\n";
    my $bytes = regular($archive);
    length($bytes) <= 10485760 && sha256_hex($bytes) eq $checksum
        or die "registry package checksum or size mismatch\n";
    require Archive::Tar;
    my $tar = Archive::Tar->new;
    # Read the exact authenticated bytes, never a second observation of the archive.
    require IO::Uncompress::Gunzip;
    my $plain = '';
    my $gzip = IO::Uncompress::Gunzip->new(\$bytes) or die "invalid crate gzip\n";
    while (1) {
        my $count = $gzip->read(my $chunk, 65536);
        defined($count) && $count >= 0 or die "invalid crate gzip stream\n";
        last unless $count;
        length($plain) + $count <= 67108864 or die "tooling package exceeds extraction budget\n";
        $plain .= $chunk;
    }
    open my $stream, '<:raw', \$plain or die "open package stream: $!\n";
    defined($tar->read($stream)) && !$tar->error or die "invalid crate archive\n";
    my (%files, %entries); my $prefix = "toko-tooling-$version/";
    for my $entry ($tar->get_files) {
        my $path = $entry->full_path;
        $path =~ s{/\z}{} if $entry->is_dir;
        relative($path);
        $path eq "toko-tooling-$version" && $entry->is_dir and next;
        index($path, $prefix) == 0 or die "archive member outside package: $path\n";
        $path = substr($path, length($prefix)); relative($path);
        $path ne '.toko-tooling.files.json' or die "reserved package member\n";
        !$entries{$path}++ or die "duplicate package member: $path\n";
        $entry->is_dir and next;
        $entry->is_file && !$entry->is_symlink && !$entry->is_hardlink
            or die "nonregular package member: $path\n";
        $files{$path} = {bytes => $entry->get_content,
            mode => ($entry->mode & 0111) ? '100755' : '100644'};
    }
    for my $path (keys %files) {
        my $parent = dirname($path);
        while ($parent ne '.') {
            !exists($files{$parent}) or die "file/directory package collision: $path\n";
            $parent = dirname($parent);
        }
    }
    exists($files{'Cargo.toml'}) && exists($files{'scripts/distribution/refresh-consumer.sh'})
        or die "selected crate does not contain the tooling payload (requires toko-tooling 0.33 or newer)\n";
    make_path($out);
    for my $path (sort keys %files) {
        make_path(dirname("$out/$path"));
        open my $f, '>:raw', "$out/$path" or die "write package member: $!\n";
        print {$f} $files{$path}{bytes} or die "write package bytes: $!\n";
        close $f or die "close package member: $!\n";
        chmod($files{$path}{mode} eq '100755' ? 0755 : 0644, "$out/$path") or die "chmod: $!\n";
        $files{$path}{checksum} = sha256_hex(delete $files{$path}{bytes});
    }
    open my $reader, '-|', $^X, "$FindBin::Bin/../ci/shared-package-version.pl", "$out/Cargo.toml"
        or die "read package version: $!\n";
    my $observed = do { local $/; <$reader> };
    close $reader && $observed eq "$version\n" or die "registry package version mismatch\n";
    open my $name_reader, '-|', 'yq', '-p', 'toml', '-o', 'json', '.package.name', "$out/Cargo.toml"
        or die "read package name: $!\n";
    my $name = do { local $/; <$name_reader> };
    close $name_reader && JSON::PP->new->decode($name) eq 'toko-tooling' or die "registry package name mismatch\n";
    open my $index, '>', "$out/.toko-tooling.files.json" or die "write package inventory: $!\n";
    print {$index} JSON::PP->new->canonical->encode({checksum=>$checksum, version=>$version, files=>\%files});
    close $index or die "close package inventory: $!\n";
} elsif ($op eq 'read' || $op eq 'mode') {
    @ARGV == 2 or die "expected package root and relative member\n";
    my ($root, $path) = @ARGV; relative($path);
    my $index = JSON::PP->new->decode(regular("$root/.toko-tooling.files.json"));
    my $entry = $index->{files}{$path} // die "package member is missing: $path\n";
    my $parent = dirname($path);
    while ($parent ne '.') {
        !-l "$root/$parent" or die "package member traverses a symlink\n";
        $parent = dirname($parent);
    }
    my $bytes = regular("$root/$path");
    sha256_hex($bytes) eq $entry->{checksum} or die "package member was modified: $path\n";
    my $mode = (-x "$root/$path") ? '100755' : '100644';
    $mode eq $entry->{mode} or die "package member mode changed: $path\n";
    print $op eq 'read' ? $bytes : "$mode\n";
} else { die "select extract, verify, read or mode\n"; }
