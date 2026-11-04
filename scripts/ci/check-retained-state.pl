#!/usr/bin/env perl
use strict;
use warnings;
use Getopt::Long qw(GetOptions);
use File::Spec;
use Fcntl qw(:mode);

# Read-only admission, never a garbage collector. Git objects/refs are not roots.
my (@roots, $max_bytes, $max_file_bytes, $max_age_days);
$max_bytes = 512 * 1024 * 1024;
$max_file_bytes = 32 * 1024 * 1024;
$max_age_days = 30;
GetOptions('root=s@' => \@roots, 'max-bytes=i' => \$max_bytes,
    'max-file-bytes=i' => \$max_file_bytes, 'max-age-days=i' => \$max_age_days)
    && !@ARGV && @roots && $max_bytes > 0 && $max_file_bytes > 0 && $max_age_days > 0
    or die "usage: check-retained-state.pl --root DIR [--root DIR ...] [--max-bytes N --max-file-bytes N --max-age-days N]\n";
my ($bytes, $files, $errors) = (0, 0, 0);
my (%seen, %roots_seen);
my $now = time;
sub refuse {
    my ($reason, $path) = @_;
    $path =~ s/([\x00-\x1f\x7f])/sprintf('\\x%02x', ord($1))/ge;
    warn "retained state refused: $reason: $path\n" if $errors++ < 20;
}
sub inspect {
    my ($path) = @_;
    my @info = lstat $path;
    if (!@info) { refuse("cannot stat ($!)", $path); return; }
    if (S_ISLNK($info[2])) { refuse('symlink requires owner review', $path); return; }
    return if $seen{"$info[0]:$info[1]"}++;
    if (S_ISDIR($info[2])) {
        my $dir;
        if (!opendir($dir, $path)) { refuse("cannot read directory ($!)", $path); return; }
        my @entries;
        $! = 0;
        while (defined(my $entry = readdir $dir)) { push @entries, $entry unless $entry eq '.' || $entry eq '..'; }
        refuse("directory traversal failed ($!)", $path) if $!;
        closedir $dir or refuse("cannot close directory ($!)", $path);
        for my $entry (sort @entries) {
            my $child = File::Spec->catfile($path, $entry);
            if ($entry =~ /\A(?:target|node_modules|\.tools|\.git)\z/ && -d $child) {
                refuse('build/cache/checkout tree belongs outside retained state', $child);
                next;
            }
            inspect($child);
        }
    } elsif (S_ISREG($info[2])) {
        $files++;
        $bytes += $info[7];
        refuse("file exceeds $max_file_bytes byte budget", $path) if $info[7] > $max_file_bytes;
        # Small journals/receipts remain usable across long recovery intervals.
        refuse("bulk evidence older than $max_age_days days", $path)
            if $info[7] >= 1024 * 1024 && $now - $info[9] > $max_age_days * 86400;
    } else { refuse('unsupported file type', $path); }
}
for my $root (@roots) {
    $root = File::Spec->rel2abs($root);
    next if $roots_seen{$root}++;
    # Missing state is normal before the first attempt; other failures are not.
    if (!lstat($root)) {
        if ($!{ENOENT}) { next; }
        refuse("cannot stat root ($!)", $root); next;
    }
    inspect($root);
}
refuse("aggregate $bytes exceeds $max_bytes byte budget", join(', ', @roots)) if $bytes > $max_bytes;
if ($errors) {
    print STDERR "Scanned retained state: $bytes logical bytes in $files files; budget $max_bytes bytes; $errors violation(s). Rejected subtrees were not scanned.\n";
    print STDERR "Preserved all state. Review active recovery and unique evidence; reclaim only authorized reproducible data, then retry. No Git pruning or automatic deletion was performed.\n";
}
exit($errors ? 1 : 0);
