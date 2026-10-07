#!/usr/bin/env perl
use strict;
use warnings;
use Cwd qw(abs_path);
use File::Spec;
use File::Path qw(make_path);
use File::Temp qw(tempdir tempfile);
use Getopt::Long qw(GetOptions);
use IPC::Open3;
use JSON::PP;

# This is an explicit maintainer operation, never part of release or cleanup.
# Git owns expected-value ref updates; consumers choose the cutoff and remote.
Getopt::Long::Configure(qw(no_auto_abbrev no_ignore_case));
my ($repo, $cutoff, $remote, $delete_local, $delete_remote, $yes, $help);
$repo = '.';
GetOptions('repo=s' => \$repo, 'cutoff=s' => \$cutoff, 'remote=s' => \$remote,
    'delete-local' => \$delete_local, 'delete-remote' => \$delete_remote,
    'yes' => \$yes, 'help|h' => \$help) or die "invalid options; use --help\n";
if ($help) {
    print "usage: delete-github-tags-up-to.pl [--repo ROOT] --cutoff [v]MAJOR.MINOR[.PATCH]\n",
        "       [--remote NAME] [--delete-local] [--delete-remote] [--yes]\n",
        "Default: read-only local inventory, plus remote inventory when --remote is supplied.\n",
        "A minor cutoff includes every patch in that line. Deletion requires --yes.\n";
    exit 0;
}
my $component = qr/(?:0|[1-9][0-9]*)/;
defined($cutoff) && $cutoff =~ /\Av?($component)\.($component)(?:\.($component))?\z/
    or die "an explicit canonical minor or exact cutoff is required\n";
my @limit = ($1, $2, $3);
$cutoff = join '.', grep { defined } @limit;
!@ARGV or die "unexpected positional arguments\n";
!defined($remote) || $remote =~ /\A[A-Za-z0-9_][A-Za-z0-9_.-]*\z/
    or die "remote must be a configured name\n";
!$delete_remote || defined($remote) or die "--delete-remote requires --remote\n";
my $mutating = $delete_local || $delete_remote;
!$mutating || $yes or die "deletion requires --yes\n";
!$yes || $mutating or die "--yes requires a deletion selection\n";
$repo = abs_path($repo) // die "repository does not exist\n";
delete @ENV{qw(GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR
    GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_NAMESPACE)};
$ENV{GIT_TERMINAL_PROMPT} = '0';
my ($log, $lock, $attempt);
$| = 1;
END {
    my $status = $?;
    close $log if $log;
    if ($lock) { unlink "$lock/owner"; rmdir $lock or warn "retained lock: $lock\n"; }
    warn "Tag maintenance evidence retained: $attempt\n" if $status && $attempt;
    $? = $status;
}
$SIG{INT} = $SIG{TERM} = sub { die "tag maintenance interrupted; reconcile saved intent on retry\n"; };
sub git {
    my ($input, @args) = @_;
    my $stderr = $log ? '>&' . fileno($log) : '>&STDERR';
    my $pid = open3(my $writer, my $reader, $stderr, 'git', '-C', $repo, @args);
    my $written = 1;
    { local $SIG{PIPE} = 'IGNORE'; $written = print {$writer} $input if length $input; }
    my $closed = close $writer;
    my $output = do { local $/; <$reader> } // '';
    close $reader;
    waitpid($pid, 0);
    my $status = $?;
    if ($log) {
        print {$log} "git $args[0]: status=$status\n$output" or die "write command evidence: $!\n";
        $log->flush or die "flush command evidence: $!\n";
    }
    $written && $closed && !$status or die "git $args[0] failed; no automatic retry\n";
    return $output;
}
sub line {
    my ($text) = @_;
    $text =~ s/\n\z//;
    length($text) && $text !~ /[\r\n\0]/ or die "expected one Git identity\n";
    return $text;
}
abs_path(line(git('', 'rev-parse', '--show-toplevel'))) eq $repo
    or die "--repo must select the checkout root\n";
sub numeric_compare { length($_[0]) <=> length($_[1]) || $_[0] cmp $_[1] }
sub parts {
    $_[0] =~ /\Av?($component)\.($component)\.($component)\z/ or return;
    return ($1, $2, $3);
}
sub selected {
    my @version = parts($_[0]);
    return 0 unless @version;
    for my $index (0 .. 2) {
        return 1 if !defined $limit[$index];
        my $cmp = numeric_compare($version[$index], $limit[$index]);
        return $cmp < 0 if $cmp;
    }
    return 1;
}
sub ordered {
    return sort {
        my @a = parts($a); my @b = parts($b);
        numeric_compare($a[0], $b[0]) || numeric_compare($a[1], $b[1]) ||
            numeric_compare($a[2], $b[2]) || $a cmp $b
    } @_;
}
sub inventory {
    my ($destination) = @_;
    my $text = defined($destination)
        ? git('', 'ls-remote', '--tags', '--refs', '--', $destination)
        : git('', 'for-each-ref', '--format=%(objectname)%09%(refname)%09%(symref)', 'refs/tags/');
    my %tags;
    for my $record (split /\n/, $text) {
        my ($oid, $ref, $symbolic) = split /\t/, $record, -1;
        defined($ref) && $oid =~ /\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z/ && $oid =~ /[1-9a-f]/
            && $ref =~ m{\Arefs/tags/[^\s]+\z} or die "malformed tag inventory\n";
        die "symbolic tag is unsupported\n" if defined($symbolic) && length($symbolic);
        my $tag = substr($ref, 10);
        next unless selected($tag);
        !exists($tags{$tag}) or die "duplicate tag inventory\n";
        $tags{$tag} = $oid;
    }
    return \%tags;
}
sub destination { line(git('', 'remote', 'get-url', '--push', '--all', $remote)) }
my $destination = defined($remote) ? destination() : undef;
sub show {
    my ($label, $tags) = @_;
    printf "%s tags selected: %d\n", $label, scalar(keys %$tags);
    print "$_\t$tags->{$_}\n" for ordered(keys %$tags);
}
if (!$mutating) {
    show('local', inventory(undef));
    show("remote $remote", inventory($destination)) if defined $remote;
    print "Dry run only; no refs or maintenance state changed.\n";
    exit 0;
}

umask 0077;
my $common = line(git('', 'rev-parse', '--git-common-dir'));
$common = File::Spec->rel2abs($common, $repo);
my $state = "$common/tag-maintenance";
!-l $state or die "maintenance state is symlinked\n";
make_path($state);
my $claim = "$state/lock";
mkdir $claim or die "maintenance lock occupied; inspect its owner before clearing it\n";
$lock = $claim;
open my $owner, '>', "$lock/owner" or die $!;
print {$owner} "pid=$$\nrepository=$repo\n" or die $!;
close $owner or die $!;
my $pending = "$state/pending.json";
my $json = JSON::PP->new->canonical->pretty;
my $intent;
my %selection = (cutoff => $cutoff, remote => $remote // '', destination => $destination // '',
    delete_local => $delete_local ? 1 : 0, delete_remote => $delete_remote ? 1 : 0);
if (-e $pending || -l $pending) {
    -f $pending && !-l $pending or die "invalid saved intent file\n";
    open my $file, '<', $pending or die $!;
    $intent = $json->decode(do { local $/; <$file> });
    close $file or die $!;
    ref($intent) eq 'HASH' && ($intent->{format} // '') eq 'tag-maintenance-1'
        && ($intent->{attempt} // '') =~ /\Aattempt\.[A-Za-z0-9_]+\z/
        && join(',', sort keys %$intent) eq 'attempt,cutoff,delete_local,delete_remote,destination,format,local,remote,remote_tags'
        or die "invalid saved intent\n";
    for my $key (keys %selection) {
        defined($intent->{$key}) && !ref($intent->{$key}) && $intent->{$key} eq $selection{$key}
            or die "saved deletion selection/destination differs; reconcile $pending\n";
    }
    for my $scope (qw(local remote_tags)) {
        ref($intent->{$scope}) eq 'HASH' or die "invalid saved tag roster\n";
        for my $tag (keys %{$intent->{$scope}}) {
            selected($tag) && defined($intent->{$scope}{$tag}) && !ref($intent->{$scope}{$tag}) &&
                $intent->{$scope}{$tag} =~ /\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z/
                && $intent->{$scope}{$tag} =~ /[1-9a-f]/
                or die "invalid saved tag identity\n";
        }
    }
    $attempt = "$state/$intent->{attempt}";
    -d $attempt && !-l $attempt or die "missing attempt evidence\n";
} else {
    $attempt = tempdir('attempt.XXXXXX', DIR => $state, CLEANUP => 0);
}
($log, my $log_path) = tempfile('run.XXXXXX', DIR => $attempt);
$log->autoflush(1);
print "Evidence: $attempt\n";
if (!$intent) {
    $intent = { format => 'tag-maintenance-1', %selection,
        attempt => (File::Spec->splitdir($attempt))[-1],
        local => inventory(undef), remote_tags => defined($remote) ? inventory($destination) : {} };
    my ($file, $temporary) = tempfile('intent.XXXXXX', DIR => $attempt);
    print {$file} $json->encode($intent) or die $!;
    close $file or die $!;
    rename $temporary, $pending or die "save intent: $!\n";
}
show('local', $intent->{local});
show("remote $remote", $intent->{remote_tags}) if defined $remote;
sub remaining {
    my ($saved, $current) = @_;
    for my $tag (keys %$saved) {
        !exists($current->{$tag}) || $current->{$tag} eq $saved->{$tag}
            or die "tag identity changed: $tag; preserve and reconcile saved intent\n";
    }
    return ordered(grep { exists $current->{$_} } keys %$saved);
}
# Check local recovery identities before attempting any remote effects.
remaining($intent->{local}, inventory(undef)) if $delete_local;
if ($delete_remote) {
    destination() eq $destination or die "push destination changed\n";
    my @pending_tags = remaining($intent->{remote_tags}, inventory($destination));
    while (@pending_tags) {
        my @batch = splice @pending_tags, 0, 50;
        destination() eq $destination or die "push destination changed\n";
        git('', 'push', '--no-follow-tags', '--atomic',
            (map { "--force-with-lease=refs/tags/$_:$intent->{remote_tags}{$_}" } @batch),
            '--', $destination, (map { ":refs/tags/$_" } @batch));
    }
    my @remaining = remaining($intent->{remote_tags}, inventory($destination));
    !@remaining
        or die "remote deletion not verified; local recovery tags retained\n";
    print "Remote deletion verified for saved selection.\n";
}
if ($delete_local) {
    my @remaining = remaining($intent->{local}, inventory(undef));
    if (@remaining) {
        my $commands = "start\n" . join('', map {
            "option no-deref\ndelete refs/tags/$_ $intent->{local}{$_}\n"
        } @remaining) . "prepare\ncommit\n";
        git($commands, 'update-ref', '--stdin');
    }
    my @after = remaining($intent->{local}, inventory(undef));
    !@after or die "local deletion not verified\n";
    print "Local deletion verified for saved selection.\n";
}
rename $pending, "$attempt/intent.json" or die "retain completed intent: $!\n";
print "Tag maintenance complete; intent and logs retained: $attempt\n",
    "Other clones can republish deleted tags; avoid broad tag pushes.\n";
