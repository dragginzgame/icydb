#!/usr/bin/env perl
use strict;
use warnings;
use Cwd qw(abs_path getcwd);
use Digest::SHA qw(sha256_hex);
use File::Basename qw(dirname);
use File::Path qw(make_path remove_tree);
use File::Temp qw(tempdir);
use FindBin qw($RealBin);
use JSON::PP;

my $root = abs_path("$RealBin/../..");
my ($json, $parent) = (0, undef);
sub usage {
    return <<'USAGE';
Usage: cloc-tooling.pl [--json] [parent-directory]

Count CI and other tooling in immediate Git checkouts, including non-Rust repos.
Defaults to the parent of this script's checkout. Reads tracked and nonignored
untracked working files; never runs repository scripts, Cargo, or Make targets.

Scope: scripts/, .github/, .githooks/, make/, tools/, xtask/, ci/, .cargo/;
Makefiles, root shell/Perl/Python/AWK helpers and .env files; Cargo package build.rs.
Docs, binaries, generated build/cache directories and symlinks are excluded.
JSON, patches and CSV/TSV tables are supporting data, counted as physical lines
separately. Other source/config LOC uses cloc, excluding comments/blank lines.

Shared LOC matches hashes AND modes in .shared-tooling*.snapshot manifests,
including nested snapshots; paths resolve beside each manifest.
Local LOC includes unrecorded files and drifted copies; drift is reported.
--json includes per-file hashes/counts, skipped files and source identities.
Repeated copies are all counted; LOC and hash matches are discovery aids,
not proof that different contracts can be consolidated.
USAGE
}
for my $arg (@ARGV) {
    if ($arg eq '--help' || $arg eq '-h') { print usage(); exit 0; }
    elsif ($arg eq '--json' && !$json) { $json = 1; }
    elsif ($arg !~ /^-/ && !defined $parent) { $parent = $arg; }
    else { die usage(); }
}
$parent = abs_path($parent // "$root/..");
die "parent directory not found\n" unless defined $parent && -d $parent;
$ENV{LC_ALL} = 'C';
$ENV{GIT_OPTIONAL_LOCKS} = '0';
$ENV{PATH} = "$root/.tools/host/bin:$ENV{PATH}";

sub capture {
    open my $fh, '-|', @_ or die "cannot execute $_[0]: $!\n";
    local $/;
    my $value = <$fh> // '';
    close $fh or die "command failed: $_[0] (status $?)\n";
    return $value;
}
sub read_file {
    open my $fh, '<', $_[0] or die "read $_[0]: $!\n";
    binmode $fh;
    local $/;
    return <$fh> // '';
}
sub write_file {
    open my $fh, '>', $_[0] or die "write $_[0]: $!\n";
    binmode $fh;
    print {$fh} $_[1] or die "write $_[0]: $!\n";
    close $fh or die "close $_[0]: $!\n";
}
sub safe_path {
    my ($path) = @_;
    die "unrepresentable path in tooling inventory\n"
        if $path =~ /[\x00-\x1f\x7f]/ || $path =~ m{^/|(?:^|/)\.{1,2}(?:/|$)|//};
    return $path;
}
sub is_linked {
    my ($path) = @_;
    while ($path ne '.') {
        return 1 if -l $path;
        $path = dirname($path);
    }
    return 0;
}
sub in_scope {
    my ($path, $logical) = @_;
    return 0 if $path =~ m{(?:^|/)(?:target|node_modules|__pycache__|\.tools|\.git|dist)(?:/|$)};
    return 0 if $path =~ /\.(?:md|txt|lock|png|svg|jpg|gif|wasm|pdf|glb|pyc)$/i;
    return 1 if $logical =~ m{^(?:scripts|\.github|\.githooks|make|tools|xtask|ci|\.cargo)/};
    return 1 if $path =~ m{(?:^|/)(?:Makefile|GNUmakefile|justfile|Dockerfile|Taskfile\.ya?ml)$};
    return 1 if $path =~ m{^[^/]+\.(?:sh|bash|pl|py|awk|env)$};
    return $path =~ m{(?:^|/)build\.rs$} && -f dirname($path) . '/Cargo.toml';
}
sub load_snapshot {
    my ($manifest) = @_;
    safe_path($manifest);
    die "symlinked snapshot manifest\n" if is_linked($manifest);
    my $base = dirname($manifest);
    my (%files, %headers);
    for my $line (split /\n/, read_file($manifest)) {
        next if $line =~ /^\s*(?:#|$)/;
        my ($kind, @values) = split /\t/, $line, -1;
        if ($kind eq 'file') {
            die "malformed or duplicate snapshot file\n" unless @values == 3 &&
                $values[0] =~ /^[a-f0-9]{64}$/ && $values[1] =~ /^(?:x|-)$/ &&
                length($values[2]) && !exists $files{$values[2]};
            safe_path($values[2]);
            $files{$values[2]} = { hash => $values[0], mode => $values[1] };
        } else {
            die "malformed snapshot header\n" unless @values == 1 &&
                $kind =~ /^(?:format|revision|source)$/ && !exists $headers{$kind};
            $headers{$kind} = $values[0];
        }
    }
    die "unsupported shared snapshot identity\n" unless ($headers{format} // '') eq '1' &&
        ($headers{revision} // '') =~ /^[a-f0-9]{40,64}$/ &&
        ($headers{source} // '') =~ m{^https://github\.com/dragginzgame/shared-tooling(?:\.git)?$};
    my %resolved = map { ($base eq '.' ? $_ : "$base/$_") => $files{$_} } keys %files;
    return (\%resolved, {path => $manifest, revision => $headers{revision}, root => $base});
}

capture('git', '--version');
my $cloc_version = eval { capture('cloc', '--version') };
die "cloc unavailable; run make install-host-tools in $root\n$@" if $@;
chomp $cloc_version;
opendir my $dh, $parent or die "open $parent: $!\n";
my @names = sort grep { !/^\.{1,2}$/ && -d "$parent/$_" && !-l "$parent/$_" && -e "$parent/$_/.git" } readdir $dh;
closedir $dh;
die "no Git checkouts found in $parent\n" unless @names;
safe_path($_) for @names;
my $temp = tempdir('shared-tooling-inventory.XXXXXX', TMPDIR => 1, CLEANUP => 0);
# jq is source, not JSON data. cloc has no built-in jq language definition.
my $jq_language = "$temp/jq.lang";
write_file($jq_language, "jq\n    filter remove_matches ^\\s*#\n    filter remove_inline #.*\$\n    extension jq\n    3rd_gen_scale 1\n");
my @keys = qw(ci_loc other_loc total_loc shared_loc local_loc data_lines);
my (%total, @repos);
@total{@keys} = (0) x @keys;
my $failed = 0;
my $start = getcwd();
for my $name (@names) {
    my $repo = { name => $name, root => "$parent/$name" };
    my $ok = eval {
        safe_path($name);
        chdir $repo->{root} or die "enter checkout: $!\n";
        my $git_root = capture('git', 'rev-parse', '--show-toplevel'); chomp $git_root;
        die "not a Git checkout root\n" unless abs_path($git_root) eq $repo->{root};
        $repo->{head} = capture('git', 'rev-parse', 'HEAD'); chomp $repo->{head};
        $repo->{dirty} = length(capture('git', 'status', '--porcelain=v1', '-z')) ? JSON::PP::true : JSON::PP::false;
        my %paths = map { $_ => 1 } split /\0/, capture('git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard');
        my (%snapshot, @manifests);
        for my $path (sort keys %paths) {
            next unless $path =~ m{(?:^|/)\.shared-tooling(?:-[^/]*)?\.snapshot$} && -f $path;
            my ($files, $manifest) = load_snapshot($path);
            for my $target (keys %$files) {
                die "conflicting snapshot declarations for $target\n" if exists $snapshot{$target} &&
                    ($snapshot{$target}{hash} ne $files->{$target}{hash} || $snapshot{$target}{mode} ne $files->{$target}{mode});
                $snapshot{$target} = $files->{$target};
            }
            push @manifests, $manifest;
        }
        $repo->{snapshot_manifests} = \@manifests;
        my @snapshot_roots = sort { length($b) <=> length($a) } map { $_->{root} } grep { $_->{root} ne '.' } @manifests;
        my (%sums, @files, @source_paths, @skipped, @drift);
        @sums{@keys} = (0) x @keys;
        my $copy = "$temp/checkouts/$name";
        make_path($copy);
        for my $path (sort keys %paths) {
            my $logical = $path;
            for my $base (@snapshot_roots) {
                if (index($path, "$base/") == 0) { $logical = substr($path, length($base) + 1); last; }
            }
            next unless in_scope($path, $logical) && -f $path;
            safe_path($path);
            # Do not follow either file symlinks or symlinked parent directories.
            next if is_linked($path);
            my $content = read_file($path);
            my $hash = sha256_hex($content);
            my $mode = -x $path ? 'x' : '-';
            my $declared = $snapshot{$path};
            my $owner = $declared && $hash eq $declared->{hash} && $mode eq $declared->{mode} ? 'shared' : 'local';
            push @drift, $path if $declared && $owner eq 'local';
            my $file = { path => $path, sha256 => $hash, owner => $owner };
            if ($path =~ /\.(?:json|jsonl|patch|diff|tsv|csv)$/i) {
                $file->{kind} = 'data';
                $file->{lines} = ($content =~ tr/\n//) + (length($content) && $content !~ /\n\z/ ? 1 : 0);
                $sums{data_lines} += $file->{lines};
            } else {
                $file->{kind} = $logical =~ m{^(?:\.github/|scripts/ci/|ci/)} ? 'ci' : 'other';
                make_path(dirname("$copy/$path"));
                write_file("$copy/$path", $content);
                push @source_paths, "./$path";
            }
            push @files, $file;
        }
        # Count captured bytes, so concurrent work cannot mismatch hashes and LOC.
        write_file("$temp/files", join('', map { "$_\n" } @source_paths));
        chdir $copy or die $!;
        my $counts = @source_paths ? decode_json(capture('cloc', '--quiet', '--json', '--by-file',
            '--skip-uniqueness', '--force-lang=Bourne Shell,env', "--read-lang-def=$jq_language", "--list-file=$temp/files")) : {};
        for my $file (@files) {
            next if $file->{kind} eq 'data';
            my $row = $counts->{"./$file->{path}"} // $counts->{$file->{path}};
            unless ($row) { push @skipped, $file->{path}; next; }
            die "invalid cloc count\n" unless defined $row->{code} && $row->{code} =~ /^\d+$/;
            $file->{code} = 0 + $row->{code};
            $file->{language} = $row->{language};
            $sums{"$file->{kind}_loc"} += $file->{code};
            $sums{"$file->{owner}_loc"} += $file->{code};
            $sums{total_loc} += $file->{code};
        }
        $repo->{files} = \@files;
        $repo->{skipped_files} = \@skipped;
        $repo->{drifted_files} = \@drift;
        $repo->{totals} = \%sums;
        warn "$name: cloc skipped " . scalar(@skipped) . " selected files; see --json\n" if @skipped;
        warn "$name: " . scalar(@drift) . " snapshot files differ; counted as local\n" if @drift;
        $total{$_} += $sums{$_} for @keys;
        1;
    };
    unless ($ok) {
        $repo->{error} = "$@";
        warn "$name: $@";
        $failed = 1;
    }
    push @repos, $repo;
}
chdir $start or die $!;
if ($json) {
    print JSON::PP->new->canonical->pretty->encode({ cloc_version => $cloc_version,
        repositories => \@repos, totals => \%total, partial => $failed ? JSON::PP::true : JSON::PP::false });
} else {
    my $width = 24;
    for my $repo (@repos) { $width = length($repo->{name}) if length($repo->{name}) > $width; }
    my $format = "%-${width}s" . (' %11s' x @keys) . "\n";
    printf $format, 'repository', @keys;
    printf $format, '-' x $width, ('-' x 11) x @keys;
    for my $repo (@repos) {
        printf $format, $repo->{name}, $repo->{error} ? ('ERROR') x @keys : @{$repo->{totals}}{@keys};
    }
    printf $format, '-' x $width, ('-' x 11) x @keys;
    printf $format, $failed ? 'TOTAL (partial)' : 'TOTAL', @total{@keys};
}
if ($failed) { warn "Failed tooling inventory retained: $temp\n"; }
else { remove_tree($temp); }
exit $failed;
