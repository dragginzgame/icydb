#!/usr/bin/env perl
# Shared companions: scripts/ci/RustSource.pm
use strict;
use warnings;
use Cwd qw(abs_path getcwd);
use Digest::SHA qw(sha256_hex);
use Encode qw(decode encode FB_CROAK LEAVE_SRC);
use File::Basename qw(dirname basename);
use File::Find qw(find);
use File::Path qw(make_path);
use File::Spec;
use File::Temp qw(tempfile);
use Getopt::Long qw(GetOptionsFromArray);
use JSON::PP;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);
use FindBin qw($RealBin);
use lib $RealBin;
use RustSource qw(has_module_path_attribute);

sub now { return clock_gettime(CLOCK_MONOTONIC); }

# Consumers supply policy inputs; this driver owns ordering, evidence and reuse.
my $mode = shift(@ARGV) // '';
my ($cargo, $derives, $no_cache, $order, $extra_order) = ('cargo', 0, 0, '', '');
my @extra;
my (@standalone, $edition);
my $rustfmt = 'rustfmt';
Getopt::Long::Configure(qw(no_auto_abbrev no_ignore_case));
my $cargo_options = 0;
my $valid = GetOptionsFromArray(\@ARGV,
    'cargo=s' => sub { $cargo = $_[1]; $cargo_options++ },
    'sort-derives' => \$derives, 'no-cache' => \$no_cache,
    'manifest-order=s' => \$order, 'extra-manifest-order=s' => \$extra_order,
    'extra-workspace=s@' => \@extra,
    'rustfmt-source=s@' => \@standalone, 'rustfmt-edition=s' => \$edition,
    'rustfmt=s' => \$rustfmt,
);
if (!$valid || @ARGV || $cargo_options > 1 || $mode !~ /\A--(?:write|check)\z/ || !length($cargo)) {
    die "usage: run-rust-formatting.pl --write|--check [--cargo PATH] [--sort-derives] [--extra-workspace PATH ...] [--manifest-order ORDER] [--extra-manifest-order ORDER] [--rustfmt-source PATH --rustfmt-edition EDITION [--rustfmt PATH]] [--no-cache]\n";
}
if (@standalone ? (!defined($edition) || $edition !~ /\A(?:2015|2018|2021|2024)\z/ || !length($rustfmt))
                : (defined($edition) || $rustfmt ne 'rustfmt')) {
    die "standalone Rustfmt sources require an explicit supported edition and one Rustfmt executable\n";
}
$ENV{CARGO_NET_OFFLINE} = 'true';
$ENV{RUSTUP_AUTO_INSTALL} = '0';
my $root = getcwd();
$cargo = File::Spec->rel2abs($cargo, $root) if $cargo =~ m{/};
$rustfmt = File::Spec->rel2abs($rustfmt, $root) if $rustfmt =~ m{/};
my @workspaces = ($root);
my %seen = ($root => 1);
for my $path (@extra) {
    my $absolute = abs_path($path);
    defined($absolute) && -d $absolute && index("$absolute/", "$root/") == 0
        or die "extra formatting workspace must be inside this checkout\n";
    !$seen{$absolute}++ or die "duplicate formatting workspace\n";
    push @workspaces, $absolute;
}
my %sources;
@standalone = map {
    my $absolute = abs_path($_);
    defined($absolute) && -f $absolute && !-l $_ && index($absolute, "$root/") == 0
        or die "standalone formatting source must be a regular file inside this checkout\n";
    !$sources{$absolute}++ or die "duplicate standalone formatting source\n";
    $absolute;
} @standalone;
my $script = abs_path($0);
my $receipt = "$root/target/shared-tooling/formatting.receipt";
my @check = $mode eq '--check' ? ('--check') : ();
my @fmt_check = $mode eq '--check' ? ('--', '--check') : ();
$| = 1;

sub visible {
    my ($value) = @_;
    my $text = eval { decode('UTF-8', $value, FB_CROAK | LEAVE_SRC) };
    if (!defined $text) {
        $value =~ s/([^\x20-\x7e])/sprintf('\\x{%x}', ord($1))/ge;
        return $value;
    }
    $text =~ s/([\p{Cc}\x{2028}\x{2029}\x{202a}-\x{202e}\x{2066}-\x{2069}])/sprintf('\\x{%x}', ord($1))/ge;
    return encode('UTF-8', $text);
}

# Use literal argv, preserve jobserver descriptors and retain only failed logs.
sub execute {
    my ($label, $capture, @command) = @_;
    my $temporary = $ENV{RUNNER_TEMP} // $ENV{TMPDIR} // '/tmp';
    my ($log, $path) = tempfile('formatting.XXXXXX', DIR => $temporary, UNLINK => 0);
    my $start = now();
    print visible($label), '... ' if length($label);
    my $pid = fork();
    defined($pid) or die "cannot start formatter: $!\n";
    if (!$pid) {
        open STDOUT, '>&', $log or die "redirect formatter output: $!\n";
        open STDERR, '>&', $log or die "redirect formatter errors: $!\n";
        exec { $command[0] } @command or do {
            print STDERR "execute formatter: $!\n";
            exit 127;
        };
    }
    waitpid($pid, 0) == $pid or die "wait for formatter: $!\n";
    my $status = $?;
    my $exit = $status & 127 ? 128 + ($status & 127) : $status >> 8;
    close $log or die "close formatter log: $!\n";
    if ($exit) {
        printf "FAILED (exit %d, %.2fs)\nDetails: %s\n", $exit, now() - $start, visible($path)
            if length($label);
        die "input observation failed; details: " . visible($path) . "\n" if $capture;
        exit $exit;
    }
    printf "ok (%.2fs)\n", now() - $start if length($label);
    my $output = $capture ? read_bytes($path) : '';
    unlink $path or die "remove successful formatter log: $!\n";
    return $output;
}

sub read_bytes {
    my ($path, $limit) = @_;
    open my $file, '<:raw', $path or die "read formatting input: $!\n";
    local $/;
    my $bytes;
    if (defined $limit) { defined(read($file, $bytes, $limit)) or die "read receipt: $!\n"; }
    else { $bytes = <$file>; }
    close $file or die "close formatting input: $!\n";
    return $bytes // '';
}

sub tool_identity {
    my ($name) = @_;
    for my $directory (split /:/, $ENV{PATH} // '') {
        my $path = $name =~ m{/} ? $name : "$directory/$name";
        next unless -f $path && -x $path;
        my @stat = Time::HiRes::stat($path);
        return [abs_path($path), @stat[0, 1, 7, 9, 10]];
    }
    return [$name, 'not-on-path'];
}

sub fingerprint {
    # An arbitrary rustfmt wrapper can depend on inputs this driver cannot know.
    !length($ENV{RUSTFMT} // '') or die "custom rustfmt selection\n";
    my (%files, %exclude, %scanned, @selection, @metadata);
    $files{$script} = 1;
    $files{"$RealBin/RustSource.pm"} = 1;
    for my $workspace (@workspaces) {
        chdir $workspace or die "enter formatting workspace: $!\n";
        my $raw_metadata = execute('', 1, $cargo, 'metadata', '--locked', '--offline', '--no-deps', '--format-version', '1');
        my $metadata = decode_json($raw_metadata);
        ref($metadata->{packages}) eq 'ARRAY' && @{$metadata->{packages}}
            or die "formatting metadata has no packages\n";
        push @metadata, $metadata;
        push @selection, ['metadata', sha256_hex(JSON::PP->new->canonical->utf8->encode($metadata))];
        push @selection, [$workspace, map { execute('', 1, $cargo, @$_) }
            (['--version'], ['sort', '--version'], ['fmt', '--version'])];
        $exclude{$metadata->{target_directory}} = 1 if defined $metadata->{target_directory};
        $exclude{"$workspace/$_"} = 1 for qw(.tools .cache .tmp node_modules target);
        $files{"$workspace/Cargo.toml"} = 1;
        $files{"$workspace/Cargo.lock"} = 1 if -f "$workspace/Cargo.lock";
        opendir my $directory, $workspace or die "inspect workspace configuration: $!\n";
        $files{"$workspace/$_"} = 1 for grep { /[.]toml\z/ && -f "$workspace/$_" } readdir $directory;
        closedir $directory or die "close workspace configuration: $!\n";
    }
    for my $index (0 .. $#workspaces) {
        my $workspace = $workspaces[$index];
        my $metadata = $metadata[$index];
        for my $package (@{$metadata->{packages}}) {
            my $manifest = $package->{manifest_path};
            defined($manifest) && -f $manifest or die "missing package manifest\n";
            for my $target (@{$package->{targets} // []}) {
                my $source = $target->{src_path} // die "missing target source\n";
                for my $excluded (keys %exclude) {
                    index("$source/", "$excluded/") != 0 or die "target source in artifact directory\n";
                }
            }
            $files{$manifest} = 1;
            my $directory = dirname($manifest);
            for my $dependency (@{$package->{dependencies} // []}) {
                next unless defined $dependency->{path};
                my $path = $dependency->{path};
                grep { grep { dirname($_->{manifest_path}) eq $path } @{$_->{packages}} } @metadata
                    or die "non-workspace path dependency disables reuse\n";
            }
            for my $target (@{$package->{targets} // []}) {
                index(($target->{src_path} // '') . '/', "$directory/") == 0
                    or die "external target source disables reuse\n";
            }
            # File::Find otherwise warns and continues with a partial inventory.
            local $SIG{__WARN__} = sub { die @_ };
            find({no_chdir => 1, wanted => sub {
                if (-d $_ && ($exclude{$_} || $_ eq "$root/target" || basename($_) eq '.git')) {
                    $File::Find::prune = 1; return;
                }
                if (-d $_ && $scanned{$_}++) { $File::Find::prune = 1; return; }
                -l $_ and die "source symlink disables formatting reuse\n";
                $files{$_} = 1 if -f $_ && /(?:[.]rs|[.]toml|rust-toolchain)\z/;
            }}, $directory);
        }
        # Both rustfmt and Cargo may select configuration above a package/root.
        my $ancestor = $workspace;
        while (1) {
            for my $name (qw(rustfmt.toml .rustfmt.toml rust-toolchain rust-toolchain.toml .cargo/config .cargo/config.toml)) {
                my $path = "$ancestor/$name";
                $files{$path} = 1 if -f $path;
            }
            last if dirname($ancestor) eq $ancestor;
            $ancestor = dirname($ancestor);
        }
    }
    chdir $root or die "return to formatting root: $!\n";
    if ($derives) {
        # The derive sorter uses ignore-aware repository traversal, beyond Cargo
        # members. rg uses the same ignore conventions; without it run uncached.
        my $sources = execute('', 1, 'rg', '--files', '--null', '--glob', '*.rs', '.');
        my @sources = sort grep { length } split /\0/, $sources;
        push @selection, ['derive-sources', @sources];
        $files{"$root/$_"} = 1 for @sources;
        push @selection, ['derives-version', execute('', 1, $cargo, 'sort-derives', '--version')];
        $files{"$root/$_"} = 1 for grep { -f "$root/$_" } qw(.sort-derives.toml sort-derives.toml);
    }
    for my $directory ($ENV{CARGO_HOME} // ($ENV{HOME} // '') . '/.cargo') {
        $files{"$directory/$_"} = 1 for grep { -f "$directory/$_" } qw(config config.toml);
    }
    for my $path (
        ($ENV{HOME} // '') . '/rustfmt.toml',
        ($ENV{HOME} // '') . '/.rustfmt.toml',
        ($ENV{XDG_CONFIG_HOME} // (($ENV{HOME} // '') . '/.config')) . '/rustfmt/rustfmt.toml',
    ) { $files{$path} = 1 if -f $path; }
    my @contents;
    for my $path (sort keys %files) {
        my $bytes = read_bytes($path);
        # Explicit module paths are outside the standard input contract. Restrict
        # inspection to balanced attributes, not unrelated later source text.
        $path =~ /[.]rs\z/ && has_module_path_attribute(decode('UTF-8', $bytes, FB_CROAK | LEAVE_SRC))
            and die "explicit module path disables reuse\n";
        push @contents, [$path, sha256_hex($bytes)];
    }
    return sha256_hex(JSON::PP->new->canonical->utf8->encode([
        \@workspaces, $derives, $order, $extra_order, \@selection, \@contents,
        [map { tool_identity($_) } ($cargo, qw(cargo-sort cargo-fmt rustfmt), $derives ? 'cargo-sort-derives' : ())],
        [map { [$_, $ENV{$_} // ''] } qw(PATH CARGO_HOME RUSTUP_HOME RUSTUP_TOOLCHAIN RUSTC_BOOTSTRAP CARGO_TARGET_DIR RUSTFLAGS CARGO_ENCODED_RUSTFLAGS)],
    ]));
}

my ($before, $started) = (undef, now());
my $reuse = !$no_cache && !@standalone && !length($ENV{SHARED_TOOLING_FORMAT_WORKTREE} // '');
print "Standalone Rust sources selected; running without shortcut\n" if @standalone;
if ($reuse) {
    print "Checking formatting inputs... ";
    $before = eval { fingerprint() };
    chdir $root or die "return to formatting root: $!\n";
    my $saved = -f $receipt && !-l $receipt ? eval { read_bytes($receipt, 256) } : undef;
    if (defined($before) && defined($saved) && $saved eq "formatting-1 $before\n") {
        printf "unchanged (%.2fs)\n", now() - $started;
        exit 0;
    }
    printf "%s (%.2fs)\n", defined($before) ? 'changed' : 'running without shortcut', now() - $started;
}
execute('Checking derive formatter', 0, $cargo, 'sort-derives', '--version') if $derives;
execute('Checking standalone Rustfmt', 0, $rustfmt, '--version') if @standalone;
for my $index (0 .. $#workspaces) {
    my $workspace = $workspaces[$index];
    chdir $workspace or die "enter formatting workspace: $!\n";
    my $label = $index ? substr($workspace, length($root) + 1) : '.';
    my $selected_order = $index ? $extra_order : $order;
    my @order = length($selected_order) ? ('--order', $selected_order) : ();
    execute("Manifest sorting [$label]", 0, $cargo, 'sort', '--workspace', '--grouped', @check, @order);
    execute('Derive sorting [.]', 0, $cargo, 'sort-derives', @check) if !$index && $derives;
    execute("Rust formatting [$label]", 0, $cargo, 'fmt', '--all', @fmt_check);
}
chdir $root or die "return to formatting root: $!\n";
execute('Standalone Rust formatting', 0, $rustfmt, '--edition', $edition, @check, @standalone)
    if @standalone;
if (defined $before) {
    my $recording = now();
    print 'Recording formatting result... ';
    my $after = eval { fingerprint() };
    chdir $root or die "return to formatting root: $!\n";
    # A changed write needs a later check/unchanged write, not a receipt for
    # inputs no formatter has observed. Failed/interrupted passes never get here.
    if (defined($after) && $after eq $before) {
        eval {
            my $directory = dirname($receipt);
            -l "$root/target" || -l "$root/target/shared-tooling" || -l $receipt and die "cache symlink\n";
            make_path($directory);
            my ($file, $temporary) = tempfile('formatting-receipt.XXXXXX', DIR => $directory, UNLINK => 0);
            print {$file} "formatting-1 $after\n" or die "write receipt: $!\n";
            close $file or die "close receipt: $!\n";
            rename $temporary, $receipt or die "publish receipt: $!\n";
        };
        warn "Formatting succeeded; shortcut receipt could not be saved.\n" if $@;
    }
    printf "%s (%.2fs)\n", defined($after) && $after eq $before ? 'stable inputs' : 'inputs changed; not cached', now() - $recording;
}
