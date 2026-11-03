#!/usr/bin/env perl
# Shared companions: scripts/ci/ValidationPlan.pm scripts/ci/validation-input-checkpoint.pl docs/group-validation-adoption.md
use strict;
use warnings;
use FindBin qw($RealBin);
use lib $RealBin;
use ValidationPlan qw(json bytes digest document relative_path keys_are names policy plan);
use JSON::PP;
use Digest::SHA qw(sha256_hex);
use Cwd qw(abs_path);
use Encode qw(decode FB_CROAK);

# Read-only observations. The caller owns exclusive custody and release authority.
my $json = JSON::PP->new->canonical->utf8;
my ($operation, $root, @args) = @ARGV;
defined($root) && defined($operation) or die "usage: validation-evidence.pl verify|check|roster|baseline ROOT ...\n";
$root = abs_path($root) // die "missing source root\n";
-d $root or die "source root is not a directory\n";
sub capture {
    open my $in, '-|', @_ or die "start observation: $!\n";
    binmode $in;
    my $raw = '';
    while (1) {
        my $n = read $in, my $chunk, 65536;
        defined($n) or die "read observation: $!\n";
        last unless $n;
        $raw .= $chunk;
        length($raw) <= 4194304 or die "observation exceeds 4 MiB\n";
    }
    close $in or die "observation failed: @_\n";
    return $raw;
}
sub git { capture('git', '-C', $root, @_) }
sub same { $_[0] eq $_[1] or die "$_[2]\n" }
sub bounded_json {
    my $raw = json($_[0]);
    length($raw) <= 1048576 or die "validation result exceeds 1 MiB\n";
    return $raw;
}
sub hash_map {
    my ($value) = @_;
    ref($value) eq 'HASH' && keys(%$value) or die "empty validation checkpoint\n";
    for my $path (keys %$value) {
        relative_path($path);
        defined($value->{$path}) && !ref($value->{$path}) && $value->{$path} =~ /\A[0-9a-f]{64}\z/
            or die "invalid validation checkpoint hash\n";
    }
}
sub subset {
    my ($small, $large) = @_;
    hash_map($small);
    for my $path (keys %$small) {
        exists($large->{$path}) && $small->{$path} eq $large->{$path}
            or die "checkpoint inventory or hash drift: $path\n";
    }
}
sub verify {
    my ($path, $source, $group, $run, $context_path) = @_;
    relative_path($run);
    my ($plan, $policy, $inputs, $rows) = plan($root, $path, $source, $group);
    my %inputs = map { $_ => 1 } @$inputs;
    $inputs{$context_path} or die "context must be a declared plan input\n";
    my $context = document($root, $context_path);
    ref($context) eq 'HASH' or die "context must be a canonical JSON object\n";
    my %files;
    my $read = sub {
        my ($name) = @_;
        my $raw = bytes($root, "$run/$name");
        $files{$name} = sha256_hex($raw);
        return $raw;
    };
    same($read->('parsed-inputs.json.plan.json'), json($plan), 'runner selected different plan');
    same($read->('parsed-inputs.json.policy.json'), json($policy), 'runner selected different policy');
    same(decode('UTF-8', $read->('plan-selection.tsv'), FB_CROAK), join('', (map { "input\t$_\n" } @$inputs), (map { "$_\n" } @$rows)),
        'runner selection differs');
    my $checkpoint_raw = $read->('plan-inputs.json');
    my $checkpoint = $json->decode($checkpoint_raw);
    hash_map($checkpoint);
    same(json($checkpoint), $checkpoint_raw, 'noncanonical checkpoint');
    for my $input (@$inputs) {
        exists($checkpoint->{$input}) or die "missing plan input checkpoint: $input\n";
    }
    my $initial = $json->decode($read->('inputs.json'));
    my $parsed = $json->decode($read->('parsed-inputs.json'));
    subset($initial, $parsed); subset($parsed, $checkpoint);
    exists($initial->{$path}) && exists($parsed->{$plan->{policy}})
        or die "missing initial plan/policy checkpoint\n";
    # Reuse the existing inventory observer. /dev/stdout avoids scratch writes.
    my $observed = capture($^X, "$RealBin/validation-input-checkpoint.pl", $root,
        "$root/$run/plan-inputs.json", '/dev/stdout', "$RealBin/verify-file-checksum.sh");
    same(json($json->decode($observed)), json($checkpoint), 'current input checkpoint differs');
    my $events = "target\tstate\treason\n";
    for my $stage (@{$plan->{stages}}) {
        $events .= join("\t", $stage->{target}, $stage->{required} ? 'blocked' : 'not-applicable', $stage->{reason})."\n";
    }
    my $timings_raw = decode('UTF-8', $read->('timings.tsv'), FB_CROAK);
    $timings_raw =~ s/\Atarget\tresult\tseconds\tlog\n// or die "invalid timings header\n";
    my @timings = split /\n/, $timings_raw, -1;
    pop(@timings) eq '' or die "unterminated timings\n";
    my @required = grep { $_->{required} } @{$plan->{stages}};
    @timings == @required or die "missing or extra stage timings\n";
    my (%proof, @stages);
    for my $index (0 .. $#required) {
        my ($target, $state, $seconds, $log) = split /\t/, $timings[$index], -1;
        defined($log) && split(/\t/, $timings[$index], -1) == 4
            && $target eq $required[$index]{target} && $state eq 'PASS'
            && $seconds =~ /\A(?:0|[1-9][0-9]{0,17})\z/
            or die "failed, duplicate, reordered or malformed stage timing\n";
        # Current runner emits absolute paths and sequential log names. Never
        # follow arbitrary paths in a saved record, including an outside log.
        same($log, decode('UTF-8', "$root/$run/$index.log", FB_CROAK), 'execution log escapes or differs from runner slot');
        $files{"$index.log"} = digest($root, "$run/$index.log");
        for my $phase ('before', 'after') {
            same($read->("$index-$phase-inputs.json"), $checkpoint_raw,
                'stage input checkpoint differs');
        }
        $events .= "$target\tPASS\t$log\n";
        $proof{$target} = {target=>$target, state=>'PASS', seconds=>0+$seconds,
            log=>"$index.log", log_sha256=>$files{"$index.log"}};
    }
    same(decode('UTF-8', $read->('plan-events.tsv'), FB_CROAK), $events, 'failed, interrupted, duplicate or reordered stage events');
    for my $stage (@{$plan->{stages}}) {
        push @stages, $proof{$stage->{target}} // {target=>$stage->{target}, state=>'not-applicable', reason=>$stage->{reason}};
    }
    # Reobserve after hashing evidence. No result is printed on drift/failure.
    for my $input (keys %$checkpoint) {
        same(digest($root, $input), $checkpoint->{$input}, 'input changed during evidence observation');
    }
    for my $file (keys %files) {
        same(digest($root, "$run/$file"), $files{$file}, 'run evidence changed during observation');
    }
    return {schema=>1, kind=>'group-validation-evidence', source=>$source, group=>$group,
        scope=>$plan->{scope}, run=>$run, plan_sha256=>sha256_hex(json($plan)),
        policy_sha256=>sha256_hex(json($policy)), context_sha256=>sha256_hex(json($context)),
        inputs=>$checkpoint, files=>\%files, stages=>\@stages};
}

if ($operation eq 'verify' || $operation eq 'check') {
    @args == ($operation eq 'verify' ? 5 : 6) or die "usage: verify|check ROOT PLAN SOURCE GROUP RUN CONTEXT [PROOF]\n";
    my $result = verify(@args[0..4]);
    my $raw = bounded_json($result);
    if ($operation eq 'check') {
        same(bytes($root, $args[5]), $raw, 'saved evidence differs; preserve original proof');
    }
    print $raw;
} elsif ($operation eq 'roster') {
    @args == 2 or die "usage: roster ROOT POLICY GROUP-NAMES.json\n";
    my $policy = policy($root, $args[0]);
    # Caller projects names from its already admitted Cargo group policy; no
    # duplicate Cargo/TOML policy parser or implicit metadata execution here.
    my $groups = document($root, $args[1]);
    names($groups);
    same(json([sort @$groups]), json([sort @{$policy->{groups}}]), 'Cargo and validation groups differ');
    print "$_\n" for @{$policy->{stages}};
} elsif ($operation eq 'baseline') {
    @args == 2 or die "usage: baseline ROOT SOURCE refs/tags/GROUP-vVERSION\n";
    my ($source, $reference) = @args;
    $source =~ /\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z/ &&
        $reference =~ m{\Arefs/tags/[A-Za-z0-9][A-Za-z0-9_.-]*\z} or die "invalid baseline identity\n";
    same(git('cat-file', '-t', $source), "commit\n", 'source is not a commit');
    my $observed = git('for-each-ref', '--format=%(refname)%09%(objectname)%09%(objecttype)', $reference);
    my @matches = grep { index($_, "$reference\t") == 0 } split /\n/, $observed;
    @matches <= 1 or die "ambiguous baseline reference\n";
    my $result = {schema=>1, source=>$source, reference=>$reference, available=>JSON::PP::false,
        reason=>'missing annotated baseline', changes=>[]};
    if (@matches) {
        my (undef, $object, $type) = split /\t/, $matches[0];
        $result->{object} = $object;
        $result->{reason} = 'baseline is not an annotated tag';
        if ($type eq 'tag') {
            my $base = git('rev-parse', '--verify', "$reference^{commit}"); chomp $base;
            my $status = system('git', '-C', $root, 'merge-base', '--is-ancestor', $base, $source);
            $status == 0 || $status == 256 or die "cannot observe baseline ancestry\n";
            $result->{baseline} = $base;
            $result->{reason} = 'baseline is not an ancestor';
            if ($status == 0) {
                my $raw = git('diff', '--no-ext-diff', '--no-renames', '--name-only', '-z', $base, $source, '--');
                my @paths = map { decode('UTF-8', $_, FB_CROAK) } split /\0/, $raw;
                relative_path($_) for @paths;
                $result->{available} = JSON::PP::true;
                $result->{reason} = 'annotated ancestor observed; caller must qualify scope';
                $result->{changes} = \@paths;
            }
        }
    }
    same(git('for-each-ref', '--format=%(refname)%09%(objectname)%09%(objecttype)', $reference), $observed,
        'baseline reference moved during observation');
    print bounded_json($result);
} else {
    die "unknown validation observation operation\n";
}
