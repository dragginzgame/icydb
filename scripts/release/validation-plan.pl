#!/usr/bin/env perl
# Consumer-owned selection only; execution/checkpointing belongs to Shared.
use strict;
use warnings;
use JSON::PP;
use Digest::SHA qw(sha256_hex);
use Fcntl qw(O_WRONLY O_CREAT O_EXCL);
use FindBin;
use lib $FindBin::Bin;
use ValidationScope qw(cli_private);
my $json = JSON::PP->new->canonical->utf8;
my ($group, $source, $baseline, $output, $scope) = @ARGV;
@ARGV == 5 && $scope =~ /\A(?:auto|workspace)\z/
    or die "usage: validation-plan.pl GROUP SOURCE GROUP-TAG OUTPUT auto|workspace\n";
sub capture {
    open my $fh, '-|', @_ or die "start @_: $!\n";
    my $value = '';
    while (1) {
        my $n = read $fh, my $chunk, 65536;
        defined($n) or die "cannot read observation\n";
        last unless $n;
        $value .= $chunk;
        length($value) <= 4194304 or die "observation exceeds 4 MiB\n";
    }
    close $fh or die "command failed: @_\n";
    return $value;
}
sub read_file {
    my ($file) = @_;
    $file !~ m{^/|(?:^|/)\.{1,2}(?:/|$)|[\x00-\x1f]|//} or die "unsafe input path\n";
    my $route = '.';
    for (split m{/}, $file) { $route .= "/$_"; !-l $route or die "redirected input $file\n"; }
    -f $file or die "non-file input $file\n";
    open my $fh, '<:raw', $file or die "read $file: $!\n";
    local $/; return <$fh>;
}
sub same_names {
    my ($a,$b) = @_;
    join("\0",@$a) eq join("\0",@$b) or die "stage/group roster drift\n";
}
my $policy_path = 'ci/release-validation.json';
my $groups = $json->decode(read_file('ci/cargo-release-groups.json'));
my @groups = sort map { $_->{name} } @{$groups->{groups}};
grep($_ eq $group, @groups) or die "unknown group\n";
# Pin the reviewed complete recipes, not just their names. A new/removed gate,
# changed aggregation, release dispatch or group policy requires explicit review.
my $contract_raw = read_file('ci/release-validation-contract.json');
my $contract = $json->decode($contract_raw);
$contract->{schema} == 1 && $contract_raw eq $json->encode($contract)."\n"
    or die "invalid consumer mapping contract\n";
for my $path ('Makefile', 'ci/cargo-release-groups.json', 'ci/release-validation.json') {
    sha256_hex(read_file($path)) eq ($contract->{files}{$path}//'')
        or die "release recipes/group policy changed; requalify mapping\n";
}
my $groups_path = "$output.groups.json";
write_new($groups_path, \@groups);
my @full = split /\n/, capture('perl', 'scripts/ci/validation-evidence.pl',
    'roster', '.', $policy_path, $groups_path);
my @full_gate = split /\n/, capture('perl', 'scripts/release/validation-stages.pl', 'full');
my @mapped = sort (@full_gate, @{$contract->{supplemental_stages}});
same_names([sort @full], \@mapped);
$source =~ /\A[0-9a-f]{40,64}\z/ && capture('git','rev-parse','HEAD') eq "$source\n"
    or die "stale release source\n";
capture('git','status','--porcelain','--untracked-files=all') eq ''
    or die "release plan requires clean committed source\n";
$baseline =~ /\A\Q$group\E-v\d+\.\d+\.\d+\z/ or die "wrong baseline group/tag\n";
my $observation = $json->decode(capture('perl', 'scripts/ci/validation-evidence.pl',
    'baseline', '.', $source, "refs/tags/$baseline"));
$observation->{available} or die "an annotated ancestor group baseline is required\n";
my ($base, $baseline_object) = @{$observation}{qw(baseline object)};
my @changed = @{$observation->{changes}};
# The CLI is a binary-only reverse consumer. Only its existing private Rust
# command/presentation/test surface may narrow. New library/build surfaces,
# manifests, dependency locks, shared tooling, generated identities, fixtures and
# any unknown path expand to the full roster. Database changes never narrow.
my $narrow = $scope eq 'auto' && cli_private($group,\@changed);
my %excluded = map { $_ => 'Only CLI-private Rust sources/tests and CLI notes changed since the group baseline; engine/schema/model/macros, manifests, features, generators, retained formats, dependencies and gate inputs are identical. CLI lint, build, process/library-consumer tests and archive verification remain required.' }
    qw(clippy check check-feature-matrix test);
my $index_inventory = capture('git','ls-files','--stage','-z');
my @inputs;
for my $entry (grep { length } split /\0/, $index_inventory) {
    my ($mode,$object,$stage,$path) = $entry =~ /\A(\d+) ([0-9a-f]+) (\d+)\t(.+)\z/s;
    defined($path) && $stage == 0 or die "unmerged/invalid source inventory\n";
    if ($mode eq '160000') {
        die "unclassified gitlink input $path\n";
    } else {
        $mode eq '100644' || $mode eq '100755' or die "redirected tracked input\n";
        read_file($path); push @inputs,$path;
    }
}
my $context = {
    schema=>1, source=>$source, group=>$group, baseline=>$base, baseline_tag=>$baseline, baseline_tag_object=>$baseline_object,
    changed=>\@changed, input_inventory=>\@inputs, index_inventory=>$index_inventory,
    qualification=>'qualification only; not admitted for release omission',
    outstanding_obligations=>[qw(macos-arm64 macos-x86_64 final-archive-provenance registry-publication independent-canic installed-recovery)],
    host=>capture('uname','-sm'), rustc=>capture('rustc','-vV'), cargo=>capture('cargo','-V'),
    # Bind the complete inherited configuration without writing secret values.
    # Shell bookkeeping changes between sibling invocations have no stage effect.
    environment=>{map {$_=>sha256_hex($ENV{$_})} grep { !/\A(?:_|SHLVL|PWD|OLDPWD)\z/ } keys %ENV},
};
my $context_path = "$output.context.json";
write_new($context_path, $context);
my $plan = {
    schema=>1, source=>$source, group=>$group, scope=>$narrow?'group':'workspace', policy=>$policy_path,
    inputs=>[sort(@inputs,$context_path,$groups_path)],
    stages=>[map { {target=>$_, required=>($narrow && exists $excluded{$_})?JSON::PP::false:JSON::PP::true,
        reason=>($narrow && exists $excluded{$_})?$excluded{$_}:'Required existing root gate or supplemental CLI obligation; separate native-host, installed and final-artifact proofs remain unqualified.'} } @full],
};
write_new($output, $plan);
print "$output\n";

# Product-owned generated inputs are immutable within a qualification attempt.
sub write_new {
    my ($path, $value) = @_;
    $path =~ m{\Atarget/[A-Za-z0-9_./-]+\z} && $path !~ m{(?:^|/)\.\.?(?:/|$)}
        or die "output must be beneath target\n";
    my $route = '.';
    for (split m{/}, $path) { $route .= "/$_"; !-l $route or die "redirected output\n"; }
    my $raw = $json->encode($value)."\n";
    length($raw) <= 1048576 or die "generated selection/context exceeds 1 MiB\n";
    sysopen my $out, $path, O_WRONLY|O_CREAT|O_EXCL or die "write new selection: $!\n";
    binmode $out;
    print {$out} $raw; close $out or die "close selection\n";
}
