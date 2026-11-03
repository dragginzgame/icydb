#!/usr/bin/env perl
# Consumer-owned selection only; execution/checkpointing belongs to Shared.
use strict;
use warnings;
use JSON::PP;
use Digest::SHA qw(sha256_hex);
my $json = JSON::PP->new->canonical->utf8;
my ($group, $source, $baseline, $output, $scope) = @ARGV;
@ARGV == 5 && $scope =~ /\A(?:auto|workspace)\z/
    or die "usage: validation-plan.pl GROUP SOURCE GROUP-TAG OUTPUT auto|workspace\n";
sub capture {
    open my $fh, '-|', @_ or die "start @_: $!\n";
    local $/; my $value = <$fh> // '';
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
my $policy_raw = read_file($policy_path);
my $policy = $json->decode($policy_raw);
$policy_raw eq $json->encode($policy)."\n" or die "noncanonical policy\n";
my $groups = $json->decode(read_file('ci/cargo-release-groups.json'));
my @groups = sort map { $_->{name} } @{$groups->{groups}};
same_names($policy->{groups}, \@groups);
grep($_ eq $group, @groups) or die "unknown group\n";
# Pin the reviewed complete recipes, not just their names. A new/removed gate,
# changed aggregation, release dispatch or group policy requires explicit review.
my $contract_raw = read_file('ci/release-validation-contract.json');
my $contract = $json->decode($contract_raw);
$contract->{schema} == 1 && $contract_raw eq $json->encode($contract)."\n"
    or die "invalid consumer mapping contract\n";
for my $path ('Makefile', 'ci/cargo-release-groups.json') {
    sha256_hex(read_file($path)) eq ($contract->{files}{$path}//'')
        or die "release recipes/group policy changed; requalify mapping\n";
}
my @full = @{$policy->{stages}};
my @mapped = sort (@{$contract->{full_stages}}, @{$contract->{supplemental_stages}});
same_names([sort @full], \@mapped);
$source =~ /\A[0-9a-f]{40,64}\z/ && capture('git','rev-parse','HEAD') eq "$source\n"
    or die "stale release source\n";
capture('git','status','--porcelain','--untracked-files=all') eq ''
    or die "release plan requires clean committed source\n";
$baseline =~ /\A\Q$group\E-v\d+\.\d+\.\d+\z/ or die "wrong baseline group/tag\n";
capture('git','cat-file','-t',"refs/tags/$baseline") eq "tag\n"
    or die "baseline must be an annotated group tag\n";
my $baseline_object = capture('git','rev-parse',"refs/tags/$baseline"); chomp $baseline_object;
my $base = capture('git','rev-parse','--verify',"refs/tags/$baseline^{commit}"); chomp $base;
system('git','merge-base','--is-ancestor',$base,$source) == 0 or die "baseline is not ancestor\n";
my @changed = grep { length } split /\0/, capture('git','diff','--name-only','--no-renames','-z',$base,$source,'--');
# The CLI is a binary-only reverse consumer. Only its existing private Rust
# command/presentation/test surface may narrow. New library/build surfaces,
# manifests, dependency locks, shared tooling, generated identities, fixtures and
# any unknown path expand to the full roster. Database changes never narrow.
my $narrow = @changed && $scope eq 'auto' && $group eq 'cli';
for my $path (@changed) {
    next if $path =~ m{\Acrates/icydb-cli/(?:src|tests)/[^\x00-\x1f]+\.rs\z}
        && $path ne 'crates/icydb-cli/src/lib.rs';
    next if $path eq 'releases/cli/CHANGELOG.md' || $path =~ m{\Areleases/cli/\d+\.\d+\.md\z};
    $narrow = 0;
}
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
for my $path ($output,$context_path) {
    $path =~ m{\Atarget/[A-Za-z0-9_./-]+\z} && $path !~ m{(?:^|/)\.\.?/} or die "output must be beneath target\n";
    !-e $path && !-l $path or die "never overwrite a plan/receipt\n";
    my $route = '.';
    for (split m{/}, $path) { $route .= "/$_"; !-l $route or die "redirected output\n"; }
}
open my $ctx, '>:raw', $context_path or die "write context: $!\n";
print {$ctx} $json->encode($context),"\n"; close $ctx or die "close context\n";
my $plan = {
    schema=>1, source=>$source, group=>$group, scope=>$narrow?'group':'workspace', policy=>$policy_path,
    inputs=>[sort(@inputs,$context_path)],
    stages=>[map { {target=>$_, required=>($narrow && exists $excluded{$_})?JSON::PP::false:JSON::PP::true,
        reason=>($narrow && exists $excluded{$_})?$excluded{$_}:'Required existing root gate or supplemental CLI obligation; separate native-host, installed and final-artifact proofs remain unqualified.'} } @full],
};
open my $out,'>:raw',$output or die "write plan: $!\n";
print {$out} $json->encode($plan),"\n"; close $out or die "close plan\n";
print "$output\n";
