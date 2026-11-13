#!/usr/bin/env perl
# Consumer qualification evidence only; historical release receipts stay unchanged.
use strict;
use warnings;
use JSON::PP;
use Digest::SHA qw(sha256_hex);
use File::Temp qw(tempdir);
use FindBin;
use lib "$FindBin::Bin/../../vendor/toko-tooling/scripts/ci";
use ValidationPlan ();
my $json=JSON::PP->new->canonical->utf8;
my ($plan_path,$run,$proof_path)=@ARGV;
@ARGV==3 or die "usage: validation-receipt.pl PLAN RUN-DIRECTORY SHARED-PROOF\n";
sub bytes {
    return ValidationPlan::bytes('.', $_[0]);
}
sub command {
    open my $fh,'-|',@_ or die "execute @_: $!\n";
    my $value='';
    while (1) {
        my $n = read $fh, my $chunk, 65536;
        defined($n) or die "cannot read observation\n";
        last unless $n;
        $value.=$chunk; length($value)<=4194304 or die "observation exceeds 4 MiB\n";
    }
    close $fh or die "failed @_; no receipt\n"; return $value;
}
for my $path ($plan_path,$run,$proof_path) {
    $path =~ m{\Atarget/[A-Za-z0-9_./-]+\z} && $path !~ m{(?:^|/)\.\.?(?:/|$)}
        or die "evidence must be beneath target\n";
    my $route='.';
    for (split m{/},$path) { $route.="/$_"; !-l $route or die "redirected evidence output\n"; }
}
my $raw=bytes($plan_path);
my $plan=$json->decode($raw);
# Shared re-admits exact plan/policy, checkpoints, event order and retained logs.
my $proof_raw=bytes($proof_path);
my $proof=$json->decode($proof_raw);
command('perl','vendor/toko-tooling/scripts/ci/validation-evidence.pl','check','.', $plan_path,
    $plan->{source},$plan->{group},$run,"$plan_path.context.json",$proof_path);
command('git','rev-parse','HEAD') eq "$plan->{source}\n" or die "stale source\n";
command('git','status','--porcelain','--untracked-files=all') eq '' or die "dirty release inputs\n";
my $context=$json->decode(bytes("$plan_path.context.json"));
command('git','ls-files','--stage','-z') eq $context->{index_inventory} or die "source inventory changed\n";
$context->{source} eq $plan->{source} && $context->{group} eq $plan->{group} or die "wrong context\n";
# Recompute the actual selection using current consumer policy. Shared admits
# plan structure; only the consumer can certify the selected obligations.
my $admission_dir=tempdir('readmission.XXXXXX',DIR=>$run,CLEANUP=>0);
my $readmitted="$admission_dir/plan.json";
command('perl','scripts/release/validation-plan.pl',$plan->{group},$plan->{source},
    $context->{baseline_tag},$readmitted,$plan->{scope} eq 'group'?'auto':'workspace');
my $selected=$json->decode(bytes($readmitted));
$selected->{inputs}=[map {$_ eq "$readmitted.context.json" ? "$plan_path.context.json" : $_ eq "$readmitted.groups.json" ? "$plan_path.groups.json" : $_} @{$selected->{inputs}}];
$selected->{inputs}=[sort @{$selected->{inputs}}];
$json->encode($selected) eq $json->encode($plan)
    && bytes("$readmitted.context.json") eq bytes("$plan_path.context.json")
    or die "consumer selection/context drift\n";
my %environment=map {$_=>sha256_hex($ENV{$_})} grep { !/\A(?:_|SHLVL|PWD|OLDPWD)\z/ } keys %ENV;
$json->encode(\%environment) eq $json->encode($context->{environment}) or die "configuration drift\n";
my ($host,$rustc,$cargo)=(command('uname','-sm'),command('rustc','-vV'),command('cargo','-V'));
$host eq $context->{host} && $rustc eq $context->{rustc} && $cargo eq $context->{cargo}
    or die "host/compiler drift\n";
my $receipt={schema=>2,contract=>'icydb-release-validation-qualification/2',source=>$plan->{source},group=>$plan->{group},
    scope=>$plan->{scope},baseline=>$context->{baseline},baseline_tag=>$context->{baseline_tag},
    baseline_tag_object=>$context->{baseline_tag_object},
    outstanding_obligations=>$context->{outstanding_obligations},
    counts=>{executed=>scalar(grep {$_->{required}} @{$plan->{stages}}),reused=>0},
    evidence_path=>$proof_path,evidence_sha256=>sha256_hex($proof_raw),evidence=>$proof,
    host=>$host,rustc=>$rustc,cargo=>$cargo,environment=>\%environment,
    release_omission_admitted=>JSON::PP::false};
# Byte custody/publication belongs to Shared's release_receipt_write caller.
my $raw_receipt=$json->encode($receipt)."\n";
length($raw_receipt)<=1048576 or die "qualification receipt exceeds 1 MiB\n";
print $raw_receipt;
