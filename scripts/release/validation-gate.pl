#!/usr/bin/env perl
# Product release authority and durable byte custody; Shared verifies run data.
use strict;
use warnings;
use JSON::PP;
use Digest::SHA qw(sha256_hex);
use File::Copy qw(copy);
use File::Path qw(make_path);
use File::Basename qw(dirname);
use Cwd qw(abs_path);
use FindBin;
use lib $FindBin::Bin;
use ValidationScope qw(cli_private database_product exclusions);
use lib "$FindBin::Bin/../ci";
use ValidationPlan ();
my $json=JSON::PP->new->canonical->utf8;
my $root=abs_path('.');
my $group=$ENV{RELEASE_GROUP}//'cli';
my $expected_exclusions=exclusions($group);
my ($operation,$directory,$argument)=@ARGV;
@ARGV==3 && $operation =~ /\A(?:capture|check)\z/
    or die "usage: validation-gate.pl capture DIRECTORY ATTEMPT | check DIRECTORY SOURCE\n";
sub command {
    open my $in,'-|',@_ or die "observe gate: $!\n";
    binmode $in;
    my $raw='';
    while (my $n=read($in,my $part,65536)) {
        $raw.=$part; length($raw)<=8388608 or die "gate observation exceeds 8 MiB\n";
    }
    eof($in) or die "gate read failed\n";
    close $in or die "gate observation failed\n"; return $raw;
}
sub regular {
    my ($path)=@_;
    $path !~ m{[\x00-\x1f]|(?:^|/)\.\.(?:/|$)} or die "unsafe gate path\n";
    my $route=$path =~ m{^/}?'/':'.';
    for (grep {length} split m{/},$path) { $route.="/$_"; !-l $route or die "redirected gate\n"; }
    -f $path or die "missing gate file\n";
    open my $in,'<:raw',$path or die "read gate: $!\n";
    my $raw;
    defined(read($in,$raw,1048577)) && length($raw)<=1048576 && eof($in)
        or die "gate document exceeds 1 MiB\n";
    return $raw;
}
sub document { $json->decode(regular($_[0])) }
sub save {
    my ($from,$to)=@_;
    regular_route($from);
    make_path(dirname($to));
    !-e $to && !-l $to && copy($from,$to) or die "retain gate bytes: $!\n";
}
sub regular_route {
    my ($path)=@_;
    $path !~ m{^/|[\x00-\x1f]|(?:^|/)\.\.?(?:/|$)|//} or die "unsafe retained input\n";
    my $route='.';
    for (split m{/},$path) { $route.="/$_"; !-l $route or die "redirected retained input\n"; }
    -f $path or die "missing retained input\n";
}
my $gate;
if ($operation eq 'capture') {
    $argument =~ m{\Atarget/release-validation/qualification\.[A-Za-z0-9]+\z}
        or die "unexpected gate attempt\n";
    my $receipt=document("$argument/receipt.json");
    my $proof_path="$argument/evidence.json";
    my $run=$receipt->{evidence}{run};
    command('perl','scripts/release/validation-receipt.pl',"$argument/plan.json",$run,$proof_path)
        eq regular("$argument/receipt.json") or die "gate receipt readmission differs\n";
    my $admission=$json->decode(command('perl','scripts/release/validation-qualified.pl',$receipt->{source},$group));
    $receipt->{group} eq $group && $receipt->{scope} eq 'group' or die "only qualified group gates may narrow\n";
    my $proof=document($proof_path);
    my $view="$directory/view";
    !-e $view && !-l $view or die "gate view already exists\n";
    make_path($view);
    for my $path (sort keys %{$proof->{inputs}}) { save($path,"$view/$path"); }
    for my $name (sort keys %{$proof->{files}}) { save("$run/$name","$view/$run/$name"); }
    for my $path ($proof_path,"$argument/receipt.json") { save($path,"$view/$path"); }
    my $logs={map { ("$run/$_" => $proof->{files}{$_}) } keys %{$proof->{files}}};
    open my $logs_out,'>:raw',"$directory/logs.json" or die "write retained log inventory: $!\n";
    print {$logs_out} $json->encode($logs),"\n"; close $logs_out or die "close log inventory\n";
    $gate={schema=>1,source=>$receipt->{source},group=>$group,mode=>'selected',
        plan=>"$argument/plan.json",context=>"$argument/plan.json.context.json",run=>$run,
        proof=>$proof_path,receipt=>"$argument/receipt.json",qualification=>$admission,
        logs_sha256=>sha256_hex(regular("$directory/logs.json")),
        proof_sha256=>sha256_hex(regular($proof_path)),
        receipt_sha256=>sha256_hex(regular("$argument/receipt.json"))};
    open my $out,'>:raw',"$directory/gate.json" or die "write gate: $!\n";
    print {$out} $json->encode($gate),"\n"; close $out or die "close gate\n";
    $argument=$receipt->{source};
} else {
    $gate=document("$directory/gate.json");
}
$gate->{schema}==1 && $gate->{source} eq $argument && $gate->{group} eq $group
    && $gate->{mode} eq 'selected' or die "wrong source/group gate\n";
my $view=abs_path("$directory/view")//die "missing retained view\n";
my $plan=document("$view/$gate->{plan}");
my $context=document("$view/$gate->{context}");
my $receipt=document("$view/$gate->{receipt}");
sha256_hex(regular("$view/$gate->{proof}")) eq $gate->{proof_sha256}
    && sha256_hex(regular("$view/$gate->{receipt}")) eq $gate->{receipt_sha256}
    or die "retained gate bytes differ\n";
my $proof=document("$view/$gate->{proof}");
# Shared admitted the real run at its original physical location above. Its
# absolute event paths are preserved verbatim. Recovery checks sealed bytes,
# not a rewritten or newly parsed event/timing record at a different root.
ValidationPlan::plan($view,$gate->{plan},$argument,$group);
sha256_hex(regular("$directory/logs.json")) eq $gate->{logs_sha256}
    && $json->encode($receipt->{evidence}) eq $json->encode($proof)
    or die "retained Shared admission differs\n";
command('perl',"$root/scripts/ci/validation-input-checkpoint.pl",$view,
    "$view/$gate->{run}/plan-inputs.json",'/dev/stdout',"$root/scripts/ci/verify-file-checksum.sh");
command('perl',"$root/scripts/ci/validation-input-checkpoint.pl",$view,
    "$directory/logs.json",'/dev/stdout',"$root/scripts/ci/verify-file-checksum.sh");
$receipt->{source} eq $argument && $receipt->{group} eq $group && $receipt->{scope} eq 'group'
    && $receipt->{counts}{executed}==scalar(grep {$_->{required}} @{$plan->{stages}}) && $receipt->{counts}{reused}==0
    && $receipt->{evidence_sha256} eq $gate->{proof_sha256}
    or die "wrong retained consumer receipt\n";
my $baseline=$json->decode(command('perl',"$root/scripts/ci/validation-evidence.pl",'baseline',
    $root,$argument,"refs/tags/$context->{baseline_tag}"));
$baseline->{available} && $baseline->{baseline} eq $context->{baseline}
    && $baseline->{object} eq $context->{baseline_tag_object}
    && $json->encode($baseline->{changes}) eq $json->encode($context->{changed})
    or die "retained baseline no longer admitted\n";
my ($source_metadata,$baseline_metadata);
my $metadata_equal=sub {
    my ($path)=@_;
    return 0 unless $path eq 'Cargo.lock' || $path eq 'ci/dependency-pinning-exceptions.json'
        || $path =~ m{(?:\A|/)Cargo\.toml\z};
    $source_metadata //= $json->decode(command('perl',"$root/scripts/release/validation-fingerprint.pl",$argument))->{cargo_inputs};
    $baseline_metadata //= $json->decode(command('perl',"$root/scripts/release/validation-fingerprint.pl",$baseline->{baseline}))->{cargo_inputs};
    return exists($source_metadata->{$path}) && exists($baseline_metadata->{$path})
        && $source_metadata->{$path} eq $baseline_metadata->{$path};
};
my %modified=map {$_=>1} split /\n/, command('git','diff','--no-renames','--diff-filter=M','--name-only',$baseline->{baseline},$argument,'--');
(cli_private($group,$baseline->{changes}) || database_product($group,$baseline->{changes},$metadata_equal,sub {$modified{$_[0]}}))
    or die "retained group scope no longer admitted\n";
my @excluded=sort map {$_->{target}} grep {!$_->{required}} @{$plan->{stages}};
$json->encode(\@excluded) eq $json->encode([sort @$expected_exclusions])
    or die "different retained omissions\n";
my $cert=document("$view/ci/release-validation-qualified.json");
grep($_->{group} eq $group && $_->{mapping_sha256} eq $gate->{qualification}{mapping_sha256}
    && $_->{host} eq $receipt->{host} && $_->{rustc} eq $receipt->{rustc}
    && $_->{cargo} eq $receipt->{cargo}, @{$cert->{comparisons}})
    or die "missing retained native qualification\n";
print $json->encode({source=>$argument,group=>$group,mode=>'selected',
    gate_sha256=>sha256_hex(regular("$directory/gate.json"))}),"\n";
