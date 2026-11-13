#!/usr/bin/env perl
# Shared companions: scripts/ci/validate-release-inputs.pl
use strict;
use warnings;
use FindBin qw($RealBin);
use File::Basename qw(dirname);
use File::Spec;
use Getopt::Long qw(GetOptions);
use JSON::PP;
my ($path,$group,$version,$records);
GetOptions('plan=s'=>\$path,'group=s'=>\$group,'version=s'=>\$version,'records'=>\$records)
    && !@ARGV && defined($path) && defined($group) && defined($version)
    or die "usage: release-plan.pl --plan PATH --group GROUP --version X.Y.Z [--records]\n";
sub fail {die "release plan refused: $_[0]\n";}
sub output {
    open my $fh,'-|',@_ or fail("cannot run $_[0]: $!");
    local $/; my $result=<$fh> // '';
    close $fh or fail("invalid release field ($_[-1])");
    $result =~ s/\n\z//; return $result;
}
$group eq '' || $group =~ /\A[A-Za-z0-9][A-Za-z0-9_-]*\z/ or fail('invalid expected group');
output($^X,"$RealBin/validate-release-inputs.pl",'version',$version);
# No checkout discovery, state locks, hooks or delivery: inspect this exact file.
my $component=File::Spec->rel2abs($path);
while (1) {
    -l $component and fail('symlinked journal path');
    my $parent=dirname($component); last if $parent eq $component; $component=$parent;
}
-f $path or fail('journal is missing or not a regular file');
open my $fh,'<',$path or fail("cannot read journal: $!");
local $/; my $text=<$fh> // ''; close $fh or fail("cannot close journal: $!");
$text !~ /[\x00-\x09\x0b-\x1f\x7f]/ or fail('control character in journal');
$text =~ /\n\z/ or fail('truncated journal');
my @lines=split /\n/,$text,-1; pop @lines;
my $schema=$lines[0] // '';
my %schemas=(
    'release-plan-1'=>['direct',0], 'release-pr-plan-1'=>['pr',0],
    'release-group-plan-1'=>['direct',1], 'release-group-pr-plan-1'=>['pr',1],
);
exists($schemas{$schema}) or fail('unknown schema');
my ($delivery,$grouped)=@{$schemas{$schema}};
@lines==11+$grouped or fail('truncated journal or extra records');
my @fields=qw(schema kind previous candidate date source remote branch destination tree phase);
my %plan; @plan{@fields}=@lines[0..10];
$plan{group}=$grouped ? $lines[11] : '';
$plan{delivery}=$delivery;
(!$grouped || $plan{group} =~ /\A[A-Za-z0-9][A-Za-z0-9_-]*\z/) && $plan{group} eq $group
    or fail('group identity mismatch');
$plan{candidate} eq $version or fail('candidate identity mismatch');
output($^X,"$RealBin/validate-release-inputs.pl",'next-version',$plan{previous},$plan{kind}) eq $version
    or fail('invalid release increment');
output($^X,"$RealBin/validate-release-inputs.pl",'date',$plan{date});
for my $field (qw(source destination)) {
    $plan{$field} =~ /\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z/ or fail("invalid $field");
}
$plan{tree} eq '' || $plan{tree} =~ /\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z/ or fail('invalid index tree');
$plan{remote} =~ /\A[A-Za-z0-9._-]+\z/ or fail('invalid remote');
length($plan{branch}) or fail('empty branch');
output('git','check-ref-format',"refs/heads/$plan{branch}");
my %phases=map {$_=>1} qw(preflight validate prepare stage commit complete);
$phases{$_}=1 for $delivery eq 'direct' ? qw(tag push) : qw(pr-publish pr-review pr-validate pr-tag pr-push);
$phases{$plan{phase}} or fail('phase conflicts with schema/delivery');
# --records is the runner's internal transport; the public interface is JSON.
if ($records) {print join("\n",@plan{@fields},$plan{group},$delivery),"\n";}
else {print JSON::PP->new->canonical->encode(\%plan),"\n";}
