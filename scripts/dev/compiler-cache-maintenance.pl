#!/usr/bin/env perl
# Shared companions: scripts/dev/OwnedTree.pm
use strict;
use warnings;
use FindBin;
use lib $FindBin::Bin;
use OwnedTree qw(directory route inventory remove_owned);
use Fcntl qw(:DEFAULT :mode);
use JSON::PP;

# A product owns policy and custody; only this helper removes its admitted
# compiler subtree. Artifacts, sources and immutable receipts MUST live outside.
my ($operation,$root,$owner,$lock,$pid,$budget,$age)=@ARGV;
@ARGV==7 && $operation =~ /\A(?:prepare|finish|preview)\z/
    && $pid =~ /\A[1-9][0-9]*\z/ && $budget =~ /\A[1-9][0-9]*\z/ && $age =~ /\A[1-9][0-9]*\z/
    or die "usage: compiler-cache-maintenance.pl prepare|finish|preview ROOT OWNER LOCK PID MAX_BYTES MAX_AGE_SECONDS\n";
my $json=JSON::PP->new->canonical;
route($_) for ($root,$owner,$lock);
directory($owner,1);
my @custody;
sub custody {
    route($lock);
    my @s=lstat($lock);
    @s && S_ISREG($s[2]) && $s[4]==$< && $s[7]<64 or die "invalid compiler custody lock\n";
    open my $fh,'<',$lock or die $!; local $/; my $bytes=<$fh>; close $fh or die $!;
    $bytes eq "$pid\n" && kill(0,$pid) or die "compiler maintenance requires active matching custody\n";
    my @identity=@s[0,1,2,9,10];
    !@custody || $json->encode(\@identity) eq $json->encode(\@custody) or die "compiler custody changed\n";
    @custody=@identity;
}
sub read_record {
    my ($path)=@_; route($path); my @s=lstat($path);
    @s && S_ISREG($s[2]) && $s[4]==$< && $s[7]<4096 or die "invalid compiler cache record: $path\n";
    open my $fh,'<',$path or die $!; local $/; my $record=$json->decode(<$fh>); close $fh or die $!;
    return $record;
}
sub write_record {
    my ($path,$record)=@_; route($path);
    my $tmp="$path.$$";
    sysopen my $fh,$tmp,O_WRONLY|O_CREAT|O_EXCL,0600 or die $!;
    print {$fh} $json->encode($record),"\n" or die $!; close $fh or die $!;
    rename $tmp,$path or die $!;
}
custody();
my $identity={schema=>1,owner=>$owner,root=>$root};
if (!-e $root && !-l $root) {
    if ($operation eq 'preview') { print "{\"status\":\"absent\"}\n"; exit; }
    $operation eq 'prepare' or die "compiler cache not prepared\n";
    mkdir $root,0700 or die "create compiler cache: $!\n";
    write_record("$root/owner.json",$identity);
    write_record("$root/state.json",{schema=>1,last_used=>time});
}
directory($root);
$json->encode(read_record("$root/owner.json")) eq $json->encode($identity) or die "unowned compiler cache\n";
my $state=read_record("$root/state.json");
ref($state) eq 'HASH' && ($state->{schema}//0)==1 && ($state->{last_used}//'') =~ /\A[0-9]+\z/
    or die "invalid compiler cache age\n";
opendir my $dir,$root or die $!;
my @unknown=grep { !/\A(?:\.{1,2}|owner\.json|state\.json|build)\z/ } readdir $dir;
closedir $dir or die $!;
@unknown and die "unrecognized compiler cache contents\n";
my ($files,$bytes)=(undef,0);
if (-e "$root/build" || -l "$root/build") {
    route("$root/build"); directory("$root/build",1);
    $files=inventory("$root/build",1);
    my %seen;
    for my $s (values %$files) {
        $bytes+=($s->[7]//0)*512 if S_ISREG($s->[2]) && !$seen{"$s->[0]:$s->[1]"}++;
    }
}
my $reason=$bytes>$budget ? 'size' : $operation ne 'finish' && time-$state->{last_used}>$age ? 'age' : 'within-budget';
my $removed=JSON::PP::false;
if ($operation ne 'preview') {
    custody();
    if ($files && $reason ne 'within-budget') {
        remove_owned("$root/build",$files,1);
        $removed=JSON::PP::true;
    }
    write_record("$root/state.json",{schema=>1,last_used=>time});
}
print $json->encode({schema=>1,root=>$root,observed_physical_bytes=>$bytes,
    max_bytes=>0+$budget,max_age_seconds=>0+$age,reason=>$reason,removed=>$removed}),"\n";
