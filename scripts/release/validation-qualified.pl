#!/usr/bin/env perl
# A reviewed mapping qualifies only its recorded native host and configuration.
use strict;
use warnings;
use JSON::PP;
use FindBin;
use lib $FindBin::Bin;
use ValidationScope qw(exclusions);
my $json=JSON::PP->new->canonical->utf8;
@ARGV>=1 && @ARGV<=2 or die "usage: validation-qualified.pl SOURCE [GROUP]\n";
my $group=$ARGV[1]//'cli';
my $excluded=exclusions($group);
sub capture {
    open my $in,'-|',@_ or die "observe qualification: $!\n";
    binmode $in;
    my $raw='';
    while (1) {
        my $n=read($in,my $part,65536);
        defined($n) or die "qualification read failed\n";
        last unless $n;
        $raw.=$part; length($raw)<=1048576 or die "qualification exceeds 1 MiB\n";
    }
    close $in or die "qualification observation failed\n"; return $raw;
}
my $fingerprint=$json->decode(capture('perl','scripts/release/validation-fingerprint.pl',$ARGV[0]));
my $raw=capture('git','show',"$ARGV[0]:ci/release-validation-qualified.json");
my $admission=$json->decode($raw);
$raw eq $json->encode($admission)."\n" && $admission->{schema}==1
    or die "invalid reviewed qualification\n";
my ($host,$rustc,$cargo)=(capture('uname','-sm'),capture('rustc','-vV'),capture('cargo','-V'));
for my $record (@{$admission->{comparisons}}) {
    next unless $record->{host} eq $host && $record->{rustc} eq $rustc && $record->{cargo} eq $cargo;
    next unless $record->{group} eq $group && $record->{mapping_sha256} eq $fingerprint->{mapping_sha256};
    $record->{contract} eq 'icydb-release-mapping-comparison/1' && $record->{group} eq $group
        && !$record->{release_omission_admitted} && $record->{full}{executed}==16
        && $record->{selected}{executed}==11 && !$record->{full}{reused} && !$record->{selected}{reused}
        or die "invalid native mapping qualification\n";
    $json->encode([sort map {$_->{target}} @{$record->{excluded}}])
        eq $json->encode([sort @$excluded])
        or die "different qualified omissions\n";
    next unless $record->{mapping_sha256} eq $fingerprint->{mapping_sha256};
    $json->encode($record->{mapping_inputs}) eq $json->encode($fingerprint->{mapping_inputs})
        && $json->encode($record->{configuration}) eq $json->encode($fingerprint->{configuration})
        or die "mapping identity drift\n";
    print $json->encode({source=>$ARGV[0],group=>$group,mapping_sha256=>$fingerprint->{mapping_sha256},host=>$host}),"\n";
    exit 0;
}
die "no matching native mapping qualification; use full validation\n";
