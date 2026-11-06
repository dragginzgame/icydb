#!/usr/bin/env perl
# Compare admitted consumer receipts; Shared owns raw execution/log verification.
use strict;
use warnings;
use JSON::PP;
use Digest::SHA qw(sha256_hex);
use FindBin;
use lib $FindBin::Bin;
use ValidationScope qw(exclusions);
my $json = JSON::PP->new->canonical->utf8;
@ARGV == 2 or die "usage: compare-validation-plans.pl FULL-ATTEMPT SELECTED-ATTEMPT\n";
sub bytes {
    my ($path) = @_;
    $path =~ m{\Atarget/[A-Za-z0-9_./-]+\z} && $path !~ m{(?:^|/)\.\.?(?:/|$)}
        or die "comparison evidence must be beneath target\n";
    my $route = '.';
    for (split m{/}, $path) { $route .= "/$_"; !-l $route or die "redirected comparison evidence\n"; }
    -f $path or die "missing comparison evidence\n";
    open my $in, '<:raw', $path or die "read evidence: $!\n";
    local $/; my $raw = <$in>;
    length($raw) <= 1048576 or die "comparison evidence exceeds 1 MiB\n";
    return $raw;
}
my (@receipts, @plans, @identities);
for my $attempt (@ARGV) {
    my $raw = bytes("$attempt/receipt.json");
    my $receipt = $json->decode($raw);
    # Reuse the consumer's complete readmission, including its saved Shared proof.
    open my $verified, '-|', 'perl', 'scripts/release/validation-receipt.pl',
        "$attempt/plan.json", $receipt->{evidence}{run}, "$attempt/evidence.json"
        or die "start receipt admission: $!\n";
    local $/; my $observed = <$verified>;
    close $verified or die "comparison receipt admission failed\n";
    $observed eq $raw or die "comparison receipt bytes changed\n";
    $receipt->{group} =~ /\A(?:cli|database)\z/ && !$receipt->{release_omission_admitted}
        && $receipt->{counts}{reused} == 0 or die "unexpected comparison receipt\n";
    push @receipts, $receipt;
    push @plans, $json->decode(bytes("$attempt/plan.json"));
    push @identities, {receipt=>"$attempt/receipt.json", sha256=>sha256_hex($raw),
        executed=>$receipt->{counts}{executed}, reused=>$receipt->{counts}{reused}};
}
for my $field (qw(source group baseline baseline_tag baseline_tag_object host rustc cargo environment)) {
    $json->encode($receipts[0]{$field}) eq $json->encode($receipts[1]{$field})
        or die "unmatched comparison $field\n";
}
$plans[0]{scope} eq 'workspace' && $plans[1]{scope} eq 'group'
    && !grep(!$_->{required}, @{$plans[0]{stages}})
    or die "comparison requires complete full and selected group plans\n";
my @excluded = grep { !$_->{required} } @{$plans[1]{stages}};
my $expected=exclusions($receipts[0]{group});
$json->encode([sort map {$_->{target}} @excluded]) eq $json->encode([sort @$expected])
    && $identities[0]{executed} - $identities[1]{executed} == @$expected
    or die "unexpected group exclusion mapping\n";
open my $fingerprint, '-|', 'perl', 'scripts/release/validation-fingerprint.pl', $receipts[0]{source}
    or die "observe normalized mapping: $!\n";
my $fingerprint_raw=do {local $/; <$fingerprint>};
close $fingerprint or die "mapping observation failed\n";
my $mapping=$json->decode($fingerprint_raw);
print $json->encode({schema=>1, contract=>'icydb-release-mapping-comparison/1',
    source=>$receipts[0]{source}, group=>$receipts[0]{group}, baseline=>$receipts[0]{baseline}, host=>$receipts[0]{host},
    %$mapping,
    rustc=>$receipts[0]{rustc}, cargo=>$receipts[0]{cargo},
    full=>$identities[0], selected=>$identities[1],
    excluded=>[map {{target=>$_->{target}, reason=>$_->{reason}}} @excluded],
    release_omission_admitted=>JSON::PP::false}), "\n";
