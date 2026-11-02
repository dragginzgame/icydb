#!/usr/bin/env perl
# Private companion of run-validation-targets.sh; dispatch remains with that runner.
use strict;
use warnings;
use JSON::PP;
use Digest::SHA qw(sha256_hex);
use Fcntl qw(O_RDONLY O_NONBLOCK S_ISREG);
my ($root, $path, $source, $group, $baseline, $output) = @ARGV;
@ARGV == 6 or die "usage: release-validation-plan.pl ROOT PLAN SOURCE GROUP CHECKPOINT OUTPUT\n";
my $json = JSON::PP->new->canonical->utf8;
my %parsed;
sub record {
    my ($name) = @_;
    $name =~ m{\A[^/\x00\t\r\n][^\x00\t\r\n]*\z} && $name !~ m{(?:^|/)\.{1,2}(?:/|$)|//|/$}
        or die "invalid validation plan input path\n";
    my $route = $root;
    for my $part (split m{/}, $name) {
        $route .= "/$part";
        !-l $route or die "symlink in validation plan route\n";
    }
    sysopen my $in, $route, O_RDONLY|O_NONBLOCK or die "read plan input $name: $!\n";
    S_ISREG((stat $in)[2]) or die "plan input is not a regular file\n";
    binmode $in;
    my $raw = '';
    while (1) {
        my $n = read $in, my $chunk, 65536;
        defined $n or die "read plan input: $!\n";
        last unless $n;
        $raw .= $chunk;
        length($raw) <= 1048576 or die "validation plan input exceeds 1 MiB\n";
    }
    close $in or die "close plan input: $!\n";
    my $value = $json->decode($raw);
    $raw eq $json->encode($value)."\n" or die "plan/policy must be canonical JSON (duplicate keys refuse)\n";
    $parsed{$name} = sha256_hex($raw);
    return $value;
}
sub keys_are {
    my ($value, @keys) = @_;
    ref($value) eq 'HASH' && join(',',sort keys %$value) eq join(',',sort @keys)
        or die "unknown or missing validation plan fields\n";
}
sub names {
    my ($value) = @_;
    ref($value) eq 'ARRAY' && @$value or die "empty or invalid validation roster\n";
    my %seen;
    for (@$value) {
        defined($_) && !ref($_) && /\A[A-Za-z0-9][A-Za-z0-9_.-]*\z/ && !$seen{$_}++
            or die "invalid or duplicate validation roster name\n";
    }
    return %seen;
}
$source =~ /\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z/ && $group =~ /\A[A-Za-z0-9][A-Za-z0-9_-]*\z/
    or die "plan requires admitted RELEASE_SOURCE and RELEASE_GROUP\n";
my $plan = record($path);
keys_are($plan, qw(schema source group scope policy inputs stages));
$json->encode($plan->{schema}) eq '1' && $plan->{source} eq $source && $plan->{group} eq $group
    && $plan->{scope} =~ /\A(?:group|workspace)\z/ or die "wrong plan schema, source, group or scope\n";
my $policy = record($plan->{policy});
keys_are($policy, qw(schema groups stages));
$json->encode($policy->{schema}) eq '1' or die "unknown validation policy schema\n";
my %groups = names($policy->{groups});
$groups{$group} or die "unknown release group\n";
my %stages = names($policy->{stages});
ref($plan->{inputs}) eq 'ARRAY' && @{$plan->{inputs}} or die "plan requires selection/configuration inputs\n";
my %inputs;
for ($path, $plan->{policy}, @{$plan->{inputs}}) {
    defined($_) && !ref($_) && length($_) && !/[\x00\t\r\n]/ or die "invalid plan input\n";
    $inputs{$_} = 1;
}
ref($plan->{stages}) eq 'ARRAY' or die "invalid plan stages\n";
my %seen;
my @rows;
my $required = 0;
for my $stage (@{$plan->{stages}}) {
    keys_are($stage, qw(target required reason));
    my $target = $stage->{target};
    defined($target) && !ref($target) && $stages{$target} && !$seen{$target}++
        or die "unknown or duplicate plan stage\n";
    JSON::PP::is_bool($stage->{required}) && defined($stage->{reason}) && !ref($stage->{reason})
        && $stage->{reason} =~ /\S/ && $stage->{reason} !~ /[\x00\t\r\n]/
        or die "stage requires boolean selection and single-line reason\n";
    $stage->{required} || $plan->{scope} eq 'group' or die "workspace scope cannot omit stages\n";
    $required += $stage->{required} ? 1 : 0;
    push @rows, join("\t", $stage->{required} ? 'required' : 'not-applicable', $target, $stage->{reason});
}
$required && keys(%seen) == keys(%stages) or die "incomplete validation plan\n";
open my $in, '<', $baseline or die "read initial checkpoint: $!\n";
my $expected = do { local $/; $json->decode(<$in>) };
close $in or die "close initial checkpoint: $!\n";
for my $name (keys %parsed) {
    !exists($expected->{$name}) || $expected->{$name} eq $parsed{$name}
        or die "plan input changed during selection: $name\n";
    $expected->{$name} = $parsed{$name};
}
open my $out, '>', $output or die "write plan checkpoint: $!\n";
print {$out} $json->encode($expected), "\n" or die "write plan checkpoint: $!\n";
close $out or die "close plan checkpoint: $!\n";
for my $item (['plan', $plan], ['policy', $policy]) {
    open my $snapshot, '>', "$output.$item->[0].json" or die "retain plan/policy: $!\n";
    print {$snapshot} $json->encode($item->[1]), "\n" or die "retain plan/policy: $!\n";
    close $snapshot or die "close retained plan/policy: $!\n";
}
# Emit only after every decision is admitted; no partial plan can dispatch.
print "input\t$_\n" for sort keys %inputs;
print "$_\n" for @rows;
