#!/usr/bin/env perl
# Shared companions: scripts/ci/RustSource.pm ci/cbor-dependencies.json
use strict;
use warnings;
use Cwd qw(abs_path);
use File::Find qw(find);
use File::Spec;
use JSON::PP;
use Encode qw(decode FB_CROAK);
use FindBin qw($RealBin);
use lib $RealBin;
use RustSource qw(code_mask);

my $cbor_mode = @ARGV && $ARGV[0] eq '--cbor' ? shift(@ARGV) : '';
@ARGV >= 3 or die "usage: check-candid-boundary.pl [--cbor] ROOT POLICY.json SOURCE...\n";
my ($root, $policy_path, @roots) = @ARGV;
$root = abs_path($root) // die "missing root\n";
sub read_text {
    open my $f, '<:raw', $_[0] or die "read $_[0]: $!\n";
    local $/; my $s = <$f>; close $f or die $!;
    return decode('UTF-8', $s, FB_CROAK);
}
sub relative_path {
    my ($path) = @_;
    defined($path) && !ref($path) && $path !~ m{\A/|(?:\A|/)\.{1,2}(?:/|\z)|//|[\x00-\x1f\\*?\[\]]} && length($path)
        or die "expected literal root-relative path\n";
    my $current = $root;
    for my $part (split '/', $path) {
        $current .= "/$part";
        -l $current and die "symlink source route: $path\n";
    }
    return $current;
}
sub lex {
    my ($source) = @_;
    my $mask = code_mask($source);
    my (@tokens, @lines);
    my ($line, $last) = (1, 0);
    while ($mask =~ /(?:r\#)?[_\p{XID_Start}][\p{XID_Continue}]*|[^\s]/sg) {
        $line += substr($mask, $last, $-[0]-$last) =~ tr/\n//;
        push @tokens, $&; push @lines, $line; $last = $+[0];
    }
    return (\@tokens, \@lines);
}
sub keys_exact {
    my ($value, @keys) = @_;
    ref($value) eq 'HASH' && join(',', sort keys %$value) eq join(',', sort @keys)
        or die "invalid policy fields\n";
}
my $policy = JSON::PP->new->decode(read_text(relative_path($policy_path)));
keys_exact($policy, qw(schema allowances), (exists($policy->{cbor_allowances}) ? 'cbor_allowances' : ()));
exists($policy->{cbor_allowances}) && !$cbor_mode and die "CBOR policy requires --cbor\n";
my (%cbor_names, %cbor_allow);
if ($cbor_mode) {
    my $inventory = JSON::PP->new->decode(read_text("$RealBin/../../ci/cbor-dependencies.json"));
    ref($inventory->{rust}) eq 'ARRAY' && @{$inventory->{rust}} or die "invalid CBOR inventory\n";
    for my $name (@{$inventory->{rust}}) {
        $name =~ /\A[A-Za-z0-9_-]+\z/ or die "invalid CBOR crate name\n";
        $name =~ tr/-/_/;
        !$cbor_names{$name}++ or die "duplicate CBOR crate name\n";
    }
}
$policy->{schema} == 1 && ref($policy->{allowances}) eq 'ARRAY' or die "invalid policy schema\n";
my %files;
for my $selection (@roots) {
    my $path = relative_path($selection);
    -e $path or die "missing source: $selection\n";
    my $count = 0;
    local $SIG{__WARN__} = sub { die "source traversal failed: @_" };
    find({no_chdir => 1, wanted => sub {
        my $name = $_;
        if (-d $name && $name =~ m{/(?:target|\.git|\.tools|node_modules)\z}) { $File::Find::prune=1; return; }
        -l $name and die "symlink in selected sources: $name\n";
        return unless $name =~ /\.rs\z/;
        -f $name or die "nonregular Rust source: $name\n";
        $files{File::Spec->abs2rel($name, $root)} = $name; $count++;
    }}, $path);
    $count or die "no Rust sources selected: $selection\n";
}
if ($cbor_mode) {
    my $entries = $policy->{cbor_allowances} // [];
    ref($entries) eq 'ARRAY' or die "invalid CBOR allowances\n";
    for my $entry (@$entries) {
        keys_exact($entry, qw(path crate reason review));
        exists($files{$entry->{path}}) or die "CBOR allowance outside selection\n";
        $cbor_names{$entry->{crate}} or die "unknown CBOR allowance crate\n";
        for (qw(reason review)) {
            defined($entry->{$_}) && !ref($entry->{$_}) && $entry->{$_} =~ /\S/
                or die "empty CBOR allowance $_\n";
        }
        !$cbor_allow{$entry->{path}}{$entry->{crate}}++ or die "duplicate CBOR allowance\n";
    }
}
my %allow;
for my $entry (@{$policy->{allowances}}) {
    keys_exact($entry, qw(path kind protocol reason declaration));
    exists $files{$entry->{path}} or die "allowance outside selection or missing: $entry->{path}\n";
    $entry->{kind} =~ /\A(?:external-protocol|codec-comparison)\z/ or die "invalid allowance kind\n";
    for (qw(protocol reason declaration)) {
        defined($entry->{$_}) && !ref($entry->{$_}) && $entry->{$_} =~ /\S/ or die "empty allowance $_\n";
    }
    my ($tokens) = lex($entry->{declaration});
    my $signature = join ' ', @$tokens;
    # Review a declaration prefix, never an entire module/body or bare trait name.
    # An inherent implementation must include its complete named header; matching
    # also requires the opening body brace, so a trait-name prefix cannot admit it.
    my $inherent = $signature =~ /\bimpl (?:<.+> )?(?:r#)?[_\p{XID_Start}][\p{XID_Continue}]*/
        && $signature !~ /\bfor\b/;
    @$tokens <= 256 && $signature !~ /[{};]/ &&
        ($signature =~ /\b(?:struct|enum|union|trait|type|fn) (?:r#)?[_\p{XID_Start}][\p{XID_Continue}]*/ ||
         $signature =~ /\bimpl\b.+\bfor (?:r#)?[_\p{XID_Start}][\p{XID_Continue}]*/ || $inherent) &&
        $signature !~ /\b(?:mod|macro_rules)\b/
        or die "allowance must be a bounded declaration prefix without a body\n";
    push @{$allow{$entry->{path}}}, [$tokens, $entry, $inherent];
}
my @errors;
for my $file (sort keys %files) {
    my ($t, $lines) = lex(read_text($files{$file}));
    if ($cbor_mode) {
        my %seen;
        for my $i (0..$#$t) {
            my $name = $t->[$i]; $name =~ s/^r#//;
            next unless $cbor_names{$name};
            $seen{$name} = 1;
            push @errors, "$file:$lines->[$i]: direct CBOR crate reference '$name'; use typed Platform APIs"
                unless $cbor_allow{$file}{$name};
        }
        for my $name (keys %{$cbor_allow{$file}//{}}) {
            $seen{$name} or die "$file: stale CBOR allowance: $name\n";
        }
    }
    my (%imports, %names); $names{CandidType}=1;
    # Imports are not implementations. Resolve explicit same-file aliases,
    # conservatively across scopes; qualify all cfg branches without compiling.
    my @uses;
    for (my $i=0; $i<@$t; $i++) {
        next unless $t->[$i] eq 'use';
        my $end=$i; $end++ while $end<@$t && $t->[$end] ne ';';
        $end<@$t or die "$file:$lines->[$i]: incomplete use statement\n";
        push @uses, [$i,$end]; $imports{$_}=1 for $i..$end; $i=$end;
    }
    my $changed=1;
    while ($changed) {
        $changed=0;
        for my $use (@uses) {
            for my $i ($use->[0]..$use->[1]-2) {
                my $name=$t->[$i]; $name =~ s/^r#//;
                next unless $names{$name} && $t->[$i+1] eq 'as';
                my $alias=$t->[$i+2]; $alias =~ s/^r#//;
                if (!$names{$alias}) { $names{$alias}=1; $changed=1; }
            }
        }
    }
    my %hits;
    for my $i (0..$#$t) {
        my $name=$t->[$i]; $name =~ s/^r#//;
        $hits{$i}=1 if $names{$name} && !$imports{$i};
    }
    my %covered;
    for my $allowance (@{$allow{$file}//[]}) {
        my ($needle,$entry,$inherent)=@$allowance;
        my @matches;
        for (my $i=0; $i+@$needle<=@$t; $i++) {
            my $same=1;
            for my $j (0..$#$needle) { if ($t->[$i+$j] ne $needle->[$j]) { $same=0; last; } }
            next if $inherent && ($t->[$i+@$needle]//'') ne '{';
            push @matches,$i if $same;
        }
        @matches == 1 or die "$file: stale or ambiguous declaration allowance: $entry->{declaration}\n";
        my @admitted = grep {$hits{$_}} $matches[0]..$matches[0]+$#$needle;
        @admitted or die "$file: unused Candid allowance\n";
        for (@admitted) { !$covered{$_}++ or die "$file: overlapping Candid allowances\n"; }
    }
    for my $i (sort {$a<=>$b} keys %hits) {
        push @errors,"$file:$lines->[$i]: Candid trait reference '$t->[$i]' outside a reviewed external declaration" unless $covered{$i};
    }
}
if (@errors) { print STDERR "$_\n" for @errors; exit 1; }
print +($cbor_mode ? "Candid/CBOR source boundary passed (" : "Candid source boundary passed (");
print scalar(keys %files)," Rust files; lexical, not macro expansion/type resolution).\n";
