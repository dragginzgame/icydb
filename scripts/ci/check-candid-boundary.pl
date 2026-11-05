#!/usr/bin/env perl
# Shared companions: scripts/ci/RustSource.pm docs/candid-boundary.md
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

@ARGV >= 3 or die "usage: check-candid-boundary.pl ROOT POLICY.json SOURCE...\n";
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
keys_exact($policy, qw(schema allowances));
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
    @$tokens <= 256 && $signature !~ /[{};]/ &&
        ($signature =~ /\b(?:struct|enum|union|trait|type|fn) (?:r#)?[_\p{XID_Start}][\p{XID_Continue}]*/ ||
         $signature =~ /\bimpl\b.+\bfor (?:r#)?[_\p{XID_Start}][\p{XID_Continue}]*/) &&
        $signature !~ /\b(?:mod|macro_rules)\b/
        or die "allowance must be a bounded declaration prefix without a body\n";
    push @{$allow{$entry->{path}}}, [$tokens, $entry];
}
my @errors;
for my $file (sort keys %files) {
    my ($t, $lines) = lex(read_text($files{$file}));
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
        my ($needle,$entry)=@$allowance;
        my @matches;
        for (my $i=0; $i+@$needle<=@$t; $i++) {
            my $same=1;
            for my $j (0..$#$needle) { if ($t->[$i+$j] ne $needle->[$j]) { $same=0; last; } }
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
print "Candid source boundary passed (",scalar(keys %files)," Rust files; lexical, not macro expansion/type resolution).\n";
