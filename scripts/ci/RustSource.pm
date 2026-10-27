package RustSource;
use strict;
use warnings;
use Exporter 'import';
our @EXPORT_OK = qw(code_mask tokens has_module_path_attribute);

# Preserve offsets/newlines in decoded source while excluding literals and
# comments. Documentation is checked against the original, unmodified source.
sub code_mask {
    my ($source) = @_;
    my $mask = $source;
    pos($source) = 0;
    while ($source =~ m{(//[^\n]*|/\*|(?:br|r)(\#*)"|b?"(?:\\.|[^"\\])*"|b?'(?:\\.|[^'\\])')}gcs) {
        my ($token, $hashes, $start, $end) = ($1, $2, $-[0], $+[0]);
        if ($token eq '/*') {
            my $depth = 1;
            while ($depth) {
                my $open = index($source, '/*', $end);
                my $close = index($source, '*/', $end);
                $close >= 0 or die "error: unterminated Rust block comment\n";
                if ($open >= 0 && $open < $close) { $depth++; $end = $open + 2; }
                else { $depth--; $end = $close + 2; }
            }
        } elsif (defined $hashes) {
            my $close = index($source, '"' . $hashes, $end);
            $close >= 0 or die "error: unterminated Rust raw string\n";
            $end = $close + 1 + length($hashes);
        }
        my $length = $end - $start;
        my $hidden = substr($source, $start, $length);
        $hidden =~ s/[^\n]/ /g;
        substr($mask, $start, $length) = $hidden;
        pos($source) = $end;
    }
    return $mask;
}

sub tokens {
    my $mask = code_mask($_[0]);
    return $mask =~ /(?:r\#)?[_\p{XID_Start}][\p{XID_Continue}]*|[^\s]/sg;
}

# Inspect attribute token trees only. Comments/literals cannot introduce path
# metadata or delimiters, and cfg_attr's predicate is not a module attribute.
# Unsupported or incomplete delimiters refuse rather than certify an inventory.
my %closing = ('(' => ')', '[' => ']', '{' => '}');
sub has_module_path_attribute {
    my @tokens = tokens($_[0]);
    for (my $index = 0; $index < @tokens; $index++) {
        next unless $tokens[$index] eq '#';
        my $start = $index + 1;
        $start++ if ($tokens[$start] // '') eq '!';
        next unless ($tokens[$start] // '') eq '[';
        my @expected = (']');
        my @body;
        my $cursor = $start + 1;
        for (; $cursor < @tokens; $cursor++) {
            my $token = $tokens[$cursor];
            if (exists $closing{$token}) { push @expected, $closing{$token}; }
            elsif ($token =~ /\A[)\]}]\z/) {
                @expected && pop(@expected) eq $token or die "error: ambiguous Rust attribute\n";
                last unless @expected;
            }
            push @body, $token;
        }
        @expected and die "error: unterminated Rust attribute\n";
        return 1 if _module_path_meta(\@body, 0);
        $index = $cursor;
    }
    return 0;
}

sub _module_path_meta {
    my ($meta, $depth) = @_;
    # Fall back before Perl's deep-recursion warning threshold.
    $depth < 64 or die "error: deeply nested Rust attributes\n";
    my $name = $meta->[0] // '';
    $name =~ s/\Ar#//;
    return 1 if $name eq 'path';
    return 0 unless $name eq 'cfg_attr';
    @$meta >= 3 && $meta->[1] eq '(' && $meta->[-1] eq ')'
        or die "error: unsupported conditional Rust attribute\n";
    my @arguments = ([]);
    my @expected;
    for my $token (@$meta[2 .. $#$meta - 1]) {
        if (exists $closing{$token}) { push @expected, $closing{$token}; }
        elsif ($token =~ /\A[)\]}]\z/) {
            @expected && pop(@expected) eq $token or die "error: ambiguous Rust attribute\n";
        }
        elsif ($token eq ',' && !@expected) { push @arguments, []; next; }
        push @{$arguments[-1]}, $token;
    }
    !@expected && @arguments >= 2 or die "error: incomplete conditional Rust attribute\n";
    shift @arguments;
    for my $argument (@arguments) {
        return 1 if _module_path_meta($argument, $depth + 1);
    }
    return 0;
}
1;
