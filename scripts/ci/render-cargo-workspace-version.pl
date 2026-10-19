#!/usr/bin/env perl
# Shared companions: scripts/ci/StableVersion.pm
use strict;
use warnings;
use FindBin qw($RealBin);
use lib $RealBin;
use StableVersion qw(stable);
use JSON::PP qw(decode_json);
use Cwd qw(abs_path);
use File::Basename qw(dirname);
use Encode qw(encode_utf8);

# Recorded identities are lexical absolute paths, not observations of whichever
# filesystem happens to exist when a saved manifest is projected.
sub identity_path {
    my ($path) = @_;
    defined($path) && !ref($path) && $path =~ m{\A/} && $path !~ /[\x00\r\n]/
        or die "expected an absolute saved-source identity path\n";
    my @parts;
    for my $part (split m{/}, $path) {
        next if $part eq '' || $part eq '.';
        if ($part eq '..') {
            @parts or die "saved-source identity escapes filesystem root\n";
            pop @parts;
        } else { push @parts, $part; }
    }
    return '/' . join('/', @parts);
}

# Pure literal rendering, not TOML admission or a release transaction. Callers
# admit the source and independently validate the candidate before installing it.
(@ARGV == 3 || @ARGV == 4)
    or die "usage: render-cargo-workspace-version.pl SOURCE PREVIOUS CANDIDATE [WORKSPACE_INPUT_JSON]\n";
my ($path, $previous, $candidate, $workspace_input) = @ARGV;
my $semver = qr/(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)/;
for ($previous, $candidate) {
    stable($_) or die "expected canonical stable version\n";
}
-f $path && !-l $path or die "manifest must be a regular file\n";
open my $input, '<', $path or die "$path: $!\n";
binmode $input or die "$path: $!\n";
my $text = do { local $/; <$input> };
close $input or die "$path: $!\n";
defined $text or die "empty manifest\n";

my ($in_package, $tables, $fields, $offset) = (0, 0, 0, 0);
my ($version, $start);
for my $line (split /(?<=\n)/, $text) {
    if ($line =~ /^[ \t]*\[/) {
        $in_package = $line =~ /^[ \t]*\[[ \t]*workspace\.package[ \t]*\][ \t]*(?:#[^\r\n]*)?(?:\r?\n|\z)/;
        $tables++ if $in_package;
    } elsif ($in_package && $line =~ /^[ \t]*version[ \t]*=/) {
        $fields++;
        $line =~ /^([ \t]*version[ \t]*=[ \t]*")($semver)("[ \t]*(?:#[^\r\n]*)?(?:\r?\n|\z))/
            or die "workspace.package.version must be a literal canonical stable version\n";
        ($version, $start) = ($2, $offset + length($1));
    }
    $offset += length($line);
}
$tables == 1 && $fields == 1
    or die "expected exactly one literal workspace.package table and version\n";
$version eq $previous
    or die "workspace version changed: expected $previous, found $version\n";
my @edits = ([$start, length($version), $candidate]);
if (defined $workspace_input) {
    open my $source, '<', $workspace_input or die "$workspace_input: $!\n";
    my $input = decode_json(do { local $/; <$source> });
    close $source or die "$workspace_input: $!\n";
    my $saved = exists $input->{source_root};
    my $source_root = $saved ? identity_path($input->{source_root}) : undef;
    my %members;
    for my $member (@{$input->{members}}) {
        exists $members{$member->{name}} and die "duplicate workspace member identity\n";
        $members{$member->{name}} = $saved
            ? identity_path($member->{manifest_path}) : $member->{manifest_path};
    }
    my %local;
    my $catalog = $input->{manifest}{workspace}{dependencies} // {};
    for my $name (keys %$catalog) {
        my $dependency = $catalog->{$name};
        next unless ref($dependency) eq 'HASH' && defined $dependency->{path}
            && defined $dependency->{version};
        my $package = $dependency->{package} // $name;
        next unless exists $members{$package};
        if ($saved) {
            # The caller admits these paths against the saved source. Do not
            # resolve them against today's checkout or the scratch file's parent.
            my $directory = $dependency->{path};
            defined($directory) && !ref($directory) && length($directory)
                or die "invalid saved dependency path: $name\n";
            $directory = "$source_root/$directory" unless $directory =~ m{\A/};
            my $manifest = identity_path("$directory/Cargo.toml");
            $manifest eq $members{$package}
                or die "cannot establish saved dependency ownership: $name\n";
        } else {
            # Cargo JSON decodes Unicode; filesystem observations retain byte paths.
            my $manifest = abs_path(dirname($path) . '/' . encode_utf8($dependency->{path}) . '/Cargo.toml');
            defined($manifest) or die "cannot establish dependency ownership: $name\n";
            next unless $manifest eq encode_utf8($members{$package});
        }
        $name =~ /\A[A-Za-z0-9][A-Za-z0-9_-]*\z/ or die "unsupported local dependency key: $name\n";
        my $requirement = $dependency->{version};
        # Composite ranges remain caller policy; Cargo's prepared graph admits
        # or refuses them. Never invent another range to make resolution pass.
        next unless $requirement =~ /\A([~^=]?[ \t]*)([0-9]+(?:\.[0-9]+){0,2})\z/;
        my ($operator, $components) = ($1, scalar(split /\./, $2));
        my @next = split /\./, $candidate;
        $local{$name} = [$requirement, $operator . join('.', @next[0 .. $components - 1])];
    }
    # Tokenize only to locate admitted literals, preserving all other bytes.
    # Strings/comments are opaque, so apparent tables or version keys inside
    # descriptions, paths and comments cannot become dependency edits.
    my @tokens;
    pos($text) = 0;
    while (pos($text) < length($text)) {
        if ($text =~ /\G(?:[ \t\r]+|\#[^\r\n]*)/gc) { next; }
        my $position = pos($text);
        if ($text =~ /\G("""(?:\\[\s\S]|(?!""")[\s\S])*"""|'''(?:(?!''')[\s\S])*'''|"(?:\\[^\r\n]|[^"\\\r\n])*"|'[^'\r\n]*')/gc) {
            push @tokens, [$1, $position, 'string'];
        } elsif ($text =~ /\G([A-Za-z0-9_-]+|[\s\S])/gc) {
            push @tokens, [$1, $position, 'token'];
        } else { die "cannot locate manifest literal\n"; }
    }
    my ($table, $line_start) = ('', 1);
    my %seen;
    my $key = sub {
        my ($token) = @_;
        my $value = $token->[0];
        $value =~ s/\A["']|["']\z//g if $token->[2] eq 'string';
        return $value;
    };
    my $record = sub {
        my ($name, $token) = @_;
        my ($old, $new) = @{$local{$name}};
        $token->[2] eq 'string' && $token->[0] =~ /\A(["'])([^"'\r\n]*)\1\z/
            && $2 eq $old or die "local dependency literal disagrees with admitted TOML: $name\n";
        ++$seen{$name} == 1 or die "duplicate local dependency version: $name\n";
        push @edits, [$token->[1] + 1, length($old), $new];
    };
    for (my $index = 0; $index < @tokens; $index++) {
        my $value = $tokens[$index][0];
        if ($value eq "\n") { $line_start = 1; next; }
        next unless $line_start;
        $line_start = 0;
        if ($value eq '[') {
            my @parts;
            while (++$index < @tokens && $tokens[$index][0] ne ']') {
                push @parts, $key->($tokens[$index]) unless $tokens[$index][0] eq '.';
            }
            $table = join '.', @parts;
            next;
        }
        my $name = $key->($tokens[$index]);
        next unless $index + 2 < @tokens && $tokens[$index + 1][0] eq '=';
        if ($table =~ /\Aworkspace\.dependencies\.([^.]*)\z/ && exists $local{$1} && $name eq 'version') {
            $record->($1, $tokens[$index + 2]);
        } elsif ($table eq 'workspace.dependencies' && exists $local{$name}) {
            $tokens[$index + 2][0] eq '{' or die "local dependency must be a literal table: $name\n";
            my $depth = 1;
            for (my $field = $index + 3; $field < @tokens && $depth; $field++) {
                my $entry = $tokens[$field][0];
                if ($entry eq '{' || $entry eq '[') { $depth++; }
                elsif ($entry eq '}' || $entry eq ']') { $depth--; }
                elsif ($depth == 1 && $key->($tokens[$field]) eq 'version'
                    && $field + 2 < @tokens && $tokens[$field + 1][0] eq '=') {
                    $record->($name, $tokens[$field + 2]);
                }
            }
        }
    }
    for my $name (keys %local) {
        ($seen{$name} // 0) == 1 or die "cannot locate local dependency version: $name\n";
    }
}
for my $edit (sort { $b->[0] <=> $a->[0] } @edits) {
    substr($text, $edit->[0], $edit->[1], $edit->[2]);
}
binmode STDOUT or die "stdout: $!\n";
print $text or die "manifest output: $!\n";
close STDOUT or die "manifest output: $!\n";
