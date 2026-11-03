package ValidationPlan;
use strict;
use warnings;
use JSON::PP;
use Digest::SHA qw(sha256_hex);
use Fcntl qw(O_RDONLY O_NONBLOCK S_ISREG);
use Exporter 'import';
our @EXPORT_OK = qw(json bytes digest document relative_path keys_are names policy plan);
my $json = JSON::PP->new->canonical->utf8;
sub json { $json->encode($_[0])."\n" }
sub relative_path {
    my ($path) = @_;
    defined($path) && !ref($path) && length($path) && $path !~ m{^/|[\x00-\x1f\x7f]|(?:^|/)\.{1,2}(?:/|$)|//|/$}
        or die "invalid validation relative path\n";
    return $path;
}
sub read_regular {
    my ($root, $name, $limit, $retain) = @_;
    relative_path($name);
    my $route = $root;
    for my $part (split m{/}, $name) {
        $route .= "/$part";
        !-l $route or die "symlink in validation path: $name\n";
    }
    sysopen my $in, $route, O_RDONLY|O_NONBLOCK or die "read validation file $name: $!\n";
    S_ISREG((stat $in)[2]) or die "validation file is not regular: $name\n";
    binmode $in;
    my ($raw, $size) = ('', 0);
    my $hash = Digest::SHA->new(256);
    while (1) {
        my $n = read $in, my $chunk, 65536;
        defined $n or die "read validation file: $!\n";
        last unless $n;
        $size += $n;
        $size <= $limit or die "validation file exceeds byte bound: $name\n";
        $hash->add($chunk);
        $raw .= $chunk if $retain;
    }
    close $in or die "close validation file: $!\n";
    return $retain ? $raw : $hash->hexdigest;
}
sub bytes { read_regular($_[0], $_[1], $_[2] // 1048576, 1) }
sub digest { read_regular($_[0], $_[1], $_[2] // 67108864, 0) }
sub document {
    my ($root, $path) = @_;
    my $raw = bytes($root, $path);
    my $value = $json->decode($raw);
    $raw eq json($value) or die "validation JSON must be canonical (duplicate keys refuse): $path\n";
    return $value;
}
sub keys_are {
    my ($value, @keys) = @_;
    ref($value) eq 'HASH' && join(',',sort keys %$value) eq join(',',sort @keys)
        or die "unknown or missing validation fields\n";
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
sub policy {
    my ($root, $path) = @_;
    my $value = document($root, $path);
    keys_are($value, qw(schema groups stages));
    json($value->{schema}) eq "1\n" or die "unknown validation policy schema\n";
    names($value->{groups}); names($value->{stages});
    return $value;
}
sub plan {
    my ($root, $path, $source, $group) = @_;
    $source =~ /\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z/ && $group =~ /\A[A-Za-z0-9][A-Za-z0-9_-]*\z/
        or die "plan requires admitted source and group\n";
    my $value = document($root, $path);
    keys_are($value, qw(schema source group scope policy inputs stages));
    json($value->{schema}) eq "1\n" && $value->{source} eq $source && $value->{group} eq $group
        && $value->{scope} =~ /\A(?:group|workspace)\z/ or die "wrong plan schema, source, group or scope\n";
    my $policy = policy($root, $value->{policy});
    my %groups = names($policy->{groups});
    $groups{$group} or die "unknown release group\n";
    my %stages = names($policy->{stages});
    ref($value->{inputs}) eq 'ARRAY' && @{$value->{inputs}} or die "plan requires inputs\n";
    my %inputs = map { relative_path($_) => 1 } ($path, $value->{policy}, @{$value->{inputs}});
    ref($value->{stages}) eq 'ARRAY' or die "invalid plan stages\n";
    my (%seen, @rows);
    my $required = 0;
    for my $stage (@{$value->{stages}}) {
        keys_are($stage, qw(target required reason));
        my $target = $stage->{target};
        defined($target) && !ref($target) && $stages{$target} && !$seen{$target}++
            or die "unknown or duplicate plan stage\n";
        JSON::PP::is_bool($stage->{required}) && defined($stage->{reason}) && !ref($stage->{reason})
            && $stage->{reason} =~ /\S/ && $stage->{reason} !~ /[\x00-\x1f\x7f]/
            or die "stage requires boolean selection and single-line reason\n";
        $stage->{required} || $value->{scope} eq 'group' or die "workspace scope cannot omit stages\n";
        $required += $stage->{required} ? 1 : 0;
        push @rows, join("\t", $stage->{required} ? 'required' : 'not-applicable', $target, $stage->{reason});
    }
    $required && keys(%seen) == keys(%stages) or die "incomplete validation plan\n";
    # Bind the bytes actually parsed, not a second read that might observe a
    # changed policy between selection and the runner's expanded checkpoint.
    my %parsed = ($path => sha256_hex(json($value)), $value->{policy} => sha256_hex(json($policy)));
    return ($value, $policy, [sort keys %inputs], \@rows, \%parsed);
}
1;
