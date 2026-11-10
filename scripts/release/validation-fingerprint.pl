#!/usr/bin/env perl
# Product mapping identity. IC Build owns Cargo version projection.
use strict;
use warnings;
use JSON::PP;
use Digest::SHA qw(sha256_hex);
use File::Temp qw(tempdir);
my $json = JSON::PP->new->canonical->utf8;
@ARGV == 1 && $ARGV[0] =~ /\A[0-9a-f]{40,64}\z/ or die "usage: validation-fingerprint.pl SOURCE\n";
my $source = $ARGV[0];
sub capture {
    open my $in, '-|', @_ or die "start mapping observation: $!\n";
    binmode $in;
    my $raw = '';
    while (my $n = read($in, my $chunk, 65536)) {
        $raw .= $chunk;
        length($raw) <= 8388608 or die "mapping input exceeds 8 MiB\n";
    }
    eof($in) or die "mapping observation failed\n";
    close $in or die "mapping observation failed\n";
    return $raw;
}
sub blob { capture('git', 'show', "$source:$_[0]") }
sub write_file {
    my ($path, $raw) = @_;
    open my $out, '>:raw', $path or die "write projection: $!\n";
    print {$out} $raw; close $out or die "close projection\n";
}
my $contract = $json->decode(blob('ci/release-validation-contract.json'));
!-l 'target' or die "redirected fingerprint scratch\n";
my $scratch = tempdir('fingerprint.XXXXXX', DIR=>'target', CLEANUP=>1);
# Normalize only each release family's owned version metadata through the same
# projection used by preparation/recovery. Dependency/features/tool changes stay
# bound, while ordinary database/CLI patch preparation does not erase proof.
my $editor = $ENV{CARGO_RELEASE_METADATA_EDITOR};
unless (defined $editor) {
    $editor = capture('bash', 'scripts/release/select-editor.sh', $source);
    $editor =~ s/\n\z//;
}
$editor =~ m{\A/} && -x $editor or die "select admitted metadata editor\n";
my %normalized;
for my $group (qw(database cli)) {
    my $projection=$json->decode(capture('env',"RELEASE_COMMIT=$source",'RELEASE_VERSION=0.0.0',
        'bash','scripts/ci/cargo-release-group.sh','plan',$group));
    write_file("$scratch/plan.json",$json->encode($projection));
    for my $path ((map {$_->{path}} @{$projection->{manifests}}),
        'Cargo.lock', (exists($projection->{qualification}) ? 'qualification/Cargo.lock' : ()),
        'ci/dependency-pinning-exceptions.json') {
        write_file("$scratch/input",exists($normalized{$path})?$normalized{$path}:blob($path));
        $normalized{$path}=capture('env',"CARGO_RELEASE_METADATA_EDITOR=$editor",
            'CARGO_RELEASE_METADATA_MAX_BYTES=8388608','bash','scripts/release/render-candidate.sh',
            $path,"$scratch/input",$projection->{previous},'0.0.0',"$scratch/plan.json");
    }
}
my %inputs = map {$_ => sha256_hex($normalized{$_})} keys %normalized;
# Bind the complete automation owner, not only dispatch entrypoints. Otherwise a
# later fixture/CI change could silently reuse an earlier omission mapping. The
# reviewed certificate is the trust input and is checkpointed by every fresh run;
# excluding it here avoids a self-referential fingerprint.
my @automation=grep {$_ ne 'ci/release-validation-qualified.json'} split /\n/,
    capture('git','ls-tree','-r','--name-only',$source,'--','scripts','ci','make','.github','AGENTS.md');
my %seen;
for my $path (@{$contract->{qualification_inputs}}, @automation) {
    $path =~ m{\A[.A-Za-z0-9_-][A-Za-z0-9_./-]*\z} && $path !~ m{(?:^|/)\.\.?(?:/|$)}
        or die "invalid mapping inventory\n";
    next if $seen{$path}++;
    $inputs{$path} = sha256_hex(exists($normalized{$path}) ? $normalized{$path} : blob($path));
}
exists($inputs{'Cargo.lock'}) && exists($inputs{'crates/icydb-cli/Cargo.toml'})
    or die "mapping must bind normalized CLI metadata\n";
# Acquisition/cache paths and terminal output do not change the selected
# contracts. Compiler flags, wrappers, profiles and Cargo configuration do.
my %configuration = map { $_ => sha256_hex($ENV{$_}) }
    grep { /\A(?:RUST|CARGO_)/ && !/\A(?:RUSTUP_HOME|RUSTUP_AUTO_INSTALL|CARGO_MAKEFLAGS|CARGO_HOME|CARGO_TARGET_DIR|CARGO_NET_OFFLINE|CARGO_TERM_COLOR|CARGO_RELEASE_METADATA_EDITOR|CARGO_RELEASE_METADATA_MAX_BYTES)\z/
        && !/TOKEN\z/ } keys %ENV;
my $cargo_home = $ENV{CARGO_HOME}//'.cache/cargo/icydb';
for my $entry (['workspace-config','.cargo/config'], ['workspace-config-toml','.cargo/config.toml'],
    ['cargo-home-config',"$cargo_home/config"], ['cargo-home-config-toml',"$cargo_home/config.toml"]) {
    my ($label, $path) = @$entry;
    !-l $path or die "redirected Cargo configuration\n";
    $configuration{$label} = -e $path ? sha256_hex(capture('cat', $path)) : 'absent';
}
my $identity = {mapping_inputs=>\%inputs, configuration=>\%configuration};
my $cargo_inputs = {map {$_=>sha256_hex($normalized{$_})} keys %normalized};
print $json->encode({%$identity, cargo_inputs=>$cargo_inputs, mapping_sha256=>sha256_hex($json->encode($identity))}), "\n";
