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
my $manifest = blob('crates/icydb-cli/Cargo.toml');
write_file("$scratch/Cargo.toml", $manifest);
write_file("$scratch/Cargo.lock", blob('Cargo.lock'));
write_file("$scratch/workspace.toml", blob('Cargo.toml'));
# Reuse Cargo metadata/group admission and its exact alias/precision projection;
# root catalog references are CLI-owned metadata too.
my $projection=$json->decode(capture('env',"RELEASE_COMMIT=$source",'RELEASE_VERSION=0.0.0',
    'bash','scripts/ci/cargo-release-group.sh','plan','cli'));
@{$projection->{members}}==1 && $projection->{members}[0] eq 'icydb-cli'
    or die "unsupported CLI release family\n";
my %edits=map {$_->{path}=>{version=>$_->{version},dependencies=>$_->{dependencies}}} @{$projection->{manifests}};
write_file("$scratch/edit.json", $json->encode($edits{'crates/icydb-cli/Cargo.toml'}));
write_file("$scratch/workspace-edit.json", $json->encode($edits{'Cargo.toml'}));
my $version = capture($ENV{YQ}//'yq', '-p', 'toml', '-o', 'json', '.package.version', "$scratch/Cargo.toml");
$version = $json->decode($version);
!ref($version) && $version =~ /\A\d+\.\d+\.\d+\z/ or die "invalid CLI version\n";
my $editor = $ENV{CARGO_RELEASE_METADATA_EDITOR};
unless (defined $editor) {
    $editor = capture('bash', 'scripts/release/select-editor.sh', $source);
    $editor =~ s/\n\z//;
}
$editor =~ m{\A/} && -x $editor or die "select admitted metadata editor\n";
my %normalized = (
    'Cargo.toml' => capture($editor, '--max-input-bytes', '8388608',
        'group-manifest', "$scratch/workspace.toml", $version, '0.0.0', "$scratch/workspace-edit.json"),
    'crates/icydb-cli/Cargo.toml' => capture($editor, '--max-input-bytes', '8388608',
        'group-manifest', "$scratch/Cargo.toml", $version, '0.0.0', "$scratch/edit.json"),
    'Cargo.lock' => capture($editor, '--max-input-bytes', '8388608',
        'lock', "$scratch/Cargo.lock", $version, '0.0.0', 'icydb-cli'),
);
my %inputs;
for my $path (@{$contract->{qualification_inputs}}) {
    $path =~ m{\A[.A-Za-z0-9_-][A-Za-z0-9_./-]*\z} && $path !~ m{(?:^|/)\.\.?(?:/|$)}
        && !exists($inputs{$path}) or die "invalid mapping inventory\n";
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
print $json->encode({%$identity, mapping_sha256=>sha256_hex($json->encode($identity))}), "\n";
