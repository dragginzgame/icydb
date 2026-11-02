package OwnedTree;
use strict;
use warnings;
use Exporter qw(import);
use File::Find qw(find);
use File::Path qw(remove_tree);
use Fcntl qw(:mode);
use JSON::PP;
our @EXPORT_OK = qw(directory route inventory remove_owned);
my $json=JSON::PP->new->canonical;
sub directory {
    my ($p,$consumer_owned)=@_; my @s=lstat($p);
    @s && S_ISDIR($s[2]) && !S_ISLNK($s[2]) && $s[4]==$< && ($consumer_owned || !($s[2]&0022))
        or die "unsafe maintenance directory: $p\n";
}
sub route {
    my ($p)=@_;
    $p =~ m{\A/} && $p !~ m{[\0\r\n]|(?:^|/)\.{1,2}(?:/|$)|//} or die "unsafe maintenance path\n";
    my $cursor='';
    for my $part (split '/', $p) { next unless length $part; $cursor.="/$part"; -l $cursor and die "redirected maintenance path: $cursor\n"; }
}
sub inventory {
    my ($p,$scratch_links)=@_; my %files;
    find({no_chdir=>1,follow=>0,wanted=>sub {
        my @s=lstat($File::Find::name); @s or die "stat $File::Find::name: $!\n";
        $s[4]==$< && (S_ISREG($s[2]) || S_ISDIR($s[2]) || ($scratch_links && S_ISLNK($s[2])))
            or die "unowned or redirected maintenance tree: $File::Find::name\n";
        $files{$File::Find::name}=[$s[0],$s[1],$s[2],$s[3],$s[7],$s[9],$s[10],$s[12]];
        push @{$files{$File::Find::name}},readlink($File::Find::name) if S_ISLNK($s[2]);
    }},$p);
    return \%files;
}
sub remove_owned {
    my ($p,$before,$scratch_links)=@_;
    $json->encode(inventory($p,$scratch_links)) eq $json->encode($before) or die "maintenance tree changed: $p\n";
    remove_tree($p,{safe=>1,error=>\my $errors});
    @$errors && die "maintenance removal failed: ".$json->encode($errors)."\n";
}
1;
