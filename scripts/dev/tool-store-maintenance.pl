#!/usr/bin/env perl
# Shared companions: scripts/dev/OwnedTree.pm
use strict;
use warnings;
use Digest::SHA qw(sha256_hex);
use FindBin;
use lib $FindBin::Bin;
use OwnedTree qw(directory route inventory remove_owned);
use Fcntl qw(:DEFAULT :mode);
use JSON::PP;

# Same-user, explicitly quiescent maintenance of the existing store. Unknown
# generations are reported, never inferred obsolete from missing new metadata.
my ($operation,$store,@args)=@ARGV;
defined($store) && $operation =~ /\A(?:register|preview|apply|trim|forget|explain|pin|manage)\z/
    or die "usage: tool-store-maintenance.pl register|preview|apply|trim|forget|explain|pin|manage STORE [ARGS]\n";
my $json=JSON::PP->new->canonical;
my @receipt_names=qw(inputs.json selection.json native-environment.txt install.log Cargo.lock .crates2.json
    build-identity.observed.json native-environment.observed.txt recovery-pin.json managed.json);
my %receipt_names=map { $_=>1 } @receipt_names;
route($store); directory($store);
sub read_json {
    my ($p)=@_; route($p); my @s=lstat($p);
    @s && S_ISREG($s[2]) && $s[4]==$< && $s[7]<1048576 or die "invalid maintenance record: $p\n";
    open my $f,'<',$p or die "read $p: $!\n"; local $/; my $v=$json->decode(<$f>); close $f or die $!; return $v;
}
sub write_json {
    my ($p,$v)=@_; route($p);
    my $tmp="$p.$$";
    sysopen my $f,$tmp,O_WRONLY|O_CREAT|O_EXCL,0600 or die "create $tmp: $!\n";
    print {$f} $json->encode($v),"\n" or die $!; close $f or die $!;
    rename $tmp,$p or die "publish $p: $!\n";
}
sub children {
    my ($p,$pattern)=@_;
    return () unless -e $p || -l $p;
    route($p); directory($p,1);
    opendir my $dir,$p or die "read directory $p: $!\n";
    my @names=sort grep { /$pattern/ } readdir $dir;
    closedir $dir or die $!;
    return map { "$p/$_" } @names;
}
sub generation {
    my ($p)=@_;
    $p =~ m{\A\Q$store\E/rust-[0-9a-f]{64}/set\.[A-Za-z0-9]+\z} or die "invalid generation: $p\n";
    route($p); directory($p); directory($p =~ s{/[^/]+$}{}r);
}
sub marker {
    my ($p)=@_;
    my $v=read_json($p);
    ref($v) eq 'HASH' && ($v->{schema}//0)==1 or die "invalid maintenance schema: $p\n";
    !exists($v->{complete}) || JSON::PP::is_bool($v->{complete})
        or die "invalid completion marker: $p\n";
    return $v;
}
if ($operation eq 'explain') {
    @args==3 or die "explain requires consumer, key and expected inputs JSON\n";
    my ($consumer,$key,$expected)=@args;
    my $p="$store/consumers/".sha256_hex($consumer)."/".sha256_hex($key).".json";
    exit unless -e $p;
    my $record=read_json($p);
    my $g=$record->{generation};
    # A removed store can leave an old consumer record; diagnostics must not
    # prevent explicit setup from repairing a missing selection.
    exit unless defined($g) && -d $g;
    generation($g);
    my $old=read_json("$g/inputs.json"); my $new=$json->decode($expected);
    my @changed;
    my %fields=(%$old,%$new);
    for my $field (sort keys %fields) {
        if ($field eq 'build') {
            my %parts=(%{$old->{build}//{}},%{$new->{build}//{}});
            for my $part (sort keys %parts) {
                push @changed,"build.$part" if $json->encode($old->{build}{$part}) ne $json->encode($new->{build}{$part});
            }
        } else { push @changed,$field if $json->encode($old->{$field}) ne $json->encode($new->{$field}); }
    }
    print STDERR "Cargo tool selection differs from prior requirement: ",join(', ',@changed),"\n" if @changed;
    print STDERR "Prior native/source details: $g/inputs.json and $g/native-environment.txt\n" if @changed;
    exit;
}
if ($operation eq 'trim') {
    @args==2 && $args[1] =~ /\A[1-9][0-9]*\z/ or die "trim requires a generation and installation owner PID\n";
    my $g=$args[0]; generation($g);
    my $slot=$g =~ s{/[^/]+$}{}r;
    route("$slot/install.lock"); directory("$slot/install.lock",1);
    route("$slot/install.lock/owner");
    open my $owner,'<',"$slot/install.lock/owner" or die "trim requires installation custody\n";
    my $pid=<$owner>; close $owner or die $!;
    defined($pid) && $pid eq "pid=$args[1]\n" or die "trim requires matching installation custody\n";
    -f "$g/selection.json" && !-l "$g/selection.json" or die "trim requires admitted successful output\n";
    # Only regenerable compiler output; receipts, archive, lock and logs survive.
    for my $leaf (qw(build home)) {
        my $p="$g/$leaf"; next unless -e $p || -l $p;
        directory($p,1); remove_owned($p,inventory($p,1),1);
    }
    exit;
}
if ($operation eq 'preview') { @args==0 or die "preview takes no extra arguments\n"; }
elsif ($operation eq 'apply') { @args==1 && $args[0] eq '--quiescent' or die "apply requires --quiescent: stop tool users first\n"; }
my $guard="$store/maintenance.lock";
my $locked=0;
END { if ($locked) { unlink "$guard/owner.json"; rmdir $guard; } }
if ($operation ne 'preview') {
    mkdir $guard,0700 or die "maintenance busy: $guard: $!\n";
    $locked=1;
    write_json("$guard/owner.json",{schema=>1,pid=>$$,operation=>$operation,started=>time});
}
if ($operation eq 'forget') {
    @args==2 && $args[1] eq '--quiescent' or die "forget requires CONSUMER --quiescent\n";
    route($args[0]);
    my $r="$store/consumers/".sha256_hex($args[0]);
    if (-e $r) { directory($r); remove_owned($r,inventory($r)); }
    exit;
}
if ($operation eq 'pin') {
    @args==3 && $args[1] =~ /\A[0-9]{10}\z/ && $args[1]>time && length($args[2]) && length($args[2])<1024
        or die "pin requires GENERATION FUTURE-UNIX-TIME REASON\n";
    generation($args[0]);
    write_json("$args[0]/recovery-pin.json",{schema=>1,until=>0+$args[1],reason=>$args[2]});
    exit;
}
if ($operation eq 'manage') {
    @args==2 && $args[1] eq '--quiescent' or die "manage requires GENERATION --quiescent after declaring every consumer\n";
    generation($args[0]);
    inventory($args[0]);
    # Explicit migration of one reviewed legacy generation, never a blanket
    # inference that all pre-policy tools are unused.
    my $complete=JSON::PP::false;
    if (-f "$args[0]/selection.json") {
        my $selection=read_json("$args[0]/selection.json");
        $complete=JSON::PP::true if ref($selection) eq 'HASH'
            && ($selection->{binary_sha256}//'') =~ /\A[0-9a-f]{64}\z/
            && ref($selection->{inputs}) eq 'HASH';
    }
    write_json("$args[0]/managed.json",{schema=>1,complete=>$complete});
    exit;
}
if ($operation eq 'register') {
    @args==3 or die "register requires consumer, logical selection key and generation\n";
    my ($consumer,$key,$g)=@args;
    route($consumer); directory($consumer,1); generation($g);
    $key =~ /\A[A-Za-z0-9_.:+-]+\z/ or die "invalid logical selection key\n";
    my $records="$store/consumers";
    mkdir $records,0700 unless -e $records; directory($records);
    my $id=sha256_hex($consumer);
    my $root="$records/$id";
    mkdir $root,0700 unless -e $root; directory($root);
    # A refreshed role replaces its obsolete version/native generation, while
    # different consumers and explicit roles retain independent requirements.
    write_json("$root/".sha256_hex($key).".json",{schema=>1,consumer=>$consumer,key=>$key,generation=>$g});
    marker("$g/managed.json") if -e "$g/managed.json" || -l "$g/managed.json";
    # Setup calls register only after byte admission. A partially written
    # selection.json left by a killed build is not proof of completion.
    write_json("$g/managed.json",{schema=>1,complete=>JSON::PP::true});
    exit;
}
my %live;
my %consumers;
for my $p (map { children($_,qr/\A[0-9a-f]{64}\.json\z/) } children("$store/consumers",qr/\A[0-9a-f]{64}\z/)) {
    my $r=marker($p);
    defined($r->{consumer}) && defined($r->{generation}) && defined($r->{key})
        && $r->{key} =~ /\A[A-Za-z0-9_.:+-]+\z/
        && $r->{generation} =~ m{\A\Q$store\E/rust-[0-9a-f]{64}/set\.[A-Za-z0-9]+\z}
        && $p eq "$store/consumers/".sha256_hex($r->{consumer})."/".sha256_hex($r->{key}).".json"
        or die "invalid consumer requirement\n";
    route($r->{consumer});
    next unless -e $r->{consumer}; # Removed temporary checkout no longer owns tools.
    directory($r->{consumer},1);
    $consumers{$r->{consumer}}=1;
    $live{$r->{generation}}=1;
}
my %common;
for my $consumer (sort keys %consumers) {
    my $root="$consumer/.tools/rust";
    next unless -e $root || -l $root;
    route($root); directory($root,1);
    -e "$root/common.lock" || -l "$root/common.lock" and die "consumer common setup is active: $root\n";
    if (-e "$root/common" || -l "$root/common") {
        my $name=readlink("$root/common");
        defined($name) && $name =~ /\Acommon-set\.[A-Za-z0-9]+\z/ or die "unmanaged common selection\n";
        my $bundle="$root/$name";
        route("$bundle/bin"); directory($bundle,1); directory("$bundle/bin",1);
        $common{$bundle}=1;
        for my $bin (children("$bundle/bin",qr/\A[^.][^\/]*\z/)) {
            my $link=readlink($bin);
            defined($link) && $link =~ m{\A\.\./\.\./(selected-[0-9a-f]{64})/bin/[^/]+\z}
                or die "unmanaged common executable: $bin\n";
            my $selection="$root/$1";
            my $ref=readlink($selection);
            defined($ref) && $ref =~ /\Arust-set\.[A-Za-z0-9]+\z/ or die "invalid common selector\n";
            my $g=readlink("$root/$ref");
            defined($g) or die "invalid common reference\n";
            generation($g); $live{$g}=1;
        }
    }
}
sub retire_references {
    my ($g)=@_;
    for my $consumer (sort keys %consumers) {
        my $root="$consumer/.tools/rust";
        next unless -e $root; route($root); directory($root,1);
        for my $ref (children($root,qr/\Arust-set\.[A-Za-z0-9]+\z/)) {
            next unless -l $ref && readlink($ref) eq $g;
            my $name=$ref =~ s{.*/}{}r;
            for my $selection (children($root,qr/\Aselected-[0-9a-f]{64}\z/)) {
                next unless -l $selection && readlink($selection) eq $name;
                unlink $selection or die "retire selector: $!\n";
            }
            unlink $ref or die "retire reference: $!\n";
        }
    }
}
my @entries;
my (%physical,%snapshots);
for my $g (map { children($_,qr/\Aset\.[A-Za-z0-9]+\z/) } children($store,qr/\Arust-[0-9a-f]{64}\z/)) {
    generation($g);
    my $slot=$g =~ s{/[^/]+$}{}r;
    my $pin=-e "$g/recovery-pin.json" || -l "$g/recovery-pin.json" ? marker("$g/recovery-pin.json") : {schema=>1,until=>0};
    $pin->{schema}==1 && defined($pin->{until}) && $pin->{until}=~/\A[0-9]+\z/ or die "invalid recovery pin\n";
    my $managed=-e "$g/managed.json" || -l "$g/managed.json";
    my $management=$managed ? marker("$g/managed.json") : {};
    my $journal="$store/receipts/".sha256_hex($g)."/reclamation.json";
    my $pending;
    if (-e $journal || -l $journal) {
        my $j=marker($journal);
        ($j->{path}//'') eq $g && ($j->{state}//'') =~ /\A(?:removing|removed)\z/ or die "invalid reclamation journal\n";
        $pending=$j->{state} eq 'removing';
    }
    my $reason= -e "$slot/install.lock" || -l "$slot/install.lock" ? 'active-installation'
        : !$managed && !$pending ? 'unregistered-generation'
        : $pin->{until}>time ? 'explicit-recovery-pin'
        : $live{$g} ? 'declared-consumer'
        : !$pending && !$management->{complete} && time-(stat("$g/managed.json"))[9]<7*86400 ? 'recent-incomplete-attempt'
        : 'obsolete';
    my $files;
    eval { $files=inventory($g); 1 } or do { push @entries,{path=>$g,status=>'protected',reason=>'unsafe-or-changing-tree'}; next; };
    for my $leaf (@receipt_names) {
        $reason='oversized-evidence' if exists($files->{"$g/$leaf"}) && $files->{"$g/$leaf"}[4]>16*1024*1024;
    }
    my $bytes=0; my %seen;
    for my $p (keys %$files) {
        my $s=$files->{$p};
        next unless S_ISREG($s->[2]); my $id="$s->[0]:$s->[1]";
        my $allocated=($s->[7]//0)*512;
        $bytes+=$allocated unless $seen{$id}++;
        my $record=$physical{$id}//={bytes=>$allocated,links=>$s->[3],paths=>0,eligible=>0};
        $record->{paths}++;
        # Kept receipts and links outside eligible generations cannot count as
        # freed bytes. Per-entry totals deliberately do not sum across hardlinks.
        $record->{eligible}++ if $reason eq 'obsolete' && !$receipt_names{substr($p,length($g)+1)};
    }
    push @entries,{path=>$g,status=>$reason eq 'obsolete'?'reclaimable':'protected',reason=>$reason,physical_bytes=>$bytes};
    $snapshots{$g}=$files if $operation eq 'apply' && $reason eq 'obsolete';
}
my ($physical_bytes,$reclaimable_bytes)=(0,0);
for my $r (values %physical) {
    $physical_bytes+=$r->{bytes};
    $reclaimable_bytes+=$r->{bytes} if $r->{eligible}==$r->{links};
}
if ($operation eq 'apply') {
    my %unlinked;
    for my $entry (@entries) {
        next unless $entry->{status} eq 'reclaimable';
        my $g=$entry->{path}; my $files=$snapshots{$g};
        my $slot=$g =~ s{/[^/]+$}{}r;
        my $now=inventory($g);
        $json->encode([sort keys %$now]) eq $json->encode([sort keys %$files]) or die "maintenance paths changed\n";
        for my $p (keys %$files) {
            my @expected=@{$files->{$p}};
            my $removed=$unlinked{"$expected[0]:$expected[1]"}//0;
            if (S_ISREG($expected[2]) && $removed) {
                # Earlier admitted removals can unlink another name for this
                # inode. Only that expected link-count/ctime change is allowed.
                $expected[3]-=$removed;
                $expected[6]=$now->{$p}[6];
            }
            $json->encode(\@expected) eq $json->encode($now->{$p}) or die "maintenance tree changed: $p\n";
        }
        $files=$now;
        # The explicit quiescence assertion covers processes holding old paths;
        # installation locks and the global maintenance guard stop new setup.
        (-e "$slot/install.lock" || -l "$slot/install.lock") && die "installation started during maintenance\n";
        my $receipts="$store/receipts";
        mkdir $receipts,0700 unless -e $receipts; directory($receipts);
        my $saved="$receipts/".sha256_hex($g);
        mkdir $saved,0700 unless -e $saved; directory($saved);
        for my $leaf (@receipt_names) {
            next unless -f "$g/$leaf";
            route("$g/$leaf");
            (stat("$g/$leaf"))[7]<=16*1024*1024 or die "oversized receipt requires explicit evidence handling: $g/$leaf\n";
            open my $in,'<',"$g/$leaf" or die $!;
            # Idempotent retry retains the exact original bytes.
            local $/; my $bytes=<$in>; close $in or die $!;
            if (-e "$saved/$leaf") {
                route("$saved/$leaf");
                my @s=lstat("$saved/$leaf");
                @s && S_ISREG($s[2]) && $s[4]==$< && $s[7]<=16*1024*1024 or die "invalid saved receipt\n";
                open my $old,'<',"$saved/$leaf" or die $!;
                my $prior=<$old>; close $old or die $!;
                $prior eq $bytes or die "retained receipt drift\n";
            } else {
                my $temporary="$saved/$leaf.$$";
                sysopen my $out,$temporary,O_WRONLY|O_CREAT|O_EXCL,0600 or die $!;
                print {$out} $bytes or die $!; close $out or die $!;
                rename $temporary,"$saved/$leaf" or die "publish immutable receipt: $!\n";
            }
        }
        write_json("$saved/reclamation.json",{schema=>1,path=>$g,state=>'removing'});
        remove_owned($g,$files);
        for my $s (values %$files) {
            $unlinked{"$s->[0]:$s->[1]"}++ if S_ISREG($s->[2]);
        }
        if (-l "$slot/current" && readlink("$slot/current") eq ($g =~ s{.*/}{}r)) {
            unlink "$slot/current" or die "retire store hint: $!\n";
        }
        # Remove only generated references to this proven-obsolete generation.
        # Live requirements from every registered consumer were admitted above.
        retire_references($g);
        write_json("$saved/reclamation.json",{schema=>1,path=>$g,state=>'removed'});
    }
    # Recover an interruption after deleting the payload but before retiring its
    # selectors. Receipts are already durable; never re-create the old payload.
    for my $saved (children("$store/receipts",qr/\A[0-9a-f]{64}\z/)) {
        next unless -e "$saved/reclamation.json" || -l "$saved/reclamation.json";
        my $j=marker("$saved/reclamation.json");
        my $g=$j->{path}//'';
        $g =~ m{\A\Q$store\E/rust-[0-9a-f]{64}/set\.[A-Za-z0-9]+\z}
            && $saved eq "$store/receipts/".sha256_hex($g) or die "invalid reclamation path\n";
        route($g);
        next if -e $g;
        retire_references($g);
        write_json("$saved/reclamation.json",{schema=>1,path=>$g,state=>'removed'}) if ($j->{state}//'') eq 'removing';
    }
    for my $consumer (sort keys %consumers) {
        my $root="$consumer/.tools/rust";
        next unless -d $root;
        for my $bundle (children($root,qr/\Acommon-set\.[A-Za-z0-9]+\z/)) {
            next if $common{$bundle};
            route($bundle); directory($bundle,1);
            my @top=children($bundle,qr/\A(?!\.{1,2}\z).+/);
            next unless @top==1 && $top[0] eq "$bundle/bin";
            route("$bundle/bin"); directory("$bundle/bin",1);
            my @bins=children("$bundle/bin",qr/\A(?!\.{1,2}\z).+/);
            next if grep { !-l $_ || readlink($_) !~ m{\A\.\./\.\./selected-[0-9a-f]{64}/bin/[^/]+\z} } @bins;
            unlink($_) or die "retire common link: $!\n" for @bins;
            rmdir "$bundle/bin" or die $!;
            rmdir $bundle or die $!;
        }
    }
}
print $json->encode({schema=>1,physical_bytes=>$physical_bytes,reclaimable_physical_bytes=>$reclaimable_bytes,entries=>\@entries}),"\n";
