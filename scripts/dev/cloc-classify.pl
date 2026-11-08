#!/usr/bin/env perl
# Static Rust source classification; Cargo owns targets and cloc owns LOC.
use strict;
use warnings;
use JSON::PP;
use File::Find;
use File::Basename qw(dirname basename);
use Cwd qw(abs_path);
use Digest::SHA qw(sha256_hex);
use Errno qw(ENOENT);
my $scratch;
my $json = JSON::PP->new->canonical;
my @categories = qw(library_app tests examples benchmarks build_tooling unclassified);
my %valid = map { $_ => 1 } @categories;
sub read_file {
    my $path=shift;
    open my $f, '<', $path or do {
        if ($! == ENOENT && defined($scratch) && index($path,"$scratch/")==0) {
            die "LOC scratch input disappeared: $path; rerun after cargo clean or other cleanup has finished. No partial report was produced.\n";
        }
        die "read $path: $!\n";
    };
    my $text='';
    while (1) {my $n=read($f,my $chunk,65536); defined($n) or die "read $path: $!\n"; last unless $n; $text.=$chunk;}
    close $f or die "close $path: $!\n"; return $text;
}
sub write_file { open my $f, '>', $_[0] or die "write $_[0]: $!\n"; print {$f} $_[1]; close $f or die $!; }
sub zero { return {map {$_ => 0} (@categories, 'total')}; }
sub add { my ($to,$from)=@_; $to->{$_} += $from->{$_} for (@categories,'total'); }
sub safe_label { my $s=shift; $s =~ s/([\x00-\x1f\x7f])/sprintf('\\x%02x',ord($1))/ge; return $s; }

# Reuse the classified file counts: presentation must never recount source or
# change category attribution. Merge foo.rs with foo/ and foo/mod.rs naturally.
sub module_parts {
    my ($crate,$file)=@_;
    my $path=$file->{path};
    my @parts;
    if (index($path,"$crate->{root}/")==0) {
        $path=substr($path,length($crate->{root})+1);
        if ($path =~ s{\Asrc/}{}) {
            $path =~ s{\A(?:lib|main)\.rs\z}{};
        }
        $path =~ s{(?:\A|/)mod\.rs\z}{};
        $path =~ s{\.rs\z}{};
        @parts=length($path) ? split(m{/},$path) : ();
    } else { $path =~ s{\.rs\z}{}; @parts=('(external)', $path); }
    return @parts;
}
sub module_rows {
    my ($crate,$records,$depth)=@_;
    my $tree={children=>{},counts=>zero()};
    for my $file (@$records) {
        my @parts=module_parts($crate,$file);
        if (defined $crate->{module_prefix}) {
            splice @parts,0,scalar(@{$crate->{module_prefix}});
        }
        my $node=$tree;
        splice @parts,$depth if @parts>$depth;
        for my $part (@parts) {
            $node=$node->{children}{$part}//={children=>{},counts=>zero()};
            add($node->{counts},$file->{counts});
        }
    }
    my (@rows,@pending);
    @pending=map {[$_,$tree->{children}{$_},1]} reverse sort keys %{$tree->{children}};
    while (my $entry=pop @pending) {
        my ($label,$node,$level)=@$entry;
        push @rows,[('  ' x ($level+1)).safe_label($label),$node->{counts},37];
        next if $level >= $depth || !keys %{$node->{children}};
        push @pending,map {[$_,$node->{children}{$_},$level+1]} reverse sort keys %{$node->{children}};
    }
    return @rows;
}
if (($ARGV[0] // '') eq '--select') {
    shift;
    @ARGV==2 or die "usage: --select REPORT CRATE[::MODULE]\n";
    my $r=$json->decode(read_file(shift));
    my $selection=shift;
    my ($name,@prefix)=split /::/,$selection,-1;
    !grep { !length($_) || /[^a-zA-Z0-9_-]/ } ($name,@prefix)
        or die "invalid crate/module selector: $selection\n";
    my ($crate)=grep {$_->{crate} eq $name} @{$r->{crates}};
    $crate or die "unknown workspace crate: $name\n";
    my @files=grep {
        my @parts=module_parts($crate,$_);
        my $matches=$_->{crate} eq $name && @parts>=@prefix;
        for my $i (0..$#prefix) { $matches=0 if !defined($parts[$i]) || $parts[$i] ne $prefix[$i]; }
        $matches;
    } @{$r->{files}};
    @files || !@prefix or die "unknown source module: $selection\n";
    my $counts=zero(); add($counts,$_->{counts}) for @files;
    $crate->{counts}=$counts;
    $crate->{module_prefix}=\@prefix;
    $r->{selection}=$selection;
    $r->{crates}=[$crate]; $r->{files}=\@files; $r->{totals}=$counts;
    my %paths=map {$_->{path}=>1} @files;
    $r->{diagnostics}=[grep {$paths{$_->{path}}} @{$r->{diagnostics}}];
    print $json->encode($r),"\n";
    exit;
}
if (($ARGV[0] // '') eq '--table') {
    shift; my $r=$json->decode(read_file(shift));
    !@ARGV || (@ARGV==2 && $ARGV[0] eq '-d' && $ARGV[1] =~ /\A(?:0|[1-9][0-9]{0,5})\z/)
        or die "usage: cloc-classify.pl --table REPORT [-d DEPTH]\n";
    my $depth=@ARGV ? $ARGV[1] : undef;
    my @rows;
    for my $w (@{$r->{workspaces} // [{name=>basename(dirname($r->{manifest})),report=>$r}]}) {
        if ($w->{report}) {
            my $label=safe_label($w->{report}{selection} // $w->{name});
            my $kind=$w->{report}{selection} ? ($w->{report}{selection} =~ /::/ ? 37 : 34) : 32;
            push @rows, [$label, $w->{report}{totals},$kind];
            if (defined $depth) {
                my %files_by_crate;
                if ($depth) {
                    push @{$files_by_crate{$_->{crate}}},$_ for @{$w->{report}{files}};
                }
                for my $crate (@{$w->{report}{crates}}) {
                    push @rows,['  '.safe_label($crate->{crate}),$crate->{counts},34] unless $w->{report}{selection};
                    if ($depth) {
                        my @modules=module_rows($crate,$files_by_crate{$crate->{crate}} // [],$depth);
                        if ($w->{report}{selection}) { $_->[0] =~ s/^  // for @modules; }
                        push @rows,@modules;
                    }
                }
            }
        } else { push @rows, [safe_label($w->{name}), {map {$_=>$w->{status}} (@categories,'total')},32]; }
    }
    my $colour=-t STDOUT && !length($ENV{NO_COLOR} // '') && ($ENV{TERM} // '') ne 'dumb';
    my $aggregate=exists $r->{workspaces};
    push @rows, [$r->{partial} ? 'TOTAL (partial)' : 'TOTAL', $r->{totals}] if $aggregate;
    my @keys=(@categories,'total');
    my @titles=('app/lib','tests','examples','bench','tooling','???','total');
    my @gaps=('     ',map {'  '} 1..$#keys);
    my @widths=map {length($_)} @titles;
    my $width=length('workspace/crate');
    for my $row (@rows) {
        $width=length($row->[0]) if length($row->[0])>$width;
        for my $i (0..$#keys) {
            my $n=length($row->[1]{$keys[$i]} // 'n/a');
            $widths[$i]=$n if $n>$widths[$i];
        }
    }
    my $separator=('-' x $width).join('',map {$gaps[$_].('-' x $widths[$_])} 0..$#keys)."\n";
    printf '%-*s', $width, 'workspace/crate';
    printf '%s%*s', $gaps[$_],$widths[$_],$titles[$_] for 0..$#keys;
    print "\n",$separator;
    for my $i (0..$#rows) {
        print $separator if $aggregate && $i==$#rows;
        my $code=$colour ? $rows[$i][2] : undef;
        print "\e[${code}m" if defined $code;
        print $rows[$i][0];
        print "\e[0m" if defined $code;
        print ' ' x ($width-length($rows[$i][0]));
        for my $j (0..$#keys) {printf '%s%*s', $gaps[$j],$widths[$j],$rows[$i][1]{$keys[$j]} // 'n/a';}
        print "\n";
    }
    exit;
}
@ARGV == 3 or die "usage: cloc-classify.pl METADATA MANIFEST SCRATCH | --table REPORT [-d DEPTH]\n";
my ($metadata_file,$manifest);
($metadata_file,$manifest,$scratch)=@ARGV;
my $metadata=$json->decode(read_file($metadata_file));
my %members=map {$_=>1} @{$metadata->{workspace_members}};
my @packages=sort {$a->{name} cmp $b->{name}} grep {$members{$_->{id}}} @{$metadata->{packages}};
@packages or die "Cargo workspace contains no packages\n";
my @roots=map {dirname($_->{manifest_path})} @packages;
my @excluded=($metadata->{target_directory}); push @excluded, abs_path($metadata->{target_directory}) if -d $metadata->{target_directory};
for my $root ($metadata->{workspace_root},@roots) {
    push @excluded, map {"$root/$_"} qw(target build dist coverage vendor recovery-evidence);
}
push @excluded, $scratch;
# Git owns custom scratch/output exclusions. Retain tracked files and new,
# nonignored source alike. Source archives (including ignored fixture workspaces
# beneath another checkout) still use filesystem discovery.
my ($git_root,%git_files,%git_dirs);
my $ancestor=$metadata->{workspace_root};
while (1) {
    if (-e "$ancestor/.git") {
        open my $listing, '-|', 'git', '-C', $ancestor, 'ls-files', '-z',
            '--cached', '--others', '--exclude-standard', '--full-name'
            or die "cannot list Git source inventory: $!\n";
        { local $/="\0";
          while (my $path=<$listing>) {chomp $path; $git_files{"$ancestor/$path"}=1;} }
        close $listing or die "cannot list Git source inventory\n";
        if ($git_files{$manifest}) {
            $git_root=$ancestor;
            for my $file (keys %git_files) {
                my $dir=dirname($file);
                while (index($dir,"$ancestor/")==0) {$git_dirs{$dir}=1; $dir=dirname($dir);}
            }
            $git_dirs{$ancestor}=1;
        }
        last;
    }
    my $parent=dirname($ancestor); last if $parent eq $ancestor; $ancestor=$parent;
}
sub excluded_path {
    my ($path,$directory)=@_;
    return 1 if grep {$path eq $_ || index($path,"$_/")==0} @excluded;
    my $relative=$path;
    $relative=substr($path,length($metadata->{workspace_root})+1)
        if index($path,"$metadata->{workspace_root}/")==0;
    return 1 if $relative =~ m{(?:\A|/)(?:\.git|\.tools|\.tmp|\.cache|node_modules)(?:/|\z)};
    if (defined($git_root) && index($path,"$git_root/")==0) {
        return !($directory ? $git_dirs{$path} : $git_files{$path});
    }
    return 0;
}
my (%files,%visited,%admitters,@diagnostics);
my $seq=0;
sub diagnostic {my ($path,$reason)=@_; push @diagnostics,{path=>$path,reason=>$reason};}
sub relative_rule {
    my $s=shift;
    defined($s) && !ref($s) && length($s) && $s !~ m{\A/|[\r\n\t]|(?:\A|/)\.\.(?:/|\z)} or die "invalid LOC path rule\n";
    return $s;
}
# Tokens retain byte ranges. Comments (including nested blocks), quoted/raw
# strings and character literals are opaque, so their braces/attributes cannot
# become module or test syntax. This does not expand macros or evaluate features.
sub tokens {
    my ($text,$path)=@_; my @t; pos($text)=0;
    while (pos($text)<length($text)) {
        my $start=pos($text);
        if ($text =~ /\G\s+/gc || $text =~ /\G\/\/[^\n]*/gc) {next;}
        if ($text =~ /\G\/\*/gc) {
            my $depth=1;
            while ($depth && pos($text)<length($text)) {
                if ($text =~ /\G\/\*/gc) {$depth++;}
                elsif ($text =~ /\G\*\//gc) {$depth--;}
                else {$text =~ /\G./gcs;}
            }
            die "unterminated comment: $path\n" if $depth; next;
        }
        if ($text =~ /\G(?:br|cr|r)(\#*)"/gc) {
            my $end=index($text,'"'.$1,pos($text)); die "unterminated raw string: $path\n" if $end<0;
            pos($text)=$end+1+length($1);
        } elsif ($text =~ /\G(?:b|c)?"(?:\\.|[^"\\])*"/gcs) {
        } elsif ($text =~ /\G(?:b)?'(?:\\(?:u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|.)|[^'\\\n])'/gcs) {
        } elsif ($text =~ /\G(?=(?:b|c)?")/gc) {
            die "unterminated string: $path\n";
        } elsif ($text =~ /\G(?:r\#)?[A-Za-z_][A-Za-z_0-9]*/gc) {
        } else {$text =~ /\G./gcs;}
        push @t,[substr($text,$start,pos($text)-$start),$start,pos($text)];
    }
    my @stack; my %pair;
    for my $i (0..$#t) {
        my $v=$t[$i][0];
        if ($v =~ /\A[({\[]\z/) {push @stack,$i;}
        elsif ($v =~ /\A[)}\]]\z/) {
            my $j=pop @stack; defined($j) or die "unbalanced Rust tokens: $path\n";
            my %close=('('=>')','['=>']','{'=>'}'); $close{$t[$j][0]} eq $v or die "unbalanced Rust tokens: $path\n";
            $pair{$j}=$i;
        }
    }
    @stack and die "unbalanced Rust tokens: $path\n";
    return (\@t,\%pair);
}
# Prove cfg is false when test=false. Unknown features stay unknown: any(test,
# feature) must not be mistaken for test-only, whereas all(test, feature) is.
sub cfg_value {
    my ($s,$test)=@_; return $test if $s eq 'test';
    if ($s =~ /\A(all|any|not)\((.*)\)\z/s) {
        my ($op,$body)=($1,$2); my (@parts,$part); $part=''; my $depth=0;
        for (split //,$body) {if ($_ eq ',' && !$depth) {push @parts,$part; $part='';next;} $part.=$_; $depth++ if $_ eq '('; $depth-- if $_ eq ')';}
        push @parts,$part if length $part;
        my @v=map {cfg_value($_,$test)} @parts;
        return @v==1 && $v[0]!=-1 ? !$v[0] : -1 if $op eq 'not';
        return 0 if $op eq 'all' && grep {$_==0} @v;
        return 1 if $op eq 'any' && grep {$_==1} @v;
        return -1 if grep {$_==-1} @v;
        return $op eq 'all' ? 1 : 0;
    }
    return -1;
}
sub test_only {
    my $cfg=shift;
    # A condition false in both modes (e.g. any()) is not evidence of tests.
    return cfg_value($cfg,0)==0 && cfg_value($cfg,1)!=0;
}
sub load_source {
    my $v=shift;
    $v->{text}=read_file($v->{count_path});
    my ($t,$pair);
    eval { ($t,$pair)=tokens($v->{text},$v->{path}); 1 } or do {
        my $error=$@; chomp $error;
        diagnostic($v->{path},"Rust token classification unavailable: $error") unless $v->{lexical_error};
        $v->{lexical_error}=$error;
        ($t,$pair)=([],{});
    };
    @{$v}{qw(tokens pair)}=($t,$pair);
}
sub unload_source {delete @{$_[0]}{qw(text tokens pair)};}
for my $p (@packages) {
    my $root=dirname($p->{manifest_path});
    my $policy=$p->{metadata}{'shared-tooling'}{loc} // {};
    ref($policy) eq 'HASH' or die "LOC metadata must be an object\n";
    for (keys %$policy) { /\A(?:category|paths|generated)\z/ or die "unknown LOC metadata field: $_\n"; }
    if (exists $policy->{category}) { $valid{$policy->{category}} or die "invalid LOC category\n"; }
    my $paths=$policy->{paths}//{}; ref($paths) eq 'HASH' or die "LOC paths must be an object\n";
    for (keys %$paths) {relative_rule($_); $valid{$paths->{$_}} or die "invalid LOC path category\n";}
    my $generated=$policy->{generated}//[]; ref($generated) eq 'ARRAY' or die "LOC generated must be an array\n"; relative_rule($_) for @$generated;
    my %matched;
    local $SIG{__WARN__}=sub {die @_};
    my $admit_file=sub {
        my $f=shift;
        return unless -f $f && !-l $f && $f =~ /\.rs\z/;
        return if excluded_path($f,0);
        return if exists $files{$f};
        $f !~ /[\r\n\t]/ or die "unsupported source path\n";
        my $relative=index($f,"$root/")==0 ? substr($f,length($root)+1) : $f;
        my $text=read_file($f);
        # Snapshot once; retain lightweight identities instead of every file's
        # text and token tree. Discovery and counting read these same bytes.
        my $count_path="$scratch/source-".$seq++.'.rs';
        write_file($count_path,$text);
        my @rules=sort {length($b)<=>length($a) || $a cmp $b} grep {$relative eq $_ || index($relative,"$_/")==0} keys %$paths;
        $matched{$_}=1 for @rules;
        my @gen=grep {$relative eq $_ || index($relative,"$_/")==0} @$generated; $matched{"generated:$_"}=1 for @gen;
        $files{$f}={path=>$f,relative=>$relative,package=>$p->{name},count_path=>$count_path,source_sha256=>sha256_hex($text),
            contexts=>{},test_ranges=>[],
            override=>@rules ? $paths->{$rules[0]} : $policy->{category},
            generated=>(@gen || $text =~ /\A(?:[^\n]*\n){0,5}[ \t]*(?:\/\/|\/\*|\*)[^\n]*(?:\@generated|DO NOT EDIT)/) ? JSON::PP::true : JSON::PP::false};
        diagnostic($f,'legacy test/ or test.rs layout: rename to tests/ or tests.rs') if $relative =~ m{(?:\A|/)test(?:/|\.rs\z)};
    };
    $admitters{$p->{name}}=$admit_file;
    find({no_chdir=>1,wanted=>sub {
        my $f=$File::Find::name;
        if (-d $f) {
            if ($f ne $root && (excluded_path($f,1)
                || (basename($f) eq 'target' && -f dirname($f).'/Cargo.toml')
                || grep {$f eq $_} (@excluded,@roots))) {$File::Find::prune=1;}
        } else {$admit_file->($f);}
    }},$root);
    # Cargo permits targets such as build = "../build.rs" outside the member.
    # They belong in the footprint too; canonicalize aliases and count once.
    for my $target (@{$p->{targets}}) {
        my $src=$target->{src_path};
        next unless -f $src && !-l $src;
        $src=abs_path($src);
        next if grep {$src eq $_ || index($src,"$_/")==0} @excluded;
        $admit_file->($src);
    }
    for (keys %$paths) { $matched{$_} or die "LOC path rule matches no Rust source: $root/$_\n"; }
    for (@$generated) { $matched{"generated:$_"} or die "generated rule matches no Rust source: $root/$_\n"; }
}
my @pending;
sub visit_file {push @pending, [@_];}
sub scan {
    # Inline Rust scopes can be arbitrarily deep; keep them off Perl's stack.
    my @scopes=([@_]);
    while (my $scope=pop @scopes) {
        scan_scope($scope,\@scopes);
    }
}
sub scan_scope {
    my ($scope,$scopes)=@_;
    my ($f,$lo,$hi,$dir,$category,$follow)=@$scope; $follow=1 unless defined $follow;
    my $v=$files{$f}; my $t=$v->{tokens}; my $pairs=$v->{pair};
    # Inner attributes describe this scope, not the following item. Consuming
    # them as an item head would hide the next outer cfg/test attribute.
    while ($lo+2<$hi && $t->[$lo][0] eq '#' && $t->[$lo+1][0] eq '!' && $t->[$lo+2][0] eq '[') {
        my $end=$pairs->{$lo+2};
        my $attr=join '',map {$_->[0]} @$t[$lo+3..$end-1];
        if ($attr =~ /\Acfg\((.*)\)\z/s && test_only($1)) {
            $category='tests';
            push @{$v->{test_ranges}},[$t->[$lo][1],$t->[$hi-1][2]];
        }
        diagnostic($f,'conditional attributes require review') if $attr =~ /\Acfg_attr\(/;
        $lo=$end+1;
    }
    for (my $i=$lo;$i<$hi;) {
        my $start=$i; my $test=0; my $path;
        while ($i+1<$hi && $t->[$i][0] eq '#' && $t->[$i+1][0] eq '[') {
            my $end=$pairs->{$i+1}; last unless defined $end;
            my $attr=join '',map {$_->[0]} @$t[$i+2..$end-1];
            $test=1 if $attr =~ /\Acfg\((.*)\)\z/s && test_only($1);
            $test=1 if $attr =~ /\A(?:test|(?:tokio|async_std)::test(?:\(.*\))?)\z/;
            $path=$1 if $attr =~ /\Apath="([^"\n]+)"\z/;
            diagnostic($f,'conditional attributes require review') if $attr =~ /\Acfg_attr\(/;
            $i=$end+1;
        }
        last if $i >= $hi;
        my $j=$i;
        while ($j<$hi && $t->[$j][0] ne '{' && $t->[$j][0] ne ';') {
            if (exists $pairs->{$j}) {$j=$pairs->{$j}+1;} else {$j++;}
        }
        last if $j >= $hi;
        my $end=$t->[$j][0] eq '{' ? $pairs->{$j} : $j;
        defined($end) or die "missing Rust delimiter\n";
        my $body_end=$end;
        $end++ if $t->[$j][0] eq '{' && $end+1<$hi && $t->[$end+1][0] eq ';';
        my $kind=$test ? 'tests' : $category;
        push @{$v->{test_ranges}},[$t->[$start][1],$t->[$end][2]] if $test;
        my $head=join ' ',map {$_->[0]} @$t[$i..$j-1];
        if ($head =~ /(?:\A| )mod (?:r\#)?([A-Za-z_][A-Za-z_0-9]*)\z/) {
            my $name=$1;
            if ($t->[$j][0] eq '{') {push @$scopes,[$f,$j+1,$body_end,"$dir/$name",$kind,$follow];}
            elsif ($follow) {
                my @candidates=defined($path) ? (dirname($f)."/$path") : ("$dir/$name.rs","$dir/$name/mod.rs");
                @candidates=map {(-f $_ && !-l $_) ? abs_path($_) : $_} @candidates;
                for my $candidate (@candidates) {
                    next if exists $files{$candidate};
                    next if grep {$candidate eq $_ || index($candidate,"$_/")==0} @excluded;
                    $admitters{$v->{package}}->($candidate);
                }
                my @found=grep {exists $files{$_}} @candidates;
                if (@found==1) {visit_file($found[0],$kind,undef);}
                else {diagnostic($f,"unresolved module $name (generated, excluded or ambiguous)");}
            }
        } elsif ($head =~ /(?:\A| )include !\z/) {
            diagnostic($f,'include! source requires explicit path classification');
        } elsif ($head =~ /(?:\A| )[A-Za-z_][A-Za-z_0-9]*(?: :: [A-Za-z_][A-Za-z_0-9]*)* !\z|\bmacro_rules\b/) {
            diagnostic($f,'macro body is counted at its source; emitted modules are not expanded');
        } elsif ($t->[$j][0] eq '{') {
            push @$scopes,[$f,$j+1,$body_end,$dir,$kind,$follow];
        }
        $i=$end+1;
    }
}
sub inspect_file {
    my ($f,$category,$target)=@_;
    unless (exists $files{$f}) {diagnostic($f,'target/module source outside admitted file inventory'); return;}
    return if $visited{"$f:$category"};
    load_source($files{$f});
    my $tokens=$files{$f}{tokens};
    for (my $i=0; $i+2<@$tokens && $tokens->[$i][0] eq '#' && $tokens->[$i+1][0] eq '!' && $tokens->[$i+2][0] eq '[';) {
        my $end=$files{$f}{pair}{$i+2};
        my $attr=join '',map {$_->[0]} @$tokens[$i+3..$end-1];
        $category='tests' if $attr =~ /\Acfg\((.*)\)\z/s && test_only($1);
        $i=$end+1;
    }
    $files{$f}{contexts}{$category}=1;
    if ($visited{"$f:$category"}++) {unload_source($files{$f}); return;}
    my $dir=dirname($f);
    $dir.='/'.basename($f,'.rs') unless $target || basename($f) eq 'mod.rs';
    scan($f,0,scalar @{$files{$f}{tokens}},$dir,$category);
    unload_source($files{$f});
}
for my $p (@packages) {
    for my $target (@{$p->{targets}}) {
        my %k=map {$_=>1} @{$target->{kind}};
        my $cat=$k{'custom-build'} ? 'build_tooling' : $k{test} ? 'tests' : $k{example} ? 'examples' : $k{bench} ? 'benchmarks' : 'library_app';
        my $src=$target->{src_path};
        $src=abs_path($src) if -f $src && !-l $src;
        visit_file($src,$cat,1);
    }
}
# Module edges are queued, so only one file's token tree is resident even for
# deeply nested graphs. Shared files still visit each distinct usage category.
while (@pending) {inspect_file(@{shift @pending});}
# Unreferenced retained files remain in the maintenance footprint. Conventional
# support paths provide a category, but never pretend that they are linked code.
for my $f (sort keys %files) {
    my $v=$files{$f}; my $r=$v->{relative};
    load_source($v) unless keys %{$v->{contexts}};
    my $path_category=$r =~ m{(?:\A|/)(?:tests?/|(?:test|tests|[^/]+_tests)\.rs\z)} ? 'tests'
        : $r =~ m{\Aexamples/} ? 'examples' : $r =~ m{\Abenches/} ? 'benchmarks'
        : $r =~ m{\A(?:build\.rs\z|tools/|scripts/|xtask/)} ? 'build_tooling' : undef;
    if (defined $v->{override}) {$v->{category}=$v->{override};}
    elsif (defined $path_category && $v->{lexical_error}) {$v->{category}=$path_category;}
    elsif ($v->{lexical_error}) {$v->{category}='unclassified';}
    elsif (keys %{$v->{contexts}}) {($v->{category})=grep {$v->{contexts}{$_}} @categories;}
    elsif (defined $path_category) {$v->{category}=$path_category;}
    else {$v->{category}='unclassified'; diagnostic($f,'source not reached from a Cargo target; classify explicitly or remove');}
    $v->{shared}=keys(%{$v->{contexts}})>1 ? JSON::PP::true : JSON::PP::false;
    diagnostic($f,'conventional path category differs from Cargo/module usage')
        if defined($path_category) && keys(%{$v->{contexts}}) && $v->{category} ne $path_category && !defined($v->{override});
    diagnostic($f,'source shared across target categories; counted once') if $v->{shared};
    # Also inspect unreachable support files for inline tests.
    scan($f,0,scalar @{$v->{tokens}},dirname($f),$v->{category},0) unless keys %{$v->{contexts}};
    unload_source($v);
}
my @count_files;
for my $f (sort keys %files) {
    my $v=$files{$f};
    push @count_files,$v->{count_path};
    if ($v->{category} eq 'tests' || !@{$v->{test_ranges}}) {
        delete @{$v}{qw(text tokens pair test_ranges)};
        next;
    }
    load_source($v);
    # Merge nested/repeated ranges once, then advance through them monotonically.
    my @ranges;
    for my $range (sort {$a->[0]<=>$b->[0] || $b->[1]<=>$a->[1]} @{$v->{test_ranges}}) {
        if (@ranges && $range->[0] <= $ranges[-1][1]) {
            $ranges[-1][1]=$range->[1] if $range->[1]>$ranges[-1][1];
        } else {push @ranges, [@$range];}
    }
    my $range_cursor=0;
    my $in_test=sub {
        my ($start,$end)=@_;
        $range_cursor++ while $range_cursor<@ranges && $ranges[$range_cursor][1]<=$start;
        return $range_cursor<@ranges && $start >= $ranges[$range_cursor][0] && $end <= $ranges[$range_cursor][1];
    };
    my ($offset,$main,$tests)=(0,'',''); my $mixed=0; my $cursor=0;
    for my $line (split /(?<=\n)/,$v->{text}) {
        my $end=$offset+length($line); my @tokens;
        $cursor++ while $cursor<@{$v->{tokens}} && $v->{tokens}[$cursor][2]<=$offset;
        for (my $k=$cursor; $k<@{$v->{tokens}} && $v->{tokens}[$k][1]<$end; $k++) {push @tokens,$v->{tokens}[$k];}
        my ($inside,$outside)=(0,0);
        for my $token (@tokens) {
            if ($in_test->($token->[1],$token->[2])) {$inside=1;} else {$outside=1;}
        }
        $inside=1 if !@tokens && $in_test->($offset,$end);
        $mixed=1 if $inside && $outside;
        if ($inside && !$outside) {$tests.=$line; $main.="\n";} else {$main.=$line; $tests.="\n";}
        $offset=$end;
    }
    diagnostic($f,'mixed-category physical line counted in enclosing category') if $mixed;
    my $base="$scratch/partition-".$seq++;
    write_file("$base-main.rs",$main); write_file("$base-tests.rs",$tests);
    $v->{partitions}=["$base-main.rs","$base-tests.rs"];
    push @count_files,@{$v->{partitions}};
    delete @{$v}{qw(text tokens pair test_ranges)};
}
write_file("$scratch/files",join("\n",@count_files)."\n");
my $pid=fork(); defined($pid) or die $!;
if (!$pid) {open STDOUT,'>',"$scratch/counts.json" or die $!; exec 'cloc',"--list-file=$scratch/files",'--include-lang=Rust','--skip-uniqueness','--by-file','--json'; die "start cloc: $!\n";}
waitpid($pid,0); if ($?) {exit(($? >> 8) || 1);}
my $counts=$json->decode(read_file("$scratch/counts.json"));
sub loc {
    my $path=shift;
    unless (exists $counts->{$path}) {
        -s $path and die "cloc omitted a nonempty source file: $path\n";
        return 0;
    }
    my $n=$counts->{$path}{code};
    defined($n) && $n =~ /\A[0-9]+\z/ or die "invalid cloc count\n";
    return 0+$n;
}
my %package_counts=map {$_->{name}=>zero()} @packages; my @records;
for my $f (sort keys %files) {
    my $v=$files{$f}; my $n=loc($v->{count_path}); my $c=zero(); $c->{total}=$n;
    if ($v->{partitions}) {
        my ($a,$b)=map {loc($_)} @{$v->{partitions}};
        if ($a+$b==$n) {$c->{$v->{category}}+=$a; $c->{tests}+=$b;}
        else {$c->{unclassified}=$n; diagnostic($f,'inline partition does not reconcile with cloc; retained as unclassified');}
    } else {$c->{$v->{category}}=$n;}
    add($package_counts{$v->{package}},$c);
    push @records,{path=>$f,crate=>$v->{package},source_sha256=>$v->{source_sha256},category=>$v->{category},counts=>$c,generated=>$v->{generated},shared=>$v->{shared},target_categories=>[sort keys %{$v->{contexts}}]};
}
my $totals=zero(); add($totals,$_) for values %package_counts;
my %seen; @diagnostics=grep {!$seen{"$_->{path}\0$_->{reason}"}++} @diagnostics;
print $json->encode({schema=>2,manifest=>$manifest,crates=>[map {{crate=>$_->{name},root=>dirname($_->{manifest_path}),counts=>$package_counts{$_->{name}}}} @packages],totals=>$totals,files=>\@records,diagnostics=>\@diagnostics}),"\n";
