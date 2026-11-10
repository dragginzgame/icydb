#!/usr/bin/env perl
# Shared companions: scripts/ci/validate-release-inputs.pl scripts/ci/check-make-execution.sh scripts/ci/check-retained-state.pl
use strict;
use warnings;
use Cwd qw(abs_path);
use File::Path qw(make_path remove_tree);
use File::Temp qw(tempdir);
use Fcntl qw(:flock);
use JSON::PP;
use Getopt::Long qw(GetOptions);
use FindBin qw($RealBin);

my ($root,$groups,$resume,$publish,$publication_result,$setup,$status,$reconcile,$retire);
my $delivery=$ENV{RELEASE_DELIVERY}//'direct';
my ($kind,$remote,$branch)=('patch','origin','main');
GetOptions('root=s'=>\$root,'groups=s'=>\$groups,'resume=s'=>\$resume,
    'kind=s'=>\$kind,'remote=s'=>\$remote,'branch=s'=>\$branch,
    'publish-target=s'=>\$publish,'delivery=s'=>\$delivery,
    'publication-result=s'=>\$publication_result,'setup-target=s'=>\$setup,
    'status=s'=>\$status,'reconcile=s'=>\$reconcile,'retire=s'=>\$retire) && !@ARGV
    or die "usage: run-release-batch.pl --root DIR --groups GROUP[,GROUP...] [--kind patch|minor|major] [--setup-target TARGET] [--publish-target TARGET] | --root DIR --resume|--status|--reconcile|--retire ID\n";
$root=abs_path($root//'') // die "missing repository root\n";
my @operations=grep {defined} ($resume,$status,$reconcile,$retire);
@operations<=1 or die "select one batch operation\n";
my $operation=defined($status)?'status':defined($reconcile)?'reconcile':defined($retire)?'retire':'resume';
$resume=$operations[0] if @operations;
(!defined($publication_result) || $operation eq 'resume') or die "publication reconciliation belongs to resume\n";
$delivery eq 'direct' or die "release batches currently require direct delivery; PR policy is not overridden\n";
!defined($publication_result) || (defined($resume) && $publication_result =~ /\A(?:published|not-published)\z/)
    or die "publication-result requires resume and verified published or not-published status\n";
-d $root or die "missing repository root\n";
for (qw(GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES CARGO_TARGET_DIR CARGO_HOME)) {
    !exists($ENV{$_}) or die "release batch requires ordinary context; unset $_ or qualify it in the consumer adapter\n";
}
for (qw(RELEASE_FROZEN_GROUPS RELEASE_FROZEN_PUBLISH_TARGET RELEASE_FROZEN_SETUP_TARGET RELEASE_FROZEN_SETUP_MODE FROZEN_RELEASE_ACTIVE)) {
    !exists($ENV{$_}) or die "retired release setting $_; adopt https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/releases/release-batches.md\n";
}
sub output {
    my (@args)=@_;
    open my $fh,'-|',@args or die "cannot start $args[0]: $!\n";
    local $/; my $text=<$fh>//'';
    close $fh or die "command failed: @args\n";
    $text =~ s/\n\z//;
    return $text;
}
sub run { system @_ ; $? == 0 or die "command failed ($?): @_\n"; }
run('bash',"$RealBin/check-make-execution.sh",'make');
sub regular_route {
    my ($path)=@_;
    $path =~ m{\A/} && $path !~ /[\r\n\0]/ or die "invalid absolute route\n";
    my $cursor='';
    for (split '/', $path) {next unless length; $cursor.="/$_"; !-l $cursor or die "symlink route: $cursor\n";}
}
sub write_json {
    my ($path,$value)=@_; regular_route($path);
    regular_route("$path.new");
    open my $fh,'>',"$path.new" or die $!;
    print {$fh} JSON::PP->new->canonical->encode($value),"\n" or die $!;
    close $fh or die $!; rename "$path.new",$path or die $!;
}
sub read_json {
    my ($path)=@_; regular_route($path);
    open my $fh,'<',$path or die $!; local $/;
    my $value=JSON::PP->new->decode(<$fh>); close $fh or die $!; return $value;
}
output('git','-C',$root,'rev-parse','--show-toplevel') eq $root or die "select the repository root\n";
my $common=output('git','-C',$root,'rev-parse','--git-common-dir');
$common=abs_path("$root/$common") unless $common =~ m{\A/};
my $state="$common/release-state/frozen";
regular_route($state); make_path($state);
# One durable queue per normal checkout. Groups in a batch execute sequentially;
# unrelated Cargo writes never need to acquire this lock.
regular_route("$state/lock");
open my $lock,'>>',"$state/lock" or die $!;
print "Waiting for release batch queue: $state\n";
flock($lock,LOCK_EX) or die $!;
run($^X,"$RealBin/check-retained-state.pl",'--root',$state) if $operation eq 'resume';
my ($record,$id);
if (defined $resume) {
    !defined($groups) && !defined($publish) && !defined($setup) or die "batch operations use the saved hook and group roster\n";
    $resume =~ /\Abatch-[A-Za-z0-9_]+\z/ or die "invalid batch identity\n";
    $id=$resume; $record=read_json("$state/$id/intent.json");
} else {
    $kind =~ /\A(?:patch|minor|major)\z/ or die "invalid release kind\n";
    $remote =~ /\A[A-Za-z0-9._-]+\z/ or die "invalid remote\n";
    run('git','check-ref-format',"refs/heads/$branch");
    output('git','-C',$root,'symbolic-ref','--short','HEAD') eq $branch or die "selected branch is not checked out\n";
    output('git','-C',$root,'status','--porcelain','--untracked-files=all') eq '' or die "start a batch from clean committed source; later edits are deferred\n";
    my @groups=split /,/,($groups//'');
    @groups or die "select all groups for this batch in supplier-first order\n";
    my %seen;
    for (@groups) { /\A[A-Za-z0-9][A-Za-z0-9_-]*\z/ && !$seen{$_}++ or die "invalid or duplicate group\n"; }
    if (defined $publish) { $publish =~ /\A[a-z][a-z0-9-]*\z/ or die "invalid publication target\n"; }
    if (defined $setup) { $setup =~ /\A[a-z][a-z0-9-]*\z/ or die "invalid setup target\n"; }
    my $source=output('git','-C',$root,'rev-parse','HEAD');
    my $url=output('git','-C',$root,'remote','get-url','--push','--all',$remote);
    length($url) && $url !~ /[\r\n]/ or die "one push destination required\n";
    my $dir=tempdir('batch-XXXXXXXX',DIR=>$state,CLEANUP=>0); ($id)=$dir =~ m{/([^/]+)\z};
    $record={schema=>2,root=>$root,source=>$source,kind=>$kind,remote=>$remote,branch=>$branch,
        destination=>$url,groups=>\@groups,publish=>$publish,setup=>$setup,index=>0,phase=>'initialize'};
    # Protect the base and its ancestors while the candidate borrows their objects.
    run('git','-C',$root,'update-ref',"refs/releases/frozen/$id",$source);
    write_json("$dir/intent.json",$record);
}
ref($record) eq 'HASH' && ($record->{schema}//0)==2
    && ($record->{root}//'') eq $root
    && ($record->{source}//'') =~ /\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z/
    && ($record->{kind}//'') =~ /\A(?:patch|minor|major)\z/
    && ($record->{remote}//'') =~ /\A[A-Za-z0-9._-]+\z/
    && ($record->{phase}//'') =~ /\A(?:initialize|release|releasing|publish|publishing|complete)\z/
    && ref($record->{groups}) eq 'ARRAY' && @{$record->{groups}}
    && ($record->{index}//'') =~ /\A[0-9]+\z/ && $record->{index}<=@{$record->{groups}}
    && length($record->{destination}//'') && $record->{destination} !~ /[\r\n\0]/
    or die "invalid saved batch intent; pre-0.31 batches require their original tooling\n";
my %unique;
for (@{$record->{groups}}) { /\A[A-Za-z0-9][A-Za-z0-9_-]*\z/ && !$unique{$_}++ or die "invalid saved group\n"; }
!defined($record->{publish}) || $record->{publish} =~ /\A[a-z][a-z0-9-]*\z/ or die "invalid saved publication target\n";
!defined($record->{setup}) || $record->{setup} =~ /\A[a-z][a-z0-9-]*\z/ or die "invalid saved setup target\n";
my $count=@{$record->{groups}};
($record->{phase} ne 'complete' || $record->{index}==$count)
    && ($record->{phase} ne 'initialize' || $record->{index}==0)
    && ($record->{phase} !~ /\A(?:releasing|publish|publishing)\z/ || $record->{index}<$count)
    && ($record->{phase} ne 'publishing' || defined($record->{publish}))
    or die "inconsistent batch phase and group index\n";
if ($record->{phase} eq 'complete') {
    ($record->{commit}//'') =~ /\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z/ or die "missing completion identity\n";
}
if ($record->{phase} =~ /\A(?:publish|publishing)\z/) {
    ($record->{release_commit}//'') =~ /\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z/
        or die "missing recorded publication identity; never infer it from current HEAD\n";
}
run('git','check-ref-format',"refs/heads/$record->{branch}");
if ($record->{phase} =~ /\A(?:releasing|publish|publishing)\z/) {
    ($record->{candidate}//'') =~ /\A[0-9]+\.[0-9]+\.[0-9]+\z/
        && ($record->{group_source}//'') =~ /\A(?:[0-9a-f]{40}|[0-9a-f]{64})\z/ or die "invalid saved candidate\n";
}
my $dir="$state/$id";
my $work="$root/target/release-frozen/$id";
my $gitdir="$dir/repository.git";
regular_route($_) for ($dir,$work,$gitdir);
print "Release batch $id: $record->{source}; groups: ",join(', ',@{$record->{groups}}),"\n";
print "Delivery checkout: $work; durable recovery: $dir\n";
sub save { write_json("$dir/intent.json",$record); }
if ($operation eq 'status') {
    print JSON::PP->new->canonical->pretty->encode($record);
    exit 0;
}
if ($operation eq 'reconcile' || $operation eq 'retire') {
    $record->{phase} eq 'complete' or die "only completed batches may be reconciled or retired\n";
    output('git','-C',$root,'symbolic-ref','--short','HEAD') eq $record->{branch}
        or die "select the batch branch before reconciliation\n";
    output('git','-C',$root,'status','--porcelain','--untracked-files=all') eq ''
        or die "deferred edits remain: preserve/commit them before reconciliation; no automatic stash or reset\n";
    output('git','--git-dir',$gitdir,'rev-parse','HEAD') eq $record->{commit}
        or die "candidate changed after completion\n";
    run('git','--git-dir',$gitdir,'diff','--cached','--quiet','HEAD','--');
    if (-d $work) {
        output('git','-C',$work,'rev-parse','--absolute-git-dir') eq $gitdir or die "candidate Git directory changed\n";
        output('git','-C',$work,'status','--porcelain','--untracked-files=all') eq '' or die "candidate has pending edits\n";
    }
    # Import objects and tags before any removal. Ref conflicts refuse without
    # force, and merge --ff-only never overwrites deferred normal-tree work.
    run('git','-C',$root,'fetch','--no-write-fetch-head','--no-tags',$gitdir,
        "refs/heads/$record->{branch}", 'refs/tags/*:refs/tags/*');
    if ($operation eq 'reconcile') {
        run('git','-C',$root,'merge','--ff-only',$record->{commit});
        print "Reconciled $id; deferred updates can now be reapplied.\n";
        exit 0;
    }
    run('git','-C',$root,'merge-base','--is-ancestor',$record->{commit},'HEAD');
    my $heads=output('git','--git-dir',$gitdir,'for-each-ref','--format=%(refname)','refs/heads/');
    $heads eq "refs/heads/$record->{branch}" or die "candidate has additional branches; preserve them before retirement\n";
    my $refs=output('git','--git-dir',$gitdir,'for-each-ref','--format=%(refname)');
    for (split /\n/,$refs) {
        next if m{\Arefs/(?:heads|tags)/};
        m{\Arefs/remotes/} or die "candidate has additional refs; preserve them before retirement: $_\n";
        my $tracked=output('git','--git-dir',$gitdir,'rev-parse',"$_^{commit}");
        run('git','-C',$root,'merge-base','--is-ancestor',$tracked,'HEAD');
    }
    if (-d $work) {
        my $ignored=output('git','-C',$work,'ls-files','--others','--ignored','--exclude-standard');
        for (split /\n/,$ignored) { m{\A(?:target|\.tools)/} or die "non-build ignored file requires preservation: $_\n"; }
        remove_tree($work,{error=>\my $errors}); @$errors==0 or die "candidate cleanup failed; durable state retained\n";
    }
    run('git','-C',$root,'update-ref','-d',"refs/releases/frozen/$id",$record->{source});
    remove_tree($dir,{error=>\my $errors}); @$errors==0 or die "batch cleanup incomplete\n";
    print "Retired $id; release commits and tags are retained in the normal repository.\n";
    exit 0;
}
if (defined $publication_result) {
    $record->{phase} eq 'publishing' or die "publication reconciliation requires uncertain publication\n";
    # Explicit caller assertion after consulting the consumer's registry receipts.
    # Never infer absence from a network error or automatically retry an upload.
    if ($publication_result eq 'published') {
        $record->{index}++; $record->{phase}='release';
    } else { $record->{phase}='publish'; }
    $record->{publication_reconciliation}=$publication_result; save();
}
if ($record->{phase} eq 'initialize') {
    !-e $gitdir && !-e $work or die "interrupted initialization retained; inspect before retrying\n";
    make_path("$root/target/release-frozen");
    run('git','clone','--shared','--no-checkout','--separate-git-dir',$gitdir,$root,$work);
    run('git','-C',$work,'checkout','-B',$record->{branch},$record->{source});
    run('git','-C',$work,'remote','remove','origin');
    run('git','-C',$work,'remote','add',$record->{remote},$record->{destination});
    for my $key ('user.name','user.email') {
        my $value=output('git','-C',$root,'config','--get',$key);
        run('git','-C',$work,'config',$key,$value);
    }
    $record->{phase}='release'; save();
}
-d $work && -f "$work/.git" or die "candidate checkout missing; recover from $gitdir before resume (do not replace its durable Git state)\n";
output('git','-C',$work,'rev-parse','--absolute-git-dir') eq $gitdir or die "candidate Git directory changed\n";
output('git','-C',$work,'remote','get-url','--push','--all',$record->{remote}) eq $record->{destination} or die "candidate destination changed\n";
# Reparse each candidate's own Makefile, without caller command-line overrides
# pointing back into the normal checkout. Consumer configuration belongs in Git.
delete @ENV{qw(MAKEFLAGS MFLAGS MAKEOVERRIDES GNUMAKEFLAGS MAKEFILES)};
if (defined $ENV{TOKO_TOOLING_ROOT}) {
    my $shared=abs_path($ENV{TOKO_TOOLING_ROOT}) // die "missing toko tooling root\n";
    $shared eq $root || index($shared,"$root/")==0 or die "toko tooling must be snapshot-owned\n";
    $ENV{TOKO_TOOLING_ROOT}=$work.substr($shared,length($root));
}
# Consumer adoption must qualify the real hooks in this independent checkout.
# Do not copy live locks, configuration, tools or evidence from the normal tree.
sub hook_context {
    my ($group)=@_;
    return ("RELEASE_GROUP=$group","RELEASE_REMOTE=$record->{remote}","RELEASE_BRANCH=$record->{branch}",
        "RELEASE_KIND=$record->{kind}","RELEASE_SOURCE=".($record->{group_source}//$record->{source}),
        "RELEASE_VERSION=".($record->{candidate}//''),"RELEASE_COMMIT=".($record->{release_commit}//''),
        'RELEASE_DELIVERY=direct','RELEASE_BATCH_ACTIVE=1');
}
sub candidate_source_state {
    return JSON::PP->new->encode([
        output('git','-C',$work,'rev-parse','HEAD'),
        output('git','-C',$work,'diff','--binary'),
        output('git','-C',$work,'diff','--cached','--binary'),
        output('git','-C',$work,'ls-files','--others','--exclude-standard','-z')]);
}
while ($record->{index}<@{$record->{groups}}) {
    my $group=$record->{groups}[$record->{index}];
    if ($record->{phase} eq 'release') {
        delete @{$record}{qw(candidate release_commit)};
        $record->{group_source}=output('git','-C',$work,'rev-parse','HEAD');
    }
    if (defined($record->{setup}) && $record->{phase} ne 'publishing') {
        my $setup_mode=$record->{phase} eq 'release'?'prepare':'check';
        my $inputs=candidate_source_state();
        run('make','-C',$work,'--no-print-directory',$record->{setup},hook_context($group),
            "RELEASE_SETUP_MODE=$setup_mode");
        candidate_source_state() eq $inputs or die "candidate setup changed release source or lock inputs\n";
    }
    if ($record->{phase} eq 'release' || $record->{phase} eq 'releasing') {
        my $target="release-$record->{kind}";
        my @version;
        if ($record->{phase} eq 'release') {
            my $previous=output('make','-C',$work,'--no-print-directory','-s','release-version',hook_context($group));
            $record->{candidate}=output($^X,"$RealBin/validate-release-inputs.pl",'next-version',$previous,$record->{kind});
            $record->{group_source}=output('git','-C',$work,'rev-parse','HEAD');
            $record->{phase}='releasing'; save();
        } else {
            my $plan="$gitdir/release-state/$group-$record->{candidate}.plan";
            regular_route($plan);
            if (-f $plan) {
                $target='release-resume'; @version=("VERSION=$record->{candidate}");
            } else {
                output('git','-C',$work,'rev-parse','HEAD') eq $record->{group_source}
                    && output('git','-C',$work,'status','--porcelain','--untracked-files=all') eq ''
                    && output('git','-C',$work,'tag','--list',"$group-v$record->{candidate}") eq ''
                    or die "missing release plan after candidate mutation; reconcile retained state before resume\n";
            }
        }
        run('make','-C',$work,'--no-print-directory',$target,@version,hook_context($group));
        $record->{release_commit}=output('git','-C',$work,'rev-parse','HEAD');
        $record->{phase}='publish'; save();
    }
    if ($record->{phase} eq 'publish') {
        if (defined $record->{publish}) {
            output('git','-C',$work,'rev-parse','HEAD') eq $record->{release_commit}
                or die "candidate changed before publication; reconcile release identity\n";
            $record->{phase}='publishing'; save();
            run('make','-C',$work,'--no-print-directory',$record->{publish},hook_context($group));
        }
        $record->{index}++; $record->{phase}='release'; save();
    } elsif ($record->{phase} eq 'publishing') {
        die "publication outcome uncertain: verify registry/receipts in $work, then resume with --publication-result published|not-published\n";
    }
}
$record->{phase}='complete'; $record->{commit}=output('git','-C',$work,'rev-parse','HEAD'); save();
print "Batch delivery complete: $record->{commit}\n";
print "Normal checkout was not modified. Any cargo update or edits there belong to the next candidate.\n";
print "Reconcile its branch/lockfile with the delivered commit before the next batch; never reset away deferred work.\n";
