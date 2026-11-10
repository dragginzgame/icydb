#!/usr/bin/env perl
# Actual selector/history and Make/Shared dispatch; Cargo bodies are substitutes.
use strict;
use warnings;
use Test::More;
use JSON::PP;
use File::Temp qw(tempdir);
use File::Path qw(make_path);
use File::Copy qw(copy);
use Cwd qw(abs_path);
my $root = abs_path('.');
make_path('target');
my $fixture = tempdir('database-validation-test.XXXXXX', DIR=>"$root/target", CLEANUP=>0);
note("Fixture evidence: $fixture");
delete @ENV{qw(MAKEFLAGS MFLAGS MAKEOVERRIDES GNUMAKEFLAGS MAKEFILES GITHUB_STEP_SUMMARY)};
$ENV{GIT_CONFIG_NOSYSTEM}=1;
$ENV{GIT_CONFIG_GLOBAL}='/dev/null';
sub run {
    system(@_) == 0 or die "fixture command failed: @_\n";
}
sub write_file {
    my ($path, $raw)=@_;
    my $parent=$path; $parent =~ s{/[^/]+$}{};
    make_path($parent) if $parent ne $path;
    open my $out, '>:raw', $path or die "$path: $!";
    print {$out} $raw; close $out or die "close $path";
}
sub capture {
    open my $in, '-|', @_ or die "start fixture observation";
    local $/; my $raw=<$in>;
    close $in or die "fixture observation failed: @_";
    return $raw;
}
delete @ENV{split /\n/, capture('git','rev-parse','--local-env-vars')};
my $repo="$fixture/repo";
make_path("$repo/scripts/ci");
for my $name (qw(database-validation-plan.pl validation-evidence.pl ValidationPlan.pm validation-input-checkpoint.pl)) {
    copy("$root/scripts/ci/$name", "$repo/scripts/ci/$name") or die "copy $name: $!";
}
chdir $repo or die "chdir fixture";
run('git', 'init', '-q');
run('git', 'config', 'user.email', 'fixture@example.invalid');
run('git', 'config', 'user.name', 'Fixture');
run('git', 'config', 'commit.gpgsign', 'false');
run('git', 'config', 'tag.gpgsign', 'false');
run('git', 'config', 'core.hooksPath', '/dev/null');
write_file('.gitignore', "/target/\n");
my $parser='crates/icydb-core/src/db/sql/parser/clauses.rs';
my $planner='crates/icydb-core/src/db/query/plan/access_plan.rs';
write_file($_, "// baseline\n") for ($parser,$planner,'Cargo.lock','crates/icydb-core/src/db/index/store.rs');
run('git', 'add', '.'); run('git', 'commit', '-qm', 'baseline');
run('git', 'tag', '-am', 'baseline', 'database-v1.0.0');
my $base=capture('git','rev-parse','HEAD'); chomp $base;
sub selection {
    my ($tag,$scope)=@_;
    return decode_json(capture('perl','scripts/ci/database-validation-plan.pl', $tag//'database-v1.0.0', $scope//'auto'));
}
sub change {
    run('git','checkout','-q','--detach',$base);
    for my $path (@_) { write_file($path, "// changed\n"); }
    run('git','add','.'); run('git','commit','-qm','change');
}
is(selection()->{target}, 'validate', 'no source delta stays full');
change($parser);
is(selection()->{target}, 'ci-database-parser', 'parser selects its bounded acceptance');
ok(!selection()->{release_omission_admitted}, 'feedback cannot admit release omission');
is(selection('database-v1.0.0','workspace')->{target}, 'validate', 'explicit workspace retains all checks');
is(selection('database-v9.0.0')->{target}, 'validate', 'missing baseline stays full');
run('git','tag','database-v1.0.1');
is(selection('database-v1.0.1')->{target}, 'validate', 'lightweight baseline stays full');
my $tree=capture('git','rev-parse','HEAD^{tree}'); chomp $tree;
my $unrelated=capture('git','commit-tree',$tree,'-m','unrelated'); chomp $unrelated;
run('git','tag','-am','nonancestor','database-v1.0.2',$unrelated);
is(selection('database-v1.0.2')->{target}, 'validate', 'non-ancestor baseline stays full');
write_file($parser,"// dirty\n");
is(selection()->{target}, 'validate', 'dirty input stays full');
run('git','restore',$parser);
write_file('untracked.rs','// unknown');
is(selection()->{target}, 'validate', 'untracked input stays full');
unlink 'untracked.rs' or die 'remove fixture input';
change($planner);
is(selection()->{target}, 'ci-database-planner', 'planner selects its bounded acceptance');
change($parser,$planner);
is(selection()->{target}, 'ci-database-query', 'parser and planner share explicit cross-subsystem acceptance');
change('releases/database/CHANGELOG.md');
is(selection()->{target}, 'validate', 'notes alone cannot establish subsystem scope');
for my $path ('Cargo.lock','crates/icydb-core/src/db/index/store.rs','unknown.rs',
    'crates/icydb-core/src/db/sql/mod.rs','crates/icydb-core/Cargo.toml') {
    change($parser,$path);
    my $selected=selection();
    is($selected->{target}, 'validate', "$path broadens scope");
    is($selected->{fallback_path},$path,'fallback identifies the triggering input');
}
# Real Make and Shared runner, with only Cargo compilation/test bodies replaced.
# A separate physical root keeps these substitute controls from inheriting the
# caller's product-wide checkpoint. The outer gate still guards product inputs.
chdir $root or die 'restore root';
my $dispatch_root="$fixture/dispatch";
make_path($dispatch_root);
run('bash', "$root/scripts/ci/prepare-script-fixture.sh", $root, $dispatch_root,
    'Makefile', 'make/consumer.mk', 'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml',
    'qualification/Cargo.toml', 'qualification/Cargo.lock',
    'scripts/ci/actionlint-checksums.tsv', 'scripts/ci/run-icydb-validation-targets.sh',
    'scripts/ci/run-validation-targets.sh', 'scripts/ci/check-rust-compiler.sh',
    'scripts/ci/read-cargo-workspace-rust-version.sh', 'scripts/ci/run-nonempty-cargo-test.sh');
make_path("$fixture/bin");
my $cargo=capture('sh','-c','command -v cargo'); chomp $cargo;
write_file("$fixture/bin/cargo", <<'CARGO');
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == -V || "$1" == --version ]]; then exec "$REAL_CARGO" "$@"; fi
printf '%s\n' "$*" >> "$DATABASE_TEST_TRACE"
printf '%s\n' "$PWD" >> "$DATABASE_TEST_ROOT_TRACE"
if [[ -n "${DATABASE_FAIL_FILTER:-}" && "$*" == *"$DATABASE_FAIL_FILTER"* ]]; then exit 17; fi
if [[ "$1" == test ]]; then
    printf 'test result: ok. %s passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n' "${DATABASE_PASS_COUNT:-1}"
fi
CARGO
chmod 0755,"$fixture/bin/cargo" or die 'chmod substitute';
local $ENV{PATH}="$fixture/bin:$ENV{PATH}";
local $ENV{REAL_CARGO}=$cargo;
local $ENV{DATABASE_TEST_TRACE}="$fixture/trace";
local $ENV{DATABASE_TEST_ROOT_TRACE}="$fixture/roots";
local $ENV{VALIDATION_LOG_DIR}="$fixture/runs";
local $ENV{YQ}=$ENV{YQ}//"$root/.tools/host/bin/yq";
# An invalid checkpoint for the parent must not taint the disposable caller.
# Accidentally dispatching from the product root now refuses before Cargo runs.
local $ENV{VALIDATION_INPUT_ROOT}=$root;
local $ENV{VALIDATION_INPUT_BASELINE}="$fixture/missing-parent-checkpoint";
chdir $dispatch_root or die 'enter bounded dispatch fixture';
my @acceptance=('db::sql::lowering::','db::session::tests::cardinality_tiebreak::',
    'tier_a_native_sqlite_reference','tier_a_native_mutation_reference',
    '-p icydb --lib --no-default-features --features sql db::sql::');
sub dispatch {
    my ($target,$label)=@_;
    write_file($ENV{DATABASE_TEST_TRACE},'');
    write_file($ENV{DATABASE_TEST_ROOT_TRACE},'');
    open my $saved_out, '>&', \*STDOUT or die 'save stdout';
    open my $saved_err, '>&', \*STDERR or die 'save stderr';
    open STDOUT, '>', "$fixture/$label.log" or die 'redirect stdout';
    open STDERR, '>&', \*STDOUT or die 'redirect stderr';
    my $status=system('make','--no-print-directory',$target);
    open STDOUT, '>&', $saved_out or die 'restore stdout';
    open STDERR, '>&', $saved_err or die 'restore stderr';
    return $status;
}
for my $scope (qw(parser planner query)) {
    is(dispatch("ci-database-$scope",$scope),0,"$scope real dispatch succeeds with substitute Cargo");
    my $trace=capture('cat',$ENV{DATABASE_TEST_TRACE});
    my %roots=map {$_=>1} split /\n/,capture('cat',$ENV{DATABASE_TEST_ROOT_TRACE});
    is_deeply([sort keys %roots],[$dispatch_root],'substitute Cargo stays in its bounded physical root');
    like($trace,qr/\Aclippy .* -D warnings\n/,'Clippy precedes tests');
    for my $filter (@acceptance) {
        is(scalar(grep { index($_,$filter)>=0 } split /\n/,$trace),1,"acceptance runs once: $filter");
    }
    like($trace,qr/db::sql::parser::/,'parser tests selected') unless $scope eq 'planner';
    like($trace,qr/db::query::plan::/,'planner tests selected') unless $scope eq 'parser';
    unlike($trace,qr/db::sql::parser::/,'planner-only omits parser unit tests') if $scope eq 'planner';
    unlike($trace,qr/db::query::plan::/,'parser-only omits planner unit tests') if $scope eq 'parser';
}
for my $filter ('clippy','db::sql::parser::','db::query::plan::',@acceptance) {
    local $ENV{DATABASE_FAIL_FILTER}=$filter;
    isnt(dispatch('ci-database-query','failed-'.($filter =~ s/[^a-z]+/-/gr)),0,"required $filter failure blocks");
    my @commands=split /\n/,capture('cat',$ENV{DATABASE_TEST_TRACE});
    like($commands[-1],qr/\Q$filter\E/,'no later tests after failure');
}
{
    local $ENV{DATABASE_PASS_COUNT}=0;
    isnt(dispatch('ci-database-query','empty-tests'),0,'empty test selection refuses');
}
done_testing();
