# Focused verification helpers

These optional helpers share mechanical checks. Consumers retain document
selection, release validation gates, publication authority and registry polling
policy. They require Perl core modules or Bash 3.2+, as noted below, and run on
Linux and macOS. The portable regression suite includes offline fixtures;
native CI qualifies each supported host separately.

## Cargo inheritance and workspace version

```bash
bash scripts/ci/check-dependency-pins.sh --consumer /path/to/repo --cargo-inheritance
bash scripts/ci/read-cargo-workspace-version.sh --stable /path/to/repo/Cargo.toml
```

The additive `--cargo-inheritance` check reuses the pin checker's Git inventory,
TOML parser and offline Cargo workspace discovery. It checks every inventoried
Cargo manifest against its owning root: member package versions inherit
`workspace.package.version`; ordinary, development, build and target dependencies
inherit catalog entries by alias. Root packages also inherit their dependencies.
Child dependencies may select only `features`, `optional` and `default-features`
alongside `workspace = true`. Versions, paths, Git/registry sources and renamed
package identities stay in the root catalog. Both inline and ordinary TOML tables
are parsed structurally. Existing pin-only callers retain their current scope.

Nested workspace discovery does not approve a governance exception: consumers
must still document their authorized independent roots. Ignored files are outside
the Git inventory. Product dependency bans, lock graph constraints and feature
qualification remain local. Adopt the option in CI/release callers before
retiring their equivalent inheritance checks.

The version reader requires Cargo, jq and Mike Farah yq v4.47.2+. It accepts one
explicit `Cargo.toml` path and prints its `workspace.package.version` as canonical
SemVer, including prerelease/build components unless `--stable` is selected.
It rejects missing/non-string versions and malformed manifests, with no accepted
version on failure. Cargo's offline `locate-project --workspace` validates the
manifest first, including duplicate keys that yq alone accepts. It neither
resolves dependencies nor builds; the selected Cargo toolchain must be prepared.
Keep root-package targets available when reading an exported manifest, since
Cargo also checks package structure. The reader never chooses a Git commit,
updates a version or lockfile, or finalizes release notes. Consumers own selection
of working versus committed sources and their release/preparation transactions.

## CI binary installers

The actionlint, ShellCheck, gitleaks and sccache entry points delegate to
`scripts/ci/install-ci-tool.sh`. Their existing version, SHA-256 and installation
directory arguments are unchanged. The implementation shares host selection,
HTTPS download, checksum admission, extraction, exact version admission and
publication. Asset names and version-output formats remain explicit per tool.
Staging lives on the destination filesystem; failures retain the candidate and
leave the installed executable intact. Successful installation removes its own
staging files. This does not merge repository-local host/IC bundle activation
or change any consumer's pins. Include the internal helper and checksum verifier
in snapshots with any of these entry points.

`install-sccache.sh` uses the same explicit version, SHA-256 and installation
directory arguments. Its reviewed asset scope is Linux x86-64, using the
`sccache-vVERSION-x86_64-unknown-linux-musl.tar.gz` release asset. Other hosts
are rejected before installation or download; consumers retain their explicit
Cargo/native setup on those hosts. Extending binary asset selection requires
reviewed pins and corresponding qualification, not guessed download names.
The installer prints the selected executable path; callers own `GITHUB_PATH`,
`RUSTC_WRAPPER`, cache configuration and any server startup. Include this entry
point, `install-ci-tool.sh` and `verify-file-checksum.sh` in the same snapshot.

## Compiler-cache launcher adoption

`scripts/ci/run-sccache.sh` owns stable, repository-scoped temporary files and
socket selection for a cache server that outlives one validation invocation.
It forwards the exact arguments, diagnostics and exit status to `SCCACHE_BIN`;
it does not select Cargo's compiler wrapper or retry compiler failures.
It refuses a symlinked repository `.tmp`, selected runtime root or `tmp` child
before creating runtime directories. An explicit `SCCACHE_RUNTIME_DIR` owns its
chosen location; its ancestors follow normal filesystem resolution (including
system `/tmp` aliases on macOS). Choose trusted parents and do not mutate those
paths concurrently with launch. This is admission ordering, not a race-proof
filesystem sandbox.

Consumers with an existing cache-failure adapter can select that executable as
`SCCACHE_BIN`, keeping the actual cache executable under a separate local
selection. The shared launcher establishes the runtime before executing the
adapter, which inherits `TMPDIR` and `SCCACHE_SERVER_UDS`. The adapter must not
call the launcher recursively or reselect/delete its stable runtime directory.
Keep fallback classification and diagnostic policy local until independently
qualified for the selected cache version; text resembling a cache diagnostic
does not by itself establish that a compiler has not already run.

During adoption, replace duplicated directory/socket setup and retain focused
checks for exact argv, compiler failure without replay, cache-management calls,
adapter failure status and runtime survival after invocation scratch cleanup.
Snapshot adoption does not change consumer toolchain or compiler-wrapper policy.

## Local documentation links

```sh
perl scripts/ci/check-documentation-links.pl --root /path/to/repo README.md docs/guide.md
```

Supply a root and at least one document. Relative document arguments resolve
from that root; absolute document arguments are also accepted. Relative links
resolve from their containing document. Absolute link paths retain filesystem
semantics: `/guide.md` means the host's `/guide.md`, not the repository root.
Use relative links for repository navigation. Targets outside the root are not
prohibited; this is an existence check, not a containment boundary.

The Perl checker handles ordinary inline links, images and reference definitions,
angle-bracket paths with spaces, percent-encoded filenames and optional titles.
It removes query/fragment suffixes before checking existence, skips URI schemes,
protocol-relative URLs and anchor-only links, and ignores backtick/tilde fenced
examples and inline backtick code. Both files and directories may be targets.
Missing input documents or local targets fail the command.

This deliberately follows IcyDB's lightweight checker. It does not validate
anchors, network availability, reference-label resolution or a complete Markdown
renderer grammar (for example, nested/escaped parentheses in bare destinations,
HTML links and indented code blocks). Use angle brackets around complex paths.
A passing check only covers the supported local references in the supplied
roster. Product-specific inventories, version examples and document selection
remain consumer-owned.

Shared Tooling's `make check-doc-links` selects root, audit, rule and guide
Markdown files and runs in CI and the release gate. Consumers choose their own
roster; snapshot-relative links must exist in the adopted file set.

## Standard release command adoption

```sh
bash scripts/ci/check-release-commands.sh /path/to/repo tool-versions.env
```

The root's Makefile is copied into a temporary checkout without Git metadata.
Additional arguments select reviewed, repository-relative files needed while
Make parses that copy. For example, Canic includes `tool-versions.env`; IcyDB
reads `scripts/ci/actionlint-checksums.tsv`. No files are implicitly discovered
or sourced from siblings. The copied Makefile must call
`scripts/ci/run-release.sh` relative to its fixture checkout (directly or through
Bash). That path contains a substitute that records exact arguments and returns
a chosen status. GNU Make and Bash are prerequisites.

The checker exercises patch/minor/major routing, resume with an explicit version,
explicit remote/branch propagation, runner success and failure propagation,
and rejection of every pair of different release targets before dispatch.
It clears inherited Make controls and validation logger identity so nested
validation cannot redirect or skip the checks. Success removes its temporary
fixture; failure prints and retains its path with separate command logs and
argument evidence.

Review the selected Makefile's parse-time expressions, includes, prerequisites
and recipes before invoking this helper. They must be read-only except for the
substituted runner. This is not a sandbox for arbitrary Makefiles: absolute
commands or unrelated prerequisites could still have effects. It verifies the
entry-point contract, not release execution, Git reconciliation, receipts,
publication or caches. Keep those consumer-specific tests alongside a thin
call to this helper, and keep the shared runner's own fixtures upstream.

Shared Tooling exercises its own Makefile in the portable suite; the focused
`make check-release-commands` target offers the same adoption check.

## Exact crates.io version observation

```sh
status=0
bash scripts/ci/check-crates-io-version.sh my-crate 0.1.2 || status=$?
case "$status" in
    0) echo 'Already present' ;;
    1) echo 'Absent; the caller may consider separately authorized publication' ;;
    2) echo 'Observation unavailable or invalid; stop' >&2; exit 2 ;;
esac
```

The Bash/curl helper accepts one crate name and one canonical stable `X.Y.Z`
version. Prerelease/build versions, leading-zero components, malformed names and
extra arguments are rejected. Its exit contract is **0 present (HTTP 200),
1 absent (HTTP 404), 2 unavailable or invalid**. Transport failure wins over any
HTTP-looking output; rate limits, authorization failures, server failures and
unexpected responses are unavailable. Diagnostics go to stderr; stdout is empty.
Call it in a conditional or capture its status as above when using `set -e`.
Never use a blanket nonzero test to decide that publication is safe.

One request targets the fixed crates.io API, with HTTPS-only redirects, a
10-second connection timeout and 30-second total timeout. It disables implicit
curl configuration, supplies no credentials, and neither polls nor retries.
It does not run Cargo, inspect dependencies, publish or clean artifacts.
A present version does not prove that a new consumer can resolve every dependency.
Post-publication waiting, time budgets and package ordering stay in the caller;
an inconclusive result must stop that flow rather than trigger publication.

## Snapshot adoption

The additional lockfile transformer and formatting adoption checker below are
optional snapshot files too. They retain consumer policy at their input boundary;
do not vendor their upstream fixtures as product tests.

After these files have a reviewed committed revision, adopt each desired helper
and this guide through the [snapshot workflow](consuming-snapshots.md). The
documentation, release-command and registry helpers have no dependencies on
other shared scripts. Their upstream fixtures
are `test-documentation-links.pl`, `test-release-commands.sh` and
`test-crates-io-version.sh`; the release fixture additionally uses the upstream
Makefile and is not a consumer's product test.

Replace only the corresponding local mechanical blocks. IcyDB retains its
persisted-inventory and version-example checks, release receipts/cache checks
and publication waiting. Canic and IC Timers can replace their duplicated
release entry-point smoke tests; IC Metrics keeps its publication checks.
Registry callers must explicitly handle all three statuses before deleting
their local observation implementation. Run each consumer's focused checks and
native host qualification, and distinguish that evidence from upstream fixtures.

## Portable file digests

```sh
bash scripts/ci/verify-file-checksum.sh --print sha256 artifact.wasm
bash scripts/ci/verify-file-checksum.sh sha256 EXPECTED_HEX artifact.wasm
```

`--print` emits only one lowercase SHA-256 or SHA-512 digest on successful stdout.
The existing three-argument comparison form retains its interface and silent
success. Both forms require a regular file (including a symlink resolving to a
regular file). Relative paths resolve from the caller's working directory.

The verifier owns backend selection: native `sha256sum`/`sha512sum` when available,
otherwise Perl's `shasum`. It hashes the file through stdin, so spaces, leading
dashes, backslashes and newlines in filenames do not become options or escaped
checksum records. File-read errors, backend failures and malformed output fail
without emitting a digest; a failing backend is never retried through another.
No network access or build is involved.

Shared Tooling uses this same owner for IC installer receipts and snapshot
manifest generation. Their existing manifest formats are unchanged. Digest
support for arbitrary filenames does not widen those formats: IC receipts still
use unescaped line records and reject backslashes/newlines. Failed receipt
traversal or hashing preserves the candidate and previous active toolset.

Consumers already vendoring `verify-file-checksum.sh` need no additional shared
file for digest generation. After refreshing its committed snapshot, replace
only the file-hash bodies in IcyDB/Canic Wasm reports with `--print`; keep report
schemas, subject selection and stream/tree identity encodings local.

## Isolated RustSec database preparation

```sh
# The parent directory exists; rustsec-attempt must be a new path.
bash scripts/ci/prepare-rustsec-db.sh online \
  https://github.com/RustSec/advisory-db.git "$work_dir/rustsec-attempt" || exit $?
cargo audit --no-fetch --db "$work_dir/rustsec-attempt/db"
```

Select exactly `online SOURCE DESTINATION` or `local SOURCE DESTINATION`.
Online mode requires an explicit HTTPS repository URL without embedded credentials,
query parameters or fragments; it creates a fresh shallow clone with Git's
low-speed connection protection and terminal prompts disabled. It resolves the
moving feed to the commit actually cloned. This records the observation, not a
pin for future refreshes. Git configuration and available credentials remain
host-owned; the helper does not install credentials or alter Git configuration.

Local mode takes an existing Git repository directory and selects its committed
HEAD before cloning. It never copies working-tree edits or untracked cache files.
It clones without hardlinks, dissociates borrowed objects, and checks out that
exact commit, retaining failure if it cannot obtain it. This allows the prepared
database to outlive the selected cache. Only the file transport is allowed in
local mode; there is no network fallback. Neither mode recurses into submodules.
Inherited repository-specific Git environment variables cannot redirect these
operations into another checkout.

The helper exclusively creates DESTINATION, rejecting occupied paths and symlinks.
Its parent must already exist. Within it, `db/` is the isolated checkout,
`prepare.log` records source/mode and Git diagnostics, and `revision` records the
verified full commit on success. Successful stdout contains that same commit.
Preparation failures retain the destination
and log, report its location, and omit the success revision. Invalid arguments or
an unavailable local directory fail before claiming a destination.

The caller owns this directory and its evidence lifetime on both success and
failure. Preserve failed attempts; choose a new destination when retrying. Do
not attach an unconditional cleanup trap that deletes the only failed evidence.
Retain `revision` beside the resulting audit evidence. Preparation success proves
isolation and source identity, not database schema validity, advisory freshness
or a passing security audit; Cargo audit remains the database reader.

Consumers must stop on preparation failure and invoke their audit with `--no-fetch`
against the prepared `db/`. Canic retains dependency fetching, lockfile selection
and its risk inventory; IC Query retains warning/exemption policy and subsequent
cargo-machete ordering. Neither adapter should let cargo-audit fetch again and
change the recorded database identity. The helper never invokes Cargo, selects
exemptions or changes a shared advisory cache.

Adopt `scripts/ci/prepare-rustsec-db.sh` and this guide from a reviewed committed
snapshot before replacing consumer preparation bodies. It requires Bash 3.2+
and Git, with no shared-script dependencies. `test-rustsec-db.sh` covers simulated
online/failure cases and real local isolation using existing Git history; it
neither creates commits nor performs a live vulnerability audit. Consumer tests
must retain policy/order checks and prove audit cannot run after preparation fails.

## Local Cargo.lock versions

```sh
perl scripts/ci/rewrite-local-lock-versions.pl Cargo.lock 0.1.0 0.1.1 my-crate helper-crate > candidate.lock
```

The Perl/core-only transformer reads a regular Cargo-generated LF lockfile in
format 3 or 4 and emits a complete candidate on stdout. Supply canonical stable
versions and the exact local package roster selected by the consumer's Cargo
metadata. Each selected package must appear exactly once without a `source`
field and have the previous version. Missing, duplicate or mismatched identities
fail without emitting a partial candidate. The input file is never written.

Only selected local versions and their exact unqualified dependency references
change. Registry/Git identities, source-qualified references, checksums, unrelated
versions, whitespace and comments stay unchanged. This is a narrow transformation
of Cargo's generated layout, not a general TOML parser or resolver. The caller
must check exit status before replacing its lockfile, retain failed candidates,
and run Cargo's locked offline validation against the prepared manifests.
Selecting independent lockfiles, discovering packages, metadata writes and
release recovery remain consumer responsibilities. Do not redirect output onto
the input file, invoke dependency resolution online, or use this to repair an
already inconsistent graph. The upstream fixture includes independent real-Cargo
locked/offline validation with local-only dependencies.

## Formatter prerequisites

```bash
source ci/tool-versions.env
bash scripts/ci/check-format-tools.sh "$SHARED_TOOLING_CARGO_SORT_VERSION"
# An independent workspace can select its prepared toolchain explicitly:
RUSTUP_TOOLCHAIN="$VALIDATION_TOOLCHAIN" \
  bash scripts/ci/check-format-tools.sh "$SHARED_TOOLING_CARGO_SORT_VERSION" /path/to/cargo
```

The helper requires an explicit exact cargo-sort version and accepts an optional
Cargo executable path, defaulting to `cargo` on PATH. It requires successful
`sort --version` with the exact expected output and successful `fmt --version`.
It forces Cargo offline and disables rustup automatic installation for these
probes; it never installs tools, formats files, resolves dependencies or builds.
A command that prints the expected version but exits unsuccessfully is rejected.
An executable path is one argument, not a shell command; use `RUSTUP_TOOLCHAIN`
for a selected rustup toolchain instead of embedding `cargo +toolchain` in it.

Vendor this file in the reviewed snapshot and call it from a prerequisite shared
by `fmt` and `fmt-check`. Keep the pin in the consumer's reviewed versions file;
setup and CI must use that same value. Retire the replaced local version checks.
Consumers retain workspace discovery, nested manifests, rustfmt configuration,
frontend adapters and additional formatting steps. Probe every independently
selected toolchain. This check establishes availability, not that formatting
covers the right files; the adoption checker below supplies that separate proof.

## Consumer formatting-hook adoption

```sh
bash scripts/ci/check-formatting-hooks.sh "$PWD" crates/example/src/lib.rs \
  crates/example/Cargo.toml /tmp/example-unsorted.toml ci/tool-versions.env
```

The first two relative paths select an existing Rust module and Cargo manifest.
The fourth argument is a consumer-prepared unsorted copy of that manifest: change
only dependency ordering, so the real formatter restores the selected sorted
bytes exactly. For a consumer with no dependency tables, pass the explicit
`--no-dependency-tables` argument instead. This caller-owned assertion omits only
the dependency-order perturbation; the real `fmt-check` still runs, and manifest
preservation and partial-staging rejection remain checked. The helper does not
infer or parse dependency absence, and that mode supplies no sorting-perturbation
proof. Never add synthetic dependencies to qualify a dependency-free consumer.
Remaining relative arguments explicitly overlay additional current
files needed by the consumer's formatter (other manifests, source, lockfiles,
configuration or Make includes). The checker exports existing HEAD, overlays
the named inputs plus the Makefile, hook, installer and Make execution check,
and stages them only in temporary repositories. It never creates commits or
activates the real checkout's hook.
README.md must exist as an unrelated-edit preservation input. Tracked files must
be regular files, matching the shared hook's support contract.
This includes unrelated historical symlinks: the checker rejects them before
running the formatter. A consumer with an approved hook adapter that exports
only regular entries must keep its own qualification for that model, including
selected-symlink refusal and unrelated-evidence preservation. Passing the shared
fixture cannot qualify that adapter. Do not remove historical links merely to
make this checker pass. See [#33](https://github.com/dragginzgame/shared-tooling/issues/33).

Review the consumer's Makefile and formatting commands before execution. This
helper executes those commands; it is not a sandbox for arbitrary Make code.
Prerequisites must already be installed. Cargo is forced offline, rustup auto
installation is disabled, and inherited Git/Make/logger checkout selections are
cleared. The baseline must pass its real `fmt-check` before perturbation.

Checks cover selected Rust refresh and manifest sorting, idempotence, partial
Rust/manifest staging, malformed Rust formatter failure, preservation of selected
lockfiles and unrelated edits, and installer alias/conflict handling. Failed
exports and logs are retained; successful helper-owned scratch is removed.
Consumer-specific formatter stages and runtime obligations still need their local
tests. Shared Tooling exercises this helper against its real nested Cargo fixture;
that does not qualify a consumer's formatter or establish native macOS adoption.
