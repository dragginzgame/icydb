# Focused verification helpers

These optional helpers share mechanical checks. Consumers retain document
selection, release validation gates, publication authority and registry polling
policy. They require Perl core modules or Bash 3.2+, as noted below, and run on
Linux and macOS. The portable regression suite includes offline fixtures;
native CI qualifies each supported host separately.

## Fixture diagnostic replay

`fixture_replay_logs FILE...` in `scripts/ci/fixture-lifecycle.sh` writes existing
regular-file inputs to stderr in caller order, without adding separators. Missing
paths, directories and unmatched glob literals are skipped. Reader/output failures
are best effort and the helper returns success; callers save their original status
before invoking it and retain their existing failure/completion handling. Callers
own explicit file/glob selection, whether replay runs, and cleanup/retention policy.
No files are removed or collected into a separate artifact.

## Cargo dependency-order fixtures

```bash
perl scripts/ci/prepare-cargo-formatting-fixture.pl Cargo.toml workspace.dependencies candid ic-build > unsorted.toml
```

The helper accepts a caller-owned valid Cargo manifest and swaps exactly two
adjacent, single-line declarations selected by table and bare dependency keys.
Only strings and inline-table values are supported; unsupported selections refuse
before any output. Other bytes, comments and newline positions remain unchanged,
and the source is never edited. This is fixture preparation, not TOML validation,
release metadata rendering or a production dependency reorder. Consumers retain
their selected pair and the formatting/hook assertions. Requires Perl core only.

## Complete release-change paths

```bash
bash scripts/ci/collect-release-changes.sh v0.3.8 > changed-paths.nul
```

The optional Bash 3.2 helper emits NUL-separated tracked changes from the selected
base commit to the current worktree, followed by nonignored untracked paths.
Without a base, it selects the most recent reachable canonical `vX.Y.Z` tag.
Explicit bases must resolve to commits. The verified commit ID, rather than its
mutable ref spelling, is used for the comparison even if the tag moves afterwards.
It anchors Git observation at the checkout
root, disables optional index writes, and publishes no paths until both Git
producers succeed. Failed observations preserve their status and clean the
scratch file; its own newly allocated file is excluded if scratch storage is
inside the worktree. Empty output after success means no changed paths. Output order is
not a classification contract. Consumers own package/repository path policy,
release authority and how they use the complete observation. This does not replace
index/worktree admission or commit-to-commit CI selection.

## Evidence archives

```bash
bash scripts/ci/archive-evidence.sh "$RUNNER_TEMP/evidence.tar.gz" \
  "$RUNNER_TEMP" portable-fixtures "$PWD" .tools/ic-set.failed
```

The Bash 3.2 helper takes a new output path followed by explicit root/relative-path
pairs. Relative roots and output paths resolve from the caller's current directory,
independently of `CDPATH` or option-like names. It requires tar and gzip.
Select only existing evidence; the caller decides
which optional paths exist. The output parent must exist and the output must stay
outside every selected input. Creation refuses an occupied output, including a
symlink or a named pipe. Failed creation retains any partial archive and all original inputs.
Success prints the archive's absolute path and leaves the inputs intact.

Each path becomes an archive member relative to its supplied root. Paths must
be canonical and relative; `.` selects a complete root. Duplicate or overlapping
member paths are refused, including across different roots. Parent-directory
symlinks are refused; final symlinks are archived without following their targets.
Git metadata named `.git` is excluded. If release-state evidence is needed,
select the specific evidence directory as a root rather than archiving `.git`.

Upload the resulting single `.tar.gz` file to preserve filenames, modes and links
that a raw artifact upload cannot reliably represent. The archive supports Unix
filenames containing spaces, colons and newlines; it does not widen line-oriented
checksum-manifest formats. Callers retain ownership of selection, source and run
identity, command outcomes, manifests, upload and retention. Archiving alone
does not qualify evidence or prove a hosted upload/download round trip.

Adopt `scripts/ci/archive-evidence.sh` through a reviewed snapshot; it has no
shared-script dependencies. Shared Tooling's failure-collection action uses this
helper too. Consumers can replace their tar mechanics while preserving their
product-specific collection and identity records.

## Tool-bundle evidence selection

`select-tool-evidence.sh full REPOSITORY DIAGNOSTICS` emits NUL-separated
root/relative-path pairs for host/IC candidates, local Rust evidence and referenced
shared Cargo generations. Both directories must
already exist; diagnostics must be a regular directory. The neutral archiver
continues to own path admission, namespace collision checks and tar creation.

`select-tool-evidence.sh compact REPOSITORY DIAGNOSTICS HOST-VERSIONS IC-PINS`
opts into smaller successful-installation evidence. For each managed relative
active link independently, it runs the existing installer in offline `--check`
mode against a retained copy of the caller's pins. Host checking includes jq,
yq, ripgrep and cloc. Partial, missing, unknown or failed selections stay full;
unselected and failed candidates stay full even when the active set passes.
A changed selection, bundle directory identity or caller pin file refuses
compaction. This is a fresh observation, not authority derived from an earlier
successful installation; stop concurrent toolset mutation during collection.

Successful checks retain caller pin bytes, command output and the exact selected
bundle path. IC checks also retain `pins.tsv`, `host` and `files.sha256`; host
sets have no equivalent bundle receipt, so caller configuration and check evidence
remain explicit. Tiny tool fixtures can grow because diagnostic metadata exceeds
the omitted payload. Archive size and collection time depend on the actual sets.
For shared host/IC storage, the selector retains local references and explicitly
selects their admitted physical bundles under the configured store. It never
scans unrelated store entries or follows arbitrary links. Use the same
`SHARED_TOOLING_STORE` setting as setup; preserved local bundles remain eligible
for full evidence retention, even though installers no longer admit them as active.
The selector requires Perl core modules and the declared host/IC check companions;
it never installs or downloads tools. Invalid selections refuse before dispatch.

The common failure action defaults to full retention. Set
upstream store configuration consistently with setup. If store admission fails,
the selector still emits safe local references and bundles without following
their targets, and returns nonzero. The action archives those selections plus
local logs/builds and the selection diagnostic/status, sets
`collection-complete=false`, uploads that partial evidence, then fails its final
completeness step. A rejected store never authorizes traversal or repair, and
partial evidence is not reported as complete retention. Set
`compact-successful-tools: 'true'` to opt in; `host-versions` and `ic-pins` select
absolute caller pin files, defaulting to the repository's common configuration.
Its native CI control uploads/downloads successful-set diagnostics alongside a
failed candidate, separately from the existing full-failure control. Native
acceptance requires matching executed jobs; a configured control is not evidence
of a successful hosted transport. The selector owns the full physical
`.tools/rust` tree plus its admitted shared generations and original build/log
evidence, independently of host/IC compaction. Custom callers must not also add
the same local tree to the archive. Links are archived without following targets.
Consumers adopt the selector only from a
reviewed committed snapshot, rather than recreating its classifier locally.

## Runner disk capacity

```bash
bash scripts/ci/check-runner-disk-space.sh \
  --path "$PWD" --min-free-mib 4096 --label 'before tests'
bash scripts/ci/check-runner-disk-space.sh \
  --path "$PWD" --min-free-mib 0 --label 'failure diagnostics' \
  --diagnostic-path "$PWD/target" --diagnostic-path "$PWD/.tools"
```

The caller selects one existing file or directory on the filesystem to check,
an inclusive minimum in MiB and an optional label. Zero reports capacity without
a positive reserve requirement; negative available capacity still fails. The
path can be absolute or relative to the caller, including spaces and literal
glob characters. Values must be single-line without tabs. The minimum is a
canonical non-negative decimal integer (no leading zeros), at most 18 digits
so integer arithmetic stays bounded on supported hosts.

The read-only Bash 3.2 helper requires system `df` and `awk`; it requests the
C locale and POSIX KiB output with `df -Pk`. It admits one filesystem record
with a KiB header and numeric capacity, rejects failed or malformed observations,
and compares whole MiB rounded down. Available KiB magnitudes over 18 digits
are refused rather than overflowing. Exit statuses are **0** for sufficient
space, **1** for insufficient space and **2** for invalid arguments or an
unavailable/malformed capacity observation. The printed summary is diagnostic
text; callers use the exit status for gating. This samples capacity without
reserving space or proving that a later build will fit.

Each repeated `--diagnostic-path` explicitly requests a recursive `du -sk`
total in KiB, on success or insufficient capacity. This can be expensive on
large trees. Missing paths and failed `du` commands warn and leave the capacity
result unchanged; they are not successful usage measurements. Without these
arguments no `du` scan runs. The helper has no built-in paths, SDK deletion list,
installation, artifact cleanup or filesystem mutation.

Adopt `scripts/ci/check-runner-disk-space.sh` and this guide through the reviewed
snapshot workflow; there are no shared-script dependencies. The upstream
`test-runner-disk-space.sh` covers real host observation plus controlled failure
and threshold cases, and runs in the portable Linux/macOS CI matrix. Consumers
own step ordering, thresholds, filesystem selection, diagnostic paths and their
native qualification. When replacing Canic's local helper, move its path lists
to a consumer-owned caller and express summary-only checks by omitting diagnostic
paths. Retire the duplicated body after qualifying those callers. Any disposable
runner-image cleanup remains a separate, explicitly scoped consumer operation.

## PocketIC provisioning and admission

IC Testkit owns server selection, provisioning, offline admission and
compatibility. Shared 0.2.0 removes its former alignment and binary checkers;
consumer callers and snapshot selections move together under the
[PocketIC ownership handoff](ic-tools.md#pocketic-ownership-handoff).
Generic checksum and evidence helpers remain available for their other callers.

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

Use `scripts/ci/install-ci-tool.sh TOOL` for actionlint, shellcheck, gitleaks,
sccache. Always supply `--install-dir`. The first three tools use validated
version/checksum records from the snapshot's `ci/tool-versions.env`, currently
qualified for Linux x86-64. `--catalog FILE` selects a reviewed catalog exception.
Alternatively supply both `--version` and `--sha256` for an explicit tool/platform
exception; these cannot be combined with `--catalog`. Sccache requires explicit
pins. A missing, malformed or unsupported catalog selection refuses before setup.
Prepare YQ through `make install-host-tools` and admit it with
`make host-tools-check`; standalone YQ installation is retired.
Since 0.16.0, installation requires an explicit destination; add the flag before
refreshing callers that used `TOOL_INSTALL_DIR` or the home-directory default.
Missing or empty destinations refuse before host probes, downloads or filesystem
setup. The implementation shares host selection,
HTTPS download, checksum admission, extraction, exact version admission and
publication. Asset names and version-output formats remain explicit per tool.
Staging lives on the destination filesystem; failures retain the candidate and
leave the installed executable intact. Successful installation removes its own
staging files. Publication uses Perl core's exact-path atomic rename: a directory
introduced at the executable destination during setup is refused, and a late
symlink is replaced without following its target. Perl must be available before
setup starts; no tool is downloaded when this prerequisite is missing.
This does not merge repository-local host/IC bundle activation
or change any consumer's explicit pins. The command declares its checksum
verifier, catalog reader and default catalog as snapshot companions.

`host-platform.sh` in `scripts/dev/` observes `uname -s` and `uname -m` and emits
one normalized `Linux|Darwin:x86_64|arm64` identity. It accepts no arguments and
performs no setup. OS and architecture aliases share this owner; a failed
observer propagates its status without publishing an identity. Each installer
still owns its supported asset matrix, including IC's Linux ARM64 refusal.

`tool-catalog-records.pl host|rust|ic|ci CATALOG [TOOL]` emits validated
tab-separated tool, version, host and digest records. Rust records use `all` and
`-` for host and digest. Optional tool selection filters only after the entire
selected group is admitted; an unknown tool refuses without partial output.
CI records normalize asset-specific host spellings to `LINUX_AMD64`.
Canonical stable-version grammar comes from `StableVersion.pm`; CLOC retains
its two-component version allowance. Catalog admission adds no release u64 bound.
Callers select records directly; the former raw `values` projection is removed.
The IC admission backend, `ic-tool-pins.awk`, always emits the complete validated
matrix. It has no version-only or validation-only output mode. Select individual
tools through `tool-catalog-records.pl ic CATALOG TOOL` after matrix admission.
For example, obtain the formatter pin without recreating catalog variables:

```bash
perl scripts/ci/tool-catalog-records.pl rust ci/tool-versions.env CARGO_SORT
```

`install-ci-tool.sh sccache` uses the same explicit version, SHA-256 and installation
directory arguments. Its reviewed asset scope is Linux x86-64, using the
`sccache-vVERSION-x86_64-unknown-linux-musl.tar.gz` release asset. Other hosts
are rejected before installation or download; consumers retain their explicit
Cargo/native setup on those hosts. Extending binary asset selection requires
reviewed pins and corresponding qualification, not guessed download names.
The installer prints the selected executable path; callers own `GITHUB_PATH`,
`RUSTC_WRAPPER`, cache configuration and any server startup. Include
`install-ci-tool.sh` and `verify-file-checksum.sh` in the same snapshot.

CI, host and IC asset downloads use `tool_store_download` from
`scripts/dev/tool-store.sh`: HTTPS-only transport, TLS 1.2, a 15-second connection
limit, a 300-second attempt limit and three transient retries within a 60-second
retry window. An in-flight attempt retains its own limit. Each installer owns
checksum admission, staging and publication; CI failures retain their candidates.

## Compiler-cache launcher adoption

`scripts/ci/run-sccache.sh` owns stable, repository-scoped temporary files and
socket selection for a cache server that outlives one validation invocation.
It forwards the exact arguments, diagnostics and exit status to `SCCACHE_BIN`.
Set that variable to an absolute executable file. Unset, empty, bare-name,
relative, directory and unavailable selections fail before runtime creation;
there is no PATH fallback. The selected file may be a caller-owned adapter.
The launcher does not select Cargo's compiler wrapper or retry compiler failures.
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

The root's Makefile and explicit additional roots are copied into a temporary
checkout without Git metadata through `prepare-script-fixture.sh`, including
their declared companion closure. All selected inputs must be present regular
files with safe parents. Select the Make include roots and let their declarations
carry dependencies; consumer-owned parse-time inputs still need explicit roots
or companion declarations. No sibling discovery occurs. The copied Makefile must call
`scripts/ci/run-release.sh` relative to its fixture checkout (directly or through
Bash). That path contains a substitute that records exact arguments and returns
a chosen status. The runner is replaced after copying and before any Make
execution. GNU Make and Bash are prerequisites; include the preparer and its
declaration reader when refreshing a manually selected checker snapshot.

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

## Consumer-owned Cargo publication admission

Consumers compose the shared source, workspace-version, release-tag, registry
and Make-execution observers in their own publication policy. Keep package
selection, required remote-tag publication, registry choice, features, credentials
and interruption reconciliation at that owner. Retain local source and identity
rechecks after network observations; unknown registry state never admits upload.

IC Jobs owns its complete preflight under `scripts/release/`
([PR #67 — Own publication admission locally](https://github.com/dragginzgame/ic-jobs/pull/67)).
Integrate that caller before adopting the command removal, and retire its former
snapshot entry through a reviewed export with the complete new entrypoint set.
The shared publication-attempt runner separately retains dispatch and recovery
evidence; a preflight observation is not a lock or an upload receipt.

## Exact crates.io version observation

The helper accepts one crate name and one canonical stable `X.Y.Z` version.
The status-only command uses Bash and curl to observe HTTP presence without
creating retained metadata. Reconciliation callers select `--metadata NEW-DIRECTORY`
and prepare jq and curl 8.4.0+ before invocation.
Prerelease/build versions, leading-zero components, malformed names and extra
arguments are rejected. Its exit contract is **0 present,
1 absent (HTTP 404), 2 unavailable or invalid**. Transport failure wins over any
HTTP-looking output. Metadata mode additionally validates the response identity,
checksum and yank flag; status-only presence does not establish archive equivalence.
Handle all three statuses. Never use a blanket
nonzero test to decide that publication is safe.

One request targets the fixed crates.io API, with HTTPS-only redirects, a
10-second connection timeout and 30-second total timeout. It disables implicit
curl configuration, supplies no credentials, and neither polls nor retries.
It does not run Cargo, inspect dependencies, publish or clean artifacts.
A present version does not prove that a new consumer can resolve every dependency.
Post-publication waiting, time budgets and package ordering stay in the caller;
an inconclusive result must stop that flow rather than trigger publication.

### Retained exact metadata

Each observation retains package identity and checksum facts:

```sh
status=0
facts="$(bash scripts/ci/check-crates-io-version.sh \
    --metadata "$logs/my-crate-observation-1" my-crate 0.1.2)" || status=$?
case "$status" in
    0) printf '%s\n' "$facts" ;; # Compare with the caller's selected payload.
    1) echo 'Absent; apply the caller publication policy' ;;
    *) echo 'Inconclusive; stop without publication' >&2; exit 2 ;;
esac
```

The directory's parent must exist; an existing directory, file or symlink is
rejected before a request, so a retry cannot overwrite or reuse earlier evidence.
The helper creates it with private permissions and retains `request.url`,
`curl-version.txt`, `response.json`, `http-status`, `curl-exit` and `curl.stderr`.
JSON parsing also retains `metadata.stderr` and its output `metadata.json` when
attempted. Keep the whole directory on success and failure; a file's presence
alone is not evidence of a completed observation. Callers select a fresh path
for every attempt and must check the exit status.

The body limit is 1 MiB, enforced during transfer and checked again before JSON
parsing. The curl floor is required because
[older curl cannot enforce this limit without a known response size](https://curl.se/docs/manpage.html#--max-filesize).
Only HTTP 200 containing exactly one JSON object with the exact requested crate
and version, a 64-character lowercase SHA-256 checksum and a boolean `yanked`
field returns 0. Stdout then contains one JSON object with `crate`, `version`,
`checksum` and `yanked`. Extra registry fields are ignored. HTTP 404 returns 1;
transport, size, malformed/multiple JSON, identity, field-type, prerequisite and
evidence-write failures return 2. Neither 404 nor an inconclusive read emits facts.

A yanked version remains an observed version (`yanked: true`), not an absent one.
The consumer decides whether to accept it. Auth keeps archive-hash comparison,
durable upload intent and uncertain-upload reconciliation; Blob keeps archive/VCS
authentication, package ordering and dependency dry-run checks. Valid registry
metadata alone never authorizes skipping or dispatching an upload. Adoption is
tracked in [Auth #12](https://github.com/dragginzgame/ic-auth/issues/12) and
[Blob #45](https://github.com/dragginzgame/ic-blob-storage/issues/45).

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

## Frontend formatting

`scripts/dev/format-frontend.sh --write|--check RELATIVE-DIRECTORY` formats the
selected index-export files during a hook write, or tracked files in the scope
for explicit invocation. Set `PRETTIER_BIN` to a prepared absolute executable
and `PRETTIER_VERSION` to its exact lockfile version. Setup stays explicit.

The pinned Prettier owns language recognition through `--ignore-unknown`;
Shared Tooling does not maintain an extension allowlist. Consumer configuration
and `.prettierignore` own formatting exclusions. The adapter retains scope and
symlink checks, NUL-delimited selection and exact-version admission. Upgrading
to this contract can include previously skipped formats such as `.mjs` and
`.vue`; review exclusions before adopting it. Unknown file types remain untouched.

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
Pass `--rust-sources` immediately after the fourth argument to discover current
Git-maintained Rust files and Cargo manifests, including unignored additions.
The checker applies their edits/additions and tracked deletions only to its
disposable checkout. Keep selected perturbation inputs and additional Make,
configuration or tooling files explicit. This option avoids maintaining a
second per-module source roster; it does not change formatter or workspace policy.
The companion `scripts/ci/select-rust-formatting-sources.sh ROOT` publishes a
complete NUL-separated relative-path selection only after Git and path admission
succeed. It rejects symlinks, nonregular current files and physical escapes;
tracked missing paths represent deletions. Ignored untracked files and unrelated
extensions are omitted. Neither command stages or edits the real source checkout.
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
Prerequisites must already be installed. The selected consumer's `.tools/host/bin`
and `.tools/rust/common/bin` precede inherited PATH for executable lookup;
products requiring IC formatters explicitly publish their prepared IC PATH.
Formatter inputs and configuration still come from the disposable checkout.
Cargo is forced offline, rustup auto
installation is disabled, and inherited Git/Make/logger checkout selections are
cleared. The baseline must pass its real `fmt-check` before perturbation.

Checks cover selected Rust refresh and manifest sorting, idempotence, partial
Rust/manifest staging, malformed Rust formatter failure, preservation of selected
lockfiles and unrelated edits, and installer alias/conflict handling. Failed
exports and logs are retained; successful helper-owned scratch is removed.
Success also requires the checker to reach its final completion point. Premature
exits, including Bash 3.2 expansion failures that report zero, return failure and
retain evidence; existing nonzero statuses are preserved.
Consumer-specific formatter stages and runtime obligations still need their local
tests. Shared Tooling exercises this helper against its real nested Cargo fixture;
that does not qualify a consumer's formatter or establish native macOS adoption.

## Coordinated Cargo graph admission

`cargo-release-metadata.sh check-graph PRIMARY_PACKAGE` validates regular root
Cargo inputs, locked/offline resolution and the complete coordinated workspace/
local-lock roster without requiring release notes or dates. It performs no
version writes, fetching or compilation. The existing `check` operation adds
release-note/date admission; both retain selected-commit and failed-evidence
semantics. Consumers can project a successful Cargo member document through
`cargo-workspace-members.jq` after a literal manifest bump; use full locked
resolution for the final graph check rather than treating `--no-deps` as proof.

`compare-stable-versions.pl VERSION BASELINE` prints -1, 0 or 1 using exact
canonical decimal component ordering. It rejects malformed stable identities
before output and uses no floating-point or shell-integer arithmetic. The release
finalizer rejects non-increasing previous/target pairs itself.

## Downloaded failure-evidence verification

The optional host-side `failure_evidence.py` provides non-extracting archive
indexing/regular reads and independent compact Shared tool receipt verification.
`verify_compact_tools` selects a nonempty set of `host`, `ic`, or both from the
caller's `tool_pins` keys. Every selected bundle requires its caller pins, fresh
check log and managed compact selection; unknown or empty selections refuse.
IC selection additionally requires matching installer pins, native host and
checksum receipts. Unselected bundles remain diagnostics and are not qualified.
Consumers keep source/job/run identity and product assertions. The Python helper
is selected only by download-verification consumers, not native shell setup.
Run `python3 scripts/ci/test-failure-evidence.py` on the download-verification
host; the portable shell suite does not introduce a Python prerequisite.

`qualify-hosted-failure.sh early|late CONSUMER EVIDENCE_PARENT HOST_CATALOG
FAILURE_LOG_ROOT` requires explicit workflow dispatch and matching checkout
identity. It creates a new isolated scenario/store, drives the canonical installer
or logger, checks preserved inputs and deliberately returns the verified original
failure status. Callers own evidence selection, upload/download and supported
host gates; the command activates no workflow and changes no active tool selection.

### Fixture allocation

Source `scripts/ci/fixture-lifecycle.sh` and call `fixture_create TEMPLATE` to
allocate one directory and return its absolute physical path before changing cwd.
The caller selects the template/prefix and retains its EXIT trap, completion flag
and evidence policy. Relative templates, ordinary spaces and directory aliases
are supported; CDPATH cannot redirect anchoring or add output. Empty templates,
leading options and CR/LF in templates are rejected before allocation. A resolved
physical path containing CR/LF is refused, retaining the allocation for inspection.
Allocation errors preserve their status; normalization failures retain and report
the created directory without attempting cleanup from an unanchored path.

For example, after sourcing the helper:

```bash
fixture="$(fixture_create "${TMPDIR:-/tmp}/consumer-check.XXXXXX")"
fixture_complete=false
trap 'fixture_finish "$?" "$fixture" "Consumer fixture retained"' EXIT
# Run the caller-owned checks, then mark successful completion.
fixture_complete=true
```

The allocation helper is additive and does not change existing cleanup callers.
Its focused fixture is `scripts/ci/test-fixture-allocation.sh`; selecting that
fixture through the snapshot exporter includes the lifecycle helper.

### Completion admission without cleanup

`fixture_completion_status STATUS COMPLETE` returns the original nonzero status,
or returns 1 for zero-status execution unless COMPLETE is `true`. STATUS must be
a canonical decimal shell exit status from 0 through 255; invalid arguments
return 2 rather than wrapping an invalid status to success. The helper performs
no cleanup, reporting or process exit. Capture refusal in an explicit conditional
under errexit. `fixture_finish` uses this admission before its existing cleanup
policy; qualification runners can instead retain all success and failure evidence.

## Consumer fixture retention probes

Source `scripts/ci/fixture-retention-probes.sh` to reuse failure injection and
retained-directory checks. Callers retain the fixture roster and mandatory
product/host assertions. `FIXTURE_PROBE_ROOT_VARIABLE` and
`FIXTURE_PROBE_DIRECTORY_VARIABLE` select explicit shell identifiers (defaults
`ROOT` and `fixture`); use `FIXTURE_ROOT` explicitly for that directory layout.
The helper renders only initialization through the first EXIT trap, replaces
selected root assignments with the supplied source root, and never executes the
actual fixture body. It qualifies nounset, failed commands, nonzero exits,
premature success and completed success/failure using the invoking Bash.
`fixture_probe_prefix SOURCE OUTPUT ROOT_VARIABLE DIRECTORY_VARIABLE INJECTION`
also renders a boundary for caller-owned contradictory assertion checks. Its
child requires `RETENTION_SOURCE_ROOT` and `RETENTION_EXIT_PATH` explicitly.
Only trusted, reviewed fixture sources and caller-owned injection are inputs.

## Tool evidence collection orchestration

`collect-tool-evidence.sh ARCHIVE REPOSITORY DIAGNOSTICS full|compact HOST IC
[ROOT PATH]...` composes the selector and neutral archiver. Callers keep their
log roots, run identity, qualified catalogs, upload and retention policy.
Status 0 means complete, 1 means safe partial evidence was archived, and 2 means
collection/archive failed. A fresh `selection-status` receipt retains the exact
selector status, `archive-status` records an attempted archive failure, and
`selections.nul` retains the original selector output. Failed output
is never used to admit physical payloads. Instead, local Rust evidence and
host/IC references are retained without following redirected roots or links.
Diagnostics remain in the partial archive; completeness is not inferred from
successful archiving. Stdout contains the created archive path only.

## Formatter evidence selection

Source `scripts/ci/formatting-evidence.sh`, then call
`select_formatting_evidence ABSOLUTE_LOG_ROOT`. It fills `formatting_evidence`
with root/relative-path pairs for the formatter's retained `formatting.*` files
and final symlinks, including dangling links. Directory/FIFO matches are omitted;
links are archived without following their targets. Missing or relative roots
return 2 and clear the selection. An empty existing root succeeds with no pairs.
Callers pass the array to the evidence archiver and retain archive layout, source
identity, Git custody and upload policy. This helper never removes logs.

The exportable `test-release-source.sh` owns generic source-admission cases in
a minimal disposable Git repository, including exact working-file and index
preservation after literal metadata admission and rename-source refusal.
It also qualifies refusal of plausible output from failed Git observations and
invalid selections before Git dispatch, preserving files and removing scratch.
Consumers retain tests of their actual allowlist, gate ordering and release
adapter. Retention probes use
`retained-completed` for fixtures that intentionally retain successful evidence;
ordinary `completed` still requires successful scratch removal.

## Native host admission

Run `bash scripts/ci/check-native-host.sh Linux x86_64` or
`bash scripts/ci/check-native-host.sh Darwin arm64 15` before native setup.
The caller selects the exact architecture and, on macOS, the explicit major.
The helper observes `uname` and `sw_vers`; it makes no PATH or tool changes.
Status 0 means a match, 1 means mismatch/malformed version, and 2 means invalid
selection. Failed observations preserve the observer's nonzero status, including
when it prints plausible output. Linux does not call `sw_vers`. Consumers retain
their matrices, workflow ordering, PATH changes and native-host qualification.
The portable fixture substitutes observations; it does not qualify macOS.

## Locked release preflight

`scripts/ci/cargo-release-preflight.sh TOOL-ROOT PREPARE` accepts repeated
`--pin PATH`, `--check TARGET` and `--prepare TARGET` pairs. Callers first admit
source/recovery and consume their preparation authority. With PREPARE=0 it
runs locked offline Cargo fetching followed by check targets; PREPARE=1 runs
locked fetching followed by the caller's complete ordered preparation targets.
Explicit Cargo offline policy remains authoritative. Cargo retains the caller's
selected-source working directory; tool targets run with Make in TOOL-ROOT.
`RELEASE_MAKE` selects one Make executable (including a path with spaces),
defaulting to `make`; its failure status is preserved without fallback.
Every selected catalog is hashed before preparation and rechecked after each
tool target. Failures preserve fetch/Make status or refuse changed selections.
The helper owns no version/source admission, index, release or publication effect.
Focused coverage is `scripts/ci/test-cargo-release-preflight.sh`, including
simulated cold/warm caches, offline policy, fetch failure, missing executables,
setup/check failure and selected-pin drift. These use substituted Cargo/Make
effects. A frozen one-dependency workspace additionally uses real Cargo with an
empty cache to verify environment/configuration offline refusal before tool
dispatch, preserving its manifest and lock. Consumers retain preparation
authority, catalog overrides, runner ordering and Git-state custody.

## Current Rust formatting inputs

Pass `--rust-sources` after the formatting checker's fourth positional argument
to use `scripts/ci/select-rust-formatting-sources.sh`. The canonical selector
includes current maintained Rust/Cargo inputs and tracked deletions, publishing
its complete NUL-separated result only after Git/path admission. Only disposable
hook checkouts are updated. Consumers retain chosen perturbations and additional
non-Rust tooling/lock/configuration overlays. Source and checker bytes match the
delivered Shared contribution; focused coverage is
`scripts/ci/test-rust-formatting-sources.sh`.

## Workflow run extraction

`scripts/ci/extract-workflow-step.sh STEPS_JSON STEP_ID` emits the selected run
body from one JSON array of workflow steps. It requires exactly one matching
object with a nonblank string `run` and no `uses`. Missing/duplicate selections,
invalid shapes and malformed or multiple JSON documents refuse without emitting
a script. It preserves jq failures and requires prepared jq on PATH. Callers
own YAML-to-JSON selection, ordering/condition assertions and actual execution;
extraction grants no authority to execute unreviewed workflow code. Focused
coverage is `scripts/ci/test-workflow-step.sh`.

## Producer and consumer fixture boundaries

Shared's Cargo preflight fixture runs the real release runner against cold,
offline, fetch-failed and tool-failed preparation, proving refusal before version
preparation and retention of its isolated source/index. Consumers keep their
preparation-authority routing, exact package/pin selections and recovery custody.
The tool-version report fixture owns catalog/admission/probe mechanics, including
partial stdout/stderr. Workflow fixtures can substitute that reporter boundary
while checking their own log names, status and later-step refusal.
The native archive fixture owns partial tar/hash failures and preservation of
readable diagnostics plus their checksum. Consumers retain a real archive round
trip for their selected payloads and workflow-level collector failure propagation.

## Consumer flow qualification

Source `scripts/ci/make-adapter-probes.sh` for
`make_probe_target_admission MAKE MAKEFILE DIRECTORY EVENTS TARGET...` and
`make_probe_parallel_dispatch` with the same arguments. Callers prepare reviewed
Makefiles and substituted effects. The first checks each unsafe behavior once;
the second requires observable dispatch under a real pipe jobserver. Exhaustive
flag aliases and hidden assignments stay in `test-consumer-flow-probes.sh`.
The producer-only `make_probe_execution_modes` is no longer exported; consumer
admission checks use `make_probe_target_admission` instead.

When adopting 0.16.0, update any calls to the removed helper before refreshing
the reviewed snapshot. Keep the consumer's target roster, effect assertions and
parallel jobserver checks. Existing snapshots stay bound to their recorded
source; refresh through the exporter rather than editing vendored helpers.

`fixture_probe_effect SCRIPT RUN PREFIX STATUS BASH MARKER` runs a fixture with
caller-prepared PATH/environment, captures `result.log` and `status.txt`, and
requires exact status, successful cleanup or one retained failure directory and
the supplied marker. `RUN/tmp` must exist. The helper publishes
`fixture_effect_retained` for consumer-owned payload/diagnostic assertions.

`collect-native-diagnostics.sh CHECKOUT EVIDENCE OUTPUT FORMAT-ROOT TOOLS-OUTCOME
HOST-PINS IC-PINS NAME...` archives only the supplied existing top-level files
and writes its SHA-256 before collecting failed tools or formatter logs. The
`OUTPUT.sha256` sidecar contains exactly one lowercase digest and LF, with no
filename. Verify the explicitly selected archive through the checksum owner:

```bash
bash scripts/ci/verify-file-checksum.sh sha256 "$(cat "$archive.sha256")" "$archive"
```

This replaces the filename-bearing sidecar contract; update `shasum -c` callers.
Names cannot traverse directories; the scratch tree is never implicitly selected.
The helper uses the existing tool/formatter owners and returns 0 on success,
the tool collector's nonfatal status on partial collection, or 2 on fatal
admission/archive errors. Consumers retain source/outcome records, the selected
roster, upload policy and complete outer archives including Git recovery state.

Focused coverage is `scripts/ci/test-consumer-flow-probes.sh` and
`scripts/ci/test-native-diagnostics.sh`. The latter owns referenced host/IC/Rust
bundle selection, rejected-store local retention and fatal candidate-archive
failures after readable diagnostics are published. Consumers retain actual
workflow dispatch/status and outer recovery-state checks. No helper is a sandbox
for an unreviewed Makefile, script or selected payload.
## Assertion retention probes

Source `scripts/ci/fixture-retention-probes.sh` for
`fixture_probe_assertion OWNER PROBE_ROOT SOURCE_ROOT NAME
--selected ASSERTION_SOURCE LITERAL_PREFIX BINDINGS`.
Since 0.16.0, assertion selection is mandatory. Replace inferred-assertion calls
with the existing explicit form before refreshing the helper. Consumers retain
the fixture roster, exact assertion selection and contradictory bindings.

Exactly one source line must match the literal prefix after leading whitespace.
The child runs only the owner's initialization through its EXIT trap, followed
by caller bindings and the selected assertion. Bindings are trusted shell source
maintained by the caller; never pass user input. Bodies that build, install or
mutate Git are excluded from execution. Keep contradictory values and product
assertion selection in the consumer.

The probe requires the supported root anchor and EXIT boundary, evidence
retention, status 1, and a receipt proving the intended assertion was reached.
Ignored assertions and earlier setup failures are rejected. The focused owner
suite is `scripts/ci/test-fixture-assertion-probe.sh`.

## Manifest ordering in selected release sources

`cargo-release-metadata.sh check PRIMARY_PACKAGE --manifest-order` additionally
runs `cargo sort --workspace --check` in the same source selected for metadata
admission. With `RELEASE_COMMIT`, Shared owns the one immutable export, exact
commit admission, cleanup and failure retention. Consumers retain the opt-in
policy and prepare the Cargo tool; this check performs no installation or
manifest mutation. Other check callers remain unchanged. The flag is rejected
for `prepare` and `check-graph`. Scripts from the selected export are never run.

For intentionally separated dependency groups, use `--manifest-order-grouped`
instead. It adds `--grouped` to the same read-only Cargo sort check, retaining
alphabetical order within each group. Both options require `check` and include
`--check`; neither rewrites the workspace nor the selected release export.
Current and committed-source checks must select the same ordering policy as
the consumer's formatter and hook. The flag does not change those callers or
verify dependency roles. Follow the
[dependency-role adoption checklist](consuming-snapshots.md#dependency-role-adoption-in-0240)
when selecting grouped mode.

## Hosted qualification admission and evidence parents

`qualify-hosted-failure.sh` owns stage/manual-dispatch admission, selected-checkout
comparison and validation of caller catalog/log paths before creating an evidence
parent. Consumers select the five arguments and delegate; they need not repeat
admission or create the parent first. A nonempty missing parent is created only
after admission, then physically anchored so relative paths, aliases and spaces
retain the same scenario identity. Refused inputs create no evidence directories.
The focused owner fixture is `scripts/ci/test-hosted-failure.sh`.

The producer fixture `scripts/ci/test-checker-retention.sh` qualifies retained
release/formatting inputs, logs and Git index after child or assertion failures.
Consumers retain their own status/diagnostic checks without inspecting these
shared checkers' internal directory layout.

IC Metrics' [recovery packaging contribution](https://github.com/dragginzgame/ic-metrics/pull/88)
owns full native recovery packaging under
`scripts/ci/package-native-recovery-evidence.sh` in its repository, including
hidden Git state, inventory replay and archive checksums. Its workflow publishes
separately selected readable diagnostics through the shared sanitized exporter.
The former shared `collect-native-evidence.sh` and its fixture are retired;
remove their snapshot entrypoints when adopting this cut. Full recovery archives
must retain their original contents and failure-preservation contract.

## Controlled hosted-scenario verification

`failure_evidence.py` also exports
`verify_hosted_scenario(members, read, stage, *, scenario_parent, failure_log_root)`.
Call it inside `regular_evidence_archive` with explicit canonical relative archive
prefixes. It independently checks the maintained hosted driver's exact failure
status, preserved input bytes/modes, scenario marker, rejected early download or
late raw/combined logs. It neither extracts archives nor derives expected bytes
from their receipts. Consumers retain source/job/run identity, collection policy,
native tool pins and compact-tool qualification. Negative owner cases exercise
both stages and malformed/missing/ambiguous evidence; consumer collector tests
still qualify the complete upload/download boundary.

## Read-only changelog preflight

`check-release-changelog.sh VERSION PREVIOUS DATE CHANGELOG` admits a regular
changelog with the canonical candidate selector before any setup or mutation.
Version components and Gregorian dates use `validate-release-inputs.pl`, including
its u64 component bounds. Invalid scalar inputs exit 2 before changelog reading.
It writes no temporary/output file, preserves failed reader/selector status and
keeps empty notes advisory. Coordinated metadata preparation remains the writer;
consumer source, tag, impact and release authority stay local. Focused coverage
is `scripts/ci/test-release-changelog.sh`.

## Focused verifier qualification

`bash scripts/ci/test-snapshot-verifier.sh` checks generic snapshot directory
admission, ordinary aliases, resolved manifest parents and corruption of inert
checksum helpers against a controlled three-file export. Distribution tests
retain refresh/source/index obligations; consumers retain their actual exported
manifest and representative payload corruption checks. This Bash 3.2 fixture
requires no Git repository, tool installation or Cargo inputs.

`python3 scripts/ci/test-failure-evidence.py` owns generic downloaded-archive,
compact-tool receipt and hosted-scenario rejection cases. The standard-library
suite runs in Linux CI; it adds no Python prerequisite to native shell consumers.
Consumers retain actual collector integration, source/run identity, caller pin
selection and representative rejection checks through their own verifier.

## Script fixture preparation

`bash scripts/ci/prepare-script-fixture.sh SOURCE EXISTING-FIXTURE ROOT...`
copies explicit reviewed roots and their transitive line-two `Shared companions`
declarations, preserving file modes. It shares the declaration reader with the
snapshot exporter; inspected payloads are never executed. Cycles are deduplicated.
The complete closure and destination parents are admitted before any copying;
unsafe paths, symlink traversal, missing inputs and overlapping roots refuse.
Existing regular fixture files may be replaced. Callers own an idle disposable
destination and stable reviewed source; copying does not protect against
concurrent mutation. The helper neither creates a snapshot nor proves supplier
custody. Consumers retain product adapters, substitutions and assertions.
Focused coverage is `bash scripts/ci/test-script-fixture.sh`.
