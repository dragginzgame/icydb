# Supported Hosts And Dependencies
<!-- Shared companions: docs/ci-change-selection.md docs/rust-toolchains.md -->

For reviewed prose-only changes, use the [documentation-only CI selection](ci-change-selection.md)
contract. Code and tooling changes retain the required native host coverage below.

## Required macOS support

Every `dragginzgame` package must work on macOS under the
[engineering baseline](../DRAGGINZGAME.md#host-support). This covers dependency setup,
native tools and the applicable build, test and deployment workflows. Canister
and frontend packages retain their product runtime targets while supporting
their host workflows on macOS.

Each consumer declares its supported macOS versions and architectures, exact
prerequisites and qualification commands in its local host matrix. Missing CI
coverage or a known macOS failure is a support gap to correct. An exception
requires explicit maintainer approval with scope and reason.

Keep the required support decision separate from passing evidence for a revision.
Native CI or recorded native execution qualifies relevant host behavior; Linux
execution, cross-compilation and available download assets do not establish
macOS filesystem, process or deployment behavior. Report unqualified workflows
explicitly while retaining the macOS support requirement.

## Host-specific setup and commands

Rust workflows can adopt the shared [toolchain preparation action](rust-toolchains.md)
while keeping their own development/MSRV selections and qualification gates.

Dependency installation and CI/deployment setup may differ by host. Document
those differences at the owning boundary and preserve the same product
contracts, validation obligations, authorization, recovery and retained artifacts.

- Declare the required shell and Make implementation, system utilities and
  package-manager prerequisites. Check GNU/BSD differences, filesystem modes,
  paths and process handling wherever the workflow relies on them.
- Select host-appropriate dependency packages and executable assets explicitly.
  Preserve reviewed versions, lockfiles and platform digests; dependency setup
  does not authorize selecting newer versions or making live deployment effects.
- Exercise applicable package builds, focused tests, dependency setup and
  operator tooling on the declared native macOS hosts. Deployment-tool validation
  does not itself require or authorize a live deployment.

The matrix below describes Shared Tooling's own portable scripts. Consumers own
their package-specific matrices within this required support policy.

## Portable script baseline

The portable scripts target Bash 3.2 or newer and standard Unix userland.
Mandatory `[[ ... ]]` assertions must explicitly fail with `|| exit 1`,
`|| return 1` or a diagnostic failure handler: Bash 3.2 does not apply `set -e`
to a standalone conditional comparison. Keep intentional status-returning
predicates and branch conditions distinct. A fixture's completion flag protects
against premature exits, but cannot detect a failed assertion that continued
through normal completion and cleanup.

Repository CI exercises the offline regression set on:

| Host | Scope |
| --- | --- |
| Ubuntu 24.04 GitHub-hosted runner | Portable scripts, ShellCheck, workflow lint, installer downloads, and secret scan |
| macOS 15 Apple Silicon GitHub-hosted runner | Portable offline regression set, including release recovery fixtures, with Apple's Bash 3.2 |
| macOS 15 Intel GitHub-hosted runner | Same portable offline regression set with Apple's Bash 3.2 |

The table describes the intended CI contract. Passing qualification for a
revision requires its matching workflow run; adding a matrix entry does not
establish that the run passed.

Routine CI keeps only the newest run for each workflow and branch or PR ref.
Use workflow-level concurrency so a newer run cancels both queued and running
checks of an older revision, including its native matrix:

```yaml
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true
```

Keep source SHA and run ID out of this group; they prevent revisions from
superseding each other. Different workflows, branches and PRs remain independent.
Omit `queue: max`: retaining every pending revision defeats this policy, and
GitHub disallows combining it with `cancel-in-progress: true`.
Cancellation is asynchronous and does not certify the interrupted revision.
Inspect the exact selected commit's result before claiming native qualification;
a newer commit's success does not qualify an older release. Consumers must update
their owned workflows when adopting this guidance; snapshot refresh alone does
not change their concurrency settings.

Routine issue acceptance can use the latest delivered version that retains and
tests the behavior; it need not wait for cancelled superseded runs. Follow the
[issue maintenance procedure](../rules/agent-maintenance.md#issue-review-and-repair)
to preserve historical evidence and require an explicit version-specific reason
for any older run kept as an active gate. This does not qualify an older release
or weaken the current version's required native matrix.

Use the [CI-health task](../tasks/ci-health.md#queued-native-jobs-and-repeated-validation)
to diagnose persistent native queues and repeated branch/tag gates. Keep all
required hosts and checks on the retained run. Release, publication and deployment
operations with separate effect/recovery obligations keep explicit concurrency
identities and their own cancellation authorization.

The portable job uses GitHub Actions' default job timeout, allowing long native
builds to finish without a shorter regression-step deadline. Ordinary failures
still run the evidence collector; reaching the platform job limit can prevent
collection. Lint/security retains its 10-minute limit.

All three jobs also run real Prettier/Rust hook qualification and a native
installer failure-artifact round trip, described below. These are separate
from the offline portable suite.

### CI setup reuse and timings

Shared Tooling's native jobs cache the existing Cargo source cache and physical
Rust tool generations, including build and receipt evidence. Checkout `.tools`
selections, user Cargo credentials and host/IC bundles are not cached. Host/IC
setup therefore continues to exercise fresh pinned downloads on every runner.
The cache key uses the installer's exact compiler/native/build-policy identity,
runner image identity, Cargo tool pins and installer/checksum sources. There are
no misses solely from comments, record order or unrelated tool pins in the
literal common catalog. Catalogs with executable configuration, duplicate
assignments or unsupported syntax retain their full bytes as key input; cache
inspection never executes them or guesses their selections. There are
no broad restore prefixes or cross-host restores. Changed inputs or an absent
cache use ordinary cold setup.

A restored cache is only a setup optimization: every job still runs complete
`make install-tools`, offline `make tools-check` and actual cross-checkout reuse
qualification. Existing receipt, byte, path and lock admission remains the
authority. Cache transport errors are nonfatal; setup and validation errors are
not. Cache steps have a three-minute bound. Only main push jobs save caches,
after successful setup/reuse and before the test suite, so later cancellation
does not necessarily discard expensive preparation. PR jobs only restore.
This follows the official [cache action](https://github.com/actions/cache)
restore/save interface; a cache hit is not native qualification.

The portable suite prints a per-fixture duration/exit-status table on success
and failure. Failed runs retain `timings.tsv` with their ordinary temporary
evidence. All fixtures remain sequential and all three hosts run the complete
suite. Compare cold/warm setup and fixture timings on matching hosted inputs;
local timings and cache fixtures do not establish a hosted speed-up or explain
runner queue capacity.
The IC-tool fixture also prints cleanup start, elapsed seconds and exit status
after successful assertions. A start without an end identifies an interrupted
cleanup; use native evidence before attributing a timeout or claiming a speed-up.

Shared Tooling's portable scripts do not support Windows or non-Bash shells.
This does not prohibit a consumer from supporting additional hosts or shells
through its own qualified tooling.

## Tool-specific dependencies

The [local setup guide](local-setup.md) provides Linux Mint/macOS bootstrap
commands and the shared `make install-tools` / `make tools-check` targets.
Pinned jq, Mike Farah yq, ripgrep and cloc install under `.tools/host/bin`; none is
required to run setup. Make targets and CI select this same local tool set.

| Tool | Additional dependencies |
| --- | --- |
| Evidence download and its portable fixture | GitHub CLI, jq, unzip and SHA-256 verification; the fixture also uses zip |
| `scripts/dev/cloc.sh` | Git, Cargo, `cloc`, `jq`, `awk`, `find`, `grep`, and `sort` |
| `scripts/dev/cloc-siblings.sh` | Git and the same prepared tools as `cloc.sh`; read-only root workspace summaries |
| `scripts/dev/cloc-tooling.pl` | Git, cloc, and core Perl modules including JSON::PP and Digest::SHA; no Cargo or consumer command execution |
| `scripts/dev/github-siblings.sh` | Git, jq, awk, sort, Perl core POSIX functions, system IANA timezone data (Europe/Paris), and an authenticated GitHub CLI |
| `scripts/dev/gh-ci.sh` | Git and an authenticated GitHub CLI |
| Local maintenance coordinator | Bash 3.2+, Git, prepared/authenticated Codex CLI with `exec --approve-for-me`, and a serial scheduler; the supplied user units require Linux systemd. Task tools remain optional consumer-qualified inputs; see [local scheduling](../tasks/local-schedule.md). The offline fixture substitutes Codex and starts no agent. |
| `scripts/ci/run-validation-targets.sh` | GNU Make plus `awk`, `grep` or `rg`, `sed`, `tail`, and `tee`; opted-in input checkpoints also use core Perl JSON::PP and the shared SHA-256 helper |
| Archive installer scripts | `curl`, `tar`, a SHA-256 implementation, and the archive codec used by the selected tool |
| CI binary installer entry points | The archive prerequisites above and Perl core for exact-path atomic publication |
| Evidence archiver | Bash 3.2+, tar and gzip; explicit existing roots and relative selections |
| `scripts/dev/install-rust-tools.sh` | Prepared Rust/Cargo toolchain, jq/yq and native compilation prerequisites; standard shared build environment, Cargo/source/receipt/byte checks offline, with common-tool version probes only |
| Local IC tool setup | Bash 3.2+, `curl`, `tar`, xz/gzip, Perl, and a SHA-256 implementation; see [IC tools](ic-tools.md) |
| Nonempty Cargo test helper | Cargo with normal libtest summaries, `awk`, and `tee` |
| Exact release-tag checker | Git and the caller's selected exact commit/version |
| Exact crates.io observation | Bash, jq and curl 8.4.0+; mandatory retained metadata with bounded downloads |
| `scripts/ci/run-sccache.sh` | An absolute executable file selected through `SCCACHE_BIN` |
| Snapshot verification | A SHA-256 implementation |
| Snapshot refresh | Git, a clean Shared Tooling checkout, and a SHA-256 implementation |
| Dependency pin checker | Git, jq, Mike Farah yq v4.47.2+; Cargo when Cargo manifests exist |
| Workspace-version reader | Prepared Cargo, jq and Mike Farah yq v4.47.2+; explicit Cargo.toml input; no dependency resolution |
| Release runner | GNU Make, Git, Perl, `date`, explicit consumer metadata/check targets, and Bash 3.2 |
| PR release delivery | The release runner prerequisites, authenticated GitHub CLI with `gh api --paginate`, jq, and Git supporting `switch`, `worktree` and `fetch --no-write-fetch-head`; see the [PR contract](releases.md#pr-delivery) |
| Changelog finalizer | System awk with regular-expression `RS` (the declared Linux/macOS hosts); preserves historical EOF bytes without GNU `RT` |
| Rust pre-commit hook and installer | Git, GNU Make, consumer-owned `fmt` prerequisites (Cargo/rustfmt and an exact `cargo-sort` version), Bash 3.2 and standard Unix file utilities |
| Consumer formatting adoption checker | The hook prerequisites above, Perl, and reviewed consumer Make inputs; no implicit downloads |
| Formatter prerequisite checker | Prepared Cargo/rustfmt and the consumer's exact cargo-sort version; optional Cargo executable and `RUSTUP_TOOLCHAIN`; no installation |
| Local lockfile transformer | Perl core only; the caller separately validates the prepared graph with Cargo |
| Explicit tag maintenance | Git and Perl core modules; atomic push support for remote deletion; see [tag maintenance](tag-maintenance.md) |

Standard repository setup always includes cloc, using one authenticated
standalone Perl payload across the supported hosts. Its host substitutions are
covered by fixtures; native CI qualifies the real script on each declared host.
Standard setup also always includes ripgrep in both installation
and offline checks. Its archive verification also requires tar/gzip and
cmp. The selected native binary must report PCRE2 support. All four Linux/macOS
architecture mappings have substitute fixtures; only native execution qualifies
the corresponding official binary. See [local setup](local-setup.md).

The complete setup/check aggregates include the pinned Cargo formatter tools on
every repository. A prepared Rust/Cargo toolchain and native build prerequisites
are therefore required even for non-Rust consumers. The native regression jobs
run the complete aggregate on Linux x86-64 and both macOS architectures; narrower
CI jobs may prepare only their own declared prerequisites without claiming a
complete developer installation. Linux ARM64 remains host-set-only because the
IC set lacks a matching Quill asset.

The standalone `scripts/ci/test-rust-tools.sh` uses substitute Cargo/compiler
executables in its own prepared directory, including when invoked by absolute
path from a consumer with `rust-toolchain.toml`. It borrows the source checkout's
existing Git commit and tracked `README.md` through a read-only object reference;
only disposable fixture refs, indexes and working files change. It creates no
commits or tags and performs no pushes. Forbidden Git attempts are recorded and
fail the fixture even when an expected-failure case consumes their exit status.
Real staged hook, partial-staging, receipt and exported-input isolation checks
remain part of this same entrypoint. This contract does not cover the separate
release-history fixtures in Shared Tooling's full suite.

The hook regression fixture also requires `jq` and the `cargo-sort` version from
`ci/tool-versions.env` (`2.1.4`). CI installs it before offline tests; local
validation requires it to be prepared beforehand and never installs it implicitly.
The separate `scripts/ci/test-frontend-formatting.sh` fixture uses the exact
Node/npm runtime in `ci/frontend/package.json` and the integrity-pinned Prettier
package in its lockfile. With that runtime and the Rust formatter prerequisites
prepared, run `npm ci --prefix ci/frontend` explicitly, then run the script.
CI uses a commit-pinned setup-node action and checks the bundled npm selection
before preparation. Node and npm are test-specific prerequisites here, not new
requirements for every consumer or for the portable suite. This qualification
covers built-in parsers; consumer plugins and product inputs still need their
own native checks under the [hook rules](../rules/git-hooks.md).
The pinning regression fixture also requires the reviewed jq and yq parsers. CI installs
them from checksum-pinned Linux and macOS binaries; checks and fixtures never
download it implicitly. Its installer also maps Linux ARM64; only matching
native execution qualifies that host.

## Cargo installation assessment

The separate manually triggered `cargo-install-qualification.yml` workflow
qualifies Cargo's common registry binary mechanics on the same three native
hosts. Blob owns the frozen published-example case through
[its native assessment](https://github.com/dragginzgame/ic-blob-storage/pull/80).
It records the prepared Cargo/Rust compiler identity and retains failed-build
evidence. The workflow leaves build duration to the GitHub Actions defaults.
It does not extend the default portable gate or establish a new consumer installer
contract. See the
[assessment procedure](local-setup.md#cargo-installation-assessment) for its
selected fixtures, offline prerequisites and outstanding extraction boundaries.

## Installer-capable platforms

The CI actionlint, Gitleaks and ShellCheck installers, and the host-tool yq installer, contain asset mappings for
Linux and Darwin on x86-64 and ARM64. Branches not exercised by the repository's
installer-download CI are install-capable, not support claims.

Consumers own the exact tool versions and platform digests they admit.

The sccache CI installer preserves Canic's Linux x86-64 binary scope. Its
consumer-supplied pin selects the official musl archive. Other hosts continue
to use consumer-owned explicit setup (such as a pinned Cargo install); this
entry point does not claim a macOS or Linux ARM64 binary installation path.

The IC toolset additionally provisions and checks native executables on all
three CI hosts above. Offline fixtures exercise digest/version refusals, retained
failed and interrupted setup, and atomic activation using substituted payloads;
only the separate native installation step qualifies actual upstream binaries.
Semantic optimizer qualification lives in
[IC Build](https://github.com/dragginzgame/ic-build/pull/62), whose native matrix
exercises `-O3`, `-Os` and `-Oz` through retained artifact transactions and the
transferred Wasm execution oracle. Shared retains archive, executable and
installation/reuse qualification. Product canister and deployment checks remain
with their owners.
Each native job also selects host/IC and common Cargo tools in a second disposable
checkout with downloads disabled and verifies identical physical installations.
The portable fixtures cover different selections, corrupt payloads,
store locks and offline refusal using isolated stores.
Failure-artifact collection runs after native qualification and includes installer
logs and retained host/IC candidate directories as well as portable fixtures.
It also selects the compact formatting runner's `formatting.*` failure logs from
the selected temporary root; CI callers use `RUNNER_TEMP` for those logs.
It also selects available `rust-tools-*.log` files, the physical `.tools/rust`
tree and its admitted shared generations, independently of compact host/IC
retention. Local links are retained without implicit traversal; shared payloads
are selected explicitly. Consumers capture setup and offline-check output with `tee` under
the selected temporary root while preserving pipeline failure status.
The local composite action `.github/actions/retain-failure-evidence/action.yml`
owns that path selection and uploads one `evidence.tar.gz` created by the shared
evidence archiver, including hidden inputs and excluding `.git` metadata.
Consumers with a product-specific collector may instead pass absolute
`prepared-archive` and `collection-log` paths. That branch never selects tools,
repackages evidence or runs a collector; the caller owns original collection
status, archive safety and content qualification. Both files are required inputs,
but a missing or final-symlink file is excluded while the other is still uploaded;
completeness then fails after upload. Relative/multiline, glob-pattern or identical paths are
rejected. The prepared branch runs even after a previous step fails or is
cancelled, preserving the caller's log independently of archive creation.
`temp-root` and `repository-root` remain required by the collecting branch only.
Uploads use zero ZIP compression because the selected archive is already compressed.

The same action collects a
disposable negative qualification on each native host: the real IC installer
downloads Quill and rejects an intentionally incorrect pin, then offline checking
rejects those native bytes under a damaged receipt. Only the fixture's pins and
receipt are altered; active tools and the reviewed catalog are untouched.
Rust qualification uses the real Make targets and installer with a failing Cargo
substitute, followed by the real offline check. Its build payload and both logs
join the same uploaded/downloaded checksum verification; this qualifies retention,
rather than compilation of registry packages.

`scripts/ci/qualify-native-failure-retention.sh <new-directory>` performs that
online negative qualification. It intentionally exits 1 after validating the IC
and Rust Make failure statuses and writing `expected.sha256`; earlier failures
leave no completed manifest. All evidence is retained. CI observes the failed step,
requires that invocation's completion output (an old manifest cannot qualify it),
uploads through the common collector, downloads the exact returned artifact ID,
and extracts the archive to check its payload against the original checksum
manifest. Both full and compact readbacks use `scripts/ci/download-evidence.sh`
with the upload output ID and SHA-256, scoped to the current repository and run.
It requires GitHub CLI, jq, unzip and an explicit `GH_TOKEN` with `actions: read`.
Only the portable job grants that permission. Direct artifact metadata must match
the ID, run, expiry status and upload digest. The downloaded ZIP must match that
digest and contain exactly `evidence.tar.gz`; payload checks remain independent.

Only reported HTTP 404 responses from metadata or the exact-ID download are
retried: six attempts with 2, 4, 8, 16 and 30 seconds of backoff. Each hosted
readback has a three-minute timeout. Other API failures, malformed metadata,
identity conflicts, unexpected archive members and checksum failures stop
immediately. Every attempt remains in the destination, which must be new.
No separate listing preflight, name fallback, upload replay or extraction of
arbitrary ZIP paths occurs. This addresses the visibility regression tracked in
[#158](https://github.com/dragginzgame/shared-tooling/issues/158); hosted
qualification is still required on every native host.
Separate checks cover a newline/colon filename's bytes and mode,
executable state and a symlink whose target stays outside the selection. Missing logs,
hidden candidate files, failed uploads, early fixture failures and corrupted
downloads fail the job. Source hashes, commit, host and run identity stay with
the artifact. Ordinary failures still reach the final collector. A local fixture
pass is not hosted upload evidence: qualification requires the matching run's
successful download/verification on Linux and both macOS hosts.
The full IC set currently excludes Linux ARM64 because its Quill release has
no matching ARM64 asset. No translation or source build is substituted silently.

## Snapshot export measurements

Shared Tooling's manual [Snapshot refresh comparison workflow](https://github.com/dragginzgame/shared-tooling/blob/main/.github/workflows/snapshot-performance.yml)
supplies comparable repeated native observations for
[#219 — Reduce repeated snapshot work](https://github.com/dragginzgame/shared-tooling/issues/219).
Select full lowercase commit SHAs for the before exporter, after exporter and
one payload shared by both. Both exporter revisions must support the three fixed
entrypoints: snapshot refresh, Rust-tool installation and release runner. The
workflow needs those commits in the fetched repository history; it installs no
tools and runs no release or full portable gate.
The two runner-local integrity helpers are frozen with the common payload, so
only the exporter blob varies. Both exporters must support those helper inputs.

On each host, one warmup pair precedes six measurement pairs with alternating
execution order. Each export starts with a fresh consumer and the same selected
Bash/Git/checksum tools. Monotonic timings exclude checkout, consumer setup and
verification. Every consumer passes snapshot verification, and paired receipts
and payloads must match. `identities.txt` records commit/blob and host/tool
identities; `samples.tsv` keeps raw ordered observations. Failed attempts retain
logs and inputs and cannot produce `complete.txt`.

The workflow packages selected observations, runner files and consumer trees
through the canonical archiver before upload. Hidden snapshot receipts survive
the archive round trip; source clones and Git stores are excluded. Successful
packaging retains the archive, SHA-256 and packaging/command logs for both
complete and incomplete measurement attempts.
A packaging failure remains a job failure and retains any partial archive;
it does not reconstruct a missing measurement completion receipt.

From a Shared Tooling checkout, the same comparison can run locally through
`bash scripts/distribution/measure-snapshot-refresh.sh BEFORE AFTER PAYLOAD NEW-DIRECTORY`.
The payload supplies both runners' integrity helpers and, when present, their
shared companion reader; its blob is recorded with the other frozen helpers.
Select a payload containing that reader when measuring exporters that require
it. Historical payloads remain suitable for their self-contained exporters.
Use an absent evidence directory beneath an existing parent, with no CR/LF in
either path. Evidence is retained on success and failure. The bounded export
workload does not establish a whole-suite speedup; compare repeated samples
within each host and retain the distinct Linux/macOS observations.
