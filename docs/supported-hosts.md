# Supported Hosts And Dependencies

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
| `scripts/dev/cloc.sh` | Git, Cargo, `cloc`, `jq`, `awk`, `find`, `grep`, and `sort` |
| `scripts/dev/cloc-siblings.sh` | Git and the same prepared tools as `cloc.sh`; read-only root workspace summaries |
| `scripts/dev/cloc-tooling.pl` | Git, cloc, and core Perl modules including JSON::PP and Digest::SHA; no Cargo or consumer command execution |
| `scripts/dev/github-siblings.sh` | Git, jq, awk, sort, Perl core POSIX functions, system IANA timezone data (Europe/Paris), and an authenticated GitHub CLI |
| `scripts/dev/gh-ci.sh` | Git and an authenticated GitHub CLI |
| Local maintenance coordinator | Bash 3.2+, Git, prepared/authenticated Codex CLI with `exec --approve-for-me`, and a serial scheduler; the supplied user units require Linux systemd. Task tools remain optional consumer-qualified inputs; see [local scheduling](../tasks/local-schedule.md). The offline fixture substitutes Codex and starts no agent. |
| `scripts/ci/run-validation-targets.sh` | GNU Make plus `awk`, `grep` or `rg`, `sed`, `tail`, and `tee` |
| Archive installer scripts | `curl`, `tar`, a SHA-256 implementation, and the archive codec used by the selected tool |
| CI binary installer entry points | The archive prerequisites above and Perl core for exact-path atomic publication |
| Evidence archiver | Bash 3.2+, tar and gzip; explicit existing roots and relative selections |
| `scripts/dev/install-rust-tools.sh` | Prepared Rust/Cargo toolchain and native compilation prerequisites for explicit registry installation; offline `--check` only runs the selected local executables |
| Local IC tool setup | Bash 3.2+, `curl`, `tar`, xz/gzip, Perl, and a SHA-256 implementation; see [IC tools](ic-tools.md) |
| Nonempty Cargo test helper | Cargo with normal libtest summaries, `awk`, and `tee` |
| Exact release-tag checker | Git and the caller's selected exact commit/version |
| Exact crates.io observation | Bash/curl for presence; metadata mode additionally requires jq and curl 8.4.0+ for bounded downloads |
| `scripts/ci/run-sccache.sh` | An executable `sccache` binary |
| Snapshot verification | A SHA-256 implementation |
| Snapshot refresh | Git, a clean Shared Tooling checkout, and a SHA-256 implementation |
| Dependency pin checker | Git, jq, Mike Farah yq v4.47.2+; Cargo when Cargo manifests exist |
| Workspace-version reader | Prepared Cargo, jq and Mike Farah yq v4.47.2+; explicit Cargo.toml input; no dependency resolution |
| Release runner | GNU Make, Git, `date`, explicit consumer metadata/check targets, and Bash 3.2 |
| PR release delivery | The release runner prerequisites, authenticated GitHub CLI with `gh api --paginate`, jq, and Git supporting `switch`, `worktree` and `fetch --no-write-fetch-head`; see the [PR contract](releases.md#pr-delivery) |
| Changelog finalizer | System awk with regular-expression `RS` (the declared Linux/macOS hosts); preserves historical EOF bytes without GNU `RT` |
| Rust pre-commit hook and installer | Git, GNU Make, consumer-owned `fmt` prerequisites (Cargo/rustfmt and an exact `cargo-sort` version), Bash 3.2 and standard Unix file utilities |
| Consumer formatting adoption checker | The hook prerequisites above, Perl-free shell utilities, and reviewed consumer Make inputs; no implicit downloads |
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

The complete setup/check aggregates include the three pinned Cargo tools on
every repository. A prepared Rust/Cargo toolchain and native build prerequisites
are therefore required even for non-Rust consumers. The native regression jobs
run the complete aggregate on Linux x86-64 and both macOS architectures; narrower
CI jobs may prepare only their own declared prerequisites without claiming a
complete developer installation. Linux ARM64 remains host-set-only because the
IC set lacks a matching Quill asset.

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
qualifies Cargo's registry binary/example mechanics on the same three native
hosts. It records the prepared Cargo/Rust compiler identity and retains failed-build
evidence. The workflow leaves build duration to the GitHub Actions defaults.
It does not extend the default portable gate or establish a new consumer installer
contract. See the
[assessment procedure](local-setup.md#cargo-installation-assessment) for its
selected fixtures, offline prerequisites and outstanding extraction boundaries.

## Installer-capable platforms

The actionlint, Gitleaks, ShellCheck and yq installers contain asset mappings for
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
After preparing the pinned Node runtime, the native jobs also run
`scripts/ci/qualify-wasm-opt.sh` against that installed IC set. Its retained
Wasm inputs/outputs exercise `-O3`, `-Os` and `-Oz` through Node's WebAssembly
engine, including integer branches and an IC-style reply import. Evidence lives
under `portable-fixtures/wasm-opt` for ordinary failure collection. This small
smoke does not qualify consumer canister builds or deployment; those remain
with the product owner.
Failure-artifact collection runs after native qualification and includes installer
logs and retained host/IC candidate directories as well as portable fixtures.
It also selects the compact formatting runner's `formatting.*` failure logs from
the selected temporary root; CI callers use `RUNNER_TEMP` for those logs.
It also selects available `rust-tools-*.log` files and `.tools/rust/build`,
independently of compact host/IC retention. Rust build evidence is selected only
through physical parent directories; a final build symlink is retained without
following it. Consumers capture setup and offline-check output with `tee` under
the selected temporary root while preserving pipeline failure status.
The local composite action `.github/actions/retain-failure-evidence/action.yml`
owns that path selection and uploads one `evidence.tar.gz` created by the shared
evidence archiver, including hidden inputs and excluding `.git` metadata.
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
manifest. Both full and compact evidence readbacks use the pinned download
action's authenticated REST path, scoped to the current repository and run.
Only the portable job grants `actions: read`; download digest mismatches fail
before the independent payload checks. This transport selection follows
[the Blob readback report](https://github.com/dragginzgame/shared-tooling/issues/93);
its original internal lookup failure has no proven cause, and local checks do
not establish hosted acceptance of the new path.
Separate checks cover a newline/colon filename's bytes and mode,
executable state and a symlink whose target stays outside the selection. Missing logs,
hidden candidate files, failed uploads, early fixture failures and corrupted
downloads fail the job. Source hashes, commit, host and run identity stay with
the artifact. Ordinary failures still reach the final collector. A local fixture
pass is not hosted upload evidence: qualification requires the matching run's
successful download/verification on Linux and both macOS hosts.
The full IC set currently excludes Linux ARM64 because its Quill release has
no matching ARM64 asset. No translation or source build is substituted silently.
