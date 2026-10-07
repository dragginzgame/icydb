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
Repository CI exercises the offline regression set on:

| Host | Scope |
| --- | --- |
| Ubuntu 24.04 GitHub-hosted runner | Portable scripts, ShellCheck, workflow lint, installer downloads, and secret scan |
| macOS 15 Apple Silicon GitHub-hosted runner | Portable offline regression set, including release recovery fixtures, with Apple's Bash 3.2 |
| macOS 15 Intel GitHub-hosted runner | Same portable offline regression set with Apple's Bash 3.2 |

The table describes the intended CI contract. Passing qualification for a
revision requires its matching workflow run; adding a matrix entry does not
establish that the run passed.

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
| `scripts/dev/gh-ci.sh` | Git and an authenticated GitHub CLI |
| `scripts/ci/run-validation-targets.sh` | GNU Make plus `awk`, `grep` or `rg`, `sed`, `tail`, and `tee` |
| Archive installer scripts | `curl`, `tar`, a SHA-256 implementation, and the archive codec used by the selected tool |
| `scripts/dev/install-rust-tools.sh` | Prepared Rust/Cargo toolchain and native compilation prerequisites for explicit registry installation; offline `--check` only runs the selected local executables |
| Local IC tool setup | Bash 3.2+, `curl`, `tar`, xz/gzip, Perl, and a SHA-256 implementation; see [IC tools](ic-tools.md) |
| Nonempty Cargo test helper | Cargo with normal libtest summaries, `awk`, and `tee` |
| Exact release-tag checker | Git and the caller's selected exact commit/version |
| `scripts/ci/run-sccache.sh` | An executable `sccache` binary |
| Snapshot verification | A SHA-256 implementation |
| Snapshot refresh | Git, a clean Shared Tooling checkout, and a SHA-256 implementation |
| Dependency pin checker | Git, jq, Mike Farah yq v4.47.2+; Cargo when Cargo manifests exist |
| Workspace-version reader | Prepared Cargo, jq and Mike Farah yq v4.47.2+; explicit Cargo.toml input; no dependency resolution |
| Release runner | GNU Make, Git, `date`, explicit consumer metadata/check targets, and Bash 3.2 |
| Rust pre-commit hook and installer | Git, GNU Make, consumer-owned `fmt` prerequisites (Cargo/rustfmt and an exact `cargo-sort` version), Bash 3.2 and standard Unix file utilities |
| Consumer formatting adoption checker | The hook prerequisites above, Perl-free shell utilities, and reviewed consumer Make inputs; no implicit downloads |
| Formatter prerequisite checker | Prepared Cargo/rustfmt and the consumer's exact cargo-sort version; optional Cargo executable and `RUSTUP_TOOLCHAIN`; no installation |
| Local lockfile transformer | Perl core only; the caller separately validates the prepared graph with Cargo |
| Explicit tag maintenance | Git and Perl core modules; atomic push support for remote deletion; see [tag maintenance](tag-maintenance.md) |

Standard repository setup selects cloc with `--with-cloc`, using one authenticated
standalone Perl payload across the supported hosts. Its host substitutions are
covered by fixtures; native CI qualifies the real script on each declared host.
Standard setup also selects ripgrep with `--with-ripgrep` in both installation
and offline checks. Its archive verification also requires tar/gzip and
cmp. The selected native binary must report PCRE2 support. All four Linux/macOS
architecture mappings have substitute fixtures; only native execution qualifies
the corresponding official binary. See [local setup](local-setup.md).

The hook regression fixture also requires `jq` and the `cargo-sort` version from
`ci/tool-versions.env` (`2.1.4`). CI installs it before offline tests; local
validation requires it to be prepared beforehand and never installs it implicitly.
The pinning regression fixture also requires the reviewed jq and yq parsers. CI installs
them from checksum-pinned Linux and macOS binaries; checks and fixtures never
download it implicitly. Its installer also maps Linux ARM64; only matching
native execution qualifies that host.

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
Failure-artifact collection runs after native qualification and includes installer
logs and retained host/IC candidate directories as well as portable fixtures.
The full IC set currently excludes Linux ARM64 because its Quill release has
no matching ARM64 asset. No translation or source build is substituted silently.
