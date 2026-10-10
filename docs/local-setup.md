# Local developer setup

From the checkout root, prepare the common executables explicitly:

```bash
make install-tools
make tools-check
export PATH="$PWD/.tools/host/bin:$PWD/.tools/ic/bin:$PWD/.tools/rust/bin:$PATH"
```

Prepare the declared Rust/Cargo toolchain and native build prerequisites first,
even when the repository has no Rust packages. The Makefile uses local tool
paths automatically. Interactive shells need the
export above; setup does not edit shell profiles. Installation downloads tools;
checks are offline and never install missing dependencies. Before downloads,
`install-tools` uses the existing IC and Rust installers to admit the complete-set
platform/pins and probe the consumer's selected `rustc` and `cargo`. These
read-only `--preflight` calls create no tool/build directories and disable
Rustup auto-installation; missing or unavailable toolchains require explicit
bootstrap. This is not a compiler/linker qualification or a toolchain upgrade.
Setup then runs host, IC and Rust installation in that order, followed by declared product tools;
`tools-check` checks the same sets in order. Each stops at the first failure.
Sets activate independently: a later failure preserves earlier completed sets.

## Required tool inventory

This is the common setup list for every repository adopting the
[engineering baseline](../DRAGGINZGAME.md). The host, IC and Cargo-tool sets are required
regardless of whether a repository currently uses every executable. Consumers
add their product prerequisites to local setup documentation; omissions or
different support scopes require an explicitly approved exception.

| Set | Executables | Setup and verification |
| --- | --- | --- |
| System bootstrap | Bash, Git, GNU Make, curl, CA certificates, tar, gzip, xz, Perl, a SHA-256 implementation and standard Unix utilities | Host package manager; see [bootstrap prerequisites](#bootstrap-prerequisites) |
| Common host tools | `jq`, Mike Farah `yq`, `rg` with PCRE2, `cloc` | `make install-host-tools`; offline `make host-tools-check`; `.tools/host/bin` |
| Common IC tools | `quill`, `icp`, `didc`, `ic-wasm`, `wasm-opt` | `make install-ic-tools`; offline `make ic-tools-check`; `.tools/ic/bin` |
| Common Cargo tools | `cargo-sort`, `cargo-sort-derives`, `candid-extractor` | `make install-rust-tools`; offline `make rust-tools-check`; `.tools/rust/bin`. All repositories prepare a declared Rust/Cargo toolchain and native build prerequisites first. |
| Rust development | rustfmt and product compilation targets | Consumer toolchain setup and the [formatter prerequisite check](verification-helpers.md#formatter-prerequisites) |
| Workflow-specific tools | ShellCheck, actionlint, Gitleaks, authenticated `gh`, Node/SDKs and other tools used by that repository | Explicit consumer setup; declare the tools required by each workflow |

`make install-tools` and `make tools-check` cover all 12 common executables,
regardless of the repository's implementation language or current use. They do
not install a Rust toolchain or authenticate GitHub. Product and workflow-specific
tools extend the aggregate through existing consumer-owned targets:

```make
LOCAL_TOOL_INSTALL_TARGETS += install-testkit
LOCAL_TOOL_CHECK_TARGETS += testkit-check
include make/tools.mk
```

These lists run sequentially after the common sets, including under parallel
Make. Declare each required product setup/check pair; do not add prerequisites
to `install-tools`/`tools-check`, override their recipes, or include an aggregate
in its own extension list. Extensions must not call their enclosing aggregate.
Select independently qualified CLI versions in their existing owner adapters.
Rustfmt, compiler targets, credentials and other product prerequisites retain
their documented setup; executable installation grants no deployment authority.
Versions and digests remain
owned by the reviewed pin files below, rather than another copied version list.
dfx is excluded from the common IC set. See the host limits below before setup.
Keep identity stores separate from resettable state as described in
[IC identity storage](ic-tools.md#identity-storage-and-local-resets); installing
executables does not authorize moving keys or wiping a CLI home.

Make and CI must select their own checkout's local tool paths. CI exports them
for subsequent steps explicitly; Make's export does not change the calling
terminal. Follow the [consumer adoption example](consuming-snapshots.md#local-ic-tool-adoption)
for the complete installation/check selections, including ripgrep and cloc.
Those commands are owned by `make/tools.mk`: consumers include the reviewed
snapshot's file instead of maintaining copied recipes or installer flags. Shared
Tooling's own Makefile and CI use the same commands. Snapshot adoption brings
command updates; explicit setup brings newly required executables.

The include also selects `make/execution.mk` and its
`scripts/ci/check-make-execution.sh` companion. It rejects dry-run, touch,
question and ignore-errors modes before recipes execute. Cargo recipes retain
Make's jobserver descriptors for parallel execution; this does not grant
installation authority to an inspection command or change the setup order.

The installed `cloc` executable and local workspace `make cloc` are common setup.
Fleet reports such as `make cloc-tooling` normally run in Shared Tooling; consumers
need not vendor those reporters or run their regression suites. See the
[optional fleet selection](consuming-snapshots.md#local-ic-tool-adoption).

## Diagnosing a missing command

Installing tools in Shared Tooling does not install them in another checkout.
From the affected repository's root, compare:

```bash
.tools/host/bin/cloc --version
command -v cloc
```

If the first command works but the second is missing or points elsewhere, select
this checkout's tools in the current terminal:

```bash
export PATH="$PWD/.tools/host/bin:$PWD/.tools/ic/bin:$PWD/.tools/rust/bin:$PATH"
```

Select the paths again when switching repositories; do not permanently select a
sibling checkout's tool directory in a shell profile. Repository Make targets
must work without this manual export. If the local executable is absent, run
`make install-host-tools` and `make host-tools-check` in that repository. If its
setup still omits cloc, adopt the reviewed shared installer, pins and complete
Make/CI selection first. A PATH change cannot supply an uninstalled executable.
Installing `cloc` also does not create a `make cloc` target: Rust LOC report
adoption needs the shared report script and the Make include shown in the
[consumer adoption guide](consuming-snapshots.md#local-ic-tool-adoption).

## Pinned local tool sets

`make install-host-tools` installs jq, **Mike Farah yq** (including its TOML
parser), ripgrep with PCRE2, and cloc under `.tools/host/bin`. `make host-tools-check`
verifies their bytes before executing version and feature checks. Failures report
the exact tool, expected version, selected path, reason and repair command.
Actual version/probe output is included only after all payloads authenticate;
missing or unauthenticated executables are never run for diagnosis. The reviewed selections
live in [ci/tool-versions.env](../ci/tool-versions.env):
[jq 1.8.2](https://github.com/jqlang/jq/releases/tag/jq-1.8.2),
[yq 4.47.2](https://github.com/mikefarah/yq/releases/tag/v4.47.2),
[ripgrep 15.2.0](https://github.com/BurntSushi/ripgrep/releases/tag/15.2.0), and
[cloc 2.10](https://github.com/AlDanial/cloc/releases/tag/v2.10).
Setup itself needs neither parser installed. Distribution packages named `yq`
may provide a different implementation; they do not replace the selected parser.

Host setup downloads official native binaries for Linux x86-64/ARM64 and macOS
Intel/Apple Silicon and cloc's portable standalone Perl script. It checks their
pinned SHA-256 values and exact versions, and
atomically activates the selected set. Previous sets and failed candidates
remain under `.tools/host-set.*`. A lock prevents simultaneous installations;
inspect `.tools/.host-tools.lock/owner` and confirm its process has stopped before
removing an abandoned lock. Cleanup remains explicit. The `--versions` option
selects a reviewed shell file, which is executable code like a Makefile.

Setup and offline checks admit the literal `.tools/host` link target. Malformed
managed names, including trailing newlines, are refused before tool execution
or downloads; the existing link and bundles remain intact.

Host installation and offline checks always include ripgrep. Consumers add `SHARED_TOOLING_RIPGREP_VERSION`
and the four `SHARED_TOOLING_RIPGREP_SHA256_*` archive digests to their reviewed
versions file.
Offline checks authenticate the retained archive and compare its executable
bytes with installed `rg` before running it. They require the exact version
(allowing the official revision annotation) and a successful `--pcre2-version`.
No Cargo installation, global PATH change or system-package replacement occurs.

Host installation and offline checks also always include the standalone cloc payload.
Consumers adopting it add `SHARED_TOOLING_CLOC_VERSION` and the single
`SHARED_TOOLING_CLOC_SHA256` pin to their reviewed versions file. All four payloads are authenticated
before executing any version check; failed candidates retain the previous active
set. cloc uses the bootstrap Perl interpreter and needs no Cargo build or system
package installation. LOC reports select the prepared local host path themselves
and never install tools during counting.

The [IC set](ic-tools.md) supplies Quill, ICP CLI, didc, ic-wasm and
wasm-opt. dfx is excluded. Linux ARM64 supports the host set only; the full IC
set lacks a matching Quill asset. Native CI qualifies both sets on Linux x86-64
and both macOS architectures; mapping Linux ARM64 is not native qualification.
PocketIC consumers additionally use their selected IC Testkit CLI's explicit
`setup` and offline `check`; follow the
[ownership handoff](ic-tools.md#pocketic-ownership-handoff).

## Rust development tools

After preparing the consumer's declared Rust/Cargo toolchain and compilation
prerequisites, use `make install-rust-tools` and offline `make rust-tools-check`.
The shared helper installs cargo-sort, cargo-sort-derives and candid-extractor
under `.tools/rust/bin`, using their exact versions from
[ci/tool-versions.env](../ci/tool-versions.env) and Cargo's `--locked` installation.
`RUST_TOOL_VERSIONS` selects a reviewed alternative catalog; it defaults to
`HOST_TOOL_VERSIONS`. Keep qualified version exceptions in that selected catalog,
and remove duplicate consumer constants when adopting the shared pins.

The complete common aggregate includes this set in every repository, including
non-Rust repositories. The narrow commands remain useful for explicit retries
or diagnostics; they do not establish that the complete toolset is ready.
Shared Make commands include `.tools/rust/bin` on PATH; interactive shells use
the export at the top of this guide. The standard formatting hook and its
adoption check preserve the original checkout's host, IC and Rust tool paths
while formatting isolated index inputs. The helper never prepares or upgrades a
toolchain implicitly. Installation may fetch registry dependencies and compile;
build output stays in `.tools/rust/build`, including on failure. Cargo owns
installation locking and registry checksum verification. Tools install one at
a time, so an interrupted setup can leave earlier tools installed; rerun setup
and require the complete offline check before use. Cleanup remains explicit.

Setup and offline checking reject symlinks or wrong file types along the Rust
installation route: `.tools`, `rust`, `bin`, `build`, the three executables and
Cargo's `.crates.toml`/`.crates2.json` receipts. The complete route is admitted
before executing a tool or Cargo, and rechecked after installation. Existing
managed `.tools/host` and `.tools/ic` links remain supported. A checkout alias
resolves to its physical root; this path check does not sandbox Cargo, authenticate
tool bytes or defend against concurrent replacement of the admitted paths.

Offline checks require each local executable to report its selected version
successfully. They do not authenticate installed bytes or replace product/native
host qualification. A matching set is reused without invoking Cargo. Formatting
still requires rustfmt and the [formatter check](verification-helpers.md#formatter-prerequisites).

### Consumer-selected Cargo tools

The same installer also accepts an exact crates.io package, one binary or example
target, and an explicit `debug` or `release` profile. Prepare the shared host tools
(including jq and Perl) and the consumer-selected Rust toolchain first:

```bash
bash scripts/dev/install-rust-tools.sh --consumer "$PWD" \
  --package ic-blob-storage --version 0.15.1 --example prepare_upload --profile debug
# Repeat the same selection with --check for an offline, non-building check.
```

This example is a caller selection, not a new fleet-wide package pin. Consumers
own package versions, profiles, compiler selection, explicit executable overrides
and product qualification. Use `--bin NAME` for a published binary. This mode
does not install the formatter bundle or read its versions catalog; `make
install-rust-tools` continues to install that existing three-tool bundle.

For a tool selected by a consumer lockfile, replace `--version` with `--lockfile`:

```bash
bash scripts/dev/install-rust-tools.sh --consumer "$PWD" \
  --package ic-testkit --lockfile Cargo.lock --bin ic-testkit-server --profile release
```

Relative lockfile paths resolve beneath `--consumer`; absolute paths select an
explicit independent graph. The prepared host yq/jq tools read TOML without Cargo
resolution, downloads or lockfile writes. Exactly one package with the selected
name must exist, with an exact stable version and the crates.io registry source.
Missing, malformed, symlinked, ambiguous, local/Git/other-registry or prerelease
selections refuse before installation. `--version` and `--lockfile` are mutually
exclusive. Consumer-local `.tools/host/bin` takes precedence for the reader.
The same arguments with `--check` admit the selected installation offline.
Selection is read again before candidate activation and before returning a path;
a changed selection fails and retains any build attempt. Retry against the new
lock selection; prior installations remain intact. Consumers still own the
selected graph, package, target/profile and Testkit's separate server setup/check.

The command prints the admitted executable path under
`.tools/rust/<package>-<version>-<kind>-<target>-<profile>/installed/bin/`.
Use that returned path in the consumer adapter. Each selection is immutable:
an existing installation must pass physical-path, exact Cargo receipt and local
byte-digest checks. Changed bytes or receipts fail without repair or execution;
each receipt must contain exactly one JSON document. Cargo installation failures
return Cargo's original exit status along with the retained attempt location;
no invented `--version` probe runs for examples. Checks invoke rustc for the
selected host but never Cargo or downloads. Digests detect local changes; they
are not publisher signatures. The consumer still owns compiler compatibility.

Missing or invalid selected-installation diagnostics identify the package, exact
version, target kind/name, profile and destination. The caller supplies its own
setup command; successful selection still prints only the executable path.
Dependency updates that change the selected CLI must prepare and check it before
preparation is complete. Authorized releases use the existing setup/check targets
in [preflight](releases.md#selected-executable-tools-before-validation); fetching
Cargo sources alone is insufficient. Complete gates and standalone qualification
check tools offline before builds, without implicit installation.

Setup compiles through the same locked Cargo installation command into a fresh
attempt directory under `.tools/rust/build`, the existing CI evidence route.
Only an admitted candidate is renamed into place. Failed
attempts retain logs/builds, and earlier version selections remain untouched.
A directory lock rejects concurrent setup for the same selection; retry after
its owner finishes. An abruptly killed process may leave a lock: inspect that
owner and retained attempt before explicitly removing the empty lock. The tool
never guesses that a lock is stale. Redirected output, receipt and lock paths
refuse. After Cargo returns, setup rechecks the shared directory ancestors and
selection slot before admitting or activating the candidate. As with the fixed
bundle, path admission is not a sandbox against another
process deliberately replacing paths while setup runs. Installation may fetch
dependencies; `CARGO_NET_OFFLINE=true` remains authoritative.

Consumers adopt this mode from a reviewed snapshot, qualify their selected
package/profile on their native hosts, then remove superseded resolver/build
helpers. Local Canic source snapshots and application evidence remain outside
this registry installer. The three-host assessment below exercises the shared
production mode; its earlier Cargo-only results do not qualify this extension.

## Cargo installation assessment

Run `scripts/ci/qualify-cargo-install.sh` in Shared Tooling to exercise Cargo's
native installation contract in a new disposable evidence directory:

```bash
mkdir -p .tools
CARGO_NET_OFFLINE=true bash scripts/ci/qualify-cargo-install.sh "$PWD/.tools/cargo-install-assessment"
```

The offline command requires the registry packages and their locked dependencies
to be prepared already. Explicit online qualification may omit
`CARGO_NET_OFFLINE=true`; it installs only into the new evidence root and retains
builds and logs on both success and failure. Relative evidence paths resolve from
the caller's working directory independently of `CDPATH`; existing roots are
refused before tool probes. It never cleans or changes an existing installation.
The fixture selects published `ic-blob-storage 0.15.1`'s
`prepare_upload` example and the existing reviewed cargo-sort version. Those are
assessment inputs, not defaults for consumer applications. Both use the debug
profile for the direct Cargo assessment. The production installer is then
exercised with the debug example and release binary, including offline reuse and
changed-byte refusal. No product MSRV or Canic CLI selection is qualified by this fixture.

It checks Cargo receipt package/registry/version/target/profile identity, offline
reuse, missing-target refusal, concurrent offline installation and preservation
of the executable and receipts after an injected compiler failure. It records
local executable digests and rejects changed bytes using the shared checksum
owner. Those digests detect changes to an observed build; they are not publisher
signatures, and a Cargo receipt alone does not authenticate executable bytes.
For the selected `--debug` command, receipt admission accepts Cargo's `dev` and
`debug` spellings and rejects release profiles; original receipts and toolchain
identity remain in the evidence root.

The manually triggered **Cargo installation qualification** workflow runs this
assessment on Linux and both macOS architectures with GitHub Actions' default
duration limits and failure evidence collection. It does not add registry builds
to the default portable gate. Native results, consumer-specific versions and an actual shared
installer's path/receipt/lock refusal still need qualification before extraction
under [#65](https://github.com/dragginzgame/shared-tooling/issues/65). The current
fixed Rust installer remains the supported setup contract.

## Bootstrap prerequisites

Use the host package manager for the shell, Git, GNU Make, curl, archive tools,
Perl and ordinary utilities. For Linux Mint/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y bash git make curl ca-certificates tar gzip xz-utils perl
```

On macOS, with Xcode Command Line Tools and Homebrew already installed:

```bash
brew install make curl xz
```

macOS supplies Bash, Git (through Command Line Tools), tar, gzip, Perl and shasum.
Homebrew GNU Make is available as `gmake`; use it where the system Make does not
meet a consumer's requirements. These system packages follow the host's package
manager updates; they are not claimed to be checksum-pinned shared binaries.
Shared setup never invokes sudo or installs a package manager implicitly.
The pinned local host set supplies ripgrep and cloc; neither is a bootstrap dependency.

Every repository prepares its declared Rust/Cargo toolchain and native build
prerequisites before the common tool setup. Rust development and Shared Tooling's
portable fixtures also need rustfmt. After that bootstrap, use the common setup
and select its paths before invoking scripts directly:

```bash
make install-tools
make tools-check
export PATH="$PWD/.tools/host/bin:$PWD/.tools/ic/bin:$PWD/.tools/rust/bin:$PATH"
```

Before starting portable fixtures, `test-portable-tools.sh` checks the required
commands on PATH (including cloc, shasum, jq, Mike Farah yq and ripgrep) and reuses
the offline formatter prerequisite check. Run this admission independently with
`bash scripts/ci/check-portable-prerequisites.sh`. It reports missing commands
together and points back here; it never installs tools or replaces the individual
fixtures' version, feature and behavior checks. Standard Unix utilities from the
bootstrap environment are still required. This is Shared Tooling's own suite
setup, not a new prerequisite gate for every consumer.

ShellCheck, actionlint and Gitleaks use the existing explicit CI installers and
pins; GitHub maintenance needs an installed, authenticated `gh`. Consumer-owned
Node/Rust toolchains, SDKs and product dependencies remain part of that repo's
declared setup. See the [complete script dependency table](supported-hosts.md#tool-specific-dependencies).
