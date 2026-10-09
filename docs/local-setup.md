# Local developer setup

From the checkout root, prepare the common executables explicitly:

```bash
make install-tools
make tools-check
export PATH="$PWD/.tools/host/bin:$PWD/.tools/ic/bin:$PWD/.tools/rust/bin:$PATH"
```

The Makefile uses those local paths automatically. Interactive shells need the
export above; setup does not edit shell profiles. Installation downloads tools;
checks are offline and never install missing dependencies. `install-tools` runs
host setup followed by IC setup, stopping on failure. Each set activates
independently: a later IC failure leaves the successfully installed host set.

## Required tool inventory

This is the common setup list for every repository adopting the
[engineering baseline](../DRAGGINZGAME.md). The host and IC sets are required
regardless of whether a repository currently uses every executable. Consumers
add their product prerequisites to local setup documentation; omissions or
different support scopes require an explicitly approved exception.

| Set | Executables | Setup and verification |
| --- | --- | --- |
| System bootstrap | Bash, Git, GNU Make, curl, CA certificates, tar, gzip, xz, Perl, a SHA-256 implementation and standard Unix utilities | Host package manager; see [bootstrap prerequisites](#bootstrap-prerequisites) |
| Common host tools | `jq`, Mike Farah `yq`, `rg` with PCRE2, `cloc` | `make install-host-tools`; offline `make host-tools-check`; `.tools/host/bin` |
| Common IC tools | `quill`, `icp`, `didc`, `ic-wasm`, `pocket-ic`, `wasm-opt` | `make install-ic-tools`; offline `make ic-tools-check`; `.tools/ic/bin` |
| Rust repositories | Declared Rust/Cargo toolchain, rustfmt, pinned Cargo tools and required compilation targets | Consumer toolchain setup, [Rust tool setup](#rust-development-tools) and the [formatter prerequisite check](verification-helpers.md#formatter-prerequisites) |
| Workflow-specific tools | ShellCheck, actionlint, Gitleaks, authenticated `gh`, Node/SDKs and other tools used by that repository | Explicit consumer setup; declare the tools required by each workflow |

`make install-tools` and `make tools-check` cover both common local sets. Rust
and workflow-specific setup stays explicit; those targets do not claim to
prepare a product toolchain or authenticate GitHub. Versions and digests remain
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
verifies their bytes before executing version and feature checks. The reviewed selections
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

The shared Make include selects the installer's `--with-ripgrep` flag in both
installation and offline checks. Consumers adopting it add `SHARED_TOOLING_RIPGREP_VERSION`
and the four `SHARED_TOOLING_RIPGREP_SHA256_*` archive digests to their reviewed
versions file, and pass the flag to both installation and offline verification.
Offline checks authenticate the retained archive and compare its executable
bytes with installed `rg` before running it. They require the exact version
(allowing the official revision annotation) and a successful `--pcre2-version`.
No Cargo installation, global PATH change or system-package replacement occurs.

The same include selects `--with-cloc` for the standalone cloc payload in both
installation and offline checks.
Consumers adopting it add `SHARED_TOOLING_CLOC_VERSION` and the single
`SHARED_TOOLING_CLOC_SHA256` pin to their reviewed versions file. The installer's
narrower selections remain available to individual helper callers; they do not
satisfy the complete repository setup contract. All selected payloads are authenticated
before executing any version check; failed candidates retain the previous active
set. cloc uses the bootstrap Perl interpreter and needs no Cargo build or system
package installation. LOC reports select the prepared local host path themselves
and never install tools during counting.

The [IC set](ic-tools.md) supplies Quill, ICP CLI, didc, ic-wasm, PocketIC and
wasm-opt. dfx is excluded. Linux ARM64 supports the host set only; the full IC
set lacks a matching Quill asset. Native CI qualifies both sets on Linux x86-64
and both macOS architectures; mapping Linux ARM64 is not native qualification.

## Rust development tools

After preparing the consumer's declared Rust/Cargo toolchain and compilation
prerequisites, use `make install-rust-tools` and offline `make rust-tools-check`.
The shared helper installs cargo-sort, cargo-sort-derives and candid-extractor
under `.tools/rust/bin`, using their exact versions from
[ci/tool-versions.env](../ci/tool-versions.env) and Cargo's `--locked` installation.
`RUST_TOOL_VERSIONS` selects a reviewed alternative catalog; it defaults to
`HOST_TOOL_VERSIONS`. Keep qualified version exceptions in that selected catalog,
and remove duplicate consumer constants when adopting the shared pins.

Rust consumers that need this set attach it to the common commands in their
local Makefile, alongside the reviewed `make/tools.mk` include:

```make
install-tools: install-rust-tools
tools-check: rust-tools-check
```

The common aggregate does not require a Rust toolchain in non-Rust repositories.
Shared Make commands include `.tools/rust/bin` on PATH; interactive shells use
the export at the top of this guide. The helper never prepares or upgrades a
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

## Cargo installation assessment

Before extending the fixed Rust tool set to consumer-selected binaries/examples,
run `scripts/ci/qualify-cargo-install.sh` in Shared Tooling to exercise Cargo's
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
profile; no product MSRV or Canic CLI selection is qualified by this fixture.

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

Rust repositories separately prepare their declared Rust toolchain, rustfmt and
pinned cargo-sort. Shared Tooling's portable fixtures also require Cargo and
the cargo-sort version in `ci/tool-versions.env`; prepare it explicitly:

```bash
source ci/tool-versions.env
cargo install cargo-sort --version "$SHARED_TOOLING_CARGO_SORT_VERSION" --locked
bash scripts/ci/check-format-tools.sh "$SHARED_TOOLING_CARGO_SORT_VERSION"
```

Before starting portable fixtures, `test-portable-tools.sh` checks the required
commands on PATH (including cloc, shasum, jq, Mike Farah yq and ripgrep) and reuses
the offline formatter check above. Run this admission independently with
`bash scripts/ci/check-portable-prerequisites.sh`. It reports missing commands
together and points back here; it never installs tools or replaces the individual
fixtures' version, feature and behavior checks. Standard Unix utilities from the
bootstrap environment are still required. This is Shared Tooling's own suite
setup, not a new prerequisite gate for every consumer.

ShellCheck, actionlint and Gitleaks use the existing explicit CI installers and
pins; GitHub maintenance needs an installed, authenticated `gh`. Consumer-owned
Node/Rust toolchains, SDKs and product dependencies remain part of that repo's
declared setup. See the [complete script dependency table](supported-hosts.md#tool-specific-dependencies).
