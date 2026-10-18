# Local developer setup
<!-- Shared companions: scripts/ci/report-tool-versions.sh -->

From the checkout root, prepare the common executables explicitly:

```bash
make install-tools
make tools-check
export PATH="$PWD/.tools/host/bin:$PWD/.tools/rust/common/bin:$PATH"
```

Prepare the declared Rust/Cargo toolchain and native build prerequisites first,
even when the repository has no Rust packages. The Makefile uses local tool
paths automatically. Interactive shells need the
export above; setup does not edit shell profiles. Installation downloads tools;
tool checks are offline and never install missing executables. Preparing missing
workspace dependency sources during authorized development/validation follows
the [locked Cargo fetch policy](../rules/cargo-dependencies.md#cargo-network-policy).
Before tool downloads,
`install-tools` uses the Rust installer to admit its selected
platform/pins and probe the consumer's selected `rustc` and `cargo`. This
read-only `--preflight` call creates no tool/build directories and disables
Rustup auto-installation; missing or unavailable toolchains require explicit
bootstrap. This is not a compiler/linker qualification or a toolchain upgrade.
Setup then runs host and Rust installation in that order, followed by declared product tools;
`tools-check` checks the same sets in order. Each stops at the first failure.
Sets activate independently: a later failure preserves earlier completed sets.

## Required tool inventory

This is the common setup list for every repository adopting the
[engineering baseline](../DRAGGINZGAME.md). The host and Cargo-tool sets are required.
The IC bundle is selected only by repositories whose workflows use it. Consumers
add their product prerequisites to local setup documentation; omissions or
different support scopes require an explicitly approved exception.

| Set | Executables | Setup and verification |
| --- | --- | --- |
| System bootstrap | Bash, Git, GNU Make, curl, CA certificates, tar, gzip, xz, Perl, a SHA-256 implementation and standard Unix utilities | Host package manager; see [bootstrap prerequisites](#bootstrap-prerequisites) |
| Common host tools | `jq`, Mike Farah `yq`, `rg` with PCRE2, `cloc` | `make install-host-tools`; offline `make host-tools-check`; `.tools/host/bin` |
| Optional product IC tools | `quill`, `icp`, `didc`, `ic-wasm`, `wasm-opt` | `make install-ic-tools`; offline `make ic-tools-check`; `.tools/ic/bin` |
| Common Cargo tools | `cargo-sort`, `cargo-sort-derives` | `make install-rust-tools`; offline `make rust-tools-check`; `.tools/rust/common/bin`. All repositories prepare a declared Rust/Cargo toolchain and native build prerequisites first. |
| Rust development | rustfmt and product compilation targets | Consumer toolchain setup and the [formatter prerequisite check](verification-helpers.md#formatter-prerequisites) |
| Workflow-specific tools | ShellCheck, actionlint, Gitleaks, authenticated `gh`, Node/SDKs and other tools used by that repository | Explicit consumer setup; declare the tools required by each workflow |

Follow the derive sorter's documented
[macro qualification and pin adoption](../rules/git-hooks.md#derive-macros-and-dollar-prefixed-paths)
before removing corruption-workaround markers. The selected 0.14.1 formatter
requires Rust 1.88 or newer to build; consumer library MSRVs stay independently owned.
Tool availability and version checks do not establish semantic preservation.

`make install-tools` and `make tools-check` cover the common host/Cargo executables,
regardless of the repository's implementation language. The five IC executables
are optional product tools; their standalone setup/check targets remain available.
Setup does not install a Rust toolchain or authenticate GitHub. Product and workflow-specific
tools extend the aggregate through existing consumer-owned targets:

```make
# Select only when this repository uses IC executables.
LOCAL_TOOL_INSTALL_TARGETS += install-ic-tools
LOCAL_TOOL_CHECK_TARGETS += ic-tools-check
include make/consumer.mk
export PATH := $(CURDIR)/.tools/ic/bin:$(PATH)
```

IC consumers also publish `.tools/ic/bin` to their CI PATH after successful
setup/check, and include it in their interactive-shell PATH. The common action
publishes only host/Cargo paths; it never activates an unselected IC bundle.

These lists run sequentially after the common sets, including under parallel
Make. Declare each required product setup/check pair; do not add prerequisites
to `install-tools`/`tools-check`, override their recipes, or include an aggregate
in its own extension list. Extensions must not call their enclosing aggregate.
Select independently qualified CLI versions in their existing owner adapters.
Rustfmt, compiler targets, credentials and other product prerequisites retain
their documented setup; executable installation grants no deployment authority.
Common versions and digests come from the reviewed snapshot catalogs below.
Do not maintain a second consumer default list. Explicit qualified exceptions
remain available as described under [catalog selection](#catalog-selection).
dfx is excluded from the common IC set. See the host limits below before setup.
Keep identity stores separate from resettable state as described in
[IC identity storage](ic-tools.md#identity-storage-and-local-resets); installing
executables does not authorize moving keys or wiping a CLI home.

Make and CI must select their own checkout's local tool paths. CI exports them
for subsequent steps explicitly; Make's export does not change the calling
terminal. Follow the [consumer adoption example](consuming-snapshots.md#local-ic-tool-adoption)
for the complete installation/check selections, including ripgrep and cloc.
Those commands are owned by `make/tools.mk`, selected through the
[consumer Make entrypoint](make-interface.md). Consumers include that reviewed
snapshot instead of maintaining copied recipes or installer flags. Shared
Tooling's own Makefile and CI use the same commands. Snapshot adoption brings
command updates; explicit setup brings newly required executables.

The include also selects `make/execution.mk` and its
`scripts/ci/check-make-execution.sh` companion. It rejects dry-run, touch,
question and ignore-errors modes before recipes execute. Cargo recipes retain
Make's jobserver descriptors for parallel execution; this does not grant
installation authority to an inspection command or change the setup order.

The installed `cloc` executable and local workspace `make cloc` are common setup.
Fleet reports such as `make cloc-tooling` run from Shared Tooling's own Makefile;
the consumer include exports local workspace LOC. Consumers need not vendor fleet
reporters or run their regression suites. See the
[optional fleet selection](consuming-snapshots.md#local-ic-tool-adoption).

## Catalog selection

Since 0.10.0, the executing reviewed snapshot owns the defaults:
`ci/tool-versions.env` for host/Cargo tools and `ci/ic-tools.tsv` for IC tools.
Make resolves both relative to `SHARED_TOOLING_ROOT`; standalone installers and
the Cargo freshness checker resolve them relative to their own snapshot.
`--consumer` selects the checkout's installations, not its default catalog.
Missing snapshot catalogs fail; there is no consumer-local fallback.
Snapshot adoption selects the pins but never installs or upgrades executables.

For a justified exception, keep a separate reviewed consumer file and select the
affected set explicitly before including `make/consumer.mk`:

```make
IC_TOOL_PINS := $(CURDIR)/config/qualified-ic-tools.tsv
```

`HOST_TOOL_VERSIONS` and `RUST_TOOL_VERSIONS` independently select alternative
host and Cargo catalogs. A host override does not change Rust or formatting pins.
Each file must contain the complete selection required by its set; no implicit
overlay is merged. Record the reason, affected hosts and native qualification in
consumer governance. Keep snapshot payloads unchanged, including when excepted.
Standalone calls use explicit `--versions` or `--pins`; freshness inspection uses
`--versions`. CI cache-key inputs and direct catalog reads must select the same
catalog as installation. The failure-evidence action defaults to the catalogs
beside its own snapshot; set `host-versions`/`ic-pins` explicitly when using an
exception so successful-tool compaction checks the intended selection.

Common catalogs do not own consumer compiler/MSRV choices, application
dependencies, Testkit/PocketIC selections or product tool adapters. Workflow-only
tools may retain consumer selections; merely adopting a catalog does not install
tools outside the common inventory or change a workflow's declared support.

## Shared executable storage

Shared Tooling owns storage for the common executable sets. Host and IC setup
reuse verified bundles in a per-user store across checkouts with the same host
and pins. Each repository retains its reviewed snapshot and command
selection; changing one repository's selection must not change another's.
Providing every common command does not require duplicating its binary in every
checkout. Consumers must adopt the canonical implementation through reviewed
snapshots rather than create independent stores or new bespoke `.tools`
installers. Reviewed script snapshots remain repository-local.

The default store is `${XDG_DATA_HOME:-$HOME/.local/share}/shared-tooling/tools`.
Set `SHARED_TOOLING_STORE` to an absolute directory to choose another location;
use the same setting for setup, checks and evidence collection. Fixtures use
isolated temporary stores. Store directories must be owned by the current user,
physical, and not writable by other users. Existing ancestor aliases such as
macOS `/var` are resolved; managed directories and payloads cannot redirect
outside their admitted locations. CR/LF paths are refused.

The existing `.tools/host/bin` and `.tools/ic/bin` command paths remain, backed
by lightweight local references to exact shared bundle generations. No new host
or IC payload is stored inside the checkout. A complete matching bundle is
verified again before reuse without downloads. Changed pins or platforms select
a different store entry; updating its reusable selection never retargets other
checkouts. Locks protect setup across repositories as well as local activation.
Offline checks never install or repair tools. Corrupt bundles are refused;
explicit setup can prepare a fresh generation without overwriting retained ones.

Adoption requires refreshed installers, `scripts/dev/tool-store.sh`, and the
evidence selector, followed by `make install-host-tools`, `make install-ic-tools`
and their offline checks. Older physical checkout-local bundles are preserved
but no longer admitted as active installations. Do not manually relocate them
or delete them during adoption. Failed attempts and earlier selections remain
referenced from the originating checkout for evidence collection. The collector
archives those admitted shared bundles, not the entire machine's store. Deleting
a checkout or a reference does not reclaim its stored payload; cleanup remains
an explicitly authorized separate operation.

Cargo tools and their build output also live in the shared store. Local
`.tools/rust` contains selections and references; previous physical installations
are retained. Reuse requires the exact compiler, native environment, published
source/lock, target and profile under the standard environment described below.
The store's `cargo-cache` holds Cargo-managed registry sources separately from
retained installation generations; preserve it for offline admission and reuse.

## Diagnosing a missing command

Installing tools in Shared Tooling does not select them in another checkout.
Run setup in each checkout; matching host/IC bundles reuse the shared store.
From the affected repository's root, compare:

```bash
.tools/host/bin/cloc --version
command -v cloc
```

If the first command works but the second is missing or points elsewhere, select
this checkout's tools in the current terminal:

```bash
export PATH="$PWD/.tools/host/bin:$PWD/.tools/rust/common/bin:$PATH"
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
remain in the shared store, referenced by `.tools/host-set.*`. Locks prevent
simultaneous installation of the same shared selection and local activation;
inspect `.tools/.host-tools.lock/owner` and confirm its process has stopped before
removing an abandoned lock; shared slots also retain `install.lock/owner`.
Offline checks report the shared lock path and refuse while that selection's
shared lock is held, including when another checkout owns the installation.
Host and IC setup attempt both lock releases on exit, even if checkout cleanup
fails. An installation failure keeps its original status and retained candidate;
a cleanup-only failure also returns nonzero. Inspect any remaining lock before
retrying, even when the selected bundle was successfully activated.
Explicit host, IC and CI asset downloads use curl's transient retry classification,
with at most three retries, a two-second delay and a 60-second retry-start window.
Each attempt keeps its 15-second connection and 300-second transfer limits;
an attempt already in flight can finish after the retry window. Permanent HTTP
errors and checksum/admission failures are not retried. Exhaustion returns curl's
final failure status and preserves the failed candidate and previous selection.
Cleanup remains explicit. The `--versions` option
selects reviewed literal assignment data. Host/Rust installers, freshness checks
and cache identities use the same parser. Shell commands, substitutions, duplicate
keys and nonliteral values refuse before installation or cache output. Replace
computed catalogs with complete literal selections before adopting this cut.

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
The shared helper installs cargo-sort and cargo-sort-derives
into shared build-identity slots and exposes the complete set at `.tools/rust/common/bin`, using their exact versions from
[ci/tool-versions.env](../ci/tool-versions.env) and Cargo's `--locked` installation.
Preflight selects the consumer's prepared compiler, including explicit Rustup
toolchain overrides. Installation then uses that compiler's real Cargo/rustc
binaries in a standard environment, separate from the consumer's build settings.
A relative `--versions`
catalog is read from the caller's directory before entering the consumer;
relative `--lockfile` paths retain their consumer-relative meaning.
`RUST_TOOL_VERSIONS` selects a reviewed alternative catalog; it defaults to
the snapshot's `ci/tool-versions.env`, independently of `HOST_TOOL_VERSIONS`.
Keep qualified version exceptions in that selected catalog,
and remove duplicate consumer constants when adopting the shared pins.

Candid extraction is a product-selected tool. Consumers that extract declarations
own its pin, explicit installation/check target and PATH selection through the
[selected Cargo tool interface](#consumer-selected-cargo-tools). Common setup does
not install it. Qualify that selection before adopting the narrower common set;
retained installation slots are not deleted.

The complete common aggregate includes this set in every repository, including
non-Rust repositories. The narrow commands remain useful for explicit retries
or diagnostics; they do not establish that the complete toolset is ready.
Shared Make commands include `.tools/rust/common/bin` on PATH; interactive shells use
the export at the top of this guide. The standard formatting hook and its
adoption check preserve the original checkout's common host and Rust tool paths
while formatting isolated index inputs. IC formatters select their prepared tool
PATH explicitly; the hook exposes `SHARED_TOOLING_FORMAT_TOOLS_FROM` for product
Make recipes that need the original checkout's tool root. The helper never prepares or upgrades a
toolchain implicitly. Installation may fetch registry dependencies and compile;
failed candidates, completed build output and install logs remain in shared
`rust-<identity>/set.*` generations, referenced from the originating checkout.
If build identity changes during installation, activation refuses and reports the
changed field names. The retained attempt contains the original `.build` identity
in `inputs.json` and `native-environment.txt`, alongside the exact new observation
in `build-identity.observed.json` and `native-environment.observed.txt`. These files
are included by the normal evidence collector; no automatic rebuild is attempted.
Both common tools and consumer-selected tools use the same locked installer,
Cargo receipt and observed byte-digest admission described below. Common tools
add a successful exact-version probe after byte admission. Shared host tools
(including jq) and the selected Rust toolchain must be prepared first.

The common setup holds `.tools/rust/common.lock`, prepares all three selections, then
atomically selects a retained `common-set.*` directory through the `common` link.
The catalog reader owns the common roster and stable-version admission; setup
projects those admitted records directly into executable selections.
Its `bin` contains only the three selected slot links. A partial failure leaves
the previous complete set active; rerunning setup reuses completed slots. Offline
checks refuse missing selections, locks, changed receipts/bytes and redirected
paths without installing or repairing them. The only permitted projection links
are generated common-set/selection links and validated references into the store.
Shared generations and their executables, builds and receipts must be physical. Existing host/IC links and checkout aliases
remain supported. Path admission does not sandbox Cargo or defend against an
unrelated process replacing admitted paths concurrently.

The 0.6 hard cut requires explicit setup of shared Cargo selections; previous
local installations are retained but never used as a fallback. Refresh the
installers and evidence selector together, then run setup and offline checks.
The evidence action retains the local `.tools/rust` tree without following its
links and separately archives only its admitted shared generations, including
successful builds and failed attempts. This Rust retention remains full even
when host/IC compaction is enabled.
Observed digests detect local changes; they are not publisher signatures or
product/native qualification. Formatting still requires rustfmt and the
[formatter check](verification-helpers.md#formatter-prerequisites).

### Standard Cargo tool-build environment

Tool builds have a cleared environment, a fresh private HOME per attempt, the
selected compiler's bin directory followed by system executable directories,
fixed C locale/UTC timezone, private permissions and two compilation jobs.
Repository flags, wrappers, custom targets, user Cargo configuration and arbitrary
environment variables are not inherited. Application builds are unaffected.
An explicit nonempty `CARGO_NET_OFFLINE` (`true`/`false` or `1`/`0`) takes
precedence over user Cargo `net.offline`. Without that override, `net.offline`
must be a TOML boolean; strings and numbers are rejected before installation or
selection changes. Empty configuration or an absent key defaults to online
permission. Cargo's `config` takes precedence over `config.toml` when both exist.
No check downloads or repairs tools. Custom registries, extra native search paths and
proxy/credential configuration are not supplied to these public crates.io builds.

The key records the exact build-helper digest, Cargo/rustc verbose identities and executable digests, host,
native compiler/linker digests, Linux package inventory or macOS/Xcode/SDK
identity, package/version, executable kind/name, profile, published crate digest
and packaged lock digest. Cargo owns registry downloads and `--locked` builds.
Changing the build helper requires explicit setup to select builds under the new
policy digest; previous builds remain retained and offline checks do not repair
their selections.
Missing packaged locks and non-registry dependency sources refuse reuse/builds;
unexpected Cargo configuration in the managed cache is rejected. This contract
supports managed Debian/RPM Linux hosts and the qualified macOS toolchain; it
does not model arbitrary custom native toolchains or out-of-band SDK/library
mutations. Such environments require a separately qualified contract before
sharing tools. It is a same-user build cache, not a hermetic sandbox or a signed
publisher attestation.

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
install-rust-tools` prepares the common formatter tools through this same engine.

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

Consumer extensions such as cargo-audit and cargo-machete use this same owner.
Keep their exact pins in the consumer and register their existing setup/check
targets in `LOCAL_TOOL_INSTALL_TARGETS` / `LOCAL_TOOL_CHECK_TARGETS`. For example,
with `AUDIT_VERSION` defined by the consumer's reviewed pins:

```make
AUDIT_TOOL = bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-rust-tools.sh" --consumer "$(CURDIR)" --package cargo-audit --version "$(AUDIT_VERSION)" --bin cargo-audit --profile release
LOCAL_TOOL_INSTALL_TARGETS += install-audit-tool
LOCAL_TOOL_CHECK_TARGETS += audit-tool-check

.PHONY: install-audit-tool audit-tool-check audit
install-audit-tool:
	+@$(AUDIT_TOOL) > /dev/null
audit-tool-check:
	+@$(AUDIT_TOOL) --check > /dev/null
audit:
	+@tool="$$( $(AUDIT_TOOL) --check )" && "$$tool" audit
```

Use the equivalent package/binary selection for cargo-machete, but its standalone
invocation differs from Audit. With `MACHETE_VERSION` selected by the consumer:

```make
MACHETE_TOOL = bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-rust-tools.sh" --consumer "$(CURDIR)" --package cargo-machete --version "$(MACHETE_VERSION)" --bin cargo-machete --profile release
LOCAL_TOOL_INSTALL_TARGETS += install-machete-tool
LOCAL_TOOL_CHECK_TARGETS += machete-tool-check

.PHONY: install-machete-tool machete-tool-check machete
install-machete-tool:
	+@$(MACHETE_TOOL) > /dev/null
machete-tool-check:
	+@$(MACHETE_TOOL) --check > /dev/null
machete:
	+@tool="$$( $(MACHETE_TOOL) --check )" && unset CARGO && "$$tool" --with-metadata
```

Audit needs `"$$tool" audit`; standalone Machete 0.9.2 needs
`"$$tool" --with-metadata`, with no extra `machete` argument. That version uses
inherited `CARGO` (when `CARGO_PKG_NAME` is absent) to select Cargo-subcommand
parsing. Unset `CARGO` only in the Machete recipe's shell before the standalone
call; application builds retain their environment. Keep the selected executable
as the final command so its failure status propagates, including on Bash 3.2.

Qualify the consumer's real dependency-check target as well as setup/check reuse.
Use separate Audit and Machete argument fixtures, including inherited `CARGO`,
failed admission with path-shaped stdout, and tool failure status/evidence
retention. An installer pass alone does not establish correct CLI invocation.

Repeated setup
verifies and returns the existing path without Cargo installation or force advice;
checks never build or download. Only the returned path is suppressed above;
failure diagnostics and status remain visible. The `+` prefix preserves Make's
recursive-command handling to the installer; standard tool builds themselves
use the documented two-job limit rather than inheriting the caller's jobserver.
Adopting this route changes global Cargo destinations to shared selections:
update execution callers to use the returned checked path, explicitly prepare
and check the tools, and retain old global installations. Do not add version-probe
fallbacks, force overwrites or another installer around the shared owner.

The command prints the admitted executable path under
`.tools/rust/selected-<build-identity>/bin/`.
Use that returned path in the consumer adapter. Each selection is immutable:
an existing installation must pass physical-path, exact Cargo receipt and local
byte-digest checks. Checks reject changed bytes or receipts without repair or execution;
each receipt must contain exactly one JSON document. Cargo installation failures
return Cargo's original exit status along with the retained attempt location;
no invented `--version` probe runs for examples. Checks invoke rustc for the
selected host and Cargo identity, but never build or download. Digests detect local changes; they
are not publisher signatures. The consumer still owns compiler compatibility.

Missing or invalid selected-installation diagnostics identify the package, exact
version, target kind/name, profile and destination. The caller supplies its own
setup command; successful selection still prints only the executable path.
Dependency updates that change the selected CLI must prepare and check it before
preparation is complete. Authorized releases use the existing setup/check targets
in [preflight](releases.md#selected-executable-tools-before-validation); fetching
Cargo sources alone is insufficient. Complete gates and standalone qualification
check tools offline before builds, without implicit installation.

Setup reuses a matching verified generation or compiles through the same locked
Cargo installation command into a fresh shared attempt. Only an admitted
candidate is activated. Explicit setup can replace a corrupt selection with a
new generation, preserving the old generation and other checkouts' references.
Failed attempts retain logs/builds, and earlier version selections remain untouched.
A directory lock rejects concurrent setup for the same selection; retry after
its owner finishes. An abruptly killed process may leave a lock: inspect that
owner and retained attempt before explicitly removing the empty lock. The tool
never guesses that a lock is stale. Redirected output, receipt and lock paths
refuse. After Cargo returns, setup rechecks the shared directory ancestors and
selection slot before admitting or activating the candidate. As with the common
tools, path admission is not a sandbox against another
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
The common fixture selects the reviewed cargo-sort version, using the debug
profile for direct Cargo assessment and release for the shared production
installer, including offline reuse and changed-byte refusal. Product compiler
floors and application CLI selections remain consumer-owned.

The frozen published Blob example assessment moves to
[IC Blob Storage PR #80 — Own frozen published-example installation qualification](https://github.com/dragginzgame/ic-blob-storage/pull/80).
Integrate that owner before adopting this snapshot. Its Linux and both native
macOS jobs retain the former example-installation proof
([#280 — Retire frozen Blob example assessment after native owner qualification](https://github.com/dragginzgame/shared-tooling/issues/280)).

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
shared Rust installer remains the supported setup contract.

## Bootstrap prerequisites

Use the host package manager for the shell, Git, GNU Make, curl, archive tools,
Perl and ordinary utilities. For Linux Mint/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y bash git make curl ca-certificates tar gzip xz-utils zip unzip perl
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
export PATH="$PWD/.tools/host/bin:$PWD/.tools/rust/common/bin:$PATH"
```

Before starting portable fixtures, `test-portable-tools.sh` checks the required
commands on PATH (including cloc, shasum, jq, Mike Farah yq and ripgrep) and reuses
the offline formatter prerequisite check. Run this admission independently with
`bash scripts/ci/check-portable-prerequisites.sh`. It reports missing commands
together and points back here; it never installs tools or replaces the individual
fixtures' version, feature and behavior checks. Standard Unix utilities from the
bootstrap environment are still required. This is Shared Tooling's own suite
setup, not a new prerequisite gate for every consumer.
The download retry fixture starts a temporary IPv4 loopback HTTP listener and
uses real curl with test-only transport substitution. It needs permission to bind
`127.0.0.1`, but contacts no public server and adds no TLS/certificate prerequisite.

ShellCheck, actionlint and Gitleaks use the existing explicit CI installers and
pins; GitHub maintenance needs an installed, authenticated `gh`. Consumer-owned
Node/Rust toolchains, SDKs and product dependencies remain part of that repo's
declared setup. See the [complete script dependency table](supported-hosts.md#tool-specific-dependencies).

## Observed common tool versions

After explicit preparation, run `bash scripts/ci/report-tool-versions.sh "$PWD"`
from an adopted snapshot. The reporter checks host and Rust sets through their
installers' read-only `--check` interfaces, then records
catalog-selected command paths, SHA-256 identities and live version output.
An optional `VERSIONS` argument selects the host/Rust catalog. Supplying a
third `IC_PINS` argument explicitly includes IC admission and observations. It never
installs tools; use the same `SHARED_TOOLING_STORE` setting as setup and checks.
Redirect stdout and stderr together when retaining evidence. Failure keeps prior
observations and the failing command's status. Compiler and product-specific CLI
selection/evidence remain consumer-owned.

### Optional IC bundle adoption

This is a breaking setup contract. Consumers needing the IC bundle must register
both ordered targets shown above and publish its PATH explicitly before adopting
the new snapshot. IC-free libraries keep the default common setup. Existing tool
bundles are retained; adoption never deletes installations or changes product pins.
Version reporting defaults to host/Cargo selections. To include an explicitly
selected IC bundle, pass both catalogs:

```bash
bash scripts/ci/report-tool-versions.sh "$PWD" ci/tool-versions.env ci/ic-tools.tsv
```

The IC install/check commands continue to verify all five executable selections
and their reviewed native-host digests. Product ownership does not weaken admission.
