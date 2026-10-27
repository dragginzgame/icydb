# Consumer Make interface

Use one reviewed snapshot entrypoint for common contributor commands:

```make
.DEFAULT_GOAL := help
override SHARED_TOOLING_ROOT := $(CURDIR)
include make/consumer.mk
```

For a nested snapshot, select its local directory and include its
`make/consumer.mk`. Never include a moving sibling checkout. A non-Rust repository
sets `SHARED_TOOLING_RUST := 0` before the include; it still prepares the complete
common toolset, but does not expose Rust formatting or hook installation.
The include preserves the consumer's default goal, even when declared later.
Parsing it never installs tools, activates hooks or starts a release.

To restrict release delivery, set `RELEASE_DELIVERY_POLICY` before the include
to a consumer-owned regular data file containing `direct`, `pr`, or both, one
per line. The common include refuses unsupported selections when a standard
release command or metadata hook is requested as a Make goal. Unset selection defaults to direct; an explicit
empty selection refuses. Unrelated goals do not read the policy. Without a policy
file, the existing direct/PR contract is unchanged. `SHARED_RELEASE_TARGETS`
provides the canonical command/hook roster for consumer declarations.

Standalone metadata entrypoints call
`bash scripts/ci/check-release-delivery.sh "$policy_file" "${RELEASE_DELIVERY-direct}"`
before effects, using that same file; never source it as shell code. An empty
file-path argument admits the standard direct/PR set. File selection and delivery
choice remain consumer-owned; this helper grants no release or merge authority.
Make admission resolves the helper beside the selected include, independently of
runtime `SHARED_TOOLING_ROOT` overrides. Keep its snapshot companion selected.

Selecting `make/consumer.mk` as a snapshot entrypoint includes the common
installers, common pin catalogs and default LOC helper through their declared
dependencies. Product pin exceptions remain consumer-owned; optional fleet
reports require explicit selection.

| Commands | Shared owner | Consumer inputs |
| --- | --- | --- |
| `install-tools`, `tools-check`, narrow host/IC/Rust commands | Common pins, ordered setup and offline admission | Explicit pin exceptions and ordered product tool extensions |
| `tools-report` | Aggregate existing offline checks and retain all failures | Same ordered local check targets; no setup or release admission |
| `fmt`, `fmt-check`, `format-tools-check` | Prerequisite check, compact output, retained failure logs | Cargo environment and complete custom formatter commands when needed |
| `install-hooks` | Safe repository-local activation | Tracked root `.githooks/pre-commit` and its companions |
| `shared-tooling-check` | Offline snapshot integrity | `SHARED_TOOLING_MANIFEST`, default `.shared-tooling.snapshot` |
| `cloc` | Workspace report | Optional report/manifest selection |
| `release-patch`, `release-minor`, `release-major`, `release-resume` | Admission and exact-version recovery runner | Metadata, validation, delivery policy, branch and remote |

The included `tools.mk`, `rust-format.mk`, `release.mk` and `execution.mk` are
the implementation owners. Existing focused includes remain supported; full
consumer adoption uses `consumer.mk` instead of including the same modules twice.
Shared Tooling itself selects the non-Rust profile. Its source checkout has no
consumer snapshot to verify; `shared-tooling-check` is for adopted copies.

Since 0.16.0, `cloc-tooling` and its `CLOC_PARENT` input belong to Shared Tooling's
own Makefile. They are not exported by the consumer include. Use the central
command or the explicitly selected reporter described in the
[fleet report adoption guidance](consuming-snapshots.md#local-ic-tool-adoption).

## Keep product policy local

Keep build, test, publication, deployment, cleanup, validation gates and release
metadata adapters in the consumer. CI and documentation call the same public Make
targets. Adapters may use documented shared helper APIs; a single entrypoint is
not a ban on the helpers that implement those adapters.

Use the existing `LOCAL_TOOL_INSTALL_TARGETS` and `LOCAL_TOOL_CHECK_TARGETS`
ordered extension lists for product tools, including IC Testkit's PocketIC CLI.
Their install phase follows the common Cargo tools; native/system bootstrap
that changes Cargo fingerprints must happen earlier in the consumer adapter.
See [setup](local-setup.md) for pins and executable storage. Do not add unordered
prerequisites to the aggregate installation target or duplicate its recipes.

Add prerequisites and target-specific environment to common targets when the
consumer needs stronger admission; do not define a second recipe. For example:

```make
release-patch release-minor release-major release-resume: local-release-preflight
release-patch release-minor release-major release-resume: export LOCAL_RELEASE_PREPARE := 1
format-tools-check: local-formatter-check
```

Those local names are examples, not new shared targets. Retain the actual
consumer's checks. The [release contract](releases.md) owns adapter requirements
and recovery. Consumers such as IC Backup also verify a product-owned receipt
after explicit resume. Set `RELEASE_RESUME_CHECK_COMMAND` to that existing check,
for example `bash scripts/release/release.sh resume-check "$(VERSION)"`.
It runs only after the runner succeeds, with the requested exact version;
failure stops Make. It must only verify existing evidence, never repeat delivery,
publish or clean. The default is a no-op. Keep receipt identity and schema at
their consumer owner; ordinary release validation remains in the existing
runner adapters. The include does not change user-owned release authority or add
publication, deployment or cleanup. Preserve unfinished plans, prior tool
installations, logs and build artifacts during adoption.

### Native bootstrap adapter

Keep native/system installation in the consumer's existing explicit setup or
release-preparation adapter, before calling the shared installer. For example,
this **consumer-owned** target has a sequential recipe, including under parallel
Make; the local target names are examples, not additions to Shared's interface:

```make
.PHONY: prepare-release-prerequisites
prepare-release-prerequisites:
	+$(MAKE) install-native-tools
	+$(MAKE) native-tools-check
	+$(MAKE) install-tools
	+$(MAKE) install-product-dependencies
	+$(MAKE) native-tools-check
	+$(MAKE) tools-check
	+$(MAKE) product-prerequisites-check
```

`install-tools` includes the consumer's selected release editor when declared in
its ordered extension list. The product-dependency phase must not change the
compiler/native inputs used to prepare those tools; move such work to bootstrap.
The final admission covers native prerequisites, declared shared/local tools and
the remaining product prerequisites before validation starts. Do not turn this
into sibling prerequisites that race under `make -j`.

Call that sequence only where setup is already authorized. Standalone
`tools-check`, `native-tools-check` and `product-prerequisites-check` remain
read-only. Validation consumes prepared prerequisites without implicitly
installing packages. Keep the actual command roster
with the consumer; see [bootstrap ordering and re-preparation](local-setup.md#native-bootstrap-before-cargo-tools).

## Custom formatting without replacement recipes

By default Rust formatting sorts the root Cargo workspace with `--grouped` and
runs rustfmt through `scripts/ci/run-rust-formatting.pl`. Each pass shows progress
and elapsed time while its verbose output stays in a private log; failures retain
the log and exact exit status. Blank-separated dependency sections are preserved.
Use reviewed literal `FORMAT_RUST_ARGS` for extra workspaces, optional derive
sorting, root/extra manifest order and standalone Rust files, instead of copying
the orchestration:

```make
FORMAT_RUST_ARGS = --sort-derives \
    --extra-workspace integrations/consumer \
    --extra-workspace 'integrations/embedded consumer' \
    --extra-manifest-order workspace,package,lib,dependencies,dev-dependencies,build-dependencies,profile
include make/consumer.mk
```

The root sequence is grouped manifest sorting, optional derive sorting, then
Rustfmt. Each extra workspace gets grouped manifest sorting and Rustfmt, in the
declared order. Derive sorting runs once at the checkout root, preserving the
sorter's ignore-aware traversal. `--manifest-order` selects root ordering;
`--extra-manifest-order` selects ordering for extra roots. Extra roots must be
distinct directories inside this checkout. Reuse a consumer's existing roster
when constructing these arguments; do not create another workspace registry.

For sources outside Cargo's formatter inventory, repeat `--rustfmt-source PATH`
and select `--rustfmt-edition 2015|2018|2021|2024` explicitly. These files are
formatted in a separate timed pass after all workspaces. Missing, duplicate or
outside-checkout files and unsupported editions reject before any write. Direct
Rustfmt is admitted before workspace mutations; `--rustfmt PATH` selects one
executable, and target-specific `RUSTUP_TOOLCHAIN` applies to it too. For example:

```make
FORMAT_RUST_ARGS = --extra-workspace testing/runtime-qualification \
    --rustfmt-source testing/package-consumer.rs --rustfmt-edition 2024
```

Use the consumer's existing file list; if it contains globs, expand them with
Make's `$(wildcard ...)` and emit one option per file with `$(foreach ...)`.
Selections containing standalone files run uncached, including their workspace
passes, because arbitrary standalone modules/configuration are not inventoried
by the cache contract. This preserves full coverage and visible progress rather
than certifying an incomplete input set.

Before running passes, the driver can reuse a successful result for identical
workspace/source/manifests/lock/configuration, formatter identities and policy.
It observes Cargo metadata and hashes source bytes; Git cleanliness or mtimes
alone do not establish an unchanged input set. Input observation and result
recording have their own timings, so shortcut overhead remains visible.
The local receipt is `target/shared-tooling/formatting.receipt`. Missing, corrupt
or unreadable bookkeeping is a miss. Incomplete discovery, custom `RUSTFMT`,
explicit module paths, external target paths or non-workspace path dependencies disable reuse;
normal formatting still runs. Optional derive reuse also needs `rg`; no tool is
installed implicitly. Standard Cargo package sources, root/ancestor Cargo and
Rustfmt configuration, toolchain inputs and derive configuration are covered.
Module-path screening uses the shared Rust lexer and balanced attribute trees,
including nested conditional attributes. Comments, literals and later source
text do not become module overrides. Incomplete token trees or unsupported
conditional metadata still disable reuse. The driver and its declared `RustSource.pm` companion are
both fingerprinted; refresh their complete snapshot and hook-fixture closure.

Only a successful check or write with unchanged observed inputs records a
reusable result. A write that changes files needs a later check or unchanged
write before reuse; it does not run a second check-before-write sequence.
Failed or interrupted runs never publish a receipt. Keep source stable during
formatting, as with other working-tree checks. Isolated staged-hook worktrees
always run their formatters; developer receipts do not qualify staged inputs.
Use `FORMAT_RUST_ARGS += --no-cache` to force all passes for explicit qualification.
Prepared-tool checks still run before a cache hit. This is local result reuse,
not a replacement for release source/evidence admission or native qualification.

For genuinely different policies or additional frontend formatting, the existing
paired command interface remains available. Those opaque commands are uncached
and use the compact `run-formatting.sh` log wrapper:

```make
FORMAT_WRITE_COMMAND = bash scripts/dev/format-workspaces.sh --write
FORMAT_CHECK_COMMAND = bash scripts/dev/format-workspaces.sh --check
include make/consumer.mk

fmt fmt-check format-tools-check: export RUSTUP_TOOLCHAIN := $(VALIDATION_TOOLCHAIN)
fmt fmt-check format-tools-check: export CARGO_HOME := $(LOCAL_CARGO_HOME)
```

Keep all required formatter coverage in a custom adapter. Do not wrap it in
`run-formatting.sh` again: the shared targets own that wrapper. A fixed
`bash -ec '...'` command is also supported; these values are reviewed Makefile
commands, not untrusted strings. Quote paths and use Make's `$$` escaping for
shell variables. Supplying only one command or an empty command is rejected.
`FORMAT_CARGO` selects the executable used for Rust prerequisite checks and the
standard driver;
target-specific environment applies to those checks and the formatter.

A snapshot refresh preserves explicit command overrides; migrating a supported
Rust-only adapter requires removing both overrides and selecting its inputs with
`FORMAT_RUST_ARGS`. Update the actual caller and hook fixtures, including manual
shared-file copies and aggregate-output assertions. Preserve standalone file and
edition inputs when replacing their adapter. Nesting
the driver inside a custom command still captures its progress in the aggregate
log; it does not provide live per-pass output.

The shared recipes preserve jobserver descriptors, prepared-tool/offline policy,
failure status and retained logs. Custom commands own extra prerequisites and
must stop on failure. Keep [hook qualification](../rules/git-hooks.md) for the
actual workspaces and frontend inputs. Native consumer tests are still required;
shared substitute fixtures prove routing, not a product's formatter behavior.

## Qualification without recipe overrides

In disposable fixtures, keep exactly one recipe per target. Appending substitute
recipes to an exported production Makefile causes repeated override warnings on
every recursive Make invocation; suppressing stderr hides genuine diagnostics.

Prefer substituting the effectful scripts in the disposable export while keeping
the real shared includes. The existing `test-tool-commands.sh` fixture does this
for installers: it records arguments, injects failures and verifies ordering,
Make admission and inherited jobserver descriptors. Its fixture gate calls the
actual `install-tools` and `tools-check` recipes, with product targets supplied
through the existing ordered extension lists. `test-make-consumer.sh` separately
checks actual release routing with a substitute runner.

When product gate recipes themselves need replacement, construct one small
fixture-owned Makefile with the actual shared includes and only the required
product targets. Define each recipe once. Keep real product metadata transforms
and assertions for release preparation/reconciliation in that fixture; substitute
external setup/build effects at their script boundary. For example, a release
fixture can define its single `ci` recipe to check prepared markers and inject a
gate failure, while its single `release-prepare-version` recipe calls the real
metadata adapter before injecting an interruption. Do not replace shared ordered
aggregates or the release runner just to avoid warnings in a recovery test.

This is consumer fixture construction, not a new production override API.
Qualify the consumer's real Makefile separately, and retain its actual disposable
Git/bare-remote recovery tests. Shared routing substitutes do not qualify product
metadata, delivery or native hosts. Check successful fixture logs for unexpected
recipe warnings rather than filtering them from every invocation.

## Adoption

1. Refresh the [recorded snapshot](consuming-snapshots.md), selecting
   `make/consumer.mk` as an entrypoint. The exporter derives its companion closure;
   preview reports all additions, updates and automatic retirements. Include other
   locally used commands and governance roots; this entrypoint is not their inventory.
2. Replace the separate shared includes and equivalent local hook/snapshot-check
   recipes. Retain stronger prerequisites, target environment and extra snapshot
   checks. `SHARED_TOOLING_MANIFEST` names one manifest; additional bundles remain
   explicitly checked by consumer-owned targets attached to `shared-tooling-check`.
3. Move custom formatting commands into the two inputs above, deleting only their
   equivalent local `fmt`/`fmt-check` recipes. Preserve every workspace, derive,
   frontend and prepared-tool check. Keep the Rust hook and its root-layout
   companions at their required paths, even if other tooling lives in a subdirectory.
4. Update CI, help, setup documentation and isolated Make/hook/release fixture
   inputs together. Export all includes that will be parsed, including
   `make/consumer.mk`; the release command checker still substitutes only the
   runner and must never reach the real checkout's release adapter.
5. Trace every command in the consumer's actual release gates, including nested
   workspaces, native generators, the metadata editor and browser tooling. Map
   each prerequisite to explicit setup and read-only admission. Prepare native
   inputs before Cargo tools, keeping post-common extensions for later tools.
   A successful `tools-check` alone does not cover the full gate's prerequisites.
6. Run focused consumer formatting/hook and release routing checks. Full delivery
   gates remain release-owned or explicitly requested; applicable native macOS
   qualification remains required. Delete local installers
   or aliases only after tracing their callers and verifying equivalent behavior;
   do not remove product setup merely because it invokes a shared helper.

No old public command is removed by this addition. Any later retirement must
identify callers and actual replacement coverage before selecting a breaking
Shared Tooling release. Consumer library adoption is normally contributor tooling
work, not a reason to bump the library's minor version.

## Hosted preparation and disposable fixtures

Shared Tooling's producer fixtures own exhaustive Make flag aliases, inherited
and replaced flags, and common-tool preflight/install/check failure permutations.
Consumers can use `make_probe_target_admission` from
`scripts/ci/make-adapter-probes.sh` to exercise each actual target with the four
unsafe behaviors (`-i`, `-n`, `-t`, `-q`), requiring status 2 and no recorded
effects. Keep local target rosters, effect stubs, parallel descriptor checks,
diagnostic selection and recovery-state custody at the consumer. The full
execution-mode matrix lives in the producer fixture; it is not exported.
The common-tool action fixture executes the actual shared Make aggregates with
only installer leaves substituted; it admits phase order, failure stopping,
stdout/stderr retention and PATH publication. No tools are installed by it.

The optional `.github/actions/prepare-common-tools` action runs `make install-tools`
then `make tools-check`, retaining complete `tools-install.log` and `tools-check.log`
under `RUNNER_TEMP`. It appends common host/Cargo executable directories to
`GITHUB_PATH` only after both commands succeed. Selected IC consumers publish
their admitted `.tools/ic/bin` separately. Consumers own compiler setup,
Make's ordered local extensions, caches and product gates. The action requires
an existing checkout, `RUNNER_TEMP` and `GITHUB_PATH`; it does not select a compiler.

Use `bash scripts/ci/prepare-script-fixture.sh SOURCE EXISTING-FIXTURE ROOT...`
for Make fixtures too. Select `make/consumer.mk` for the consumer interface,
`make/tools.mk` for tool aggregates or `make/release.mk` for release routing.
Their declared companions supply the complete closure. Apply local Makefiles,
catalogs and leaf stubs after preparation; callers retain assertions and evidence.
The former `prepare-make-fixture.sh` command is removed.

For an adapter with native bootstrap, exercise the **actual preparation sequence**
in a disposable consumer export, replacing installation/process leaves at their
existing script boundaries. Let the native installer record a setup marker and
make the Cargo-tool setup substitute refuse without it. Assert the successful
order (also with parallel Make), reverse the bootstrap/Cargo calls and require
refusal before later setup or validation. Check that failed prerequisite admission
stops gate dispatch and that standalone checks reach no installer. Do not test a
handwritten duplicate of the adapter or invoke host package managers in a fixture.

Shared's Cargo-tool fixtures own native-environment fingerprint invalidation and
explicit re-preparation. Retain that rejection/recovery behavior; consumers
qualify their wiring and named missing product proofs instead of copying the
generic matrix. Substituted ordering fixtures do not prove native compilation,
product behavior or macOS support. These requirements belong in the
[consumer qualification selection](consuming-snapshots.md#consumer-qualification-selection).

## Reusing fixture exit qualification

`scripts/ci/fixture-retention-probes.sh` owns the common failure injections,
status comparisons and retained-evidence checks. Consumers keep source rosters,
working directories and domain-specific diagnostic assertions.
`fixture_probe_exits` includes cleanup refusal alongside command, early-exit,
completion and incomplete-run cases; callers need not append `cleanup-failure`.
Existing
`FIXTURE_PROBE_ROOT_VARIABLE` / `FIXTURE_PROBE_DIRECTORY_VARIABLE` selections may
be paired with `FIXTURE_PROBE_COMPLETION_VARIABLE` (default `fixture_complete`)
and `FIXTURE_PROBE_TEMPORARY_ROOT` (default the probe root). Variable names are
admitted as shell identifiers before child execution. Relative and aliased scratch
roots are checked against their physical directory; a caller using RUNNER_TEMP
selects that environment explicitly too. On success `fixture_probe_retained`
contains the checked physical path for any caller-owned diagnostic comparison.
Selected assertion probes preserve a function context for return-based guards.
Repeated exit probes require a fresh receipt from each child; earlier retained
directories remain available when a later child fails during initialization.
No consumer fixture body is run by these prefix-only probes.
