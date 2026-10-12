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
Selecting `make/consumer.mk` as a snapshot entrypoint includes the common
installers and default LOC helper through their declared dependencies. Pin
catalogs remain consumer-owned; optional fleet reports require explicit selection.

| Commands | Shared owner | Consumer inputs |
| --- | --- | --- |
| `install-tools`, `tools-check`, narrow host/IC/Rust commands | Ordered setup and offline admission | Reviewed pins and ordered product tool extensions |
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

## Keep product policy local

Keep build, test, publication, deployment, cleanup, validation gates and release
metadata adapters in the consumer. CI and documentation call the same public Make
targets. Adapters may use documented shared helper APIs; a single entrypoint is
not a ban on the helpers that implement those adapters.

Use the existing `LOCAL_TOOL_INSTALL_TARGETS` and `LOCAL_TOOL_CHECK_TARGETS`
ordered extension lists for product tools, including IC Testkit's PocketIC CLI.
See [setup](local-setup.md) for pins and executable storage. Do not add unordered
prerequisites to the aggregate installation target or duplicate its recipes.

Add prerequisites and target-specific environment to common targets when the
consumer needs stronger admission; do not define a second recipe. For example:

```make
release-patch release-minor release-major release-resume: release-delivery-check
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

## Custom formatting without replacement recipes

By default Rust formatting sorts the root Cargo workspace and runs rustfmt.
Repositories with independent workspaces, derive policies or frontend formatting
set **both** complete commands before the include:

```make
FORMAT_WRITE_COMMAND = bash scripts/dev/format-workspaces.sh --write
FORMAT_CHECK_COMMAND = bash scripts/dev/format-workspaces.sh --check
include make/consumer.mk

fmt fmt-check format-tools-check: export RUSTUP_TOOLCHAIN := $(VALIDATION_TOOLCHAIN)
fmt fmt-check format-tools-check: export CARGO_HOME := $(LOCAL_CARGO_HOME)
```

Keep the existing formatter adapter and its complete sequence. Do not wrap it in
`run-formatting.sh` again: the shared targets own that wrapper. A fixed
`bash -ec '...'` command is also supported; these values are reviewed Makefile
commands, not untrusted strings. Quote paths and use Make's `$$` escaping for
shell variables. Supplying only one command or an empty command is rejected.
`FORMAT_CARGO` still selects the executable used for Rust prerequisite checks;
target-specific environment applies to those checks and the formatter.

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
5. Run focused consumer formatting/hook and release routing checks, then its full
   delivery gate. Native macOS evidence remains necessary. Delete local installers
   or aliases only after tracing their callers and verifying equivalent behavior;
   do not remove product setup merely because it invokes a shared helper.

No old public command is removed by this addition. Any later retirement must
identify callers and actual replacement coverage before selecting a breaking
Shared Tooling release. Consumer library adoption is normally contributor tooling
work, not a reason to bump the library's minor version.
