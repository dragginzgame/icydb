# Rust workspace layout

These rules are part of the mandatory [engineering baseline](../DRAGGINZGAME.md).
They standardise maintained Rust package locations while preserving locally
owned APIs, canister identities, validation gates and deployment configuration.

## Standard layout

Every repository containing maintained Rust packages uses a virtual root,
including repositories with only one package. The standard package locations
are `crates/` and `apps/`, according to ownership, with explicit approved layout
exceptions below. Repositories need only the trees they use.

This is a deliberate fleet consistency convention: even a single library keeps
both the root workspace `Cargo.toml` and its package `crates/<package-name>/Cargo.toml`.
Retain the standard shape and the approved exceptions below; routine cleanup
does not collapse the two manifests or require moving an approved layout:

```text
Cargo.toml                 # Virtual workspace; no [package]
Cargo.lock                 # Selected workspace dependency graph
crates/
  package-name/
    Cargo.toml             # [package].name = "package-name"
    src/
    tests/                 # When needed
apps/
  app-name/
    canic.toml             # Application configuration, when used
    component-role/
      Cargo.toml           # Existing package name; directory may name its role
      src/
```

- The repository root is a virtual workspace: its `Cargo.toml` declares
  `[workspace]`, without `[package]`, package targets or package dependency
  tables. Select the resolver explicitly using the supported toolchain; a layout
  change does not authorize a resolver, edition or MSRV upgrade.
- Use `crates/<package-name>/` for reusable libraries, tools, shared schemas and
  test support, using the existing Cargo package name. Standalone binaries,
  canisters, examples and probes may also live here. An existing package under
  `crates/` remains valid; adding support for `apps/` does not require moving it.
- Application-owned Rust packages may live under `apps/<app-name>/` alongside
  their application configuration and assets. A single-package application may
  use `apps/<app-name>/Cargo.toml`; a multi-package application may use
  `apps/<app-name>/<component-role>/Cargo.toml` for its canisters, binaries and
  app-specific schema or fixture packages. Component directories may use role
  names, such as `apps/demo/user_hub/`, without renaming their Cargo packages.
  This is a standard layout, requiring no exception or nested `crates/` wrapper.
- Keep ordinary module tests, examples and fixtures with their owning package;
  they do not need separate packages merely to follow this layout.
- Declare the maintained members in the root workspace. Keep common package
  metadata in `[workspace.package]`, with inherited child values. Package versions
  follow the [release-group rules](#coordinated-versions-and-workspace-boundaries):
  a coordinated workspace uses root inheritance; independent groups all use
  package literals. Keep the complete direct dependency catalog in
  `[workspace.dependencies]` under the [Cargo dependency rules](cargo-dependencies.md).
  Those rules allow documented package-specific MSRVs; a higher internal-tool
  floor must not become the public libraries' minimum through inheritance alone.
  Preserve package identities, selected dependencies and effective features.
  An `apps/` directory does not create an independent Cargo workspace: its
  packages belong to the same root, dependency catalog and declared release groups.
- Keep the selected `Cargo.lock`, workspace profiles and workspace-wide Cargo
  configuration at their owning root. Preserve intentional `default-members`,
  target distinctions and publication selections; a directory move must not
  silently widen normal builds or published packages.
- Application/deployment configuration, frontend code, scripts and documentation
  retain their locally owned locations. An `apps/` directory may contain Rust
  packages, non-Rust application files, or both; its package references follow
  the owning application's discovery contract.
  Moving a package does not rename a deployed canister or authorize deployment.
- Repositories without maintained Rust packages do not need a Cargo workspace
  or empty `crates/` or `apps/` directories. Generated scratch projects, vendored
  upstream sources and frozen historical evidence are not maintained package
  locations; do not move them or rewrite history to satisfy this layout.

## Coordinated versions and workspace boundaries

Keep related crates together for development and qualification. A Cargo workspace
does not require one release version: use a small number of explicit release
groups when independent contracts otherwise force unrelated breaking releases.
Each group has one coordinated version and one changelog. A workspace whose
packages genuinely evolve together remains a single group, with the existing
workspace-version inheritance and release commands.

Prefer release groups within the existing workspace over moving public crates
to new workspaces or repositories solely to give them independent versions.
This retains one dependency catalog, lockfile and development workflow. It does
not isolate Cargo resolution; the
[core and integration graph boundary](#core-and-integration-graph-boundary)
still governs demonstrated integration blockers.

- Group packages by actual shared contracts, dependency direction, consumer
  upgrades and release obligations. Keep tightly coupled generators/runtime
  helpers coordinated; do not create one group per crate by default. Shared
  ownership, directory placement or qualification alone does not require matching
  versions. Similar names or occasional unchanged releases do not justify a split.
- For independent public versions or workspace migrations beyond the scoped
  integration isolation below, show a recurring maintenance problem, why a local
  fix or release batching is insufficient, and the total migration and
  qualification cost for consumers.
  Preserve approved independent graphs and record maintainer agreement for the
  selected boundary or versioning-policy change. Carry an existing approval
  forward; do not request it again for implementing that agreed boundary.
  Approved release groups may keep one workspace and lockfile using the existing
  [explicit release-group contract](../docs/releases.md#explicit-cargo-release-groups).
  Its admitted member versions are the scoped exception to workspace-version
  inheritance; root dependency inheritance and full workspace qualification remain.
- In a workspace with independent release groups, every member, including private
  tooling and fixtures, belongs to one explicit group. Remove
  `[workspace.package].version`; every member declares a literal package version
  equal to its group's primary package. The primary is only that group's version
  authority, not a master package for the workspace. Other common metadata may
  still inherit from `[workspace.package]`.
- Cargo has no per-group version-inheritance mechanism. Do not add an intermediate
  group `Cargo.toml`, nested workspace, shadow version file or custom inheritance
  syntax to represent a release group. Keep existing crate paths, one root
  dependency catalog and lockfile, and the existing release runner. Group release
  material lives under `releases/<group>/`; package relocation is not part of
  adopting this convention.
- Treat every group symmetrically: each owns
  `releases/<group>/CHANGELOG.md`, and root `CHANGELOG.md` is an index. Release,
  resume and publication commands require an explicit `RELEASE_GROUP`; never
  silently default to the former root-version group or the whole public roster.
  Follow the [selection contract](../docs/releases.md#explicit-cargo-release-groups)
  and [history migration rules](changelogs.md#release-group-changelogs-and-shared-history).
- Leave unaffected groups' versions and changelogs unchanged. Cross-group
  requirements change only when needed for the selected dependency contract;
  they need not numerically match the consumer's own version. Exposed Rust types,
  runtime identities and wire/storage contracts still require real compatibility
  assessment. A changed dependency declaration cannot be republished under an
  already published package version.
- Start each group from its existing package/release history. Do not reset
  versions, rewrite tags or re-upload published identities when adopting groups.
  Keep one [canonical changelog per group](changelogs.md#release-group-changelogs-and-shared-history)
  and preserve the earlier shared history. Boundary approval alone does not
  authorize version bumps, release execution or publication.
- Prefer batching related, already-planned breaking changes into one coordinated
  release before downstream adoption. Avoid repeated consumer migrations for
  adjacent changes, but do not hold urgent fixes for unrelated work or enlarge a
  release with speculative changes. Classify the combined release and downstream
  adoption using the [package-impact rules](changelogs.md#classify-the-released-packages-own-impact).
- Keep unrelated repositories independently versioned. Shared Tooling snapshot
  adoption, a common dependency or a common owner does not require fleet-wide
  version synchronization. Verify adoption through the consumer's
  [selected dependency graphs](../docs/cargo-graph-checks.md#coordinated-release-adoption).

### Visible release-group map in Cargo.toml

For a workspace with independent release groups, include every workspace member
in root `[workspace.dependencies]` with its local path and full current version.
This is a complete crate catalog, including private qualification/tooling members
and crates that no other member currently depends on.
List release groups once in this manifest, as contiguous blocks in
`[workspace.dependencies]`. Give each block a short title identifying the policy
group and a separate `# Changelog : releases/<group>/CHANGELOG.md` line.
Separate blocks with a blank line. Sort entries within each block using the
formatter's grouped mode; do not interleave groups to impose a global alphabetic
order. The declarations already show package names, paths and current versions;
do not repeat them in a header above `[workspace]` or add a second comment map.
Include private members and annotate their role only where it adds useful context.
For example:

```toml
[workspace]
members = [
    "crates/ic-logging-stable",
    "crates/ic-memory",
    "crates/ic-metrics-stable",
    "crates/ic-storage",
]
resolver = "3"

[workspace.dependencies]
# Persistence
# Changelog : releases/persistence/CHANGELOG.md
ic-memory = { path = "crates/ic-memory", version = "0.60.0" }
ic-storage = { path = "crates/ic-storage", version = "0.60.0" }

# Metrics-stable
# Changelog : releases/metrics-stable/CHANGELOG.md
ic-metrics-stable = { path = "crates/ic-metrics-stable", version = "0.60.0" }

# Logging-stable
# Changelog : releases/logging-stable/CHANGELOG.md
ic-logging-stable = { path = "crates/ic-logging-stable", version = "0.60.0" }
```

The example illustrates the convention, not an instruction to create or rename
packages. An unused workspace dependency declaration adds no dependency edge;
members inherit only what they use with `workspace = true`. Do not add member
dependencies just to consume catalog entries. The existing `ci/cargo-release-groups.json`
is the executable policy; member manifests supply package versions. Root dependency
values remain actual Cargo requirements, not package-version inheritance. Do not
copy numeric versions into comments or add a generated summary or shadow catalog.

List each catalog alias in its owning release group's `dependencies` roster so
release preparation updates it, including unused and renamed aliases. For every
root path dependency naming a workspace member, spell the full
current member version. Preserve its existing bare, `^`, `~` or `=` operator; do
not add exact pins merely for display. Group admission checks this equality for
all groups, including through the normal dependency checker. It also checks
complete member coverage, alias ownership and each path's actual member manifest,
including unused aliases that Cargo does not resolve. It rejects both
abbreviated versions and compatible but stale lower bounds that Cargo itself
would accept. Release preparation updates the selected group's local requirements
inside its existing transaction; other groups and external dependency requirements
remain unchanged. Manual member-version edits must update the corresponding real
requirements too. Before adopting the check, align root declarations with current
members without changing package versions.

Use comment headings to distinguish local group catalogs from external production,
build and test/tooling dependencies. Classify by actual member usage: mark shared
production/test dependencies as mixed, and describe target/feature restrictions
where they change the graph (for example, native-only schema support). The root
table itself does not assign a dependency kind; member tables and features do.
Keep group and ledger labels near their catalog entries. The root view reports
declared versions, not proof of registry publication.

Review the catalog against the policy and actual Cargo members when membership,
version ownership or changelog paths change. Keep it readable after the normal
group-aware formatter runs; update formatting callers consistently during adoption
rather than bypassing them. The `members` array need not repeat release groups;
optional [member-role comments](#workspace-member-roles) describe development roles.
A single-group workspace needs
no JSON policy solely for documentation; an optional comment may name its shared
version source and root changelog.

An explicit task to adopt this convention includes converting inherited versions
to their existing numeric values, moving ledgers with preserved history and
updating the policy, callers and fixtures. It does not authorize numeric version
bumps, package moves, publication or changes to existing release identities.
Reconcile unfinished attempts against their recorded source and policy; do not
reinterpret old journals after changing version ownership or ledger paths.

## Core and integration graph boundary

Reusable engines own their core contracts; adapters and applications own the
composition of those contracts with a framework or deployment stack. An adapter
may require matching Platform, Wire, Memory, runtime and framework identities.
Those requirements belong to that integration, not automatically to every core
library it consumes. Preserve dependencies required by the core's actual public
types, behavior and retained-data contracts.

- Keep consumer-framework dependencies out of reusable core libraries and their
  ordinary tests. Put framework-specific mounting, lifecycle, deployment and
  composed endpoint qualification at the adapter/application owner or an
  explicitly scoped integration harness. A `dev-dependency`, CLI, example or
  unpublished member can create the same maintenance constraint as a runtime
  dependency; inspect actual callers rather than dependency-role comments alone.
- Preserve real core regression coverage. Replacing a framework-hosted test means
  retaining its engine invariant in owner-local tests and its composition proof
  at the integration boundary; moving a test is not permission to drop that proof.
- Before declaring a core upgrade blocked, name the package, target/features,
  conflicting identities and introducing paths. Distinguish a core contract
  change from an adapter's compatibility constraint. Do not require unrelated
  consumers or every workspace to adopt one fleet-wide dependency generation.
- Prefer deleting an unused edge or moving a composition test to its existing
  adapter owner. If a maintained adapter, tool or qualification stack still
  blocks an otherwise valid core upgrade, isolate its resolving workspace and
  lockfile. Cargo resolves workspace members together, including optional and
  development dependencies; `default-members`, `-p`, feature gates and comments
  alone do not establish independent lockfile resolution. See
  [Cargo's resolver contract](https://doc.rust-lang.org/cargo/reference/resolver.html#features).
- This rule permits a separate integration workspace for that demonstrated
  constraint. Record its root, package scope, reason, lockfile and focused
  qualification commands in the local overlay. Keep existing package identities,
  approved physical layouts and coordinated public versions; unrelated workspace
  migrations or independent public versioning still require maintainer agreement.
- An integration may retain an older qualified matching stack while the core
  advances. Its evidence qualifies only its selected source/dependency identities.
  A path dependency on current core source must be qualified again when those
  inputs change; an old integration pass cannot qualify the new core. Keep core
  and integration checks explicit, with required integration acceptance attached
  to the affected adapter/product change rather than every unrelated core edit.
- Preserve actual runtime singleton, exposed-type, wire/storage and lifecycle
  obligations within each composed graph. Isolation does not authorize bypassing
  a guard, claiming incompatible types interoperate, retaining compatibility
  shims, publishing unqualified combinations or skipping release-owned gates.

## Workspace member roles

The root `[workspace]` table's `members` array may group packages with comment
headers `# Production packages` and `# Tests and tooling packages`:

```toml
[workspace]
members = [
    # Production packages
    "crates/example",
    # Tests and tooling packages
    "crates/example-tests",
]
```

Use section headers rather than a role annotation after each member. Classify
packages by their role in this workspace; a package used in both production and
tests belongs in the production group. Publication settings and registry
availability do not establish that role. These comments describe membership;
they do not change `default-members`, build selections or publication policy.

Member-role headers are distinct from the dependency-role headers in
`[workspace.dependencies]`, governed by the
[Cargo dependency rules](cargo-dependencies.md#dependency-roles-and-deployment-checks).
Grouping does not require moving or renaming packages or changing their approved
layout.

## Independent workspaces and exceptions

The default is one workspace and one selected lockfile. A separate maintained
workspace requires either the documented integration isolation allowed by the
[core boundary rule](#core-and-integration-graph-boundary) or a maintainer-approved
exception in the local `AGENTS.md`, naming its root, package scope, reason,
lockfile and validation coverage. Preserve
existing approved dependency-graph boundaries; layout standardisation alone
does not authorize merging their graphs or upgrading their dependencies.

An approved independent workspace uses the same virtual-root and `crates/` or
`apps/` choices relative to its own root. For example, an approved
`testing/Cargo.toml` owns `testing/crates/<package-name>/` and `testing/Cargo.lock`.
Keeping a package outside these shapes, or keeping a root package, requires an
explicit layout exception as well. Approval may be recorded in this shared rule
or in the local `AGENTS.md`; reference a shared approval in the local overlay when
adopting it, without requesting that approval again. A path's existing name or
use as a test, example or canister is not implicit approval.

### Approved IcyDB layout

The maintainer explicitly approved retaining IcyDB's role-based package layout
on 2026-10-07, following reversal of the crates-only moves. IcyDB may keep:

- `crates/` for its library and tool packages;
- `canisters/` for its audit, demonstration and test canisters;
- `schema/` for its schema and fixture packages; and
- `testing/` for its test harnesses and support packages.

This is an approved physical-layout exception. It requires no relocation into
`crates/` or `apps/` as part of shared-tooling adoption or routine maintenance.
Keep the virtual core root, its selected graph and lockfile, root-owned
versions/dependencies, package identities and complete member coverage. The
[core boundary rule](#core-and-integration-graph-boundary) permits documented
integration isolation when a consumer stack blocks core adoption; this does not
require flattening the four approved trees or redesigning the public packages.
Each maintained workspace retains its own catalog, lockfile and member coverage;
directory names do not waive those requirements.

Record the approval's reference during normal governance adoption. That
documentation step does not make permission to retain the layout conditional on
another approval or a package move. Physical-layout redesign beyond the documented
integration isolation above must be clearly labelled optional feedback, explain
a concrete problem, benefit and migration cost, and receive separate explicit
maintainer approval before edits.

## Adoption and verification

Adopt this rule through a reviewed [governance snapshot](../docs/consuming-snapshots.md).
Coordinate consumer work through issues and preserve unrelated working changes.
In the owning repository's authorized change:

1. Inventory maintained manifests, workspace roots, selected lockfiles and
   approved exceptions. Retain packages already in a standard or approved layout;
   application grouping alone is not a reason to flatten packages into `crates/`.
   Record old-to-new package paths before any necessary moves.
2. Update workspace membership, dependency paths, package include/readme/license
   paths, build scripts, fixtures, source-relative paths, Make/CI commands,
   publication inputs and deployment package references together. Change any
   maintained scaffolding at its generator. Retire obsolete source locations
   rather than leaving duplicate packages or symlink aliases.
   Formatting, release inventories and source checks must cover all declared
   workspace members, including approved layout trees, using Cargo metadata or
   a verified member inventory rather than assuming every package matches
   `crates/*` or `apps/*`.
3. Preserve package names, versions, APIs, features, publication policy, canister
   identities and selected dependencies. Review packaging contents and
   `CARGO_MANIFEST_DIR`-relative inputs; a successful directory move alone does
   not establish equivalent behavior. For coordinated packages still sharing
   their released versions, follow the
   [archive qualification guidance](cargo-dependencies.md#qualifying-coordinated-package-archives-before-a-version-bump).
4. Run the owning focused locked metadata, formatting and path/packaging checks
   for every affected workspace with prepared tools and caches. Keep the existing
   Linux/macOS qualification obligations and local command authority. Adoption
   does not add a new gate or authorize publication, cleanup or version changes.
   Qualify directly affected consumers with focused checks before handoff;
   reserve local full-suite runs for release or an explicit request under the
   baseline.
5. Verify the adopted snapshot and update local instructions and documentation.
   Distinguish committed adoption from local preparation and native execution
   evidence from source inspection.

Source reports must independently exclude Cargo's selected build-output directory
using metadata, including custom target directories. The standard layout keeps
default build output outside package trees, but is not a substitute for correct
file inventory in the reporting helper.
