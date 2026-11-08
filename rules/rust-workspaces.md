# Rust workspace layout

These rules are part of the mandatory [engineering baseline](../DRAGGINZGAME.md).
They standardise maintained Rust package locations while preserving locally
owned APIs, canister identities, validation gates and deployment configuration.

## Standard layout

Every repository containing maintained Rust packages uses a virtual root,
including repositories with only one package. The standard package locations
are `crates/<package-name>/` for Rust packages and `apps/<fleet-name>/`
for Canic fleets and their deployed components, with explicit approved layout
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
    canic.toml             # Canic fleet configuration
    component-role/
      Cargo.toml           # Existing package name; directory may name its role
      src/
```

- The repository root is a virtual workspace: its `Cargo.toml` declares
  `[workspace]`, without `[package]`, package targets or package dependency
  tables. Select the resolver explicitly using the supported toolchain; a layout
  change does not authorize a resolver, edition or MSRV upgrade.
- Use `crates/<package-name>/` for Rust packages, whether published or private:
  libraries, standalone binaries/CLIs, tools, shared schemas, test support,
  qualification runners, standalone canisters, examples and probes. Publication
  eligibility and `publish = false` do not determine directory placement.
- Reserve `apps/<fleet-name>/` for Canic fleet configuration and the canister
  components deployed by that fleet. Component packages may use role directories
  such as `apps/demo/user_hub/`, preserving their existing Cargo package names.
  A native publisher, command-line application, generator, test harness or
  qualification package belongs in `crates/` even when it supports a fleet.
  A standalone canister is not a Canic fleet merely because it is an application.
  Do not create general-purpose `apps/tooling/` or `apps/qualification/` trees,
  or reserve an `apps/` path for a future non-fleet service. Existing explicit
  approved layout exceptions below remain valid.
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
  retain their locally owned locations. A Canic fleet under `apps/` may contain
  its deployment configuration, assets and component packages; package references
  follow that fleet's discovery contract.
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
- When an unrelated capability change forces downstream upgrades, trace the
  public-type, implementation and singleton edges before choosing release groups.
  Record the cause and affected consumers in the existing issue or change notes.
  A feature gate does not give a capability an independent package version.
  If an approved split separates a runtime from unrelated capabilities, move the
  implementation owner with its tightly coupled helpers; loosening an empty
  guard's pin cannot establish one runtime. Preserve real singleton refusal.
  Qualify that an unrelated group change leaves the runtime supplier and its
  existing consumers usable, and that incompatible runtime suppliers still fail.
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
  inheritance; root dependency inheritance and repository-wide CI remain.
  Release qualification defaults to the full gate until the consumer qualifies
  an explicit [group plan](../docs/release-group-validation.md).
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
- Private tooling/test maintenance does not by itself select its group's public
  packages for publication. Apply the
  [publication-impact decision](../docs/releases.md#decide-whether-publication-is-needed)
  before bumping versions; group membership and qualification ownership remain
  unchanged. Retain current aligned member versions until an actual group release.
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
in root `[workspace.dependencies]` with its local path. Publishable members need
their full current version. Members with `publish = false` may omit the dependency
version, so Cargo can omit private path-only dev-dependencies from published packages.
Publishable members may depend on private members only through these path-only
development edges; versioned private dependencies and normal/build edges are rejected
before release preparation.
This is a complete crate catalog, including private qualification/tooling members
and crates that no other member currently depends on.
List release groups once in this manifest, as contiguous blocks **at the top of
`[workspace.dependencies]`, before every external dependency**. Keep all local
groups together, including their private members; do not move a private member
into the external test/tooling section. Follow the local groups with external
production dependencies, mixed production/test dependencies, then test/tooling-only
dependencies, omitting empty sections. Build dependencies belong in the section
matching their actual use, with a separate heading when useful.
Give each group block a short title identifying the policy
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
    "crates/example-core",
    "crates/example-derive",
    "crates/example-host",
    "crates/example-tests",
]
resolver = "3"

[workspace.dependencies]
# Core
# Changelog : releases/core/CHANGELOG.md
example-core = { path = "crates/example-core", version = "0.6.0" }
example-derive = { path = "crates/example-derive", version = "0.6.0" }
example-tests = { path = "crates/example-tests" } # Private qualification member

# Host
# Changelog : releases/host/CHANGELOG.md
example-host = { path = "crates/example-host", version = "0.2.0" }

# External production dependencies
thiserror = "2.0"

# External tests and tooling only
serde_json = "1.0"
```

The example illustrates the convention, not an instruction to create or rename
packages. An unused workspace dependency declaration adds no dependency edge;
members inherit only what they use with `workspace = true`. Do not add member
dependencies just to consume catalog entries. The existing `ci/cargo-release-groups.json`
is the executable policy; member manifests supply package versions. Root dependency
values remain actual Cargo requirements, not package-version inheritance. Do not
copy numeric versions into comments or add a generated summary or shadow catalog.

List each catalog alias in its owning release group's `dependencies` roster so
release preparation accounts for it, including unused and renamed aliases.
Path-only private entries remain unchanged; their package versions still advance
with their group. For every versioned root path dependency naming a workspace
member, spell the full current member version. Preserve its existing bare, `^`, `~` or `=` operator; do
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

Preserve this local-groups-first order during formatting, dependency adoption and
release preparation; global alphabetic sorting must not move external entries
ahead of the groups. Use comment headings to distinguish external production,
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
- Core qualification must have an owner-local path that does not require its
  downstream adapter to release first. Merely placing a composed fixture in a
  separate workspace is insufficient if core gates still require it unconditionally.
  Keep composition qualification explicit and required where the changed contract
  affects it. Demonstrate that an incompatible adapter graph fails its own check
  while independent core checks retain accurate results; no silent skipping or
  stale-source evidence may substitute for required integration acceptance.
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

An approved independent workspace uses the same virtual-root layout relative
to its own root: ordinary Rust packages in `crates/`, Canic fleets in `apps/`. For example, an approved
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
The narrowed Canic-fleet-only `apps/` rule requires Toko Tooling 0.35.0 or a later
release containing this correction. Consumers must first verify that the corrected
release is published on crates.io, then adopt its
release-appropriate Cargo selection and authenticated snapshot before moving packages. Version
0.34.0 still contains the broader application-layout wording. Do not patch
consumer snapshot-owned rules or use unpublished producer files as adopted policy.
In the owning repository's authorized change:

1. Inventory maintained manifests, workspace roots, selected lockfiles and
   approved exceptions. Retain packages already in a standard or approved layout;
   preserve Canic fleet component grouping under `apps/`. General application or
   tooling grouping alone does not qualify a package for `apps/`.
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
