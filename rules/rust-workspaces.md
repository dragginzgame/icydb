# Rust workspace layout

These rules are part of the mandatory [engineering baseline](../TOKO-TOOLING.md).
They standardise maintained Rust package locations while preserving locally
owned APIs, canister identities, validation gates and deployment configuration.

## Standard layout

Every repository containing maintained Rust packages uses a virtual root,
including repositories with only one package. The standard package locations
are `crates/` for product packages, `apps/<fleet-name>/` for Canic fleets and
their deployed components, and `qualification/` for separate qualification-only
packages. Meaningful subdirectories are supported: both `crates/<package-name>/`
and `crates/<domain>/<package-name>/` are standard, as are the corresponding
qualification layouts. Toko's grouped product packages need no layout exception.
Repositories need only the trees they use. The baseline's
[product and qualification rule](../TOKO-TOOLING.md#product-and-qualification-workspaces)
owns this standard across repositories.

This is a deliberate fleet consistency convention: even a single library keeps
both the root workspace `Cargo.toml` and its package `crates/<package-name>/Cargo.toml`.
That example is the simplest shape, not a maximum directory depth. List actual
member paths explicitly in the owning manifest. Grouping introduces no extra
workspace, catalog, version owner or release group; discover members with Cargo
metadata rather than a fixed-depth glob or directory scan.
Retain the standard shape; adoption changes existing layouts through a scoped
migration rather than incidental cleanup:

```text
Cargo.toml                 # Virtual workspace; no [package]
Cargo.lock                 # Selected product dependency graph
crates/
  package-name/
    Cargo.toml             # [package].name = "package-name"
    src/
    tests/                 # When needed
  domain/                  # Optional meaningful product grouping
    grouped-package/
      Cargo.toml
      src/
apps/
  app-name/
    canic.toml             # Canic fleet configuration
    component-role/
      Cargo.toml           # Existing package name; directory may name its role
      src/
qualification/             # Only when separate qualification packages exist
  Cargo.toml               # Independent virtual workspace
  Cargo.lock               # Selected qualification graph
  package-name/            # No qualification/crates/ directory
    Cargo.toml             # publish = false; no product release group
    src/
  domain/                  # Optional meaningful qualification grouping
    grouped-fixture/
      Cargo.toml
      src/
```

- The repository root is a virtual workspace: its `Cargo.toml` declares
  `[workspace]`, without `[package]`, package targets or package dependency
  tables. Select the resolver explicitly using the supported toolchain; a layout
  change does not authorize a resolver, edition or MSRV upgrade.
- Use packages under `crates/` for product libraries, standalone binaries/CLIs,
  reusable tools, shared schemas and standalone product canisters. Packages
  maintained solely to test, benchmark, fuzz or qualify those products, including
  private fixture canisters and test-support packages, belong under
  `qualification/` in its independent workspace. A reusable testing tool shipped
  as a product stays in `crates/`; publication flags and names alone do not decide
  responsibility. No product normal, build or dev dependency may point back to
  a qualification package. Keep a small unit-test helper local to its product
  instead of creating a reverse dependency.
- Reserve `apps/<fleet-name>/` for Canic fleet configuration and the canister
  components deployed by that fleet. Component packages may use role directories
  such as `apps/demo/user_hub/`, preserving their existing Cargo package names.
  A native publisher, command-line application or product generator belongs in
  `crates/`; a qualification-only harness or fixture belongs in `qualification/`.
  A standalone canister is not a Canic fleet merely because it is an application.
  Do not create general-purpose `apps/tooling/` or `apps/qualification/` trees,
  or reserve an `apps/` path for a future non-fleet service. Test-only fleet
  packages follow the qualification workspace's package layout; retained
  deployment configuration may reference those packages explicitly.
- Classify canisters by purpose, not execution format. Standalone product
  canisters use `crates/`; product fleet components use `apps/`; separate test,
  audit and benchmark canisters use `qualification/`, with optional meaningful
  grouping. Do not introduce a parallel top-level `canisters/` tree or a separate
  workspace merely because a package compiles to a canister. Preserve package
  names and update all build/deployment references when migrating existing test
  canisters. Small fixtures without their own manifest stay beside their tests.
- Keep ordinary module tests, examples and fixtures with their owning package;
  they do not need separate packages merely to follow this layout.
- Declare product members in the root workspace and exclude `qualification`.
  Declare qualification-only members in `qualification/Cargo.toml`, each with
  `publish = false`. Keep common package
  metadata in `[workspace.package]`, with inherited child values. Package versions
  follow the [release-group rules](#coordinated-versions-and-workspace-boundaries):
  a coordinated product workspace uses root inheritance; independent product
  groups all use package literals. Qualification versions are private metadata
  owned by their workspace, may use workspace inheritance, and do not advance
  with product releases. Keep each workspace's complete direct dependency catalog in
  `[workspace.dependencies]` under the [Cargo dependency rules](cargo-dependencies.md).
  Those rules allow documented package-specific MSRVs; a higher internal-tool
  floor must not become the public libraries' minimum through inheritance alone.
  Preserve package identities, selected dependencies and effective features.
  An `apps/` directory does not create an independent Cargo workspace: its
  product packages belong to the product root, catalog and declared release groups.
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
  or empty `crates/`, `apps/` or `qualification/` directories. Generated scratch projects, vendored
  upstream sources and frozen historical evidence are not maintained package
  locations; do not move them or rewrite history to satisfy this layout.

## Coordinated versions and workspace boundaries

Keep related product crates together for development. A Cargo workspace
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
still governs the independent qualification graph.

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
- For independent public versions or workspace migrations beyond the standard
  product/qualification separation, show a recurring maintenance problem, why a local
  fix or release batching is insufficient, and the total migration and
  qualification cost for consumers.
  Preserve approved independent graphs and record maintainer agreement for the
  selected boundary or versioning-policy change. Carry an existing approval
  forward; do not request it again for implementing that agreed boundary.
  Approved product release groups share the product workspace and lockfile using the existing
  [explicit release-group contract](../docs/releases.md#explicit-cargo-release-groups).
  Its admitted member versions are the scoped exception to workspace-version
  inheritance; root dependency inheritance and repository-wide CI remain.
  Release qualification defaults to the full gate until the consumer qualifies
  an explicit [group plan](../docs/release-group-validation.md).
- In a product workspace with independent release groups, every product member
  belongs to one explicit group. Qualification-only tooling and fixtures are
  members of the separate qualification workspace, never of these groups. Remove
  the product root's `[workspace.package].version`; every product member declares a literal package version
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
- Qualification-only maintenance does not select a product group for publication
  or change the qualification packages' versions to match it. Changes to private
  product tools require their actual product-impact assessment. Apply the
  [publication-impact decision](../docs/releases.md#decide-whether-publication-is-needed)
  before bumping versions. Qualification selection is a validation relationship,
  not release-group membership.
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
- Keep unrelated repositories independently versioned. Toko Tooling snapshot
  adoption, a common dependency or a common owner does not require fleet-wide
  version synchronization. Verify adoption through the consumer's
  [selected dependency graphs](../docs/cargo-graph-checks.md#coordinated-release-adoption).

### Visible release-group map in Cargo.toml

For a product workspace with independent release groups, include every product member
in root `[workspace.dependencies]` with its local path. Publishable members need
their full current version. Members with `publish = false` may omit the dependency
version, so Cargo can omit private path-only dev-dependencies from published packages.
Publishable members may depend on private members only through these path-only
development edges; versioned private dependencies and normal/build edges are rejected
before release preparation.
This is a complete product crate catalog, including product tools and crates
that no other product member currently depends on. The qualification catalog
separately owns its private members and references to the product packages.
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
]
exclude = ["qualification"]
resolver = "3"

[workspace.dependencies]
# Core
# Changelog : releases/core/CHANGELOG.md
example-core = { path = "crates/example-core", version = "0.6.0" }
example-derive = { path = "crates/example-derive", version = "0.6.0" }

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
- Delete unused edges and keep composition tests with their responsible adapter
  or qualification owner. Separate qualification packages use the independent
  `qualification/` workspace by default, before a conflict occurs. Cargo resolves
  workspace members together, including optional and
  development dependencies; `default-members`, `-p`, feature gates and comments
  alone do not establish independent lockfile resolution. See
  [Cargo's resolver contract](https://doc.rust-lang.org/cargo/reference/resolver.html#features).
- Record the qualification root, member scope, lockfile and focused commands in
  the local overlay. Adopting this standard needs no additional architectural
  exception once migration is authorized. Preserve package identities and public
  release versions; do not move a product adapter into qualification merely
  because its dependency graph is inconvenient.
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

Workspace membership follows responsibility, not an optional comment grouping:

- Product libraries, applications and reusable product tools belong to the root.
- Separate qualification-only runners, benchmarks, fuzzers, fixture canisters
  and test-support packages belong to `qualification/` and are unpublished.
- Ordinary package-local tests, examples and fixtures remain with their product;
  this rule does not turn every test file into another crate or workspace.

Classify mixed-use code by its real product contract. A reusable product such as
Testkit is not qualification-only just because other products use it in tests.
Do not use `publish = false` to move a product build dependency out of its owning
graph. Mark qualification package roles in the existing LOC metadata, keeping
reports consistent with the actual source responsibility.

Dependency-role headers inside each workspace's `[workspace.dependencies]` remain
governed by the
[Cargo dependency rules](cargo-dependencies.md#dependency-roles-and-deployment-checks).
They do not assign release-group membership or change enabled dependency kinds.

## Independent workspaces and exceptions

The standard is one product workspace plus one independent qualification
workspace when separate qualification packages exist. Each has one catalog and
lockfile. Qualification packages live under `qualification/`, optionally grouped
by meaningful domain, never under a redundant `qualification/crates/` layer.
Do not repeat the product directory convention inside the qualification root.
Both roots are virtual workspaces. Nested package directories do not create
nested workspaces or weaken the single-catalog rule for each root.

Additional independent workspaces or a nonstandard package layout need a scoped
maintainer exception naming the root, members, reason, lock and validation
coverage. Preserve existing independent resolution boundaries until their
replacement is qualified; standardization does not authorize merging incompatible
graphs, upgrading suppliers or skipping required checks.

### Qualification and release-group relationships

A release group contains only product-workspace packages. A qualification package
may validate several groups but belongs to none of them. The consumer's explicit
validation plan maps each group/change to required qualification roots, packages,
targets, features and commands. An unqualified mapping retains the existing
complete gate; separate workspaces do not waive release acceptance.

Qualification inherits no product release version and owns no product release
ledger. Product releases may update qualification's product path requirements and
resolved lock identities. Prepare and validate all affected metadata in one
transaction; preserve unrelated dependency selections and private package versions,
and roll back the complete candidate on failure. A shared qualification lock may
appear once as shared release metadata without making its packages group members.

Toko Tooling owns reusable multi-lock rendering, admission and rollback mechanics.
Consumers own their validation mapping and product-specific checks. Declaring
additional metadata in the current runner does not itself render or qualify it.
Do not copy generic release machinery into every consumer, or claim multi-lock
support merely from this policy. Qualify shared mechanics before consumer release
adoption; keep any existing qualified adapter narrowly scoped until replacement.

### Approved IcyDB layout

The 2026-10-07 permission to retain IcyDB's `crates/`, `canisters/`, `schema/`
and `testing/` trees is historical migration context. The maintainer's 2026-11-09
common product/qualification standard supersedes that general layout exemption.
During scoped adoption, classify actual product schemas/tools and qualification
fixtures separately, preserve package identities and move them into the standard
owners. Do not collapse IcyDB's independently resolved Canic qualification into
the product graph. This policy change is not authority to edit IcyDB or silently
move packages during unrelated maintenance; complete its migration as a coherent,
separately scoped consumer task.

## Adoption and verification

Adopt this rule through a reviewed [governance snapshot](../docs/consuming-snapshots.md).
Coordinate consumer work through issues and preserve unrelated working changes.
The product/qualification standard is introduced in the pending 0.38.0 policy.
Consumers must verify the supplying release is actually published, then adopt
its exact selection and authenticated snapshot before claiming adoption. A pending
version or this document does not establish publication. Do not patch consumer
snapshot-owned rules or use unpublished producer files as adopted policy.
In the owning repository's authorized change:

1. Inventory maintained manifests, workspace roots, selected lockfiles and
   approved exceptions. Retain packages already in the standard layout;
   preserve Canic fleet component grouping under `apps/`. General application or
   tooling grouping alone does not qualify a package for `apps/`. Move separate
   qualification packages to the independent qualification tree, remove them
   from product groups and keep their existing private versions. Migrate old
   general layout exemptions to the common standard rather than treating them as
   permanent grandfathering; any new exception must be explicit and scoped.
   Record old-to-new package paths before any necessary moves.
2. Update workspace membership, dependency paths, package include/readme/license
   paths, build scripts, fixtures, source-relative paths, Make/CI commands,
   publication inputs and deployment package references together. Change any
   maintained scaffolding at its generator. Retire obsolete source locations
   rather than leaving duplicate packages or symlink aliases.
   Formatting, release inventories and source checks must cover all declared
   workspace members in both roots, using Cargo metadata or
   a verified member inventory rather than assuming every package matches
   `crates/*`, `qualification/*` or `apps/*`. Retain meaningful nested grouping;
   do not flatten supported paths merely to simplify a script. Keep required group qualification mapped explicitly,
   with atomic lock/metadata handling and rollback evidence. Demonstrate that an
   incompatible qualification selection refuses there without blocking independent
   product resolution; do not weaken genuine runtime/type identity checks.
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
