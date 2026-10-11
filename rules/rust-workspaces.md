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
- Declare the maintained members in the root workspace. Keep shared package
  versions and common package metadata in `[workspace.package]`, with inherited
  child values. Keep the complete direct dependency catalog in
  `[workspace.dependencies]` under the [Cargo dependency rules](cargo-dependencies.md).
  Those rules allow documented package-specific MSRVs; a higher internal-tool
  floor must not become the public libraries' minimum through inheritance alone.
  Preserve package identities, selected dependencies and effective features.
  An `apps/` directory does not create an independent Cargo workspace: its
  packages belong to the same root and inherit the same versions and dependencies.
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

## Independent workspaces and exceptions

The default is one workspace and one selected lockfile. A separate maintained
workspace requires a maintainer-approved exception in the local `AGENTS.md`,
naming its root, package scope, reason, lockfile and validation coverage. Preserve
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
Keep the virtual root, one selected workspace graph and lockfile, root-owned
versions/dependencies, package identities and complete member coverage. These
requirements apply across all four trees; directory names do not waive them.

Record the approval's reference during normal governance adoption. That
documentation step does not make permission to retain the layout conditional on
another approval or a package move. Any proposed IcyDB layout redesign must be
clearly labelled optional feedback, explain a concrete problem, benefit and
migration cost, and receive separate explicit maintainer approval before edits.

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
   Code changes still complete the repository's existing full validation suite
   before delivery under the baseline.
5. Verify the adopted snapshot and update local instructions and documentation.
   Distinguish committed adoption from local preparation and native execution
   evidence from source inspection.

Source reports must independently exclude Cargo's selected build-output directory
using metadata, including custom target directories. The standard layout keeps
default build output outside package trees, but is not a substitute for correct
file inventory in the reporting helper.
