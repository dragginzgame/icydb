# Cargo dependency ownership

These rules are part of the mandatory [engineering baseline](../DRAGGINZGAME.md).
The repository's root `Cargo.toml` is the authoritative catalog of direct
dependencies and their version/source selections.
The [workspace layout rules](rust-workspaces.md) require a virtual root, define
standard `crates/` and application-owned `apps/` trees, and record scoped
exceptions including IcyDB's role-based layout. Every member inherits from the
same owning root catalog regardless of its directory; package grouping does
not authorize separate dependency selections.
The [pinning rules](dependency-pinning.md) define compatible ranges, justified
exact constraints, immutable Git revisions, lockfiles and external path inputs.

## Required inheritance

- Declare every direct dependency in the root `[workspace.dependencies]`, even
  if only one child crate uses it. This includes third-party packages, local
  workspace crates, development dependencies, build dependencies and dependencies
  used only on particular targets or behind optional features.
- Every child `Cargo.toml` must reference that declaration with
  `name.workspace = true` or `name = { workspace = true, ... }`. Apply this to
  `[dependencies]`, `[dev-dependencies]`, `[build-dependencies]` and all of their
  target-specific forms. If an explicitly approved layout exception retains a
  root package, its dependency tables also inherit from the catalog rather than
  duplicating version/source selections.
- Versions, registry choices, local paths, Git URLs/revisions/branches/tags and
  renamed-package identities belong only in the root catalog. Child dependency
  declarations must not repeat or override them. Declare dependency aliases in
  the catalog too; do not use an alias to hide a drifting child version.
- Children retain their target conditions, optionality and needed features.
  Keep `optional` in the child, since Cargo does not permit it in
  `[workspace.dependencies]`. Preserve effective feature/default-feature behavior
  for the supported Cargo versions when centralizing declarations; inheritance
  must not silently enable features or raise MSRV.
- Excluded packages, examples, integration harnesses and nested workspaces are
  not implicit exemptions. An independently scoped child workspace requires a
  maintainer-approved exception naming its authoritative root, scope and reason;
  record it in the local overlay so dependency ownership remains visible.
- Add or update the root catalog and affected inherited uses in the same batch.
  During manifest review, inspect every child dependency table, including target,
  development and build tables, for standalone version/source declarations.
  Preserve the selected lockfile dependencies and build/publication behavior;
  centralization alone does not authorize dependency upgrades or release changes.
- Keep root and child manifests ordered with `cargo sort --workspace` under the
  [shared formatting contract](git-hooks.md). Include manifest sorting in both
  automatic pre-commit formatting and independent CI/release checks; ordering
  must preserve the catalog and every inherited selection.

## Preparing authorized dependency changes

- For an authorized version, source or feature change, trace every affected
  independent workspace graph, including examples, probes and test harnesses
  reached through path dependencies. A root library and an independent
  `testing/` workspace that depends on it can both need lockfile updates even
  when only the root library's dependency declaration changed.
- Prepare every affected lockfile in the same change batch using the narrowest
  authorized dependency operation. Preserve unrelated selections and local
  command-authority exceptions. Independent graphs must satisfy their declared
  contracts; compatible requirements need not resolve to identical versions.
  This does not collapse approved workspace boundaries or authorize an otherwise
  unrequested dependency upgrade.
- Before declaring dependency preparation complete, run each affected graph's
  owning cheap locked metadata check, including both root and `testing/` checks
  in the example above. Prepare selected caches explicitly when required and
  report any unavailable evidence. Metadata checks establish graph consistency,
  not build, test or native-host qualification; a full gate is not required just
  to prepare a dependency change.
- Keep ordinary release cache fetching locked. Reconcile manifest/lockfile
  mismatches during the authorized dependency change, before the pre-bump
  validation gate. Never silently unlock, upgrade or automatically repair
  dependencies during release. Preserve the complete consumer gate and its
  existing phase order.

## Minimum supported Rust version (MSRV)

- Keep each package's MSRV as low as its supported code, edition and dependency
  graph permit. Declare the verified minimum in `package.rust-version`; inherit
  a common floor from `[workspace.package]` where applicable. A development
  toolchain pin is a separate choice: installing newer Rust for rustfmt, Clippy
  or internal tests does not justify copying that version into the MSRV.
- Document intentional package groups with different floors and their CI
  coverage. Public libraries may support older Rust than application binaries,
  internal tools or test harnesses. This is permitted within one workspace;
  do not raise every member to the highest internal requirement merely to make
  metadata uniform. A public package's supported normal/build dependencies,
  including local packages and proc macros, must work on its advertised floor.
- Before raising a floor, identify the concrete language feature, standard
  library API, manifest/edition requirement or selected dependency that needs it.
  Prefer a simple equivalent supported on the existing floor. Explain any
  retained higher requirement and the cost or security/support reason for
  rejecting the lower alternative; convenience or matching a sibling's current
  development compiler alone is insufficient. Do not downgrade necessary
  dependency fixes or add compatibility machinery simply to retain an older
  compiler.
- Qualify a proposed lower floor with that actual compiler before changing the
  support claim. Record the packages, supported feature combinations, targets
  and selected dependency graph checked, and the blocker if a lower candidate
  fails. Edition minima and dependency manifests narrow the candidates but do
  not prove a complete build works. Do not treat a manifest edit, Clippy's MSRV
  lint or `--ignore-rust-version` as qualification.
- CI must check the advertised minimum explicitly with `cargo +<MSRV>` or a
  scoped `RUSTUP_TOOLCHAIN=<MSRV>`, and record the Cargo/rustc versions used by
  that check. Merely installing a toolchain or setting rustup's default can
  leave `rust-toolchain.toml` selecting the newer development compiler. Keep
  formatting and lint checks on their separately selected development toolchain.
- The minimum lane checks the supported package dependency path with the
  selected lockfile, covering supported features and relevant native/Wasm
  targets. For libraries, check the consumer path without relying on workspace
  feature unification; retain a focused consumer fixture where workspace-only
  checks cannot establish this. Document any higher test-harness floor rather
  than silently dropping minimum coverage or forcing it onto consumers.
  Dependency and lockfile changes must retain this CI coverage. Rust-version-aware
  resolution can help choose compatible dependencies; it does not replace the
  old-compiler check or authorize resolver changes, unlocked retries or automatic
  dependency downgrades.

These rules follow Cargo's [Rust version contract](https://doc.rust-lang.org/cargo/reference/rust-version.html)
and rustup's [toolchain selection order](https://rust-lang.github.io/rustup/overrides.html).
Repositories own their tested floors and focused commands; Shared Tooling does
not prescribe one global Rust version. Tool/cache preparation and sibling edits
retain their existing authorization boundaries.

## Example

Root `Cargo.toml`:

```toml
[workspace.dependencies]
serde = "1"
tempfile = "3"
cc = "1"
rustix = { version = "1", features = ["fs"] }
```

Child `Cargo.toml`:

```toml
[dependencies]
serde = { workspace = true, features = ["derive"] }

[dev-dependencies]
tempfile.workspace = true

[build-dependencies]
cc.workspace = true

[target.'cfg(unix)'.dependencies]
rustix.workspace = true
```

The root lists the complete direct dependency catalog; each child declares which
entries it uses. Cargo's supported inheritance and feature rules are documented
in [the Cargo Book](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#inheriting-a-dependency-from-a-workspace).
