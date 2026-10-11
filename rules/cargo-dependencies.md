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

## Cargo network policy

- An authorized dependency update includes normal registry/Git access needed to
  resolve and obtain the selected dependencies. Agents and helpers must not add
  `--offline`, `--frozen` or `CARGO_NET_OFFLINE=true` to `cargo update` merely
  because ordinary validation is offline. Do not require another permission
  request for network access already inherent in that authorized operation;
  actual execution-environment network restrictions still apply.
- Scope the update to the requested packages/versions and affected workspaces.
  Use `cargo update --package NAME --precise VERSION` when an exact version was
  selected. Review its lockfile changes, including required transitive changes.
  Network access does not authorize unrelated upgrades or a broad `cargo update`.
- Release/deployment preparation for an already selected graph uses
  `cargo fetch --locked` before dependent builds or checks. That preparation is
  included in the authorized release/deployment workflow. A plain deployment
  request does not authorize dependency upgrades; when updating dependencies is
  expressly part of the request, finish that bounded update before qualifying
  and freezing the deployment inputs.
  Fetching sources does not install executable tools. Prepare each required
  consumer-selected CLI through its existing setup target, then run its offline
  admission before dependent validation or builds. The documented preparation
  includes those selected installations, never an unrelated tool upgrade.
- Keep `--locked` on validation and artifact builds; it preserves dependency
  selection without prohibiting downloads. Add `--offline` or `--frozen` only
  where the workflow deliberately validates against an already prepared cache.
  Scope offline environment settings to those commands, rather than exporting
  them across a workflow that must also prepare dependencies.
- Honour an explicit caller-selected offline mode, including Cargo environment
  or configuration settings. If required inputs are unavailable, identify the
  setting and missing input; do not silently unset it, retry online, or substitute
  an older cached version. Agent-added flags are not a maintainer request to work
  offline. Do not repeatedly retry the same cache-only resolution when registry
  access is required.
- Offline dependency-free fixtures and qualified local workspace-version
  synchronization may stay offline when the external dependency graph is
  unchanged and the required inputs are prepared. They do not establish the
  policy for adopting newly published dependencies.

Cargo documents that [offline updates](https://doc.rust-lang.org/cargo/commands/cargo-update.html)
are limited by the local cache and can resolve differently from online updates.
[Locked fetching](https://doc.rust-lang.org/cargo/commands/cargo-fetch.html)
prepares the selected graph for subsequent offline commands. Use these distinct
operations according to their purpose rather than applying one flag everywhere.

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
- When that change selects a different executable CLI, run its existing explicit
  setup target and offline check before declaring preparation complete. Preserve
  independent consumer selections; a library update does not automatically
  change a separately selected CLI. Keep the selected lockfiles, earlier tool
  installations and server bytes intact. Recheck the selection after preparation;
  changed inputs or invalid receipts stop the workflow rather than selecting a
  fallback or repairing an existing immutable installation.
- Keep ordinary release cache fetching locked. Reconcile manifest/lockfile
  mismatches during the authorized dependency change, before the pre-bump
  validation gate. Never silently unlock, upgrade or automatically repair
  dependencies during release. Preserve the complete consumer gate and its
  existing phase order.

## Qualifying coordinated package archives before a version bump

Ordinary contributions leave package versions unchanged. When coordinated crates
change together, registry verification can select an older published dependency
with the same version instead of the newly packaged workspace crate. Inspect the
resolved graph before treating that failure as a source defect or changing
versions. Cargo's [packaging procedure](https://doc.rust-lang.org/cargo/commands/cargo-package.html)
normalizes manifests and removes dependency paths; creating both archives alone
does not prove that verification built both current packages.

For this demonstrated same-version case, a consumer may qualify the coordinated
archives locally through its existing packaging check:

1. Preserve the selected source, manifests, lockfiles and failed verification
   evidence. Let Cargo perform normal archive/manifest admission. `--no-verify`
   may separate archive creation from the mandatory build below; archive creation
   alone must never report a successful package check.
2. Unpack the exact new archives into an isolated fixture. Build an independent
   consumer of those packages, using a fixture-only
   [registry override](https://doc.rust-lang.org/cargo/reference/overriding-dependencies.html)
   where needed to select the unpacked dependency. Do not patch product manifests,
   use live workspace/sibling source paths, or rewrite the packaged manifests.
3. Admit the resolved graph before compilation: every coordinated package must
   resolve to its exact unpacked manifest and expected name/version. Retain the
   original name/version/source selections for external dependencies and the
   consumer's feature/target and forbidden-dependency checks. A fixture lock may
   project the selected graph and add its own root, without dependency upgrades.
   Build that admitted graph with `--offline --locked` using prepared caches;
   missing inputs or mismatched selections fail without an online fallback.
4. Require the build and final integrity checks to finish successfully. Verify
   unchanged archive and unpacked-source hashes and original manifests/lockfiles;
   retain logs, selected identities and failed artifacts. Keep the existing full
   gate and native-host qualification requirements.

This establishes local archive interoperability, not registry publication
readiness. Authorized release preparation still qualifies changed metadata;
publication keeps normal Cargo registry admission with the final versions.
Do not propagate the fixture override or `--no-verify` into publication. Keep
this scoped check with its consumer; it does not require a new shared packager,
release transaction or duplicate check in every repository.

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
