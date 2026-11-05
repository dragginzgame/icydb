# Cargo dependency ownership
<!-- Shared companions: ci/examples/platform-owner-boundaries.json rules/rust-workspaces.md -->

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

## Shared boundary check

Consumers can adopt the reviewed inventory in `ci/external-ic-dependencies.txt`
through `bash scripts/ci/check-cargo-dependency-boundaries.sh --consumer "$PWD"
--external-ic`. This checks canonical dependency names, including aliases,
optional, build, development and target-specific declarations, using locked
offline Cargo metadata. The sole allowed direct package owner for the listed
external IC dependencies is `ic-platform`, subject to the narrow identity and
native implementation exceptions below. Its canonical `ic-platform-types`
identity supplier additionally owns only the optional `ic_principal` dependency
needed for explicitly selected registry conversions. Self-authentication uses
upstream SHA-224 separately and does not activate that conversion dependency.

This option composes with `ci/cargo-dependency-boundaries.json`: local rules may
tighten the common boundary but cannot relax it. The local file may be absent
when only the common inventory is selected. Without the option, the existing
local-only contract is unchanged. Remove duplicate local prohibitions only after
the consumer actually enables and qualifies the common inventory.

The inventory is a reviewed list of known packages, not proof that every external
IC package has been discovered. Review new dependencies against the broader
[external IC boundary](#external-ic-dependency-boundary); add missing names here.
Do not infer ownership from an `ic-` prefix or prohibit our owned packages.

## Platform package owners

Select the owner of the needed capability directly, with default features
disabled. These are three independently versioned groups in one repository:

| Package | Consumer responsibility |
| --- | --- |
| `ic-platform-types` | Canonical Principal, Account and related identity/error types. Use directly in identity-only models and schemas. |
| `ic-platform-runtime` | IC0, the executor, raw calls and CDK adapters. Select only the needed `ic0`, `executor`, `runtime` or `cdk` feature. |
| `ic-platform` | Explicit crypto, certification, storage-substrate and external-protocol/native-client capabilities. Retain it where those capabilities are actually used. |

For example, the published Types 0.16 and Runtime 0.17.1 generation supports
these root catalog entries; members inherit only what they use:

```toml
[workspace.dependencies]
ic-platform-types = { version = "0.16.0", default-features = false }
ic-platform-runtime = { version = "0.17.1", default-features = false }
ic-platform = { version = "0.17.0", default-features = false }
```

An identity-only member inherits Types. An IC0-only member inherits Runtime with
`features = ["ic0"]`; this graph needs neither Types, Candid, CDK, executor guard
nor crypto. A crypto consumer may separately inherit Platform with, for example,
`features = ["blake3"]`. Runtime's higher-level features have different closures:
the executor needs its guard; raw calls need Types; CDK adds entrypoint generation.
Do not apply the IC0-only exclusions to a consumer that actually needs those APIs.

Types' `principal-self-authenticating` selects the retained SHA-224 derivation.
It does not imply `principal-external`; select the latter explicitly for registry
Principal conversions. Conversion impls remain with the canonical owned type.
This narrow exception does not admit Candid or raw SDK dependencies in Types or
application packages. Runtime uses its owned implementations; consuming Runtime
does not authorize direct `ic0`, `ic-cdk` or executor-package dependencies.

The [example declaration policy](../ci/examples/platform-owner-boundaries.json)
permits Types in `identity-model` and `crypto-adapter`, Runtime in `ic0-adapter`,
and the facade only in `crypto-adapter`. Replace these example package names with
actual local owners and run the existing boundary checker with `--external-ic`.
Canonical aliases, optional declarations and build/dev/target tables remain
subject to the same owner policy. This is a consumer policy, not a blanket ban
on facade use or a new common external-package exception.

Declaration admission alone does not establish the enabled transitive graph.
Use [selected owner graph checks](../docs/cargo-graph-checks.md#platform-owner-graphs)
for actual roots, targets and features. Types crossing public boundaries must
resolve to one compatible identity; runtime/executor consumers need one selected
execution owner and guard where applicable. A facade-only release must not
mechanically advance Types or Runtime. Preserve consumer MSRV, published supplier
availability and independent adapter qualification; a registry graph or native
compile does not prove installed IC or macOS behavior.

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
  documented boundary under the
  [core and integration graph rule](rust-workspaces.md#core-and-integration-graph-boundary)
  or a maintainer-approved exception. Name its authoritative root, scope and
  reason in the local overlay so dependency ownership remains visible; each
  independent workspace still owns its catalog and lockfile.
- Add or update the root catalog and affected inherited uses in the same batch.
  During manifest review, inspect every child dependency table, including target,
  development and build tables, for standalone version/source declarations.
  Preserve the selected lockfile dependencies and build/publication behavior;
  centralization alone does not authorize dependency upgrades or release changes.
- Keep root and child manifests ordered with `cargo sort --workspace --grouped` under the
  [shared formatting contract](git-hooks.md). Include manifest sorting in both
  automatic pre-commit formatting and independent CI/release checks; ordering
  must preserve the catalog and every inherited selection.

## External IC dependency boundary

The Tooling-owned `ic-testkit` native PocketIC implementation may select
`candid`, `ic-certification`, `ic-management-canister-types` and
`ic-transport-types` only as normal dependencies under
`cfg(not(target_family = "wasm"))`. This narrow maintenance
exception preserves the transferred client and native-only graph; it grants no
server ownership, other IC dependencies, portable/Wasm declarations or build/dev
exception. Consumer dependency policies can restrict it further.

All IC-specific dependencies maintained outside `dragginzgame`, including DFINITY
packages, must be consumed through explicit `ic-platform` features and interfaces.
The canonical identity owner is `ic-platform-types` for `ic_principal`: conversion
impls must live with the canonical owned type to satisfy Rust coherence. That
exception admits no Candid, runtime, storage or other external IC dependency.
Apart from this native Testkit exception, IC Platform is the direct dependency
owner for these upstream packages. Other repositories must not declare or import
them directly, including through renamed dependencies, generated code or
proc-macro expansions.

This section and its automated inventory govern Cargo packages. JavaScript and
TypeScript SDK integration uses the separate `@tokostudios/ic-platform` npm
package under the [npm IC boundary](dependency-pinning.md#ic-javascript-and-typescript-dependencies).
Selecting the Rust crate does not supply or qualify the npm package, and the
Cargo boundary checker does not inspect npm manifests or JavaScript imports.

- This covers Candid and its parser/derives, CDK/runtime/system APIs, principals,
  Agent/PocketIC clients, certification/signature and ledger/management types,
  stable-structures and any other external package specific to the Internet
  Computer, regardless of its publisher or package-name prefix.
- Apply the boundary to normal, build, development, test and tooling dependencies,
  optional features, target-specific tables and independently scoped workspaces.
  A private probe, example, unpublished package or macro-generated upstream path
  is not an automatic exception.
- If Platform lacks a required capability, file or update its owning issue and
  qualify an explicit Platform interface before adopting it. Do not bypass that
  work with a new direct dependency, copied implementation or consumer facade.
  Track existing direct uses as migration debt; retire them with their actual
  callers and focused checks, preserving external bytes and recovery obligations.
  Any temporary exception requires express maintainer approval with scope and
  reason under the baseline's exception rules.
- Product ownership stays with its Dragginzgame crate: for example, Memory/Storage
  own memory and storage policy, Testkit owns simulator lifecycle, and Wire owns
  application codecs and resource admission. Platform owns the external dependency boundary;
  this rule does not move those product implementations into Platform. General
  purpose dependencies such as Serde remain with their consuming crates.

Apply the baseline's [IC application encoding policy](../DRAGGINZGAME.md#ic-application-encoding)
before adding or retaining a Candid dependency or requesting another Platform
Candid facade. Owned application endpoints use `ic-wire`, including public
endpoints with third-party callers. `ic-platform` supplies the explicit adapter
for independently defined protocols that require Candid and existing checkpoint
compatibility; dependency isolation does not make Candid an application-codec choice.
Deriving or implementing `CandidType` through Platform is still forbidden on
owned application types. Keep unavoidable external wire projections separate
from domain, persistence and RPC models under that encoding contract.

## Dependency roles and deployment checks

Place local release-group blocks first under the
[visible group-map convention](rust-workspaces.md#visible-release-group-map-in-cargotoml).
Follow them with external production, mixed production/test, then test/tooling-only
dependencies; omit empty sections. Use one short comment heading per block, a
blank line between blocks and alphabetical order within each. Do not repeat role
annotations after individual dependencies or split local groups by dependency role.
These dependency-role headers belong only inside `[workspace.dependencies]`.
The `members` array may separately use `# Production packages` and
`# Tests and tooling packages` under the
[workspace member-role guidance](rust-workspaces.md#workspace-member-roles).
Dependency roles describe use within this workspace, not whether a selected
version is published to a registry.

- A production dependency is needed by a production package through normal or
  build dependencies, including supported optional features and targets. Label
  external entries shared with tests/tooling as mixed; they still belong to the
  production graph. Production build dependencies also belong here even though
  their code need not enter the artifact.
- Tests and tooling entries are used only through development dependencies or
  non-production packages. A package can be published elsewhere and still serve
  only this role here. Workspace membership and `package.publish` do not prove
  deployment use or successful registry publication.
- Keep one catalog and child inheritance. Annotations do not select dependencies;
  Cargo's manifests and resolved graph remain authoritative. Preserve the sections
  through the [formatting contract](git-hooks.md), using Cargo sort's grouped mode
  consistently in formatting, hooks, CI and release checks.

Before deployment, inspect the production package's locked graph with the same
target and feature selection as its build. For example, for a default-feature
Wasm package, replacing `production-package` with its actual package name:

```bash
cargo tree --locked -p production-package --target wasm32-unknown-unknown --edges normal,build
```

Add the build's `--no-default-features` and `--features` selections when used.
Check that tests/tooling-only entries do not enter this graph as dependencies,
including through transitive dependencies; exclude the selected root package
itself and account for renamed dependency identities. A check for one deployment
does not establish every supported feature/target combination.
The existing direct-dependency boundary checker can enforce package ownership
rules, but does not check transitive reachability or these comment labels.

[Cargo tree](https://doc.rust-lang.org/cargo/commands/cargo-tree.html) is an
inspection aid, not an exact compilation trace. Automated deployment admission
belongs with the consumer's build: enforce its dependency exclusions, retain the
successful locked build evidence, and bind the inspected source, lockfile,
target/features and final artifact digest to the bytes deployed. Registry
availability is separate evidence; a cached offline build or a `version` beside
`path` does not establish that the selected source is a live registry package.

## Cargo network policy

- Authorized development, tests and validation include preparing missing sources
  with `cargo fetch --locked` for each affected workspace's existing lockfile.
  Run it with that workspace's selected toolchain and Cargo cache; do not ask for
  separate maintainer approval merely because a selected source is not cached.
  Keep manifests, lockfiles and version selections unchanged. If the lockfile is
  missing or inconsistent, report the failure; this authority does not include
  generating a lockfile, an unlocked retry or a dependency update.
  Honour explicit offline settings and actual environment/sandbox restrictions;
  an agent-added offline flag does not create a new approval requirement.
  This covers dependency-source preparation only, not executable-tool downloads
  or installation, runtime network access, new credentials/targets, paid effects,
  deployment or release execution. Keep preparation separate from offline checks
  and read-only helpers; do not add automatic fetches to every check target.
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
- Before planning a downstream release cascade, apply the
  [adoption planning contract](../docs/cargo-graph-checks.md#dependency-adoption-planning).
  Every additional dependency upgrade must have a required contract/security
  reason or be explicitly selected as an optional refresh. Compatible external
  patch availability alone does not justify raising a minimum or republishing
  unchanged consumers. Raise minimums when the delivered contract actually needs
  the newer fix/capability, and qualify that requirement. This does not relax the
  exact current-version catalog rule for local workspace members.
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
- For focused post-update qualification, follow the
  [dependency-adoption contract](../docs/cargo-graph-checks.md#focused-dependency-adoption-contract).
  Keep manifest inventories, target/features, singleton policy, affected-package
  lint and artifact verification consumer-owned. Reuse existing observations and
  sequential fail-fast orchestration before adding shared machinery.
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

- Use **Rust 1.99.1** as both the minimum supported compiler and the pinned
  development, lint and release compiler for the Dragginzgame Rust stack. The
  maintainer retired older-compiler support because it has no required consumers;
  reducing duplicate qualification and compatibility maintenance is the reason
  for this change. Do not retain an older floor merely because code still builds
  with it. This decision supersedes the previous lowest-possible-MSRV policy.
- Declare `rust-version = "1.99.1"` in `[workspace.package]` and inherit it in
  members. Pin `rust-toolchain.toml` to `channel = "1.99.1"`; do not use moving
  `stable`. Reconcile explicit package overrides, independent fixture workspaces,
  compiler selectors in CI/release scripts and current support documentation.
  Release groups retain independent package versions and changelogs.
- Qualify each affected supported package/feature graph with the selected locked
  dependencies on 1.99.1 before claiming adoption. Include native and applicable
  Wasm consumer paths, local packages and proc macros. A manifest edit, successful
  metadata resolution, Clippy's MSRV lint or `--ignore-rust-version` is not build
  qualification. Preserve isolated consumer fixtures where workspace feature
  unification would hide a missing dependency or feature.
- Record and enforce actual Cargo/rustc 1.99.1 identities in validation. Merely
  installing a compiler or setting rustup's default does not override a selected
  repository toolchain. Read the admitted local manifest/toolchain policy instead
  of retaining independent hardcoded old-version selectors.
- Consolidate a separate MSRV job only when the retained 1.99.1 checks cover its
  package, feature, target and consumer obligations. Reuse that evidence instead
  of compiling the same graph twice under different job names. Keep distinct
  native hosts, Wasm, installed-runtime, feature-isolation and archive-consumer
  checks; update required-check wiring with any job removal. Do not infer a
  performance improvement or an amount of disk savings from a manifest bump.
- Remove obsolete compatibility branches and suppressions when their replacements
  preserve behavior. Do not refresh unrelated dependencies or synchronize sibling
  releases as part of compiler adoption. Retain old artifacts/evidence until an
  authorized cleanup; dropping support does not authorize deleting caches.
- Future compiler changes and any older-compiler exception require an explicit
  maintainer decision, a concrete support reason and applicable qualification.
  A newer Rust release does not automatically advance this pinned policy.

These rules follow Cargo's [Rust version contract](https://doc.rust-lang.org/cargo/reference/rust-version.html)
and rustup's [toolchain selection order](https://rust-lang.github.io/rustup/overrides.html).
Shared owns this support policy; repositories own implementation, focused commands
and adoption evidence. Tool/cache preparation, sibling edits and publication
retain their existing authorization boundaries. A pending shared rule does not
prove that a consumer or previously published archive has adopted the new floor.

## Example

Root `Cargo.toml`:

```toml
[workspace.dependencies]
# Production dependencies
cc = "1"
rustix = { version = "1", features = ["fs"] }
serde = "1"

# Tests and tooling only
tempfile = "3"
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
