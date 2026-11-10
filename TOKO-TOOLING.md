# Toko Tooling Engineering Baseline
<!-- Detailed references are pinned; select governance entrypoints explicitly for offline use. -->

Toko Tooling owns the common engineering rules for `dragginzgame` repositories.
Read this core, the local `AGENTS.md` and the current handoff when present. Read
linked specialist sections only when the task touches their subject, before the
affected action. Do not recursively read every linked guide at startup or reread
unchanged instructions during the same task. Local overlays should follow the
same approach: mandatory local constraints first, task-specific references after.

Detailed links below identify an immutable reviewed policy revision. Selecting
this file does not install their documents, audit procedures or task runners.
For offline work, explicitly select the needed guides or the complete governance
entrypoint list from the same tooling release; do not assume linked content is
available locally. The full crate also contains those documents. If a required
reference is unavailable, obtain the pinned reference before the affected task.

## Adoption and local authority

Installing tooling and adopting this policy are separate decisions. Before adding
the adoption statement to a consumer's `AGENTS.md`, classify existing rules:

| Category | What the consumer does |
| --- | --- |
| Binding common rules | Apply authorization boundaries, source integrity, evidence requirements, dependency ownership and explicit prohibitions. These are not optional defaults. |
| Consumer-owned choices | Retain architecture, product limits, local commands, release inputs and validation gates within the common contract. No exception is needed for a choice the baseline delegates. |
| Explicit defaults | Keep a different choice only where the owning rule explicitly allows it; a prohibition is not a default. |
| Conflicts requiring an exception | Record the exact rule, affected repository/workflow, reason and maintainer approval in the local overlay before claiming adoption. Existing practice alone is not approval. |

For example, an existing Python workflow needs a scoped exception to the Python
tooling prohibition. A consumer-specific test command is ordinarily a local
choice; it must still supply the required validation evidence. Installing the
crate or vendoring this file grants no exception and does not activate audits.

- Apply the repository's reviewed snapshot or revision-bound baseline, never a
  moving sibling checkout. Keep snapshot-owned files intact; repair at Toko
  Tooling and adopt a reviewed committed revision through the exporter.
- Consumers own architecture, product limits, local commands and release inputs.
  Toko Tooling owns common engineering practice and fleet audits. Local exceptions to
  common rules need maintainer approval with scope and reason. Preserve explicit
  exclusions and approved product restrictions.
- During adoption, remove redundant or conflicting overlay wording; retain only
  local choices and documented exceptions. Do not turn a general prohibition
  into a new permission prompt when the common rule already authorizes the task.
  See [adoption details](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#adoption-and-local-authority).

## Scope and authorization

The [contribution rules](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/contributions.md) own detailed action authority;
local overlays must not duplicate it or add routine permission gates.

- Prefer direct edits in the normal authorized checkout when a PR is optional.
  Use branches there when needed; coordinate or wait when another session owns
  the inputs. Separate checkouts are explicit exceptions, not the default for
  isolation, PRs or deployment.
  Ordinary fixes include focused checks and pending notes, not staging file
  contents, commits or delivery. They do authorize `git add -N -- <explicit-paths>`
  for newly created, task-owned files when an applicable validation check requires
  Git to know those paths (for example, a new qualification lockfile). This is
  intent-to-add bookkeeping, not content staging: inspect the index, preserve
  existing staged work, explain the reason and proceed without asking again.
  This narrow permission supersedes older linked wording that reserves every
  index change. Toko Tooling alone also prepares its pending `VERSION`.
- A PR request includes its topic branch, scoped commits, branch push and PR.
  Release/ship/deploy requests include their necessary delivery steps and direct
  integration-branch pushes. An unqualified “deploy”, “ship” or “release” applies
  only to the current repository. All three include both GitHub delivery and
  crates.io publication for its affected publishable groups, without another
  publication confirmation. Sibling delivery requires an explicit request;
  do not scan siblings or ask whether to include them by default. Announce the
  selected plan, then
  proceed under [delegated delivery](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/contributions.md#delegated-delivery).
- Named scope, no-push/read-only instructions and exclusions take precedence.
  `toko` and `toko-miner` require explicit inclusion; DFINITY contributions remain
  excluded. A dependency blocker alone does not expand delivery scope.
- Preserve unrelated dirty work and reread files before editing. Sibling inspection
  is read-only; source contributions need scoped authority. Coordinate active
  sessions before changing their inputs or preparing an authorized fleet release.
- Do not create new directories directly under `~/projects`; only an explicit
  user exception for that exact directory permits it. Ordinary fix, PR and deploy
  authority does not. Additional checkouts require approval of their location.
  Never leave the only copy of lasting work in `/tmp`, `target/`, caches or a
  disposable patch.
  Follow the [checkout contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/contributions.md#durable-work-and-checkout-location).
- Carry authorization forward without repeated approval. Inspection and readiness
  questions alone do not authorize repair or delivery. Ask only for a genuinely
  missing decision; do not create unsolicited PRs or sibling-release waiting issues.
- Runtime approval restrictions and repository protections remain independent.
  If rejected, report the exact action/reason, preserve work and continue independent
  tasks. Do not switch execution routes to evade the rejection.

## Ownership and simplification

Prefer deleting, reusing or narrowing an existing mechanism before adding one.
Give each policy and invariant one owner; consumers project its decisions.
Keep independent checks at real trust, corruption and recovery boundaries.
Trace callers before combining flows. Change generated code at its generator.
New modes and abstractions need a demonstrated requirement and simpler-alternative
check. See [ownership details](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#ownership-and-simplification)
when changing component boundaries.

## Complete, reviewable changes

Finish the accepted coherent outcome, including directly required callers,
fixtures, docs and cleanup. Split independent outcomes, not compiler fallout or
each proof of the same change. Stop at new independent scope or a release boundary.
Maintain meaningful [changelog notes](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/changelogs.md) once per completed batch;
governance-only edits need no note unless they change a maintained surface or one
is requested. Report outcome, relevant evidence and actionable blockers.
Detailed line metrics and symbol inventories are optional under the
[reporting rules](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/change-reporting.md), not a routine completion step.
See [contract-change details](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#complete-reviewable-changes)
when changing interfaces, persistence or installation/recovery behavior.

## Validation and evidence

- Ordinary coding and cleanup end after the relevant focused checks pass. Do not
  turn that handoff into release preparation, fleet adoption or qualification of
  every supported host. Those are separately requested workflows; missing broad
  release evidence alone is not unfinished development work.
- **Finish a coherent edit batch, then run its smallest meaningful checks once.**
  During development, run checks when they answer a specific implementation or
  debugging question. Do not test after every edit or rerun merely because it is
  time to hand off. After a failure or further edit, rerun only affected checks.
- Select checks by changed behavior and directly affected callers, including
  relevant rejection/recovery cases. Reuse passing evidence while the relevant
  source, dependencies, configuration and tools remain unchanged. An unrelated
  documentation edit or new Git commit identity alone does not invalidate code
  checks; source-identity-sensitive checks retain their own contract.
- Name the specific package, test, script or contract being checked. A target
  called `clippy`, `check-wasm` or `test` is not focused merely because it is a
  single command. Local command guides must label aggregate targets and provide
  narrower selections. Do not stack check/build/test commands that prove the same
  compilation, or add feature/MSRV/native matrices without a changed obligation.
  Distinct lint, runtime, platform and product proofs still apply when relevant.
- Inspect nested work before calling a fixture focused. Select its affected
  cases rather than an aggregate that also runs unrelated installer, export or
  release suites. If no narrow entrypoint exists, provide one when practical or
  report that limitation; do not silently broaden an ordinary task into the full
  suite. Name the changed behavior each selected check proves. Keep unchanged
  generic regression matrices with their producer.
- Repository tooling uses one exact root Cargo dependency pin inherited as a
  dev-dependency by every real workspace package. `cargo update -v` reports newer
  releases; after changing the pin and Cargo.lock, run `make toko-tooling-update`.
  Offline admission checks every member, Cargo.lock and installed bytes together.
  See the
  [tooling selection rule](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/cargo-dependencies.md#toko-tooling-during-dependency-updates).
- Give repeated validation one execution owner: selected group scope must reach
  the leaf commands, and aggregate gates must not repeat full lint/setup through
  nested targets. Reuse unchanged release-stage evidence only through the
  qualified contract; see [validation mechanics](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#validation-and-evidence).
- Run Gitleaks once per repository CI run, outside host/package/release-group
  matrices. Do not repeat it during deployment or publication; follow the
  [repository secret-scanning contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#repository-secret-scanning).
- Reserve local full repository/workspace suites and broad gates for an explicitly
  authorized release or explicit request for those checks. Implementation,
  continuation, PR delivery and readiness wording do not authorize them. Focused
  passing evidence suffices for ordinary handoff; an unrun full suite alone is
  not a blocker. Preserve approved local restrictions on focused execution.
- Close ordinary issues after implementation and focused validation; pushing,
  publishing and deployment are separate. State the local/delivery status and
  preserve genuinely issue-specific acceptance under the
  [issue closure policy](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/agent-maintenance.md#issue-review-and-repair).
- Run expensive hosted integration/native CI asynchronously for development,
  PR delivery and separately authorized merges. Report pending runs; do not poll
  or hold a turn open solely for the matrix. Enforced checks/reviews, known defects,
  release gates and issue-specific acceptance still apply. Never invent optional
  GitHub approvals or bypass protections. Synchronous qualification is opt-in.
- Toko Tooling owns its general regression matrix. Consumers check snapshot integrity,
  actual caller wiring and their product/native contracts. Do not replay generic
  producer fixtures by default; retain one only for a named missing consumer proof.
  Use [consumer qualification selection](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/docs/consuming-snapshots.md#consumer-qualification-selection)
  before changing existing rosters.
- Documentation-only changes need link, consistency and diff checks, not code
  suites. Inspection-only work does not activate tests. Test maintained behavior,
  typed failures and real boundaries, not removed names or private layout.
- Check for active builds before compilation or edits affecting their inputs;
  preserve unrelated artifacts. Bind evidence to actual inputs and commands;
  distinguish review, substitutes, local execution and live observations. Pending,
  cancelled or failed checks are not passes. Report limitations without inventing
  qualification. Read [validation mechanics](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#validation-and-evidence)
  for release ordering, Cargo preparation and external-effect recovery.

## DFINITY dependencies: own the solution

Do not submit DFINITY PRs or depend on future DFINITY work. Use available qualified
dependencies or own the solution under the [maintenance contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#dfinity-dependencies-own-the-solution).

## Current dependencies and upstream independence

- When adding or updating a direct dependency, select its latest stable,
  non-yanked release, including a newer major version where applicable. Verify
  registry availability at selection time and qualify the affected APIs, targets
  and features. An existing version requirement is not a reason to stop at the
  latest release within an old range. Keep compatible requirements and reviewed
  lockfiles; this rule does not require exact pins or prereleases.
- Select general-purpose dependencies independently of DFINITY's transitive
  selections. Do not downgrade or hold back our `num-bigint`, `sha2` or similar
  dependencies merely to match an upstream version or eliminate duplicates.
  Two versions are acceptable when their types need not interoperate and their
  actual size, build and runtime costs are reasonable for the consuming product.
  A whole-lockfile duplicate count does not establish deployed code size.
- Keep genuinely required shared type identities, runtime singletons, protocol
  bytes and security guarantees intact. If duplication may materially affect a
  product budget, measure the selected target/feature graph and resulting
  artifact; for Wasm, use complete uncompressed and code-section sizes. Prefer
  removing unused edges and narrowing features over holding back independent
  dependencies. Optional upstream edges can still constrain resolution; prove
  independence rather than assuming a disabled feature removes the constraint.
- Retaining an older direct selection is exceptional. Record the package and
  versions, concrete blocker (for example a regression, approved compiler/target
  incompatibility, shared public type or measured budget breach), affected scope,
  maintainer approval and condition for revisiting it in the local overlay or
  its linked policy. Upstream age, deduplication alone or unexamined migration
  effort is not sufficient. An upstream-only older transitive selection needs
  no freshness exception for our independent direct dependency; security
  findings still require remediation.
- Reduce DFINITY coupling incrementally at the existing capability owner as
  affected work permits. Preserve reviewed cryptographic implementations,
  external behavior and provenance; do not bypass the Platform boundary or
  rewrite cryptography to obtain a newer dependency.
- Apply this selection rule during authorized dependency maintenance. It does
  not trigger background fleet updates, unrelated upgrades or republishing on
  every upstream release. Routine validation and deployment use their reviewed
  locked inputs; batch optional refreshes at requested maintenance boundaries.

<a id="repository-files-no-symlinks"></a>

## Repository files: self-contained source and packages

Keep source and distributions self-contained; prefer regular files. Read the
[file and symlink contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#repository-files-no-symlinks)
for justified internal links, packaging and filesystem security boundaries.

## CLI applications

Use Clap for as much Rust CLI behavior as its supported APIs can own, including
typed values, validation, argument relationships, defaults, help and diagnostics.
Check its built-ins and extension points before writing custom CLI mechanics.
Read the [CLI contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#cli-applications) when adding,
changing or reviewing commands.

## Metrics and logging

**`ic-metrics` is only for operational instrumentation of the Canic application
tree, reported through the shared endpoint owned by Canic's metrics adapter.**
It is not a general math, statistics or validation library; do not depend on it
or re-export its helpers for those purposes. Use `ic-logging` for logging.
Read the
[observability contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#metrics-and-logging)
when adding, changing or reviewing metrics and logging.

## IC application encoding

Hashing choices follow the [shared algorithm/purpose inventory](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/hashing.md).
Review new algorithms and direct hashing packages explicitly; pure hashing may
use approved direct dependencies without a Platform facade. Preserve existing
hash bytes and domains when changing dependency ownership.

Owned application types must not derive or implement `CandidType`. Use `ic-wire`
for owned RPC; keep unavoidable Candid in dedicated external-protocol boundary
types, not our domain, state or RPC models. CBOR belongs to Platform's typed
external-protocol APIs, including browser and test fixtures. Replace owned CBOR
records and hash inputs with Wire through an explicit format/identity migration.
Read the [encoding contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#ic-application-encoding)
and [RPC budgets](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/rpc-budgets.md) for codecs, bindings or protocol changes.

## Wasm optimization

Toko Tooling owns optimization selection and evidence rules; IC Tooling's
`ic-build` owns reusable build/optimizer mechanics, recommended recipes and their
qualification evidence. Consumers select qualified recipes against their product
budgets. Wasm size comparisons report both the complete uncompressed module and
its code-section size. Gzip/compressed size is not a Wasm size metric and must not
appear in those summaries unless transport/compression measurement is explicitly
requested. Read the [Wasm optimization contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#wasm-optimization)
before comparing Wasm sizes or changing deployment profiles, optimizer settings
or performance matrices.

## Gzip encoding

IC Tooling's `ic-host-artifacts::artifact::encode_gzip` owns gzip encoding.
Production code, tests, examples and generators reuse it; consumers retain
compression levels, budgets and artifact admission. Read the
[gzip contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#gzip-encoding) before changing
encoders or compressed-byte identities.

## Pre-1.0 contracts

Keep one current repository-owned contract, without compatibility shims or parallel
versions. Breaking public changes require a minor release; retained data and
external obligations remain real. Read [the hard-cut contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#pre-10-contracts)
before interface or storage changes.

## Release commands

`make release-patch`, `make release-minor` and `make release-major` share the
[release contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#release-commands). Read it and the
local adapter before authorized release work. Normal releases start fresh;
specific-attempt recovery uses `release-resume`. Preserve complete qualification
of the selected release. Independent groups may adopt an explicitly qualified
[group validation plan](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/docs/release-group-validation.md); until adoption, keep
the existing full gate. Repository-wide CI remains separate and unchanged.

## Changelogs and artifact preservation

Keep decisions and compact results in Git, not raw experiment archives. Routine
checks need no new evidence report. Experimental evidence defaults to one current
summary and at most two small data files per topic, 100 KiB combined, with a 5 MiB
repository budget; raw output belongs under `target/evidence/`. See the
[retention contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#changelogs-and-artifact-preservation)
for exceptions, historical cleanup and protected release/recovery records.

Use IcyDB-style emoji category sections in the owning release changelog; read the
[changelog rules](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/changelogs.md) when updating them. Every issue fixed must
have a direct link in that changelog before handoff: root `CHANGELOG.md` for a
workspace without independent release groups, or `releases/<group>/CHANGELOG.md`
for the owning group. A grouped workspace's root remains a navigation index;
it does not need duplicate issue links. Preserve selected
deliverables and required provenance/recovery evidence. Regenerable tool-build
scratch and obsolete cache generations need bounded retention; they are not
permanent evidence. Consumer release artifacts remain protected from implicit
cleanup. See
[artifact preservation](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#changelogs-and-artifact-preservation).
Keep plain `cargo clean` able to remove the owning workspace's entire `target/`
directory. Put workspace build/test scratch and generated fixtures beneath
`target/`; use `.tmp/` or other outside-target scratch only for a documented
technical need. Artifact retention must not narrow explicit cleanup.
Keep `.git` free of build trees and bulk retired-checkout archives. Git-side
tool state needs explicit size/retention limits under the artifact contract;
preserving evidence must not silently preserve every compiler output.

## IC stable-memory governance

Production stable stores use `ic-storage`-issued checked allocation handles. Read the
[memory contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#ic-stable-memory-governance) for
storage allocation, admission, backing access or persistence integrations.

## Rust workspaces and portable tooling

Keep Rust tests and test-only helpers in `tests.rs`, `*_tests.rs` or `tests/`;
declare fixture-only package roles explicitly. Adopt the naming layout as a
hard cut, including references, under the
[test layout rule](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/docs/principles/rust-code-hygiene.md#tests).

Use ordinary Rust module discovery. `#[path]` module attributes, including
conditional path overrides, are prohibited. Do not inject embedded Rust source
or patch generated entrypoints to change executable code, including in tests.
Follow the
[module layout rule](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/docs/principles/rust-code-hygiene.md#ownership-and-module-shape)
for source, tests and generated code.

Prefer `#[expect(..., reason = "...")]` over `#[allow(...)]` for Rust lint
suppressions. Repositories must review every `allow` for conversion and retain
one only with a concrete reason why an expectation cannot work. Follow the
[lint suppression rule](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/docs/principles/rust-code-hygiene.md#lint-suppressions).

### Product and qualification workspaces

Use one repository-root product workspace, with product packages under `crates/`
and Canic fleet components in `apps/<fleet-name>/`. Meaningful grouping such as
`crates/<domain>/<package-name>/` is supported, including Toko's grouped packages;
packages need not be immediate children. Grouping does not create another
workspace, dependency catalog or release group. Declare actual member paths
explicitly and discover packages through Cargo metadata, not a `crates/*` scan.
Ordinary unit tests, package integration tests and examples stay beside their
owning product code. Separate packages used only for qualification, benchmarks,
fuzzing, test fixtures or test support live under `qualification/`, with
`qualification/Cargo.toml` and its own `Cargo.lock`. Meaningful grouping such as
`qualification/<domain>/<package-name>/` is supported too. Do not add a redundant
`qualification/crates/` level or create an empty qualification
workspace. This is the standard whenever separate qualification packages exist;
it does not require first demonstrating a dependency conflict.

Classify canisters by purpose: standalone product canisters belong under
`crates/`, product fleet components under `apps/`, and separate test, audit or
benchmark canisters under `qualification/`. Do not introduce a parallel top-level
`canisters/` tree. A test fleet's configuration may reference qualification
packages without moving them into the product workspace. Small non-package
fixtures remain beside their owning tests.

Each workspace owns its catalog, lockfile, profiles and member metadata. The
product root excludes `qualification`; qualification consumes product packages,
never the reverse. Reusable shipped testing tools are products, not automatically
qualification packages. Classify by actual responsibility, not publication flags
or names. Keep core tests independent of downstream frameworks.

Release groups contain only product-workspace packages. Qualification packages
are unpublished, belong to no product release group and do not inherit its
version. A group's validation plan explicitly selects the qualification checks
it needs. Required checks remain required even though their packages are outside
the group; no duplicate group or release ledger is created for a test harness.
Changes to product references and qualification locks must be admitted and
updated atomically with affected release metadata, preserving unrelated supplier
selections and private versions. Toko Tooling owns reusable multi-lock mechanics;
consumers own the validation mapping and semantic acceptance.

Apply this standard across repositories through scoped adoption work, preserving
package identities, coverage and release history. Previous general permission to
keep qualification packages in product groups or a nested `crates/` tree does not
exempt a migration. Policy adoption does not itself move sibling files or publish
packages. This section supersedes conflicting layout/group wording in the older
revision-bound detailed references below; it specifies policy, not a claim that
every required multi-lock helper is already implemented.

Keep related product crates in that development workspace, with coordinated
versions within explicit release groups where contracts need independent
releases. Independent product groups all use explicit package versions and
`releases/<group>/CHANGELOG.md`; the root has no shared version and its changelog
is an index. Require explicit group selection for releases and publication.
Show every product member's path and every publishable member's version in root
`Cargo.toml` dependency declarations. Private product members may use path-only
catalog entries; qualification members have their own workspace catalog.
Group them once at the top of `[workspace.dependencies]`, before all external
dependencies, with short titles and separate changelog comments, under the
[workspace versioning rules](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/rust-workspaces.md#coordinated-versions-and-workspace-boundaries).
Dependency adoption must distinguish required compatibility/security changes from
optional refreshes and check the destination graph before planning downstream
releases. A newer external patch alone does not require republishing consumers.
Defer optional dependency and snapshot refreshes by default, batching them at a
natural release boundary when requested. Adopt Toko Tooling for a needed fix,
capability or explicit refresh; an intact older snapshot alone is not work to do.
Required API, security or compatibility migrations qualify the affected consumer
boundary, rather than automatically updating the whole fleet.
Follow the [adoption planning contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/docs/cargo-graph-checks.md#dependency-adoption-planning).
Use root-owned dependency selections, reviewed toko tooling and explicit setup.
Rust 1.99.1 is the approved minimum and pinned development/release compiler;
follow the [compiler support policy](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/cargo-dependencies.md#minimum-supported-rust-version-msrv)
when adopting it and consolidating duplicate compiler checks. Preserve product
host requirements. Select canonical identities
from `ic-platform-types`, IC0/execution from `ic-platform-runtime`, and crypto or
external IC adapters from `ic-platform`, following the
[Platform ownership rules](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/cargo-dependencies.md#platform-package-owners).
External IC dependencies remain behind that reviewed boundary; Types alone may
add `ic_principal` for explicit registry conversions. Self-authentication does
not enable those conversions. npm SDK Core uses
`@tokostudios/ic-platform`. Keep reusable cores independently upgradable;
adapters own their matching integration stacks and must not impose consumer
framework dependencies on core development. Follow the
[core and integration graph boundary](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/rust-workspaces.md#core-and-integration-graph-boundary).
Do not add Python tooling. Read the applicable
[workspace/tooling contracts](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#rust-workspaces-and-portable-tooling)
for manifests, dependencies, hooks, installers or shared snapshots, and the
[Rust hygiene guide](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/docs/principles/rust-code-hygiene.md) for Rust implementation.

## Host support

All packages support their applicable workflows on macOS. Read the [host contract](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#host-support)
when changing platform behavior or CI. Routine CI cancels superseded runs while
preserving required hosts on the retained run.

## Feedback and handoff

Shared dependency defects and advisories have one remediation issue at the
supplying owner. Do not create a waiting issue in every consumer; separate
consumer issues need distinct actionable work. Apply the
[dependency finding rule](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#shared-dependency-findings).

Use owning GitHub issues for follow-up; no parallel local trackers. CI/issue
maintenance and scheduled work run only when activated. Read [feedback details](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#feedback-and-handoff)
when reporting findings across repositories or maintaining issues.
Use the [shared audit methods](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/audits/README.md) for selected code/structural
audits; adoption activates neither an audit schedule nor repair authority.

### Repository handoff files

Keep the current handoff a replacement summary of accepted scope, active blockers,
next actions and latest relevant evidence. Aim for 50–100 lines; trim above 150
on the next update while preserving unresolved safety/recovery facts. Link
canonical evidence instead of copying logs, test inventories or final reports.
Read [handoff retention details](https://github.com/dragginzgame/toko-tooling/blob/626da7cd9626cc2e152594862a2d82ea82ef51da/rules/engineering-details.md#repository-handoff-files)
when trimming historical content. Do not create a handoff file just for ceremony.
