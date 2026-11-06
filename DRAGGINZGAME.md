# Dragginzgame Engineering Baseline
<!-- Shared companions: audits/README.md docs/principles/rust-code-hygiene.md rules/change-reporting.md rules/contributions.md rules/engineering-details.md rules/rpc-budgets.md rules/rust-workspaces.md -->

Shared Tooling owns the common engineering rules for `dragginzgame` repositories.
Read this core, the local `AGENTS.md` and the current handoff when present. Read
linked specialist sections only when the task touches their subject, before the
affected action. Do not recursively read every linked guide at startup or reread
unchanged instructions during the same task. Local overlays should follow the
same approach: mandatory local constraints first, task-specific references after.

## Adoption and local authority

- Apply the repository's reviewed snapshot or revision-bound baseline, never a
  moving sibling checkout. Keep snapshot-owned files intact; repair at Shared
  Tooling and adopt a reviewed committed revision through the exporter.
- Consumers own architecture, product limits, local commands and release inputs.
  Shared owns common engineering practice and fleet audits. Local exceptions to
  common rules need maintainer approval with scope and reason. Preserve explicit
  exclusions and approved product restrictions.
- During adoption, remove redundant or conflicting overlay wording; retain only
  local choices and documented exceptions. Do not turn a general prohibition
  into a new permission prompt when the common rule already authorizes the task.
  See [adoption details](rules/engineering-details.md#adoption-and-local-authority).

## Scope and authorization

The [contribution rules](rules/contributions.md) own detailed action authority;
local overlays must not duplicate it or add routine permission gates.

- Prefer direct edits in the normal authorized checkout when a PR is optional.
  Use branches there when needed; coordinate or wait when another session owns
  the inputs. Separate checkouts are explicit exceptions, not the default for
  isolation, PRs or deployment.
  Ordinary fixes include focused checks and pending notes, not staging, commits
  or delivery. Shared alone also prepares its pending `VERSION`.
- A PR request includes its topic branch, scoped commits, branch push and PR.
  Release/ship/deploy requests include their necessary delivery steps and direct
  integration-branch pushes. An unqualified “deploy” covers all changed eligible
  Dragginzgame repositories in the established projects directory and crates.io
  publication for affected publishable groups. Announce the selected plan, then
  proceed under [delegated delivery](rules/contributions.md#delegated-delivery).
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
  Follow the [checkout contract](rules/contributions.md#durable-work-and-checkout-location).
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
check. See [ownership details](rules/engineering-details.md#ownership-and-simplification)
when changing component boundaries.

## Complete, reviewable changes

Finish the accepted coherent outcome, including directly required callers,
fixtures, docs and cleanup. Split independent outcomes, not compiler fallout or
each proof of the same change. Stop at new independent scope or a release boundary.
Maintain meaningful [changelog notes](rules/changelogs.md) once per completed batch;
governance-only edits need no note unless they change a maintained surface or one
is requested. Report outcome, relevant evidence and actionable blockers.
Detailed line metrics and symbol inventories are optional under the
[reporting rules](rules/change-reporting.md), not a routine completion step.
See [contract-change details](rules/engineering-details.md#complete-reviewable-changes)
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
- Give repeated validation one execution owner: selected group scope must reach
  the leaf commands, and aggregate gates must not repeat full lint/setup through
  nested targets. Reuse unchanged release-stage evidence only through the
  qualified contract; see [validation mechanics](rules/engineering-details.md#validation-and-evidence).
- Reserve local full repository/workspace suites and broad gates for an explicitly
  authorized release or explicit request for those checks. Implementation,
  continuation, PR delivery and readiness wording do not authorize them. Focused
  passing evidence suffices for ordinary handoff; an unrun full suite alone is
  not a blocker. Preserve approved local restrictions on focused execution.
- Run expensive hosted integration/native CI asynchronously for development,
  PR delivery and separately authorized merges. Report pending runs; do not poll
  or hold a turn open solely for the matrix. Enforced checks/reviews, known defects,
  release gates and issue-specific acceptance still apply. Never invent optional
  GitHub approvals or bypass protections. Synchronous qualification is opt-in.
- Shared owns its general regression matrix. Consumers check snapshot integrity,
  actual caller wiring and their product/native contracts. Do not replay generic
  producer fixtures by default; retain one only for a named missing consumer proof.
  Use [consumer qualification selection](docs/consuming-snapshots.md#consumer-qualification-selection)
  before changing existing rosters.
- Documentation-only changes need link, consistency and diff checks, not code
  suites. Inspection-only work does not activate tests. Test maintained behavior,
  typed failures and real boundaries, not removed names or private layout.
- Check for active builds before compilation or edits affecting their inputs;
  preserve unrelated artifacts. Bind evidence to actual inputs and commands;
  distinguish review, substitutes, local execution and live observations. Pending,
  cancelled or failed checks are not passes. Report limitations without inventing
  qualification. Read [validation mechanics](rules/engineering-details.md#validation-and-evidence)
  for release ordering, Cargo preparation and external-effect recovery.

## DFINITY dependencies: own the solution

Do not submit DFINITY PRs or depend on future DFINITY work. Use available qualified
dependencies or own the solution under the [maintenance contract](rules/engineering-details.md#dfinity-dependencies-own-the-solution).

<a id="repository-files-no-symlinks"></a>

## Repository files: self-contained source and packages

Keep source and distributions self-contained; prefer regular files. Read the
[file and symlink contract](rules/engineering-details.md#repository-files-no-symlinks)
for justified internal links, packaging and filesystem security boundaries.

## CLI applications

Use Clap for as much Rust CLI behavior as its supported APIs can own, including
typed values, validation, argument relationships, defaults, help and diagnostics.
Check its built-ins and extension points before writing custom CLI mechanics.
Read the [CLI contract](rules/engineering-details.md#cli-applications) when adding,
changing or reviewing commands.

## Metrics and logging

**`ic-metrics` is only for operational instrumentation of the Canic application
tree, reported through the shared endpoint owned by Canic's metrics adapter.**
It is not a general math, statistics or validation library; do not depend on it
or re-export its helpers for those purposes. Use `ic-logging` for logging.
Read the
[observability contract](rules/engineering-details.md#metrics-and-logging)
when adding, changing or reviewing metrics and logging.

## IC application encoding

Owned application types must not derive or implement `CandidType`. Use `ic-wire`
for owned RPC; keep unavoidable Candid in dedicated external-protocol boundary
types, not our domain, state or RPC models. CBOR belongs to Platform's typed
external-protocol APIs, including browser and test fixtures. Replace owned CBOR
records and hash inputs with Wire through an explicit format/identity migration.
Read the [encoding contract](rules/engineering-details.md#ic-application-encoding)
and [RPC budgets](rules/rpc-budgets.md) for codecs, bindings or protocol changes.

## Wasm optimization

Shared Tooling owns optimization selection and evidence rules; IC Tooling's
`ic-build` owns reusable build/optimizer mechanics, recommended recipes and their
qualification evidence. Consumers select qualified recipes against their product
budgets. Wasm size comparisons report both the complete uncompressed module and
its code-section size. Gzip/compressed size is not a Wasm size metric and must not
appear in those summaries unless transport/compression measurement is explicitly
requested. Read the [Wasm optimization contract](rules/engineering-details.md#wasm-optimization)
before comparing Wasm sizes or changing deployment profiles, optimizer settings
or performance matrices.

## Gzip encoding

IC Tooling's `ic-host-artifacts::artifact::encode_gzip` owns gzip encoding.
Production code, tests, examples and generators reuse it; consumers retain
compression levels, budgets and artifact admission. Read the
[gzip contract](rules/engineering-details.md#gzip-encoding) before changing
encoders or compressed-byte identities.

## Pre-1.0 contracts

Keep one current repository-owned contract, without compatibility shims or parallel
versions. Breaking public changes require a minor release; retained data and
external obligations remain real. Read [the hard-cut contract](rules/engineering-details.md#pre-10-contracts)
before interface or storage changes.

## Release commands

`make release-patch`, `make release-minor` and `make release-major` share the
[release contract](rules/engineering-details.md#release-commands). Read it and the
local adapter before authorized release work. Normal releases start fresh;
specific-attempt recovery uses `release-resume`. Preserve complete qualification
of the selected release. Independent groups may adopt an explicitly qualified
[group validation plan](docs/release-group-validation.md); until adoption, keep
the existing full gate. Repository-wide CI remains separate and unchanged.

## Changelogs and artifact preservation

Keep decisions and compact results in Git, not raw experiment archives. Routine
checks need no new evidence report. Experimental evidence defaults to one current
summary and at most two small data files per topic, 100 KiB combined, with a 5 MiB
repository budget; raw output belongs under `target/evidence/`. See the
[retention contract](rules/engineering-details.md#changelogs-and-artifact-preservation)
for exceptions, historical cleanup and protected release/recovery records.

Use IcyDB-style emoji category sections in the owning release changelog; read the
[changelog rules](rules/changelogs.md) when updating them. Every issue fixed must
have a direct link in that changelog before handoff: root `CHANGELOG.md` for a
workspace without independent release groups, or `releases/<group>/CHANGELOG.md`
for the owning group. A grouped workspace's root remains a navigation index;
it does not need duplicate issue links. Preserve selected
deliverables and required provenance/recovery evidence. Regenerable tool-build
scratch and obsolete cache generations need bounded retention; they are not
permanent evidence. Consumer release artifacts remain protected from implicit
cleanup. See
[artifact preservation](rules/engineering-details.md#changelogs-and-artifact-preservation).
Keep plain `cargo clean` able to remove the owning workspace's entire `target/`
directory. Put workspace build/test scratch and generated fixtures beneath
`target/`; use `.tmp/` or other outside-target scratch only for a documented
technical need. Artifact retention must not narrow explicit cleanup.
Keep `.git` free of build trees and bulk retired-checkout archives. Git-side
tool state needs explicit size/retention limits under the artifact contract;
preserving evidence must not silently preserve every compiler output.

## IC stable-memory governance

Production stable stores use `ic-storage`-issued checked allocation handles. Read the
[memory contract](rules/engineering-details.md#ic-stable-memory-governance) for
storage allocation, admission, backing access or persistence integrations.

## Rust workspaces and portable tooling

Keep Rust tests and test-only helpers in `tests.rs`, `*_tests.rs` or `tests/`;
declare fixture-only package roles explicitly. Adopt the naming layout as a
hard cut, including references, under the
[test layout rule](docs/principles/rust-code-hygiene.md#tests).

Use ordinary Rust module discovery. `#[path]` module attributes, including
conditional path overrides, are prohibited. Do not inject embedded Rust source
or patch generated entrypoints to change executable code, including in tests.
Follow the
[module layout rule](docs/principles/rust-code-hygiene.md#ownership-and-module-shape)
for source, tests and generated code.

Prefer `#[expect(..., reason = "...")]` over `#[allow(...)]` for Rust lint
suppressions. Repositories must review every `allow` for conversion and retain
one only with a concrete reason why an expectation cannot work. Follow the
[lint suppression rule](docs/principles/rust-code-hygiene.md#lint-suppressions).

Keep related crates in one development workspace, with coordinated versions
within explicit release groups where unrelated contracts need independent
releases. Independent groups all use explicit package versions and
`releases/<group>/CHANGELOG.md`; the root has no shared version and its changelog
is an index. Require explicit group selection for releases and publication.
Show every member's path and every publishable member's version in root
`Cargo.toml` dependency declarations. Private dev helpers may use path-only entries.
Group them once at the top of `[workspace.dependencies]`, before all external
dependencies, with short titles and separate changelog comments, under the
[workspace versioning rules](rules/rust-workspaces.md#coordinated-versions-and-workspace-boundaries).
Dependency adoption must distinguish required compatibility/security changes from
optional refreshes and check the destination graph before planning downstream
releases. A newer external patch alone does not require republishing consumers.
Defer optional dependency and snapshot refreshes by default, batching them at a
natural release boundary when requested. Adopt Shared Tooling for a needed fix,
capability or explicit refresh; an intact older snapshot alone is not work to do.
Required API, security or compatibility migrations qualify the affected consumer
boundary, rather than automatically updating the whole fleet.
Follow the [adoption planning contract](docs/cargo-graph-checks.md#dependency-adoption-planning).
Use root-owned dependency selections, reviewed shared tooling and explicit setup.
Rust 1.99.1 is the approved minimum and pinned development/release compiler;
follow the [compiler support policy](rules/cargo-dependencies.md#minimum-supported-rust-version-msrv)
when adopting it and consolidating duplicate compiler checks. Preserve product
host requirements. Select canonical identities
from `ic-platform-types`, IC0/execution from `ic-platform-runtime`, and crypto or
external IC adapters from `ic-platform`, following the
[Platform ownership rules](rules/cargo-dependencies.md#platform-package-owners).
External IC dependencies remain behind that reviewed boundary; Types alone may
add `ic_principal` for explicit registry conversions. Self-authentication does
not enable those conversions. npm SDK Core uses
`@tokostudios/ic-platform`. Keep reusable cores independently upgradable;
adapters own their matching integration stacks and must not impose consumer
framework dependencies on core development. Follow the
[core and integration graph boundary](rules/rust-workspaces.md#core-and-integration-graph-boundary).
Do not add Python tooling. Read the applicable
[workspace/tooling contracts](rules/engineering-details.md#rust-workspaces-and-portable-tooling)
for manifests, dependencies, hooks, installers or shared snapshots, and the
[Rust hygiene guide](docs/principles/rust-code-hygiene.md) for Rust implementation.

## Host support

All packages support their applicable workflows on macOS. Read the [host contract](rules/engineering-details.md#host-support)
when changing platform behavior or CI. Routine CI cancels superseded runs while
preserving required hosts on the retained run.

## Feedback and handoff

Shared dependency defects and advisories have one remediation issue at the
supplying owner. Do not create a waiting issue in every consumer; separate
consumer issues need distinct actionable work. Apply the
[dependency finding rule](rules/engineering-details.md#shared-dependency-findings).

Use owning GitHub issues for follow-up; no parallel local trackers. CI/issue
maintenance and scheduled work run only when activated. Read [feedback details](rules/engineering-details.md#feedback-and-handoff)
when reporting findings across repositories or maintaining issues.
Use the [shared audit methods](audits/README.md) for selected code/structural
audits; adoption activates neither an audit schedule nor repair authority.

### Repository handoff files

Keep the current handoff a replacement summary of accepted scope, active blockers,
next actions and latest relevant evidence. Aim for 50–100 lines; trim above 150
on the next update while preserving unresolved safety/recovery facts. Link
canonical evidence instead of copying logs, test inventories or final reports.
Read [handoff retention details](rules/engineering-details.md#repository-handoff-files)
when trimming historical content. Do not create a handoff file just for ceremony.
