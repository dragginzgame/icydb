# Dragginzgame Engineering Baseline
<!-- Shared companions: rules/engineering-details.md rules/rpc-budgets.md -->

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

- Preserve unrelated dirty work and reread dirty files before editing. Task-relevant
  sibling inspection needs no additional permission. Never edit another
  contributor's sibling working checkout in place.
- Ordinary fixes authorize local implementation and focused checks. Leave staging,
  commits, own release versions, tags and release execution to the user unless
  delegated. A PR request includes its branch, scoped commits, branch push and PR.
- Relevant cross-repository contributions in `dragginzgame/*` have standing
  isolated-PR authority. Follow the [contribution workflow](rules/contributions.md#cross-repository-pull-requests)
  before external writes. `toko` and `toko-miner` remain excluded without explicit
  authorization; DFINITY contributions remain excluded. Merges, integration-branch
  pushes, history rewrites, releases, publication, deployment and paid effects
  retain their separate authorization and enforced protections.
- Carry established authorization forward. Do not ask again for necessary
  substeps or stop an accepted coherent batch after each small slice. Inspection
  alone does not authorize repair. Resolve actual scope or effect ambiguity,
  not routine implementation choices. Read [scope details](rules/engineering-details.md#scope-and-authorization)
  for issue work or other external effects.
- Runtime approval restrictions remain independent. If an action is rejected,
  retain prepared work, report the exact action and reason, and continue
  independent work. Do not switch execution paths to evade a rejection or wait
  on optional CI merely to outlast it.

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

Use Clap for Rust CLI parsing. Read the [CLI contract](rules/engineering-details.md#cli-applications)
when adding or changing commands.

## IC application encoding

Owned application types must not derive or implement `CandidType`. Use `ic-wire`
for owned RPC; keep unavoidable Candid in dedicated external-protocol boundary
types, not our domain, state or RPC models. Read the [encoding contract](rules/engineering-details.md#ic-application-encoding)
and [RPC budgets](rules/rpc-budgets.md) for codecs, bindings or protocol changes.

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
specific-attempt recovery uses `release-resume`. Preserve required full validation.

## Changelogs and artifact preservation

Use IcyDB-style emoji category sections in root release notes; read the
[changelog rules](rules/changelogs.md) when updating them. Preserve build and
recovery evidence on success, failure and retry; no implicit cleanup. See
[artifact preservation](rules/engineering-details.md#changelogs-and-artifact-preservation).

## IC stable-memory governance

Production stable stores use `ic-memory`-issued checked handles. Read the
[memory contract](rules/engineering-details.md#ic-stable-memory-governance) for
storage allocation, admission, backing access or persistence integrations.

## Rust workspaces and portable tooling

Use root-owned workspace dependencies and versions, reviewed shared tooling,
explicit setup and the product's declared compiler/host requirements. External
IC dependencies go through `ic-platform`; npm SDK Core uses
`@tokostudios/ic-platform`. Do not add Python tooling. Read the applicable
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
