# Application Migration Rehearsal

Status: idea intake only; no implementation or release authority

Recorded: 2026-09-06

## Problem And Candidate Outcome

Application authors need evidence that a proposed migration can validate,
rewrite, recover, and finish against representative populated data before
upgrading their deployment. Knowing that a migration compiles or that the
engine's generic fixtures pass does not establish that an application's rows
satisfy the successor schema.

The candidate is a repeatable local rehearsal using the application's exact
predecessor and successor Wasms, a defined data fixture, and existing migration
commands. Its output is a report of observed behavior and costs. It does not
introduce a production dry-run mode or a second migration lifecycle.

## Maintained Starting Point

- The [migration guide](../../guides/schema-migrations.md) describes exact
  plan/head checks, bounded advancement, publication, recovery, and abort limits.
- The [migration command contract](../../../crates/icydb-core/src/db/schema/migration_api.rs)
  owns typed operations, status, findings, and terminal receipts.
- The [PocketIC migration closeout fixture](../../../testing/integration/tests/schema_migration_closeout.rs)
  already installs predecessor Wasm, loads rows, upgrades to a successor,
  advances migration, interrupts it with another upgrade, and verifies results.

First audit what this harness can already do for a real application. A focused
application fixture and documented invocation may be sufficient; a new CLI
command, configuration language, or generic test framework is not assumed.

## Smallest Useful Rehearsal

1. Freeze the application revision, Wasm hashes, dependency/toolchain inputs,
   migration plan identity, and fixture seed/content identity.
2. Install the predecessor into an isolated local canister and populate it
   through maintained application/write admission. Confirm the starting
   accepted head and representative read, index, and relation results.
3. Upgrade to the exact successor and use the existing status/advance operations
   with bounded host-side stopping limits. Record typed findings and phase work.
4. Interrupt a selected durable phase and resume with the exact successor.
   Compare final results with an uninterrupted control; prove pre-rewrite abort
   separately when it is relevant to the application transition.
5. Verify the accepted target head, terminal receipt, transformed values,
   identities, indexed queries, and relations using application expectations.
   A terminal phase alone is not a sufficient data-correctness assertion.

Begin with one actual migration and its meaningful success and rejection cases.
Do not create a combinatorial phase/fault matrix before identifying the gaps in
existing recovery coverage. A host timeout or step-limit stop must be reported
as incomplete, not as a successful migration or evidence of engine corruption.

## Data And Evidence Contract

The first fixture should cover the application's relevant edge values, row
sizes, index fanout, and relation shapes. Record its construction and limits.
Synthetic or sampled rows cannot prove that every production row will pass.

No supported production backup/import path is assumed. If exact deployed data
is necessary, first establish an authorized and supported acquisition/restore
workflow. That is a separate prerequisite; this idea does not add a raw-byte
importer or an old-format decoder to obtain a more realistic fixture.

The report should contain:

- Exact artifacts, plan/head identities, fixture coverage, and final outcome.
- Bounded typed validation findings and the application checks performed.
- Advancement counts, validated/rewritten rows, index work, and observed
  per-message instructions where the harness supports them.
- Database gating and recovery observations, including the tested interruption.
- Raw Wasm sizes and measured memory growth where available, with unsupported
  measurements explicitly marked as such.

Local elapsed time and message counts are observations, not a guaranteed
production downtime estimate. Keep data content out of reports unless needed
and authorized; row identities and typed findings should usually suffice.

## Ownership, Alternative, And Complexity

Application code owns fixture data and expected business results. Existing
IcyDB migration and accepted-schema owners retain all admission, rewriting,
publication, and recovery semantics. PocketIC and the host harness own isolated
execution and evidence collection.

The simplest alternative is an application integration test following the
maintained fixture. Prefer that if it provides a reproducible rehearsal without
repeated infrastructure code. Any tooling extraction must remove demonstrated
duplication from those tests.

Expected state-space delta: host-only orchestration and report data; no new
production endpoint, persisted phase, runtime mode, or migration format.
Source schema versions may change through maintained migration semantics;
internal format discriminators remain version 1 under the pre-1.0 hard cut.
An incompatible format transition is not made upgradeable by this harness.

## Promotion Gate

Select one real application migration, audit harness reuse, and demonstrate the
remaining manual or missing steps. Define the smallest complete rehearsal and
its acceptance evidence before proposing an explicitly authorized design.
Report files/lines and tooling complexity; if production wiring is proposed,
justify it separately and measure its runtime/Wasm cost. No minor line, public
syntax, deployment, or production data access is authorized by this note.
