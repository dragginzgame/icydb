# Complexity And Technical Debt — Cleanup Rerun

## Preamble And Comparability

- Method: `CTD-1.0`; run `2026-10-01/01`; auditor: Codex.
- Definition: [CTD-1.0 definition](../../../../../../../audits/recurring/crosscutting/crosscutting-complexity-and-technical-debt.md).
- Trigger: user-requested rerun of the conversation's complexity inspection
  after all four approved findings were implemented as A6.
- Snapshot: HEAD `bdec3835dc0d9b775413999cab4715ebcedcd150` plus the existing dirty worktree.
- Tracked diff SHA-256 at entry:
  `5c12e797ba6909447bec83d444db5a2b50301088d571864c1817c8fd0ac40363`.
- [Structured findings](findings.json) retain hashes of the 14 selected source files.
- Scope: A6 codecs, macro path resolution, exact SQL aggregate selection and
  UPDATE diagnostics; adjacent catalog/SQL introspection, budget profiles and
  exact metadata folding. Duplicate screening covered production Rust under
  core, facade, model and model-macros; manual conclusions apply only to the
  selected owners, not every screened symbol.
- Pre-existing changes include A4/A5 storage/aggregate work, other 0.264 schema
  cleanup, measurement fixtures and release/status documentation. They remain
  intact. This run writes only its report and findings; it does not fix findings,
  update a tracker/ledger, or modify release metadata.
- Baseline: the earlier conversation inspection and [A6 handoff](../../../../../../../design/0.264-signed-index-admission/0.264-status.md) provide the four
  stable cleanup anchors. Comparison to [2026-08-26 CTD report](../../../../08/26/complexity-and-technical-debt/01/report.md) is `N/A (scope change)`;
  the older report is not a current backlog. [2026-10-01 flow audit](../../flow-convergence-and-duplication/01/report.md) is supporting historical
  evidence, not fresh validation of this snapshot.

## Verdict

**PASS WITH FINDINGS**, limited to the inspected owners.

All four A6 corrections remain present and retain their maintained boundary
contracts. Two `LOW` findings remain in metadata introspection. Neither is an
observed correctness defect or a scoped release blocker. No new framework,
execution mode, configuration, cache or persisted representation is warranted.

## State-Space Map

| Axis | Maintained values | Canonical owner | Interactions and rejection boundary |
| --- | --- | --- | --- |
| Accepted field storage | by-kind scalar/structural payload or catalog value | accepted field/row persistence contracts | accepted kind, nullability and recursive bounds decide the codec; composite/enum by-kind decode still rejects |
| Generated dependency path | automatic runtime path; model path from direct model, facade or explicit override | macro crate-path resolver; separate actor resolver | package dependencies and generated token targets; unresolved paths fail during generation |
| Global aggregate cached plan | entity count, leading distinct, leading numeric fold, leading range count, prefix counts or prepared plan | one compiled aggregate cache entry | accepted fingerprint and metadata generation; mutually exclusive payloads and one prepared fallback |
| UPDATE execution contract | public primary-key, public bounded, trusted exact or resumable | update policy and validated plan types | shared rejection diagnostics; execution adapters still reject incompatible plan/lane inputs |
| Metadata response | compact/verbose describe, constraints, columns or relations | schema descriptions and session projection | same accepted catalog; different output subsets, no second schema authority |
| Budget scope/profile | aggregate request, per-read execution, mutation execution | request scope and executor budget owners | distinct counters, operation ceilings and failure reserves; shared numeric tails do not make profiles interchangeable |

A6 added no values to these axes. Removed private helpers/options were not
maintained product modes or format versions.

## Decision And Ownership Spread

| Decision | Owner | Semantic consumers | Plumbing consumers | Inspected switch/assembly spread |
| --- | --- | --- | --- | --- |
| Leaf payload support | leaf codecs and accepted dispatch | encode/decode/validate | accepted row adapters | separate operation dispatches protect their own contracts |
| Exact shortcut eligibility | exact aggregate resolver and command facts | global aggregate execution | compiled cache and result projection | one resolver gate after cache lookup; removed wrapper no longer repeats it |
| UPDATE diagnostic code | SqlUpdatePolicyRejection | exact and resumable adapters | QueryError projection | one common mapping; two distinct incompatible-input checks |
| Verbose entity metadata assembly | session catalog | public describe and verbose SQL describe | SQL result envelope | identical accepted catalog metadata assembled at two sites |
| Constraint-only output | schema constraint description helper | full entity description; indirect SQL constraint output | SHOW CONSTRAINTS envelope | SQL constructs a full description then copies out constraints |

These counts describe the inspected roles, not a complexity score. Similar
codec match lists and UPDATE adapter checks enforce different obligations.

## Extension Rehearsals

1. **Add an accepted description metadata member.** Its semantic owner belongs
   to accepted catalog/schema descriptions. Today the public and verbose SQL
   adapters both wire entity tag, fingerprint, identity and validation jobs.
   Reusing the existing catalog assembler removes that repeated coordination
   without changing the response or adding an abstraction (CTD-003).
2. **Change constraint description projection.** The schema owner already builds
   accepted constraints and live activation/job descriptions. SQL currently
   also traverses fields, indexes and relations to discard them. Reusing the
   constraint projection is the narrow alternative; qualify admission and
   typed corruption behavior before removing unrelated full-description checks
   (CTD-004).
3. **Add an adjacent exact numeric aggregate.** Current range and numeric folds
   already converge on `try_fold_exact_first_components` for generation,
   multiplicity, bounds and stop-after handling. The old CTD-002 duplicated-loop
   evidence does not describe this inspected owner. Future types still require
   an admitted numeric contract and permitted cost evidence, not sibling loops.

These rehearsals identify ownership and friction; they do not authorize features.

## Findings

| ID | Family | Risk | Owner/evidence | Present friction | Disposition | Trigger |
| --- | --- | --- | --- | --- | --- | --- |
| CTD-003 | DuplicatedFlowDebt | LOW | [session catalog](../../../../../../../../crates/icydb-core/src/db/session/catalog.rs):234 and [SQL metadata projection](../../../../../../../../crates/icydb-core/src/db/session/sql/execute/metadata.rs):42 | verbose SQL repeats the existing catalog assembler's validation-job/identity collection and metadata construction | FIX WHEN TOUCHED | next authorized metadata cleanup: make the existing assembler session-visible and reuse it; preserve compact projection and SQL error wrapping |
| CTD-004 | OwnershipDebt | LOW | [SQL metadata projection](../../../../../../../../crates/icydb-core/src/db/session/sql/execute/metadata.rs):82 and [schema description owner](../../../../../../../../crates/icydb-core/src/db/schema/describe.rs):1129 | SHOW CONSTRAINTS depends on full field/index/relation description construction and copies out only constraints | FIX WHEN TOUCHED | next authorized metadata cleanup: reuse the existing constraint projection with live validation jobs; prove accepted admission, ordering and corruption boundaries remain correct |

CTD-003 is source-visible duplicated adapter assembly, not a second semantic
schema engine. CTD-004 makes an output subset depend on unrelated description
work. No performance saving is claimed without Wasm/cycle/instruction evidence.
This report does not create another active debt ledger.

## Accepted And Not-Debt Signals

- The unreachable composite-null leaf branches and three private codec helpers
  are removed. Current nullable rows retain their canonical storage owner.
- Macro runtime resolution has no unused override arguments. Supported model
  and actor overrides remain distinct; sharing their token walkers through a
  new crate would increase dependency complexity for a small overlap.
- Exact selection keeps cache lookup before its single eligibility gate, then
  resolves accepted authority. Exact and prepared results share one cache slot.
- UPDATE diagnostics have one policy-owned projection. Exact/resumable guards
  against incompatible inputs remain boundary checks, not duplicated policy.
- Decode, encode and validation match arms differ in output, materialization and
  failure obligations. A generic codec visitor is not justified by similar lists.
- Request/read/mutation profiles have different operation limits, instruction
  ceilings and failure reserves. Do not merge them to remove repeated constants.
- Model and macro primitive definitions already consume the schema-owned
  registry. Separate generated consumer types are not independent scalar policy.
- Older startup recovery debt lies outside this run; no fresh whole-startup
  verdict is inferred from its historical report or accepted disposition.

## Complexity Delta

At the four stable conversation anchors, A6 changes eight production Rust files
by approximately +56/-129 lines, or 73 fewer lines. It removes unreachable paths,
unused arguments and repeated gating, and converges UPDATE diagnostic mapping.
No public/configuration/execution/persisted axis is added. Structure is simpler.

This audit adds two documentation/evidence files and no production Rust. The
older affected-owner audit's numerical delta is non-comparable. Raw Wasm bytes,
IC cycles and instructions are unmeasured; native elapsed output is not used as
a performance metric.

## Focused Verification Readout

| Verification | Status | Evidence and limits |
| --- | --- | --- |
| Four cleanup anchors and callers | PASS | current source and call sites inspected; no removed-path test scaffolding added |
| Structural codecs and nullable row contract | PASS | 40 structural-field tests and 3 nullable contract tests freshly executed |
| UPDATE policy | PASS | 26 existing policy tests freshly executed; adapter mappings additionally compared in source; not exhaustive end-to-end diagnostics coverage |
| Exact/prepared cache boundary | PASS | one fingerprint-bound aggregate cache test freshly executed |
| Description behavior | PASS | 13 describe-selected tests freshly executed; existing SHOW CONSTRAINTS canister assertions inspected, not run |
| Generated dependency paths | PASS | 2 macro resolver tests and 4 renamed-facade tests freshly executed |
| Structural gates | PASS | formatting check, layer authority, schema/model boundary, SQL branch ownership and format policy freshly executed |
| Documentation and diff | PASS | final references, JSON shape and diff checks pass |
| Full suite / canister qualification | BLOCKED | user-owned full suites; no new runtime implementation requiring a network was made |
| Proposed introspection corrections | BLOCKED | evidence only; implementation and focused semantic qualification belong to a separately authorized landing |

Total freshly executed focused tests: **89**, all passing. The prior handoff's
189 tests and lint results are contextual evidence and are not counted again.
No networks were started/stopped and no Cargo versions changed.
