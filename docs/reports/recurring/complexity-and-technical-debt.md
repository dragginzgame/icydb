> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Complexity And Technical Debt — Cleanup Rerun

## Preamble And Comparability

- Method: `CTD-1.0`; run `2026-10-01/01`; auditor: Codex.

- Definition: [CTD-1.0 definition](../../audits/recurring/crosscutting/crosscutting-complexity-and-technical-debt.md).

- Trigger: user-requested rerun of the conversation's complexity inspection
  after all four approved findings were implemented as A6.

- Snapshot: HEAD `bdec3835dc0d9b775413999cab4715ebcedcd150` plus the existing dirty worktree.

- Tracked diff SHA-256 at entry:
  `5c12e797ba6909447bec83d444db5a2b50301088d571864c1817c8fd0ac40363`.

- [Structured findings](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/10/01/complexity-and-technical-debt/01/findings.json) retain hashes of the 14 selected source files.

- Scope: A6 codecs, macro path resolution, exact SQL aggregate selection and
  UPDATE diagnostics; adjacent catalog/SQL introspection, budget profiles and
  exact metadata folding. Duplicate screening covered production Rust under
  core, facade, model and model-macros; manual conclusions apply only to the
  selected owners, not every screened symbol.

- Pre-existing changes include A4/A5 storage/aggregate work, other 0.264 schema
  cleanup, measurement fixtures and release/status documentation. They remain
  intact. This run writes only its report and findings; it does not fix findings,
  update a tracker/ledger, or modify release metadata.

## Verdict

**PASS WITH FINDINGS**, limited to the inspected owners.

All four A6 corrections remain present and retain their maintained boundary
contracts. Two `LOW` findings remain in metadata introspection. Neither is an
observed correctness defect or a scoped release blocker. No new framework,
execution mode, configuration, cache or persisted representation is warranted.

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

## Findings

| ID | Family | Risk | Owner/evidence | Present friction | Disposition | Trigger |
| --- | --- | --- | --- | --- | --- | --- |
| CTD-003 | DuplicatedFlowDebt | LOW | [session catalog](../../../crates/icydb-core/src/db/session/catalog.rs):234 and [SQL metadata projection](../../../crates/icydb-core/src/db/session/sql/execute/metadata.rs):42 | verbose SQL repeats the existing catalog assembler's validation-job/identity collection and metadata construction | FIX WHEN TOUCHED | next authorized metadata cleanup: make the existing assembler session-visible and reuse it; preserve compact projection and SQL error wrapping |
| CTD-004 | OwnershipDebt | LOW | [SQL metadata projection](../../../crates/icydb-core/src/db/session/sql/execute/metadata.rs):82 and [schema description owner](../../../crates/icydb-core/src/db/schema/describe.rs):1129 | SHOW CONSTRAINTS depends on full field/index/relation description construction and copies out only constraints | FIX WHEN TOUCHED | next authorized metadata cleanup: reuse the existing constraint projection with live validation jobs; prove accepted admission, ordering and corruption boundaries remain correct |

CTD-003 is source-visible duplicated adapter assembly, not a second semantic
schema engine. CTD-004 makes an output subset depend on unrelated description
work. No performance saving is claimed without Wasm/cycle/instruction evidence.
This report does not create another active debt ledger.

## Focused Verification Readout

Total freshly executed focused tests: **89**, all passing. The prior handoff's
189 tests and lint results are contextual evidence and are not counted again.
No networks were started/stopped and no Cargo versions changed.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/10/01/complexity-and-technical-debt/01/report.md).
