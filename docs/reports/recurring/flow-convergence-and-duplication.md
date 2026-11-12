> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Flow Convergence And Duplication — Broad Baseline

## Preamble And Comparability

- Method: `FCD-1.0`; run `2026-10-01/01`; auditor `Codex`.

- Definition: [flow-convergence and duplication](../../audits/recurring/crosscutting/crosscutting-flow-convergence-and-duplication.md).

- Snapshot: HEAD `45ff7e8270514a505260460a8db1f7f0ed96c1a6`, tree
  `3cbc0202d8dad27a2e477893a14db918c0b5d951`, released `v0.262.2`, plus the
  pre-existing dirty worktree described below.

- Tracked diff SHA-256 at entry:
  `2e6e35137238eb56176b16ef9a8b40878f5e0c127e068f554e5034369025b0f5`.

- Cargo.lock SHA-256 at entry:
  `6abd52233d9de42eed2740b6eb1d5b4ccd7e69a80ed1247ba9b75ba7ed2a2e5f`.

- Dirty worktree: eight schema package manifests, Cargo.lock, root/detailed
  changelogs and the 0.262 status tracker already contain dependency-cleanup
  work. These twelve files are excluded from this audit's edits. No Cargo
  version changes were made. The source snapshot remained unchanged.

- Baseline: [2026-08-26 affected-owner run](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/08/26/flow-convergence-and-duplication/01/report.md).
  Scope comparison is **N/A**: this explicitly requested broad baseline covers
  more behavior families. Only named stable owner anchors are compared below.

- Output: this report and [structured findings](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/10/01/flow-convergence-and-duplication/01/findings.json). An audit is
  evidence; it does not authorize implementing the findings or creating a debt
  ledger. No source, release metadata, design or older report was edited.

## Scope And Discovery Evidence

Locked offline Cargo metadata identifies 49 workspace packages and 120 targets.
Rust discovery covered `crates/`, `testing/`, `schema/`, `canisters/` and
`scripts/`: 1,659 Rust files and 21,809 lexical function-body candidates. These
are scanner candidates, including macro/fixture bodies, rather than
compiler-proven function or reachability counts.

Exact-body screening found 23 groups of substantial identical bodies at a
60-token threshold. A second test/helper screening at 15 tokens found 32
exact-body groups. Structural screening with renamed local identifiers also
exposed the equivalent named-type walkers. These scans select inspection
candidates; counts do not establish equivalence, deadness or severity.

Manual inspection traced SQL, typed/dynamic session APIs, prepared execution,
exact aggregation, physical streams, catalog application, startup/recovery,
write publication, diagnostics, scalar capability registries, generated/model
consumers, replay evidence, migration tests and canister fixtures. Invariant
scripts and focused tests qualify the selected boundaries. Native elapsed
results are not used as performance evidence.

This is a broad maintained-flow audit, not a compiler-backed proof that every
symbol is reachable or every duplicate is redundant. Generated build outputs,
external consumers, historical prose and the full workspace correctness suite
are outside its proof scope. No new unused-code deletion is recommended solely
from a zero reference count.

## Verdict

**PASS WITH FINDINGS**

Five consolidation opportunities remain: a shared physical seek-control
contract, direct named-type dependency enumeration, a parser diagnostic map,
registry-derived numeric capabilities, and shared budgeted-request test setup.
Three have `MEDIUM` maintenance risk and two have `LOW` risk. No inspected
finding establishes a present correctness failure or a release blocker.

The inspected runtime flows retain accepted-catalog authority, common prepared
execution and journal publication owners. The proposed changes can remove
handwritten switch sites without adding modes, caches, compatibility routes or
persisted forms. Whole-engine replacement and a generic traversal framework
are not needed.

## Findings

| ID | Class | Risk | Owner and evidence | Present friction | Disposition and action trigger |
| --- | --- | --- | --- | --- | --- |
| FCD-004 | DuplicateFlow | MEDIUM | [physical streams](../../../crates/icydb-core/src/db/executor/stream/access/physical/mod.rs), ensure_physical_head at 1207/1655 and seek_head_at_or_after at 1300/1688 | identical held-head, pull admission, comparison charging and page-stop control must be changed twice | CONSOLIDATE before another backend or a shared accounting/resume change; share only the control contract within the existing owner |
| FCD-005 | DuplicateFlow | MEDIUM | [source digest](../../../crates/icydb-schema/src/source_digest/mod.rs):223/254 and [application lowering](../../../crates/icydb-core/src/db/schema/application_lowering/mod.rs):281/315 | the same named-type and nested field-type dependency branches are handwritten twice | CONSOLIDATE during the next authorized schema cleanup; put direct dependency enumeration with schema fragments |
| FCD-006 | DuplicateFlow | LOW | [predicate parser](../../../crates/icydb-core/src/db/predicate/parser/mod.rs):74 and [statement parser](../../../crates/icydb-core/src/db/sql/parser/mod.rs):307 | the same trailing-token diagnostic map has two copies | LOCALIZE during a parser diagnostic edit; share pure diagnostic projection through an ungated parse-error owner |
| FCD-007 | PolicyRediscovery | LOW | [Value semantics](../../../crates/icydb-core/src/value/semantics.rs):12/29 and [scalar registry](../../../crates/icydb-schema/src/scalar_macros.rs) | two variant lists independently reproduce authoritative capability flags | LOCALIZE during a scalar-capability edit if registry projection preserves const/lightweight execution |
| FCD-008 | DuplicateFlow | MEDIUM | 59 identical factories in 59 files; representative [comparison tests](https://github.com/dragginzgame/icydb/blob/45ff7e8270514a505260460a8db1f7f0ed96c1a6/crates/icydb-core/src/db/query/construction/comparison/tests.rs):17 and [index metadata tests](../../../crates/icydb-core/src/db/session/tests/cardinality_tiebreak/index_metadata.rs):12 | the same uniform budget profile and construction imports are maintained in 59 places | CONSOLIDATE as one bounded test-setup cleanup in existing request test support; preserve assertions and distinct profiles |

## Focused Verification

Total focused tests: **40 passed, zero failed**. A first physical test selector
matched zero tests; the corrected physical_seek_tests selector ran the 16 tests
counted above. No PocketIC/ICP lifecycle action or external service was used.
Clippy and formatting were not rerun because no Rust source changed.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/10/01/flow-convergence-and-duplication/01/report.md).
