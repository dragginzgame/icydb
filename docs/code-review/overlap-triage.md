# Remaining-review overlap triage

Scanned 2026-10-02 against the current 0.264 worktree after A22. This is a quick
read-only code scan, with this report and status links saved under the standing
documentation request. It does not authorize implementation, extend the planned
landing queue or provide a complete closeout verdict.

Authority: [saved review](IcyDB%20Code%20Review.html), [status](status.md) and
[prior scoped audit](closeout-audit.md).

## Scope and confidence

All 291 Needs verification findings were screened by title, severity, category
and cited owner. Selected findings' detailed evidence/recommendations were then
compared with current source at the shared boundaries below. Current source
supports several overlap hypotheses, but these are candidate repair groups,
not promises that a single edit closes every member. Each ID still needs its
own semantic regression and closure verdict. The saved review already merges
duplicate aliases; counts below refer to distinct findings.

The 291 comprise 25 high, 84 medium, 153 low and 29 info findings. Their original
categories include 25 documentation, 12 testing and 37 performance findings.
Those categories overlap with severity, not with one another. This is not a
list of 291 confirmed current runtime bugs. Performance improvements require
raw Wasm, IC cycle or instruction evidence; none is measured in this scan.

## Candidate groups

Seven groups contain 21 distinct unchecked reports. The first two are the
clearest candidates for one canonical correction each. The others require
qualification or splitting before any combined implementation is promised.

| Group | Distinct IDs | Canonical correction hypothesis | Size / confidence |
| --- | --- | --- | --- |
| Filtered-index predicate authority | `xc-architecture-1`, `sql-parser-5`, `r2-persisted-sql-text-1`, `r2-persisted-sql-text-2`, `r2-persisted-sql-text-3`, `xc-architecture-2`, `r2-recursive-bounds-5` | Persist the existing accepted, typed, field/variant-ID-bound predicate tree; membership, identity, reconciliation and rename derive from it; render SQL for display | **7; strongest structural overlap.** One coherent end-to-end contract, but substantial persisted-metadata work; design first |
| Residual-filter preservation | `query-plan-4`, `query-plan-5` | Preserve incomplete expression coverage and require the maintained residual proof before selecting index-only terminals | **2; best small next candidate.** Two consumer escape hatches, one residual-preservation contract |
| Canonical nested-value decoding | `data-2`, `data-7`, `r2-recursive-bounds-1` | Reuse accepted canonical-wire decoding/validation and one depth authority for nested reads and historical fills | **3; plausible convergence.** Traversal and historical-fill validation have different entry seams; split if reuse does not yield one bounded outcome |
| Secondary-index physical continuation | `executor-stream-1`, `executor-stream-5` | Carry a validated secondary resume anchor into the physical stream so resumed fetch bounds apply after physical progress | **2; plausible shared cause.** Removing the fetch hint alone addresses omission, not repeated traversal; physical qualification is required |
| Grouped ordering | `executor-aggregate-3`, `query-plan-3` | Carry and consume resolved grouped order consistently in generic finalization and admission | **2; related order contract.** Passing one direction fixes DESC only; mixed directions need their own admissibility/comparator proof |
| Instruction accounting lifetime | `executor-core-1`, `executor-core-2` | Establish instruction ownership at the correct read/message lifetime and account cumulative work without resetting allocations | **2; shared tracker, different seams.** Read baseline and message-wide mutation/maintenance ownership may require separate outcomes; IC qualification is required |
| Accepted source-lineage publication | `schema-migration-2`, `schema-migration-3`, `schema-migration-4` | Keep accepted head, entity-source version/digest and entity membership consistent through catalog publication | **3; related family, likely split.** Head freshness, plan-less source changes and removed-entity membership are independent invariants |

### Current source evidence

- Filtered predicates still render a bound accepted tree into text in
  [application lowering](../../crates/icydb-core/src/db/schema/application_lowering.rs),
  persist text in the [index codec](../../crates/icydb-core/src/db/schema/codec/index.rs),
  parse it through [accepted predicate normalization](../../crates/icydb-core/src/db/predicate/mod.rs),
  and compare exact text or sequentially relabel names in
  [migration planning](../../crates/icydb-core/src/db/schema/migration_planner.rs) and
  [index identity](../../crates/icydb-core/src/db/schema/mutation/index_candidate.rs).
  Typed literals, rename identity, enum-variant identity and parser-depth drift
  therefore have a common representation boundary. Replacing that boundary can
  remove multiple text conversions and classifiers. Existing accepted check
  trees are the simplest reuse candidate. Before 1.0 this would replace the
  current version-1 representation in place and require reinstall/recreation or
  explicit regeneration, with no predecessor decoder or compatibility path.
- [Plan assembly](../../crates/icydb-core/src/db/query/plan/pipeline.rs) still
  calls `without_filter_expr()` on a stripped primary-key predicate without a
  coverage check. [Covering eligibility](../../crates/icydb-core/src/db/query/plan/covering/mod.rs)
  still returns true for an absent predicate before checking the residual
  expression. Reuse the existing coverage/residual facts; avoid another filter
  mode or proof representation. Qualify SELECT, COUNT/EXISTS and mutation scopes
  with exact-key access plus an unextractable expression.
- The [nested skipper](../../crates/icydb-core/src/db/data/structural_field/value_storage/skip.rs)
  still treats extension envelopes as an inner value at offset + 1; the
  [materializer](../../crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs)
  has no enum arm, and collection walkers add another depth increment.
  [Historical-fill validation](../../crates/icydb-core/src/db/schema/transition/compatibility.rs)
  uses the general runtime decoder, while
  [default validation](../../crates/icydb-core/src/db/data/persisted_row/canonical.rs)
  already distinguishes canonical wire and validates accepted catalogs.
- [Scalar continuation](../../crates/icydb-core/src/db/executor/planning/continuation/scalar.rs)
  still supplies no index resume anchor.
  [Index-set hints](../../crates/icydb-core/src/db/executor/pipeline/entrypoints/scalar/hints.rs)
  set per-branch fetch bounds without a resumed secondary-order guard. Related
  `executor-aggregate-6` also starts grouped streams without an anchor, but its
  group-boundary and cumulative-budget semantics make it a separate consumer
  qualification, excluded from the two-item scalar group.
- [Generic grouped finalization](../../crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/generic/page_finalize.rs)
  uses an ascending sorted bundle for unbounded output, while
  [canonical grouped-order validation](../../crates/icydb-core/src/db/query/plan/validate/grouped/cursor/mod.rs)
  checks field alignment without rejecting mixed directions. Grouped projection
  cursor finding `executor-aggregate-2` concerns canonical key identity before
  shaping, so it is excluded from this order group.
- The [budget tracker](../../crates/icydb-core/src/db/executor/budget.rs) still
  starts with no instruction watermark; read setup does not establish it,
  mutation setup samples on entry, and maintenance creates fresh trackers.
  `canisters-testing-ci-4` concerns ineffective/stale measurement assertions;
  repairing tracker ownership does not automatically fix those assertions.
- [Lineage application](../../crates/icydb-core/src/db/schema/application.rs)
  still bypasses ordinary source preflight without a migration, retains existing
  lineage entries while re-stamping heads, and compares proposal/entity counts.
  Store-topology lineage identity and irreversibility findings are excluded:
  they require separate upgrade/recovery work.

## Related themes that should not become one large fix

- SQL NOT, casefolding, lazy CASE/COALESCE and coercion-aware predicate stripping
  are different semantic invariants. `query-plan-2`, `session-sql-1`,
  `query-expr-3` and `query-expr-10` can share differential fixtures, but a common
  test suite does not make them one production correction.
- Decimal multiplication precision, alignment overflow, remainder and mixed
  numeric ordering need operation-specific proofs. A decimal helper correction
  should not claim all numeric reports. The numeric order reports
  `value-types-error-2`/`value-types-error-8` are related, but removing a lossy
  conversion alone does not prove a total order across all variants.
- CLI SQL errors and migration failure exit codes (`cli-3`/`cli-4`) share a user
  outcome but have separate response owners. Subsequent A26 fixes and qualifies
  SQL endpoint failure status across its three call lanes. Subsequently authorized
  A27 independently fixes migration operation outcomes in their existing dispatcher
  and qualifies returned phases, bounded progress, cleanup and process status.
  The shared process fixture now covers both corrections; neither correction
  alone closes the other finding.
- CI nonempty selection (`canisters-testing-ci-1`) and ignored-test coverage
  (`canisters-testing-ci-10`) both concern qualification evidence. Current
  Makefile filters still name absent oracle modules; the existing all-feature
  core test listing contains neither module. Coverage validation still checks
  test declarations without ignored-test handling. Restoring runnable evidence
  is useful systemic work, but these are separate checks; it does not fix the
  reported query semantics. No full CI or repository test suite was run here.
- Documentation findings can be updated with their owning correction. Unrelated
  guide, release-tooling and API findings should not be bundled into a generic
  cleanup merely to lower the finding count.

## Existing-correction verification candidates

`query-expr-2` currently has null-rejecting FALSE-set guards in the shared
predicate compiler. All five maintained SQL NOT read/count/UPDATE/DELETE tests
pass, including nested NOT/AND/OR and both predicate/expression execution lanes.
`value-types-error-3` currently uses checked division/remainder; its maintained
signed-overflow regression passes. These findings deserve a closure audit of
the original claims and current consumers before adding replacement code.
They remain Needs verification in this quick triage; no inventory counters are
changed and no alias is counted as another fix.

## Recommended next decision

For the smallest multi-finding correction, qualify the residual-preservation
pair first. For the largest demonstrated common cause, design the seven-item
filtered-index authority replacement, including current-format recreation and
the complete consumer matrix, before implementing it. Record a bounded outcome
in the 0.264 tracker only once the implementation scope is selected. Do not
combine the related lifecycle, order, budget and predicate families by default.

Six focused existing tests pass. Documentation-link and diff checks pass.
Production code, release notes and Cargo versions are unchanged by this scan;
only this report and links in the status/audit documents are authored. Raw Wasm,
IC cycles and instructions are unmeasured; no network lifecycle action occurred.

## Subsequent authorized A23 work

The user selected the two-item residual-preservation group for one bounded
handoff. The planning correction reuses existing intent coverage; covering
eligibility reuses existing residual compatibility. The latter helper currently
feeds aggregate EXPLAIN, so its COUNT/EXISTS descriptor proof is distinguished
from aggregate execution controls. The original scan and other candidate
groups retain their historical scope; they are not authorized together.

Qualification also reproduced a separate mixed expression/predicate-only cache
identity defect, documented in the [audit](closeout-audit.md). A23 originally
isolated different predicate-only scopes by clearing the shared test cache.
User-authorized A24 subsequently retains both existing filter identities and
qualifies changed/revisited scopes without that workaround. All 81 focused
tests pass; no additional saved finding is closed by inference. Its additional
singleton-IN control exposes a separate simultaneous-residual runtime defect
even with caching disabled. User-authorized A25 subsequently enforces both
residual authorities through the existing effective filter. All 116 focused
tests pass, including scan controls requiring both conjuncts and the original
singleton-IN cases. Neither follow-up closes another saved ID by inference.
