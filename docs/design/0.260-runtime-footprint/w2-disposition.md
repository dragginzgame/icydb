# W2 — Projection/page footprint disposition

Date: 2026-09-20. **Complete: no-build.** Source inspection did not establish
a safe, simpler owner-local reduction to take into a matched experiment.
This is not proof that the executor cannot be made smaller; it closes the
bounded W1-selected investigation without a speculative runtime change.

## Evidence and scope

The [W1 reconciliation](w1-reconciliation.md) measures a +892,813 raw-byte
increment for the first typed page in Canic's published-IcyDB-0.259.6 subjects.
Its named projection closure accounts for 43,282 shallow / 330,541 retained
bytes. These figures identify an inspection target, not removable bytes or
execution cost. No W2 raw-Wasm, IC-cycle or instruction delta was measured.

Inspection used the current IcyDB worktree at base
`4698cecb8dbba6786079b759b43494b020559997`, including the earlier 0.260 slices.
The inspected projection facade and query/preparation owners have no local
changes. In particular, the SHA-256 of `executor/projection/facade.rs` is
`c17108008eb275418f5a35fd465d17b0d3e6d36e2aed69bf407ac984a43527ce`.
This is a source review, not a new composition baseline for that worktree.

## Alternatives checked

| Simplest proposed reduction | Source finding | Disposition |
| --- | --- | --- |
| Delete cursorless covering work from the page coordinator. | The dynamic page caller deliberately omits cursor emission for a prepared exact single-primary-key exhaustion. SQL also uses the same coordinator without a cursor. | A page call does not prove cursor emission. Removing the branch outright is not justified. This does not prove that every secondary-index covering branch executes in the fixed W1 fixture. |
| Reuse scalar predicate preparation instead of strict covering compilation. | Scalar preparation retains `ConservativeSubset`; covering requires `StrictAllOrNone`. Their optional results are not interchangeable, and compilation charges construction work before allocating literals. | No equivalent already-prepared strict resident was established at this boundary. Moving or caching another program would need its own cost and failure-ordering evidence. |
| Remove DISTINCT because the public dynamic query cannot request it. | The public builder indeed does not expose projection DISTINCT; test-only dynamic callers and the SQL frontend exercise the shared semantics. The coordinator consumes a general prepared plan. | Public grammar is narrower than this owner. A feature-specific narrowing of prepared state might be investigable, but no simpler converged representation or matched saving is established here. Do not silently ignore an admitted plan's DISTINCT flag. |
| Merge or outline the two budget wrappers. | Rows and pages already delegate to one inner coordinator. The rows entry is exported internally only with SQL, which W1 disables. | Apparent source duplication is not demonstrated duplicate code in the measured SQL-off subject. Outlining alone has no established raw-byte benefit. |

The canonical owner remains
[`execute_structural_projection_rows_inner`](../../../crates/icydb-core/src/db/executor/projection/facade.rs).
The caller's exact-exhaustion decision is in
[`session/query/dynamic.rs`](../../../crates/icydb-core/src/db/session/query/dynamic.rs),
and the public/test-only grammar is in
[`query/dynamic.rs`](../../../crates/icydb-core/src/db/query/dynamic.rs).
Strict versus conservative preparation is owned by
[`planning/preparation.rs`](../../../crates/icydb-core/src/db/executor/planning/preparation.rs);
the shared plan's
[`runtime handoff`](../../../crates/icydb-core/src/db/executor/prepared_execution_plan/shared_plan.rs)
initializes scalar preparation after covering selection. Pure/hybrid covering
selection remains under its existing
[`covering owner`](../../../crates/icydb-core/src/db/executor/projection/covering/mod.rs).

No alternate page route, feature switch, cache, admission shortcut or authority
reconstruction is introduced. No numerical acceptance tolerance was needed:
no runtime candidate passed the source-level gate, and no size experiment ran.
The existing standalone actors are available for a future justified candidate,
but their sizes cannot be subtracted from Canic's W1 artifacts. Sibling builds,
edits, dependency changes and deployment remain outside this line's authority.

## Validation and handoff

Ten focused existing SQL-disabled core tests passed: four page tests and six
predicate-preparation tests, detailed in the status tracker. They validate
maintained page and predicate boundaries, not production SQL-off
reachability or IC performance; unit-test-only DISTINCT is included explicitly.
No Rust changes, new tests, actor builds, Clippy runs or network lifecycle
actions belong to this slice. Full repository validation remains user-owned.

Five documentation files change, approximately +85 net lines, with no production
line or state-space delta; implementation shape stays unchanged. The pre-existing lockfile change from
rand 0.10.2 to 0.10.3 is untouched, as are all earlier slice changes.

Next is Q1's read-only closeout audit of the current 0.260 candidate, not another
runtime optimisation. Full Toko Miner ICYDB-033 attribution is still open;
W1's controlled comparison and W2's no-build decision do not close it or qualify
the composed size of the current candidate.
