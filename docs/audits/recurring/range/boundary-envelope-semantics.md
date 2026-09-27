# Recurring Audit — Planner Boundary & Envelope Semantics

Method: `BOUNDARY-5` + `DOMAIN-1`.

Apply [Domain Scope And Change Triggers](../../README.md#domain-scope-and-change-triggers)
and [Executed-Test Evidence](../../README.md#executed-test-evidence).
This is a correctness audit of `icydb-core`, not a performance, style, or
refactoring review. A run does not authorize fixing its findings.

## 0. Freeze Scope And Invariants

Record the requested baseline or change trigger, snapshot (including dirty
inputs), affected owners, selected obligations, and exclusions with reasons.
Follow each selected contract through planning, accepted-schema lowering,
continuation admission, and traversal. Inspect current callers before deciding
whether a path is reachable. Historical names and tests are discovery hints.
Keep this method and scope fixed once execution begins.

Freeze these invariants before evaluating implementation:

| Invariant | Required contract |
| --- | --- |
| Resume | For an anchor admitted inside the original envelope, ASC yields `(Excluded(anchor), upper)`; DESC yields `(lower, Excluded(anchor))`. Without an anchor, retain both bounds. |
| Containment | The resumed envelope is a subset of the original. Included endpoints admit equality; excluded endpoints reject it. Check containment before rewriting. |
| Opposite edge | ASC retains the upper bound byte-for-byte; DESC retains the lower bound. Monotonicity is directional, not always increasing. |
| Bound lowering | Logical strictness survives value encoding, equality prefixes, remaining component sentinels, and primary-key suffixes. Equal-bound tightening cannot loosen the interval. |
| Ordering | Bounds, anchor comparison, and raw traversal agree within the same physical key domain. Each route establishes how physical order relates to requested logical order and tie-breaks. |
| Plan binding | External continuation remains bound to accepted authority, query/order/window and route identity before execution. It cannot select a wider predicate, different index, or different access shape. |
| Empty envelope | A proven empty traversal envelope returns before constructing or advancing a store range. Distinguish an empty interval from a nonempty interval containing no stored keys. |
| Progress | The anchor cannot recur; no eligible row is skipped by resume, merge, residual filtering, or tie-break handling under the declared data-consistency contract. |

Produce an invariant registry with owner symbols, enforcement type
(structural, runtime guard, or assumption), and source references. A debug
assertion alone is not production admission evidence.

## 1. Transformation And Ordering Proof

Trace logical predicate -> tightened semantic interval -> accepted index
contract -> encoded component bounds -> complete raw bounds -> admitted
continuation -> effective bounds -> store traversal -> logical result order.

Produce one table with location, transformation, invariant, enforcement, and
remaining risk. Include every reachable selected traversal adapter, including
merged or covering routes when they consume these bounds. Do not assume a
guard in one scanner protects another.

Restate the logical-to-raw mapping using complete keys. Let `low(v)` and
`high(v)` denote the full key with the equality prefix and encoded `v`, then
low/high sentinels for every remaining component and primary-key suffix:

| Logical operator | Semantic bound | Complete raw bound |
| --- | --- | --- |
| `>` | lower `Excluded(v)` | lower `Excluded(high(v))` |
| `>=` | lower `Included(v)` | lower `Included(low(v))` |
| `<` | upper `Excluded(v)` | upper `Excluded(low(v))` |
| `<=` | upper `Included(v)` | upper `Included(high(v))` |

Verify the current encoder rather than copying this notation as proof. A
logically unbounded component can still have a physical bound that confines
index identity and equality prefix; distinguish that from `Bound::Unbounded`.
Check that sentinel ordering encloses all admitted suffixes.

For ordering, identify the actual `Ord` implementation; serialized framing
bytes need not be the comparator. Separate:

- independent semantic-value versus encoded-component ordering evidence;
- full-key comparison (namespace, index, components, primary-key tie-break);
- containment/advancement agreement with that comparator; and
- any planner-approved merge, sort, or residual route that changes result order.

Comparing the raw comparator against helpers using that same comparator does
not prove semantic encoding order. Name the tested value domain and avoid
claiming all types from a sampled subset. Unsupported values must follow the
current admission contract; they are not missing features.

## 2. Adversarial Envelope Matrix

For both ASC and DESC, reason through the following cases. Share rows when the
proof is symmetric; identify direction-specific outcomes explicitly.

1. Anchor equals included lower endpoint.
2. Anchor equals excluded lower endpoint.
3. Anchor equals included upper endpoint.
4. Anchor equals excluded upper endpoint.
5. Anchor below lower or above upper.
6. Inverted bounds and all equal-endpoint inclusion combinations.
7. Singleton interval, including continuation after its only key.
8. Lower unbounded, upper unbounded, and both unbounded.
9. Continuation collapses to an empty envelope.
10. Equal indexed values with different suffix components or primary keys.
11. Equal-bound tightening and prefix boundaries with neighboring keys.
12. Mismatched index/authority/query/order/route, including composite access.

Use columns: scenario, direction, expected outcome, production enforcement,
source evidence, executed proof (or gap), and risk. Reasoning a case through
source is not executing it.

For the empty-envelope case, trace the early return ahead of range creation,
then identify a focused check that observes no traversal (instrumentation or
a populated-store callback plus source proof of the pre-range guard). An
emptiness-helper assertion alone does not establish the storage obligation.

## 3. Continuation And Plan Binding

Distinguish public cursor admission from internal raw scan anchors. Identify
who creates each anchor, whether it denotes the last emitted or last physically
consumed key, and how any separate logical boundary is enforced. Do not assume
a current token carries the raw anchor used by an earlier implementation.

Verify the current signature/authentication and accepted-authority checks,
order/window/route binding, and boundary-shape admission before row execution.
For composite access, inspect per-child bounds and merge order; establish
rejection or supported semantics from current owners rather than requiring
all composite plans to be rejected.

Show why resume preserves the original access contract and opposite edge,
excludes the anchor, and handles equal collapse deterministically. Trace
residual filtering and tie-breaks to explain duplication/omission behavior.
State the between-page data-consistency assumptions; do not claim snapshot
pagination for a live mutable view.

## 4. Focused Verification

Inspect assertions, discover and list current selectors, then execute only
focused package/target selections as required by the shared evidence contract.
Cover these selected obligations with existing tests where possible:

- directional containment, strict advancement, unchanged no-anchor bounds,
  bounded/unbounded edges, and equal-bound collapse;
- component inequality lowering including suffix and prefix boundaries;
- independent semantic/encoded ordering and full-key tie-break order;
- empty-envelope traversal suppression;
- current continuation rejection and a maintained multi-page route proving
  ordering and no duplicate/omitted eligible rows under stated assumptions.

A missing required test is a verification gap, not permission to silently omit
its obligation or add implementation. Do not resurrect removed APIs or tests.
Record exact commands and selected/passed/failed/ignored counts, including
failed or blocked attempts. Never count zero-test success as behavioral proof.
Full workspace/repository suites remain user-owned.

## 5. Report And Verdict

Write a new `boundary-semantics` run under the canonical report hierarchy.
Required sections:

1. Metadata, selection rationale, scope/exclusions, and method changes.
2. Invariant registry and transformation proof (including logical/raw mapping).
3. Adversarial matrix and opposite-edge/empty-envelope proof.
4. Ordering, continuation binding, and duplication/omission analysis.
5. Findings, drift triggers, and verdict.
6. Verification readout.

Apply [Findings And Verdicts](../../README.md#findings-and-verdicts) and the
shared finding-ownership rules. Findings distinguish demonstrated defects
from missing proof, with owner, consequence, disposition, and action trigger.
Do not use composite risk scores or report excluded families as passing.

Run `01` compares to the latest prior comparable `boundary-semantics` report;
subsequent same-day runs compare to run `01`. If none is comparable, record
`N/A` and link the historical reference separately. `BOUNDARY-5` corrects the
ASC-only invariants, component/raw-key conflation, universal raw/logical order
claim, and current-cursor assumptions, and makes proof coverage explicit.
Mark affected historical deltas `N/A (method change)`; retain stable evidence
such as directional exclusive resume where it remains applicable. Historical
reports are immutable and their test results are not newly executed evidence.
