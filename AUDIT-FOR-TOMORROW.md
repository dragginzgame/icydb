

Codex Prompt: Pre-EXPLAIN Feature Audit
You are performing a PRE-EXPLAIN CAPABILITY AUDIT for a database engine.

Repository: IcyDB
Language: Rust

Architecture layers:

intent
→ query builder
→ planner
→ access strategy
→ executor
→ index/storage

Goal:
Produce a COMPLETE LIST of features and execution capabilities that should exist BEFORE implementing EXPLAIN, so that EXPLAIN does not need to be rewritten later when execution shapes change.

EXPLAIN will ship in version 0.43.

Version plan:

0.41 — small fast paths and missing micro-features
0.42 — medium / large execution features and planner capabilities
0.43 — EXPLAIN implementation

You must identify EVERYTHING that changes the execution shape of queries so EXPLAIN can represent the final model.

------------------------------------------------------------

STEP 1 — Detect Existing Execution Shapes

Search for structures that represent execution plans or strategies, including:

AccessStrategy
AccessPlan
AccessRouteClass
ExecutionKernel
ExecutablePlan
ExecutionDescriptor
RouteShapeKind
AggregateKind
AggregateFoldMode

List every execution shape currently supported, including:

FullScan
IndexLookup
IndexPrefix
IndexRange
ByKey
ByKeys
Grouped aggregation
Cursor pagination
Projection
Aggregate reducers

For each shape, record:

• module
• struct / enum name
• where the executor consumes it

------------------------------------------------------------

STEP 2 — Detect Existing Pushdowns / Fast Paths

Search executor and planner code for optimizations such as:

COUNT fast path
covering projection
index-only scan
index-backed aggregates
predicate pushdown
LIMIT early termination
ORDER BY index usage

Produce a table:

feature | implemented? | module | execution shape

------------------------------------------------------------

STEP 3 — Detect Missing Index-Based Optimizations

Look for opportunities where the planner or executor still materializes rows unnecessarily.

Specifically check for missing optimizations in:

EXISTS
MIN
MAX
FIRST
LAST
DISTINCT
ORDER BY
ORDER BY + LIMIT
COUNT with predicates
secondary index IN queries
covering DISTINCT
covering ORDER BY
index prefix scans
multi-key lookup paths

Mark which are:

• missing
• partially implemented
• implemented but not pushed down

------------------------------------------------------------

STEP 4 — Detect Planner Capabilities Missing Before EXPLAIN

Inspect query/plan modules and determine if the planner supports:

secondary index selection
prefix matching
IN → multi-key lowering
BETWEEN → range lowering
projection covering analysis
order satisfaction via index
limit pushdown

Report missing capabilities.

------------------------------------------------------------

STEP 5 — Detect Missing Execution Descriptor Fields

EXPLAIN will depend on a stable execution descriptor.

Search for structures that describe plan execution.

Determine whether the system exposes these fields:

access_strategy
index_used
predicate
projection
aggregation
ordering_source
limit
cursor
covering_scan
rows_expected (optional)

If any are missing, report them.

------------------------------------------------------------

STEP 6 — Detect Missing Developer Commands

Before EXPLAIN, databases usually provide minimal introspection.

Check if these commands exist:

SHOW INDEXES
SHOW ENTITIES
DESCRIBE ENTITY
SHOW SCHEMA

If missing, report.

------------------------------------------------------------

STEP 7 — Identify Medium / Large Features Needed Before EXPLAIN

Some features materially change execution plans and should be implemented before EXPLAIN.

Look for missing support in:

covering DISTINCT
index ORDER BY elimination
MIN/MAX index seek
ORDER BY + LIMIT seek
EXISTS fast path
secondary index IN pushdown
partial covering projection
projection pruning
predicate normalization

For each feature provide:

• description
• modules likely affected
• estimated complexity (small / medium / large)

------------------------------------------------------------

STEP 8 — Produce Final Pre-EXPLAIN Checklist

Output a prioritized list grouped into:

SMALL FEATURES (0.41)
MEDIUM FEATURES (0.42)
STRUCTURAL PRE-EXPLAIN REQUIREMENTS
OPTIONAL DEVELOPER COMMANDS

Each item should include:

• feature name
• why EXPLAIN depends on it
• modules affected

------------------------------------------------------------

STEP 9 — Output Final Execution Shape Vocabulary

Produce the final set of execution nodes EXPLAIN should represent.

Example format:

Execution nodes:

IndexLookup
IndexRange
IndexPrefix
FullScan
IndexOnlyScan
AggregateCount
AggregateMin
AggregateMax
DistinctIndex
Sort
Limit
Cursor

This vocabulary should remain stable once EXPLAIN is implemented.

------------------------------------------------------------

IMPORTANT CONSTRAINTS

Do NOT propose:

• JOIN
• cost-based optimizer
• distributed execution

Focus only on features that affect single-entity query execution.

------------------------------------------------------------

OUTPUT FORMAT

Produce:

1) Execution capability inventory
2) Missing pushdowns / fast paths
3) Planner gaps
4) Pre-EXPLAIN feature roadmap
5) Final execution node vocabulary
Why This Prompt Works

This prompt forces Codex to:

scan the actual code

detect execution shapes

detect missing pushdowns

identify planner gaps

generate a stable execution vocabulary

That vocabulary is the key thing EXPLAIN needs.

What You Should Expect Back

The output will likely include items such as:

EXISTS fast path
MIN/MAX index seek
DISTINCT covering
ORDER BY index elimination
ORDER BY + LIMIT seek
secondary index IN pushdown
projection pruning
partial covering

plus structural items like:

ExecutionDescriptor stabilization
AccessStrategy debug formatting
limit operator representation

---------------------------------------------------------------------------------------------------------


• 1. Execution capability inventory

  A. Access and plan shapes currently modeled

  | Execution shape | Plan contract | Module | Executor consumer |
  |---|---|---|---|
  | ByKey | AccessPath::ByKey | path.rs | physical.rs (resolve_by_key) |
  | ByKeys | AccessPath::ByKeys | path.rs | physical.rs (resolve_by_keys) |
  | KeyRange | AccessPath::KeyRange | path.rs | physical.rs (resolve_key_range) |
  | IndexPrefix | AccessPath::IndexPrefix | path.rs | physical.rs (resolve_index_prefix) |
  | IndexRange | AccessPath::IndexRange + SemanticIndexRangeSpec | path.rs | physical.rs (resolve_index_range)
  |
  | FullScan | AccessPath::FullScan | path.rs | physical.rs (resolve_full_scan) |
  | Union | AccessPlan::Union | plan.rs | traversal.rs (union stream merge) |
  | Intersection | AccessPlan::Intersection | plan.rs | traversal.rs (intersection stream merge) |

  B. Route/execution-shape contracts

  | Shape contract | Enum/type | Module | Consumption point |
  |---|---|---|---|
  | Coarse route shape | RouteShapeKind::{LoadScalar, AggregateCount, AggregateNonCount, AggregateGrouped,
  MutationDelete} | contracts.rs | execution/mod.rs, aggregate/load entrypoints |
  | Route mode | ExecutionMode::{Streaming, Materialized} | contracts.rs | aggregate/mod.rs, load/execute.rs |
  | Grouped strategy | GroupedExecutionStrategy::{HashMaterialized, OrderedMaterialized} | contracts.rs |
  grouped_route.rs |
  | Access class | AccessRouteClass | execution_contract.rs | route capability/pushdown derivation |

  C. Aggregate and reduction execution shapes

  | Shape | Contract | Module | Consumption |
  |---|---|---|---|
  | Aggregate kinds | AggregateKind::{Count, Sum, Exists, Min, Max, First, Last} | model.rs | aggregate
  terminals/reducers |
  | Fold mode | AggregateFoldMode::{KeysOnly, ExistingRows} | contracts.rs | kernel/reducer.rs |
  | Distinct strategy | DistinctExecutionStrategy::{None, PreOrdered, HashMaterialize} | model.rs | kernel/
  distinct.rs |

  D. Requested shapes present

  - FullScan, IndexPrefix, IndexRange, ByKey, ByKeys: implemented.
  - IndexLookup: represented as equality IndexPrefix paths (not a separate enum variant).
  - Grouped aggregation: implemented via RouteShapeKind::AggregateGrouped + grouped handoff.
  - Cursor pagination: implemented via continuation contracts and route continuation planning.
  - Projection: scalar projection is materialized projection (ProjectionSpec over rows); covering projection
    exists for field-value terminals.
  - Aggregate reducers: implemented via kernel reducer pipeline and terminal adapters.

  ———

  2. Missing pushdowns / fast paths

  A. Existing pushdowns/fast paths status

  | Feature | Implemented? | Module(s) | Execution shape |
  |---|---|---|---|
  | COUNT fast path | Yes (multiple paths) | aggregate/terminals.rs, route/fast_path.rs | AggregateCount
  streaming/materialized |
  | EXISTS fast path | Yes | aggregate/terminals.rs | index-covering stream |
  | Covering projection | Partial | aggregate/projection.rs | field terminals only (values_by,
  distinct_values_by, first/last value) |
  | Index-only scan / prefilter | Partial | index/predicate/compile.rs, index/scan.rs | index prefix/range
  scans |
  | Predicate pushdown | Partial | same as above + route strict/conservative compile policy | index predicate
  subset / all-or-none |
  | LIMIT early termination | Yes (shape-gated) | route/hints.rs, load/index_range_limit.rs | index-range
  limit pushdown |
  | ORDER BY index usage | Partial | route/pushdown.rs, execution_contract.rs | secondary prefix pushdown only
  |

  B. Missing/partial index-based optimizations (requested checks)

  | Feature | Status |
  |---|---|
  | EXISTS | Implemented |
  | MIN | Partial (strong for key-target; field-target has eligibility limits) |
  | MAX | Partial (field-target fast path more constrained than MIN) |
  | FIRST | Partial (bounded hints exist; not a generalized index-seek node for all shapes) |
  | LAST | Partial |
  | DISTINCT | Partial (row DISTINCT via key-stream; grouped/global DISTINCT constrained) |
  | ORDER BY | Partial (secondary prefix unique contract; index-range order pushdown explicitly rejected) |
  | ORDER BY + LIMIT | Partial (index-range pushdown exists; general top-N seek still materialized in row
  terminals) |
  | COUNT with predicates | Implemented but not pushed down in count fast path (falls back) |
  | Secondary-index IN queries | Partial (lowered to union of prefix paths, no dedicated multi-key physical
  path) |
  | Covering DISTINCT | Partial (field projection terminals only) |
  | Covering ORDER BY | Partial (terminal-specific covering projection order contract only) |
  | Index prefix scans | Implemented |
  | Multi-key lookup paths | Partial (PK ByKeys exists; no secondary multi-key lookup node) |

  ———

  3. Planner gaps

  | Capability | Current state | Gap before EXPLAIN stability |
  |---|---|---|
  | Secondary index selection | Implemented (deterministic best-index rules) | No explicit explainable
  selection rationale object (chosen/rejected candidates) |
  | Prefix matching | Implemented | No first-class execution node for “equality lookup vs prefix scan” split |
  | IN -> multi-key lowering | Implemented as union expansion | Missing dedicated multi-key secondary access
  shape |
  | BETWEEN -> range lowering | Partial (via Gte/Lte in AND) | No explicit Between operator in predicate model
  |
  | Projection covering analysis | Mostly executor-side terminal heuristics | Missing planner-owned covering/
  projection contract for general load projection |
  | Order satisfaction via index | Partial | Index-range pushdown path explicitly unsupported for secondary
  ORDER contract |
  | Limit pushdown | Partial | Limited to index-range eligibility; no general top-N seek planner contract |

  ———

  4. Pre-EXPLAIN feature roadmap

  SMALL FEATURES (0.41)

  - COUNT(predicate) strict index prefilter pushdown
      - Why EXPLAIN depends: adds a distinct execution shape (AggregateCount + IndexPredicatePrefilter) that
        should be represented from day one.
      - Modules: aggregate/terminals.rs, route/feasibility.rs, index/predicate/compile.rs
  - Field-target MAX bounded probe parity (where safe)
      - Why EXPLAIN depends: seek/probe route labels should not change right after EXPLAIN ships.
      - Modules: route/hints.rs, aggregate/field_extrema.rs
  - Index-range ORDER satisfaction micro-extension
      - Why EXPLAIN depends: currently index-range ORDER paths show as non-pushdown; adding this later would
        force explain node changes.
      - Modules: execution_contract.rs, route/pushdown.rs, kernel/post_access/mod.rs

  MEDIUM FEATURES (0.42)

  - Secondary-index IN pushdown as dedicated multi-key access path (medium)
      - Why EXPLAIN depends: replaces union-expanded plans with a new stable node.
      - Modules: planner compare/prefix + access path/plan + stream traversal
  - Covering DISTINCT beyond terminal helpers (medium)
      - Why EXPLAIN depends: introduces index-only distinct node(s) instead of materialized dedupe fallback.
      - Modules: aggregate/projection.rs, DISTINCT planning/executor kernels
  - Covering ORDER BY (medium)
      - Why EXPLAIN depends: needs explicit “sort eliminated by index order” node/flag.
      - Modules: route pushdown + post-access ordering + projection contracts
  - General ORDER BY + LIMIT seek (top-N without full materialization) (large)
      - Why EXPLAIN depends: adds a major execution operator; must be in vocabulary before EXPLAIN freeze.
      - Modules: route hints/planner, load terminal execution, stream layer
  - Projection pruning + partial covering projection for scalar loads (large)
      - Why EXPLAIN depends: changes from full-row materialization to selected-field/index-only execution
        paths.
      - Modules: planner projection lowering, executable plan descriptor, load projection materialization

  STRUCTURAL PRE-EXPLAIN REQUIREMENTS

  - Stabilize one executor-facing descriptor schema
      - Why EXPLAIN depends: EXPLAIN should read one stable contract, not infer from mixed structs.
      - Modules: query/explain/mod.rs, executor/executable_plan.rs
      - Required fields to add/clarify: explicit index_used, predicate, projection, covering_scan, optional
        rows_expected.
  - Unify query-level explain and route-level execution explain
      - Why EXPLAIN depends: query explain currently emits order_pushdown = MissingModelContext; execution
        explain has route truth.
      - Modules: same as above + route planner contracts
  - Freeze execution node naming + serialization
      - Why EXPLAIN depends: prevents post-0.43 output churn.

  OPTIONAL DEVELOPER COMMANDS

  - SHOW ENTITIES (missing)
      - Why useful pre-EXPLAIN: quick schema inventory for interpreting explain/index choices.
      - Modules: session API + schema registry surface
  - SHOW SCHEMA (missing)
      - Why useful pre-EXPLAIN: full model/index introspection alongside explain output.
      - Modules: session + describe/introspection
  - SHOW INDEXES (exists)
  - DESCRIBE ENTITY (exists)

  ———

  5. Final execution node vocabulary (target for EXPLAIN stability)

  Execution nodes:

  - ByKeyLookup
  - ByKeysLookup
  - PrimaryKeyRangeScan
  - IndexPrefixScan
  - IndexRangeScan
  - FullScan
  - Union
  - Intersection
  - IndexPredicatePrefilter
  - ResidualFilter
  - DistinctPreOrdered
  - DistinctMaterialized
  - OrderByAccessSatisfied
  - OrderByMaterializedSort
  - LimitOffset
  - CursorResume
  - ProjectionMaterialized
  - ProjectionIndexOnly
  - AggregateCount
  - AggregateExists
  - AggregateMin
  - AggregateMax
  - AggregateFirst
  - AggregateLast
  - AggregateSum
  - AggregateSeekFirst
  - AggregateSeekLast
  - GroupedAggregateHashMaterialized
  - GroupedAggregateOrderedMaterialized
  - IndexRangeLimitPushdown
  - SecondaryOrderPushdown

  -----------------------------------------------AUDIT AUDIT----------------------------------------

  1. What absolutely must land before EXPLAIN

These are the only items that materially change execution nodes.

If these land after EXPLAIN, you'll end up rewriting explain output.

A. COUNT(predicate) index prefilter pushdown

Scope: small

Changes:

aggregate/terminals.rs

index/predicate/compile.rs

route/feasibility.rs

Adds node:

IndexPredicatePrefilter

Estimated effort:

~1–2 days

Low planner risk.

B. Field-target MAX probe parity

Scope: small

You already have most machinery.

Changes:

route/hints.rs

aggregate/field_extrema.rs

Adds no new node — just more consistent use of

AggregateSeekLast

Estimated effort:

~1 day

C. Index-range ORDER satisfaction extension

Scope: small–medium

Currently:

IndexRange
 → materialized sort

Extend to:

IndexRange
 → OrderByAccessSatisfied

Modules:

execution_contract.rs

route/pushdown.rs

kernel/post_access

Estimated effort:

~2 days

2. The two medium items that SHOULD land before EXPLAIN

These affect planner output shape, so they are worth doing before EXPLAIN.

D. Secondary-index IN pushdown (multi-key lookup)

Current:

Union
  IndexPrefix
  IndexPrefix
  IndexPrefix

Better:

IndexMultiLookup

Changes:

Planner:

query/plan

Access layer:

AccessPath::IndexMultiLookup

Executor:

resolve_index_multi_lookup

Estimated effort:

~2–3 days

This is actually simpler than it looks because your PK version already exists (ByKeys).

E. ORDER BY + LIMIT top-N seek

Current:

IndexRange
 → materialize
 → sort
 → limit

Better:

TopNSeek

Modules:

route/hints.rs

execution_contract.rs

load execution terminal

Estimated effort:

~3–4 days

This is the largest remaining operator change.

3. Items you can safely postpone AFTER EXPLAIN

These do not require new execution nodes, so they won't break explain.

Safe to postpone:

Covering DISTINCT generalization

Explain still shows:

DistinctMaterialized

Even if later optimized.

Projection pruning for scalar loads

Explain still shows:

ProjectionMaterialized

You can later switch to:

ProjectionIndexOnly

without breaking explain schema.

4. Total realistic effort

If you focus only on the explain-blocking work:

Feature	Effort
COUNT predicate pushdown	1–2 days
MAX probe parity	1 day
Index-range order extension	2 days
Secondary IN pushdown	2–3 days
Top-N seek	3–4 days

Total:

~9–12 days of work

Which is very reasonable for a single release.

5. What 0.41 would represent

0.41 becomes the execution completeness release.

It would include:

Full access vocabulary
Full aggregate vocabulary
Index-aware ordering
Top-N index seek
Multi-key index lookup
Predicate index prefilter

That means the engine execution model is basically complete.

6. Then 0.42 can be pure EXPLAIN

That is a very clean milestone.

0.42 would only include:

Explain infrastructure

Modules:

query/explain
executor/explain
Stable execution descriptor

Single canonical structure:

ExecutionNodeDescriptor

Fields like:

node_type
index_used
predicate_pushdown
residual_predicate
projection
covering_scan
rows_expected
children
Output formats

You can support:

EXPLAIN
EXPLAIN VERBOSE
EXPLAIN JSON

later.

7. Why this ordering is ideal

Doing EXPLAIN after execution vocabulary stabilizes means:

You never have to change:

node names
node hierarchy
pushdown flags

This saves enormous maintenance pain.

Many databases regret not doing this.

8. Strategic impact

If you land EXPLAIN after these improvements, IcyDB immediately gains:

Professional debugging surface

Users will see:

AggregateCount
  IndexPredicatePrefilter(user_email_idx)
  IndexRangeScan
Planner transparency

People can see:

IndexRangeScan
  OrderByAccessSatisfied

instead of guessing.

Regression protection

Explain trees can become planner snapshot tests.

Extremely powerful.

9. My recommendation

Yes — do exactly this:

0.41  → execution completeness
0.42  → explain

That will make 0.42 look like a major capability jump even though the heavy lifting happened in 0.41.

10. Final honest assessment

Your current architecture is already extremely strong.

Once EXPLAIN lands, the system will reach the level where it becomes much easier to evolve safely.

Right now you're at the stage where the engine is powerful but opaque.

EXPLAIN fixes that permanently.