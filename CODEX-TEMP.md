### LLMs PLEASE IGNORE THIS FILE
### It's just here so I can manage multiple prompts without scrolling up and down constantly


1. LIMIT as a First-Class Plan Operator (if not already)

Even if your executor stops early internally, it’s useful to make LIMIT explicit in the plan.

Example plan shape:

AccessStrategy: IndexRange(email)
Limit: 10

Benefits:

EXPLAIN can show it clearly

planner can push it down later

enables future optimizations like:

ORDER BY + LIMIT → index seek

Implementation is small:

Plan:
  limit: Option<u32>
Executor:
  break after N rows
2. EXISTS Fast Path (as discussed)

This affects execution shape and therefore EXPLAIN output.

Without fast path:

IndexLookup
→ RowFetch
→ Exists

With fast path:

IndexLookup (exists-fast-path)

If you implement EXPLAIN first, you’ll have to change its output later.

3. MIN/MAX Index Seek

These are signature database optimizations and show up prominently in EXPLAIN.

Example:

SELECT MAX(created_at)

Without optimization:

IndexRangeScan
→ Aggregate

With optimization:

IndexSeekLast

This changes the plan shape, so it’s good to land before EXPLAIN.

4. DISTINCT Covering Optimization

This also changes execution structure:

Without optimization:

IndexRangeScan
→ HashDistinct

With optimization:

IndexRangeScan (dedupe)

Even if DISTINCT isn’t heavily used yet, it’s a meaningful plan difference.

5. Stable Execution Descriptor (Important for EXPLAIN)

Before building EXPLAIN, ensure you have a single canonical object describing execution.

Something like:

ExecutionDescriptor {
  access_strategy
  covering_projection
  aggregation
  ordering_source
  limit
  cursor
}

Then EXPLAIN just prints this descriptor.

If this abstraction isn’t stable yet, EXPLAIN becomes messy.

6. EXPLAIN ANALYZE Primitives

Even if you don’t ship ANALYZE yet, ensure the executor can collect:

rows_scanned
rows_materialized
index_only
execution_time

You already have record_rows_scanned, so you’re halfway there.

This allows EXPLAIN to evolve naturally into:

EXPLAIN ANALYZE

later.

7. Debug Formatting for AccessStrategy

This is a small but helpful step.

Example debug output:

IndexRange(email prefix="foo")
IndexLookup(pk=123)
FullScan

EXPLAIN can reuse this.

8. SHOW INDEXES / Introspection Command (Optional but Nice)

Before EXPLAIN, having some schema introspection is helpful.

Example command:

SHOW INDEXES User

Output:

PRIMARY KEY (id)
INDEX email
INDEX created_at

Very small feature but improves usability.

9. DESCRIBE ENTITY / Schema Command

If you don’t already have it:

DESCRIBE User

Returns:

fields
indexes
relations

Helps developers understand what EXPLAIN is referring to.

10. Query Fingerprint / Plan Hash

You already have predicate fingerprinting.

A plan fingerprint would be useful for:

query cache
EXPLAIN comparison
debugging

Example:

PlanHash: 0x7e92a1d4

Optional but useful.

11. Optional Developer Command: TRACE

Some engines add a simple trace mode:

TRACE QUERY ...

Outputs:

planner decisions
pushdown eligibility
selected index



---------------------------- AFTEr --------------

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