# IcyDB Resource Model (Internet Computer)

**Status:** Authoritative (current baseline; resource model introduced in `0.36`)

All query execution paths must conform to this model.

## 1. Purpose

IcyDB runs on the Internet Computer (IC), which imposes strict runtime constraints:

- No local disk spill
- Metered heap memory
- Bounded instruction execution per call
- Deterministic execution requirements

This document defines the resource model and operator classification used to keep
execution bounded and deterministic.

A query shape is valid only if its worst-case runtime behavior is bounded by
explicit contracts.

## 2. Resource Dimensions

### 2.1 Heap Memory (`M`)

Heap memory consumed during query execution. This includes:

- Group state
- Aggregate state
- DISTINCT sets
- Hash tables
- Key buffers
- Row decode buffers

Constraint (grouped runtime path):

`M <= M_max`

Where `M_max` is represented by explicit grouped execution limits:

- `max_groups`
- `max_group_bytes`
- `max_distinct_values_per_group`
- `max_distinct_values_total`

### 2.2 Instruction Cost (`I`)

Instruction execution cost per call. Major contributors include:

- Key traversal
- Row decode
- Predicate evaluation
- Aggregate fold updates
- Hash and DISTINCT bookkeeping

Constraint:

`I <= I_max`

Enforcement is contract-based, not one universal scalar cap. Current controls:

- Route-derived load scan-budget hints for eligible scalar load shapes
- Grouped memory and DISTINCT caps that bound grouped hash/distinct work

All Class B operators must ensure the same cardinality caps that bound memory
also bound hash, DISTINCT, and fold operation counts, so worst-case instruction
growth remains proportional to capped structures.

### 2.3 Stable Memory Growth (`S`)

Persistent memory growth during execution.

- Read/query paths: `S_growth = 0`
- Write/mutation paths: bounded by mutation contracts and committed writes

Stable memory growth must remain explicit and bounded by operation semantics.

### 2.4 Boundedness Authorities

Boundedness is enforced by two distinct authorities:

- Planner proof (shape admission): the planner must prove a query shape is bounded
  before admitting it into execution.
- Runtime caps (enforcement backstops): runtime counters and hard limits enforce
  boundedness during execution and fail deterministically when exceeded.

Runtime caps are necessary but are not a substitute for planner proof. A shape
without planner-bounded admission must not be classified as Class A only because
runtime caps exist.

## 3. Operator Classification

### 3.1 Class A: Structurally Bounded Operators

Definition:

Memory usage depends on plan structure, not unbounded data cardinality.

Examples:

- Scalar predicates
- Key-range scans
- Aggregates with fixed-size state

These require no extra cardinality growth structures beyond fixed operator
state.

Class A allocations must be structurally bounded or window-bounded by an
explicit plan-admission bound.

Local `Vec`/set allocations are Class A only when their maximum size is proven by
plan shape (for example fixed operator structure) or by explicit admitted window
bounds (for example planner-admitted `LIMIT`/fetch window). Otherwise they are
Class B or Class C depending on enforced bounds.

### 3.2 Class B: Cardinality-Bounded Operators

Definition:

Memory usage depends on runtime cardinality, but is guarded by explicit caps.

Examples:

- Hash grouped execution
- Grouped DISTINCT aggregates
- Global DISTINCT field aggregates (`COUNT(DISTINCT field)`, `SUM(DISTINCT field)`)
- DISTINCT sets within grouped aggregation

Required guardrails:

- `groups <= max_groups`
- `estimated_group_bytes <= max_group_bytes` (as computed by conservative accounting)
- `distinct_per_group <= max_distinct_values_per_group`
- `distinct_total <= max_distinct_values_total`

Failure mode:

- Deterministic typed error
- No silent truncation
- No spill fallback

### 3.3 Class C: Unbounded Operators (Disallowed)

Definition:

Memory or instruction cost scales with full input size without enforceable
runtime bounds.

Examples:

- Global unbounded materialization shapes
- Unbounded hash joins without caps
- Window functions over unbounded partitions

Class C operators are disallowed unless rewritten into bounded forms.

## 4. Grouped Execution Resource Contract

For grouped queries, resource admission and execution must enforce:

- `groups <= max_groups`
- `estimated_group_bytes <= max_group_bytes` (as computed by conservative accounting)
- `distinct_per_group <= max_distinct_values_per_group`
- `distinct_total <= max_distinct_values_total`

Distinct insertions must pass through grouped budget accounting.
Budget enforcement over these counters is authoritative.
This includes global DISTINCT field aggregates modeled as grouped execution with
zero group keys.
Non-grouped scalar DISTINCT projection helpers (for example
`count_distinct_by(field)` / `distinct_values_by(field)`) are effective-window
materialized terminals and are not grouped Class B operators. Their DISTINCT
key admission is owned by the materialized helper boundary
`executor::aggregate::materialized_distinct`.

All cardinality-sensitive state must be reachable exclusively through
budget-accounted structures.

Typed grouped failures are part of the contract surface.

## 5. Grouped Strategy Eligibility Contract

`OrderedGroup` strategy eligibility is permitted only when all grouped
eligibility conditions hold. Current planner+executor matrix includes:

- Ordered strategy hint present
- Direction compatibility with access capabilities
- Streaming-safe access shape
- Streaming-compatible grouped aggregates
- Streaming-compatible HAVING operators
- Streaming-compatible grouped DISTINCT shape

Planner may propose ordered grouping; executor revalidates and downgrades to
hash grouping when any eligibility condition fails.
Executor revalidation must never upgrade execution beyond planner-declared
eligibility.

Grouped execution mode remains explicit and authoritative at runtime.
In the current line, grouped execution remains materialized for both ordered
and hash strategy labels.

## 6. Scan Budget Contract

For eligible scalar load routes, scan budgeting is applied by hinting a bounded
key fetch/scan budget and enforcing it through budgeted key-stream traversal.

Current constraints:

- Scan-budget hints are shape-gated (non-continuation, streaming-safe scalar
  load paths)
- Grouped routes do not currently consume the scalar load scan-budget hint path
- Budgeting is fail-closed when eligibility preconditions are violated

This keeps budget behavior explicit and route-authoritative.

## 7. Continuation Invariants

For grouped pagination:

- Group emission is atomic per group
- Resume continues strictly after the last emitted group key
- Grouped continuation tokens are versioned and signature-scoped
- Cursor signatures must diverge when grouped shape diverges
  (for example: group keys, aggregate structure, DISTINCT modifiers, HAVING
  shape, grouped strategy/budget-relevant shape)

Invalid grouped continuation payloads fail deterministically with typed cursor
errors.

## 8. Deterministic Failure Principle

If a resource bound is exceeded:

- Execution fails deterministically
- Error type is explicit
- No silent degradation, spill, or approximation

IcyDB does not auto-materialize to disk, silently truncate, or approximate
results to satisfy resource pressure.

## 9. Architectural Ceiling

IcyDB operates within:

- Class A and Class B operators only
- Explicit resource caps on cardinality-sensitive structures
- No disk spill
- No distributed execution coordination assumptions
- Rule-based planner with executor revalidation gates

## 10. Constitutional Rule

Any new operator must provide a proof sketch that its worst-case memory and
instruction behavior is bounded by explicit, enforceable limits.

If such bounds cannot be stated and enforced, the operator is disallowed.

## 11. Execution Metrics Counters

Runtime observability is additive and must not affect query behavior.

Current row-flow counters emitted through the metrics sink/report surface:

- `rows_scanned`: candidate rows read by execution paths
- `rows_filtered`: rows dropped between scan and emitted output
- `rows_aggregated`: rows folded by grouped aggregation paths
- `rows_emitted`: rows emitted to response payloads

These counters are diagnostics surfaces only. They must remain side-effect-free
and must never alter planner, routing, or execution semantics.
