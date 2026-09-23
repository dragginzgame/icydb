# IcyDB Resource Model (Internet Computer)

**Status:** Authoritative (current baseline; resource model introduced in `0.36`)

This is the documentation home for resource scope and limits. Compiled input,
admission, request and execution owners enforce them; prose is not runtime
policy. All query execution paths must conform to this model.

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

Grouped state has explicit accounting limits:

- `max_groups`
- `max_group_bytes`
- `max_distinct_values_per_group`
- `max_distinct_values_total`

`max_group_bytes` caps the total accounted live grouped state, not bytes per
group. These estimates are not measurements of the complete Wasm heap or
allocator overhead. Request/execution byte counters separately bound charged
construction, decoding and materialization work.

### 2.2 Instruction Cost (`I`)

Instruction execution cost per call. Major contributors include:

- Key traversal
- Row decode
- Predicate evaluation
- Aggregate fold updates
- Hash and DISTINCT bookkeeping

Finite per-execution and cumulative request budgets charge named work and
sample IC instruction usage at maintained boundaries. Scan/page bounds and
grouped-state limits are additional controls, not replacements for them.
One group can contain many input rows: a group-count cap does not bound all
row visits, predicate evaluations or aggregate fold operations.

Instruction sampling is not continuous, and construction estimates are not IC
instruction measurements. Failure headroom reserves capacity to return typed
exhaustion; these controls do not prove that every intervening allocation or
operation is individually bounded below the replica's hard limit.

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

### Public Read And Input Ceilings

The following table is checked against the compiled default admission policy
and shared query-input constants. Byte values are exact bytes. These are
admission/input ceilings, not the larger internal execution profiles below.
They are not caller-selectable modes.

<!-- icydb-read-resource-limits:start -->

| Limit | Value | Scope |
| --- | ---: | --- |
| `max_returned_rows` | 100 | Public returned-row bound; scalar pages supply an envelope |
| `max_primary_key_input_terms` | 1024 | Public primary-key predicate input |
| `max_primary_key_input_bytes` | 65536 | Public primary-key predicate input bytes |
| `max_groups` | 100 | Maximum public grouped-query group limit |
| `max_group_bytes` | 65536 | Maximum public grouped-query total live-state byte limit |
| `max_input_depth` | 128 | Authored expression/value nesting |
| `max_input_nodes` | 4096 | Counted nodes across query components |
| `max_input_bytes` | 2097152 | Counted variable payload across query components |

<!-- icydb-read-resource-limits:end -->

Grouped calls must supply positive group and memory limits within the public
ceilings. Their byte limit is shared across retained groups and states.
Trusted reads bypass public shape policy, not query-input or execution limits.
For scalar pages, an authored `LIMIT` restricts the whole traversal, not each
page. See [read admission](READ_ADMISSION.md) for eligibility and diagnostics.

DISTINCT execution counts are derived from the grouped byte and group limits
by the grouped accounting owner: the per-group cap is the byte limit divided
by 64 (with a minimum of one), and the total cap is that count multiplied by
the group limit with saturating arithmetic. Live-memory and request/execution
caps also apply and can reject before either count cap is reached.

### Request And Execution Budgets

| Authority | Scope and responsibility | Compiled owner |
| --- | --- | --- |
| Input admission | Authored query components, including effective SQL parameter copies; not planner-produced work | [input](../../crates/icydb-core/src/db/query/admission/input.rs) |
| Public admission | Selected-plan row, access, key-input and grouped-policy proofs | [policy](../../crates/icydb-core/src/db/query/admission/policy.rs) |
| Request root | Monotonic preparation and attached execution charges across nested calls and async polls | [request](../../crates/icydb-core/src/db/session/request.rs) |
| Read execution | One prepared execution's physical work, attached to its request scope | [execution budget](../../crates/icydb-core/src/db/executor/budget.rs) |
| Mutation advancement | Separate fixed engine-owned profile and explicit pre-publication instruction checks | [execution budget](../../crates/icydb-core/src/db/executor/budget.rs) |
| Grouped state | Live group memory plus cumulative group/DISTINCT work | [grouped accounting](../../crates/icydb-core/src/db/executor/aggregate/contracts/grouped/context.rs) |

Named resources include query/planning work, key and row visits, stored/decoded/
materialized bytes, expression/value steps, sort work and scratch, group/DISTINCT
state, cursor work, temporary bytes, result rows/bytes and instruction units.
Current internal profile values and failure reserves live in the linked owners;
they are not a second public tuning API.

Generated endpoints establish a request root automatically. Manual entries use
the [request scope](../guides/public-facade-api.md#request-entry). Re-entering a
helper, deriving another session, catching an error or resuming after `await`
does not reset that root's counters. An outbound call does not share the root
with another canister. Native counters do not measure IC instructions.

### Preparation Accounting And Coverage

Construction charges apply before the maintained allocation/copy boundaries,
on cold and warm calls and on public and trusted reads. Reservations may be
conservative: discarded candidates can leave unused admitted work or capacity.
Limits do not imply every implementation step has complete accounting.

- Typed/dynamic clause copies, accepted predicate materialization, grouped
  destination vectors and expression-to-predicate lowering use the existing
  request's work/temporary-byte authority.
- Filtered plan-cache hits still normalize current operands and derive identity.
  Filterless early hits skip that normalization, but cache-key lookup and warm
  lifecycle validation still charge their instruction intervals. Hits are not
  a budget bypass. See the [cache owner](../../crates/icydb-core/src/db/session/query/cache.rs).
- Key/fingerprint/continuation construction admits retained backing and supported
  value copies. Only complete successful identities or plans publish. Exhaustion
  cannot become a placeholder fingerprint, missing predicate or absent candidate.
- Access planning admits maintained candidate lists, child dispatch, selected
  operand copies and bound construction. Optional compilation preserves the
  distinction between unsupported input and resource exhaustion. Successful
  cache/memo reuse skips work it no longer performs, not work still required.
- Cold accepted-runtime and inspection-plan construction has its own admitted
  boundaries. A shared request profile is not proof of complete schema compilation,
  journal replay or publication accounting. Atomic candidate publication and
  independent persisted-data verification remain required.

Full allocation/stack bounds for boolean normalization, payload comparisons,
schema lookup/validation, proof/scoring internals and parser/normalizer scratch
remain separately qualified work. Endpoint decoding and the final application
response also remain application responsibilities. Partial construction guards
must not be described as a whole-pipeline bound. The pinned-toolchain lowercase
expansion allowance must be requalified on toolchain changes.

Detailed historical construction changes and their original measurement scope
remain in the [0.257](../changelog/0.257.md) and [0.261](../changelog/0.261.md)
notes. They do not replace current code or certify unmeasured paths.

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
- Constraint-activation validation pages

Required guardrails:

- `groups <= max_groups`
- `estimated_group_bytes <= max_group_bytes` (as computed by conservative accounting)
- `distinct_per_group <= max_distinct_values_per_group`
- `distinct_total <= max_distinct_values_total`

Failure mode:

- Deterministic typed error
- No silent truncation
- No spill fallback

Constraint-activation pages use a separate bounded workflow contract rather
than the grouped-query limits above. Each page caps rows, decoded bytes,
findings, and staged unique-index or reverse-relation work and carries an exact
continuation checkpoint. Reaching a cap leaves the job incomplete; it never
promotes a constraint or fabricates successful validation.

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

- `groups_observed <= max_groups`
- `current_live_group_bytes <= max_group_bytes` (as computed by conservative accounting)
- `distinct_per_group <= max_distinct_values_per_group`
- `distinct_values_observed <= max_distinct_values_total`

Group and total-DISTINCT counters remain cumulative work bounds. Group bytes
are a live-memory bound: hash execution retains its admitted group table,
whereas ordered streaming releases each active group at its proven key
transition. The budget records peak live groups, aggregate states, DISTINCT
values, and estimated bytes for diagnostics without weakening cumulative caps.

Grouped DISTINCT insertions pass through the authoritative
[grouped budget accounting](../../crates/icydb-core/src/db/executor/aggregate/contracts/grouped/context.rs).
This includes global DISTINCT field aggregates modeled as grouped execution
with zero group keys.

Outside grouped execution,
[scalar aggregate reducers](../../crates/icydb-core/src/db/executor/aggregate/scalar_terminals/reducer.rs)
own DISTINCT value sets, while
[projected-row DISTINCT](../../crates/icydb-core/src/db/executor/projection/materialize/distinct.rs)
owns adjacent deduplication or global replay. These paths charge execution
budgets for DISTINCT entries and retained state; they do not obtain their
limits from grouped-query configuration.

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
- No grouped DISTINCT domain that crosses the active canonical group boundary

Planner may propose ordered grouping; executor revalidates and downgrades to
hash grouping when any eligibility condition fails.
Executor revalidation must never upgrade execution beyond planner-declared
eligibility.

Grouped execution mode remains explicit and authoritative at runtime.
Eligible generic and dedicated grouped-`COUNT(*)` routes use
`OrderedStreaming`: they share one transition owner, retain one active group,
finalize it only at a proven canonical group-key transition, and keep only
bounded response-page state. Incompatible routes use `HashMaterialized` and
retain the budget-accounted group table. Grouped field-target DISTINCT remains
explicitly hash materialized.

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

## 11. Entity Execution Metrics

The optional `metrics` capability records replicated entity execution in one
heap-only accumulator. Each accepted entity reports observed execution spans,
their saturating total local instructions, and the largest local instruction
delta. Report collection must not alter planner, routing, storage, or execution
semantics.

IC query calls do not retain heap mutations, so entity metrics deliberately do
no recording in query execution. Canic remains the authority for endpoint-level
query and update cost. The entity report does not duplicate route, cache,
row-flow, query-shape, or endpoint attribution.
