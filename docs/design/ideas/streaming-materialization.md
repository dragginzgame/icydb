# IcyDB Design Note — Streaming Execution, Key-First Pipelines, and Materialization Boundaries

## Status

Proposed / TODO

## Purpose

This note captures the long-term execution strategy for IcyDB query planning and execution.

The public API can continue returning:

```text
rows + cursor
```

because that is the right shape for canister calls, HTTP-like endpoints, UI pagination, bounded responses, and application ergonomics.

Internally, however, the executor should avoid treating that public API shape as the execution model.

The target internal model is:

```text
query plan
  → resumable key/row streams
  → late row loading
  → residual filtering/projection
  → materialize only at real semantic boundaries
  → page + cursor
```

The goal is to prevent accidental full materialization, enable index intersection, prepare for future joins, and keep IcyDB performant as query complexity grows.

---

# Summary

IcyDB should distinguish two layers:

## Public/API boundary

```rust
QueryResult<T> {
    rows: Vec<T>,
    cursor: Option<Cursor>,
}
```

This is good and should remain the normal user-facing shape.

## Internal execution boundary

Internally, query execution should prefer resumable streams:

```text
next candidate key
next row view
next projected output
```

rather than eagerly collecting intermediate `Vec`s.

The executor should materialize only when semantics require it, such as:

* response page construction
* cursor token construction
* `ORDER BY`
* `DISTINCT`
* `GROUP BY` / aggregation state
* hash join build side
* sort-merge buffering where needed
* top-k heap
* deduplication sets

Everything else should prefer streaming, borrowed access, key streams, row views, visitors, or bounded batches.

---

# Design Principle

The core rule:

```text
Do not allocate ownership before the query semantics require ownership.
```

Or more concretely:

```text
Access paths should produce keys first where possible.
Rows should be loaded only after candidate keys survive key-level pruning.
Projection should borrow until the page/output boundary.
```

---

# Target Architecture

The desired internal execution spine is:

```text
PhysicalPlan
  ↓
AccessSource
  ↓
CandidateKeyStream
  ↓
Key-level operators
  ↓
RowLoader
  ↓
RowView stream
  ↓
ResidualFilter
  ↓
Projection
  ↓
Blocking operators where required
  ↓
PageBuilder
  ↓
Vec<ResultRow> + Cursor
```

Expanded:

```text
AccessSource
  - ByKey
  - PrimaryRange
  - SecondaryIndexRange
  - FullScan

CandidateKeyStream
  - yields entity tag + primary key / row identity
  - can resume from cursor state
  - can advance/seek where supported

Key-level operators
  - intersection
  - union
  - difference
  - deduplication
  - key-limit where legal
  - semi-join / anti-join foundations

RowLoader
  - loads RawRow only for surviving candidate keys
  - produces borrowed RowView where possible

Row-level operators
  - residual filter
  - expression evaluation
  - projection
  - aggregation
  - ordering
  - distinct

PageBuilder
  - owns final response rows
  - constructs continuation cursor
```

---

# Why Key-First Matters

For many indexed queries, the engine does not need rows immediately.

Example:

```rust
.load::<MovementIntent>()
.filter(session_id.eq(s))
.filter(turn_number.eq(t))
.filter(status.eq(Open))
```

If the planner has usable indexes, it may produce candidate primary keys first.

A key-first executor can do:

```text
index session_id = s      → primary keys
index turn_number = t     → primary keys
index status = Open       → primary keys
intersect keys
load rows only for surviving keys
apply residual filters
project
return page
```

Instead of:

```text
scan/load rows
filter rows
collect rows
filter again
project
return
```

This becomes increasingly important for:

* multi-index filtering
* index intersection
* future joins
* existence checks
* semi-joins
* anti-joins
* pagination
* stable-memory cost control
* deterministic instruction budgeting

---

# Example: Index Intersection

Given three ordered key streams:

```text
session_id = S1:
  [10, 12, 20, 25, 40]

status = Pending:
  [3, 12, 19, 20, 41]

turn_number = 7:
  [12, 20, 30, 99]
```

The intersection is:

```text
[12, 20]
```

The executor should be able to find this by advancing ordered streams, without collecting all candidates into owned vectors.

Conceptual algorithm:

```rust
loop {
    let target = max(current_key(a), current_key(b), current_key(c));

    a.advance_to(target);
    b.advance_to(target);
    c.advance_to(target);

    if all_current_keys_equal() {
        yield target;
        advance_all();
    }
}
```

This requires streams that support either:

```rust
next()
```

or preferably:

```rust
advance_to(key)
```

The latter is crucial for efficient intersection.

---

# Important Internal Trait Direction

Do not prematurely force everything into Rust’s standard `Iterator`.

A custom trait is probably better because IcyDB has:

* stable-memory cursors
* borrowed row views
* continuation tokens
* seek/advance requirements
* deterministic execution budgeting
* possible resumable state machines
* different row/key lifetimes

Possible internal traits:

```rust
trait CandidateKeySource {
    fn peek_key(&self) -> Option<&CandidateKey>;

    fn next_key(&mut self) -> Result<Option<CandidateKey>, QueryError>;

    fn advance_to(&mut self, target: &CandidateKey)
        -> Result<Option<CandidateKey>, QueryError>;

    fn cursor_state(&self) -> CursorFragment;
}
```

```rust
trait RowSource<'a> {
    fn next_row(&'a mut self) -> Result<Option<RowView<'a>>, QueryError>;

    fn cursor_state(&self) -> CursorFragment;
}
```

A batch version may be useful later:

```rust
trait KeyBatchSource {
    fn next_key_batch(&mut self, limit: usize)
        -> Result<KeyBatch, QueryError>;
}
```

But the first design goal should be a correct resumable stream abstraction, not a full vectorized engine.

---

# Volcano vs Vectorized vs IcyDB

IcyDB does not need to copy a textbook Volcano executor exactly.

## Volcano model

```text
operator.next() -> row
```

Pros:

* simple
* composable
* streaming-friendly
* good for OLTP-style queries

Cons:

* per-row call overhead
* awkward borrowing/lifetimes in Rust
* may underuse batch/cache locality

## Vectorized model

```text
operator.next_batch() -> batch
```

Pros:

* faster for analytical scans
* lower call overhead
* better cache/SIMD behavior

Cons:

* more complex
* may over-materialize for canister/query workloads
* less necessary for small bounded pages

## IcyDB target

IcyDB should prefer:

```text
resumable pull-based key/row streams
```

with the option to add small bounded batches later.

The important goal is not “be Volcano.” The important goal is:

```text
defer ownership and materialization until required.
```

---

# Legitimate Materialization Boundaries

Materialization is acceptable and often required at real semantic boundaries.

## Required or defensible boundaries

```text
API response page
cursor token construction
ORDER BY sorting
DISTINCT retained key set
GROUP BY / aggregate state
hash join build side
sort-merge join buffering
top-k heap
deduplication set
set operation state
```

## Suspicious boundaries

These should be audited and avoided where possible:

```text
collect all candidate rows before filtering
collect all rows before LIMIT
collect full rows when only primary keys are needed
collect projected values before page boundary
collect all index candidates before intersection
collect full rows before residual predicates
collect groups before bounded/top-k reduction where streaming works
```

The audit question for every `Vec` should be:

```text
Is this Vec required by query semantics, or is it convenience plumbing?
```

---

# Target Physical Operators

IcyDB should eventually have clear physical operator categories.

## Access operators

```text
ByKeyLookup
PrimaryRangeScan
SecondaryIndexScan
FullScan
```

These should ideally yield candidate keys first.

## Key-level operators

```text
KeyIntersection
KeyUnion
KeyDifference
KeyDedup
KeyLimit
KeySortMerge
```

These operate before row loading.

## Row-loading operator

```text
RowLoader
```

Converts surviving candidate keys into row views.

## Row-level operators

```text
ResidualFilter
Projection
ExpressionEval
AggregateFold
Distinct
OrderBy
Limit
```

## Output operator

```text
PageBuilder
```

Owns final output materialization and cursor construction.

---

# Access Path Contract

The planner should classify physical access paths by what they can produce.

Example:

```rust
enum AccessOutput {
    CandidateKeys,
    RowViews,
    MaterializedRows,
}
```

Preferred:

```text
Index scans produce CandidateKeys.
Primary scans may produce CandidateKeys or RowViews depending on path.
Full scans may produce RowViews, but should still avoid owned rows where possible.
```

The planner should prefer the cheapest legal output form.

For example:

```text
COUNT(*) with index coverage
```

may not need row loading at all.

```text
EXISTS(...)
```

may only need the first matching candidate.

```text
SELECT id WHERE indexed_predicate
```

may need only candidate keys.

---

# Cursor Model

The public cursor must remain stable and compact, but internal streams need cursor fragments.

A cursor should be constructed from the actual physical progress point, not from the final `Vec`.

Possible cursor fragments:

```text
primary scan last key
secondary index scan last index key
intersection child stream states
order-by spill/top-k state, if supported
projection offset/state, if needed
```

For simple scans:

```text
cursor = last consumed RawDataStoreKey or RawIndexStoreKey
```

For intersections:

```text
cursor = child stream cursor states + current intersection state
```

For blocking operators like `ORDER BY`, the cursor story may require materialized state or different constraints.

The design must explicitly distinguish:

```text
streamable cursors
```

from:

```text
blocking operator cursors
```

---

# Relationship to Existing RowView / Materialization Work

Previous IcyDB work already moved toward borrowed projection materialization with `RowView`-style access.

That direction is correct.

The next step is to ensure the executor is not merely:

```text
row-stream first
```

but also:

```text
key-stream first
```

Where possible, the execution order should be:

```text
candidate keys
  → key pruning/intersection
  → row load
  → residual row predicates
  → projection
  → page materialization
```

This avoids decoding/loading rows that are eliminated by other indexed predicates.

---

# Index Intersection Strategy

Index intersection should be treated as a first-class future planner strategy.

## Required capabilities

Each index candidate stream should support:

```text
peek current primary key
advance next
advance_to primary key
resume from cursor
```

## Required ordering

To intersect efficiently, streams must yield compatible primary-key ordering.

This means secondary index scans need to expose their primary-key suffix ordering clearly.

If an index scan is ordered by:

```text
index components + primary key suffix
```

then within a fixed component range, candidates are naturally ordered by primary key suffix.

For broader ranges, the planner must verify whether output ordering is still compatible with intersection.

## Planner decision

The planner should choose intersection only when:

* multiple usable indexes exist
* candidate streams can produce compatible primary-key ordering
* predicates are selective enough to justify it
* `advance_to` or efficient seeking exists
* residual row loading is more expensive than key-level pruning

---

# Joins Strategy

This note does not require implementing joins now, but the execution model should not block them.

Future join operators should be expressible as stream composition:

```text
NestedLoopJoin
IndexNestedLoopJoin
MergeJoin
HashJoin
SemiJoin
AntiJoin
```

The key-first model helps with:

```text
IndexNestedLoopJoin:
  outer row/key stream
  → seek inner index
  → yield matches

MergeJoin:
  ordered left stream
  ordered right stream
  → advance lower side

SemiJoin:
  yield outer row if inner existence stream has match

AntiJoin:
  yield outer row if inner existence stream has no match
```

This is why a resumable internal execution model matters even before joins are exposed publicly.

---

# LIMIT Pushdown

`LIMIT` should be pushed as far down as semantics allow.

Good:

```text
IndexScan → Limit → RowLoader → Projection
```

Suspicious:

```text
IndexScan → collect all rows → Projection → Limit
```

However, `LIMIT` cannot always be pushed through:

* `ORDER BY`
* `DISTINCT`
* `GROUP BY`
* joins that change multiplicity
* filters that may reject rows after loading

The planner should have explicit rules for safe limit pushdown.

---

# Projection Strategy

Projection should avoid owning `Vec<Value>` until the final output boundary.

Preferred:

```text
RowView + ProjectionPlan
```

then:

```text
PageBuilder owns projected result row
```

Suspicious:

```text
project every candidate into owned Vec<Value>
then apply LIMIT/page
```

Projection should only load fields needed by:

* selected output
* residual filters
* ordering
* grouping
* cursor stability, if required

---

# Aggregation Strategy

Aggregations are often real materialization boundaries, but not always full-row boundaries.

Good:

```text
stream rows/values into aggregate state
```

Avoid:

```text
collect all rows
then aggregate
```

Aggregate state should retain only what the aggregate requires.

Examples:

```text
COUNT(*)        → counter only
MIN/MAX indexed → maybe key/index-only if legal
SUM/AVG         → numeric state
GROUP BY        → hash/sorted group state
```

Grouped aggregation may require materialization of group state, but not necessarily full input rows.

---

# ORDER BY Strategy

`ORDER BY` is usually a blocking boundary unless the access path already provides compatible ordering.

Planner should classify:

```text
ORDER BY satisfied by primary key order
ORDER BY satisfied by secondary index order
ORDER BY requires sort
```

If satisfied by index order:

```text
IndexScan → Limit → RowLoader → Projection
```

If not:

```text
Input stream → TopK heap if LIMIT exists
```

rather than always full sorting.

Full sort is acceptable only when semantics require it.

---

# DISTINCT Strategy

`DISTINCT` requires retaining seen keys/values, but should retain the smallest canonical representation possible.

Prefer:

```text
canonical distinct key set
```

over:

```text
full materialized rows
```

Where projection is simple and canonicalization can occur on borrowed values, avoid full row ownership before distinct comparison.

---

# FullScan Strategy

`FullScan` is sometimes unavoidable, but it should still stream.

Bad:

```text
load all rows into Vec
filter
project
page
```

Better:

```text
scan row views
apply residual filter
project into page until page full
return cursor
```

Even full scans should respect page boundaries and avoid loading the entire entity.

---

# Public API Guidance

Do not expose low-level `queryIter` as the main public API yet.

The public API should remain:

```text
query -> rows + cursor
```

because it is ergonomic and safe.

Possible future advanced/internal API:

```rust
query.iter()
```

or:

```rust
query.stream()
```

But only after:

* cursor semantics are stable
* lifetime model is clean
* execution budgeting is clear
* row borrowing is safe
* error handling is defined
* mutation interaction is defined

Until then, the iterator model should be internal.

---

# Audit TODO

Perform a repository-wide materialization audit.

## Search targets

Search for:

```text
collect::<Vec
Vec<
Materialized
rows.push
values.push
into_iter().collect
scan_all
load_all
projected_rows
candidate_rows
candidate_keys
RowView
RawRow
PrimaryKey
Cursor
```

## Classify every materialization

Each allocation should be classified as:

```text
REQUIRED_BOUNDARY
AVOIDABLE_CONVENIENCE
LEGACY_MATERIALIZATION
TEST_ONLY
SMALL_BOUNDED_OK
```

## Required questions

For each materialization point:

```text
What semantic boundary requires ownership?
Could this be a stream?
Could this be a borrowed RowView?
Could this be key-only?
Could LIMIT stop earlier?
Could a cursor be built from the last consumed key?
Does ORDER BY / DISTINCT / GROUP BY actually require this allocation?
```

---

# Specific Audit Areas

## 1. Access path execution

Check whether access paths return:

```text
Vec<Row>
Vec<PrimaryKey>
Iterator-like stream
RowView stream
```

Target:

```text
Access paths should stream candidate keys or row views.
```

## 2. Index scans

Check whether index scans:

* collect all matching keys
* load rows immediately
* can resume from raw index key
* can seek/advance to a primary key
* expose primary-key suffix ordering

Target:

```text
Index scans should yield candidate keys and support cursor/seek semantics.
```

## 3. Filters

Check whether filters:

* operate row-by-row
* require owned row values
* can run on RowView
* are split into index-bound and residual filters

Target:

```text
Index-bound filters constrain access paths.
Residual filters stream over RowView.
```

## 4. Projection

Check whether projection:

* owns rows too early
* borrows via RowView
* materializes only at response boundary

Target:

```text
Projection should produce owned output only for the final page.
```

## 5. Aggregation

Check whether aggregation:

* collects input rows before folding
* streams values into state
* retains unnecessary row data

Target:

```text
Aggregation should stream inputs into minimal aggregate state.
```

## 6. ORDER BY

Check whether ORDER BY:

* always collects full rows
* can use index ordering
* can use top-k when LIMIT exists
* sorts minimal keys/projections rather than full rows

Target:

```text
ORDER BY materializes only when access order cannot satisfy requested order.
```

## 7. DISTINCT

Check whether DISTINCT:

* stores full rows
* stores canonical keys
* can deduplicate projected borrowed values

Target:

```text
DISTINCT retains minimal canonical distinct keys.
```

## 8. Cursor construction

Check whether cursors are built from:

* final Vec offsets
* raw store keys
* physical operator state

Target:

```text
Cursor should represent physical progress, not merely response position.
```

---

# Implementation TODO

## Phase 1 — Audit materialization points

Deliverable:

```text
docs/design/<version>-streaming-execution/materialization-audit.md
```

Include:

* file
* function
* current allocation
* classification
* reason
* proposed action

## Phase 2 — Define internal stream traits

Introduce internal traits or state-machine interfaces for:

```text
CandidateKeySource
RowSource
PhysicalOperator
CursorFragment
```

Do not refactor every call site yet.

## Phase 3 — Convert simple access paths

Convert:

```text
ByKey
PrimaryRange
SecondaryIndexRange
FullScan
```

to stream through a common internal interface.

## Phase 4 — RowLoader boundary

Centralize row loading:

```text
CandidateKeyStream → RowView
```

Ensure residual filters and projections consume `RowView`.

## Phase 5 — PageBuilder boundary

Centralize final materialization into:

```text
Vec<ResultRow> + Cursor
```

Make this the normal ownership boundary.

## Phase 6 — Index intersection prototype

Add planner/executor support for one narrow case:

```text
two equality index scans
same entity
compatible primary-key ordering
AND predicates
```

Implement:

```text
KeyIntersection
```

with tests.

## Phase 7 — Generalize key operators

Add:

```text
KeyUnion
KeyDifference
KeyDedup
advance_to
```

only when use cases justify them.

## Phase 8 — Future joins

Use the same stream model to support joins later.

Do not design joins separately from the streaming/key-source execution spine.

---

# Required Tests

## Streaming behavior tests

* LIMIT stops scan early.
* FullScan with LIMIT does not load all rows.
* Projection materializes only returned page.
* Residual filter streams rows.
* Cursor resumes from last consumed key.

## Key-source tests

* Primary range key stream order.
* Secondary index candidate key order.
* Duplicate index component values ordered by primary key suffix.
* `advance_to` works for primary key streams.
* `advance_to` works for secondary candidate streams where supported.

## Intersection tests

* Two index streams intersect correctly.
* Three index streams intersect correctly.
* Empty intersection stops early.
* Sparse intersection uses advancement correctly.
* Duplicate index component values do not duplicate rows.
* Cursor resumes inside intersection.

## Materialization regression tests

* Query with `LIMIT 1` does not materialize all matching rows.
* Query with index-only count avoids row loading where legal.
* Query with residual predicate loads only index candidates.
* Query with `ORDER BY` using compatible index avoids explicit sort.
* Query with `ORDER BY` incompatible index uses bounded top-k when `LIMIT` exists.

## Cursor tests

* Data scan cursor.
* Index scan cursor.
* Intersection cursor.
* Projection cursor.
* FullScan cursor.
* Cursor after residual filters skip rows.

---

# Design Invariants

The following invariants should hold long-term:

```text
Public query APIs may return Vec + Cursor.
Internal execution should not use Vec as the default operator boundary.
Access paths should produce candidate keys where possible.
Rows should be loaded only after key-level pruning.
Projection should borrow until the response boundary.
FullScan should stream and respect page limits.
ORDER BY, DISTINCT, GROUP BY, and joins are explicit materialization/state boundaries.
Cursor state should describe physical execution progress.
Index intersection should operate on ordered candidate key streams.
```

---

# Anti-Patterns to Avoid

```text
Collecting all rows before LIMIT.
Collecting full rows when only keys are needed.
Using public page shape as internal executor shape.
Loading rows before intersecting indexed predicates.
Duplicating cursor state outside physical operators.
Treating every filter as post-row-load residual filtering.
Sorting full rows when projected/order keys are enough.
Using standard Iterator if lifetimes/cursors make it awkward.
Adding queryIter as public API before internal semantics are stable.
```

---

# Open Questions

1. Should internal streams be row-at-a-time or small-batch?
2. What is the minimum cursor state for intersection?
3. Can all secondary index scans expose `advance_to(primary_key)`?
4. Should index streams expose raw primary-key suffixes or decoded `EncodedPrimaryKey`?
5. Can some projections be index-only?
6. Should `COUNT`, `EXISTS`, and `LIMIT 1` get special physical operators?
7. How should deterministic instruction budgets interrupt/resume long pipelines?
8. Which operators are allowed to be blocking?
9. Should full scans yield keys first or row views directly?
10. How does this interact with future join syntax and planner semantics?

---

# Recommended Immediate Next Step

Run a focused materialization audit before implementing new executor machinery.

The first audit should answer:

```text
Where does IcyDB currently allocate Vecs in query execution?
Which allocations are semantic boundaries?
Which are convenience materialization?
Which could become CandidateKeySource or RowSource streams?
```

The audit should produce a concrete cleanup map.

Suggested deliverable:

```text
docs/design/0.159-streaming-execution/materialization-audit.md
```

or, if this belongs under the current key/index work:

```text
docs/design/0.159-index-key-refactor/streaming-execution-addendum.md
```

---

# Final Direction

IcyDB should keep the public query API simple:

```text
query → page of rows + cursor
```

But the internal engine should move toward:

```text
resumable key-first physical streams
```

The most important long-term shift is:

```text
from row-materializing execution
to key-pruning-first execution
```

That means:

```text
index scans produce candidate keys
key operators intersect/prune candidates
row loading happens late
projection owns only at page boundary
```

This strategy aligns with modern query execution practice without forcing IcyDB into an oversized analytical/vectorized engine prematurely.
