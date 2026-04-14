# 0.80 Cache Evolution Addendum

## Purpose

This note records the intended evolution path for the 0.80 compiled SQL cache so future work can widen reuse without weakening the original architectural rule.

The goal is to preserve a durable direction:

* one semantic cache boundary
* one local cache in the initial slice
* widening by generalizing semantic identity
* not by turning the cache into a second planner or runtime system

## Core Rule

0.80 caches one artifact:

`CompiledSqlCommand`

That artifact represents the compiled semantic meaning of one SQL statement under one compatible schema and execution-affecting feature context.

Future widening should preserve this rule:

**reuse should expand by making cache identity more general, not by letting cache values absorb more executor-specific machinery.**

## Why This Addendum Exists

It is likely that 0.80 will succeed structurally but still leave reuse on the table because the initial key is intentionally narrow:

* exact raw SQL text
* conservative schema compatibility fingerprint
* execution-affecting flags

That narrow key is correct for the first slice. It keeps correctness obvious and avoids premature entanglement with normalization, parameterization, and value-sensitive planning.

However, future maintainers may be tempted to improve hit rate by widening the cache value instead of widening cache identity. That is the wrong direction.

This addendum exists to make the intended widening path explicit.

## Approved Evolution Direction

### Stage 0: Exact-text semantic reuse

This is 0.80.

One cache entry serves:

* one exact SQL text
* one compatible schema fingerprint
* one execution-affecting flag context

This is the minimum-risk baseline and remains the correctness reference point for later widening.

Current implementation note:

* the exact-text component is the raw submitted SQL string
* lookup does not canonicalize whitespace, keyword casing, or literal forms yet

### Stage 1: Normalized-text semantic reuse

A later slice may allow multiple surface-different but semantically equivalent SQL strings to map to the same cache identity.

Examples:

* whitespace differences
* casing differences
* other purely lexical trivia that does not change semantic meaning

This stage should only normalize syntax noise. It must not merge statements whose semantic interpretation could differ.

Rule:

* widen by canonicalizing equivalent SQL text
* do not widen by weakening schema compatibility or planner correctness

### Stage 2: Parameterized semantic skeleton reuse

The main long-term widening target is parameterized reuse.

At this stage, multiple queries with the same semantic structure but different literal values may reuse one cached semantic template.

Examples:

* `WHERE id = 1`
* `WHERE id = 2`
* `WHERE id = 99`

could all map to one semantic skeleton such as:

* `WHERE id = ?`

The intended artifact at this stage is not a fatter 0.80 cache entry. It is a new, more general semantic artifact, for example:

`CompiledSqlCommandTemplate`

This template should own:

* semantic statement family
* resolved field/projection/grouping/order structure
* parameter slot definitions
* planner output that is valid across the template’s parameter domain

It should not own:

* parameter-instance scratch state
* request-local materialization buffers
* deep executor-specific runtime bundles

### Stage 3: Selected derivative runtime reuse

Only after semantic widening proves worthwhile should the system consider caching selected runtime-derived bundles under the same semantic artifact.

Examples might include:

* reused projection materialization layouts
* allocation reuse bundles
* narrowly scoped executor preparation objects

This work is explicitly subordinate to the semantic cache contract.

Rule:

* semantic artifact remains the primary cache truth
* runtime-derived reuse is optional and derivative
* runtime-derived reuse must never become a second semantic planning path

## Disallowed Evolution Direction

The following progression is specifically discouraged:

1. start with `CompiledSqlCommand`
2. add executor-prepared state into the same cache entry
3. add query-family-specific runtime shortcuts
4. add value-sensitive planning into the cached value
5. end up with cache entries that behave like a hidden second planner

Why this is wrong:

* it fragments ownership
* it makes invalidation harder
* it couples cache stability to executor internals
* it increases recode risk when new query families land

Future widening must happen by broadening semantic identity, not by bloating cached values.

## Preferred Artifact Evolution

The expected progression is:

* `CompiledSqlCommand` for exact-text semantic reuse
* later, `CompiledSqlCommandTemplate` or equivalent for parameterized semantic reuse
* optional derivative runtime reuse under the same semantic owner

This preserves the same top-level cache model while allowing broader applicability.

## Guardrails for Future Work

Any widening slice after 0.80 should answer these questions explicitly.

### 1. What new set of queries can share one semantic artifact?

The answer must be framed in terms of semantic equivalence, not convenience.

### 2. What part of the current cache key is being generalized?

Examples:

* raw text identity -> normalized text identity
* literal-bound identity -> parameterized skeleton identity

### 3. What new correctness risk does widening introduce?

Examples:

* accidental aliasing of semantically distinct SQL
* value-sensitive planner decisions being incorrectly reused
* under-invalidation across schema or capability changes

### 4. Does the widening keep the cache artifact query-owned?

If the answer is no, the design is moving in the wrong direction.

### 5. Can a cache hit still be proven not to re-run semantic compile work?

If widening causes semantic planning to leak back into execution, the cache contract has weakened.

## Recommended Future Acceptance Criteria

Any widening slice should satisfy all of the following:

* the widened cache still has one canonical semantic owner
* semantic equivalence class is defined explicitly
* cache hits do not re-run semantic compile stages
* schema compatibility remains fail-closed
* executor-derived reuse, if any, remains derivative and optional
* new query families widen the semantic artifact model rather than introducing parallel cache systems

## Suggested Future Milestones

### 0.80.x / 0.81 candidate

Evaluate normalized-text identity only if it is simple, obvious, and does not blur semantic distinctions.

### Later milestone

Design parameterized semantic templates as a first-class follow-up, not as an ad hoc extension of 0.80 cache entries.

### Much later milestone

Consider selected derivative executor-preparation reuse only after semantic template reuse has been measured and validated.

## Final Reminder

The durable path is:

**exact SQL reuse first, then normalized semantic reuse, then parameterized semantic-template reuse.**

The non-durable path is:

**keep stuffing more runtime behavior into the cache entry until the cache becomes a hidden second planner.**

Future work should choose the first path.
