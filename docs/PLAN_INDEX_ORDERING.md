# PLAN-INDEX-ORDERING.md

## Overview

This document defines the *index ordering model* established as part of the 0.10 index protocol redesign.

0.10 replaces the prior fixed-fingerprint key format with a **canonical, framed, variable-length, lexicographically ordered index key**, supporting true range queries, composite ordering, and stable prefix bounds.

## Goals

* Ensure *ordering semantics* align with semantic value ordering.
* Enable *efficient range scans* via lexicographic B-tree mappings.
* Guarantee *behavioral stability* for query planners and execution.
* Eliminate hash/fingerprint ordering artifacts.

## Definitions

### IndexKey Semantics

An index key in stable memory encodes:

```
(kind, index_id, component_0, component_1, ..., component_n, primary_key)
```

Each component is encoded canonically, with explicit segment lengths and value tags.

Canonical comparison semantics:

* Scalar, numeric, text, enum, and other types are ordered according to their semantic value order.
* Floats are normalized with *NaN rejection and +0.0/−0.0 canonical equivalence*.
* Composite ordering respects left-to-right component precedence. ([System Overflow][1])

## Ordering Invariants

1. **Lexicographic vs. Semantic Equivalence**

Sorting by raw bytes is *semantically equivalent* to tuple-wise ordering:

```
IndexKey::Ord == raw_byte_lexicographic_order ==
Value::canonical_cmp_key over components + PK tie-break
```

This holds for all supported scalar and composite index shapes.

## Unblocked Capabilities

### 1) Efficient Range Queries

With lexicographic ordering, typical range predicates (e.g., `>=`, `<`, `BETWEEN`) can be implemented as bounded traversals over the keyspace without scanning entire tables.

This matches classical B-tree range index behavior. ([DEV Community][2])

### 2) Composite Index Support

Index ordering on `component_0, component_1, ..., component_n` matches relational composite index semantics:

* The leftmost component controls major sorting.
* Subsequent components refine ordering under equal prefixes. ([System Overflow][1])

This enables queries filtering on prefixes of composite keys to be pushed down into range scans.

### 3) Stable Pagination / Cursor Semantics

Because the ordering is canonical and byte-stable, continuation tokens can be represented as raw key bytes.

Clients and planners can resume scans deterministically at a given position in the index.

### 4) Unique Constraint Enforcement via Prefix Scan

Uniqueness is enforced by scanning for existing keys that match the *non-PK components* as a prefix.

If a conflicting prefix exists with a different PK, the insert/update is rejected.

Compared to the prior fingerprint model, this eliminates hash collision as a correctness risk.

### 5) Index-Only Ordering Pushdown

Operations like `ORDER BY indexed_field` can be satisfied directly by range scans without secondary sorting.

Composite ordering enables satisfying `ORDER BY (field_1, field_2)` if the index matches the field sequence.

### 6) Planner Simplification

Planner eligibility checks now rely strictly on canonical encodability and prefix membership, not fingerprint heuristics.

Index ordering is fully determined by encoded component succession.

## Protocol Stability Guarantees

### Canonical Format Freeze

* Component framing (length + bytes) is protocol-level.
* Value tag sets and ordering rules are stable across releases.
* Float and decimal normalization rules are fixed.
* No implicit or situational interpretation of index bytes is permitted.

## Testable Properties

Canonical ordering correctness must be validated via tests asserting:

* **Encoded bytes match semantically expected order**.
* **Range scan bounds exclude keys outside prefix**.
* **Primary key pivot ensures tie-break resolution**.
* **Cross-namespace isolation (User vs System)**.
* **Type normalization (float, decimal, enum) freeze behaviors**.

## Non-Goals / Out of Scope

This design does *not* automatically provide:

* Cost-based optimizer selection based on index statistics.
* Multi-index intersection strategies.
* Bitmap or full-text index semantics.
* Secondary index compression strategies.

These may leverage the canonical ordering layer but require separate design work.

## Reference Summary

Database indexes used as sorted structures support efficient range traversal, equality, and composite prefix matching by leveraging lexicographic ordering of key bytes over sorted components. Composite index semantics align with value-level ordering of fields in sequence. Canonical encodings enforce stability and deterministic ordering with respect to application query semantics. ([System Overflow][1])

