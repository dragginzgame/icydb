# Index Key Encoding Release Roadmap (0.10.x)

`0.10.x` is the **Index Keys release**.

The 0.10 series focuses on replacing fixed-slot secondary index key encoding
with canonical variable-length ordered keys.

This release exists to make ordered secondary traversal, range scans, and
ordering semantics mechanically correct at the storage-key layer.

0.10 performs this as an in-place replacement of the current index-key
encoding. There is no `V1`/`V2` dual format, no compatibility mode, and no
backward-compatibility requirement for old index-key bytes.

0.10 is intentionally breaking: we replace `v1` behavior in situ, move fast,
and do not preserve backward compatibility for prior index-key/storage
contracts.

---

## 0.10 Coherent Arc

`0.10.x` follows one coherent execution arc:

* Canonical ordered secondary indexes
* ORDER BY pushdown
* Range scan correctness
* Index traversal semantics

---

## Progress Snapshot (as of TBD)

Estimated completion toward the `0.10.x` goals in this plan:

* RawIndexKey Variable-Length Redesign: **0%**
* Canonical Primitive Encoding: **0%**
* Schema Constraints and Field Limit Discipline: **100%** (carried forward)
* In-Place Index Key Replacement: **0%**
* Ordered Traversal and Pagination Parity: **0%**
* Verification and Property Coverage: **0%**

Overall estimated progress: **~17%**

---

# 1. RawIndexKey Variable-Length Redesign

IcyDB 0.10 will replace fixed-slot index keys with variable-length canonical
keys.

## Goals

* Replace fixed-slot `RawIndexKey` layout with variable-length storage
* Bake an explicit key-kind discriminator into encoded keys (for example, user vs system/reverse)
* Remove layout dependency on `MAX_INDEX_FIELDS`
* Keep keys bounded with explicit maximum size computation
* Keep primary key as final key component in composite index keys

## Outcomes

* Index keys are no longer constrained by fixed 16-byte field slots
* Key kind is encoded directly in key bytes rather than inferred from index-name namespace
* Storage layout no longer includes slot padding
* Composite key boundaries are unambiguous and deterministic

## Non-Goals

* Compression-focused key packing
* Relaxing bounded-size requirements

---

# 2. Canonical Primitive Encoding for Ordered Keys

IcyDB 0.10 will define and enforce canonical encoding for all indexable
primitive types.

## Goals

* Guarantee `a < b` iff `encode(a) < encode(b)` (lexicographic bytes)
* Guarantee equal values always encode identically
* Enforce per-type canonicalization (for example, normalized decimal encoding)
* Disallow unsupported/non-orderable edge payloads where required

## Outcomes

* Ordered scans over encoded keys reflect logical ordering
* Equality consistency holds between semantic and byte-level comparisons
* Canonical encoding is testable and deterministic across upgrades

## Non-Goals

* Cost-based query optimization
* Cross-type coercion in index key encoding

---

# 3. Schema Constraints and Field Limit Discipline

IcyDB 0.10 keeps schema limits explicit and separate from storage layout.

## Goals

* Keep `MAX_INDEX_FIELDS = 8` enforced at schema-definition time
* Ensure field-count limits are not encoded as storage slot assumptions
* Provide clear schema errors for index definitions exceeding limits

## Outcomes

* Predictable planner and storage bounds
* Clean separation between schema constraints and key byte layout

## Non-Goals

* Increasing composite-index arity beyond current schema limits

---

# 4. In-Place Index Key Replacement (No Compatibility Layer)

IcyDB 0.10 replaces the current fixed-slot index-key encoding in situ.

This scope is limited to secondary-index key representation and deterministic
rebuild behavior from authoritative row data. It does not include row/commit
format versioning or a generic migration engine.

## Goals

* Replace the current fixed-slot index encoding directly with canonical variable-length keys
* Keep a single active encoding format in 0.10 (no `V1`/`V2` schema)
* Rebuild secondary indexes deterministically from row data during upgrade/startup gate
* Define failure behavior explicitly (fail closed with classified errors; no silent partial rebuild)
* Prohibit mixed-key modes and dual-write modes in steady-state execution

## Outcomes

* Post-upgrade index behavior uses one canonical key format only
* Fixed-slot keys are eliminated during rebuild
* Runtime/index code paths do not carry compatibility branches

## Non-Goals

* Row format versioning
* Commit marker wire-format versioning
* Generic row-op migration engine
* Backward-compatibility decode support for old fixed-slot index-key bytes
* Dual-write or mixed-encoding index modes in normal operation
* Best-effort partial conversion with mixed encoding semantics

---

# 5. Ordered Traversal, Range Scans, and Pagination Parity

IcyDB 0.10 will wire canonical index keys into ordered execution paths without
semantic drift.

## Goals

* Enable ordered secondary traversal based on canonical key bytes
* Enable deterministic range scans over ordered secondary indexes
* Gate ORDER BY pushdown on strict correctness checks:
  field sequence, direction, canonical missing/null ordering, and primary-key tie-break requirements
* Fallback to existing non-pushdown execution when correctness checks fail
* Preserve continuation signature and cursor semantics
* Keep pagination behavior equivalent to existing contract

## Outcomes

* ORDER BY pushdown paths can rely on byte-order correctness
* Pagination outputs remain contract-compatible while execution cost improves

## Non-Goals

* Bidirectional cursor contracts
* Snapshot-consistent pagination across requests

---

# 6. Verification and Safety Gates

IcyDB 0.10 requires heavy property and boundary testing for canonical index
encoding.

## Goals

* Property tests for per-primitive logical order vs byte order equivalence
* Composite tuple ordering parity tests
* Golden-vector encode tests per primitive to detect byte-format drift
* Decimal normalization and float edge-case tests
* In-place rebuild correctness tests from fixed-slot input state to canonical keys
* Regression tests proving no continuation/pagination semantic drift

## Outcomes

* Canonical encoding behavior is proven, not assumed
* Upgrade and replay safety are validated in tests

## Non-Goals

* Replacing runtime invariants with test-only guarantees

---

## Invariants Introduced in 0.10

The following become explicit structural guarantees:

* Secondary index keys are canonical and variable-length
* Key-type discrimination is an explicit encoded key component, not a naming convention
* Lexicographic key order matches logical order for supported primitives
* Secondary index key encoding is replaced in situ with no compatibility mode
* Pagination/continuation semantics are preserved through the encoding shift

---

## Explicit Non-Goals (0.10.x)

The following remain out of scope:

* Row format versioning and backward-compatible row decode rules (0.11)
* Commit marker format versioning and replay-compatibility work (0.11)
* Generic migration engine for persisted row transformations (0.11)
* Any `V1`/`V2` index-key version negotiation or backward-compatibility layer
* Cost-based planning
* Multi-index merge/intersection planning
* Index compression pipelines
* Distributed/cross-canister transaction semantics

---

## Summary

0.10.x is the **Index Keys release**.

If 0.9 strengthens correctness boundaries,
0.10 makes ordered secondary indexing canonical and single-format by replacing
the old encoding in place.

The release arc is explicit:

* Canonical ordered secondary indexes
* ORDER BY pushdown
* Range scan correctness
* Index traversal semantics
