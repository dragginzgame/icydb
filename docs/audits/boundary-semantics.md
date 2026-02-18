# audits/boundary-semantics.md

# Weekly Audit: Boundary & Envelope Semantics

## Scope

This audit verifies correctness of:

* Bound transformations (inclusive/exclusive)
* Range envelope preservation
* Raw vs logical ordering alignment
* Anchor → bound conversion
* Envelope containment across pagination

Do NOT discuss:

* Performance
* Refactoring
* API improvements
* Feature work

Only correctness and invariant preservation.

---

# Ground Truth Specification

## A. Bound Model

All range execution must reduce to:

* `Bound::Included(key)`
* `Bound::Excluded(key)`
* `Unbounded`

All execution must operate on **raw index keys**.

Logical ordering must map 1:1 to raw lexicographic ordering.

---

## B. Envelope Definition

Given:

```
IndexRange {
    lower: Bound<RawIndexKey>,
    upper: Bound<RawIndexKey>,
}
```

The envelope is defined as:

```
lower <= key < upper
```

respecting inclusive/exclusive semantics.

The envelope must:

1. Never widen during planning.
2. Never widen during cursor continuation.
3. Never silently invert inclusivity.
4. Never convert Included → Excluded unless explicitly required by resume semantics.
5. Never convert Excluded → Included.

---

## C. Cursor Resume Rule

Continuation must always be:

```
new_lower = Bound::Excluded(last_emitted_raw_key)
```

This must:

* Preserve monotonicity.
* Never duplicate.
* Never omit.
* Never escape envelope.

---

# Audit Targets

Inspect:

* Planner bound construction
* Predicate → IndexRange lowering
* `plan_cursor`
* `execute_paged_with_cursor`
* Anchor-to-bound conversion logic
* Store-level traversal bound usage
* Any logical → raw ordering transformations

---

# Required Analysis Per Target

For each:

1. List all inclusivity/exclusivity transitions.
2. Identify all conversions between logical and raw keys.
3. Confirm envelope preservation.
4. Confirm no implicit widening.
5. Confirm no bound inversion.
6. Confirm resume always uses Excluded.
7. Confirm upper bound is never modified by cursor.

---

# Explicit Attack Scenarios

You must reason through:

1. Lower = Included(x), anchor = x
2. Lower = Excluded(x), anchor = x
3. Upper = Included(x), anchor = x
4. Upper = Excluded(x), anchor = x
5. Anchor exactly equal to upper bound
6. Anchor exactly equal to lower bound
7. Anchor just below lower
8. Anchor just above upper
9. Empty range (lower == upper)
10. Single-element range
11. Full unbounded range

For each, state:

* Can envelope be escaped?
* Can duplication occur?
* Can omission occur?
* Is behavior deterministic?

---

# Required Output Format

## 1. Bound Transformation Table

| Location | Original Bound | Transformed Bound | Correct? | Risk |
| -------- | -------------- | ----------------- | -------- | ---- |

---

## 2. Envelope Containment Table

| Scenario | Can Escape Envelope? | Why / Why Not | Risk |
| -------- | -------------------- | ------------- | ---- |

---

## 3. Duplication/Omission Table

| Case | Duplication Risk | Omission Risk | Explanation | Risk |
| ---- | ---------------- | ------------- | ----------- | ---- |

---

## 4. Raw vs Logical Ordering Alignment

| Area | Raw Ordering Used? | Logical Conversion? | Drift Risk |
| ---- | ------------------ | ------------------- | ---------- |

---

## 5. Drift Sensitivity

Identify:

* Areas relying on implicit ordering assumptions.
* Areas lacking explicit envelope checks.
* Areas lacking boundary tests.

---

## Overall Risk Rating

* Critical
* Medium
* Low

---
