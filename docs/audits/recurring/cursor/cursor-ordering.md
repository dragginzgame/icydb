# WEEKLY AUDIT: Cursor + Ordering Correctness (Drift Detection)

## Scope

This audit is **strictly limited to correctness of continuation semantics and ordering invariants**.

Do NOT discuss:

* Performance
* Refactoring
* Code style
* Architecture improvements
* New features

Only evaluate whether current implementation preserves ordering and pagination safety guarantees.

---

# Ground Truth Invariants (Must Be Treated As Spec)

All findings must be evaluated against these invariants.

## A. Ordering Invariants

1. All pagination ordering is defined by **raw index key lexicographic ordering**.
2. Logical ordering (ASC only for now) must map 1:1 to raw key ordering.
3. The resume anchor must always be treated as:

   ```
   Bound::Excluded(last_raw_key)
   ```
4. Raw key comparison is the single source of ordering truth.
5. Anchor ordering must be monotonic across pages.

---

## B. Envelope Invariants

Given original `IndexRange { lower, upper }`:

1. Continuation anchor must never:

   * Fall below original lower bound
   * Exceed original upper bound
2. Continuation must remain inside the original access-path envelope.
3. Cursor must not widen the envelope.
4. Cursor must not shrink the envelope incorrectly (no accidental inclusive/exclusive inversion).

---

## C. Structural Invariants

The cursor token must not be able to:

* Change index id
* Change key namespace
* Change component count (arity)
* Change component ordering
* Change index type
* Escape `AccessPath::IndexRange`
* Convert into composite access path
* Modify predicate
* Modify order direction

Any violation = correctness failure.

---

## D. Pagination Guarantees

Across all pages:

1. No duplication.
2. No omission.
3. Stable ordering across page boundaries.
4. Page N+1 must strictly start after last item of page N.
5. End-of-results must be deterministic and final.

---

# Audit Targets

You must inspect and analyze:

1. Cursor token decode
2. PlanError classification for:

   * hex decode failure
   * payload decode failure
   * anchor mismatch
   * index id mismatch
   * component arity mismatch
   * out-of-envelope anchor
3. `plan_cursor`
4. `execute_paged_with_cursor`
5. `AccessPath::IndexRange` anchor handling

You must trace raw key construction and bound application.

---

# Required Analysis For Each Target

For each audited area:

1. List all ordering invariants assumed.
2. Identify all raw key comparisons.
3. Identify all boundary conversions (inclusive/exclusive).
4. Identify any logic that transforms anchor → bound.
5. Verify envelope containment.
6. Verify no structural mutation is possible.
7. Verify error classification matches failure type.
8. Verify resume semantics use `Bound::Excluded`.

Explicitly attempt to break:

* Envelope containment
* Monotonic ordering
* Raw/logical ordering alignment
* Composite leakage
* Arity mismatch handling
* Incorrect error classification
* Off-by-one boundary transitions

---

# Explicit Attack Scenarios To Attempt

You must simulate reasoning for:

1. Anchor exactly equal to upper bound.
2. Anchor exactly equal to lower bound.
3. Anchor between two valid keys.
4. Anchor with correct bytes but wrong index id.
5. Anchor with valid hex but corrupted payload.
6. Anchor with correct arity but wrong namespace prefix.
7. Anchor referencing a different index entirely.
8. Anchor outside original envelope but lexicographically valid.
9. Cursor generated from different predicate.
10. Cursor generated from composite access path.
11. Anchor that sorts before lower bound.
12. Anchor that sorts after upper bound.

State explicitly whether each is:

* Prevented structurally
* Prevented via validation
* Not prevented
* Unclear / risky

---

# Required Output Format

Produce:

## 1. Invariant Table

| Area | Invariants Assumed | Verified? | Evidence | Risk |
| ---- | ------------------ | --------- | -------- | ---- |

---

## 2. Failure Mode Classification Table

| Failure Type | Expected Error | Actual Error | Correct? | Risk |
| ------------ | -------------- | ------------ | -------- | ---- |

---

## 3. Envelope Safety Table

| Scenario | Can Escape Envelope? | Why / Why Not | Risk |
| -------- | -------------------- | ------------- | ---- |

---

## 4. Duplication/Omission Safety Table

| Mechanism | Duplication Risk | Omission Risk | Explanation | Risk |
| --------- | ---------------- | ------------- | ----------- | ---- |

---

## 5. Structural Mutation Table

| Property | Can Change? | Protection Mechanism | Risk |
| -------- | ----------- | -------------------- | ---- |

---

## 6. Overall Risk Assessment

Provide:

* Critical issues
* Medium-risk drift
* Low-risk observations
* Areas requiring additional tests

Be strict. If something is not explicitly guarded, mark as risk.

## 7. Overall Cursor/Ordering Risk Index (1–10, lower is better)

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

---

# Drift Detection Requirement

Assume this audit will be run weekly.

If logic relies on implicit invariants (not enforced structurally), mark as **drift-sensitive**.

Highlight:

* Areas where small refactors could silently break ordering.
* Areas lacking explicit validation.
* Areas lacking tests.

---

# Hard Constraints

* Do not propose refactors.
* Do not propose new features.
* Do not discuss performance.
* Only correctness and invariants.
* Assume malicious cursor input.
