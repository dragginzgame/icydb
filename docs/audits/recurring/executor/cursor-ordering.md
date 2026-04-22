# Weekly Audit: Cursor Ordering & Continuation Correctness

## Scope

This audit is strictly limited to correctness of continuation semantics,
ordering invariants, anchor validation, and resume determinism.

Do NOT discuss:

* Performance
* Refactoring
* Code style
* Architecture improvements
* New features

Only evaluate whether the current cursor boundary preserves ordering and
pagination safety guarantees.

---

# Ground Truth Specification

All findings must be evaluated against these invariants.

## A. Ordering Invariants

1. Pagination ordering is defined by canonical ordered boundary slots and, for
   index-range resume, canonical raw index-key ordering.
2. Resume semantics must be monotonic across pages.
3. Page N+1 must begin strictly after the last visible row of page N.
4. Boundary-to-resume conversion must preserve ordering direction exactly.
5. Raw anchor comparison remains the single source of truth for index-range
   envelope containment.

## B. Envelope Invariants

Given the original planned access envelope:

1. A cursor must never widen the envelope.
2. A cursor must never escape the original `AccessPath::IndexRange` bounds.
3. Equal-to-upper resume must collapse to an empty continuation deterministically.
4. Lower/upper inclusive-exclusion semantics must not drift during resume.

## C. Structural Invariants

A continuation token must not be able to change:

* entity path / continuation signature
* initial offset
* order field count
* boundary slot types
* primary-key slot type
* index id
* key namespace
* index component arity
* scalar vs grouped lane
* access-path family

Any violation is a correctness failure.

## D. Pagination Guarantees

Across all pages:

1. No duplication.
2. No omission.
3. Stable ordering across page boundaries.
4. Final-page exhaustion is deterministic.
5. Invalid continuation state is rejected before execution.

---

# Current Ownership Boundary

This audit must use the current cursor boundary, not historical names.

Primary owners to inspect:

* `db/cursor/mod.rs`
  * `decode_optional_cursor_token`
  * `decode_optional_grouped_cursor_token`
  * `prepare_cursor`
  * `revalidate_cursor`
  * `prepare_grouped_cursor_token`
  * `revalidate_grouped_cursor`
  * `validate_grouped_cursor_order_plan`
* `db/cursor/anchor.rs`
  * `validate_index_range_anchor`
  * `validate_index_range_boundary_anchor_consistency`
* `db/index/envelope/*`
  * `resume_bounds_for_continuation`
  * `resume_bounds_from_refs`
  * `continuation_advanced`
  * `key_within_envelope`
* `db/query/plan/continuation.rs`
* `db/executor/prepared_execution_plan.rs`
* `db/executor/planning/continuation/*`
* `db/executor/pipeline/entrypoints/mod.rs`
  * `execute_paged_with_cursor_traced`
  * grouped paged continuation entrypoints when applicable

Historical targets such as `plan_cursor` are obsolete and must not be used as
the audit frame.

---

# Required Analysis Areas

## 1. Token Decode Boundary

Verify:

* invalid external cursor text fails at decode boundary
* grouped token decode stays grouped-owned
* payload decode errors map to the correct cursor-plan taxonomy

## 2. Scalar Cursor Preparation

Verify:

* `prepare_cursor` rejects missing required order
* boundary arity/type mismatches fail before execution
* signature and offset mismatches fail before execution
* scalar cursor preparation cannot mutate plan shape

## 3. Grouped Cursor Preparation

Verify:

* grouped order-plan validation rejects invalid explicit order shapes
* grouped signature/direction/offset mismatches fail before execution
* grouped cursor preparation cannot cross into scalar resume semantics

## 4. Index-Range Anchor Validation

Verify:

* canonical anchor round-trip is enforced
* index-id mismatch is rejected
* key-namespace mismatch is rejected
* component-arity mismatch is rejected
* out-of-envelope anchors are rejected
* boundary/anchor primary-key consistency is enforced

## 5. Resume Bound Substitution

Verify:

* resume bound substitution uses the original planned envelope
* equal-to-upper resumes to empty deterministically
* bound conversion preserves monotonic advance
* no inclusive/exclusive inversion is introduced

## 6. Revalidation Boundary

Verify:

* executor revalidation does not reinterpret cursor meaning
* `revalidate_cursor` and `revalidate_grouped_cursor` preserve the same
  invariants as initial preparation
* prepared execution plans do not bypass cursor revalidation

## 7. Execution Resume Boundary

Verify:

* `execute_paged_with_cursor_traced` and grouped equivalents consume only
  validated cursor state
* execution resumes from the validated boundary, not from untrusted token data
* invalid cursors are rejected before material page execution begins

---

# Required Analysis For Each Area

For each audited area:

1. List the ordering invariants assumed.
2. Identify the canonical comparison or boundary owner.
3. Identify all inclusive/exclusive bound conversions.
4. Identify any token-data-to-runtime-state transformation.
5. Verify envelope preservation.
6. Verify structural mutation is impossible or explicitly rejected.
7. Verify error classification matches the failure mode.
8. Verify resume semantics remain monotonic.

Be explicit about whether each protection is:

* structural
* validation-based
* execution-gated
* missing / unclear

---

# Required Attack Scenarios

Every run must reason through these scenarios explicitly:

1. Cursor text is invalid hex.
2. Cursor payload decodes as malformed bytes.
3. Scalar cursor has wrong boundary arity.
4. Scalar cursor has wrong boundary value type.
5. Scalar cursor has wrong primary-key type.
6. Cursor signature/entity path does not match the prepared plan.
7. Cursor initial offset does not match the prepared plan.
8. Grouped cursor direction does not match execution direction.
9. Grouped cursor is supplied to a scalar path or vice versa.
10. Index-range anchor has correct bytes but wrong index id.
11. Index-range anchor has correct bytes but wrong key namespace.
12. Index-range anchor has correct bytes but wrong component arity.
13. Index-range anchor is lexicographically valid but outside the original
    envelope.
14. Anchor equals the upper bound exactly.
15. Anchor equals the lower bound exactly.

State explicitly whether each is:

* prevented structurally
* prevented via validation
* rejected at execution boundary
* not prevented
* unclear / risky

---

# Required Verification Baseline

Every run must include evidence from current tests and live source inspection.

Prefer current tests from:

* `db/cursor/tests/mod.rs`
* `db/executor/tests/cursor_validation.rs`
* `db/executor/tests/pagination.rs`
* `db/index/envelope/tests.rs`

If a critical scenario is not covered by an existing test, call that out
explicitly as a coverage gap.

---

# Required Output Format

Produce:

## 0. Run Metadata + Comparability Note

- compared baseline report path
  - daily baseline rule: first run of day compares to latest prior comparable
    report or `N/A`
  - same-day reruns compare to that day’s `cursor-ordering.md` baseline
- method tag/version
- comparability status (`comparable` or `non-comparable` with reason)

## 1. Boundary Table

| Boundary | Owner | Verified? | Evidence | Risk |
| -------- | ----- | --------- | -------- | ---- |

## 2. Failure Classification Table

| Failure Type | Expected Error | Actual Error | Correct? | Risk |
| ------------ | -------------- | ------------ | -------- | ---- |

## 3. Envelope Safety Table

| Scenario | Can Escape Envelope? | Why / Why Not | Risk |
| -------- | -------------------- | ------------- | ---- |

## 4. Duplication/Omission Safety Table

| Mechanism | Duplication Risk | Omission Risk | Explanation | Risk |
| --------- | ---------------- | ------------- | ----------- | ---- |

## 5. Structural Mutation Table

| Property | Can Change? | Protection Mechanism | Risk |
| -------- | ----------- | -------------------- | ---- |

## 6. Coverage Gaps

List:

* critical scenarios without direct test evidence
* scenarios covered only indirectly
* new cursor surfaces added since the previous comparable run

## 7. Overall Risk Assessment

Provide:

* critical issues
* medium-risk drift
* low-risk observations
* tests that should be added if coverage is thin

Be strict. If something is not explicitly guarded, mark it as risk.

## 8. Overall Cursor/Ordering Risk Index (1-10, lower is better)

Interpretation:

* `1-3` = Low risk / structurally healthy
* `4-6` = Moderate risk / manageable pressure
* `7-8` = High risk / requires monitoring
* `9-10` = Critical risk / structural instability
