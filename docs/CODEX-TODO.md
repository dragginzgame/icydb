
Codex Prompt

We are standardizing error-construction discipline across the entire workspace.

Objective

Update AGENTS.md with a new architectural rule governing error helper constructors.

Refactor existing free-floating helper functions that construct error types so they become associated functions on the owning error type.

Preserve semantics exactly.

Do not introduce unrelated refactors.

Part 1 — Update AGENTS.md

Add a new section titled:

Error Construction Discipline

This section must establish the following rules:

Any helper that constructs an error type must be implemented as an associated function (impl) on the owning error type.

Free-floating functions that return an error type (e.g., fn some_error(...) -> MyError) are not permitted.

Constructors must remain domain-anchored: error construction logic belongs to the error type itself.

Constructors must not embed business logic — only simple field population or normalization.

Error taxonomy boundaries must remain explicit; constructors must not collapse or obscure error domains.

Defensive/internal error construction must remain in the internal error type.

User-facing error construction must remain in user-facing error types.

The tone must be authoritative and architectural, not advisory.

Do not modify unrelated sections of AGENTS.md.

Part 2 — Refactor Existing Code

Search the entire workspace for free-floating functions that:

Return an error type (any XxxError)

Exist solely to construct that error

Example pattern:

fn invalid_something(reason: impl Into<String>) -> MyError


For each such function:

Remove the free function.

Move it into an impl MyError block as an associated constructor.

Update all call sites accordingly.

Preserve visibility (pub, pub(crate), etc.).

Do not change variant names or payload structure.

Example transformation:

Before:

fn invalid_continuation_cursor_payload(reason: impl Into<String>) -> PlanError {
    PlanError::Cursor(
        CursorPlanError::InvalidContinuationCursorPayload {
            reason: reason.into(),
        }
    )
}


After:

impl PlanError {
    pub(crate) fn invalid_continuation_cursor_payload(
        reason: impl Into<String>,
    ) -> Self {
        Self::Cursor(
            CursorPlanError::InvalidContinuationCursorPayload {
                reason: reason.into(),
            },
        )
    }
}


If the constructor logically belongs to a grouped sub-error, place it in that type instead:

impl CursorPlanError {
    pub(crate) fn invalid_payload(reason: impl Into<String>) -> Self {
        Self::InvalidContinuationCursorPayload {
            reason: reason.into(),
        }
    }
}


Then wrap via the parent enum at call site.

Constraints

No semantic changes.

No error reclassification.

No renaming of variants.

No restructuring of enums.

No formatting-only churn.

Minimal diff.

Verification Criteria

After refactor:

No free-floating functions remain that exist solely to construct an error.

All helper constructors live inside impl blocks on the owning error type.

All tests compile and behavior remains identical.

Make only the minimal changes required to enforce this discipline.