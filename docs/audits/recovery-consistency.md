Perform a recovery consistency audit.

Goal:
Verify that recovery replay enforces exactly the same invariants and side-effects as normal execution.

Scope:
- CommitMarker
- CommitRowOp
- begin_commit / finish_commit
- ensure_recovered_for_write
- Reverse-relation index mutation
- Index entry mutation
- Save/replace/delete flows

For each mutation type:
1. Compare normal execution path vs recovery replay path.
2. Verify:
   - Same ordering of operations
   - Same invariant checks
   - Same error classification
   - Same index mutation logic
3. Identify:
   - Any invariant enforced in normal execution but skipped in recovery
   - Any mutation performed twice or not rolled back on prepare failure
   - Any difference in reverse index behavior

Produce:
- A side-by-side execution flow table
- A section titled "Divergence Risks"
- A section titled "Idempotence Verification"

No refactors unless a correctness divergence is found.
