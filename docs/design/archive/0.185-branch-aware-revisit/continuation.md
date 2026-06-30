# Branch-Aware Continuation Notes

This note records the current branch-aware continuation model for 0.185. It is
not a full cursor redesign.

## Current Model

- Scalar continuation planning decodes one global cursor boundary for the last
  emitted row.
- When the route order is primary-key-only, continuation runtime forwards that
  decoded primary-key boundary to access-stream resolution.
- Branch-set and child-prefix-expanded streams turn the primary-key boundary
  into one raw index resume anchor per active prefix stream.
- The merged stream then resumes each branch/prefix after the same global
  primary-key boundary and continues duplicate-suppressing key merge.

## Existing Proof

- Branch-set SQL continuation tests prove that page two resumes after page one's
  primary-key boundary.
- Diagnostics-gated tests prove branch-prefix entries are not replayed
  wholesale after continuation.
- Sparse `IN` child-prefix expansion now has a matching continuation proof:
  page two resumes expanded child-prefix streams after the global primary-key
  boundary, keeps child-prefix stream opens bounded, and does not replay all
  collection-prefix index entries.
- The physical merged-prefix helper now centralizes the prefix-empty pruning,
  fair chunk sizing, primary-key suffix resume-anchor creation, and ordered
  merge construction used by branch-set and related prefix-expansion paths.

## Constraints

- This model is valid for the first branch-aware slice because each active
  branch is ordered by the same primary-key suffix and direction.
- It does not encode independent per-branch cursor positions.
- It should not be generalized to arbitrary non-primary-key ordering without a
  separate cursor-format hard-cut.
- It should not be used when the access route cannot prove every prefix stream
  shares the same ordered suffix.

## 0.185 Hard-Cut Decision

No per-branch cursor format hard-cut is needed for the branch-aware routes
currently admitted in 0.185.

The route planner admits lazy branch-set and child-prefix-expanded continuation
only when every active prefix stream shares the same primary-key suffix order
and direction. Under that contract the existing global cursor boundary is
sufficient: each active prefix stream can derive its own resume anchor from the
same primary-key boundary, and the shared merge continues after the last
emitted global key.

Route tests now guard this directly for branch-set, sparse child-prefix ASC,
and sparse child-prefix DESC routes under resumed execution.

## Future Hard-Cut Boundary

A future branch-tree cursor format may still need per-child anchors if a later
line broadens branch merging beyond global primary-key suffix continuation.
Do not add compatibility fallbacks before 1.0.0; if the format changes,
hard-cut to the latest cursor contract.
