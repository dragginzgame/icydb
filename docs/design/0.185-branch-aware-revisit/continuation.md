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

## Deferred Hard-Cut Question

A future branch-tree cursor format may need to carry per-child anchors if 0.185
or later broadens branch merging beyond global primary-key suffix continuation.
Do not add compatibility fallbacks before 1.0.0; if the format changes, hard-cut
to the latest cursor contract.
