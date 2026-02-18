Perform a full invariant-preservation audit of icydb-core.

This is NOT a style or DRY audit.
This is NOT a feature request.
Do not refactor anything yet.

Your task:

1. Identify all boundary-crossing layers:
   - serialize / deserialize
   - identity / key types
   - planner
   - executable plan
   - save executor
   - delete executor
   - recovery / commit replay
   - cursor planning
   - index key encode/decode
   - reverse-relation index mutation

2. For each boundary:
   A. List the invariants assumed on entry.
   B. Locate exactly where each invariant is enforced.
   C. Confirm it is enforced:
      - exactly once
      - at the narrowest boundary
      - with correct error classification
   D. Identify any invariant that:
      - is enforced in multiple layers
      - is not enforced
      - is enforced too late
      - is enforced only in normal execution but not in recovery

3. Special focus:
   - Continuation cursor envelope safety
   - Index key ordering guarantees
   - Reverse relation index correctness
   - Idempotence of recovery replay
   - Expected-key vs decoded-entity key match

4. Produce:
   - A structured report grouped by subsystem
   - A “High Risk Invariants” section
   - A “Redundant Enforcement” section
   - A “Missing Enforcement” section
   - No speculative redesign unless a violation is found

Do not suggest stylistic improvements.
Only discuss invariants.
