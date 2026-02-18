Perform a cursor + ordering safety audit.

Focus only on continuation semantics and ordering invariants.

Audit:

1. Cursor token decode
2. PlanError classification for:
   - hex decode failure
   - payload decode failure
   - anchor mismatch
   - index id mismatch
   - component arity mismatch
   - out-of-envelope anchor
3. plan_cursor
4. execute_paged_with_cursor
5. IndexRange anchor handling

For each:
- List ordering invariants assumed.
- Confirm anchor keys cannot:
  - escape original range envelope
  - change index id
  - change component count
  - change key namespace
- Confirm pagination guarantees:
  - No duplication
  - No omission
  - Stable ordering across pages

Explicitly attempt to find:
- Envelope escape possibilities
- Off-by-one boundary mistakes
- Mismatch between logical ordering and raw key ordering
- Composite access-path anchor leakage

Produce:
- Invariant table
- Failure-mode table
- Risk level per finding

Do not discuss performance.
Only correctness.
