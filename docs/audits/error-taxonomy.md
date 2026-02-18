Perform a strict error taxonomy audit.

Goal:
Verify correct classification of errors across all layers.

Audit:
- InternalError
- PlanError
- QueryError
- ErrorClass
- ErrorOrigin
- CursorDecodeError
- IdentityDecodeError
- SerializeError mapping

For each error:
1. Identify its semantic domain:
   - Corruption
   - Unsupported
   - Invalid input
   - Invariant violation
   - System failure
2. Confirm:
   - It is mapped only upward, never reclassified incorrectly
   - Corruption is never downgraded
   - Invalid input is never escalated to corruption
   - Store-origin errors do not leak incorrect origins

Produce:
- Error classification matrix
- Incorrect classification list (if any)
- Layer violation list (if any)

No suggestions about naming.
Only classification correctness.