Perform an index integrity audit.

Focus:
- IndexKey encode/decode
- IndexStore entry layout
- Reverse relation index
- Unique index behavior
- Delete + replace flows
- Recovery replay of index mutations

Verify:
1. Key byte ordering is strictly lexicographic and stable.
2. No key can decode into a different logical index id.
3. Reverse relation entries cannot orphan.
4. Unique index enforcement is consistent between save and recovery.
5. No index mutation can occur without corresponding row mutation.

Attempt to find:
- Key collisions
- Namespace confusion
- Index id mismatch vulnerabilities
- Partial mutation risk

Produce:
- Index invariant list
- High risk mutation paths
- Storage-layer assumptions

No performance discussion.
Only correctness.