# Binary Cursor Design

## 1. Purpose

Version 0.13 introduces **versioned, binary-encoded cursors** for pagination continuation.

This phase:

* Stabilizes cursor wire format
* Defines compatibility guarantees
* Introduces versioned envelope structure
* Binds cursor to canonical plan identity
* Prevents unsafe cross-query reuse

This phase does **not** introduce:

* Schema evolution guarantees
* Cross-major-version cursor stability
* Composite access tree pagination
* DESC traversal (unless already implemented)

---

# 2. Goals

Binary cursors must:

1. Be opaque to users.
2. Be versioned.
3. Bind to canonical plan identity.
4. Fail safely when incompatible.
5. Avoid exposing physical storage layout.
6. Preserve deterministic continuation semantics established in 0.12.

---

# 3. Non-Goals

0.13 does not guarantee:

* Cursor validity across index definition changes.
* Cursor validity after schema migrations.
* Cursor portability between deployments.
* Long-lived cursor durability across major format changes.

Binary cursor is a **protocol-level continuation token**, not a schema compatibility contract.

---

# 4. High-Level Model

In 0.12, continuation anchor is:

```rust
struct IndexRangeCursor {
    last_raw_key: RawIndexKey,
}
```

In 0.13, this becomes wrapped in a versioned envelope:

```
BinaryCursor {
    version: u8,
    payload: CursorPayload,
}
```

Where `CursorPayload` contains:

* Plan identity binding
* Access path binding
* Raw continuation anchor
* Optional integrity metadata

---

# 5. Cursor Envelope Format

## 5.1 Versioning

First byte is a version discriminator.

```
[version: u8]
```

Initial version: `1`.

Future versions must increment.

---

## 5.2 CursorPayload (Version 1)

Version 1 payload contains:

```
struct CursorV1 {
    plan_fingerprint: [u8; 16],  // or existing fingerprint size
    index_id: IndexId,
    key_kind: IndexKeyKind,
    component_arity: u8,
    last_raw_key: Vec<u8>,
}
```

### Field Rationale

#### plan_fingerprint

Prevents cross-query misuse.

Cursor is valid only for the identical canonical plan shape.

---

#### index_id

Prevents reuse across different indexes.

---

#### key_kind

Prevents namespace crossover (User/System).

---

#### component_arity

Prevents misuse against different index definitions.

---

#### last_raw_key

Physical traversal anchor.

Stored as raw bytes, but interpreted only by engine.

---

# 6. Encoding Format

Binary cursor is serialized as:

```
[ version (1 byte) ]
[ fingerprint length + bytes ]
[ index_id bytes ]
[ key_kind (1 byte) ]
[ component_arity (1 byte) ]
[ raw_key_length (u32 BE) ]
[ raw_key_bytes ]
```

The entire structure may then be:

* Base64 encoded
* Or directly returned as bytes (depending on API surface)

---

# 7. Validation Rules

On continuation request:

1. Decode version.

   * If unsupported → reject.
2. Decode payload.
3. Compare `plan_fingerprint` to current canonical plan.

   * If mismatch → reject.
4. Verify `index_id`, `key_kind`, `component_arity`.

   * If mismatch → reject.
5. Validate `last_raw_key` can be parsed as `IndexKey`.
6. Verify parsed key matches index identity.
7. Verify key lies within original IndexRange envelope.

   * Prefix match.
   * Respect lower/upper bounds.

If any check fails → reject cursor.

No fallback behavior.

---

# 8. Security & Safety Considerations

Binary cursor must:

* Reject malformed input.
* Reject oversized raw_key.
* Avoid allocation bombs.
* Fail deterministically.

Cursor parsing must never panic.

---

# 9. Stability Guarantees (0.13)

Within the same minor release series:

* Cursor format is stable.
* Cursor remains valid across restarts.
* Cursor remains valid across deployments using identical storage format.

Across major versions:

* No guarantee unless explicitly stated in release notes.

---

# 10. Interaction With Schema Evolution

Binary cursor in 0.13 does **not** guarantee:

* Validity across schema migrations.
* Validity after index rebuild.
* Validity after index definition change.

If index definition changes:

* Component arity mismatch will cause rejection.

This is intentional.

Schema evolution policy will define future behavior.

---

# 11. Backward Compatibility

If 0.13 encounters a 0.12-style transparent cursor:

* It may reject.
* Or provide a migration path (optional).

This decision must be explicit in release notes.

---

# 12. Error Semantics

On cursor rejection:

Return structured error:

```
CursorInvalid {
    reason: ...
}
```

Never silently restart from beginning.

Never partially reuse.

---

# 13. Testing Requirements

Must include:

* Round-trip encode/decode tests.
* Cross-plan misuse rejection.
* Index mismatch rejection.
* Namespace mismatch rejection.
* Component arity mismatch rejection.
* Raw key corruption rejection.
* Oversized key rejection.
* Boundary-escape rejection.
* Continuation correctness equivalence with 0.12.

---

# 14. Upgrade Path from 0.12

Steps:

1. Implement versioned envelope.
2. Wrap existing `RawIndexKey` continuation.
3. Preserve existing continuation semantics.
4. Add validation layer.
5. Maintain identical page behavior.

0.13 must not change semantic pagination behavior.

---

# 15. Architectural Principle

0.12 established:

> Correct physical continuation semantics.

0.13 establishes:

> Stable, versioned cursor representation.

Representation must never outpace semantics.

---

# 16. Future Extensions

Potential 0.14+ additions:

* Composite access tree cursor encoding.
* DESC support encoding.
* Cross-schema-version cursor negotiation.
* Cursor expiration timestamps.
* Cursor integrity MAC / signature (if needed).

---

# Final Evaluation

This design:

* Does not leak storage internals.
* Anchors continuation at physical layer safely.
* Binds to canonical plan identity.
* Is versioned from day one.
* Avoids premature schema guarantees.

