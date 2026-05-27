# 0.165 Schema Runtime Proof Naming

## Status

Complete.

## Accepted Renames

### `AcceptedGeneratedCompatibleRowShape` -> `AcceptedGeneratedRowCompatibilityProof`

Role proof:

- Owning module: `db::schema::runtime`
- Payload: schema-runtime proof that one accepted row layout remains compatible
  with generated field codecs
- Main consumers: executor authority assembly and terminal row decode layout
  construction
- Chosen family: `*Proof`
- Rejected alternatives:
  - `*Shape`: wrong because the value is not a compact structural family; it is
    a guard proving the generated bridge is still admissible for an accepted
    row layout
  - `*Descriptor`: wrong because the value is not renderable diagnostics
    metadata
  - `*Context`: wrong because the value crosses schema/runtime/executor
    boundaries and is consumed as a proof object
- Public-surface impact: none; this remains `pub(in crate::db)`
- Hard-cut rule: remove the old type and helper names from live schema/runtime
  code

Accepted renames:

```text
AcceptedGeneratedCompatibleRowShape -> AcceptedGeneratedRowCompatibilityProof
generated_compatible_row_shape_for_model(...) -> generated_row_compatibility_proof_for_model(...)
```

## Old-Vocabulary Scan Terms

Live-code scan for this slice:

```bash
rg -n "AcceptedGeneratedCompatibleRowShape|generated_compatible_row_shape|row_shape|row shape proof|generated-compatible shape" crates/icydb-core/src/db/schema/runtime.rs crates/icydb-core/src/db/executor/terminal/row_decode crates/icydb-core/src/db/executor/authority/entity.rs docs/design/0.165-naming-audit-and-role-alignment
```

Remaining hits are active 0.165 notes or legitimate role-proof examples.
