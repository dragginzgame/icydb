# 0.165 Test And Publication Input Naming

## Status

Complete.

## Accepted Renames

### Projection Materialization Test Inputs

Role proof:

- Owning module: `db::executor::projection::materialize`
- Payload: test-only constructor for a prepared projection shape with explicit
  projection spec, prepared projection plan, direct projection slots, and
  projected slot mask
- Main consumers: executor projection materialization tests and SQL projection
  materialization tests
- Chosen family: explicit test-input vocabulary
- Rejected alternatives:
  - `from_test_parts`: too weak because the helper constructs a full prepared
    projection test shape from named inputs
  - `from_components_for_test`: less clear than saying the call accepts test
    inputs for the prepared projection shape
- Public-surface impact: none; the helper is `#[cfg(test)]`
- Hard-cut rule: remove generic parts vocabulary from live test helpers that
  otherwise preserve old construction language

Accepted rename:

```text
PreparedProjectionShape::from_test_parts(...) -> from_test_inputs(...)
```

### Staged Publication Readiness Inputs

Role proof:

- Owning module: `db::schema::mutation::field_path::publication`
- Payload: internal readiness constructor for staged field-path index
  publication diagnostics after validation/invalidation/snapshot/store reports
- Main consumers: staged schema mutation publication readiness constructors
- Chosen family: validation-input vocabulary
- Rejected alternatives:
  - `from_validated_parts`: too weak because the helper consumes the validated
    report inputs used to derive blockers, not arbitrary parts
  - `from_inputs`: too broad because the important boundary is validation and
    staged publication readiness
- Public-surface impact: none; visibility remains schema-internal and the
  staged publication types are still dead-code-gated for future publication
  wiring
- Hard-cut rule: remove the leftover private `from_validated_parts` helper name
  from staged schema publication code

Accepted rename:

```text
from_validated_parts(...) -> from_validation_inputs(...)
```

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "PreparedProjectionShape::from_test_parts|from_test_parts\\(|from_validated_parts|from_validation_inputs|from_test_inputs" crates/icydb-core/src/db/executor/projection crates/icydb-core/src/db/session/sql/projection crates/icydb-core/src/db/schema/mutation docs/design/0.165-naming-audit-and-role-alignment
```

Remaining old-name hits are allowed only inside this family note, changelog
history, or older completed 0.165 scan terms.
