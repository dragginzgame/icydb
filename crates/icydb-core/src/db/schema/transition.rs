//! Module: db::schema::transition
//! Responsibility: schema transition policy and rejection diagnostics.
//! Does not own: startup reconciliation orchestration or schema-store persistence.
//! Boundary: decides whether one accepted snapshot may become another.

use crate::db::schema::{PersistedFieldSnapshot, PersistedSchemaSnapshot};

///
/// SchemaTransitionDecision
///
/// SchemaTransitionDecision is the schema-owned result of comparing a
/// persisted accepted snapshot with the generated proposal for the same entity.
/// It exists so reconciliation policy can distinguish accepted transitions
/// from rejected transitions before any live migration rules are added.
/// Today the only accepted transition is exact equality.
///

#[derive(Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaTransitionDecision {
    ExactMatch,
    Rejected(SchemaTransitionRejection),
}

///
/// SchemaTransitionRejectionKind
///
/// SchemaTransitionRejectionKind classifies rejected schema transitions into
/// stable low-cardinality buckets. Reconciliation metrics use this taxonomy so
/// dashboards can track trust-boundary failures without parsing diagnostic text.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaTransitionRejectionKind {
    EntityIdentity,
    FieldContract,
    FieldSlot,
    RowLayout,
    SchemaVersion,
    Snapshot,
}

///
/// SchemaTransitionRejection
///
/// SchemaTransitionRejection carries the schema-owned diagnostic for one
/// rejected transition decision. It keeps policy selection separate from final
/// user-facing error formatting, so future migration decisions can add richer
/// rejection metadata without changing the reconciliation call shape.
///

#[derive(Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaTransitionRejection {
    kind: SchemaTransitionRejectionKind,
    detail: String,
}

impl SchemaTransitionRejection {
    // Build one transition rejection from the first schema mismatch detail
    // produced by the diagnostic comparison helpers below.
    const fn new(kind: SchemaTransitionRejectionKind, detail: String) -> Self {
        Self { kind, detail }
    }

    // Return the stable rejection bucket for metrics and audit readouts.
    pub(in crate::db::schema) const fn kind(&self) -> SchemaTransitionRejectionKind {
        self.kind
    }

    // Borrow the first rejected transition detail for final error formatting.
    pub(in crate::db::schema) const fn detail(&self) -> &str {
        self.detail.as_str()
    }
}

// Decide whether one persisted snapshot may transition to the generated
// proposal. The policy is intentionally exact-only for now, but the closed
// decision type prevents future migration work from hiding inside diagnostics.
pub(in crate::db::schema) fn decide_schema_transition(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> SchemaTransitionDecision {
    if actual == expected {
        return SchemaTransitionDecision::ExactMatch;
    }

    let (kind, detail) = schema_snapshot_mismatch_detail(actual, expected);

    SchemaTransitionDecision::Rejected(SchemaTransitionRejection::new(kind, detail))
}

// Return the first human-readable schema difference between the stored
// snapshot and the current generated proposal. This is diagnostic-only: the
// acceptance policy remains exact equality until schema transitions exist.
fn schema_snapshot_mismatch_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> (SchemaTransitionRejectionKind, String) {
    if actual.version() != expected.version() {
        return (
            SchemaTransitionRejectionKind::SchemaVersion,
            format!(
                "schema version changed: stored={} generated={}",
                actual.version().get(),
                expected.version().get(),
            ),
        );
    }

    if actual.entity_path() != expected.entity_path() {
        return (
            SchemaTransitionRejectionKind::EntityIdentity,
            format!(
                "entity path changed: stored='{}' generated='{}'",
                actual.entity_path(),
                expected.entity_path(),
            ),
        );
    }

    if actual.entity_name() != expected.entity_name() {
        return (
            SchemaTransitionRejectionKind::EntityIdentity,
            format!(
                "entity name changed: stored='{}' generated='{}'",
                actual.entity_name(),
                expected.entity_name(),
            ),
        );
    }

    schema_snapshot_structural_mismatch_detail(actual, expected)
}

// Compare schema internals after version/path/name have already matched. The
// split keeps the top-level diagnostic helper readable while preserving a
// deterministic first-difference order for startup failures.
fn schema_snapshot_structural_mismatch_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> (SchemaTransitionRejectionKind, String) {
    if actual.primary_key_field_id() != expected.primary_key_field_id() {
        return (
            SchemaTransitionRejectionKind::EntityIdentity,
            format!(
                "primary key field id changed: stored={} generated={}",
                actual.primary_key_field_id().get(),
                expected.primary_key_field_id().get(),
            ),
        );
    }

    if actual.row_layout() != expected.row_layout() {
        return (
            SchemaTransitionRejectionKind::RowLayout,
            format!(
                "row layout changed: stored={:?} generated={:?}",
                actual.row_layout(),
                expected.row_layout(),
            ),
        );
    }

    if actual.fields().len() != expected.fields().len() {
        return (
            SchemaTransitionRejectionKind::FieldContract,
            format!(
                "field count changed: stored={} generated={}",
                actual.fields().len(),
                expected.fields().len(),
            ),
        );
    }

    for (index, (actual_field, expected_field)) in
        actual.fields().iter().zip(expected.fields()).enumerate()
    {
        if let Some(mismatch) = field_snapshot_mismatch_detail(index, actual_field, expected_field)
        {
            return mismatch;
        }
    }

    (
        SchemaTransitionRejectionKind::Snapshot,
        "schema snapshot changed".to_string(),
    )
}

// Compare one field snapshot in a stable order so diagnostics point at the
// first durable field contract that would require explicit migration support.
fn field_snapshot_mismatch_detail(
    index: usize,
    actual: &PersistedFieldSnapshot,
    expected: &PersistedFieldSnapshot,
) -> Option<(SchemaTransitionRejectionKind, String)> {
    if actual.id() != expected.id() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            format!(
                "field[{index}] id changed: stored={} generated={}",
                actual.id().get(),
                expected.id().get(),
            ),
        ));
    }

    if actual.name() != expected.name() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            format!(
                "field[{index}] name changed: stored='{}' generated='{}'",
                actual.name(),
                expected.name(),
            ),
        ));
    }

    field_snapshot_contract_mismatch_detail(index, actual, expected)
}

// Compare non-identity field metadata separately from durable ID/name so the
// mismatch order stays explicit without turning reconciliation into a large
// monolithic branch list.
fn field_snapshot_contract_mismatch_detail(
    index: usize,
    actual: &PersistedFieldSnapshot,
    expected: &PersistedFieldSnapshot,
) -> Option<(SchemaTransitionRejectionKind, String)> {
    if actual.slot() != expected.slot() {
        return Some((
            SchemaTransitionRejectionKind::FieldSlot,
            format!(
                "field[{index}] slot changed: stored={} generated={}",
                actual.slot().get(),
                expected.slot().get(),
            ),
        ));
    }

    if actual.kind() != expected.kind() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            format!(
                "field[{index}] kind changed: stored={:?} generated={:?}",
                actual.kind(),
                expected.kind(),
            ),
        ));
    }

    if actual.nested_leaves() != expected.nested_leaves() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            format!(
                "field[{index}] nested leaf metadata changed: stored={} generated={}",
                actual.nested_leaves().len(),
                expected.nested_leaves().len(),
            ),
        ));
    }

    field_snapshot_storage_mismatch_detail(index, actual, expected)
}

// Compare nullable/default/storage codec metadata last. These are still schema
// contracts, but they are subordinate to field identity and physical layout
// when reporting the first rejected transition.
fn field_snapshot_storage_mismatch_detail(
    index: usize,
    actual: &PersistedFieldSnapshot,
    expected: &PersistedFieldSnapshot,
) -> Option<(SchemaTransitionRejectionKind, String)> {
    if actual.nullable() != expected.nullable() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            format!(
                "field[{index}] nullability changed: stored={} generated={}",
                actual.nullable(),
                expected.nullable(),
            ),
        ));
    }

    if actual.default() != expected.default() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            format!(
                "field[{index}] default changed: stored={:?} generated={:?}",
                actual.default(),
                expected.default(),
            ),
        ));
    }

    if actual.storage_decode() != expected.storage_decode() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            format!(
                "field[{index}] storage decode changed: stored={:?} generated={:?}",
                actual.storage_decode(),
                expected.storage_decode(),
            ),
        ));
    }

    if actual.leaf_codec() != expected.leaf_codec() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            format!(
                "field[{index}] leaf codec changed: stored={:?} generated={:?}",
                actual.leaf_codec(),
                expected.leaf_codec(),
            ),
        ));
    }

    None
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::schema::{
            FieldId, PersistedFieldKind, PersistedFieldSnapshot, PersistedSchemaSnapshot,
            SchemaFieldDefault, SchemaFieldSlot, SchemaRowLayout, SchemaTransitionDecision,
            SchemaVersion, decide_schema_transition,
        },
        model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
    };

    // Build the stable two-field snapshot used by transition-policy tests.
    // Keeping the fixture local avoids depending on reconciliation test entities.
    fn expected_snapshot() -> PersistedSchemaSnapshot {
        PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "test::SchemaReconcileEntity".to_string(),
            "SchemaReconcileEntity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                ],
            ),
            vec![
                PersistedFieldSnapshot::new(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "name".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Text { max_len: None },
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Text),
                ),
            ],
        )
    }

    // Preserve the expected snapshot shape except for entity name so tests can
    // assert that transition diagnostics report the first rejected identity fact.
    fn changed_entity_name_snapshot(expected: &PersistedSchemaSnapshot) -> PersistedSchemaSnapshot {
        PersistedSchemaSnapshot::new(
            expected.version(),
            expected.entity_path().to_string(),
            "ChangedSchemaReconcileEntity".to_string(),
            expected.primary_key_field_id(),
            expected.row_layout().clone(),
            expected.fields().to_vec(),
        )
    }

    #[test]
    fn schema_transition_policy_accepts_only_exact_snapshot_match() {
        let expected = expected_snapshot();

        assert_eq!(
            decide_schema_transition(&expected, &expected),
            SchemaTransitionDecision::ExactMatch,
        );

        let changed = changed_entity_name_snapshot(&expected);
        let SchemaTransitionDecision::Rejected(rejection) =
            decide_schema_transition(&changed, &expected)
        else {
            panic!("changed schema snapshot should be rejected");
        };
        assert!(
            rejection
                .detail()
                .contains("entity name changed: stored='ChangedSchemaReconcileEntity' generated='SchemaReconcileEntity'"),
            "transition rejection should retain the first schema mismatch detail",
        );
    }

    #[test]
    fn schema_transition_policy_reports_row_layout_mismatch_after_entity_identity() {
        let expected = expected_snapshot();
        let changed = PersistedSchemaSnapshot::new(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(1)),
                    (FieldId::new(2), SchemaFieldSlot::new(0)),
                ],
            ),
            expected.fields().to_vec(),
        );

        let SchemaTransitionDecision::Rejected(rejection) =
            decide_schema_transition(&changed, &expected)
        else {
            panic!("changed row layout should be rejected");
        };

        assert!(
            rejection.detail().contains("row layout changed"),
            "row-layout drift should be reported before field metadata drift",
        );
    }
}
