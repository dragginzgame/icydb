//! Module: db::schema::transition
//! Responsibility: schema transition policy and rejection diagnostics.
//! Does not own: startup reconciliation orchestration or schema-store persistence.
//! Boundary: decides whether one accepted snapshot may become another.

use crate::{
    db::{
        data::decode_structural_field_by_accepted_kind_bytes,
        schema::{
            FieldId, PersistedFieldSnapshot, PersistedNestedLeafSnapshot, PersistedSchemaSnapshot,
            SchemaFieldSlot,
        },
    },
    value::Value,
};

///
/// SchemaTransitionDecision
///
/// SchemaTransitionDecision is the schema-owned result of comparing a
/// persisted accepted snapshot with the generated proposal for the same entity.
/// It exists so reconciliation policy can distinguish accepted transitions
/// from rejected transitions before any live migration rules are added.
/// Today the only accepted plan is exact equality.
///

#[derive(Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaTransitionDecision {
    Accepted(SchemaTransitionPlan),
    Rejected(SchemaTransitionRejection),
}

///
/// SchemaTransitionPlanKind
///
/// SchemaTransitionPlanKind classifies accepted schema transitions. The enum
/// is intentionally small so migration support must add explicit accepted
/// cases instead of smuggling behavior through loose booleans.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaTransitionPlanKind {
    AppendOnlyNullableFields,
    ExactMatch,
}

///
/// SchemaTransitionPlan
///
/// SchemaTransitionPlan is the schema-owned artifact that authorizes startup
/// reconciliation to accept a generated proposal against a stored schema
/// snapshot. Later live-layout work will hang runtime remapping/default
/// instructions from this plan rather than asking executor code to recompute
/// schema meaning from raw snapshots.
///

#[derive(Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaTransitionPlan {
    kind: SchemaTransitionPlanKind,
    added_field_count: usize,
}

impl SchemaTransitionPlan {
    // Build the current no-op transition plan used when stored and generated
    // schema snapshots are exactly equal.
    const fn exact_match() -> Self {
        Self {
            kind: SchemaTransitionPlanKind::ExactMatch,
            added_field_count: 0,
        }
    }

    // Build the accepted append-only nullable transition plan. Runtime decode
    // consumes accepted absence policies, so reconciliation only needs to
    // publish the new generated snapshot after this schema-owned policy check.
    const fn append_only_nullable_fields(added_field_count: usize) -> Self {
        Self {
            kind: SchemaTransitionPlanKind::AppendOnlyNullableFields,
            added_field_count,
        }
    }

    // Return the stable accepted-plan bucket for reconciliation and future
    // metrics. This avoids exposing raw transition internals to callers.
    pub(in crate::db::schema) const fn kind(&self) -> SchemaTransitionPlanKind {
        self.kind
    }

    // Return how many generated fields were accepted by this transition.
    #[cfg(test)]
    pub(in crate::db::schema) const fn added_field_count(&self) -> usize {
        self.added_field_count
    }
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
        return SchemaTransitionDecision::Accepted(SchemaTransitionPlan::exact_match());
    }

    if let Some(added_fields) = append_only_additive_transition_fields(actual, expected)
        && added_fields
            .iter()
            .all(field_has_supported_missing_absence_policy)
    {
        return SchemaTransitionDecision::Accepted(
            SchemaTransitionPlan::append_only_nullable_fields(added_fields.len()),
        );
    }

    let (kind, detail) = schema_snapshot_mismatch_detail(actual, expected);

    SchemaTransitionDecision::Rejected(SchemaTransitionRejection::new(kind, detail))
}

// Decide whether one added field can be absent from older physical rows.
// Nullable no-default fields materialize as `NULL`; fields with explicit
// persisted default payloads materialize from that slot payload.
fn field_has_supported_missing_absence_policy(field: &PersistedFieldSnapshot) -> bool {
    (field.nullable() && field.default().is_none()) || field_default_payload_is_valid(field)
}

// Validate one accepted default payload before a schema transition can rely on
// it for missing-slot materialization. Defaults are persisted bytes, so policy
// must ask the accepted field-codec boundary to prove the payload is decodable
// and non-null instead of trusting the schema metadata blindly.
fn field_default_payload_is_valid(field: &PersistedFieldSnapshot) -> bool {
    let Some(payload) = field.default().slot_payload() else {
        return false;
    };

    decode_structural_field_by_accepted_kind_bytes(payload, field.kind())
        .is_ok_and(|value| !matches!(value, Value::Null))
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

    if let Some(detail) = unsupported_generated_additive_field_detail(actual, expected) {
        return (SchemaTransitionRejectionKind::FieldContract, detail);
    }

    if let Some(detail) = unsupported_generated_removed_field_detail(actual, expected) {
        return (SchemaTransitionRejectionKind::FieldContract, detail);
    }

    if actual.row_layout() != expected.row_layout() {
        return (
            SchemaTransitionRejectionKind::RowLayout,
            row_layout_mismatch_detail(actual, expected),
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

// Detect an append-only additive-field transition shape that still cannot be
// accepted. Nullable no-default additions are accepted earlier; this diagnostic
// names additive fields whose absence policy is not supported yet.
fn unsupported_generated_additive_field_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> Option<String> {
    let added_fields = append_only_additive_transition_fields(actual, expected)?;
    let index = actual.fields().len();
    let field = &added_fields[0];
    Some(format!(
        "unsupported additive field transition: generated field[{index}] id={} slot={} name='{}' kind={:?} nullable={} default={:?}; field must be nullable without a default or carry a valid explicit persisted default payload",
        field.id().get(),
        field.slot().get(),
        field.name(),
        field.kind(),
        field.nullable(),
        field.default(),
    ))
}

// Return generated fields for the only additive shape that can become an
// accepted transition: stored fields and row-layout entries must be exact
// prefixes of the generated proposal. This helper does not decide whether the
// added fields are safe; callers still inspect absence policy eligibility.
fn append_only_additive_transition_fields<'a>(
    actual: &PersistedSchemaSnapshot,
    expected: &'a PersistedSchemaSnapshot,
) -> Option<&'a [PersistedFieldSnapshot]> {
    if actual.fields().len() >= expected.fields().len()
        || actual.row_layout().field_to_slot().len() >= expected.row_layout().field_to_slot().len()
    {
        return None;
    }

    if !actual
        .fields()
        .iter()
        .zip(expected.fields())
        .all(|(actual_field, expected_field)| actual_field == expected_field)
    {
        return None;
    }

    if !actual
        .row_layout()
        .field_to_slot()
        .iter()
        .zip(expected.row_layout().field_to_slot())
        .all(|(actual_pair, expected_pair)| actual_pair == expected_pair)
    {
        return None;
    }

    Some(&expected.fields()[actual.fields().len()..])
}

// Detect the symmetric field-removal transition shape without accepting it.
// A generated snapshot is a removal candidate only when the generated fields
// and row-layout mappings are exact prefixes of the stored accepted snapshot.
// That means the new code has stopped declaring a field that old rows may
// still carry, which needs explicit retained-slot semantics before acceptance.
fn unsupported_generated_removed_field_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> Option<String> {
    if actual.fields().len() <= expected.fields().len()
        || actual.row_layout().field_to_slot().len() <= expected.row_layout().field_to_slot().len()
    {
        return None;
    }

    if !actual
        .fields()
        .iter()
        .zip(expected.fields())
        .all(|(actual_field, expected_field)| actual_field == expected_field)
    {
        return None;
    }

    if !actual
        .row_layout()
        .field_to_slot()
        .iter()
        .zip(expected.row_layout().field_to_slot())
        .all(|(actual_pair, expected_pair)| actual_pair == expected_pair)
    {
        return None;
    }

    let index = expected.fields().len();
    let field = &actual.fields()[index];
    Some(format!(
        "unsupported removed field transition: stored field[{index}] id={} slot={} name='{}' kind={:?}; retained-slot support is not enabled yet",
        field.id().get(),
        field.slot().get(),
        field.name(),
        field.kind(),
    ))
}

// Summarize row-layout drift without dumping every field/slot pair into the
// startup error. Full layout dumps are too noisy for normal schema-change
// rejection, while the first changed/missing/added fact is enough to debug the
// generated-vs-accepted mismatch.
fn row_layout_mismatch_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> String {
    let stored_count = actual.row_layout().field_to_slot().len();
    let generated_count = expected.row_layout().field_to_slot().len();
    let prefix = format!(
        "row layout changed: stored_version={} generated_version={} stored_fields={} generated_fields={}",
        actual.row_layout().version().get(),
        expected.row_layout().version().get(),
        stored_count,
        generated_count,
    );

    if actual.row_layout().version() != expected.row_layout().version() {
        return prefix;
    }

    if let Some(detail) = row_layout_first_pair_mismatch_detail(actual, expected) {
        return format!("{prefix}; {detail}");
    }

    prefix
}

// Report the first row-layout pair difference in deterministic vector order.
// Schema evolution is still exact-match only, so diagnostics should identify
// the earliest changed fact without attempting a migration diff.
fn row_layout_first_pair_mismatch_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> Option<String> {
    for (index, (actual_pair, expected_pair)) in actual
        .row_layout()
        .field_to_slot()
        .iter()
        .zip(expected.row_layout().field_to_slot())
        .enumerate()
    {
        if actual_pair != expected_pair {
            return Some(format!(
                "first_difference=row_layout[{index}] {}; {}",
                row_layout_field_detail("stored", actual_pair.0, actual_pair.1, actual.fields()),
                row_layout_field_detail(
                    "generated",
                    expected_pair.0,
                    expected_pair.1,
                    expected.fields(),
                ),
            ));
        }
    }

    if actual.row_layout().field_to_slot().len() > expected.row_layout().field_to_slot().len() {
        let index = expected.row_layout().field_to_slot().len();
        let (field_id, slot) = actual.row_layout().field_to_slot()[index];

        return Some(format!(
            "first_difference=stored_extra row_layout[{index}] {}; generated_has_no_layout_entry",
            row_layout_field_detail("stored", field_id, slot, actual.fields()),
        ));
    }

    if expected.row_layout().field_to_slot().len() > actual.row_layout().field_to_slot().len() {
        let index = actual.row_layout().field_to_slot().len();
        let (field_id, slot) = expected.row_layout().field_to_slot()[index];

        return Some(format!(
            "first_difference=generated_extra row_layout[{index}] stored_has_no_layout_entry; {}",
            row_layout_field_detail("generated", field_id, slot, expected.fields()),
        ));
    }

    None
}

// Attach field metadata to a row-layout mismatch when the field ID can still
// be resolved through the same persisted snapshot. This keeps diagnostics
// useful for added/removed fields while preserving the row-layout authority as
// the first rejected transition fact.
fn row_layout_field_detail(
    label: &str,
    field_id: FieldId,
    slot: SchemaFieldSlot,
    fields: &[PersistedFieldSnapshot],
) -> String {
    let Some(field) = fields.iter().find(|field| field.id() == field_id) else {
        return format!(
            "{label}_field_id={} {label}_slot={} {label}_field_metadata=missing",
            field_id.get(),
            slot.get(),
        );
    };

    format!(
        "{label}_field_id={} {label}_slot={} {label}_name='{}' {label}_kind={:?}",
        field_id.get(),
        slot.get(),
        field.name(),
        field.kind(),
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
            nested_leaf_mismatch_detail(index, actual.nested_leaves(), expected.nested_leaves()),
        ));
    }

    field_snapshot_storage_mismatch_detail(index, actual, expected)
}

// Summarize nested field-path drift on the owning top-level field. Nested
// leaves do not carry physical row slots, so the first changed path/kind/codec
// fact is more useful than a raw debug dump when generated metadata drifts.
fn nested_leaf_mismatch_detail(
    field_index: usize,
    actual: &[PersistedNestedLeafSnapshot],
    expected: &[PersistedNestedLeafSnapshot],
) -> String {
    let prefix = format!(
        "field[{field_index}] nested leaf metadata changed: stored={} generated={}",
        actual.len(),
        expected.len(),
    );

    if let Some(detail) = nested_leaf_first_mismatch_detail(actual, expected) {
        return format!("{prefix}; {detail}");
    }

    prefix
}

// Find the first changed nested leaf fact in stable vector order. The transition
// policy still rejects every nested metadata drift; this helper only names the
// earliest changed fact so operators can see what generated code changed.
fn nested_leaf_first_mismatch_detail(
    actual: &[PersistedNestedLeafSnapshot],
    expected: &[PersistedNestedLeafSnapshot],
) -> Option<String> {
    for (index, (actual_leaf, expected_leaf)) in actual.iter().zip(expected).enumerate() {
        if actual_leaf != expected_leaf {
            return Some(format!(
                "first_difference=nested_leaf[{index}] {}; {}",
                nested_leaf_detail("stored", actual_leaf),
                nested_leaf_detail("generated", expected_leaf),
            ));
        }
    }

    if actual.len() > expected.len() {
        let index = expected.len();
        return Some(format!(
            "first_difference=stored_extra nested_leaf[{index}] {}; generated_has_no_nested_leaf",
            nested_leaf_detail("stored", &actual[index]),
        ));
    }

    if expected.len() > actual.len() {
        let index = actual.len();
        return Some(format!(
            "first_difference=generated_extra nested_leaf[{index}] stored_has_no_nested_leaf; {}",
            nested_leaf_detail("generated", &expected[index]),
        ));
    }

    None
}

// Render one nested leaf descriptor without exposing the full debug shape.
// Path/kind/nullability/codec are the facts needed to understand whether field
// path planning or row-value decoding would need an explicit migration rule.
fn nested_leaf_detail(label: &str, leaf: &PersistedNestedLeafSnapshot) -> String {
    let path = if leaf.path().is_empty() {
        "<root>".to_string()
    } else {
        leaf.path().join(".")
    };

    format!(
        "{label}_path='{path}' {label}_kind={:?} {label}_nullable={} {label}_storage_decode={:?} {label}_leaf_codec={:?}",
        leaf.kind(),
        leaf.nullable(),
        leaf.storage_decode(),
        leaf.leaf_codec(),
    )
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
            FieldId, PersistedFieldKind, PersistedFieldSnapshot, PersistedNestedLeafSnapshot,
            PersistedSchemaSnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaRowLayout,
            SchemaTransitionDecision, SchemaTransitionPlanKind, SchemaVersion,
            decide_schema_transition, transition::SchemaTransitionRejectionKind,
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
    fn schema_transition_policy_accepts_exact_snapshot_match() {
        let expected = expected_snapshot();

        let SchemaTransitionDecision::Accepted(plan) =
            decide_schema_transition(&expected, &expected)
        else {
            panic!("exact snapshot match should produce an accepted transition plan");
        };
        assert_eq!(plan.kind(), SchemaTransitionPlanKind::ExactMatch);

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
    fn schema_transition_policy_accepts_append_only_nullable_fields() {
        let stored = expected_snapshot();
        let mut generated_fields = stored.fields().to_vec();
        generated_fields.push(PersistedFieldSnapshot::new(
            FieldId::new(3),
            "nickname".to_string(),
            SchemaFieldSlot::new(2),
            PersistedFieldKind::Text { max_len: None },
            Vec::new(),
            true,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        ));
        let generated = PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.primary_key_field_id(),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                    (FieldId::new(3), SchemaFieldSlot::new(2)),
                ],
            ),
            generated_fields,
        );

        let SchemaTransitionDecision::Accepted(plan) =
            decide_schema_transition(&stored, &generated)
        else {
            panic!("append-only nullable generated field should be an accepted transition");
        };

        assert_eq!(
            plan.kind(),
            SchemaTransitionPlanKind::AppendOnlyNullableFields
        );
        assert_eq!(plan.added_field_count(), 1);
    }

    #[test]
    fn schema_transition_policy_accepts_append_only_defaulted_fields() {
        let stored = expected_snapshot();
        let mut generated_fields = stored.fields().to_vec();
        generated_fields.push(PersistedFieldSnapshot::new(
            FieldId::new(3),
            "score".to_string(),
            SchemaFieldSlot::new(2),
            PersistedFieldKind::Uint,
            Vec::new(),
            false,
            SchemaFieldDefault::SlotPayload(vec![0x10, 0, 0, 0, 0, 0, 0, 0, 7]),
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Uint64),
        ));
        let generated = PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.primary_key_field_id(),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                    (FieldId::new(3), SchemaFieldSlot::new(2)),
                ],
            ),
            generated_fields,
        );

        let SchemaTransitionDecision::Accepted(plan) =
            decide_schema_transition(&stored, &generated)
        else {
            panic!("append-only defaulted generated field should be an accepted transition");
        };

        assert_eq!(
            plan.kind(),
            SchemaTransitionPlanKind::AppendOnlyNullableFields
        );
        assert_eq!(plan.added_field_count(), 1);
    }

    #[test]
    fn schema_transition_policy_rejects_malformed_append_only_default_payloads() {
        let stored = expected_snapshot();
        let mut generated_fields = stored.fields().to_vec();
        generated_fields.push(PersistedFieldSnapshot::new(
            FieldId::new(3),
            "score".to_string(),
            SchemaFieldSlot::new(2),
            PersistedFieldKind::Uint,
            Vec::new(),
            false,
            SchemaFieldDefault::SlotPayload(vec![0x00]),
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Uint64),
        ));
        let generated = PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.primary_key_field_id(),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                    (FieldId::new(3), SchemaFieldSlot::new(2)),
                ],
            ),
            generated_fields,
        );

        let SchemaTransitionDecision::Rejected(rejection) =
            decide_schema_transition(&stored, &generated)
        else {
            panic!("malformed append-only default payload should be rejected");
        };

        assert_eq!(
            rejection.kind(),
            SchemaTransitionRejectionKind::FieldContract
        );
        assert!(
            rejection
                .detail()
                .contains("field must be nullable without a default or carry a valid explicit persisted default payload"),
            "unexpected malformed default payload rejection detail: {}",
            rejection.detail(),
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
        assert!(
            rejection
                .detail()
                .contains("stored_fields=2 generated_fields=2"),
            "row-layout drift should summarize layout sizes",
        );
        assert!(
            rejection.detail().contains(
                "first_difference=row_layout[0] stored_field_id=1 stored_slot=1 stored_name='id' stored_kind=Ulid; generated_field_id=1 generated_slot=0 generated_name='id' generated_kind=Ulid"
            ),
            "row-layout drift should identify the first changed field/slot pair",
        );
        assert!(
            !rejection.detail().contains("SchemaRowLayout"),
            "row-layout drift should not dump raw layout debug output",
        );
    }

    #[test]
    fn schema_transition_policy_rejects_primary_key_field_changes() {
        let expected = expected_snapshot();
        let changed = PersistedSchemaSnapshot::new(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            FieldId::new(2),
            expected.row_layout().clone(),
            expected.fields().to_vec(),
        );

        let SchemaTransitionDecision::Rejected(rejection) =
            decide_schema_transition(&changed, &expected)
        else {
            panic!("primary-key field drift should be rejected");
        };

        assert_eq!(
            rejection.kind(),
            SchemaTransitionRejectionKind::EntityIdentity
        );
        assert!(
            rejection
                .detail()
                .contains("primary key field id changed: stored=2 generated=1"),
            "primary-key drift should be identified before row decode can run",
        );
    }

    #[test]
    fn schema_transition_policy_rejects_field_type_changes() {
        let expected = expected_snapshot();
        let mut changed_fields = expected.fields().to_vec();
        changed_fields[1] = PersistedFieldSnapshot::new(
            FieldId::new(2),
            "name".to_string(),
            SchemaFieldSlot::new(1),
            PersistedFieldKind::Uint,
            Vec::new(),
            false,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Uint64),
        );
        let changed = PersistedSchemaSnapshot::new(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            expected.row_layout().clone(),
            changed_fields,
        );

        let SchemaTransitionDecision::Rejected(rejection) =
            decide_schema_transition(&changed, &expected)
        else {
            panic!("field type drift should be rejected");
        };

        assert_eq!(
            rejection.kind(),
            SchemaTransitionRejectionKind::FieldContract
        );
        assert!(
            rejection
                .detail()
                .contains("field[1] kind changed: stored=Uint generated=Text"),
            "field type drift should name the first changed field contract",
        );
    }

    #[test]
    fn schema_transition_policy_reports_first_nested_leaf_mismatch() {
        let stored = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "test::NestedSchemaEntity".to_string(),
            "NestedSchemaEntity".to_string(),
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
                    "profile".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Structured { queryable: false },
                    vec![PersistedNestedLeafSnapshot::new(
                        vec!["nickname".to_string()],
                        PersistedFieldKind::Text { max_len: None },
                        false,
                        FieldStorageDecode::ByKind,
                        LeafCodec::Scalar(ScalarCodec::Text),
                    )],
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::StructuralFallback,
                ),
            ],
        );
        let mut generated_fields = stored.fields().to_vec();
        generated_fields[1] = PersistedFieldSnapshot::new(
            FieldId::new(2),
            "profile".to_string(),
            SchemaFieldSlot::new(1),
            PersistedFieldKind::Structured { queryable: false },
            vec![PersistedNestedLeafSnapshot::new(
                vec!["score".to_string()],
                PersistedFieldKind::Uint,
                false,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Uint64),
            )],
            false,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::StructuralFallback,
        );
        let generated = PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.primary_key_field_id(),
            stored.row_layout().clone(),
            generated_fields,
        );

        let SchemaTransitionDecision::Rejected(rejection) =
            decide_schema_transition(&stored, &generated)
        else {
            panic!("nested leaf metadata drift should be rejected");
        };

        assert!(
            rejection.detail().contains(
                "field[1] nested leaf metadata changed: stored=1 generated=1; first_difference=nested_leaf[0]"
            ),
            "nested leaf drift should identify the owning field and first changed leaf",
        );
        assert!(
            rejection.detail().contains(
                "stored_path='nickname' stored_kind=Text { max_len: None } stored_nullable=false stored_storage_decode=ByKind stored_leaf_codec=Scalar(Text)"
            ),
            "nested leaf drift should describe the stored leaf contract",
        );
        assert!(
            rejection.detail().contains(
                "generated_path='score' generated_kind=Uint generated_nullable=false generated_storage_decode=ByKind generated_leaf_codec=Scalar(Uint64)"
            ),
            "nested leaf drift should describe the generated leaf contract",
        );
        assert_eq!(
            rejection.kind(),
            SchemaTransitionRejectionKind::FieldContract,
            "nested leaf drift remains a rejected field-contract transition",
        );
    }

    #[test]
    fn schema_transition_policy_names_unsupported_generated_removed_fields() {
        let expected = expected_snapshot();
        let mut stored_fields = expected.fields().to_vec();
        stored_fields.push(PersistedFieldSnapshot::new(
            FieldId::new(3),
            "legacy_score".to_string(),
            SchemaFieldSlot::new(2),
            PersistedFieldKind::Uint,
            Vec::new(),
            false,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Uint64),
        ));
        let changed = PersistedSchemaSnapshot::new(
            expected.version(),
            expected.entity_path().to_string(),
            expected.entity_name().to_string(),
            expected.primary_key_field_id(),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                    (FieldId::new(3), SchemaFieldSlot::new(2)),
                ],
            ),
            stored_fields,
        );

        let SchemaTransitionDecision::Rejected(rejection) =
            decide_schema_transition(&changed, &expected)
        else {
            panic!("stored extra row-layout field should be rejected");
        };

        assert!(
            rejection.detail().contains(
                "unsupported removed field transition: stored field[2] id=3 slot=2 name='legacy_score' kind=Uint; retained-slot support is not enabled yet"
            ),
            "removed field drift should be named as an unsupported future transition shape",
        );
        assert_eq!(
            rejection.kind(),
            SchemaTransitionRejectionKind::FieldContract,
            "unsupported removals are future field-contract transitions, not generic row-layout mismatches",
        );
    }

    #[test]
    fn schema_transition_policy_names_unsupported_generated_additive_fields() {
        let stored = expected_snapshot();
        let mut generated_fields = stored.fields().to_vec();
        generated_fields.push(PersistedFieldSnapshot::new(
            FieldId::new(3),
            "new_score".to_string(),
            SchemaFieldSlot::new(2),
            PersistedFieldKind::Uint,
            Vec::new(),
            false,
            SchemaFieldDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Uint64),
        ));
        let generated = PersistedSchemaSnapshot::new(
            stored.version(),
            stored.entity_path().to_string(),
            stored.entity_name().to_string(),
            stored.primary_key_field_id(),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                    (FieldId::new(3), SchemaFieldSlot::new(2)),
                ],
            ),
            generated_fields,
        );

        let SchemaTransitionDecision::Rejected(rejection) =
            decide_schema_transition(&stored, &generated)
        else {
            panic!("generated additive field should be rejected until additive policy exists");
        };

        assert!(
            rejection.detail().contains(
                "unsupported additive field transition: generated field[2] id=3 slot=2 name='new_score' kind=Uint nullable=false default=None; field must be nullable without a default or carry a valid explicit persisted default payload"
            ),
            "additive field drift should be named as an unsupported future transition shape",
        );
        assert_eq!(
            rejection.kind(),
            SchemaTransitionRejectionKind::FieldContract,
            "unsupported additive fields are a future field-contract transition, not a generic row-layout mismatch",
        );
    }
}
