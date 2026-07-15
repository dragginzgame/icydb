//! Module: db::schema::transition
//! Responsibility: schema transition policy and rejection diagnostics.
//! Does not own: startup reconciliation orchestration or schema-store persistence.
//! Boundary: decides whether one accepted snapshot may become another.

mod admission;
mod compatibility;

use crate::db::schema::{
    MutationPlan, MutationPublicationPreflight, PersistedFieldSnapshot,
    PersistedNestedLeafSnapshot, PersistedSchemaSnapshot, SchemaFieldPathIndexRebuildTarget,
    SchemaMutationRequest, schema_mutation_request_for_snapshots,
};

#[cfg(any(test, feature = "sql"))]
use crate::db::schema::SchemaExpressionIndexRebuildTarget;

#[cfg(test)]
use crate::db::schema::{FieldId, SchemaFieldSlot};

#[cfg(any(test, feature = "sql"))]
pub(in crate::db::schema) use admission::SchemaAdmissionRejectionReason;
pub(in crate::db::schema) use admission::{
    SchemaAdmissionIdentityComparison, SchemaAdmissionRejectionClassification,
    schema_admission_rejection,
};
use compatibility::{
    accepted_snapshot_extends_generated_indexes,
    accepted_snapshot_extends_generated_with_ddl_fields, accepted_snapshot_matches_generated_shape,
    field_has_supported_missing_absence_policy, generated_index_names_only_changed,
};

#[cfg(test)]
use admission::classify_schema_admission_rejection;

macro_rules! transition_detail {
    ($code:expr, $rich:expr) => {{
        #[cfg(test)]
        {
            SchemaTransitionRejectionDetail::new($code, $rich)
        }
        #[cfg(not(test))]
        {
            SchemaTransitionRejectionDetail::new($code)
        }
    }};
}

///
/// SchemaTransitionDecision
///
/// SchemaTransitionDecision is the schema-owned result of comparing a
/// persisted accepted snapshot with the generated proposal for the same entity.
/// It exists so reconciliation policy can distinguish accepted transitions
/// from rejected transitions before reconciliation publishes a new accepted
/// snapshot.
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
    AddExpressionIndex,
    AddFieldPathIndex,
    AppendOnlyNullableFields,
    ExactMatch,
    MetadataOnlyIndexRename,
}

///
/// SchemaTransitionPlan
///
/// SchemaTransitionPlan is the schema-owned artifact that authorizes startup
/// reconciliation to accept a generated proposal against a stored schema
/// snapshot and carries the canonical mutation plan for that transition.
///

#[derive(Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaTransitionPlan {
    kind: SchemaTransitionPlanKind,
    mutation_plan: MutationPlan,
}

impl SchemaTransitionPlan {
    // Build one transition plan from a schema-owned mutation request after
    // transition policy has selected the accepted plan kind.
    fn from_mutation_request(
        kind: SchemaTransitionPlanKind,
        request: SchemaMutationRequest<'_>,
    ) -> Self {
        Self {
            kind,
            mutation_plan: request.into(),
        }
    }

    // Return the accepted-plan bucket used by reconciliation diagnostics.
    pub(in crate::db::schema) const fn kind(&self) -> SchemaTransitionPlanKind {
        self.kind
    }

    // Return the schema-owned publication decision. Physical work must complete
    // through the matching concrete runner before its snapshot can be stored.
    pub(in crate::db::schema) const fn publication_preflight(
        &self,
    ) -> MutationPublicationPreflight {
        self.mutation_plan.publication_preflight()
    }

    pub(in crate::db::schema) const fn field_path_index_target(
        &self,
    ) -> Option<&SchemaFieldPathIndexRebuildTarget> {
        self.mutation_plan.field_path_index_target()
    }

    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db::schema) const fn expression_index_target(
        &self,
    ) -> Option<&SchemaExpressionIndexRebuildTarget> {
        self.mutation_plan.expression_index_target()
    }

    // Borrow the catalog-native mutation plan behind this reconciliation
    // transition.
    pub(in crate::db::schema) const fn mutation_plan(&self) -> &MutationPlan {
        &self.mutation_plan
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
/// SchemaTransitionRejectionDetailCode
///
/// Compact transition-detail taxonomy. This keeps production rejection state
/// structured without retaining rendered diagnostic prose in wasm builds.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaTransitionRejectionDetailCode {
    EntityPath,
    EntityName,
    PrimaryKeyFields,
    UnsupportedAdditiveField { field_index: usize },
    UnsupportedRemovedField { field_index: usize },
    RowLayout,
    FieldCount,
    FieldId { field_index: usize },
    FieldName { field_index: usize },
    FieldSlot { field_index: usize },
    FieldKind { field_index: usize },
    NestedLeaf { field_index: usize },
    FieldNullability { field_index: usize },
    FieldDefault { field_index: usize },
    FieldStorageDecode { field_index: usize },
    FieldLeafCodec { field_index: usize },
    Snapshot,
    SchemaAdmission,
}

///
/// SchemaTransitionRejectionDetail
///
/// Production keeps only a compact detail code. Tests retain the rich
/// first-difference text so transition diagnostics can stay well specified
/// without carrying those strings into runtime builds.
///

#[derive(Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaTransitionRejectionDetail {
    code: SchemaTransitionRejectionDetailCode,
    #[cfg(test)]
    rich: String,
}

impl SchemaTransitionRejectionDetail {
    #[cfg(test)]
    const fn new(code: SchemaTransitionRejectionDetailCode, rich: String) -> Self {
        Self { code, rich }
    }

    #[cfg(not(test))]
    const fn new(code: SchemaTransitionRejectionDetailCode) -> Self {
        Self { code }
    }

    #[cfg(test)]
    const fn as_str(&self) -> &str {
        self.rich.as_str()
    }
}

///
/// SchemaTransitionRejection
///
/// SchemaTransitionRejection carries the schema-owned diagnostic for one
/// rejected transition decision. It keeps policy selection separate from final
/// user-facing error formatting and preserves typed rejection metadata.
///

#[derive(Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaTransitionRejection {
    kind: SchemaTransitionRejectionKind,
    detail: SchemaTransitionRejectionDetail,
    admission: Option<SchemaAdmissionRejectionClassification>,
}

impl SchemaTransitionRejection {
    // Build one transition rejection from the first schema mismatch detail
    // produced by the diagnostic comparison helpers below.
    pub(super) const fn new(
        kind: SchemaTransitionRejectionKind,
        detail: SchemaTransitionRejectionDetail,
        admission: Option<SchemaAdmissionRejectionClassification>,
    ) -> Self {
        Self {
            kind,
            detail,
            admission,
        }
    }

    // Return the stable rejection bucket for metrics and audit readouts.
    pub(in crate::db::schema) const fn kind(&self) -> SchemaTransitionRejectionKind {
        self.kind
    }

    // Borrow the first rejected transition detail for final error formatting.
    #[cfg(test)]
    pub(in crate::db::schema) const fn detail(&self) -> &str {
        self.detail.as_str()
    }

    // Return the structured schema-version admission decision when this
    // rejection came from the version/method/fingerprint gate.
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db::schema) const fn admission(
        &self,
    ) -> Option<SchemaAdmissionRejectionClassification> {
        self.admission
    }
}

// Decide whether one persisted snapshot may transition to the generated
// proposal. Mutation shape classification lives in schema::mutation; this
// policy layer validates whether the classified delta can be published now.
pub(in crate::db::schema) fn decide_schema_transition(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> SchemaTransitionDecision {
    if generated_index_names_only_changed(actual, expected) {
        return SchemaTransitionDecision::Accepted(SchemaTransitionPlan::from_mutation_request(
            SchemaTransitionPlanKind::MetadataOnlyIndexRename,
            SchemaMutationRequest::ExactMatch,
        ));
    }

    if accepted_snapshot_extends_generated_indexes(actual, expected) {
        return SchemaTransitionDecision::Accepted(SchemaTransitionPlan::from_mutation_request(
            SchemaTransitionPlanKind::ExactMatch,
            SchemaMutationRequest::ExactMatch,
        ));
    }

    if accepted_snapshot_extends_generated_with_ddl_fields(actual, expected) {
        return SchemaTransitionDecision::Accepted(SchemaTransitionPlan::from_mutation_request(
            SchemaTransitionPlanKind::ExactMatch,
            SchemaMutationRequest::ExactMatch,
        ));
    }

    if accepted_snapshot_matches_generated_shape(actual, expected) {
        return SchemaTransitionDecision::Accepted(SchemaTransitionPlan::from_mutation_request(
            SchemaTransitionPlanKind::ExactMatch,
            SchemaMutationRequest::ExactMatch,
        ));
    }

    match schema_mutation_request_for_snapshots(actual, expected) {
        Some(SchemaMutationRequest::ExactMatch) => {
            return SchemaTransitionDecision::Accepted(
                SchemaTransitionPlan::from_mutation_request(
                    SchemaTransitionPlanKind::ExactMatch,
                    SchemaMutationRequest::ExactMatch,
                ),
            );
        }
        Some(SchemaMutationRequest::AppendOnlyFields(added_fields))
            if added_fields
                .iter()
                .all(field_has_supported_missing_absence_policy) =>
        {
            return SchemaTransitionDecision::Accepted(
                SchemaTransitionPlan::from_mutation_request(
                    SchemaTransitionPlanKind::AppendOnlyNullableFields,
                    SchemaMutationRequest::AppendOnlyFields(added_fields),
                ),
            );
        }
        Some(SchemaMutationRequest::AddFieldPathIndex { target }) => {
            return SchemaTransitionDecision::Accepted(
                SchemaTransitionPlan::from_mutation_request(
                    SchemaTransitionPlanKind::AddFieldPathIndex,
                    SchemaMutationRequest::AddFieldPathIndex { target },
                ),
            );
        }
        Some(SchemaMutationRequest::AddExpressionIndex { target }) => {
            return SchemaTransitionDecision::Accepted(
                SchemaTransitionPlan::from_mutation_request(
                    SchemaTransitionPlanKind::AddExpressionIndex,
                    SchemaMutationRequest::AddExpressionIndex { target },
                ),
            );
        }
        Some(SchemaMutationRequest::AppendOnlyFields(_)) | None => {}
    }

    let (kind, detail) = schema_snapshot_mismatch_detail(actual, expected);

    SchemaTransitionDecision::Rejected(SchemaTransitionRejection::new(kind, detail, None))
}

// Return the first human-readable schema difference between the stored
// snapshot and the current generated proposal. Schema version differences are
// owned by the admission gate; transition diagnostics describe the shape
// that remains after a candidate has passed version/fingerprint admission.
fn schema_snapshot_mismatch_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> (
    SchemaTransitionRejectionKind,
    SchemaTransitionRejectionDetail,
) {
    if actual.entity_path() != expected.entity_path() {
        return (
            SchemaTransitionRejectionKind::EntityIdentity,
            transition_detail!(
                SchemaTransitionRejectionDetailCode::EntityPath,
                format!(
                    "entity path changed: stored='{}' generated='{}'",
                    actual.entity_path(),
                    expected.entity_path(),
                )
            ),
        );
    }

    if actual.entity_name() != expected.entity_name() {
        return (
            SchemaTransitionRejectionKind::EntityIdentity,
            transition_detail!(
                SchemaTransitionRejectionDetailCode::EntityName,
                format!(
                    "entity name changed: stored='{}' generated='{}'",
                    actual.entity_name(),
                    expected.entity_name(),
                )
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
) -> (
    SchemaTransitionRejectionKind,
    SchemaTransitionRejectionDetail,
) {
    if actual.primary_key_field_ids() != expected.primary_key_field_ids() {
        return (
            SchemaTransitionRejectionKind::EntityIdentity,
            transition_detail!(
                SchemaTransitionRejectionDetailCode::PrimaryKeyFields,
                format!(
                    "primary key field ids changed: stored={:?} generated={:?}",
                    actual
                        .primary_key_field_ids()
                        .iter()
                        .map(|field_id| field_id.get())
                        .collect::<Vec<_>>(),
                    expected
                        .primary_key_field_ids()
                        .iter()
                        .map(|field_id| field_id.get())
                        .collect::<Vec<_>>(),
                )
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
            transition_detail!(
                SchemaTransitionRejectionDetailCode::FieldCount,
                format!(
                    "field count changed: stored={} generated={}",
                    actual.fields().len(),
                    expected.fields().len(),
                )
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
        transition_detail!(
            SchemaTransitionRejectionDetailCode::Snapshot,
            "schema snapshot changed".to_string()
        ),
    )
}

// Detect an append-only additive-field transition shape that still cannot be
// accepted. Nullable no-default additions are accepted earlier; this diagnostic
// names additive fields whose absence policy is not supported yet.
fn unsupported_generated_additive_field_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> Option<SchemaTransitionRejectionDetail> {
    let Some(SchemaMutationRequest::AppendOnlyFields(added_fields)) =
        schema_mutation_request_for_snapshots(actual, expected)
    else {
        return None;
    };

    #[cfg(not(test))]
    let _ = added_fields;

    Some(transition_detail!(
        SchemaTransitionRejectionDetailCode::UnsupportedAdditiveField {
            field_index: actual.fields().len()
        },
        {
            let index = actual.fields().len();
            let field = &added_fields[0];
            format!(
                "unsupported additive field transition: generated field[{index}] id={} slot={} name='{}' kind={:?} nullable={} default={:?}; field must be nullable without a default or carry a valid explicit persisted default payload",
                field.id().get(),
                field.slot().get(),
                field.name(),
                field.kind(),
                field.nullable(),
                field.default(),
            )
        }
    ))
}

// Detect the symmetric field-removal transition shape without accepting it.
// A generated snapshot is a removal candidate only when the generated fields
// and row-layout mappings are exact prefixes of the stored accepted snapshot.
// That means the new code has stopped declaring a field that old rows may
// still carry, which needs catalog-native physical DDL work before acceptance.
fn unsupported_generated_removed_field_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> Option<SchemaTransitionRejectionDetail> {
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

    Some(transition_detail!(
        SchemaTransitionRejectionDetailCode::UnsupportedRemovedField {
            field_index: expected.fields().len()
        },
        {
            let index = expected.fields().len();
            let field = &actual.fields()[index];
            format!(
                "unsupported generated field removal: stored field[{index}] id={} slot={} name='{}' kind={:?}; startup reconciliation does not perform physical DDL work",
                field.id().get(),
                field.slot().get(),
                field.name(),
                field.kind(),
            )
        }
    ))
}

// Summarize row-layout drift without dumping every field/slot pair into the
// startup error. Full layout dumps are too noisy for normal schema-change
// rejection, while the first changed/missing/added fact is enough to debug the
// generated-vs-accepted mismatch.
#[cfg(test)]
fn row_layout_mismatch_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> SchemaTransitionRejectionDetail {
    transition_detail!(SchemaTransitionRejectionDetailCode::RowLayout, {
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
            prefix
        } else if let Some(detail) = row_layout_first_pair_mismatch_detail(actual, expected) {
            format!("{prefix}; {detail}")
        } else {
            prefix
        }
    })
}

#[cfg(not(test))]
const fn row_layout_mismatch_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> SchemaTransitionRejectionDetail {
    let _ = (actual, expected);
    SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::RowLayout)
}

// Report the first row-layout pair difference in deterministic vector order.
// Schema evolution is still exact-match only, so diagnostics should identify
// the earliest changed fact without attempting a migration diff.
#[cfg(test)]
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
#[cfg(test)]
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
) -> Option<(
    SchemaTransitionRejectionKind,
    SchemaTransitionRejectionDetail,
)> {
    #[cfg(not(test))]
    let _ = index;

    if actual.id() != expected.id() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            transition_detail!(
                SchemaTransitionRejectionDetailCode::FieldId { field_index: index },
                format!(
                    "field[{index}] id changed: stored={} generated={}",
                    actual.id().get(),
                    expected.id().get(),
                )
            ),
        ));
    }

    if actual.name() != expected.name() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            transition_detail!(
                SchemaTransitionRejectionDetailCode::FieldName { field_index: index },
                format!(
                    "field[{index}] name changed: stored='{}' generated='{}'",
                    actual.name(),
                    expected.name(),
                )
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
) -> Option<(
    SchemaTransitionRejectionKind,
    SchemaTransitionRejectionDetail,
)> {
    if actual.slot() != expected.slot() {
        return Some((
            SchemaTransitionRejectionKind::FieldSlot,
            transition_detail!(
                SchemaTransitionRejectionDetailCode::FieldSlot { field_index: index },
                format!(
                    "field[{index}] slot changed: stored={} generated={}",
                    actual.slot().get(),
                    expected.slot().get(),
                )
            ),
        ));
    }

    if actual.kind() != expected.kind() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            transition_detail!(
                SchemaTransitionRejectionDetailCode::FieldKind { field_index: index },
                format!(
                    "field[{index}] kind changed: stored={:?} generated={:?}",
                    actual.kind(),
                    expected.kind(),
                )
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
#[cfg(test)]
fn nested_leaf_mismatch_detail(
    field_index: usize,
    actual: &[PersistedNestedLeafSnapshot],
    expected: &[PersistedNestedLeafSnapshot],
) -> SchemaTransitionRejectionDetail {
    transition_detail!(
        SchemaTransitionRejectionDetailCode::NestedLeaf { field_index },
        {
            let prefix = format!(
                "field[{field_index}] nested leaf metadata changed: stored={} generated={}",
                actual.len(),
                expected.len(),
            );

            if let Some(detail) = nested_leaf_first_mismatch_detail(actual, expected) {
                format!("{prefix}; {detail}")
            } else {
                prefix
            }
        }
    )
}

#[cfg(not(test))]
const fn nested_leaf_mismatch_detail(
    field_index: usize,
    actual: &[PersistedNestedLeafSnapshot],
    expected: &[PersistedNestedLeafSnapshot],
) -> SchemaTransitionRejectionDetail {
    let _ = (actual, expected);
    SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::NestedLeaf {
        field_index,
    })
}

// Find the first changed nested leaf fact in stable vector order. The transition
// policy still rejects every nested metadata drift; this helper only names the
// earliest changed fact so operators can see what generated code changed.
#[cfg(test)]
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
#[cfg(test)]
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
) -> Option<(
    SchemaTransitionRejectionKind,
    SchemaTransitionRejectionDetail,
)> {
    #[cfg(not(test))]
    let _ = index;

    if actual.nullable() != expected.nullable() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            transition_detail!(
                SchemaTransitionRejectionDetailCode::FieldNullability { field_index: index },
                format!(
                    "field[{index}] nullability changed: stored={} generated={}",
                    actual.nullable(),
                    expected.nullable(),
                )
            ),
        ));
    }

    if actual.default() != expected.default() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            transition_detail!(
                SchemaTransitionRejectionDetailCode::FieldDefault { field_index: index },
                format!(
                    "field[{index}] default changed: stored={:?} generated={:?}",
                    actual.default(),
                    expected.default(),
                )
            ),
        ));
    }

    if actual.storage_decode() != expected.storage_decode() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            transition_detail!(
                SchemaTransitionRejectionDetailCode::FieldStorageDecode { field_index: index },
                format!(
                    "field[{index}] storage decode changed: stored={:?} generated={:?}",
                    actual.storage_decode(),
                    expected.storage_decode(),
                )
            ),
        ));
    }

    if actual.leaf_codec() != expected.leaf_codec() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            transition_detail!(
                SchemaTransitionRejectionDetailCode::FieldLeafCodec { field_index: index },
                format!(
                    "field[{index}] leaf codec changed: stored={:?} generated={:?}",
                    actual.leaf_codec(),
                    expected.leaf_codec(),
                )
            ),
        ));
    }

    None
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
