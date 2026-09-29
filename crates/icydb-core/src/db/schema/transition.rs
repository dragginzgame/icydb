//! Module: db::schema::transition
//! Responsibility: schema transition policy and rejection diagnostics.
//! Does not own: startup reconciliation orchestration or schema-store persistence.
//! Boundary: decides whether one accepted snapshot may become another.

mod admission;
mod compatibility;

use crate::db::schema::{
    MutationPlan, MutationPublicationPreflight, PersistedFieldSnapshot, PersistedSchemaSnapshot,
    SchemaFieldPathIndexRebuildTarget, SchemaMutationRequest,
    schema_mutation_request_for_snapshots,
};

#[cfg(any(test, feature = "sql"))]
use crate::db::schema::SchemaExpressionIndexRebuildTarget;

pub(in crate::db::schema) use admission::SchemaAdmissionRejectionClassification;
#[cfg(feature = "sql")]
pub(in crate::db::schema) use admission::SchemaAdmissionRejectionReason;
#[cfg(feature = "sql")]
pub(in crate::db::schema) use admission::{
    SchemaAdmissionIdentityComparison, schema_admission_rejection,
};
use compatibility::{
    accepted_snapshot_extends_generated_indexes,
    accepted_snapshot_extends_generated_with_ddl_fields, accepted_snapshot_matches_generated_shape,
    field_has_supported_historical_fill, generated_constraint_activations_only_changed,
    generated_field_defaults_only_changed, generated_field_follows_accepted_ddl_extension,
    generated_index_names_only_changed,
};

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
    AppendOnlyFields,
    ConstraintActivation,
    ExactMatch,
    MetadataOnlyFieldDefault,
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
    GeneratedFieldAfterDdlField { field_index: usize },
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
    FieldWritePolicy { field_index: usize },
    FieldStorageDecode { field_index: usize },
    FieldLeafCodec { field_index: usize },
    Snapshot,
    SchemaAdmission,
}

///
/// SchemaTransitionRejectionDetail
///
/// Retains the compact first-difference code for typed rejection diagnostics.
///

#[derive(Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaTransitionRejectionDetail {
    code: SchemaTransitionRejectionDetailCode,
}

impl SchemaTransitionRejectionDetail {
    const fn new(code: SchemaTransitionRejectionDetailCode) -> Self {
        Self { code }
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

    // Return the structured schema-version admission decision when this
    // rejection came from the version/method/fingerprint gate.
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
    if generated_constraint_activations_only_changed(actual, expected) {
        return SchemaTransitionDecision::Accepted(SchemaTransitionPlan::from_mutation_request(
            SchemaTransitionPlanKind::ConstraintActivation,
            SchemaMutationRequest::ExactMatch,
        ));
    }
    if generated_index_names_only_changed(actual, expected) {
        return SchemaTransitionDecision::Accepted(SchemaTransitionPlan::from_mutation_request(
            SchemaTransitionPlanKind::MetadataOnlyIndexRename,
            SchemaMutationRequest::ExactMatch,
        ));
    }

    if generated_field_defaults_only_changed(actual, expected) {
        return SchemaTransitionDecision::Accepted(SchemaTransitionPlan::from_mutation_request(
            SchemaTransitionPlanKind::MetadataOnlyFieldDefault,
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
            if added_fields.iter().all(|field| {
                field_has_supported_historical_fill(field, expected.row_layout().history_floor())
            }) =>
        {
            return SchemaTransitionDecision::Accepted(
                SchemaTransitionPlan::from_mutation_request(
                    SchemaTransitionPlanKind::AppendOnlyFields,
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

// Return the first typed schema difference between the stored
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
            SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::EntityPath),
        );
    }

    if actual.entity_name() != expected.entity_name() {
        return (
            SchemaTransitionRejectionKind::EntityIdentity,
            SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::EntityName),
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
            SchemaTransitionRejectionDetail::new(
                SchemaTransitionRejectionDetailCode::PrimaryKeyFields,
            ),
        );
    }

    if let Some(field_index) = generated_field_follows_accepted_ddl_extension(actual, expected) {
        return (
            SchemaTransitionRejectionKind::FieldSlot,
            SchemaTransitionRejectionDetail::new(
                SchemaTransitionRejectionDetailCode::GeneratedFieldAfterDdlField { field_index },
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
            SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::RowLayout),
        );
    }

    if actual.fields().len() != expected.fields().len() {
        return (
            SchemaTransitionRejectionKind::FieldContract,
            SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::FieldCount),
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
        SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::Snapshot),
    )
}

// Detect a freshly versioned append-only candidate whose historical-fill
// contract still cannot be accepted. Unlowered generated proposals fail the
// row-layout boundary before reaching this diagnostic.
fn unsupported_generated_additive_field_detail(
    actual: &PersistedSchemaSnapshot,
    expected: &PersistedSchemaSnapshot,
) -> Option<SchemaTransitionRejectionDetail> {
    let Some(SchemaMutationRequest::AppendOnlyFields(_)) =
        schema_mutation_request_for_snapshots(actual, expected)
    else {
        return None;
    };

    Some(SchemaTransitionRejectionDetail::new(
        SchemaTransitionRejectionDetailCode::UnsupportedAdditiveField {
            field_index: actual.fields().len(),
        },
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

    Some(SchemaTransitionRejectionDetail::new(
        SchemaTransitionRejectionDetailCode::UnsupportedRemovedField {
            field_index: expected.fields().len(),
        },
    ))
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
    if actual.id() != expected.id() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::FieldId {
                field_index: index,
            }),
        ));
    }

    if actual.name() != expected.name() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::FieldName {
                field_index: index,
            }),
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
            SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::FieldSlot {
                field_index: index,
            }),
        ));
    }

    if actual.kind() != expected.kind() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::FieldKind {
                field_index: index,
            }),
        ));
    }

    if actual.nested_leaves() != expected.nested_leaves() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            SchemaTransitionRejectionDetail::new(SchemaTransitionRejectionDetailCode::NestedLeaf {
                field_index: index,
            }),
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
) -> Option<(
    SchemaTransitionRejectionKind,
    SchemaTransitionRejectionDetail,
)> {
    if actual.nullable() != expected.nullable() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            SchemaTransitionRejectionDetail::new(
                SchemaTransitionRejectionDetailCode::FieldNullability { field_index: index },
            ),
        ));
    }

    if actual.insert_default() != expected.insert_default() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            SchemaTransitionRejectionDetail::new(
                SchemaTransitionRejectionDetailCode::FieldDefault { field_index: index },
            ),
        ));
    }

    if actual.write_policy() != expected.write_policy() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            SchemaTransitionRejectionDetail::new(
                SchemaTransitionRejectionDetailCode::FieldWritePolicy { field_index: index },
            ),
        ));
    }

    if actual.storage_decode() != expected.storage_decode() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            SchemaTransitionRejectionDetail::new(
                SchemaTransitionRejectionDetailCode::FieldStorageDecode { field_index: index },
            ),
        ));
    }

    if actual.leaf_codec() != expected.leaf_codec() {
        return Some((
            SchemaTransitionRejectionKind::FieldContract,
            SchemaTransitionRejectionDetail::new(
                SchemaTransitionRejectionDetailCode::FieldLeafCodec { field_index: index },
            ),
        ));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::{
        AcceptedConstraintCatalog, AcceptedFieldKind, AcceptedNamedTypeIdentity,
        AcceptedRuleOperation, AcceptedRuleTarget, AcceptedSchemaFingerprint,
        ConstraintActivationKind, ConstraintActivationSnapshot, ConstraintActivationState,
        ConstraintId, ConstraintIdAllocator, ConstraintOrigin, FieldId, FieldInsertGeneration,
        FieldStorageDecode, LeafCodec, PersistedFieldOrigin, PersistedRelationEdgeSnapshot,
        PersistedRelationPathStepSnapshot, RelationId, RowLayoutVersion, ScalarCodec,
        SchemaFieldSlot, SchemaFieldWritePolicy, SchemaHistoricalFill, SchemaInsertDefault,
        SchemaRowLayout, SchemaVersion, composite_catalog::CompositeTypeId,
    };

    fn snapshot(generation: Option<FieldInsertGeneration>) -> PersistedSchemaSnapshot {
        PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "schema::transition::Identity".to_string(),
            "Identity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
            vec![PersistedFieldSnapshot::new_initial_with_write_policy(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                SchemaFieldWritePolicy::from_model_policies(generation, None),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            )],
        )
    }

    #[test]
    fn generated_transition_rejects_identity_policy_drift() {
        for (actual, expected) in [
            (None, Some(FieldInsertGeneration::Identity)),
            (Some(FieldInsertGeneration::Identity), None),
        ] {
            let decision = decide_schema_transition(&snapshot(actual), &snapshot(expected));
            let SchemaTransitionDecision::Rejected(rejection) = decision else {
                panic!("identity policy drift must require an explicit migration");
            };

            assert_eq!(rejection.kind, SchemaTransitionRejectionKind::FieldContract);
            assert_eq!(
                rejection.detail.code,
                SchemaTransitionRejectionDetailCode::FieldWritePolicy { field_index: 0 },
            );
        }
    }

    #[test]
    fn generated_transition_accepts_exact_stable_identity_targeted_replacement() {
        let actual = snapshot(None);
        let target = AcceptedRuleTarget::new(
            FieldId::new(1),
            AcceptedNamedTypeIdentity::Composite(
                CompositeTypeId::new(1).expect("test composite identity should be non-zero"),
            ),
        );
        let catalog = actual
            .constraint_catalog()
            .clone()
            .with_added_targeted_rule(
                "limit".to_string(),
                ConstraintOrigin::Generated,
                target,
                AcceptedRuleOperation::LengthRangeInclusive { min: 1, max: 8 },
            )
            .expect("accepted targeted rule should allocate");
        let actual = actual.with_constraint_catalog(catalog);
        let id = actual
            .constraints()
            .last()
            .expect("targeted rule should exist")
            .id();
        let candidate_catalog = actual
            .constraint_catalog()
            .clone()
            .with_replaced_targeted_rule_activation(
                id,
                target,
                AcceptedRuleOperation::LengthRangeInclusive { min: 2, max: 7 },
                AcceptedSchemaFingerprint::new([0x91; 32]),
                2,
            )
            .expect("stable-identity semantic replacement should stage");
        let expected = actual.clone().with_constraint_catalog(candidate_catalog);

        let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(&actual, &expected)
        else {
            panic!("stable-identity semantic replacement should use constraint activation");
        };
        assert_eq!(plan.kind(), SchemaTransitionPlanKind::ConstraintActivation);
    }

    #[test]
    fn generated_transition_accepts_nested_relation_rooted_in_one_appended_field() {
        let actual = snapshot(None);
        let current_layout = RowLayoutVersion::INITIAL
            .checked_next()
            .expect("test layout should advance");
        let mut fields = actual.fields().to_vec();
        fields.push(PersistedFieldSnapshot::new_with_write_policy_and_origin(
            FieldId::new(2),
            "target_id".to_string(),
            SchemaFieldSlot::new(1),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            true,
            current_layout,
            SchemaInsertDefault::None,
            SchemaHistoricalFill::Null,
            SchemaFieldWritePolicy::none(),
            PersistedFieldOrigin::Generated,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        ));
        let relation = PersistedRelationEdgeSnapshot::new_nested(
            RelationId::new(1).expect("test relation identity should be non-zero"),
            "target_id".to_string(),
            "schema::transition::Target".to_string(),
            FieldId::new(2),
            vec![PersistedRelationPathStepSnapshot::OptionalSome],
        )
        .clone_with_physical_generation(7);
        let activation_id = ConstraintId::new(
            actual
                .constraint_catalog()
                .allocator()
                .high_water()
                .checked_add(1)
                .expect("test constraint identity should advance"),
        )
        .expect("test constraint identity should be non-zero");
        let activation = ConstraintActivationSnapshot::new(
            activation_id,
            relation.name().to_string(),
            ConstraintOrigin::Generated,
            ConstraintActivationKind::Relation {
                relation_id: relation.id(),
            },
            ConstraintActivationState::EnforcingNewWrites,
            AcceptedSchemaFingerprint::new([0x53; 32]),
            relation.physical_generation(),
        );
        let catalog = AcceptedConstraintCatalog::from_persisted_parts(
            ConstraintIdAllocator::new(activation_id.get()),
            actual.constraints().to_vec(),
            vec![activation],
        );
        let expected = PersistedSchemaSnapshot::new(
            actual.version(),
            actual.entity_path().to_string(),
            actual.entity_name().to_string(),
            actual.primary_key_field_ids().to_vec(),
            SchemaRowLayout::new(
                current_layout,
                RowLayoutVersion::INITIAL,
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                ],
            ),
            fields,
        )
        .with_constraint_catalog(catalog)
        .with_constraint_candidates(Vec::new(), vec![relation]);

        let SchemaTransitionDecision::Accepted(plan) = decide_schema_transition(&actual, &expected)
        else {
            panic!("nested relation on one appended field should use constraint activation");
        };
        assert_eq!(plan.kind(), SchemaTransitionPlanKind::ConstraintActivation);
    }
}
