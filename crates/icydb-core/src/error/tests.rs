//! Module: error::tests
//! Covers the error taxonomy mapping and constructor invariants defined by the
//! core error surface.

use std::mem::size_of;

use super::*;
use crate::db::{
    access::AccessPlanError,
    cursor::{CursorPlanError, CursorSignaturePrefix},
    query::plan::{
        PlanError, PolicyPlanError,
        validate::{GroupPlanError, OrderPlanError, PlanErrorKind, PlanPolicyError, PlanUserError},
    },
};

#[test]
fn internal_error_taxonomy_axes_remain_one_byte() {
    assert_eq!(size_of::<ErrorClass>(), 1);
    assert_eq!(size_of::<ErrorOrigin>(), 1);
    assert_eq!(format!("{:?}", ErrorClass::Corruption), "0");
    assert_eq!(format!("{:?}", ErrorOrigin::Serialize), "0");
}

fn from_group_plan_error(err: PlanError) -> InternalError {
    match err.into_kind() {
        PlanErrorKind::User(inner) => match *inner {
            PlanUserError::Group(_) => InternalError::query_invalid_logical_plan(),
            _ => InternalError::planner_executor_invariant(),
        },
        PlanErrorKind::Policy(inner) => match *inner {
            PlanPolicyError::Group(_) => InternalError::query_invalid_logical_plan(),
            PlanPolicyError::Policy(_) => InternalError::planner_executor_invariant(),
        },
        PlanErrorKind::Cursor(_) => InternalError::planner_executor_invariant(),
    }
}

fn plan_invariant_violation(err: PolicyPlanError) -> InternalError {
    let _ = err;
    InternalError::planner_executor_invariant()
}

fn assert_runtime_invariant(err: &InternalError, origin: ErrorOrigin) {
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, origin);

    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation
    );
    assert_eq!(diagnostic.origin(), origin.diagnostic_origin());
    assert_eq!(diagnostic.detail(), None);
}

fn assert_runtime_corruption(err: &InternalError, origin: ErrorOrigin) {
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, origin);

    let diagnostic = err.diagnostic();
    let expected_code = if matches!(origin, ErrorOrigin::Store) {
        icydb_diagnostic_code::DiagnosticCode::StoreCorruption
    } else {
        icydb_diagnostic_code::DiagnosticCode::RuntimeCorruption
    };
    assert_eq!(diagnostic.code(), expected_code);
    assert_eq!(diagnostic.origin(), origin.diagnostic_origin());
}

const fn cursor_payload_error() -> CursorPlanError {
    CursorPlanError::grouped_continuation_cursor_direction_mismatch()
}

const fn cursor_signature_mismatch_error() -> CursorPlanError {
    CursorPlanError::ContinuationCursorSignatureMismatch {
        expected: CursorSignaturePrefix::UNKNOWN,
        actual: CursorSignaturePrefix::UNKNOWN,
    }
}

const fn cursor_window_error() -> CursorPlanError {
    CursorPlanError::ContinuationCursorWindowMismatch {
        expected_offset: 4,
        actual_offset: 2,
    }
}

#[test]
fn index_plan_index_corruption_uses_index_origin() {
    let err = InternalError::index_plan_index_corruption();
    assert_runtime_corruption(&err, ErrorOrigin::Index);
}

#[test]
fn index_plan_store_corruption_uses_store_origin() {
    let err = InternalError::index_plan_store_corruption();
    assert_runtime_corruption(&err, ErrorOrigin::Store);
}

#[test]
fn index_plan_serialize_corruption_uses_serialize_origin() {
    let err = InternalError::index_plan_serialize_corruption();
    assert_runtime_corruption(&err, ErrorOrigin::Serialize);
}

#[test]
fn serialize_incompatible_persisted_format_uses_serialize_origin() {
    let err = InternalError::serialize_incompatible_persisted_format();
    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Serialize);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeIncompatiblePersistedFormat,
    );
}

#[test]
fn recovery_format_version_facts_preserve_required_and_optional_found_versions() {
    let err = InternalError::recovery_unsupported_database_format(Some(7), 9);
    assert_eq!(
        err.diagnostic_facts(),
        vec![
            (icydb_diagnostic_code::DiagnosticFactTag::ExpectedVersion, 9,),
            (icydb_diagnostic_code::DiagnosticFactTag::ActualVersion, 7,),
        ],
    );

    let missing = InternalError::recovery_unsupported_database_format(None, 9);
    assert_eq!(
        missing.diagnostic_facts(),
        vec![(icydb_diagnostic_code::DiagnosticFactTag::ExpectedVersion, 9,)],
    );
}

#[test]
fn recovery_format_marker_facts_preserve_only_the_bounded_reason() {
    let cases = [
        (
            RecoveryFormatMarkerError::Magic,
            icydb_diagnostic_code::DiagnosticDecodeReason::RecoveryMarkerMagic,
        ),
        (
            RecoveryFormatMarkerError::Checksum,
            icydb_diagnostic_code::DiagnosticDecodeReason::RecoveryMarkerChecksum,
        ),
        (
            RecoveryFormatMarkerError::State,
            icydb_diagnostic_code::DiagnosticDecodeReason::RecoveryMarkerState,
        ),
    ];

    for (marker_error, expected) in cases {
        let err = InternalError::recovery_malformed_database_format_marker(marker_error);
        assert_eq!(
            err.diagnostic_facts(),
            vec![(
                icydb_diagnostic_code::DiagnosticFactTag::DecodeReason,
                expected.raw(),
            )],
        );
    }
}

#[test]
fn persisted_row_layout_facts_preserve_only_the_accepted_window() {
    let outside = InternalError::persisted_row_layout_outside_accepted_window(3, 4, 7);
    assert_eq!(
        outside.diagnostic_facts(),
        vec![
            (icydb_diagnostic_code::DiagnosticFactTag::RowLayout, 3),
            (icydb_diagnostic_code::DiagnosticFactTag::HistoryFloor, 4),
            (icydb_diagnostic_code::DiagnosticFactTag::CurrentLayout, 7),
        ],
    );

    let slots = InternalError::persisted_row_slot_count_mismatch(6, 9, 8);
    assert_eq!(
        slots.diagnostic_facts(),
        vec![
            (icydb_diagnostic_code::DiagnosticFactTag::RowLayout, 6),
            (
                icydb_diagnostic_code::DiagnosticFactTag::ExpectedSlotCount,
                9,
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::ActualSlotCount, 8,),
        ],
    );
}

#[test]
fn storage_index_and_relation_facts_keep_only_safe_numeric_context() {
    let memory = InternalError::commit_memory_id_mismatch(12, 30);
    assert_eq!(
        memory.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ExpectedMemoryId,
                12,
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::ActualMemoryId, 30,),
        ],
    );

    let component = InternalError::commit_component_length_invalid(513, 512);
    assert_eq!(
        component.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ComponentKind,
                icydb_diagnostic_code::DiagnosticComponentKind::CommitDataKey.raw(),
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::ActualLength, 513,),
            (icydb_diagnostic_code::DiagnosticFactTag::Limit, 512),
        ],
    );

    let index = InternalError::index_component_exceeds_max_size_at(23, 5, 2, 257, 256);
    assert_eq!(
        index.diagnostic_facts(),
        vec![
            (icydb_diagnostic_code::DiagnosticFactTag::EntityTag, 23),
            (
                icydb_diagnostic_code::DiagnosticFactTag::PhysicalGeneration,
                5,
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::ComponentIndex, 2),
            (
                icydb_diagnostic_code::DiagnosticFactTag::ComponentKind,
                icydb_diagnostic_code::DiagnosticComponentKind::IndexKeyComponent.raw(),
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::ActualLength, 257,),
            (icydb_diagnostic_code::DiagnosticFactTag::Limit, 256),
        ],
    );

    let relation = InternalError::relation_target_primary_key_arity_mismatch(2, 1);
    assert_eq!(
        relation.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ComponentKind,
                icydb_diagnostic_code::DiagnosticComponentKind::RelationTargetPrimaryKey.raw(),
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::ExpectedArity, 2),
            (icydb_diagnostic_code::DiagnosticFactTag::ActualArity, 1),
        ],
    );

    let entity = InternalError::relation_target_entity_mismatch(
        "decode", "Source", "relation", "Target", "Target", 31, 37,
    );
    assert_eq!(
        entity.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ExpectedEntityTag,
                31,
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::ActualEntityTag,
                37,
            ),
        ],
    );
}

#[test]
fn accepted_constraint_facts_bind_exact_authority_operation_and_path() {
    let fingerprint = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
        0x18,
    ];
    let context = AcceptedConstraintFactContext::write_admission(
        3,
        fingerprint,
        29,
        41,
        icydb_diagnostic_code::DiagnosticConstraintKind::TargetedRule,
        Some(MutationDiagnosticContext::new(
            29,
            icydb_diagnostic_code::DiagnosticMutationOperation::Update,
            7,
        )),
        Some(ConstraintValuePath::new(vec![
            ConstraintValuePathComponent::RootField { field_id: 5 },
            ConstraintValuePathComponent::RecordMember {
                composite_type_id: 9,
                member_id: 11,
            },
            ConstraintValuePathComponent::ListElement { index: 13 },
        ])),
    );
    let error = InternalError::mutation_constraint_violation(context);

    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
    );
    assert_eq!(
        error.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION,
    );
    assert_eq!(
        error.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::AcceptedSchemaFingerprintMethod,
                3,
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::AcceptedSchemaFingerprintHigh,
                0x0102_0304_0506_0708,
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::AcceptedSchemaFingerprintLow,
                0x1112_1314_1516_1718,
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::EntityTag, 29),
            (icydb_diagnostic_code::DiagnosticFactTag::ConstraintId, 41),
            (
                icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
                icydb_diagnostic_code::DiagnosticConstraintKind::TargetedRule.raw(),
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::ConstraintContext,
                icydb_diagnostic_code::DiagnosticConstraintContext::WriteAdmission.raw(),
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::MutationOperation,
                icydb_diagnostic_code::DiagnosticMutationOperation::Update.raw(),
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::BatchPosition, 7),
            (icydb_diagnostic_code::DiagnosticFactTag::RootField, 5),
            (
                icydb_diagnostic_code::DiagnosticFactTag::RecordMember,
                icydb_diagnostic_code::pack_u32_pair(9, 11),
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::ListElement, 13),
        ],
    );
}

#[test]
fn accepted_constraint_activation_block_uses_e225_with_the_same_authority_schema() {
    let context = AcceptedConstraintFactContext::write_admission(
        1,
        [0xA5; 16],
        8,
        12,
        icydb_diagnostic_code::DiagnosticConstraintKind::Unique,
        None,
        None,
    );
    let error = InternalError::mutation_constraint_activation_write_blocked(context);

    assert_eq!(error.class(), ErrorClass::Conflict);
    assert_eq!(error.origin(), ErrorOrigin::Executor);
    assert_eq!(
        error.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_ACTIVATION_WRITE_BLOCKED,
    );
    assert_eq!(error.diagnostic_facts().len(), 7);
}

#[test]
fn index_plan_store_invariant_uses_store_origin() {
    let err = InternalError::index_plan_store_invariant();
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
    );
}

#[test]
fn query_executor_invariant_uses_invariant_violation_class() {
    let err = InternalError::query_executor_invariant();
    assert_runtime_invariant(&err, ErrorOrigin::Query);
}

#[test]
fn cursor_executor_invariant_uses_cursor_origin() {
    let err = InternalError::cursor_executor_invariant();
    assert_runtime_invariant(&err, ErrorOrigin::Cursor);
}

#[test]
fn query_unsupported_uses_query_origin() {
    let err = InternalError::query_unsupported();

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Query);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeUnsupported
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_unsupported_sql_feature_preserves_query_detail_label() {
    let err =
        InternalError::query_unsupported_sql_feature(icydb_diagnostic_code::SqlFeatureCode::Join);

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Query);
    assert!(
        matches!(
            err.detail(),
            Some(ErrorDetail::Query(QueryErrorDetail::UnsupportedSqlFeature { feature }))
                if feature == &icydb_diagnostic_code::SqlFeatureCode::Join
        ),
        "query unsupported SQL feature helper should preserve structured feature code detail",
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_unsupported_sql_feature_exposes_compact_diagnostic_detail() {
    let err =
        InternalError::query_unsupported_sql_feature(icydb_diagnostic_code::SqlFeatureCode::Join);
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryUnsupportedSqlFeature
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Query
    );
    assert_eq!(
        diagnostic.detail(),
        Some(
            &icydb_diagnostic_code::DiagnosticDetail::UnsupportedSqlFeature {
                feature: icydb_diagnostic_code::SqlFeatureCode::Join,
            }
        ),
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_sql_lowering_exposes_compact_diagnostic_detail() {
    let err = InternalError::query_sql_lowering(
        icydb_diagnostic_code::SqlLoweringCode::DistinctOrderByProjection,
    );
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryUnsupportedSqlFeature
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::SqlLowering {
            reason: icydb_diagnostic_code::SqlLoweringCode::DistinctOrderByProjection,
        }),
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_sql_surface_mismatch_exposes_compact_diagnostic_detail() {
    let err = InternalError::query_sql_surface_mismatch(
        icydb_diagnostic_code::SqlSurfaceMismatchCode::QueryRejectsInsert,
    );
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QuerySqlSurfaceMismatch
    );
    assert_eq!(
        diagnostic.detail(),
        Some(
            &icydb_diagnostic_code::DiagnosticDetail::SqlSurfaceMismatch {
                mismatch: icydb_diagnostic_code::SqlSurfaceMismatchCode::QueryRejectsInsert,
            }
        ),
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_sql_write_boundary_exposes_compact_diagnostic_detail() {
    let err = InternalError::query_sql_write_boundary(
        icydb_diagnostic_code::SqlWriteBoundaryCode::MissingPrimaryKey,
    );
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QuerySqlWriteBoundary
    );
    assert_eq!(
        diagnostic.detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::SqlWriteBoundary {
            boundary: icydb_diagnostic_code::SqlWriteBoundaryCode::MissingPrimaryKey,
        }),
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_sql_write_boundary_with_facts_preserves_the_exact_public_code() {
    let err = InternalError::query_sql_write_boundary_with_facts(
        icydb_diagnostic_code::SqlWriteBoundaryCode::UnknownReturningField,
        vec![(icydb_diagnostic_code::DiagnosticFactTag::ProjectionIndex, 2)],
    );
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QuerySqlWriteBoundary,
    );
    assert_eq!(
        diagnostic.error_code(),
        icydb_diagnostic_code::ErrorCode::SQL_WRITE_UNKNOWN_RETURNING_FIELD,
    );
    assert_eq!(
        err.diagnostic_facts(),
        vec![(icydb_diagnostic_code::DiagnosticFactTag::ProjectionIndex, 2,)],
    );
}

#[cfg(feature = "sql")]
#[test]
fn invalid_fact_projection_fails_compactly_without_partial_facts() {
    let err = InternalError::with_diagnostic_facts(
        ErrorClass::Unsupported,
        ErrorOrigin::Query,
        Some(icydb_diagnostic_code::DiagnosticDetail::SqlWriteBoundary {
            boundary: icydb_diagnostic_code::SqlWriteBoundaryCode::UnknownReturningField,
        }),
        vec![(icydb_diagnostic_code::DiagnosticFactTag::Limit, 4_096)],
    );

    assert_runtime_invariant(&err, ErrorOrigin::Query);
    assert!(err.diagnostic_facts().is_empty());
}

#[cfg(feature = "sql")]
#[test]
fn query_schema_ddl_admission_exposes_compact_diagnostic_detail() {
    let err =
        InternalError::query_schema_ddl_admission(SchemaDdlAdmissionError::PublicationRaceLost);
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::SchemaDdlAdmission
    );
    assert_eq!(
        diagnostic.detail(),
        Some(
            &icydb_diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                reason: icydb_diagnostic_code::SchemaDdlAdmissionCode::PublicationRaceLost,
            }
        ),
    );
}

#[test]
fn schema_ddl_publication_race_exposes_compact_admission_detail() {
    let err = InternalError::schema_ddl_publication_race_lost("User");
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::SchemaDdlAdmission
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Store
    );
    assert_eq!(
        diagnostic.detail(),
        Some(
            &icydb_diagnostic_code::DiagnosticDetail::SchemaDdlAdmission {
                reason: icydb_diagnostic_code::SchemaDdlAdmissionCode::PublicationRaceLost,
            }
        ),
    );
}

#[test]
fn internal_error_without_detail_uses_class_origin_compact_code() {
    let err = InternalError::classified(ErrorClass::InvariantViolation, ErrorOrigin::Planner);
    let diagnostic = err.diagnostic();

    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Planner
    );
    assert_eq!(diagnostic.detail(), None);
}

#[test]
fn executor_access_plan_error_mapping_stays_invariant_violation() {
    let err = AccessPlanError::IndexPrefixEmpty.into_internal_error();
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
}

#[test]
fn plan_policy_error_mapping_uses_runtime_invariant_code() {
    let err = plan_invariant_violation(PolicyPlanError::DeleteWindowRequiresOrder);
    assert_runtime_invariant(&err, ErrorOrigin::Planner);
}

#[test]
fn group_plan_error_mapping_uses_runtime_invariant_code() {
    let err = from_group_plan_error(PlanError::from(GroupPlanError::UnknownGroupField {
        group_index: None,
        field: "tenant".to_string(),
    }));

    assert_runtime_invariant(&err, ErrorOrigin::Planner);
}

#[test]
fn group_plan_error_mapping_rejects_non_group_user_variant() {
    let err = from_group_plan_error(PlanError::from(PlanUserError::Order(Box::new(
        OrderPlanError::UnknownField {
            term_index: 0,
            field: "tenant".to_string(),
        },
    ))));

    assert_runtime_invariant(&err, ErrorOrigin::Planner);
}

#[test]
fn group_plan_error_mapping_rejects_non_group_policy_variant() {
    let err = from_group_plan_error(PlanError::from(PlanPolicyError::Policy(Box::new(
        PolicyPlanError::UnorderedPagination,
    ))));

    assert_runtime_invariant(&err, ErrorOrigin::Planner);
}

#[test]
fn group_plan_error_mapping_rejects_cursor_variant() {
    let err = from_group_plan_error(PlanError::from(cursor_window_error()));

    assert_runtime_invariant(&err, ErrorOrigin::Planner);
}

#[test]
fn cursor_plan_error_mapping_classifies_invalid_payload_as_e6() {
    let err = cursor_payload_error().into_internal_error();

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Cursor);
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor,
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Cursor,
    );
    assert_eq!(diagnostic.detail(), None);
    assert_eq!(
        diagnostic.error_code(),
        icydb_diagnostic_code::ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR,
    );
}

#[test]
fn cursor_plan_error_mapping_classifies_signature_mismatch_as_unsupported() {
    let err = cursor_signature_mismatch_error().into_internal_error();

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Cursor);
    assert_eq!(
        err.diagnostic().code(),
        icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor,
    );
}

#[test]
fn query_error_preserves_cursor_window_facts_without_internal_error_conversion() {
    let query_error = crate::db::QueryError::Plan(Box::new(PlanError::from(cursor_window_error())));

    assert_eq!(
        query_error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor,
    );
    assert_eq!(
        query_error.diagnostic_facts(),
        vec![
            (icydb_diagnostic_code::DiagnosticFactTag::ExpectedOffset, 4,),
            (icydb_diagnostic_code::DiagnosticFactTag::ActualOffset, 2,),
        ],
    );
}

#[test]
fn cursor_plan_error_mapping_keeps_invariant_violation_class() {
    let err = CursorPlanError::ContinuationCursorInvariantViolation.into_internal_error();

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Cursor);
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Cursor,
    );
    assert_eq!(diagnostic.detail(), None);
}

#[test]
fn classification_integrity_helpers_preserve_error_class() {
    let classes = [
        ErrorClass::Corruption,
        ErrorClass::IncompatiblePersistedFormat,
        ErrorClass::NotFound,
        ErrorClass::Internal,
        ErrorClass::Conflict,
        ErrorClass::Unsupported,
        ErrorClass::InvariantViolation,
    ];

    for class in classes {
        let base = InternalError::classified(class, ErrorOrigin::Query);
        let reorigined = base.with_origin(ErrorOrigin::Store);
        assert_eq!(
            reorigined.class, class,
            "class must be preserved across helper relabeling operations",
        );
    }
}

#[test]
fn recovery_reorigining_preserves_safe_numeric_facts() {
    let base = InternalError::commit_component_length_invalid(513, 512);
    let reorigined = base.with_origin(ErrorOrigin::Recovery);

    assert_eq!(reorigined.class, ErrorClass::Corruption);
    assert_eq!(reorigined.origin, ErrorOrigin::Recovery);
    assert_eq!(
        reorigined.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ComponentKind,
                icydb_diagnostic_code::DiagnosticComponentKind::CommitDataKey.raw(),
            ),
            (icydb_diagnostic_code::DiagnosticFactTag::ActualLength, 513,),
            (icydb_diagnostic_code::DiagnosticFactTag::Limit, 512),
        ],
    );
}

#[test]
fn classification_integrity_cursor_conversion_matrix_is_restricted() {
    fn expected_class_from_cursor_variant(err: &CursorPlanError) -> ErrorClass {
        match err {
            CursorPlanError::InvalidContinuationCursor { .. }
            | CursorPlanError::InvalidContinuationCursorPayload { .. }
            | CursorPlanError::ContinuationCursorSignatureMismatch { .. }
            | CursorPlanError::ContinuationCursorWindowMismatch { .. } => ErrorClass::Unsupported,
            CursorPlanError::ContinuationCursorInvariantViolation => ErrorClass::InvariantViolation,
        }
    }

    let cases = vec![
        cursor_payload_error(),
        CursorPlanError::ContinuationCursorInvariantViolation,
        cursor_signature_mismatch_error(),
        cursor_window_error(),
    ];

    for cursor_err in cases {
        let expected_class = expected_class_from_cursor_variant(&cursor_err);
        let expected_code = match expected_class {
            ErrorClass::Unsupported => {
                icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor
            }
            ErrorClass::InvariantViolation => {
                icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation
            }
            _ => unreachable!("cursor conversion matrix only permits two error classes"),
        };
        let err = cursor_err.into_internal_error();
        assert_eq!(err.origin, ErrorOrigin::Cursor);
        assert_eq!(
            err.class, expected_class,
            "cursor conversion class must remain stable for each cursor variant: {err:?}",
        );
        assert_eq!(
            err.diagnostic().code(),
            expected_code,
            "cursor conversion diagnostic must remain stable for each cursor variant: {err:?}",
        );
    }
}

#[test]
fn classification_integrity_access_plan_conversion_stays_invariant() {
    let err = AccessPlanError::InvalidKeyRange.into_internal_error();

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Query);
}

#[test]
fn classification_integrity_corruption_constructors_never_downgrade() {
    let corruption_cases = [
        InternalError::store_corruption(),
        InternalError::index_corruption(),
        InternalError::serialize_corruption(),
        InternalError::identity_corruption(),
        InternalError::index_plan_index_corruption(),
        InternalError::index_plan_store_corruption(),
        InternalError::index_plan_serialize_corruption(),
    ];

    for err in corruption_cases {
        assert_eq!(
            err.class,
            ErrorClass::Corruption,
            "corruption constructors must remain corruption-classed",
        );
        assert!(
            !matches!(err.class, ErrorClass::Unsupported),
            "corruption constructors must never downgrade to unsupported",
        );
    }
}

#[test]
fn mutation_unknown_field_uses_compact_executor_invariant() {
    let err = InternalError::mutation_structural_field_unknown("tests::User", "missing_name");

    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
    );
}

#[test]
fn mutation_error_details_project_exact_bounded_numeric_facts() {
    use icydb_diagnostic_code::{
        DiagnosticFactTag as Tag, DiagnosticMutationOperation as Operation,
    };

    let context = MutationDiagnosticContext::new(17, Operation::Insert, 3);
    let required = InternalError::mutation_required_field_missing(context, 9);
    assert_eq!(
        required.diagnostic_facts(),
        vec![
            (Tag::EntityTag, 17),
            (Tag::FieldId, 9),
            (Tag::MutationOperation, Operation::Insert.raw()),
            (Tag::BatchPosition, 3),
        ],
    );

    let explicit = InternalError::mutation_database_owned_field_explicit(context, 11);
    assert_eq!(
        explicit.diagnostic_facts(),
        vec![
            (Tag::EntityTag, 17),
            (Tag::FieldId, 11),
            (Tag::MutationOperation, Operation::Insert.raw()),
            (Tag::BatchPosition, 3),
        ],
    );

    let managed = InternalError::mutation_managed_timestamp_regression(
        MutationDiagnosticContext::new(17, Operation::Update, 5),
    );
    assert_eq!(
        managed.diagnostic_facts(),
        vec![
            (Tag::EntityTag, 17),
            (Tag::MutationOperation, Operation::Update.raw()),
            (Tag::BatchPosition, 5),
        ],
    );

    assert_eq!(
        InternalError::mutation_batch_empty().diagnostic_facts(),
        vec![(Tag::ActualCount, 0)],
    );
    assert_eq!(
        InternalError::mutation_batch_too_many_items(5_000, 4_096).diagnostic_facts(),
        vec![(Tag::ActualCount, 5_000), (Tag::Limit, 4_096)],
    );
    assert_eq!(
        InternalError::mutation_batch_staged_bytes_exceeded(Some(101), 100).diagnostic_facts(),
        vec![(Tag::ActualLength, 101), (Tag::Limit, 100)],
    );
    assert_eq!(
        InternalError::mutation_batch_staged_bytes_exceeded(None, 100).diagnostic_facts(),
        vec![(Tag::Limit, 100)],
    );
    assert_eq!(
        InternalError::mutation_batch_result_bytes_exceeded(101, 100).diagnostic_facts(),
        vec![(Tag::ActualLength, 101), (Tag::Limit, 100)],
    );
    assert_eq!(
        InternalError::mutation_batch_commit_work_exceeded(Some(16_385), 16_384).diagnostic_facts(),
        vec![(Tag::ActualCount, 16_385), (Tag::Limit, 16_384)],
    );
    assert_eq!(
        InternalError::mutation_batch_commit_work_exceeded(None, 16_384).diagnostic_facts(),
        vec![(Tag::Limit, 16_384)],
    );
    assert_eq!(
        InternalError::mutation_batch_store_mismatch(2, 17, 18).diagnostic_facts(),
        vec![
            (Tag::BatchPosition, 2),
            (Tag::ExpectedEntityTag, 17),
            (Tag::ActualEntityTag, 18),
        ],
    );
    assert_eq!(
        InternalError::mutation_batch_too_many_entities(65, 64).diagnostic_facts(),
        vec![(Tag::ActualCount, 65), (Tag::Limit, 64)],
    );
    assert_eq!(
        InternalError::mutation_atomic_save_duplicate_key(17, 1, 4).diagnostic_facts(),
        vec![
            (Tag::EntityTag, 17),
            (Tag::FirstBatchPosition, 1),
            (Tag::DuplicateBatchPosition, 4),
        ],
    );
}

#[test]
fn convergence_backlog_pressure_projects_exact_bounded_numeric_facts() {
    use icydb_diagnostic_code::DiagnosticFactTag as Tag;

    let pressure = InternalError::convergence_backlog_pressure(
        icydb_diagnostic_code::DiagnosticBacklogResource::Batches,
        64,
        1,
        64,
    );
    assert_eq!(
        pressure.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeConflict,
    );
    assert_eq!(
        pressure.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONVERGENCE_BACKLOG_PRESSURE,
    );
    assert_eq!(
        pressure.diagnostic_facts(),
        vec![
            (Tag::BacklogResource, 1),
            (Tag::CurrentCount, 64),
            (Tag::ProposedCount, 1),
            (Tag::Limit, 64),
        ],
    );
}

#[test]
fn stale_accepted_authority_projects_expected_and_optional_current_revision() {
    use icydb_diagnostic_code::DiagnosticFactTag as Tag;

    assert_eq!(
        InternalError::query_stale_accepted_schema_revision(7, Some(9)).diagnostic_facts(),
        vec![(Tag::ExpectedRevision, 7), (Tag::CurrentRevision, 9)],
    );
    assert_eq!(
        InternalError::query_stale_accepted_schema_revision(7, None).diagnostic_facts(),
        vec![(Tag::ExpectedRevision, 7)],
    );
}
