use super::{
    DiagnosticAggregateKind, DiagnosticBacklogResource, DiagnosticComponentKind,
    DiagnosticConstraintContext, DiagnosticConstraintKind, DiagnosticDecodeReason,
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
    DiagnosticFactSchemaMismatch, DiagnosticFactTag, DiagnosticFunctionKind,
    DiagnosticGroupBudgetResource, DiagnosticMutationOperation, DiagnosticOperatorKind,
    DiagnosticTypeFamily, ORDERED_FACT_TAGS, pack_u32_pair, unpack_u32_pair,
    validate_known_diagnostic_fact_schema, validate_raw_diagnostic_fact_schema,
};
use crate::ErrorCode;

#[test]
fn schema_page_budget_facts_distinguish_counts_and_bytes() {
    let code = ErrorCode::SCHEMA_DDL_TRANSITION_BUDGET_EXCEEDED;
    assert_eq!(validate_known_diagnostic_fact_schema(code, &[]), Ok(()));
    for (tag, limit) in [
        (DiagnosticFactTag::ActualCount, 256),
        (DiagnosticFactTag::ActualLength, 4 * 1024 * 1024),
    ] {
        let facts = [(DiagnosticFactTag::Limit, limit), (tag, limit + 1)];
        assert_eq!(validate_known_diagnostic_fact_schema(code, &facts), Ok(()));
        assert!(validate_known_diagnostic_fact_schema(code, &facts[..1]).is_err());
        assert!(validate_known_diagnostic_fact_schema(code, &[facts[1], facts[0]]).is_err());
    }
}

#[test]
fn serialized_size_limit_facts_require_both_ordered_byte_counts() {
    let code = ErrorCode::RUNTIME_UNSUPPORTED;
    let actual = (DiagnosticFactTag::ActualLength, 4_194_305);
    let limit = (DiagnosticFactTag::Limit, 4_194_304);
    assert_eq!(
        validate_known_diagnostic_fact_schema(code, &[actual, limit]),
        Ok(())
    );
    for facts in [
        vec![actual],
        vec![limit],
        vec![limit, actual],
        vec![actual, limit, limit],
    ] {
        assert!(validate_known_diagnostic_fact_schema(code, &facts).is_err());
    }
}

#[test]
fn memory_admission_fact_schemas_keep_counts_and_ids_bounded() {
    for code in [
        ErrorCode::RUNTIME_BOUNDARY_MEMORY_ALLOCATION_RESOLUTION_FAILED,
        ErrorCode::RUNTIME_BOUNDARY_MEMORY_HISTORICAL_JOURNAL_UNAVAILABLE,
    ] {
        assert_eq!(validate_known_diagnostic_fact_schema(code, &[]), Ok(()));
        for id in [0, 42, 254] {
            assert_eq!(
                validate_known_diagnostic_fact_schema(
                    code,
                    &[(DiagnosticFactTag::ActualMemoryId, id)]
                ),
                Ok(())
            );
        }
        for facts in [
            vec![(DiagnosticFactTag::ActualMemoryId, 255)],
            vec![(DiagnosticFactTag::ExpectedMemoryId, 42)],
        ] {
            assert!(validate_known_diagnostic_fact_schema(code, &facts).is_err());
        }
    }
    let roles = ErrorCode::RUNTIME_BOUNDARY_MEMORY_ALLOCATION_ROLES_INCOMPLETE;
    for count in [3, 4] {
        assert_eq!(
            validate_known_diagnostic_fact_schema(
                roles,
                &[(DiagnosticFactTag::ExpectedCount, count)]
            ),
            Ok(())
        );
    }
    for facts in [
        vec![],
        vec![(DiagnosticFactTag::ExpectedCount, 0)],
        vec![(DiagnosticFactTag::ExpectedCount, 5)],
        vec![(DiagnosticFactTag::ActualCount, 3)],
    ] {
        assert!(validate_known_diagnostic_fact_schema(roles, &facts).is_err());
    }
    let mismatch = ErrorCode::RUNTIME_BOUNDARY_MEMORY_DECLARATION_SNAPSHOT_MISMATCH;
    assert_eq!(validate_known_diagnostic_fact_schema(mismatch, &[]), Ok(()));
    assert_eq!(
        validate_known_diagnostic_fact_schema(
            mismatch,
            &[
                (DiagnosticFactTag::ExpectedMemoryId, 0),
                (DiagnosticFactTag::ActualMemoryId, 254)
            ]
        ),
        Ok(())
    );
    for facts in [
        vec![(DiagnosticFactTag::ExpectedMemoryId, 42)],
        vec![
            (DiagnosticFactTag::ExpectedMemoryId, 42),
            (DiagnosticFactTag::ActualMemoryId, 42),
        ],
        vec![
            (DiagnosticFactTag::ActualMemoryId, 42),
            (DiagnosticFactTag::ExpectedMemoryId, 43),
        ],
        vec![
            (DiagnosticFactTag::ExpectedMemoryId, 255),
            (DiagnosticFactTag::ActualMemoryId, 42),
        ],
    ] {
        assert!(validate_known_diagnostic_fact_schema(mismatch, &facts).is_err());
    }
    for code in [
        ErrorCode::RUNTIME_BOUNDARY_MEMORY_NAMESPACE_REMOVED,
        ErrorCode::RUNTIME_BOUNDARY_MEMORY_DECLARATION_INVALID,
    ] {
        assert_eq!(validate_known_diagnostic_fact_schema(code, &[]), Ok(()));
        assert!(
            validate_known_diagnostic_fact_schema(code, &[(DiagnosticFactTag::ActualMemoryId, 42)])
                .is_err()
        );
    }
}

#[test]
fn bucket_mismatch_requires_two_distinct_nonzero_page_counts() {
    let code = ErrorCode::RUNTIME_BOUNDARY_MEMORY_BUCKET_SIZE_MISMATCH;
    let expected = DiagnosticFactTag::Expected;
    let actual = DiagnosticFactTag::Actual;
    assert_eq!(
        validate_known_diagnostic_fact_schema(code, &[(expected, 16), (actual, 128)]),
        Ok(()),
    );
    for facts in [
        vec![],
        vec![(expected, 16)],
        vec![(actual, 128), (expected, 16)],
        vec![(expected, 16), (expected, 128)],
        vec![(expected, 16), (actual, 16)],
        vec![(expected, 0), (actual, 128)],
        vec![(expected, 16), (actual, 0)],
        vec![(expected, 65_536), (actual, 128)],
        vec![(expected, 16), (actual, 65_536)],
    ] {
        assert_eq!(
            validate_known_diagnostic_fact_schema(code, &facts),
            Err(DiagnosticFactSchemaMismatch::InvalidSequence),
        );
    }
    assert_eq!(
        validate_known_diagnostic_fact_schema(
            code,
            &[(expected, 16), (actual, 128), (actual, 128)],
        ),
        Err(DiagnosticFactSchemaMismatch::CodeMaximumExceeded),
    );
}

#[test]
fn fact_tag_registry_is_fixed_unique_and_contiguous() {
    for (index, tag) in ORDERED_FACT_TAGS.iter().copied().enumerate() {
        let expected = u8::try_from(index + 1).expect("fact-tag index fits u8");
        assert_eq!(tag.raw(), expected);
        assert_eq!(DiagnosticFactTag::known(expected), Some(tag));
    }

    assert_eq!(DiagnosticFactTag::known(0), None);
    assert_eq!(DiagnosticFactTag::known(97), None);
    assert_eq!(DiagnosticFactTag::known(u8::MAX), None);
}

#[test]
fn execution_budget_fact_value_registries_are_fixed() {
    assert_eq!(DiagnosticBacklogResource::Batches.raw(), 1);
    assert_eq!(DiagnosticBacklogResource::Records.raw(), 2);
    assert_eq!(DiagnosticBacklogResource::EncodedBytes.raw(), 3);
    assert_eq!(DiagnosticBacklogResource::known(4), None);

    for (index, resource) in DiagnosticExecutionBudgetResource::ALL
        .iter()
        .copied()
        .enumerate()
    {
        let expected = u64::try_from(index + 1).expect("resource index fits u64");
        assert_eq!(resource.raw(), expected);
        assert_eq!(
            DiagnosticExecutionBudgetResource::known(expected),
            Some(resource)
        );
    }
    assert_eq!(DiagnosticExecutionBudgetResource::known(0), None);
    assert_eq!(DiagnosticExecutionBudgetResource::known(21), None);

    assert_eq!(DiagnosticExecutionBudgetScope::Execution.raw(), 1);
    assert_eq!(DiagnosticExecutionBudgetScope::Request.raw(), 2);
    assert_eq!(DiagnosticExecutionBudgetScope::known(3), None);

    assert_eq!(DiagnosticExecutionLane::PublicRead.raw(), 1);
    assert_eq!(DiagnosticExecutionLane::TrustedRead.raw(), 2);
    assert_eq!(DiagnosticExecutionLane::Diagnostic.raw(), 3);
    assert_eq!(DiagnosticExecutionLane::Mutation.raw(), 4);
    assert_eq!(DiagnosticExecutionLane::Recovery.raw(), 5);
    assert_eq!(DiagnosticExecutionLane::known(6), None);
}

#[test]
fn accepted_identity_pair_packing_is_exact() {
    for pair in [
        (0, 0),
        (1, 2),
        (u32::MAX, 0),
        (0, u32::MAX),
        (u32::MAX, u32::MAX),
    ] {
        assert_eq!(unpack_u32_pair(pack_u32_pair(pair.0, pair.1)), pair);
    }
}

#[test]
fn constraint_fact_value_registries_are_fixed() {
    assert_eq!(DiagnosticConstraintKind::Check.raw(), 1);
    assert_eq!(DiagnosticConstraintKind::NotNull.raw(), 2);
    assert_eq!(DiagnosticConstraintKind::Relation.raw(), 3);
    assert_eq!(DiagnosticConstraintKind::TargetedRule.raw(), 4);
    assert_eq!(DiagnosticConstraintKind::Unique.raw(), 5);
    assert_eq!(DiagnosticConstraintKind::known(0), None);
    assert_eq!(DiagnosticConstraintKind::known(6), None);

    assert_eq!(DiagnosticConstraintContext::Integrity.raw(), 1);
    assert_eq!(DiagnosticConstraintContext::MigrationValidation.raw(), 2);
    assert_eq!(DiagnosticConstraintContext::WriteAdmission.raw(), 3);
    assert_eq!(DiagnosticConstraintContext::known(0), None);
    assert_eq!(DiagnosticConstraintContext::known(4), None);
}

#[test]
fn component_kind_registry_is_fixed_and_numeric() {
    let kinds = [
        DiagnosticComponentKind::CommitDataKey,
        DiagnosticComponentKind::IndexKey,
        DiagnosticComponentKind::IndexKeyComponent,
        DiagnosticComponentKind::RelationTargetPrimaryKey,
    ];

    for (index, kind) in kinds.iter().copied().enumerate() {
        let expected = (index + 1) as u64;
        assert_eq!(kind.raw(), expected);
        assert_eq!(DiagnosticComponentKind::known(expected), Some(kind));
        assert_eq!(format!("{kind:?}"), expected.to_string());
    }
    assert_eq!(DiagnosticComponentKind::known(0), None);
    assert_eq!(DiagnosticComponentKind::known(5), None);
}

#[test]
fn decode_reason_registry_is_fixed_and_numeric() {
    let reasons = [
        DiagnosticDecodeReason::CursorEmpty,
        DiagnosticDecodeReason::CursorTooLong,
        DiagnosticDecodeReason::CursorInvalidLength,
        DiagnosticDecodeReason::CursorInvalidBase64,
        DiagnosticDecodeReason::CursorGroupedDirectionMismatch,
        DiagnosticDecodeReason::CursorTokenEncode,
        DiagnosticDecodeReason::CursorTokenDecode,
        DiagnosticDecodeReason::RecoveryMarkerMagic,
        DiagnosticDecodeReason::RecoveryMarkerChecksum,
        DiagnosticDecodeReason::RecoveryMarkerState,
        DiagnosticDecodeReason::CursorGroupedContinuationRequiresLimit,
        DiagnosticDecodeReason::CursorGlobalDistinctContinuationUnsupported,
    ];

    for (index, reason) in reasons.iter().copied().enumerate() {
        let expected = (index + 1) as u64;
        assert_eq!(reason.raw(), expected);
        assert_eq!(DiagnosticDecodeReason::known(expected), Some(reason));
        assert_eq!(format!("{reason:?}"), expected.to_string());
    }

    assert_eq!(DiagnosticDecodeReason::known(0), None);
    assert_eq!(DiagnosticDecodeReason::known(13), None);
}

#[test]
fn mutation_operation_registry_is_fixed_and_numeric() {
    let operations = [
        DiagnosticMutationOperation::Insert,
        DiagnosticMutationOperation::Replace,
        DiagnosticMutationOperation::Update,
        DiagnosticMutationOperation::Delete,
    ];

    for (index, operation) in operations.iter().copied().enumerate() {
        let expected = (index + 1) as u64;
        assert_eq!(operation.raw(), expected);
        assert_eq!(
            DiagnosticMutationOperation::known(expected),
            Some(operation)
        );
        assert_eq!(format!("{operation:?}"), expected.to_string());
    }

    assert_eq!(DiagnosticMutationOperation::known(0), None);
    assert_eq!(DiagnosticMutationOperation::known(5), None);
}

#[test]
fn query_kind_registries_are_fixed_contiguous_and_numeric() {
    for raw in 1..=9 {
        let value = DiagnosticTypeFamily::known(raw).expect("type family should be known");
        assert_eq!(value.raw(), raw);
        assert_eq!(format!("{value:?}"), raw.to_string());
    }
    assert_eq!(DiagnosticTypeFamily::known(0), None);
    assert_eq!(DiagnosticTypeFamily::known(10), None);

    for raw in 1..=39 {
        let value = DiagnosticFunctionKind::known(raw).expect("function kind should be known");
        assert_eq!(value.raw(), raw);
        assert_eq!(format!("{value:?}"), raw.to_string());
    }
    assert_eq!(DiagnosticFunctionKind::known(0), None);
    assert_eq!(DiagnosticFunctionKind::known(40), None);

    for raw in 1..=18 {
        let value = DiagnosticOperatorKind::known(raw).expect("operator kind should be known");
        assert_eq!(value.raw(), raw);
        assert_eq!(format!("{value:?}"), raw.to_string());
    }
    assert_eq!(DiagnosticOperatorKind::known(0), None);
    assert_eq!(DiagnosticOperatorKind::known(19), None);

    for raw in 1..=8 {
        let value = DiagnosticAggregateKind::known(raw).expect("aggregate kind should be known");
        assert_eq!(value.raw(), raw);
        assert_eq!(format!("{value:?}"), raw.to_string());
    }
    assert_eq!(DiagnosticAggregateKind::known(0), None);
    assert_eq!(DiagnosticAggregateKind::known(9), None);
}

#[test]
fn per_code_schema_rejects_missing_disallowed_and_noncanonical_tags() {
    let valid = [
        (DiagnosticFactTag::ActualCount, 5),
        (DiagnosticFactTag::Limit, 4),
    ];
    assert_eq!(
        validate_known_diagnostic_fact_schema(
            ErrorCode::RUNTIME_BOUNDARY_MUTATION_BATCH_TOO_MANY_ITEMS,
            &valid,
        ),
        Ok(())
    );
    assert_eq!(
        validate_known_diagnostic_fact_schema(
            ErrorCode::RUNTIME_BOUNDARY_MUTATION_BATCH_TOO_MANY_ITEMS,
            &valid[..1],
        ),
        Err(DiagnosticFactSchemaMismatch::InvalidSequence)
    );
    assert_eq!(
        validate_known_diagnostic_fact_schema(
            ErrorCode::RUNTIME_BOUNDARY_MUTATION_BATCH_TOO_MANY_ITEMS,
            &[valid[1], valid[0]],
        ),
        Err(DiagnosticFactSchemaMismatch::InvalidSequence)
    );
    assert_eq!(
        validate_known_diagnostic_fact_schema(ErrorCode::QUERY_VALIDATE, &valid),
        Err(DiagnosticFactSchemaMismatch::CodeMaximumExceeded)
    );

    assert_eq!(
        validate_known_diagnostic_fact_schema(
            ErrorCode::RUNTIME_BOUNDARY_MUTATION_BATCH_COMMIT_WORK_EXCEEDED,
            &valid,
        ),
        Ok(())
    );
    assert_eq!(
        validate_known_diagnostic_fact_schema(
            ErrorCode::RUNTIME_BOUNDARY_MUTATION_BATCH_COMMIT_WORK_EXCEEDED,
            &valid[1..],
        ),
        Ok(())
    );
}

#[test]
fn grouped_limit_facts_require_matching_resources_and_valid_values() {
    let code = ErrorCode::RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED;
    for resource in [
        DiagnosticGroupBudgetResource::Groups,
        DiagnosticGroupBudgetResource::DistinctValuesPerGroup,
        DiagnosticGroupBudgetResource::DistinctValuesTotal,
        DiagnosticGroupBudgetResource::EstimatedBytes,
    ] {
        assert_eq!(
            DiagnosticGroupBudgetResource::known(resource.raw()),
            Some(resource)
        );
        let mut facts = vec![
            (
                DiagnosticFactTag::BudgetResource,
                resource.execution_resource().raw(),
            ),
            (DiagnosticFactTag::Limit, 10),
            (DiagnosticFactTag::Actual, 11),
            (DiagnosticFactTag::ExecutionBudgetScope, 1),
            (DiagnosticFactTag::ExecutionLane, 2),
            (DiagnosticFactTag::QueryShapeFingerprintPrefix, 17),
            (DiagnosticFactTag::GroupedLimit, resource.raw()),
        ];
        assert_eq!(validate_known_diagnostic_fact_schema(code, &facts), Ok(()));
        assert_eq!(
            validate_known_diagnostic_fact_schema(code, &facts[..6]),
            Ok(())
        );
        facts[0].1 = DiagnosticExecutionBudgetResource::StoredBytesRead.raw();
        assert_eq!(
            validate_known_diagnostic_fact_schema(code, &facts),
            Err(DiagnosticFactSchemaMismatch::InvalidSequence)
        );
        facts[0].1 = resource.execution_resource().raw();
        facts[6].1 = 5;
        assert_eq!(
            validate_known_diagnostic_fact_schema(code, &facts),
            Err(DiagnosticFactSchemaMismatch::InvalidSequence)
        );
        facts[6].1 = resource.raw();
        facts.swap(5, 6);
        assert_eq!(
            validate_known_diagnostic_fact_schema(code, &facts),
            Err(DiagnosticFactSchemaMismatch::InvalidSequence)
        );
    }
    assert_eq!(DiagnosticGroupBudgetResource::known(0), None);
}

#[test]
fn execution_budget_schema_requires_complete_typed_attribution() {
    let valid = [
        (
            DiagnosticFactTag::BudgetResource,
            DiagnosticExecutionBudgetResource::StoredBytesRead.raw(),
        ),
        (DiagnosticFactTag::Limit, 4_096),
        (DiagnosticFactTag::Actual, 4_097),
        (
            DiagnosticFactTag::ExecutionBudgetScope,
            DiagnosticExecutionBudgetScope::Request.raw(),
        ),
        (
            DiagnosticFactTag::ExecutionLane,
            DiagnosticExecutionLane::TrustedRead.raw(),
        ),
        (DiagnosticFactTag::QueryShapeFingerprintPrefix, 17),
    ];
    assert_eq!(
        validate_known_diagnostic_fact_schema(
            ErrorCode::RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED,
            &valid,
        ),
        Ok(())
    );
    assert_eq!(
        validate_known_diagnostic_fact_schema(
            ErrorCode::RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED,
            &valid[..5],
        ),
        Err(DiagnosticFactSchemaMismatch::InvalidSequence)
    );

    let mut invalid_resource = valid;
    invalid_resource[0].1 = 0;
    assert_eq!(
        validate_known_diagnostic_fact_schema(
            ErrorCode::RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED,
            &invalid_resource,
        ),
        Err(DiagnosticFactSchemaMismatch::InvalidValue)
    );
}

#[test]
fn raw_schema_keeps_unknown_context_numeric_but_marks_it_invalid() {
    assert_eq!(
        validate_raw_diagnostic_fact_schema(
            ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR,
            &[(u8::MAX, 17)],
        ),
        Err(DiagnosticFactSchemaMismatch::InvalidSequence)
    );
    assert_eq!(
        validate_raw_diagnostic_fact_schema(
            ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR,
            &[(DiagnosticFactTag::DecodeReason.raw(), u64::MAX)],
        ),
        Err(DiagnosticFactSchemaMismatch::InvalidSequence)
    );
}

#[test]
fn constraint_schema_enforces_authority_operation_and_bounded_path_suffix() {
    let mut targeted = vec![
        (DiagnosticFactTag::AcceptedSchemaFingerprintMethod, 1),
        (DiagnosticFactTag::AcceptedSchemaFingerprintHigh, 2),
        (DiagnosticFactTag::AcceptedSchemaFingerprintLow, 3),
        (DiagnosticFactTag::EntityTag, 17),
        (DiagnosticFactTag::ConstraintId, 4),
        (
            DiagnosticFactTag::ConstraintKind,
            DiagnosticConstraintKind::TargetedRule.raw(),
        ),
        (
            DiagnosticFactTag::ConstraintContext,
            DiagnosticConstraintContext::WriteAdmission.raw(),
        ),
        (
            DiagnosticFactTag::MutationOperation,
            DiagnosticMutationOperation::Insert.raw(),
        ),
        (DiagnosticFactTag::BatchPosition, 0),
    ];
    targeted.extend((0..64).map(|index| (DiagnosticFactTag::ListElement, index)));
    assert_eq!(targeted.len(), 73);
    assert_eq!(
        validate_known_diagnostic_fact_schema(
            ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION,
            targeted.as_slice(),
        ),
        Ok(())
    );

    let mut overlong = targeted.clone();
    overlong.push((DiagnosticFactTag::ListElement, 64));
    assert_eq!(
        validate_known_diagnostic_fact_schema(
            ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION,
            overlong.as_slice(),
        ),
        Err(DiagnosticFactSchemaMismatch::CodeMaximumExceeded)
    );

    let mut non_targeted_path = targeted;
    non_targeted_path[5].1 = DiagnosticConstraintKind::Unique.raw();
    assert_eq!(
        validate_known_diagnostic_fact_schema(
            ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION,
            non_targeted_path.as_slice(),
        ),
        Err(DiagnosticFactSchemaMismatch::InvalidSequence)
    );
}

#[test]
fn schema_enforces_global_ceiling_before_per_code_ceiling() {
    let facts = vec![(DiagnosticFactTag::ActualCount.raw(), 0); 81];
    assert_eq!(
        validate_raw_diagnostic_fact_schema(ErrorCode::QUERY_PLAN, facts.as_slice()),
        Err(DiagnosticFactSchemaMismatch::GlobalMaximumExceeded)
    );
}
