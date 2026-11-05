use super::*;

fn assert_byte_bound(
    error: InternalError,
    boundary: icydb_diagnostic_code::RuntimeBoundaryCode,
    actual: usize,
    limit: usize,
) {
    assert!(matches!(
        error.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: actual_boundary,
        }) if *actual_boundary == boundary,
    ));
    assert_eq!(
        error.diagnostic_facts(),
        vec![
            (
                icydb_diagnostic_code::DiagnosticFactTag::ActualLength,
                actual as u64,
            ),
            (
                icydb_diagnostic_code::DiagnosticFactTag::Limit,
                limit as u64,
            ),
        ],
    );
}

#[test]
fn exact_key_byte_admission_reports_each_bounded_resource() {
    let mut input = MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES;
    let input_error = checked_add_bytes(
        &mut input,
        1,
        MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES,
        InternalError::exact_key_batch_input_bytes_exceeded,
    )
    .expect_err("input key bytes above the cap should reject");
    assert_byte_bound(
        input_error,
        icydb_diagnostic_code::RuntimeBoundaryCode::ExactKeyBatchInputBytesExceeded,
        MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES + 1,
        MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES,
    );

    let mut stored = MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES;
    let stored_error = checked_add_bytes(
        &mut stored,
        1,
        MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES,
        InternalError::exact_key_batch_stored_bytes_exceeded,
    )
    .expect_err("stored row bytes above the cap should reject");
    assert_byte_bound(
        stored_error,
        icydb_diagnostic_code::RuntimeBoundaryCode::ExactKeyBatchStoredBytesExceeded,
        MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES + 1,
        MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES,
    );
}

#[test]
fn exact_key_result_bytes_charge_original_duplicate_positions() {
    let row = Some(vec![OutputValue::text(
        "x".repeat(MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES / 2),
    )]);
    let rows = vec![row];
    let mut budget = HardExecutionBudgetTracker::new(
        &EXACT_KEY_HARD_BUDGET,
        HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            DiagnosticExecutionLane::PublicRead,
            1,
        ),
    );
    let error = validate_logical_result_bytes(
        "Large",
        &["payload".to_string()],
        &rows,
        &[0, 0],
        &mut budget,
    )
    .expect_err("duplicate logical positions must each charge result bytes");
    assert!(matches!(
        error.diagnostic().detail(),
        Some(icydb_diagnostic_code::DiagnosticDetail::RuntimeBoundary {
            boundary: icydb_diagnostic_code::RuntimeBoundaryCode::ExactKeyBatchResultBytesExceeded,
        }),
    ));
    assert_eq!(
        error.diagnostic_facts()[1],
        (
            icydb_diagnostic_code::DiagnosticFactTag::Limit,
            MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES as u64,
        ),
    );
    assert!(
        budget.observed(DiagnosticExecutionBudgetResource::ResultBytes)
            > MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES as u64,
        "rejected result work remains charged even when the exact-key boundary is returned",
    );
}

#[test]
fn exact_key_result_sizing_uses_the_logical_limit_above_transport_size() {
    for payload_bytes in [
        icydb_schema::wire::MAX_REPLY_BYTES + 1,
        MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES,
    ] {
        let columns = ["payload".to_string()];
        let rows = [Some(vec![OutputValue::text("x".repeat(payload_bytes))])];
        let expected = icydb_schema::wire::encoded_len(&("Large", columns.as_slice())).unwrap()
            + icydb_schema::wire::encoded_len(&rows[0]).unwrap();
        let mut budget = HardExecutionBudgetTracker::new(
            &EXACT_KEY_HARD_BUDGET,
            HardExecutionContext::new(
                DiagnosticExecutionBudgetScope::Execution,
                DiagnosticExecutionLane::PublicRead,
                1,
            ),
        );
        let result = validate_logical_result_bytes("Large", &columns, &rows, &[0], &mut budget);
        if expected <= MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES {
            result.expect("native result admission is independent of transport delivery");
        } else {
            assert_byte_bound(
                result.expect_err("logical overflow must retain its typed boundary"),
                icydb_diagnostic_code::RuntimeBoundaryCode::ExactKeyBatchResultBytesExceeded,
                expected,
                MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES,
            );
        }
        assert_eq!(
            budget.observed(DiagnosticExecutionBudgetResource::ResultBytes),
            expected as u64,
        );
    }
}

#[test]
fn exact_key_lowering_preserves_composite_distinct_positions() {
    let composite = |tenant, local| {
        PrimaryKeyValue::Composite(
            crate::db::CompositePrimaryKeyValue::try_from_components(&[
                crate::db::PrimaryKeyComponent::Nat64(tenant),
                crate::db::PrimaryKeyComponent::Nat64(local),
            ])
            .expect("two non-Unit components should form a composite key"),
        )
    };
    let first = composite(7, 11);
    let second = composite(7, 12);
    let mut budget = HardExecutionBudgetTracker::new(
        &EXACT_KEY_HARD_BUDGET,
        HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            DiagnosticExecutionLane::PublicRead,
            2,
        ),
    );

    let entity = crate::types::EntityTag::new(9);
    // Reverse canonical order makes accidental map-order output visible.
    let inputs = [second, first, second];
    let lowered = lower_exact_keys(entity, &inputs, &mut budget)
        .expect("composite exact keys should lower through the concrete boundary");

    assert_eq!(lowered.positions, vec![0, 1, 0]);
    assert_eq!(lowered.distinct.len(), 2);
    for (position, expected) in inputs.iter().enumerate() {
        let (key, raw) = &lowered.distinct[lowered.positions[position] as usize];
        assert_eq!(key.primary_key_value(), *expected);
        assert_eq!(key.entity_tag(), entity);
        assert_eq!(
            raw,
            &DecodedDataStoreKey::new(entity, expected).to_raw().unwrap()
        );
    }
    let repeated = vec![first; MAX_TYPED_EXACT_KEY_BATCH_ITEMS];
    let lowered = lower_exact_keys(entity, &repeated, &mut budget).unwrap();
    assert_eq!(lowered.distinct.len(), 1);
    assert_eq!(lowered.positions, vec![0; repeated.len()]);
    let empty = lower_exact_keys(entity, &[], &mut budget).unwrap();
    assert!(empty.distinct.is_empty());
    assert!(empty.positions.is_empty());
}
